//! The core of the ring buffer: one heap buffer, reader/writer positions
//! packed into a single `AtomicUsize`.
//!
//! # Design
//!
//! `RingBuffer` exclusively owns **one** heap-allocated `[T]` buffer. The
//! concrete storage type is generic (`B: DerefMut<Target = [T]>`), so any
//! heap pointer such as `Box<[T]>` works.
//!
//! The reader position `rp` and the writer position `wp` are packed into a
//! single `AtomicUsize` (`rp` in the low half, `wp` in the high half), so a
//! single atomic load observes both positions:
//!
//! * `data = (wp - rp) mod cap` — the number of buffered items;
//! * `free = cap - 1 - data` — the number of free slots.
//!
//! The ring is **full** when `free == 0`, i.e. when the writer position is
//! immediately behind the reader position (`(wp + 1) mod cap == rp`); the
//! ring is **empty** when `wp == rp`. One slot is always left unused (the
//! classic single-gap scheme), which is what makes the full/empty states
//! distinguishable from the two packed positions alone.
//!
//! Because `rp` and `wp` each occupy half of the `AtomicUsize`, the buffer
//! length is limited to `2^(usize::BITS/2) - 1` items (e.g. `u32::MAX` on
//! 64-bit targets). This matches the vectored-IO requirement: each slice
//! submitted to the kernel fits in the native iovec size field.
//!
//! The readable region `[rp, rp+data)` and the writable region
//! `[wp, wp+free)` may wrap around the end of the buffer; they are exposed as
//! **two** slices (scatter/gather). The runtime side takes them as an iovec
//! pair and submits them to the kernel with a single `readv` / `writev`
//! syscall ([`RingBuffer::take_send_iovecs`],
//! [`RingBuffer::take_recv_iovecs`]).
//!
//! All state transitions are single-atomic, so the ring works from two
//! threads (one producer, one consumer) without any lock and without any
//! async-runtime dependency. Parking/waking uses a single waker slot per side
//! ([`DemandSlot`]).

use core::{
    mem::MaybeUninit,
    ops::DerefMut,
    ptr,
    sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering},
    task::{Context, Poll, Waker},
};

/// Number of bits reserved for each position.
const POS_BITS: u32 = usize::BITS / 2;
/// Mask for one position.
const POS_MASK: usize = (1usize << POS_BITS) - 1;

/// The maximum buffer length (also the maximum per-slice length).
pub const MAX_CAPACITY: usize = POS_MASK;

#[inline]
fn unpack(state: usize) -> (usize, usize) {
    (state & POS_MASK, (state >> POS_BITS) & POS_MASK)
}

#[inline]
fn pack(rp: usize, wp: usize) -> usize {
    rp | (wp << POS_BITS)
}

/// A waker slot: the ring only ever points at the *single* waiter that is
/// currently parked on a given side. SPSC guarantees at most one waiter per
/// slot.
pub(super) struct DemandSlot(AtomicPtr<Waiter>);

impl DemandSlot {
    pub const fn new() -> Self {
        DemandSlot(AtomicPtr::new(ptr::null_mut()))
    }

    /// Register `w` as the current waiter of this slot. Spins if another
    /// (stale) waiter is still registered; the previous waiter deregisters
    /// itself on completion or drop, so the spin always terminates.
    pub fn register(&self, w: &Waiter) {
        let p = w as *const Waiter as *mut Waiter;
        loop {
            match self.0.compare_exchange_weak(
                ptr::null_mut(),
                p,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(cur) if cur == p => return, // already registered by us
                Err(_) => core::hint::spin_loop(),
            }
        }
    }

    /// Remove `w` from this slot if it is still the registered waiter.
    pub fn deregister(&self, w: &Waiter) {
        let p = w as *const Waiter as *mut Waiter;
        let _ = self
            .0
            .compare_exchange(p, ptr::null_mut(), Ordering::AcqRel, Ordering::Acquire);
    }

    /// Wake the currently registered waiter, if any.
    pub fn signal(&self) {
        let p = self.0.swap(ptr::null_mut(), Ordering::AcqRel);
        if !p.is_null() {
            let w = unsafe { &*p };
            if let Some(waker) = w.waker.as_ref() {
                waker.wake_by_ref();
            }
        }
    }
}

/// The per-waiter state. Lives inside the parking future (or the half, for
/// the poll-based traits) and is referenced by the ring through a raw
/// pointer, so it must not move while registered.
pub(super) struct Waiter {
    pub waker: Option<Waker>,
}

impl Waiter {
    pub const fn new() -> Self {
        Waiter { waker: None }
    }
}

/// Which demand slot a park target registers into.
#[derive(Clone, Copy)]
pub(super) enum ParkSide {
    /// The user writer waits for free space.
    TxUser,
    /// The runtime waits for buffered data to send (writev).
    TxRuntime,
    /// The user reader waits for buffered data.
    RxUser,
    /// The runtime waits for free space to receive (readv).
    RxRuntime,
}

impl ParkSide {
    fn register<B, T>(self, ring: &RingBuffer<B, T>, w: &Waiter)
    where
        B: DerefMut<Target = [T]>,
    {
        match self {
            ParkSide::TxUser => ring.register_tx_user(w),
            ParkSide::TxRuntime => ring.register_tx_runtime(w),
            ParkSide::RxUser => ring.register_rx_user(w),
            ParkSide::RxRuntime => ring.register_rx_runtime(w),
        }
    }

    fn deregister<B, T>(self, ring: &RingBuffer<B, T>, w: &Waiter)
    where
        B: DerefMut<Target = [T]>,
    {
        match self {
            ParkSide::TxUser => ring.deregister_tx_user(w),
            ParkSide::TxRuntime => ring.deregister_tx_runtime(w),
            ParkSide::RxUser => ring.deregister_rx_user(w),
            ParkSide::RxRuntime => ring.deregister_rx_runtime(w),
        }
    }
}

/// Condition checked by a parked future before (re)registering its waker.
/// `arg` is a parameter (e.g. the requested borrow length).
pub(super) type ParkCheck<B, T> = fn(&RingBuffer<B, T>, usize) -> bool;

/// A parking helper: registers a single waker on the ring when the condition
/// does not hold yet. The ring signals the registered waker on every relevant
/// state change; the future re-checks the condition on wake-up.
pub(super) struct Park<B, T>
where
    B: DerefMut<Target = [T]>,
{
    waiter: Waiter,
    registered: bool,
    side: ParkSide,
    check: ParkCheck<B, T>,
}

impl<B, T> Park<B, T>
where
    B: DerefMut<Target = [T]>,
{
    pub const fn new(side: ParkSide, check: ParkCheck<B, T>) -> Self {
        Park {
            waiter: Waiter::new(),
            registered: false,
            side,
            check,
        }
    }

    /// Poll the park: if the condition holds, deregister and return `Ready`;
    /// otherwise register the waker and return `Pending`.
    ///
    /// The condition is re-checked *after* registering to close the
    /// lost-wakeup window: a state change that happens between the first
    /// check and the registration would otherwise signal nobody.
    pub fn poll(&mut self, cx: &mut Context<'_>, ring: &RingBuffer<B, T>, arg: usize) -> Poll<()> {
        if (self.check)(ring, arg) {
            self.deregister(ring);
            return Poll::Ready(());
        }
        self.waiter.waker = Some(cx.waker().clone());
        self.side.register(ring, &self.waiter);
        self.registered = true;
        if (self.check)(ring, arg) {
            self.deregister(ring);
            return Poll::Ready(());
        }
        Poll::Pending
    }

    pub fn deregister(&mut self, ring: &RingBuffer<B, T>) {
        if self.registered {
            self.side.deregister(ring, &self.waiter);
            self.registered = false;
        }
    }
}

/// A ring buffer between a user thread and a runtime (kernel) side. See the
/// [module docs](self) for the design.
pub struct RingBuffer<B, T = u8>
where
    B: DerefMut<Target = [T]>,
{
    /// The one heap buffer.
    buffer: B,
    /// `buffer.len()`.
    cap: usize,
    /// `rp` in the low half, `wp` in the high half.
    rw_pos: AtomicUsize,
    /// The readable region is reserved by the runtime for a kernel write.
    send_in_flight: AtomicBool,
    /// The writable region is reserved by the runtime for a kernel read.
    recv_in_flight: AtomicBool,
    tx_user_demand: DemandSlot,
    tx_runtime_demand: DemandSlot,
    rx_user_demand: DemandSlot,
    rx_runtime_demand: DemandSlot,
    tx_closed: AtomicBool,
    rx_closed: AtomicBool,
}

impl<B, T> RingBuffer<B, T>
where
    B: DerefMut<Target = [T]>,
{
    /// Create a ring buffer from one owned heap buffer.
    ///
    /// Returns `Err(len)` if the buffer is too large (longer than
    /// [`MAX_CAPACITY`]).
    pub fn try_new(buffer: B) -> Result<Self, usize> {
        let cap = buffer.len();
        if cap > MAX_CAPACITY {
            return Err(cap);
        }
        Ok(RingBuffer {
            buffer,
            cap,
            rw_pos: AtomicUsize::new(0),
            send_in_flight: AtomicBool::new(false),
            recv_in_flight: AtomicBool::new(false),
            tx_user_demand: DemandSlot::new(),
            tx_runtime_demand: DemandSlot::new(),
            rx_user_demand: DemandSlot::new(),
            rx_runtime_demand: DemandSlot::new(),
            tx_closed: AtomicBool::new(false),
            rx_closed: AtomicBool::new(false),
        })
    }

    /// The buffer length.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.cap
    }

    /// A snapshot of the number of buffered items.
    #[inline]
    pub fn data_size(&self) -> usize {
        let (rp, wp) = unpack(self.rw_pos.load(Ordering::Acquire));
        self.data_(rp, wp)
    }

    /// The number of free slots.
    #[inline]
    pub fn free_size(&self) -> usize {
        let (rp, wp) = unpack(self.rw_pos.load(Ordering::Acquire));
        self.free_(rp, wp)
    }

    #[inline]
    fn data_(&self, rp: usize, wp: usize) -> usize {
        (wp + self.cap - rp) % self.cap
    }

    /// Free slots; the single-gap scheme always keeps one slot unused.
    #[inline]
    fn free_(&self, rp: usize, wp: usize) -> usize {
        self.cap - 1 - self.data_(rp, wp)
    }

    /// The reader position.
    pub fn reader_pos(&self) -> usize {
        unpack(self.rw_pos.load(Ordering::Acquire)).0
    }

    /// The writer position.
    pub fn writer_pos(&self) -> usize {
        unpack(self.rw_pos.load(Ordering::Acquire)).1
    }

    pub fn is_tx_closed(&self) -> bool {
        self.tx_closed.load(Ordering::Acquire)
    }

    pub fn is_rx_closed(&self) -> bool {
        self.rx_closed.load(Ordering::Acquire)
    }

    /// Atomically advance the writer position by `amount` (mod `cap`).
    pub fn advance_write(&self, amount: usize) {
        self.update_pos(|rp, wp| (rp, (wp + amount) % self.cap));
        self.signal_all();
    }

    /// Atomically advance the reader position by `amount` (mod `cap`).
    pub fn advance_read(&self, amount: usize) {
        self.update_pos(|rp, wp| ((rp + amount) % self.cap, wp));
        self.signal_all();
    }

    /// The ring is shared by both the user pipe ends and the runtime drivers,
    /// so every state change potentially satisfies any parked side. Waking all
    /// four slots is cheap; the parked futures re-check their conditions.
    fn signal_all(&self) {
        self.tx_user_demand.signal();
        self.tx_runtime_demand.signal();
        self.rx_user_demand.signal();
        self.rx_runtime_demand.signal();
    }

    fn update_pos(&self, f: impl Fn(usize, usize) -> (usize, usize)) {
        let mut state = self.rw_pos.load(Ordering::Acquire);
        loop {
            let (rp, wp) = unpack(state);
            let (nr, nw) = f(rp, wp);
            debug_assert!(nr < self.cap && nw < self.cap);
            match self.rw_pos.compare_exchange_weak(
                state,
                pack(nr, nw),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(x) => state = x,
            }
        }
    }

    // ------------------------------------------------------------------
    // user side: write (contiguous borrow)
    // ------------------------------------------------------------------

    /// Borrow up to `length` contiguous writable items starting at `wp`.
    ///
    /// * `TxError::Stuffed` — the ring is full (or the writable region is
    ///   reserved by the runtime for a kernel read).
    /// * `TxError::Closing` — the tx end is closed.
    pub fn try_write_at(&self, length: usize) -> Result<(usize, usize), super::TxError<usize>> {
        use super::TxError;
        let (rp, wp) = unpack(self.rw_pos.load(Ordering::Acquire));
        let free = self.free_(rp, wp);
        if free == 0 || self.recv_in_flight.load(Ordering::Acquire) {
            if self.tx_closed.load(Ordering::Acquire) {
                return Err(TxError::Closing);
            }
            return Err(TxError::Stuffed(wp));
        }
        let take = core::cmp::min(length, core::cmp::min(free, self.cap - wp));
        debug_assert!(take > 0);
        Ok((wp, take))
    }

    // ------------------------------------------------------------------
    // user side: read (contiguous borrow)
    // ------------------------------------------------------------------

    /// Borrow up to `length` contiguous readable items starting at `rp`.
    pub fn try_read_at(&self, length: usize) -> Result<(usize, usize), super::RxError<usize>> {
        use super::RxError;
        let (rp, wp) = unpack(self.rw_pos.load(Ordering::Acquire));
        let data = self.data_(rp, wp);
        if data == 0 || self.send_in_flight.load(Ordering::Acquire) {
            if self.rx_closed.load(Ordering::Acquire) {
                return Err(RxError::Closing);
            }
            return Err(RxError::Drained(rp));
        }
        let take = core::cmp::min(length, core::cmp::min(data, self.cap - rp));
        debug_assert!(take > 0);
        Ok((rp, take))
    }

    /// Borrow all contiguous readable items starting at `rp` (for peeking).
    pub fn try_peek_at(&self) -> Result<(usize, usize), super::RxError<usize>> {
        use super::RxError;
        let (rp, wp) = unpack(self.rw_pos.load(Ordering::Acquire));
        let data = self.data_(rp, wp);
        if data == 0 || self.send_in_flight.load(Ordering::Acquire) {
            if self.rx_closed.load(Ordering::Acquire) {
                return Err(RxError::Closing);
            }
            return Err(RxError::Drained(rp));
        }
        let take = core::cmp::min(data, self.cap - rp);
        debug_assert!(take > 0);
        Ok((rp, take))
    }

    // ------------------------------------------------------------------
    // runtime side: kernel submission (scatter / gather)
    // ------------------------------------------------------------------

    /// Take the readable region as an iovec pair for a kernel `writev`.
    ///
    /// The readable region `[rp, rp+data)` is returned as one or two
    /// `&'static [T]` slices (the second is empty when the region does not
    /// wrap). While the region is reserved, the user reader is blocked
    /// (`RxError::Drained`). After the kernel completes, call
    /// [`RingBuffer::put_back_send`] with the number of bytes actually
    /// written, which advances the reader position.
    ///
    /// With `T = u8` the slices can be submitted to compio directly, e.g.
    /// `socket.write_vectored((a, b)).await` — a single syscall.
    pub fn take_send_iovecs(&self) -> Option<(&'static [T], &'static [T])> {
        let (rp, wp) = unpack(self.rw_pos.load(Ordering::Acquire));
        let data = self.data_(rp, wp);
        if data == 0 || self.send_in_flight.load(Ordering::Acquire) {
            return None;
        }
        if self
            .send_in_flight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return None;
        }
        let (a, b) = self.readable_slices_(rp, data);
        Some((a, b))
    }

    /// Return the reserved region after the kernel `writev` completed,
    /// advancing the reader position by `written`.
    pub fn put_back_send(&self, written: usize) {
        debug_assert!(self.send_in_flight.load(Ordering::Relaxed));
        self.send_in_flight.store(false, Ordering::Release);
        self.advance_read(written);
    }

    /// Take the writable region as an iovec pair for a kernel `readv`.
    ///
    /// The writable region `[wp, wp+free)` is returned as one or two
    /// `&'static mut [T]` slices. While reserved, the user writer is blocked
    /// (`TxError::Stuffed`). After the kernel completes, call
    /// [`RingBuffer::put_back_recv`] with the number of bytes actually read,
    /// which advances the writer position.
    pub fn take_recv_iovecs(&self) -> Option<(&'static mut [T], &'static mut [T])> {
        let (rp, wp) = unpack(self.rw_pos.load(Ordering::Acquire));
        let free = self.free_(rp, wp);
        if free == 0 || self.recv_in_flight.load(Ordering::Acquire) {
            return None;
        }
        if self
            .recv_in_flight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return None;
        }
        let (a, b) = self.writable_slices_(wp, free);
        Some((a, b))
    }

    /// Return the reserved region after the kernel `readv` completed,
    /// advancing the writer position by `received`.
    pub fn put_back_recv(&self, received: usize) {
        debug_assert!(self.recv_in_flight.load(Ordering::Relaxed));
        self.recv_in_flight.store(false, Ordering::Release);
        self.advance_write(received);
    }

    /// The readable region `[rp, rp+len)` as up to two slices.
    fn readable_slices_(&self, rp: usize, len: usize) -> (&'static [T], &'static [T]) {
        let base = self.buffer.as_ptr();
        let first = core::cmp::min(len, self.cap - rp);
        let a = unsafe { core::slice::from_raw_parts(base.add(rp), first) };
        let b = if first < len {
            unsafe { core::slice::from_raw_parts(base, len - first) }
        } else {
            &[]
        };
        (a, b)
    }

    /// The writable region `[wp, wp+len)` as up to two slices.
    fn writable_slices_(&self, wp: usize, len: usize) -> (&'static mut [T], &'static mut [T]) {
        let base = self.buffer.as_ptr().cast_mut();
        let first = core::cmp::min(len, self.cap - wp);
        let a = unsafe { core::slice::from_raw_parts_mut(base.add(wp), first) };
        let b = if first < len {
            unsafe { core::slice::from_raw_parts_mut(base, len - first) }
        } else {
            &mut []
        };
        (a, b)
    }

    /// A read view over the whole buffer (used by the framework adapters).
    #[inline]
    pub(super) fn buffer_ref(&self) -> &[T] {
        unsafe { core::slice::from_raw_parts(self.buffer.as_ptr(), self.cap) }
    }

    /// A write view over the whole buffer (used by the framework adapters).
    #[inline]
    pub(super) fn buffer_uninit(&self) -> &mut [MaybeUninit<T>] {
        unsafe {
            core::slice::from_raw_parts_mut(
                self.buffer.as_ptr().cast_mut().cast::<MaybeUninit<T>>(),
                self.cap,
            )
        }
    }

    /// A writable borrow exists right now (used by `TrBuffWrite::is_blocked`).
    pub(super) fn has_tx_space(&self) -> bool {
        self.try_write_at(1).is_ok()
    }

    /// Buffered data is available for a kernel `writev`.
    pub(super) fn has_tx_data(&self) -> bool {
        let (rp, wp) = unpack(self.rw_pos.load(Ordering::Acquire));
        self.data_(rp, wp) > 0 && !self.send_in_flight.load(Ordering::Acquire)
    }

    /// Free space is available for a kernel `readv`.
    pub(super) fn has_recv_space(&self) -> bool {
        let (rp, wp) = unpack(self.rw_pos.load(Ordering::Acquire));
        self.free_(rp, wp) > 0 && !self.recv_in_flight.load(Ordering::Acquire)
    }

    /// Borrow a write segment over `[start, start + take)`.
    pub(super) fn write_segm<'a>(
        &'a self,
        start: usize,
        take: usize,
    ) -> super::reclaim_::ReclSliceMut<'a, B, T> {
        let whole: &'a mut [MaybeUninit<T>] = self.buffer_uninit();
        let slice: &'a mut [MaybeUninit<T>] = &mut whole[start..start + take];
        segm_buff::SegmMut::new(
            slice,
            Option::Some(super::reclaim_::WriterReclaim::new(self)),
        )
    }

    /// Borrow a read segment over `[start, start + take)`.
    pub(super) fn read_segm<'a>(
        &'a self,
        start: usize,
        take: usize,
    ) -> super::reclaim_::ReclSliceRef<'a, B, T> {
        let whole: &'a [T] = self.buffer_ref();
        let slice: &'a [T] = &whole[start..start + take];
        segm_buff::SegmRef::new(
            slice,
            Option::Some(super::reclaim_::ReaderReclaim::new(self)),
        )
    }

    /// Borrow a peek segment (no reclaim) over `[start, start + take)`.
    pub(super) fn peek_segm<'a>(
        &'a self,
        start: usize,
        take: usize,
    ) -> super::reclaim_::ReclSliceRef<'a, B, T> {
        let whole: &'a [T] = self.buffer_ref();
        let slice: &'a [T] = &whole[start..start + take];
        segm_buff::SegmRef::new(slice, Option::None)
    }

    // ------------------------------------------------------------------
    // closing
    // ------------------------------------------------------------------

    /// Close the tx end: no more data will be written by the user.
    pub fn close_tx(&self) {
        self.tx_closed.store(true, Ordering::Release);
        self.signal_all();
    }

    /// Close the rx end: no more data will be read by the user.
    pub fn close_rx(&self) {
        self.rx_closed.store(true, Ordering::Release);
        self.signal_all();
    }

    // ------------------------------------------------------------------
    // waker registration helpers (used by the futures)
    // ------------------------------------------------------------------

    pub(super) fn register_tx_user(&self, w: &Waiter) {
        self.tx_user_demand.register(w);
    }
    pub(super) fn deregister_tx_user(&self, w: &Waiter) {
        self.tx_user_demand.deregister(w);
    }
    pub(super) fn register_tx_runtime(&self, w: &Waiter) {
        self.tx_runtime_demand.register(w);
    }
    pub(super) fn deregister_tx_runtime(&self, w: &Waiter) {
        self.tx_runtime_demand.deregister(w);
    }
    pub(super) fn register_rx_user(&self, w: &Waiter) {
        self.rx_user_demand.register(w);
    }
    pub(super) fn deregister_rx_user(&self, w: &Waiter) {
        self.rx_user_demand.deregister(w);
    }
    pub(super) fn register_rx_runtime(&self, w: &Waiter) {
        self.rx_runtime_demand.register(w);
    }
    pub(super) fn deregister_rx_runtime(&self, w: &Waiter) {
        self.rx_runtime_demand.deregister(w);
    }
}

// --- park conditions -----------------------------------------------------

/// The user writer can proceed if there is free space (and the runtime is not
/// receiving into the ring).
pub(super) fn check_tx_writable<B, T>(ring: &RingBuffer<B, T>, arg: usize) -> bool
where
    B: DerefMut<Target = [T]>,
{
    ring.try_write_at(arg.max(1)).is_ok()
}

/// The user reader can proceed if there is data (and the runtime is not
/// sending from the ring), or the rx end is closed.
pub(super) fn check_rx_readable<B, T>(ring: &RingBuffer<B, T>, arg: usize) -> bool
where
    B: DerefMut<Target = [T]>,
{
    ring.try_read_at(arg).is_ok() || ring.is_rx_closed()
}

/// Same as [`check_rx_readable`] for peeking.
pub(super) fn check_rx_peekable<B, T>(ring: &RingBuffer<B, T>, _: usize) -> bool
where
    B: DerefMut<Target = [T]>,
{
    ring.try_peek_at().is_ok() || ring.is_rx_closed()
}

/// The runtime can proceed if buffered data is available for a kernel write,
/// or the tx end is closed.
pub(super) fn check_tx_flushed<B, T>(ring: &RingBuffer<B, T>, _: usize) -> bool
where
    B: DerefMut<Target = [T]>,
{
    ring.has_tx_data() || ring.is_tx_closed()
}

/// The runtime can proceed if free space is available for a kernel read, or
/// the rx end is closed.
pub(super) fn check_rx_idle<B, T>(ring: &RingBuffer<B, T>, _: usize) -> bool
where
    B: DerefMut<Target = [T]>,
{
    ring.has_recv_space() || ring.is_rx_closed()
}

// SAFETY: all shared state is atomic; the buffer memory is only touched
// through the position state machine, so a `RingBuffer` can be shared between
// the user thread and the runtime thread.
unsafe impl<B, T> Send for RingBuffer<B, T>
where
    B: DerefMut<Target = [T]>,
    B: Send,
    T: Send,
{
}

unsafe impl<B, T> Sync for RingBuffer<B, T>
where
    B: DerefMut<Target = [T]>,
    B: Sync,
    T: Send + Sync,
{
}

impl<B, T> Drop for RingBuffer<B, T>
where
    B: DerefMut<Target = [T]>,
{
    fn drop(&mut self) {
        // A region reserved by the runtime is referenced by `&'static`
        // slices. Returning them (put_back_*) before the last reference to
        // the ring drops is part of the protocol; dropping the ring
        // otherwise would dangle those references.
        debug_assert!(
            !self.send_in_flight.load(Ordering::Relaxed)
                && !self.recv_in_flight.load(Ordering::Relaxed),
            "[RingBuffer::drop] a region is still reserved by the runtime"
        );
    }
}
