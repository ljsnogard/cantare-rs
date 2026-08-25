//! 环形核心：位置状态机 + 两个 hook 槽位。
//!
//! # 设计
//!
//! 与 [`crate::ring_buffer`] 相同的思路：读写位置（rp / wp）与两个关闭标志
//! （tx_closed / rx_closed）打包进**一个** `AtomicUsize`，单次原子加载即可看到
//! 全部状态，每次状态迁移是一个自旋 compare-exchange 循环；环形满 / 空用
//! 经典的单空槽方案区分（始终保留一个槽不用）：
//!
//! * `data = (wp - rp) mod cap`
//! * `free = cap - 1 - data`
//!
//! 核心还持有**两个 hook**（见 [`super::hook_`]）：状态提交（写入 / 读取推进、
//! 关闭）后触发对端 hook。被动 hook 唤醒等待者；主动 hook 同步泵设备。
//!
//! # 主动泵的同步驱动（不 spawn）
//!
//! 主动 hook 在提交路径（`&self`）上同步搬运设备数据：用
//! [`super::hook_::block_on`] 把设备的 `read_async` / `write_async` 轮询到
//! 完成。泵自身的提交会再次触发对端 hook，为避免「输入泵 → 输出泵 → 输入泵
//! → …」的无界递归，泵采用**待办标志 + 单层 `drive()` 循环**收敛：
//!
//! * hook 只设置待办标志并调用 `drive()`；
//! * `drive()` 有重入保护（`pumping` 标志）：已在泵中时直接返回，由最外层
//!   的循环把全部待办泵执行完毕。
//!
//! # 线程安全（只使用原子）
//!
//! 全部共享状态是原子（状态字、唤醒槽位、泵标志）。缓冲内存与主动设备经
//! 内部可变性（裸指针）访问，其正确性由以下**调用者义务**保证（与
//! `ring_buffer` 的 SPSC 约定一致）：
//!
//! * 至多一个生产线程、一个消费线程；
//! * 主动泵只在其触发线程上执行（被动端在哪一侧，泵就在哪一侧的线程上）；
//! * 活段（写段 / 读段）与泵的操作不重叠。
//!
//! 基于这些约定，[`RingCore`] 无条件实现 `Send + Sync`（见文件末尾的安全
//! 说明）。

use core::{
    cell::UnsafeCell, marker::PhantomData, mem::MaybeUninit, ptr, slice, sync::atomic::{AtomicPtr, AtomicUsize, Ordering}, task::Waker,
};

use abs_buff::Demand;
use atomex::AtomicFlags;
use atomic_sync::{
    mutex::preemptive::{SpinningMutexOwned, MutexGuard},
    x_deps::atomex,
};

use super::{
    abs_comp::{
        ConsumerHookEvent, ProducerHookEvent,
        TrConsumer, TrProducer, TrCircBuffCore,
    },
    error_::{RxError, TxError},
    reclaim_::{ReclSliceMut, ReclSliceRef, ReaderReclaim, WriterReclaim}
};

// ---------------------------------------------------------------------------
// 状态字布局
// ---------------------------------------------------------------------------

/// 保留高8位作为状态字
const RSV_BITS: u32 = 8;

/// 生产者（写端）已关闭。
const TX_CLOSED: usize = 1usize << (usize::BITS - 1);
/// 消费者（读端）已关闭。
const RX_CLOSED: usize = 1usize << (usize::BITS - 2);
/// 生产者（写端）可以接收 ProducerHookEvent
const TX_STNDBY: usize = 1usize << (usize::BITS - 3);
/// 消费者（读端）可以接收 ConsumerHookEvent
const RX_STNDBY: usize = 1usize << (usize::BITS - 4);

/// 两个关闭标志的掩码。
const FLAG_MASK: usize = TX_CLOSED | RX_CLOSED | TX_STNDBY | RX_STNDBY;
/// 每个位置占用的位数（两个位置共享低位，两个标志占高位）。
const POS_BITS: u32 = (usize::BITS - RSV_BITS) / 2;
/// 位置掩码。
const POS_MASK: usize = (1usize << POS_BITS) - 1;

pub(super) const MIN_CAPACITY: usize = 2;
/// 环形缓冲的最大容量（与 `ring_buffer` 的 `MAX_CAPACITY` 同量级）。
pub(super) const MAX_CAPACITY: usize = POS_MASK;

#[inline]
fn unpack(state: usize) -> (usize, usize) {
    (state & POS_MASK, (state >> POS_BITS) & POS_MASK)
}

#[inline]
fn pack(rp: usize, wp: usize) -> usize {
    rp | (wp << POS_BITS)
}

#[inline]
fn has_flag(state: usize, flag: usize) -> bool {
    state & flag != 0
}

// ---------------------------------------------------------------------------
// 唤醒槽位
// ---------------------------------------------------------------------------

/// 等待者：注册进唤醒槽位的 waker。活在等待 future（或被等待方）内部，
/// 注册期间不得移动，因此由槽位以裸指针引用。
pub(super) struct Waiter {
    pub waker: Option<Waker>,
}

impl Waiter {
    pub const fn new() -> Self {
        Waiter { waker: None }
    }
}

/// 唤醒槽位：同一时刻至多一个等待者（SPSC 保证每侧至多一个等待的 future）。
pub(super) struct WakeSlot(AtomicPtr<Waiter>);

impl WakeSlot {
    pub const fn new() -> Self {
        WakeSlot(AtomicPtr::new(ptr::null_mut()))
    }

    /// 把 `w` 注册为当前等待者。若仍有（陈旧的）等待者未注销则自旋等待其
    /// 注销（等待者在完成或 drop 时必然注销，因此自旋必然终止）。
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
                Err(cur) if cur == p => return, // 已由我们注册
                Err(_) => core::hint::spin_loop(),
            }
        }
    }

    /// 若 `w` 仍是当前注册的等待者，则注销之。
    pub fn deregister(&self, w: &Waiter) {
        let p = w as *const Waiter as *mut Waiter;
        let _ = self
            .0
            .compare_exchange(p, ptr::null_mut(), Ordering::AcqRel, Ordering::Acquire);
    }

    /// 唤醒当前注册的等待者（若有），并清空槽位。
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

// ---------------------------------------------------------------------------
// 环形核心
// ---------------------------------------------------------------------------

/// 环形核心：一个原子状态字 + 缓冲基址 + 两个 hook + 泵状态。
/// 必须保证 Send + Sync,
pub(super) struct CircCore<P, C, T = u8>
where
    P: TrProducer<Data = T>,
    C: TrConsumer<Data = T>,
{
    /// `rp`（低 `POS_BITS` 位）| `wp`（次 `POS_BITS` 位）| 两个关闭标志（高位）。
    atm_stat_: AtomicFlags<usize>,
    capacity_: usize,

    /// 环形缓冲基址（统一 `[MaybeUninit<T>]` 视图）。有效性由持有本核心的
    /// `CircularBuff` 的借用期保证（见模块文档的安全说明）。
    buf_base_: *mut MaybeUninit<T>,
    producer_: UnsafeCell<P>,
    consumer_: UnsafeCell<C>,
    _unuse_t_: PhantomData<fn() -> T>,
}

impl<P, C, T> CircCore<P, C, T>
where
    P: TrProducer<Data = T>,
    C: TrConsumer<Data = T>,
    // T: 'static,
{
    /// 构造核心：状态归零，挂载两个 hook。
    ///
    /// # Safety
    ///
    /// `buffer` 必须指向一段长度 `capacity` 的 `[MaybeUninit<T>]`，且在本核心
    /// 存活期间有效（由调用方保证，通常是 `CircularBuff` 的存储借用）。
    pub(super) fn new(
        buf_base: *mut MaybeUninit<T>,
        capacity: usize,
        producer: P,
        consumer: C,
    ) -> Self {
        CircCore {
            atm_stat_: AtomicFlags::new(AtomicUsize::new(0usize)),
            capacity_: capacity,
            buf_base_: buf_base,
            producer_: UnsafeCell::new(producer),
            consumer_: UnsafeCell::new(consumer),
            _unuse_t_: PhantomData,
        }
    }

    // ------------------------------------------------------------------
    // 状态查询
    // ------------------------------------------------------------------

    #[inline]
    pub(super) fn capacity(&self) -> usize {
        self.capacity_
    }

    /// 当前可读数据量。
    #[inline]
    pub(super) fn data_size(&self) -> usize {
        let (rp, wp) = unpack(self.atm_stat_.value());
        self.data_(rp, wp)
    }

    /// 当前可写空间量。
    #[inline]
    pub(super) fn free_size(&self) -> usize {
        let (rp, wp) = unpack(self.atm_stat_.value());
        self.free_(rp, wp)
    }

    #[inline]
    pub(super) fn is_tx_closed(&self) -> bool {
        has_flag(self.atm_stat_.value(), TX_CLOSED)
    }

    #[inline]
    pub(super) fn is_rx_closed(&self) -> bool {
        has_flag(self.atm_stat_.value(), RX_CLOSED)
    }

    #[inline]
    fn data_(&self, rp: usize, wp: usize) -> usize {
        (wp + self.capacity_ - rp) % self.capacity_
    }

    /// 可写空间；单空槽方案始终保留一个槽不用。
    #[inline]
    fn free_(&self, rp: usize, wp: usize) -> usize {
        self.capacity_ - 1 - self.data_(rp, wp)
    }

    // ------------------------------------------------------------------
    // 区域借出（尊重 Demand 语义）
    // ------------------------------------------------------------------

    /// 借出可写区，返回 `(start, take)`。
    ///
    /// 尊重 `Demand` 的 `[min, max]` 区间：**可写空间不足下限时不返回**（返回
    /// `Stuffed`），满足时最多借出 `max`。区域可能跨末端环绕（由段类型表达）。
    pub(super) fn try_write_at(&self, demand: &Demand<usize>) -> Result<(usize, usize), TxError<usize>> {
        let min_len = demand.min().copied().unwrap_or(0);
        let max_len = demand.max().copied().unwrap_or(usize::MAX);
        let state = self.atm_stat_.value();
        let (rp, wp) = unpack(state);
        let free = self.free_(rp, wp);
        if free == 0 || free < min_len {
            if has_flag(state, TX_CLOSED) {
                return Err(TxError::Closing);
            }
            return Err(TxError::Stuffed(wp));
        }
        let take = core::cmp::min(max_len, free);
        debug_assert!(take > 0 && take >= min_len);
        Ok((wp, take))
    }

    /// 借出可读区，返回 `(start, take)`。
    ///
    /// 尊重 `Demand` 的 `[min, max]` 区间：可读数据不足下限且未关闭时**不返回**
    /// （返回 `Drained`）；**EOF 例外**——写端已关闭（不再会有更多数据）时，
    /// 返回现有部分（可能不足下限）；读端已关闭或缓冲区已空时返回 `Closing` /
    /// `Drained`。
    pub(super) fn try_read_at(&self, demand: &Demand<usize>) -> Result<(usize, usize), RxError<usize>> {
        let min_len = demand.min().copied().unwrap_or(0);
        let max_len = demand.max().copied().unwrap_or(usize::MAX);
        let state = self.atm_stat_.value();
        let (rp, wp) = unpack(state);
        let data = self.data_(rp, wp);
        if data == 0 {
            if has_flag(state, TX_CLOSED) || has_flag(state, RX_CLOSED) {
                return Err(RxError::Closing); // EOF：写端已关且读空
            }
            return Err(RxError::Drained(rp));
        }
        if data < min_len && !has_flag(state, TX_CLOSED) && !has_flag(state, RX_CLOSED) {
            return Err(RxError::Drained(rp)); // 不足下限且未关闭：等待更多
        }
        let take = core::cmp::min(max_len, data);
        debug_assert!(take > 0);
        Ok((rp, take))
    }

    /// 写者可以继续的条件（供等待 future 的 park 检查）：
    /// 可写空间 ≥ min，或写端已关闭（返回 `Closing` 不再等待）。
    pub(super) fn producer_ready(&self, min: usize) -> bool {
        self.try_write_at(&Demand::at_least(min.max(1))).is_ok() || self.is_tx_closed()
    }

    /// 读者可以继续的条件：可读数据 ≥ min（或 EOF 有部分数据），或任一端关闭。
    pub(super) fn consumer_ready(&self, min: usize) -> bool {
        self.try_read_at(&Demand::at_least(min.max(1))).is_ok()
            || self.is_tx_closed()
            || self.is_rx_closed()
    }

    // ------------------------------------------------------------------
    // 段构建
    // ------------------------------------------------------------------

    /// 构建写段：覆盖 `[start, start+take)`，跨末端时拆成两段物理空间。
    pub(super) fn write_segm<'s>(&'s self, start: usize, take: usize) -> super::segm_::WrSegm<'s, T> {
        // SAFETY: `start`/`take` 来自 `try_write_at`，区域在缓冲内；可写区与
        // 其他活段 / 泵操作不重叠是调用者义务（SPSC）。
        let whole: &'s mut [MaybeUninit<T>] = self.buffer_view_mut();
        let first = core::cmp::min(take, self.capacity_ - start);
        let pieces = if first < take {
            let (head, tail) = whole.split_at_mut(start);
            let b = &mut head[..take - first];
            super::segm_::PiecesMut::Two(tail, b)
        } else {
            super::segm_::PiecesMut::One(&mut whole[start..start + take])
        };
        super::segm_::WrSegm::new(pieces, super::segm_::CommitWrite::new(self))
    }

    /// 构建读段：覆盖 `[start, start+take)`，跨末端时拆成两段物理空间。
    pub(super) fn read_segm<'s>(&'s self, start: usize, take: usize) -> super::segm_::RdSegm<'s, T> {
        // SAFETY: 同 [`RingCore::write_segm`]。
        let base = self.buf_base_.cast::<T>();
        let first = core::cmp::min(take, self.capacity_ - start);
        let pieces = if first < take {
            let a = unsafe { slice::from_raw_parts(base.add(start), first) };
            let b = unsafe { slice::from_raw_parts(base, take - first) };
            super::segm_::PiecesRef::Two(a, b)
        } else {
            let a = unsafe { slice::from_raw_parts(base.add(start), take) };
            super::segm_::PiecesRef::One(a)
        };
        super::segm_::RdSegm::new(pieces, super::segm_::CommitRead::new(self))
    }

    // ------------------------------------------------------------------
    // 状态迁移（提交路径：推进位置 / 关闭，随后触发对端 hook）
    // ------------------------------------------------------------------

    /// 写提交：按已消费量推进写位置，触发消费端 hook。
    pub(super) fn advance_write(&self, amount: usize) {
        self.update_state(|s| {
            let (rp, wp) = unpack(s);
            debug_assert!(rp < self.capacity_ && wp < self.capacity_);
            pack(rp, (wp + amount) % self.capacity_) | (s & FLAG_MASK)
        });
        if self.is_tx_closed() {
            self.fire_consumer(ConsumerHookEvent::ProducerClose(self.data_size()));
        } else {
            self.fire_consumer(ConsumerHookEvent::Available(self.data_size()));
        }
    }

    /// 读提交：按已消费量推进读位置，触发生产端 hook。
    pub(super) fn advance_read(&self, amount: usize) {
        self.update_state(|s| {
            let (rp, wp) = unpack(s);
            debug_assert!(rp < self.capacity_ && wp < self.capacity_);
            pack((rp + amount) % self.capacity_, wp) | (s & FLAG_MASK)
        });
        if self.is_rx_closed() {
            self.fire_producer(ProducerHookEvent::ConsumerClose(self.free_size()));
        } else {
            self.fire_producer(ProducerHookEvent::Available(self.free_size()));
        }
    }

    /// 关闭写端：不再接受写入，触发消费端 hook（`ProducerClose`）。
    pub(super) fn close_tx(&self) {
        self.set_flag(TX_CLOSED);
        self.fire_consumer(ConsumerHookEvent::ProducerClose(self.data_size()));
    }

    /// 关闭读端：不再读取，触发生产端 hook（`ConsumerClose`）。
    pub(super) fn close_rx(&self) {
        self.set_flag(RX_CLOSED);
        self.fire_producer(ProducerHookEvent::ConsumerClose(self.free_size()));
    }

    // ------------------------------------------------------------------
    // hook 分发与泵
    // ------------------------------------------------------------------

    /// 触发生产端 hook（消费端完成读取 / 关闭后）。
    fn fire_producer(&self, event: ProducerHookEvent) -> Option<MutexGuard<'_, P>> {
        const MAX_TRY: usize = 3;
        let mut acq = self.producer_.acquire();
        let mut cnt = 0usize;
        while cnt < MAX_TRY {
            let Option::Some(mut guard) = acq.try_lock() else {
                cnt += 1;
                continue;
            };
            if guard.check(event) {
                return Option::Some(guard);
            }
        }
        Option::None
    }

    /// 触发消费端 hook（生产端完成写入 / 关闭后）。
    fn fire_consumer(&self, event: ConsumerHookEvent) -> Option<MutexGuard<'_, '_, C>> {
        const MAX_TRY: usize = 3;
        let mut acq = self.consumer_.acquire();
        let mut cnt = 0usize;
        while cnt < MAX_TRY {
            let Option::Some(mut guard) = acq.try_lock() else {
                cnt += 1;
                continue;
            };
            if guard.check(event) {
                return Option::Some(guard);
            }
        }
        Option::None
    }

    /// 生产端是否为被动模式（对外可访问）。
    pub(super) fn producer_is_passive(&self) -> bool {
        matches!(self.producer_hook, ProducerHook::Passive(_))
    }

    /// 消费端是否为被动模式（对外可访问）。
    pub(super) fn consumer_is_passive(&self) -> bool {
        matches!(self.consumer_hook, ConsumerHook::Passive(_))
    }

    /// 被动生产端 hook 的唤醒槽位（只有被动模式存在写者等待）。
    pub(super) fn producer_wake_slot(&self) -> &WakeSlot {
        match &self.producer_hook {
            ProducerHook::Passive(slot) => slot,
            // 主动生产端没有写者等待：半部只会在被动模式下被创建。
            ProducerHook::Active(_) => unreachable!("主动生产端没有唤醒槽位"),
        }
    }

    /// 被动消费端 hook 的唤醒槽位。
    pub(super) fn consumer_wake_slot(&self) -> &WakeSlot {
        match &self.consumer_hook {
            ConsumerHook::Passive(slot) => slot,
            ConsumerHook::Active(_) => unreachable!("主动消费端没有唤醒槽位"),
        }
    }

    /// 构建期初始驱动：主动端先各自泵一轮，让数据开始流动。
    pub(super) fn start(&self) {
        if matches!(self.producer_hook, ProducerHook::Active(_)) {
            self.input_pending.store(true, Ordering::Release);
        }
        if matches!(self.consumer_hook, ConsumerHook::Active(_)) {
            self.output_pending.store(true, Ordering::Release);
        }
        self.drive();
    }

    /// 驱动泵：单层循环收敛所有待办泵（避免 hook 递归）。
    fn drive(&self) {
        if self.pumping.swap(true, Ordering::AcqRel) {
            return; // 已在泵中：待办标志由最外层循环处理
        }
        loop {
            let in_p = self.input_pending.swap(false, Ordering::AcqRel);
            let out_p = self.output_pending.swap(false, Ordering::AcqRel);
            if !in_p && !out_p {
                break;
            }
            if in_p {
                self.pump_input();
            }
            if out_p {
                self.pump_output();
            }
        }
        self.pumping.store(false, Ordering::Release);
    }

    /// 输入泵：从输入设备读入可写区（一次读一段物理连续的可写区），直到
    /// 写满或设备暂无数据。
    fn pump_input(&self) {
        loop {
            let state = self.atm_stat_.load(Ordering::Acquire);
            let (rp, wp) = unpack(state);
            let free = self.free_(rp, wp);
            // 无空间、写端关闭、或消费者已关闭（泵进去也没人消费）则停止。
            if free == 0 || has_flag(state, TX_CLOSED) || has_flag(state, RX_CLOSED) {
                break;
            }
            let take = core::cmp::min(free, self.capacity_ - wp);
            // SAFETY: 可写区不与任何活段重叠（泵运行在提交之后，SPSC 纪律）。
            let dst = unsafe { slice::from_raw_parts_mut(self.buf_base_.add(wp), take) };
            let ProducerHook::Active(input) = &self.producer_hook else {
                unreachable!("被动模式不进入泵");
            };
            let n = input.pump_into(dst);
            if n == 0 {
                break; // 设备暂无数据（或错误）
            }
            self.advance_write(n);
        }
    }

    /// 输出泵：把可读区（一次一段物理连续的部分）同步写入输出设备，直到
    /// 排空或设备暂不能接收。
    fn pump_output(&self) {
        loop {
            let state = self.atm_stat_.value();
            let (rp, wp) = unpack(state);
            let data = self.data_(rp, wp);
            if data == 0 || has_flag(state, RX_CLOSED) {
                break;
            }
            let take = core::cmp::min(data, self.capacity_ - rp);
            // SAFETY: 可读区为已初始化数据；不与活段重叠（同 `pump_input`）。
            let src = unsafe { slice::from_raw_parts(self.buf_base_.add(rp), take) };
            let ConsumerHook::Active(output) = &self.consumer_hook else {
                unreachable!("被动模式不进入泵");
            };
            let n = output.pump_from(src);
            if n == 0 {
                break; // 设备暂不能接收（或错误）
            }
            self.advance_read(n);
        }
    }

    // ------------------------------------------------------------------
    // 原子辅助
    // ------------------------------------------------------------------

    /// 自旋 compare-exchange 循环：把状态字替换为 `f(state)`。
    fn update_state(&self, desire: impl Fn(usize) -> usize) {
        let expect = |_| true;
        self.atm_stat_
            .try_spin_compare_exchange_weak(expect, desire);
    }

    fn set_flag(&self, flag: usize) {
        self.update_state(|s| s | flag);
    }

    /// 整块缓冲的可变视图（内部可变性：由 SPSC 借用纪律保证不与活段重叠）。
    #[allow(clippy::mut_from_ref)]
    fn buffer_view_mut<'s>(&'s self) -> &'s mut [MaybeUninit<T>] {
        unsafe { slice::from_raw_parts_mut(self.buf_base_, self.capacity_) }
    }
}

impl<P, C, T> TrCircBuffCore for CircCore<P, C, T>
where
    P: Send + Sync + TrProducer<Data = T>,
    C: Send + Sync + TrConsumer<Data = T>,
    T: Send + Sync + 'static,
{
    type Data = T;

    fn advance_read(&self, amount: usize) {
        CircCore::advance_read(self, amount);
    }

    fn advance_write(&self, amount: usize) {
        CircCore::advance_write(self, amount);
    }
}

// ---------------------------------------------------------------------------
// 线程安全
// ---------------------------------------------------------------------------

unsafe impl<P, C, T> Send for CircCore<P, C, T>
where
    P: Send + TrProducer<Data = T>,
    C: Send + TrConsumer<Data = T>,
    T: Send,
{}

unsafe impl<P, C, T> Sync for CircCore<P, C, T>
where
    P: Send + Sync + TrProducer<Data = T>,
    C: Send + Sync + TrConsumer<Data = T>,
    T: Send + Sync,
{}
