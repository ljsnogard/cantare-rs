use core::{
    borrow::{Borrow, BorrowMut},
    error::Error,
    fmt,
    future::Future,
    marker::PhantomPinned,
    mem::MaybeUninit,
    ops::Try,
    pin::Pin,
    slice,
    task::{Context, Poll},
};

use abs_cancel::{TrCancellationToken, TrMayCancel};
use anylr::SomeOf;

use crate::{
    Demand, TrBuffRead, TrBuffTryRead, TrBuffTryWrite, TrBuffWrite,
    buffer::{
        SegmMut, SegmReclaim, SegmRef, TrBuffSegmMut, TrBuffSegmRef,
        TrBuffSegmView,
    },
};

/// Error returned when a borrowed byte slice is empty.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BorrowedSliceError {
    Empty,
}

impl fmt::Display for BorrowedSliceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BorrowedSliceError::Empty => {
                write!(f, "borrowed byte slice is empty")
            }
        }
    }
}

impl Error for BorrowedSliceError {}

/// A simple immediately-ready `TrMayCancel` future carrying a `SomeOf`.
struct ReadySegm<S, E>(Option<SomeOf<S, E>>);

impl<S, E> ReadySegm<S, E> {
    fn new(value: SomeOf<S, E>) -> Self {
        ReadySegm(Option::Some(value))
    }
}

impl<S, E> Future for ReadySegm<S, E> {
    type Output = SomeOf<S, E>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        Poll::Ready(this.0.take().expect("a ready future must be polled once"))
    }
}

impl<'f, S: 'f, E: 'f> TrMayCancel<'f> for ReadySegm<S, E> {
    type MayCancelFuture<'g, C>
        = ReadySegm<S, E>
    where
        Self: 'g,
        C: TrCancellationToken + Clone,
        C: 'f,
        C: 'g,
        'g: 'f;
    type MayCancelOutput = SomeOf<S, E>;

    fn may_cancel_with<'g, C>(
        self,
        _cancel: &'g mut C,
    ) -> Self::MayCancelFuture<'g, C>
    where
        Self: 'g,
        'g: 'f,
        C: TrCancellationToken + Clone,
    {
        self
    }
}

// ---------------------------------------------------------------------------
// Advancing a borrowed slice when a segment is reclaimed.
//
// The blanket implementations cover every `Borrow<[u8]>` / `BorrowMut<[u8]>`
// type.  The default does not mutate the underlying value, while the slice
// reference specializations mirror `std::io::Read for &[u8]` and
// `std::io::Write for &mut [u8]` by advancing the reference itself.
// ---------------------------------------------------------------------------

trait TrReadAdvance {
    fn advance_slice(&mut self, amount: usize);
}

impl<T> TrReadAdvance for T
where
    T: Borrow<[u8]>,
{
    default fn advance_slice(&mut self, _amount: usize) {}
}

impl TrReadAdvance for &[u8] {
    fn advance_slice(&mut self, amount: usize) {
        let old = *self;
        *self = &old[amount..];
    }
}

impl TrReadAdvance for &mut [u8] {
    fn advance_slice(&mut self, amount: usize) {
        let old = core::mem::take(self);
        *self = &mut old[amount..];
    }
}

trait TrWriteAdvance {
    fn advance_slice(&mut self, amount: usize);
}

impl<T> TrWriteAdvance for T
where
    T: BorrowMut<[u8]>,
{
    default fn advance_slice(&mut self, _amount: usize) {}
}

impl TrWriteAdvance for &mut [u8] {
    fn advance_slice(&mut self, amount: usize) {
        let old = core::mem::take(self);
        *self = &mut old[amount..];
    }
}

fn advance_read<T>(src: &mut T, amount: usize)
where
    T: Borrow<[u8]>,
{
    TrReadAdvance::advance_slice(src, amount);
}

fn advance_write<T>(dst: &mut T, amount: usize)
where
    T: BorrowMut<[u8]>,
{
    TrWriteAdvance::advance_slice(dst, amount);
}

// ---------------------------------------------------------------------------
// Read segment over a `T: Borrow<[u8]>`
// ---------------------------------------------------------------------------

pub struct BorrowedReadSegm<'a, T>
where
    T: Borrow<[u8]>,
{
    src: &'a mut T,
    offset: usize,
    end: usize,
    _pinned: PhantomPinned,
}

impl<'a, T> BorrowedReadSegm<'a, T>
where
    T: Borrow<[u8]>,
{
    fn with_limit(src: &'a mut T, max: Option<usize>) -> Self {
        let len = Borrow::<[u8]>::borrow(&*src).len();
        let end = match max {
            Option::Some(m) if m < len => m,
            _ => len,
        };
        BorrowedReadSegm {
            src,
            offset: 0,
            end,
            _pinned: PhantomPinned,
        }
    }

    fn remaining(&self) -> &[u8] {
        &Borrow::<[u8]>::borrow(&*self.src)[self.offset..self.end]
    }

    fn as_segm_ref<'f>(&'f mut self) -> SegmRef<'f, u8, SegmReclaim<'f>> {
        let data = &Borrow::<[u8]>::borrow(&*self.src)[self.offset..self.end];
        SegmRef::new(data, SegmReclaim::new(Pin::new(&mut self.offset)))
    }

    fn take_segm_ref<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> Option<SegmRef<'f, u8, SegmReclaim<'f>>> {
        let c = self.end - self.offset;
        if c == 0 {
            return Option::None;
        }
        let available = Demand::less_than(c);
        let agreement = demand.compromise(&available)?;
        let max_len = *agreement.max()?;
        let data = &Borrow::<[u8]>::borrow(&*self.src)
            [self.offset..self.offset + max_len];
        let reclaim = SegmReclaim::new(Pin::new(&mut self.offset));
        Option::Some(SegmRef::new(data, reclaim))
    }
}

impl<T> Drop for BorrowedReadSegm<'_, T>
where
    T: Borrow<[u8]>,
{
    fn drop(&mut self) {
        advance_read(&mut *self.src, self.offset);
    }
}

impl<T> TrBuffSegmView for BorrowedReadSegm<'_, T>
where
    T: Borrow<[u8]>,
{
    type Item = u8;

    #[inline]
    fn is_empty(&self) -> bool {
        self.remaining().is_empty()
    }

    #[inline]
    fn least_count(&self) -> usize {
        self.remaining().len()
    }

    fn iter_slices(&self) -> impl IntoIterator<Item = &[u8]> {
        let data = self.remaining();
        if data.is_empty() {
            Option::None
        } else {
            Option::Some(data)
        }
    }
}

impl<'a, T> TrBuffSegmRef<'a, u8> for BorrowedReadSegm<'a, T>
where
    T: Borrow<[u8]>,
{
    type Reclaimer<'f>
        = SegmReclaim<'f>
    where
        Self: 'f;

    #[inline]
    fn take_segm_ref<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> impl Try<Output: TrBuffSegmRef<'f, u8>> {
        BorrowedReadSegm::take_segm_ref(self, demand)
    }

    #[inline]
    fn as_segm_ref<'f>(&'f mut self) -> SegmRef<'f, u8, Self::Reclaimer<'f>> {
        BorrowedReadSegm::as_segm_ref(self)
    }
}

// ---------------------------------------------------------------------------
// Write segment over a `T: BorrowMut<[u8]>`
// ---------------------------------------------------------------------------

pub struct BorrowedWriteSegm<'a, T>
where
    T: BorrowMut<[u8]>,
{
    dst: &'a mut T,
    offset: usize,
    end: usize,
    _pinned: PhantomPinned,
}

impl<'a, T> BorrowedWriteSegm<'a, T>
where
    T: BorrowMut<[u8]>,
{
    fn with_limit(dst: &'a mut T, max: Option<usize>) -> Self {
        let len = Borrow::<[u8]>::borrow(&*dst).len();
        let end = match max {
            Option::Some(m) if m < len => m,
            _ => len,
        };
        BorrowedWriteSegm {
            dst,
            offset: 0,
            end,
            _pinned: PhantomPinned,
        }
    }

    fn remaining(&self) -> &[MaybeUninit<u8>] {
        let bytes = &Borrow::<[u8]>::borrow(&*self.dst)[self.offset..self.end];
        // SAFETY: `MaybeUninit<u8>` has the same layout as `u8`, and the
        // slice lifetime is tied to the underlying borrowed bytes.
        unsafe {
            slice::from_raw_parts(
                bytes.as_ptr().cast::<MaybeUninit<u8>>(),
                bytes.len(),
            )
        }
    }

    fn as_segm_mut<'f>(&'f mut self) -> SegmMut<'f, u8, SegmReclaim<'f>> {
        let bytes = &mut BorrowMut::<[u8]>::borrow_mut(&mut *self.dst)
            [self.offset..self.end];
        // SAFETY: `MaybeUninit<u8>` has the same layout as `u8`, and the
        // mutable slice is exclusively borrowed from `T`.
        let data = unsafe {
            slice::from_raw_parts_mut(
                bytes.as_mut_ptr().cast::<MaybeUninit<u8>>(),
                bytes.len(),
            )
        };
        let p_offs = Pin::new(&mut self.offset);
        SegmMut::new(data, SegmReclaim::new(p_offs))
    }

    fn take_segm_mut<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> Option<SegmMut<'f, u8, SegmReclaim<'f>>> {
        let c = self.end - self.offset;
        if c == 0 {
            return Option::None;
        }
        let available = Demand::less_than(c);
        let agreement = demand.compromise(&available)?;
        let max_len = *agreement.max()?;
        let data = &mut BorrowMut::<[u8]>::borrow_mut(&mut *self.dst)
            [self.offset..self.offset + max_len];
        let data = unsafe {
            slice::from_raw_parts_mut(
                data.as_mut_ptr().cast::<MaybeUninit<u8>>(),
                data.len(),
            )
        };
        let reclaim = SegmReclaim::new(Pin::new(&mut self.offset));
        Option::Some(SegmMut::new(data, reclaim))
    }
}

impl<T> Drop for BorrowedWriteSegm<'_, T>
where
    T: BorrowMut<[u8]>,
{
    fn drop(&mut self) {
        advance_write(&mut *self.dst, self.offset);
    }
}

impl<T> TrBuffSegmView for BorrowedWriteSegm<'_, T>
where
    T: BorrowMut<[u8]>,
{
    type Item = MaybeUninit<u8>;

    #[inline]
    fn is_empty(&self) -> bool {
        self.remaining().is_empty()
    }

    #[inline]
    fn least_count(&self) -> usize {
        self.remaining().len()
    }

    fn iter_slices(&self) -> impl IntoIterator<Item = &[MaybeUninit<u8>]> {
        let data = self.remaining();
        if data.is_empty() {
            Option::None
        } else {
            Option::Some(data)
        }
    }
}

impl<'a, T> TrBuffSegmMut<'a, u8> for BorrowedWriteSegm<'a, T>
where
    T: BorrowMut<[u8]>,
{
    type Reclaimer<'f>
        = SegmReclaim<'f>
    where
        Self: 'f;

    #[inline]
    fn take_segm_mut<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> impl Try<Output: TrBuffSegmMut<'f, u8>> {
        BorrowedWriteSegm::take_segm_mut(self, demand)
    }

    #[inline]
    fn as_segm_mut<'f>(&'f mut self) -> SegmMut<'f, u8, Self::Reclaimer<'f>> {
        BorrowedWriteSegm::as_segm_mut(self)
    }
}

// ---------------------------------------------------------------------------
// Blanket impls
// ---------------------------------------------------------------------------

impl<T> TrBuffRead<u8> for T
where
    T: Borrow<[u8]>,
{
    type SegmRef<'f>
        = BorrowedReadSegm<'f, T>
    where
        Self: 'f;
    type Err = BorrowedSliceError;

    #[inline]
    fn is_drained_closing(&self) -> bool {
        Borrow::<[u8]>::borrow(self).is_empty()
    }

    fn read_async<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> impl TrMayCancel<'f, MayCancelOutput = SomeOf<Self::SegmRef<'f>, Self::Err>>
    {
        let len = Borrow::<[u8]>::borrow(self).len();
        let min_len = demand.min().copied().unwrap_or(0);
        if len == 0 || len < min_len {
            return ReadySegm::new(SomeOf::new_right(
                BorrowedSliceError::Empty,
            ));
        }
        let max_len = demand.max().copied();
        ReadySegm::new(SomeOf::new_left(BorrowedReadSegm::with_limit(
            self, max_len,
        )))
    }
}

impl<T> TrBuffTryRead<u8> for T
where
    T: Borrow<[u8]>,
{
    #[inline]
    fn try_read<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> SomeOf<Self::SegmRef<'f>, Self::Err> {
        let len = Borrow::<[u8]>::borrow(self).len();
        let min_len = demand.min().copied().unwrap_or(0);
        if len == 0 || len < min_len {
            return SomeOf::new_right(BorrowedSliceError::Empty);
        }
        let max_len = demand.max().copied();
        SomeOf::new_left(BorrowedReadSegm::with_limit(self, max_len))
    }
}

impl<T> TrBuffWrite<u8> for T
where
    T: BorrowMut<[u8]>,
{
    type SegmMut<'f>
        = BorrowedWriteSegm<'f, T>
    where
        Self: 'f;
    type Err = BorrowedSliceError;

    #[inline]
    fn is_blocked_closing(&self) -> bool {
        Borrow::<[u8]>::borrow(self).is_empty()
    }

    fn write_async<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> impl TrMayCancel<'f, MayCancelOutput = SomeOf<Self::SegmMut<'f>, Self::Err>>
    {
        let len = Borrow::<[u8]>::borrow(self).len();
        let min_len = demand.min().copied().unwrap_or(0);
        if len == 0 || len < min_len {
            return ReadySegm::new(SomeOf::new_right(
                BorrowedSliceError::Empty,
            ));
        }
        let max_len = demand.max().copied();
        ReadySegm::new(SomeOf::new_left(BorrowedWriteSegm::with_limit(
            self, max_len,
        )))
    }
}

impl<T> TrBuffTryWrite<u8> for T
where
    T: BorrowMut<[u8]>,
{
    #[inline]
    fn try_write<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> SomeOf<Self::SegmMut<'f>, Self::Err> {
        let len = Borrow::<[u8]>::borrow(self).len();
        let min_len = demand.min().copied().unwrap_or(0);
        if len == 0 || len < min_len {
            return SomeOf::new_right(BorrowedSliceError::Empty);
        }
        let max_len = demand.max().copied();
        SomeOf::new_left(BorrowedWriteSegm::with_limit(self, max_len))
    }
}

#[cfg(test)]
mod tests_ {
    use super::*;

    #[test]
    fn read_borrowed_slice_advances_like_std() {
        let mut data: &[u8] = b"hello";

        let mut segm = data
            .try_read(&Demand::less_than(5))
            .pick_left()
            .expect("read should return a segment");
        let mut child = segm.as_segm_ref();
        let mut dst = [MaybeUninit::<u8>::uninit(); 2];
        let n = unsafe { child.move_items_to_buff(&mut dst) };
        assert_eq!(n, 2);
        drop(child);
        drop(segm);

        assert_eq!(data, b"llo");
        assert!(!data.is_drained_closing());
    }

    #[test]
    fn read_borrowed_mut_slice_advances() {
        let mut storage = [1u8, 2, 3, 4];
        let mut data: &mut [u8] = &mut storage;

        let mut segm = data
            .try_read(&Demand::less_than(4))
            .pick_left()
            .expect("read should return a segment");
        let mut child = segm.as_segm_ref();
        let mut dst = [MaybeUninit::<u8>::uninit(); 3];
        let n = unsafe { child.move_items_to_buff(&mut dst) };
        assert_eq!(n, 3);
        drop(child);
        drop(segm);

        assert_eq!(data, &[4u8][..]);
    }

    #[test]
    fn write_borrowed_mut_slice_advances_like_std() {
        let mut storage = [0u8; 5];
        {
            let mut data: &mut [u8] = &mut storage;

            let mut segm = data
                .try_write(&Demand::less_than(5))
                .pick_left()
                .expect("write should return a segment");
            let mut child = segm.as_segm_mut();
            let src = [
                MaybeUninit::new(b'a'),
                MaybeUninit::new(b'b'),
                MaybeUninit::new(b'c'),
            ];
            let n = unsafe { child.move_items_from_buff(&mut src.clone()) };
            assert_eq!(n, 3);
            drop(child);
            drop(segm);

            // `&mut [u8]` advances past the written prefix, exactly like
            // `std::io::Write for &mut [u8]`.
            assert_eq!(data, [0u8, 0]);
        }
        assert_eq!(&storage[..3], b"abc");
    }
}
