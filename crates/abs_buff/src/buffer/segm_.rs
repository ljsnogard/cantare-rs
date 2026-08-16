use core::{
    borrow::BorrowMut,
    cmp,
    marker::PhantomPinned,
    mem::MaybeUninit,
    ops::Try,
    ptr,
    slice,
};

use crate::Demand;

/// Represent a sequence of slices who are logically the same array but
/// physically not.
pub trait TrBuffSegmView {
    type Item: Sized;

    /// Returns true if no available items to consume, false otherwise.
    fn is_empty(&self) -> bool;

    /// The minimum count of unconsumed items. For a segment view that
    /// has only ONE PIECE, this is the unconsumed item count. For those
    /// have more than once slice, this is the unconsumed item count for
    /// the first slice.
    fn least_count(&self) -> usize;

    /// Iterate the unconsumed parts of the segment slice by slice.
    fn iter_slices(
        &self,
    ) -> impl IntoIterator<Item = &[Self::Item]>;
}

/// An instance to instantly tell the consumer usage of a buffer.
pub trait TrReclaim
where
    Self: Send + Sync,
{
    /// Indicate the reclaimer the amount of consumption, and returns the
    /// amount before the consumption.
    fn reclaim(&self, amount: usize) -> usize;
}

impl<F> TrReclaim for F
where
    F: Fn(usize) -> usize + Send + Sync,
{
    fn reclaim(&self, amount: usize) -> usize {
        let f = self;
        f(amount)
    }
}

/// A buffer that its data is organized with one or more slices
pub trait TrBuffSegmRef<'a, T>
where
    Self: TrBuffSegmView<Item = T>,
{
    type Reclaimer<'f>: TrReclaim where Self: 'f;

    /// Take a slice starting from the beginning of the unconsumed part, length
    /// suggested by the demand argument. Will reduce the length of the segment
    /// when the taken slice drops.
    ///
    /// The amount of the reducing will be the size of taken slice no matter if
    /// the items in it are actually moved or not. No drop. So this may leak.
    fn take_segm_ref<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> impl Try<Output: TrBuffSegmRef<'f, T>>;

    /// To end the evaluation of recursive downcast from TrBuffSegmRef.
    /// A `SegmRef<T>` can move items to a `SegmMut<T>`.
    fn as_segm_ref<'f>(&'f mut self) -> SegmRef<'f, T, Self::Reclaimer<'f>>;
}

/// A buffer that its data is organized with one or more slices mut.
pub trait TrBuffSegmMut<'a, T>
where
    Self: TrBuffSegmView<Item = MaybeUninit<T>>,
{
    type Reclaimer<'f>: TrReclaim where Self: 'f;

    /// Take a slice starting from the beginning of the unconsumed part, length
    /// suggested by the demand argument. Will reduce the length of the segment
    /// when the taken slice drops.
    ///
    /// The amount of the reducing will be the size of taken slice no matter if
    /// the items in it are actually moved or not. No drop. So this may leak.
    fn take_segm_mut<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> impl Try<Output: TrBuffSegmMut<'f, T>>;

    fn as_segm_mut<'f>(&'f mut self) -> SegmMut<'f, T, Self::Reclaimer<'f>>;
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// SegmReclaim,
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

pub struct SegmReclaim<'a>(&'a mut usize);

impl<'a> SegmReclaim<'a> {
    const fn new(p: &'a mut usize) -> Self {
        SegmReclaim(p)
    }
}

impl<'a> TrReclaim for SegmReclaim<'a> {
    #[inline]
    fn reclaim(&self, amount: usize) -> usize {
        // This safe if the one who creates this `SegmReclaim` guarantees that,
        // It is always created within a borrow mut context.
        unsafe {
            let p = self.0 as *const _ as *mut usize;
            let c = &mut *p;
            let x = *c;
            *c += amount;
            x
        }
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// SegmRef, SegmMut, declaration
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// A wrapper around a slice borrowed from a buffer and its reclaim function.
/// Designed for [RingBuffer](crate::ring_buffer::RingBuffer) but capable of
/// being a simple stream buffer to support the consuming semantics.
#[repr(C)]
pub struct SegmRef<'a, T, R>
where
    R: TrReclaim,
{
    buffer_: &'a mut [T],
    offset_: usize,
    reclaim_: Option<R>,
    _pinned_: PhantomPinned,
}

pub struct SegmMut<'a, T, R>
where
    R: TrReclaim,
{
    buffer_: &'a mut [MaybeUninit<T>],
    offset_: usize,
    reclaim_: Option<R>,
    _pinned_: PhantomPinned,
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// SegmRef, SegmMut, implementation
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

impl<'a, T, R> SegmRef<'a, T, R>
where
    R: TrReclaim,
{
    /// Create by borrowing a slice from an implicit source. And the items of
    /// this slice will be returned back to or moved out of the source by
    /// `reclaim`.
    pub const fn new(
        buffer: &'a mut [T],
        reclaim: R,
    ) -> Self {
        SegmRef {
            buffer_: buffer,
            offset_: 0usize,
            reclaim_: Option::Some(reclaim),
            _pinned_: PhantomPinned,
        }
    }

    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.least_count() == 0
    }

    #[inline]
    pub const fn least_count(&self) -> usize {
        self.buffer_.len()
    }

    pub const fn iter_slices(&self) -> Option<&[T]> {
        if self.is_empty() {
            Option::None
        } else {
            Option::Some(self.buffer_)
        }
    }

    pub fn as_segm_ref<'f>(&'f mut self) -> SegmRef<'f, T, SegmReclaim<'f>> {
        let buffer = &mut self.buffer_[self.offset_..];
        let reclaim = SegmReclaim::new(&mut self.offset_);
        SegmRef::new(buffer, reclaim)
    }

    /// Do a memory copy to the target `SegmMut<T>`.
    ///
    /// The items that are being memory copied will be treated as moved and
    /// will no longer drop by this `SegmRef<T>`. And the return result of
    /// `least_count()`, either from this `SegmRef<T>` or from the target
    /// `SegmMut<T>` shall change.
    pub fn move_items_to_segm<TyRecl>(
        &mut self,
        target: &mut SegmMut<'_, T, TyRecl>,
    ) -> usize
    where
        TyRecl: TrReclaim,
    {
        let dst = &mut target.buffer_[target.offset_..];
        let count = unsafe { self.move_items_to_buff(dst) };
        debug_assert!(count <= target.least_count());
        target.offset_ += count;
        count
    }

    /// Do a memory copy to the target buffer. And the items that are being
    /// memory copied will be treated as moved and will no longer drop. The
    /// result of `least_count()` shall change after calling this function.
    ///
    /// # Safety
    /// - The target buffer should guaraneed that the items being moved into
    ///   will drop properly if needed.
    /// - The target buffer must not be any borrowed form from this segment
    ///   buff.
    pub unsafe fn move_items_to_buff(
        &mut self,
        buff: &mut [MaybeUninit<T>],
    ) -> usize {
        let dst_size = buff.borrow_mut().len();
        let src_size = self.least_count();
        let count = cmp::min(dst_size, src_size);
        if count == 0 {
            return 0;
        };
        let src = self.buffer_[self.offset_..self.offset_ + count].as_ptr() as *const MaybeUninit<T>;
        let dst = buff.borrow_mut()[0..count].as_mut_ptr();
        unsafe { ptr::copy_nonoverlapping(src, dst, count); }
        self.offset_ += count;
        return count;
    }

    pub fn clone_items_to_segm<TyRecl>(&self, target: &mut SegmMut<'_, T, TyRecl>) -> usize
    where
        TyRecl: TrReclaim,
        [MaybeUninit<T>]: Sized,
        T: Clone,
    {
        let dst = &mut target.buffer_[target.offset_..];
        let count = unsafe { self.clone_items_to_buff(dst) };
        debug_assert!(count <= target.least_count());
        target.offset_ += count;
        count
    }

    /// Clone items to buffer and keep this `SegmRef<T>` unchanged. However,
    /// it is the `buff`'s responsibility to keep tracking the lifetime of
    /// the copied items.
    ///
    /// # Safety
    /// - The target buffer should guaraneed that the items being moved into
    ///   will drop properly if needed.
    pub unsafe fn clone_items_to_buff(
        &self,
        buff: &mut [MaybeUninit<T>],
    ) -> usize
    where
        T: Clone,
    {
        let dst_size = buff.borrow_mut().len();
        let src_size = self.least_count();
        let count = cmp::min(dst_size, src_size);
        if count == 0 {
            return 0;
        };
        let src = &self.buffer_[self.offset_..self.offset_ + count];
        let dst = &mut buff.borrow_mut()[0..count];
        let dst = dst.as_mut_ptr() as *mut T;
        let dst = unsafe { slice::from_raw_parts_mut(dst, count) };
        dst.clone_from_slice(src);
        count
    }

    pub fn take_segm_ref<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> Option<SegmRef<'f, T, SegmReclaim<'f>>> {
        let c = self.least_count();
        if c == 0usize {
            return Option::None;
        };
        let available = Demand::less_than(c);
        let Option::Some(agreement) = demand.compromise(&available) else {
            return Option::None;
        };
        let max_len = agreement.max()?;
        let dst = &mut self.buffer_[self.offset_..self.offset_ + max_len];
        let reclaim = SegmReclaim::new(&mut self.offset_);
        let child = SegmRef::new(dst, reclaim);
        Option::Some(child)
    }
}

impl<'a, T, R> SegmMut<'a, T, R>
where
    R: TrReclaim,
{
    /// Create by borrowing a slice from an implicit source. And the items of
    /// this slice will be returned back to or moved out of the source by
    /// `reclaim`.
    pub const fn new(
        buffer: &'a mut [MaybeUninit<T>],
        reclaim: R,
    ) -> Self {
        SegmMut {
            buffer_: buffer,
            offset_: 0usize,
            reclaim_: Option::Some(reclaim),
            _pinned_: PhantomPinned,
        }
    }

    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.least_count() == 0
    }

    #[inline]
    pub const fn least_count(&self) -> usize {
        self.buffer_.len()
    }

    pub const fn iter_slices(&self) -> Option<&[MaybeUninit<T>]> {
        if self.is_empty() {
            Option::None
        } else {
            Option::Some(self.buffer_)
        }
    }

    pub const fn iter_slices_mut(&mut self) -> Option<&mut [MaybeUninit<T>]> {
        if self.is_empty() {
            Option::None
        } else {
            Option::Some(self.buffer_)
        }
    }

    pub fn as_segm_mut<'f>(&'f mut self) -> SegmMut<'f, T, SegmReclaim<'f>> {
        let buffer = &mut self.buffer_[self.offset_..];
        let reclaim = SegmReclaim::new(&mut self.offset_);
        SegmMut::new(buffer, reclaim)
    }

    /// Do a memory copy to the target `SegmMut<T>`.
    ///
    /// The items that are being memory copied will be treated as moved and
    /// will no longer drop by this `SegmRef<T>`. And the return result of
    /// `least_count()`, either from this `SegmRef<T>` or from the target
    /// `SegmMut<T>` shall change.
    #[inline]
    pub fn move_items_from_segm<TyRecl>(
        &mut self,
        source: &mut SegmRef<'_, T, TyRecl>,
    ) -> usize
    where
        TyRecl: TrReclaim,
        [MaybeUninit<T>]: Sized,
    {
        source.move_items_to_segm(self)
    }

    /// Do a memory copy to the target buffer. And the items that are being
    /// memory copied will be treated as moved and will no longer drop. The
    /// result of `least_count()` shall change after calling this function.
    ///
    /// # Safety
    /// - The source buffer should guaraneed that the remaining items will
    ///   drop properly if needed.
    pub unsafe fn move_items_from_buff(
        &mut self,
        source: &mut [MaybeUninit<T>],
    ) -> usize {
        let dst_size = self.least_count();
        let src_size = source.len();
        let count = cmp::min(dst_size, src_size);
        if count == 0 {
            return 0;
        };
        let dst = self.buffer_[self.offset_..self.offset_ + count].as_ptr() as *mut MaybeUninit<T>;
        let src = source.borrow_mut()[0..count].as_mut_ptr();
        unsafe { ptr::copy_nonoverlapping(src, dst, count); }
        self.offset_ += count;
        return count;
    }

    pub fn clone_items_from_buff(&mut self, source: &[T]) -> usize
    where
        T: Clone,
    {
        let dst_size = self.least_count();
        let src_size = source.len();
        let count = cmp::min(dst_size, src_size);
        if count == 0 {
            return 0;
        };
        let dst = &mut self.buffer_[self.offset_..self.offset_ + count];
        let dst = dst.as_mut_ptr() as *mut _ as *mut T;
        let dst = unsafe { slice::from_raw_parts_mut(dst, count) };
        dst.clone_from_slice(&source[..count]);
        self.offset_ += count;
        count
    }

    pub fn take_segm_mut<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> Option<SegmMut<'f, T, SegmReclaim<'f>>> {
        let c = self.least_count();
        if c == 0usize {
            return Option::None;
        };
        let available = Demand::less_than(c);
        let Option::Some(agreement) = demand.compromise(&available) else {
            return Option::None;
        };
        let max_len = agreement.max()?;
        let dst = &mut self.buffer_[self.offset_..self.offset_ + max_len];
        let reclaim = SegmReclaim::new(&mut self.offset_);
        let child = SegmMut::new(dst, reclaim);
        Option::Some(child)
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// impl Drop for SegmRef and SegmMut
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

impl<'a, T, R> Drop for SegmRef<'a, T, R>
where
    R: TrReclaim,
{
    fn drop(&mut self) {
        let Option::Some(r) = self.reclaim_.take() else {
            return;
        };
        r.reclaim(self.offset_);
    }
}

impl<'a, T, R> Drop for SegmMut<'a, T, R>
where
    R: TrReclaim,
{
    fn drop(&mut self) {
        let Option::Some(r) = self.reclaim_.take() else {
            return;
        };
        r.reclaim(self.offset_);
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// impl TrBuffSegmRef for SegmRef
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

impl<'a, T, R> TrBuffSegmView for SegmRef<'a, T, R>
where
    R: TrReclaim,
{
    type Item = T;

    #[inline]
    fn is_empty(&self) -> bool {
        SegmRef::is_empty(self)
    }

    #[inline]
    fn least_count(&self) -> usize {
        SegmRef::least_count(self)
    }

    #[inline]
    fn iter_slices(&self) -> impl IntoIterator<Item = &[Self::Item]> {
        SegmRef::iter_slices(self)
    }
}

impl<'a, T, R> TrBuffSegmRef<'a, T> for SegmRef<'a, T, R>
where
    R: TrReclaim,
{
    type Reclaimer<'f> = SegmReclaim<'f> where Self: 'f;

    #[inline]
    fn take_segm_ref<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> impl Try<Output: TrBuffSegmRef<'f, T>> {
        SegmRef::take_segm_ref(self, demand)
    }

    #[inline]
    fn as_segm_ref<'f>(&'f mut self) -> SegmRef<'f, T, Self::Reclaimer<'f>> {
        SegmRef::as_segm_ref(self)
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// impl TrBuffSegmMut for SegmMut
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----


impl<'a, T, R> TrBuffSegmView for SegmMut<'a, T, R>
where
    R: TrReclaim,
{
    type Item = MaybeUninit<T>;

    #[inline]
    fn is_empty(&self) -> bool {
        SegmMut::is_empty(self)
    }

    #[inline]
    fn least_count(&self) -> usize {
        SegmMut::least_count(self)
    }

    #[inline]
    fn iter_slices(&self) -> impl IntoIterator<Item = &[Self::Item]> {
        SegmMut::iter_slices(self)
    }
}

impl<'a, T, R> TrBuffSegmMut<'a, T> for SegmMut<'a, T, R>
where
    R: TrReclaim,
{
    type Reclaimer<'f> = SegmReclaim<'f> where Self: 'f;

    #[inline]
    fn take_segm_mut<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> impl Try<Output: TrBuffSegmMut<'f, T>> {
        SegmMut::take_segm_mut(self, demand)
    }

    #[inline]
    fn as_segm_mut<'f>(&'f mut self) -> SegmMut<'f, T, Self::Reclaimer<'f>> {
        SegmMut::as_segm_mut(self)
    }
}
