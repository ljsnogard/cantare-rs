use core::{
    borrow::BorrowMut,
    cmp,
    marker::PhantomPinned,
    mem::MaybeUninit,
    ops::Try,
    pin::Pin,
    ptr, slice,
};

use abs_cancel::{TrCancellationToken, TrMayCancel};
use anylr::SomeOf;
use gen_mcf_macro::gen_may_cancel_future;

use crate::{Demand, io::{TrInput, TrOutput}};

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
    fn iter_slices(&self) -> impl IntoIterator<Item = &[Self::Item]>;
}

/// An instance to instantly tell the consumer usage of a buffer.
pub trait TrReclaim
where
    Self: Send + Sync,
{
    /// Indicate the reclaimer the amount of consumption, and returns the
    /// amount before the consumption.
    fn reclaim(&mut self, amount: usize) -> usize;
}

impl<F> TrReclaim for F
where
    F: Fn(usize) -> usize + Send + Sync,
{
    fn reclaim(&mut self, amount: usize) -> usize {
        let f = self;
        f(amount)
    }
}

/// A buffer that its data is organized with one or more slices
pub trait TrBuffSegmRef<'a, T>
where
    Self: TrBuffSegmView<Item = T>,
{
    type Reclaimer<'f>: TrReclaim
    where
        Self: 'f;

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

    /// Do a memory copy to the target `SegmMut<T>`.
    ///
    /// The items that are being memory copied will be treated as moved and
    /// will no longer drop by this `SegmRef<T>`. And the return result of
    /// `least_count()`, either from this `SegmRef<T>` or from the target
    /// `SegmMut<T>` shall change.
    fn move_items_to_segm<'f, TyTarget>(
        &'f mut self,
        target: &'f mut TyTarget,
    ) -> usize
    where
        // 注意：trait 生命周期 `'a` 与借用生命周期 `'f` 解耦——若把约束写为
        // `TrBuffSegmMut<'f, T>`，`'f` 会被绑定到目标段的生命周期，导致搬移后
        // 段仍处于借用中、无法继续使用。
        TyTarget: TrBuffSegmMut<'a, T>,
    {
        let mut c = 0usize;
        while self.least_count() > 0 && target.least_count() > 0 {
            let mut src_segm = self.as_segm_ref();
            let mut dst_segm = target.as_segm_mut();
            c += src_segm.move_items_to_segm(&mut dst_segm);
        }
        c
    }

    /// 把本段剩余元素搬出到 `dst`（按两段顺序取出），并推进已消费量。
    ///
    /// ## Safety
    ///
    /// 搬移为位拷贝：被搬出的元素不再由本段 drop，调用方需保证 `T` 无需要
    /// drop 的资源（或由 `dst` 负责）。
    unsafe fn move_items_to_buff(
        &mut self,
        dst: &mut [MaybeUninit<T>],
    ) -> usize {
        let mut c = 0usize;
        while self.least_count() > 0 && c < dst.len() {
            let mut segm = self.as_segm_ref();
            let dst_buff = &mut dst[c..];
            c += unsafe { segm.move_items_to_buff(dst_buff) };
        }
        c
    }
}

/// A buffer that its data is organized with one or more slices mut.
pub trait TrBuffSegmMut<'a, T>
where
    Self: TrBuffSegmView<Item = MaybeUninit<T>>,
{
    type Reclaimer<'f>: TrReclaim
    where
        Self: 'f;

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

    /// Do a memory copy to the target `SegmMut<T>`.
    ///
    /// This function heavily relies on `as_segm_mut` and `as_segm_ref` to
    /// do the actual memory copying.
    fn move_items_from_segm<'f, TySource>(
        &'f mut self,
        source: &'f mut TySource,
    ) -> usize
    where
        // 同 [`TrBuffSegmRef::move_items_to_segm`]：trait 生命周期与借用解耦。
        TySource: TrBuffSegmRef<'a, T>,
    {
        let mut c = 0usize;
        while self.least_count() > 0 && source.least_count() > 0 {
            let mut dst_segm = self.as_segm_mut();
            let mut src_segm = source.as_segm_ref();
            c += dst_segm.move_items_from_segm(&mut src_segm);
        }
        c
    }

    /// 把 `src` 的元素搬进本段（按两段顺序填充），并推进已消费量。
    ///
    /// ## Safety
    ///
    /// 搬移为位拷贝：`src` 中被搬走的元素在搬移后不再被 drop，调用方需保证
    /// `T` 无需要 drop 的资源（或自行处理 `src` 剩余元素）。
    unsafe fn move_items_from_buff(
        &mut self,
        src: &mut [MaybeUninit<T>],
    ) -> usize {
        let mut c = 0usize;
        while self.least_count() > 0 && c < src.len() {
            let mut segm = self.as_segm_mut();
            let src_buff = &mut src[c..];
            c += unsafe { segm.move_items_from_buff(src_buff) };
        }
        c
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// SegmReclaim,
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

pub struct SegmReclaim<'a>(Pin<&'a mut usize>);

impl<'a> SegmReclaim<'a> {
    pub const fn new(p: Pin<&'a mut usize>) -> Self {
        SegmReclaim(p)
    }
}

impl<'a> TrReclaim for SegmReclaim<'a> {
    #[inline]
    fn reclaim(&mut self, amount: usize) -> usize {
        let p = self.0.as_mut();
        // This safe if the one who creates this `SegmReclaim` guarantees that,
        // It is always created within a borrow mut context.
        unsafe {
            let p = p.get_mut() as *mut usize;
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
    buffer_: &'a [T],
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
    pub const fn new(buffer: &'a [T], reclaim: R) -> Self {
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
        self.buffer_.len() - self.offset_
    }

    pub const fn iter_slices(&self) -> Option<&[T]> {
        let len = self.buffer_.len() - self.offset_;
        if len == 0 {
            Option::None
        } else {
            // SAFETY: `self.offset_ <= self.buffer_.len()` always holds (it is
            // only ever advanced by at most the remaining count), so the
            // sub-slice `[offset_, offset_ + len)` stays inside the buffer.
            unsafe {
                Option::Some(slice::from_raw_parts(
                    self.buffer_.as_ptr().add(self.offset_),
                    len,
                ))
            }
        }
    }

    pub fn as_segm_ref<'f>(&'f mut self) -> SegmRef<'f, T, SegmReclaim<'f>> {
        let buffer = &self.buffer_[self.offset_..];
        let reclaim = SegmReclaim::new(Pin::new(&mut self.offset_));
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

    pub fn move_items_to_output_async<'f, TyOutput>(
        &'f mut self,
        output: &'f mut TyOutput,
        demand: &'f Demand<usize>,
    ) -> SegmRefOutputAsync<'a, 'f, T, R, TyOutput>
    where
        TyOutput: TrOutput<T>,
    {
        SegmRefOutputAsync(self, output, demand)
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
        let src = self.buffer_[self.offset_..self.offset_ + count].as_ptr()
            as *const MaybeUninit<T>;
        let dst = buff.borrow_mut()[0..count].as_mut_ptr();
        unsafe {
            ptr::copy_nonoverlapping(src, dst, count);
        }
        self.offset_ += count;
        count
    }

    pub fn clone_items_to_segm<TyRecl>(
        &self,
        target: &mut SegmMut<'_, T, TyRecl>,
    ) -> usize
    where
        TyRecl: TrReclaim,
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
        let agreement = demand.compromise(&available)?;
        let max_len = agreement.max()?;
        let dst = &self.buffer_[self.offset_..self.offset_ + max_len];
        let reclaim = SegmReclaim::new(Pin::new(&mut self.offset_));
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
    pub const fn new(buffer: &'a mut [MaybeUninit<T>], reclaim: R) -> Self {
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
        self.buffer_.len() - self.offset_
    }

    pub const fn iter_slices(&self) -> Option<&[MaybeUninit<T>]> {
        let len = self.buffer_.len() - self.offset_;
        if len == 0 {
            Option::None
        } else {
            // SAFETY: `self.offset_ <= self.buffer_.len()` always holds.
            unsafe {
                Option::Some(slice::from_raw_parts(
                    self.buffer_.as_ptr().add(self.offset_),
                    len,
                ))
            }
        }
    }

    pub const fn iter_slices_mut(&mut self) -> Option<&mut [MaybeUninit<T>]> {
        let len = self.buffer_.len() - self.offset_;
        if len == 0 {
            Option::None
        } else {
            // SAFETY: `self.offset_ <= self.buffer_.len()` always holds.
            unsafe {
                Option::Some(slice::from_raw_parts_mut(
                    self.buffer_.as_mut_ptr().add(self.offset_),
                    len,
                ))
            }
        }
    }

    pub fn as_segm_mut<'f>(&'f mut self) -> SegmMut<'f, T, SegmReclaim<'f>> {
        let buffer = &mut self.buffer_[self.offset_..];
        let reclaim = SegmReclaim::new(Pin::new(&mut self.offset_));
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
    {
        source.move_items_to_segm(self)
    }

    pub fn move_items_from_input_async<'f, TyInput>(
        &'f mut self,
        input: &'f mut TyInput,
        demand: &'f Demand<usize>,
    ) -> SegmMutInputAsync<'a, 'f, T, R, TyInput>
    where
        TyInput: TrInput<T>,
    {
        SegmMutInputAsync(self, input, demand)
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
        let dst = self.buffer_[self.offset_..self.offset_ + count].as_ptr()
            as *mut MaybeUninit<T>;
        let src = source.borrow_mut()[0..count].as_mut_ptr();
        unsafe {
            ptr::copy_nonoverlapping(src, dst, count);
        }
        self.offset_ += count;
        count
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
        let agreement = demand.compromise(&available)?;
        let max_len = agreement.max()?;
        let dst = &mut self.buffer_[self.offset_..self.offset_ + max_len];
        let reclaim = SegmReclaim::new(Pin::new(&mut self.offset_));
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
        let Option::Some(mut r) = self.reclaim_.take() else {
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
        let Option::Some(mut r) = self.reclaim_.take() else {
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
    type Reclaimer<'f>
        = SegmReclaim<'f>
    where
        Self: 'f;

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
    type Reclaimer<'f>
        = SegmReclaim<'f>
    where
        Self: 'f;

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

#[gen_may_cancel_future(SegmRefOutput)]
async fn segm_ref_output_async_<'a, 'f, TyData, TyRecl, TyOut, TyTok>(
    segm: &'f mut SegmRef<'a, TyData, TyRecl>,
    output: &'f mut TyOut,
    demand: &'f Demand<usize>,
    cancel: &'f mut TyTok,
) -> SomeOf<usize, <TyOut as TrOutput<TyData>>::Err>
where
    'a: 'f,
    TyRecl: TrReclaim,
    TyOut: TrOutput<TyData>,
    TyTok: TrCancellationToken + Clone,
{
    let buff = &segm.buffer_[segm.offset_..];
    let size = buff.len();
    let Option::Some(compromised) = demand.compromise(&Demand::less_than(size))
    else {
        return SomeOf::new_left(0usize);
    };
    let Option::Some(max) = compromised.max() else {
        unreachable!()
    };
    let max = *max;
    debug_assert!(max <= size);
    let mut c = 0usize;
    loop {
        if c >= max {
            return SomeOf::new_left(c);
        };
        let remaining = size - c;
        let take = core::cmp::min(remaining, max - c);
        let buff = &buff[c..];
        let source = {
            let p = buff.as_ptr() as *const _ as *const MaybeUninit<TyData>;
            unsafe { slice::from_raw_parts(p, take) }
        };
        let x = output.write_async(source).may_cancel_with(cancel).await;
        if let Option::Some(cc) = x.as_ref().pick_left() {
            if *cc == 0 {
                return SomeOf::new_left(c);
            }
            segm.offset_ += *cc;
            c += cc;
        };
        if let Option::Some(err) = x.pick_right() {
            return SomeOf::new_both(c, err);
        };
        if core::hint::black_box(false) {
            // this is just to please the compiler, will never enter.
            // However, if it did enter, we have to know.
            assert!(c > 0usize);
            break;
        }
    }
    SomeOf::new_left(c)
}

#[gen_may_cancel_future(SegmMutInput)]
async fn segm_mut_input_async_<'a, 'f, TyData, TyRecl, TyInput, TyTok>(
    segm: &'f mut SegmMut<'a, TyData, TyRecl>,
    input: &'f mut TyInput,
    demand: &'f Demand<usize>,
    cancel: &'f mut TyTok,
) -> SomeOf<usize, <TyInput as TrInput<TyData>>::Err>
where
    'a: 'f,
    TyRecl: TrReclaim,
    TyInput: TrInput<TyData>,
    TyTok: TrCancellationToken + Clone,
{
    let buff = &mut segm.buffer_[segm.offset_..];
    let size = buff.len();
    let Option::Some(compromised) = demand.compromise(&Demand::less_than(size))
    else {
        return SomeOf::new_left(0usize);
    };
    let Option::Some(max) = compromised.max() else {
        unreachable!()
    };
    let max = *max;
    debug_assert!(max <= size);
    let mut c = 0usize;
    loop {
        if c >= max {
            return SomeOf::new_left(c);
        };
        let remaining = size - c;
        let take = core::cmp::min(remaining, max - c);
        let buff = &mut buff[c..];
        let target = {
            let p = buff.as_mut_ptr();
            unsafe { slice::from_raw_parts_mut(p, take) }
        };
        let x = input.read_async(target).may_cancel_with(cancel).await;
        if let Option::Some(cc) = x.as_ref().pick_left() {
            if *cc == 0 {
                return SomeOf::new_left(c);
            }
            segm.offset_ += *cc;
            c += cc;
        };
        if let Option::Some(err) = x.pick_right() {
            return SomeOf::new_both(c, err);
        };
        if core::hint::black_box(false) {
            // this is just to please the compiler, will never enter.
            // However, if it did enter, we have to know.
            assert!(c > 0usize);
            break;
        }
    }
    SomeOf::new_left(c)
}

#[cfg(test)]
mod tests_ {
    use std::{vec, vec::Vec};

    use super::*;

    /// Fill `dst` (of MaybeUninit) with the values consumed from `segm`, moving
    /// them out of the segment.
    fn move_all<T, R>(
        segm: &mut SegmRef<'_, T, R>,
        dst: &mut [MaybeUninit<T>],
    ) -> usize
    where
        R: TrReclaim,
    {
        unsafe { segm.move_items_to_buff(dst) }
    }

    fn read_init<T: Copy>(dst: &[MaybeUninit<T>]) -> Vec<T> {
        dst.iter()
            .map(|m| unsafe { m.assume_init_read() })
            .collect()
    }

    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // SegmRef: borrow → consume → the next borrow is exactly the next content
    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    #[test]
    fn segm_ref_take_consume_next_is_next() {
        const LEN: usize = 64;
        let mut data: Vec<usize> = (0..LEN).collect();
        let mut consumed = 0usize;
        let mut segm = SegmRef::new(
            data.as_mut_slice(),
            SegmReclaim::new(Pin::new(&mut consumed)),
        );

        // 1st borrow: exactly 10 items, starting at the beginning.
        {
            let mut child = segm
                .take_segm_ref(&Demand::less_than(10))
                .expect("first take must succeed");
            assert_eq!(child.least_count(), 10);
            let slice = child.iter_slices().expect("child must not be empty");
            assert_eq!(slice.len(), 10);
            for (i, &v) in slice.iter().enumerate() {
                assert_eq!(v, i);
            }
            let mut dst = [MaybeUninit::<usize>::uninit(); 10];
            let n = move_all(&mut child, &mut dst);
            assert_eq!(n, 10);
            assert_eq!(child.least_count(), 0);
            assert_eq!(read_init(&dst), (0..10).collect::<Vec<_>>());
        }
        // The parent consumed exactly the first 10; nothing is reclaimed to the
        // outside until the parent itself drops (verified by the final `consumed`
        // total at the end of this test — it equals exactly the sum of the
        // consumed parts, so no early reclaim happened).
        assert_eq!(segm.least_count(), LEN - 10);
        let rest = segm.iter_slices().expect("non-empty");
        assert_eq!(rest[0], 10, "iter_slices must skip the consumed part");

        // 2nd borrow: the demand exceeds what remains, so the child is exactly
        // the rest — which is the next content to consume.
        {
            let mut child = segm
                .take_segm_ref(&Demand::less_than(LEN))
                .expect("second take must succeed");
            assert_eq!(child.least_count(), LEN - 10);
            let slice = child.iter_slices().expect("non-empty");
            for (i, &v) in slice.iter().enumerate() {
                assert_eq!(
                    v,
                    i + 10,
                    "next borrow must start right after the consumed part"
                );
            }
            let mut dst = [MaybeUninit::<usize>::uninit(); 20];
            let n = move_all(&mut child, &mut dst);
            assert_eq!(n, 20);
            assert_eq!(read_init(&dst), (10..30).collect::<Vec<_>>());
        }
        assert_eq!(segm.least_count(), LEN - 30);

        // 3rd borrow: only peek, do not consume.
        {
            let child = segm
                .take_segm_ref(&Demand::less_than(LEN))
                .expect("third take must succeed");
            assert_eq!(child.least_count(), LEN - 30);
            let slice = child.iter_slices().expect("non-empty");
            for (i, &v) in slice.iter().enumerate() {
                assert_eq!(v, i + 30);
            }
        }
        assert_eq!(segm.least_count(), LEN - 30);

        // Dropping the parent reports the whole consumed amount to the reclaimer.
        drop(segm);
        assert_eq!(consumed, 30);
    }

    #[test]
    fn segm_ref_as_segm_ref_consume_next_is_next() {
        const LEN: usize = 24;
        let mut data: Vec<u32> = (0..LEN as u32).collect();
        let mut consumed = 0usize;
        let mut segm = SegmRef::new(
            data.as_mut_slice(),
            SegmReclaim::new(Pin::new(&mut consumed)),
        );

        // Round 1: as_segm_ref borrows everything that is left; consume 16.
        {
            let mut child = segm.as_segm_ref();
            assert_eq!(child.least_count(), LEN);
            let mut dst = [MaybeUninit::<u32>::uninit(); 16];
            let n = move_all(&mut child, &mut dst);
            assert_eq!(n, 16);
            // The child itself still shows the remaining data as the next content.
            assert_eq!(child.least_count(), LEN - 16);
            let slice = child.iter_slices().expect("non-empty");
            for (i, &v) in slice.iter().enumerate() {
                assert_eq!(v, (i + 16) as u32);
            }
        }
        assert_eq!(segm.least_count(), LEN - 16);

        // Round 2: the rest.
        {
            let mut child = segm.as_segm_ref();
            assert_eq!(child.least_count(), LEN - 16);
            let mut dst = [MaybeUninit::<u32>::uninit(); 16];
            let n = move_all(&mut child, &mut dst);
            assert_eq!(n, LEN - 16);
            assert_eq!(child.least_count(), 0);
        }
        assert_eq!(segm.least_count(), 0);
        assert!(segm.is_empty());
        assert!(
            segm.iter_slices().is_none(),
            "no unconsumed items -> no slices"
        );

        drop(segm);
        assert_eq!(consumed, LEN);
    }

    #[test]
    fn segm_ref_move_items_to_segm_transfers_in_order() {
        const LEN: usize = 40;
        let mut src_data: Vec<u64> = (0..LEN as u64).collect();
        let mut src_consumed = 0usize;
        let mut dst1_data = [MaybeUninit::<u64>::uninit(); 16];
        let mut dst1_consumed = 0usize;
        let mut dst2_data = [MaybeUninit::<u64>::uninit(); 16];
        let mut dst2_consumed = 0usize;

        let mut src = SegmRef::new(
            src_data.as_mut_slice(),
            SegmReclaim::new(Pin::new(&mut src_consumed)),
        );

        // Move as much as the first destination can take: 16 of 40.
        let mut dst1 = SegmMut::new(
            &mut dst1_data[..],
            SegmReclaim::new(Pin::new(&mut dst1_consumed)),
        );
        {
            let mut src_child = src.as_segm_ref();
            let mut dst_child = dst1.as_segm_mut();
            let n = src_child.move_items_to_segm(&mut dst_child);
            assert_eq!(n, 16);
            assert_eq!(src_child.least_count(), LEN - 16);
            assert_eq!(dst_child.least_count(), 0);
        }
        assert_eq!(src.least_count(), LEN - 16);
        assert_eq!(dst1.least_count(), 0);

        // The next move picks up right after the first 16 items.
        let mut dst2 = SegmMut::new(
            &mut dst2_data[..],
            SegmReclaim::new(Pin::new(&mut dst2_consumed)),
        );
        {
            let mut src_child = src.as_segm_ref();
            let mut dst_child = dst2.as_segm_mut();
            let n = src_child.move_items_to_segm(&mut dst_child);
            assert_eq!(n, 16);
            assert_eq!(src_child.least_count(), LEN - 32);
        }
        assert_eq!(src.least_count(), LEN - 32);

        drop(src);
        drop(dst1);
        drop(dst2);
        assert_eq!(
            read_init(&dst1_data),
            (0..16).collect::<Vec<_>>(),
            "dst1 holds src[0..16]"
        );
        assert_eq!(
            read_init(&dst2_data),
            (16..32).collect::<Vec<_>>(),
            "dst2 holds src[16..32]"
        );
        assert_eq!(src_consumed, 32);
        assert_eq!(dst1_consumed, 16);
        assert_eq!(dst2_consumed, 16);
    }

    #[test]
    fn segm_ref_move_items_from_segm_into_mut() {
        let mut src_data = [7usize, 8, 9, 10];
        let mut src_consumed = 0usize;
        let mut dst_data = [MaybeUninit::<usize>::uninit(); 4];
        let mut dst_consumed = 0usize;

        let mut src = SegmRef::new(
            src_data.as_mut_slice(),
            SegmReclaim::new(Pin::new(&mut src_consumed)),
        );
        let mut dst = SegmMut::new(
            &mut dst_data[..],
            SegmReclaim::new(Pin::new(&mut dst_consumed)),
        );

        {
            let mut src_child = src.as_segm_ref();
            let mut dst_child = dst.as_segm_mut();
            let n = dst_child.move_items_from_segm(&mut src_child);
            assert_eq!(n, 4);
            assert_eq!(src_child.least_count(), 0);
            assert_eq!(dst_child.least_count(), 0);
        }
        drop(src);
        drop(dst);
        assert_eq!(read_init(&dst_data), vec![7, 8, 9, 10]);
        assert_eq!(src_consumed, 4);
        assert_eq!(dst_consumed, 4);
    }

    #[test]
    fn segm_ref_clone_items_keeps_source_position() {
        const LEN: usize = 24;
        let mut src_data: Vec<usize> = (0..LEN).collect();
        let mut src_consumed = 0usize;
        let mut dst_data = [MaybeUninit::<usize>::uninit(); 12];
        let mut dst_consumed = 0usize;

        let mut src = SegmRef::new(
            src_data.as_mut_slice(),
            SegmReclaim::new(Pin::new(&mut src_consumed)),
        );
        let mut dst = SegmMut::new(
            &mut dst_data[..],
            SegmReclaim::new(Pin::new(&mut dst_consumed)),
        );

        // Cloning does NOT advance the source; the first 8 items land in dst.
        {
            let src_child = src.as_segm_ref();
            let mut dst_child = dst
                .take_segm_mut(&Demand::less_than(8))
                .expect("dst take must succeed");
            let n = src_child.clone_items_to_segm(&mut dst_child);
            assert_eq!(n, 8);
            assert_eq!(
                src_child.least_count(),
                LEN,
                "cloning must not consume the source"
            );
        }
        assert_eq!(src.least_count(), LEN);
        assert_eq!(dst.least_count(), 12 - 8);

        // Clone again: the same source content fills the remaining dst slots.
        {
            let src_child = src.as_segm_ref();
            let mut dst_child = dst
                .take_segm_mut(&Demand::less_than(8))
                .expect("dst take must succeed");
            let n = src_child.clone_items_to_segm(&mut dst_child);
            assert_eq!(n, 4);
        }
        assert_eq!(src.least_count(), LEN);
        assert_eq!(dst.least_count(), 0);

        let mut expected: Vec<usize> = (0..8).collect();
        expected.extend(0..4);
        drop(src);
        drop(dst);
        assert_eq!(read_init(&dst_data), expected);
        assert_eq!(
            src_consumed, 0,
            "cloning must never reclaim from the source"
        );
        assert_eq!(dst_consumed, 12);
    }

    #[test]
    fn segm_ref_clone_items_to_buff() {
        let mut data = [10usize, 20, 30, 40];
        let mut consumed = 0usize;
        let segm = SegmRef::new(
            data.as_mut_slice(),
            SegmReclaim::new(Pin::new(&mut consumed)),
        );

        let mut dst = [MaybeUninit::<usize>::uninit(); 4];
        let n = unsafe { segm.clone_items_to_buff(&mut dst) };
        assert_eq!(n, 4);
        assert_eq!(segm.least_count(), 4, "clone leaves the source untouched");
        assert_eq!(read_init(&dst), vec![10, 20, 30, 40]);

        drop(segm);
        assert_eq!(consumed, 0);
    }

    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // SegmMut: borrow → write → the next borrow is exactly the next free part
    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    #[test]
    fn segm_mut_take_write_next_is_next() {
        const LEN: usize = 32;
        let mut storage = [MaybeUninit::<u64>::uninit(); LEN];
        let mut consumed = 0usize;

        let mut segm = SegmMut::new(
            &mut storage[..],
            SegmReclaim::new(Pin::new(&mut consumed)),
        );

        // 1st borrow: 8 slots; receive 8 items.
        let mut src1: Vec<MaybeUninit<u64>> =
            (0..8).map(MaybeUninit::new).collect();
        {
            let mut child = segm
                .take_segm_mut(&Demand::less_than(8))
                .expect("first take must succeed");
            assert_eq!(child.least_count(), 8);
            let n = unsafe { child.move_items_from_buff(&mut src1) };
            assert_eq!(n, 8);
            assert_eq!(child.least_count(), 0);
        }
        assert_eq!(segm.least_count(), LEN - 8);

        // 2nd borrow: demand exceeds what is left → the child is exactly the
        // remaining free space, which starts right after the first 8 items.
        let mut src2: Vec<MaybeUninit<u64>> =
            (10..20).map(MaybeUninit::new).collect();
        {
            let mut child = segm
                .take_segm_mut(&Demand::less_than(LEN))
                .expect("second take must succeed");
            assert_eq!(child.least_count(), LEN - 8);
            let n = unsafe { child.move_items_from_buff(&mut src2) };
            assert_eq!(n, 10);
        }
        assert_eq!(segm.least_count(), LEN - 18);

        drop(segm);
        // The two borrows wrote into storage in order: [0..8) then [8..18).
        assert_eq!(read_init(&storage[..8]), (0..8).collect::<Vec<_>>());
        assert_eq!(read_init(&storage[8..18]), (10..20).collect::<Vec<_>>());
        assert_eq!(consumed, 18);
    }

    #[test]
    fn segm_mut_as_segm_mut_write_next_is_next() {
        const LEN: usize = 16;
        let mut storage = [MaybeUninit::<u8>::uninit(); LEN];
        let mut consumed = 0usize;

        let mut segm = SegmMut::new(
            &mut storage[..],
            SegmReclaim::new(Pin::new(&mut consumed)),
        );

        // Round 1: receive 6 items.
        let mut src1: Vec<MaybeUninit<u8>> =
            (1..=6).map(MaybeUninit::new).collect();
        {
            let mut child = segm.as_segm_mut();
            let n = unsafe { child.move_items_from_buff(&mut src1) };
            assert_eq!(n, 6);
            // The child's own view now starts at slot 6.
            let slots = child.iter_slices_mut().expect("non-empty");
            assert_eq!(slots.len(), LEN - 6);
        }
        assert_eq!(segm.least_count(), LEN - 6);

        // Round 2: the rest.
        let mut src2: Vec<MaybeUninit<u8>> =
            (7..=16).map(MaybeUninit::new).collect();
        {
            let mut child = segm.as_segm_mut();
            let n = unsafe { child.move_items_from_buff(&mut src2) };
            assert_eq!(n, 10);
            assert_eq!(child.least_count(), 0);
        }
        assert_eq!(segm.least_count(), 0);
        assert!(segm.is_empty());
        assert!(
            segm.iter_slices_mut().is_none(),
            "no free space -> no slices"
        );

        drop(segm);
        assert_eq!(read_init(&storage), (1..=16).collect::<Vec<_>>());
        assert_eq!(consumed, LEN);
    }

    #[test]
    fn segm_mut_clone_items_from_buff() {
        const LEN: usize = 16;
        let mut storage = [MaybeUninit::<usize>::uninit(); LEN];
        let mut consumed = 0usize;

        let mut segm = SegmMut::new(
            &mut storage[..],
            SegmReclaim::new(Pin::new(&mut consumed)),
        );

        {
            let mut child = segm
                .take_segm_mut(&Demand::less_than(8))
                .expect("take must succeed");
            let n = child.clone_items_from_buff(&[1usize, 2, 3, 4, 5]);
            assert_eq!(n, 5);
            assert_eq!(child.least_count(), 8 - 5);
        }
        assert_eq!(segm.least_count(), LEN - 5);

        drop(segm);
        assert_eq!(read_init(&storage[..5]), vec![1, 2, 3, 4, 5]);
        assert_eq!(consumed, 5);
    }

    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // Reclaim behavior
    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    #[test]
    fn segm_reclaim_reports_amount_before_consumption() {
        let mut counter = 0usize;
        let mut r = SegmReclaim::new(Pin::new(&mut counter));
        assert_eq!(
            r.reclaim(3),
            0,
            "returns the amount before the consumption"
        );
        assert_eq!(r.reclaim(4), 3);
        assert_eq!(counter, 7);
    }

    #[test]
    fn segm_reclaim_reclaimed_only_on_drop() {
        let mut data = [1u8, 2, 3, 4];
        let mut consumed = 0usize;
        {
            let mut segm = SegmRef::new(
                data.as_mut_slice(),
                SegmReclaim::new(Pin::new(&mut consumed)),
            );
            {
                let mut child = segm
                    .take_segm_ref(&Demand::less_than(2))
                    .expect("take must succeed");
                let mut dst = [MaybeUninit::<u8>::uninit(); 2];
                let n = move_all(&mut child, &mut dst);
                assert_eq!(n, 2);
            }
            // The segment accounted for the consumption internally; the outside
            // counter only changes when the segment itself drops (checked after
            // the scope ends, via the exact final total).
            assert_eq!(segm.least_count(), 2);
        }
        assert_eq!(consumed, 2);
    }

    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // 泛型段测试：move_items_* 的 trait 默认实现（SegmRef / SegmMut）
    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    //
    // 测试意图：abs_buff 把 `move_items_*` 提升为 `TrBuffSegmRef` /
    // `TrBuffSegmMut` 的 trait 默认方法，任何实现者都必须满足这些默认实现的
    // 语义。这里用 `buffer::segm_tests` 的泛型函数验证本 crate 自己的
    // `SegmRef` / `SegmMut`：数据按序搬移、消费量正确推进、无重复无丢失。
    //
    // 内部执行设计：每个方向用一个"源 Vec + 目标数组"的简单存储，段持有
    // `SegmReclaim` 计数器；泛型函数在段层面断言搬移数量与消费量，测试主体
    // 在底层存储上断言内容按序到达、回收计数器精确提交。

    /// 通过 trait 默认实现把 `SegmRef` 的全部元素搬进 `SegmMut`
    /// （`move_items_to_segm` 与镜像的 `move_items_from_segm` 都验证）。
    #[test]
    fn segm_move_items_trait_defaults() {
        use crate::buffer::segm_tests as t;

        // —— move_items_to_segm（源段一侧发起）——
        {
            let mut src_data: Vec<u64> = (0..16).collect();
            let mut dst_data = [MaybeUninit::<u64>::uninit(); 16];
            let mut src_consumed = 0usize;
            let mut dst_consumed = 0usize;
            let expect: Vec<u64> = (0..16).collect();
            let mut src = SegmRef::new(
                src_data.as_mut_slice(),
                SegmReclaim::new(Pin::new(&mut src_consumed)),
            );
            let mut dst = SegmMut::new(
                &mut dst_data[..],
                SegmReclaim::new(Pin::new(&mut dst_consumed)),
            );
            let moved = t::test_move_items_to_segm(&mut src, &mut dst, &expect);
            assert_eq!(moved, 16, "泛型函数应返回搬移数量");
            // 泛型函数已内部断言 src/dst 的 least_count == 0；这里再校验
            // 底层存储与回收计数（段 drop 时提交消费量）；
            drop(src);
            drop(dst);
            assert_eq!(read_init(&dst_data), expect, "内容必须按序搬入目标");
            assert_eq!(src_consumed, 16, "源段必须按消费量提交");
            assert_eq!(dst_consumed, 16, "目标段必须按消费量提交");
        }

        // —— move_items_from_segm（目标段一侧发起，镜像）——
        {
            let mut src_data: Vec<u32> = (10..26).collect();
            let mut dst_data = [MaybeUninit::<u32>::uninit(); 16];
            let mut src_consumed = 0usize;
            let mut dst_consumed = 0usize;
            let expect: Vec<u32> = (10..26).collect();
            let mut src = SegmRef::new(
                src_data.as_mut_slice(),
                SegmReclaim::new(Pin::new(&mut src_consumed)),
            );
            let mut dst = SegmMut::new(
                &mut dst_data[..],
                SegmReclaim::new(Pin::new(&mut dst_consumed)),
            );
            let moved =
                t::test_move_items_from_segm(&mut src, &mut dst, &expect);
            assert_eq!(moved, 16);
            drop(src);
            drop(dst);
            assert_eq!(read_init(&dst_data), expect, "内容必须按序搬入目标");
            assert_eq!(src_consumed, 16);
            assert_eq!(dst_consumed, 16);
        }

        // —— move_items_to_buff（源段 → 普通缓冲）——
        {
            let mut src_data: Vec<u8> = (0..16).collect();
            let mut dst_buf = [MaybeUninit::<u8>::uninit(); 16];
            let mut consumed = 0usize;
            let expect: Vec<u8> = (0..16).collect();
            let mut src = SegmRef::new(
                src_data.as_mut_slice(),
                SegmReclaim::new(Pin::new(&mut consumed)),
            );
            // SAFETY: u8 无 drop，位拷贝安全；
            let moved = unsafe {
                t::test_move_items_to_buff(&mut src, &mut dst_buf, &expect)
            };
            assert_eq!(moved, 16);
            drop(src);
            assert_eq!(read_init(&dst_buf), expect, "缓冲内容必须按序");
            assert_eq!(consumed, 16);
        }

        // —— move_items_from_buff（普通缓冲 → 目标段）——
        {
            let mut dst_data = [MaybeUninit::<usize>::uninit(); 16];
            let mut src_buf = [MaybeUninit::<usize>::uninit(); 16];
            let mut consumed = 0usize;
            let expect: Vec<usize> = (100..116).collect();
            let mut dst = SegmMut::new(
                &mut dst_data[..],
                SegmReclaim::new(Pin::new(&mut consumed)),
            );
            // SAFETY: usize 无 drop，位拷贝安全；
            let moved = unsafe {
                t::test_move_items_from_buff(&mut dst, &mut src_buf, &expect)
            };
            assert_eq!(moved, 16);
            drop(dst);
            assert_eq!(read_init(&dst_data), expect, "目标段内容必须按序");
            assert_eq!(consumed, 16);
        }
    }
}
