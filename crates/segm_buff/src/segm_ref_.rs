use core::{
    borrow::Borrow,
    marker::PhantomPinned,
    ops::{Deref, Try, RangeBounds},
    ptr::NonNull,
};

use abs_iter::TrAsSlice;
use abs_buff::{
    x_deps::abs_iter,
    Demand, TrBuffSegmRef, TrBuffSegmView,
};

use super::{
    reclaim_::SegmSelfReclaim,
    NoReclaim, TrReclaim,
};

type SliceElem<B> = <<B as Deref>::Target as TrAsSlice>::Elem;

type SliceInit<B> = [SliceElem<B>];

/// A wrapper around a slice borrowed from a buffer and its reclaim function.
/// Designed for [RingBuffer](crate::ring_buffer::RingBuffer) but capable of
/// being a simple stream buffer to support the consuming semantics.
#[repr(C)]
pub struct SegmRef<B, R>
where
    B: Deref<Target: TrAsSlice>,
    R: TrReclaim,
{
    _pinned_: PhantomPinned,
    offset_: usize,
    reclaim_: Option<R>,
    buffer_: B,
}

impl<B, R> SegmRef<B, R>
where
    B: Deref<Target: TrAsSlice>,
    R: TrReclaim,
{
    /// Create by borrowing a slice from an implicit source. And the items of
    /// this slice will be returned back to or moved out of the source by
    /// `reclaim`.
    pub const fn new(slice: B, reclaim: Option<R>) -> Self {
        SegmRef {
            _pinned_: PhantomPinned,
            offset_: 0usize,
            reclaim_: reclaim,
            buffer_: slice,
        }
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.buffer_.as_slice().len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.capacity() == self.offset_
    }

    pub fn as_slice(&self) -> &SliceInit<B> {
        let slice = self.buffer_.as_slice();
        #[cfg(test)]
        {
            let p = self as *const Self;
            let l = slice.len();
            std::println!("[{:p}]SegmRef::as_slice(capacity: {}), self.offset_: {}, slice.len: {}", p, self.capacity(), self.offset_, l);
        }
        &slice[self.offset_..]
    }

    pub fn iter_slices(&self) -> Option<&SliceInit<B>> {
        if self.is_empty() {
            Option::None
        } else {
            Option::Some(self.as_slice())
        }
    }

    pub fn take_segm_ref<'a>(
        &'a mut self,
        demand: &Demand<usize>,
    ) -> Option<SegmRef<&'a SliceInit<B>, SegmSelfReclaim<'a>>> {
        if self.is_empty() {
            return Option::None
        };
        debug_assert!(!self.as_slice().is_empty());
        let mut offset_ptr = unsafe {
            // self.offset_ is to be update only during drop, where no race should happen.
            NonNull::new_unchecked(&mut self.offset_ as *mut usize)
        };
        let available = Demand::less_than(self.as_slice().len());
        let compromised = demand.compromise(&available)?;
        let dst = &self.as_slice()[0..compromised.len()];
        let reclaim = SegmSelfReclaim::new(unsafe { offset_ptr.as_mut() });
        Option::Some(SegmRef::new(dst, Option::Some(reclaim)))
    }
}

impl<B> SegmRef<B, NoReclaim>
where
    B: Deref<Target: TrAsSlice>,
{
    /// Create by borrowing a slice from an implicit source but no reclaim
    #[inline]
    pub const fn no_reclaim(slice: B) -> Self {
        SegmRef::new(slice, Option::None)
    }
}

impl<B, R> Drop for SegmRef<B, R>
where
    B: Deref<Target: TrAsSlice>,
    R: TrReclaim,
{
    fn drop(&mut self) {
        let Option::Some(mut r) = self.reclaim_.take() else {
            return;
        };
        #[cfg(test)]std::println!("[{:p}]SegmRef::drop, before reclaim, self.offset_: {}", self as *mut Self, self.offset_);
        r.reclaim(self.capacity());
        #[cfg(test)]std::println!("[{:p}]SegmRef::drop, after reclaim, self.offset_: {}", self as *mut Self, self.offset_);
    }
}

impl<B, R> Deref for SegmRef<B, R>
where
    B: Deref<Target: TrAsSlice>,
    R: TrReclaim,
{
    type Target = SliceInit<B>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<B, R> Borrow<SliceInit<B>> for SegmRef<B, R>
where
    B: Deref<Target: TrAsSlice>,
    R: TrReclaim,
{
    #[inline]
    fn borrow(&self) -> &SliceInit<B> {
        self.as_slice()
    }
}

impl<B, R> AsRef<SliceInit<B>> for SegmRef<B, R>
where
    B: Deref<Target: TrAsSlice>,
    R: TrReclaim,
{
    #[inline]
    fn as_ref(&self) -> &SliceInit<B> {
        self.as_slice()
    }
}

impl<B, R> TrBuffSegmView for SegmRef<B, R>
where
    B: Deref<Target: TrAsSlice>,
    R: TrReclaim,
{
    type Item = SliceElem<B>;

    fn is_empty(&self) -> bool {
        SegmRef::is_empty(self)
    }

    fn capacity(&self) -> usize {
        SegmRef::capacity(self)
    }

    /// Iterate the unconsumed parts of the segment slice by slice.
    fn iter_slices<'a>(
        &'a self,
    ) -> impl IntoIterator<Item: 'a + AsRef<[Self::Item]>> {
        SegmRef::iter_slices(self)
    }
}

impl<B, R> TrBuffSegmRef<SliceElem<B>> for SegmRef<B, R>
where
    B: Deref<Target: TrAsSlice>,
    R: TrReclaim,
{
    #[inline]
    fn take_segm_ref<'a>(
        &'a mut self,
        demand: &impl RangeBounds<usize>,
    ) -> impl 'a + Try<Output: 'a + TrBuffSegmRef<SliceElem<B>>> {
        let r= Demand::try_from_usize_range(demand);
        let Result::Ok(demand) = &r else {
            return Option::None
        };
        SegmRef::take_segm_ref(self, demand)
    }
}

#[cfg(test)]
mod tests_ {
    use core::ptr::NonNull;

    use abs_buff::Demand;

    use crate::SegmSelfReclaim;

    use super::SegmRef;

    #[test]
    fn segm_len_should_eq_as_slice_len() {
        const ARR_SIZE: usize = 64;
        let mut buff = [0usize; ARR_SIZE];
        for (u, x) in buff.iter_mut().enumerate() {
            *x = u
        }
        let mut segm = SegmRef::new(buff.as_mut_slice(), Option::<SegmSelfReclaim>::None);
        segm.reclaim_ = Option::Some(unsafe {
            let mut offset_ptr = NonNull::new_unchecked(&mut segm.offset_ as *mut usize);
            SegmSelfReclaim::new(offset_ptr.as_mut())
        });
        let slice = segm.as_slice();
        assert_eq!(segm.len(), ARR_SIZE);
        assert_eq!(slice.len(), ARR_SIZE);

        const SLICE_LEN: usize = ARR_SIZE >> 1;
        if true {
            let first_range = Demand::less_than(SLICE_LEN);
            let first_take = segm.take_segm_ref(&first_range);

            std::println!("segm_ref first_take");

            if let Option::Some(taken_slice) = &first_take {
                assert_eq!(taken_slice.len(), SLICE_LEN);
                for (u, x) in taken_slice.iter().enumerate() {
                    assert_eq!(*x, u)
                }
            } else {
                panic!("first_take failed")
            }
        }
        assert_eq!(segm.len(), ARR_SIZE - SLICE_LEN);
        if true {
            let second_range = Demand::less_than(ARR_SIZE);
            let second_take = segm.take_segm_ref(&second_range);

            std::println!("segm_ref second_take");

            if let Option::Some(taken_slice) = &second_take {
                assert_eq!(taken_slice.len(), ARR_SIZE - SLICE_LEN);
                for (u, x) in taken_slice.iter().enumerate() {
                    assert_eq!(*x, u + SLICE_LEN)
                }
            }
        }
        assert_eq!(segm.len(), 0);
        std::println!("segm_ref test succ");
    }
}
