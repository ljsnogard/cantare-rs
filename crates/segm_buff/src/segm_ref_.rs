use core::{
    borrow::Borrow,
    marker::{PhantomData, PhantomPinned},
    ops::{Deref, Try, RangeBounds},
    ptr::NonNull,
};

use abs_buff::{Demand, TrBuffSegmRef, TrBuffSegmView};

use super::{
    reclaim_::SegmSelfReclaim,
    NoReclaim, TrReclaim,
};

/// A wrapper around a slice borrowed from a buffer and its reclaim function.
/// Designed for [RingBuffer](crate::ring_buffer::RingBuffer) but capable of
/// being a simple stream buffer to support the consuming semantics.
#[repr(C)]
pub struct SegmRef<'b, B, T, R>
where
    B: Borrow<[T]>,
    R: TrReclaim<T>,
{
    _mark_t_: PhantomData<&'b [T]>,
    _pinned_: PhantomPinned,
    offset_: usize,
    reclaim_: Option<R>,
    slice_ref_: B,
}

impl<B, T, R> SegmRef<'_, B, T, R>
where
    B: Borrow<[T]>,
    R: TrReclaim<T>,
{
    /// Create by borrowing a slice from an implicit source. And the items of
    /// this slice will be returned back to or moved out of the source by
    /// `reclaim`.
    pub const fn new(slice: B, reclaim: Option<R>) -> Self {
        SegmRef {
            _mark_t_: PhantomData,
            _pinned_: PhantomPinned,
            offset_: 0usize,
            reclaim_: reclaim,
            slice_ref_: slice,
        }
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.slice_ref_.borrow().len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.slice_ref_.borrow().len() == self.offset_
    }

    pub fn as_slice(&self) -> &[T] {
        #[cfg(test)]
        {
            let p = self as *const Self;
            std::println!("[{:p}]SegmRef::as_slice_mut, self.offset_: {}", p,  self.offset_);
        }
        let slice: &[T] = self.slice_ref_.borrow();
        &slice[self.offset_..]
    }

    pub fn iter_slices(&self) -> Option<&[T]> {
        if self.is_empty() {
            Option::None
        } else {
            Option::Some(self.as_slice())
        }
    }

    pub fn take_segm_ref<'a>(
        &'a mut self,
        length: &impl RangeBounds<usize>,
    ) -> Option<SegmRef<'a, &'a [T], T, SegmSelfReclaim>> {
        let Result::Ok(demand) = Demand::try_from_usize_range(length) else {
            return Option::None
        };
        if self.is_empty() {
            return Option::None
        };
        debug_assert!(self.as_slice().len() >= 1usize);
        let offset_ptr = unsafe {
            // self.offset_ is to be update only during drop, where no race should happen.
            NonNull::new_unchecked(&mut self.offset_ as *mut usize)
        };
        let available = Demand::less_than(self.as_slice().len());
        let compromised = demand.compromise(&available)?;
        let len = compromised.len();
        let dst = &self.as_slice()[..len];
        let reclaim = SegmSelfReclaim::new(offset_ptr);
        Option::Some(SegmRef::new(dst, Option::Some(reclaim)))
    }
}

impl<B, T> SegmRef<'_, B, T, NoReclaim>
where
    B: Borrow<[T]>,
{
    /// Create by borrowing a slice from an implicit source but no reclaim
    #[inline]
    pub const fn no_reclaim(slice: B) -> Self {
        SegmRef::new(slice, Option::None)
    }
}

impl<B, T, R> Drop for SegmRef<'_, B, T, R>
where
    B: Borrow<[T]>,
    R: TrReclaim<T>,
{
    fn drop(&mut self) {
        let Option::Some(mut r) = self.reclaim_.take() else {
            return;
        };
        #[cfg(test)]std::println!("[{:p}]SegmRef::drop, before reclaim, self.offset_: {}", self as *mut Self, self.offset_);
        r.reclaim(self);
        #[cfg(test)]std::println!("[{:p}]SegmRef::drop, after reclaim, self.offset_: {}", self as *mut Self, self.offset_);
    }
}

impl<B, T, R> Deref for SegmRef<'_, B, T, R>
where
    B: Borrow<[T]>,
    R: TrReclaim<T>,
{
    type Target = [T];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<B, T, R> Borrow<[T]> for SegmRef<'_, B, T, R>
where
    B: Borrow<[T]>,
    R: TrReclaim<T>,
{
    #[inline]
    fn borrow(&self) -> &[T] {
        self.as_slice()
    }
}

impl<B, T, R> AsRef<[T]> for SegmRef<'_, B, T, R>
where
    B: Borrow<[T]>,
    R: TrReclaim<T>,
{
    #[inline]
    fn as_ref(&self) -> &[T] {
        self.as_slice()
    }
}

impl<B, T, R> TrBuffSegmView for SegmRef<'_, B, T, R>
where
    B: Borrow<[T]>,
    R: TrReclaim<T>,
{
    type Item = T;

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

impl<B, T, R> TrBuffSegmRef<T> for SegmRef<'_, B, T, R>
where
    B: Borrow<[T]>,
    R: TrReclaim<T>,
{
    #[inline]
    fn take_segm_ref<'a>(
        &'a mut self,
        length: &impl RangeBounds<usize>,
    ) -> impl 'a + Try<Output: 'a + TrBuffSegmRef<T>> {
        SegmRef::take_segm_ref(self, length)
    }
}

#[cfg(test)]
mod tests_ {
    use core::ptr::NonNull;

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
            let offset_ptr = NonNull::new_unchecked(&mut segm.offset_ as *mut usize);
            SegmSelfReclaim::new(offset_ptr)
        });
        let slice = segm.as_slice();
        assert_eq!(segm.len(), ARR_SIZE);
        assert_eq!(slice.len(), ARR_SIZE);

        const SLICE_LEN: usize = ARR_SIZE >> 1;
        if true {
            let first_range = ..SLICE_LEN;
            let first_take = segm.take_segm_ref(&first_range);

            std::println!("segm_ref first_take");

            if let Option::Some(taken_slice) = &first_take {
                assert_eq!(taken_slice.as_slice().len(), SLICE_LEN);
                for (u, x) in taken_slice.as_slice().iter().enumerate() {
                    assert_eq!(*x, u)
                }
            } else {
                panic!("first_take failed")
            }
        }
        assert_eq!(segm.as_slice().len(), ARR_SIZE - SLICE_LEN);
        if true {
            let second_range = ..ARR_SIZE;
            let second_take = segm.take_segm_ref(&second_range);

            std::println!("segm_ref second_take");

            if let Option::Some(taken_slice) = &second_take {
                assert_eq!(taken_slice.len(), ARR_SIZE - SLICE_LEN);
                for (u, x) in taken_slice.as_ref().iter().enumerate() {
                    assert_eq!(*x, u + SLICE_LEN)
                }
            }
        }
        assert_eq!(segm.len(), 0);
        std::println!("segm_ref test succ");
    }
}
