use core::{
    borrow::{Borrow, BorrowMut},
    marker::{PhantomData, PhantomPinned},
    mem::MaybeUninit,
    ops::{Deref, DerefMut, RangeBounds, Try},
    ptr::NonNull,
};

use abs_buff::{Demand, TrBuffSegmMut, TrBuffSegmView};

use super::{
    reclaim_::SegmSelfReclaim,
    NoReclaim, TrReclaim,
};

/// The rented slice for tx of the [RingBuffer](crate::ring_buffer::RingBuffer)
#[repr(C)]
pub struct SegmMut<'b, B, T, R>
where
    B: BorrowMut<[MaybeUninit<T>]>,
    R: TrReclaim<MaybeUninit<T>>,
{
    _mark_t_: PhantomData<&'b mut [MaybeUninit<T>]>,
    _pinned_: PhantomPinned,
    offset_: usize,
    reclaim_: Option<R>,
    slice_mut_: B,
}

impl<B, T, R> SegmMut<'_, B, T, R>
where
    B: BorrowMut<[MaybeUninit<T>]>,
    R: TrReclaim<MaybeUninit<T>>,
{
    /// Create by borrowing a mut slice from an implicit source. And the items
    /// of this mut slice will be returned back to or moved out from the source
    /// by `reclaim`.
    ///
    /// ## Safety
    ///
    /// - `reclaim` should be capable of semantically move item out when this
    ///     slice `into_iter`
    pub const fn new(slice_mut: B, reclaim: Option<R>) -> Self {
        SegmMut {
            _mark_t_: PhantomData,
            _pinned_: PhantomPinned,
            offset_: 0usize,
            reclaim_: reclaim,
            slice_mut_: slice_mut,
        }
    }

    /// Check if the segment has no available item to consume.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.slice_mut_.borrow().len() == self.offset_
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.slice_mut_.borrow().len()
    }

    pub fn iter_slices(&self) -> Option<&[MaybeUninit<T>]> {
        if self.is_empty() {
            Option::None
        } else {
            Option::Some(self.as_slice())
        }
    }

    pub fn iter_slices_mut(&mut self) -> Option<&mut [MaybeUninit<T>]> {
        if self.is_empty() {
            Option::None
        } else {
            Option::Some(self.as_slice_mut())
        }
    }

    pub fn as_slice(&self) -> &[MaybeUninit<T>] {
        #[cfg(test)]
        {
            let p = self as *const Self;
            std::println!("[{:p}]SegmMut::as_slice_mut, self.offset_: {}", p,  self.offset_);
        }
        let slice: &[MaybeUninit<T>] = self.slice_mut_.borrow();
        &slice[self.offset_..]
    }

    pub fn as_slice_mut(&mut self) -> &mut [MaybeUninit<T>] {
        #[cfg(test)]
        {
            let p = self as *mut Self;
            std::println!("[{:p}]SegmMut::as_slice_mut, self.offset_: {}", p,  self.offset_);
        }
        let slice: &mut [MaybeUninit<T>] = self.slice_mut_.borrow_mut();
        &mut slice[self.offset_..]
    }

    pub fn take_segm_mut<'a>(
        &'a mut self,
        length: &impl RangeBounds<usize>,
    ) -> Option<SegmMut<'a, &'a mut [MaybeUninit<T>], T, SegmSelfReclaim>> {
        let Result::Ok(length) = Demand::try_from_usize_range(length) else {
            return Option::None
        };
        if self.is_empty() {
            return Option::None
        };
        let offset_ptr = unsafe {
            // self.offset_ is to be update only during drop, where no race should happen.
            NonNull::new_unchecked(&mut self.offset_ as *mut usize)
        };
        debug_assert!(self.as_slice().len() >= 1usize);
        let available = Demand::less_than(self.as_slice().len());
        let compromised = length.compromise(&available)?;
        let len = compromised.len();
        let dst = &mut self.as_slice_mut()[..len];
        let reclaim = SegmSelfReclaim::new(offset_ptr);
        Option::Some(SegmMut::new(dst, Option::Some(reclaim)))
    }
}

impl<B, T> SegmMut<'_, B, T, NoReclaim>
where
    B: BorrowMut<[MaybeUninit<T>]>,
{
    /// Create by borrowing a slice from an implicit source.
    pub const fn no_reclaim(slice_mut: B) -> Self {
        SegmMut::new(slice_mut, Option::None)
    }
}

impl<B, T, R> Drop for SegmMut<'_, B, T, R>
where
    B: BorrowMut<[MaybeUninit<T>]>,
    R: TrReclaim<MaybeUninit<T>>,
{
    fn drop(&mut self) {
        let Option::Some(mut r) = self.reclaim_.take() else {
            return;
        };
        #[cfg(test)]std::println!("[{:p}]SegmMut::drop, before reclaim, self.offset_: {}", self as *mut Self, self.offset_);
        r.reclaim(self);
        #[cfg(test)]std::println!("[{:p}]SegmMut::drop, after reclaim, self.offset_: {}", self as *mut Self, self.offset_);
    }
}

impl<B, T, R> Borrow<[MaybeUninit<T>]> for SegmMut<'_, B, T, R>
where
    B: BorrowMut<[MaybeUninit<T>]>,
    R: TrReclaim<MaybeUninit<T>>,
{
    #[inline]
    fn borrow(&self) -> &[MaybeUninit<T>] {
        self.as_slice()
    }
}

impl<B, T, R> BorrowMut<[MaybeUninit<T>]> for SegmMut<'_, B, T, R>
where
    B: BorrowMut<[MaybeUninit<T>]>,
    R: TrReclaim<MaybeUninit<T>>,
{
    #[inline]
    fn borrow_mut(&mut self) -> &mut [MaybeUninit<T>] {
        self.as_slice_mut()
    }
}

impl<B, T, R> AsRef<[MaybeUninit<T>]> for SegmMut<'_, B, T, R>
where
    B: BorrowMut<[MaybeUninit<T>]>,
    R: TrReclaim<MaybeUninit<T>>,
{
    #[inline]
    fn as_ref(&self) -> &[MaybeUninit<T>] {
        self.as_slice()
    }
}

impl<B, T, R> AsMut<[MaybeUninit<T>]> for SegmMut<'_, B, T, R>
where
    B: BorrowMut<[MaybeUninit<T>]>,
    R: TrReclaim<MaybeUninit<T>>,
{
    #[inline]
    fn as_mut(&mut self) -> &mut [MaybeUninit<T>] {
        self.as_slice_mut()
    }
}

impl<B, T, R> Deref for SegmMut<'_, B, T, R>
where
    B: BorrowMut<[MaybeUninit<T>]>,
    R: TrReclaim<MaybeUninit<T>>,
{
    type Target = [MaybeUninit<T>];

    #[inline]
    fn deref(&self) -> &[MaybeUninit<T>] {
        self.as_slice()
    }
}

impl<B, T, R> DerefMut for SegmMut<'_, B, T, R>
where
    B: BorrowMut<[MaybeUninit<T>]>,
    R: TrReclaim<MaybeUninit<T>>,
{
    #[inline]
    fn deref_mut(&mut self) -> &mut [MaybeUninit<T>] {
        self.as_slice_mut()
    }
}

impl<B, T, R> TrBuffSegmView for SegmMut<'_, B, T, R>
where
    B: BorrowMut<[MaybeUninit<T>]>,
    R: TrReclaim<MaybeUninit<T>>,
{
    type Item = MaybeUninit<T>;

    #[inline]
    fn is_empty(&self) -> bool {
        SegmMut::is_empty(self)
    }

    #[inline]
    fn capacity(&self) -> usize {
        SegmMut::capacity(self)
    }

    fn iter_slices<'a>(
        &'a self,
    ) -> impl IntoIterator<Item: 'a + AsRef<[Self::Item]>> {
        SegmMut::iter_slices(self)
    }
}

impl<B, T, R> SegmMut<'_, B, T, R>
where
    B: BorrowMut<[MaybeUninit<T>]>,
    T: Copy,
    R: TrReclaim<MaybeUninit<T>>,
{
    /// A convenient wrapper around [copy_from_slice](<[T]>::copy_from_slice)
    pub fn copy_from_slice(&mut self, src: &[T]) {
        let slice = unsafe {
            let p = self.deref_mut() as *mut [MaybeUninit<T>] as *mut [T];
            &mut *p
        };
        slice.copy_from_slice(src);
    }
}

impl<B, T, R> TrBuffSegmMut<T> for SegmMut<'_, B, T, R>
where
    B: BorrowMut<[MaybeUninit<T>]>,
    R: TrReclaim<MaybeUninit<T>>,
{
    #[inline]
    fn iter_slices_mut<'a>(
        &'a mut self,
    ) -> impl IntoIterator<Item: 'a + AsMut<[MaybeUninit<T>]>>
    where
        T: 'a,
    {
        SegmMut::iter_slices_mut(self)
    }

    #[inline]
    fn take_segm_mut<'a>(
        &'a mut self,
        length: &impl RangeBounds<usize>,
    ) -> impl 'a + Try<Output: 'a + TrBuffSegmMut<T>> {
        SegmMut::take_segm_mut(self, length)
    }
}

#[cfg(test)]
mod tests_ {
    use core::{mem::MaybeUninit, ptr::NonNull};

    use crate::SegmSelfReclaim;

    use super::SegmMut;

    #[test]
    fn segm_len_should_eq_as_slice_len() {
        const ARR_SIZE: usize = 64;
        let mut buff = [MaybeUninit::zeroed(); ARR_SIZE];
        for (u, x) in buff.iter_mut().enumerate() {
            let _ = *x.write(u);
        }
        let mut segm = SegmMut::new(buff.as_mut_slice(), Option::<SegmSelfReclaim>::None);
        segm.reclaim_ = Option::Some(unsafe {
            let offset_ptr = NonNull::new_unchecked(&mut segm.offset_ as *mut usize);
            SegmSelfReclaim::new(offset_ptr)
        });
        let slice = segm.as_slice();
        assert_eq!(segm.len(), ARR_SIZE);
        assert_eq!(slice.len(), segm.len());

        const SLICE_LEN: usize = ARR_SIZE >> 1;
        if true {
            let first_range = ..SLICE_LEN;
            let first_take = segm.take_segm_mut(&first_range);

            std::println!("segm_mut first take");

            if let Option::Some(taken_slice) = &first_take {
                assert_eq!(taken_slice.as_slice().len(), SLICE_LEN);
                for (u, x) in taken_slice.as_slice().iter().enumerate() {
                    assert_eq!(unsafe { x.assume_init_read() }, u)
                }
            } else {
                panic!("segm_mut first_take failed.")
            }
        }
        assert_eq!(segm.as_slice().len(), ARR_SIZE - SLICE_LEN);
        if true {
            let second_range = ..ARR_SIZE;
            let second_take = segm.take_segm_mut(&second_range);

            std::println!("segm_mut second take");

            if let Option::Some(taken_slice) = &second_take {
                assert_eq!(taken_slice.len(), ARR_SIZE - SLICE_LEN);
                for (u, x) in taken_slice.as_slice().iter().enumerate() {
                    assert_eq!(unsafe { x.assume_init_read() }, u + SLICE_LEN)
                }
            } else {
                panic!("segm_mut second_take failed.")
            }
        }
        assert!(segm.is_empty());
        std::println!("segm_mut test succ")
    }
}
