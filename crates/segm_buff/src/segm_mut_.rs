use core::{
    borrow::{Borrow, BorrowMut},
    marker::PhantomPinned,
    mem::MaybeUninit,
    ops::{Deref, DerefMut, RangeBounds, Try},
};

use abs_buff::{Demand, TrBuffSegmMut, TrBuffSegmView, TrBuffer, TrMaybeUninit};

use super::{
    reclaim_::SegmSelfReclaim,
    NoReclaim, TrReclaim,
};

type SliceElem<B> =
    <<<B as Deref>::Target as TrBuffer>::Slot as TrMaybeUninit>::Inner;

type SliceInit<B> = [SliceElem<B>];

type SliceUninit<B> = [MaybeUninit<SliceElem<B>>];

type ChildSegm<'a, B, R> = SegmMut<
    &'a mut [MaybeUninit<SliceElem<B>>],
    R,
>;

/// The rented slice for tx of the [RingBuffer](crate::ring_buffer::RingBuffer)
#[repr(C)]
pub struct SegmMut<B, R>
where
    B: DerefMut<Target: TrBuffer>,
    R: TrReclaim,
{
    _pinned_: PhantomPinned,
    offset_: usize,
    reclaim_: Option<R>,
    buffer_: B,
}

impl<B, R> SegmMut<B, R>
where
    B: DerefMut<Target: TrBuffer>,
    R: TrReclaim,
{
    /// Create by borrowing a mut slice from an implicit source. And the items
    /// of this mut slice will be returned back to or moved out from the source
    /// by `reclaim`.
    ///
    /// ## Safety
    ///
    /// - `reclaim` should be capable of semantically move item out when this slice `into_iter`
    pub const fn new(buffer: B, reclaim: Option<R>) -> Self {
        SegmMut {
            _pinned_: PhantomPinned,
            offset_: 0usize,
            reclaim_: reclaim,
            buffer_: buffer,
        }
    }

    /// Check if the segment has no available item to consume.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.capacity() == self.offset_
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.buffer_.as_slice_uninit().len()
    }

    pub fn iter_slices(&self) -> Option<&SliceUninit<B>> {
        if self.is_empty() {
            Option::None
        } else {
            Option::Some(self.as_slice())
        }
    }

    pub fn iter_slices_mut(&mut self) -> Option<&mut SliceUninit<B>> {
        if self.is_empty() {
            Option::None
        } else {
            Option::Some(self.as_slice_mut())
        }
    }

    pub fn as_slice(&self) -> &SliceUninit<B> {
        let slice= self.buffer_.as_slice_uninit();
        #[cfg(test)]
        {
            let c = self.capacity();
            let o = self.offset_;
            let p = self as *const Self;
            let l = slice.len();
            std::println!("[{:p}]SegmMut::as_slice(capacity: {c}), self.offset_: {o}, buff.len: {l}", p);
        }
        &slice[self.offset_..]
    }

    pub fn as_slice_mut(&mut self) -> &mut SliceUninit<B> {
        
        #[cfg(test)]
        {
            let c = self.capacity();
            let o = self.offset_;
            let p = self as *mut Self;
            let l = self.buffer_.as_mut_slice_uninit().len();
            std::println!("[{:p}]SegmMut::as_slice_mut(capacity: {c}), self.offset_: {o}, buff.len: {l}", p);
        }
        let slice = self.buffer_.as_mut_slice_uninit();
        &mut slice[self.offset_..]
    }

    pub fn take_segm_mut<'a>(
        &'a mut self,
        demand: &Demand<usize>,
    ) -> Option<ChildSegm<'a, B, SegmSelfReclaim<'a>>> {
        if self.is_empty() {
            return Option::None
        };
        debug_assert!(!self.as_slice().is_empty());
        let available = Demand::less_than(self.as_slice().len());
        let compromised = demand.compromise(&available)?;
        let len = compromised.len();
        let dst: &'a mut SliceUninit<B> = unsafe {
            let p = &mut self.as_slice_mut()[..len]
                as *mut SliceUninit<B>;
            &mut *p
        };
        let reclaim = Option::Some(SegmSelfReclaim::new(&mut self.offset_));
        Option::Some(SegmMut::new(dst, reclaim))
    }
}

impl<B> SegmMut<B, NoReclaim>
where
    B: DerefMut<Target: TrBuffer>,
{
    /// Create by borrowing a slice from an implicit source.
    pub const fn no_reclaim(slice_mut: B) -> Self {
        SegmMut::new(slice_mut, Option::None)
    }
}

impl<B, R> SegmMut<B, R>
where
    B: DerefMut<Target: TrBuffer>,
    <<<B as Deref>::Target as TrBuffer>::Slot as TrMaybeUninit>::Inner: Copy,
    R: TrReclaim,
{
    /// A convenient wrapper around [copy_from_slice](<[T]>::copy_from_slice)
    pub fn copy_from_slice(&mut self, src: &SliceInit<B>) {
        let slice = unsafe {
            let p = self.as_slice_mut()
                as *mut [MaybeUninit<_>]
                as *mut SliceInit<B>;
            &mut *p
        };
        slice.copy_from_slice(src);
    }
}

impl<B, R> Drop for SegmMut<B, R>
where
    B: DerefMut<Target: TrBuffer>,
    R: TrReclaim,
{
    fn drop(&mut self) {
        let Option::Some(mut r) = self.reclaim_.take() else {
            return;
        };
        #[cfg(test)]std::println!("[{:p}]SegmMut::drop, before reclaim, self.offset_: {}", self as *mut Self, self.offset_);
        r.reclaim(self.capacity());
        #[cfg(test)]std::println!("[{:p}]SegmMut::drop, after reclaim, self.offset_: {}", self as *mut Self, self.offset_);
    }
}

impl<B, R> Borrow<SliceUninit<B>> for SegmMut<B, R>
where
    B: DerefMut<Target: TrBuffer>,
    R: TrReclaim,
{
    #[inline]
    fn borrow(&self) -> &SliceUninit<B> {
        self.as_slice()
    }
}

impl<B, R> BorrowMut<SliceUninit<B>> for SegmMut<B, R>
where
    B: DerefMut<Target: TrBuffer>,
    R: TrReclaim,
{
    #[inline]
    fn borrow_mut(&mut self) -> &mut SliceUninit<B> {
        self.as_slice_mut()
    }
}

impl<B, R> AsRef<SliceUninit<B>> for SegmMut<B, R>
where
    B: DerefMut<Target: TrBuffer>,
    R: TrReclaim,
{
    #[inline]
    fn as_ref(&self) -> &SliceUninit<B> {
        self.as_slice()
    }
}

impl<B, R> AsMut<SliceUninit<B>> for SegmMut<B, R>
where
    B: DerefMut<Target: TrBuffer>,
    R: TrReclaim,
{
    #[inline]
    fn as_mut(&mut self) -> &mut SliceUninit<B> {
        self.as_slice_mut()
    }
}

impl<B, R> Deref for SegmMut<B, R>
where
    B: DerefMut<Target: TrBuffer>,
    R: TrReclaim,
{
    type Target = SliceUninit<B>;

    #[inline]
    fn deref(&self) -> &SliceUninit<B> {
        self.as_slice()
    }
}

impl<B, R> DerefMut for SegmMut<B, R>
where
    B: DerefMut<Target: TrBuffer>,
    R: TrReclaim,
{
    #[inline]
    fn deref_mut(&mut self) -> &mut SliceUninit<B> {
        self.as_slice_mut()
    }
}

impl<B, R> TrBuffSegmView for SegmMut<B, R>
where
    B: DerefMut<Target: TrBuffer>,
    R: TrReclaim,
{
    type Item = MaybeUninit<SliceElem<B>>;

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

impl<B, R> TrBuffSegmMut<SliceElem<B>> for SegmMut<B, R>
where
    B: DerefMut<Target: TrBuffer>,
    R: TrReclaim,
{
    #[inline]
    fn iter_slices_mut<'a>(
        &'a mut self,
    ) -> impl IntoIterator<Item: 'a + AsMut<[MaybeUninit<SliceElem<B>>]>> {
        SegmMut::iter_slices_mut(self)
    }

    #[inline]
    fn take_segm_mut<'a>(
        &'a mut self,
        demand: &impl RangeBounds<usize>,
    ) -> impl 'a + Try<Output: 'a + TrBuffSegmMut<SliceElem<B>>> {
        let r = Demand::try_from_usize_range(demand);
        let Result::Ok(demand) = &r else {
            return Option::None
        };
        SegmMut::take_segm_mut(self, demand)
    }
}

#[cfg(test)]
mod tests_ {
    use core::{mem::MaybeUninit, ptr::NonNull};

    use crate::SegmSelfReclaim;

    use super::{Demand, SegmMut};

    #[test]
    fn segm_len_should_eq_as_slice_len() {
        const ARR_SIZE: usize = 64;
        let mut buff = [MaybeUninit::zeroed(); ARR_SIZE];
        for (u, x) in buff.iter_mut().enumerate() {
            let _ = *x.write(u);
            std::println!("segm_mut wrote: {}", u)
        }
        let mut segm = SegmMut::new(
            &mut buff,
            Option::<SegmSelfReclaim>::None,
        );
        segm.reclaim_ = Option::Some(unsafe {
            let mut offset_ptr = NonNull::new_unchecked(&mut segm.offset_ as *mut usize);
            SegmSelfReclaim::new(offset_ptr.as_mut())
        });
        let slice = segm.as_slice();
        assert_eq!(segm.len(), ARR_SIZE);
        assert_eq!(slice.len(), segm.len());

        const SLICE_LEN: usize = ARR_SIZE >> 1;
        if true {
            let first_range = Demand::less_than(SLICE_LEN);
            let first_take = segm.take_segm_mut(&first_range);

            std::println!("segm_mut first take");

            if let Option::Some(taken_slice) = &first_take {
                assert_eq!(taken_slice.len(), SLICE_LEN);
                for (u, x) in taken_slice.iter().enumerate() {
                    assert_eq!(unsafe { x.assume_init_read() }, u)
                }
            } else {
                panic!("segm_mut first_take failed.")
            }
        }
        assert_eq!(segm.len(), ARR_SIZE - SLICE_LEN);
        if true {
            let second_range = Demand::less_than(ARR_SIZE);
            let second_take = segm.take_segm_mut(&second_range);

            std::println!("segm_mut second take");

            if let Option::Some(taken_slice) = &second_take {
                assert_eq!(taken_slice.len(), ARR_SIZE - SLICE_LEN);
                for (u, x) in taken_slice.iter().enumerate() {
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
