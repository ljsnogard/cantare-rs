use core::{
    borrow::BorrowMut,
    convert::Infallible,
    marker::PhantomData,
    mem::MaybeUninit,
    ops::{ControlFlow, Try},
    pin::Pin,
    ptr
};

use abs_sync::{cancellation::TrCancellationToken, may_cancel::TrMayCancel};
use anylr::SomeOf;
use gen_mcf_macro::gen_may_cancel_future;

use crate::{
    buff_segm_::{TrBuffSegmRef, TrBuffSegmView},
    io::TrInput,
    Demand,
};

pub struct BuffSegmRefAsInput<B, S, T>
where
    B: BorrowMut<S>,
    S: TrBuffSegmRef<T>,
{
    _mark_s_: PhantomData<S>,
    _mark_t_: PhantomData<T>,
    segment_: B,
}

impl<B, S, T> BuffSegmRefAsInput<B, S, T>
where
    B: BorrowMut<S>,
    S: TrBuffSegmRef<T>,
{
    pub const fn new(segment: B) -> Self {
        BuffSegmRefAsInput {
            _mark_s_: PhantomData,
            _mark_t_: PhantomData,
            segment_: segment,
        }
    }

    pub fn read<'a>(
        &'a mut self,
        target: &'a mut [MaybeUninit<T>],
    ) -> usize {
        self::buff_segm_ref_read(self.segment_.borrow_mut(), target)
    }

    pub fn read_async<'a>(
        &'a mut self,
        target: &'a mut [MaybeUninit<T>],
    ) -> BuffSegmRefInputAsync<'a, S, T> {
        BuffSegmRefInputAsync(self.segment_.borrow_mut(), target)
    }
}

impl<B, S, T> TrInput<T> for BuffSegmRefAsInput<B, S, T>
where
    B: BorrowMut<S>,
    S: TrBuffSegmRef<T>,
{
    type Err = Infallible;

    #[inline]
    fn read_async<'a>(
        &'a mut self,
        target: &'a mut [MaybeUninit<T>],
    ) -> impl TrMayCancel<'a, MayCancelOutput = SomeOf<usize, Self::Err>> {
        BuffSegmRefAsInput::read_async(self, target)
    }
}

#[gen_may_cancel_future(BuffSegmRefInput)]
pub(crate) async fn buff_segm_ref_input_async<'f, S, T, C>(
    segm_ref: &'f mut S,
    target: &'f mut [MaybeUninit<T>],
    _: Pin<&'f mut C>,
) -> SomeOf<usize, Infallible>
where
    S: TrBuffSegmRef<T>,
    C: TrCancellationToken,
{
    SomeOf::new_left(self::buff_segm_ref_read(segm_ref, target))
}

pub(crate) fn buff_segm_ref_read<'a, S, T>(
    segment: &'a mut S,
    target: &'a mut [MaybeUninit<T>],
) -> usize
where
    S: TrBuffSegmRef<T>,
    T: Sized,
{
    let demand = Demand::less_than(target.len());
    let branch = segment.take_segm_ref(&demand).branch();
    let ControlFlow::Continue(parts) = branch else {
        return 0;
    };
    let mut copied = 0usize;
    for src in parts.iter_slices() {
        let src = src.as_ref();
        let copy_len = src.len();
        let dst = &mut target[copied..copy_len];
        let src_head = (&src[0]) as *const T;
        let dst_head = (&mut dst[0]) as *mut MaybeUninit<T> as *mut T;

        // This is sound because it is semantically a move operation since `src`
        // will drop and convert the "copied" items into `MaybeUninit`
        unsafe { ptr::copy_nonoverlapping(src_head, dst_head, copy_len) };
        copied += copy_len;
    }
    copied
}
