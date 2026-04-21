use core::{
    borrow::BorrowMut,
    convert::Infallible,
    marker::PhantomData,
    mem::{self, MaybeUninit},
    ops::{ControlFlow, Try},
    pin::Pin,
    ptr,
};

use abs_sync::{cancellation::TrCancellationToken, may_cancel::TrMayCancel};
use anylr::SomeOf;
use gen_mcf_macro::gen_may_cancel_future;

use crate::{TrBuffSegmMut, TrOutput};

pub struct BuffSegmMutAsOutput<B, S, T>
where
    B: BorrowMut<S>,
    S: TrBuffSegmMut<T>,
{
    _mark_s_: PhantomData<S>,
    _mark_t_: PhantomData<T>,
    segment_: B,
}

impl<B, S, T> BuffSegmMutAsOutput<B, S, T>
where
    B: BorrowMut<S>,
    S: TrBuffSegmMut<T>,
{
    pub const fn new(segment: B) -> Self {
        BuffSegmMutAsOutput {
            _mark_s_: PhantomData,
            _mark_t_: PhantomData,
            segment_: segment,
        }
    }

    pub fn write<'a>(&'a mut self, source: &'a [MaybeUninit<T>]) -> usize {
        self::buff_segm_mut_write(self.segment_.borrow_mut(), source)
    }

    pub fn write_cloned<'a>(&'a mut self, source: &'a [T]) -> usize
    where
        T: Clone
    {
        self::buff_segm_mut_write_cloned(self.segment_.borrow_mut(), source)
    }

    pub fn write_async<'a>(
        &'a mut self,
        source: &'a [MaybeUninit<T>],
    ) -> BuffSegmMutOutputAsync<'a, S, T> {
        BuffSegmMutOutputAsync(self.segment_.borrow_mut(), source)
    }

    pub fn write_cloned_async<'a>(
        &'a mut self,
        source: &'a [T],
    ) -> BuffSegmMutOutputClonedAsync<'a, S, T>
    where
        T: Clone,
    {
        BuffSegmMutOutputClonedAsync(self.segment_.borrow_mut(), source)
    }
}

impl<B, S, T> TrOutput<T> for BuffSegmMutAsOutput<B, S, T>
where
    B: BorrowMut<S>,
    S: TrBuffSegmMut<T>,
{
    type Err = Infallible;

    #[inline]
    fn write_async<'a>(
        &'a mut self,
        source: &'a [MaybeUninit<T>],
    ) -> impl TrMayCancel<'a, MayCancelOutput = SomeOf<usize, Self::Err>> {
        BuffSegmMutAsOutput::write_async(self, source)
    }
}

#[gen_may_cancel_future(BuffSegmMutOutput)]
async fn buff_segm_output_async<'f, S, T, C>(
    segm_mut: &'f mut S,
    source: &'f [MaybeUninit<T>],
    _: Pin<&'f mut C>,
) -> SomeOf<usize, Infallible>
where
    S: TrBuffSegmMut<T>,
    C: TrCancellationToken,
{
    SomeOf::new_left(buff_segm_mut_write(segm_mut, source))
}

#[gen_may_cancel_future(BuffSegmMutOutputCloned)]
async fn buff_segm_output_cloned_async<'f, S, T, C>(
    segm_mut: &'f mut S,
    source: &'f [T],
    _: Pin<&'f mut C>,
) -> SomeOf<usize, Infallible>
where
    S: TrBuffSegmMut<T>,
    T: Clone,
    C: TrCancellationToken,
{
    SomeOf::new_left(buff_segm_mut_write_cloned(segm_mut, source))
}

pub(crate) fn buff_segm_mut_write<'f, S, T>(
    segment: &'f mut S,
    source: &'f [MaybeUninit<T>],
) -> usize
where
    S: TrBuffSegmMut<T>,
{
    let length = ..source.len();
    let branch = segment.take_segm_mut(&length).branch();
    let ControlFlow::Continue(mut parts) = branch else {
        return 0
    };
    let mut copied = 0usize;
    for mut dst in parts.iter_slices_mut() {
        let dst = dst.as_mut();
        let copy_len = dst.len();
        let src = &source[copied..copy_len];
        let src_head = (&src[0]) as *const MaybeUninit<T>;
        let dst_head = (&mut dst[0]) as *mut MaybeUninit<T>;

        // This is sound because it is semantically a move operation since `src`
        // will drop and convert the "copied" items into `MaybeUninit`
        unsafe { ptr::copy_nonoverlapping(src_head, dst_head, copy_len) };
        copied += copy_len;
    }
    copied
}

pub(crate) fn buff_segm_mut_write_cloned<'f, S, T>(
    segment: &'f mut S,
    source: &'f [T],
) -> usize
where
    S: TrBuffSegmMut<T>,
    T: Clone,
{
    let length = ..source.len();
    let branch = segment.take_segm_mut(&length).branch();
    let ControlFlow::Continue(mut parts) = branch else {
        return 0
    };
    let mut copied = 0usize;

    // If `T: Clone` needs drop, we must preserve the clone semantic when
    // copying into the segment. This promises the correct behaviours when
    // cloning items like `Rc` or `Arc`
    if mem::needs_drop::<T>() {
        for mut dst in parts.iter_slices_mut() {
            let dst = dst.as_mut();
            let src = &source[copied..];
            for i in 0..dst.len() {
                let m = &mut dst[i];
                m.write(src[i].clone());
            }
            copied += dst.len()
        }
    } else {
        for mut dst in parts.iter_slices_mut() {
            let dst = unsafe {
                let p = dst.as_mut() as *mut _ as *mut [T];
                &mut *p
            };
            let src = &source[copied..copied + dst.len()];
            dst.clone_from_slice(src);
            copied += dst.len();
        }
    }
    copied
}
