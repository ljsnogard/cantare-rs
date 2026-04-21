use core::{
    borrow::BorrowMut,
    marker::PhantomData,
    mem::MaybeUninit,
    ops::{ControlFlow, Try},
    pin::Pin,
};

use abs_sync::{
    cancellation::TrCancellationToken,
    may_cancel::TrMayCancel,
};
use anylr::SomeOf;
use gen_mcf_macro::gen_may_cancel_future;

use crate::{
    buff_segm_as_input_::buff_segm_ref_read,
    TrBuffPeek, TrBuffSegmRef, TrInput,
};

pub struct BuffPeekAsInput<B, P, T>
where
    B: BorrowMut<P>,
    P: TrBuffPeek<T>,
{
    buff_p_: B,
    offset_: usize,
    _use_p_: PhantomData<P>,
    _use_t_: PhantomData<[T]>,
}

impl<B, P, T> BuffPeekAsInput<B, P, T>
where
    B: BorrowMut<P>,
    P: TrBuffPeek<T>,
{
    pub const fn new(buff_p: B, offset: usize) -> Self {
        BuffPeekAsInput {
            buff_p_: buff_p,
            offset_: offset,
            _use_p_: PhantomData,
            _use_t_: PhantomData,
        }
    }
}

impl<'a, P, T> From<&'a mut P> for BuffPeekAsInput<&'a mut P, P, T>
where
    P: TrBuffPeek<T>,
{
    fn from(value: &'a mut P) -> Self {
        BuffPeekAsInput::new(value, 0usize)
    }
}

impl<P, T> From<P> for BuffPeekAsInput<P, P, T>
where
    P: TrBuffPeek<T>,
{
    fn from(value: P) -> Self {
        BuffPeekAsInput::new(value, 0usize)
    }
}

impl<B, P, T> TrInput<T> for BuffPeekAsInput<B, P, T>
where
    B: BorrowMut<P>,
    P: TrBuffPeek<T>,
{
    type Err = <P as TrBuffPeek<T>>::Err;

    fn read_async<'a>(
        &'a mut self,
        target: &'a mut [MaybeUninit<T>],
    ) -> impl TrMayCancel<'a, MayCancelOutput = SomeOf<usize, Self::Err>> {
        BuffPeekInputAsync(self, target)
    }
}

#[gen_may_cancel_future(BuffPeekInput)]
async fn buff_peek_input_async<'f, B, P, T, C>(
    input: &'f mut BuffPeekAsInput<B, P, T>,
    target: &'f mut [MaybeUninit<T>],
    cancel: Pin<&'f mut C>,
) -> SomeOf<usize, <P as TrBuffPeek<T>>::Err>
where
    B: BorrowMut<P>,
    P: TrBuffPeek<T>,
    C: TrCancellationToken,
{
    let buff_p: &mut P = input.buff_p_.borrow_mut();
    let (opt_segm, opt_err) = buff_p
        .peek_async()
        .may_cancel_with(cancel)
        .await
        .into_any_of()
        .split();
    let mut copied = 0usize;
    if let Option::Some(mut segment) = opt_segm {
        let length = ..input.offset_;
        if true {
            let branch = segment.take_segm_ref(&length).branch();
            let ControlFlow::Continue(prev_done) = branch else {
                return SomeOf::new_left(copied)
            };
            drop(prev_done);
        }
        copied = buff_segm_ref_read(&mut segment, target);
    };
    input.offset_ += copied;
    if let Option::Some(err) = opt_err {
        if copied > 0 {
            SomeOf::new_both(copied, err)
        } else {
            SomeOf::new_right(err)
        }
    } else {
        SomeOf::new_left(copied)
    }
}
