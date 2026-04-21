use core::{
    borrow::BorrowMut,
    marker::PhantomData,
    mem::MaybeUninit,
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
    TrBuffRead, TrInput,
};

pub struct BuffReadAsInput<B, R, T>(B, PhantomData<R>, PhantomData<[T]>)
where
    B: BorrowMut<R>,
    R: TrBuffRead<T>;

impl<B, R, T> BuffReadAsInput<B, R, T>
where
    B: BorrowMut<R>,
    R: TrBuffRead<T>,
{
    pub const fn new(r: B) -> Self {
        BuffReadAsInput(r, PhantomData, PhantomData)
    }
}

impl<'a, R, T> From<&'a mut R> for BuffReadAsInput<&'a mut R, R, T>
where
    R: TrBuffRead<T>,
{
    fn from(value: &'a mut R) -> Self {
        BuffReadAsInput::new(value)
    }
}

impl<R, T> From<R> for BuffReadAsInput<R, R, T>
where
    R: TrBuffRead<T>,
{
    fn from(value: R) -> Self {
        BuffReadAsInput::new(value)
    }
}

impl<B, R, T> TrInput<T> for BuffReadAsInput<B, R, T>
where
    B: BorrowMut<R>,
    R: TrBuffRead<T>,
{
    type Err = <R as TrBuffRead<T>>::Err;

    fn read_async<'a>(
        &'a mut self,
        target: &'a mut [MaybeUninit<T>],
    ) -> impl TrMayCancel<'a, MayCancelOutput = SomeOf<usize, Self::Err>> {
        BuffReadInputAsync(self.0.borrow_mut(), target)
    }
}

#[gen_may_cancel_future(BuffReadInput)]
async fn buff_read_input_async<'a, R, T, C>(
    buff_r: &'a mut R,
    target: &'a mut [MaybeUninit<T>],
    cancel: Pin<&'a mut C>,
) -> SomeOf<usize, <R as TrBuffRead<T>>::Err>
where
    R: TrBuffRead<T>,
    C: TrCancellationToken,
{
    let demand = ..target.len();
    buff_r
        .read_async(&demand)
        .may_cancel_with(cancel)
        .await
        .map_left(|mut s| buff_segm_ref_read(&mut s, target))
}
