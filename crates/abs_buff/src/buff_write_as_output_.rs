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
    buff_segm_as_output_::{buff_segm_mut_write, buff_segm_mut_write_cloned},
    TrBuffWrite, TrOutput,
};

pub struct BuffWriteAsOutput<B, W, T>(B, PhantomData<W>, PhantomData<[T]>)
where
    B: BorrowMut<W>,
    W: TrBuffWrite<T>;

impl<B, W, T> BuffWriteAsOutput<B, W, T>
where
    B: BorrowMut<W>,
    W: TrBuffWrite<T>,
{
    pub const fn new(r: B) -> Self {
        BuffWriteAsOutput(r, PhantomData, PhantomData)
    }

    pub fn write_async<'a>(
        &'a mut self,
        source: &'a [MaybeUninit<T>],
    ) -> BuffWriteOutputAsync<'a, W, T> {
        BuffWriteOutputAsync(self.0.borrow_mut(), source)
    }

    pub fn write_cloned_async<'a>(
        &'a mut self,
        source: &'a [T],
    ) -> BuffWriteOutputClonedAsync<'a, W, T>
    where
        T: Clone,
    {
        BuffWriteOutputClonedAsync(self.0.borrow_mut(), source)
    }
}

impl<'a, W, T> From<&'a mut W> for BuffWriteAsOutput<&'a mut W, W, T>
where
    W: TrBuffWrite<T>,
{
    fn from(value: &'a mut W) -> Self {
        BuffWriteAsOutput::<&'a mut W, W, T>::new(value)
    }
}

impl<W, T> From<W> for BuffWriteAsOutput<W, W, T>
where
    W: TrBuffWrite<T>,
{
    fn from(value: W) -> Self {
        BuffWriteAsOutput::new(value)
    }
}

impl<B, W, T> TrOutput<T> for BuffWriteAsOutput<B, W, T>
where
    B: BorrowMut<W>,
    W: TrBuffWrite<T>,
{
    type Err = <W as TrBuffWrite<T>>::Err;

    #[inline]
    fn write_async<'a>(
        &'a mut self,
        source: &'a [MaybeUninit<T>],
    ) -> impl TrMayCancel<'a, MayCancelOutput = SomeOf<usize, Self::Err>> {
        BuffWriteAsOutput::write_async(self, source)
    }

    #[inline]
    fn write_cloned_async<'a>(
        &'a mut self,
        source: &'a [T],
    ) -> impl TrMayCancel<'a, MayCancelOutput = SomeOf<usize, Self::Err>>
    where
        T: Clone,
    {
        BuffWriteAsOutput::write_cloned_async(self, source)
    }
}

#[gen_may_cancel_future(BuffWriteOutput)]
async fn buff_write_output_async<'f, W, T, C>(
    buff_w: &'f mut W,
    source: &'f [MaybeUninit<T>],
    cancel: Pin<&'f mut C>,
) -> SomeOf<usize, <W as TrBuffWrite<T>>::Err>
where
    W: TrBuffWrite<T>,
    C: TrCancellationToken,
{
    let demand = ..source.len();
    buff_w
        .write_async(&demand)
        .may_cancel_with(cancel)
        .await
        .map_left(|mut s| buff_segm_mut_write(&mut s, source))
}

#[gen_may_cancel_future(BuffWriteOutputCloned)]
async fn buff_write_output_cloned_async<'f, W, T, C>(
    buff_w: &'f mut W,
    source: &'f [T],
    cancel: Pin<&'f mut C>
) -> SomeOf<usize, <W as TrBuffWrite<T>>::Err>
where
    W: TrBuffWrite<T>,
    T: Clone,
    C: TrCancellationToken,
{
    let demand = ..source.len();
    buff_w
        .write_async(&demand)
        .may_cancel_with(cancel)
        .await
        .map_left(|mut s| buff_segm_mut_write_cloned(&mut s, source))
}
