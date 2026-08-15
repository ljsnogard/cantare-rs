use core::{
    borrow::BorrowMut,
    marker::PhantomData,
    mem::MaybeUninit,
};

use abs_cancel::{TrCancellationToken, TrMayCancel};

use anylr:: SomeOf;

use gen_mcf_macro::gen_may_cancel_future;

use crate::{
    buff_segm_as_output_::{buff_segm_mut_write, buff_segm_mut_write_cloned},
    Demand, TrBuffWrite, io::TrOutput,
};

/// The default return type of `fn as_output()` in `TrBuffWrite`.
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
    cancel: &'f mut C,
) -> SomeOf<usize, <W as TrBuffWrite<T>>::Err>
where
    W: TrBuffWrite<T>,
    C: TrCancellationToken + Clone,
{
    let mut c = 0usize;
    loop {
        if c >= source.len() {
            return SomeOf::new_left(c);
        };
        let src = &source[c..];
        let demand = Demand::less_than(src.len());
        let result = buff_w
            .write_async(&demand)
            .may_cancel_with(cancel)
            .await
            .map_left(|mut s| buff_segm_mut_write(&mut s, src));
        if let Option::Some(delta) = result.as_ref().pick_left() {
            c += *delta;
        }
        if let Option::Some(err) = result.pick_right() {
            return if c == 0 {
                SomeOf::new_right(err)
            } else {
                SomeOf::new_both(c, err)
            };
        }
        if cancel.is_cancelled() {
            return SomeOf::new_left(c);
        }
    }
}

#[gen_may_cancel_future(BuffWriteOutputCloned)]
async fn buff_write_output_cloned_async<'f, W, T, C>(
    buff_w: &'f mut W,
    source: &'f [T],
    cancel: &'f mut C,
) -> SomeOf<usize, <W as TrBuffWrite<T>>::Err>
where
    W: TrBuffWrite<T>,
    T: Clone,
    C: TrCancellationToken + Clone,
{
    let mut c = 0usize;
    loop {
        if c >= source.len() {
            return SomeOf::new_left(c);
        };
        let src = &source[c..];
        let demand = Demand::less_than(src.len());
        let result = buff_w
            .write_async(&demand)
            .may_cancel_with(cancel)
            .await
            .map_left(|mut s| buff_segm_mut_write_cloned(&mut s, src));
        if let Option::Some(delta) = result.as_ref().pick_left() {
            c += *delta;
        }
        if let Option::Some(err) = result.pick_right() {
            return if c == 0 {
                SomeOf::new_right(err)
            } else {
                SomeOf::new_both(c, err)
            };
        }
        if cancel.is_cancelled() {
            return SomeOf::new_left(c);
        }
    }
}
