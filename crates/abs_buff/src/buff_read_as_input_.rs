use core::{
    borrow::BorrowMut,
    marker::PhantomData,
    mem::MaybeUninit,
};

use abs_cancel::{TrCancellationToken, TrMayCancel};
use anylr::SomeOf;
use gen_mcf_macro::gen_may_cancel_future;

use crate::{
    buff_segm_as_input_::buff_segm_ref_read,
    io::TrInput,
    Demand, TrBuffRead,
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

    pub fn read_async<'a>(
        &'a mut self,
        target: &'a mut [MaybeUninit<T>],
    ) -> BuffReadInputAsync<'a, R, T> {
        BuffReadInputAsync(self.0.borrow_mut(), target)
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

    #[inline]
    fn read_async<'a>(
        &'a mut self,
        target: &'a mut [MaybeUninit<T>],
    ) -> impl TrMayCancel<'a, MayCancelOutput = SomeOf<usize, Self::Err>> {
        BuffReadAsInput::read_async(self, target)
    }
}

#[gen_may_cancel_future(BuffReadInput)]
async fn buff_read_input_async<'a, R, T, C>(
    buff_r: &'a mut R,
    target: &'a mut [MaybeUninit<T>],
    cancel: &'a mut C,
) -> SomeOf<usize, <R as TrBuffRead<T>>::Err>
where
    R: TrBuffRead<T>,
    C: TrCancellationToken + Clone,
{
    let mut c = 0usize;
    loop {
        if c >= target.len() {
            return SomeOf::new_left(c);
        }
        let dest = &mut target[c..];
        let demand = Demand::less_than(dest.len());
        let result = buff_r
            .read_async(&demand)
            .may_cancel_with(cancel)
            .await
            .map_left(|mut s| buff_segm_ref_read(&mut s, dest));
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
