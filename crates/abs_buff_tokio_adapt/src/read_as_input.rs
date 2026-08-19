use std::{mem::MaybeUninit, slice};

use abs_buff::{
    gen_may_cancel_future,
    io::TrInput,
    x_deps::{abs_cancel, anylr},
};
use abs_cancel::TrCancellationToken;
use anylr::SomeOf;

pub struct ReadAsInput<'a, R>(&'a mut R)
where
    R: tokio::io::AsyncRead + Unpin;

impl<'a, R> ReadAsInput<'a, R>
where
    R: tokio::io::AsyncRead + Unpin,
{
    pub const fn new(w: &'a mut R) -> Self {
        ReadAsInput(w)
    }

    pub fn read_async<'f>(
        &'f mut self,
        target: &'f mut [MaybeUninit<u8>],
    ) -> InputReadAsync<'f, R> {
        InputReadAsync(&mut self.0, target)
    }
}

impl<'a, R> TrInput<u8> for ReadAsInput<'a, R>
where
    R: tokio::io::AsyncRead + Unpin,
{
    type ReadAsync<'f> = InputReadAsync<'f, R> where Self: 'f, u8: 'f;

    type Err = std::io::Error;

    #[inline]
    fn read_async<'f>(
        &'f mut self,
        target: &'f mut [MaybeUninit<u8>],
    ) -> Self::ReadAsync<'f> {
        ReadAsInput::read_async(self, target)
    }
}

#[gen_may_cancel_future(InputRead)]
async fn input_read_impl_async_<'f, R, C>(
    input: &'f mut R,
    target: &'f mut [MaybeUninit<u8>],
    _token: &'f mut C,
) -> SomeOf<usize, std::io::Error>
where
    R: tokio::io::AsyncRead + Unpin,
    C: TrCancellationToken + Clone,
{
    let size = target.len();
    let buff = target.as_mut_ptr() as *mut u8;
    let buff = unsafe { slice::from_raw_parts_mut(buff, size) };
    <R as tokio::io::AsyncReadExt>::read(input, buff).await.into()
}
