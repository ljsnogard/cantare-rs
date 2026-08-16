extern crate std;

use std::{io, ptr, string::ToString};

use abs_cancel::{NonCancellableToken, TrCancellationToken};

use crate::{
    buffer::TrBuffSegmView,
    Demand, TrBuffRead, TrBuffTryRead,
};

pub struct AsStdRead<'a, R, C = NonCancellableToken>
where
    R: TrBuffTryRead,
    C: TrCancellationToken,
{
    buff_r_: &'a mut R,
    cancel_: &'a mut C,
}

impl<'a, R, C> AsStdRead<'a, R, C>
where
    R: TrBuffTryRead,
    C: TrCancellationToken,
{
    pub const fn new(r: &'a mut R, cancel: &'a mut C) -> Self {
        AsStdRead {
            buff_r_: r,
            cancel_: cancel,
        }
    }

    pub fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize>
    where
        <R as TrBuffRead>::Err: core::error::Error,
    {
        let mut c = 0usize;
        let buf_len = buf.len();
        loop {
            if c >= buf_len || self.buff_r_.is_drained() || self.cancel_.is_cancelled() {
                return Result::Ok(c)
            }
            let demand = Demand::less_than(buf_len - c);
            let r_res = self.buff_r_.try_read(&demand);
            if let Option::Some(segm) = r_res.as_ref().pick_left() {
                let mut cc = 0usize;
                for src_slice in segm.iter_slices() {
                    let src_len = src_slice.len();
                    let dst_slice = &mut buf[c + cc..];

                    assert!(src_len <= dst_slice.len());

                    // 获取源指针和目标指针（目标转为 *mut T，布局与 MaybeUninit<T> 相同）
                    let src_ptr = src_slice.as_ptr();
                    let dst_ptr = dst_slice.as_mut_ptr();

                    // 安全地复制非重叠内存
                    unsafe { ptr::copy_nonoverlapping(src_ptr, dst_ptr, src_len); }
                    cc += src_len;
                }
                c += cc;
            }
            if let Option::Some(err) = r_res.pick_right() {
                let err = io::Error::other(err.to_string());
                return Result::Err(err);
            }
        }
    }
}

impl<'a, R> AsStdRead<'a, R, NonCancellableToken>
where
    R: TrBuffTryRead,
{
    pub fn uncancellable(r: &'a mut R) -> Self {
        Self::new(r, NonCancellableToken::shared_mut())
    }
}

impl<'a, R, C> std::io::Read for AsStdRead<'a, R, C>
where
    R: TrBuffTryRead,
    C: TrCancellationToken,
{
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        AsStdRead::read(self, buf)
    }
}
