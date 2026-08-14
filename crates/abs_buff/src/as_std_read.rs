extern crate std;

use std::{io, ptr, string::ToString};

use crate::{Demand, TrBuffSegmView, TrBuffRead, TrBuffTryRead};

pub struct AsStdRead<'a, R>(&'a mut R)
where
    R: TrBuffTryRead;

impl<'a, R> AsStdRead<'a, R>
where
    R: TrBuffTryRead,
{
    pub const fn new(r: &'a mut R) -> Self {
        AsStdRead(r)
    }

    pub fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize>
    where
        <R as TrBuffRead>::Err: core::error::Error,
    {
        let mut c = 0usize;
        let buf_len = buf.len();
        loop {
            if c >= buf_len || self.0.is_drained() {
                return Result::Ok(c)
            }
            let demand = Demand::less_than(buf_len - c);
            let r_res = self.0.try_read(&demand);
            if let Option::Some(segm) = r_res.as_ref().pick_left() {
                let mut cc = 0usize;
                for src_slice in segm.iter_slices() {
                    let src_len = src_slice.len();
                    let dst_slice = &mut buf[c..c + cc];

                    assert!(src_len <= dst_slice.len());

                    // 获取源指针和目标指针（目标转为 *mut T，布局与 MaybeUninit<T> 相同）
                    let src_ptr = src_slice.as_ptr() as *const u8;
                    let dst_ptr = dst_slice.as_mut_ptr() as *mut u8;

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

impl<'a, R> std::io::Read for AsStdRead<'a, R>
where
    R: TrBuffTryRead,
{
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        AsStdRead::read(self, buf)
    }
}
