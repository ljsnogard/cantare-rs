use core::{
    mem::{self, MaybeUninit},
    marker::PhantomData,
    ptr,
};

use abs_cancel::{TrCancellationToken, TrMayCancel};

use gen_mcf_macro::gen_may_cancel_future;

use crate::{TrBuffRead, TrBuffSegmMut, TrBuffSegmView, TrBuffWrite, Demand};

pub enum ChainingIoResult<W, R, T>
where
    W: TrBuffWrite<T>,
    R: TrBuffRead<T>,
{
    BuffWriteErr {
        count: usize,
        err: <W as TrBuffWrite<T>>::Err,
    },
    BuffReadErr {
        count: usize,
        err: <R as TrBuffRead<T>>::Err,
    },
    WriteBlocked(usize),
    ReadDrained(usize),
    NoOps,
}

pub struct Chain<'a, W, R, T = u8>
where
    W: TrBuffWrite<T>,
    R: TrBuffRead<T>,
{
    buff_w_: &'a mut W,
    buff_r_: &'a mut R,
    _use_t_: PhantomData<fn() -> [T]>,
}

impl<'a, W, R, T> Chain<'a, W, R, T>
where
    W: TrBuffWrite<T>,
    R: TrBuffRead<T>,
{
    pub const fn new(
        buff_write: &'a mut W,
        buff_read: &'a mut R,
    ) -> Self {
        Chain {
            buff_w_: buff_write,
            buff_r_: buff_read,
            _use_t_: PhantomData,
        }
    }

    pub fn chain_io_async<'f>(&'f mut self) -> ChainIoAsync<'f, W, R, T> {
        ChainIoAsync(&PhantomData, self.buff_w_, self.buff_r_)
    }
}

#[gen_may_cancel_future(ChainIo)]
async fn chain_io_async_<'f, W, R, T, C>(
    _no_t_: &'f PhantomData<T>,
    buff_w: &'f mut W,
    buff_r: &'f mut R,
    cancel: &'f mut C,
) -> ChainingIoResult<W, R, T>
where
    W: TrBuffWrite<T>,
    R: TrBuffRead<T>,
    C: TrCancellationToken,
{
    if mem::size_of::<T>() == 0 {
        return ChainingIoResult::NoOps;
    }
    let mut c = 0usize;
    loop {
        if buff_r.is_drained() {
            return ChainingIoResult::ReadDrained(c);
        }
        let read_demand = Demand::no_less_than(1usize);
        let mut read_result = buff_r
            .read_async(&read_demand)
            .may_cancel_with(cancel)
            .await;
        let opt_input_segm = read_result.as_mut().pick_left();

        if let Option::Some(input_segm) = opt_input_segm {
            let capacity = input_segm.capacity();
            let mut cc = 0usize;
            loop {
                if cc >= capacity {
                    break;
                }
                if buff_w.is_blocked() {
                    return ChainingIoResult::WriteBlocked(c);
                }
                let write_demand = Demand::less_than(capacity - cc);
                let mut write_result = buff_w
                    .write_async(&write_demand)
                    .may_cancel_with(cancel)
                    .await;

                let opt_output_segm = write_result.as_mut().pick_left();
                if let Option::Some(output_segm) = opt_output_segm {
                    let it_input = input_segm.iter_slices().into_iter();
                    let it_output = output_segm.iter_slices_mut().into_iter();

                    let item_cp = unsafe { copy_data_(it_input, it_output) };
                    cc += item_cp;
                    c += item_cp;
                }
                if let Option::Some(output_err) = write_result.pick_right() {
                    return ChainingIoResult::BuffWriteErr {
                        count: c,
                        err: output_err,
                    };
                }
            }
        }
        if let Option::Some(input_err) = read_result.pick_right() {
            return ChainingIoResult::BuffReadErr {
                count: c,
                err: input_err,
            };
        }
    }
}

/// 将来源迭代器中的 `T` 数据搬运到目标迭代器的 `MaybeUninit<T>` 切片中，
/// 直接进行内存复制（不依赖 `Clone`），返回实际搬运的元素个数。
///
/// # Safety
/// - 来源与目标内存**绝不重叠**（由调用者保证）。
/// - 目标切片总长度 ≤ 来源切片总长度（由调用者保证），否则本函数会 panic。
/// - 目标切片中的 `MaybeUninit<T>` 在复制完成后将被视为已初始化。
unsafe fn copy_data_<'f, T>(
    mut src_iter: impl Iterator<Item = &'f [T]>,
    mut dst_iter: impl Iterator<Item = &'f mut [MaybeUninit<T>]>,
) -> usize
where
    T: 'f,
{
    let mut total_copied = 0;

    // 当前来源切片及其已消费的偏移
    let mut current_src = src_iter.next();
    let mut src_offset = 0;

    // 逐目标切片填充
    while let Some(dst_slice) = dst_iter.next() {
        let dst_len = dst_slice.len();
        let mut dst_offset = 0;

        while dst_offset < dst_len {
            // 若当前来源切片已耗尽，则取下一个
            while current_src.is_none() || src_offset >= current_src.unwrap().len() {
                current_src = src_iter.next();
                src_offset = 0;
                if current_src.is_none() {
                    panic!("Source iterator exhausted before all destination slices are filled");
                }
            }

            let src_slice = current_src.unwrap();
            let src_remaining = src_slice.len() - src_offset;
            let dst_remaining = dst_len - dst_offset;
            let copy_count = src_remaining.min(dst_remaining);

            // 获取源指针和目标指针（目标转为 *mut T，布局与 MaybeUninit<T> 相同）
            let src_ptr = unsafe { src_slice.as_ptr().add(src_offset) };
            let dst_ptr = unsafe { dst_slice.as_mut_ptr().add(dst_offset) as *mut T };

            // 安全地复制非重叠内存
            unsafe {
                ptr::copy_nonoverlapping(src_ptr, dst_ptr, copy_count);
            }

            // 更新偏移与总计数
            src_offset += copy_count;
            dst_offset += copy_count;
            total_copied += copy_count;
        }
    }

    total_copied
}
