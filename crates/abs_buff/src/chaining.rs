use core::{
    mem::{self, MaybeUninit},
    marker::PhantomData,
    ops::{ControlFlow, Try},
    ptr,
};

use abs_cancel::{TrCancellationToken, TrMayCancel};

use gen_mcf_macro::gen_may_cancel_future;

use crate::{Demand, TrBuffRead, TrBuffSegmMut, TrBuffSegmRef, TrBuffSegmView, TrBuffWrite};

pub enum ChainingIoResult<W, R, T>
where
    W: TrBuffWrite<T>,
    R: TrBuffRead<T>,
{
    TxErr {
        count: usize,
        err: <W as TrBuffWrite<T>>::Err,
    },
    RxErr {
        count: usize,
        err: <R as TrBuffRead<T>>::Err,
    },
    TxBlocked(usize),
    RxDrained(usize),
    SizeLimit(usize),
    NoOps,
}

/// Moves data from R to W.
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
    _no_t_: &'f PhantomData<T>, // This is a work-around for macro gen_may_cancel_future.
    buff_w: &'f mut W,
    buff_r: &'f mut R,
    cancel: &'f mut C,
) -> ChainingIoResult<W, R, T>
where
    W: TrBuffWrite<T>,
    R: TrBuffRead<T>,
    C: TrCancellationToken + Clone,
{
    if mem::size_of::<T>() == 0 {
        return ChainingIoResult::NoOps;
    }
    let mut c = 0usize;
    let mut tx_cancel = cancel.clone();
    let mut rx_cancel = cancel.clone();
    loop {
        if buff_w.is_blocked() {
            return ChainingIoResult::TxBlocked(c);
        }
        if c == usize::MAX {
            return ChainingIoResult::SizeLimit(c);
        }
        if buff_r.is_drained() {
            return ChainingIoResult::RxDrained(c);
        }
        // Read a segment first, then write it out in write-segment-sized
        // pieces. This guarantees every borrowed write segment is *fully*
        // consumed before it drops: with the "borrow N commits N" contract of
        // the segment buffers, a partially filled write segment would commit
        // its whole capacity, leaking stale bytes to the output.
        let r_demand = Demand::less_than(usize::MAX - c);
        let mut r_res = buff_r
            .read_async(&r_demand)
            .may_cancel_with(&mut rx_cancel)
            .await;
        let opt_rx_segm = r_res.as_mut().pick_left();

        if let Option::Some(rx_segm) = opt_rx_segm {
            let rx_buf_capacity = rx_segm.least_count();
            let mut cc = 0usize;
            while cc < rx_buf_capacity {
                let w_demand = Demand::less_than(rx_buf_capacity - cc);
                let mut w_res = buff_w
                    .write_async(&w_demand)
                    .may_cancel_with(&mut tx_cancel)
                    .await;
                let opt_tx_segm = w_res.as_mut().pick_left();
                if let Option::Some(segm_dst) = opt_tx_segm {
                    let w_len = segm_dst.least_count();
                    // The read segment has at least `w_len` unconsumed items
                    // left (its total remaining is `rx_buf_capacity - cc` and
                    // the write borrow is no larger than that), so the child
                    // is exactly `w_len` bytes — the full write borrow.
                    let ControlFlow::Continue(segm_src) = rx_segm
                        .take_segm_ref(&Demand::less_than(w_len))
                        .branch()
                    else {
                        unreachable!("[Chain] the read segment ran out");
                    };
                    let dst_it = segm_dst.iter_slices_mut().into_iter();
                    let src_it = segm_src.iter_slices().into_iter();
                    // SAFETY: the source and destination regions never
                    // overlap (they come from two different buffers).
                    let copied_len = unsafe { copy_data_(src_it, dst_it) };
                    drop(segm_src); // advance the read segment's offset
                    cc += copied_len;
                    c += copied_len;
                }
                if let Option::Some(tx_err) = w_res.pick_right() {
                    return ChainingIoResult::TxErr {
                        count: c,
                        err: tx_err,
                    };
                }
            }
        }
        if let Option::Some(rx_err) = r_res.pick_right() {
            return ChainingIoResult::RxErr {
                count: c,
                err: rx_err,
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
    dst_iter: impl Iterator<Item = &'f mut [MaybeUninit<T>]>,
) -> usize
where
    T: 'f,
{
    let mut total_copied = 0;

    // 当前来源切片及其已消费的偏移
    let mut current_src = src_iter.next();
    let mut src_offset = 0;

    // 逐目标切片填充
    for dst_slice in dst_iter {
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
