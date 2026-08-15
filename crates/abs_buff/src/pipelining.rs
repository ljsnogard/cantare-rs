use core::{
    mem::{self, ManuallyDrop, MaybeUninit},
    marker::PhantomData,
    ops::{ControlFlow, Try},
    ptr,
};

use abs_cancel::{TrCancellationToken, TrMayCancel};

use gen_mcf_macro::gen_may_cancel_future;

use crate::{Demand, TrBuffRead, TrBuffSegmMut, TrBuffSegmRef, TrBuffSegmView, TrBuffWrite};

pub enum PipeIoResult<W, R, T>
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
pub struct Pipe<'a, W, R, T = u8>
where
    W: TrBuffWrite<T>,
    R: TrBuffRead<T>,
{
    buff_w_: &'a mut W,
    buff_r_: &'a mut R,
    _use_t_: PhantomData<fn() -> [T]>,
}

impl<'a, W, R, T> Pipe<'a, W, R, T>
where
    W: TrBuffWrite<T>,
    R: TrBuffRead<T>,
{
    pub const fn new(
        buff_write: &'a mut W,
        buff_read: &'a mut R,
    ) -> Self {
        Pipe {
            buff_w_: buff_write,
            buff_r_: buff_read,
            _use_t_: PhantomData,
        }
    }

    pub fn pipe_async<'f>(&'f mut self) -> PipeIoAsync<'f, W, R, T> {
        PipeIoAsync(&PhantomData, self.buff_w_, self.buff_r_)
    }
}

#[gen_may_cancel_future(PipeIo)]
async fn pipe_async_<'f, W, R, T, C>(
    _no_t_: &'f PhantomData<T>, // This is a work-around for macro gen_may_cancel_future.
    buff_w: &'f mut W,
    buff_r: &'f mut R,
    cancel: &'f mut C,
) -> PipeIoResult<W, R, T>
where
    W: TrBuffWrite<T>,
    R: TrBuffRead<T>,
    C: TrCancellationToken + Clone,
{
    if mem::size_of::<T>() == 0 {
        return PipeIoResult::NoOps;
    }
    let mut c = 0usize;
    let mut tx_cancel = cancel.clone();
    let mut rx_cancel = cancel.clone();
    loop {
        if buff_w.is_blocked() {
            return PipeIoResult::TxBlocked(c);
        }
        if c == usize::MAX {
            return PipeIoResult::SizeLimit(c);
        }
        if buff_r.is_drained() {
            return PipeIoResult::RxDrained(c);
        }
        // Read a segment first, then write it out in write-segment-sized
        // pieces. Every borrowed parent segment is consumed *by value* and
        // held in a `ManuallyDrop`: the write position only advances when the
        // piece's copy fully succeeded (`ManuallyDrop::drop`), and the read
        // segment is only committed once the whole segment has been written
        // out. If the copy panics (or the write side fails mid-segment), the
        // `ManuallyDrop` suppresses the parent drops, so neither position
        // moves — the transfer is atomic per read segment.
        let r_demand = Demand::less_than(usize::MAX - c);
        let r_res = buff_r
            .read_async(&r_demand)
            .may_cancel_with(&mut rx_cancel)
            .await;
        if r_res.contains_right() {
            return PipeIoResult::RxErr {
                count: c,
                err: r_res.pick_right().expect("[Chain] contains_right"),
            };
        }
        let mut rx_segm = ManuallyDrop::new(
            r_res
                .pick_left()
                .expect("[Chain] read_async returned neither side"),
        );
        let rx_buf_capacity = rx_segm.least_count();
        let mut cc = 0usize;
        while cc < rx_buf_capacity {
            let w_demand = Demand::less_than(rx_buf_capacity - cc);
            let w_res = buff_w
                .write_async(&w_demand)
                .may_cancel_with(&mut tx_cancel)
                .await;
            if w_res.contains_right() {
                // The write side failed (closed or cancelled) mid-segment: the
                // read parent is left in `ManuallyDrop`, so the reader
                // position does not move. Note: pieces written before this
                // failure were committed on the write side (they cannot be
                // rolled back), so a retry after a *cancellation* would
                // duplicate them.
                return PipeIoResult::TxErr {
                    count: c,
                    err: w_res.pick_right().expect("[Chain] contains_right"),
                };
            }
            let mut segm_dst = ManuallyDrop::new(
                w_res
                    .pick_left()
                    .expect("[Chain] write_async returned neither side"),
            );
            let w_len = segm_dst.least_count();
            // The read segment has at least `w_len` unconsumed items left
            // (its total remaining is `rx_buf_capacity - cc` and the write
            // borrow is no larger than that), so the child is exactly `w_len`
            // bytes — the full write borrow.
            let ControlFlow::Continue(segm_src) = rx_segm
                .take_segm_ref(&Demand::less_than(w_len))
                .branch()
            else {
                unreachable!("[Chain] the read segment ran out");
            };
            let dst_it = segm_dst.iter_slices_mut().into_iter();
            let src_it = segm_src.iter_slices().into_iter();
            // SAFETY: the source and destination regions never overlap (they
            // come from two different buffers). If this panics (source
            // exhausted before the destination is filled), both parents stay
            // inside their `ManuallyDrop` while unwinding, so neither the
            // read nor the write position changes.
            let copied_len = unsafe { copy_data_(src_it, dst_it) };
            drop(segm_src); // advance the read segment's internal offset
            // Commit this write piece: the whole borrow was copied.
            // SAFETY: the copy succeeded, so the segment is fully initialized.
            unsafe { ManuallyDrop::drop(&mut segm_dst) };
            cc += copied_len;
            c += copied_len;
        }
        // The whole read segment was written out: commit it.
        // SAFETY: the segment was fully consumed by its children.
        unsafe { ManuallyDrop::drop(&mut rx_segm) };
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
