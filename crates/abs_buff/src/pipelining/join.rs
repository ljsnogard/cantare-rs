use core::{marker::PhantomData, mem};

use abs_cancel::{TrCancellationToken, TrMayCancel};

use gen_mcf_macro::gen_may_cancel_future;

use crate::{
    buffer::{TrBuffSegmMut, TrBuffSegmRef, TrBuffSegmView},
    Demand, TrBuffRead, TrBuffWrite,
};

pub enum PipeJoinIoResult<W, R, T>
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
pub struct PipeJoin<'a, W, R, T = u8>
where
    W: TrBuffWrite<T>,
    R: TrBuffRead<T>,
{
    buff_w_: &'a mut W,
    buff_r_: &'a mut R,
    _use_t_: PhantomData<fn() -> [T]>,
}

impl<'a, W, R, T> PipeJoin<'a, W, R, T>
where
    W: TrBuffWrite<T>,
    R: TrBuffRead<T>,
{
    pub const fn new(
        buff_write: &'a mut W,
        buff_read: &'a mut R,
    ) -> Self {
        PipeJoin {
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
) -> PipeJoinIoResult<W, R, T>
where
    W: TrBuffWrite<T>,
    R: TrBuffRead<T>,
    C: TrCancellationToken + Clone,
{
    if mem::size_of::<T>() == 0 {
        return PipeJoinIoResult::NoOps;
    }
    let mut c = 0usize;
    let mut tx_cancel = cancel.clone();
    let mut rx_cancel = cancel.clone();
    loop {
        if c == usize::MAX {
            return PipeJoinIoResult::SizeLimit(c);
        }
        if buff_w.is_blocked() {
            return PipeJoinIoResult::TxBlocked(c);
        }
        if buff_r.is_drained() {
            return PipeJoinIoResult::RxDrained(c);
        }
        let r_demand = Demand::less_than(usize::MAX - c);
        let mut r_res = buff_r
            .read_async(&r_demand)
            .may_cancel_with(&mut rx_cancel)
            .await;

        if let Option::Some(rx_segm) = r_res.as_mut().pick_left() {
            loop {
                let rx_buf_capacity = rx_segm.least_count();
                if rx_buf_capacity == 0 {
                    if c == 0usize {
                        unreachable!("read_async returns an empty segment.")
                    } else {
                        break;
                    }
                }
                let w_demand = Demand::less_than(rx_buf_capacity);
                let mut w_res = buff_w
                    .write_async(&w_demand)
                    .may_cancel_with(&mut tx_cancel)
                    .await;

                if let Option::Some(tx_segm) = w_res.as_mut().pick_left() {
                    let mut rx_child = rx_segm.as_segm_ref();
                    let mut tx_child = tx_segm.as_segm_mut();
                    let copied = rx_child.move_items_to_segm(&mut tx_child);
                    c += copied;
                }
                if let Option::Some(tx_err) = w_res.pick_right() {
                    return PipeJoinIoResult::TxErr { count: c, err: tx_err }
                }
            }
        }
        if let Option::Some(rx_err) = r_res.pick_right() {
            return PipeJoinIoResult::RxErr { count: c, err: rx_err }
        }
    }
}
