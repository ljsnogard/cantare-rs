use core::{marker::PhantomData, mem};

use abs_cancel::{TrCancellationToken, TrMayCancel};
use gen_mcf_macro::gen_may_cancel_future;

use crate::{
    Demand, TrBuffRead, TrBuffWrite,
    buffer::{TrBuffSegmMut, TrBuffSegmRef, TrBuffSegmView},
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
    pub const fn new(buff_write: &'a mut W, buff_read: &'a mut R) -> Self {
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
        if buff_w.is_blocked_closing() {
            return PipeJoinIoResult::TxBlocked(c);
        }
        if buff_r.is_drained_closing() {
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
                    return PipeJoinIoResult::TxErr {
                        count: c,
                        err: tx_err,
                    };
                }
            }
        }
        if let Option::Some(rx_err) = r_res.pick_right() {
            return PipeJoinIoResult::RxErr {
                count: c,
                err: rx_err,
            };
        }
    }
}

#[cfg(test)]
mod tests_ {
    use core::{
        error::Error,
        fmt,
        future::Future,
        mem::MaybeUninit,
        pin::Pin,
        task::{Context, Poll, Waker},
    };
    use std::{vec, vec::Vec};

    use abs_cancel::{TrCancellationToken, TrMayCancel};
    use anylr::SomeOf;

    use super::*;
    use crate::buffer::{SegmMut, SegmReclaim, SegmRef};

    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // Test doubles: a read buffer and a write buffer built directly on
    // `SegmRef` / `SegmMut` with `SegmReclaim`, so the pipe exercises the real
    // segment machinery end to end.
    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum TestErr {
        Blocked,
    }

    impl fmt::Display for TestErr {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                TestErr::Blocked => write!(f, "blocked"),
            }
        }
    }

    impl Error for TestErr {}

    /// An immediately-ready future carrying a `SomeOf` result. It ignores the
    /// cancellation token (the operations complete synchronously), which keeps
    /// the pipe loop deterministic.
    ///
    /// Note: `ReadySegm` intentionally does not name a lifetime; the borrow is
    /// carried by the `S`/`E` type parameters. The `TrMayCancel<'f>` impl below
    /// ties the trait lifetime to them via `S: 'f, E: 'f`, and `IntoFuture`
    /// comes from the standard blanket impl for `Future`.
    struct ReadySegm<S, E>(Option<SomeOf<S, E>>);

    impl<S, E> ReadySegm<S, E> {
        fn new(value: SomeOf<S, E>) -> Self {
            ReadySegm(Option::Some(value))
        }
    }

    impl<S, E> Future for ReadySegm<S, E> {
        type Output = SomeOf<S, E>;

        fn poll(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Self::Output> {
            let this = unsafe { self.get_unchecked_mut() };
            Poll::Ready(
                this.0.take().expect("a ready future must be polled once"),
            )
        }
    }

    impl<'f, S: 'f, E: 'f> TrMayCancel<'f> for ReadySegm<S, E> {
        type MayCancelFuture<'g, C>
            = ReadySegm<S, E>
        where
            Self: 'g,
            C: TrCancellationToken + Clone,
            C: 'f,
            C: 'g,
            'g: 'f;
        type MayCancelOutput = SomeOf<S, E>;

        fn may_cancel_with<'g, C>(
            self,
            _cancel: &'g mut C,
        ) -> Self::MayCancelFuture<'g, C>
        where
            Self: 'g,
            'g: 'f,
            C: TrCancellationToken + Clone,
        {
            self
        }
    }

    /// The read (rx) half: a `Vec` of unconsumed data plus a consumption
    /// counter advanced by the `SegmReclaim` of the borrowed segments.
    struct TestRx<T> {
        data: Vec<T>,
        pos: usize,
        chunk: usize,
        closed: bool,
    }

    impl<T> TestRx<T> {
        fn new(data: Vec<T>, closed: bool) -> Self {
            TestRx {
                data,
                pos: 0,
                chunk: 0,
                closed,
            }
        }

        fn new_chunked(data: Vec<T>, closed: bool, chunk: usize) -> Self {
            TestRx {
                data,
                pos: 0,
                chunk,
                closed,
            }
        }
    }

    impl<T> TrBuffRead<T> for TestRx<T> {
        type SegmRef<'f>
            = SegmRef<'f, T, SegmReclaim<'f>>
        where
            Self: 'f;
        type Err = TestErr;

        fn is_drained_closing(&self) -> bool {
            self.closed && self.pos == self.data.len()
        }

        fn read_async<'f>(
            &'f mut self,
            demand: &Demand<usize>,
        ) -> impl TrMayCancel<
            'f,
            MayCancelOutput = SomeOf<Self::SegmRef<'f>, Self::Err>,
        > {
            let mut take = demand.max().copied().unwrap_or(usize::MAX);
            if self.chunk > 0 {
                take = core::cmp::min(take, self.chunk);
            }
            take = core::cmp::min(take, self.data.len() - self.pos);
            let buffer = &mut self.data[self.pos..self.pos + take];
            let reclaim = SegmReclaim::new(Pin::new(&mut self.pos));
            let segm = SegmRef::new(buffer, reclaim);
            ReadySegm::new(SomeOf::new_left(segm))
        }
    }

    /// The write (tx) half: a fixed `MaybeUninit` storage; the borrowed
    /// segments advance `pos` via `SegmReclaim` as data is written into them.
    struct TestTx<T> {
        buff: Vec<MaybeUninit<T>>,
        pos: usize,
    }

    impl<T> TestTx<T> {
        fn with_capacity(cap: usize) -> Self {
            let mut buff = Vec::with_capacity(cap);
            buff.resize_with(cap, MaybeUninit::uninit);
            TestTx { buff, pos: 0 }
        }

        /// The items actually written so far, in order.
        fn collected(&self) -> Vec<T>
        where
            T: Copy,
        {
            self.buff[..self.pos]
                .iter()
                .map(|m| unsafe { m.assume_init_read() })
                .collect()
        }
    }

    impl<T> TrBuffWrite<T> for TestTx<T> {
        type SegmMut<'f>
            = SegmMut<'f, T, SegmReclaim<'f>>
        where
            Self: 'f;
        type Err = TestErr;

        fn is_blocked_closing(&self) -> bool {
            self.pos == self.buff.len()
        }

        fn write_async<'f>(
            &'f mut self,
            demand: &Demand<usize>,
        ) -> impl TrMayCancel<
            'f,
            MayCancelOutput = SomeOf<Self::SegmMut<'f>, Self::Err>,
        > {
            let free = self.buff.len() - self.pos;
            if free == 0 {
                return ReadySegm::new(SomeOf::new_right(TestErr::Blocked));
            }
            let take = core::cmp::min(
                demand.max().copied().unwrap_or(usize::MAX),
                free,
            );
            let segm = SegmMut::new(
                &mut self.buff[self.pos..self.pos + take],
                SegmReclaim::new(Pin::new(&mut self.pos)),
            );
            ReadySegm::new(SomeOf::new_left(segm))
        }
    }

    /// Poll a future to completion without an executor; all the futures used
    /// here are ready on their first poll.
    fn block_on<F: Future>(fut: F) -> F::Output {
        let mut fut = core::pin::pin!(fut);
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(v) => v,
            Poll::Pending => {
                panic!("the pipe future must complete on the first poll")
            }
        }
    }

    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // PipeJoin behavior
    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    /// The happy path: everything readable is moved into the writer, in order,
    /// and the pipe reports `RxDrained` with the exact transferred count.
    #[test]
    fn pipe_transfers_all_data_and_reports_drained() {
        const TOTAL: usize = 100;
        let expected: Vec<u8> = (0..TOTAL).map(|i| (i % 256) as u8).collect();
        let mut rx = TestRx::new(expected.clone(), true);
        let mut tx = TestTx::with_capacity(TOTAL + 32);

        let result = block_on(async {
            let mut pipe = PipeJoin::new(&mut tx, &mut rx);
            pipe.pipe_async().await
        });

        assert!(matches!(result, PipeJoinIoResult::RxDrained(c) if c == TOTAL));
        assert_eq!(rx.pos, TOTAL, "the reader must consume everything");
        assert_eq!(
            tx.collected(),
            expected,
            "the writer must receive everything in order"
        );
    }

    /// The reader yields its data in chunks: each `read_async` must hand out
    /// exactly the next chunk, and the pipe must drain all of them.
    #[test]
    fn pipe_reads_in_chunks_next_chunk_is_next_content() {
        const TOTAL: usize = 100;
        const CHUNK: usize = 30;
        let expected: Vec<u8> = (0..TOTAL).map(|i| (i % 256) as u8).collect();
        let mut rx = TestRx::new_chunked(expected.clone(), true, CHUNK);
        let mut tx = TestTx::with_capacity(TOTAL + 32);

        let result = block_on(async {
            let mut pipe = PipeJoin::new(&mut tx, &mut rx);
            pipe.pipe_async().await
        });

        assert!(matches!(result, PipeJoinIoResult::RxDrained(c) if c == TOTAL));
        assert_eq!(tx.collected(), expected);
    }

    /// A mid-transfer blockage: the writer accepts one piece, then reports
    /// `Blocked`. The pipe must report `TxErr` with exactly that piece size,
    /// leave the reader right after the transferred data, and allow a retry on
    /// a fresh writer to transfer the rest — no duplication, no loss.
    #[test]
    fn pipe_partial_transfer_then_retry_no_dup_no_loss() {
        const TOTAL: usize = 100;
        const TX_CAP: usize = 16;
        let expected: Vec<u8> = (0..TOTAL).map(|i| (i % 256) as u8).collect();

        let mut rx = TestRx::new(expected.clone(), true);
        let mut tx = TestTx::with_capacity(TX_CAP);

        let result = block_on(async {
            let mut pipe = PipeJoin::new(&mut tx, &mut rx);
            pipe.pipe_async().await
        });
        assert!(
            matches!(result, PipeJoinIoResult::TxErr { count, err: TestErr::Blocked } if count == TX_CAP),
            "exactly one write piece must be transferred"
        );
        // The reader stopped right after the transferred piece...
        assert_eq!(rx.pos, TX_CAP);
        // ...and the writer holds exactly the first piece.
        assert_eq!(tx.collected(), expected[..TX_CAP]);

        // Retry with a fresh, big-enough writer: the rest arrives, exactly once.
        let mut tx2 = TestTx::with_capacity(TOTAL + 32);
        let result2 = block_on(async {
            let mut pipe = PipeJoin::new(&mut tx2, &mut rx);
            pipe.pipe_async().await
        });
        assert!(
            matches!(result2, PipeJoinIoResult::RxDrained(c) if c == TOTAL - TX_CAP)
        );
        assert_eq!(tx2.collected(), expected[TX_CAP..]);
    }

    /// The writer is already full before the pipe starts: report `TxBlocked`
    /// without consuming anything.
    #[test]
    fn pipe_blocked_tx_reports_blocked_without_consuming() {
        let mut rx = TestRx::new(vec![1u8, 2, 3], true);
        let mut tx = TestTx::with_capacity(0);

        let result = block_on(async {
            let mut pipe = PipeJoin::new(&mut tx, &mut rx);
            pipe.pipe_async().await
        });

        assert!(matches!(result, PipeJoinIoResult::TxBlocked(0)));
        assert_eq!(
            rx.pos, 0,
            "nothing must be consumed when the writer is blocked"
        );
    }

    /// The reader is already drained before the pipe starts: report
    /// `RxDrained(0)`.
    #[test]
    fn pipe_drained_rx_reports_drained() {
        let mut rx = TestRx::new(Vec::<u8>::new(), true);
        let mut tx = TestTx::with_capacity(8);

        let result = block_on(async {
            let mut pipe = PipeJoin::new(&mut tx, &mut rx);
            pipe.pipe_async().await
        });

        assert!(matches!(result, PipeJoinIoResult::RxDrained(0)));
        assert_eq!(
            tx.pos, 0,
            "nothing must be written when the reader is drained"
        );
    }

    /// A zero-sized item type short-circuits the pipe into `NoOps`.
    #[test]
    fn pipe_zst_returns_no_ops() {
        let mut rx = TestRx::new(vec![(); 4], true);
        let mut tx = TestTx::with_capacity(4);

        let result = block_on(async {
            let mut pipe = PipeJoin::new(&mut tx, &mut rx);
            pipe.pipe_async().await
        });

        assert!(matches!(result, PipeJoinIoResult::NoOps));
        assert_eq!(rx.pos, 0);
        assert_eq!(tx.pos, 0);
    }
}
