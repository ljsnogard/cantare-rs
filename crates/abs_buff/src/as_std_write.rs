extern crate std;

use std::{io, string::ToString};

use abs_cancel::{NonCancellableToken, TrCancellationToken};

use crate::{Demand, TrBuffTryWrite, TrBuffWrite, buffer::TrBuffSegmMut};

/// An adapter that exposes a [`TrBuffTryWrite`] buffer as a non-blocking
/// `std::io::Write`.
///
/// Each `write` call pushes as many bytes as the sink currently accepts: the
/// borrowed segment's buffer *is* the sink's own memory, and the source bytes
/// are cloned straight into it through the segment's `clone_items_from_buff`
/// primitive, which advances the segment's offset — so the sink commits
/// exactly the written amount when the segment drops (the `abs_buff`
/// per-piece reclaim granularity). Nothing is copied through an intermediate
/// buffer.
///
/// The loop stops when `buf` is exhausted, the sink is blocked, or the
/// cancellation token is signalled. Following the std convention, an error
/// reported by `try_write` (e.g. the sink being stuffed) is deferred: if
/// anything was already written it is returned first, and the error is only
/// surfaced by the call that makes no progress.
pub struct AsStdWrite<'a, W, C = NonCancellableToken>
where
    W: TrBuffTryWrite,
    C: TrCancellationToken,
{
    buff_w_: &'a mut W,
    cancel_: &'a mut C,
}

impl<'a, W, C> AsStdWrite<'a, W, C>
where
    W: TrBuffTryWrite,
    C: TrCancellationToken,
{
    pub const fn new(w: &'a mut W, cancel: &'a mut C) -> Self {
        AsStdWrite {
            buff_w_: w,
            cancel_: cancel,
        }
    }

    /// Write as many bytes from `buf` as the sink currently accepts.
    pub fn write(&mut self, buf: &[u8]) -> io::Result<usize>
    where
        <W as TrBuffWrite>::Err: core::error::Error,
    {
        let mut c = 0usize;
        let buf_len = buf.len();
        loop {
            if c >= buf_len
                || self.buff_w_.is_blocked_closing()
                || self.cancel_.is_cancelled()
            {
                return Result::Ok(c);
            }
            let demand = Demand::less_than(buf_len - c);
            let mut w_res = self.buff_w_.try_write(&demand);
            if let Option::Some(segm) = w_res.as_mut().pick_left() {
                // `as_segm_mut` yields the concrete `SegmMut` over the
                // remaining free items (the borrowed segment's buffer *is*
                // the sink's own memory), on which the inherent clone
                // primitive exists.
                let mut child = segm.as_segm_mut();
                let take = core::cmp::min(child.least_count(), buf_len - c);
                // Clone (bitwise, for `u8`) the source bytes straight into
                // the segment and advance the child's offset; the child's
                // drop advances the parent's offset, and the sink commits
                // exactly `take` units when the parent drops.
                let moved = child.clone_items_from_buff(&buf[c..c + take]);
                debug_assert_eq!(moved, take);
                c += moved;
                if moved == 0 {
                    // the segment accepted nothing; no progress possible now
                    return Result::Ok(c);
                }
            }
            if let Option::Some(err) = w_res.pick_right() {
                // The sink reported an error (e.g. stuffed / closed). Per the
                // std convention, defer it: if anything was already written,
                // report that first and let the next call surface the error;
                // only fail outright when nothing was written.
                if c > 0 {
                    return Result::Ok(c);
                }
                let err = io::Error::other(err.to_string());
                return Result::Err(err);
            }
        }
    }
}

impl<'a, W> AsStdWrite<'a, W, NonCancellableToken>
where
    W: TrBuffTryWrite,
{
    pub fn uncancellable(w: &'a mut W) -> Self {
        Self::new(w, NonCancellableToken::shared_mut())
    }
}

impl<'a, W, C> std::io::Write for AsStdWrite<'a, W, C>
where
    W: TrBuffTryWrite,
    C: TrCancellationToken,
{
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        AsStdWrite::write(self, buf)
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        // the written bytes are handed to the sink as soon as the borrowed
        // segment drops; the adapter itself buffers nothing
        Result::Ok(())
    }
}

#[cfg(test)]
mod tests_ {
    use core::{
        fmt,
        future::{Future, IntoFuture},
        mem::MaybeUninit,
        pin::Pin,
        task::{Context, Poll},
    };
    use std::{vec, vec::Vec};

    use abs_cancel::{CancelledToken, TrCancellationToken, TrMayCancel};
    use anylr::SomeOf;

    use super::AsStdWrite;
    use crate::{
        Demand, TrBuffTryWrite, TrBuffWrite,
        buffer::{SegmMut, SegmReclaim},
    };

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum TestErr {
        Stuffed,
        Closed,
    }

    impl fmt::Display for TestErr {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                TestErr::Stuffed => write!(f, "stuffed"),
                TestErr::Closed => write!(f, "closed"),
            }
        }
    }

    impl core::error::Error for TestErr {}

    /// An immediately-ready `TrMayCancel` future carrying a `SomeOf` result,
    /// so the async trait methods of the test doubles complete on the first
    /// poll.
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
        type MayCancelOutput = SomeOf<S, E>;

        fn may_cancel_with<'g, C>(
            self,
            _cancel: &'g mut C,
        ) -> impl IntoFuture<Output = Self::MayCancelOutput>
        where
            Self: 'g,
            'g: 'f,
            C: TrCancellationToken + Clone,
        {
            self
        }
    }

    /// A write sink over a fixed `MaybeUninit<u8>` storage: the borrowed
    /// segments advance `pos` through `SegmReclaim` as the adapter writes
    /// into them, so `pos` is exactly the number of bytes accepted so far.
    struct TestSink {
        buff: Vec<MaybeUninit<u8>>,
        pos: usize,
        /// When set, `try_write` reports `Closed` (only when it is also
        /// full), mirroring the ring-buffer "write while closing still has
        /// space" semantics.
        closed: bool,
    }

    impl TestSink {
        fn with_capacity(cap: usize) -> Self {
            let mut buff = Vec::with_capacity(cap);
            buff.resize_with(cap, MaybeUninit::uninit);
            TestSink {
                buff,
                pos: 0,
                closed: false,
            }
        }

        /// The bytes accepted so far, in order.
        fn written(&self) -> Vec<u8> {
            self.buff[..self.pos]
                .iter()
                .map(|m| unsafe { m.assume_init_read() })
                .collect()
        }

        /// Forget everything accepted so far; the storage is reused from the
        /// start (simulating a downstream consumer draining the sink).
        fn reset(&mut self) {
            self.pos = 0;
        }

        fn set_closed(&mut self) {
            self.closed = true;
        }
    }

    impl TrBuffWrite<u8> for TestSink {
        type SegmMut<'f>
            = SegmMut<'f, u8, SegmReclaim<'f>>
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
            let result = if free == 0 {
                let e = if self.closed {
                    TestErr::Closed
                } else {
                    TestErr::Stuffed
                };
                SomeOf::new_right(e)
            } else {
                let take = core::cmp::min(
                    demand.max().copied().unwrap_or(usize::MAX),
                    free,
                );
                let segm = SegmMut::new(
                    &mut self.buff[self.pos..self.pos + take],
                    SegmReclaim::new(&mut self.pos),
                );
                SomeOf::new_left(segm)
            };
            ReadySegm::new(result)
        }
    }

    impl TrBuffTryWrite<u8> for TestSink {
        fn try_write<'f>(
            &'f mut self,
            demand: &Demand<usize>,
        ) -> SomeOf<Self::SegmMut<'f>, Self::Err> {
            let free = self.buff.len() - self.pos;
            if free == 0 {
                let e = if self.closed {
                    TestErr::Closed
                } else {
                    TestErr::Stuffed
                };
                return SomeOf::new_right(e);
            }
            let take = core::cmp::min(
                demand.max().copied().unwrap_or(usize::MAX),
                free,
            );
            let segm = SegmMut::new(
                &mut self.buff[self.pos..self.pos + take],
                SegmReclaim::new(&mut self.pos),
            );
            SomeOf::new_left(segm)
        }
    }

    /// Writing moves the whole offered slice into the sink, in order, and the
    /// sink commits exactly the written amount.
    #[test]
    fn write_moves_all_bytes_in_order() {
        let data: Vec<u8> = (0..100u8).collect();
        let mut sink = TestSink::with_capacity(100);

        let n = {
            let mut wr = AsStdWrite::uncancellable(&mut sink);
            wr.write(&data).expect("write")
        };
        assert_eq!(n, 100);
        assert_eq!(sink.written(), data);
    }

    /// A small sink takes one borrow per call; once it is full the adapter
    /// reports `Ok(0)` (blocked). After the sink is drained externally, the
    /// next write continues right after the previously written data.
    #[test]
    fn write_partial_and_blocks_when_full() {
        let data: Vec<u8> = (0..100u8).collect();
        let mut sink = TestSink::with_capacity(16);

        // first call: the sink takes at most 16 bytes
        let n = {
            let mut wr = AsStdWrite::uncancellable(&mut sink);
            wr.write(&data).expect("first write")
        };
        assert_eq!(n, 16);
        assert_eq!(sink.written(), data[..16]);

        // the sink is now full: blocked, no progress
        let n = {
            let mut wr = AsStdWrite::uncancellable(&mut sink);
            wr.write(&data[16..]).expect("blocked write returns Ok(0)")
        };
        assert_eq!(n, 0);

        // drain and continue: the next chunk lands right after the previous
        let mut collected = sink.written();
        sink.reset();
        let n = {
            let mut wr = AsStdWrite::uncancellable(&mut sink);
            wr.write(&data[16..]).expect("write after drain")
        };
        assert_eq!(n, 16);
        collected.extend_from_slice(&sink.written());
        assert_eq!(collected, data[..32]);
    }

    /// A closed-but-not-full sink still accepts writes ("write while closing
    /// still has space"), mirroring the ring-buffer semantics.
    #[test]
    fn write_still_accepts_when_closed_but_not_full() {
        let mut sink = TestSink::with_capacity(16);
        sink.set_closed();

        let n = {
            let mut wr = AsStdWrite::uncancellable(&mut sink);
            wr.write(&[1u8, 2, 3]).expect("write while closing")
        };
        assert_eq!(n, 3);
        assert_eq!(sink.written(), vec![1, 2, 3]);
    }

    /// A pre-cancelled token stops the write before consuming anything.
    #[test]
    fn write_respects_cancellation() {
        let mut sink = TestSink::with_capacity(16);

        let n = {
            let mut wr =
                AsStdWrite::new(&mut sink, CancelledToken::shared_mut());
            wr.write(&[1u8, 2, 3])
                .expect("cancelled write returns Ok(0)")
        };
        assert_eq!(n, 0);
        assert_eq!(sink.written().len(), 0);
    }

    /// `flush` is a no-op: written bytes are handed to the sink as soon as
    /// the borrowed segment drops.
    #[test]
    fn flush_is_noop() {
        let mut sink = TestSink::with_capacity(16);

        let n = {
            let mut wr = AsStdWrite::uncancellable(&mut sink);
            let n = wr.write(&[1u8, 2, 3]).expect("write");
            std::io::Write::flush(&mut wr).expect("flush");
            n
        };
        assert_eq!(n, 3);
        assert_eq!(sink.written(), vec![1, 2, 3]);
    }

    /// The `std::io::Write` trait impl drives `write_all` to completion.
    #[test]
    fn write_all_via_std_trait() {
        let data: Vec<u8> = (0..200u8).map(|i| i.wrapping_mul(13)).collect();
        let mut sink = TestSink::with_capacity(256);

        {
            let mut wr = AsStdWrite::uncancellable(&mut sink);
            std::io::Write::write_all(&mut wr, &data).expect("write_all");
        }
        assert_eq!(sink.written(), data);
    }
}
