extern crate std;

use std::{io, mem::MaybeUninit, string::ToString};

use abs_cancel::{NonCancellableToken, TrCancellationToken};

use crate::{Demand, TrBuffRead, TrBuffTryRead, buffer::TrBuffSegmRef};

/// An adapter that exposes a [`TrBuffTryRead`] buffer as a non-blocking
/// `std::io::Read`.
///
/// Each `read` call drains as much data as the source currently offers: the
/// borrowed segment's buffer *is* the source's own memory, and the data is
/// moved straight into the caller's `buf` through the segment's move
/// primitive (`SegmRef::move_items_to_buff`), which advances the segment's
/// offset — so the source commits exactly the moved amount when the segment
/// drops (the `abs_buff` per-piece reclaim granularity). Nothing is copied
/// through an intermediate buffer.
///
/// The loop stops when `buf` is full, the source is drained (EOF), or the
/// cancellation token is signalled. Following the std convention, an error
/// reported by `try_read` (e.g. the source being temporarily empty) is
/// deferred: if anything was already read it is returned first, and the error
/// is only surfaced by the call that makes no progress.
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

    /// Read as many bytes as the source currently offers into `buf`.
    pub fn read(&mut self, buf: &mut [u8]) -> io::Result<usize>
    where
        <R as TrBuffRead>::Err: core::error::Error,
    {
        let mut c = 0usize;
        let buf_len = buf.len();
        loop {
            if c >= buf_len
                || self.buff_r_.is_drained_closing()
                || self.cancel_.is_cancelled()
            {
                return Result::Ok(c);
            }
            let demand = Demand::less_than(buf_len - c);
            let mut r_res = self.buff_r_.try_read(&demand);
            if let Option::Some(segm) = r_res.as_mut().pick_left() {
                // `as_segm_ref` yields the concrete `SegmRef` over the
                // remaining items (the borrowed segment's buffer *is* the
                // source's own memory), on which the inherent move primitive
                // exists. The remaining `buf` viewed as `MaybeUninit<u8>`
                // (`&mut [u8]` has the same layout).
                let mut child = segm.as_segm_ref();
                let dst = unsafe {
                    core::slice::from_raw_parts_mut(
                        buf[c..].as_mut_ptr().cast::<MaybeUninit<u8>>(),
                        buf_len - c,
                    )
                };
                // SAFETY: the items being moved are plain `u8` (no drop
                // needs), and `dst` is exclusively borrowed for the whole
                // move. Moving advances the child's offset, the child's drop
                // advances the parent's offset, and the source commits the
                // moved amount when the parent drops.
                let moved = unsafe { child.move_items_to_buff(dst) };
                debug_assert!(moved <= buf_len - c);
                c += moved;
                if moved == 0 {
                    // the segment yielded nothing; no progress possible now
                    return Result::Ok(c);
                }
            }
            if let Option::Some(err) = r_res.pick_right() {
                // The source reported an error (e.g. temporarily drained).
                // Per the std convention, defer it: if anything was already
                // read, report that first and let the next call surface the
                // error; only fail outright when nothing was read.
                if c > 0 {
                    return Result::Ok(c);
                }
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

#[cfg(test)]
mod tests_ {
    use core::{
        fmt,
        future::{Future, IntoFuture},
        pin::Pin,
        task::{Context, Poll},
    };
    use std::{string::ToString, vec, vec::Vec};

    use abs_cancel::{CancelledToken, TrCancellationToken, TrMayCancel};
    use anylr::SomeOf;

    use super::AsStdRead;
    use crate::{
        Demand, TrBuffRead, TrBuffTryRead,
        buffer::{SegmReclaim, SegmRef},
    };

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum TestErr {
        Drained,
        Closed,
    }

    impl fmt::Display for TestErr {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                TestErr::Drained => write!(f, "drained"),
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

    /// A read source over a `Vec<u8>`: the borrowed segments advance `pos`
    /// through `SegmReclaim` as the adapter moves data out of them, so
    /// `pos` is exactly the number of bytes consumed so far.
    struct TestSrc {
        data: Vec<u8>,
        pos: usize,
        closed: bool,
    }

    impl TestSrc {
        fn new(data: Vec<u8>) -> Self {
            TestSrc {
                data,
                pos: 0,
                closed: false,
            }
        }

        fn closed(data: Vec<u8>) -> Self {
            TestSrc {
                data,
                pos: 0,
                closed: true,
            }
        }

        fn consumed(&self) -> usize {
            self.pos
        }
    }

    impl TrBuffRead<u8> for TestSrc {
        type SegmRef<'f>
            = SegmRef<'f, u8, SegmReclaim<'f>>
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
            let take = core::cmp::min(
                demand.max().copied().unwrap_or(usize::MAX),
                self.data.len() - self.pos,
            );
            let result = if take == 0 {
                let e = if self.closed {
                    TestErr::Closed
                } else {
                    TestErr::Drained
                };
                SomeOf::new_right(e)
            } else {
                let segm = SegmRef::new(
                    &mut self.data[self.pos..self.pos + take],
                    SegmReclaim::new(&mut self.pos),
                );
                SomeOf::new_left(segm)
            };
            ReadySegm::new(result)
        }
    }

    impl TrBuffTryRead<u8> for TestSrc {
        fn try_read<'f>(
            &'f mut self,
            demand: &Demand<usize>,
        ) -> SomeOf<Self::SegmRef<'f>, Self::Err> {
            let take = core::cmp::min(
                demand.max().copied().unwrap_or(usize::MAX),
                self.data.len() - self.pos,
            );
            if take == 0 {
                let e = if self.closed {
                    TestErr::Closed
                } else {
                    TestErr::Drained
                };
                return SomeOf::new_right(e);
            }
            let segm = SegmRef::new(
                &mut self.data[self.pos..self.pos + take],
                SegmReclaim::new(&mut self.pos),
            );
            SomeOf::new_left(segm)
        }
    }

    /// Reading moves the whole offered segment into `buf`, in order, and the
    /// source advances by exactly the read amount.
    #[test]
    fn read_moves_all_available_bytes_in_order() {
        let data: Vec<u8> = (0..100u8).collect();
        let mut src = TestSrc::closed(data.clone());

        let (n, buf) = {
            let mut rd = AsStdRead::uncancellable(&mut src);
            let mut buf = vec![0u8; 100];
            let n = rd.read(&mut buf).expect("read");
            (n, buf)
        };
        assert_eq!(n, 100);
        assert_eq!(buf, data);
        assert_eq!(src.consumed(), 100);

        // the source is now drained (closed and empty): EOF
        let mut buf2 = vec![0u8; 8];
        let n = {
            let mut rd = AsStdRead::uncancellable(&mut src);
            rd.read(&mut buf2).expect("read at EOF")
        };
        assert_eq!(n, 0);
    }

    /// A small caller buffer takes one borrow per call; consecutive reads
    /// continue right after the previously consumed data (no duplication).
    #[test]
    fn read_partial_and_next_read_continues() {
        let data: Vec<u8> = (0..100u8).collect();
        let mut src = TestSrc::closed(data.clone());

        let (n1, first, n2, second) = {
            let mut rd = AsStdRead::uncancellable(&mut src);
            let mut first = vec![0u8; 40];
            let n1 = rd.read(&mut first).expect("first read");
            let mut second = vec![0u8; 60];
            let n2 = rd.read(&mut second).expect("second read");
            (n1, first, n2, second)
        };
        assert_eq!(n1, 40);
        assert_eq!(first, data[..40]);
        assert_eq!(n2, 60);
        assert_eq!(second, data[40..]);
        assert_eq!(src.consumed(), 100);
    }

    /// A source that is temporarily empty (but not closed) reports a
    /// `Drained` error, which the adapter surfaces as an `io::Error`.
    #[test]
    fn read_surfaces_temporary_empty_error() {
        let mut src = TestSrc::new(vec![1u8, 2, 3]);
        let mut rd = AsStdRead::uncancellable(&mut src);

        let mut buf = vec![0u8; 8];
        let n = rd.read(&mut buf).expect("read the available bytes");
        assert_eq!(n, 3);
        assert_eq!(&buf[..3], &[1, 2, 3]);

        let err = rd.read(&mut buf).expect_err("temporarily empty must error");
        assert!(err.to_string().contains("drained"));
    }

    /// A pre-cancelled token stops the read before consuming anything.
    #[test]
    fn read_respects_cancellation() {
        let mut src = TestSrc::new(vec![1u8, 2, 3]);

        let n = {
            let mut rd = AsStdRead::new(&mut src, CancelledToken::shared_mut());
            let mut buf = vec![0u8; 8];
            rd.read(&mut buf).expect("cancelled read returns Ok(0)")
        };
        assert_eq!(n, 0);
        assert_eq!(src.consumed(), 0);
    }

    /// The `std::io::Read` trait impl drives `read_to_end` until EOF.
    #[test]
    fn read_to_end_via_std_trait() {
        let data: Vec<u8> = (0..=255u8).map(|i| i.wrapping_mul(7)).collect();
        let mut src = TestSrc::closed(data.clone());

        let mut out = Vec::new();
        {
            let mut rd = AsStdRead::uncancellable(&mut src);
            std::io::Read::read_to_end(&mut rd, &mut out).expect("read_to_end");
        }
        assert_eq!(out, data);
        assert_eq!(src.consumed(), data.len());
    }
}
