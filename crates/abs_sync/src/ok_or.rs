use core::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use pin_project::pin_project;

pub trait XtOkOr<E>
where
    Self: Sized + Future,
    E: Future,
{
    fn ok_or(self, other: E) -> OkOr<Self, E>;
}

#[derive(Debug)]
#[pin_project]
pub struct OkOr<F, G>
where
    F: Future,
    G: Future,
{
    #[pin]succ_: F,
    #[pin]fail_: G,
    done_: bool,
}

impl<F, G> OkOr<F, G>
where
    F: Future,
    G: Future,
{
    pub const fn new(succeed: F, otherwise: G) -> Self {
        OkOr {
            succ_: succeed,
            fail_: otherwise,
            done_: false,
        }
    }
}

impl<F, G> Future for OkOr<F, G>
where
    F: Future,
    G: Future,
{
    type Output = Result<F::Output, G::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        if *this.done_ {
            panic!("polled after completion");
        };
        if let Poll::Ready(x) = this.succ_.as_mut().poll(cx) {
            *this.done_ = true;
            return Poll::Ready(Result::Ok(x));
        }
        if let Poll::Ready(e) = this.fail_.as_mut().poll(cx) {
            *this.done_ = true;
            return Poll::Ready(Result::Err(e));
        }
        Poll::Pending
    }
}

impl<F, G> XtOkOr<G> for F
where
    F: Future,
    G: Future,
{
    fn ok_or(self, other: G) -> OkOr<Self, G> {
        OkOr::new(self, other)
    }
}

#[cfg(test)]
mod tests_ {
    use std::{sync::atomic::*, time::Duration};

    use super::XtOkOr;

    #[tokio::test]
    async fn or_else_should_poll_both_future() {
        let a1 = AtomicUsize::new(1);
        let a2 = AtomicUsize::new(2);

        async fn fetch_add_async(a: &AtomicUsize) -> usize {
            let u = a.fetch_add(1, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_micros(100)).await;
            if u.is_multiple_of(2) {
                tokio::time::sleep(Duration::from_micros(100)).await;
            }
            u
        }

        let f1 = fetch_add_async(&a1);
        let f2 = fetch_add_async(&a2);

        let x = f1.ok_or(f2).await;
        assert!(matches!(x, Result::Ok(1)));
        assert_eq!(a1.load(Ordering::SeqCst), 2);
        assert_eq!(a2.load(Ordering::SeqCst), 3);
    }
}
