use core::pin::Pin;

use crate::cancellation::TrCancellationToken;

/// An instance of [IntoFuture] for an async task that may or may not be
/// cancelled by an optional cancellation token.
///
/// Note: the lifetime here is required by `rustc` when implementing
/// [TrMayCancel] for your type. Along with future release of rustc, the `<'a>`
/// may be removed.
pub trait TrMayCancel<'a>
where
    Self: 'a + Sized + IntoFuture
{
    type MayCancelOutput;

    fn may_cancel_with<'f, C: TrCancellationToken>(
        self,
        cancel: Pin<&'f mut C>,
    ) -> impl IntoFuture<Output = Self::MayCancelOutput>
    where
        Self: 'f;
}
