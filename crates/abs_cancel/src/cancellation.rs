use core::{
    future::{self, IntoFuture},
    ops::Try,
};

/// An instance of [IntoFuture] for an async task that may or may not be
/// cancelled by an optional cancellation token.
///
/// Note: the lifetime here is required by `rustc` when implementing
/// [TrMayCancel] for your type. Along with future release of rustc, the `<'a>`
/// may be removed.
pub trait TrMayCancel<'a>
where
    Self: 'a + IntoFuture,
{
    type MayCancelFuture<'f, C>: IntoFuture<Output = Self::MayCancelOutput>
    where
        Self: 'f,
        C: TrCancellationToken + Clone,
        C: 'a,
        C: 'f,
        'f: 'a;

    type MayCancelOutput;

    fn may_cancel_with<'f, C>(
        self,
        cancel: &'f mut C,
    ) -> Self::MayCancelFuture<'f, C>
    where
        Self: 'f,
        // 当 `MayCancelOutput` 携带生命周期（即返回类型借用了 `Self` 的数据）时，
        // 生成的 future 需要把 cancel token 的借用以 `&'a mut C` 的形式保存，
        // 因此要求 cancel 借用存活期不短于 `'a`。没有这一条，宏生成的
        // `may_cancel_with` 无法用 `&'f mut C` 构造出输出类型引用 `'a` 的 future。
        'f: 'a,
        C: TrCancellationToken + Clone;
}


/// A cancellation token can receive cancellation signal.
///
/// In actual usage, a `Clone` impl is usually needed. See `may_cancel_with`
/// for the reason why.
///
/// So if you are developing an cancellation token, consider adding impl for
/// `Clone`.
pub trait TrCancellationToken
where
    Self: Send + Sync,
{
    type Cancellation: Future;

    /// Tests whether this token has received cancellation signal or not.
    fn is_cancelled(&self) -> bool;

    /// Tests whether this token will receive cancellation signal or not.
    fn can_be_cancelled(&self) -> bool;

    fn try_spawn_child_token(&mut self) -> impl Try<Output: TrCancellationToken>;

    /// Creates a future that will become ready when the cancellation signal is
    /// received by this token.
    fn cancellation(&mut self) -> Self::Cancellation;
}

/// A token that is already cancelled and will never reset.
#[derive(Debug, Default, Clone, Copy)]
pub struct CancelledToken;

impl CancelledToken {
    #[allow(static_mut_refs)]
    pub fn shared_mut() -> &'static mut CancelledToken {
        static mut SHARED: CancelledToken = CancelledToken::new();
        unsafe { &mut SHARED }
    }

    /// Create an instance of `CancelledToken`
    pub const fn new() -> Self {
        CancelledToken
    }

    /// Always true
    pub const fn is_cancelled(&self) -> bool {
        true
    }
    /// Always false
    pub const fn can_be_cancelled(&self) -> bool {
        false
    }

    pub const fn child_token(&self) -> CancelledToken {
        CancelledToken::new()
    }

    /// Always return a ready future.
    pub fn cancellation(&mut self) -> future::Ready<()> {
        future::ready(())
    }
}

impl TrCancellationToken for CancelledToken {
    type Cancellation = future::Ready<()>;

    #[inline]
    fn is_cancelled(&self) -> bool {
        CancelledToken::is_cancelled(self)
    }

    #[inline]
    fn can_be_cancelled(&self) -> bool {
        CancelledToken::can_be_cancelled(self)
    }

    #[inline]
    fn try_spawn_child_token(&mut self) -> impl Try<Output: TrCancellationToken> {
        Option::Some(*self)
    }

    #[inline]
    fn cancellation(&mut self) -> Self::Cancellation {
        CancelledToken::cancellation(self)
    }
}

/// A cancellation token that will never be cancelled, usually used
/// as a dummy for `TrCancellationToken`.
#[derive(Debug, Default, Clone, Copy)]
pub struct NonCancellableToken;

impl NonCancellableToken {
    #[allow(static_mut_refs)]
    pub fn shared_mut() -> &'static mut NonCancellableToken {
        static mut SHARED: NonCancellableToken = NonCancellableToken::new();
        unsafe { &mut SHARED }
    }

    pub const fn new() -> Self {
        NonCancellableToken
    }

    /// Always false
    pub const fn is_cancelled(&self) -> bool {
        false
    }

    /// Always false
    pub const fn can_be_cancelled(&self) -> bool {
        false
    }

    pub const fn child_token(&self) -> NonCancellableToken {
        NonCancellableToken::new()
    }

    /// Always returns a pending future.
    pub fn cancellation(&mut self) -> future::Pending<()> {
        future::pending()
    }
}

impl TrCancellationToken for NonCancellableToken {
    type Cancellation = future::Pending<()>;

    #[inline]
    fn is_cancelled(&self) -> bool {
        NonCancellableToken::is_cancelled(self)
    }

    #[inline]
    fn can_be_cancelled(&self) -> bool {
        NonCancellableToken::can_be_cancelled(self)
    }

    #[inline]
    fn try_spawn_child_token(&mut self) -> impl Try<Output: TrCancellationToken> {
        Option::Some(*self)
    }

    #[inline]
    fn cancellation(&mut self) -> Self::Cancellation {
        NonCancellableToken::cancellation(self)
    }
}

#[cfg(test)]
mod tests_ {
    use crate::cancellation::{CancelledToken, NonCancellableToken};

    fn assure_send<T: Send>(t: T) -> T { t }

    fn assure_sync<T: Sync>(t: T) -> T { t }

    #[test]
    fn non_cancellable_token_shared_mut_should_be_send_and_sync() {
        let tok = NonCancellableToken::new();
        let tok = assure_send(tok);
        let _ = assure_sync(tok);
    }

    #[test]
    fn cancelled_token_shared_mut_should_be_send_and_sync() {
        let tok = CancelledToken::new();
        let tok = assure_send(tok);
        let _ = assure_sync(tok);
    }
}
