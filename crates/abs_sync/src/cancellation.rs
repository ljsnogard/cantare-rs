use core::{
    future::{self, IntoFuture},
    pin::Pin,
};

/// A cancellation token can receive cancellation signal.
pub trait TrCancellationToken {
    /// Tests whether this token has received cancellation signal or not.
    fn is_cancelled(&self) -> bool;

    /// Tests whether this token will receive cancellation signal or not.
    fn can_be_cancelled(&self) -> bool;

    /// Creates a new token that will receive cancellation signal when this
    /// token receives the signal.
    fn child_token(&self) -> impl TrCancellationToken;

    /// Creates a future that will become ready when the cancellation signal is
    /// received by this token.
    fn cancellation(self: Pin<&mut Self>) -> impl IntoFuture;
}

/// A token that is already cancelled and will never reset.
#[derive(Debug, Default, Clone, Copy)]
pub struct CancelledToken;

impl CancelledToken {
    /// Get a mut reference to the global static instance of `CancelledToken`.
    ///
    /// ## Example
    /// ```
    /// # futures_lite::future::block_on(async {
    /// use abs_sync::{
    ///     cancellation::CancelledToken,
    ///     ok_or::XtOkOr,
    /// };
    ///
    /// let token = CancelledToken::shared_pin();
    /// assert!(token.is_cancelled());
    /// assert!(!token.can_be_cancelled());
    ///
    /// let r = token.cancellation().ok_or(async {42}).await;
    /// assert!(r.is_ok());
    /// # })
    /// ```
    pub fn shared_pin() -> Pin<&'static mut Self> {
        static mut SHARED: CancelledToken = CancelledToken::new();
        unsafe {
            #[allow(static_mut_refs)]
            Pin::new_unchecked(&mut SHARED)
        }
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
    pub fn cancellation(self: Pin<&mut Self>) -> future::Ready<()> {
        future::ready(())
    }
}

impl TrCancellationToken for CancelledToken {
    #[inline]
    fn is_cancelled(&self) -> bool {
        CancelledToken::is_cancelled(self)
    }

    #[inline]
    fn can_be_cancelled(&self) -> bool {
        CancelledToken::can_be_cancelled(self)
    }

    #[inline]
    fn child_token(&self) -> impl TrCancellationToken {
        CancelledToken::child_token(self)
    }

    #[inline]
    fn cancellation(self: Pin<&mut Self>) -> impl IntoFuture {
        CancelledToken::cancellation(self)
    }
}

/// A cancellation token that will never be cancelled, usually used
/// as a dummy for `TrCancellationToken`.
#[derive(Debug, Default, Clone, Copy)]
pub struct NonCancellableToken;

impl NonCancellableToken {
    /// Get a mut reference to the global static instance of `NonCancellableToken`.
    ///
    /// ## Example
    /// ```
    /// # futures_lite::future::block_on(async {
    /// use abs_sync::{
    ///     cancellation::NonCancellableToken,
    ///     ok_or::XtOkOr,
    /// };
    ///
    /// let token = NonCancellableToken::shared_pin();
    /// assert!(!token.is_cancelled());
    /// assert!(!token.can_be_cancelled());
    ///
    /// let r = token.cancellation().ok_or(async {42}).await;
    /// assert!(r.is_err());
    /// # })
    /// ```
    pub fn shared_pin() -> Pin<&'static mut Self> {
        static mut SHARED: NonCancellableToken = NonCancellableToken::new();
        unsafe {
            #[allow(static_mut_refs)]
            Pin::new_unchecked(&mut SHARED)
        }
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
    pub fn cancellation(self: Pin<&mut Self>) -> future::Pending<()> {
        future::pending()
    }
}

impl TrCancellationToken for NonCancellableToken {
    #[inline]
    fn is_cancelled(&self) -> bool {
        NonCancellableToken::is_cancelled(self)
    }

    #[inline]
    fn can_be_cancelled(&self) -> bool {
        NonCancellableToken::can_be_cancelled(self)
    }

    #[inline]
    fn child_token(&self) -> impl TrCancellationToken {
        NonCancellableToken::child_token(self)
    }

    #[inline]
    fn cancellation(self: Pin<&mut Self>) -> impl IntoFuture {
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
        let tok = NonCancellableToken::shared_pin();
        let tok = assure_send(tok);
        let _ = assure_sync(tok);
    }

    #[test]
    fn cancelled_token_shared_mut_should_be_send_and_sync() {
        let tok = CancelledToken::shared_pin();
        let tok = assure_send(tok);
        let _ = assure_sync(tok);
    }
}
