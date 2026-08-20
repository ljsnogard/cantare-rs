//! A C#-style [`CancellationTokenSource`] / [`CancellationToken`] built on
//! top of [`futures_channel::oneshot`].
//!
//! The model mirrors .NET's `CancellationTokenSource`:
//!
//! - a [`CancellationTokenSource`] owns the cancellation state; [`cancel`]
//!   (`CancellationTokenSource::cancel`) signals it once, and further calls
//!   are no-ops;
//! - [`token`](CancellationTokenSource::token) hands out [`CancellationToken`]s
//!   — cheap (`Arc::clone`) handles that observe the shared state;
//! - a token implements [`TrCancellationToken`], so it works everywhere the
//!   `abs_cancel` trait is expected: [`is_cancelled`](TrCancellationToken::is_cancelled),
//!   [`cancellation`](TrCancellationToken::cancellation) (a
//!   `futures_channel::oneshot`-backed future that resolves when the token is
//!   cancelled), and [`try_spawn_child_token`](TrCancellationToken::try_spawn_child_token)
//!   (a child token that is cancelled whenever its parent is);
//! - [`register`](CancellationToken::register) runs callbacks on cancellation,
//!   returning a [`CancellationTokenRegistration`] that unregisters on drop
//!   (mirroring `CancellationToken.Register` / `CancellationTokenRegistration`).
//!
//! # `no_std`
//!
//! The crate is `#![no_std]` (with `alloc`). The only locking primitive used
//! is a small internal spin lock.
//!
//! # Allocator
//!
//! The shared state is allocated with [`Global`] by default. Enabling the
//! `allocator_api` feature makes the public types generic over a user-supplied
//! allocator: `CancellationTokenSource<A>` / `CancellationToken<A>` with
//! `A: Allocator` (defaulting to `Global`), and adds the
//! [`with_allocator`](CancellationTokenSource::with_allocator) constructor.
//! Without the feature the crate compiles on stable `no_std` and no unstable
//! feature is required. (The internal callback/sender `Vec`s still use
//! `Global`; the allocator parameter governs the shared-state allocation.)
//!
//! # Example
//!
//! ```
//! use cancel_src::CancellationTokenSource;
//!
//! let source = CancellationTokenSource::new();
//! let mut token = source.token(); // cheap `Arc::clone`-style handle
//!
//! // `cancellation()` returns a futures-channel future that resolves with
//! // `Err(futures_channel::oneshot::Canceled)` once the source is cancelled.
//! let _wait = token.cancellation();
//!
//! source.cancel();
//!
//! assert!(token.is_cancelled());
//! ```

#![no_std]
#![cfg_attr(feature = "allocator_api", feature(allocator_api))]

#[cfg(test)]
extern crate std;

extern crate alloc;

mod spin_mutex;

use alloc::{boxed::Box, vec::Vec};
use core::fmt;

use abs_cancel::TrCancellationToken;
use futures_channel::oneshot::{self, Receiver, Sender};

use spin_mutex::SpinMutex;

#[cfg(feature = "allocator_api")]
use alloc::alloc::Global;
#[cfg(feature = "allocator_api")]
use alloc::sync::{Arc, Weak};
#[cfg(feature = "allocator_api")]
use core::alloc::Allocator;

#[cfg(not(feature = "allocator_api"))]
use alloc::sync::{Arc, Weak};

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// Shared state (allocator-agnostic)
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

type Callback = Box<dyn FnMut() + Send + 'static>;

struct State {
    cancelled: bool,
    next_id: u64,
    callbacks: Vec<(u64, Callback)>,
    cancel_senders: Vec<Sender<()>>,
}

impl State {
    fn new() -> Self {
        State {
            cancelled: false,
            next_id: 0,
            callbacks: Vec::new(),
            cancel_senders: Vec::new(),
        }
    }
}

/// The shared, allocator-agnostic cancellation state behind every
/// [`CancellationTokenSource`] and [`CancellationToken`].
struct Inner {
    state: SpinMutex<State>,
}

impl Inner {
    fn new() -> Self {
        Inner {
            state: SpinMutex::new(State::new()),
        }
    }

    fn is_cancelled(&self) -> bool {
        self.state.lock().cancelled
    }

    /// Signals cancellation. Returns `true` if this call performed the
    /// cancellation and `false` if the source was already cancelled.
    fn cancel(&self) -> bool {
        let callbacks = {
            let mut st = self.state.lock();
            if st.cancelled {
                return false;
            }
            st.cancelled = true;
            // Dropping the senders resolves every pending `cancellation()`
            // future with `Err(Canceled)`.
            st.cancel_senders.clear();
            // Run the callbacks outside the lock, so a callback may safely
            // re-enter `cancel` / `register` on this or any other source.
            core::mem::take(&mut st.callbacks)
        };
        for (_, mut callback) in callbacks {
            callback();
        }
        true
    }

    /// Registers a callback. If the source is already cancelled, the callback
    /// runs synchronously and `None` is returned (there is nothing to
    /// unregister). Otherwise `Some(id)` is returned.
    fn register(&self, callback: Callback) -> Option<u64> {
        let mut st = self.state.lock();
        if st.cancelled {
            drop(st);
            let mut callback = callback;
            callback();
            return None;
        }
        let id = st.next_id;
        st.next_id += 1;
        st.callbacks.push((id, callback));
        Some(id)
    }

    fn unregister(&self, id: u64) {
        let mut st = self.state.lock();
        st.callbacks.retain(|(i, _)| *i != id);
    }

    /// Creates a future that resolves as soon as the source is cancelled.
    fn cancel_future(&self) -> Receiver<()> {
        let (tx, rx) = oneshot::channel();
        let mut st = self.state.lock();
        if st.cancelled {
            // Already cancelled: dropping the sender makes the receiver
            // resolve immediately with `Err(Canceled)`.
            drop(st);
            drop(tx);
        } else {
            st.cancel_senders.push(tx);
        }
        rx
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// Public API
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

#[cfg(feature = "allocator_api")]
/// The source of a cancellation token, mirroring C#'s
/// `CancellationTokenSource`. Generic over the allocator used for the shared
/// state; defaults to [`Global`].
pub struct CancellationTokenSource<A: Allocator = Global> {
    inner: Arc<Inner, A>,
}

#[cfg(feature = "allocator_api")]
impl<A: Allocator> CancellationTokenSource<A> {
    /// Creates a new source, allocating its shared state with the given
    /// allocator.
    pub fn with_allocator(alloc: A) -> Self {
        CancellationTokenSource {
            inner: Arc::new_in(Inner::new(), alloc),
        }
    }

    /// Signals cancellation. Returns `true` if this call performed the
    /// cancellation and `false` if the source was already cancelled.
    pub fn cancel(&self) -> bool {
        self.inner.cancel()
    }

    /// C#-style alias of [`TrCancellationToken::is_cancelled`].
    pub fn is_cancellation_requested(&self) -> bool {
        self.inner.is_cancelled()
    }
}

#[cfg(feature = "allocator_api")]
impl<A: Allocator + Clone> CancellationTokenSource<A> {
    /// Hands out a cheap, clonable token observing this source. Requires
    /// `A: Clone` (matching `Arc<T, A>: Clone`).
    pub fn token(&self) -> CancellationToken<A> {
        CancellationToken {
            inner: Arc::clone(&self.inner),
        }
    }
}

#[cfg(feature = "allocator_api")]
impl CancellationTokenSource<Global> {
    /// Creates a new source with the default [`Global`] allocator.
    pub fn new() -> Self {
        Self::with_allocator(Global)
    }
}

#[cfg(feature = "allocator_api")]
impl Default for CancellationTokenSource<Global> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "allocator_api")]
impl<A: Allocator> fmt::Debug for CancellationTokenSource<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CancellationTokenSource")
            .field("cancelled", &self.inner.is_cancelled())
            .finish()
    }
}

#[cfg(not(feature = "allocator_api"))]
/// The source of a cancellation token, mirroring C#'s
/// `CancellationTokenSource`. The shared state is allocated with the
/// [`Global`] allocator; enable the `allocator_api` feature to make the type
/// generic over a user-supplied allocator.
pub struct CancellationTokenSource {
    inner: Arc<Inner>,
}

#[cfg(not(feature = "allocator_api"))]
impl CancellationTokenSource {
    /// Creates a new source.
    pub fn new() -> Self {
        CancellationTokenSource {
            inner: Arc::new(Inner::new()),
        }
    }

    /// Hands out a cheap, clonable token observing this source.
    pub fn token(&self) -> CancellationToken {
        CancellationToken {
            inner: self.inner.clone(),
        }
    }

    /// Signals cancellation. Returns `true` if this call performed the
    /// cancellation and `false` if the source was already cancelled.
    pub fn cancel(&self) -> bool {
        self.inner.cancel()
    }

    /// C#-style alias of [`TrCancellationToken::is_cancelled`].
    pub fn is_cancellation_requested(&self) -> bool {
        self.inner.is_cancelled()
    }
}

#[cfg(not(feature = "allocator_api"))]
impl Default for CancellationTokenSource {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(not(feature = "allocator_api"))]
impl fmt::Debug for CancellationTokenSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CancellationTokenSource")
            .field("cancelled", &self.inner.is_cancelled())
            .finish()
    }
}

#[cfg(feature = "allocator_api")]
/// A cheap (`Arc::clone`) handle observing a [`CancellationTokenSource`].
/// Implements [`TrCancellationToken`].
pub struct CancellationToken<A: Allocator = Global> {
    inner: Arc<Inner, A>,
}

#[cfg(feature = "allocator_api")]
impl<A: Allocator + Clone> Clone for CancellationToken<A> {
    fn clone(&self) -> Self {
        CancellationToken {
            inner: Arc::clone(&self.inner),
        }
    }
}

#[cfg(feature = "allocator_api")]
impl<A: Allocator> CancellationToken<A> {
    /// Whether cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.inner.is_cancelled()
    }

    /// Whether this token can be cancelled (always `true` for tokens backed
    /// by a real source).
    pub fn can_be_cancelled(&self) -> bool {
        true
    }

    /// C#-style alias of [`TrCancellationToken::is_cancelled`].
    pub fn is_cancellation_requested(&self) -> bool {
        self.is_cancelled()
    }

    /// Creates a future that resolves as soon as this token is cancelled.
    /// The future is a `futures_channel::oneshot` receiver and resolves with
    /// `Err(Canceled)`.
    pub fn cancellation(&mut self) -> Receiver<()> {
        self.inner.cancel_future()
    }
}

#[cfg(feature = "allocator_api")]
impl<A: Allocator + Clone + Send + 'static> CancellationToken<A> {
    /// Registers a callback to run when the source is cancelled. If the
    /// source is already cancelled the callback runs synchronously. The
    /// returned [`CancellationTokenRegistration`] unregisters the callback on
    /// drop.
    pub fn register<F>(&self, callback: F) -> CancellationTokenRegistration<A>
    where
        F: FnMut() + Send + 'static,
    {
        let id = self.inner.register(Box::new(callback));
        CancellationTokenRegistration {
            inner: Arc::downgrade(&self.inner),
            id,
        }
    }

    /// Creates a child token that is cancelled whenever this token is
    /// cancelled (and is born cancelled if this token already is). Cancelling
    /// the child does not affect its parent.
    pub fn spawn_child_token(&self) -> CancellationToken<A> {
        let child = Arc::new_in(Inner::new(), Arc::allocator(&self.inner).clone());
        let weak = Arc::downgrade(&child);
        let callback = move || {
            if let Option::Some(child) = weak.upgrade() {
                child.cancel();
            }
        };
        // If this token is already cancelled, `register` runs the callback
        // synchronously, so the child is born cancelled.
        self.inner.register(Box::new(callback));
        CancellationToken { inner: child }
    }
}

#[cfg(feature = "allocator_api")]
impl<A: Allocator> fmt::Debug for CancellationToken<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CancellationToken")
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

#[cfg(feature = "allocator_api")]
impl<A: Allocator + Clone + Send + 'static> TrCancellationToken for CancellationToken<A> {
    #[inline]
    fn is_cancelled(&self) -> bool {
        CancellationToken::is_cancelled(self)
    }

    #[inline]
    fn can_be_cancelled(&self) -> bool {
        CancellationToken::can_be_cancelled(self)
    }

    #[inline]
    #[allow(refining_impl_trait)] // return the concrete `Option` instead of the trait's opaque `Try`
    fn try_spawn_child_token(&mut self) -> Option<CancellationToken<A>> {
        Option::Some(self.spawn_child_token())
    }

    #[inline]
    fn cancellation(&mut self) -> impl IntoFuture {
        CancellationToken::cancellation(self)
    }
}

#[cfg(not(feature = "allocator_api"))]
/// A cheap (`Arc::clone`) handle observing a [`CancellationTokenSource`].
/// Implements [`TrCancellationToken`].
pub struct CancellationToken {
    inner: Arc<Inner>,
}

#[cfg(not(feature = "allocator_api"))]
impl Clone for CancellationToken {
    fn clone(&self) -> Self {
        CancellationToken {
            inner: Arc::clone(&self.inner),
        }
    }
}

#[cfg(not(feature = "allocator_api"))]
impl CancellationToken {
    /// Whether cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.inner.is_cancelled()
    }

    /// Whether this token can be cancelled (always `true` for tokens backed
    /// by a real source).
    pub fn can_be_cancelled(&self) -> bool {
        true
    }

    /// C#-style alias of [`TrCancellationToken::is_cancelled`].
    pub fn is_cancellation_requested(&self) -> bool {
        self.is_cancelled()
    }

    /// Registers a callback to run when the source is cancelled. If the
    /// source is already cancelled the callback runs synchronously. The
    /// returned [`CancellationTokenRegistration`] unregisters the callback on
    /// drop.
    pub fn register<F>(&self, callback: F) -> CancellationTokenRegistration
    where
        F: FnMut() + Send + 'static,
    {
        let id = self.inner.register(Box::new(callback));
        CancellationTokenRegistration {
            inner: Arc::downgrade(&self.inner),
            id,
        }
    }

    /// Creates a future that resolves as soon as this token is cancelled.
    /// The future is a `futures_channel::oneshot` receiver and resolves with
    /// `Err(Canceled)`.
    pub fn cancellation(&mut self) -> Receiver<()> {
        self.inner.cancel_future()
    }

    /// Creates a child token that is cancelled whenever this token is
    /// cancelled (and is born cancelled if this token already is). Cancelling
    /// the child does not affect its parent.
    pub fn spawn_child_token(&self) -> CancellationToken {
        let child = Arc::new(Inner::new());
        let weak = Arc::downgrade(&child);
        let callback = move || {
            if let Option::Some(child) = weak.upgrade() {
                child.cancel();
            }
        };
        // If this token is already cancelled, `register` runs the callback
        // synchronously, so the child is born cancelled.
        self.inner.register(Box::new(callback));
        CancellationToken { inner: child }
    }
}

#[cfg(not(feature = "allocator_api"))]
impl fmt::Debug for CancellationToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CancellationToken")
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

#[cfg(not(feature = "allocator_api"))]
impl TrCancellationToken for CancellationToken {
    type Cancellation = Receiver<()>;

    #[inline]
    fn is_cancelled(&self) -> bool {
        CancellationToken::is_cancelled(self)
    }

    #[inline]
    fn can_be_cancelled(&self) -> bool {
        CancellationToken::can_be_cancelled(self)
    }

    #[inline]
    #[allow(refining_impl_trait)] // return the concrete `Option` instead of the trait's opaque `Try`
    fn try_spawn_child_token(&mut self) -> Option<CancellationToken> {
        Option::Some(self.spawn_child_token())
    }

    #[inline]
    fn cancellation(&mut self) -> Self::Cancellation {
        CancellationToken::cancellation(self)
    }
}

#[cfg(feature = "allocator_api")]
/// The handle returned by [`CancellationToken::register`]; unregisters the
/// callback on drop (mirroring C#'s `CancellationTokenRegistration`). `A`
/// must be `Clone` because the `Drop` impl upgrades a `Weak<Inner, A>`.
pub struct CancellationTokenRegistration<A: Allocator + Clone = Global> {
    inner: Weak<Inner, A>,
    id: Option<u64>,
}

#[cfg(feature = "allocator_api")]
impl<A: Allocator + Clone> CancellationTokenRegistration<A> {
    /// Explicitly unregisters the callback (the `Drop` impl does the same).
    pub fn dispose(self) {}
}

#[cfg(feature = "allocator_api")]
impl<A: Allocator + Clone> Drop for CancellationTokenRegistration<A> {
    fn drop(&mut self) {
        let Option::Some(id) = self.id else {
            return;
        };
        if let Option::Some(inner) = self.inner.upgrade() {
            inner.unregister(id);
        }
    }
}

#[cfg(feature = "allocator_api")]
impl<A: Allocator + Clone> fmt::Debug for CancellationTokenRegistration<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CancellationTokenRegistration")
            .field("id", &self.id)
            .finish()
    }
}

#[cfg(not(feature = "allocator_api"))]
/// The handle returned by [`CancellationToken::register`]; unregisters the
/// callback on drop (mirroring C#'s `CancellationTokenRegistration`).
pub struct CancellationTokenRegistration {
    inner: Weak<Inner>,
    id: Option<u64>,
}

#[cfg(not(feature = "allocator_api"))]
impl CancellationTokenRegistration {
    /// Explicitly unregisters the callback (the `Drop` impl does the same).
    pub fn dispose(mut self) {
        // `Drop` performs the unregistration.
        let _ = &mut self;
    }
}

#[cfg(not(feature = "allocator_api"))]
impl Drop for CancellationTokenRegistration {
    fn drop(&mut self) {
        let Option::Some(id) = self.id else {
            return;
        };
        if let Option::Some(inner) = self.inner.upgrade() {
            inner.unregister(id);
        }
    }
}

#[cfg(not(feature = "allocator_api"))]
impl fmt::Debug for CancellationTokenRegistration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CancellationTokenRegistration")
            .field("id", &self.id)
            .finish()
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// Tests
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

#[cfg(test)]
mod tests_ {
    use super::*;

    use alloc::format;
    use core::task::{Context, Poll, Waker};

    fn assert_send_sync<T: Send + Sync>() {}

    fn poll_once<F: core::future::Future>(fut: core::pin::Pin<&mut F>) -> Poll<F::Output> {
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        fut.poll(&mut cx)
    }

    #[test]
    fn tokens_share_state_and_cancel_is_idempotent() {
        let source = CancellationTokenSource::new();
        let token_a = source.token();
        let token_b = source.token();

        assert!(!token_a.is_cancelled());
        assert!(!token_b.is_cancelled());
        assert!(token_a.can_be_cancelled());

        assert!(source.cancel(), "the first cancel performs the cancellation");
        assert!(!source.cancel(), "a second cancel is a no-op");
        assert!(token_a.is_cancelled());
        assert!(token_b.is_cancelled());
        assert!(source.is_cancellation_requested());
        assert!(token_a.is_cancellation_requested());
    }

    #[test]
    fn token_clone_is_arc_cheap_and_shares_state() {
        let source = CancellationTokenSource::new();
        let token = source.token();
        let strong = Arc::strong_count(&token.inner);
        let _clone = token.clone();
        assert_eq!(Arc::strong_count(&token.inner), strong + 1);
        assert_send_sync::<CancellationToken>();
        assert_send_sync::<CancellationTokenSource>();
        assert_send_sync::<CancellationTokenRegistration>();
    }

    #[test]
    fn cancellation_future_resolves_on_cancel() {
        let source = CancellationTokenSource::new();
        let mut token = source.token();

        let mut future = core::pin::pin!(token.cancellation());
        assert!(matches!(poll_once(future.as_mut()), Poll::Pending));

        source.cancel();

        assert!(matches!(
            poll_once(future.as_mut()),
            Poll::Ready(Err(oneshot::Canceled))
        ));
    }

    #[test]
    fn cancellation_future_after_cancel_resolves_immediately() {
        let source = CancellationTokenSource::new();
        let mut token = source.token();
        source.cancel();

        let mut future = core::pin::pin!(token.cancellation());
        assert!(matches!(
            poll_once(future.as_mut()),
            Poll::Ready(Err(oneshot::Canceled))
        ));
    }

    #[test]
    fn multiple_cancellation_futures_all_resolve() {
        let source = CancellationTokenSource::new();
        let mut token = source.token();

        let mut future_a = core::pin::pin!(token.cancellation());
        let mut future_b = core::pin::pin!(token.cancellation());
        assert!(matches!(poll_once(future_a.as_mut()), Poll::Pending));
        assert!(matches!(poll_once(future_b.as_mut()), Poll::Pending));

        source.cancel();

        assert!(matches!(poll_once(future_a.as_mut()), Poll::Ready(Err(oneshot::Canceled))));
        assert!(matches!(poll_once(future_b.as_mut()), Poll::Ready(Err(oneshot::Canceled))));
    }

    #[test]
    fn child_token_is_cancelled_with_parent() {
        let source = CancellationTokenSource::new();
        let parent = source.token();
        let child = parent.spawn_child_token();
        let grandchild = child.spawn_child_token();

        assert!(!parent.is_cancelled());
        assert!(!child.is_cancelled());
        assert!(!grandchild.is_cancelled());

        source.cancel();

        assert!(parent.is_cancelled());
        assert!(child.is_cancelled(), "the child must follow the parent");
        assert!(grandchild.is_cancelled(), "propagation is transitive");
    }

    #[test]
    fn child_token_born_cancelled_when_parent_already_cancelled() {
        let source = CancellationTokenSource::new();
        let parent = source.token();
        source.cancel();

        let child = parent.spawn_child_token();
        assert!(parent.is_cancelled());
        assert!(child.is_cancelled(), "a child of a cancelled token is born cancelled");
    }

    #[test]
    fn child_token_via_trait_is_cancelled_with_parent() {
        let source = CancellationTokenSource::new();
        let mut parent = source.token();
        let child = parent.try_spawn_child_token().expect("child");
        assert!(!child.is_cancelled());

        source.cancel();

        assert!(parent.is_cancelled());
        assert!(child.is_cancelled());
    }

    #[test]
    fn register_callback_fires_on_cancel() {
        use core::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let source = CancellationTokenSource::new();
        let token = source.token();
        let hits = Arc::new(AtomicUsize::new(0));

        let _registration = token.register({
            let hits = hits.clone();
            move || {
                hits.fetch_add(1, Ordering::Relaxed);
            }
        });

        assert_eq!(hits.load(Ordering::Relaxed), 0);
        source.cancel();
        assert_eq!(hits.load(Ordering::Relaxed), 1);
        source.cancel();
        assert_eq!(hits.load(Ordering::Relaxed), 1, "callbacks run exactly once");
    }

    #[test]
    fn register_after_cancel_runs_immediately() {
        use core::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let source = CancellationTokenSource::new();
        let token = source.token();
        source.cancel();

        let hits = Arc::new(AtomicUsize::new(0));
        let registration = token.register({
            let hits = hits.clone();
            move || {
                hits.fetch_add(1, Ordering::Relaxed);
            }
        });
        assert_eq!(
            hits.load(Ordering::Relaxed),
            1,
            "a callback registered on a cancelled token runs now"
        );
        assert!(registration.id.is_none(), "nothing was registered to unregister");
    }

    #[test]
    fn registration_drop_unregisters() {
        use core::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let source = CancellationTokenSource::new();
        let token = source.token();
        let hits = Arc::new(AtomicUsize::new(0));

        let registration = token.register({
            let hits = hits.clone();
            move || {
                hits.fetch_add(1, Ordering::Relaxed);
            }
        });
        drop(registration);

        source.cancel();
        assert_eq!(hits.load(Ordering::Relaxed), 0, "the unregistered callback must not run");
    }

    #[test]
    fn registration_keeps_working_after_token_dropped() {
        use core::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let source = CancellationTokenSource::new();
        let hits = Arc::new(AtomicUsize::new(0));
        let registration = {
            let token = source.token();
            token.register({
                let hits = hits.clone();
                move || {
                    hits.fetch_add(1, Ordering::Relaxed);
                }
            })
        };
        source.cancel();
        assert_eq!(hits.load(Ordering::Relaxed), 1);
        drop(registration);
    }

    #[test]
    fn satisfies_tr_cancellation_token() {
        fn exercise<T: TrCancellationToken>(token: &mut T) {
            let _ = token.is_cancelled();
            let _ = token.can_be_cancelled();
            // The trait hands back an opaque `impl Try`; creating a child and
            // dropping it right away exercises the child-token path generically.
            let _ = token.try_spawn_child_token();
            let _ = token.cancellation();
        }

        let source = CancellationTokenSource::new();
        let mut token = source.token();
        exercise(&mut token);
        assert!(!token.is_cancelled());
    }

    #[test]
    fn source_default_and_debug() {
        let source = CancellationTokenSource::default();
        let token = source.token();
        let _ = format!("{source:?}");
        let _ = format!("{token:?}");
        let _ = format!("{:?}", token.register(|| {}));
    }

    #[test]
    fn cancel_through_any_clone_is_observed_by_all() {
        let source = CancellationTokenSource::new();
        let token = source.token();
        let mut clone = token.clone();
        let other = source.token();

        let mut future = core::pin::pin!(clone.cancellation());
        assert!(matches!(poll_once(future.as_mut()), Poll::Pending));

        source.cancel();

        assert!(token.is_cancelled());
        assert!(clone.is_cancelled());
        assert!(other.is_cancelled());
        assert!(matches!(poll_once(future.as_mut()), Poll::Ready(Err(oneshot::Canceled))));
    }
}

#[cfg(all(test, feature = "allocator_api"))]
mod allocator_api_tests_ {
    use super::*;

    use core::{
        alloc::{AllocError, Layout},
        ptr::NonNull,
        sync::atomic::{AtomicUsize, Ordering},
    };

    static ALLOCS: AtomicUsize = AtomicUsize::new(0);

    /// An allocator that forwards to [`Global`] while counting allocations.
    #[derive(Clone, Copy)]
    struct Counting;

    unsafe impl Allocator for Counting {
        fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
            Global.allocate(layout)
        }

        unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
            unsafe { Global.deallocate(ptr, layout) }
        }
    }

    #[test]
    fn custom_allocator_allocates_the_shared_state() {
        let before = ALLOCS.load(Ordering::Relaxed);

        let source = CancellationTokenSource::with_allocator(Counting);
        let mut token = source.token();
        let _clone = token.clone();
        let _child = token.spawn_child_token();
        let _future = token.cancellation();

        assert!(
            ALLOCS.load(Ordering::Relaxed) > before,
            "the shared state must be allocated with the custom allocator"
        );
        assert!(!token.is_cancelled());
        source.cancel();
        assert!(token.is_cancelled());
    }

    #[test]
    fn generic_source_defaults_to_global() {
        let source = CancellationTokenSource::<Global>::new();
        let token = source.token();
        let _child = token.spawn_child_token();
        assert!(!token.is_cancelled());
        source.cancel();
        assert!(token.is_cancelled());
    }

    #[test]
    fn child_token_uses_the_parent_allocator() {
        let before = ALLOCS.load(Ordering::Relaxed);
        let source = CancellationTokenSource::with_allocator(Counting);
        let token = source.token();
        let _child = token.spawn_child_token();
        assert!(
            ALLOCS.load(Ordering::Relaxed) >= before + 2,
            "both the parent and the child state must use the custom allocator"
        );
    }
}
