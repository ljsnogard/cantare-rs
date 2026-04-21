## gen_mcf_macro

Assumed to work with following unstable features:

```rust
// Allow for implementation of the AsyncFn*
#![feature(async_fn_traits)]

// To enable `extern "rust-call" fn` which is used in `impl AsyncFnOnce`
#![feature(unboxed_closures)]

// To enable `type CallOnceFuture = impl ::core::future::Future<Output = Self::Output>;`
#![feature(type_alias_impl_trait)]
#![feature(impl_trait_in_assoc_type)]
```

## Usage

```rust

use abs_sync::cancellation::{NonCancellableToken, TrMayCancel, TrCancellationToken};

/// # Usage Rules:
/// 0. Must be an `async fn`;
/// 1. Use one and only one lifetime and DO NOT name a lifetime with a underscore suffix;
/// 2. The last param must be the cancellation token type and constrained with: `TrCancellationToken`;
/// 3. Use a where clause to constrain the cancel token type;
/// 4. If an argument is not `Copy` then it should be borrowed;
#[gen_may_cancel_future(DoThing)]
async fn do_thing_async<'f, A, B, C>(
    a: &'f mut A,
    b: &'f mut B,
    l: usize,               // copy, ok
    x: &'f Result<A, B>,    // see rule 4
    cancel: Pin<&'f mut C>, // see rule 2
) -> R
where
    A: Send,
    B: Sync,
    C: TrCancellationToken,
{
    // ...
}

```

Which expands to codes:

```rust

async fn do_thing_async<'f, A, B, C>(
    a: &'f mut A,
    b: &'f mut B,
    l: usize,
    x: &'f Result<A, B>,
    cancel: Pin<&'f mut C>,
) -> usize
where
    A: Send,
    B: Sync,
    C: TrCancellationToken,
{
    42
}
pub struct DoThingAsync<'f, A, B>(
    &'f mut A,
    &'f mut B,
    usize,
    &'f Result<A, B>,
)
where
    A: Send,
    B: Sync;
pub struct DoThingFuture<'f, A, B, C>
where
    A: Send,
    B: Sync,
    C: TrCancellationToken,
{
    params_: DoThingAsync<'f, A, B>,
    cancel_: Pin<&'f mut C>,
    future_: Option<
        <DoThingFutureState<
            'f,
            A,
            B,
            C,
        > as ::core::ops::AsyncFnOnce<()>>::CallOnceFuture,
    >,
}
struct DoThingFutureState<'f, A, B, C>(
    ::core::pin::Pin<&'f mut DoThingFuture<'f, A, B, C>>,
)
where
    A: Send,
    B: Sync,
    C: TrCancellationToken;
impl<'f, A, B> ::core::future::IntoFuture for DoThingAsync<'f, A, B>
where
    A: Send,
    B: Sync,
{
    type IntoFuture = DoThingFuture<
        'f,
        A,
        B,
        ::abs_sync::cancellation::NonCancellableToken,
    >;
    type Output = usize;
    fn into_future(self) -> Self::IntoFuture {
        DoThingFuture {
            params_: self,
            cancel_: ::abs_sync::cancellation::NonCancellableToken::pinned(),
            future_: Option::None,
        }
    }
}
impl<'f, A, B> ::abs_sync::cancellation::TrMayCancel<'f>
for DoThingAsync<'f, A, B>
where
    A: Send,
    B: Sync,
{
    type MayCancelOutput = usize;
    fn may_cancel_with<
        'cancel_,
        C: ::abs_sync::cancellation::TrCancellationToken,
    >(
        self,
        cancel: ::core::pin::Pin<&'cancel_ mut C>,
    ) -> impl ::core::future::Future<Output = Self::MayCancelOutput>
    where
        Self: 'cancel_,
    {
        DoThingFuture {
            params_: self,
            cancel_: cancel,
            future_: Option::None,
        }
    }
}
impl<'f, A, B, C> ::core::future::Future for DoThingFuture<'f, A, B, C>
where
    A: Send,
    B: Sync,
    C: TrCancellationToken,
{
    type Output = usize;
    fn poll(
        self: ::core::pin::Pin<&mut Self>,
        cx: &mut ::core::task::Context<'_>,
    ) -> ::core::task::Poll<Self::Output> {
        let mut this = unsafe {
            let p = self.get_unchecked_mut();
            ::core::ptr::NonNull::new_unchecked(p)
        };
        loop {
            let mut fut_field_ptr = unsafe {
                let ptr = &mut this.as_mut().future_;
                ::core::ptr::NonNull::new_unchecked(ptr)
            };
            let opt_fut = unsafe { fut_field_ptr.as_mut() };
            if let Option::Some(fut) = opt_fut {
                let fut_pin = unsafe { ::core::pin::Pin::new_unchecked(fut) };
                break fut_pin.poll(cx);
            } else {
                let state = DoThingFutureState(unsafe {
                    ::core::pin::Pin::new_unchecked(this.as_mut())
                });
                let fut = AsyncFnOnce::async_call_once(state, ());
                let fut_field_mut = unsafe { fut_field_ptr.as_mut() };
                *fut_field_mut = Option::Some(fut);
            }
        }
    }
}
impl<A, B, C> ::core::ops::AsyncFnOnce<()> for DoThingFutureState<'_, A, B, C>
where
    A: Send,
    B: Sync,
    C: TrCancellationToken,
{
    type Output = usize;
    type CallOnceFuture = impl ::core::future::Future<Output = Self::Output>;
    extern "rust-call" fn async_call_once(self, _: ()) -> Self::CallOnceFuture {
        let f = unsafe { self.0.get_unchecked_mut() };
        let p = &mut f.params_;
        self::do_thing_async(p.0, p.1, p.2, p.3, f.cancel_.as_mut())
    }
}

```