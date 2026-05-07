use core::pin::Pin;

use abs_sync::cancellation::TrCancellationToken;

use gen_mcf_macro::gen_may_cancel_future;

/// # Usage Rules:
/// 0. Must be an `async fn`;
/// 1. At least one lifetime and the last one must be for the cancellation token;
/// 2. The last argument and generic parameter type must be the cancellation token type and constrained with: `TrCancellationToken`;
/// 3. Use a where clause to constrain the cancel token type;
/// 4. If an argument is not `Copy` then it should be borrowed with **NAMED** lifetime;
#[gen_may_cancel_future(DoThing)]
async fn do_thing_async<'a, 'b, 'x, 'y, 'c, A, B, C>(
    a: &'a mut A,
    b: &'b mut B,
    l: usize,
    x: &'x core::slice::Iter<'y, A>,
    cancel: Pin<&'c mut C>,
) -> usize
where
    'a: 'c,
    'b: 'c,
    'x: 'c,
    'y: 'c,
    A: Send,
    B: Sync,
    C: TrCancellationToken,
{
    let _ = (a, b, l, x, cancel);
    42
}

// async fn do_thing_async<'a, 'b, 'x, 'y, 'c, A, B, C>(
//     a: &'a mut A,
//     b: &'b mut B,
//     l: usize,
//     x: &'x core::slice::Iter<'y, A>,
//     cancel: Pin<&'c mut C>,
// ) -> usize
// where
//     'a: 'c,
//     'b: 'c,
//     'x: 'c,
//     'y: 'c,
//     A: Send,
//     B: Sync,
//     C: TrCancellationToken,
// {
//     let _ = (a, b, l, x, cancel);
//     42
// }
// pub struct DoThingAsync<'c, A, B>(
//     &'c mut A,
//     &'c mut B,
//     usize,
//     &'c core::slice::Iter<'c, A>,
// )
// where
//     A: Send,
//     B: Sync;
// pub struct DoThingFuture<'c, A, B, C>
// where
//     A: Send,
//     B: Sync,
//     C: TrCancellationToken,
// {
//     params_: DoThingAsync<'c, A, B>,
//     cancel_: Pin<&'c mut C>,
//     future_: Option<
//         <DoThingFutureState<
//             'c,
//             A,
//             B,
//             C,
//         > as ::core::ops::AsyncFnOnce<()>>::CallOnceFuture,
//     >,
// }
// struct DoThingFutureState<'c, A, B, C>(
//     ::core::pin::Pin<&'c mut DoThingFuture<'c, A, B, C>>,
// )
// where
//     A: Send,
//     B: Sync,
//     C: TrCancellationToken;
// impl<'c, A, B> ::core::future::IntoFuture for DoThingAsync<'c, A, B>
// where
//     A: Send,
//     B: Sync,
// {
//     type IntoFuture = DoThingFuture<
//         'c,
//         A,
//         B,
//         abs_sync::cancellation::NonCancellableToken,
//     >;
//     type Output = usize;
//     fn into_future(self) -> Self::IntoFuture {
//         DoThingFuture {
//             params_: self,
//             cancel_: abs_sync::cancellation::NonCancellableToken::shared_pin(),
//             future_: Option::None,
//         }
//     }
// }
// impl<'c, A, B> abs_sync::may_cancel::TrMayCancel<'c> for DoThingAsync<'c, A, B>
// where
//     A: Send,
//     B: Sync,
// {
//     type MayCancelOutput = usize;
//     fn may_cancel_with<'cancel_, C: abs_sync::cancellation::TrCancellationToken>(
//         self,
//         cancel: ::core::pin::Pin<&'cancel_ mut C>,
//     ) -> impl ::core::future::IntoFuture<Output = Self::MayCancelOutput>
//     where
//         Self: 'cancel_,
//     {
//         DoThingFuture {
//             params_: self,
//             cancel_: cancel,
//             future_: Option::None,
//         }
//     }
// }
// impl<'c, A, B, C> ::core::future::Future for DoThingFuture<'c, A, B, C>
// where
//     A: Send,
//     B: Sync,
//     C: TrCancellationToken,
// {
//     type Output = usize;
//     fn poll(
//         self: ::core::pin::Pin<&mut Self>,
//         cx: &mut ::core::task::Context<'_>,
//     ) -> ::core::task::Poll<Self::Output> {
//         let mut this = unsafe {
//             let p = self.get_unchecked_mut();
//             ::core::ptr::NonNull::new_unchecked(p)
//         };
//         loop {
//             let mut fut_field_ptr = unsafe {
//                 let ptr = &mut this.as_mut().future_;
//                 ::core::ptr::NonNull::new_unchecked(ptr)
//             };
//             let opt_fut = unsafe { fut_field_ptr.as_mut() };
//             if let Option::Some(fut) = opt_fut {
//                 let fut_pin = unsafe { ::core::pin::Pin::new_unchecked(fut) };
//                 break fut_pin.poll(cx);
//             } else {
//                 let state = DoThingFutureState(unsafe {
//                     ::core::pin::Pin::new_unchecked(this.as_mut())
//                 });
//                 let fut = AsyncFnOnce::async_call_once(state, ());
//                 let fut_field_mut = unsafe { fut_field_ptr.as_mut() };
//                 *fut_field_mut = Option::Some(fut);
//             }
//         }
//     }
// }
// impl<'c, A, B, C> ::core::ops::AsyncFnOnce<()> for DoThingFutureState<'c, A, B, C>
// where
//     A: Send,
//     B: Sync,
//     C: TrCancellationToken,
// {
//     type Output = usize;
//     type CallOnceFuture = impl ::core::future::Future<Output = Self::Output>;
//     extern "rust-call" fn async_call_once(self, _: ()) -> Self::CallOnceFuture {
//         let f = unsafe { self.0.get_unchecked_mut() };
//         let p = &mut f.params_;
//         self::do_thing_async(p.0, p.1, p.2, p.3, f.cancel_.as_mut())
//     }
// }
