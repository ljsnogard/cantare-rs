use core::pin::Pin;

use tokio;

use abs_sync::cancellation::{NonCancellableToken, TrCancellationToken};

use gen_mcf_macro::gen_may_cancel_future;

/// # Usage Rules:
/// 0. Must be an `async fn`;
/// 1. At least one lifetime and the last one must be for the cancellation token;
/// 2. The last argument and generic parameter type must be the cancellation token type and constrained with: `TrCancellationToken`;
/// 3. Use a where clause to constrain the cancel token type;
#[gen_may_cancel_future(DoThing)]
async fn do_thing_async<'a, 'b, 'x, 'c, A, B, C>(
    a: &'a mut A,
    b: &'b mut B,
    l: usize,
    x: core::slice::Iter<'x, A>,
    cancel: Pin<&'c mut C>,
) -> usize
where
    'a: 'c,
    'b: 'c,
    'x: 'c,
    A: Send,
    B: Sync,
    C: TrCancellationToken,
{
    let _ = (a, b, l, x, cancel);
    42
}

#[tokio::test]
pub async fn run() {
    let mut a = 1usize;
    let mut b = 2.0f32;
    let l = 3usize;
    let x = [0usize; 1usize].as_ref().iter();
    let _ = do_thing_async(&mut a, &mut b, l, x, NonCancellableToken::shared_pin()).await;
}
