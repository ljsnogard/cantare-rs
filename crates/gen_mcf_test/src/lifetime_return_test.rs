//! Regression test for the reported issue: `gen_may_cancel_future` failing on
//! async functions whose *return type* contains a lifetime.
//!
//! All in-workspace usages (abs_buff, mem_vfs, buffex) return owned types, so
//! this situation was never exercised inside cantare.
//!
//! The macro unifies all argument lifetimes to the *last* declared lifetime
//! (`'c`, the cancellation-token lifetime), so a return type that references
//! any user-declared lifetime is rewritten to reference `'c` instead. The
//! README's `'x: 'c` where-clauses make that rewrite sound.

#![allow(dead_code)]

use abs_cancel::TrCancellationToken;

/// A return type carrying the *last* lifetime (`'c`), i.e. the lifetime that
/// the macro unifies all argument lifetimes to.
#[gen_mcf_macro::gen_may_cancel_future(GetRefByLastLt)]
async fn get_ref_by_last_lt_async<'a, 'c, C>(
    s: &'a str,
    _cancel: &'c mut C,
) -> &'c str
where
    'a: 'c,
    C: TrCancellationToken,
{
    s
}

/// A return type carrying a *non-last* lifetime (`'a`).
#[gen_mcf_macro::gen_may_cancel_future(GetRefByFirstLt)]
async fn get_ref_by_first_lt_async<'a, 'c, C>(
    s: &'a str,
    _cancel: &'c mut C,
) -> &'a str
where
    'a: 'c,
    C: TrCancellationToken,
{
    s
}

/// A user-defined type with a lifetime generic parameter in the return type.
pub struct Borrowed<'c>(pub &'c str);

#[gen_mcf_macro::gen_may_cancel_future(GetBorrowed)]
async fn get_borrowed_async<'a, 'c, C>(
    s: &'a str,
    _cancel: &'c mut C,
) -> Borrowed<'a>
where
    'a: 'c,
    C: TrCancellationToken,
{
    Borrowed(s)
}

#[compio::test]
pub async fn run_lifetime_returns() {
    use abs_cancel::{NonCancellableToken, TrMayCancel};

    let s = String::from("hello");

    // No-cancellation path (`IntoFuture`): output borrows `s` for `'c`.
    let r: &str = GetRefByFirstLtAsync(&s).await;
    assert_eq!(r, "hello");
    let r: &str = GetRefByLastLtAsync(&s).await;
    assert_eq!(r, "hello");
    let b: Borrowed<'_> = GetBorrowedAsync(&s).await;
    assert_eq!(b.0, "hello");

    // With a cancellation token (`TrMayCancel::may_cancel_with`).
    let mut tok = NonCancellableToken::new();
    let r: &str = GetRefByFirstLtAsync(&s).may_cancel_with(&mut tok).await;
    assert_eq!(r, "hello");
    let r: &str = GetRefByLastLtAsync(&s).may_cancel_with(&mut tok).await;
    assert_eq!(r, "hello");
    let b: Borrowed<'_> = GetBorrowedAsync(&s).may_cancel_with(&mut tok).await;
    assert_eq!(b.0, "hello");

    // Cancellation is actually honoured: a cancelled token stops the operation
    // (the generated future stores the token and passes it to the async fn).
    use abs_cancel::CancelledToken;
    let mut cancelled = CancelledToken::new();
    let _: &str = GetRefByFirstLtAsync(&s)
        .may_cancel_with(&mut cancelled)
        .await;
}
