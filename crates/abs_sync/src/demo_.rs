use std::{
    borrow::BorrowMut,
    io::Result,
    ops::{ControlFlow, Deref, DerefMut, Try},
};

use abs_cancel::{NonCancellableToken, TrMayCancel};

use crate::async_lock::*;

#[allow(dead_code)]
async fn generic_rwlock_smoke_<B, L, T>(rwlock: B) -> Result<()>
where
    B: BorrowMut<L>,
    L: TrAsyncRwLock<Target = T>,
{
    let mut acq = rwlock.borrow().acquire();
    let read_async = acq.read_async();
    // let write_async = acq.write_async(); // illegal

    // `TrMayCancel::may_cancel_with` stores the cancel-token borrow for the
    // whole operation (its future keeps `&'a mut C`), so each operation needs
    // its own named token that outlives the guard it feeds, not a temporary.
    let mut token = NonCancellableToken::new();

    // let read_guard = read_async
    //     .may_cancel_with(&mut NonCancellableToken::new())
    //     .await?;
    let ControlFlow::Continue(read_guard) = read_async
        .may_cancel_with(&mut token)
        .await
        .branch()
    else {
        panic!()
    };
    let _ = read_guard.deref();
    // let write_async = acq.write_async(); // illegal
    drop(read_guard);
    let mut token = NonCancellableToken::new();
    let ControlFlow::Continue(upgradable) = acq
        .upgradable_read_async()
        .may_cancel_with(&mut token)
        .await
        .branch()
    else {
        panic!()
    };
    let _ = upgradable.deref();
    let mut upgrade = upgradable.upgrade();
    let mut token = NonCancellableToken::new();
    let ControlFlow::Continue(mut write_guard) = upgrade
        .upgrade_async()
        .may_cancel_with(&mut token)
        .await
        .branch()
    else {
        panic!()
    };
    let _ = write_guard.deref_mut();
    let upgradable = write_guard.downgrade_to_upgradable();
    drop(upgradable);
    Ok(())
}
