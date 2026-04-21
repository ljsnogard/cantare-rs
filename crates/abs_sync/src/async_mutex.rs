use core::ops::Try;

use crate::{
    may_cancel::TrMayCancel,
    sync_guard::TrAcqMutGuard,
};

/// Mutex for asynchronous task pattern.
pub trait TrAsyncMutex {
    type Target: ?Sized;

    fn acquire(&self) -> impl TrAsyncMutexAcquire<'_, Self::Target>;
}

pub trait TrAsyncMutexAcquire<'a, T>
where
    Self: 'a,
    T: 'a + ?Sized,
{
    type Guard<'g>: TrAcqMutGuard<'a, 'g, T> where 'a: 'g;

    fn try_lock<'g>(&'g mut self) -> impl Try<Output = Self::Guard<'g>>
    where
        'a: 'g;

    fn lock_async<'g>(&'g mut self) -> impl TrMayCancel<'g, MayCancelOutput: Try<Output = Self::Guard<'g>>>
    where
        'a: 'g;
}
