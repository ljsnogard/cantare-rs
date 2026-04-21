use core::ops::Try;

use crate::may_break::TrMayBreak;
pub use crate::sync_guard::{TrAcqMutGuard, TrAcqRefGuard};

pub trait TrSyncMutex {
    type Target: ?Sized;

    fn acquire(&self) -> impl TrSyncMutexAcquire<'_, Self::Target>;
}

pub trait TrSyncMutexAcquire<'a, T>
where
    Self: 'a,
    T: 'a + ?Sized,
{
    type Guard<'g>: TrAcqMutGuard<'a, 'g, T> where 'a: 'g;

    fn try_lock<'g>(
        &'g mut self,
    ) -> impl Try<Output = Self::Guard<'g>>
    where
        'a: 'g;

    fn lock<'g>(
        &'g mut self,
    ) -> impl TrMayBreak<MayBreakOutput: Try<Output = Self::Guard<'g>>>
    where
        'a: 'g;
}
