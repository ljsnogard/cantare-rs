use core::ops::Try;

use crate::may_break::TrMayBreak;
pub use crate::sync_guard::{TrAcqMutGuard, TrAcqRefGuard};

pub trait TrSyncRwLock {
    type Target: ?Sized;

    fn acquire(&self) -> impl TrSyncRwLockAcquire<'_, Self::Target>;
}

pub trait TrSyncRwLockAcquire<'a, T>
where
    Self: 'a,
    T: 'a + ?Sized,
{
    type ReaderGuard<'g>: TrSyncReaderGuard<'a, 'g, T> where 'a: 'g;

    type WriterGuard<'g>: TrSyncWriterGuard<'a, 'g, T> where 'a: 'g;

    type UpgradableGuard<'g>: TrSyncUpgradableReaderGuard<'a, 'g, T> where 'a: 'g;

    fn try_read<'g>(
        &'g mut self,
    ) -> impl Try<Output = Self::ReaderGuard<'g>>
    where
        'a: 'g;

    fn try_write<'g>(
        &'g mut self,
    ) -> impl Try<Output = Self::WriterGuard<'g>>
    where
        'a: 'g;

    fn try_upgradable_read<'g>(
        &'g mut self,
    ) -> impl Try<Output = Self::UpgradableGuard<'g>>
    where
        'a: 'g;

    fn read<'g>(
        &'g mut self,
    ) -> impl TrMayBreak<MayBreakOutput: Try<Output = Self::ReaderGuard<'g>>>
    where
        'a: 'g;

    fn write<'g>(
        &'g mut self,
    ) -> impl TrMayBreak<MayBreakOutput: Try<Output = Self::WriterGuard<'g>>>
    where
        'a: 'g;

    fn upgradable_read<'g>(
        &'g mut self,
    ) -> impl TrMayBreak<MayBreakOutput: Try<Output = Self::UpgradableGuard<'g>>>
    where
        'a: 'g;
}

pub trait TrSyncReaderGuard<'a, 'g, T>
where
    'a: 'g,
    Self: 'g + Sized + TrAcqRefGuard<'a, 'g, T>,
    T: 'a + ?Sized,
{
    type Acquire: TrSyncRwLockAcquire<'a, T>;
}

pub trait TrSyncUpgradableReaderGuard<'a, 'g, T>
where
    'a: 'g,
    Self: 'g + TrSyncReaderGuard<'a, 'g, T>,
    T: 'a + ?Sized,
{
    fn downgrade(
        self,
    ) -> <Self::Acquire as TrSyncRwLockAcquire<'a, T>>::ReaderGuard<'g>;

    fn try_upgrade(
        self,
    ) -> Result<<Self::Acquire as TrSyncRwLockAcquire<'a, T>>::WriterGuard<'g> , Self>;

    fn upgrade(
        self,
    ) -> impl TrSyncUpgrade<'a, 'g, T, Acquire = Self::Acquire>;
}

pub trait TrSyncWriterGuard<'a, 'g, T>
where
    'a: 'g,
    Self: 'g + TrSyncReaderGuard<'a, 'g, T> + TrAcqMutGuard<'a, 'g, T>,
    T: 'a + ?Sized,
{
    fn downgrade_to_reader(
        self,
    ) -> <Self::Acquire as TrSyncRwLockAcquire<'a, T>>::ReaderGuard<'g>;

    fn downgrade_to_upgradable(
        self,
    ) -> <Self::Acquire as TrSyncRwLockAcquire<'a, T>>::UpgradableGuard<'g>;
}

pub trait TrSyncUpgrade<'a, 'g, T>
where
    'a: 'g,
    T: 'a + ?Sized,
{
    type Acquire: TrSyncRwLockAcquire<'a, T>;

    fn try_upgrade<'u>(
        &'u mut self,
    ) -> impl Try<Output =
            <Self::Acquire as TrSyncRwLockAcquire<'a, T>>::WriterGuard<'u>>
    where
        'g: 'u;

    fn upgrade<'u>(
        &'u mut self,
    ) -> impl TrMayBreak<MayBreakOutput: Try<Output =
        <Self::Acquire as TrSyncRwLockAcquire<'a, T>>::WriterGuard<'u>>>
    where
        'g: 'u;

    fn into_guard(
        self,
    ) -> <Self::Acquire as TrSyncRwLockAcquire<'a, T>>::UpgradableGuard<'g>;
}
