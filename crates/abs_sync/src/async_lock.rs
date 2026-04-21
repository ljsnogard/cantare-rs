use core::ops::Try;

use crate::{
    may_cancel::TrMayCancel,
    sync_guard::{TrAcqMutGuard, TrAcqRefGuard},
};

/// Reader-Writer lock for asynchronous task pattern.
pub trait TrAsyncRwLock {
    type Target: ?Sized;

    fn acquire(&self) -> impl TrAsyncRwLockAcquire<'_, Self::Target>;
}

pub trait TrAsyncRwLockAcquire<'a, T>
where
    Self: 'a,
    T: 'a + ?Sized,
{
    type ReaderGuard<'g>: TrReaderGuard<'a, 'g, T> where 'a: 'g;

    type WriterGuard<'g>: TrWriterGuard<'a, 'g, T> where 'a: 'g;

    type UpgradableGuard<'g>: TrUpgradableReaderGuard<'a, 'g, T> where 'a: 'g;

    fn try_read<'g>(&'g mut self) -> impl Try<Output = Self::ReaderGuard<'g>>
    where
        'a: 'g;

    fn try_write<'g>(&'g mut self) -> impl Try<Output = Self::WriterGuard<'g>>
    where
        'a: 'g;

    fn try_upgradable_read<'g>(
        &'g mut self,
    ) -> impl Try<Output = Self::UpgradableGuard<'g>>
    where
        'a: 'g;

    fn read_async<'g>(
        &'g mut self,
    ) -> impl TrMayCancel<'g,
        MayCancelOutput: Try<Output = Self::ReaderGuard<'g>>>
    where
        'a: 'g;

    fn write_async<'g>(
        &'g mut self,
    ) -> impl TrMayCancel<'g,
        MayCancelOutput: Try<Output = Self::WriterGuard<'g>>>
    where
        'a: 'g;

    fn upgradable_read_async<'g>(
        &'g mut self,
    ) -> impl TrMayCancel<'g,
        MayCancelOutput: Try<Output = Self::UpgradableGuard<'g>>>
    where
        'a: 'g;
}

pub trait TrReaderGuard<'a, 'g, T>
where
    'a: 'g,
    Self: 'g + Sized + TrAcqRefGuard<'a, 'g, T>,
    T: 'a + ?Sized,
{
    type Acquire: TrAsyncRwLockAcquire<'a, T>;
}

pub trait TrUpgradableReaderGuard<'a, 'g, T>
where
    'a: 'g,
    Self: 'g + TrReaderGuard<'a, 'g, T>,
    T: 'a + ?Sized,
{
    fn downgrade(self) -> <Self::Acquire as TrAsyncRwLockAcquire<'a, T>>::ReaderGuard<'g>;

    fn upgrade(self) -> impl TrUpgrade<'a, 'g, T, Acquire = Self::Acquire>;
}

pub trait TrWriterGuard<'a, 'g, T>
where
    'a: 'g,
    Self: 'g + TrReaderGuard<'a, 'g, T> +TrAcqMutGuard<'a, 'g, T>,
    T: 'a + ?Sized,
{
    fn downgrade(self) -> <Self::Acquire as TrAsyncRwLockAcquire<'a, T>>::ReaderGuard<'g>;

    fn downgrade_to_upgradable(
        self,
    ) -> <Self::Acquire as TrAsyncRwLockAcquire<'a, T>>::UpgradableGuard<'g>;
}

pub trait TrUpgrade<'a, 'g, T>
where
    'a: 'g,
    T: 'a + ?Sized,
{
    type Acquire: TrAsyncRwLockAcquire<'a, T>;

    fn try_upgrade<'u>(
        &'u mut self,
    ) -> impl Try<Output = <Self::Acquire as TrAsyncRwLockAcquire<'a, T>>::WriterGuard<'u>>
    where
        'g: 'u;

    fn upgrade_async<'u>(
        &'u mut self,
    ) -> impl TrMayCancel<'u, MayCancelOutput: Try<Output =
        <Self::Acquire as TrAsyncRwLockAcquire<'a, T>>::WriterGuard<'u>>>
    where
        'g: 'u;

    fn into_guard(
        self,
    ) -> <Self::Acquire as TrAsyncRwLockAcquire<'a, T>>::UpgradableGuard<'g>;
}
