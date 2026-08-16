use core::{
    cell::UnsafeCell,
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicBool, Ordering},
};

/// A minimal `no_std` spin lock. The protected value is only ever accessed
/// through a [`SpinMutexGuard`], which releases the lock on drop, so a panic
/// while the guard is held cannot leave the lock permanently held.
pub(crate) struct SpinMutex<T: ?Sized> {
    locked: AtomicBool,
    data: UnsafeCell<T>,
}

// SAFETY: exclusive access to `T` is enforced by the lock, so the mutex is
// `Sync` (and `Send`) as long as `T: Send`.
unsafe impl<T: ?Sized + Send> Sync for SpinMutex<T> {}
unsafe impl<T: ?Sized + Send> Send for SpinMutex<T> {}

impl<T> SpinMutex<T> {
    pub(crate) const fn new(value: T) -> Self {
        SpinMutex {
            locked: AtomicBool::new(false),
            data: UnsafeCell::new(value),
        }
    }

    pub(crate) fn lock(&self) -> SpinMutexGuard<'_, T> {
        while self
            .locked
            .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            core::hint::spin_loop();
        }
        SpinMutexGuard { mutex: self }
    }
}

pub(crate) struct SpinMutexGuard<'a, T: ?Sized> {
    mutex: &'a SpinMutex<T>,
}

impl<T: ?Sized> Deref for SpinMutexGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        // SAFETY: the guard holds the lock, so no other thread can access the
        // value while this reference is alive.
        unsafe { &*self.mutex.data.get() }
    }
}

impl<T: ?Sized> DerefMut for SpinMutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        // SAFETY: the guard holds the lock, so no other thread can access the
        // value while this reference is alive.
        unsafe { &mut *self.mutex.data.get() }
    }
}

impl<T: ?Sized> Drop for SpinMutexGuard<'_, T> {
    fn drop(&mut self) {
        self.mutex.locked.store(false, Ordering::Release);
    }
}

#[cfg(test)]
mod tests_ {
    use super::*;

    #[test]
    fn lock_excludes_and_releases() {
        let m = SpinMutex::new(0usize);
        {
            let mut g = m.lock();
            *g += 1;
        }
        let g = m.lock();
        assert_eq!(*g, 1);
    }

    #[test]
    fn mutex_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<SpinMutex<usize>>();
    }
}
