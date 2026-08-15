//! Boundary-condition tests for `mutex::preemptive`.
//!
//! These tests target the edge cases of the two mutex signals
//! ([`MsbAsMutexSignal`], [`PtrAsMutexSignal`]) and of the lock state
//! machine, without modifying the implementation.
//!
//! Two of the tests at the bottom are `#[ignore]`d because they demonstrate a
//! livelock in [`Acquire::try_spin_acquire_`] (the code path behind
//! [`MayBreakLock::wait`] / [`MayBreakLock::may_break_with`]): when another
//! thread holds the lock, the spin loop keeps passing a stale "acquired"
//! value to `try_once_compare_exchange_weak`, which keeps answering
//! `CmpxchResult::Unexpected`, and the loop `continue`s forever without
//! reloading the state or consulting the cancellation token. Run them with
//! `cargo test -p atomic_sync -- --ignored --nocapture` (each will spin for a
//! bounded watchdog interval before failing, and leak one spinner thread).

use std::{
    boxed::Box,
    string::String,
    sync::{
        atomic::{AtomicPtr, AtomicUsize, Ordering},
        mpsc, Arc,
    },
    thread,
    time::Duration,
    vec::Vec,
};

use atomex::StrictOrderings;
use abs_sync::x_deps::abs_cancel::CancelledToken;

use super::preemptive::{
    MsbAsMutexSignal, PtrAsMutexSignal, SpinningMutexEmbedded, SpinningMutexOwned,
    TrMutexSignal,
};

// -------------------------------------------------------------------------
// Signal functions: boundary values
// -------------------------------------------------------------------------

#[test]
fn msb_signal_boundaries() {
    type Sig = MsbAsMutexSignal<usize>;
    let msb = Sig::K_MSB_FLAG();

    // Released state is the default.
    assert!(!Sig::is_acquired(0));
    assert!(Sig::is_released(0));

    // Acquire sets the MSB; release clears it.
    assert_eq!(Sig::make_acquired(0), msb);
    assert!(Sig::is_acquired(msb));
    assert_eq!(Sig::make_released(msb), 0);

    // Data bits are preserved across acquire/release round-trips.
    let data = 0x0F0Fusize;
    assert_eq!(Sig::make_acquired(data) & !msb, data);
    assert_eq!(Sig::make_released(Sig::make_acquired(data)), data);

    // Boundary: all data bits set (MSB clear) is still "released".
    let all_data = !msb;
    assert!(!Sig::is_acquired(all_data));

    // Boundary: all bits set is "acquired" and releases back to all-data.
    assert!(Sig::is_acquired(usize::MAX));
    assert_eq!(Sig::make_released(usize::MAX), all_data);

    // Make-again on an already acquired value keeps it acquired.
    assert!(Sig::is_acquired(Sig::make_acquired(msb)));
}

#[test]
fn ptr_signal_boundaries() {
    type Sig = PtrAsMutexSignal<usize>;

    let null = 0usize as *mut usize;
    let even = 0x10usize as *mut usize;
    let odd = 0x11usize as *mut usize;

    // Null / even pointers are "released".
    assert!(Sig::is_released(null));
    assert!(Sig::is_released(even));

    // Acquire makes the pointer odd; release makes it even again.
    assert!(Sig::is_acquired(Sig::make_acquired(even)));
    assert_eq!(Sig::make_released(Sig::make_acquired(even)), even);
    assert_eq!(Sig::make_acquired(null) as usize, 1);

    // Boundary: an *odd* pointer is mis-detected as acquired. The signal
    // scheme silently requires 2-aligned pointers; this is not enforced
    // anywhere, so a user who stores an odd pointer into the cell gets a
    // mutex that looks permanently locked.
    assert!(Sig::is_acquired(odd));
    // `make_released` on an odd pointer produces an *even* pointer that is a
    // different address than the original — i.e. the round-trip is broken for
    // non-2-aligned pointers.
    assert_eq!(Sig::make_released(odd) as usize, 0x10);
}

#[test]
fn ptr_signal_odd_cell_breaks_mutex() {
    // Construct an embedded mutex whose cell already holds an ODD pointer.
    // Because `new` (unlike `new_embedded`) does not reset the cell, the
    // mutex believes it is permanently acquired: `try_lock` must fail.
    let mut data = Box::new(7usize);
    let mut cell = Box::new(AtomicPtr::new(0x3usize as *mut usize));
    let lock = SpinningMutexEmbedded::<
        &mut usize,
        AtomicPtr<usize>,
        PtrAsMutexSignal<usize>,
        StrictOrderings,
    >::new(&mut data, &mut cell);

    assert!(lock.is_acquired());
    let mut acq = lock.acquire();
    assert!(acq.try_lock().is_none());
}

// -------------------------------------------------------------------------
// Lock state machine: single thread
// -------------------------------------------------------------------------

#[test]
fn try_lock_lifecycle() {
    let mutex = SpinningMutexOwned::<usize>::new_owned(42);
    assert!(!mutex.is_acquired());

    let mut acq1 = mutex.acquire();
    let g = acq1.try_lock().expect("first try_lock should succeed");
    assert!(mutex.is_acquired());
    assert_eq!(*g, 42);

    // The lock is held: a second try_lock (via another Acquire handle) must
    // fail without blocking.
    let mut acq2 = mutex.acquire();
    assert!(acq2.try_lock().is_none());

    drop(g);
    assert!(!mutex.is_acquired());
    let g = acq2.try_lock().expect("try_lock after release");
    assert_eq!(*g, 42);
}

#[test]
fn lock_wait_single_thread() {
    let mutex = SpinningMutexOwned::<usize>::new_owned(1);
    let mut acq = mutex.acquire();
    {
        let mut g = acq.lock().wait().expect("lock().wait()");
        *g += 1;
        assert_eq!(*g, 2);
    }
    {
        let g = acq.lock().wait().expect("re-lock");
        assert_eq!(*g, 2);
    }
}

#[test]
fn embedded_new_resets_cell_to_default() {
    // `new_embedded` overwrites the cell with the default (released) value,
    // so a dirty cell must not break the mutex.
    let data = 9usize;
    let mut cell = AtomicUsize::new(usize::MAX); // dirty
    let lock = SpinningMutexEmbedded::<usize, AtomicUsize>::new_embedded(
        data,
        &mut cell,
    );
    assert!(!lock.is_acquired());
    let mut acq = lock.acquire();
    let g = acq.try_lock().expect("try_lock after reset");
    assert_eq!(*g, 9);
}

#[test]
fn into_inner_roundtrip() {
    let mutex = SpinningMutexOwned::<String>::new_owned(String::from("hi"));
    // (String is used so `into_inner` is exercised on an owned value.)
    assert_eq!(mutex.into_inner(), "hi");
}

// -------------------------------------------------------------------------
// Cancellation semantics (safe cases)
// -------------------------------------------------------------------------

#[test]
fn may_break_free_lock_ignores_already_cancelled_token() {
    // When the lock is immediately available, the spin loop succeeds before
    // the cancellation token is ever consulted, so a pre-cancelled token
    // still yields the guard. This pins down the current (arguably
    // permissive) behavior.
    let mutex = SpinningMutexOwned::<usize>::new_owned(3);
    let mut acq = mutex.acquire();
    let mut token = CancelledToken::new();
    assert!(token.is_cancelled());
    let g = acq.lock().may_break_with(&mut token).expect("guard");
    assert_eq!(*g, 3);
}

// -------------------------------------------------------------------------
// Lock state machine: contention via try_lock (bounded, no livelock)
// -------------------------------------------------------------------------

#[test]
fn contended_increment_3_to_8_threads() {
    for n_threads in 3..=8usize {
        let counter = Arc::new(SpinningMutexOwned::<usize>::new_owned(0));
        let iters = 1000usize;
        let handles: Vec<_> = (0..n_threads)
            .map(|_| {
                let c = counter.clone();
                thread::spawn(move || {
                    let mut acq = c.acquire();
                    let mut done = 0usize;
                    while done < iters {
                        // try_lock never livelocks: it is a single CAS
                        // attempt, so busy-waiting with it is safe.
                        let Some(mut g) = acq.try_lock() else {
                            thread::yield_now();
                            continue;
                        };
                        *g += 1;
                        done += 1;
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        let final_val = unsafe { counter.as_mut_ptr().read() };
        assert_eq!(
            final_val,
            n_threads * iters,
            "lost increments with {n_threads} threads"
        );
    }
}

// -------------------------------------------------------------------------
// Livelock demonstrations (gated; reveal a bug in `try_spin_acquire_`)
// -------------------------------------------------------------------------

fn held_lock_livelock_probe<F>(work: F) -> mpsc::Receiver<()>
where
    F: FnOnce() + Send + 'static,
{
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        work();
        let _ = tx.send(());
    });
    rx
}

#[test]
#[ignore = "demonstrates the try_spin_acquire_ livelock (run with --ignored)"]
fn lock_wait_livelocks_under_contention() {
    // Thread A holds the lock for a while. Thread B calls `lock().wait()`.
    // BUG: B's first state load sees "acquired"; every subsequent
    // `try_once_compare_exchange_weak` answers `Unexpected`, and the loop
    // `continue`s with the same stale value forever — even after A releases.
    let mutex = Arc::new(SpinningMutexOwned::<usize>::new_owned(0));
    let started = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let holder = {
        let m = mutex.clone();
        let started = started.clone();
        thread::spawn(move || {
            let mut acq = m.acquire();
            let _g = acq.lock().wait().unwrap();
            started.store(true, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(300));
        })
    };
    while !started.load(Ordering::SeqCst) {
        thread::yield_now();
    }

    let rx = held_lock_livelock_probe(move || {
        let mut acq = mutex.acquire();
        let _g = acq.lock().wait().expect("should eventually acquire");
    });

    let timeout = Duration::from_secs(2);
    match rx.recv_timeout(timeout) {
        Ok(()) => {
            holder.join().unwrap();
        }
        Err(mpsc::RecvTimeoutError::Timeout) => {
            panic!(
                "LIVELOCK: lock().wait() did not acquire even after the \
                 holder released the lock within {timeout:?}"
            )
        }
        Err(e) => panic!("{e:?}"),
    }
}

#[test]
#[ignore = "demonstrates the try_spin_acquire_ livelock (run with --ignored)"]
fn may_break_with_cancelled_token_livelocks_while_held() {
    // A pre-cancelled token must make `may_break_with` return `None`
    // immediately. When another thread holds the lock, the spin loop never
    // consults the token (the `Unexpected` path `continue`s before the
    // `cancel.is_cancelled()` check), so the caller spins forever.
    let mutex = Arc::new(SpinningMutexOwned::<usize>::new_owned(0));
    let started = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let holder = {
        let m = mutex.clone();
        let started = started.clone();
        thread::spawn(move || {
            let mut acq = m.acquire();
            let _g = acq.lock().wait().unwrap();
            started.store(true, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(300));
        })
    };
    while !started.load(Ordering::SeqCst) {
        thread::yield_now();
    }

    let rx = held_lock_livelock_probe(move || {
        let mut acq = mutex.acquire();
        let mut token = CancelledToken::new();
        assert!(token.is_cancelled());
        let g = acq.lock().may_break_with(&mut token);
        assert!(g.is_none(), "pre-cancelled token must not acquire");
    });

    let timeout = Duration::from_secs(2);
    match rx.recv_timeout(timeout) {
        Ok(()) => {
            holder.join().unwrap();
        }
        Err(mpsc::RecvTimeoutError::Timeout) => {
            panic!(
                "LIVELOCK: may_break_with(pre-cancelled token) did not \
                 return within {timeout:?}"
            )
        }
        Err(e) => panic!("{e:?}"),
    }
}
