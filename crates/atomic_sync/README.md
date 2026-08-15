# atomic_sync

Atomic-based synchronization utils implementing traits in `abs_sync`.

- mod [`mutex`](src/mutex/mod.rs): a configurable preemptive spin lock
  (`SpinningMutex`), with an MSB flag signal or a pointer-parity signal.
- mod [`rwlock`](src/rwlock/mod.rs): reader-writer locks.

## Benchmark: spin lock vs `std::sync::Mutex`

`benches/mutex_contention.rs` (`cargo bench -p atomic_sync --bench mutex_contention`)
has 3–8 threads each incrementing a shared counter through a tiny critical
section, comparing:

- `SpinningMutex` with `StrictOrderings` (SeqCst CAS);
- `SpinningMutex` with `LocksOrderings` (Acquire/Relaxed CAS);
- `std::sync::Mutex`.

Because the spin lock's blocking path is driven by `try_lock` + `yield_now`
(the `lock().wait()` path is for cancellation-aware use), the std mutex uses
its blocking `lock()`. Reported values are operations per second (higher is
better) after one warm-up round, taking the best of 3 rounds.

Measured on an AMD Ryzen 7 3700X (8 cores / 16 threads), x86_64, 2026-08:

```
threads |  spin(seqcst) | spin(acquire) |    std::Mutex |   vs std | locks/spin
     3  |    102238737 |     94165638 |     35684042 |    2.87x |    0.92x
     4  |     91551826 |     84608156 |     27020799 |    3.39x |    0.92x
     5  |     73405378 |     78509765 |     22989399 |    3.19x |    1.07x
     6  |     66814480 |     67921274 |     20320446 |    3.29x |    1.02x
     7  |     51824477 |     57565241 |     20150962 |    2.57x |    1.11x
     8  |     52715570 |     52470355 |     19970300 |    2.64x |    1.00x
```

On this workload (a very short critical section) the spin lock is about
**2.6–3.4× faster** than `std::sync::Mutex`: the std mutex parks the losing
threads (system-call overhead) while the spin lock busy-waits. The trade-off
is that spinning burns CPU, so a spin lock only pays off for short critical
sections and when the lock is not held for long. The SeqCst and
Acquire/Relaxed variants perform about the same here.
