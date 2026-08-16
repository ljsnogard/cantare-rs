# cancel_src

A C#-style `CancellationTokenSource` / `CancellationToken` for Rust, built on
[`futures-channel`](https://docs.rs/futures-channel) (`oneshot`) and
[`abs_cancel`](../abs_cancel).

```rust
use cancel_src::CancellationTokenSource;

let source = CancellationTokenSource::new();
let mut token = source.token();      // cheap Arc::clone-style handle
let child = token.spawn_child_token();

source.cancel();                     // cancels token and child
assert!(token.is_cancelled());
assert!(child.is_cancelled());
```

- `CancellationToken` implements `abs_cancel::TrCancellationToken`:
  `is_cancelled`, `can_be_cancelled`, `try_spawn_child_token`,
  `cancellation` (a `futures_channel::oneshot` future that resolves with
  `Err(Canceled)` when the token is cancelled).
- `register` runs callbacks on cancellation; the returned
  `CancellationTokenRegistration` unregisters on drop.
- `#![no_std]` (with `alloc`); the only sync primitive is a small internal
  spin lock.
- The shared state is allocated with `Global` by default. Enable the
  `allocator_api` feature to make the types generic over a user-supplied
  allocator:

  ```toml
  [dependencies]
  cancel_src = { path = "../cancel_src", features = ["allocator_api"] }
  ```

  ```rust
  use cancel_src::CancellationTokenSource;
  let source = CancellationTokenSource::with_allocator(my_allocator);
  ```
