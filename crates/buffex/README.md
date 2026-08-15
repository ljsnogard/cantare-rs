# buffex

BUFFer EXtensions: ring buffer and buffer chaining without `std` dependency.

## `ring_buffer`

A single-buffer, lock-free ring buffer between a user thread and a runtime
(kernel) side.

* The ring exclusively owns **one** heap-allocated `[T]` buffer; the storage
  type is generic (`B: DerefMut<Target = [T]>`, e.g. `Box<[u8]>`).
* The reader position and the writer position are packed into a **single
  `AtomicUsize`** (half a word each), so one atomic load observes both. The
  ring is full when the writer position is immediately behind the reader
  position (one slot stays unused); the buffer length is limited to
  `u32::MAX` on 64-bit targets, matching the native iovec length field.
* The user side borrows partial segments through `segm_buff`
  (`SegmRef` / `SegmMut` with reclaim), compatible with `abs_buff`
  (`TrBuffRead` / `TrBuffWrite` / `TrBuffPeek`).
* The runtime side takes the readable / writable region as an **iovec pair**
  (scatter/gather; two slices when the region wraps) and submits it to the
  kernel with a single `writev` / `readv` syscall
  (`take_send_iovecs` / `take_recv_iovecs`, then `put_back_send` /
  `put_back_recv`).

The core is `no_std` and async-runtime agnostic. `AsyncRead` / `AsyncWrite`
implementations are provided through Cargo features:

| feature   | traits                                |
|-----------|---------------------------------------|
| `compio` (default) | `compio::io::AsyncRead` / `AsyncWrite` + vectored kernel handoff |
| `tokio`   | `tokio::io::AsyncRead` / `AsyncWrite` |
| `smol`    | `futures_io::AsyncRead` / `AsyncWrite` |

Under heavy development.
