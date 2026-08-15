//! A single-buffer, lock-free ring buffer between a user thread and a runtime
//! (kernel) side.
//!
//! # Design
//!
//! `RingBuffer` exclusively owns **one** heap-allocated `[T]` buffer. The
//! storage type is generic and only requires `DerefMut<Target = [T]>`, so any
//! heap pointer such as `Box<[T]>` works:
//!
//! ```ignore
//! let ring = RingBuffer::<Box<[u8]>>::try_new(Box::from([0u8; 4096])).unwrap();
//! ```
//!
//! The reader position `rp` and the writer position `wp` are packed into a
//! single `AtomicUsize` (half a word each), so one atomic load observes both.
//! The ring is full when the writer position is immediately behind the reader
//! position (one slot is always left unused):
//!
//! * `data = (wp - rp) mod cap`
//! * `free = cap - 1 - data`
//!
//! Two usage modes are supported on the same core:
//!
//! * **User pipe (tokio / smol poll mode)**: the user writes through
//!   [`RingTx`] (segm_buff segment borrows, abs_buff compatible) and reads
//!   through [`RingRx`]. This is the classic SPSC channel: one writer thread,
//!   one reader thread, no locks, no runtime dependency.
//! * **Kernel handoff (compio mode, scatter/gather)**: the runtime side takes
//!   the readable / writable region of the ring as an iovec pair
//!   ([`RingBuffer::take_send_iovecs`] / [`RingBuffer::take_recv_iovecs`])
//!   and submits it to the kernel with a single `writev` / `readv` syscall.
//!   The wrapped region becomes two slices packed into one iovec array; the
//!   kernel handles them in order. The region is returned with
//!   [`RingBuffer::put_back_send`] / [`RingBuffer::put_back_recv`].
//!
//! Because the positions occupy half of the `AtomicUsize`, the buffer length
//! is limited to [`state_::MAX_CAPACITY`] (e.g. `u32::MAX` on 64-bit
//! targets) — exactly the range of the native iovec length field.
//!
//! # Async framework support
//!
//! The core is async-runtime agnostic. On top of it, `AsyncRead` /
//! `AsyncWrite` implementations are provided for:
//!
//! * compio (default feature `compio`): `compio::io::AsyncRead` /
//!   `compio::io::AsyncWrite`, plus the vectored kernel-handoff mode above;
//! * tokio (feature `tokio`): `tokio::io::AsyncRead` / `tokio::io::AsyncWrite`;
//! * smol & friends (feature `smol`): `futures_io::AsyncRead` /
//!   `futures_io::AsyncWrite`.
//!
//! # Safety
//!
//! A region handed to the runtime is referenced by `&'static` slices; it must
//! be returned via `put_back_send` / `put_back_recv` before the last
//! reference to the ring is dropped (asserted in debug builds).

mod abs_;
mod buffer_;
mod error_;
mod futures_;
mod reclaim_;
mod rx_;
mod state_;
mod tx_;

#[cfg(test)]
mod tests_;

pub use abs_::TrRingBuffer;
pub use error_::{RxError, TxError};
pub use futures_::{
    PeekAsync, PeekFuture, ReadAsync, ReadFuture, WaitFlushed, WaitFlushedFuture,
    WaitRxIdle, WaitRxIdleFuture, WaitTxIdle, WaitTxIdleFuture, WriteAsync, WriteFuture,
};
pub use reclaim_::{ReclSliceMut, ReclSliceRef};
pub use rx_::RingRx;
pub use state_::{MAX_CAPACITY, RingBuffer};
pub use tx_::RingTx;

#[cfg(feature = "compio")]
mod compio_;

#[cfg(feature = "tokio")]
mod tokio_;

#[cfg(feature = "smol")]
mod smol_;
