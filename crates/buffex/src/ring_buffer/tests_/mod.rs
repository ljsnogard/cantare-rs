//! Test suite for the ring buffer.
//!
//! * [`sync_`] — abs_buff / segm_buff semantics (partial segment borrows,
//!   reclaim-on-drop, wrap-around, peek, errors, closing, the `TrRingBuffer`
//!   trait), the vectored-IO kernel handoff, and the multithreaded SPSC pipe
//!   with no async runtime.
//! * [`scenario_`] — the *shared* pipe / kernel scenarios written only
//!   against the framework-agnostic core API, run under every selected
//!   framework's executor.
//! * [`frameworks_`] — per-framework tests of the `AsyncRead` / `AsyncWrite`
//!   trait implementations (compio default, tokio, smol) and a real compio
//!   kernel-IO test using the iovec (scatter/gather) handoff.

#[cfg(test)]
extern crate std;

#[cfg(test)]
use std::{boxed::Box, vec};

#[cfg(all(feature = "compio", unix))]
mod unix_stream_;

mod frameworks_;
mod mini_exec;
mod scenario_;
mod sync_;

use std::sync::Arc;

use crate::ring_buffer::{RingBuffer, RingRx, RingTx};

/// The byte written by the producer at position `i`.
#[inline]
pub(super) fn seq_byte(i: usize) -> u8 {
    (i % 256) as u8
}

/// The byte the kernel-sim fills at rx position `i`.
#[inline]
pub(super) fn pat_byte(i: usize) -> u8 {
    ((i * 7) % 256) as u8
}

/// A 16-byte ring buffer.
pub(super) const RING_CAP: usize = 16;

pub(super) type SharedRing = Arc<RingBuffer<Box<[u8]>>>;
pub(super) type SharedTx = RingTx<SharedRing, Box<[u8]>>;
pub(super) type SharedRx = RingRx<SharedRing, Box<[u8]>>;

/// Create a full-duplex-in-time ring (16-byte buffer) with the halves.
pub(super) fn make_ring() -> (SharedRing, SharedTx, SharedRx) {
    let ring = Arc::new(
        RingBuffer::<Box<[u8]>>::try_new(vec![0u8; RING_CAP].into_boxed_slice()).unwrap(),
    );
    let (tx, rx) = RingBuffer::try_split_shared(ring.clone());
    (ring, tx, rx)
}

/// Create a shared ring without splitting (for the kernel-mode drivers).
pub(super) fn make_ring_shared() -> SharedRing {
    Arc::new(
        RingBuffer::<Box<[u8]>>::try_new(vec![0u8; RING_CAP].into_boxed_slice()).unwrap(),
    )
}
