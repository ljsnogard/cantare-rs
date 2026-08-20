//! Types and helpers shared by the read and write adapters.

use std::{boxed::Box, mem::MaybeUninit, sync::Arc, vec};

use buffex::ring_buffer::{RingBuffer, RingRx, RingTx};

/// Shared ring storage used by both [`crate::IrohReader`] and
/// [`crate::IrohWriter`].
pub(super) type SharedRing = Arc<RingBuffer<Box<[MaybeUninit<u8>]>>>;

/// Write half of a shared ring, used as the internal buffer of
/// [`crate::IrohWriter`].
pub(super) type SharedTx = RingTx<SharedRing, Box<[MaybeUninit<u8>]>>;

/// Read half of a shared ring, used as the internal buffer of
/// [`crate::IrohReader`].
pub(super) type SharedRx = RingRx<SharedRing, Box<[MaybeUninit<u8>]>>;

/// Create a new shared ring with at least two usable bytes.
pub(super) fn new_shared_ring(cap: usize) -> SharedRing {
    let cap = cap.max(2);
    let buf: Box<[MaybeUninit<u8>]> = vec![MaybeUninit::uninit(); cap].into_boxed_slice();
    Arc::new(
        RingBuffer::try_new(buf)
            .unwrap_or_else(|len| panic!("invalid iroh buffer capacity {len}")),
    )
}
