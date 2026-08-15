//! Reclaim functions for the segments borrowed from the ring buffer.
//!
//! A segment borrowed from the ring commits its whole region back to the ring
//! when it drops (the segm_buff contract): the writer reclaim advances the
//! writer position, the reader reclaim advances the reader position.

use core::{mem::MaybeUninit, ops::DerefMut};

use segm_buff::{SegmMut, SegmRef, TrReclaim};

use super::state_::RingBuffer;

/// A write segment borrowed from the ring buffer.
pub type ReclSliceMut<'a, B, T> = SegmMut<&'a mut [MaybeUninit<T>], WriterReclaim<'a, B, T>>;

/// A read segment borrowed from the ring buffer.
pub type ReclSliceRef<'a, B, T> = SegmRef<&'a [T], ReaderReclaim<'a, B, T>>;

/// Advances the writer position when a borrowed write segment drops.
pub struct WriterReclaim<'a, B, T>
where
    B: DerefMut<Target = [T]>,
{
    ring: &'a RingBuffer<B, T>,
}

impl<'a, B, T> WriterReclaim<'a, B, T>
where
    B: DerefMut<Target = [T]>,
{
    pub(super) const fn new(ring: &'a RingBuffer<B, T>) -> Self {
        WriterReclaim { ring }
    }
}

impl<B, T> TrReclaim for WriterReclaim<'_, B, T>
where
    B: DerefMut<Target = [T]>,
{
    fn reclaim(&mut self, amount: usize) {
        self.ring.advance_write(amount);
    }
}

/// Advances the reader position when a borrowed read segment drops.
pub struct ReaderReclaim<'a, B, T>
where
    B: DerefMut<Target = [T]>,
{
    ring: &'a RingBuffer<B, T>,
}

impl<'a, B, T> ReaderReclaim<'a, B, T>
where
    B: DerefMut<Target = [T]>,
{
    pub(super) const fn new(ring: &'a RingBuffer<B, T>) -> Self {
        ReaderReclaim { ring }
    }
}

impl<B, T> TrReclaim for ReaderReclaim<'_, B, T>
where
    B: DerefMut<Target = [T]>,
{
    fn reclaim(&mut self, amount: usize) {
        self.ring.advance_read(amount);
    }
}
