use core::error::Error;

use abs_cancel::TrMayCancel;
use anylr::SomeOf;

use crate::{Demand, buffer::TrBuffSegmRef};

/// A kind of buffer that owns the memory for reading data by lending some
/// segments to the consumer.
///
/// This design is to keep compatible with `io_uring` and polling model.
pub trait TrBuffRead<T = u8> {
    type SegmRef<'f>: TrBuffSegmRef<'f, T>
    where
        Self: 'f;
    type Err: Error;

    /// Indicates whether this buff will no longer emits any data.
    ///
    /// This function lets the user knows when to stop consuming loop regardless
    /// any knowledge of the error type.
    fn is_drained_closing(&self) -> bool;

    /// Emits borrowed segment which carries the buffered items. The amount of items
    /// can be specified by the parameter `demand`.
    fn read_async<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> impl TrMayCancel<'f, MayCancelOutput = SomeOf<Self::SegmRef<'f>, Self::Err>>;
}

pub trait TrBuffTryRead<T = u8>: TrBuffRead<T> {
    fn try_read<'a>(
        &'a mut self,
        demand: &Demand<usize>,
    ) -> SomeOf<Self::SegmRef<'a>, Self::Err>;
}
