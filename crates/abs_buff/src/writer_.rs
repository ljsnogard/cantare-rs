use core::error::Error;

use abs_cancel::TrMayCancel;
use anylr::SomeOf;

use crate::{Demand, buffer::TrBuffSegmMut};

/// A kind of buffer that owns the memory for writing data by lending some
/// segments to the producer.
///
/// This design is to keep compatible with `io_uring` and polling model.
pub trait TrBuffWrite<T = u8> {
    type SegmMut<'f>: TrBuffSegmMut<'f, T>
    where
        Self: 'f;
    type Err: Error;

    /// Indicates whethe the buff will no longer accept data writing.
    ///
    /// This function lets the user knows when to stop producing loop regardless
    /// any knowledge of the error type.
    fn is_blocked_closing(&self) -> bool;

    /// Lend some segments for writing in an async manner. The total amount of
    /// items is specified by the parameter `demand`.
    fn write_async<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> impl TrMayCancel<'f, MayCancelOutput = SomeOf<Self::SegmMut<'f>, Self::Err>>;
}

pub trait TrBuffTryWrite<T = u8>: TrBuffWrite<T> {
    fn try_write<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> SomeOf<Self::SegmMut<'f>, Self::Err>;
}
