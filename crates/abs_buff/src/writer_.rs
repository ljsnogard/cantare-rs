use core::error::Error;

use abs_cancel::TrMayCancel;
use anylr::SomeOf;

use crate::{BuffWriteAsOutput, Demand, TrBuffSegmMut, TrOutput};

/// A kind of buffer that owns the memory for writing data by lending some
/// segments to the producer.
///
/// This design is to keep compatible with `io_uring` and polling model.
pub trait TrBuffWrite<T = u8> {
    type SegmMut<'a>: TrBuffSegmMut<T> where Self: 'a;
    type Err: Error;

    /// Indicates whethe the buff will no longer accept data writing.
    ///
    /// This function lets the user knows when to stop producing loop regardless
    /// any knowledge of the error type.
    fn is_blocked(&self) -> bool;

    /// Lend some segments for writing in an async manner. The total amount of
    /// items is specified by the parameter `demand`.
    fn write_async<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> impl TrMayCancel<'f, MayCancelOutput = SomeOf<Self::SegmMut<'f>, Self::Err>>;

    /// Turns the mutable borrow of the buffer into an output.
    /// It has a default implementation that yields `BuffWriteAsOutput`
    fn as_output(&mut self) -> impl TrOutput<T>
    where
        Self: Sized,
    {
        BuffWriteAsOutput::<&mut Self, Self, T>::new(self)
    }
}

pub trait TrBuffTryWrite<T = u8>: TrBuffWrite<T> {
    fn try_write<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> SomeOf<Self::SegmMut<'f>, Self::Err>;
}
