use core::error::Error;

use abs_cancel::TrMayCancel;
use anylr::SomeOf;

use crate::{BuffReadAsInput, Demand, TrInput, TrBuffSegmRef};

/// A kind of buffer that owns the memory for reading data by lending some
/// segments to the consumer.
///
/// This design is to keep compatible with `io_uring` and polling model.
pub trait TrBuffRead<T = u8> {
    type SegmRef<'a>: TrBuffSegmRef<T> where Self: 'a;
    type Err: Error;

    /// Indicates whether this buff will no longer emits any data.
    ///
    /// This function lets the user knows when to stop consuming loop regardless
    /// any knowledge of the error type.
    fn is_drained(&self) -> bool;

    /// Emits borrowed segment which carries the buffered items. The amount of items
    /// can be specified by the parameter `demand`.
    fn read_async<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> impl TrMayCancel<'f, MayCancelOutput = SomeOf<Self::SegmRef<'f>, Self::Err>>;

    /// Turns the mutable borrow of the buffer into an input.
    /// It has a default implementation that yields `BuffReadAsInput`
    fn as_input(&mut self) -> impl TrInput<T>
    where
        Self: Sized,
    {
        BuffReadAsInput::<&mut Self, Self, T>::new(self)
    }
}

pub trait TrBuffTryRead<T = u8>: TrBuffRead<T> {
    fn try_read<'a>(
        &'a mut self,
        demand: &Demand<usize>,
    ) -> SomeOf<Self::SegmRef<'a>, Self::Err>;
}
