use core::error::Error;

use abs_sync::may_cancel::TrMayCancel;
use anylr::SomeOf;

use crate::{BuffWriteAsOutput, Demand, TrBuffSegmMut, TrOutput};

/// Buffer that will emit zero or more segments for producer (and update cursor)
pub trait TrBuffWrite<T = u8> {
    type Err: Error;

    /// Lend some segments for writing in an async manner. The total amount of
    /// items is specified by the parameter `demand`.
    fn write_async<'a>(
        &'a mut self,
        demand: &'a Demand<usize>,
    ) -> impl TrMayCancel<'a, MayCancelOutput = SomeOf<impl 'a + TrBuffSegmMut<T>, Self::Err>>;

    fn as_output(&mut self) -> impl TrOutput<T>
    where
        Self: Sized,
    {
        BuffWriteAsOutput::<&mut Self, Self, T>::new(self)
    }
}

pub trait TrBuffTryWrite<T = u8>: TrBuffWrite<T> {
    fn try_write<'a>(
        &'a mut self,
        demand: &'a Demand<usize>,
    ) -> SomeOf<impl 'a + TrBuffSegmMut<T>, Self::Err>;
}
