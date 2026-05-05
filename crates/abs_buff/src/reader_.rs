use core::{
    error::Error,
    ops::RangeBounds,
};

use abs_sync::may_cancel::TrMayCancel;
use anylr::SomeOf;

use crate::{BuffReadAsInput, TrInput, TrBuffSegmRef};

/// Buffer that will emit zero or more segments for consumer (and update cursor)
pub trait TrBuffRead<T = u8> {
    type Err: Error;

    /// Emits borrowed segment which carries the buffered items. The amount of items
    /// can be specified by the parameter `demand`.
    fn read_async<'a>(
        &'a mut self,
        demand: &impl RangeBounds<usize>,
    ) -> impl TrMayCancel<'a, MayCancelOutput = SomeOf<impl 'a + TrBuffSegmRef<T>, Self::Err>>;

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
        demand: &impl RangeBounds<usize>,
    ) -> SomeOf<impl 'a + TrBuffSegmRef<T>, Self::Err>;
}
