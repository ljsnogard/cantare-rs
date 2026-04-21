use core::error::Error;

use abs_sync::may_cancel::TrMayCancel;
use anylr::SomeOf;

use crate::{BuffPeekAsInput, TrBuffSegmRef, TrInput};

/// Buffer that will borrow zero or more segments for data observation without
/// consuming them.
pub trait TrBuffPeek<T = u8> {
    type Err: Error;

    /// Lend some slices for peeking. The number and the length of the slices 
    /// to peek are decided by the buffer.
    fn peek_async<'a>(
        &'a mut self,
    ) -> impl TrMayCancel<'a,
        MayCancelOutput = SomeOf<impl 'a + TrBuffSegmRef<T>, Self::Err>>;

    fn as_intput(&mut self) -> impl TrInput<T>
    where
        Self: Sized,
    {
        BuffPeekAsInput::<&mut Self, Self, T>::new(self, 0usize)
    }
}

pub trait TrBuffTryPeek<T = u8>: TrBuffPeek<T> {
    fn try_peek<'a>(
        &'a mut self
    ) -> SomeOf<impl 'a + TrBuffSegmRef<T>, Self::Err>;
}
