use core::{mem::MaybeUninit, ops::Try};

use crate::{
    io::{TrInput, TrOutput},
    BuffSegmRefAsInput, BuffSegmMutAsOutput, Demand,
};

/// Represent a sequence of slices who are logically the same array but
/// physically not.
pub trait TrBuffSegmView {
    type Item: Sized;

    /// Returns true if no available items to consume, false otherwise.
    fn is_empty(&self) -> bool;

    /// Returns the capacity of the segment, no matter the elements are
    /// consumed or not.
    fn capacity(&self) -> usize;

    /// The minimum count of unconsumed items. For a segment view that
    /// has only ONE PIECE, this is the unconsumed item count. For those
    /// have more than once slice, this is the unconsumed item count for
    /// the first slice.
    fn least_count(&self) -> usize;

    /// Iterate the unconsumed parts of the segment slice by slice.
    fn iter_slices(
        &self,
    ) -> impl IntoIterator<Item = &[Self::Item]>;
}

/// A buffer that its data is organized with one or more slices
pub trait TrBuffSegmRef<T>
where
    Self: TrBuffSegmView<Item = T>,
{
    /// Take a slice starting from the beginning of the unconsumed part, length
    /// suggested by the demand argument. Will reduce the length of the segment
    /// when the taken slice drops.
    ///
    /// The amount of the reducing will be the size of taken slice no matter if
    /// the items in it are actually moved or not. No drop. So this may leak.
    fn take_segm_ref<'a>(
        &'a mut self,
        demand: &Demand<usize>,
    ) -> impl 'a + Try<Output: 'a + TrBuffSegmRef<T>>;

    /// Turn the borrow of this segment into an input so that its internal data
    /// can be read by copying or moving.
    fn as_input(&mut self) -> impl TrInput<T>
    where
        Self: Sized,
    {
        BuffSegmRefAsInput::<&mut Self, Self, T>::new(self)
    }
}

/// A buffer that its data is organized with one or more slices mut.
pub trait TrBuffSegmMut<T>
where
    Self: TrBuffSegmView<Item = MaybeUninit<T>>,
{
    /// Take a slice starting from the beginning of the unconsumed part, length
    /// suggested by the demand argument. Will reduce the length of the segment
    /// when the taken slice drops.
    ///
    /// The amount of the reducing will be the size of taken slice no matter if
    /// the items in it are actually moved or not. No drop. So this may leak.
    fn take_segm_mut<'a>(
        &'a mut self,
        demand: &Demand<usize>,
    ) -> impl 'a + Try<Output: 'a + TrBuffSegmMut<T>>;

    /// Iterate the unconsumed parts of the segment one by one in the form of
    /// mut slices.
    fn iter_slices_mut<'a>(
        &'a mut self,
    ) -> impl IntoIterator<Item = &'a mut [MaybeUninit<T>]>
    where
        T: 'a;

    /// Turn the mutable borrow of this segment into an output device so that
    /// its internal buffer can be filled by copying or moving.
    fn as_output(&mut self) -> impl TrOutput<T>
    where
        Self: Sized,
    {
        BuffSegmMutAsOutput::<&mut Self, Self, T>::new(self)
    }
}
