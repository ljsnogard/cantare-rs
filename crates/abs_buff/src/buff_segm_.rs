use core::{
    mem::MaybeUninit,
    ops::Try,
};

use crate::{BuffSegmRefAsInput, BuffSegmMutAsOutput, Demand, TrInput, TrOutput};

pub trait TrBuffSegmView {
    type Item: Sized;

    /// Returns true if no available items to consume, false otherwise.
    fn is_empty(&self) -> bool;

    /// Returns the capacity of the segment, no matter the elements are
    /// consumed or not.
    fn capacity(&self) -> usize;

    /// Iterate the unconsumed parts of the segment slice by slice.
    fn iter_slices<'a>(
        &'a self,
    ) -> impl IntoIterator<Item: 'a + AsRef<[Self::Item]>>;
}

/// A buffer that its data is organized with one or more slices
pub trait TrBuffSegmRef<T>
where
    Self: TrBuffSegmView<Item = T>,
{
    /// Take a slice starting from the beginning out of this segment, length
    /// specified by the demand argument, reducing the length of this segment
    /// when the taken slice drops.
    fn take_segm_ref<'a>(
        &'a mut self,
        demand: &'a Demand<usize>,
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
    /// Take a slice starting from the beginning out of this segment, length
    /// specified by the demand argument, reducing the length of this segment
    /// when the taken slice drops.
    fn take_segm_mut<'a>(
        &'a mut self,
        demand: &'a Demand<usize>,
    ) -> impl 'a + Try<Output: 'a + TrBuffSegmMut<T>>;

    /// Iterate the unconsumed parts of the segment one by one in the form of
    /// mut slices.
    fn iter_slices_mut<'a>(
        &'a mut self,
    ) -> impl IntoIterator<Item: 'a + AsMut<[MaybeUninit<T>]>>;

    /// Turn the mutable borrow of this segment into an output device so that 
    /// its internal buffer can be filled by copying or moving.
    fn as_output(&mut self) -> impl TrOutput<T>
    where
        Self: Sized,
    {
        BuffSegmMutAsOutput::<&mut Self, Self, T>::new(self)
    }
}
