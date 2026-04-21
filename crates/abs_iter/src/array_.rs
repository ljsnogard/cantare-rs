use core::{
    borrow::{Borrow, BorrowMut},
    convert::{AsRef, AsMut},
};

/// This trait provides the associated type `Elem` and a the `as_slice` function
/// for types that implementing both `AsRef<[T]>` and `Borrow<[T]>`
pub trait TrAsSlice
where
    Self: AsRef<[Self::Elem]> + Borrow<[Self::Elem]>,
{
    type Elem: Sized;

    fn as_slice(&self) -> &[Self::Elem] {
        self.as_ref()
    }
}

/// This trait provides the associated type `Elem` and a the `as_slice_mut`
/// function for types that implementing both `AsMut<[T]>` and `BorrowMut<[T]>`
pub trait TrAsSliceMut
where
    Self: TrAsSlice
        + AsMut<[Self::Elem]>
        + BorrowMut<[Self::Elem]>,
{
    fn as_slice_mut(&mut self) -> &mut [Self::Elem] {
        self.as_mut()
    }
}

/// An abstraction over the arrays
pub trait TrArray : TrAsSliceMut {
    const LENGTH: usize;
}

impl<T, const N: usize> TrArray for [T; N] {
    const LENGTH: usize = N;
}

impl<T, const N: usize> TrAsSlice for [T; N] {
    type Elem = T;
}

impl<T, const N: usize> TrAsSliceMut for [T; N]
{}

impl<T> TrAsSlice for [T] {
    type Elem = T;
}

impl<T> TrAsSliceMut for [T]
{}

impl<T> TrAsSlice for &[T] {
    type Elem = T;
}

impl<T> TrAsSlice for &mut [T] {
    type Elem = T;
}

impl<T> TrAsSliceMut for &mut [T]
{}
