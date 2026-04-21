#![allow(unused_features)]
#![feature(allocator_api)] // This is used when collections feature is on

#![no_std]

mod iter_;
mod array_;

#[cfg(test)]
extern crate std;

#[cfg(any(feature = "collections", test))]
mod collections_;

pub use array_::{TrArray, TrAsSlice, TrAsSliceMut};
pub use iter_::{TrItemsRefView, TrItemsMutView};
