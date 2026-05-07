#![allow(unused_features)]
#![feature(allocator_api)] // This is used when collections feature is on

#![no_std]

mod impl_items_view_;
mod iter_;
mod array_;

#[cfg(test)]
extern crate std;

pub use array_::{TrArray, TrAsSlice, TrAsSliceMut};
pub use iter_::{TrItemsRefView, TrItemsMutView};
