#![feature(allocator_api)]
#![feature(btreemap_alloc)]
#![feature(btree_cursors)]

#![allow(unused_features)]
// to enable no hand-written poll
#![feature(async_fn_traits)]
#![feature(impl_trait_in_assoc_type)]
#![feature(unboxed_closures)]

#![no_std]

extern crate alloc;

mod opr_;
mod tree_node_;

pub use opr_::{FsTree};
pub use tree_node_::NodeId;
