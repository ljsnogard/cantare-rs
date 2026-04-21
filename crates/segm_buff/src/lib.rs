#![no_std]

#![feature(try_trait_v2)]

// We always pull in `std` during tests, because it's just easier
// to write tests when you can assume you're on a capable platform
#[cfg(test)]
extern crate std;

mod reclaim_;
mod segm_mut_;
mod segm_ref_;

pub use reclaim_::{SegmSelfReclaim, NoReclaim, TrReclaim};
pub use segm_mut_::SegmMut;
pub use segm_ref_::SegmRef;

pub mod x_deps {
    pub use abs_buff;

    pub use abs_buff::x_deps::abs_sync;
}
