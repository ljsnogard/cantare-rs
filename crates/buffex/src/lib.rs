#![no_std]
#![cfg_attr(test, feature(try_trait_v2))]

// We always pull in `std` during tests, because it's just easier
// to write tests when you can assume you're on a capable platform
#[cfg(test)]
extern crate std;

pub mod ring_buffer;

#[cfg(all(feature = "compio", unix))]
pub mod unix_stream;

pub mod x_deps {
    pub use abs_buff;
    pub use abs_buff::x_deps::{abs_cancel, anylr};
    pub use atomex;
    pub use atomex::x_deps::funty;
}
