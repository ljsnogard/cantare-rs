#![no_std]

// The ring buffer tests (unix_stream_, frameworks_, pipe_retry_) use
// `try_trait_v2` APIs (e.g. `branch()`); the feature flag applies to the
// whole crate, tests included.
#![feature(try_trait_v2)]

// We always pull in `std` during tests, because it's just easier
// to write tests when you can assume you're on a capable platform
#[cfg(test)]
extern crate std;

pub mod ring_buffer;

#[cfg(all(feature = "compio", unix))]
pub mod unix_stream;

pub mod x_deps {
    pub use abs_buff;
    pub use abs_cancel;
    pub use atomex;
    pub use atomex::x_deps::funty;

    pub use segm_buff;
}
