// to enable no hand-written poll
#![feature(impl_trait_in_assoc_type)]
#![feature(unboxed_closures)]
#![feature(try_trait_v2)]
#![feature(min_specialization)]
#![no_std]

// We always pull in `std` during tests, because it's just easier
// to write tests when you can assume you're on a capable platform
#[cfg(test)]
extern crate std;

pub use gen_mcf_macro::gen_may_cancel_future;

pub mod buffer;
pub mod io;
pub mod pipelining;

mod demand_;
mod peeker_;
mod reader_;
mod slice_impl_;
mod writer_;

pub use demand_::Demand;
pub use peeker_::{TrBuffPeek, TrBuffTryPeek};
pub use reader_::{TrBuffRead, TrBuffTryRead};
pub use writer_::{TrBuffTryWrite, TrBuffWrite};

pub mod x_deps {
    pub use abs_cancel;
    pub use abs_iter;
    pub use anylr;
    pub use funty;
    pub use gen_mcf_macro;
}
