// to enable no hand-written poll
#![feature(async_fn_traits)]
#![feature(impl_trait_in_assoc_type)]
#![feature(unboxed_closures)]
#![feature(try_trait_v2)]

#![no_std]

// We always pull in `std` during tests, because it's just easier
// to write tests when you can assume you're on a capable platform
#[cfg(test)]
extern crate std;

pub use gen_mcf_macro::gen_may_cancel_future;

pub mod io;

mod buff_peek_as_input_;
mod buff_read_as_input_;
mod buff_write_as_output_;
mod buff_segm_;
mod buff_segm_as_input_;
mod buff_segm_as_output_;
mod demand_;
mod peeker_;
mod reader_;
mod writer_;

pub use buff_peek_as_input_::{
    BuffPeekAsInput, BuffPeekInputAsync, BuffPeekInputFuture,
};
pub use buff_read_as_input_::{
    BuffReadAsInput, BuffReadInputAsync, BuffReadInputFuture,
};
pub use buff_write_as_output_::{
    BuffWriteAsOutput, BuffWriteOutputAsync, BuffWriteOutputFuture,
};
pub use buff_segm_::{TrBuffSegmView, TrBuffSegmMut, TrBuffSegmRef};
pub use buff_segm_as_input_::{
    BuffSegmRefAsInput, BuffSegmRefInputAsync, BuffSegmRefInputFuture,
};
pub use buff_segm_as_output_::{
    BuffSegmMutAsOutput, BuffSegmMutOutputAsync, BuffSegmMutOutputFuture,
    BuffSegmMutOutputClonedAsync, BuffSegmMutOutputClonedFuture,
};
pub use demand_::Demand;
pub use io::{TrInput, TrOutput};
pub use peeker_::{TrBuffPeek, TrBuffTryPeek};
pub use reader_::{TrBuffRead, TrBuffTryRead};
pub use writer_::{TrBuffWrite, TrBuffTryWrite};

pub mod x_deps {
    pub use abs_iter;
    pub use abs_sync;
    pub use anylr;
    pub use gen_mcf_macro;
}
