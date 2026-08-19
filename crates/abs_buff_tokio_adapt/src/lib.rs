// to enable no hand-written poll
#![feature(async_fn_traits)]
#![feature(impl_trait_in_assoc_type)]
#![feature(unboxed_closures)]
// #![feature(try_trait_v2)]
#![feature(min_specialization)]

mod read_as_input;
mod write_as_output;

pub use read_as_input::{ReadAsInput, InputReadAsync, InputReadFuture};
pub use write_as_output::{WriteAsOutput, OutputWriteAsync, OutputWriteFuture};
