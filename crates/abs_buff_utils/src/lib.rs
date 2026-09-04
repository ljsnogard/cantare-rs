// to enable no hand-written poll
#![allow(unused_features)]
#![feature(async_fn_traits)]
#![feature(impl_trait_in_assoc_type)]
#![feature(unboxed_closures)]

pub mod multipart;

pub mod x_deps {
    pub use abs_buff;
}
