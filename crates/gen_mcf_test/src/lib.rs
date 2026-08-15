#![allow(unused_features)]
// to enable no hand-written poll
#![feature(async_fn_traits)]
#![feature(impl_trait_in_assoc_type)]
#![feature(unboxed_closures)]

// #[cfg(test)]
mod tests_;

/// Regression test for return types containing lifetimes.
mod lifetime_return_test;

/// Empty mod to check the output of `cargo expand`
mod out_;
