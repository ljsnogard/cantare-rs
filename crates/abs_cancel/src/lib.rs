#![feature(try_trait_v2)]

#![no_std]

mod cancellation;

pub use cancellation::{
    CancelledToken, NonCancellableToken,
    TrCancellationToken, TrMayCancel,
};
