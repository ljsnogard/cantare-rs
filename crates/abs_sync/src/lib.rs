#![no_std]

// to enable no hand-written poll
// #![feature(async_fn_traits)]
// #![feature(impl_trait_in_assoc_type)]
#![feature(unboxed_closures)]

#![feature(try_trait_v2)]
// #![feature(type_alias_impl_trait)]

#[cfg(test)]
extern crate std;

#[cfg(test)]
mod demo_;

pub mod async_lock;
pub mod async_mutex;
pub mod cancellation;
pub mod sync_guard;
pub mod may_break;
pub mod may_cancel;
pub mod ok_or;
pub mod sync_lock;
pub mod sync_mutex;

pub mod preludes {
    pub use super::async_lock::TrAsyncRwLock;
    pub use super::async_mutex::TrAsyncMutex;
    pub use super::cancellation::TrCancellationToken;
    pub use super::may_break::TrMayBreak;
    pub use super::may_cancel::TrMayCancel;
    pub use super::ok_or::{OkOr, XtOkOr};
    pub use super::sync_lock::TrSyncRwLock;
    pub use super::sync_mutex::TrSyncMutex;
}
