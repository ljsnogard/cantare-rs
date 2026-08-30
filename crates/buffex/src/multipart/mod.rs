//! 以 circular buff 为基础，实现自动化的带长度前缀的分块发送与接收

pub mod prefix;
mod decode_;
mod encode_;

pub use encode_::{MultipartEncode, MultipartEncodeStartAsync, MultipartEncodeStartFuture};
