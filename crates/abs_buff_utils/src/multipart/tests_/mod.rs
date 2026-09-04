//! multipart 模块的单元测试与集成测试。
//!
//! 本文件只负责声明测试子模块与导出测试共用符号；具体测试按主题拆分到：
//!
//! * [`prefix_`]——长度前缀参数约定；
//! * [`encode_`]——编码端协议流格式、取消；
//! * [`decode_`]——解码读流异常流、EOF、取消；
//! * [`integration_`]——编码 → 解码端到端往返与并发驱动。

mod common_;
mod decode_;
mod encode_;
mod integration_;
mod prefix_;

use common_::*;

use super::{
    decode_::{DecodeError, MultipartDecode},
    encode_::MultipartEncode,
    prefix::{TrMultipartPrefix, U8Prefix, U16Prefix, U32Prefix},
};
