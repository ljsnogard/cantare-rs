//! 以 circular buff 为基础，实现自动化的带长度前缀的分块发送与接收。
//!
//! # 协议
//!
//! 面向「长度未知的字节流」，本模块把发送端（源）到接收端（目标）之间需要
//! 搬运的数据切成若干**带长度前缀的分块**：
//!
//! ```text
//! [len][payload][len][payload] ... [0]
//! ```
//!
//! * 每个分块 = 一个长度前缀 + 紧随其后的 `len` 个数据字节；
//! * 前缀默认两个字节、大端序（[`prefix::U16Prefix`]），0 值表示数据流结束
//!   （EOF）；
//! * **发送端**（[`MultipartEncode`]）循环：取源缓冲当前数据量（封顶到
//!   `PREFIX_MAX - 1`）→ 写长度前缀 → 按长度把载荷从源搬运到目标 → 重复；
//!   源 drained（关闭且无数据）时写 0 前缀并结束；
//! * **接收端**（[`MultipartDecode`]）识别同一协议：读前缀 → 按长度把载荷
//!   搬运到目标缓冲 → 遇到 0 前缀即结束。
//!
//! 两端只依赖 `abs_buff` 的缓冲接口（[`abs_buff::TrBuffRead`] /
//! [`abs_buff::TrBuffWrite`]），因此可以桥接任意实现了这些接口的缓冲实现，
//! 例如 [`crate::circular_buff`] 的
//! [`Producer`](crate::circular_buff::Producer) /
//! [`Consumer`](crate::circular_buff::Consumer)。
//!
//! # 前缀选项
//!
//! 前缀位宽由泛型参数 `P: TrMultipartPrefix` 决定，默认
//! [`prefix::U16Prefix`]（两个字节），也可选 [`prefix::U8Prefix`] /
//! [`prefix::U32Prefix`]。
//!
//! # 取消
//!
//! 编码 / 解码 future 均支持 `may_cancel_with` 取消令牌：取消后立即停止，
//! 返回已搬运的载荷字节数（`SomeOf::new_left(count)`）。
//!
//! # 现状
//!
//! 编码端与解码端已实现并配套测试（见 `tests_` 模块）。仍待定 / 未完成：
//!
//! * [`MultipartDecode`]、`DecodeError` 及其生成类型**尚未从本模块导出**
//!   （不构成公开 API）——导出需另行确认；
//! * `T ≠ u8` 时的实践（前缀始终按字节搬运，`T` 仅作元素类型）待验证；
//! * 被动端取消：底层 `circular_buff` 的 `may_cancel_with` 目前忽略取消令牌
//!   （见 `circular_buff` 模块文档「仍待定」），因此取消只在 multipart 自身的
//!   循环检查点上生效。

mod encode_;
// `decode_` 尚未从本模块导出（不构成公开 API，见模块文档「现状」），因此
// 在 lib 目标中视为死代码；导出确认后移除该 allow。
#[allow(dead_code)]
mod decode_;
pub mod prefix;

use abs_buff::error::{ReadErrTag, TrTaggedError};

pub use encode_::MultipartEncode;
pub use decode_::MultipartDecode;


/// 判断读侧错误是否表示「数据流结束」（[`ReadErrTag::Closing`] /
/// [`ReadErrTag::Drained`]）。
///
/// 编码端据此决定「源 drained → 写 0 前缀 EOF」；解码端据此判定「流在没有
/// 终止块的情况下结束 → 格式错误」。
fn is_read_eof_<E>(err: &E) -> bool
where
    E: TrTaggedError<ReadErrTag>,
{
    matches!(err.err_tag(), ReadErrTag::Closing | ReadErrTag::Drained)
}

#[cfg(test)]
mod tests_;
