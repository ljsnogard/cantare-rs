mod encode_;
mod decode_;
pub mod prefix;

#[cfg(test)]
mod tests_;

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
