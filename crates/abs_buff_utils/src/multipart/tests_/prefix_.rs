//! 长度前缀参数约定测试。

use super::{TrMultipartPrefix, U8Prefix, U16Prefix, U32Prefix};

/// 测试目标：验证各前缀选项的 `PREFIX_MAX`（前缀可表达的数据长度值个数，
/// 编码端据此对分块长度封顶）。
/// - 手段：直接调用 `TrMultipartPrefix::PREFIX_MAX()` 并断言返回值。
/// - 判定：`U8Prefix` / `U16Prefix` / `U32Prefix` 分别返回 `1 << 8`、
///   `1 << 16`、`1 << 32`，且递增。
#[test]
fn prefix_max_by_bits_() {
    let u8_ = U8Prefix::PREFIX_MAX();
    let u16_ = U16Prefix::PREFIX_MAX();
    let u32_ = U32Prefix::PREFIX_MAX();

    assert_eq!(u8_, 1usize << 8);
    assert_eq!(u16_, 1usize << 16);
    assert_eq!(u32_, 1usize << 32);
    assert!(u8_ < u16_ && u16_ < u32_, "前缀位宽越大，可表达长度越多");
}

/// 测试目标：验证 `PREFIX_LEN` 为前缀字段的**字节长度**（`BITS / 8`）。
/// - 手段：调用 `TrMultipartPrefix::PREFIX_LEN()` 并断言返回值。
/// - 判定：`U8Prefix` / `U16Prefix` / `U32Prefix` 分别返回 1 / 2 / 4——
///   与「默认两字节前缀」的协议约定一致，编码 / 解码端按此读写前缀字段。
#[test]
fn prefix_len_is_bytes_() {
    assert_eq!(U8Prefix::PREFIX_LEN(), 1);
    assert_eq!(U16Prefix::PREFIX_LEN(), 2);
    assert_eq!(U32Prefix::PREFIX_LEN(), 4);
}
