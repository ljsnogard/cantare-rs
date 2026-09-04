//! 长度前缀的参数化：前缀值的位宽类型（`u8` / `u16` / `u32`）与由此导出的
//! 前缀字段长度、最大可表达数据长度。

use abs_buff::x_deps::funty;

mod sealed_prefix_ {
    pub trait SealedPrefix_ {}
}

/// 长度前缀的编码参数。
///
/// multipart 协议的每个分块以一个**长度前缀**开头。本 trait 用关联类型
/// `Prefix`（`u8` / `u16` / `u32`）参数化前缀的位宽，并导出两个常量：
///
/// * [`TrMultipartPrefix::PREFIX_LEN`]——前缀字段的**字节长度**
///   （`BITS / 8`，对 `u8` / `u16` / `u32` 分别为 1 / 2 / 4）；
/// * [`TrMultipartPrefix::PREFIX_MAX`]——前缀可表达的数据长度值个数
///   （`1 << BITS`，含 0）。
///
/// 该 trait 是 **sealed** 的：只有本模块内的 [`U8Prefix`] / [`U16Prefix`] /
/// [`U32Prefix`] 三个标记类型可以实现，调用者不能自定义前缀格式，从而保证
/// 编码端与解码端的线格式一致。
///
/// # 约定（与编码 / 解码端一致）
///
/// * 单个分块的数据长度取值范围为 `0 .. PREFIX_MAX`，其中 `0` 表示数据流
///   结束（EOF）；编码端把分块长度封顶为 `PREFIX_MAX - 1`，因此 0 前缀严格
///   只表示 EOF；
/// * 前缀以**大端序**写入（数值的高字节在前）。
// `PREFIX_LEN` / `PREFIX_MAX` 采用常量式全大写命名（读作协议常量而非方法），
// 属有意为之，故忽略 `non_snake_case`。
#[allow(non_snake_case)]
pub trait TrMultipartPrefix
where
    Self: sealed_prefix_::SealedPrefix_,
{
    /// 前缀值的无符号整型类型（`u8` / `u16` / `u32`）。
    type Prefix: funty::Unsigned;

    /// 前缀字段的**字节长度**，即 `BITS / 8`（对 `u8` / `u16` / `u32` 分别为
    /// 1 / 2 / 4）。
    ///
    /// 编码 / 解码端据此读写前缀字段：`Demand` 区间、`to_be_bytes()[..PREFIX_LEN]`
    /// 等均以字节为单位。
    fn PREFIX_LEN() -> usize {
        (<Self::Prefix as funty::Integral>::BITS / 8) as usize
    }

    /// 前缀可表达的数据长度值个数，即 `1 << BITS`。
    ///
    /// * [`U8Prefix`]：256（单分块最多 255 字节数据，另 0 表示 EOF）；
    /// * [`U16Prefix`]：65536（默认）；
    /// * [`U32Prefix`]：2^32。
    fn PREFIX_MAX() -> usize {
        1usize << <Self::Prefix as funty::Integral>::BITS
    }
}

/// 32 位长度前缀：单分块可携带 `0 ..= 2^32 - 1` 字节数据。
///
/// # Examples
///
/// ```
/// use abs_buff_utils::multipart::prefix::{TrMultipartPrefix, U32Prefix};
///
/// assert_eq!(U32Prefix::PREFIX_MAX(), 1usize << 32);
/// ```
pub struct U32Prefix;

/// 16 位长度前缀（默认）：单分块可携带 `0 ..= 65535` 字节数据。
///
/// # Examples
///
/// ```
/// use abs_buff_utils::multipart::prefix::{TrMultipartPrefix, U16Prefix};
///
/// assert_eq!(U16Prefix::PREFIX_MAX(), 1usize << 16);
/// ```
pub struct U16Prefix;

/// 8 位长度前缀：单分块可携带 `0 ..= 255` 字节数据。
///
/// # Examples
///
/// ```
/// use abs_buff_utils::multipart::prefix::{TrMultipartPrefix, U8Prefix};
///
/// assert_eq!(U8Prefix::PREFIX_MAX(), 256);
/// ```
pub struct U8Prefix;

impl TrMultipartPrefix for U32Prefix {
    type Prefix = u32;
}
impl TrMultipartPrefix for U16Prefix {
    type Prefix = u16;
}
impl TrMultipartPrefix for U8Prefix {
    type Prefix = u8;
}

impl sealed_prefix_::SealedPrefix_ for U32Prefix {}
impl sealed_prefix_::SealedPrefix_ for U16Prefix {}
impl sealed_prefix_::SealedPrefix_ for U8Prefix {}
