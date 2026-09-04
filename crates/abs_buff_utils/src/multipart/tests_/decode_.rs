//! 解码读流（`MultipartDecode`）的独立测试。

use std::{vec, vec::Vec};

use abs_buff::{
    Demand,
    x_deps::abs_cancel::{CancelledToken, TrMayCancel},
};

use super::{
    DecodeError, MultipartDecode, U16Prefix, drain_decode_to_writer_async_,
    feed_, make_pair_async_,
};

/// 测试目标：无 0 前缀终止块的截断流被解码读流判为格式错误。
/// - 手段：手工构造流 `[0x00, 0x05, 1, 2, 3]`（声明 5 字节载荷、实际只有
///   3 字节）写入解码源并关闭写端；把解码读流逐段搬到输出端直到 EOF/错误。
/// - 判定：驱动返回 `DecodeError::Invalid`——载荷中途源结束即分块不完整。
#[compio::test]
async fn decode_invalid_on_truncated_stream_() {
    let (mut tgt_tx_, mut tgt_rx_) = make_pair_async_::<16>().await;
    let (mut out_tx_, mut _out_rx_) = make_pair_async_::<16>().await;

    let stream_: Vec<u8> = vec![0x00, 0x05, 1, 2, 3];
    feed_(&mut tgt_tx_, &stream_);
    tgt_tx_.close();

    let mut dec_ = MultipartDecode::<'_, _, u8, U16Prefix>::new(&mut tgt_rx_);
    let res_ = drain_decode_to_writer_async_(&mut dec_, &mut out_tx_).await;
    assert!(
        matches!(res_.err(), Option::Some(DecodeError::Invalid)),
        "截断流必须报格式错误"
    );
}

/// 测试目标：仅含 0 前缀终止块的空流被解码读流正常结束。
/// - 手段：向解码源写入 `[0x00, 0x00]` 并关闭写端；把解码读流搬到输出端。
/// - 判定：返回 `Ok(0)`，不产生错误。
#[compio::test]
async fn decode_empty_stream_ends_cleanly_() {
    let (mut tgt_tx_, mut tgt_rx_) = make_pair_async_::<16>().await;
    let (mut out_tx_, mut _out_rx_) = make_pair_async_::<16>().await;

    let stream_: Vec<u8> = vec![0x00, 0x00];
    feed_(&mut tgt_tx_, &stream_);
    tgt_tx_.close();

    let mut dec_ = MultipartDecode::<'_, _, u8, U16Prefix>::new(&mut tgt_rx_);
    let res_ = drain_decode_to_writer_async_(&mut dec_, &mut out_tx_).await;
    assert_eq!(res_.ok(), Option::Some(0), "空流应返回 0 载荷字节");
}

/// 测试目标：解码读流的一次读取在已取消的令牌下立即返回取消错误。
/// - 手段：构造 `CancelledToken` 传入 `read_async(...).may_cancel_with` 并
///   在 compio 运行时中 `await`。
/// - 判定：返回 `SomeOf::new_right(DecodeError::Cancelled)`。
#[compio::test]
async fn decode_returns_zero_on_cancel_() {
    let (mut _tgt_tx_, mut tgt_rx_) = make_pair_async_::<64>().await;

    let mut dec_ = MultipartDecode::<'_, _, u8, U16Prefix>::new(&mut tgt_rx_);
    let demand_ = Demand::at_least(1);
    let mut tok_ = CancelledToken::new();
    let res_ = dec_.read_async(&demand_).may_cancel_with(&mut tok_).await;
    assert!(
        matches!(res_.pick_right(), Option::Some(DecodeError::Cancelled)),
        "取消后不应返回数据段"
    );
}
