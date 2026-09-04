//! 编码 → 解码的端到端集成测试。

use std::vec::Vec;

use super::{
    MultipartDecode, MultipartEncode, U16Prefix, drain_all_,
    drain_decode_to_writer_async_, feed_, make_pair_async_, new_dec8_,
    new_enc8_,
};

/// 测试目标：`U8Prefix` 下编码 → 解码完整往返。载荷 600 字节超过单分块上限
/// （255），必须切成多个分块；中间缓冲容量（64）小于单分块，验证解码读流
/// 可让调用者分多段把载荷自行搬出。
/// - 手段：600 字节写入源并关闭写端；编码器与“读解码段 → 写输出缓冲”的
///   辅助 future 用 `futures_lite::future::zip` 并发驱动。
/// - 判定：编码返回 600 载荷字节；解码辅助 future 返回 `Ok(600)`；输出缓冲
///   内容与输入完全一致。
#[compio::test]
async fn roundtrip_u8_prefix_multi_chunk_() {
    let (mut src_tx_, mut src_rx_) = make_pair_async_::<1024>().await;
    let (mut tgt_tx_, mut tgt_rx_) = make_pair_async_::<64>().await;
    let (mut out_tx_, mut out_rx_) = make_pair_async_::<1024>().await;

    let data_: Vec<u8> = (0..600u16).map(|i_| (i_ % 251) as u8).collect();
    feed_(&mut src_tx_, &data_);
    src_tx_.close();

    let mut enc_ = new_enc8_(&mut src_rx_, &mut tgt_tx_);
    let enc_fut_ = enc_.start_async().into_future();
    let mut dec_ = new_dec8_(&mut tgt_rx_);
    let dec_fut_ = drain_decode_to_writer_async_(&mut dec_, &mut out_tx_);

    let (enc_res_, dec_res_) =
        futures_lite::future::zip(enc_fut_, dec_fut_).await;
    assert_eq!(
        enc_res_.pick_left(),
        Option::Some(600),
        "编码应搬完 600 载荷字节"
    );
    assert_eq!(
        dec_res_.as_ref().ok().copied(),
        Option::Some(600),
        "解码读流应还原 600 载荷字节"
    );
    assert_eq!(drain_all_(&mut out_rx_), data_, "输出必须与输入完全一致");
}

/// 测试目标：默认 `U16Prefix`（两字节前缀）下编码 → 解码往返。
/// - 手段：100 字节写入源并关闭；编码 / 解码读流并发驱动；中间缓冲容量（32）
///   小于「前缀 + 载荷」，验证小容量下仍能由调用者逐段搬运。
/// - 判定：编码返回 100，解码辅助 future 返回 `Ok(100)`；输出内容与输入一致。
#[compio::test]
async fn roundtrip_default_u16_prefix_() {
    let (mut src_tx_, mut src_rx_) = make_pair_async_::<128>().await;
    let (mut tgt_tx_, mut tgt_rx_) = make_pair_async_::<32>().await;
    let (mut out_tx_, mut out_rx_) = make_pair_async_::<128>().await;

    let data_: Vec<u8> =
        (0..100u8).map(|i_| (i_ as usize * 7 % 251) as u8).collect();
    feed_(&mut src_tx_, &data_);
    src_tx_.close();

    let mut enc_ = MultipartEncode::<'_, _, _, u8, U16Prefix>::new(
        &mut src_rx_,
        &mut tgt_tx_,
    );
    let enc_fut_ = enc_.start_async().into_future();
    let mut dec_ = MultipartDecode::<'_, _, u8, U16Prefix>::new(&mut tgt_rx_);
    let dec_fut_ = drain_decode_to_writer_async_(&mut dec_, &mut out_tx_);

    let (enc_res_, dec_res_) =
        futures_lite::future::zip(enc_fut_, dec_fut_).await;
    assert_eq!(enc_res_.pick_left(), Option::Some(100));
    assert_eq!(dec_res_.as_ref().ok().copied(), Option::Some(100));
    assert_eq!(drain_all_(&mut out_rx_), data_);
}

/// 测试目标：编码 / 解码读流在数据已经就绪后并发推进，并正常以 0 前缀收尾。
/// - 手段：先把 100 字节写入源并关闭，再并发驱动编码器与解码读流。
/// - 判定：编码返回 100、解码读流返回 `Ok(100)`，输出内容与输入一致。
#[compio::test]
async fn encode_and_decode_drive_concurrently_() {
    let (mut src_tx_, mut src_rx_) = make_pair_async_::<128>().await;
    let (mut tgt_tx_, mut tgt_rx_) = make_pair_async_::<64>().await;
    let (mut out_tx_, mut out_rx_) = make_pair_async_::<128>().await;

    let data_: Vec<u8> = (0..100u8)
        .map(|i_| (i_ as usize * 13 % 251) as u8)
        .collect();
    feed_(&mut src_tx_, &data_);
    src_tx_.close();

    let mut enc_ = MultipartEncode::<'_, _, _, u8, U16Prefix>::new(
        &mut src_rx_,
        &mut tgt_tx_,
    );
    let enc_fut_ = enc_.start_async().into_future();
    let mut dec_ = MultipartDecode::<'_, _, u8, U16Prefix>::new(&mut tgt_rx_);
    let dec_fut_ = drain_decode_to_writer_async_(&mut dec_, &mut out_tx_);

    let (enc_res_, dec_res_) =
        futures_lite::future::zip(enc_fut_, dec_fut_).await;
    assert_eq!(enc_res_.pick_left(), Option::Some(100));
    assert_eq!(dec_res_.as_ref().ok().copied(), Option::Some(100));
    assert_eq!(drain_all_(&mut out_rx_), data_);
}
