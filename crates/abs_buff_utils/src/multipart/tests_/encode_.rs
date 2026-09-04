//! 编码端（multipart 写协议流）测试。

use std::vec::Vec;

use abs_buff::x_deps::abs_cancel::{CancelledToken, TrMayCancel};

use super::{
    MultipartEncode, U16Prefix, drain_all_, feed_, make_pair_async_, new_enc8_,
};

/// 测试目标：编码端产出的线格式符合「[len][payload]... [0]」协议。
/// - 手段：600 字节（`U8Prefix`，单分块上限 255）写入源并关闭；目标容量
///   足够大（1024 ≥ 整条流），在 compio 运行时直接 `await` 编码 future；
///   随后把目标缓冲全部取出，按分块解析。
/// - 判定：每个分块的长度前缀与载荷字节数一致、载荷按序还原出原始数据；
///   流以 0 前缀终止且终止块之后无多余字节。
#[compio::test]
async fn encode_wire_format_chunks_and_eof_() {
    let (mut src_tx_, mut src_rx_) = make_pair_async_::<1024>().await;
    let (mut tgt_tx_, mut tgt_rx_) = make_pair_async_::<1024>().await;

    let data_: Vec<u8> = (0..600u16).map(|i_| (i_ % 251) as u8).collect();
    feed_(&mut src_tx_, &data_);
    src_tx_.close();

    let mut enc_ = new_enc8_(&mut src_rx_, &mut tgt_tx_);
    let res_ = enc_.start_async().into_future().await;
    assert_eq!(res_.pick_left(), Option::Some(600), "编码应搬完全部载荷");

    let wire_ = drain_all_(&mut tgt_rx_);
    let mut i_ = 0usize;
    let mut recon_: Vec<u8> = Vec::new();
    loop {
        assert!(i_ < wire_.len(), "线格式必须以 0 前缀终止");
        let len_ = wire_[i_] as usize;
        i_ += 1;
        if len_ == 0 {
            break;
        }
        assert!(
            len_ <= 255 && i_ + len_ <= wire_.len(),
            "分块长度越界：len={len_}, i={i_}"
        );
        recon_.extend_from_slice(&wire_[i_..i_ + len_]);
        i_ += len_;
    }
    assert_eq!(i_, wire_.len(), "0 前缀终止块之后不应再有字节");
    assert_eq!(recon_, data_, "分块内容必须按序还原");
}

/// 测试目标：编码 future 在已取消的令牌下立即返回 0（不搬运、不写前缀）。
/// - 手段：构造 `CancelledToken`（恒已取消），经 `may_cancel_with` 传入
///   编码 future，在 compio 运行时中 `await`。
/// - 判定：返回 `SomeOf::new_left(0)`。
#[compio::test]
async fn encode_returns_zero_on_cancel_() {
    let (mut _src_tx_, mut src_rx_) = make_pair_async_::<64>().await;
    let (mut tgt_tx_, mut _tgt_rx_) = make_pair_async_::<64>().await;

    let mut enc_ = MultipartEncode::<'_, _, _, u8, U16Prefix>::new(
        &mut src_rx_,
        &mut tgt_tx_,
    );
    let mut tok_ = CancelledToken::new();
    let res_ = enc_.start_async().may_cancel_with(&mut tok_).await;
    assert_eq!(res_.pick_left(), Option::Some(0), "取消后不应搬运任何字节");
}
