//! multipart 模块的单元测试与集成测试。
//!
//! * 前缀参数约定（`PREFIX_LEN` 字节长度、`PREFIX_MAX` 取值范围）；
//! * 编码 → 解码往返（`U8Prefix` 多分块 / 默认 `U16Prefix`；目标缓冲容量
//!   小于单分块时载荷跨多个目标段）；
//! * 线格式校验（分块边界、0 前缀 EOF 终止块）；
//! * 异常流：无终止块的截断流 → `Invalid`；仅 EOF 的空流 → 正常结束；
//! * 取消：编码 / 解码在已取消的令牌下立即返回 0；
//! * 等待：源暂时无数据时编码挂起，数据到达后继续并正常收尾。
//!
//! 测试基础设施（缓冲对、段操作、最小执行器）为 multipart 自备，与
//! `circular_buff::tests_` 的同类辅助互不依赖。异步 future 用最小执行器
//! （`poll_once_` + 交替轮询）驱动，与仓库其余测试一致。

use core::{mem::MaybeUninit, pin::Pin};
use std::{
    pin::pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
    vec,
    vec::Vec,
};

use abs_buff::{
    Demand, TrBuffTryRead, TrBuffTryWrite,
    buffer::{TrBuffSegmMut, TrBuffSegmRef, TrBuffSegmView, TrBufferState},
    x_deps::abs_cancel::{CancelledToken, TrMayCancel},
};
use buffex::circular_buff::{CoreAlloc, SpscPair, builder};
use mm_ptr::Owned;

use super::{
    decode_::{DecodeError, MultipartDecode},
    encode_::MultipartEncode,
    prefix::{TrMultipartPrefix, U8Prefix, U16Prefix, U32Prefix},
};

// ---------------------------------------------------------------------------
// 测试基础设施
// ---------------------------------------------------------------------------

/// 被动 × 被动构建产出的半部对（元素 `u8`、分配器 `CoreAlloc`）。
type Pair_ = SpscPair<Owned<[MaybeUninit<u8>], CoreAlloc>>;

/// 构建一个容量 `N` 的被动 × 被动半部对。
fn make_pair_<const N: usize>() -> Pair_ {
    let mut ready_ = builder::CircularBuffBuilder::<
        Owned<[MaybeUninit<u8>], CoreAlloc>,
    >::with_capacity(N)
    .unwrap()
    .producer_passive()
    .consumer_passive();
    futures_lite::future::block_on(ready_.build_async().into_future()).unwrap()
}

/// 把 `data_` 全部写入写端（`try_write` 可多次借段；`u8` 位拷贝）。
fn feed_<W>(tx_: &mut W, data_: &[u8])
where
    W: TrBuffTryWrite<u8>,
{
    let mut c_ = 0usize;
    while c_ < data_.len() {
        let demand_ = Demand::at_least(1);
        let mut segm_ = TrBuffTryWrite::try_write(tx_, &demand_)
            .pick_left()
            .expect("写端应可写");
        let take_ = core::cmp::min(segm_.least_count(), data_.len() - c_);
        fill_segm_(&mut segm_, &data_[c_..c_ + take_]);
        c_ += take_;
    }
}

/// 把 `data_` 全部写入写段（经 `move_items_from_buff`，`u8` 位拷贝）。
fn fill_segm_<'x, S>(segm_: &mut S, data_: &[u8])
where
    S: TrBuffSegmMut<'x, u8>,
{
    assert!(
        data_.len() <= segm_.least_count(),
        "fill: len({}) > segm({})",
        data_.len(),
        segm_.least_count()
    );
    let mut staging_: Vec<MaybeUninit<u8>> =
        data_.iter().map(|&b_| MaybeUninit::new(b_)).collect();
    // SAFETY: 测试数据为 u8，位拷贝搬入段中，staging 无剩余需 drop 的内容。
    let moved_ = TrBuffSegmMut::move_items_from_buff(segm_, &mut staging_);
    assert_eq!(moved_, data_.len());
}

/// 读端把全部可读数据取出（`try_read` 到 `Drained` 为止）。
fn drain_all_<R>(rx_: &mut R) -> Vec<u8>
where
    R: TrBuffTryRead<u8>,
{
    let mut out_ = Vec::new();
    let demand_ = Demand::at_least(1);
    while let Option::Some(mut segm_) =
        TrBuffTryRead::try_read(rx_, &demand_).pick_left()
    {
        let n_ = segm_.least_count();
        let mut dst_: Vec<MaybeUninit<u8>> = Vec::with_capacity(n_);
        dst_.resize(n_, MaybeUninit::uninit());
        // SAFETY: 测试数据为 u8，`move_items_to_buff` 已写入全部 n 个槽位。
        let moved_ = TrBuffSegmRef::move_items_to_buff(&mut segm_, &mut dst_);
        assert_eq!(moved_, n_);
        for m_ in dst_ {
            out_.push(unsafe { m_.assume_init() });
        }
    }
    out_
}

/// 测试 waker：唤醒时置位一个 `AtomicBool`。
struct TestWaker(Arc<AtomicBool>);

impl TestWaker {
    /// 创建 waker 与其唤醒标志（测试轮询后检查标志以确认被唤醒）。
    fn make_waker_tuple() -> (Waker, Arc<AtomicBool>) {
        let flag_ = Arc::new(AtomicBool::new(false));
        let waker_ = Waker::from(Arc::new(TestWaker(flag_.clone())));
        (waker_, flag_)
    }
}

impl Wake for TestWaker {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::Release);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.store(true, Ordering::Release);
    }
}

/// 轮询一次 future，返回其 `Poll` 结果（配合 [`TestWaker`] 检查唤醒）。
fn poll_once_<F: core::future::Future>(
    fut_: Pin<&mut F>,
    waker_: &Waker,
) -> Poll<F::Output> {
    let mut cx_ = Context::from_waker(waker_);
    fut_.poll(&mut cx_)
}

/// 交替驱动两个 future 直至都完成（模拟并发推进）。
///
/// 一侧 park（等空间 / 等数据）时，另一侧的轮询推进会经 hook 唤醒它，因此
/// 简单交替轮询即可推进；带迭代上限，防止「两侧互相等待」的死锁在测试里
/// 无限挂起。
fn drive_pair_<A, B>(
    mut fa_: Pin<&mut A>,
    mut fb_: Pin<&mut B>,
) -> (A::Output, B::Output)
where
    A: core::future::Future,
    B: core::future::Future,
{
    let (waker_, _flag_) = TestWaker::make_waker_tuple();
    let mut out_a_ = Option::None;
    let mut out_b_ = Option::None;
    let mut guard_ = 0u64;
    #[allow(clippy::collapsible_if)]
    while (out_a_.is_none() || out_b_.is_none()) && guard_ < 10_000_000 {
        if out_a_.is_none() {
            if let Poll::Ready(r_) = poll_once_(fa_.as_mut(), &waker_) {
                out_a_ = Option::Some(r_);
            }
        }
        if out_b_.is_none() {
            if let Poll::Ready(r_) = poll_once_(fb_.as_mut(), &waker_) {
                out_b_ = Option::Some(r_);
            }
        }
        guard_ += 1;
    }
    assert!(
        out_a_.is_some() && out_b_.is_some(),
        "驱动超时：两侧未完成（死锁或实现错误）"
    );
    (out_a_.unwrap(), out_b_.unwrap())
}

/// 构造 `U8Prefix` 的编码器（`new` 的 `P` 无法从参数推断，需显式指定）。
fn new_enc8_<'a, R, W>(
    src_: &'a mut R,
    tgt_: &'a mut W,
) -> MultipartEncode<'a, R, W, u8, U8Prefix>
where
    R: abs_buff::TrBuffRead<u8> + TrBufferState,
    W: abs_buff::TrBuffWrite<u8>,
{
    MultipartEncode::new(src_, tgt_)
}

/// 构造 `U8Prefix` 的解码器（同上）。
fn new_dec8_<'a, R, W>(
    src_: &'a mut R,
    tgt_: &'a mut W,
) -> MultipartDecode<'a, R, W, u8, U8Prefix>
where
    R: abs_buff::TrBuffRead<u8>,
    W: abs_buff::TrBuffWrite<u8>,
{
    MultipartDecode::new(src_, tgt_)
}

// ---------------------------------------------------------------------------
// 前缀参数约定
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// 编码 / 解码往返
// ---------------------------------------------------------------------------

/// 测试目标：`U8Prefix` 下编码 → 解码完整往返。载荷 600 字节超过单分块上限
/// （255），必须切成多个分块；目标缓冲容量（64）小于单分块，载荷必须跨
/// 多个目标段搬运。
/// - 手段：600 字节写入源并关闭写端（EOF 信号）；编码（源 Consumer → 目标
///   Producer）与解码（目标 Consumer → 输出 Producer）用 `drive_pair_` 交替
///   驱动至完成。
/// - 判定：编码 / 解码均返回 `SomeOf::new_left(600)`（载荷字节数）；输出
///   缓冲内容与输入完全一致。
#[test]
fn roundtrip_u8_prefix_multi_chunk_() {
    let (mut src_tx_, mut src_rx_) = make_pair_::<1024>();
    let (mut tgt_tx_, mut tgt_rx_) = make_pair_::<64>();
    let (mut out_tx_, mut out_rx_) = make_pair_::<1024>();

    let data_: Vec<u8> = (0..600u16).map(|i_| (i_ % 251) as u8).collect();
    feed_(&mut src_tx_, &data_);
    src_tx_.close();

    let mut enc_ = new_enc8_(&mut src_rx_, &mut tgt_tx_);
    let enc_fut_ = enc_.start_async().into_future();
    let mut dec_ = new_dec8_(&mut tgt_rx_, &mut out_tx_);
    let dec_fut_ = dec_.start_async().into_future();
    let mut enc_ = pin!(enc_fut_);
    let mut dec_ = pin!(dec_fut_);
    let (enc_res_, dec_res_) = drive_pair_(enc_.as_mut(), dec_.as_mut());

    assert_eq!(
        enc_res_.pick_left(),
        Option::Some(600),
        "编码应搬完 600 载荷字节"
    );
    assert_eq!(
        dec_res_.pick_left(),
        Option::Some(600),
        "解码应还原 600 载荷字节"
    );
    assert_eq!(drain_all_(&mut out_rx_), data_, "输出必须与输入完全一致");
}

/// 测试目标：默认 `U16Prefix`（两字节前缀）下编码 → 解码往返。
/// - 手段：100 字节写入源并关闭；编码 / 解码交替驱动至完成；目标缓冲容量
///   （32）小于「前缀 + 载荷」，验证小容量目标下的多段搬运。
/// - 判定：编码 / 解码返回 100；输出内容与输入一致。
#[test]
fn roundtrip_default_u16_prefix_() {
    let (mut src_tx_, mut src_rx_) = make_pair_::<128>();
    let (mut tgt_tx_, mut tgt_rx_) = make_pair_::<32>();
    let (mut out_tx_, mut out_rx_) = make_pair_::<128>();

    let data_: Vec<u8> =
        (0..100u8).map(|i_| (i_ as usize * 7 % 251) as u8).collect();
    feed_(&mut src_tx_, &data_);
    src_tx_.close();

    let mut enc_ = MultipartEncode::<'_, _, _, u8, U16Prefix>::new(
        &mut src_rx_,
        &mut tgt_tx_,
    );
    let enc_fut_ = enc_.start_async().into_future();
    let mut dec_ = MultipartDecode::<'_, _, _, u8, U16Prefix>::new(
        &mut tgt_rx_,
        &mut out_tx_,
    );
    let dec_fut_ = dec_.start_async().into_future();
    let mut enc_ = pin!(enc_fut_);
    let mut dec_ = pin!(dec_fut_);
    let (enc_res_, dec_res_) = drive_pair_(enc_.as_mut(), dec_.as_mut());

    assert_eq!(enc_res_.pick_left(), Option::Some(100));
    assert_eq!(dec_res_.pick_left(), Option::Some(100));
    assert_eq!(drain_all_(&mut out_rx_), data_);
}

// ---------------------------------------------------------------------------
// 线格式校验（仅编码端）
// ---------------------------------------------------------------------------

/// 测试目标：编码端产出的线格式符合「[len][payload]... [0]」协议。
/// - 手段：600 字节（`U8Prefix`，单分块上限 255）写入源并关闭；目标容量
///   足够大（1024 ≥ 整条流），`block_on` 单独驱动编码至完成；随后把目标
///   缓冲全部取出，按分块解析。
/// - 判定：每个分块的长度前缀与载荷字节数一致、载荷按序还原出原始数据；
///   流以 0 前缀终止且终止块之后无多余字节。
#[test]
fn encode_wire_format_chunks_and_eof_() {
    let (mut src_tx_, mut src_rx_) = make_pair_::<1024>();
    let (mut tgt_tx_, mut tgt_rx_) = make_pair_::<1024>();

    let data_: Vec<u8> = (0..600u16).map(|i_| (i_ % 251) as u8).collect();
    feed_(&mut src_tx_, &data_);
    src_tx_.close();

    let mut enc_ = new_enc8_(&mut src_rx_, &mut tgt_tx_);
    let enc_fut_ = enc_.start_async().into_future();
    let res_ = futures_lite::future::block_on(enc_fut_);
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

// ---------------------------------------------------------------------------
// 异常流与边界
// ---------------------------------------------------------------------------

/// 测试目标：无 0 前缀终止块的截断流被解码端判为格式错误。
/// - 手段：手工构造流 `[0x00, 0x05, 1, 2, 3]`（声明 5 字节载荷、实际只有
///   3 字节）写入解码源并关闭写端；`block_on` 单独驱动解码。
/// - 判定：解码返回 `SomeOf::new_both(count, DecodeError::Invalid)`——载荷
///   中途源结束即分块不完整。
#[test]
fn decode_invalid_on_truncated_stream_() {
    let (mut tgt_tx_, mut tgt_rx_) = make_pair_::<16>();
    let (mut out_tx_, _out_rx_) = make_pair_::<16>();

    let stream_: Vec<u8> = vec![0x00, 0x05, 1, 2, 3];
    feed_(&mut tgt_tx_, &stream_);
    tgt_tx_.close();

    let mut dec_ = MultipartDecode::<'_, _, _, u8, U16Prefix>::new(
        &mut tgt_rx_,
        &mut out_tx_,
    );
    let dec_fut_ = dec_.start_async().into_future();
    let res_ = futures_lite::future::block_on(dec_fut_);
    assert!(
        matches!(res_.pick_right(), Option::Some(DecodeError::Invalid)),
        "截断流必须报格式错误"
    );
}

/// 测试目标：仅含 0 前缀终止块的空流被解码端正常结束（返回 0，无错误）。
/// - 手段：向解码源写入 `[0x00, 0x00]` 并关闭写端；`block_on` 驱动解码。
/// - 判定：解码返回 `SomeOf::new_left(0)`。
#[test]
fn decode_empty_stream_ends_cleanly_() {
    let (mut tgt_tx_, mut tgt_rx_) = make_pair_::<16>();
    let (mut out_tx_, _out_rx_) = make_pair_::<16>();

    let stream_: Vec<u8> = vec![0x00, 0x00];
    feed_(&mut tgt_tx_, &stream_);
    tgt_tx_.close();

    let mut dec_ = MultipartDecode::<'_, _, _, u8, U16Prefix>::new(
        &mut tgt_rx_,
        &mut out_tx_,
    );
    let dec_fut_ = dec_.start_async().into_future();
    let res_ = futures_lite::future::block_on(dec_fut_);
    assert_eq!(res_.pick_left(), Option::Some(0), "空流应返回 0 载荷字节");
}

// ---------------------------------------------------------------------------
// 取消
// ---------------------------------------------------------------------------

/// 测试目标：编码 future 在已取消的令牌下立即返回 0（不搬运、不写前缀）。
/// - 手段：构造 `CancelledToken`（恒已取消），经 `may_cancel_with` 传入
///   编码 future；`block_on` 驱动。
/// - 判定：返回 `SomeOf::new_left(0)`。
#[test]
fn encode_returns_zero_on_cancel_() {
    let (mut _src_tx_, mut src_rx_) = make_pair_::<64>();
    let (mut tgt_tx_, mut _tgt_rx_) = make_pair_::<64>();

    let mut enc_ = MultipartEncode::<'_, _, _, u8, U16Prefix>::new(
        &mut src_rx_,
        &mut tgt_tx_,
    );
    let mut tok_ = CancelledToken::new();
    let fut_ = enc_.start_async().may_cancel_with(&mut tok_);
    let res_ = futures_lite::future::block_on(fut_);
    assert_eq!(res_.pick_left(), Option::Some(0), "取消后不应搬运任何字节");
}

/// 测试目标：解码 future 在已取消的令牌下立即返回 0。
/// - 手段：同编码侧，构造 `CancelledToken` 传入解码 future 并 `block_on`。
/// - 判定：返回 `SomeOf::new_left(0)`。
#[test]
fn decode_returns_zero_on_cancel_() {
    let (mut _tgt_tx_, mut tgt_rx_) = make_pair_::<64>();
    let (mut out_tx_, mut _out_rx_) = make_pair_::<64>();

    let mut dec_ = MultipartDecode::<'_, _, _, u8, U16Prefix>::new(
        &mut tgt_rx_,
        &mut out_tx_,
    );
    let mut tok_ = CancelledToken::new();
    let fut_ = dec_.start_async().may_cancel_with(&mut tok_);
    let res_ = futures_lite::future::block_on(fut_);
    assert_eq!(res_.pick_left(), Option::Some(0), "取消后不应搬运任何字节");
}

// ---------------------------------------------------------------------------
// 等待与恢复
// ---------------------------------------------------------------------------

/// 测试目标：源暂时无数据时编码挂起等待，数据到达后继续搬运并正常以
/// 0 前缀收尾。
/// - 手段：源为空时先轮询编码 / 解码各一次（均应 Pending）；随后写入 100
///   字节并关闭源；再用 `drive_pair_` 驱动至完成。
/// - 判定：初始两次轮询为 Pending；完成后编码 / 解码返回 100，输出内容与
///   输入一致。
#[test]
fn encode_waits_for_data_then_drains_() {
    let (mut src_tx_, mut src_rx_) = make_pair_::<128>();
    let (mut tgt_tx_, mut tgt_rx_) = make_pair_::<64>();
    let (mut out_tx_, mut out_rx_) = make_pair_::<128>();

    let mut enc_ = MultipartEncode::<'_, _, _, u8, U16Prefix>::new(
        &mut src_rx_,
        &mut tgt_tx_,
    );
    let enc_fut_ = enc_.start_async().into_future();
    let mut dec_ = MultipartDecode::<'_, _, _, u8, U16Prefix>::new(
        &mut tgt_rx_,
        &mut out_tx_,
    );
    let dec_fut_ = dec_.start_async().into_future();
    let mut enc_ = pin!(enc_fut_);
    let mut dec_ = pin!(dec_fut_);

    // 初始无数据：编码等源数据、解码等目标数据，都应挂起。
    let (waker_, _flag_) = TestWaker::make_waker_tuple();
    assert!(
        poll_once_(enc_.as_mut(), &waker_).is_pending(),
        "源无数据时编码必须挂起"
    );
    assert!(
        poll_once_(dec_.as_mut(), &waker_).is_pending(),
        "目标无数据时解码必须挂起"
    );

    // 数据到达 + 源关闭（EOF 信号）。
    let data_: Vec<u8> = (0..100u8)
        .map(|i_| (i_ as usize * 13 % 251) as u8)
        .collect();
    feed_(&mut src_tx_, &data_);
    src_tx_.close();

    let (enc_res_, dec_res_) = drive_pair_(enc_.as_mut(), dec_.as_mut());
    assert_eq!(enc_res_.pick_left(), Option::Some(100));
    assert_eq!(dec_res_.pick_left(), Option::Some(100));
    assert_eq!(drain_all_(&mut out_rx_), data_);
}
