//! multipart 测试共用辅助：缓冲对构建、数据填充、解码读流搬运等。

use core::mem::MaybeUninit;
use std::vec::Vec;

use abs_buff::{
    Demand, TrBuffTryRead, TrBuffTryWrite,
    buffer::{TrBuffSegmMut, TrBuffSegmRef, TrBuffSegmView, TrBufferState},
    x_deps::abs_cancel,
};
use abs_cancel::{NonCancellableToken, TrMayCancel};
use buffex::circular_buff::{CoreAlloc, SpscPair, builder};
use mm_ptr::Owned;

use super::{
    RecvError, MultipartRecv, MultipartEncode, TrMultipartPrefix, U8Prefix,
};

/// 被动 × 被动构建产出的半部对（元素 `u8`、分配器 `CoreAlloc`）。
pub(super) type Pair_ = SpscPair<Owned<[MaybeUninit<u8>], CoreAlloc>>;

/// 异步构建一个容量 `N` 的被动 × 被动半部对。
pub(super) async fn make_pair_async_<const N: usize>() -> Pair_ {
    let mut ready_ = builder::CircularBuffBuilder::<
        Owned<[MaybeUninit<u8>], CoreAlloc>,
    >::with_capacity(N)
    .unwrap()
    .producer_passive()
    .consumer_passive();
    ready_.build_async().await.expect("双端被动构建不可能失败")
}

/// 把 `data_` 全部写入写端（`try_write` 可多次借段；`u8` 位拷贝）。
pub(super) fn feed_<W>(tx_: &mut W, data_: &[u8])
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
pub(super) fn fill_segm_<'x, S>(segm_: &mut S, data_: &[u8])
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
pub(super) fn drain_all_<R>(rx_: &mut R) -> Vec<u8>
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

/// 构造 `U8Prefix` 的编码器（`new` 的 `P` 无法从参数推断，需显式指定）。
pub(super) fn new_enc8_<'a, R, W>(
    src_: &'a mut R,
    tgt_: &'a mut W,
) -> MultipartEncode<'a, R, W, u8, U8Prefix>
where
    R: abs_buff::TrBuffRead<u8> + TrBufferState,
    W: abs_buff::TrBuffWrite<u8>,
{
    MultipartEncode::new(src_, tgt_)
}

/// 构造 `U8Prefix` 的解码读流（同上）。
pub(super) fn new_dec8_<'a, R>(
    src: &'a mut R,
) -> MultipartRecv<'a, R, u8, U8Prefix>
where
    R: abs_buff::TrBuffRead<u8>,
{
    MultipartRecv::new(src)
}

/// 把 `MultipartDecode` 输出的载荷逐段搬运到写端，直到正常 EOF。
///
/// 这是解码读流的测试端到端驱动：解码器本身不负责搬运，因此测试里由本辅助
/// 函数扮演“调用者自行搬运”的角色。
pub(super) async fn drain_decode_to_writer_async_<'a, 'b, R, W, P>(
    decode: &'b mut MultipartRecv<'a, R, u8, P>,
    out: &'b mut W,
) -> Result<usize, RecvError<<R as abs_buff::TrBuffRead<u8>>::Err>>
where
    'a: 'b,
    R: abs_buff::TrBuffRead<u8>,
    W: abs_buff::TrBuffWrite<u8>,
    P: TrMultipartPrefix,
{
    let mut c = 0usize;
    loop {
        let demand = Demand::at_least(1);
        let read_fut = decode.read_async(&demand).into_future();
        let opt = read_fut.await;
        let has_left = opt.as_ref().pick_left().is_some();
        let mut segm = if has_left {
            opt.pick_left().expect("刚已确认读取成功")
        } else {
            match opt.pick_right() {
                Option::Some(RecvError::Eof) => return Ok(c),
                Option::Some(err) => return Err(err),
                Option::None => unreachable!(),
            }
        };
        while segm.least_count() > 0 {
            let write_demand = Demand::at_least(1);
            let write_fut = out
                .write_async(&write_demand)
                .may_cancel_with(NonCancellableToken::shared_mut());
            let opt_out = write_fut.await;
            let mut out_segm = opt_out.pick_left().expect("输出缓冲应始终可写");
            let mut src_child = segm.as_segm_ref();
            let mut dst_child = out_segm.as_segm_mut();
            let moved = src_child.move_items_to_segm(&mut dst_child);
            assert!(moved > 0, "搬运必须产生进展");
            c += moved;
        }
    }
}
