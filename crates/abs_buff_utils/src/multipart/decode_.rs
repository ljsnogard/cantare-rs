use core::{marker::PhantomData, mem::MaybeUninit};

use abs_buff::{
    Demand, TrBuffRead, TrBuffWrite,
    buffer::{TrBuffSegmMut, TrBuffSegmRef, TrBuffSegmView},
    gen_may_cancel_future,
    x_deps::{abs_cancel, anylr},
};
use abs_cancel::{TrCancellationToken, TrMayCancel};
use anylr::SomeOf;

use super::{
    is_read_eof_,
    prefix::{TrMultipartPrefix, U16Prefix},
};

/// 解码（接收）侧的错误类型。
///
/// 语义与编码侧错误（`EncodeError`）对称：源缓冲（读侧）出错、目标缓冲
/// （写侧）出错、**格式错误**（协议被破坏，例如流在没有 0 前缀终止块的情况
/// 下结束、或分块载荷不完整），以及调用者经取消令牌主动取消。
///
/// 注意：本类型目前位于私有模块 `decode_` 中，尚未从
/// [`crate::multipart`] 导出（不构成公开 API）。
pub enum DecodeError<R, W, T>
where
    R: TrBuffRead<T>,
    W: TrBuffWrite<T>,
{
    /// 数据来源方错误。
    Source(<R as TrBuffRead<T>>::Err),

    /// 数据接收方错误。
    Target(<W as TrBuffWrite<T>>::Err),

    /// 格式错误：流在没有 0 前缀终止块的情况下结束（前缀 / 载荷不完整）。
    Invalid,

    /// 调用者主动取消。
    Cancelled,
}

/// 识别 multipart 协议、把带长度前缀的分块流还原为原始数据流（接收端）。
///
/// 从源缓冲（`R: TrBuffRead`）读出「前缀 + 载荷」分块，按前缀长度把载荷
/// 搬运到目标缓冲（`W: TrBuffWrite`），遇到 0 长度前缀（EOF）即结束。
/// 解码循环：
///
/// 1. 读出 `PREFIX_LEN` 字节的长度前缀（大端序）并解析为数值 `n`；
/// 2. `n == 0` → EOF，结束；否则把恰好 `n` 字节的载荷搬运到目标缓冲（载荷
///    可跨多个目标段，不受目标缓冲容量限制）；
/// 3. 源缓冲暂时无数据时挂起等待；**流在没有 0 前缀终止块的情况下结束
///    （`Closing` / `Drained`）视为格式错误**（[`DecodeError::Invalid`]）。
///
/// 泛型参数与 [`MultipartEncode`](crate::multipart::MultipartEncode) 对应：
/// `T` 为元素类型（默认 `u8`），`P` 为前缀位宽选项（默认 [`U16Prefix`]，
/// 必须与编码端一致）。
///
/// # 取消
///
/// 返回的异步 future 支持经 `may_cancel_with` 传入取消令牌：取消后立即停止
/// 搬运，并返回已搬运的载荷字节数（`SomeOf::new_left(count)`）。
///
/// 注意：本类型目前位于私有模块 `decode_` 中，尚未从
/// [`crate::multipart`] 导出（不构成公开 API）。
pub struct MultipartDecode<'a, R, W, T = u8, P = U16Prefix>
where
    R: TrBuffRead<T>,
    W: TrBuffWrite<T>,
    P: TrMultipartPrefix,
{
    source_: &'a mut R,
    target_: &'a mut W,
    _use_t_: PhantomData<fn() -> T>,
    _use_p_: PhantomData<fn() -> P>,
}

impl<'a, R, W, T, P> MultipartDecode<'a, R, W, T, P>
where
    R: TrBuffRead<T>,
    W: TrBuffWrite<T>,
    P: TrMultipartPrefix,
{
    /// 用给定的原始源缓冲与目标缓冲构造一个解码器。
    ///
    /// 只保存借用，不进行任何搬运；真正的搬运由
    /// [`MultipartDecode::start_async`] 返回的 future 驱动。
    pub const fn new(source: &'a mut R, target: &'a mut W) -> Self {
        MultipartDecode {
            source_: source,
            target_: target,
            _use_t_: PhantomData,
            _use_p_: PhantomData,
        }
    }
}

impl<'a, R, W, P> MultipartDecode<'a, R, W, u8, P>
where
    R: TrBuffRead<u8>,
    W: TrBuffWrite<u8>,
    P: TrMultipartPrefix,
{
    /// 启动 multipart 解码：返回一个异步 future，驱动「读前缀 → 搬载荷」
    /// 循环，直至遇到 0 长度前缀（EOF）、出错或被取消。
    ///
    /// 返回 [`MultipartDecodeStartAsync`]（`TrMayCancel` future，可
    /// `into_future()` 或 `may_cancel_with(&mut token)` 后 await）。成功 / 取消
    /// 时返回已搬运的**载荷**字节数（不含前缀）；出错时返回
    /// `SomeOf::new_both(count, err)`。
    ///
    /// 与编码端一致，异步解码路径按元素类型 `u8` 实现（前缀按字节解析、
    /// 载荷按字节搬运）。
    pub fn start_async<'f>(
        &'f mut self,
    ) -> MultipartDecodeStartAsync<'a, 'f, R, W, P> {
        MultipartDecodeStartAsync(self)
    }
}

// `multipart_decode_start_async_` 是被 `#[gen_may_cancel_future(MultipartDecodeStart)]`
// 包装的实现体：宏据此生成 `MultipartDecodeStartAsync`（启动器）与
// `MultipartDecodeStartFuture`（真正轮询的 future）。
//
// 设计要点（与 `encode_` 的 `multipart_send_async_` 严格对应）：
//
// * **读前缀**：`read_async(at_least(PREFIX_LEN))` 挂起等待完整前缀；把恰好
//   `PREFIX_LEN` 字节搬出到本地数组（大端解析），段内多余数据留在源中；
// * **EOF**：解析值为 0 → 结束，返回累计载荷字节数；
// * **搬载荷**：目标段每次申请 `at_least(1)`，读侧需求 =
//   `min(目标段剩余, 载荷剩余)`——源里可能还有下一块的前缀，不能搬超本块；
//   载荷中途源结束（`Closing` / `Drained`）→ `Invalid`（分块不完整）；
// * **流无终止块**：读前缀时源已结束 → `Invalid`（编码端正常流程总是先写
//   0 前缀再结束，缺少终止块即协议破坏）。
#[gen_may_cancel_future(MultipartDecodeStart)]
async fn multipart_decode_start_async_<'a, 'f, R, W, P, K>(
    decode: &'f mut MultipartDecode<'a, R, W, u8, P>,
    cancel: &'f mut K,
) -> SomeOf<usize, DecodeError<R, W, u8>>
where
    'a: 'f,
    R: TrBuffRead<u8>,
    W: TrBuffWrite<u8>,
    P: TrMultipartPrefix,
    K: TrCancellationToken + Clone,
{
    let source = &mut *decode.source_;
    let target = &mut *decode.target_;
    let prefix_len: usize = <P as TrMultipartPrefix>::PREFIX_LEN();
    let mut c = 0usize;
    let mut src_tok = cancel.clone();
    let mut tgt_tok = cancel.clone();
    loop {
        if cancel.is_cancelled() {
            break;
        }
        // 1) 读长度前缀（大端序）。前缀段在解析后立即随块作用域结束而释放，
        //    否则其携带的「源借用」会在载荷搬运期间存续（E0499）。
        let n_opt = {
            let prefix_demand = Demand::at_least(prefix_len);
            let mut opt_prefix = source
                .read_async(&prefix_demand)
                .may_cancel_with(&mut src_tok)
                .await;
            match opt_prefix.as_mut().pick_left() {
                Option::Some(src_segm) => {
                    let mut prefix_buf = [MaybeUninit::<u8>::uninit(); 8];
                    let moved = TrBuffSegmRef::move_items_to_buff(
                        src_segm,
                        &mut prefix_buf[..prefix_len],
                    );
                    debug_assert!(moved == prefix_len);
                    let mut n_ = 0usize;
                    for m_ in &prefix_buf[..prefix_len] {
                        // SAFETY: 槽位刚由 `move_items_to_buff` 写入
                        // （moved == prefix_len），且 `u8` 无 drop 需求。
                        n_ = (n_ << 8) | unsafe { m_.assume_init() } as usize;
                    }
                    Option::Some(n_)
                }
                Option::None => {
                    if let Option::Some(err_) = opt_prefix.pick_right() {
                        // 流在没有 0 前缀终止块的情况下结束 → 格式错误
                        if is_read_eof_(&err_) {
                            return SomeOf::new_both(c, DecodeError::Invalid);
                        }
                        return SomeOf::new_both(c, DecodeError::Source(err_));
                    }
                    Option::None
                }
            }
        };
        let Option::Some(n) = n_opt else {
            // 理论不可达：`pick_left` 必有段；防御性继续下一轮。
            unreachable!()
        };
        if n == 0 {
            break; // EOF
        }
        // 2) 搬载荷 n 字节（跨多个目标段）
        let mut cc = 0usize;
        while cc < n {
            let tgt_demand = Demand::at_least(1);
            let mut opt_tgt = target
                .write_async(&tgt_demand)
                .may_cancel_with(&mut tgt_tok)
                .await;
            if let Option::Some(tgt_segm) = opt_tgt.as_mut().pick_left() {
                // 尽量填满目标段（可能跨其内部多个物理段）
                while tgt_segm.least_count() > 0 && cc < n {
                    // 读侧需求 = min(目标段剩余空间, 本块剩余载荷)
                    let src_demand = Demand::less_than(core::cmp::min(
                        tgt_segm.least_count(),
                        n - cc,
                    ));
                    let mut opt_src = source
                        .read_async(&src_demand)
                        .may_cancel_with(&mut src_tok)
                        .await;
                    if let Option::Some(src_segm) =
                        opt_src.as_mut().pick_left()
                    {
                        // 同编码侧：经具体 `SegmRef` / `SegmMut` 的固有
                        // 方法搬移，避免 trait 默认实现把源 / 目标段的
                        // 「数据生命周期」绑定相等（E0597）。
                        let mut src_child_ = src_segm.as_segm_ref();
                        let mut tgt_child_ = tgt_segm.as_segm_mut();
                        let moved =
                            src_child_.move_items_to_segm(&mut tgt_child_);
                        debug_assert!(moved > 0);
                        cc += moved;
                        c += moved;
                        continue;
                    }
                    if let Option::Some(err) = opt_src.pick_right() {
                        // 载荷中途源结束 → 分块不完整，格式错误
                        if is_read_eof_(&err) {
                            return SomeOf::new_both(c, DecodeError::Invalid);
                        }
                        return SomeOf::new_both(c, DecodeError::Source(err));
                    }
                }
            }
            if let Option::Some(err) = opt_tgt.pick_right() {
                return SomeOf::new_both(c, DecodeError::Target(err));
            }
        }
    }
    SomeOf::new_left(c)
}
