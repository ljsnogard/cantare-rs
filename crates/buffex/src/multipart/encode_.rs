use core::marker::PhantomData;

use abs_buff::{
    Demand, TrBuffRead, TrBuffWrite,
    buffer::{TrAsBufferMut, TrBuffSegmMut, TrBuffSegmRef, TrBuffSegmView},
    gen_may_cancel_future,
    x_deps::{abs_cancel, anylr},
};
use abs_cancel::{TrCancellationToken, TrMayCancel};
use anylr::SomeOf;

use super::{
    is_read_eof_,
    prefix::{TrMultipartPrefix, U16Prefix},
};
use crate::observer::TrObserver;

/// 编码（发送）侧的错误类型。
///
/// 搬运过程中可能出现的错误：源缓冲（读侧）出错、目标缓冲（写侧）出错、
/// 格式错误，以及调用者经取消令牌主动取消。错误通常与「已成功搬运的字节数」
/// 一起以 [`SomeOf`] 的 `Both` 变体返回。
///
/// 注意：本类型目前位于私有模块 `encode_` 中，尚未从
/// [`crate::multipart`] 导出（不构成公开 API）。
pub enum EncodeError<R, W, T>
where
    R: TrBuffRead<T>,
    W: TrBuffWrite<T>,
{
    /// 数据来源方（源缓冲读侧）错误。
    Source(<R as TrBuffRead<T>>::Err),

    /// 数据发送方（目标缓冲写侧）错误。
    Target(<W as TrBuffWrite<T>>::Err),

    /// 格式错误：分块载荷在源端中途耗尽（协议被破坏）。
    Invalid,

    /// 调用者经取消令牌主动取消。
    Cancelled,
}

/// 把长度未知的字节流从源缓冲编码为带长度前缀的分块，写入目标缓冲
/// （发送端）。
///
/// 对应协议：每个分块 = 长度前缀（默认两个字节、大端序）+ 载荷；长度前缀
/// 取 0 表示数据流结束（EOF）。编码循环：
///
/// 1. 读取源缓冲当前数据量（[`TrObserver::data_size`]），封顶到前缀可表达的
///    最大值减一（`PREFIX_MAX - 1`）作为本块载荷长度——0 前缀因此严格只表示
///    EOF；
/// 2. 向目标缓冲写入长度前缀，再把载荷从源缓冲搬到目标缓冲（载荷可跨多个
///    目标段，不受目标缓冲容量限制）；
/// 3. 源缓冲暂时无数据时挂起等待；**源 drained（关闭且无数据）时写入 0 前缀
///    并结束**——调用者通过关闭源（或源自身报告 drained）来表达流结束；
/// 4. 全程可通过取消令牌中断，中断后返回已搬运的载荷字节数。
///
/// 泛型参数：
///
/// * `R`——源缓冲，须同时实现 [`TrBuffRead`] 与 [`TrObserver`]
///   （例如 [`crate::circular_buff::Consumer`]）；
/// * `W`——目标缓冲，实现 [`TrBuffWrite`]
///   （例如 [`crate::circular_buff::Producer`]）；
/// * `T`——元素类型，默认 `u8`；
/// * `P`——前缀位宽选项，默认 [`U16Prefix`]。
///
/// # 取消
///
/// 返回的异步 future 支持经 `may_cancel_with` 传入取消令牌：取消后立即停止
/// 搬运，并返回已搬运的载荷字节数（`SomeOf::new_left(count)`）。
pub struct MultipartEncode<'a, R, W, T = u8, P = U16Prefix>
where
    R: TrBuffRead<T> + TrObserver,
    W: TrBuffWrite<T>,
    P: TrMultipartPrefix,
{
    source_: &'a mut R,
    target_: &'a mut W,
    _use_t_: PhantomData<fn() -> T>,
    _use_p_: PhantomData<fn() -> P>,
}

impl<'a, R, W, T, P> MultipartEncode<'a, R, W, T, P>
where
    R: TrBuffRead<T> + TrObserver,
    W: TrBuffWrite<T>,
    P: TrMultipartPrefix,
{
    /// 用给定的源缓冲与目标缓冲构造一个编码器。
    ///
    /// 只保存借用，不进行任何搬运；真正的搬运由
    /// [`MultipartEncode::start_async`] 返回的 future 驱动。
    pub const fn new(source: &'a mut R, target: &'a mut W) -> Self {
        MultipartEncode {
            source_: source,
            target_: target,
            _use_t_: PhantomData,
            _use_p_: PhantomData,
        }
    }
}

impl<'a, R, W, P> MultipartEncode<'a, R, W, u8, P>
where
    R: TrBuffRead<u8> + TrObserver,
    W: TrBuffWrite<u8>,
    P: TrMultipartPrefix,
{
    /// 启动 multipart 编码：返回一个异步 future，驱动「写前缀 → 搬载荷」
    /// 循环，直至源 drained（写 0 前缀 EOF 后结束）、出错或被取消。
    ///
    /// 返回 [`MultipartEncodeStartAsync`]（`TrMayCancel` future，可
    /// `into_future()` 或 `may_cancel_with(&mut token)` 后 await）。成功 / 取消
    /// 时返回已搬运的**载荷**字节数（不含前缀）；出错时返回
    /// `SomeOf::new_both(count, err)`。
    pub fn start_async<'f>(
        &'f mut self,
    ) -> MultipartEncodeStartAsync<'a, 'f, R, W, P> {
        MultipartEncodeStartAsync(self)
    }
}

// `multipart_send_async_` 是被 `#[gen_may_cancel_future(MultipartEncodeStart)]`
// 包装的实现体：宏据此生成 `MultipartEncodeStartAsync`（启动器，保存参数）
// 与 `MultipartEncodeStartFuture`（真正轮询的 future）。
//
// 设计要点（与 `decode_` 的 `multipart_decode_start_async_` 严格对应）：
//
// * **分块长度**：`min(源当前数据量, PREFIX_MAX - 1)`。封顶保证前缀值落在
//   `1 ..= PREFIX_MAX - 1`，0 前缀因此**只**表示 EOF（避免「源恰好有
//   PREFIX_MAX 整数倍字节时误发 0 前缀」的歧义）；
// * **前缀与载荷分开写**：前缀写进一个独立的目标段，载荷可跨多个目标段
//   搬运——目标缓冲容量可以远小于单个分块，不会因 `at_least(前缀+载荷)`
//   申请过大而活锁；
// * **等待与终止**：源暂时无数据时经 `read_async(at_least(1))` 挂起（返回的
//   段不消费、直接丢弃，只用于等待）；源 drained（`is_drained_closing`，或
//   等待期间读侧报告 Closing / Drained）时写入 0 前缀并结束；
// * **错误**：源读错误 → `Source`；目标写错误 → `Target`；分块载荷在源端
//   中途耗尽 → `Invalid`（协议破坏）；取消 → 返回已搬运字节数（`new_left`）。
//
// 借用结构说明：所有 `Demand` 均为循环内局部值，且任何段都不会跨 `await`
// 存活到下一次迭代（前缀段、载荷的目标段都在各自语句块内 drop），因此不
// 存在「函数级 `'f` 与循环局部 `Demand` 生命周期冲突」的问题（E0597 /
// E0499 已在重构中消除）。
#[gen_may_cancel_future(MultipartEncodeStart)]
async fn multipart_send_async_<'a, 'f, R, W, P, K>(
    encode: &'f mut MultipartEncode<'a, R, W, u8, P>,
    cancel: &'f mut K,
) -> SomeOf<usize, EncodeError<R, W, u8>>
where
    R: TrBuffRead<u8> + TrObserver,
    W: TrBuffWrite<u8>,
    P: TrMultipartPrefix,
    K: TrCancellationToken + Clone,
{
    let source = &mut *encode.source_;
    let target = &mut *encode.target_;
    let prefix_len: usize = <P as TrMultipartPrefix>::PREFIX_LEN();
    let prefix_max: usize = <P as TrMultipartPrefix>::PREFIX_MAX();
    let mut c = 0usize;
    let mut src_tok = cancel.clone();
    let mut tgt_tok = cancel.clone();
    loop {
        if cancel.is_cancelled() {
            break;
        }
        // 源当前可读数据量；为 0 时区分「暂时无数据」与「已 drained（EOF）」
        let avail = source.data_size();
        if avail == 0 {
            if source.is_drained_closing() {
                // EOF：写入 0 长度前缀并结束
                let opt_eof =
                    write_zero_prefix_async_(target, prefix_len, &mut tgt_tok)
                        .await;
                if let Option::Some(err) = opt_eof.pick_right() {
                    return SomeOf::new_both(c, EncodeError::Target(err));
                }
                break;
            }
            // 暂时无数据：挂起等待数据到来（返回的段不消费、直接丢弃）
            let wait_demand = Demand::at_least(1);
            let opt_wait = source
                .read_async(&wait_demand)
                .may_cancel_with(&mut src_tok)
                .await;
            if let Option::Some(err) = opt_wait.pick_right() {
                // 等待期间源关闭 / 耗尽 → 视为 drained，写 EOF 结束
                if is_read_eof_(&err) {
                    let opt_eof = write_zero_prefix_async_(
                        target,
                        prefix_len,
                        &mut tgt_tok,
                    )
                    .await;
                    if let Option::Some(err_) = opt_eof.pick_right() {
                        return SomeOf::new_both(c, EncodeError::Target(err_));
                    }
                    break;
                }
                return SomeOf::new_both(c, EncodeError::Source(err));
            }
            continue;
        }
        // 本块载荷长度：封顶到前缀可表达的最大值 - 1（0 保留给 EOF）
        let data_size = core::cmp::min(avail, prefix_max - 1);
        debug_assert!(data_size > 0);

        // 1) 写长度前缀（大端 `uN` 表示，即 BE usize 的末尾 prefix_len 字节）
        let prefix_demand = Demand::at_least(prefix_len);
        let mut opt_segm = target
            .write_async(&prefix_demand)
            .may_cancel_with(&mut tgt_tok)
            .await;
        if let Option::Some(target_segm) = opt_segm.as_mut().pick_left() {
            // 前缀 = `data_size` 的大端表示（`uN` 的 BE 字节）。注意取
            // `usize::to_be_bytes()` 的**末尾** `prefix_len` 字节（低字节侧）：
            // 高字节侧是全 0，取高字节会把非零长度写成 0（被解码端误判为
            // EOF）。
            let mut be = data_size.to_be_bytes();
            let mut prefix_src =
                &mut be[core::mem::size_of::<usize>() - prefix_len..];
            let moved = target_segm
                .move_items_from_buff(prefix_src.as_mut_slice_uninit());
            debug_assert!(moved == prefix_len);
        }
        if let Option::Some(err_) = opt_segm.pick_right() {
            return SomeOf::new_both(c, EncodeError::Target(err_));
        }

        // 2) 搬载荷：跨多个目标段，直至本块搬满
        let mut cc = 0usize;
        while cc < data_size {
            let write_demand = Demand::at_least(1);
            let mut opt_tgt = target
                .write_async(&write_demand)
                .may_cancel_with(&mut tgt_tok)
                .await;
            if let Option::Some(tgt_segm) = opt_tgt.as_mut().pick_left() {
                // 尽量填满目标段（可能跨其内部多个物理段）
                while tgt_segm.least_count() > 0 && cc < data_size {
                    // 读侧需求 = min(目标段剩余空间, 本块剩余载荷)——源可能
                    // 还有下一块的数据，不能一次搬超本块范围。
                    let read_demand = Demand::less_than(core::cmp::min(
                        tgt_segm.least_count(),
                        data_size - cc,
                    ));
                    let mut opt_src = source
                        .read_async(&read_demand)
                        .may_cancel_with(&mut src_tok)
                        .await;
                    if let Option::Some(src_segm) =
                        opt_src.as_mut().pick_left()
                    {
                        // 经具体 `SegmRef` / `SegmMut` 的固有方法搬移：trait
                        // 默认实现的约束会把源 / 目标段的「数据生命周期」绑定
                        // 相等，而读 / 写两侧各自的 `Demand` 生命周期不同
                        // （会导致 E0597），固有方法不受此约束。
                        let mut src_child = src_segm.as_segm_ref();
                        let mut tgt_child = tgt_segm.as_segm_mut();
                        let moved =
                            src_child.move_items_to_segm(&mut tgt_child);
                        debug_assert!(moved > 0);
                        cc += moved;
                        c += moved;
                        continue;
                    }
                    if let Option::Some(err) = opt_src.pick_right() {
                        // 载荷中途源 drained / 关闭 → 分块不完整，格式错误；
                        // 其余读错误原样返回
                        if is_read_eof_(&err) {
                            return SomeOf::new_both(c, EncodeError::Invalid);
                        }
                        return SomeOf::new_both(c, EncodeError::Source(err));
                    }
                }
            }
            if let Option::Some(err) = opt_tgt.pick_right() {
                return SomeOf::new_both(c, EncodeError::Target(err));
            }
        }
    }
    SomeOf::new_left(c)
}

// `write_zero_prefix_async_`：向目标缓冲写入一个全 0 的长度前缀，作为数据流
// 的 EOF 终止块。目标段写满 / 关闭等错误原样返回（`SomeOf::new_right`）。
#[gen_may_cancel_future(WriteZeroPrefix)]
async fn write_zero_prefix_async_<'f, W, K>(
    target: &'f mut W,
    prefix_len: usize,
    cancel: &'f mut K,
) -> SomeOf<usize, <W as TrBuffWrite<u8>>::Err>
where
    W: TrBuffWrite<u8>,
    K: TrCancellationToken + Clone,
{
    let prefix_demand = Demand::at_least(prefix_len);
    let mut opt_segm = target
        .write_async(&prefix_demand)
        .may_cancel_with(cancel)
        .await;
    if let Option::Some(target_segm) = opt_segm.as_mut().pick_left() {
        let zeros: usize = 0;
        let mut zero_bytes = &mut zeros.to_be_bytes()[..prefix_len];
        let moved = target_segm
            .move_items_from_buff(zero_bytes.as_mut_slice_uninit());
        debug_assert!(moved == prefix_len);
        return SomeOf::new_left(moved);
    }
    if let Option::Some(err) = opt_segm.pick_right() {
        return SomeOf::new_right(err);
    }
    SomeOf::new_left(0usize)
}
