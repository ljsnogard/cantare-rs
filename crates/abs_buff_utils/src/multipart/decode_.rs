use core::{marker::PhantomData, mem::MaybeUninit};

use abs_buff::{
    Demand, TrBuffRead,
    buffer::{SegmRef, TrBuffSegmRef, TrBuffSegmView},
    error::{ReadErrTag, TrTaggedError},
    gen_may_cancel_future,
    x_deps::{abs_cancel, anylr, funty},
};
use abs_cancel::{TrCancellationToken, TrMayCancel};
use anylr::SomeOf;
use funty::Unsigned;

use super::{
    is_read_eof_,
    prefix::{TrMultipartPrefix, U16Prefix},
};

/// 解码（接收）侧的错误类型。
///
/// 本类型作为 [`MultipartDecode`] 实现 [`TrBuffRead`] 时返回的读错误：
/// 源缓冲出错、**格式错误**（协议被破坏，例如流在没有 0 前缀终止块的情况
/// 下结束、或分块载荷不完整），以及正常 EOF（返回 `Closing` 标记）。
///
/// 注意：本类型目前位于私有模块 `decode_` 中，尚未从
/// [`crate::multipart`] 导出（不构成公开 API）。
pub enum DecodeError<E>
where
    E: core::error::Error,
{
    /// 数据来源方错误。
    Source(E),

    /// 格式错误：流在没有 0 前缀终止块的情况下结束（前缀 / 载荷不完整）。
    Invalid,

    /// 已读到 0 前缀终止块，数据流正常结束。
    Eof,

    /// 调用者主动取消。
    Cancelled,
}

impl<E> core::fmt::Debug for DecodeError<E>
where
    E: core::error::Error + core::fmt::Debug,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            DecodeError::Source(e) => f.debug_tuple("Source").field(e).finish(),
            DecodeError::Invalid => f.write_str("Invalid"),
            DecodeError::Eof => f.write_str("Eof"),
            DecodeError::Cancelled => f.write_str("Cancelled"),
        }
    }
}

impl<E> core::fmt::Display for DecodeError<E>
where
    E: core::error::Error + core::fmt::Debug,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            DecodeError::Source(e) => {
                write!(f, "multipart decode source error: {e}")
            }
            DecodeError::Invalid => {
                f.write_str("multipart decode error: invalid stream")
            }
            DecodeError::Eof => {
                f.write_str("multipart decode error: end of stream")
            }
            DecodeError::Cancelled => {
                f.write_str("multipart decode error: cancelled")
            }
        }
    }
}

impl<E> core::error::Error for DecodeError<E> where
    E: core::error::Error + core::fmt::Debug
{
}

impl<E> TrTaggedError<ReadErrTag> for DecodeError<E>
where
    E: core::error::Error + TrTaggedError<ReadErrTag>,
{
    fn err_tag(&self) -> ReadErrTag {
        match self {
            DecodeError::Source(e) => e.err_tag(),
            DecodeError::Invalid => ReadErrTag::Unknown,
            DecodeError::Eof => ReadErrTag::Closing,
            DecodeError::Cancelled => ReadErrTag::Cancelled,
        }
    }
}

/// 把底层源错误转换为 [`DecodeError`]：若底层已经用 `Cancelled` 标签表达取消，
/// 统一为 [`DecodeError::Cancelled`]。
fn decode_source_err_<E>(err: E) -> DecodeError<E>
where
    E: core::error::Error + TrTaggedError<ReadErrTag>,
{
    if err.err_tag() == ReadErrTag::Cancelled {
        DecodeError::Cancelled
    } else {
        DecodeError::Source(err)
    }
}

/// 一个已经剥掉当前 multipart 长度前缀、只包含载荷的读段。
///
/// 内部包装底层源的 `SegmRef`；`Drop` 时根据底层段实际被消费的字节数，
/// 同步削减 [`MultipartDecode`] 中“当前分块剩余载荷”的计数。
pub struct MultipartDecodeSegm<'f, R, T>
where
    R: TrBuffRead<T> + 'f,
{
    inner_: R::SegmRef<'f>,
    payload_left_: &'f mut usize,
    initial_: usize,
}

impl<'f, R, T> MultipartDecodeSegm<'f, R, T>
where
    R: TrBuffRead<T> + 'f,
{
    fn new(inner: R::SegmRef<'f>, payload_left: &'f mut usize) -> Self {
        let initial = inner.least_count();
        MultipartDecodeSegm {
            inner_: inner,
            payload_left_: payload_left,
            initial_: initial,
        }
    }
}

impl<'f, R, T> Drop for MultipartDecodeSegm<'f, R, T>
where
    R: TrBuffRead<T> + 'f,
{
    fn drop(&mut self) {
        let consumed = self.initial_ - self.inner_.least_count();
        *self.payload_left_ -= consumed;
    }
}

impl<'f, R, T> TrBuffSegmView for MultipartDecodeSegm<'f, R, T>
where
    R: TrBuffRead<T> + 'f,
{
    type SlicesIter<'g>
        = <R::SegmRef<'f> as TrBuffSegmView>::SlicesIter<'g>
    where
        Self: 'g,
        R::SegmRef<'f>: 'g;

    type Item = T;

    fn is_empty(&self) -> bool {
        self.inner_.is_empty()
    }

    fn least_count(&self) -> usize {
        self.inner_.least_count()
    }

    fn iter_slices(&self) -> Self::SlicesIter<'_> {
        self.inner_.iter_slices()
    }
}

impl<'f, R, T> TrBuffSegmRef<'f, T> for MultipartDecodeSegm<'f, R, T>
where
    R: TrBuffRead<T> + 'f,
{
    type Reclaimer<'g>
        = <R::SegmRef<'f> as TrBuffSegmRef<'f, T>>::Reclaimer<'g>
    where
        Self: 'g,
        R::SegmRef<'f>: 'g;

    type TakeSegmRef<'g>
        = <R::SegmRef<'f> as TrBuffSegmRef<'f, T>>::TakeSegmRef<'g>
    where
        Self: 'g,
        R::SegmRef<'f>: 'g;

    fn take_segm_ref<'g>(
        &'g mut self,
        demand: &Demand<usize>,
    ) -> Self::TakeSegmRef<'g> {
        self.inner_.take_segm_ref(demand)
    }

    fn as_segm_ref<'g>(&'g mut self) -> SegmRef<'g, T, Self::Reclaimer<'g>> {
        self.inner_.as_segm_ref()
    }
}

/// 识别 multipart 协议、把带长度前缀的分块流作为**无前缀的载荷读流**暴露。
///
/// 与旧版“内部搬运到目标缓冲”不同，本类型只包裹源缓冲并保存必要的分块状态；
/// 调用者直接通过 [`TrBuffRead`] 读取到的每一段都是剥掉长度前缀后的载荷，
/// 可自行搬运或解读。遇到 0 长度前缀（EOF）后，`read_async` 返回
/// [`DecodeError::Eof`]（标记为 `Closing`）。
///
/// 当 `T` 不是 `u8` 时，前缀仍占用 `PREFIX_LEN` **个** `T` 元素；每个元素按
/// 无符号整数解析为该“字节”的值。因此实际读取实现要求
/// `T: abs_buff::x_deps::funty::Unsigned`。
pub struct MultipartDecode<'a, R, T = u8, P = U16Prefix>
where
    R: TrBuffRead<T>,
    P: TrMultipartPrefix,
{
    source_: &'a mut R,
    /// 当前分块还剩多少载荷字节未暴露；为 0 时下一次读取会先解析下一个长度前缀。
    payload_left_: usize,
    /// 是否已读到 0 长度前缀终止块（正常 EOF）。
    eof_: bool,
    /// 每次 `read_async` 时用于限制底层源“最多读到当前分块末尾”的临时需求。
    ///
    /// 之所以保存在结构体而不是局部变量，是因为底层 `TrBuffRead::read_async`
    /// 会把 `Demand` 的借用与返回段的生命周期绑定；只有放在与 `&mut self`
    /// 同生命周期的字段里，才能把底层段安全地返回给调用者。
    read_demand_: Demand<usize>,
    _use_p_: PhantomData<fn() -> P>,
    _use_t_: PhantomData<fn() -> T>,
}

impl<'a, R, T, P> MultipartDecode<'a, R, T, P>
where
    R: TrBuffRead<T>,
    P: TrMultipartPrefix,
{
    /// 用给定的原始源缓冲构造一个解码读流。
    ///
    /// 构造时不读取也不搬运任何数据；所有前缀裁剪都发生在
    /// [`TrBuffRead::read_async`] 返回的读段中。
    pub const fn new(source: &'a mut R) -> Self {
        MultipartDecode {
            source_: source,
            payload_left_: 0,
            eof_: false,
            read_demand_: Demand::at_least(1),
            _use_p_: PhantomData,
            _use_t_: PhantomData,
        }
    }

    /// 启动一次无前缀载荷读取。
    ///
    /// 该方法是 [`TrBuffRead`] 实现中 `read_async` 的直接入口，单独保留便于
    /// 在无需 trait 方法解析的场景下调用。
    pub fn read_async<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> MultipartDecodeReadAsync<'a, 'f, R, T, P>
    where
        T: Unsigned,
    {
        MultipartDecodeReadAsync(self, demand)
    }
}

// `multipart_decode_read_async_` 是 `MultipartDecode` 的 [`TrBuffRead`] 实现体。
// 宏生成 `MultipartDecodeReadAsync` 作为关联 `ReadAsync<'f>`。
//
// 状态说明：
// * 当前分块未开始时，先向源读取完整的 `PREFIX_LEN` 个前缀元素；0 前缀表示
//   正常 EOF，截断前缀 / 无终止块结束都映射为 `Invalid`；
// * 当前分块开始后，用 `Demand::less_than(payload_left_)` 向源申请一段“最多
//   包含本块剩余载荷”的段，避免把下一分块的前缀误暴露给调用者；
// * 调用者实际消费多少由 [`MultipartDecodeSegm`] 在 drop 时回写。
#[gen_may_cancel_future(MultipartDecodeRead)]
async fn multipart_decode_read_async_<'a, 'f, R, T, P, K>(
    decode: &'f mut MultipartDecode<'a, R, T, P>,
    demand: &'f Demand<usize>,
    cancel: &'f mut K,
) -> SomeOf<MultipartDecodeSegm<'f, R, T>, DecodeError<<R as TrBuffRead<T>>::Err>>
where
    'a: 'f,
    R: TrBuffRead<T>,
    T: Unsigned,
    P: TrMultipartPrefix,
    K: TrCancellationToken + Clone,
{
    if cancel.is_cancelled() {
        return SomeOf::new_right(DecodeError::Cancelled);
    }

    if decode.eof_ {
        return SomeOf::new_right(DecodeError::Eof);
    }

    // 先取出对底层源的借用；后续仅在同一结构体的其它字段上做状态更新，
    // Rust 的字段级借用拆分允许二者同时存活。
    let source = &mut *decode.source_;

    // 1) 开始新分块时，先读并丢弃长度前缀。
    if decode.payload_left_ == 0 {
        let prefix_len = <P as TrMultipartPrefix>::PREFIX_LEN();
        let prefix_demand = Demand::at_least(prefix_len);
        let mut opt_prefix = source
            .read_async(&prefix_demand)
            .may_cancel_with(cancel)
            .await;

        let prefix_segm =
            if let Option::Some(segm) = opt_prefix.as_mut().pick_left() {
                segm
            } else {
                if let Option::Some(err) = opt_prefix.pick_right() {
                    // 没有完整前缀时源就结束 → 格式错误（除非是取消等非 EOF 错误）
                    if is_read_eof_(&err) {
                        return SomeOf::new_right(DecodeError::Invalid);
                    }
                    return SomeOf::new_right(decode_source_err_(err));
                }
                unreachable!()
            };

        let mut prefix_buf = [MaybeUninit::<T>::uninit(); 4];
        let moved = TrBuffSegmRef::move_items_to_buff(
            prefix_segm,
            &mut prefix_buf[..prefix_len],
        );
        if moved < prefix_len {
            // 源关闭时可能返回不足 `at_least` 的部分段；按截断前缀处理。
            return SomeOf::new_right(DecodeError::Invalid);
        }
        let _ = prefix_segm;

        let mut n = 0usize;
        for m_ in &prefix_buf[..prefix_len] {
            // SAFETY: 上一步 `move_items_to_buff` 已写入 `prefix_len` 个槽位，
            // 且 `T: Unsigned` 为无符号整数，无 drop 需求。
            n = (n << 8) | unsafe { m_.assume_init().as_usize() };
        }

        if n == 0 {
            decode.eof_ = true;
            return SomeOf::new_right(DecodeError::Eof);
        }
        decode.payload_left_ = n;
    }

    // 2) 暴露本分块剩余载荷中最多 `payload_left_` 字节。
    // 若调用者要求的下限不超过本块剩余，则把它传给底层源让其等待足够数据；
    // 否则退化为“有多少先给多少”（调用者通常以 `at_least(1)` 循环读取）。
    let payload_left = decode.payload_left_;
    let source_demand = match demand.min().copied() {
        Option::Some(min) if min < payload_left => {
            Demand::between(min, payload_left)
        }
        Option::Some(_) | Option::None => Demand::less_than(payload_left),
    };

    decode.read_demand_ = source_demand;
    let opt_src = source
        .read_async(&decode.read_demand_)
        .may_cancel_with(cancel)
        .await;

    // 先通过借用检查左侧是否成功；成功后再取得所有权，避免依赖 anylr 的
    // 内部枚举形状。
    let has_left = opt_src.as_ref().pick_left().is_some();
    let inner = if has_left {
        opt_src
            .pick_left()
            .expect("刚已确认左侧有值，pick_left 不会为空")
    } else {
        let err = opt_src.pick_right().expect("无左侧时必有右侧错误");
        // 载荷中途源结束 → 分块不完整，格式错误。
        if is_read_eof_(&err) {
            return SomeOf::new_right(DecodeError::Invalid);
        }
        return SomeOf::new_right(decode_source_err_(err));
    };

    // 此时底层源位于本分块载荷的起始处；`source_demand` 已限制最多返回
    // `payload_left` 字节，因此不会把下一分块的前缀带入本读段。
    // 把底层段连同“剩余载荷计数器”的可变借用一起交给调用者。
    let segm = MultipartDecodeSegm::new(inner, &mut decode.payload_left_);
    SomeOf::new_left(segm)
}

impl<'a, R, T, P> TrBuffRead<T> for MultipartDecode<'a, R, T, P>
where
    R: TrBuffRead<T>,
    T: Unsigned,
    P: TrMultipartPrefix,
{
    type ReadAsync<'f>
        = MultipartDecodeReadAsync<'a, 'f, R, T, P>
    where
        Self: 'f;

    type SegmRef<'f>
        = MultipartDecodeSegm<'f, R, T>
    where
        Self: 'f;

    type Err = DecodeError<<R as TrBuffRead<T>>::Err>;

    fn is_drained_closing(&self) -> bool {
        self.eof_
    }

    fn read_async<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> Self::ReadAsync<'f> {
        MultipartDecodeReadAsync(self, demand)
    }
}
