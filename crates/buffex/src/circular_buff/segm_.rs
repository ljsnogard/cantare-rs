//! 两段式段类型：可写 / 可读区域跨缓冲区末端环绕时，物理上拆成两段连续空间，
//! 逻辑上视为**一段**——这样生产者 / 消费者一次就能拿到跨末端的全部空间，
//! 而不会被"单连续 slice"的表示卡死。
//!
//! 这些类型实现 `abs_buff` 的 `TrBuffSegmMut` / `TrBuffSegmRef`，因此可以
//! 直接用于 `abs_buff` 的管道机制；`as_segm_mut` / `as_segm_ref` 每次交出
//! 当前物理段的子段，父段 offset 在子段 drop 时累计，段整体 drop 时按
//! 已消费量提交给环形核心（并触发对端 hook，见 [`super::core_`]）。

use core::{mem::MaybeUninit, pin::Pin};

use abs_buff::{
    Demand,
    buffer::{SegmMut, SegmReclaim, SegmRef, TrBuffSegmMut, TrBuffSegmRef, TrBuffSegmView},
};

use super::core_::CircCore;

/// 过滤空段（两段式表示的辅助函数）。
fn non_empty_slice<T>(s: &&[T]) -> bool {
    !s.is_empty()
}

// ---------------------------------------------------------------------------
// 物理空间的两段式表示
// ---------------------------------------------------------------------------

/// 写段持有的物理空间：一段连续，或两段连续（跨越缓冲区末端）。
pub(super) enum PiecesMut<'a, T> {
    One(&'a mut [MaybeUninit<T>]),
    Two(&'a mut [MaybeUninit<T>], &'a mut [MaybeUninit<T>]),
}

impl<'a, T> PiecesMut<'a, T> {
    #[inline]
    fn len(&self) -> usize {
        match self {
            PiecesMut::One(a) => a.len(),
            PiecesMut::Two(a, b) => a.len() + b.len(),
        }
    }

    /// 按 `offset` 返回剩余可写空间的两段（不足两段时以空段补齐）。
    fn remaining_mut(&mut self, offset: usize) -> [&mut [MaybeUninit<T>]; 2] {
        match self {
            PiecesMut::One(a) => [&mut a[offset..], &mut []],
            PiecesMut::Two(a, b) => {
                let la = a.len();
                if offset < la {
                    [&mut a[offset..], b]
                } else {
                    [&mut b[offset - la..], &mut []]
                }
            }
        }
    }

    /// `offset` 所在物理段的剩余部分（子段的基础）。
    fn current_mut(&mut self, offset: usize) -> &mut [MaybeUninit<T>] {
        match self {
            PiecesMut::One(a) => &mut a[offset..],
            PiecesMut::Two(a, b) => {
                let la = a.len();
                if offset < la {
                    &mut a[offset..]
                } else {
                    &mut b[offset - la..]
                }
            }
        }
    }

    /// 只读视图版本的 [`PiecesMut::remaining_mut`]。
    fn remaining_ref(&self, offset: usize) -> [&[MaybeUninit<T>]; 2] {
        match self {
            PiecesMut::One(a) => [&a[offset..], &[]],
            PiecesMut::Two(a, b) => {
                let la = a.len();
                if offset < la {
                    [&a[offset..], b]
                } else {
                    [&b[offset - la..], &[]]
                }
            }
        }
    }
}

/// 读段持有的物理空间：一段连续，或两段连续。
pub(super) enum PiecesRef<'a, T> {
    One(&'a [T]),
    Two(&'a [T], &'a [T]),
}

impl<'a, T> PiecesRef<'a, T> {
    #[inline]
    fn len(&self) -> usize {
        match self {
            PiecesRef::One(a) => a.len(),
            PiecesRef::Two(a, b) => a.len() + b.len(),
        }
    }

    fn current(&self, offset: usize) -> &[T] {
        match self {
            PiecesRef::One(a) => &a[offset..],
            PiecesRef::Two(a, b) => {
                let la = a.len();
                if offset < la {
                    &a[offset..]
                } else {
                    &b[offset - la..]
                }
            }
        }
    }

    fn remaining_ref(&self, offset: usize) -> [&[T]; 2] {
        match self {
            PiecesRef::One(a) => [&a[offset..], &[]],
            PiecesRef::Two(a, b) => {
                let la = a.len();
                if offset < la {
                    [&a[offset..], b]
                } else {
                    [&b[offset - la..], &[]]
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// 提交器（段 drop 时按已消费量推进位置并触发对端 hook）
// ---------------------------------------------------------------------------

/// 写段提交器：drop 时推进写位置。
pub(super) struct CommitWrite<'s, T> {
    core: &'s CircCore<T>,
}

impl<'s, T> CommitWrite<'s, T> {
    pub(super) fn new(core: &'s CircCore<T>) -> Self {
        CommitWrite { core }
    }

    /// 提交：按已消费量推进写位置并触发消费端 hook。
    ///
    /// 用固有方法而非 `TrReclaim`（其要求 `Self: Send + Sync`，会把
    /// `T: Send + Sync` 的约束传染给整个段类型；固有方法无此要求）。
    pub(super) fn commit(&mut self, amount: usize) {
        self.core.advance_write(amount);
    }
}

/// 读段提交器：drop 时推进读位置。
pub(super) struct CommitRead<'s, T> {
    core: &'s CircCore<T>,
}

impl<'s, T> CommitRead<'s, T> {
    pub(super) fn new(core: &'s CircCore<T>) -> Self {
        CommitRead { core }
    }

    /// 提交：按已消费量推进读位置并触发生产端 hook。
    pub(super) fn commit(&mut self, amount: usize) {
        self.core.advance_read(amount);
    }
}

// ---------------------------------------------------------------------------
// 写段
// ---------------------------------------------------------------------------

/// 环形核心专用写段：两段物理空间视作逻辑上的一段（跨末端环绕时）。
///
/// drop 时按已消费量提交给环形核心（推进写位置并触发消费端 hook）。
pub struct WrSegm<'a, T> {
    pieces: PiecesMut<'a, T>,
    /// 已消费（已提交给核心）的逻辑单元数，跨两段累计。
    offset: usize,
    reclaim: Option<CommitWrite<'a, T>>,
}

impl<'a, T> WrSegm<'a, T> {
    pub(super) fn new(pieces: PiecesMut<'a, T>, reclaim: CommitWrite<'a, T>) -> Self {
        WrSegm {
            pieces,
            offset: 0,
            reclaim: Option::Some(reclaim),
        }
    }

    /// 逻辑上剩余（未消费）的单元数：两段物理空间之和减去已消费量。
    #[inline]
    pub fn least_count(&self) -> usize {
        self.pieces.len() - self.offset
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.least_count() == 0
    }

    /// 剩余可写空间按物理段切出（最多两段）；空段被过滤。
    pub fn iter_slices_mut(&mut self) -> impl Iterator<Item = &mut [MaybeUninit<T>]> {
        self.pieces
            .remaining_mut(self.offset)
            .into_iter()
            .filter(|s| !s.is_empty())
    }

    /// 当前物理段的剩余部分作为一个 abs_buff 子段；子段 drop 时通过
    /// [`SegmReclaim`] 把其已消费量累计到父段的 `offset`。
    pub fn as_segm_mut<'f>(&'f mut self) -> SegmMut<'f, T, SegmReclaim<'f>> {
        let slice = self.pieces.current_mut(self.offset);
        let reclaim = SegmReclaim::new(Pin::new(&mut self.offset));
        SegmMut::new(slice, reclaim)
    }

    /// 按 `Demand` 取当前物理段的一个子段（跨段部分由下一次 take 处理）。
    pub fn take_segm_mut<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> Option<SegmMut<'f, T, SegmReclaim<'f>>> {
        let c = self.least_count();
        if c == 0 {
            return Option::None;
        }
        let available = Demand::less_than(c);
        let agreement = demand.compromise(&available)?;
        let max_len = agreement.max()?;
        let cur = self.pieces.current_mut(self.offset);
        let take = core::cmp::min(*max_len, cur.len());
        let slice = &mut cur[..take];
        let reclaim = SegmReclaim::new(Pin::new(&mut self.offset));
        Option::Some(SegmMut::new(slice, reclaim))
    }
}

impl<'a, T> Drop for WrSegm<'a, T> {
    fn drop(&mut self) {
        let Option::Some(mut r) = self.reclaim.take() else {
            return;
        };
        r.commit(self.offset);
    }
}

impl<'a, T> TrBuffSegmView for WrSegm<'a, T> {
    type SlicesIter<'f>
        = core::iter::Filter<
            core::array::IntoIter<&'f [MaybeUninit<T>], 2>,
            fn(&&'f [MaybeUninit<T>]) -> bool,
        >
    where
        Self: 'f,
        T: 'f;
    type Item = MaybeUninit<T>;

    #[inline]
    fn is_empty(&self) -> bool {
        WrSegm::is_empty(self)
    }

    #[inline]
    fn least_count(&self) -> usize {
        WrSegm::least_count(self)
    }

    fn iter_slices(&self) -> Self::SlicesIter<'_> {
        let filter: fn(&&[MaybeUninit<T>]) -> bool = non_empty_slice;
        self.pieces
            .remaining_ref(self.offset)
            .into_iter()
            .filter(filter)
    }
}

impl<'a, T> TrBuffSegmMut<'a, T> for WrSegm<'a, T> {
    type Reclaimer<'f> = SegmReclaim<'f> where Self: 'f;

    type TakeSegmMut<'f>
        = Option<SegmMut<'f, T, SegmReclaim<'f>>>
    where
        Self: 'f,
        T: 'f;

    #[inline]
    fn as_segm_mut<'f>(&'f mut self) -> SegmMut<'f, T, Self::Reclaimer<'f>> {
        WrSegm::as_segm_mut(self)
    }

    #[inline]
    fn take_segm_mut<'f>(&'f mut self, demand: &Demand<usize>) -> Self::TakeSegmMut<'f> {
        WrSegm::take_segm_mut(self, demand)
    }
}

// ---------------------------------------------------------------------------
// 读段
// ---------------------------------------------------------------------------

/// 环形核心专用读段：两段物理空间视作逻辑上的一段（跨末端环绕时）。
///
/// drop 时按已消费量提交给环形核心（推进读位置并触发生产端 hook）。
pub struct RdSegm<'a, T> {
    pieces: PiecesRef<'a, T>,
    /// 已消费（已提交给核心）的逻辑单元数。
    offset: usize,
    reclaim: Option<CommitRead<'a, T>>,
}

impl<'a, T> RdSegm<'a, T> {
    pub(super) fn new(pieces: PiecesRef<'a, T>, reclaim: CommitRead<'a, T>) -> Self {
        RdSegm {
            pieces,
            offset: 0,
            reclaim: Option::Some(reclaim),
        }
    }

    /// 逻辑上剩余（未消费）的单元数。
    #[inline]
    pub fn least_count(&self) -> usize {
        self.pieces.len() - self.offset
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.least_count() == 0
    }

    /// 剩余可读数据按物理段切出（最多两段）；空段被过滤。
    pub fn iter_slices(&self) -> impl Iterator<Item = &[T]> {
        self.pieces
            .remaining_ref(self.offset)
            .into_iter()
            .filter(|s| !s.is_empty())
    }

    /// 当前物理段的剩余部分作为一个 abs_buff 子段。
    pub fn as_segm_ref<'f>(&'f mut self) -> SegmRef<'f, T, SegmReclaim<'f>> {
        let slice = self.pieces.current(self.offset);
        let reclaim = SegmReclaim::new(Pin::new(&mut self.offset));
        SegmRef::new(slice, reclaim)
    }

    /// 按 `Demand` 取当前物理段的一个子段。
    pub fn take_segm_ref<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> Option<SegmRef<'f, T, SegmReclaim<'f>>> {
        let c = self.least_count();
        if c == 0 {
            return Option::None;
        }
        let available = Demand::less_than(c);
        let agreement = demand.compromise(&available)?;
        let max_len = agreement.max()?;
        let cur = self.pieces.current(self.offset);
        let take = core::cmp::min(*max_len, cur.len());
        let slice = &cur[..take];
        let reclaim = SegmReclaim::new(Pin::new(&mut self.offset));
        Option::Some(SegmRef::new(slice, reclaim))
    }
}

impl<'a, T> Drop for RdSegm<'a, T> {
    fn drop(&mut self) {
        let Option::Some(mut r) = self.reclaim.take() else {
            return;
        };
        r.commit(self.offset);
    }
}

impl<'a, T> TrBuffSegmView for RdSegm<'a, T> {
    type SlicesIter<'f>
        = core::iter::Filter<core::array::IntoIter<&'f [T], 2>, fn(&&'f [T]) -> bool>
    where
        Self: 'f,
        T: 'f;
    type Item = T;

    #[inline]
    fn is_empty(&self) -> bool {
        RdSegm::is_empty(self)
    }

    #[inline]
    fn least_count(&self) -> usize {
        RdSegm::least_count(self)
    }

    fn iter_slices(&self) -> Self::SlicesIter<'_> {
        let filter: fn(&&[T]) -> bool = non_empty_slice;
        self.pieces
            .remaining_ref(self.offset)
            .into_iter()
            .filter(filter)
    }
}

impl<'a, T> TrBuffSegmRef<'a, T> for RdSegm<'a, T> {
    type Reclaimer<'f> = SegmReclaim<'f> where Self: 'f;

    type TakeSegmRef<'f>
        = Option<SegmRef<'f, T, SegmReclaim<'f>>>
    where
        Self: 'f,
        T: 'f;

    #[inline]
    fn as_segm_ref<'f>(&'f mut self) -> SegmRef<'f, T, Self::Reclaimer<'f>> {
        RdSegm::as_segm_ref(self)
    }

    #[inline]
    fn take_segm_ref<'f>(&'f mut self, demand: &Demand<usize>) -> Self::TakeSegmRef<'f> {
        RdSegm::take_segm_ref(self, demand)
    }
}
