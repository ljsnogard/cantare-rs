//! 环形核心：位置状态机 + 两个端（生产端 / 消费端）+ 泵状态。
//!
//! # 设计
//!
//! 与 [`crate::ring_buffer`] 相同的思路：读写位置（rp / wp）与**全部标志**
//! （关闭 ×2、预留待机 ×2、泵互斥 ×1、待办泵 ×2）打包进**一个**
//! `AtomicUsize`，单次原子加载即可看到全部状态，每次状态迁移是一个自旋
//! compare-exchange 循环；环形满 / 空用经典的单空槽方案区分（始终保留一个
//! 槽不用）：
//!
//! * `data = (wp - rp) mod cap`
//! * `free = cap - 1 - data`
//!
//! 状态字高 8 位全部用于标志（无额外 `AtomicBool`）：`TX_CLOSED` / `RX_CLOSED`
//! （关闭）、`TX_STNDBY` / `RX_STNDBY`（预留）、`PUMPING`（泵互斥）、
//! `INPUT_PENDING` / `OUTPUT_PENDING`（待办泵）。位置更新（`update_state`）
//! 保留这些位；标志操作（`set_flag` / `clear_flag` / `try_enter_pump` /
//! `exit_pump`）经 CAS 原子地读写，与位置更新共享同一把原子。
//!
//! 与 `ring_buffer` 的最大不同：核心**泛型于端类型**（`CircCore<P, C, T, A>`，
//! `P` / `C` 见 `circ_buff_`），两端以**具体类型**存放在核心中——
//! 主动端的设备因此无需类型擦除；缓冲由核心**拥有**（[`mm_ptr::Owned`]，
//! 分配器 `A`，默认 `CoreAlloc`）。
//!
//! # 事件分发与同步泵（不 spawn）
//!
//! 状态提交（写入 / 读取推进、关闭）后，核心向对端触发事件
//! （[`super::abs_comp_`] 的 `ProducerHookEvent` / `ConsumerHookEvent`），
//! **先问对端 `check(event)` 是否对当前数据量感兴趣**，感兴趣才行动：
//!
//! * **被动端**：唤醒等待者——`signal` 核心持有的唤醒槽位（[`WakeSlot`]）。
//!   槽位是原子指针，无需锁；等待者被唤醒后重查条件（spurious 唤醒无害）；
//! * **主动端**：置「待办泵」标志并调用 `drive()`——泵循环在 `drive` 内
//!   构造两段式段、调用端类型的 `react_async`（设备搬数据），并把它返回的
//!   future 用 `Waker::noop()` 同步轮询到完成（等价于微型 `block_on`）。
//!   泵自身的段 drop 提交（`advance_*`）会再次触发对端事件，为避免
//!   「输入泵 → 输出泵 → 输入泵 → …」的无界递归，泵采用**待办标志 + 单层
//!   `drive()` 循环**收敛（`check` 裁决「是否值得驱动」，`drive` 机制负责
//!   收敛，两者缺一不可）。
//!
//! # 为什么不需要端锁（STNDBY 位作 armed 协议）
//!
//! 端类型只被**泵**可变访问（`&mut P` / `&mut C`，见 [`CircCore::producer_mut`] /
//! [`CircCore::consumer_mut`]），而泵只运行在 `drive()` 内；`drive()` 以
//! `PUMPING` 位的 test-and-set（[`CircCore::try_enter_pump`]）作**跨线程互斥**
//! （并发线程获取失败直接返回，待办标志由最外层循环处理）。因此对端类型的
//! 可变访问天然串行，无需再加锁。
//!
//! **被动端的 `STNDBY` armed 协议**（`TX_STNDBY` / `RX_STNDBY` 位）：等待者
//! park 时先写 demand（端类型普通字段）、再 CAS 置 armed 位（[`CircCore::arm_producer`] /
//! [`CircCore::arm_consumer`]）；fire 侧从状态字 **Acquire 读**到 armed 位才
//! 访问 demand（[`CircCore::fire_producer`] / [`CircCore::fire_consumer`]），与该
//! CAS 建立 happens-before——demand 无需原子。被动端三态：无等待者（STNDBY=0，
//! fire 不行动）/ 正在登记（有 demand、STNDBY 仍 0，fire 不行动，等待者注册后
//! 重查）/ 等待中（STNDBY=1，接受 fire）。等待者完成 / drop 时先清位再清
//! demand（[`CircCore::unpark_producer`] / [`CircCore::unpark_consumer`]），armed
//! 期间 demand 恒有效。
//!
//! 被动唤醒的另一半不变量：**任何「重新就位」的路径都必须重查状态**——等待
//! future 注册 waker 后会重查条件（见 `spsc_` 的 `Park`），关闭标志
//! 也包含在重查条件中（`producer_ready` / `consumer_ready`）。因此事件即使
//! 丢失 / 被取代，等待者也不会永久挂起。
//!
//! # 线程安全（只使用原子）
//!
//! 全部共享状态是原子（状态字、唤醒槽位）。缓冲内存与主动设备经
//! 内部可变性（裸指针 / `UnsafeCell`）访问，其正确性由以下**调用者义务**
//! 保证（与 `ring_buffer` 的 SPSC 约定一致）：
//!
//! * 至多一个生产线程、一个消费线程；
//! * 主动泵只在其触发线程上执行（`drive()` 的 `PUMPING` 标志保证互斥）；
//! * 活段（写段 / 读段）与泵的操作不重叠。
//!
//! 基于这些约定，[`CircCore`] 在端类型满足 `Send + Sync` 时实现 `Send + Sync`
//! （见文件末尾的安全说明）。

use core::{
    borrow::{Borrow, BorrowMut},
    cell::UnsafeCell,
    future::Future,
    marker::PhantomData,
    mem::MaybeUninit,
    pin::pin,
    ptr,
    slice,
    sync::atomic::{AtomicPtr, AtomicUsize, Ordering},
    task::{Context, Poll, Waker},
};

use abs_buff::Demand;
use atomic_sync::x_deps::atomex::{AtomicFlags, CmpxchResult};
use mm_ptr::{
    Owned,
    x_deps::abs_mm::mem_alloc::{CoreAlloc, TrMalloc},
};

use super::{
    abs_comp_::{
        ConsumerHookEvent, ProducerHookEvent,
        TrCircBuffCore, TrConsumer, TrProducer,
    },
    error_::{RxError, TxError},
    reclaim_::{ReaderReclaim, ReclSliceMut, ReclSliceRef, WriterReclaim},
};

// ---------------------------------------------------------------------------
// 状态字布局
// ---------------------------------------------------------------------------

/// 保留高8位作为状态字（4 位已用：关闭 + 待机；4 位留给泵标志，恰好放满）。
const RSV_BITS: u32 = 8;

/// 生产者（写端）已关闭。
const TX_CLOSED: usize = 1usize << (usize::BITS - 1);
/// 消费者（读端）已关闭。
const RX_CLOSED: usize = 1usize << (usize::BITS - 2);
/// 生产端等待唤醒。被动模式下 demand 有值
const TX_STNDBY: usize = 1usize << (usize::BITS - 3);
/// 消费端等待唤醒。
const RX_STNDBY: usize = 1usize << (usize::BITS - 4);
/// 写入端已跨段标志，即此时 wp <= rp 是合法状态
const REVERSION: usize = 1usize << (usize::BITS - 5);
/// 待办输入泵标志。
const INPUT_PENDING: usize = 1usize << (usize::BITS - 6);
/// 待办输出泵标志。
const OUTPUT_PENDING: usize = 1usize << (usize::BITS - 7);

/// 状态字全部标志的掩码（位置更新（`update_state`）保留这些位）。
const FLAG_MASK: usize = TX_CLOSED
    | RX_CLOSED
    | TX_STNDBY
    | RX_STNDBY
    | REVERSION
    | INPUT_PENDING
    | OUTPUT_PENDING;
/// 每个位置占用的位数（两个位置共享低位，两个标志占高位）。
const POS_BITS: u32 = (usize::BITS - RSV_BITS) / 2;
/// 位置掩码。
const POS_MASK: usize = (1usize << POS_BITS) - 1;

pub(super) const MIN_CAPACITY: usize = 2;
/// 环形缓冲的最大容量（与 `ring_buffer` 的 `MAX_CAPACITY` 同量级）。
pub(super) const MAX_CAPACITY: usize = POS_MASK;

struct IoPos {
    /// 与 REVERSION flag 含义一致
    pub rv: bool,
    /// 读者位置
    pub rp: usize,
    /// 写这位置
    pub wp: usize,
    /// circular buff 的容量
    capacity_: usize,
}

impl IoPos {
    pub fn unpack(state: usize, cap: usize) -> Self {
        let rp = state & POS_MASK;
        let wp = (state >> POS_BITS) & POS_MASK;
        let rv = has_flag(state, REVERSION);
        IoPos { rv, rp, wp, capacity_: cap }
    }
    #[inline]
    pub fn data_size(&self) -> usize {
        (self.wp + self.capacity_ - self.rp) % self.capacity_
    }
    #[inline]
    pub fn free_size(&self) -> usize {
        self.capacity_ - self.data_size()
    }
    pub fn pack(&self) -> usize {
        let s = self.rp | (self.wp << POS_BITS);
        if self.rv { s | REVERSION } else { s & !REVERSION }
    }
    /// 推进写者位置，如果写者位置越过缓冲区物理末端，会设置 REVERSION flag。
    /// 返回值可以直接写在 atm_stat_。
    pub fn advance_wp(&self, amount: usize) -> usize {
        debug_assert!(amount <= self.free_size());
        todo!()
    }
    /// 推进读者位置，如果读者位置越过缓冲区物理末端，会清除 REVERSION flag。
    /// 返回值可以直接写在 atm_stat_。
    pub fn advance_rp(&self, amount: usize) -> usize {
        debug_assert!(amount <= self.data_size());
        todo!()
    }
}

#[inline]
fn has_flag(state: usize, flag: usize) -> bool {
    state & flag != 0
}

// ---------------------------------------------------------------------------
// 环形核心
// ---------------------------------------------------------------------------

/// 环形核心：一个原子状态字 + 自有缓冲 + 两个端 + 泵状态。
/// 必须保证 Send + Sync。
///
/// **公开可见性说明**：本类型被半部（`spsc_::Producer` /
/// `spsc_::Consumer`）借出的段类型指名（`WriterReclaim<CircCore>`），
/// 因此必须 `pub`；它不是稳定的公共 API，通常只作为段类型的泛型参数出现。
///
/// # 泛型参数
///
/// * `P`——生产端类型：被动 `BuffProducer`
///   / 主动 `DeviceProducer`，实现
///   [`TrProducer`](super::abs_comp_::TrProducer)；
/// * `C`——消费端类型：被动 `BuffConsumer`
///   / 主动 `DeviceConsumer`，实现
///   [`TrConsumer`](super::abs_comp_::TrConsumer)；
/// * `T`——元素类型（默认 `u8`）；
/// * `A`——分配器（默认 `CoreAlloc`）：用于分配自有缓冲（[`Owned`]）。
///
/// 泛型于端类型是本设计**刻意为之**：主动端的设备以具体类型存放
/// （`producer_: UnsafeCell<P>` / `consumer_: UnsafeCell<C>`），**不做类型擦除**
/// （`TrInput` / `TrOutput` 带泛型关联类型、不能直接 `dyn`）。
///
/// # 职责
///
/// * **状态机**：`rp` / `wp` 与全部标志（关闭、待机、泵互斥、待办泵）打包进
///   一个 `AtomicUsize`（见本文件顶部的布局常量），单次原子加载看到全部状态，
///   每次迁移是一个自旋 compare-exchange 循环；
/// * **区域借出**：`try_write_at` / `try_read_at` 尊重 `Demand` 的 `[min, max]`
///   区间，把可写 / 可读区借出为两段式段（[`super::reclaim_`]），段 drop 时
///   按已消费量提交回本核心（经 [`TrCircBuffCore`](super::abs_comp_::TrCircBuffCore)）；
/// * **事件分发**：提交路径上向对端触发事件——被动端 `signal` 唤醒槽位，
///   主动端置待办泵标志 + `drive()`；
/// * **同步泵**：`drive()` 内构造段、调用端类型 `react_async`、以
///   [`block_on`] 轮询到完成；泵自身的段 drop 提交再次触发对端事件，由
///   「待办标志 + 单层 `drive()` 循环 + 重入保护（`PUMPING` 位）」收敛。
pub struct CircCore<P, C, B, T = u8>
where
    P: TrProducer<Data = T>,
    C: TrConsumer<Data = T>,
    B: BorrowMut<[MaybeUninit<T>]>,
{
    buffer_: B,
    /// `rp`（低 `POS_BITS` 位）| `wp`（次 `POS_BITS` 位）| 全部标志（高位：
    /// 关闭 ×2、待机 ×2、泵互斥 ×1、待办泵 ×2）。
    atm_stat_: AtomicFlags<usize>,
    /// 环形缓冲（拥有）：统一 `[MaybeUninit<T>]` 视图。基址经裸指针访问，
    /// `Owned` 只负责所有权与生命周期。
    producer_: UnsafeCell<P>,
    consumer_: UnsafeCell<C>,
    _unuse_t_: PhantomData<fn() -> T>,
}

impl<P, C, B, T> CircCore<P, C, B, T>
where
    P: Send + Sync + TrProducer<Data = T>,
    C: Send + Sync + TrConsumer<Data = T>,
    B: Send + Sync + BorrowMut<[MaybeUninit<T>]>,
    T: Send + Sync,
{
    /// 构造核心：分配缓冲、状态归零、装入两端。
    ///
    /// `capacity` 必须已在 [`MIN_CAPACITY`] ..= [`MAX_CAPACITY`] 之间（builder
    /// 保证）。
    pub(super) fn new(
        producer: P,
        consumer: C,
        buffer: B,
    ) -> Self {
        CircCore {
            buffer_: buffer,
            atm_stat_: AtomicFlags::new(AtomicUsize::new(0usize)),
            producer_: UnsafeCell::new(producer),
            consumer_: UnsafeCell::new(consumer),
            _unuse_t_: PhantomData,
        }
    }

    // ------------------------------------------------------------------
    // 端类型访问（UnsafeCell；安全论证见模块文档）
    // ------------------------------------------------------------------

    /// 生产端共享访问（读 `is_passive` 等）。
    ///
    /// # Safety
    ///
    /// `&P` 读取与泵的 `&mut P`（[`CircCore::producer_mut`]）不会并发：
    /// `&mut P` 只发生在 `drive()` 的 `pump_input` 内（`PUMPING` 标志互斥），
    /// 而所有 `&P` 读取（`fire_producer` / `start` / 半部 guard）都在该侧的
    /// 泵线程上（SPSC 单线程纪律 + 构建完成后半部才可用），与泵同线程串行。
    #[inline]
    fn producer_ref(&self) -> &P {
        unsafe { &*self.producer_.get() }
    }

    /// 消费端共享访问。安全论证同 [`CircCore::producer_ref`]。
    #[inline]
    fn consumer_ref(&self) -> &C {
        unsafe { &*self.consumer_.get() }
    }

    // ------------------------------------------------------------------
    // 状态查询
    // ------------------------------------------------------------------

    #[inline]
    pub(super) fn capacity(&self) -> usize {
        self.buffer_.borrow().len()
    }

    /// 当前可读数据量。
    #[inline]
    pub(super) fn data_size(&self) -> usize {
        let state = self.atm_stat_.value();
        let pos = IoPos::unpack(state, self.capacity());
        pos.data_size()
    }

    /// 当前可写空间量。
    #[inline]
    pub(super) fn free_size(&self) -> usize {
        let state = self.atm_stat_.value();
        let pos = IoPos::unpack(state, self.capacity());
        pos.free_size()
    }

    #[inline]
    pub(super) fn is_tx_closed(&self) -> bool {
        has_flag(self.atm_stat_.value(), TX_CLOSED)
    }

    #[inline]
    pub(super) fn is_rx_closed(&self) -> bool {
        has_flag(self.atm_stat_.value(), RX_CLOSED)
    }

    #[inline]
    pub(super) fn producer_is_passive(&self) -> bool {
        self.producer_ref().is_passive()
    }

    #[inline]
    pub(super) fn consumer_is_passive(&self) -> bool {
        self.consumer_ref().is_passive()
    }

    // /// 被动生产端的唤醒槽位（等待写者注册用；主动端无等待者）。
    // pub(super) fn producer_wake_slot(&self) -> &WakeSlot {
    //     &self.producer_wake_
    // }

    // /// 被动消费端的唤醒槽位（等待读者注册用；主动端无等待者）。
    // pub(super) fn consumer_wake_slot(&self) -> &WakeSlot {
    //     &self.consumer_wake_
    // }

    // ------------------------------------------------------------------
    // 区域借出（尊重 Demand 语义）
    // ------------------------------------------------------------------

    /// 借出可写区，返回 `(start, take)`。
    ///
    /// 尊重 `Demand` 的 `[min, max]` 区间：**可写空间不足下限时不返回**（返回
    /// `Stuffed`），满足时最多借出 `max`。区域可能跨末端环绕（由段类型表达）。
    pub(super) fn try_write_at(
        &self,
        demand: &Demand<usize>,
    ) -> Result<(usize, usize), TxError<usize>> {
        let min_len = demand.min().copied().unwrap_or(0);
        let max_len = demand.max().copied().unwrap_or(usize::MAX);
        let state = self.atm_stat_.value();
        let cap = self.capacity();
        let pos = IoPos::unpack(state, cap);
        let free = pos.free_size();
        if free == 0 || free < min_len {
            if has_flag(state, TX_CLOSED) {
                return Err(TxError::Closing);
            }
            return Err(TxError::Stuffed(pos.wp));
        }
        let take = core::cmp::min(max_len, free);
        debug_assert!(take > 0 && take >= min_len);
        Ok((pos.wp, take))
    }

    /// 借出可读区，返回 `(start, take)`。
    ///
    /// 尊重 `Demand` 的 `[min, max]` 区间：可读数据不足下限且未关闭时**不返回**
    /// （返回 `Drained`）；**EOF 例外**——写端已关闭（不再会有更多数据）时，
    /// 返回现有部分（可能不足下限）；读端已关闭或缓冲区已空时返回 `Closing` /
    /// `Drained`。
    pub(super) fn try_read_at(
        &self,
        demand: &Demand<usize>,
    ) -> Result<(usize, usize), RxError<usize>> {
        let min_len = demand.min().copied().unwrap_or(0);
        let max_len = demand.max().copied().unwrap_or(usize::MAX);
        let state = self.atm_stat_.value();
        let cap = self.capacity();
        let pos = IoPos::unpack(state, cap);
        let ready = pos.data_size();
        if ready == 0 {
            if has_flag(state, TX_CLOSED) || has_flag(state, RX_CLOSED) {
                return Err(RxError::Closing); // EOF：写端已关且读空
            }
            return Err(RxError::Drained(pos.rp));
        }
        if ready < min_len && !has_flag(state, TX_CLOSED) && !has_flag(state, RX_CLOSED) {
            return Err(RxError::Drained(pos.rp)); // 不足下限且未关闭：等待更多
        }
        let take = core::cmp::min(max_len, ready);
        debug_assert!(take > 0);
        Ok((pos.rp, take))
    }

    /// 写者可以继续的条件（供等待 future 的 park 检查）：
    /// 可写空间 ≥ min，或写端已关闭（返回 `Closing` 不再等待）。
    pub(super) fn producer_ready(&self, min: usize) -> bool {
        self.try_write_at(&Demand::at_least(min.max(1))).is_ok()
            || self.is_tx_closed()
    }

    /// 读者可以继续的条件：可读数据 ≥ min（或 EOF 有部分数据），或任一端关闭。
    pub(super) fn consumer_ready(&self, min: usize) -> bool {
        self.try_read_at(&Demand::at_least(min.max(1))).is_ok()
            || self.is_tx_closed()
            || self.is_rx_closed()
    }

    // ------------------------------------------------------------------
    // 段构建（提交目标：本核心，经 TrCircBuffCore）
    // ------------------------------------------------------------------

    /// 构建写段：覆盖 `[start, start+take)`，跨末端时拆成两段物理空间。
    ///
    /// 段 drop 时经 [`WriterReclaim`] 提交回本核心（推进写位置并触发消费端
    /// 事件）。
    pub(super) fn write_segm<'s>(
        &'s self,
        start: usize,
        take: usize,
    ) -> ReclSliceMut<'s, T, WriterReclaim<'s, Self>> {
        // SAFETY: `start`/`take` 来自 `try_write_at`，区域在缓冲内；可写区与
        // 其他活段 / 泵操作不重叠是调用者义务（SPSC）。
        let whole: &'s mut [MaybeUninit<T>] = self.buffer_view_mut();
        let first = core::cmp::min(take, self.capacity() - start);
        let pieces = if first < take {
            let (head, tail) = whole.split_at_mut(start);
            let b = &mut head[..take - first];
            super::reclaim_::SegmSlicesMut::Two(tail, b)
        } else {
            super::reclaim_::SegmSlicesMut::One(&mut whole[start..start + take])
        };
        ReclSliceMut::new(pieces, WriterReclaim::new(self))
    }

    /// 构建读段：覆盖 `[start, start+take)`，跨末端时拆成两段物理空间。
    ///
    /// 段 drop 时经 [`ReaderReclaim`] 提交回本核心（推进读位置并触发生产端
    /// 事件）。
    pub(super) fn read_segm<'s>(
        &'s self,
        start: usize,
        take: usize,
    ) -> ReclSliceRef<'s, T, ReaderReclaim<'s, Self>> {
        // SAFETY: 同 [`CircCore::write_segm`]。
        let base = self.buffer_.borrow().as_ptr().cast::<T>();
        let first = core::cmp::min(take, self.capacity() - start);
        let pieces = if first < take {
            let a = unsafe { slice::from_raw_parts(base.add(start), first) };
            let b = unsafe { slice::from_raw_parts(base, take - first) };
            super::reclaim_::SegmSlicesRef::Two(a, b)
        } else {
            let a = unsafe { slice::from_raw_parts(base.add(start), take) };
            super::reclaim_::SegmSlicesRef::One(a)
        };
        ReclSliceRef::new(pieces, ReaderReclaim::new(self))
    }

    // ------------------------------------------------------------------
    // 状态迁移（提交路径：推进位置 / 关闭，随后触发对端事件）
    // ------------------------------------------------------------------

    /// 写提交：按已消费量推进写位置，触发消费端事件。
    pub(super) fn advance_write(&self, amount: usize) {
        let cap = self.capacity();
        self.update_pos_(|s| {
            let pos = IoPos::unpack(s, cap);
            pos.advance_rp(amount)
        });
        let ev = if self.is_tx_closed() {
            ConsumerHookEvent::ProducerClose(self.data_size())
        } else {
            ConsumerHookEvent::Available(self.data_size())
        };
        self.fire_consumer(ev);
    }

    /// 读提交：按已消费量推进读位置，触发生产端事件。
    pub(super) fn advance_read(&self, amount: usize) {
        let cap = self.capacity();
        self.update_pos_(|s| {
            let pos = IoPos::unpack(s, cap);
            pos.advance_rp(amount)
        });
        let event = if self.is_rx_closed() {
            ProducerHookEvent::ConsumerClose(self.free_size())
        } else {
            ProducerHookEvent::Available(self.free_size())
        };
        self.fire_producer(event);
    }

    /// 关闭写端：不再接受写入，触发消费端事件（`ProducerClose`）。
    pub(super) fn close_tx(&self) {
        self.set_flag_(TX_CLOSED);
        self.fire_consumer(ConsumerHookEvent::ProducerClose(self.data_size()));
    }

    /// 关闭读端：不再读取，触发生产端事件（`ConsumerClose`）。
    pub(super) fn close_rx(&self) {
        self.set_flag_(RX_CLOSED);
        self.fire_producer(ProducerHookEvent::ConsumerClose(self.free_size()));
    }

    fn update_pos_<F>(&self, f: F)
    where
        F: Fn(usize) -> usize,
    {
        let expect = |_| true;
        let desire = f;
        self.atm_stat_
            .try_spin_compare_exchange_weak(expect, desire);
    }

    fn set_flag_(&self, flag: usize) {
        let expect = |s| s & flag == 0;
        let desire = |s| s | flag;
        self.atm_stat_
            .try_spin_compare_exchange_weak(expect, desire);
    }

    // ------------------------------------------------------------------
    // 事件分发与泵
    // ------------------------------------------------------------------

    /// 触发生产端事件（消费端完成读取 / 关闭后）。
    ///
    /// **check 裁决**：先问对端（[`TrProducer::check`]）是否对当前数据量感兴趣——
    /// 被动端仅在等待者 **armed**（`TX_STNDBY=1`）时访问其 demand（按登记的下限
    /// 裁决，不足下限不唤醒）；主动端由设备决定（有可写空间 / 关闭等）。
    /// 感兴趣才行动：
    ///
    /// * 被动端：唤醒等待可写空间的写者（槽位为原子，无锁）；
    /// * 主动端：置待办输入泵标志并 `drive()`（泵循环内对端类型做 `react_async`，
    ///   递归由「待办标志 + 单层 `drive()` 循环 + `PUMPING` 互斥」收敛）。
    ///
    /// # Safety（取 `&mut P`）
    ///
    /// 本方法只由 `advance_read` / `close_rx` 触发，而这两者的调用点（用户读
    /// 提交、[`CircCore::pump_output`] 的提交）都**不持有 `&mut P`**——泵对 P
    /// 的 `&mut` 只在其自身的 `pump_input` 内，而 `pump_input` 的提交触发的是
    /// 对端（`fire_consumer`）。因此 `&mut P` 不与任何活借用重叠。
    ///
    /// # 被动端 demand 的可见性（STNDBY armed 协议）
    ///
    /// 等待者先写 demand（普通字段）、再 CAS 置 `TX_STNDBY`（AcqRel）；本方法
    /// 对状态字的 Acquire 读（`has_flag(…, TX_STNDBY)`）与该 CAS 建立
    /// happens-before，故此后对 demand（普通字段）的读取必为当前等待者的值。
    /// `TX_STNDBY=0` 时（无等待者 / 等待者正在登记）直接返回——等待者注册后
    /// 会重查条件，不会丢唤醒。
    fn fire_producer(&self, event: ProducerHookEvent) {
        todo!()
    }

    /// 触发消费端事件（生产端完成写入 / 关闭后）。与 [`CircCore::fire_producer`]
    /// 对称（`&mut C` 的安全性同理：本方法只由 `advance_write` / `close_tx`
    /// 触发，其调用点不持有 `&mut C`；被动端的 armed 门控同理经
    /// `RX_STNDBY`）。
    fn fire_consumer(&self, event: ConsumerHookEvent) {
        todo!()
    }

    /// 原子地读取并清除一个标志（等价于 `swap(false)` 的原子读-清）。
    ///
    /// CAS 循环保证「读旧值 + 清位」是一个原子操作：成功时返回旧值中该位
    /// 是否置位，其余位（位置、其他标志）不受影响。
    fn clear_flag(&self, flag: usize) -> bool {
        let expect = |s| s & flag == flag;
        let desire = |s| s & !flag;
        let res = self
            .atm_stat_
            .try_spin_compare_exchange_weak(expect, desire);
        res.is_succ()
    }

    /// 整块缓冲的可变视图（内部可变性：由 SPSC 借用纪律保证不与活段重叠）。
    #[allow(clippy::mut_from_ref)]
    fn buffer_view_mut(&self) -> &mut [MaybeUninit<T>] {
        let base = self.buffer_.borrow().as_ptr() as *const _ as *mut MaybeUninit<T>;
        unsafe { slice::from_raw_parts_mut(base, self.capacity()) }
    }
}

// ---------------------------------------------------------------------------
// TrCircBuffCore（段提交接口）
// ---------------------------------------------------------------------------

impl<P, C, B, T> TrCircBuffCore for CircCore<P, C, B, T>
where
    P: Send + Sync + TrProducer<Data = T>,
    C: Send + Sync + TrConsumer<Data = T>,
    B: Send + Sync + BorrowMut<[MaybeUninit<T>]>,
    T: Send + Sync,
{
    type Data = T;

    fn advance_read(&self, amount: usize) {
        CircCore::advance_read(self, amount);
    }

    fn advance_write(&self, amount: usize) {
        CircCore::advance_write(self, amount);
    }
}

// ---------------------------------------------------------------------------
// 线程安全
// ---------------------------------------------------------------------------

// SAFETY: 核心的全部共享状态为原子（状态字、唤醒槽位、泵标志）。缓冲内存
// （`Owned` 的裸指针基址）与两端（`UnsafeCell`）只经内部可变性访问，其
// 正确性由模块文档的 SPSC 调用者义务 + `pumping` 互斥保证：
// * 至多一个生产线程、一个消费线程；
// * 泵只在其触发线程上执行（`drive()` 互斥）；
// * 活段与泵的操作不重叠。
unsafe impl<P, C, B, T> Send for CircCore<P, C, B, T>
where
    P: Send + TrProducer<Data = T>,
    C: Send + TrConsumer<Data = T>,
    B: Send + Sync + BorrowMut<[MaybeUninit<T>]>,
    T: Send,
{}

unsafe impl<P, C, B, T> Sync for CircCore<P, C, B, T>
where
    P: Send + Sync + TrProducer<Data = T>,
    C: Send + Sync + TrConsumer<Data = T>,
    B: Send + Sync + BorrowMut<[MaybeUninit<T>]>,
    T: Send + Sync,
{}

// ---------------------------------------------------------------------------
// 唤醒槽位
// ---------------------------------------------------------------------------

/// 等待者：注册进唤醒槽位的 waker。活在等待 future（或被等待方）内部，
/// 注册期间不得移动，因此由槽位以裸指针引用。
pub(super) struct Waiter {
    pub waker: Option<Waker>,
}

impl Waiter {
    pub const fn new() -> Self {
        Waiter { waker: None }
    }
}

/// 唤醒槽位：同一时刻至多一个等待者（SPSC 保证每侧至多一个等待的 future）。
///
/// 槽位由核心持有（`producer_wake_` / `consumer_wake_`），被动端专属；等待
/// future 以 `&WakeSlot` 注册 / 注销，提交路径 `signal` 唤醒。原子指针操作，
/// 无需锁。
pub(super) struct WakeSlot(AtomicPtr<Waiter>);

impl WakeSlot {
    pub const fn new() -> Self {
        WakeSlot(AtomicPtr::new(ptr::null_mut()))
    }

    /// 把 `w` 注册为当前等待者。若仍有（陈旧的）等待者未注销则自旋等待其
    /// 注销（等待者在完成或 drop 时必然注销，因此自旋必然终止）。
    pub fn register(&self, w: &Waiter) {
        let p = w as *const Waiter as *mut Waiter;
        loop {
            match self.0.compare_exchange_weak(
                ptr::null_mut(),
                p,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(cur) if cur == p => return, // 已由我们注册
                Err(_) => core::hint::spin_loop(),
            }
        }
    }

    /// 若 `w` 仍是当前注册的等待者，则注销之。
    pub fn deregister(&self, w: &Waiter) {
        let p = w as *const Waiter as *mut Waiter;
        let _ = self
            .0
            .compare_exchange(p, ptr::null_mut(), Ordering::AcqRel, Ordering::Acquire);
    }

    /// 唤醒当前注册的等待者（若有），并清空槽位。
    pub fn signal(&self) {
        let p = self.0.swap(ptr::null_mut(), Ordering::AcqRel);
        if !p.is_null() {
            let w = unsafe { &*p };
            if let Some(waker) = w.waker.as_ref() {
                waker.wake_by_ref();
            }
        }
    }
}

