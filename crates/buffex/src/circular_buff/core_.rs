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

use abs_buff::{
    Demand,
    error::{ReadErrTag, WriteErrTag, TrTaggedError, TrErrTag},
    gen_may_cancel_future,
    x_deps::{anylr, abs_cancel},
};
use abs_cancel::{TrCancellationToken, TrMayCancel};
use abs_mm::mem_alloc::{CoreAlloc, TrMalloc};
use abs_sync::ok_or::XtOkOr;

use anylr::SomeOf;
use atomex::{AtomicFlags, CmpxchResult};
use atomic_sync::x_deps::{abs_sync, atomex};
use mm_ptr::{Owned, x_deps::abs_mm,};

use super::{
    abs_comp_::{
        ConsumerHookEvent, ProducerHookEvent, ReceiverReact,
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

/// 待办输入泵标志。
const INPUT_PENDING: usize = 1usize << (usize::BITS - 5);
/// 待办输出泵标志。
const OUTPUT_PENDING: usize = 1usize << (usize::BITS - 6);

/// 写入端已跨段标志，即此时 wp <= rp 是合法状态
pub(super) const REVERSION: usize = 1usize << (usize::BITS - 7);

/// 状态字全部标志的掩码（位置更新（`update_state`）保留这些位）。
pub(super) const FLAG_MASK: usize = TX_CLOSED
    | RX_CLOSED
    | TX_STNDBY
    | RX_STNDBY
    | REVERSION
    | INPUT_PENDING
    | OUTPUT_PENDING;
/// 每个位置占用的位数（两个位置共享低位，两个标志占高位）。
pub(super) const POS_BITS: u32 = (usize::BITS - RSV_BITS) / 2;
/// 位置掩码。
pub(super) const POS_MASK: usize = (1usize << POS_BITS) - 1;

pub(super) const MIN_CAPACITY: usize = 2;
/// 环形缓冲的最大容量（与 `ring_buffer` 的 `MAX_CAPACITY` 同量级）。
pub(super) const MAX_CAPACITY: usize = POS_MASK;

/// 环形位置状态：读者位置 `rp` / 写者位置 `wp`（均为 `[0, capacity_)` 内的
/// 物理索引）+ 跨末端标志 `rv`（即状态字中的 [`REVERSION`] 位）。
///
/// # 位置约定（REVERSION 方案，不再使用「空一槽」）
///
/// 读写位置打包进状态字低位（`rp` 占低 `POS_BITS` 位、`wp` 占次 `POS_BITS`
/// 位），容量**全部可用**——不再刻意保留一个空槽。满 / 空由 `rv` 区分：
///
/// * `rv == false`（写者未跨过物理末端）：原始 `wp >= rp`，数据量 = `wp - rp`；
///   其中 `wp == rp` 表示**空**（data = 0）；
/// * `rv == true`（写者已跨过物理末端，原始 `wp <= rp`）：数据量 =
///   `wp + capacity - rp`；其中 `wp == rp` 表示**满**（整环都是数据，
///   data = capacity）。
///
/// `rv` 只随位置推进而置位 / 清除（其余标志位原样保留）：
///
/// * [`IoPos::advance_wp`]：写者越过物理末端（`wp + amount >= capacity`）时
///   置位，且置位后一直保持——写者始终「在读者之后（含追上成满环）」；
/// * [`IoPos::advance_rp`]：读者越过物理末端（`rp + amount >= capacity`）时
///   清除——读者跨过末端后，写者的原始位置重新位于读者之前，恢复未跨状态。
pub(super) struct IoPos {
    /// 与 REVERSION flag 含义一致：写者是否已越过缓冲区物理末端（此时原始
    /// `wp <= rp`；`wp == rp` 表示环满）。
    pub rv: bool,
    /// 读者位置（物理索引，`[0, capacity_)`）。
    pub rp: usize,
    /// 写者位置（物理索引，`[0, capacity_)`）。
    pub wp: usize,
    /// circular buff 的容量。
    capacity_: usize,
}

impl IoPos {
    /// 本类型在状态字中「拥有」的位：两个位置字段（`rp` 低 `POS_BITS` 位、
    /// `wp` 次 `POS_BITS` 位）与 REVERSION 位。`pack` 只覆盖这些位，其余
    /// 标志位由传入的基座状态字原样保留。
    pub const MASK: usize = REVERSION | POS_MASK | (POS_MASK << POS_BITS);

    /// 从 `atm_stat_` 的状态字解出位置与保留标志。
    pub fn unpack(state: usize, cap: usize) -> Self {
        let rp = state & POS_MASK;
        let wp = (state >> POS_BITS) & POS_MASK;
        let rv = has_flag(state, REVERSION);
        IoPos { rv, rp, wp, capacity_: cap }
    }

    /// 当前可读数据量（遵循上述约定：空环 = 0、满环 = 容量）。
    #[inline]
    pub fn data_size(&self) -> usize {
        if self.rv && self.wp == self.rp {
            // 写者跨过末端后恰好追上读者：整环都是数据（满）。
            self.capacity_
        } else {
            // 未跨（wp > rp）或已跨未满（wp < rp）：`(wp - rp) mod capacity`。
            (self.wp + self.capacity_ - self.rp) % self.capacity_
        }
    }

    /// 当前可写空间量：`capacity - data_size`（满环 = 0、空环 = 容量）。
    #[inline]
    pub fn free_size(&self) -> usize {
        self.capacity_ - self.data_size()
    }

    /// 以传入的基座状态字 `state` 打包回完整状态字：`state` 中本类型不拥有的
    /// 位（[`IoPos::MASK`] 之外——关闭、待机、待办泵等标志）**原样保留**；
    /// 本类型拥有的位（`rp` / `wp` 位置字段与 REVERSION 位）用自身的新值
    /// **覆盖**（先清除基座中的旧值再写入，而非按位或——否则旧位置会残留并
    /// 与新位置混合）。
    pub fn pack(&self, state: usize) -> usize {
        let s = (state & !Self::MASK) | self.rp | (self.wp << POS_BITS);
        if self.rv { s | REVERSION } else { s & !REVERSION }
    }

    /// 推进写者位置：`wp += amount`（物理上环绕）；**写者越过缓冲区物理末端
    /// （`wp + amount >= capacity`）时设置 REVERSION flag**，且一旦置位保持
    /// 到读者追上来为止。返回推进后的**新位置状态**（不含任何标志位）；需要
    /// 写回 `atm_stat_` 时以原状态字为基座调用 [`IoPos::pack`]。
    ///
    /// 前置：`amount <= free_size`（不允许写过头；恰好写满时进入
    /// `wp == rp && rv` 的满态）。
    pub fn advance_wp(&self, amount: usize) -> Self {
        debug_assert!(amount <= self.free_size());
        let new_wp = self.wp + amount;
        let crossed = new_wp >= self.capacity_;
        Self {
            // 越过末端置位；已置位则保持（写者仍在读者之后，直到读者追上）。
            rv: self.rv || crossed,
            rp: self.rp,
            wp: new_wp % self.capacity_,
            capacity_: self.capacity_,
        }
    }

    /// 推进读者位置：`rp += amount`（物理上环绕）；**读者越过缓冲区物理末端
    /// （`rp + amount >= capacity`）时清除 REVERSION flag**——读者跨过末端后，
    /// 写者的原始位置重新位于读者之前，恢复未跨状态。返回推进后的**新位置状态**
    /// （不含任何标志位）；需要写回 `atm_stat_` 时以原状态字为基座调用
    /// [`IoPos::pack`]。
    ///
    /// 前置：`amount <= data_size`（不允许读过头；恰好读空时回到
    /// `wp == rp && !rv` 的空态）。
    pub fn advance_rp(&self, amount: usize) -> Self {
        debug_assert!(amount <= self.data_size());
        let new_rp = self.rp + amount;
        let crossed = self.rp + amount >= self.capacity_;
        Self {
            rv: self.rv && !crossed,
            rp: new_rp % self.capacity_,
            wp: self.wp,
            capacity_: self.capacity_,
        }
    }
}

#[inline]
fn has_flag(state: usize, flag: usize) -> bool {
    state & flag != 0
}

/// 微型 `block_on`：用 `Waker::noop()` 自旋驱动单个 future 到完成。
///
/// 主动端的「搬运」在 hook 内部**同步**完成（不 spawn、无运行时）：设备
/// `read_async` / `write_async` 返回的 future 在此被轮询到 `Ready`。若设备在
/// 暂无数据 / 空间时返回 `Pending`，这里会自旋等待（无任务模型下「自动搬运」
/// 的固有语义；非阻塞设备立即 `Ready`，不会空转）。
fn block_on<F: Future>(fut: F) -> F::Output {
    let mut fut = pin!(fut);
    let waker = Waker::noop();
    let mut cx = Context::from_waker(&waker);
    loop {
        if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
            return v;
        }
    }
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

/// 设计为只给 SPSC 中的 Consumer<P, C, B, T, A> 或者 Producer<P, C, B, T, A> 
/// 调用。实际上不可并发调用。
impl<C, B, T> CircCore<BufProducer<T>, C, B, T>
where
    // P: Send + Sync + TrProducer<Data = T>,
    C: Send + Sync + TrConsumer<Data = T>,
    B: Send + Sync + BorrowMut<[MaybeUninit<T>]>,
    T: Send + Sync,
{
    pub fn try_write_<'f>(
        &'f self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<ReclSliceMut<'f, T, WriterReclaim<'f, Self>>, TxError<usize>> {
        let x: SomeOf<
            ReclSliceMut<'_, T, WriterReclaim<'_, Self>>,
            TxError<usize>,
        > = self
            .try_write_at(demand)
            .map(|(start, take)| self.write_segm(start, take))
            .into();
        if x.contains_left() {
            return x;
        };
        let err = x.as_ref().pick_right().expect("");
        if err.err_tag().should_terminate() {
            return x;
        };
        // 未终止（Stuffed）：对端（消费端）为主动 → 同步泵出一轮释放空间后
        // 重试（无后台任务模型下「操作即事件」；`pump_output` 内部门控保证
        // 仅一端主动一端被动时实际泵出，双被动时原样返回）。
        self.pump_output();
        self.try_write_at(demand)
            .map(|(start, take)| self.write_segm(start, take))
            .into()
    }

    pub fn write_async_<'f>(
        &'f self,
        demand: &'f Demand<usize>,
    ) -> CorePassiveWriteAsync<'f, C, B, T> {
        CorePassiveWriteAsync(self, demand)
    }

    pub fn on_buf_producer_drop_(&self) {
        self.clear_flag(TX_STNDBY);
        let p = unsafe { &*self.producer_.get() };
        let x = p.try_reset_demand();
        if let Result::Err(_) = x {
            // todo: clear existing demand
        }
    }
}

impl<P, B, T> CircCore<P, BufConsumer<T>, B, T>
where
    P: Send + Sync + TrProducer<Data = T>,
    // C: Send + Sync + TrConsumer<Data = T>,
    B: Send + Sync + BorrowMut<[MaybeUninit<T>]>,
    T: Send + Sync,
{
    pub fn try_read_<'f>(
        &'f self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<ReclSliceRef<'f, T, ReaderReclaim<'f, Self>>, RxError<usize>> {
        // #[allow(clippy::type_complexity)]
        let x: SomeOf<
            ReclSliceRef<'_, T, ReaderReclaim<'_, Self>>,
            RxError<usize>,
        > = self
            .try_read_at(demand)
            .map(|(start, take)| self.read_segm(start, take))
            .into();
        if x.contains_left() {
            return x;
        };
        let err = x.as_ref().pick_right().expect("");
        if err.err_tag().should_terminate() {
            return x;
        };
        // 未终止（Drained）：对端（生产端）为主动 → 同步泵入一轮补位后重试
        // （`try_read` 自动驱动输入泵；`pump_input` 内部门控保证仅一端主动
        // 一端被动时实际泵入，双被动时原样返回）。
        self.pump_input();
        self.try_read_at(demand)
            .map(|(start, take)| self.read_segm(start, take))
            .into()
    }

    pub fn read_async_<'f>(
        &'f self,
        demand: &'f Demand<usize>,
    ) -> CorePassiveReadAsync<'f, P, B, T> {
        CorePassiveReadAsync(self, demand)
    }

    pub fn on_buf_consumer_drop_(&self) {
        self.clear_flag(RX_STNDBY);
        let c = unsafe { &*self.consumer_.get() };
        let x = c.try_reset_demand();
        if let Result::Err(_) = x {
            // todo: clear existing demand
        }
    }
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
            pos.advance_wp(amount).pack(s)
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
            pos.advance_rp(amount).pack(s)
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

    pub fn on_consumer_drop_(&self) {
        // let c = unsafe { &*self.consumer_.get() };
        self.clear_flag(RX_STNDBY);
        // let x = c.try_reset_demand();
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
    // 同步泵（一端主动一端被动）：主动端的数据搬运
    // ------------------------------------------------------------------

    /// 构建期初始泵：一端主动一端被动时，主动端先各自泵一轮（生产端把输入
    /// 设备的数据填满缓冲、消费端把缓冲排空到输出设备），让数据从构建完成
    /// 起就开始流动。双端被动 / 双端主动时按内部门控为 no-op。
    pub(super) fn start(&self) {
        self.pump_input();
        self.pump_output();
    }

    /// 输入泵（主动生产端）：从输入设备读入缓冲（同步，`block_on` 轮询设备
    /// future 到完成）。循环直到**缓冲已满 / 任一端关闭 / 设备暂无数据**。
    ///
    /// 每轮借出**当前全部可写区**（两段式段，跨末端时拆两段），段 drop 时经
    /// `WriterReclaim` 提交（`advance_write` → 触发消费端事件）。
    ///
    /// # 门控（为何只在一端主动一端被动时运行）
    ///
    /// 泵的提交会触发对端事件（`fire_consumer`），若对端也是主动端，会再次
    /// 进入对端泵，形成「输入泵 → 输出泵 → 输入泵 → …」的**无界递归**。
    /// 双主动（`Pipeline`）由流水线 future 异步驱动设备，不走本同步泵；因此
    /// 本方法只在「生产端主动 **且** 消费端被动」时工作，其余组合直接返回。
    fn pump_input(&self) {
        let producer = unsafe { &*self.producer_.get() };
        if producer.is_passive() {
            return;
        }
        let consumer = unsafe { &*self.consumer_.get() };
        if !consumer.is_passive() {
            return;
        }
        loop {
            let state = self.atm_stat_.value();
            if has_flag(state, TX_CLOSED) || has_flag(state, RX_CLOSED) {
                break;
            }
            let pos = IoPos::unpack(state, self.capacity());
            let free = pos.free_size();
            if free == 0 {
                break; // 缓冲已满（整环都是数据）
            }
            // 借出全部可写区（`write_segm` 覆盖跨末端的两段式情形）。
            // SAFETY: 可写区不与任何活段 / 泵操作重叠（SPSC 纪律：泵运行在
            // 提交之后、且本方法只在调用者线程上执行）。
            let mut segm = self.write_segm(pos.wp, free);
            let producer = unsafe { &mut *self.producer_.get() };
            let r = block_on(producer.react_async(&mut segm));
            if r == ReceiverReact::Continue {
                break; // 设备暂无数据 / 错误：本轮无进展
            }
            // segm drop：提交（advance_write → 触发消费端事件，被动端被唤醒）
        }
    }

    /// 输出泵（主动消费端）：把缓冲数据写到输出设备（同步）。循环直到
    /// **缓冲排空 / 读端关闭 / 设备暂不能接收**。每轮借出全部可读区，段 drop
    /// 时提交（`advance_read` → 触发生产端事件）。门控同 [`CircCore::pump_input`]。
    fn pump_output(&self) {
        let consumer = unsafe { &*self.consumer_.get() };
        if consumer.is_passive() {
            return;
        }
        let producer = unsafe { &*self.producer_.get() };
        if !producer.is_passive() {
            return;
        }
        loop {
            let state = self.atm_stat_.value();
            if has_flag(state, RX_CLOSED) {
                break;
            }
            let pos = IoPos::unpack(state, self.capacity());
            let data = pos.data_size();
            if data == 0 {
                break;
            }
            // 借出全部可读区（跨末端时两段式）。
            let mut segm = self.read_segm(pos.rp, data);
            let consumer = unsafe { &mut *self.consumer_.get() };
            let r = block_on(consumer.react_async(&mut segm));
            if r == ReceiverReact::Continue {
                break; // 设备暂不能接收 / 错误
            }
        }
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
        let producer = unsafe { &*self.producer_.get() };
        if !producer.check(event) || producer.is_passive()  {
            return;
        }
        // 主动生产端：对端（被动消费端）完成读取 / 关闭，同步泵入一轮补位
        // （内部门控：仅一端主动一端被动时实际泵入）。
        self.pump_input();
    }

    /// 触发消费端事件（生产端完成写入 / 关闭后）。与 [`CircCore::fire_producer`]
    /// 对称（`&mut C` 的安全性同理：本方法只由 `advance_write` / `close_tx`
    /// 触发，其调用点不持有 `&mut C`；被动端的 armed 门控同理经
    /// `RX_STNDBY`）。
    fn fire_consumer(&self, event: ConsumerHookEvent) {
        let consumer = unsafe { &*self.consumer_.get() };
        if !consumer.check(event) || consumer.is_passive()  {
            return;
        }
        // 主动消费端：对端（被动生产端）完成写入 / 关闭，同步泵出一轮排空
        // （含 `ProducerClose` 驱动下排空残留数据）。
        self.pump_output();
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

/// 被动等待（park）的守卫：持有注册进唤醒槽位的 [`Waiter`]，并保证等待
/// future 在**任何退出路径**上完成收尾。
///
/// 注册进槽位的 `Waiter` 由槽位以**裸指针**持有（`WakeSlot::register`），
/// 因此等待 future 在未完成时被 drop（取消 / 被对端抢占）也必须注销槽位，
/// 否则槽位会留下指向已销毁 `Waiter` 的悬垂指针——下一个等待者的
/// `WakeSlot::register` 会因此自旋（`register` 的文档：「等待者在完成或 drop
/// 时必然注销，因此自旋必然终止」）。同时，park 期间登记的 demand 也必须
/// 复位，否则下一次 `try_set_demand`（CAS null → 非空）会失败并触发
/// 「并发调用」断言。
///
/// 因此本守卫的 [`Drop`] 统一执行两件事：
///
/// 1. 若仍注册在槽位中，则注销（`unregister`）；
/// 2. 复位 demand（`reset_demand`）。
///
/// 正常完成路径上（需求满足 / 终止错误），守卫随 `poll_fn` future 在
/// `.await` 结束时被 drop，同样执行复位——与取消路径共用同一份收尾逻辑。
struct WaitGuard<'a, E> {
    /// 被等待的被动端（`BufProducer` / `BufConsumer`）。
    end: &'a E,
    /// 注册进 `end.wakeslot()` 的等待者（槽位以裸指针引用它，注册期间不得
    /// 移动——它活在 `poll_fn` future 内，而该 future 被 async 状态机钉住）。
    waiter: Waiter,
    /// 当前是否已注册进槽位。
    registered: bool,
    /// 注销槽位：`end.wakeslot().deregister(&waiter)`。
    unregister: fn(&E, &Waiter),
    /// 复位需求：`end.try_reset_demand()`。
    reset_demand: fn(&E),
}

impl<E> Drop for WaitGuard<'_, E> {
    fn drop(&mut self) {
        if self.registered {
            (self.unregister)(self.end, &self.waiter);
        }
        (self.reset_demand)(self.end);
    }
}

use super::circ_buff_::{BufConsumer, BufProducer};

#[gen_may_cancel_future(CorePassiveRead)]
async fn core_passive_read_async_<'f, P, B, T, C>(
    core: &'f CircCore<P, BufConsumer<T>, B, T>,
    demand: &'f Demand<usize>,
    cancel: &'f mut C,
) -> SomeOf<
    ReclSliceRef<'f, T, ReaderReclaim<'f, CircCore<P, BufConsumer<T>, B, T>>>,
    RxError<usize>,
>
where
    P: Send + Sync + TrProducer<Data = T>,
    // K: Send + Sync + TrConsumer<Data = T>,
    B: Send + Sync + BorrowMut<[MaybeUninit<T>]>,
    T: Send + Sync,
    C: TrCancellationToken + Clone,
{
    let x = core.try_read_(demand);
    if x.contains_left() {
        return x;
    };
    let err = x.as_ref().pick_right().expect("");
    if err.err_tag().should_terminate() {
        return x;
    };
    let consume = unsafe { &*core.consumer_.get() };
    if !consume.try_set_demand(demand) {
        unreachable!("Concurrent call `core_passive_read_async_`")
    }
    // —— 等待（park）——
    //
    // demand 已登记进 `BufConsumer`（对端提交路径的 `fire_consumer` 会经
    // `check` 裁决是否唤醒本等待者）。此后循环直到需求满足（或出现终止错误）：
    //
    // 1. 重查 `try_read_at`：满足 / 终止 → `Ready`（收尾由守卫的 Drop 完成：
    //    注销槽位 + 复位 demand）；
    // 2. 否则把 waker 注册进 `BufConsumer` 的唤醒槽位，返回 `Pending`——对端
    //    写入提交触发 `fire_consumer` → `signal` 唤醒本等待者；
    // 3. **注册后重查一次**：关闭「检查与注册之间对端恰好完成提交」的丢失
    //    唤醒窗口（`signal` 只唤醒已注册的等待者，不会重查条件；注册前的
    //    那次提交若已发生，重查能立刻发现而不必等下一次事件）。
    let mut guard = WaitGuard {
        end: consume,
        waiter: Waiter::new(),
        registered: false,
        unregister: |end, waiter| end.wakeslot().deregister(waiter),
        reset_demand: |end| {
            let _ = end.try_reset_demand();
        },
    };
    let res = core::future::poll_fn(|cx| {
        let can_stop = || match core.try_read_at(demand) {
            Ok(_) => true,
            Err(e) => e.err_tag().should_terminate(),
        };
        if can_stop() {
            return Poll::Ready(());
        }
        guard.waiter.waker = Some(cx.waker().clone());
        guard.end.wakeslot().register(&guard.waiter);
        guard.registered = true;
        if can_stop() {
            return Poll::Ready(());
        }
        Poll::Pending
    });
    // 守卫在此已被 drop：槽位注销、demand 复位。重新尝试——等待期间可读数据
    // 只增不减（SPSC：只有本消费者读），结果必为可读段或终止错误。
    if res.ok_or(cancel.cancellation()).await.is_ok() {
        core.try_read_(demand)
    } else {
        SomeOf::new_right(RxError::Cancelled)
    }
}

#[gen_may_cancel_future(CorePassiveWrite)]
async fn core_passive_write_async_<'f, K, B, T, C>(
    core: &'f CircCore<BufProducer<T>, K, B, T>,
    demand: &'f Demand<usize>,
    cancel: &'f mut C,
) -> SomeOf<
    ReclSliceMut<'f, T, WriterReclaim<'f, CircCore<BufProducer<T>, K, B, T>>>,
    TxError<usize>,
>
where
    // P: Send + Sync + TrProducer<Data = T>,
    K: Send + Sync + TrConsumer<Data = T>,
    B: Send + Sync + BorrowMut<[MaybeUninit<T>]>,
    T: Send + Sync,
    C: TrCancellationToken + Clone,
{
    let x = core.try_write_(demand);
    if x.contains_left() {
        return x;
    };
    let err = x.as_ref().pick_right().expect("");
    if err.err_tag().should_terminate() {
        return x;
    };
    let producer = unsafe { &*core.producer_.get() };
    if !producer.try_set_demand(demand) {
        unreachable!("Concurrent call `core_passive_write_async_`")
    };
    // —— 等待（park）——与读侧（`core_passive_read_async_`）对称：
    // demand 已登记进 `BufProducer`；循环直到可写空间满足需求（或出现终止
    // 错误），否则把 waker 注册进 `BufProducer` 的唤醒槽位等待对端读取提交
    // 触发 `fire_producer` → `signal` 唤醒；注册后重查一次关闭丢失唤醒窗口。
    let mut guard = WaitGuard {
        end: producer,
        waiter: Waiter::new(),
        registered: false,
        unregister: |end, waiter| end.wakeslot().deregister(waiter),
        reset_demand: |end| {
            let _ = end.try_reset_demand();
        },
    };
    let res = core::future::poll_fn(|cx| {
        let can_stop = || match core.try_write_at(demand) {
            Ok(_) => true,
            Err(e) => e.err_tag().should_terminate(),
        };
        if can_stop() {
            return Poll::Ready(());
        }
        guard.waiter.waker = Some(cx.waker().clone());
        guard.end.wakeslot().register(&guard.waiter);
        guard.registered = true;
        if can_stop() {
            return Poll::Ready(());
        }
        Poll::Pending
    });
    // 守卫在此已被 drop：槽位注销、demand 复位。重新尝试——等待期间可写
    // 空间只增不减（SPSC：只有本生产者写），结果必为可写段或终止错误。
    if res.ok_or(cancel.cancellation()).await.is_ok() {
        core.try_write_(demand)
    } else {
        SomeOf::new_right(TxError::Cancelled)
    }
}
