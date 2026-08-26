//! 提供给最终用户、暴露的公共接口 Producer 和 Consumer。
//! 所有对 Circular Buffer 的操作都必须通过这两个实例。
//! 如果有一端在 Circular Buffer 构建时就已经被指定（主动模式），那么这一端
//! **不产出半部**——`build` 只把被动端的半部交给调用者（见
//! [`super::builder::BuildOutcome`]）；全主动时产出 [`Pipeline`]（流水线
//! future，由设备驱动）。
//!
//! # 设计意图（拥有型访问模型）
//!
//! 核心被分配在堆上（[`CoreRef`] = `Shared<CircCore<P, C, T, A>, A>`），
//! [`Producer`] / [`Consumer`] 各持一份引用，可独立分发、跨线程使用
//! （SPSC：至多一个生产线程 + 一个消费线程）。构建器产出 [`SpscPair`]，
//! 没有「缓冲聚合体」这一层——使用者只需持有这对半部（或其一）。
//!
//! 异步等待（`write_async` / `read_async`）把 waker 注册进核心的被动唤醒
//! 槽位：对端完成读取 / 写入时，hook 唤醒等待者，等待者重新检查条件。

use core::{
    marker::PhantomPinned,
    pin::Pin,
    sync::atomic::{AtomicBool, Ordering},
    task::{Context, Poll},
};

use abs_buff::{
    Demand, TrBuffRead, TrBuffTryRead, TrBuffTryWrite, TrBuffWrite,
    x_deps::{abs_cancel::TrMayCancel, anylr::SomeOf},
};
use mm_ptr::{
    Shared,
    x_deps::abs_mm::mem_alloc::{CoreAlloc, TrMalloc},
};

use crate::circular_buff::{BuffConsumer, BuffProducer};

use super::{
    abs_comp_::{TrConsumer, TrProducer},
    core_::{CircCore, WakeSlot},
    error_::{RxError, TxError},
    reclaim_::{ReaderReclaim, ReclSliceMut, ReclSliceRef, WriterReclaim},
};

/// 堆上核心的共享引用。
///
/// 名义 `pub`（模块 `spsc_` 私有，对外不可达）：构建产物装配
/// （[`super::builder::BuildOutcome`]）的公开 trait 方法签名需要引用它。
pub type CoreRef<P, C, T, A> = Shared<CircCore<P, C, T, A>, A>;

/// 构建器产出的半部对：`(Producer, Consumer)`。使用者可持有两者或其一。
pub type SpscPair<T = u8, A = CoreAlloc> = (
    Producer<BuffProducer<T>, BuffConsumer<T>, T, A>,
    Consumer<BuffProducer<T>, BuffConsumer<T>, T, A>,
);

// ---------------------------------------------------------------------------
// 生产端半部（拥有型）
// ---------------------------------------------------------------------------

/// 生产端半部：持有一份堆上核心的引用，代理转发用户请求到 `CircCore`。
///
/// `P` / `C` / `T` 与核心的端类型一致（被动 / 主动由构建期决定），`A` 是
/// 分配器（默认 `CoreAlloc`）。
///
/// **主动端不产出半部**：若生产端为主动模式（`P = DeviceProducer`），构建器
/// 不会把它交给调用者（见 [`super::builder::BuildOutcome`]）——本半部只代表
/// 被动生产端。
pub struct Producer<P, C, T, A>
where
    P: TrProducer<Data = T>,
    C: TrConsumer<Data = T>,
    A: TrMalloc + Clone,
{
    core_ref_: CoreRef<P, C, T, A>,
}

/// 消费端半部：与 [`Producer`] 对称（读路径）。
///
/// **主动端不产出半部**：若消费端为主动模式（`C = DeviceConsumer`），构建器
/// 不会把它交给调用者——本半部只代表被动消费端。
pub struct Consumer<P, C, T, A>
where
    P: TrProducer<Data = T>,
    C: TrConsumer<Data = T>,
    A: TrMalloc + Clone,
{
    core_ref_: CoreRef<P, C, T, A>,
}

/// 半部的公共借用约束：段类型（[`ReclSliceMut`] / [`ReclSliceRef`]）要求
/// 核心实现 `TrCircBuffCore`（其超类
/// `Send + Sync`），故两端与元素类型必须 `Send + Sync`。此约束由各 impl 的
/// where 子句直接表达。
impl<P, C, T, A> Producer<P, C, T, A>
where
    P: Send + Sync + TrProducer<Data = T>,
    C: Send + Sync + TrConsumer<Data = T>,
    T: Send + Sync,
    A: Send + Sync + TrMalloc + Clone,
{
    pub(super) fn new(core_ref_: CoreRef<P, C, T, A>) -> Self {
        Producer { core_ref_ }
    }

    /// 环形缓冲的容量（单元数）。
    pub fn capacity(&self) -> usize {
        self.core_ref_.capacity()
    }

    /// 当前可写空间。
    pub fn free_size(&self) -> usize {
        self.core_ref_.free_size()
    }

    /// 当前可读数据量（观察用）。
    pub fn data_size(&self) -> usize {
        self.core_ref_.data_size()
    }

    /// 写端（本端）是否已关闭。
    pub fn is_closed(&self) -> bool {
        self.core_ref_.is_tx_closed()
    }

    /// 消费端（对端）是否已关闭。
    pub fn is_consumer_closed(&self) -> bool {
        self.core_ref_.is_rx_closed()
    }

    /// 关闭写端：不再写入，触发消费端事件（`ProducerClose`）。
    pub fn close(&mut self) {
        self.core_ref_.close_tx();
    }

    /// 本端是否为被动模式（对外可访问）。
    pub fn is_passive(&self) -> bool {
        self.core_ref_.producer_is_passive()
    }
}

impl<P, C, T, A> Consumer<P, C, T, A>
where
    P: Send + Sync + TrProducer<Data = T>,
    C: Send + Sync + TrConsumer<Data = T>,
    T: Send + Sync,
    A: Send + Sync + TrMalloc + Clone,
{
    pub(super) fn new(core_ref_: CoreRef<P, C, T, A>) -> Self {
        Consumer { core_ref_ }
    }

    /// 环形缓冲的容量（单元数）。
    pub fn capacity(&self) -> usize {
        self.core_ref_.capacity()
    }

    /// 当前可读数据量。
    pub fn data_size(&self) -> usize {
        self.core_ref_.data_size()
    }

    /// 当前可写空间（观察用）。
    pub fn free_size(&self) -> usize {
        self.core_ref_.free_size()
    }

    /// 读端（本端）是否已关闭。
    pub fn is_closed(&self) -> bool {
        self.core_ref_.is_rx_closed()
    }

    /// 生产端（对端）是否已关闭（EOF）。
    pub fn is_producer_closed(&self) -> bool {
        self.core_ref_.is_tx_closed()
    }

    /// 关闭读端：不再读取，触发生产端事件（`ConsumerClose`）。
    pub fn close(&mut self) {
        self.core_ref_.close_rx();
    }

    /// 本端是否为被动模式（对外可访问）。
    pub fn is_passive(&self) -> bool {
        self.core_ref_.consumer_is_passive()
    }
}

// ---------------------------------------------------------------------------
// 全主动流水线（双端主动时 `build` 的产物）：一个由设备驱动的 Future
// ---------------------------------------------------------------------------

/// 双端全主动（`TrInput → 缓冲 → TrOutput`）时 `build` 的产物：一条**流水线
/// Future**。
///
/// 由调用者交给异步运行时（`spawn`）驱动：**只要本 future 存活（未被取消 /
/// 未结束），数据就持续从输入设备流向输出设备**——泵循环 await 两端设备的
/// `read_async` / `write_async`，由设备自身的就绪/阻塞驱动流动；直到：
///
/// * 一端**出错**（设备 future 返回错误——表现为该方向不再有进展）；
/// * 一端**关闭**（核心的 tx / rx 端被关闭）；
/// * 调用者**请求断开**（[`Pipeline::disconnect_handle`] 的
///   [`PipelineDisconnect::request`]，或直接 drop / 取消本 future）。
///
/// 断开时流水线关闭两端、把缓冲残留排空到输出设备后结束（future 返回
/// `()`）。
///
/// # 设备契约
///
/// 为让「数据一到达就流动」，输入/输出设备的异步操作应在暂时无数据 / 无空间
/// 时返回 `Pending`（并注册 waker），由设备的运行时在就绪时唤醒——本 future
/// 的轮询完全由设备的就绪驱动。若设备在无数据时立即返回 `Ready(0)`（非阻塞
/// 风格），流水线在本轮无进展后停驻（不再流动，也不会空转）。
///
/// # 与被动端混合
///
/// 若需要流水线**自动**持续流动且不想持有本 future，请让至少一端保持被动：
/// 被动端的每次读写都会自动驱动对端的主动泵（见 [`Consumer`] / [`Producer`]）。
pub struct Pipeline<P, C, T, A>
where
    P: TrProducer<Data = T>,
    C: TrConsumer<Data = T>,
    A: TrMalloc + Clone,
{
    core_ref_: CoreRef<P, C, T, A>,
    /// 断开请求标志：调用者持有一份副本（[`PipelineDisconnect`]），置位后
    /// 流水线在下一轮 pump 时关闭并结束。
    disconnect_: Shared<AtomicBool, A>,
    /// 是否已持有核心的泵互斥（`PUMPING`）——首次 poll 时获取，防止段提交
    /// 触发的 `fire_*` 同步 `drive()` 与本异步泵竞争。
    pumping_: bool,
}

impl<P, C, T, A> Pipeline<P, C, T, A>
where
    P: Send + Sync + TrProducer<Data = T>,
    C: Send + Sync + TrConsumer<Data = T>,
    T: Send + Sync,
    A: Send + Sync + TrMalloc + Clone,
{
    pub(super) fn new(core_ref_: CoreRef<P, C, T, A>, alloc: A) -> Self {
        let disconnect_ = Shared::new(AtomicBool::new(false), alloc);
        Pipeline {
            core_ref_,
            disconnect_,
            pumping_: false,
        }
    }

    /// 取得**断开句柄**：把它交给任意线程 / 调用方持有，在想要停止数据流动
    /// 时调用 [`PipelineDisconnect::request`]——流水线在下一轮 pump 时关闭
    /// 两端、排空残留并结束（future 返回）。
    ///
    /// 句柄只引用断开标志，不持有核心与设备：流水线自身（future）被 drop /
    /// 取消后，核心与设备随之释放。
    pub fn disconnect_handle(&self) -> PipelineDisconnect<A> {
        PipelineDisconnect {
            flag: self.disconnect_.clone(),
        }
    }

    /// 环形缓冲的容量（单元数）。
    pub fn capacity(&self) -> usize {
        self.core_ref_.capacity()
    }

    /// 缓冲中当前的数据量。
    pub fn data_size(&self) -> usize {
        self.core_ref_.data_size()
    }

    /// 缓冲中当前的可写空间。
    pub fn free_size(&self) -> usize {
        self.core_ref_.free_size()
    }

    /// 关闭两端并排空缓冲残留到输出设备（断开收尾，供结束路径共用）。
    fn shutdown(&mut self, cx: &mut Context<'_>) {
        let core = &*self.core_ref_;
        core.close_tx(); // 写端关闭（输出 pump 仍可排空：RX 未关）
        // 显式排空残留（`fire_*` 的 drive 被 PUMPING 互斥抑制，须手动泵）；
        // 输出设备阻塞时尽力而为（残留随核心释放丢弃）。
        let drain = core.pump_output_once_async();
        let mut drain = core::pin::pin!(drain);
        let _ = drain.as_mut().poll(cx);
        core.close_rx(); // 读端关闭：输入 pump 停止
        core.exit_pump();
        self.pumping_ = false;
    }
}

impl<P, C, T, A> core::future::Future for Pipeline<P, C, T, A>
where
    P: Send + Sync + TrProducer<Data = T>,
    C: Send + Sync + TrConsumer<Data = T>,
    T: Send + Sync,
    A: Send + Sync + TrMalloc + Clone,
{
    type Output = ();

    /// 驱动一轮流水线泵：
    ///
    /// * 检查断开请求 / 端关闭 → 关闭两端、排空残留并 `Ready`；
    /// * 否则**输出优先**地交替 await 输出泵与输入泵（设备阻塞时挂起、由设备
    ///   唤醒后继续），同一 poll 内收敛到无进展；
    /// * 输入设备挂起时，若本段已搬入部分数据，先尽力排空一次输出再挂起，
    ///   避免数据滞留缓冲；
    /// * 两端都无进展（非阻塞设备当前无数据）→ `Pending` 停驻。
    ///
    /// 首次 poll 获取核心的泵互斥（`PUMPING`），结束路径释放。
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        // 首次 poll 持有泵互斥：段提交触发的 fire_* 只会置待办标志，不会
        // 同步 drive（block_on）与本异步泵竞争。
        if !this.pumping_ {
            if !this.core_ref_.try_enter_pump() {
                // 理论不可达（全主动无其他泵）：让出，等待下一轮。
                return Poll::Pending;
            }
            this.pumping_ = true;
        }
        loop {
            if this.disconnect_.load(Ordering::Acquire) {
                this.shutdown(cx);
                return Poll::Ready(());
            }
            let core = &*this.core_ref_;
            if core.is_tx_closed() || core.is_rx_closed() {
                this.shutdown(cx);
                return Poll::Ready(());
            }
            let mut progressed = false;
            // 输出优先：先把缓冲中的数据排空（输出设备阻塞则挂起——已消费部分
            // 已提交，残留留在缓冲，安全）。
            if core.data_size() > 0 {
                let out_pump = core.pump_output_once_async();
                let mut out_pump = core::pin::pin!(out_pump);
                match out_pump.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(p) => progressed |= p,
                }
            }
            // 再补输入：读一次设备到可写段并提交（读不到则干净挂起）。
            if core.free_size() > 0 {
                let in_pump = core.pump_input_once_async();
                let mut in_pump = core::pin::pin!(in_pump);
                match in_pump.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(p) => progressed |= p,
                }
            }
            if !progressed {
                // 两端均无进展（非阻塞设备当前无数据 / 错误）：停驻等待设备
                // 唤醒。阻塞设备不会走到这里——await 已挂起。
                return Poll::Pending;
            }
        }
    }
}

/// 流水线的断开句柄（见 [`Pipeline::disconnect_handle`]）。
///
/// 只引用一个原子标志，不持有核心与设备；可跨线程持有，随时请求断开。
pub struct PipelineDisconnect<A = CoreAlloc>
where
    A: TrMalloc + Clone,
{
    flag: Shared<AtomicBool, A>,
}

impl<A> PipelineDisconnect<A>
where
    A: TrMalloc + Clone,
{
    /// 请求断开：流水线在下一轮 pump 时关闭两端、排空残留并结束。
    pub fn request(&self) {
        self.flag.store(true, Ordering::Release);
    }
}

// ---------------------------------------------------------------------------
// abs_buff 读写 trait
// ---------------------------------------------------------------------------

impl<P, C, T, A> TrBuffWrite<T> for Producer<P, C, T, A>
where
    P: Send + Sync + TrProducer<Data = T>,
    C: Send + Sync + TrConsumer<Data = T>,
    T: Send + Sync,
    A: Send + Sync + TrMalloc + Clone,
{
    type WriteAsync<'f> = WriteAsync<'f, P, C, T, A> where Self: 'f;
    type SegmMut<'f> = ReclSliceMut<'f, T, WriterReclaim<'f, CircCore<P, C, T, A>>> where Self: 'f;
    type Err = TxError<usize>;

    #[inline]
    fn is_stuffed_closing(&self) -> bool {
        self.core_ref_.is_tx_closed() || !self.core_ref_.producer_ready(1)
    }

    #[inline]
    fn write_async<'f>(&'f mut self, demand: &Demand<usize>) -> Self::WriteAsync<'f> {
        WriteAsync::new(&self.core_ref_, demand)
    }
}

impl<P, C, T, A> TrBuffTryWrite<T> for Producer<P, C, T, A>
where
    P: Send + Sync + TrProducer<Data = T>,
    C: Send + Sync + TrConsumer<Data = T>,
    T: Send + Sync,
    A: Send + Sync + TrMalloc + Clone,
{
    #[inline]
    fn try_write<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> SomeOf<Self::SegmMut<'f>, Self::Err> {
        // 主动生产端不对外暴露：半部操作返回错误。
        if !self.core_ref_.producer_is_passive() {
            return SomeOf::new_right(TxError::Unavailable);
        }
        // 对端（消费端）为主动设备时，先驱动一轮输出泵冲刷（释放可写空间）——
        // 无后台任务模型下「操作即事件」；对端被动时不驱动（泵会在用户可能
        // 持有活读段的同一侧构造段，造成别名 UB，且无意义）。
        if !self.core_ref_.consumer_is_passive() {
            self.core_ref_.drive_output();
        }
        let min_len = demand.min().copied().unwrap_or(0);
        match self.core_ref_.try_write_at(demand) {
            Ok((start, take)) => {
                if take < min_len {
                    // 不足下限（理论上 try_write_at 已保证，这里是双重保险）。
                    let e = if self.core_ref_.is_tx_closed() {
                        TxError::Closing
                    } else {
                        TxError::Stuffed(start)
                    };
                    SomeOf::new_right(e)
                } else {
                    SomeOf::new_left(self.core_ref_.write_segm(start, take))
                }
            }
            Err(err) => SomeOf::new_right(err),
        }
    }
}

impl<P, C, T, A> TrBuffRead<T> for Consumer<P, C, T, A>
where
    P: Send + Sync + TrProducer<Data = T>,
    C: Send + Sync + TrConsumer<Data = T>,
    T: Send + Sync,
    A: Send + Sync + TrMalloc + Clone,
{
    type ReadAsync<'f> = ReadAsync<'f, P, C, T, A> where Self: 'f;
    type SegmRef<'f> = ReclSliceRef<'f, T, ReaderReclaim<'f, CircCore<P, C, T, A>>> where Self: 'f;
    type Err = RxError<usize>;

    #[inline]
    fn is_drained_closing(&self) -> bool {
        self.core_ref_.is_rx_closed() || self.core_ref_.is_tx_closed()
    }

    #[inline]
    fn read_async<'f>(&'f mut self, demand: &Demand<usize>) -> Self::ReadAsync<'f> {
        ReadAsync::new(&self.core_ref_, demand)
    }
}

impl<P, C, T, A> TrBuffTryRead<T> for Consumer<P, C, T, A>
where
    P: Send + Sync + TrProducer<Data = T>,
    C: Send + Sync + TrConsumer<Data = T>,
    T: Send + Sync,
    A: Send + Sync + TrMalloc + Clone,
{
    #[inline]
    fn try_read<'f>(&'f mut self, demand: &Demand<usize>) -> SomeOf<Self::SegmRef<'f>, Self::Err> {
        // 主动消费端不对外暴露：半部操作返回错误。
        if !self.core_ref_.consumer_is_passive() {
            return SomeOf::new_right(RxError::Unavailable);
        }
        // 对端（生产端）为主动设备时，先驱动一轮输入泵拉取数据——无后台任务
        // 模型下「操作即事件」；对端被动时不驱动（泵会在用户可能持有活写段的
        // 同一侧构造段，造成别名 UB，且无意义）。
        if !self.core_ref_.producer_is_passive() {
            self.core_ref_.drive_input();
        }
        let min_len = demand.min().copied().unwrap_or(0);
        match self.core_ref_.try_read_at(demand) {
            Ok((start, take)) => {
                // EOF 例外：写端已关闭时允许返回不足下限的部分数据。
                if take < min_len && !self.core_ref_.is_tx_closed() {
                    let e = if self.core_ref_.is_rx_closed() {
                        RxError::Closing
                    } else {
                        RxError::Drained(start)
                    };
                    SomeOf::new_right(e)
                } else {
                    SomeOf::new_left(self.core_ref_.read_segm(start, take))
                }
            }
            Err(err) => SomeOf::new_right(err),
        }
    }
}

// ---------------------------------------------------------------------------
// 等待（park）辅助
// ---------------------------------------------------------------------------

/// 把 `[min, max]` 区间重新构造为 `Demand`（处理 0 / `usize::MAX` 边界，
/// 避免 `Demand::between(a, a)` 的 panic）。
fn demand_of(min: usize, max: usize) -> Demand<usize> {
    match (min, max) {
        (0, usize::MAX) => Demand::at_least(1),
        (0, m) => Demand::less_than(m),
        (n, usize::MAX) => Demand::at_least(n),
        (n, m) => Demand::between(n, m),
    }
}

/// 等待辅助：把 waker 注册进核心的被动唤醒槽位；条件满足或关闭时返回
/// `Ready`，否则 `Pending`。注册后**重新检查条件**，以关闭丢失唤醒窗口
/// （这是事件可被安全丢弃 / 取代的不变量之一）。
///
/// park 时先经 `arm`（写 demand → 置 `STNDBY` armed 位）再注册槽位；完成 /
/// drop 时经 `unpark`（清 armed 位 → 清 demand）。fire 侧仅在 armed 时行动，
/// 经状态字 Acquire 读与 armed 置位 CAS 建立 happens-before（见 `core_` 的
/// `fire_*` / `arm_*`）。
struct Park<'a, P, C, T, A>
where
    P: TrProducer<Data = T>,
    C: TrConsumer<Data = T>,
    A: TrMalloc + Clone,
{
    core: &'a CircCore<P, C, T, A>,
    waiter: super::core_::Waiter,
    registered: bool,
    slot: &'a WakeSlot,
    /// 本等待者的完整需求（park 时经 `arm` 登记；固定不变）。
    demand: Option<Demand<usize>>,
    /// park：写 demand 并置 armed 位（`CircCore::arm_producer` / `arm_consumer`）。
    arm: fn(&CircCore<P, C, T, A>, Option<Demand<usize>>),
    /// 完成 / drop：清 armed 位与 demand（`CircCore::unpark_producer` /
    /// `unpark_consumer`）。
    unpark: fn(&CircCore<P, C, T, A>),
    check: fn(&CircCore<P, C, T, A>, usize) -> bool,
}

impl<'a, P, C, T, A> Park<'a, P, C, T, A>
where
    P: TrProducer<Data = T>,
    C: TrConsumer<Data = T>,
    A: TrMalloc + Clone,
{
    fn new(
        core: &'a CircCore<P, C, T, A>,
        slot: &'a WakeSlot,
        arm: fn(&CircCore<P, C, T, A>, Option<Demand<usize>>),
        unpark: fn(&CircCore<P, C, T, A>),
        check: fn(&CircCore<P, C, T, A>, usize) -> bool,
        demand: Option<Demand<usize>>,
    ) -> Self {
        Park {
            core,
            waiter: super::core_::Waiter::new(),
            registered: false,
            slot,
            demand,
            arm,
            unpark,
            check,
        }
    }

    /// 轮询：条件满足则注销并返回 `Ready`；否则注册 waker（先 armed 登记
    /// demand，再注册槽位）返回 `Pending`。
    fn poll(&mut self, cx: &mut Context<'_>, core: &CircCore<P, C, T, A>, arg: usize) -> Poll<()> {
        if (self.check)(core, arg) {
            self.deregister();
            return Poll::Ready(());
        }
        self.waiter.waker = Some(cx.waker().clone());
        // 先 armed（写 demand → 置 STNDBY），再注册槽位：fire 侧经状态字
        // Acquire 读（armed）与置位 CAS 建立 happens-before，保证能读到 demand。
        (self.arm)(self.core, self.demand.clone());
        self.slot.register(&self.waiter);
        self.registered = true;
        // 注册后重新检查：注册与条件检查之间发生的状态变化不会丢失唤醒。
        if (self.check)(core, arg) {
            self.deregister();
            return Poll::Ready(());
        }
        Poll::Pending
    }

    fn deregister(&mut self) {
        if self.registered {
            self.slot.deregister(&self.waiter);
            // 清 armed 位与 demand（SPSC：同侧等待者先后，不与下一等待者竞争）。
            (self.unpark)(self.core);
            self.registered = false;
        }
    }
}

// ---------------------------------------------------------------------------
// 写等待（WriteAsync / WriteFuture）
// ---------------------------------------------------------------------------

/// 写端的异步段借出（见 [`TrBuffWrite::write_async`]）。
pub struct WriteAsync<'a, P, C, T, A>
where
    P: TrProducer<Data = T>,
    C: TrConsumer<Data = T>,
    A: TrMalloc + Clone,
{
    core: &'a CircCore<P, C, T, A>,
    min_len: usize,
    max_len: usize,
}

impl<'a, P, C, T, A> WriteAsync<'a, P, C, T, A>
where
    P: TrProducer<Data = T>,
    C: TrConsumer<Data = T>,
    A: TrMalloc + Clone,
{
    pub(super) fn new(core: &'a CircCore<P, C, T, A>, demand: &Demand<usize>) -> Self {
        WriteAsync {
            core,
            min_len: demand.min().copied().unwrap_or(0),
            max_len: demand.max().copied().unwrap_or(usize::MAX),
        }
    }
}

impl<'a, P, C, T, A> IntoFuture for WriteAsync<'a, P, C, T, A>
where
    P: Send + Sync + TrProducer<Data = T>,
    C: Send + Sync + TrConsumer<Data = T>,
    T: Send + Sync,
    A: Send + Sync + TrMalloc + Clone,
{
    type IntoFuture = WriteFuture<'a, P, C, T, A>;
    type Output = SomeOf<
        ReclSliceMut<'a, T, WriterReclaim<'a, CircCore<P, C, T, A>>>,
        TxError<usize>,
    >;

    fn into_future(self) -> Self::IntoFuture {
        WriteFuture::new(self.core, self.min_len, self.max_len)
    }
}

impl<'a, P, TyC, T, A> TrMayCancel<'a> for WriteAsync<'a, P, TyC, T, A>
where
    P: Send + Sync + TrProducer<Data = T>,
    TyC: Send + Sync + TrConsumer<Data = T>,
    T: Send + Sync,
    A: Send + Sync + TrMalloc + Clone,
{
    type MayCancelFuture<'f, C> = WriteFuture<'a, P, TyC, T, A>
    where
        Self: 'f,
        C: abs_buff::x_deps::abs_cancel::TrCancellationToken + Clone,
        C: 'a,
        C: 'f,
        'f: 'a;
    type MayCancelOutput = SomeOf<
        ReclSliceMut<'a, T, WriterReclaim<'a, CircCore<P, TyC, T, A>>>,
        TxError<usize>,
    >;

    fn may_cancel_with<'f, C>(
        self,
        _cancel: &'f mut C,
    ) -> Self::MayCancelFuture<'f, C>
    where
        Self: 'f,
        'f: 'a,
        C: abs_buff::x_deps::abs_cancel::TrCancellationToken + Clone,
    {
        // 当前不支持取消：忽略 token（与立即就绪型 future 的处理一致）。
        WriteFuture::new(self.core, self.min_len, self.max_len)
    }
}

/// 写等待 future：直到可写空间 ≥ 下限（或写端关闭）才借出写段。
///
/// # 设计意图
///
/// 等待 = 注册 + 重查：把 waker 注册进核心的被动生产端唤醒槽位（[`Park`]），
/// 对端读取释放空间时由 hook 唤醒；唤醒后重新检查条件，防丢失唤醒。条件满足
/// 或关闭时注销并返回。drop 时必然注销（`Drop` 实现），保证槽位不悬挂。
pub struct WriteFuture<'ctx, P, C, T, A>
where
    P: TrProducer<Data = T>,
    C: TrConsumer<Data = T>,
    A: TrMalloc + Clone,
{
    _pin: PhantomPinned,
    core: &'ctx CircCore<P, C, T, A>,
    min_len: usize,
    max_len: usize,
    park: Park<'ctx, P, C, T, A>,
}

/// 写者可以继续的条件（供 [`Park`] 检查）。
fn producer_ready<P, C, T, A>(core: &CircCore<P, C, T, A>, min: usize) -> bool
where
    P: Send + Sync + TrProducer<Data = T>,
    C: Send + Sync + TrConsumer<Data = T>,
    T: Send + Sync,
    A: Send + Sync + TrMalloc + Clone,
{
    core.producer_ready(min)
}

impl<'ctx, P, C, T, A> WriteFuture<'ctx, P, C, T, A>
where
    P: Send + Sync + TrProducer<Data = T>,
    C: Send + Sync + TrConsumer<Data = T>,
    T: Send + Sync,
    A: Send + Sync + TrMalloc + Clone,
{
    fn new(core: &'ctx CircCore<P, C, T, A>, min_len: usize, max_len: usize) -> Self {
        WriteFuture {
            _pin: PhantomPinned,
            core,
            min_len,
            max_len,
            park: Park::new(
                core,
                core.producer_wake_slot(),
                CircCore::arm_producer,
                CircCore::unpark_producer,
                producer_ready::<P, C, T, A>,
                Some(demand_of(min_len, max_len)),
            ),
        }
    }
}

impl<'ctx, P, C, T, A> Future for WriteFuture<'ctx, P, C, T, A>
where
    P: Send + Sync + TrProducer<Data = T>,
    C: Send + Sync + TrConsumer<Data = T>,
    T: Send + Sync,
    A: Send + Sync + TrMalloc + Clone,
{
    type Output = SomeOf<
        ReclSliceMut<'ctx, T, WriterReclaim<'ctx, CircCore<P, C, T, A>>>,
        TxError<usize>,
    >;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        // 主动生产端不对外暴露：等待直接返回错误。
        if !this.core.producer_is_passive() {
            this.park.deregister();
            return Poll::Ready(SomeOf::new_right(TxError::Unavailable));
        }
        // 对端（消费端）为主动设备时，先驱动一轮输出泵（冲刷、释放空间）——
        // 泵自身的读提交会唤醒本等待者；对端被动时不驱动（无意义且可能别名
        // 用户活段）。
        if !this.core.consumer_is_passive() {
            this.core.drive_output();
        }
        loop {
            match this
                .core
                .try_write_at(&demand_of(this.min_len, this.max_len))
            {
                Ok((start, take)) => {
                    if take < this.min_len {
                        // 不足下限：继续等待（`demand_of` 已保证 take ≥ min）。
                        if this.park.poll(cx, this.core, this.min_len).is_pending() {
                            return Poll::Pending;
                        }
                        continue;
                    }
                    this.park.deregister();
                    return Poll::Ready(SomeOf::new_left(this.core.write_segm(start, take)));
                }
                Err(TxError::Closing) => {
                    this.park.deregister();
                    return Poll::Ready(SomeOf::new_right(TxError::Closing));
                }
                Err(_) => {
                    // 空间不足：等待（被唤醒后重新检查）。
                    if this.park.poll(cx, this.core, this.min_len).is_pending() {
                        return Poll::Pending;
                    }
                }
            }
        }
    }
}

impl<'ctx, P, C, T, A> Drop for WriteFuture<'ctx, P, C, T, A>
where
    P: TrProducer<Data = T>,
    C: TrConsumer<Data = T>,
    A: TrMalloc + Clone,
{
    fn drop(&mut self) {
        self.park.deregister();
    }
}

// ---------------------------------------------------------------------------
// 读等待（ReadAsync / ReadFuture）
// ---------------------------------------------------------------------------

/// 读端的异步段借出（见 [`TrBuffRead::read_async`]）。
pub struct ReadAsync<'a, P, C, T, A>
where
    P: TrProducer<Data = T>,
    C: TrConsumer<Data = T>,
    A: TrMalloc + Clone,
{
    core: &'a CircCore<P, C, T, A>,
    min_len: usize,
    max_len: usize,
}

impl<'a, P, C, T, A> ReadAsync<'a, P, C, T, A>
where
    P: TrProducer<Data = T>,
    C: TrConsumer<Data = T>,
    A: TrMalloc + Clone,
{
    pub(super) fn new(core: &'a CircCore<P, C, T, A>, demand: &Demand<usize>) -> Self {
        ReadAsync {
            core,
            min_len: demand.min().copied().unwrap_or(0),
            max_len: demand.max().copied().unwrap_or(usize::MAX),
        }
    }
}

impl<'a, P, C, T, A> IntoFuture for ReadAsync<'a, P, C, T, A>
where
    P: Send + Sync + TrProducer<Data = T>,
    C: Send + Sync + TrConsumer<Data = T>,
    T: Send + Sync,
    A: Send + Sync + TrMalloc + Clone,
{
    type IntoFuture = ReadFuture<'a, P, C, T, A>;
    type Output = SomeOf<
        ReclSliceRef<'a, T, ReaderReclaim<'a, CircCore<P, C, T, A>>>,
        RxError<usize>,
    >;

    fn into_future(self) -> Self::IntoFuture {
        ReadFuture::new(self.core, self.min_len, self.max_len)
    }
}

impl<'a, P, TyC, T, A> TrMayCancel<'a> for ReadAsync<'a, P, TyC, T, A>
where
    P: Send + Sync + TrProducer<Data = T>,
    TyC: Send + Sync + TrConsumer<Data = T>,
    T: Send + Sync,
    A: Send + Sync + TrMalloc + Clone,
{
    type MayCancelFuture<'f, C> = ReadFuture<'a, P, TyC, T, A>
    where
        Self: 'f,
        C: abs_buff::x_deps::abs_cancel::TrCancellationToken + Clone,
        C: 'a,
        C: 'f,
        'f: 'a;
    type MayCancelOutput = SomeOf<
        ReclSliceRef<'a, T, ReaderReclaim<'a, CircCore<P, TyC, T, A>>>,
        RxError<usize>,
    >;

    fn may_cancel_with<'f, C>(
        self,
        _cancel: &'f mut C,
    ) -> Self::MayCancelFuture<'f, C>
    where
        Self: 'f,
        'f: 'a,
        C: abs_buff::x_deps::abs_cancel::TrCancellationToken + Clone,
    {
        ReadFuture::new(self.core, self.min_len, self.max_len)
    }
}

/// 读等待 future：直到可读数据 ≥ 下限（或关闭：EOF）才借出读段。
///
/// 与 [`WriteFuture`] 对称：注册进核心的被动消费端唤醒槽位，生产端写入 /
/// 关闭时由 hook 唤醒，重查条件后返回或继续等待。
pub struct ReadFuture<'ctx, P, C, T, A>
where
    P: TrProducer<Data = T>,
    C: TrConsumer<Data = T>,
    A: TrMalloc + Clone,
{
    _pin: PhantomPinned,
    core: &'ctx CircCore<P, C, T, A>,
    min_len: usize,
    max_len: usize,
    park: Park<'ctx, P, C, T, A>,
}

/// 读者可以继续的条件（供 [`Park`] 检查）。
fn consumer_ready<P, C, T, A>(core: &CircCore<P, C, T, A>, min: usize) -> bool
where
    P: Send + Sync + TrProducer<Data = T>,
    C: Send + Sync + TrConsumer<Data = T>,
    T: Send + Sync,
    A: Send + Sync + TrMalloc + Clone,
{
    core.consumer_ready(min)
}

impl<'ctx, P, C, T, A> ReadFuture<'ctx, P, C, T, A>
where
    P: Send + Sync + TrProducer<Data = T>,
    C: Send + Sync + TrConsumer<Data = T>,
    T: Send + Sync,
    A: Send + Sync + TrMalloc + Clone,
{
    fn new(core: &'ctx CircCore<P, C, T, A>, min_len: usize, max_len: usize) -> Self {
        ReadFuture {
            _pin: PhantomPinned,
            core,
            min_len,
            max_len,
            park: Park::new(
                core,
                core.consumer_wake_slot(),
                CircCore::arm_consumer,
                CircCore::unpark_consumer,
                consumer_ready::<P, C, T, A>,
                Some(demand_of(min_len, max_len)),
            ),
        }
    }
}

impl<'ctx, P, C, T, A> Future for ReadFuture<'ctx, P, C, T, A>
where
    P: Send + Sync + TrProducer<Data = T>,
    C: Send + Sync + TrConsumer<Data = T>,
    T: Send + Sync,
    A: Send + Sync + TrMalloc + Clone,
{
    type Output = SomeOf<
        ReclSliceRef<'ctx, T, ReaderReclaim<'ctx, CircCore<P, C, T, A>>>,
        RxError<usize>,
    >;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        // 主动消费端不对外暴露：等待直接返回错误。
        if !this.core.consumer_is_passive() {
            this.park.deregister();
            return Poll::Ready(SomeOf::new_right(RxError::Unavailable));
        }
        // 对端（生产端）为主动设备时，先驱动一轮输入泵（拉取数据）——泵自身
        // 的写提交会唤醒本等待者；对端被动时不驱动（无意义且可能别名用户活段）。
        if !this.core.producer_is_passive() {
            this.core.drive_input();
        }
        loop {
            match this.core.try_read_at(&demand_of(this.min_len, this.max_len)) {
                Ok((start, take)) => {
                    // EOF 例外：写端已关闭时允许返回不足下限的部分数据。
                    if take < this.min_len && !this.core.is_tx_closed() {
                        if this.park.poll(cx, this.core, this.min_len).is_pending() {
                            return Poll::Pending;
                        }
                        continue;
                    }
                    this.park.deregister();
                    return Poll::Ready(SomeOf::new_left(this.core.read_segm(start, take)));
                }
                Err(RxError::Closing) => {
                    this.park.deregister();
                    return Poll::Ready(SomeOf::new_right(RxError::Closing));
                }
                Err(_) => {
                    // 数据不足：等待（被唤醒后重新检查）。
                    if this.park.poll(cx, this.core, this.min_len).is_pending() {
                        return Poll::Pending;
                    }
                }
            }
        }
    }
}

impl<'ctx, P, C, T, A> Drop for ReadFuture<'ctx, P, C, T, A>
where
    P: TrProducer<Data = T>,
    C: TrConsumer<Data = T>,
    A: TrMalloc + Clone,
{
    fn drop(&mut self) {
        self.park.deregister();
    }
}
