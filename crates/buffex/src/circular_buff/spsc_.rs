//! 提供给最终用户、暴露的公共接口 Producer 和 Consumer。
//! 所有对 Circular Buffer 的操作都必须通过这两个实例。
//! 如果有一端在 Circular Buffer 构建时就已经被指定（主动模式），那么这一端
//! 的半部虽然存在，但操作返回错误（不对外暴露，见 [`TxError::Unavailable`] /
//! [`RxError::Unavailable`]）。
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
pub(super) type CoreRef<P, C, T, A> = Shared<CircCore<P, C, T, A>, A>;

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
/// 分配器（默认 `CoreAlloc`）。若生产端为主动模式（`P = DeviceProducer`），
/// 本半部的写操作返回 [`TxError::Unavailable`]。
pub struct Producer<P, C, T, A>
where
    P: TrProducer<Data = T>,
    C: TrConsumer<Data = T>,
    A: TrMalloc + Clone,
{
    core_ref_: CoreRef<P, C, T, A>,
}

/// 消费端半部：与 [`Producer`] 对称（读路径）。若消费端为主动模式
/// （`C = DeviceConsumer`），读操作返回 [`RxError::Unavailable`]。
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
