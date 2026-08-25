//! 被动半部：对外可访问的写端（生产者）与读端（消费者）。
//!
//! 半部借用环形核心（`&'s RingCore`），实现 `abs_buff` 的
//! `TrBuffTryWrite` / `TrBuffTryRead`，并尊重 `Demand` 的 `[min, max]` 语义：
//! **可写空间 / 可读数据不足下限时不返回**（EOF 例外：写端已关闭时返回现有
//! 部分，见 [`super::core_`] 的 `try_read_at`）。
//!
//! 异步等待（`write_async` / `read_async`）把 waker 注册进核心的被动 hook
//! 槽位：对端完成读取 / 写入时，hook 唤醒等待者，等待者重新检查条件。

use core::{
    future::{Future, IntoFuture},
    marker::PhantomPinned,
    pin::Pin,
    task::{Context, Poll},
};

use abs_buff::{
    Demand, TrBuffRead, TrBuffTryRead, TrBuffTryWrite, TrBuffWrite,
    x_deps::{abs_cancel::TrMayCancel, anylr::SomeOf},
};

use super::{
    core_::{CircCore, WakeSlot},
    error_::{RxError, TxError},
    segm_::{RdSegm, WrSegm},
};

// ---------------------------------------------------------------------------
// 写（生产）半部
// ---------------------------------------------------------------------------

/// 被动生产端的写半部：借用环形核心，实现 `TrBuffTryWrite`。
pub struct ProducerHalf<'s, T = u8> {
    core: &'s CircCore<T>,
}

impl<'s, T> ProducerHalf<'s, T> {
    pub(super) fn new(core: &'s CircCore<T>) -> Self {
        ProducerHalf { core }
    }

    /// 环形缓冲容量。
    pub fn capacity(&self) -> usize {
        self.core.capacity()
    }

    /// 当前可写空间。
    pub fn free_size(&self) -> usize {
        self.core.free_size()
    }

    /// 当前可读数据量（观察用）。
    pub fn data_size(&self) -> usize {
        self.core.data_size()
    }

    /// 写端（本端）是否已关闭。
    pub fn is_closed(&self) -> bool {
        self.core.is_tx_closed()
    }

    /// 消费端（对端）是否已关闭。
    pub fn is_consumer_closed(&self) -> bool {
        self.core.is_rx_closed()
    }

    /// 关闭写端：不再写入，触发消费端 hook（`ProducerClose`）。
    pub fn close(&mut self) {
        self.core.close_tx();
    }
}

impl<'s, T> TrBuffWrite<T> for ProducerHalf<'s, T> {
    type WriteAsync<'f> = WriteAsync<'f, T> where Self: 'f;
    type SegmMut<'f> = WrSegm<'f, T> where Self: 'f;
    type Err = TxError<usize>;

    #[inline]
    fn is_blocked_closing(&self) -> bool {
        self.core.is_tx_closed() || !self.core.producer_ready(1)
    }

    #[inline]
    fn write_async<'f>(&'f mut self, demand: &Demand<usize>) -> Self::WriteAsync<'f> {
        WriteAsync::new(self.core, demand)
    }
}

impl<'s, T> TrBuffTryWrite<T> for ProducerHalf<'s, T> {
    #[inline]
    fn try_write<'f>(&'f mut self, demand: &Demand<usize>) -> SomeOf<Self::SegmMut<'f>, Self::Err> {
        let min_len = demand.min().copied().unwrap_or(0);
        match self.core.try_write_at(demand) {
            Ok((start, take)) => {
                if take < min_len {
                    // 不足下限（理论上 try_write_at 已保证，这里是双重保险）。
                    let e = if self.core.is_tx_closed() {
                        TxError::Closing
                    } else {
                        TxError::Stuffed(start)
                    };
                    SomeOf::new_right(e)
                } else {
                    SomeOf::new_left(self.core.write_segm(start, take))
                }
            }
            Err(err) => SomeOf::new_right(err),
        }
    }
}

// ---------------------------------------------------------------------------
// 读（消费）半部
// ---------------------------------------------------------------------------

/// 被动消费端的读半部：借用环形核心，实现 `TrBuffTryRead`。
pub struct ConsumerHalf<'s, T = u8> {
    core: &'s CircCore<T>,
}

impl<'s, T> ConsumerHalf<'s, T> {
    pub(super) fn new(core: &'s CircCore<T>) -> Self {
        ConsumerHalf { core }
    }

    /// 环形缓冲容量。
    pub fn capacity(&self) -> usize {
        self.core.capacity()
    }

    /// 当前可读数据量。
    pub fn data_size(&self) -> usize {
        self.core.data_size()
    }

    /// 当前可写空间（观察用）。
    pub fn free_size(&self) -> usize {
        self.core.free_size()
    }

    /// 读端（本端）是否已关闭。
    pub fn is_closed(&self) -> bool {
        self.core.is_rx_closed()
    }

    /// 生产端（对端）是否已关闭（EOF）。
    pub fn is_producer_closed(&self) -> bool {
        self.core.is_tx_closed()
    }

    /// 关闭读端：不再读取，触发生产端 hook（`ConsumerClose`）。
    pub fn close(&mut self) {
        self.core.close_rx();
    }
}

impl<'s, T> TrBuffRead<T> for ConsumerHalf<'s, T> {
    type ReadAsync<'f> = ReadAsync<'f, T> where Self: 'f;
    type SegmRef<'f> = RdSegm<'f, T> where Self: 'f;
    type Err = RxError<usize>;

    #[inline]
    fn is_drained_closing(&self) -> bool {
        self.core.is_rx_closed() || self.core.is_tx_closed()
    }

    #[inline]
    fn read_async<'f>(&'f mut self, demand: &Demand<usize>) -> Self::ReadAsync<'f> {
        ReadAsync::new(self.core, demand)
    }
}

impl<'s, T> TrBuffTryRead<T> for ConsumerHalf<'s, T> {
    #[inline]
    fn try_read<'f>(&'f mut self, demand: &Demand<usize>) -> SomeOf<Self::SegmRef<'f>, Self::Err> {
        let min_len = demand.min().copied().unwrap_or(0);
        match self.core.try_read_at(demand) {
            Ok((start, take)) => {
                // EOF 例外：写端已关闭时允许返回不足下限的部分数据。
                if take < min_len && !self.core.is_tx_closed() {
                    let e = if self.core.is_rx_closed() {
                        RxError::Closing
                    } else {
                        RxError::Drained(start)
                    };
                    SomeOf::new_right(e)
                } else {
                    SomeOf::new_left(self.core.read_segm(start, take))
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

/// 等待辅助：把 waker 注册进核心的被动 hook 槽位；条件满足或关闭时返回
/// `Ready`，否则 `Pending`。注册后**重新检查条件**，以关闭丢失唤醒窗口。
struct Park<'a, T> {
    waiter: super::core_::Waiter,
    registered: bool,
    slot: &'a WakeSlot,
    check: fn(&CircCore<T>, usize) -> bool,
}

impl<'a, T> Park<'a, T> {
    fn new(slot: &'a WakeSlot, check: fn(&CircCore<T>, usize) -> bool) -> Self {
        Park {
            waiter: super::core_::Waiter::new(),
            registered: false,
            slot,
            check,
        }
    }

    /// 轮询：条件满足则注销并返回 `Ready`；否则注册 waker 并返回 `Pending`。
    fn poll(&mut self, cx: &mut Context<'_>, core: &CircCore<T>, arg: usize) -> Poll<()> {
        if (self.check)(core, arg) {
            self.deregister();
            return Poll::Ready(());
        }
        self.waiter.waker = Some(cx.waker().clone());
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
            self.registered = false;
        }
    }
}

// ---------------------------------------------------------------------------
// 写等待（WriteAsync / WriteFuture）
// ---------------------------------------------------------------------------

/// 写端的异步段借出（见 [`ProducerHalf::write_async`] 与 `TrBuffWrite`）。
pub struct WriteAsync<'a, T>
where
    T: 'a,
{
    core: &'a CircCore<T>,
    min_len: usize,
    max_len: usize,
}

impl<'a, T> WriteAsync<'a, T> {
    pub(super) fn new(core: &'a CircCore<T>, demand: &Demand<usize>) -> Self {
        WriteAsync {
            core,
            min_len: demand.min().copied().unwrap_or(0),
            max_len: demand.max().copied().unwrap_or(usize::MAX),
        }
    }
}

impl<'a, T> IntoFuture for WriteAsync<'a, T> {
    type IntoFuture = WriteFuture<'a, T>;
    type Output = SomeOf<WrSegm<'a, T>, TxError<usize>>;

    fn into_future(self) -> Self::IntoFuture {
        WriteFuture::new(self.core, self.min_len, self.max_len)
    }
}

impl<'a, T> TrMayCancel<'a> for WriteAsync<'a, T>
where
    T: 'a,
{
    type MayCancelFuture<'f, C> = WriteFuture<'a, T>
    where
        Self: 'f,
        C: abs_buff::x_deps::abs_cancel::TrCancellationToken + Clone,
        C: 'a,
        C: 'f,
        'f: 'a;
    type MayCancelOutput = SomeOf<WrSegm<'a, T>, TxError<usize>>;

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
pub struct WriteFuture<'ctx, T>
where
    T: 'ctx,
{
    _pin: PhantomPinned,
    core: &'ctx CircCore<T>,
    min_len: usize,
    max_len: usize,
    park: Park<'ctx, T>,
}

impl<'ctx, T> WriteFuture<'ctx, T> {
    fn new(core: &'ctx CircCore<T>, min_len: usize, max_len: usize) -> Self {
        WriteFuture {
            _pin: PhantomPinned,
            core,
            min_len,
            max_len,
            park: Park::new(core.producer_wake_slot(), producer_ready),
        }
    }
}

/// 写者可以继续的条件（供 [`Park`] 检查）。
fn producer_ready<T>(core: &CircCore<T>, min: usize) -> bool {
    core.producer_ready(min)
}

impl<'ctx, T> Future for WriteFuture<'ctx, T> {
    type Output = SomeOf<WrSegm<'ctx, T>, TxError<usize>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
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

impl<'ctx, T> Drop for WriteFuture<'ctx, T> {
    fn drop(&mut self) {
        self.park.deregister();
    }
}

// ---------------------------------------------------------------------------
// 读等待（ReadAsync / ReadFuture）
// ---------------------------------------------------------------------------

/// 读端的异步段借出（见 [`ConsumerHalf::read_async`] 与 `TrBuffRead`）。
pub struct ReadAsync<'a, T>
where
    T: 'a,
{
    core: &'a CircCore<T>,
    min_len: usize,
    max_len: usize,
}

impl<'a, T> ReadAsync<'a, T> {
    pub(super) fn new(core: &'a CircCore<T>, demand: &Demand<usize>) -> Self {
        ReadAsync {
            core,
            min_len: demand.min().copied().unwrap_or(0),
            max_len: demand.max().copied().unwrap_or(usize::MAX),
        }
    }
}

impl<'a, T> IntoFuture for ReadAsync<'a, T> {
    type IntoFuture = ReadFuture<'a, T>;
    type Output = SomeOf<RdSegm<'a, T>, RxError<usize>>;

    fn into_future(self) -> Self::IntoFuture {
        ReadFuture::new(self.core, self.min_len, self.max_len)
    }
}

impl<'a, T> TrMayCancel<'a> for ReadAsync<'a, T>
where
    T: 'a,
{
    type MayCancelFuture<'f, C> = ReadFuture<'a, T>
    where
        Self: 'f,
        C: abs_buff::x_deps::abs_cancel::TrCancellationToken + Clone,
        C: 'a,
        C: 'f,
        'f: 'a;
    type MayCancelOutput = SomeOf<RdSegm<'a, T>, RxError<usize>>;

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
pub struct ReadFuture<'ctx, T>
where
    T: 'ctx,
{
    _pin: PhantomPinned,
    core: &'ctx CircCore<T>,
    min_len: usize,
    max_len: usize,
    park: Park<'ctx, T>,
}

impl<'ctx, T> ReadFuture<'ctx, T> {
    fn new(core: &'ctx CircCore<T>, min_len: usize, max_len: usize) -> Self {
        ReadFuture {
            _pin: PhantomPinned,
            core,
            min_len,
            max_len,
            park: Park::new(core.consumer_wake_slot(), consumer_ready),
        }
    }
}

/// 读者可以继续的条件（供 [`Park`] 检查）。
fn consumer_ready<T>(core: &CircCore<T>, min: usize) -> bool {
    core.consumer_ready(min)
}

impl<'ctx, T> Future for ReadFuture<'ctx, T> {
    type Output = SomeOf<RdSegm<'ctx, T>, RxError<usize>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
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

impl<'ctx, T> Drop for ReadFuture<'ctx, T> {
    fn drop(&mut self) {
        self.park.deregister();
    }
}
