//! 读侧适配器：[`IrohReader`]。
//!
//! 基于 `circular_buff`：`RecvStream` 作为**主动生产端**（设备见
//! [`super::device::StreamInput`]，try-once 读），用户经被动消费端半部以
//! [`TrBuffTryRead`] / [`TrBuffRead`] 消费。
//!
//! # 无后台任务模型
//!
//! 不 spawn 任何任务。网络数据只在用户操作时被拉取：
//!
//! * [`TrBuffTryRead::try_read`] 先显式 `drive()` 拉一轮（try-once、非阻塞），
//!   再借出缓冲段；当前确无数据则返回 `Drained`（正确的 try 语义）；
//! * [`TrBuffRead::read_async`] 返回一个循环 future：反复 `drive()` + 尝试，
//!   数据未到则 `yield` 让运行时处理网络，直到有数据 / EOF / 错误；
//! * 流结束（EOF）或读错误经共享状态合成 [`RxError::Closing`]（错误详情经
//!   [`IrohReader::take_error`] 取回）。

use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

use abs_buff::{
    Demand, TrBuffRead, TrBuffTryRead,
    x_deps::abs_cancel::TrMayCancel,
};
use anylr::SomeOf;
use buffex::circular_buff::{
    BuffConsumer, CircularBuffBuilder, Consumer, CoreAlloc, DeviceProducer, RxError,
};
use iroh::endpoint::{ReadError, RecvStream};

use super::{common::sanitize_capacity, device::StreamInput};

/// 内部缓冲的消费端半部（被动消费端 = 调用者驱动）。
type Inner = Consumer<
    DeviceProducer<StreamInput, u8>,
    BuffConsumer<u8>,
    u8,
    CoreAlloc,
>;

/// 读半部（读端适配器）of a buffered iroh stream。
///
/// 实现 [`TrBuffRead`] / [`TrBuffTryRead`]，底层是 `circular_buff` 的被动
/// 消费端半部。
pub struct IrohReader {
    rx: Inner,
    /// 输入设备是否已到 EOF（或出错）：之后缓冲读空即合成 `Closing`。
    eof: Arc<AtomicBool>,
    /// 最近一次网络读错误（经 [`IrohReader::take_error`] 取回）。
    err: Arc<Mutex<Option<ReadError>>>,
}

impl IrohReader {
    /// 把一个 QUIC 接收流包装成 `cap` 字节的缓冲读端。
    ///
    /// 构造即 `drive()` 拉一轮（有数据则入缓冲，无则立即返回，不阻塞）。
    /// 不 spawn 任何任务。
    pub fn new(stream: RecvStream, cap: usize) -> Self {
        let eof = Arc::new(AtomicBool::new(false));
        let err = Arc::new(Mutex::new(None));
        let input = StreamInput::new(stream, eof.clone(), err.clone());
        let (_tx, rx) = CircularBuffBuilder::with_capacity(sanitize_capacity(cap))
            .pipe_from_input(input)
            .consumer_passive()
            .build()
            .expect("valid iroh buffer capacity");
        Self { rx, eof, err }
    }

    /// Report the last network read error, if any.
    pub fn take_error(&self) -> Option<ReadError> {
        self.err.lock().ok().and_then(|mut guard| guard.take())
    }

    /// Close the read side. 无后台任务可等待；数据流随后经 read 端报
    /// `Closing`（EOF）或 `Drained`。
    pub async fn shutdown(mut self) {
        self.rx.close();
    }

    /// EOF 已到且缓冲已空：之后读取应报 `Closing`。
    fn eof_drained(&self) -> bool {
        self.eof.load(Ordering::Acquire) && self.rx.data_size() == 0
    }
}

impl Drop for IrohReader {
    fn drop(&mut self) {
        self.rx.close();
    }
}

/// 把 `[min, max]` 重构为 `Demand`（处理 0 / `usize::MAX` 边界）。
fn demand_of(min: Option<usize>, max: Option<usize>) -> Demand<usize> {
    match (min, max) {
        (None, None) => Demand::at_least(1),
        (None, Some(m)) => Demand::less_than(m),
        (Some(n), None) => Demand::at_least(n),
        (Some(n), Some(m)) => Demand::between(n, m),
    }
}

impl TrBuffRead<u8> for IrohReader {
    type ReadAsync<'f> = IrohReadAsync<'f> where Self: 'f;
    type SegmRef<'f> = <Inner as TrBuffRead<u8>>::SegmRef<'f> where Self: 'f;
    type Err = <Inner as TrBuffRead<u8>>::Err;

    fn is_drained_closing(&self) -> bool {
        self.rx.is_drained_closing() || self.eof_drained()
    }

    fn read_async<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> Self::ReadAsync<'f> {
        IrohReadAsync {
            reader: self,
            min: demand.min().copied(),
            max: demand.max().copied(),
            state: State::Try,
        }
    }
}

impl TrBuffTryRead<u8> for IrohReader {
    fn try_read<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> SomeOf<Self::SegmRef<'f>, Self::Err> {
        // 底层半部在对端（生产端）为主动时已自动 drive 拉一轮（try-once、
        // 非阻塞）——适配器无需手动驱动。
        let some = self.rx.try_read(demand);
        // EOF 合成：空 + 设备已 EOF → Closing（错误详情经 take_error 取回）。
        if let Some(RxError::Drained(_)) = some.as_ref().pick_right() {
            if self.eof.load(Ordering::Acquire) {
                return SomeOf::new_right(RxError::Closing);
            }
        }
        some
    }
}

// ---------------------------------------------------------------------------
// read_async 的循环 future：drive + 尝试，数据未到则 yield
// ---------------------------------------------------------------------------

/// 循环驱动的读等待 future：反复「拉网络数据 → 尝试借段」，直到有数据 /
/// EOF / 错误。不 spawn——等待发生在调用者的任务里（`yield` 让出）。
///
/// 持有**共享** `&'f IrohReader`（共享引用可 Copy，借出的段才能绑定到结构体
/// 自身的 `'f`；读路径经 [`Consumer::try_read_shared`] 以 `&self` 完成）。
pub struct IrohReadAsync<'f> {
    reader: &'f IrohReader,
    min: Option<usize>,
    max: Option<usize>,
    state: State,
}

enum State {
    /// 下一轮：拉网络 + 尝试借段。
    Try,
    /// 数据未到：让出运行时（tokio 定时器零延时），之后回到 `Try`。
    Yield(tokio::time::Sleep),
}

impl<'f> Future for IrohReadAsync<'f> {
    type Output = SomeOf<<IrohReader as TrBuffRead<u8>>::SegmRef<'f>, RxError<usize>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // 本 future 无自引用字段（`Sleep` 只在原地轮询、从不移动），
        // 故 get_unchecked_mut 与对 `Sleep` 的 new_unchecked 均安全。
        let this = unsafe { self.get_unchecked_mut() };
        loop {
            match &mut this.state {
                State::Try => {
                    // 底层 `try_read_shared` 在对端（生产端）为主动时已自动
                    // drive 拉一轮——适配器无需手动驱动。
                    let demand = demand_of(this.min, this.max);
                    let some = this.reader.rx.try_read_shared(&demand);
                    if some.as_ref().pick_left().is_some() {
                        return Poll::Ready(SomeOf::new_left(
                            some.pick_left().expect("checked above"),
                        ));
                    }
                    match some.pick_right() {
                        Some(RxError::Drained(_)) => {
                            if this.reader.eof.load(Ordering::Acquire) {
                                // 空 + 设备已 EOF：合成 Closing。
                                return Poll::Ready(SomeOf::new_right(RxError::Closing));
                            }
                            // 暂无数据：让出运行时处理网络，下轮再拉。
                            this.state =
                                State::Yield(tokio::time::sleep(core::time::Duration::ZERO));
                        }
                        Some(err) => return Poll::Ready(SomeOf::new_right(err)),
                        None => unreachable!("SomeOf has no both variant here"),
                    }
                }
                State::Yield(sleep) => {
                    // SAFETY: `sleep` 位于被 pin 的本 future 内部，原地轮询、
                    // 不移动。
                    let fut = unsafe { Pin::new_unchecked(sleep) };
                    if fut.poll(cx).is_pending() {
                        return Poll::Pending;
                    }
                    this.state = State::Try;
                }
            }
        }
    }
}

impl<'f> TrMayCancel<'f> for IrohReadAsync<'f>
where
    Self: 'f,
{
    type MayCancelFuture<'g, C> = IrohReadAsync<'f>
    where
        Self: 'g,
        C: abs_buff::x_deps::abs_cancel::TrCancellationToken + Clone,
        C: 'f,
        C: 'g,
        'g: 'f;
    type MayCancelOutput = <Self as Future>::Output;

    fn may_cancel_with<'g, C>(
        self,
        _cancel: &'g mut C,
    ) -> Self::MayCancelFuture<'g, C>
    where
        Self: 'g,
        'g: 'f,
        C: abs_buff::x_deps::abs_cancel::TrCancellationToken + Clone,
    {
        // 当前不支持取消：忽略 token（与立即就绪型 future 的处理一致）。
        self
    }
}
