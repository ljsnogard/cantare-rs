//! iroh QUIC 流的设备适配：作为 `circular_buff` 的**主动端**
//! （[`StreamInput`] 实现 `TrInput`、[`StreamOutput`] 实现 `TrOutput`）。
//!
//! # 无后台任务模型（不 spawn）
//!
//! `circular_buff` 的主动端由 **hook 同步泵**驱动（不 spawn 任何任务）。网络
//! 流是 tokio 异步的，因此本模块把两种语义钉在设备上：
//!
//! * **读（[`StreamInput`]）——try-once（非阻塞）**：`read_async` 只轮询一次
//!   `RecvStream::read`，当前无数据立即返回 0（泵停止）。数据由适配器在每次
//!   用户操作前显式 `drive()` 拉取（见 [`crate::IrohReader`]）。
//! * **写（[`StreamOutput`]）——同步阻塞**：`write_async` 用 `Waker::noop()`
//!   自旋轮询 `SendStream::write_all` 到完成，保证泵冲刷的数据必然送达（否则
//!   缓冲满时写者 park、泵未写、无人唤醒，会死锁）。
//!
//! `stream` 放在共享盒（`Arc<Mutex<Option<_>>>`）中：EOF / 错误记入共享状态
//! 供适配器合成 `Closing` / `take_error`；适配器在 `shutdown` 时可取回流
//! 执行 `finish()`。

use std::{
    future::Future,
    mem::MaybeUninit,
    pin::{Pin, pin},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Waker},
};

use abs_buff::{
    io::{TrInput, TrOutput},
    x_deps::{
        abs_cancel::{TrCancellationToken, TrMayCancel},
        anylr::SomeOf,
    },
};
use iroh::endpoint::{ReadError, RecvStream, SendStream, WriteError};

// ---------------------------------------------------------------------------
// 已就绪的 future（设备操作在方法内同步完成后返回它）
// ---------------------------------------------------------------------------

/// 一个立即就绪的 `TrMayCancel` future：包装设备操作的同步结果。
#[doc(hidden)]
pub struct ReadyIo<S, E>(Option<SomeOf<S, E>>);

impl<S, E> ReadyIo<S, E> {
    fn new(value: SomeOf<S, E>) -> Self {
        ReadyIo(Some(value))
    }
}

impl<S, E> Future for ReadyIo<S, E> {
    type Output = SomeOf<S, E>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        // `SomeOf` 内部含 `PhantomPinned`（anylr 的类型设计），无自引用字段，
        // 按既有的就绪 future 惯例使用 get_unchecked_mut。
        let this = unsafe { self.get_unchecked_mut() };
        Poll::Ready(this.0.take().expect("ready future polled once"))
    }
}

impl<'f, S: 'f, E: 'f> TrMayCancel<'f> for ReadyIo<S, E> {
    type MayCancelFuture<'g, C> = ReadyIo<S, E>
    where
        Self: 'g,
        C: TrCancellationToken + Clone,
        C: 'f,
        C: 'g,
        'g: 'f;
    type MayCancelOutput = SomeOf<S, E>;

    fn may_cancel_with<'g, C>(
        self,
        _cancel: &'g mut C,
    ) -> Self::MayCancelFuture<'g, C>
    where
        Self: 'g,
        'g: 'f,
        C: TrCancellationToken + Clone,
    {
        self
    }
}

// ---------------------------------------------------------------------------
// 轮询辅助
// ---------------------------------------------------------------------------

/// 用 noop waker 把 future 轮询到完成（同步阻塞；无执行器、不注册任务）。
fn block_on<F: Future>(fut: F) -> F::Output {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let mut fut = pin!(fut);
    loop {
        if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
            return v;
        }
    }
}

/// 只轮询一次（非阻塞）：返回当前状态，不等待。
fn poll_once<F: Future>(fut: Pin<&mut F>) -> Poll<F::Output> {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    fut.poll(&mut cx)
}

// ---------------------------------------------------------------------------
// 输入设备（RecvStream → 缓冲）
// ---------------------------------------------------------------------------

/// 输入设备：把 QUIC `RecvStream` 作为 `circular_buff` 的主动生产端。
///
/// `read_async` **只轮询一次**（try-once）：网络当前无数据时返回 0（泵停止），
/// 不阻塞、不等待。数据由适配器在每次用户操作前显式 `drive()` 拉取。
///
/// 字段说明（哪些需要共享、哪些不需要）：
///
/// * `stream`——**普通字段**：只有泵（单线程，经核心的 `&mut` 纪律）访问它，
///   读侧从不取回流（`RecvStream` drop 即停止、无需 finish）——不需要
///   `Arc<Mutex>`；
/// * `eof` / `err`——**共享句柄**：设备在核心内，适配器无法直接观察其状态，
///   故经 `Arc` 共享。`eof` 用 `AtomicBool`（同线程读写，但 Arc 内容必须
///   `Sync` 以维持设备的 `Send + Sync`）；`err` 用 `Mutex`（`take_error` 是
///   公开 `&self`，可跨线程读取，与设备的写入无类型层面互斥）。
#[doc(hidden)]
pub struct StreamInput {
    stream: RecvStream,
    eof: Arc<AtomicBool>,
    err: Arc<Mutex<Option<ReadError>>>,
}

impl StreamInput {
    pub fn new(
        stream: RecvStream,
        eof: Arc<AtomicBool>,
        err: Arc<Mutex<Option<ReadError>>>,
    ) -> Self {
        StreamInput { stream, eof, err }
    }
}

impl TrInput<u8> for StreamInput {
    type ReadAsync<'f> = ReadyIo<usize, ReadError> where Self: 'f;
    type Err = ReadError;

    fn read_async<'f>(
        &'f mut self,
        target: &'f mut [MaybeUninit<u8>],
    ) -> Self::ReadAsync<'f> {
        // SAFETY: 以 u8 视图写入未初始化内存；u8 无 drop 需求，
        // 写入的字节即已初始化。
        let bytes: &mut [u8] = unsafe {
            core::slice::from_raw_parts_mut(
                target.as_mut_ptr() as *mut u8,
                target.len(),
            )
        };
        let mut fut = pin!(self.stream.read(bytes));
        let result = match poll_once(fut.as_mut()) {
            Poll::Ready(Ok(Some(n))) => SomeOf::new_left(n),
            Poll::Ready(Ok(None)) => {
                // EOF：记入共享状态，本轮返回 0（泵停止）。
                self.eof.store(true, Ordering::Release);
                SomeOf::new_left(0)
            }
            Poll::Ready(Err(e)) => {
                // 读错误：记录并视为 EOF（适配器可经 take_error 取回）。
                if let Ok(mut g) = self.err.lock() {
                    *g = Some(e);
                }
                self.eof.store(true, Ordering::Release);
                SomeOf::new_left(0)
            }
            Poll::Pending => SomeOf::new_left(0), // 无数据：本轮不搬
        };
        ReadyIo::new(result)
    }
}

// ---------------------------------------------------------------------------
// 输出设备（缓冲 → SendStream）
// ---------------------------------------------------------------------------

/// 输出设备：把 QUIC `SendStream` 作为 `circular_buff` 的主动消费端。
///
/// `write_async` **同步阻塞**写（noop waker 自旋到完成）：保证泵冲刷的数据
/// 必然送达。在 tokio 多线程运行时下，流控随对端 ACK 推进（由运行时其他
/// 任务驱动），自旋必然终止；单线程运行时下会自旋饿死，属无任务模型的固有
/// 边界（见 `buffex` 的 `core_` 模块文档）。
///
/// # 为什么这里必须保留 `Arc<Mutex>`（与 [`StreamInput`] 不同）
///
/// * `stream`——写侧需要 **`shutdown` 取回流执行 `finish()`**（对端读侧由此
///   看到 EOF）。取流与泵的 `write_async` 是**两条独立路径**（设备在核心的
///   `UnsafeCell` 内经 `&mut` 访问，适配器经本 Arc 访问），类型系统无法证明
///   二者互斥（`shutdown` 是公开方法、可跨线程调用）——并发即双重 `&mut`
///   UB，`Mutex` 是 soundness 保障；
/// * `err`——同 [`StreamInput`]：`take_error` 可跨线程读取，与设备的写入
///   无类型层面互斥，`Mutex` 必需。
#[doc(hidden)]
pub struct StreamOutput {
    stream: Arc<Mutex<Option<SendStream>>>,
    err: Arc<Mutex<Option<WriteError>>>,
}

impl StreamOutput {
    /// `stream` 是已装入流的共享盒（适配器与设备共享；适配器在 `shutdown`
    /// 时取回流执行 `finish()`）。
    pub fn new(stream: Arc<Mutex<Option<SendStream>>>, err: Arc<Mutex<Option<WriteError>>>) -> Self {
        StreamOutput { stream, err }
    }
}

impl TrOutput<u8> for StreamOutput {
    type WriteAsync<'f> = ReadyIo<usize, WriteError> where Self: 'f;
    type Err = WriteError;

    fn write_async<'f>(
        &'f mut self,
        source: &'f [MaybeUninit<u8>],
    ) -> Self::WriteAsync<'f> {
        let mut guard = self.stream.lock().unwrap();
        let result = match guard.as_mut() {
            Some(stream) => {
                // SAFETY: 泵只把已提交（已初始化）的数据区交给输出设备。
                let bytes: &[u8] = unsafe {
                    core::slice::from_raw_parts(source.as_ptr() as *const u8, source.len())
                };
                match block_on(stream.write_all(bytes)) {
                    Ok(()) => SomeOf::new_left(bytes.len()),
                    Err(e) => {
                        // 记录错误（适配器经 take_error 取回）；本轮返回 0
                        // （泵停止，数据滞留缓冲）。
                        if let Ok(mut g) = self.err.lock() {
                            *g = Some(e);
                        }
                        SomeOf::new_left(0)
                    }
                }
            }
            // 已 finish（流被 shutdown 取走）：丢弃后续数据。
            None => SomeOf::new_left(0),
        };
        ReadyIo::new(result)
    }
}
