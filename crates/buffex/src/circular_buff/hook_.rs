//! hook 机制：环形核心在状态变化（写入/读取提交、关闭）时，向两端各触发一个
//! **事件**；具体的 hook（被动=唤醒等待者，主动=同步搬运设备数据）根据事件
//! 决定自己的行为。
//!
//! # 事件流向
//!
//! ```text
//! 生产端完成写入 / 生产者关闭  →  触发【消费端 hook】：ConsumerHookEvent
//! 消费端完成读取 / 消费者关闭  →  触发【生产端 hook】：ProducerHookEvent
//! ```
//!
//! 事件携带的新容量 / 新数据量只是**参考值**：被动 hook 唤醒等待者后由等待者
//! 重新检查条件（防丢失唤醒）；主动 hook 直接泵设备直到满足为止（参考值不参与
//! 决策）。

use core::{marker::PhantomData, mem::MaybeUninit};

use abs_buff::{
    io::{TrInput, TrOutput},
    x_deps::abs_cancel::{NonCancellableToken, TrMayCancel},
};

use super::core_::WakeSlot;

/// 生产端 hook 收到的事件（消费端完成读取 / 消费者关闭后触发）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ProducerHookEvent {
    /// 缓冲区中的可供生产的数据有变化，携带新的可写容量
    Available(usize),

    /// 消费者已关闭，携带剩余可写容量
    ConsumerClose(usize),
}

/// 消费端 hook 收到的事件（生产端完成写入 / 生产者关闭后触发）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ConsumerHookEvent {
    /// 缓冲区中的可供消费的数据有变化，携带新的可读数据量
    Available(usize),

    /// 生产者端已关闭，携带剩余可读数据量
    ProducerClose(usize),
}

/// 主动模式下持有「类型擦除」的输入设备。
///
/// # 为什么需要类型擦除
///
/// `CircularBuff` 的泛型参数（`P` / `C` / `B` / `T`）里没有设备类型的位置，
/// 而 `TrInput` 带泛型关联类型（GAT）、不能直接 `dyn`。因此这里在**构建期**
/// （builder 的 `pipe_from_input`，此时设备类型 `I` 具体可知）把 `&'a mut I`
/// 擦除为裸指针 + 单态化的泵函数指针。
///
/// # Safety
///
/// - `dev` 始终指向同一个 `&mut I`，其有效性由持有本类型的 [`CircularBuff`]
///   （`super::CircularBuff`）的生命周期参数与借用约束保证；
/// - 泵函数 `pump` 只由 `pump` 自身（知道具体类型 `I`）解引用 `dev`；
/// - 同一时刻至多一个线程执行泵（SPSC + 单泵线程约束，见 [`super::core_`]）。
pub(super) struct ActiveInput<T> {
    dev: *mut (),
    pump: fn(*mut (), &mut [MaybeUninit<T>]) -> usize,
    _marker: PhantomData<fn() -> T>,
}

impl<T> ActiveInput<T> {
    /// 由 builder 在 `I` 具体已知时构造（单态化泵函数）。
    pub(super) fn new<I>(input: &mut I) -> Self
    where
        I: TrInput<T>,
    {
        ActiveInput {
            dev: input as *mut I as *mut (),
            pump: input_pump::<I, T>,
            _marker: PhantomData,
        }
    }

    /// 泵一轮：把设备数据同步读入 `dst`，返回读入量（0 = 无更多数据或设备错误）。
    pub(super) fn pump_into(&self, dst: &mut [MaybeUninit<T>]) -> usize {
        (self.pump)(self.dev, dst)
    }
}

/// 主动模式下持有「类型擦除」的输出设备。同 [`ActiveInput`] 的擦除设计。
pub(super) struct ActiveOutput<T> {
    dev: *mut (),
    pump: fn(*mut (), &[MaybeUninit<T>]) -> usize,
    _marker: PhantomData<fn() -> T>,
}

impl<T> ActiveOutput<T> {
    /// 由 builder 在 `O` 具体已知时构造（单态化泵函数）。
    pub(super) fn new<O>(output: &mut O) -> Self
    where
        O: TrOutput<T>,
    {
        ActiveOutput {
            dev: output as *mut O as *mut (),
            pump: output_pump::<O, T>,
            _marker: PhantomData,
        }
    }

    /// 泵一轮：把 `src` 的数据同步写入设备，返回写入量（0 = 设备暂时无法接收）。
    pub(super) fn pump_from(&self, src: &[MaybeUninit<T>]) -> usize {
        (self.pump)(self.dev, src)
    }
}

/// 生产端 hook：被动=唤醒等待可写空间的写者；主动=从输入设备泵数据。
pub(super) enum ProducerHook<T> {
    /// 被动生产：hook 的工作是唤醒进入 pending 状态的写者（future 的 waker）。
    Passive(WakeSlot),
    /// 主动生产：hook 的工作是同步搬运输入设备的数据进缓冲。
    Active(ActiveInput<T>),
}

/// 消费端 hook：被动=唤醒等待可读数据的读者；主动=把缓冲数据泵到输出设备。
pub(super) enum ConsumerHook<T> {
    /// 被动消费：hook 的工作是唤醒进入 pending 状态的读者。
    Passive(WakeSlot),
    /// 主动消费：hook 的工作是同步搬运缓冲数据到输出设备。
    Active(ActiveOutput<T>),
}

/// 输入泵：同步轮询 `TrInput::read_async` 到完成（`Waker::noop` + 自旋，
/// 无运行时依赖），把设备数据读入 `dst`，返回读入量。
fn input_pump<I, T>(dev: *mut (), dst: &mut [MaybeUninit<T>]) -> usize
where
    I: TrInput<T>,
{
    // SAFETY: `dev` 由 [`ActiveInput::new`] 构造，始终指向同一个 `&mut I`；
    // 泵只在单线程（SPSC 的触发线程）上执行。
    let dev = unsafe { &mut *(dev as *mut I) };
    // 经 `may_cancel_with` 把设备的 `ReadAsync`（TrMayCancel）驱动为
    // `MayCancelOutput = SomeOf<usize, Err>`，再轮询到完成。
    let fut = dev.read_async(dst).may_cancel_with(NonCancellableToken::shared_mut());
    match block_on(fut).pick_left() {
        Some(n) => n,
        // 设备错误：本轮视为无更多数据。错误如何跨过 hook 通知对端，待核心
        // 实现错误传播时再定。
        None => 0,
    }
}

/// 输出泵：同步轮询 `TrOutput::write_async` 到完成，把 `src` 的数据写入设备，
/// 返回写入量。
fn output_pump<O, T>(dev: *mut (), src: &[MaybeUninit<T>]) -> usize
where
    O: TrOutput<T>,
{
    // SAFETY: 同 [`input_pump`]。
    let dev = unsafe { &mut *(dev as *mut O) };
    let fut = dev
        .write_async(src)
        .may_cancel_with(NonCancellableToken::shared_mut());
    match block_on(fut).pick_left() {
        Some(n) => n,
        None => 0,
    }
}

/// 微型 `block_on`：用 `Waker::noop()` 自旋驱动单个 future 到完成。
///
/// 只用于主动 hook 的同步搬运——这里不存在执行器，waker 永远不会被唤醒，
/// 因此 future 必须能在不被唤醒的情况下完成（设备实现要保证这一点，例如
/// 立即就绪的 future，或轮询不依赖 waker 的实现）。
pub(super) fn block_on<F>(fut: F) -> F::Output
where
    F: core::future::IntoFuture,
{
    use core::task::{Context, Poll, Waker};
    let waker = Waker::noop();
    let mut cx = Context::from_waker(&waker);
    let mut fut = core::pin::pin!(core::future::IntoFuture::into_future(fut));
    loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(v) => return v,
            Poll::Pending => core::hint::spin_loop(),
        }
    }
}
