//! `CircularBuff` 的核心类型骨架。
//!
//! 完整的设计与使用思路见 [`crate::circular_buff`] 的模块文档。
//!
//! 当前这里只承载**类型签名**，核心状态机、hook 槽位、主动 pump、以及端
//! （`TrProducer` / `TrConsumer`）的实现均未落地：
//!
//! * 环形核心状态机（rp / wp / 容量、原子推进、关闭标志）——未实现；
//! * hook 槽位的挂载与触发逻辑——未实现；
//! * 主动模式下的同步搬运（pump，poll-to-completion）——未实现；
//! * 四个端类型（[`DeviceProducer`] / [`DeviceConsumer`] / [`PassiveProducer`] /
//!   [`PassiveConsumer`]）对 [`TrProducer`] / [`TrConsumer`] 的实现——未实现，
//!   随核心一起落地。
//!
//! 这些都属于 [`crate::circular_buff::builder`] 的 `build` 路径负责的部分，
//! 目前以 `todo!()` 占位。

use core::{
    borrow::BorrowMut,
    marker::PhantomData,
    mem::MaybeUninit,
};

use abs_buff::io::{TrInput, TrOutput};

use super::abs_::{TrConsumer, TrProducer};

/// 构建期对**生产端**「模式 + 设备」的配置（仅构建流程内部使用）。
///
/// # 设计要点：hook 对调用者透明
///
/// 调用者只通过
/// [`CircularBuffBuilder::producer_passive`](crate::circular_buff::builder::CircularBuffBuilder::producer_passive)
/// / [`pipe_from_input`](crate::circular_buff::builder::CircularBuffBuilder::pipe_from_input)
/// 选择模式并提供设备。本类型只在构建流程内部传递，`build` 时被挂载进
/// [`CircularBuff`] 的内部状态——**hook 不会出现在 `CircularBuff` 的泛型参数里**，
/// 调用者既看不到、也不参与构造 hook。
// 骨架阶段：`build` 尚未实现，变体字段暂时只被搬运、不被读取；
// 核心落地后（build 真正读取配置并挂载设备）即可移除。
#[allow(dead_code)]
pub(super) enum ProducerConfig<'a, I> {
    /// 被动生产：调用者通过 `TrBuffTryWrite` 自行决定何时写入。
    Passive,
    /// 主动生产：构造后立即（并在每次消费端读取、释放可写空间后）
    /// 从该输入设备抽取数据填充缓冲。
    Active(&'a mut I),
}

/// 构建期对**消费端**「模式 + 设备」的配置（仅构建流程内部使用）。
///
/// 同 [`ProducerConfig`]：hook 对调用者透明，只在构建流程内部传递。
// 骨架阶段：见 [`ProducerConfig`] 的说明。
#[allow(dead_code)]
pub(super) enum ConsumerConfig<'a, O> {
    /// 被动消费：调用者通过 `TrBuffTryRead` 自行决定何时读取。
    Passive,
    /// 主动消费：一旦有数据写入缓冲区，立即搬运到该输出设备。
    Active(&'a mut O),
}

/// 主动生产端对外不可访问时的**占位类型**（写半部）。
///
/// # 设计要点：主动端不对外暴露
///
/// 使用了主动模式的那一端由设备驱动，不可能再让外部调用者访问：主动生产端
/// 不会再提供任何有实际效果的 `TrBuffTryWrite` 实现。因此 `CircularBuff` 的
/// 对外接口（类似
/// [`RingBuffer::try_split_io`](crate::ring_buffer::TrRingBuffer::try_split_io)
/// 的拆分）对主动端返回本占位类型，而不是一个可用的写半部——接口形状保持统一
/// （永远返回两端），但主动端拿到的是「无实际效果」的占位。这也对应了
/// `RingBuffer` 中「写半部不存在」（`try_split_io` 返回 `None`）的情形，只是用
/// 占位类型而非 `Option` 来表达。
///
/// # 设备类型的携带
///
/// 泛型参数 `TyInput` 携带输入设备的**实际类型**（`TrInput`，默认 `u8`），
/// `T` 是缓冲区元素类型。核心通过
/// [`TrDeviceProducer`](crate::circular_buff::TrDeviceProducer) 的关联类型
/// `InputDevice` 从 `P` 中取回设备类型，从而**无需任何类型擦除**（`TrInput`
/// 带泛型关联类型、不能直接 `dyn`）即可在内部持有设备。
///
/// `DeviceProducer` 的 `TrProducer` 实现（`try_as_buff` 永远返回错误，见
/// [`TrProducer`] 的文档）随核心实现一起落地，当前仅作为类型存在。
pub struct DeviceProducer<TyInput, T>
where
    TyInput: TrInput,
{
    _marker: PhantomData<fn() -> T>,
    _input: PhantomData<TyInput>,
}

/// 主动消费端对外不可访问时的**占位类型**（读半部）。
///
/// 同 [`DeviceProducer`]：主动消费端由输出设备驱动，不再提供任何有实际效果的
/// `TrBuffTryRead` 实现，对外接口对主动消费端返回本占位类型。`TyOutput`
/// 携带输出设备的实际类型（`TrOutput`，默认 `u8`），核心通过
/// [`TrDeviceConsumer`](crate::circular_buff::TrDeviceConsumer) 的关联类型
/// `OutputDevice` 取回设备类型，无需类型擦除。
///
/// `DeviceConsumer` 的 `TrConsumer` 实现（`try_as_buff` 永远返回错误）随核心
/// 实现一起落地，当前仅作为类型存在。
pub struct DeviceConsumer<TyOutput, T>
where
    TyOutput: TrOutput,
{
    _marker: PhantomData<fn() -> T>,
    _output: PhantomData<TyOutput>,
}

/// 被动生产端的类型骨架（对外可访问的写半部）。
///
/// 被动模式的生产端由调用者通过 `TrBuffTryWrite` 驱动。实现 `TrProducer<T>`：
/// `try_as_buff` 返回可用的写半部。真正的半部类型（借用环形核心、实现
/// `TrBuffTryWrite`）随核心实现落地，当前仅作为类型占位。
pub struct PassiveProducer<'a, T = u8> {
    _marker: PhantomData<&'a mut [MaybeUninit<T>]>,
}

/// 被动消费端的类型骨架（对外可访问的读半部）。
///
/// 被动模式的消费端由调用者异步等待就绪的缓冲区后自行读取。实现
/// `TrConsumer<T>`：`try_as_buff` 返回可用的读半部（随核心实现落地），
/// 当前仅作为类型占位。
pub struct PassiveConsumer<'a, T = u8> {
    _marker: PhantomData<&'a mut [MaybeUninit<T>]>,
}

/// 唤醒式环形缓冲器 `CircularBuff` 的类型骨架。
///
/// # 泛型参数
///
/// * `'a`——内部缓冲区（以及被动半部、主动端设备）的借用期；
/// * `P`——**生产端类型**：被动 = [`PassiveProducer`]，主动 = [`DeviceProducer`]；
///   实现 `TrProducer<T>`；
/// * `C`——**消费端类型**：被动 = [`PassiveConsumer`]，主动 = [`DeviceConsumer`]；
///   实现 `TrConsumer<T>`；
/// * `B`——内部缓冲区拥有者（实践中为 `&'a mut [MaybeUninit<T>]`），只要求
///   能通过 `BorrowMut` 提供 `&mut [MaybeUninit<T>]` 视图；
/// * `T`——元素类型，默认 `u8`。
///
/// # 设计要点
///
/// * **hook 对调用者透明**：核心内部使用哪个 hook（被动=唤醒 waker/parker，
///   主动=同步搬运设备数据）是构建期由 builder 决定并隐藏的。类型参数里的
///   `P` / `C` 是**端类型**（决定可访问性：被动端可访问、主动端是占位类型），
///   **不是** hook——调用者不参与构造 hook，hook 完全在内部。
/// * **主动端不对外暴露**：主动端的端类型是 [`DeviceProducer`] /
///   [`DeviceConsumer`]（占位），对外接口不再提供有实际效果的
///   `TrBuffTryWrite` / `TrBuffTryRead`。设备类型经
///   [`TrDeviceProducer`](crate::circular_buff::TrDeviceProducer) /
///   [`TrDeviceConsumer`](crate::circular_buff::TrDeviceConsumer) 的关联类型
///   携带在端类型里，核心无需类型擦除。
/// * **存储统一为 `[MaybeUninit<T>]`**：与 `RingBuffer` 的设计理念不同，这里
///   **不引入** `RingStorage` 之类的存储抽象层；`B` 只要求
///   `BorrowMut<[MaybeUninit<T>]>`（能提供 `&mut [MaybeUninit<T>]` 视图），
///   内部一律以 `[MaybeUninit<T>]` 视图操作缓冲区（`no_std`、无 alloc，
///   借用而非拥有）。
///
/// 字段布局：`buffer_` 即上述统一存储；`producer_` / `consumer_` 是两个端；
/// 核心状态机（rp / wp / hook 槽位 / 设备引用）在核心实现时补全。
// 骨架阶段：无构造路径，字段暂时只被声明、不被读取；核心落地后即可移除。
#[allow(dead_code)]
pub struct CircularBuff<'a, P, C, B, T = u8>
where
    P: TrProducer<T>,
    C: TrConsumer<T>,
    B: BorrowMut<[MaybeUninit<T>]>,
{
    /// 内部缓冲区拥有者，统一以 `[MaybeUninit<T>]` 视图访问。
    buffer_: B,
    /// 生产端（被动=`PassiveProducer`，主动=`DeviceProducer`）。
    producer_: P,
    /// 消费端（被动=`PassiveConsumer`，主动=`DeviceConsumer`）。
    consumer_: C,
    /// 借用期标记：缓冲区与被动半部共享的借用期 `'a`。
    _marker: PhantomData<&'a mut [MaybeUninit<T>]>,
}
