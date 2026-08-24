//! `CircularBuff` 的核心类型骨架。
//!
//! 完整的设计与使用思路见 [`crate::circular_buff`] 的模块文档。
//!
//! 当前这里只承载**类型签名**，核心状态机、hook 槽位、主动 pump 均未实现：
//!
//! * 环形核心状态机（rp / wp / 容量、原子推进、关闭标志）——未实现；
//! * hook 槽位的挂载与触发逻辑——未实现；
//! * 主动模式下的同步搬运（pump，poll-to-completion）——未实现；
//! * 主动端设备的类型擦除持有——未实现（见模块文档「核心实现（进行中）」）。
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
/// / [`producer_active`](crate::circular_buff::builder::CircularBuffBuilder::producer_active)
/// 选择模式并提供设备。本类型只在构建流程内部传递，`build` 时被擦除进
/// [`CircularBuff`] 的内部状态——**hook 不会出现在 `CircularBuff` 的泛型参数里**，
/// 调用者既看不到、也不参与构造 hook。
// 骨架阶段：`build` 尚未实现，变体字段暂时只被搬运、不被读取；
// 核心落地后（build 真正读取配置并擦除设备）即可移除。
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
/// 占位类型是否实现 `TrBuffTryWrite` / `TrBuffTryRead`（作为永远失败的 stub）
/// 待核心实现时定；当前仅作为类型存在。
pub struct DeviceProducer<TyInput, T>
where
    TyInput: TrInput,
{
    _marker: PhantomData<fn() -> T>,
    _input: PhantomData<TyInput>,
}

/// 主动消费端对外不可访问时的**占位类型**（读半部）。
///
/// 同 [`TxPlaceholder`]：主动消费端不再提供任何有实际效果的 `TrBuffTryRead`
/// 实现，对外接口对主动消费端返回本占位类型。
pub struct DeviceConsumer<TyOutput, T>
where
    TyOutput: TrOutput,
{
    _marker: PhantomData<fn() -> T>,
    _output: PhantomData<TyOutput>,
}

/// 唤醒式环形缓冲器 `CircularBuff` 的类型骨架。
///
/// # 泛型参数
///
/// * `'a`——内部缓冲区（以及主动端设备）的借用期；
/// * `T`——元素类型，默认 `u8`。
///
/// # 设计要点
///
/// * **hook 对调用者透明**：核心内部使用哪个 hook（被动=唤醒 waker/parker，
///   主动=同步搬运设备数据）是构建期由 builder 决定并隐藏的，**不通过泛型参数
///   暴露**，调用者也不参与构造。模式带来的、对调用者唯一可见的影响是
///   **可访问性**：主动端不对外暴露，见 [`TxPlaceholder`] / [`RxPlaceholder`]。
/// * **存储统一为 `[MaybeUninit<T>]`**：与 `RingBuffer` 的设计理念不同，这里
///   **不引入** `RingStorage` 之类的存储抽象来兼容支持多种缓冲区类型；内部
///   一律把缓冲区视为 `[MaybeUninit<T>]`，内存由调用者在构建时以
///   `&'a mut [MaybeUninit<T>]` 提供（`no_std`、无 alloc，借用而非拥有）。
///
/// 字段布局：`buffer` 即上述统一存储；生产端 / 消费端的内部状态（各自的 hook：
/// 被动=唤醒槽位，主动=擦除后的设备 + 泵函数）在核心实现时补全。
// 骨架阶段：`buffer` 暂时只被声明、不被读取；核心落地后即可移除。
#[allow(dead_code)]
pub struct CircularBuff<'a, P, C, B, T = u8>
where
    P: TrProducer<T>,
    C: TrConsumer<T>,
    B: BorrowMut<[MaybeUninit<T>]>,
{
    /// 内部缓冲区，统一视为 `[MaybeUninit<T>]`（不经过任何存储抽象）。
    buffer_: &'a mut [MaybeUninit<T>],
}
