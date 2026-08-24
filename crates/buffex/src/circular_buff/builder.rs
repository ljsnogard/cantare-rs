//! `CircularBuff` 的构建器。
//!
//! # 设计思路
//!
//! 构建器把「模式」与「用途」的决策**集中在构建期间**完成：
//!
//! * **模式**：生产端与消费端各自独立地选择「被动」或「主动」；
//! * **用途**：主动端在构建时绑定具体的设备——生产端通过
//!   [`CircularBuffBuilder::pipe_from_input`] 绑定 `TrInput`（输入设备），
//!   消费端通过 [`ProducerSetBuilder::pipe_into_output`] 绑定 `TrOutput`
//!   （输出设备）。
//!
//! 一旦 `build` 完成，两端的工作方式就固定下来。核心机制（环形状态机 + hook）
//! 对四种组合完全一致，只有 hook 的行为不同：
//!
//! | 生产端 \ 消费端 | 被动                               | 主动                                  |
//! |-----------------|------------------------------------|---------------------------------------|
//! | **被动**        | 调用者自行写入 / 自行读取           | 调用者写入；写入后自动搬运到 `TrOutput` |
//! | **主动**        | 构造后自动从 `TrInput` 灌入；自行读取 | 从 `TrInput` 自动灌入，自动搬运到 `TrOutput`（同步流水线） |
//!
//! # 命名：pipe_from_input / pipe_into_output
//!
//! 主动模式的构建方法特意命名为 `pipe_from_input` / `pipe_into_output`，强调
//! 语义是「把输入设备**管道进**缓冲」/「把缓冲**管道进**输出设备」——数据在
//! 构建完成后就开始流动（构造期即执行初始 pump），而不是「提供一个活跃的
//! 生产者 / 消费者对象」。
//!
//! # hook 对调用者透明
//!
//! 构建器内部持有 `ProducerConfig` / `ConsumerConfig`（模式 + 设备，均为
//! crate 内部类型）。**hook 不会出现在 `CircularBuff` 的泛型参数里**——
//! `CircularBuff` 的类型参数是**端类型**（`P` / `C`，决定可访问性）与存储
//! （`B`），hook 完全在内部。
//!
//! 模式对调用者唯一可见的影响是**可访问性**：主动端不对外暴露，其端类型是
//! 占位类型（[`DeviceProducer`] / [`DeviceConsumer`]，并携带设备实际类型）；
//! 被动端才是可访问的端类型（[`PassiveProducer`] / [`PassiveConsumer`]）。
//!
//! # 类型状态（type-state）编码
//!
//! 构建流程是三个阶段，用不同的类型表达，因此「两端都必须在构建期被决定」是
//! 类型层面保证的——漏设一端无法编译：
//!
//! ```text
//! CircularBuffBuilder          // 阶段一：只有容量，尚未设置生产端
//!   └─ producer_passive / pipe_from_input
//!      → ProducerSetBuilder   // 阶段二：已设置生产端，尚未设置消费端
//!         └─ consumer_passive / pipe_into_output
//!            → ReadyBuilder   // 阶段三：两端都已设置，可以 build
//!               └─ build(&mut [MaybeUninit<T>]) → CircularBuff<'a, P, C, B, T>
//! ```
//!
//! 所有借用（缓冲区、主动端设备）共享同一个生命周期 `'a`。阶段类型携带
//! 设备类型（`I` / `O`，被动端用 `()` 占位）与**端类型**（`P` / `C`）：
//! 主动分支的端类型是 `DeviceProducer<I, T>` / `DeviceConsumer<O, T>`，
//! 被动分支是 `PassiveProducer<'a, T>` / `PassiveConsumer<'a, T>`。
//!
//! # 设备元素类型（u8）
//!
//! 主动模式绑定的设备按 abs_buff 的默认类型参数取 `TrInput<u8>` /
//! `TrOutput<u8>`（见 [`DeviceProducer`] / [`DeviceConsumer`] 的 `where`
//! 约束），因此主动模式与缓冲区元素类型 `T = u8`（默认值）搭配。`T ≠ u8`
//! 与主动模式的组合（是否需要类型转换）待核心实现时确定。
//!
//! # 未设计完成的细节（`todo!()`）
//!
//! `build` 目前是占位实现，以下细节待核心落地时补全（详见
//! [`crate::circular_buff`] 模块文档的「核心实现（进行中）」一节）：
//!
//! * 容量校验（`2..=MAX_CAPACITY`，与 `ring_buffer` 的上限对齐）；
//! * 环形核心状态机（rp / wp / 原子推进 / 关闭标志）的初始化；
//! * 把 `producer` / `consumer` 两个配置挂载成核心内部 hook（被动=唤醒槽位，
//!   主动=设备 + 泵函数）；
//! * 端类型（[`DeviceProducer`] / [`DeviceConsumer`] / [`PassiveProducer`] /
//!   [`PassiveConsumer`]）对 `TrProducer` / `TrConsumer` /
//!   `TrDeviceProducer` / `TrDeviceConsumer` 的实现——当前**缺失**：`build`
//!   的返回类型带 `where P: TrProducer<T>, C: TrConsumer<T>`，真实调用
//!   `.build()` 前必须先实现这些 trait（库内暂无调用点，因此可编译；模块
//!   文档中的示例代码也因此保持 `ignore`）；
//! * 生产端为主动时，构建完成后立即执行一轮「初始输入 pump」；
//! * `CircularBuff` 的对外拆分接口（主动端返回占位类型）。

use core::marker::PhantomData;
use core::mem::MaybeUninit;

use abs_buff::io::{TrInput, TrOutput};

use super::abs_::{TrConsumer, TrProducer};
use super::circ_buff_::{
    CircularBuff, ConsumerConfig, DeviceConsumer, DeviceProducer, PassiveConsumer,
    PassiveProducer, ProducerConfig,
};

// ---------------------------------------------------------------------------
// 阶段一：尚未设置生产端
// ---------------------------------------------------------------------------

/// 阶段一：只确定了容量，生产端 / 消费端的模式尚未决定。
pub struct CircularBuffBuilder<T = u8> {
    capacity: usize,
    _marker: PhantomData<fn() -> T>,
}

impl<T> CircularBuffBuilder<T> {
    /// 以期望容量开始构建（实际容量受存储与上限限制，`build` 时校验）。
    pub fn with_capacity(capacity: usize) -> Self {
        CircularBuffBuilder {
            capacity,
            _marker: PhantomData,
        }
    }

    /// 生产端设为**被动模式**：调用者通过 `TrBuffTryWrite` 自行决定何时写入，
    /// 该端对外可访问（端类型为 [`PassiveProducer`]）。
    pub fn producer_passive<'a>(self) -> ProducerSetBuilder<'a, (), PassiveProducer<'a, T>, T> {
        ProducerSetBuilder {
            capacity: self.capacity,
            producer: ProducerConfig::Passive,
            _producer: PhantomData,
            _marker: PhantomData,
        }
    }

    /// 生产端设为**主动模式**：构造完成后立即从 `input` 抽取数据填充内部缓冲；
    /// 此后每当消费端读取、释放出可写空间，hook 立即从 `input` 拉取新数据。
    /// 该端由输入设备驱动，**对外不可访问**（端类型为
    /// [`DeviceProducer`]，`try_as_buff`
    /// 永远返回错误）。
    ///
    /// `I` 必须实现 `TrInput`（abs_buff 的输入设备抽象，默认 `u8` 元素）。
    pub fn pipe_from_input<'a, I>(
        self,
        input: &'a mut I,
    ) -> ProducerSetBuilder<'a, I, DeviceProducer<I, T>, T>
    where
        I: TrInput,
    {
        ProducerSetBuilder {
            capacity: self.capacity,
            producer: ProducerConfig::Active(input),
            _producer: PhantomData,
            _marker: PhantomData,
        }
    }
}

// ---------------------------------------------------------------------------
// 阶段二：已设置生产端，尚未设置消费端
// ---------------------------------------------------------------------------

/// 阶段二：生产端的模式与设备已确定，消费端尚未决定。
///
/// `I` 是设备类型（被动端为 `()`），`P` 是最终 [`CircularBuff`] 的生产端类型
/// 参数。
pub struct ProducerSetBuilder<'a, I, P, T = u8> {
    capacity: usize,
    producer: ProducerConfig<'a, I>,
    _producer: PhantomData<P>,
    _marker: PhantomData<fn() -> T>,
}

impl<'a, I, P, T> ProducerSetBuilder<'a, I, P, T> {
    /// 消费端设为**被动模式**：调用者通过 `TrBuffTryRead` 异步等待就绪的内部
    /// 缓冲区，然后自行读取，该端对外可访问（端类型为 [`PassiveConsumer`]）。
    pub fn consumer_passive(self) -> ReadyBuilder<'a, I, P, (), PassiveConsumer<'a, T>, T> {
        ReadyBuilder {
            capacity: self.capacity,
            producer: self.producer,
            consumer: ConsumerConfig::Passive,
            _producer: PhantomData,
            _consumer: PhantomData,
            _marker: PhantomData,
        }
    }

    /// 消费端设为**主动模式**：一旦有任何数据写入缓冲区，立即搬运到 `output`
    /// 输出设备。该端由输出设备驱动，**对外不可访问**（端类型为
    /// [`DeviceConsumer`]，`try_as_buff`
    /// 永远返回错误）。
    ///
    /// `O` 必须实现 `TrOutput`（abs_buff 的输出设备抽象，默认 `u8` 元素）。
    /// 设备借用与生产端设备、内部缓冲区共享同一个生命周期 `'a`。
    pub fn pipe_into_output<O>(
        self,
        output: &'a mut O,
    ) -> ReadyBuilder<'a, I, P, O, DeviceConsumer<O, T>, T>
    where
        O: TrOutput,
    {
        ReadyBuilder {
            capacity: self.capacity,
            producer: self.producer,
            consumer: ConsumerConfig::Active(output),
            _producer: PhantomData,
            _consumer: PhantomData,
            _marker: PhantomData,
        }
    }
}

// ---------------------------------------------------------------------------
// 阶段三：两端都已设置，可以构建
// ---------------------------------------------------------------------------

/// 阶段三：生产端与消费端的模式、设备都已确定，随时可以 `build`。
///
/// `I` / `O` 是设备类型（被动端为 `()`），`P` / `C` 是最终 [`CircularBuff`]
/// 的生产端 / 消费端类型参数。
// 骨架阶段：`build` 尚未实现，字段暂时只被搬运、不被读取；
// 核心落地后（build 真正读取字段）即可移除。
#[allow(dead_code)]
pub struct ReadyBuilder<'a, I, P, O, C, T = u8> {
    capacity: usize,
    producer: ProducerConfig<'a, I>,
    consumer: ConsumerConfig<'a, O>,
    _producer: PhantomData<P>,
    _consumer: PhantomData<C>,
    _marker: PhantomData<fn() -> T>,
}

impl<'a, I, P, O, C, T> ReadyBuilder<'a, I, P, O, C, T>
where
    P: TrProducer<T>,
    C: TrConsumer<T>,
{
    /// 提供内部缓冲区并完成构建，返回固定好两端模式与设备的
    /// [`CircularBuff`]，其端类型参数即 `P` / `C`，存储参数 `B` 即传入的
    /// `&'a mut [MaybeUninit<T>]`（统一视为 `[MaybeUninit<T>]`，不经过任何
    /// 存储抽象，借用而非拥有；`no_std`、无 alloc）。
    ///
    /// 生产端为主动时，构建过程本身会执行第一轮「初始输入 pump」：构造返回前，
    /// 数据已经从 `TrInput` 流入内部缓冲（甚至已经流到 `TrOutput`）。
    ///
    /// 注意：`P: TrProducer<T>` / `C: TrConsumer<T>` 的实现随核心一起落地，
    /// 在此之前 `.build()` 的返回类型约束无法被满足（见模块文档「核心实现
    /// （进行中）」）。
    pub fn build(
        self,
        _storage: &'a mut [MaybeUninit<T>],
    ) -> Result<CircularBuff<'a, P, C, &'a mut [MaybeUninit<T>], T>, usize> {
        // TODO: 完整构建流程（见本模块文档「未设计完成的细节」一节）：
        // 1. 容量校验（2..=MAX_CAPACITY）；
        // 2. 初始化环形核心状态机（rp / wp / 原子推进 / 关闭标志）；
        // 3. 把 producer / consumer 两个配置挂载成核心内部 hook；
        // 4. 若生产端主动，执行初始输入 pump；
        // 5. 组装并返回 CircularBuff。
        todo!("CircularBuff 核心尚未实现：见 crate::circular_buff 模块文档")
    }
}
