//! `CircularBuff` 的构建器。
//!
//! # 设计思路
//!
//! 构建器把「模式」与「用途」的决策**集中在构建期间**完成：
//!
//! * **模式**：生产端与消费端各自独立地选择「被动」或「主动」；
//! * **用途**：主动端在构建时绑定具体的设备——生产端绑定 `TrInput`（输入设备），
//!   消费端绑定 `TrOutput`（输出设备）。
//!
//! 一旦 `build` 完成，两端的工作方式就固定下来。核心机制（环形状态机 + hook）
//! 对四种组合完全一致，只有 hook 的行为不同：
//!
//! | 生产端 \ 消费端 | 被动                               | 主动                                  |
//! |-----------------|------------------------------------|---------------------------------------|
//! | **被动**        | 调用者自行写入 / 自行读取           | 调用者写入；写入后自动搬运到 `TrOutput` |
//! | **主动**        | 构造后自动从 `TrInput` 灌入；自行读取 | 从 `TrInput` 自动灌入，自动搬运到 `TrOutput`（同步流水线） |
//!
//! # hook 对调用者透明
//!
//! 构建器内部持有 `ProducerConfig` / `ConsumerConfig`（模式 + 设备，均为
//! crate 内部类型），`build` 时把它们**擦除**进 [`CircularBuff`] 的内部状态
//! （被动=唤醒槽位，主动=擦除后的设备 + 泵函数）。**hook 不会出现在
//! `CircularBuff` 的泛型参数里**——`CircularBuff` 对四种组合都是同一个类型
//! `CircularBuff<'a, T>`（`T` 默认 `u8`），调用者看不到、也不参与构造 hook。
//!
//! 模式对调用者唯一可见的影响是**可访问性**：主动端不对外暴露，对外接口对
//! 主动端返回占位类型（[`TxPlaceholder`](crate::circular_buff::TxPlaceholder) /
//! [`RxPlaceholder`](crate::circular_buff::RxPlaceholder)），被动端才返回
//! 可用的半部。
//!
//! # 类型状态（type-state）编码
//!
//! 构建流程是三个阶段，用不同的类型表达，因此「两端都必须在构建期被决定」是
//! 类型层面保证的——漏设一端无法编译：
//!
//! ```text
//! CircularBuffBuilder        // 阶段一：只有容量，尚未设置生产端
//!   └─ producer_passive / producer_active
//!      → ProducerSetBuilder  // 阶段二：已设置生产端，尚未设置消费端
//!         └─ consumer_passive / consumer_active
//!            → ReadyBuilder  // 阶段三：两端都已设置，可以 build
//!               └─ build(&mut [MaybeUninit<T>]) → CircularBuff<'a, T>
//! ```
//!
//! 所有借用（缓冲区、主动端设备）共享同一个生命周期 `'a`：`CircularBuff<'a, T>`
//! 只携带这一个生命周期参数。被动端用单元类型 `()` 占位设备参数（例如
//! `producer_passive()` 产生 `ProducerSetBuilder<'a, (), T>`），不影响 `'a` 的
//! 统一。
//!
//! # 未设计完成的细节（`todo!()`）
//!
//! `build` 目前是占位实现，以下细节待核心落地时补全（详见
//! [`crate::circular_buff`] 模块文档的「核心实现（进行中）」一节）：
//!
//! * 容量校验（`2..=MAX_CAPACITY`，与 `ring_buffer` 的上限对齐）；
//! * 环形核心状态机（rp / wp / 原子推进 / 关闭标志）的初始化；
//! * 把 `producer` / `consumer` 两个配置擦除成核心内部 hook（被动=唤醒槽位，
//!   主动=设备 + 泵函数）并挂载；
//! * 主动端设备的**类型擦除**机制（`TrInput` 带泛型关联类型，不能直接做
//!   `dyn`；候选：手动 vtable 结构体（设备裸指针 + 单态化的泵函数指针）、或
//!   单线程专用的 `UnsafeCell` 持有，待定）；
//! * 生产端为主动时，构建完成后立即执行一轮「初始输入 pump」；
//! * `CircularBuff` 的字段布局与对外拆分接口（主动端返回占位类型）。

use core::marker::PhantomData;
use core::mem::MaybeUninit;

use abs_buff::io::{TrInput, TrOutput};

use super::circ_buff_::{CircularBuff, ConsumerConfig, ProducerConfig};

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
    /// 该端对外可访问。
    pub fn producer_passive<'a>(self) -> ProducerSetBuilder<'a, (), T> {
        ProducerSetBuilder {
            capacity: self.capacity,
            producer: ProducerConfig::Passive,
            _marker: PhantomData,
        }
    }

    /// # pipe_from_input
    /// 
    /// 生产端设为**主动模式**：构造完成后立即从 `input` 抽取数据填充内部缓冲；
    /// 此后每当消费端读取、释放出可写空间，hook 立即从 `input` 拉取新数据。
    /// 该端由输入设备驱动，**对外不可访问**（对外接口返回
    /// [`TxPlaceholder`](crate::circular_buff::TxPlaceholder)）。
    ///
    /// `I` 必须实现 `TrInput<T>`（abs_buff 的输入设备抽象）。
    pub fn pipe_from_input<'a, I>(self, input: &'a mut I) -> ProducerSetBuilder<'a, I, T>
    where
        I: TrInput<T>,
    {
        ProducerSetBuilder {
            capacity: self.capacity,
            producer: ProducerConfig::Active(input),
            _marker: PhantomData,
        }
    }
}

// ---------------------------------------------------------------------------
// 阶段二：已设置生产端，尚未设置消费端
// ---------------------------------------------------------------------------

/// 阶段二：生产端的模式与设备已确定，消费端尚未决定。
pub struct ProducerSetBuilder<'a, I, T = u8> {
    capacity: usize,
    producer: ProducerConfig<'a, I>,
    _marker: PhantomData<fn() -> T>,
}

impl<'a, I, T> ProducerSetBuilder<'a, I, T> {
    /// 消费端设为**被动模式**：调用者通过 `TrBuffTryRead` 异步等待就绪的内部
    /// 缓冲区，然后自行读取，该端对外可访问。
    pub fn consumer_passive(self) -> ReadyBuilder<'a, I, (), T> {
        ReadyBuilder {
            capacity: self.capacity,
            producer: self.producer,
            consumer: ConsumerConfig::Passive,
            _marker: PhantomData,
        }
    }

    /// # pipe_into_output 
    /// 
    /// 在构造时把消费端设为**主动模式**：一旦有任何数据写入缓冲区，立即搬运到
    ///  `output` 输出设备。该端由输出设备驱动，**对外不可访问**（对外接口返回
    /// [`RxPlaceholder`](crate::circular_buff::RxPlaceholder)）。
    ///
    /// `O` 必须实现 `TrOutput<T>`（abs_buff 的输出设备抽象）。设备借用与生产端
    /// 设备、内部缓冲区共享同一个生命周期 `'a`。
    pub fn pipe_into_output<O>(self, output: &'a mut O) -> ReadyBuilder<'a, I, O, T>
    where
        O: TrOutput<T>,
    {
        ReadyBuilder {
            capacity: self.capacity,
            producer: self.producer,
            consumer: ConsumerConfig::Active(output),
            _marker: PhantomData,
        }
    }
}

// ---------------------------------------------------------------------------
// 阶段三：两端都已设置，可以构建
// ---------------------------------------------------------------------------

/// 阶段三：生产端与消费端的模式、设备都已确定，随时可以 `build`。
// 骨架阶段：`build` 尚未实现，字段暂时只被搬运、不被读取；
// 核心落地后（build 真正读取字段）即可移除。
#[allow(dead_code)]
pub struct ReadyBuilder<'a, I, O, T = u8> {
    capacity: usize,
    producer: ProducerConfig<'a, I>,
    consumer: ConsumerConfig<'a, O>,
    _marker: PhantomData<fn() -> T>,
}

impl<'a, I, O, T> ReadyBuilder<'a, I, O, T> {
    /// 提供内部缓冲区并完成构建，返回固定好两端模式与设备的
    /// [`CircularBuff`]（对四种组合都是同一个类型 `CircularBuff<'a, T>`）。
    ///
    /// `storage` 是环形缓冲的内存，**统一视为 `[MaybeUninit<T>]`**（不经过任何
    /// 存储抽象，借用而非拥有；`no_std`、无 alloc）。
    ///
    /// 生产端为主动时，构建过程本身会执行第一轮「初始输入 pump」：构造返回前，
    /// 数据已经从 `TrInput` 流入内部缓冲（甚至已经流到 `TrOutput`）。
    pub fn build(
        self,
        _storage: &'a mut [MaybeUninit<T>],
    ) -> Result<CircularBuff<'a, T>, usize> {
        // TODO: 完整构建流程（见本模块文档「未设计完成的细节」一节）：
        // 1. 容量校验（2..=MAX_CAPACITY）；
        // 2. 初始化环形核心状态机（rp / wp / 原子推进 / 关闭标志）；
        // 3. 把 producer / consumer 两个配置擦除成核心内部 hook 并挂载；
        // 4. 若生产端主动，执行初始输入 pump；
        // 5. 组装并返回 CircularBuff。
        todo!("CircularBuff 核心尚未实现：见 crate::circular_buff 模块文档")
    }
}
