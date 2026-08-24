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
//! 核心机制（环形状态机 + hook）对四种组合完全一致，只有 hook 的行为不同：
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
//! 构建完成后就开始流动（构造期即执行初始泵）。
//!
//! # hook 对调用者透明
//!
//! 构建器内部持有 `ProducerConfig` / `ConsumerConfig`（模式 + 设备，均为
//! crate 内部类型），`build` 时把它们挂载成核心内部的 hook（被动=唤醒槽位，
//! 主动=擦除后的设备 + 泵函数）。**hook 不会出现在 `CircularBuff` 的泛型参数
//! 里**——`CircularBuff` 的类型参数是**端类型**（`P` / `C`，决定可访问性）与
//! 存储（`B`）。
//!
//! 模式对调用者唯一可见的影响是**可访问性**：主动端不对外暴露，其端类型是
//! 占位类型（[`DeviceProducer`] / [`DeviceConsumer`]，携带设备实际类型）；
//! 被动端是可访问的端类型（[`PassiveProducer`] / [`PassiveConsumer`]）。
//!
//! # 类型状态（type-state）编码
//!
//! 构建流程是三个阶段，用不同的类型表达，因此「两端都必须在构建期被决定」是
//! 类型层面保证的——漏设一端无法编译：
//!
//! ```text
//! CircularBuffBuilder          // 阶段一：只有容量，尚未设置生产端
//!   └─ producer_passive / pipe_from_input
//!      → ProducerSetBuilder   // 阶段二：已设置生产端（P），尚未设置消费端
//!         └─ consumer_passive / pipe_into_output
//!            → ReadyBuilder   // 阶段三：两端都已设置，可以 build
//!               └─ build(&mut [MaybeUninit<T>]) → CircularBuff<'a, P, C, B, T>
//! ```
//!
//! 阶段类型携带**端类型**（`P` / `C`）：主动分支的端类型是
//! `DeviceProducer<I, T>` / `DeviceConsumer<O, T>`（`I` / `O` 为设备类型），
//! 被动分支是 `PassiveProducer<T>` / `PassiveConsumer<T>`。所有借用（缓冲区、
//! 主动端设备）共享同一个生命周期 `'a`。
//!
//! # 未设计完成的细节
//!
//! * 设备错误的传播（当前泵把设备错误视为「本轮无数据」，错误如何跨过 hook
//!   通知对端待定）；
//! * 被动端的取消（当前 `TrMayCancel` 忽略 token）。

use core::marker::PhantomData;
use core::mem::MaybeUninit;

use abs_buff::io::{TrInput, TrOutput};

use super::{
    abs_::{TrConsumer, TrProducer},
    circ_buff_::{CircularBuff, DeviceConsumer, DeviceProducer, PassiveConsumer, PassiveProducer},
    core_::{RingCore, WakeSlot, MAX_CAPACITY},
    hook_::{ActiveInput, ActiveOutput, ConsumerHook, ProducerHook},
};

/// 构建期对**生产端**「模式 + 设备」的配置（仅构建流程内部使用）。
///
/// 主动模式持有**已擦除**的输入设备（[`ActiveInput`]）：设备在
/// `pipe_from_input`（此时类型 `I` 具体可知）处被单态化，`build` 直接挂载，
/// 因此本类型不需要设备类型参数。设备借用的存续由**构建阶段类型**的生命周期
/// 参数（`PhantomData<&'a mut ()>`）保证，`build` 把它转入 `CircularBuff<'a>`。
pub(super) enum ProducerConfig<T> {
    /// 被动生产：调用者通过 `TrBuffTryWrite` 自行决定何时写入。
    Passive,
    /// 主动生产：构造后立即（并在每次消费端读取、释放可写空间后）
    /// 从该输入设备抽取数据填充缓冲。
    Active(ActiveInput<T>),
}

/// 构建期对**消费端**「模式 + 设备」的配置（仅构建流程内部使用）。
pub(super) enum ConsumerConfig<T> {
    /// 被动消费：调用者通过 `TrBuffTryRead` 自行决定何时读取。
    Passive,
    /// 主动消费：一旦有数据写入缓冲区，立即搬运到该输出设备。
    Active(ActiveOutput<T>),
}

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
    pub fn producer_passive<'a>(self) -> ProducerSetBuilder<'a, PassiveProducer<T>, T> {
        ProducerSetBuilder {
            capacity: self.capacity,
            producer: ProducerConfig::Passive,
            _producer: PhantomData,
            _borrow: PhantomData,
            _marker: PhantomData,
        }
    }

    /// 生产端设为**主动模式**：构造完成后立即从 `input` 抽取数据填充内部缓冲；
    /// 此后每当消费端读取、释放出可写空间，hook 立即从 `input` 拉取新数据。
    /// 该端由输入设备驱动，**对外不可访问**（端类型为
    /// [`DeviceProducer`]，`try_as_buff` 永远返回错误）。
    ///
    /// `I` 必须实现 `TrInput<T>`（abs_buff 的输入设备抽象）。
    pub fn pipe_from_input<'a, I>(
        self,
        input: &'a mut I,
    ) -> ProducerSetBuilder<'a, DeviceProducer<I, T>, T>
    where
        I: TrInput<T>,
    {
        ProducerSetBuilder {
            capacity: self.capacity,
            producer: ProducerConfig::Active(ActiveInput::new(input)),
            _producer: PhantomData,
            _borrow: PhantomData,
            _marker: PhantomData,
        }
    }
}

// ---------------------------------------------------------------------------
// 阶段二：已设置生产端，尚未设置消费端
// ---------------------------------------------------------------------------

/// 阶段二：生产端的模式与设备已确定，消费端尚未决定。
///
/// `P` 是最终 [`CircularBuff`] 的生产端类型参数（被动=可访问，主动=占位）。
/// `'a` 覆盖主动端设备与（最终）内部缓冲区的借用期。
pub struct ProducerSetBuilder<'a, P, T = u8> {
    capacity: usize,
    producer: ProducerConfig<T>,
    _producer: PhantomData<P>,
    /// 借用期标记：保持主动端设备（以及最终缓冲区）的借用存活。
    _borrow: PhantomData<&'a mut ()>,
    _marker: PhantomData<fn() -> T>,
}

impl<'a, P, T> ProducerSetBuilder<'a, P, T> {
    /// 消费端设为**被动模式**：调用者通过 `TrBuffTryRead` 异步等待就绪的内部
    /// 缓冲区，然后自行读取，该端对外可访问（端类型为 [`PassiveConsumer`]）。
    pub fn consumer_passive(self) -> ReadyBuilder<'a, P, PassiveConsumer<T>, T> {
        ReadyBuilder {
            capacity: self.capacity,
            producer: self.producer,
            consumer: ConsumerConfig::Passive,
            _producer: PhantomData,
            _consumer: PhantomData,
            _borrow: PhantomData,
            _marker: PhantomData,
        }
    }

    /// 消费端设为**主动模式**：一旦有任何数据写入缓冲区，立即搬运到 `output`
    /// 输出设备。该端由输出设备驱动，**对外不可访问**（端类型为
    /// [`DeviceConsumer`]，`try_as_buff` 永远返回错误）。
    ///
    /// `O` 必须实现 `TrOutput<T>`（abs_buff 的输出设备抽象）。设备借用与生产端
    /// 设备、内部缓冲区共享同一个生命周期 `'a`。
    pub fn pipe_into_output<O>(
        self,
        output: &'a mut O,
    ) -> ReadyBuilder<'a, P, DeviceConsumer<O, T>, T>
    where
        O: TrOutput<T>,
    {
        ReadyBuilder {
            capacity: self.capacity,
            producer: self.producer,
            consumer: ConsumerConfig::Active(ActiveOutput::new(output)),
            _producer: PhantomData,
            _consumer: PhantomData,
            _borrow: PhantomData,
            _marker: PhantomData,
        }
    }
}

// ---------------------------------------------------------------------------
// 阶段三：两端都已设置，可以构建
// ---------------------------------------------------------------------------

/// 阶段三：生产端与消费端的模式、设备都已确定，随时可以 `build`。
///
/// `P` / `C` 是最终 [`CircularBuff`] 的生产端 / 消费端类型参数。`'a` 覆盖
/// 主动端设备与内部缓冲区的借用期。
pub struct ReadyBuilder<'a, P, C, T = u8> {
    capacity: usize,
    producer: ProducerConfig<T>,
    consumer: ConsumerConfig<T>,
    _producer: PhantomData<P>,
    _consumer: PhantomData<C>,
    /// 借用期标记：保持主动端设备（以及最终缓冲区）的借用存活。
    _borrow: PhantomData<&'a mut ()>,
    _marker: PhantomData<fn() -> T>,
}

impl<'a, P, C, T> ReadyBuilder<'a, P, C, T>
where
    P: TrProducer<T>,
    C: TrConsumer<T>,
{
    /// 提供内部缓冲区并完成构建，返回固定好两端模式与设备的
    /// [`CircularBuff`]，其端类型参数即 `P` / `C`，存储参数 `B` 即传入的
    /// `&'a mut [MaybeUninit<T>]`（统一视为 `[MaybeUninit<T>]`，不经过任何
    /// 存储抽象，借用而非拥有；`no_std`、无 alloc）。
    ///
    /// `with_capacity` 指定的容量是**期望容量**：`storage` 的长度必须与之
    /// 完全一致（不一致时返回 `Err`，携带实际长度），且必须在
    /// `2..=MAX_CAPACITY` 范围内。
    ///
    /// 生产端为主动时，构建过程本身会执行第一轮「初始输入 pump」：构造返回前，
    /// 数据已经从 `TrInput` 流入内部缓冲（甚至已经流到 `TrOutput`）。
    pub fn build(
        self,
        storage: &'a mut [MaybeUninit<T>],
    ) -> Result<CircularBuff<'a, P, C, &'a mut [MaybeUninit<T>], T>, usize> {
        // 容量校验：期望容量与存储长度必须一致，且与 `ring_buffer` 的上限
        // 对齐（过小 / 过大都拒绝）。
        if self.capacity != storage.len() {
            return Err(storage.len());
        }
        if !(2..=MAX_CAPACITY).contains(&self.capacity) {
            return Err(self.capacity);
        }

        // 把构建期配置挂载成核心内部的 hook。
        let producer_hook = match self.producer {
            ProducerConfig::Passive => ProducerHook::Passive(WakeSlot::new()),
            ProducerConfig::Active(input) => ProducerHook::Active(input),
        };
        let consumer_hook = match self.consumer {
            ConsumerConfig::Passive => ConsumerHook::Passive(WakeSlot::new()),
            ConsumerConfig::Active(output) => ConsumerHook::Active(output),
        };

        let core = RingCore::new(
            storage.as_mut_ptr(),
            self.capacity,
            producer_hook,
            consumer_hook,
        );
        // 主动端初始泵：让数据开始流动（`start` 内部对被动模式无操作）。
        core.start();
        Ok(CircularBuff::new(core, storage))
    }
}
