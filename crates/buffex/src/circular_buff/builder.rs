//! 构建器：在**构建期**决定两端的模式（被动 / 主动）与用途（数据从哪来、到
//! 哪去），并把端类型（`circ_buff_` 的 `BuffProducer` /
//! `BuffConsumer` / `DeviceProducer` / `DeviceConsumer`）装配进核心。
//!
//! # 设计意图
//!
//! * **构造期定模式**：模式一旦构建完成即固定，之后由 hook 联动驱动；
//! * **类型状态强制**：`CircularBuffBuilder`（仅容量）→ 任一「单端已定」的中态
//!   （[`ProducerSetBuilder`] 生产端已定 / [`ConsumerSetBuilder`] 消费端已定）→
//!   [`ReadyBuilder`]（两端已定）→ `build`。type-state 链强制「两端模式必须在
//!   构建期决定」，漏设一端无法编译；
//! * **两端顺序自由**：生产端与消费端**任意顺序**设置——可以从生产端开始
//!   （[`CircularBuffBuilder::pipe_from_input`] / [`CircularBuffBuilder::producer_passive`]）
//!   再从消费端收尾，也可以从消费端开始
//!   （[`CircularBuffBuilder::pipe_into_output`] / [`CircularBuffBuilder::consumer_passive`]）
//!   再从生产端收尾；还可以用 [`CircularBuffBuilder::pipe_between`] 一步同时设置
//!   两端，或两端都不设置、直接 [`CircularBuffBuilder::build`] 得到双端被动
//!   （经典手动管道）；
//! * **设备 move 进核心**：`pipe_from_input` / `pipe_into_output` 把设备以
//!   具体类型装入端类型，核心无需类型擦除；
//! * **拥有型产物**：`build` 在堆上分配核心与缓冲（`Shared<CircCore>`，
//!   分配器 `A` 默认 [`CoreAlloc`]），产出 [`SpscPair`]——没有「缓冲聚合体」
//!   这一层，使用者只持有 [`Producer`] / [`Consumer`] 半部。
//!
//! # 用法一览（生产端/消费端可任意换序）
//!
//! ```ignore
//! // 双端被动（默认）：经典手动管道，两端都可访问——不 pipe 任何设备
//! let (mut tx, mut rx) = CircularBuffBuilder::with_capacity(4096)
//!     .build()?;
//!
//! // 显式双端被动（先设生产端，再设消费端）
//! let (mut tx, mut rx) = CircularBuffBuilder::with_capacity(4096)
//!     .producer_passive()
//!     .consumer_passive()
//!     .build()?;
//!
//! // 显式双端被动（先设消费端，再设生产端——顺序与上例对调）
//! let (mut tx, mut rx) = CircularBuffBuilder::with_capacity(4096)
//!     .consumer_passive()
//!     .producer_passive()
//!     .build()?;
//!
//! // 主动生产 × 被动消费：从 TrInput 自动灌入，用户自行读取
//! let (tx, mut rx) = CircularBuffBuilder::with_capacity(4096)
//!     .pipe_from_input(input)      // 生产端先行
//!     .consumer_passive()
//!     .build()?;
//!
//! // 被动生产 × 主动消费：用户自行写入，写后自动搬运到 TrOutput
//! let (mut tx, rx) = CircularBuffBuilder::with_capacity(4096)
//!     .pipe_into_output(output)    // 消费端先行
//!     .producer_passive()
//!     .build()?;
//!
//! // 主动 × 主动：TrInput → 缓冲 → TrOutput 自动流水线（两端都不可直接访问）
//! let (tx, rx) = CircularBuffBuilder::with_capacity(4096)
//!     .pipe_into_output(output)    // 消费端先行
//!     .pipe_from_input(input)      // 生产端后设
//!     .build()?;
//!
//! // 一步同时设置两端（等价于上例）
//! let (tx, rx) = CircularBuffBuilder::with_capacity(4096)
//!     .pipe_between(input, output)
//!     .build()?;
//! ```
//!
//! 缓冲容量在 `build` 时校验（`core_::MIN_CAPACITY` ..= `core_::MAX_CAPACITY`）。

use core::marker::PhantomData;

use abs_buff::io::{TrInput, TrOutput};
use mm_ptr::{
    Shared,
    x_deps::abs_mm::mem_alloc::{CoreAlloc, TrMalloc},
};

use super::{
    abs_comp_::{TrConsumer, TrProducer},
    circ_buff_::{BuffConsumer, BuffProducer, DeviceConsumer, DeviceProducer},
    core_::{self, CircCore},
    spsc_::{Consumer, Producer, SpscPair},
};

/// 构建错误：容量超出合法区间（`core_::MIN_CAPACITY` ..= `core_::MAX_CAPACITY`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuilderError<T> {
    /// 容量过小。
    SizeTooSmall(T),
    /// 容量过大。
    SizeTooBig(T),
}

/// 构建链起点：只定了容量（与分配器），两端模式均未决定。
///
/// 从这里可以：
///
/// * 先定**生产端**（[`Self::producer_passive`] / [`Self::pipe_from_input`]），
///   再进入 [`ProducerSetBuilder`] 定消费端；
/// * 先定**消费端**（[`Self::consumer_passive`] / [`Self::pipe_into_output`]），
///   再进入 [`ConsumerSetBuilder`] 定生产端；
/// * 一步同时定两端（[`Self::pipe_between`]）；
/// * 两端都不定，直接 [`Self::build`]——默认双端被动（经典手动管道）。
pub struct CircularBuffBuilder<T = u8, A = CoreAlloc>
where
    A: TrMalloc + Clone + Default,
{
    capacity: usize,
    alloc: A,
    _use_t_: PhantomData<fn() -> T>,
}

impl<T, A> CircularBuffBuilder<T, A>
where
    A: TrMalloc + Clone + Default,
{
    /// 以默认分配器（[`CoreAlloc`]）构造，容量在 `build` 时校验。
    pub fn with_capacity(capacity: usize) -> Self {
        CircularBuffBuilder {
            capacity,
            alloc: A::default(),
            _use_t_: PhantomData,
        }
    }

    /// 以自定义分配器构造。
    pub fn with_allocator(capacity: usize, alloc: A) -> Self {
        CircularBuffBuilder {
            capacity,
            alloc,
            _use_t_: PhantomData,
        }
    }

    /// 生产端为被动模式：调用者驱动写入。进入 [`ProducerSetBuilder`]，
    /// 下一步（`consumer_passive` / `pipe_into_output`）定消费端。
    pub fn producer_passive(self) -> ProducerSetBuilder<BuffProducer<T>, T, A> {
        ProducerSetBuilder {
            capacity: self.capacity,
            producer: BuffProducer::new(),
            alloc: self.alloc,
            _use_t_: PhantomData,
        }
    }

    /// 生产端为主动模式：从输入设备 `input` 自动灌入缓冲（设备 move 进核心）。
    /// 进入 [`ProducerSetBuilder`]，下一步（`consumer_passive` /
    /// `pipe_into_output`）定消费端。
    pub fn pipe_from_input<I>(
        self,
        input: I,
    ) -> ProducerSetBuilder<DeviceProducer<I, T>, T, A>
    where
        I: TrInput<T>,
    {
        ProducerSetBuilder {
            capacity: self.capacity,
            producer: DeviceProducer::new(input),
            alloc: self.alloc,
            _use_t_: PhantomData,
        }
    }

    /// 消费端为被动模式：调用者驱动读取。进入 [`ConsumerSetBuilder`]，
    /// 下一步（`producer_passive` / `pipe_from_input`）定生产端。
    pub fn consumer_passive(self) -> ConsumerSetBuilder<BuffConsumer<T>, T, A> {
        ConsumerSetBuilder {
            capacity: self.capacity,
            consumer: BuffConsumer::new(),
            alloc: self.alloc,
            _use_t_: PhantomData,
        }
    }

    /// 消费端为主动模式：缓冲数据自动搬运到输出设备 `output`（设备 move 进
    /// 核心）。进入 [`ConsumerSetBuilder`]，下一步（`producer_passive` /
    /// `pipe_from_input`）定生产端。
    pub fn pipe_into_output<O>(
        self,
        output: O,
    ) -> ConsumerSetBuilder<DeviceConsumer<O, T>, T, A>
    where
        O: TrOutput<T>,
    {
        ConsumerSetBuilder {
            capacity: self.capacity,
            consumer: DeviceConsumer::new(output),
            alloc: self.alloc,
            _use_t_: PhantomData,
        }
    }

    /// 一步同时决定两端：生产端为主动（`input` 自动灌入）、消费端为主动
    /// （自动搬运到 `output`）——`TrInput → 缓冲 → TrOutput` 同步流水线。
    ///
    /// 等价于 `pipe_from_input(input).pipe_into_output(output)` 或
    /// `pipe_into_output(output).pipe_from_input(input)`。直接进入
    /// [`ReadyBuilder`]，下一步即 `build`。
    pub fn pipe_between<I, O>(
        self,
        input: I,
        output: O,
    ) -> ReadyBuilder<DeviceProducer<I, T>, DeviceConsumer<O, T>, T, A>
    where
        I: TrInput<T>,
        O: TrOutput<T>,
    {
        ReadyBuilder {
            capacity: self.capacity,
            producer: DeviceProducer::new(input),
            consumer: DeviceConsumer::new(output),
            alloc: self.alloc,
            _use_t_: PhantomData,
        }
    }

    /// 两端都不设置时的默认：双端被动（经典手动管道），直接产出半部对。
    ///
    /// 等价于 `producer_passive().consumer_passive().build()` 或
    /// `consumer_passive().producer_passive().build()`。
    pub fn build(self) -> Result<SpscPair<T, A>, BuilderError<usize>>
    where
        T: Send + Sync,
        A: Send + Sync + TrMalloc + Clone,
    {
        ReadyBuilder {
            capacity: self.capacity,
            producer: BuffProducer::new(),
            consumer: BuffConsumer::new(),
            alloc: self.alloc,
            _use_t_: PhantomData,
        }
        .build()
    }
}

/// 构建链中段：**生产端**模式已定，消费端模式尚未决定。
///
/// 由 [`CircularBuffBuilder::producer_passive`] / [`CircularBuffBuilder::pipe_from_input`]
/// 进入；下一步（`consumer_passive` / `pipe_into_output`）定消费端。
pub struct ProducerSetBuilder<P, T = u8, A = CoreAlloc>
where
    P: TrProducer<Data = T>,
    A: TrMalloc + Clone,
{
    capacity: usize,
    producer: P,
    alloc: A,
    _use_t_: PhantomData<fn() -> T>,
}

impl<P, T, A> ProducerSetBuilder<P, T, A>
where
    P: TrProducer<Data = T>,
    A: TrMalloc + Clone,
{
    /// 消费端为被动模式：调用者驱动读取。
    pub fn consumer_passive(self) -> ReadyBuilder<P, BuffConsumer<T>, T, A> {
        ReadyBuilder {
            capacity: self.capacity,
            producer: self.producer,
            consumer: BuffConsumer::new(),
            alloc: self.alloc,
            _use_t_: PhantomData,
        }
    }

    /// 消费端为主动模式：缓冲数据自动搬运到输出设备 `output`。
    pub fn pipe_into_output<O>(
        self,
        output: O,
    ) -> ReadyBuilder<P, DeviceConsumer<O, T>, T, A>
    where
        O: TrOutput<T>,
    {
        ReadyBuilder {
            capacity: self.capacity,
            producer: self.producer,
            consumer: DeviceConsumer::new(output),
            alloc: self.alloc,
            _use_t_: PhantomData,
        }
    }
}

/// 构建链中段：**消费端**模式已定，生产端模式尚未决定。
///
/// 与 [`ProducerSetBuilder`] 对称——由
/// [`CircularBuffBuilder::consumer_passive`] / [`CircularBuffBuilder::pipe_into_output`]
/// 进入（「先设消费端、再设生产端」的顺序）；下一步（`producer_passive` /
/// `pipe_from_input`）定生产端。
pub struct ConsumerSetBuilder<C, T = u8, A = CoreAlloc>
where
    C: TrConsumer<Data = T>,
    A: TrMalloc + Clone,
{
    capacity: usize,
    consumer: C,
    alloc: A,
    _use_t_: PhantomData<fn() -> T>,
}

impl<C, T, A> ConsumerSetBuilder<C, T, A>
where
    C: TrConsumer<Data = T>,
    A: TrMalloc + Clone,
{
    /// 生产端为被动模式：调用者驱动写入。
    pub fn producer_passive(self) -> ReadyBuilder<BuffProducer<T>, C, T, A> {
        ReadyBuilder {
            capacity: self.capacity,
            producer: BuffProducer::new(),
            consumer: self.consumer,
            alloc: self.alloc,
            _use_t_: PhantomData,
        }
    }

    /// 生产端为主动模式：从输入设备 `input` 自动灌入缓冲（设备 move 进核心）。
    pub fn pipe_from_input<I>(
        self,
        input: I,
    ) -> ReadyBuilder<DeviceProducer<I, T>, C, T, A>
    where
        I: TrInput<T>,
    {
        ReadyBuilder {
            capacity: self.capacity,
            producer: DeviceProducer::new(input),
            consumer: self.consumer,
            alloc: self.alloc,
            _use_t_: PhantomData,
        }
    }
}

/// 构建链终点：两端模式均已决定，可以 `build`。
pub struct ReadyBuilder<P, C, T = u8, A = CoreAlloc>
where
    P: TrProducer<Data = T>,
    C: TrConsumer<Data = T>,
    A: TrMalloc + Clone,
{
    capacity: usize,
    producer: P,
    consumer: C,
    alloc: A,
    _use_t_: PhantomData<fn() -> T>,
}

impl<P, C, T, A> ReadyBuilder<P, C, T, A>
where
    P: Send + Sync + TrProducer<Data = T>,
    C: Send + Sync + TrConsumer<Data = T>,
    T: Send + Sync,
    A: Send + Sync + TrMalloc + Clone,
{
    /// 装配核心并产出半部对 `(Producer, Consumer)`。
    ///
    /// 校验容量 → 分配缓冲并装入两端（`CircCore::new`）→ 移到堆上
    /// （[`Shared`]）→ 构建期初始驱动（主动端先泵一轮，让数据开始流动）。
    pub fn build(self) -> Result<(Producer<P, C, T, A>, Consumer<P, C, T, A>), BuilderError<usize>> {
        let cap = self.capacity;
        if cap < core_::MIN_CAPACITY {
            return Err(BuilderError::SizeTooSmall(cap));
        }
        if cap > core_::MAX_CAPACITY {
            return Err(BuilderError::SizeTooBig(cap));
        }
        let core = CircCore::new(
            cap,
            self.producer,
            self.consumer,
            self.alloc.clone(),
        );
        let shared = Shared::new(core, self.alloc);
        let producer = Producer::new(shared.clone());
        let consumer = Consumer::new(shared.clone());
        // 构建期初始驱动：主动端先各自泵一轮（被动端为无操作）。
        shared.start();
        Ok((producer, consumer))
    }
}
