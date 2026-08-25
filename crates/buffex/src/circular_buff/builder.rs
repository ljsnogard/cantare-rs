//! 构建器：在**构建期**决定两端的模式（被动 / 主动）与用途（数据从哪来、到
//! 哪去），并把端类型（`circ_buff_` 的 `BuffProducer` /
//! `BuffConsumer` / `DeviceProducer` / `DeviceConsumer`）装配进核心。
//!
//! # 设计意图
//!
//! * **构造期定模式**：模式一旦构建完成即固定，之后由 hook 联动驱动；
//! * **类型状态强制**：`CircularBuffBuilder → ProducerSetBuilder →
//!   ReadyBuilder` 的 type-state 链强制「两端模式必须在构建期决定」，
//!   漏设一端无法编译；
//! * **设备 move 进核心**：`pipe_from_input` / `pipe_into_output` 把设备以
//!   具体类型装入端类型，核心无需类型擦除；
//! * **拥有型产物**：`build` 在堆上分配核心与缓冲（`Shared<CircCore>`，
//!   分配器 `A` 默认 [`CoreAlloc`]），产出 [`SpscPair`]——没有「缓冲聚合体」
//!   这一层，使用者只持有 [`Producer`] / [`Consumer`] 半部。
//!
//! 缓冲容量在 `build` 时校验（`core_::MIN_CAPACITY` ..= `core_::MAX_CAPACITY`）。

use core::marker::PhantomData;

use abs_buff::io::{TrInput, TrOutput};
use mm_ptr::{
    Shared,
    x_deps::abs_mm::mem_alloc::{CoreAlloc, TrMalloc},
};

use super::{
    abs_comp::{TrConsumer, TrProducer},
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

/// 构建链起点：只定了容量（与分配器），生产端模式尚未决定。
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

    /// 生产端为被动模式：调用者驱动写入。
    pub fn producer_passive(self) -> ProducerSetBuilder<BuffProducer<T>, T, A> {
        ProducerSetBuilder {
            capacity: self.capacity,
            producer: BuffProducer::new(),
            alloc: self.alloc,
            _use_t_: PhantomData,
        }
    }

    /// 生产端为主动模式：从输入设备 `input` 自动灌入缓冲（设备 move 进核心）。
    pub fn pipe_from_input<I>(self, input: I) -> ProducerSetBuilder<DeviceProducer<I, T>, T, A>
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
}

/// 构建链中段：生产端模式已定，消费端模式尚未决定。
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
    pub fn pipe_into_output<O>(self, output: O) -> ReadyBuilder<P, DeviceConsumer<O, T>, T, A>
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
    /// 校验容量 → 分配缓冲并装入两端（[`CircCore::new`]）→ 移到堆上
    /// （[`Shared`]）→ 构建期初始驱动（主动端先泵一轮，让数据开始流动）。
    pub fn build(self) -> Result<SpscPair<P, C, T, A>, BuilderError<usize>> {
        let cap = self.capacity;
        if cap < core_::MIN_CAPACITY {
            return Err(BuilderError::SizeTooSmall(cap));
        }
        if cap > core_::MAX_CAPACITY {
            return Err(BuilderError::SizeTooBig(cap));
        }
        let core = CircCore::new(cap, self.producer, self.consumer, self.alloc.clone());
        let shared = Shared::new(core, self.alloc);
        let producer = Producer::new(shared.clone());
        let consumer = Consumer::new(shared.clone());
        // 构建期初始驱动：主动端先各自泵一轮（被动端为无操作）。
        shared.start();
        Ok((producer, consumer))
    }
}
