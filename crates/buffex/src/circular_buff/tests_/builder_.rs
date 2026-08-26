//! 构建器**顺序灵活性**的测试：生产端 / 消费端可以任意顺序设置
//! （`pipe_from_input` / `pipe_into_output` / `producer_passive` /
//! `consumer_passive`），可以一步同时设置两端（`pipe_between`），也可以两端
//! 都不设置、直接 `build()` 得到双端被动（经典手动管道）。
//!
//! 等价性断言建立在 `pump_` / `sync_` 已有行为之上——换序与 `pipe_between`
//! 必须产生与原有链完全一致的结果。

use std::{sync::atomic::Ordering, vec, vec::Vec};

use abs_buff::{Demand, TrBuffTryRead, TrBuffTryWrite};
use abs_mm::mem_alloc::CoreAlloc;
use mm_ptr::x_deps::abs_mm;

use crate::{
    circular_buff::{
        Consumer, Producer,
        BuffConsumer, BuffProducer, BuilderError, CircularBuffBuilder,
        DeviceConsumer, DeviceProducer, SpscPair,
        tests_::{TestInput, TestOutput, fill_segm, take_segm},
    },
};

type Pair<P, C, T = u8, A = CoreAlloc> = (Producer<P, C, T, A>, Consumer<P, C, T, A>);

/// 默认双端被动：不 pipe 任何设备，`with_capacity(...).build()` 直接得到
/// 一对可用的 Producer / Consumer（经典手动管道）。
#[test]
fn build_without_pipe_defaults_to_passive_pair() {
    let pair: SpscPair = CircularBuffBuilder::with_capacity(8).build().unwrap();

    let (mut tx, mut rx) = pair;
    assert_eq!(tx.capacity(), 8);

    // 写 3 字节 → 读回同样的 3 字节（被动 × 被动语义完整）。
    let mut ws = TrBuffTryWrite::try_write(&mut tx, &Demand::at_least(3))
        .pick_left()
        .unwrap();
    fill_segm(&mut ws, &[1, 2, 3]);
    drop(ws);
    assert_eq!(rx.data_size(), 3);

    let mut rs = TrBuffTryRead::try_read(&mut rx, &Demand::at_least(3))
        .pick_left()
        .unwrap();
    assert_eq!(take_segm(&mut rs, 3), vec![1, 2, 3]);
    drop(rs);
    assert_eq!(rx.data_size(), 0);
}

/// 显式双端被动，但**消费端先设、生产端后设**（与传统的
/// `producer_passive().consumer_passive()` 顺序对调）。
#[test]
fn consumer_first_then_producer_passive() {
    let pair: SpscPair = CircularBuffBuilder::with_capacity(8)
        .consumer_passive()
        .producer_passive()
        .build()
        .unwrap();
    let (mut tx, mut rx) = pair;

    let mut ws = TrBuffTryWrite::try_write(&mut tx, &Demand::at_least(2))
        .pick_left()
        .unwrap();
    fill_segm(&mut ws, &[7, 8]);
    drop(ws);
    let mut rs = TrBuffTryRead::try_read(&mut rx, &Demand::at_least(2))
        .pick_left()
        .unwrap();
    assert_eq!(take_segm(&mut rs, 2), vec![7, 8]);
    drop(rs);
}

/// 消费端先行（`pipe_into_output`）、生产端后设（`pipe_from_input`）：
/// 与 `pipe_from_input(...).pipe_into_output(...)` 完全等价——构建期一轮
/// drive 即把输入全部流到输出。
#[test]
fn pipe_into_output_then_pipe_from_input() {
    let input = TestInput::new((0..20).collect());
    let pos = input.pos.clone();
    let output = TestOutput::new();
    let out_data = output.data.clone();

    let _pair: Pair<
        DeviceProducer<TestInput, u8>,
        DeviceConsumer<TestOutput, u8>,
    > = CircularBuffBuilder::with_capacity(8)
        .pipe_into_output(output)
        .pipe_from_input(input)
        .build()
        .unwrap();

    assert_eq!(*out_data.lock().unwrap(), (0..20).collect::<Vec<_>>());
    assert_eq!(pos.load(Ordering::Relaxed), 20);
}

/// `pipe_between(input, output)`：一步同时设置两端，等价于两段式全主动
/// 流水线。
#[test]
fn pipe_between_builds_active_pipeline() {
    let input = TestInput::new((0..20).collect());
    let pos = input.pos.clone();
    let output = TestOutput::new();
    let out_data = output.data.clone();

    let _pair: Pair<
        DeviceProducer<TestInput, u8>,
        DeviceConsumer<TestOutput, u8>,
    > = CircularBuffBuilder::with_capacity(8)
        .pipe_between(input, output)
        .build()
        .unwrap();

    assert_eq!(*out_data.lock().unwrap(), (0..20).collect::<Vec<_>>());
    assert_eq!(pos.load(Ordering::Relaxed), 20);
}

/// 消费端先行（`pipe_into_output`）+ 生产端被动（`producer_passive`）：
/// 与 `producer_passive().pipe_into_output(...)` 顺序对调的等价形态——
/// 写入缓冲的数据立即被搬运到输出设备。
#[test]
fn pipe_into_output_then_passive_producer() {
    let output = TestOutput::new();
    let out_data = output.data.clone();

    let (mut tx, _rx): Pair<BuffProducer<u8>, DeviceConsumer<TestOutput, u8>> =
        CircularBuffBuilder::with_capacity(8)
            .pipe_into_output(output)
            .producer_passive()
            .build()
            .unwrap();

    let mut ws = TrBuffTryWrite::try_write(&mut tx, &Demand::at_least(3))
        .pick_left()
        .unwrap();
    fill_segm(&mut ws, &[1, 2, 3]);
    drop(ws);
    assert_eq!(*out_data.lock().unwrap(), vec![1, 2, 3]);
    assert_eq!(tx.data_size(), 0);
}

/// 消费端先行（`consumer_passive`）+ 生产端主动（`pipe_from_input`）：
/// 与 `pipe_from_input(...).consumer_passive()` 顺序对调的等价形态——
/// 构造完成即已泵入，消费驱动补位。
#[test]
fn consumer_passive_then_pipe_from_input() {
    let input = TestInput::new((0..10).collect());
    let pos = input.pos.clone();

    let (_tx, mut rx): Pair<DeviceProducer<TestInput, u8>, BuffConsumer<u8>> =
        CircularBuffBuilder::with_capacity(8)
            .consumer_passive()
            .pipe_from_input(input)
            .build()
            .unwrap();

    // 构造完成即已泵入：容量 8 → 单空槽 → 最多 7 格数据。
    assert_eq!(rx.data_size(), 7);
    assert_eq!(pos.load(Ordering::Relaxed), 7);

    let mut total = Vec::new();
    loop {
        let some = TrBuffTryRead::try_read(&mut rx, &Demand::at_least(1));
        let mut rs = match some.pick_left() {
            Some(s) => s,
            None => break,
        };
        let n = rs.least_count();
        total.extend(take_segm(&mut rs, n));
        drop(rs);
    }
    assert_eq!(total, (0..10).collect::<Vec<_>>());
    assert_eq!(pos.load(Ordering::Relaxed), 10);
}

/// 容量校验仍然生效：直接 `build()`（默认双端被动）同样执行容量区间检查
/// （`MIN_CAPACITY` = 2 ..= `MAX_CAPACITY` = `(1 << 28) - 1`）。
#[test]
fn build_default_still_validates_capacity() {
    type Built = Result<SpscPair, BuilderError<usize>>;

    let r0: Built = CircularBuffBuilder::with_capacity(0).build();
    assert!(matches!(r0, Err(BuilderError::SizeTooSmall(0))));

    let r1: Built = CircularBuffBuilder::with_capacity(1).build();
    assert!(matches!(r1, Err(BuilderError::SizeTooSmall(1))));

    let too_big = 1usize << 28; // 超出 POS_MASK
    let rb: Built = CircularBuffBuilder::with_capacity(too_big).build();
    assert!(matches!(
        rb,
        Err(BuilderError::SizeTooBig(c)) if c == too_big
    ));
}
