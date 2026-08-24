//! 主动模式的测试：输入泵（`pipe_from_input`）、输出泵（`pipe_into_output`）、
//! 以及全主动流水线。主动端不 `spawn` 任何任务：数据在构建期 / 对端操作时
//! 由 hook 同步搬运。
//!
//! 设备状态经 `Arc` 观察：缓冲借用设备（`&mut input`）期间，测试通过自己
//! 持有的 `Arc` 副本读取设备内部数据，避免借用冲突。

use std::{
    sync::atomic::Ordering,
    vec,
    vec::Vec,
};

use abs_buff::{Demand, TrBuffTryRead, TrBuffTryWrite};

use super::{
    super::{CircularBuffBuilder, TrConsumer, TrProducer},
    fill_segm, storage, take_segm, TestInput, TestOutput,
};

/// 主动生产 × 被动消费：构造即从 `TrInput` 泵入；消费端每读取一次，
/// 释放的可写空间立即被新数据补满；输入耗尽后停止。
#[test]
fn pipe_from_input_fills_and_refills() {
    let input = TestInput::new((0..20).collect());
    let data = input.data.clone();
    let pos = input.pos.clone();
    let mut input = input; // 交给缓冲借用

    let mut st = storage::<8>();
    let mut buff = CircularBuffBuilder::with_capacity(8)
        .pipe_from_input(&mut input)
        .consumer_passive()
        .build(&mut st)
        .unwrap();

    // 构造完成即已泵入：容量 8 → 单空槽 → 最多 7 格数据。
    assert_eq!(buff.data_size(), 7);
    assert_eq!(pos.load(Ordering::Relaxed), 7, "输入设备已被读走 7 字节");

    // 主动生产端不对外暴露：TrProducer::try_as_buff 永远返回错误。
    assert!(
        TrProducer::try_as_buff(&mut buff).is_err(),
        "主动生产端不可访问"
    );
    // 消费端被动可访问。
    let mut rx = TrConsumer::try_as_buff(&mut buff).expect("消费端被动，可访问");

    // 边读边补：读空当前数据 → hook 立即从输入设备拉取下一批。
    let mut total = Vec::new();
    loop {
        let some = TrBuffTryRead::try_read(&mut rx, &Demand::at_least(1));
        let mut rs = match some.pick_left() {
            Some(s) => s,
            None => break, // 输入已耗尽且缓冲已空
        };
        let n = rs.least_count();
        let got = take_segm(&mut rs, n);
        drop(rs);
        total.extend(got);
        // 读取后（输入未耗尽时）应立即补满。
        let p = pos.load(Ordering::Relaxed);
        if p < 20 {
            assert_eq!(rx.data_size(), 7, "读取后应立即补满可写空间");
        }
    }
    assert_eq!(total, (0..20).collect::<Vec<_>>(), "读回全部输入");
    assert_eq!(rx.data_size(), 0);
    assert_eq!(pos.load(Ordering::Relaxed), 20, "输入设备已全部读完");
    assert_eq!(data.lock().unwrap().len(), 20);
}

/// 被动生产 × 主动消费：写入缓冲的数据**立即**被搬运到 `TrOutput`。
#[test]
fn pipe_into_output_drains_on_write() {
    let output = TestOutput::new();
    let out_data = output.data.clone();
    let mut output = output;

    let mut st = storage::<8>();
    let mut buff = CircularBuffBuilder::with_capacity(8)
        .producer_passive()
        .pipe_into_output(&mut output)
        .build(&mut st)
        .unwrap();

    // 生产端被动可访问；主动消费端不对外暴露（TrConsumer 报错）。
    assert!(
        TrConsumer::try_as_buff(&mut buff).is_err(),
        "主动消费端不可访问"
    );
    let mut tx = TrProducer::try_as_buff(&mut buff).expect("生产端被动，可访问");

    // 写 3 字节 → 写段 drop 提交 → 消费端 hook 立即泵出。
    let mut ws = TrBuffTryWrite::try_write(&mut tx, &Demand::at_least(3))
        .pick_left()
        .unwrap();
    fill_segm(&mut ws, &[1, 2, 3]);
    drop(ws);
    assert_eq!(
        *out_data.lock().unwrap(),
        vec![1, 2, 3],
        "写入后应立即泵到输出设备"
    );
    assert_eq!(tx.data_size(), 0, "泵出后缓冲应为空");

    // 连续多次写入：每次都即时泵出。
    for chunk in 0..3u8 {
        let mut ws = TrBuffTryWrite::try_write(&mut tx, &Demand::at_least(2))
            .pick_left()
            .unwrap();
        fill_segm(&mut ws, &[chunk * 10 + 1, chunk * 10 + 2]);
        drop(ws);
    }
    assert_eq!(*out_data.lock().unwrap(), vec![1, 2, 3, 1, 2, 11, 12, 21, 22]);
}

/// 主动 × 主动：`TrInput → 缓冲 → TrOutput` 同步流水线，构建完成即全部贯通。
#[test]
fn pipe_both_active_pipeline() {
    let input = TestInput::new((0..20).collect());
    let pos = input.pos.clone();
    let mut input = input;
    let output = TestOutput::new();
    let out_data = output.data.clone();
    let mut output = output;

    let mut st = storage::<8>();
    let buff = CircularBuffBuilder::with_capacity(8)
        .pipe_from_input(&mut input)
        .pipe_into_output(&mut output)
        .build(&mut st)
        .unwrap();

    // 构建期的一轮 drive 把整个流水线跑完：输入数据全部流到输出。
    assert_eq!(*out_data.lock().unwrap(), (0..20).collect::<Vec<_>>());
    assert_eq!(buff.data_size(), 0);
    assert_eq!(pos.load(Ordering::Relaxed), 20, "输入设备已全部读完");
}

/// 主动生产端在输入耗尽后停止泵入；再次消费时不再有数据。
#[test]
fn pipe_from_input_stops_when_exhausted() {
    let input = TestInput::new(vec![1, 2, 3]);
    let pos = input.pos.clone();
    let mut input = input;

    let mut st = storage::<8>();
    let mut buff = CircularBuffBuilder::with_capacity(8)
        .pipe_from_input(&mut input)
        .consumer_passive()
        .build(&mut st)
        .unwrap();

    let mut rx = TrConsumer::try_as_buff(&mut buff).unwrap();
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
    assert_eq!(total, vec![1, 2, 3]);
    assert_eq!(rx.data_size(), 0);
    assert_eq!(pos.load(Ordering::Relaxed), 3);
}
