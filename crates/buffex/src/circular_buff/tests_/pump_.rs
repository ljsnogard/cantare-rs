//! 主动模式的测试：输入泵（`pipe_from_input`）、输出泵（`pipe_into_output`）、
//! 以及全主动流水线。主动端不 `spawn` 任何任务：数据在构建期 / 对端操作时
//! 由 hook 同步搬运。
//!
//! 设备 move 进核心后测试无法直接访问，经 `Arc` 观察其内部状态。

use std::{
    sync::atomic::Ordering,
    vec,
    vec::Vec,
};

use abs_buff::{Demand, TrBuffTryRead, TrBuffTryWrite};
use mm_ptr::x_deps::abs_mm::mem_alloc::CoreAlloc;

use super::{
    super::{
        BuffConsumer, BuffProducer, CircularBuffBuilder, Consumer, DeviceConsumer,
        DeviceProducer, Producer, RxError, TxError,
    },
    fill_segm, take_segm, TestInput, TestOutput,
};

type Pair<P, C, T = u8, A = CoreAlloc> = (Producer<P, C, T, A>, Consumer<P, C, T, A>);

/// 主动生产 × 被动消费：构造即从 `TrInput` 泵入；消费端每读取一次，
/// 释放的可写空间立即被新数据补满；输入耗尽后停止。
#[test]
fn pipe_from_input_fills_and_refills() {
    let input = TestInput::new((0..20).collect());
    let data = input.data.clone();
    let pos = input.pos.clone();

    let (mut tx, mut rx): Pair<DeviceProducer<TestInput, u8>, BuffConsumer<u8>> =
        CircularBuffBuilder::with_capacity(8)
            .pipe_from_input(input)
            .consumer_passive()
            .build()
            .unwrap();

    // 构造完成即已泵入：容量 8 → 单空槽 → 最多 7 格数据。
    assert_eq!(rx.data_size(), 7);
    assert_eq!(pos.load(Ordering::Relaxed), 7, "输入设备已被读走 7 字节");

    // 主动生产端不对外暴露：写半部操作返回 Unavailable。
    assert!(
        TrBuffTryWrite::try_write(&mut tx, &Demand::at_least(1))
            .pick_right()
            .is_some(),
        "主动生产端的写半部不可用"
    );

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

    let (mut tx, mut rx): Pair<BuffProducer<u8>, DeviceConsumer<TestOutput, u8>> =
        CircularBuffBuilder::with_capacity(8)
            .producer_passive()
            .pipe_into_output(output)
            .build()
            .unwrap();

    // 主动消费端不对外暴露：读半部操作返回 Unavailable。
    assert!(
        TrBuffTryRead::try_read(&mut rx, &Demand::at_least(1))
            .pick_right()
            .is_some(),
        "主动消费端的读半部不可用"
    );

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
    let output = TestOutput::new();
    let out_data = output.data.clone();

    let _pair: Pair<DeviceProducer<TestInput, u8>, DeviceConsumer<TestOutput, u8>> =
        CircularBuffBuilder::with_capacity(8)
            .pipe_from_input(input)
            .pipe_into_output(output)
            .build()
            .unwrap();

    // 构建期的一轮 drive 把整个流水线跑完：输入数据全部流到输出。
    assert_eq!(*out_data.lock().unwrap(), (0..20).collect::<Vec<_>>());
    assert_eq!(pos.load(Ordering::Relaxed), 20, "输入设备已全部读完");
}

/// 主动生产端在输入耗尽后停止泵入；再次消费时不再有数据。
#[test]
fn pipe_from_input_stops_when_exhausted() {
    let input = TestInput::new(vec![1, 2, 3]);
    let pos = input.pos.clone();

    let (_tx, mut rx): Pair<DeviceProducer<TestInput, u8>, BuffConsumer<u8>> =
        CircularBuffBuilder::with_capacity(8)
            .pipe_from_input(input)
            .consumer_passive()
            .build()
            .unwrap();

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

/// 主动端的半部错误类型（编译期检查 Unavailable 变体存在且可达）。
#[allow(dead_code)]
fn _assert_unavailable_errors() {
    let _ = TxError::<usize>::Unavailable;
    let _ = RxError::<usize>::Unavailable;
}

/// 一个「数据迟到」的输入设备：`gate` 置位后才开始供数，并记录 `read_async`
/// 的调用次数（用于验证半部操作的自动驱动）。
struct GatedInput {
    data: Vec<u8>,
    pos: usize,
    gate: std::sync::Arc<std::sync::atomic::AtomicBool>,
    calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl abs_buff::io::TrInput<u8> for GatedInput {
    type ReadAsync<'f> = super::ReadySegm<usize, super::TestErr> where Self: 'f;
    type Err = super::TestErr;

    fn read_async<'f>(
        &'f mut self,
        target: &'f mut [core::mem::MaybeUninit<u8>],
    ) -> Self::ReadAsync<'f> {
        use std::sync::atomic::Ordering as O;
        self.calls.fetch_add(1, O::Relaxed);
        if !self.gate.load(O::Acquire) || self.pos >= self.data.len() {
            return super::ReadySegm::new(abs_buff::x_deps::anylr::SomeOf::new_left(0));
        }
        let n = core::cmp::min(target.len(), self.data.len() - self.pos);
        for (i, slot) in target[..n].iter_mut().enumerate() {
            *slot = core::mem::MaybeUninit::new(self.data[self.pos + i]);
        }
        self.pos += n;
        super::ReadySegm::new(abs_buff::x_deps::anylr::SomeOf::new_left(n))
    }
}

/// 对端（生产端）为主动时，`try_read` **自动**驱动一轮输入泵：缓冲为空、
/// 设备数据「迟到」（构造后才可用）时，单次 `try_read` 即拉到数据——
/// 调用者无需任何手动 drive（无后台任务模型下「操作即事件」）。
#[test]
fn try_read_auto_drives_active_producer() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize},
    };

    let gate = Arc::new(AtomicBool::new(false));
    let calls = Arc::new(AtomicUsize::new(0));
    let input = GatedInput {
        data: vec![7, 8, 9],
        pos: 0,
        gate: gate.clone(),
        calls: calls.clone(),
    };

    let (_tx, mut rx): Pair<DeviceProducer<GatedInput, u8>, BuffConsumer<u8>> =
        CircularBuffBuilder::with_capacity(8)
            .pipe_from_input(input)
            .consumer_passive()
            .build()
            .unwrap();

    // 构造期 start() 泵了一轮，但门未开 → 缓冲为空。
    assert_eq!(rx.data_size(), 0);
    assert_eq!(calls.load(Ordering::Relaxed), 1);

    // 数据「迟到」：门打开后，单次 try_read 自动驱动输入泵并读到数据。
    gate.store(true, Ordering::Release);
    let some = TrBuffTryRead::try_read(&mut rx, &Demand::at_least(3));
    let mut rs = some.pick_left().expect("try_read 应自动拉到数据");
    assert_eq!(rs.least_count(), 3);
    assert_eq!(take_segm(&mut rs, 3), vec![7, 8, 9]);
    drop(rs);

    // 设备已供完：后续 try_read 自动驱动一次（读到 Drained），不阻塞。
    assert!(calls.load(Ordering::Relaxed) >= 3, "try_read 应自动驱动输入泵");
    let some = TrBuffTryRead::try_read(&mut rx, &Demand::at_least(1));
    assert!(
        matches!(some.pick_right(), Some(RxError::Drained(_))),
        "设备无更多数据时 try_read 返回 Drained"
    );
}

/// 写端关闭（`ProducerClose` 事件）时，消费端 `check` 仍感兴趣 → 泵排空残留
/// 数据（写后未及搬运的部分在 close 时被搬走）。
#[test]
fn close_tx_drains_remaining_output() {
    let output = TestOutput::new();
    let out_data = output.data.clone();

    let (mut tx, _rx): Pair<BuffProducer<u8>, DeviceConsumer<TestOutput, u8>> =
        CircularBuffBuilder::with_capacity(8)
            .producer_passive()
            .pipe_into_output(output)
            .build()
            .unwrap();

    // 写 5 字节（一次借出可写区，全部写入并提交）。
    let mut ws = TrBuffTryWrite::try_write(&mut tx, &Demand::at_least(5))
        .pick_left()
        .expect("应有 5 格可写空间");
    fill_segm(&mut ws, &[1, 2, 3, 4, 5]);
    drop(ws);
    assert_eq!(
        *out_data.lock().unwrap(),
        vec![1, 2, 3, 4, 5],
        "写入提交即泵出"
    );

    // 再写 2 字节后立即 close：残留数据必须在 close 的 ProducerClose 事件
    // 驱动下被排空（check 对 ProducerClose 感兴趣）。
    let mut ws = TrBuffTryWrite::try_write(&mut tx, &Demand::at_least(2))
        .pick_left()
        .expect("应有 2 格可写空间");
    fill_segm(&mut ws, &[6, 7]);
    drop(ws);
    assert_eq!(*out_data.lock().unwrap(), vec![1, 2, 3, 4, 5, 6, 7]);

    tx.close();
    assert_eq!(*out_data.lock().unwrap(), vec![1, 2, 3, 4, 5, 6, 7]);
}
