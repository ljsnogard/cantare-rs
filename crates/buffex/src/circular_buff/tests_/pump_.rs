//! 主动模式的测试：输入泵（`pipe_from_input`）、输出泵（`pipe_into_output`）、
//! 以及全主动流水线（`Pipeline` future）。主动端不 `spawn` 任何任务：数据在
//! 构建期 / 对端操作时由 hook 同步搬运；全主动流水线由设备驱动的 `Pipeline`
//! future 持续搬运。
//!
//! 主动端**不产出半部**：`build` 只把被动端的半部交给调用者（主动生产 ×
//! 被动消费 → 仅消费端；被动生产 × 主动消费 → 仅生产端；主动 × 主动 →
//! `Pipeline` future）。
//!
//! 设备 move 进核心后测试无法直接访问，经 `Arc` 观察其内部状态。

use std::{pin::Pin, sync::atomic::Ordering, vec, vec::Vec};

use abs_buff::{
    Demand, TrBuffTryRead, TrBuffTryWrite,
    x_deps::{
        abs_cancel::{TrCancellationToken, TrMayCancel},
        anylr::SomeOf,
    },
};
use mm_ptr::x_deps::abs_mm::mem_alloc::CoreAlloc;

use super::{
    super::{CircularBuffBuilder, RxError, TxError},
    TestErr, TestInput, TestOutput, TestWaker, fill_segm, poll_once, take_segm,
};

/// 主动生产 × 被动消费：构造即从 `TrInput` 泵入；消费端每读取一次，
/// 释放的可写空间立即被新数据补满；输入耗尽后停止。
///
/// `build` 只返回消费端半部——主动生产端由设备驱动，不产出写半部。
#[test]
fn pipe_from_input_fills_and_refills() {
    let input = TestInput::new((0..20).collect());
    let data = input.data.clone();
    let pos = input.pos.clone();

    let mut rx = CircularBuffBuilder::<u8, CoreAlloc>::with_capacity(8)
        .pipe_from_input(input)
        .consumer_passive()
        .build()
        .unwrap();

    // 构造完成即已泵入：容量 8 → 单空槽 → 最多 7 格数据。
    assert_eq!(rx.data_size(), 7);
    assert_eq!(pos.load(Ordering::Relaxed), 7, "输入设备已被读走 7 字节");

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
///
/// `build` 只返回生产端半部——主动消费端由设备驱动，不产出读半部。
#[test]
fn pipe_into_output_drains_on_write() {
    let output = TestOutput::new();
    let out_data = output.data.clone();

    let mut tx = CircularBuffBuilder::<u8, CoreAlloc>::with_capacity(8)
        .producer_passive()
        .pipe_into_output(output)
        .build()
        .unwrap();

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
    assert_eq!(
        *out_data.lock().unwrap(),
        vec![1, 2, 3, 1, 2, 11, 12, 21, 22]
    );
}

/// 主动 × 主动：`TrInput → 缓冲 → TrOutput` 流水线。`build` 返回
/// [`Pipeline`] future；首个 poll 即由设备驱动把当前可用的输入全部流到输出。
#[test]
fn pipe_both_active_pipeline() {
    let input = TestInput::new((0..20).collect());
    let pos = input.pos.clone();
    let output = TestOutput::new();
    let out_data = output.data.clone();

    let mut pipeline = CircularBuffBuilder::<u8, CoreAlloc>::with_capacity(8)
        .pipe_from_input(input)
        .pipe_into_output(output)
        .build()
        .unwrap();

    // 首个 poll：输入泵 + 输出泵跑完整个流水线（非阻塞设备立即就绪）。
    let (waker, _wake_flag) = TestWaker::make_waker_tuple();
    let mut pinned = Pin::new(&mut pipeline);
    let _ = poll_once(pinned.as_mut(), &waker);

    assert_eq!(*out_data.lock().unwrap(), (0..20).collect::<Vec<_>>());
    assert_eq!(pos.load(Ordering::Relaxed), 20, "输入设备已全部读完");
}

/// 阻塞式输入设备：无数据时 `read_async` 挂起（注册 waker）；调用方经 `Arc`
/// 压入数据并唤醒后，下一次 poll 即有数据可读。用于验证「由设备驱动」的
/// 流水线流动。
struct BlockingInput {
    data: std::sync::Arc<std::sync::Mutex<Vec<u8>>>,
    pos: usize,
    waker: std::sync::Arc<std::sync::Mutex<Option<std::task::Waker>>>,
}

impl BlockingInput {
    fn new() -> (
        Self,
        std::sync::Arc<std::sync::Mutex<Vec<u8>>>,
        std::sync::Arc<std::sync::Mutex<Option<std::task::Waker>>>,
    ) {
        let data = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let waker = std::sync::Arc::new(std::sync::Mutex::new(None));
        (
            BlockingInput {
                data: data.clone(),
                pos: 0,
                waker: waker.clone(),
            },
            data,
            waker,
        )
    }
}

struct BlockingRead<'f> {
    input: &'f mut BlockingInput,
    target: &'f mut [core::mem::MaybeUninit<u8>],
}

impl core::future::Future for BlockingRead<'_> {
    type Output = SomeOf<usize, TestErr>;

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut core::task::Context<'_>,
    ) -> core::task::Poll<Self::Output> {
        let this = &mut *self;
        let data = this.input.data.lock().unwrap();
        let avail = data.len().saturating_sub(this.input.pos);
        if avail == 0 {
            // 无数据：注册 waker 并挂起；数据到达时由 push 侧唤醒。
            *this.input.waker.lock().unwrap() = Some(cx.waker().clone());
            return core::task::Poll::Pending;
        }
        let n = core::cmp::min(this.target.len(), avail);
        for (i, slot) in this.target[..n].iter_mut().enumerate() {
            *slot = core::mem::MaybeUninit::new(data[this.input.pos + i]);
        }
        this.input.pos += n;
        core::task::Poll::Ready(SomeOf::new_left(n))
    }
}

impl<'f> TrMayCancel<'f> for BlockingRead<'f> {
    type MayCancelFuture<'g, C> = BlockingRead<'f>
    where
        Self: 'g,
        C: TrCancellationToken + Clone,
        C: 'f,
        C: 'g,
        'g: 'f;
    type MayCancelOutput = SomeOf<usize, TestErr>;

    fn may_cancel_with<'g, C>(
        self,
        _cancel: &'g mut C,
    ) -> Self::MayCancelFuture<'g, C>
    where
        Self: 'g,
        'g: 'f,
        C: TrCancellationToken + Clone,
    {
        self
    }
}

impl abs_buff::io::TrInput<u8> for BlockingInput {
    type ReadAsync<'f> = BlockingRead<'f> where Self: 'f;
    type Err = TestErr;

    fn read_async<'f>(
        &'f mut self,
        target: &'f mut [core::mem::MaybeUninit<u8>],
    ) -> Self::ReadAsync<'f> {
        BlockingRead {
            input: self,
            target,
        }
    }
}

/// 双端全主动：数据流动**由两端设备驱动**——`Pipeline` future 存活期间持续
/// 搬运，直到调用者请求断开。
///
/// 1. 输入设备无数据 → 流水线挂起（await 输入设备的 `read_async`，Pending）；
/// 2. 压入数据并唤醒 → 流水线自动把数据流到输出（无需任何显式 drive）；
/// 3. 再次压入 → 再次流动；
/// 4. `disconnect_handle().request()` → 流水线关闭两端并结束（future 返回）。
#[test]
fn pipeline_flows_driven_by_devices_until_disconnect() {
    let (input, in_data, in_waker) = BlockingInput::new();
    let output = TestOutput::new();
    let out_data = output.data.clone();

    let pipeline = CircularBuffBuilder::<u8, CoreAlloc>::with_capacity(8)
        .pipe_between(input, output)
        .build()
        .unwrap();
    let disconnect = pipeline.disconnect_handle();

    let (waker, _wake_flag) = TestWaker::make_waker_tuple();
    let mut pipeline = pipeline;
    let mut pinned = Pin::new(&mut pipeline);

    // 输入设备无数据：流水线挂起，等待输入设备唤醒。
    assert!(
        poll_once(pinned.as_mut(), &waker).is_pending(),
        "输入设备无数据时应挂起"
    );
    assert!(out_data.lock().unwrap().is_empty());

    // 设备就绪（压入数据 + 唤醒）：流水线由输入设备驱动，数据自动流到输出。
    in_data.lock().unwrap().extend_from_slice(&[1, 2, 3]);
    if let Some(w) = in_waker.lock().unwrap().take() {
        w.wake();
    }
    assert!(poll_once(pinned.as_mut(), &waker).is_pending());
    assert_eq!(
        *out_data.lock().unwrap(),
        vec![1, 2, 3],
        "设备就绪后数据应自动流动"
    );

    // 第二批数据：同样自动流动（输入设备再次就绪）。
    in_data.lock().unwrap().extend_from_slice(&[4, 5]);
    if let Some(w) = in_waker.lock().unwrap().take() {
        w.wake();
    }
    assert!(poll_once(pinned.as_mut(), &waker).is_pending());
    assert_eq!(*out_data.lock().unwrap(), vec![1, 2, 3, 4, 5]);

    // 请求断开：流水线关闭两端、排空残留并结束。
    disconnect.request();
    assert!(poll_once(pinned.as_mut(), &waker).is_ready());
    assert_eq!(*out_data.lock().unwrap(), vec![1, 2, 3, 4, 5]);
}

/// 主动生产端在输入耗尽后停止泵入；再次消费时不再有数据。
#[test]
fn pipe_from_input_stops_when_exhausted() {
    let input = TestInput::new(vec![1, 2, 3]);
    let pos = input.pos.clone();

    let mut rx = CircularBuffBuilder::<u8, CoreAlloc>::with_capacity(8)
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
    type ReadAsync<'f>
        = super::ReadySegm<usize, super::TestErr>
    where
        Self: 'f;
    type Err = super::TestErr;

    fn read_async<'f>(
        &'f mut self,
        target: &'f mut [core::mem::MaybeUninit<u8>],
    ) -> Self::ReadAsync<'f> {
        use std::sync::atomic::Ordering as O;
        self.calls.fetch_add(1, O::Relaxed);
        if !self.gate.load(O::Acquire) || self.pos >= self.data.len() {
            return super::ReadySegm::new(
                abs_buff::x_deps::anylr::SomeOf::new_left(0),
            );
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

    let mut rx = CircularBuffBuilder::<u8, CoreAlloc>::with_capacity(8)
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
    assert!(
        calls.load(Ordering::Relaxed) >= 3,
        "try_read 应自动驱动输入泵"
    );
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

    let mut tx = CircularBuffBuilder::<u8, CoreAlloc>::with_capacity(8)
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
