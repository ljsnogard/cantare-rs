//! 单线程异步运行时下的「主动端 + 泵」测试。
//!
//! # 被测约定：泵由 executor 驱动，而非自旋自驱动
//!
//! circular_buff 一端主动一端被动时，被动操作会驱动主动泵。泵与设备的交互
//! 分两条路径：
//!
//! * **同步路径**（`start` / 提交路径 / `try_*`）：泵只做**非阻塞尝试**——
//!   设备 future 单次 poll，`Pending`（设备需外部唤醒 / 其它执行体推进）即
//!   放弃本轮，**不自旋**；
//! * **异步路径**（`read_async` / `write_async` 的 park）：泵 `await` 设备
//!   future——设备阻塞（`Pending`）时等待 future **挂起**（设备已注册其
//!   waker），由 **executor 驱动**；设备就绪后数据流入缓冲。
//!
//! 本模块用一个「必须等另一个 task 置位才就绪、Pending 时注册 executor
//! waker」的阻塞式设备（[`CrossTaskInput`]，模拟真实连接如 iroh 的行为），
//! 在单线程 executor（`futures_executor::LocalPool`）中验证：
//!
//! 1. 构建期同步泵不阻塞：设备未就绪 → `build_async()` 正常返回（缓冲为空），
//!    不会把单线程执行器饿死；
//! 2. 异步等待被 executor 驱动：`read_async` 的泵 await 设备 → 挂起 →
//!    另一个 task 置位并唤醒 → 泵继续 → 数据流入 → 读完成。

use std::{
    future::Future,
    mem::MaybeUninit,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
    vec::Vec,
};

use futures_executor::LocalPool;
use futures_util::task::LocalSpawnExt;

use abs_buff::{
    Demand,
    io::TrInput,
    x_deps::{abs_cancel, anylr},
};
use abs_cancel::{TrCancellationToken, TrMayCancel};
use anylr::SomeOf;

use super::{DefaultBuilder, TestErr, take_segm};

/// 一个必须等待外部 task 置位 `ready` 后才能完成的输入设备。
///
/// `poll` 时：
///
///     ready == false -> 注册 executor waker 并返回 Pending
///     ready == true  -> 返回数据
///
/// 这是真实阻塞设备（如 iroh 连接）的语义：无数据时挂起并注册 waker，数据
/// 到达（其它执行体置位）后唤醒等待者。
struct CrossTaskInput {
    data: Vec<u8>,
    pos: usize,
    ready: Arc<AtomicBool>,
    waker_slot: Arc<Mutex<Option<Waker>>>,
}

impl CrossTaskInput {
    fn new(
        data: Vec<u8>,
        ready: Arc<AtomicBool>,
        waker_slot: Arc<Mutex<Option<Waker>>>,
    ) -> Self {
        Self {
            data,
            pos: 0,
            ready,
            waker_slot,
        }
    }
}

struct CrossTaskRead<'f> {
    dev: &'f mut CrossTaskInput,
    target: &'f mut [MaybeUninit<u8>],
}

impl Future for CrossTaskRead<'_> {
    type Output = SomeOf<usize, TestErr>;

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if !this.dev.ready.load(Ordering::Acquire) {
            // 未就绪：注册 executor waker（真实阻塞设备语义——数据到达后
            // 由置位方唤醒），然后挂起。
            *this.dev.waker_slot.lock().unwrap() = Some(cx.waker().clone());
            return Poll::Pending;
        }

        let n = core::cmp::min(
            this.target.len(),
            this.dev.data.len() - this.dev.pos,
        );
        for (i, slot) in this.target[..n].iter_mut().enumerate() {
            *slot = MaybeUninit::new(this.dev.data[this.dev.pos + i]);
        }
        this.dev.pos += n;

        Poll::Ready(SomeOf::new_left(n))
    }
}

impl<'f> TrMayCancel<'f> for CrossTaskRead<'f> {
    type MayCancelFuture<'g, C> = CrossTaskRead<'f>
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

impl TrInput<u8> for CrossTaskInput {
    type ReadAsync<'f> = CrossTaskRead<'f> where Self: 'f;
    type Err = TestErr;

    fn read_async<'f>(
        &'f mut self,
        target: &'f mut [MaybeUninit<u8>],
    ) -> Self::ReadAsync<'f> {
        CrossTaskRead { dev: self, target }
    }
}

/// # 被测约定
/// 泵由 **executor 驱动**，而非 `Waker::noop()` 循环自驱动：
///
/// 1. **同步泵不阻塞**：`build_async()` 的同步泵对未就绪设备只做一次非阻塞尝试，
///    `Pending` 即放弃——`build_async()` 正常返回，**不会**把单线程执行器饿死；
/// 2. **异步等待由 executor 驱动**：`read_async` 的 park 每 poll 重建一轮泵
///    并 `await` 设备——设备 `Pending`（已注册 executor waker）时挂起；另一个
///    task 置位并唤醒后，executor 重新驱动泵 → 数据流入 → 读完成。
///
/// # 构造
/// 线程内：先用 [`CrossTaskInput`]（未就绪）构建「主动生产 × 被动消费」，
/// 确认 `build_async()` 返回（同步泵不阻塞）；再在 `LocalPool` 里同时 spawn
/// `read_async` 与「置位 + 唤醒」两个 task，`run()` 驱动。
///
/// # 判定
/// (1) `build_async()` 在 100ms 内返回（同步泵非阻塞、不自旋）；(2) `read_async`
/// 在 2s 内完成并读出 `0..8`（executor 驱动泵，而非自旋空转）。
#[test]
fn pump_is_executor_driven_in_single_thread_executor() {
    use std::sync::mpsc;

    let ready = Arc::new(AtomicBool::new(false));
    let waker_slot: Arc<Mutex<Option<Waker>>> = Arc::new(Mutex::new(None));

    let (built_tx, built_rx) = mpsc::channel();
    let (result_tx, result_rx) = mpsc::channel();

    let handle = std::thread::spawn(move || {
        // —— 1. 同步泵（非阻塞尝试）：设备未就绪 → build 不阻塞、不自旋。 ——
        // （build 现为异步 `build_async`，用 `futures_lite::future::block_on`
        // 驱动；其内部对未就绪设备的同步泵仍只做非阻塞尝试。）
        let mut ready_builder = DefaultBuilder::with_capacity(8)
            .unwrap()
            .pipe_from_input(CrossTaskInput::new(
                (0..20).collect(),
                ready.clone(),
                waker_slot.clone(),
            ))
            .consumer_passive();
        let mut rx =
            futures_lite::future::block_on(ready_builder.build_async().into_future())
                .unwrap();
        built_tx.send(()).unwrap();

        // —— 2. executor 驱动：LocalPool 中，read_async 的泵 await 设备。 ——
        let mut pool = LocalPool::new();
        let spawner = pool.spawner();

        // 读者 task：其 park 每 poll 重建一轮输入泵并 await 设备——设备
        // Pending（注册 executor waker）时挂起，由 executor 驱动。
        spawner
            .spawn_local(async move {
                let demand = Demand::at_least(8);
                let fut = rx.read_async(&demand);
                let mut segm = fut
                    .into_future()
                    .await
                    .pick_left()
                    .expect("executor 驱动后应有数据");
                let n = segm.least_count();
                let got = take_segm(&mut segm, n);
                result_tx.send(got).unwrap();
            })
            .unwrap();

        // 「设备就绪」task：先让出（给读者 task 先 poll、注册 waker 的机会），
        // 再置位并唤醒——验证唤醒确实由 executor 调度，而非自旋。
        spawner
            .spawn_local(async move {
                futures_lite::future::yield_now().await;
                ready.store(true, Ordering::Release);
                if let Some(w) = waker_slot.lock().unwrap().take() {
                    w.wake();
                }
            })
            .unwrap();

        pool.run();
    });

    // (1) 同步泵不阻塞：build_async() 必须在 100ms 内返回。
    built_rx
        .recv_timeout(Duration::from_millis(100))
        .expect("build 不应阻塞：同步泵对未就绪设备只做非阻塞尝试");

    // (2) executor 驱动：read_async 必须在 2s 内由 executor 驱动完成并读出数据。
    let got = result_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("executor 驱动应让读完成（而非自旋空转）");
    assert_eq!(got, (0..8).collect::<Vec<_>>(), "读出的数据应与输入一致");

    // 不要 join：若实现回归为自旋，线程会永久卡死，join 会让测试挂住。
    drop(handle);
}
