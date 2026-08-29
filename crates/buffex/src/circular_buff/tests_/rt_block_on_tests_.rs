//! 阻塞式输入设备下的「主动端 + 泵」测试。
//!
//! # 被测约定：泵非阻塞、由操作驱动（不 spawn、不自旋）
//!
//! circular_buff 一端主动一端被动时，泵由**操作 / 提交路径**驱动：
//!
//! * 构建期 `init_async` 的初始搬运（`DevProducer` 填满缓冲）只做**非阻塞
//!   尝试**——设备 future 单次 poll，`Pending`（设备需外部唤醒 / 其它执行体
//!   推进）即放弃，**不自旋**；
//! * `try_read` 的 `Drained` 重试同样驱动一次非阻塞尝试，`Pending` 即返回
//!   `Drained`，不阻塞调用线程；
//! * 设备就绪后，下一次操作（`try_read` / 读取提交 `advance_read`）再次驱动
//!   泵，数据流入缓冲。
//!
//! 本模块用一个「必须等外部置位 `ready` 才就绪、Pending 时注册 waker」的
//! 阻塞式设备（[`CrossTaskInput`]，模拟真实连接如 iroh 的行为）验证上述
//! 语义，确保泵**不会自旋**饿死调用线程。

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

use abs_buff::{
    Demand,
    io::TrInput,
    x_deps::{abs_cancel, anylr},
};
use abs_cancel::{TrCancellationToken, TrMayCancel};
use anylr::SomeOf;

use super::{
    super::RxError,
    DefaultBuilder, TestErr, take_segm,
};

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
/// 泵**非阻塞、由操作驱动**（不 spawn、不自旋）：
///
/// 1. **构建不阻塞**：`build_async` 的 `init_async` 初始搬运对未就绪设备只做
///    非阻塞尝试（`Pending` 即放弃）——`build_async()` 正常返回、缓冲为空，
///    **不会**把调用线程饿死；
/// 2. **try_read 驱动非阻塞尝试**：设备未就绪时，`try_read` 的 `Drained`
///    重试驱动输入泵（单次 poll，`Pending` 即放弃）→ 返回 `Drained`，不阻塞；
/// 3. **设备就绪后操作驱动补位**：置位 `ready` 后，下一次 `try_read` 的驱动
///    拉到数据 → 成功。
///
/// # 构造
/// 线程内：用 [`CrossTaskInput`]（未就绪）构建「主动生产 × 被动消费」，确认
/// `build_async()` 返回；随后按上述 2/3 步操作 `try_read`。
///
/// # 判定
/// (1) `build_async()` 在 100ms 内返回；(2) 设备未就绪时 `try_read` 返回
/// `Drained`（不阻塞）；(3) 设备就绪后 `try_read` 在 2s 内读到 `0..8`。
#[test]
fn pump_is_non_blocking_and_operation_driven() {
    use std::sync::mpsc;

    let ready = Arc::new(AtomicBool::new(false));
    let waker_slot: Arc<Mutex<Option<Waker>>> = Arc::new(Mutex::new(None));

    let (built_tx, built_rx) = mpsc::channel();
    let (result_tx, result_rx) = mpsc::channel();

    let handle = std::thread::spawn(move || {
        // —— 1. 构建不阻塞：init_async 初始搬运只做非阻塞尝试。 ——
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
        assert_eq!(rx.data_size(), 0, "设备未就绪：初始搬运无数据");

        // —— 2. try_read 驱动一次非阻塞尝试：设备未就绪 → Drained，不阻塞。 ——
        let demand = Demand::at_least(8);
        let some = rx.try_read(&demand);
        assert!(
            matches!(some.pick_right(), Some(RxError::Drained(_))),
            "设备未就绪：try_read 应返回 Drained（驱动不做非阻塞尝试后放弃）"
        );

        // —— 3. 设备就绪后：下一次 try_read 的驱动拉到数据。 ——
        ready.store(true, Ordering::Release);
        let some = rx.try_read(&demand);
        let mut segm = some
            .pick_left()
            .expect("设备就绪后 try_read 应经驱动拉到数据");
        let n = segm.least_count();
        let got = take_segm(&mut segm, n);
        result_tx.send(got).unwrap();
    });

    // (1) build_async 必须在 100ms 内返回（init_async 初始搬运非阻塞、不自旋）。
    built_rx
        .recv_timeout(Duration::from_millis(100))
        .expect("build 不应阻塞：init_async 对未就绪设备只做非阻塞尝试");

    // (3) 设备就绪后 try_read 必须在 2s 内成功（非阻塞驱动，不会自旋空转）。
    let got = result_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("设备就绪后 try_read 应经驱动拉到数据（而非自旋）");
    assert_eq!(got, (0..8).collect::<Vec<_>>(), "读出的数据应与输入一致");

    // 不要 join：若实现回归为自旋，线程会永久卡死，join 会让测试挂住。
    drop(handle);
}
