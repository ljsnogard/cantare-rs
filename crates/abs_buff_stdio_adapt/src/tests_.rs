//! # 集成测试：`AsStdRead` / `AsStdWrite` 与「异步数据流」的真实对接
//!
//! ## 背景与测试意图
//!
//! 适配器内部通过 `abs_art-bridge` 的 `TrBlockOn::block_on` 把 `abs_buff` 的
//! 异步借用操作（`read_async` / `write_async`）同步驱动到完成。本模块要证明的
//! 不是「字节搬移」本身正确（那是 `abs_buff` / `buffex` 自己的测试范围），而是
//! **适配器真的等到了异步数据**：
//!
//! - 当数据流中的数据要**稍后**才由另一个执行体（后台线程 / tokio 异步任务）
//!   产生时，一次 `read` 必须阻塞等待并返回完整正确的数据；
//! - 当数据流**暂时为空但尚未关闭**时，`read` 不能把「暂时为空」误判成 EOF
//!   提前退出（返回 0 或半截数据）；
//! - 写方向同理：`write` 写进异步接收端的数据必须完整、按序、无丢失。
//!
//! 若实现存在「第一轮 poll 没有数据就提前退出」型错误，下面的测试会在内容、
//! 数量或耗时断言上失败——这正是本模块存在的意义。
//!
//! ## 测试载体：`buffex` 的 SPSC 环形管道
//!
//! [`RingTx`] / [`RingRx`] 实现了 `abs_buff` 的 `TrBuffWrite` / `TrBuffRead`：
//! 空管道上 `read_async` 会保持 Pending 直到写端送来数据并唤醒 waker；满管道上
//! `write_async` 会保持 Pending 直到读端腾出空间。它是一条**真实、可延迟、可跨
//! 线程**的异步数据流，正适合用来暴露「提前退出」型错误。
//!
//! ## 运行前提
//!
//! 适配器内部的 `block_on`（tokio 后端）要求当前线程处于**多线程 tokio 运行时**
//! 上下文（`Handle::current()` 可用，且 `block_in_place` 需要其它 worker 承接
//! 任务）。因此所有直接调用适配器的测试都包在 [`with_tokio_rt`] 里执行。
//! 测试所用的 `backend-tokio` 由本 crate 的 `[dev-dependencies]` 启用。

use std::{
    mem::MaybeUninit,
    sync::{mpsc, Arc},
    thread,
    time::{Duration, Instant},
    vec,
    vec::Vec,
};

use abs_art_bridge::{BLOCK_ON, Runtime, TrBlockOn};
use abs_buff::{
    Demand, TrBuffTryRead, TrBuffTryWrite, TrBuffWrite,
    x_deps::anylr,
};
use abs_mm::mem_alloc::CoreAlloc;
use anylr::SomeOf;
use buffex::{
    ring_buffer::{RingBuffer, RingRx, RingTx},
    x_deps::{abs_mm, mm_ptr},
};
use mm_ptr::Owned;

use crate::{AsStdRead, AsStdWrite};

// ===========================================================================
// 公共辅助
// ===========================================================================

/// 环形管道各类型的别名：写半区 / 读半区共享同一个 `Arc<RingBuffer>`。
type SharedRing = Arc<RingBuffer<Box<[MaybeUninit<u8>]>>>;
type Tx = RingTx<SharedRing, Box<[MaybeUninit<u8>]>>;
type Rx = RingRx<SharedRing, Box<[MaybeUninit<u8>]>>;

/// circular buff（`buffex::circular_buff`）各类型的别名：双端被动、`u8` 元素、
/// `CoreAlloc` 分配器。
type CbBuf = Owned<[MaybeUninit<u8>], CoreAlloc>;
type CbPair = buffex::circular_buff::SpscPair<CbBuf>;
type CbTx = buffex::circular_buff::Producer<
    buffex::circular_buff::BufConsumer<u8>,
    CbBuf,
    u8,
    CoreAlloc,
>;
type CbRx = buffex::circular_buff::Consumer<
    buffex::circular_buff::BufProducer<u8>,
    CbBuf,
    u8,
    CoreAlloc,
>;

/// 建立一条容量为 `cap` 的 SPSC 环形管道，返回 (写半区, 读半区)。
///
/// 说明：`try_split_shared` 要求以「唯一持有者」身份拆分（`Arc` 强引用计数
/// == 1），所以这里把新建的 `Arc`（计数为 1）直接移入拆分；拆分后写/读半区
/// 各自持有一个 `Arc`，管道随半区存活。
fn make_ring(cap: usize) -> (Tx, Rx) {
    let ring = Arc::new(
        RingBuffer::<Box<[MaybeUninit<u8>]>>::try_new(
            vec![MaybeUninit::uninit(); cap].into_boxed_slice(),
        )
        .expect("cap 必须在 [2, MAX_CAPACITY] 内"),
    );
    let (tx, rx) = RingBuffer::try_split_shared(ring, Arc::strong_count, Arc::weak_count)
        .expect("新建 ring 的引用计数为 1，拆分必须成功");
    (tx, rx)
}

/// 把 `data` 全部写入写半区（同步 API，供后台线程/测试主体直接使用）。
///
/// 借用段是 ring 自身的内存：数据通过段原语 `move_items_from_buff` 直接拷入，
/// 段 drop 时按消费量推进写位置（提交）。可能分多次借用直到写完。
fn write_all_into_tx(tx: &mut Tx, data: &[u8]) {
    let mut off = 0usize;
    while off < data.len() {
        let mut segm = tx
            .try_write_at_most(data.len() - off)
            .expect("写半区尚有空间，借用必须成功");
        let n = segm.least_count();
        assert!(n > 0, "借用得到的写段不可能为空");
        let mut staging: Vec<MaybeUninit<u8>> = data[off..off + n]
            .iter()
            .map(|&b| MaybeUninit::new(b))
            .collect();
        // SAFETY: 把 `u8` 逐位拷入 ring 段并推进段内偏移（段 drop 时提交给
        // ring）；`u8` 无 drop 需求，staging 中剩余内容无需处理。
        let moved = unsafe { segm.move_items_from_buff(&mut staging) };
        assert_eq!(moved, n);
        drop(segm); // 段 drop = 提交这 n 字节（写位置前进、唤醒读端）
        off += n;
    }
}

/// 把读半区当前可读的数据全部读出（同步 API，供后台线程/测试主体直接使用）。
fn drain_available(rx: &mut Rx) -> Vec<u8> {
    let mut out = Vec::new();
    while let Ok(mut segm) = rx.try_read_at_most(usize::MAX) {
        let n = segm.least_count();
        assert!(n > 0, "读段不可能为空");
        let mut dst: Vec<MaybeUninit<u8>> = (0..n).map(|_| MaybeUninit::uninit()).collect();
        // SAFETY: 把 ring 段中的 `u8` 逐位搬进 `dst`（段 drop 时推进读位置）；
        // `u8` 无 drop 需求。
        let moved = unsafe { segm.move_items_to_buff(&mut dst) };
        assert_eq!(moved, n);
        out.extend(dst.into_iter().map(|m| unsafe { m.assume_init_read() }));
        drop(segm);
    }
    out
}

// ---------------------------------------------------------------------------
// circular buff（`buffex::circular_buff`）载体：双端被动，实现 TrBuffRead /
// TrBuffWrite（空/满时会真实 Pending 等待对端并注册 waker）
// ---------------------------------------------------------------------------

/// 建立一条容量为 `cap` 的**双端被动** circular buff，返回 (写端, 读端)。
///
/// 说明：`circular_buff` 的构建器以类型状态强制两端模式；双端被动是经典
/// 手动管道——`AsStdWrite` 写进生产端、数据经环形缓冲流动、`AsStdRead` 从
/// 消费端读出。
fn make_cb_pair(cap: usize) -> CbPair {
    let mut ready = buffex::circular_buff::CircularBuffBuilder::with_capacity(cap)
        .expect("cap 必须在 [MIN_CAPACITY, MAX_CAPACITY] 内")
        .producer_passive()
        .consumer_passive();
    // `abs_art_bridge::Runtime::block_on` 的 inherent 方法要求 `F: 'static`，
    // 但 `build_async` 返回的 future 借用 `ready`，因此这里走 trait 方法
    // `TrBlockOn::block_on`（无 `'static` 限制），并在临时多线程 tokio 运行时
    // 上下文中驱动。
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("构建 tokio 运行时失败");
    rt.block_on(async {
        <Runtime<{ BLOCK_ON }> as TrBlockOn<_>>::block_on(
            ready.build_async().into_future(),
        )
        .expect("双端被动构建不可能失败")
    })
}

/// 把 `data` 全部写入 circular buff 写端（同步 API，供后台线程使用）。
///
/// 经 `TrBuffTryWrite::try_write` 借出两段式写段，数据经段原语
/// `move_items_from_buff` 位拷贝进缓冲，段 drop 时提交（推进写位置、唤醒
/// 读端）。可能分多次借用直到写完。
fn cb_write_all(tx: &mut CbTx, data: &[u8]) {
    let mut off = 0usize;
    while off < data.len() {
        let demand = Demand::at_least(1);
        let mut segm = TrBuffTryWrite::try_write(tx, &demand)
            .pick_left()
            .expect("写端应有可写空间");
        // 只搬本轮剩余载荷：借到的写段可能比剩余数据大。
        let n = core::cmp::min(segm.least_count(), data.len() - off);
        assert!(n > 0, "借到的写段不可能为空");
        let mut staging: Vec<MaybeUninit<u8>> = data[off..off + n]
            .iter()
            .map(|&b| MaybeUninit::new(b))
            .collect();
        // SAFETY: `u8` 无 drop，位拷贝安全；段 drop 时按 `n` 提交给缓冲。
        let moved = unsafe { segm.move_items_from_buff(&mut staging) };
        assert_eq!(moved, n);
        drop(segm);
        off += n;
    }
}

/// 把 circular buff 读端当前可读的数据全部读出（同步 API，供后台线程使用）。
/// 读端空（且未关闭）时返回空——「等待」由调用方的循环 + 适配器负责。
fn cb_drain_available(rx: &mut CbRx) -> Vec<u8> {
    let mut out = Vec::new();
    let demand = Demand::at_least(1);
    while let Some(mut segm) = TrBuffTryRead::try_read(rx, &demand).pick_left() {
        let n = segm.least_count();
        assert!(n > 0, "读段不可能为空");
        let mut dst: Vec<MaybeUninit<u8>> = (0..n).map(|_| MaybeUninit::uninit()).collect();
        // SAFETY: 把缓冲段中的 `u8` 逐位搬进 `dst`（段 drop 时推进读位置）；
        // `u8` 无 drop 需求。
        let moved = unsafe { segm.move_items_to_buff(&mut dst) };
        assert_eq!(moved, n);
        out.extend(dst.into_iter().map(|m| unsafe { m.assume_init_read() }));
        drop(segm);
    }
    out
}

/// 在**多线程 tokio 运行时上下文**内执行 `f`，并返回其结果。
///
/// 适配器内部的 `block_on`（tokio 后端）要求：① 当前线程处于运行时上下文
/// （`Handle::current()` 可用）；② 必须是多线程运行时（`block_in_place` 需要
/// 其它 worker 承接被让渡的任务队列）。`enable_all` 让 `tokio::time::sleep`
/// 等定时能力可用。
fn with_tokio_rt<F, R>(f: F) -> R
where
    F: FnOnce(&tokio::runtime::Runtime) -> R,
{
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("构建多线程 tokio 运行时失败");
    // 注意：不能用 `async move`（会把 `rt` 整个移入 future，导致下方
    // `rt.block_on` 借用已移动的值）。非 move 的 async 块按需捕获：`f` 被
    // 移动（`FnOnce` 调用），`rt` 只被借用，future 与 `block_on` 同为不可变
    // 借用，合法。
    rt.block_on(async { f(&rt) })
}

// ===========================================================================
// 读方向：异步数据流 → std::io::Read
// ===========================================================================

/// 目的：证明 `AsStdRead::read` 面对「数据要过一段时间才从异步数据流到达」的
/// 场景时，会通过 `abs_art-bridge` 的 `block_on` **阻塞等待**直到数据真正到达，
/// 而不是因为第一轮 poll 没有数据就立即返回 0（提前退出）。
///
/// 测试方法：
/// 1. 建立容量 64 的环形管道，读半区包上 `AsStdRead`；
/// 2. 启动后台线程作为异步生产者：先 `sleep(150ms)`（保证此刻读端的 `read`
///    已经进入 `read_async` 等待、第一轮 poll 必然 Pending），再把 32 字节的
///    已知载荷写入管道并关闭写端（EOF）；
/// 3. 主线程在 tokio 运行时上下文里调用**一次** `read`，并记录耗时；
///
/// 通过依据（同时满足才算通过）：
/// - `read` 返回的字节数必须等于 32（若提前退出，返回 0，读缓冲保持全 0）；
/// - 读出的 32 字节必须与载荷逐字节相同（内容与顺序正确）；
/// - 耗时必须 ≥ 130ms（150ms - 20ms 余量）：直接证明调用阻塞到了异步数据
///   到达之后才返回，而不是「没等到数据就退出」。
#[test]
fn read_waits_for_async_data_arriving_later() {
    const PAYLOAD_LEN: usize = 32;

    let (mut tx, mut rx) = make_ring(64);
    let payload: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i * 7 + 1) as u8).collect();

    // 异步生产者：延迟 150ms 后才把数据送进管道，随后关闭写端（EOF）。
    let payload_producer = payload.clone();
    let producer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(150));
        write_all_into_tx(&mut tx, &payload_producer);
        drop(tx); // 关闭写端：数据流 EOF
    });

    with_tokio_rt(|_rt| {
        let mut reader = AsStdRead::uncancellable(&mut rx);
        let mut buf = [0u8; PAYLOAD_LEN];
        let t0 = Instant::now();
        let n = reader.read(&mut buf).expect("read 不应返回错误");
        let elapsed = t0.elapsed();
        assert_eq!(n, PAYLOAD_LEN, "必须读满 {PAYLOAD_LEN} 字节；提前退出会得到 0");
        assert_eq!(&buf[..], &payload[..], "读出的内容必须与载荷一致");
        assert!(
            elapsed >= Duration::from_millis(130),
            "read 必须等待异步数据到达：耗时 {elapsed:?} 远小于 150ms 延迟，说明提前退出了"
        );
    });

    producer.join().expect("生产者线程不应 panic");
}

/// 目的：证明 `AsStdRead::read` 在「数据流暂时为空但尚未关闭」的窗口期不会
/// 提前退出。一次 `read` 消费完第一批数据后，管道变空（但写端还开着）；此时
/// 适配器的读循环必须继续阻塞等待第二批异步数据，而不是把「暂时为空」误判成
/// EOF、带着半截数据返回。
///
/// 测试方法：
/// 1. 后台生产者：睡 100ms → 写入第一批 8 字节 → **等待读者消费完第一批**
///    （轮询 `data_size()` 归零，此时读者必然已进入第二批的等待）→ 再睡 20ms
///    确保读者已 park 进 `read_async` → 写入第二批 8 字节 → 关闭写端；
/// 2. 主线程在 tokio 运行时上下文里**只调用一次** `read`（缓冲区 32 字节）；
///    一次 `read` 内部应经历：等待第一批 → 读出 → 管道暂时为空 →（继续等待）
///    → 第二批到达 → 读出 → 写端关闭 → 返回；
///
/// 通过依据（同时满足才算通过）：
/// - `read` 返回的字节数必须等于 16（两批之和）：这是最关键的判别——若适配器
///   在「暂时为空」处提前退出，只会返回第一批的 8 字节；
/// - 读出的 16 字节必须等于第一批 ++ 第二批（顺序、内容正确，第二批没有被
///   丢弃）。
#[test]
fn read_waits_across_transient_empty_phase() {
    let (mut tx, mut rx) = make_ring(64);
    let batch1: Vec<u8> = (0..8).map(|i| (i as u8) + 1).collect(); // [1..=8]
    let batch2: Vec<u8> = (0..8).map(|i| (i as u8) + 100).collect(); // [100..=107]
    let mut expect = batch1.clone();
    expect.extend_from_slice(&batch2);

    let producer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(100));
        write_all_into_tx(&mut tx, &batch1);
        // 等读者消费完第一批（ring 数据量归零 = 读者已读完第一批，
        // 即将进入第二批的 `read_async` 等待）。等待有上限：若读者异常
        // （例如适配器提前退出/panic），生产者不能无限自旋挂死测试。
        let deadline = Instant::now() + Duration::from_secs(2);
        while tx.data_size() > 0 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(1));
        }
        thread::sleep(Duration::from_millis(20)); // 确保读者已 park 进等待
        write_all_into_tx(&mut tx, &batch2);
        drop(tx); // EOF
    });

    with_tokio_rt(|_rt| {
        let mut reader = AsStdRead::uncancellable(&mut rx);
        let mut buf = [0u8; 32];
        let n = reader.read(&mut buf).expect("read 不应返回错误");
        assert_eq!(
            n,
            16,
            "必须读到两批共 16 字节；若在流暂时为空时提前退出，只会返回 8"
        );
        assert_eq!(&buf[..16], &expect[..], "两批数据必须按序完整到达");
    });

    producer.join().expect("生产者线程不应 panic");
}

/// 目的：在最贴近真实「异步数据流」的场景下验证读适配：数据由**同一个 tokio
/// 运行时里的异步任务**（用 ring 的异步写接口 `write_async` + `tokio::time::sleep`
/// 延迟）产生，主线程用同步的 `AsStdRead::read` 消费。这同时验证适配器的
/// `block_on`（`block_in_place` + `Handle::block_on`）在阻塞当前线程等待期间
/// **不会卡死运行时的调度**——否则异步生产者任务永远得不到调度，测试会死锁。
///
/// 测试方法：
/// 1. 建立环形管道，把写半区移入一个 tokio 异步任务；
/// 2. 该任务：`sleep(50ms)` → 用 `write_async` 写 8 字节 → 再 `sleep(50ms)`
///    → 写 8 字节 → 关闭写端 → 通过 mpsc 通知测试主体；
/// 3. 主线程（同一运行时）调用一次 `read`，读出 16 字节；随后等待生产者任务
///    的完成通知（`recv_timeout(5s)`，若任务未完成即失败）；
///
/// 通过依据（同时满足才算通过）：
/// - `read` 返回 16 且内容 == 两批拼接（若中途提前退出则只有 8 字节）；
/// - 生产者任务的完成通知在 5 秒内收到：证明等待期间运行时仍在调度异步任务
///   （若 `block_in_place` 让渡失效，任务不被调度 → 数据永远不来 → 死锁/超时
///   → 失败）；
/// - 读操作耗时 ≥ 80ms（两批数据总延迟 100ms - 20ms 余量）。
#[test]
fn read_from_real_tokio_task_stream() {
    let (mut tx, mut rx) = make_ring(64);
    let batch1: Vec<u8> = (0..8).map(|i| (i as u8) + 1).collect();
    let batch2: Vec<u8> = (0..8).map(|i| (i as u8) + 100).collect();
    let mut expect = batch1.clone();
    expect.extend_from_slice(&batch2);

    let (done_tx, done_rx) = mpsc::channel::<()>();

    with_tokio_rt(|rt| {
        // 真正的异步生产者：同一运行时里的任务，用 ring 的异步写接口。
        let _handle = rt.spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            write_batch_async(&mut tx, &batch1).await;
            tokio::time::sleep(Duration::from_millis(50)).await;
            write_batch_async(&mut tx, &batch2).await;
            drop(tx); // EOF
            let _ = done_tx.send(());
        });

        let mut reader = AsStdRead::uncancellable(&mut rx);
        let mut buf = [0u8; 32];
        let t0 = Instant::now();
        let n = reader.read(&mut buf).expect("read 不应返回错误");
        let elapsed = t0.elapsed();
        assert_eq!(
            n,
            16,
            "必须读到两批共 16 字节；若在流暂时为空时提前退出，只会返回 8"
        );
        assert_eq!(&buf[..16], &expect[..], "两批数据必须按序完整到达");
        assert!(
            elapsed >= Duration::from_millis(80),
            "read 必须跨过两批数据之间的等待期：耗时 {elapsed:?}"
        );
    });

    done_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("生产者异步任务必须完成（若未完成：运行时调度被 block_on 卡死，或任务 panic）");
}

/// 把 `data` 通过 ring 的**异步**写接口 `write_at_most_async` 写入写半区。
///
/// 供 tokio 异步任务使用；`write_async` future 在空间不足时会 Pending 等待，
/// 借出的段 drop 时提交（写位置前进、唤醒读端）。
async fn write_batch_async(tx: &mut Tx, data: &[u8]) {
    let mut off = 0usize;
    while off < data.len() {
        let res = tx.write_at_most_async(data.len() - off).await;
        let mut segm = res.pick_left().expect("写端应有可写空间");
        let n = segm.least_count();
        assert!(n > 0, "借用得到的写段不可能为空");
        let mut staging: Vec<MaybeUninit<u8>> = data[off..off + n]
            .iter()
            .map(|&b| MaybeUninit::new(b))
            .collect();
        // SAFETY: `u8` 无 drop，位拷贝安全；段 drop 时按 `n` 提交给 ring。
        let moved = unsafe { segm.move_items_from_buff(&mut staging) };
        assert_eq!(moved, n);
        drop(segm);
        off += n;
    }
}

// ===========================================================================
// 写方向：std::io::Write → 异步数据流
// ===========================================================================

/// 目的：证明 `AsStdWrite` 写进异步数据流的数据完整、按序、无丢失——即使接收端
/// （消费者）**要过一段时间才异步地腾出空间**，重复调用 `write` 最终也必须把
/// 整个载荷送达，不能因为「管道满」就把数据悄悄丢掉或写坏。
///
/// 说明：适配器的单次 `write` 遵循 std 惯例——写满当前可用空间就返回（管道满
/// 时返回 `Ok(0)`），真正的等待由调用方（本测试的循环）驱动；这正是一般 std
/// 同步代码对接异步流的用法。
///
/// 测试方法：
/// 1. 建立容量 16 的管道（单次最多可写 15 字节），载荷 100 字节（远超容量，
///    写端必然反复撞上「满」）；
/// 2. 消费者线程：先 `sleep(100ms)`（保证写端已经填满管道并开始等待），然后
///    持续读出，直到收满整个载荷；
/// 3. 主线程在 tokio 运行时上下文里循环调用 `write`：`Ok(0)`（管道满）就
///    稍等重试，直到全部写出；记录耗时；
///
/// 通过依据（同时满足才算通过）：
/// - 累计写出的字节数必须等于 100（若适配器在管道满时把数据弄丢或返回错误，
///   循环无法完成）；
/// - 消费者收到的字节必须与载荷逐字节相同（内容、顺序正确，无重复无丢失）；
/// - 耗时必须 ≥ 90ms（100ms - 10ms 余量）：证明写端真的等到了异步消费者腾出
///   空间，而不是「假装写成功」。
#[test]
fn write_delivers_all_to_delayed_async_consumer() {
    const CAP: usize = 16;
    const PAYLOAD_LEN: usize = 100;

    let (mut tx, mut rx) = make_ring(CAP);
    let payload: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i * 13 + 7) as u8).collect();

    // 异步消费者：延迟 100ms 才开始腾空间（读走数据），直到收满整个载荷。
    // 等待有上限：若写端异常（数据丢失/提前退出），消费者不能无限自旋。
    let consumer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(100));
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut got = Vec::new();
        while got.len() < PAYLOAD_LEN && Instant::now() < deadline {
            let chunk = drain_available(&mut rx);
            got.extend_from_slice(&chunk);
            if got.len() < PAYLOAD_LEN && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(1));
            }
        }
        got
    });

    with_tokio_rt(|_rt| {
        let mut writer = AsStdWrite::uncancellable(&mut tx);
        let t0 = Instant::now();
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut off = 0usize;
        while off < payload.len() && Instant::now() < deadline {
            match writer.write(&payload[off..]) {
                Ok(0) => {
                    // 管道满：等待异步消费者腾出空间后重试。
                    thread::sleep(Duration::from_millis(1));
                }
                Ok(n) => {
                    assert!(n > 0, "write 返回的已写字节数不可能为负向增长");
                    off += n;
                }
                Err(e) => panic!("write 不应返回错误：{e}"),
            }
        }
        let elapsed = t0.elapsed();
        assert_eq!(
            off, PAYLOAD_LEN,
            "必须写出全部 {PAYLOAD_LEN} 字节"
        );
        assert!(
            elapsed >= Duration::from_millis(90),
            "写端必须等待异步消费者腾出空间（消费者延迟 100ms）：耗时 {elapsed:?}"
        );
    });

    let got = consumer.join().expect("消费者线程不应 panic");
    assert_eq!(got.len(), PAYLOAD_LEN, "消费者必须收到全部载荷");
    assert_eq!(&got[..], &payload[..], "消费者收到的内容必须与载荷逐字节一致");
}

/// 一个"保守"的写端包装：`is_blocked_closing` 恒返回 `false`。
///
/// 为什么需要它：真实 ring 的 `is_blocked_closing` 在管道满时会返回 `true`，
/// 导致适配器在调用 `write_async` **之前**就提前返回（这是适配器的设计：
/// 单次 `write` 写满当前可用空间就返回）。这样一来，普通 ring 永远不会触发
/// 适配器内部 `write_async` + `block_on` 的**等待**路径。本包装故意让
/// 「是否阻塞/关闭」恒为 `false`——于是适配器在管道满时仍会调用
/// `write_async`，后者（ring 实现）会保持 Pending 直到读端腾出空间，
/// `block_on` 必须真正阻塞等待。仅用于测试，不代表真实 sink 的行为。
struct ConservativeTx<W>(W);

impl<W: TrBuffWrite<u8>> TrBuffWrite<u8> for ConservativeTx<W> {
    type WriteAsync<'f> = W::WriteAsync<'f> where Self: 'f;
    type SegmMut<'f> = W::SegmMut<'f> where Self: 'f;
    type Err = W::Err;

    fn is_stuffed_closing(&self) -> bool {
        false // 故意保守：永不提前退出，强制走 write_async 的等待路径
    }

    fn write_async<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> Self::WriteAsync<'f> {
        self.0.write_async(demand)
    }
}

impl<W: TrBuffTryWrite<u8>> TrBuffTryWrite<u8> for ConservativeTx<W> {
    fn try_write<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<Self::SegmMut<'f>, Self::Err> {
        self.0.try_write(demand)
    }
}

/// 目的：验证写方向适配器内部的 `write_async` + `block_on` **确实会阻塞等待**
/// 异步空间——当接收端要过一段时间才异步地腾出空间时，**一次** `write` 调用
/// 必须完整写出整个载荷，而不是带着部分字节提前返回（"没等到就退出"）。
///
/// 说明：普通 ring 的 `is_blocked_closing` 在满时会提前让适配器返回，不会触发
/// `write_async` 的等待；本测试用 [`ConservativeTx`] 包装（`is_blocked_closing`
/// 恒 `false`）强制走等待路径，单独验证适配器的阻塞等待能力。
///
/// 测试方法：
/// 1. 建立容量 16 的管道（单次最多可写 15 字节），载荷 100 字节（远超容量）；
/// 2. 写端用 [`ConservativeTx`] 包装后再包上 `AsStdWrite`；
/// 3. 消费者线程：先 `sleep(100ms)`（此时写端第一次 `write` 必已撞上"满"、
///    进入 `write_async` 的 Pending 等待），然后持续读出全部 100 字节；
/// 4. 主线程在 tokio 运行时上下文里**只调用一次** `write`，记录耗时；
///
/// 通过依据（同时满足才算通过）：
/// - 这次 `write` 返回的字节数必须等于 100（若适配器没有等待、在满时提前
///   退出，返回的只是当前可用空间 15 字节或更少）；
/// - 消费者收到的 100 字节必须与载荷逐字节一致（内容、顺序正确）；
/// - 耗时必须 ≥ 90ms（100ms - 10ms 余量）：证明 `block_on` 真正阻塞到了异步
///   消费者腾出空间，而不是"没等到就退出"。
#[test]
fn write_blocks_until_async_consumer_frees_space() {
    const CAP: usize = 16;
    const PAYLOAD_LEN: usize = 100;

    let (tx, mut rx) = make_ring(CAP);
    let payload: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i * 13 + 7) as u8).collect();
    let payload_consumer = payload.clone();

    // 异步消费者：延迟 100ms 才开始腾空间，直到收满整个载荷。
    let consumer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(100));
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut got = Vec::new();
        while got.len() < payload_consumer.len() && Instant::now() < deadline {
            let chunk = drain_available(&mut rx);
            got.extend_from_slice(&chunk);
            if got.len() < payload_consumer.len() && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(1));
            }
        }
        got
    });

    with_tokio_rt(|_rt| {
        let mut sink = ConservativeTx(tx);
        let mut writer = AsStdWrite::uncancellable(&mut sink);
        let t0 = Instant::now();
        let n = writer.write(&payload).expect("write 不应返回错误");
        let elapsed = t0.elapsed();
        assert_eq!(
            n, PAYLOAD_LEN,
            "一次 write 必须等满整个载荷；若提前退出只会写入当前可用空间"
        );
        assert!(
            elapsed >= Duration::from_millis(90),
            "write 必须等待异步消费者腾出空间（消费者延迟 100ms）：耗时 {elapsed:?}"
        );
    });

    let got = consumer.join().expect("消费者线程不应 panic");
    assert_eq!(got.len(), PAYLOAD_LEN, "消费者必须收到全部载荷");
    assert_eq!(&got[..], &payload[..], "消费者收到的内容必须与载荷逐字节一致");
}

// ===========================================================================
// 端到端往返：AsStdWrite 写出 → 异步管道 → AsStdRead 读入
// ===========================================================================

/// 目的：端到端验证 `AsStdWrite` + `AsStdRead` 这对适配器能把一段**超过管道
/// 容量**的载荷，穿过「同步代码 → 异步数据流 → 同步代码」的双向边界完整送达：
/// 写端必须反复等待读端腾出空间，读端必须反复等待写端补充数据，最终载荷逐
/// 字节一致、无丢失、无重复、无乱序。
///
/// 测试方法：
/// 1. 建立容量 64 的管道（单次最多承载 63 字节），载荷 1000 字节（远超容量，
///    保证两侧都必须多次「等待对端」）；
/// 2. 生产者线程（自带 tokio 运行时上下文）：`AsStdWrite` + 重试循环把载荷
///    全部写入，返回累计写出的字节数；
/// 3. 消费者线程（自带 tokio 运行时上下文）：`AsStdRead` 循环读到错误（写端
///    关闭后的 EOF）为止，收集全部字节；
/// 4. 比较两侧数据。
///
/// 通过依据（同时满足才算通过）：
/// - 生产者累计写出的字节数 == 1000（写方向无丢失）；
/// - 消费者读到的字节数 == 1000，且与载荷逐字节一致（读方向完整、按序）；
/// - 两个线程都正常 join（无死锁：写端不会永远等不到空间，读端不会永远等不到
///   数据）。
#[test]
fn roundtrip_write_read_across_async_boundary() {
    const CAP: usize = 64;
    const PAYLOAD_LEN: usize = 1000;

    let (mut tx, mut rx) = make_ring(CAP);
    let payload: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i % 251) as u8).collect();

    // 生产者线程：AsStdWrite 把载荷全部写入（满则稍等重试）。
    // 轮询有 2s 上限：若写端异常（丢失数据/提前退出），不会无限自旋，
    // 而是以不完整的 `written` 结束，由测试末尾的断言判定失败。
    let payload_producer = payload.clone();
    let producer = thread::spawn(move || {
        with_tokio_rt(|_rt| {
            let mut writer = AsStdWrite::uncancellable(&mut tx);
            let deadline = Instant::now() + Duration::from_secs(2);
            let mut off = 0usize;
            while off < payload_producer.len() && Instant::now() < deadline {
                match writer.write(&payload_producer[off..]) {
                    Ok(0) => thread::sleep(Duration::from_millis(1)),
                    Ok(n) => {
                        assert!(n > 0);
                        off += n;
                    }
                    Err(e) => panic!("write 不应返回错误：{e}"),
                }
            }
            off
        })
    });

    // 消费者线程：AsStdRead 循环读到 EOF（写端关闭后 read 返回错误）为止。
    let consumer = thread::spawn(move || {
        with_tokio_rt(|_rt| {
            let mut reader = AsStdRead::uncancellable(&mut rx);
            let mut got = Vec::new();
            let mut buf = [0u8; CAP];
            loop {
                match reader.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => got.extend_from_slice(&buf[..n]),
                    Err(_) => break, // 写端已关闭且数据读空：EOF
                }
            }
            got
        })
    });

    let written = producer.join().expect("生产者线程不应 panic");
    let got = consumer.join().expect("消费者线程不应 panic");

    assert_eq!(written, PAYLOAD_LEN, "生产者必须写出全部载荷");
    assert_eq!(got.len(), PAYLOAD_LEN, "消费者必须收到全部载荷");
    assert_eq!(&got[..], &payload[..], "往返数据必须逐字节一致");
}

// ===========================================================================
// circular buff 载体：双端被动 + AsStdRead / AsStdWrite（全同步代码）
// ===========================================================================

/// 目的：证明 circular buff 的两端分别包上 `AsStdWrite`（生产端）与
/// `AsStdRead`（消费端）后，用**全同步**的代码就能让数据从生产端流进
/// circular buff、再从消费端读出——往返内容、顺序、数量完全一致。
///
/// 测试方法：
/// 1. 建立容量 64 的双端被动 circular buff，写端包 `AsStdWrite`、读端包
///    `AsStdRead`（两个适配器都只调用同步的 `write` / `read`）；
/// 2. 先 `write` 写入 32 字节载荷，再 `close()` 关闭写端（EOF）；
/// 3. 用一次 `read`（64 字节缓冲）读出；
///
/// 通过依据（同时满足才算通过）：
/// - 一次 `write` 返回 32（载荷小于容量，必须全部写入）；
/// - `read` 返回 32 且内容与载荷逐字节一致——数据确实经环形缓冲从生产端
///   流到了消费端；写端关闭（EOF）后读循环能正确结束，不把 EOF 误当错误。
#[test]
fn cb_sync_write_then_read_roundtrip() {
    const CAP: usize = 64;
    const PAYLOAD_LEN: usize = 32;

    let (mut tx, mut rx) = make_cb_pair(CAP);
    let payload: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i * 7 + 1) as u8).collect();

    with_tokio_rt(|_rt| {
        // 双端包装：生产端 → AsStdWrite，消费端 → AsStdRead。
        let mut writer = AsStdWrite::uncancellable(&mut tx);
        let mut reader = AsStdRead::uncancellable(&mut rx);

        // 全同步写入：数据流进 circular buff（段 drop 即提交、唤醒读端）。
        let n = writer.write(&payload).expect("write 不应失败");
        assert_eq!(n, PAYLOAD_LEN, "载荷小于容量，一次 write 应全部写入");
        tx.close(); // 写端关闭（EOF）：让读循环能确定结束

        // 全同步读出：数据从 circular buff 流到调用者缓冲。
        let mut buf = [0u8; 64];
        let rn = reader.read(&mut buf).expect("read 不应失败");
        assert_eq!(rn, PAYLOAD_LEN, "应读出全部写入的数据");
        assert_eq!(&buf[..rn], &payload[..], "读出内容必须与写入一致");
    });
}

/// 目的：证明 circular buff 的读端包上 `AsStdRead` 后，面对「数据要过一段
/// 时间才由另一个执行体（后台线程）写入」的场景，一次 `read` 会通过适配器
/// 内部的 `block_on` **阻塞等待**直到数据真正到达（并随写端关闭结束），
/// 而不是因为第一轮 poll 没有数据就立即返回 0（提前退出）。
///
/// 测试方法：
/// 1. 建立容量 64 的 circular buff，读端包上 `AsStdRead`；
/// 2. 后台生产者线程：先 `sleep(150ms)`（保证读端的 `read` 已进入
///    `read_async` 等待、第一轮 poll 必然 Pending），再用同步 API 写入 32 字节
///    载荷并 `close()` 关闭写端（EOF）；
/// 3. 主线程在 tokio 运行时上下文里调用**一次** `read`，记录耗时；
///
/// 通过依据（同时满足才算通过）：
/// - `read` 返回 32（提前退出会得到 0）；
/// - 内容与载荷逐字节相同；
/// - 耗时 ≥ 130ms（150ms - 20ms 余量）：直接证明调用阻塞到了异步数据到达
///   之后才返回，且跨线程唤醒（生产者线程提交 → 消费端 waker 被 signal）生效。
#[test]
fn cb_read_waits_for_async_data_arriving_later() {
    const PAYLOAD_LEN: usize = 32;

    let (mut tx, mut rx) = make_cb_pair(64);
    let payload: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i * 7 + 1) as u8).collect();

    // 异步生产者线程：延迟 150ms 后写入载荷并关闭写端（EOF）。
    let payload_producer = payload.clone();
    let producer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(150));
        cb_write_all(&mut tx, &payload_producer);
        tx.close(); // 写端关闭 → 数据流 EOF
    });

    with_tokio_rt(|_rt| {
        let mut reader = AsStdRead::uncancellable(&mut rx);
        let mut buf = [0u8; PAYLOAD_LEN];
        let t0 = Instant::now();
        let n = reader.read(&mut buf).expect("read 不应返回错误");
        let elapsed = t0.elapsed();
        assert_eq!(n, PAYLOAD_LEN, "必须读满 {PAYLOAD_LEN} 字节；提前退出会得到 0");
        assert_eq!(&buf[..], &payload[..], "读出的内容必须与载荷一致");
        assert!(
            elapsed >= Duration::from_millis(130),
            "read 必须等待异步数据到达：耗时 {elapsed:?} 远小于 150ms 延迟，说明提前退出了"
        );
    });

    producer.join().expect("生产者线程不应 panic");
}

/// 目的：证明 circular buff 读端在「数据流暂时为空但尚未关闭」的窗口期不会
/// 提前退出。一次 `read` 消费完第一批后缓冲变空（写端还开着），适配器的读
/// 循环必须继续阻塞等待第二批，而不是把「暂时为空」误判成 EOF 提前返回。
///
/// 测试方法：
/// 1. 后台生产者：睡 100ms → 写第一批 8 字节 → 等读者消费完（轮询
///    `tx.data_size()` 归零，此时读者必已进入第二批等待）→ 再睡 20ms 确保
///    读者已 park → 写第二批 8 字节 → 关闭写端；
/// 2. 主线程在 tokio 运行时上下文里**只调用一次** `read`（缓冲 32 字节）；
///
/// 通过依据：
/// - `read` 返回 16（两批之和）——若在「暂时为空」处提前退出，只会返回 8；
/// - 内容 == 第一批 ++ 第二批（按序、无丢失）。
#[test]
fn cb_read_waits_across_transient_empty_phase() {
    let (mut tx, mut rx) = make_cb_pair(64);
    let batch1: Vec<u8> = (0..8).map(|i| (i as u8) + 1).collect(); // [1..=8]
    let batch2: Vec<u8> = (0..8).map(|i| (i as u8) + 100).collect(); // [100..=107]
    let mut expect = batch1.clone();
    expect.extend_from_slice(&batch2);

    let producer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(100));
        cb_write_all(&mut tx, &batch1);
        // 等读者消费完第一批（数据量归零 = 读者已读完，即将进入第二批等待）。
        let deadline = Instant::now() + Duration::from_secs(2);
        while tx.data_size() > 0 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(1));
        }
        thread::sleep(Duration::from_millis(20)); // 确保读者已 park 进等待
        cb_write_all(&mut tx, &batch2);
        tx.close(); // EOF
    });

    with_tokio_rt(|_rt| {
        let mut reader = AsStdRead::uncancellable(&mut rx);
        let mut buf = [0u8; 32];
        let n = reader.read(&mut buf).expect("read 不应返回错误");
        assert_eq!(
            n,
            16,
            "必须读到两批共 16 字节；若在流暂时为空时提前退出，只会返回 8"
        );
        assert_eq!(&buf[..16], &expect[..], "两批数据必须按序完整到达");
    });

    producer.join().expect("生产者线程不应 panic");
}

/// 目的：证明 circular buff 写端包上 `AsStdWrite` 后，载荷远超容量时数据
/// **完整、按序、无丢失**地送达延迟腾出空间的异步消费者——写端在管道满时
/// 返回 `Ok(0)`（std 惯例：写满当前可用空间即返回），由调用方循环重试，
/// 直到消费者腾出空间后全部写出。
///
/// 测试方法：
/// 1. 建立容量 16 的 circular buff，载荷 100 字节（远超容量，写端必然反复
///    撞上「满」）；
/// 2. 消费者线程：先 `sleep(100ms)`（保证写端已经填满管道并开始等待），
///    然后持续读出，直到收满整个载荷；
/// 3. 主线程在 tokio 运行时上下文里循环 `write`：`Ok(0)`（管道满）就稍等
///    重试，直到全部写出；记录耗时；
///
/// 通过依据：
/// - 累计写出的字节数必须等于 100（若管道满时数据被弄丢 / 返回错误，循环
///   无法完成）；
/// - 消费者收到的字节与载荷逐字节相同（内容、顺序、无重复无丢失）；
/// - 耗时 ≥ 90ms：证明写端真的等到了异步消费者腾出空间。
#[test]
fn cb_write_delivers_all_to_delayed_async_consumer() {
    const CAP: usize = 16;
    const PAYLOAD_LEN: usize = 100;

    let (mut tx, mut rx) = make_cb_pair(CAP);
    let payload: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i * 13 + 7) as u8).collect();

    // 异步消费者：延迟 100ms 才开始腾空间，直到收满整个载荷。
    let consumer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(100));
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut got = Vec::new();
        while got.len() < PAYLOAD_LEN && Instant::now() < deadline {
            let chunk = cb_drain_available(&mut rx);
            got.extend_from_slice(&chunk);
            if got.len() < PAYLOAD_LEN && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(1));
            }
        }
        got
    });

    with_tokio_rt(|_rt| {
        let mut writer = AsStdWrite::uncancellable(&mut tx);
        let t0 = Instant::now();
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut off = 0usize;
        while off < payload.len() && Instant::now() < deadline {
            match writer.write(&payload[off..]) {
                Ok(0) => {
                    // 管道满：等待异步消费者腾出空间后重试。
                    thread::sleep(Duration::from_millis(1));
                }
                Ok(n) => {
                    assert!(n > 0, "write 返回的已写字节数不可能为负向增长");
                    off += n;
                }
                Err(e) => panic!("write 不应返回错误：{e}"),
            }
        }
        let elapsed = t0.elapsed();
        assert_eq!(off, PAYLOAD_LEN, "必须写出全部 {PAYLOAD_LEN} 字节");
        assert!(
            elapsed >= Duration::from_millis(90),
            "写端必须等待异步消费者腾出空间（消费者延迟 100ms）：耗时 {elapsed:?}"
        );
    });

    let got = consumer.join().expect("消费者线程不应 panic");
    assert_eq!(got.len(), PAYLOAD_LEN, "消费者必须收到全部载荷");
    assert_eq!(&got[..], &payload[..], "消费者收到的内容必须与载荷逐字节一致");
}

/// 目的：证明 circular buff 写端适配器内部的 `write_async` + `block_on`
/// **确实会阻塞等待**异步空间——用 [`ConservativeTx`] 包装（`is_stuffed_closing`
/// 恒 `false`）强制走等待路径：当环形缓冲写满、读端要过一段时间才腾出空间
/// 时，**一次** `write` 调用必须完整写出整个载荷，而不是带着部分字节提前返回。
///
/// 说明：普通 circular buff 写端的 `is_stuffed_closing` 在满时会返回 `true`，
/// 让适配器在调用 `write_async` 之前就提前返回（std 惯例）；本测试用
/// [`ConservativeTx`] 强制走 `write_async` 的 Pending 等待路径，单独验证
/// 适配器（以及 circular buff 的写等待 park + 读端提交唤醒）的阻塞等待能力。
///
/// 测试方法：
/// 1. 建立容量 16 的 circular buff，载荷 100 字节（远超容量）；
/// 2. 写端 `ConservativeTx(tx)` 包上 `AsStdWrite`；
/// 3. 消费者线程：先 `sleep(100ms)`（此时写端第一次 `write` 必已撞上「满」、
///    进入 `write_async` 的 Pending 等待），然后持续读出全部 100 字节；
/// 4. 主线程在 tokio 运行时上下文里**只调用一次** `write`，记录耗时；
///
/// 通过依据：
/// - 这次 `write` 返回 100（若适配器没等待、在满时提前退出，返回的只是当前
///   可用空间 ≤15 字节）；
/// - 消费者收到的 100 字节与载荷逐字节一致；
/// - 耗时 ≥ 90ms：证明 `block_on` 真正阻塞到了异步消费者腾出空间。
#[test]
fn cb_write_blocks_until_async_consumer_frees_space() {
    const CAP: usize = 16;
    const PAYLOAD_LEN: usize = 100;

    let (tx, mut rx) = make_cb_pair(CAP);
    let payload: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i * 13 + 7) as u8).collect();
    let payload_consumer = payload.clone();

    // 异步消费者：延迟 100ms 才开始腾空间，直到收满整个载荷。
    let consumer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(100));
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut got = Vec::new();
        while got.len() < payload_consumer.len() && Instant::now() < deadline {
            let chunk = cb_drain_available(&mut rx);
            got.extend_from_slice(&chunk);
            if got.len() < payload_consumer.len() && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(1));
            }
        }
        got
    });

    with_tokio_rt(|_rt| {
        let mut sink = ConservativeTx(tx);
        let mut writer = AsStdWrite::uncancellable(&mut sink);
        let t0 = Instant::now();
        let n = writer
            .write(&payload)
            .expect("write 不应返回错误");
        let elapsed = t0.elapsed();
        assert_eq!(
            n, PAYLOAD_LEN,
            "一次 write 必须完整写出全部 {PAYLOAD_LEN} 字节（适配器应等待异步空间，而非提前退出）"
        );
        assert!(
            elapsed >= Duration::from_millis(90),
            "write 必须阻塞等待异步消费者腾出空间（消费者延迟 100ms）：耗时 {elapsed:?}"
        );
    });

    let got = consumer.join().expect("消费者线程不应 panic");
    assert_eq!(got.len(), PAYLOAD_LEN, "消费者必须收到全部载荷");
    assert_eq!(&got[..], &payload[..], "消费者收到的内容必须与载荷逐字节一致");
}
