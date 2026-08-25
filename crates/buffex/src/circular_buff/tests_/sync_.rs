//! 被动 × 被动模式的测试：读写往返、`Demand` 语义（不足下限不返回）、
//! 跨末端环绕的两段式段、以及异步等待（hook 唤醒）。

use std::pin::pin;

use std::{vec, vec::Vec};

use abs_buff::{Demand, TrBuffRead, TrBuffTryRead, TrBuffTryWrite, TrBuffWrite};

use super::{
    super::{CircularBuffBuilder, RxError},
    poll_once, storage, take_segm, fill_segm, TestWaker,
};

/// 构建一个容量 `N` 的被动 × 被动 `CircularBuff`（测试辅助）。
fn make_buff<'a, const N: usize>(
    st: &'a mut [core::mem::MaybeUninit<u8>; N],
) -> super::super::CircularBuff<
    'a,
    super::super::BuffProducer<u8>,
    super::super::BuffConsumer<u8>,
    &'a mut [core::mem::MaybeUninit<u8>],
    u8,
> {
    CircularBuffBuilder::with_capacity(N)
        .producer_passive()
        .consumer_passive()
        .build(st)
        .unwrap()
}

/// 写入 / 读出往返：写 3 字节，读回同样的 3 字节，位置正确推进。
#[test]
fn write_read_roundtrip() {
    let mut st = storage::<8>();
    let mut buff = make_buff(&mut st);
    let (mut tx, mut rx) = buff.try_split_io().expect("被动 × 被动可拆分");

    // 写 3 字节。
    let some = TrBuffTryWrite::try_write(&mut tx, &Demand::at_least(3));
    let mut ws = some.pick_left().expect("应有 3 格可写空间");
    fill_segm(&mut ws, &[1, 2, 3]);
    drop(ws);
    assert_eq!(rx.data_size(), 3, "写后应有 3 字节可读");

    // 读回 3 字节。
    let some = TrBuffTryRead::try_read(&mut rx, &Demand::at_least(3));
    let mut rs = some.pick_left().expect("应有 3 字节可读");
    let n = rs.least_count();
    let got = take_segm(&mut rs, n);
    drop(rs);
    assert_eq!(got, vec![1, 2, 3]);
    assert_eq!(rx.data_size(), 0, "读后应为空");
}

/// `try_read(&Demand::at_least(4))`：环里只有 2 字节时，**不得**返回 2 字节的
/// 段，而应返回 `Drained` 错误（数量不足下限）。
#[test]
fn try_read_honours_at_least() {
    let mut st = storage::<8>();
    let mut buff = make_buff(&mut st);
    let (mut tx, mut rx) = buff.try_split_io().unwrap();

    let mut ws = TrBuffTryWrite::try_write(&mut tx, &Demand::at_least(2))
        .pick_left()
        .unwrap();
    fill_segm(&mut ws, &[1, 2]);
    drop(ws);

    let some = TrBuffTryRead::try_read(&mut rx, &Demand::at_least(4));
    assert!(
        matches!(some.pick_right(), Some(RxError::Drained(_))),
        "数据不足下限时必须返回 Drained，而不是不足量的段"
    );
}

/// `try_write(&Demand::at_least(4))`：可写空间只有 3 格时，**不得**返回 3 格的
/// 段，而应返回 `Stuffed` 错误。
#[test]
fn try_write_honours_at_least() {
    let mut st = storage::<8>();
    let mut buff = make_buff(&mut st);
    let (mut tx, mut _rx) = buff.try_split_io().unwrap();

    // 写 5 字节（一次借出整个可写区，只提交 5）：容量 8 → 单空槽 → free = 2。
    let mut ws = TrBuffTryWrite::try_write(&mut tx, &Demand::at_least(1))
        .pick_left()
        .expect("应可写");
    assert!(ws.least_count() >= 5, "可写区应至少 5 格");
    fill_segm(&mut ws, &vec![0; 5]);
    drop(ws);
    assert_eq!(tx.free_size(), 2, "写 5 后应剩 2 格可写空间");

    let some = TrBuffTryWrite::try_write(&mut tx, &Demand::at_least(4));
    assert!(
        matches!(some.pick_right(), Some(super::super::TxError::Stuffed(_))),
        "可写空间不足下限时必须返回 Stuffed"
    );
}

/// 跨末端环绕：可写 / 可读区域绕到缓冲区开头时，段拆成两段物理空间、
/// 逻辑上仍是一段，数据按顺序填入 / 读出。
#[test]
fn wrap_around_two_pieces() {
    let mut st = storage::<5>();
    let mut buff = make_buff(&mut st);
    let (mut tx, mut rx) = buff.try_split_io().unwrap();

    // 写 [1,2,3]：wp = 3。
    let mut ws = TrBuffTryWrite::try_write(&mut tx, &Demand::at_least(3))
        .pick_left()
        .unwrap();
    fill_segm(&mut ws, &[1, 2, 3]);
    drop(ws);

    // 读 2：[1,2] → rp = 2。
    let mut rs = TrBuffTryRead::try_read(&mut rx, &Demand::at_least(2))
        .pick_left()
        .unwrap();
    assert_eq!(take_segm(&mut rs, 2), vec![1, 2]);
    drop(rs);

    // 再写 3：可写区 = [3,5) 两格 + [0,1) 一格（跨末端，两段式写段）。
    let some = TrBuffTryWrite::try_write(&mut tx, &Demand::at_least(3));
    let mut ws = some.pick_left().expect("跨末端也应一次借出全部 3 格");
    let slices: Vec<usize> = ws.iter_slices_mut().map(|s| s.len()).collect();
    assert_eq!(slices, vec![2, 1], "跨末端写段应为两段：[3,5) 与 [0,1)");
    fill_segm(&mut ws, &[10, 11, 12]);
    drop(ws);

    // 读全部 4：可读区 = [2,5) + [0,1)，两段式读段，顺序读出。
    let mut rs = TrBuffTryRead::try_read(&mut rx, &Demand::at_least(4))
        .pick_left()
        .expect("应有 4 字节可读");
    let n = rs.least_count();
    let got = take_segm(&mut rs, n);
    drop(rs);
    assert_eq!(got, vec![3, 10, 11, 12]);
}

/// 读侧异步等待：无数据时 `read_async` 进入 pending 并注册 waker；
/// 写端写入触发消费端 hook，唤醒读者后恢复为 Ready。
#[test]
fn read_async_wakes_on_write() {
    let mut st = storage::<8>();
    let mut buff = make_buff(&mut st);
    let (mut tx, mut rx) = buff.try_split_io().unwrap();

    // 读者先等 3 字节：当前为空 → Pending。
    let fut = rx.read_async(&Demand::at_least(3));
    let mut fut = pin!(fut.into_future());
    let (waker, _flag) = TestWaker::new();
    assert!(
        poll_once(fut.as_mut(), &waker).is_pending(),
        "无数据时读等待必须 pending"
    );

    // 写端写入 → 提交路径触发消费端 hook（被动=唤醒读者）。
    let mut ws = TrBuffTryWrite::try_write(&mut tx, &Demand::at_least(3))
        .pick_left()
        .unwrap();
    fill_segm(&mut ws, &[7, 8, 9]);
    drop(ws);

    // 重新轮询：应 Ready 并借出 3 字节的读段。
    let res = poll_once(fut.as_mut(), &waker);
    let mut rs = match res {
        std::task::Poll::Ready(r) => r.pick_left().expect("读等待应成功"),
        std::task::Poll::Pending => panic!("写入后读者应被唤醒"),
    };
    assert_eq!(rs.least_count(), 3);
    assert_eq!(take_segm(&mut rs, 3), vec![7, 8, 9]);
}

/// 写侧异步等待：环写满时 `write_async` 进入 pending 并注册 waker；
/// 读端读取释放空间，触发生产端 hook，唤醒写者后恢复为 Ready。
#[test]
fn write_async_wakes_on_read() {
    let mut st = storage::<4>(); // 容量 4 → 最多 3 字节数据
    let mut buff = make_buff(&mut st);
    let (mut tx, mut rx) = buff.try_split_io().unwrap();

    // 写满 3 字节。
    let mut ws = TrBuffTryWrite::try_write(&mut tx, &Demand::at_least(3))
        .pick_left()
        .unwrap();
    fill_segm(&mut ws, &[1, 2, 3]);
    drop(ws);
    assert_eq!(tx.free_size(), 0, "环应已写满");

    // 写者等 1 格空间：当前满 → Pending。
    let fut = tx.write_async(&Demand::at_least(1));
    let mut fut = pin!(fut.into_future());
    let (waker, _flag) = TestWaker::new();
    assert!(
        poll_once(fut.as_mut(), &waker).is_pending(),
        "环满时写等待必须 pending"
    );

    // 读端读走 1 字节 → 释放空间 → 生产端 hook 唤醒写者。
    let mut rs = TrBuffTryRead::try_read(&mut rx, &Demand::at_least(1))
        .pick_left()
        .unwrap();
    assert_eq!(take_segm(&mut rs, 1), vec![1]);
    drop(rs);

    let res = poll_once(fut.as_mut(), &waker);
    let mut ws = match res {
        std::task::Poll::Ready(r) => r.pick_left().expect("写等待应成功"),
        std::task::Poll::Pending => panic!("读取后写者应被唤醒"),
    };
    // 容量 4：读走 1 格后 free = 1（单空槽方案）。
    assert_eq!(ws.least_count(), 1, "读走 1 格后应可写 1 格");
    fill_segm(&mut ws, &[4]);
    drop(ws);
    assert_eq!(rx.data_size(), 2 + 1, "原有 2 + 新写 1");
}
