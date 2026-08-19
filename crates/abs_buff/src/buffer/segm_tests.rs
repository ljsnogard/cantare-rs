//! 针对 `TrBuffSegmRef` / `TrBuffSegmMut` 的泛型测试函数。
//!
//! 本模块只在测试配置（`cfg(test)`）或显式启用 `segm-tests` feature 时编译。
//! 任何实现了这两个 trait 的段类型（例如本 crate 的 `SegmRef` / `SegmMut`，
//! buffex 的 `ReclSliceRef` / `ReclSliceMut`）都可以调用这里的泛型函数，
//! 验证其 `move_items_*` 的 **trait 默认实现** 是否满足：
//!
//! * 搬移数量正确（不多不少、不重复不丢失）；
//! * 源段被按序消费（`least_count` 正确推进到 0）；
//! * 目标段被按序填充（消费量正确推进）；
//! * 数据内容按序到达目标（内容由调用方在自己的底层存储上校验——trait
//!   接口无法窥探具体类型的底层存储）。
//!
//! 调用约定：`src` / `dst` 的容量必须不小于 `expect.len()`，否则搬移会在
//! 中途停止，断言会失败。
//!
//! 说明：trait 默认方法的借用区域已经与段的生命周期解耦，搬移返回后两个段
//! 都可继续使用，因此这里的断言既包含前置内容校验，也包含搬移后的段状态
//! （`least_count`）校验。

use core::{fmt::Debug, mem::MaybeUninit};

use super::{TrBuffSegmMut, TrBuffSegmRef, TrBuffSegmView};

/// 断言 `segm` 的剩余内容（各物理段按顺序拼接）恰好等于 `expect`。
///
/// 内部设计：`iter_slices` 可能把逻辑上的一段拆成多个物理段（例如跨缓冲区
/// 末端的环绕段），这里按顺序拼接校验，确保视图语义与内容都正确。
fn assert_view_eq<T, S>(segm: &S, expect: &[T])
where
    S: TrBuffSegmView<Item = T>,
    T: Copy + PartialEq + Debug,
{
    assert_eq!(segm.least_count(), expect.len(), "剩余数量必须等于期望数量");
    let mut off = 0usize;
    for piece in segm.iter_slices() {
        for (i, v) in piece.iter().enumerate() {
            assert_eq!(*v, expect[off + i], "第 {} 个元素", off + i);
        }
        off += piece.len();
    }
    assert_eq!(off, expect.len(), "各物理段拼接后必须恰好覆盖期望内容");
}

/// 泛型测试：`TrBuffSegmRef::move_items_to_segm` 的 trait 默认实现。
///
/// 把 `src` 的全部剩余元素搬进 `dst`，校验搬移数量、两侧消费量与源内容；
/// `dst` 的内容由调用方在底层存储上校验。
pub fn test_move_items_to_segm<'seg, T, S, D>(
    src: &mut S,
    dst: &mut D,
    expect: &[T],
) -> usize
where
    S: TrBuffSegmRef<'seg, T>,
    D: TrBuffSegmMut<'seg, T>,
    T: Copy + PartialEq + Debug,
{
    // 先验证源段的剩余内容；
    assert_view_eq(src, expect);
    // 显式调用 trait 默认实现（不经过具体类型可能存在的固有 move 方法）；
    let moved = TrBuffSegmRef::move_items_to_segm(src, dst);
    assert_eq!(moved, expect.len(), "搬移数量必须等于期望数量");
    assert_eq!(src.least_count(), 0, "源段必须被全部消费");
    assert_eq!(
        dst.least_count(),
        0,
        "目标段必须被全部填充（其容量需恰好为 expect.len()）"
    );
    moved
}

/// 泛型测试：`TrBuffSegmMut::move_items_from_segm` 的 trait 默认实现
/// （`move_items_to_segm` 的镜像，从目标段一侧发起）。
pub fn test_move_items_from_segm<'seg, T, S, D>(
    src: &mut S,
    dst: &mut D,
    expect: &[T],
) -> usize
where
    S: TrBuffSegmRef<'seg, T>,
    D: TrBuffSegmMut<'seg, T>,
    T: Copy + PartialEq + Debug,
{
    assert_view_eq(src, expect);
    let moved = TrBuffSegmMut::move_items_from_segm(dst, src);
    assert_eq!(moved, expect.len(), "搬移数量必须等于期望数量");
    assert_eq!(src.least_count(), 0, "源段必须被全部消费");
    assert_eq!(
        dst.least_count(),
        0,
        "目标段必须被全部填充（其容量需恰好为 expect.len()）"
    );
    moved
}

/// 泛型测试：`TrBuffSegmRef::move_items_to_buff` 的 trait 默认实现。
///
/// 把 `src` 的全部剩余元素搬进 `dst` 缓冲，内容直接在缓冲上校验。
///
/// ## Safety
///
/// 与 `move_items_to_buff` 相同：被搬移的元素按位拷贝、不再由段 drop；
/// 调用方需保证 `T` 无需要 drop 的资源。
pub unsafe fn test_move_items_to_buff<'seg, T, S>(
    src: &mut S,
    dst: &mut [MaybeUninit<T>],
    expect: &[T],
) -> usize
where
    S: TrBuffSegmRef<'seg, T>,
    T: Copy + PartialEq + Debug,
{
    assert_view_eq(src, expect);
    assert!(dst.len() >= expect.len(), "目标缓冲必须足以容纳全部元素");
    // SAFETY: 测试数据为无需 drop 的简单类型（调用方保证）；
    let moved = unsafe { TrBuffSegmRef::move_items_to_buff(src, dst) };
    assert_eq!(moved, expect.len(), "搬移数量必须等于期望数量");
    assert_eq!(src.least_count(), 0, "源段必须被全部消费");
    for (i, m) in dst[..moved].iter().enumerate() {
        assert_eq!(
            unsafe { m.assume_init_read() },
            expect[i],
            "第 {} 个元素",
            i
        );
    }
    moved
}

/// 泛型测试：`TrBuffSegmMut::move_items_from_buff` 的 trait 默认实现。
///
/// 先用 `expect` 填充 `src` 缓冲，再整体搬进 `dst` 段；`dst` 的内容由调用方
/// 在底层存储上校验。
///
/// ## Safety
///
/// 与 `move_items_from_buff` 相同：`src` 中被搬走的元素按位拷贝、不再被 drop；
/// 调用方需保证 `T` 无需要 drop 的资源。
pub unsafe fn test_move_items_from_buff<'seg, T, D>(
    dst: &mut D,
    src: &mut [MaybeUninit<T>],
    expect: &[T],
) -> usize
where
    D: TrBuffSegmMut<'seg, T>,
    T: Copy + PartialEq + Debug,
{
    assert!(src.len() >= expect.len(), "源缓冲必须足以容纳全部元素");
    // 填充源缓冲；
    for (i, m) in src[..expect.len()].iter_mut().enumerate() {
        *m = MaybeUninit::new(expect[i]);
    }
    // SAFETY: 测试数据为无需 drop 的简单类型（调用方保证）；
    let moved = unsafe { TrBuffSegmMut::move_items_from_buff(dst, src) };
    assert_eq!(moved, expect.len(), "搬移数量必须等于期望数量");
    assert_eq!(
        dst.least_count(),
        0,
        "目标段必须被全部填充（其容量需恰好为 expect.len()）"
    );
    moved
}

#[cfg(test)]
mod tests_ {
    use core::{
        error::Error,
        fmt,
        future::Future,
        mem::MaybeUninit,
        pin::Pin,
        task::{Context, Poll, Waker},
    };
    use std::vec::Vec;

    use abs_cancel::{NonCancellableToken, TrCancellationToken, TrMayCancel};
    use anylr::SomeOf;

    use super::super::segm_::{SegmMut, SegmReclaim, SegmRef};
    use crate::{
        Demand,
        io::{TrInput, TrOutput},
    };

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    #[allow(dead_code)]
    enum TestErr {
        Boom,
    }

    impl fmt::Display for TestErr {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                TestErr::Boom => write!(f, "boom"),
            }
        }
    }

    impl Error for TestErr {}

    /// 一个立即就绪的 `TrMayCancel` future，用于测试 `TrInput` / `TrOutput`
    /// 的简单实现。
    struct ReadySegm<S, E>(Option<SomeOf<S, E>>);

    impl<S, E> ReadySegm<S, E> {
        fn new(value: SomeOf<S, E>) -> Self {
            ReadySegm(Option::Some(value))
        }
    }

    impl<S, E> Future for ReadySegm<S, E> {
        type Output = SomeOf<S, E>;

        fn poll(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Self::Output> {
            let this = unsafe { self.get_unchecked_mut() };
            Poll::Ready(this.0.take().expect("ready future polled once"))
        }
    }

    impl<'f, S: 'f, E: 'f> TrMayCancel<'f> for ReadySegm<S, E> {
        type MayCancelFuture<'g, C>
            = ReadySegm<S, E>
        where
            Self: 'g,
            C: TrCancellationToken + Clone,
            C: 'f,
            C: 'g,
            'g: 'f;
        type MayCancelOutput = SomeOf<S, E>;

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

    /// 轮询一个立即就绪的 future 到完成。
    fn block_on<F: Future>(fut: F) -> F::Output {
        let mut fut = core::pin::pin!(fut);
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(v) => v,
            Poll::Pending => panic!("test future must be immediately ready"),
        }
    }

    /// 一个简单的 `TrOutput<u8>`：每次把传入的源数据全部“写出”到内部 Vec。
    struct TestOutput {
        data: Vec<u8>,
    }

    impl TrOutput<u8> for TestOutput {
        type WriteAsync<'f> = ReadySegm<usize, TestErr> where Self: 'f;

        type Err = TestErr;

        fn write_async<'f>(
            &'f mut self,
            source: &'f [MaybeUninit<u8>],
        ) -> Self::WriteAsync<'f>
        {
            let n = source.len();
            for m in source {
                // SAFETY: 测试数据为 u8，无 drop 需求。
                self.data.push(unsafe { m.assume_init_read() });
            }
            ReadySegm::new(SomeOf::new_left(n))
        }
    }

    /// 一个简单的 `TrInput<u8>`：每次把内部数据读入调用方提供的目标缓冲。
    struct TestInput {
        data: Vec<u8>,
        pos: usize,
    }

    impl TrInput<u8> for TestInput {
        type ReadAsync<'f> = ReadySegm<usize, TestErr> where Self: 'f;
        type Err = TestErr;

        fn read_async<'f>(
            &'f mut self,
            target: &'f mut [MaybeUninit<u8>],
        ) -> Self::ReadAsync<'f>
        {
            let n = core::cmp::min(target.len(), self.data.len() - self.pos);
            for (i, slot) in target[..n].iter_mut().enumerate() {
                *slot = MaybeUninit::new(self.data[self.pos + i]);
            }
            self.pos += n;
            ReadySegm::new(SomeOf::new_left(n))
        }
    }

    /// 测试 `SegmRef::move_items_to_output_async`：从段中把数据移动到
    /// `TrOutput`，并正确推进段内部的 `offset_`。
    #[test]
    fn segm_ref_output_async_moves_data_and_advances_offset() {
        let mut data: Vec<u8> = (0..10).collect();
        let mut consumed = 0usize;
        let mut segm = SegmRef::new(
            data.as_mut_slice(),
            SegmReclaim::new(Pin::new(&mut consumed)),
        );
        let mut output = TestOutput { data: Vec::new() };

        {
            let res = block_on(async {
                let mut child = segm.as_segm_ref();
                child
                    .move_items_to_output_async(
                        &mut output,
                        &Demand::less_than(6),
                    )
                    .may_cancel_with(NonCancellableToken::shared_mut())
                    .await
            });

            let moved = res.pick_left().expect("output_async should succeed");
            assert_eq!(moved, 6, "应按 demand 上界移动 6 个元素");
        }

        assert_eq!(segm.least_count(), 4, "父段应反映子段消费的 6 个元素");
        assert_eq!(output.data, (0..6).collect::<Vec<_>>(), "输出内容应有序");

        drop(segm);
        assert_eq!(consumed, 6, "段 drop 时应把消费量提交给 reclaimer");
    }

    /// 测试 `SegmMut::move_items_from_input_async`：从 `TrInput` 读取数据到段中，
    /// 并正确推进段内部的 `offset_`。
    #[test]
    fn segm_mut_input_async_reads_data_and_advances_offset() {
        let mut storage = [MaybeUninit::<u8>::uninit(); 10];
        let mut consumed = 0usize;
        let mut segm = SegmMut::new(
            &mut storage[..],
            SegmReclaim::new(Pin::new(&mut consumed)),
        );
        let mut input = TestInput {
            data: (0..10).collect(),
            pos: 0,
        };

        {
            let res = block_on(async {
                let mut child = segm.as_segm_mut();
                child
                    .move_items_from_input_async(
                        &mut input,
                        &Demand::less_than(7),
                    )
                    .may_cancel_with(NonCancellableToken::shared_mut())
                    .await
            });

            let moved = res.pick_left().expect("input_async should succeed");
            assert_eq!(moved, 7, "应按 demand 上界读入 7 个元素");
        }

        assert_eq!(segm.least_count(), 3, "父段应反映子段读入的 7 个元素");

        drop(segm);
        assert_eq!(consumed, 7, "段 drop 时应把消费量提交给 reclaimer");

        let got: Vec<u8> = storage[..7]
            .iter()
            .map(|m| unsafe { m.assume_init_read() })
            .collect();
        assert_eq!(got, (0..7).collect::<Vec<_>>(), "读入内容应有序");
    }
}
