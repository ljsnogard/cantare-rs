#[allow(dead_code)]
mod tests_expanded_ {
    use abs_cancel::TrCancellationToken;
    use gen_mcf_macro::gen_may_cancel_future;

    /// # Usage Rules:
    /// 0. Must be an `async fn`;
    /// 1. At least one lifetime and the last one must be for the cancellation token;
    /// 2. The last argument and generic parameter type must be the cancellation token type and constrained with: `TrCancellationToken`;
    /// 3. Use a where clause to constrain the cancel token type;
    #[gen_may_cancel_future(DoThing)]
    pub async fn do_thing_async<'a, 'b, 'x, 'c, A, B, C>(
        a: &'a mut A,
        b: &'b mut B,
        l: usize,
        x: core::slice::Iter<'x, A>,
        cancel: &'c mut C,
    ) -> usize
    where
        'a: 'c,
        'b: 'c,
        'x: 'c,
        A: Send,
        B: Sync,
        C: TrCancellationToken,
    {
        let _ = (a, b, l, x, cancel);
        42
    }

    // -------------------------------------------------------------------
    // 回归用例：where 子句中「与 cancel token 生命周期（最后一个 `'f`）关联
    // 的生命周期」（`'a`，经 `S: Trait<'a>` + `'a: 'f` 关联）必须成为生成类型
    // 的泛型参数并补上 `'a: 'f` 约束——否则生成的 future / factory 缺失真实
    // 约束（E0277 / E0271），或 E0597（借用寿命被统一成 `'f` 超出局部变量）。
    // -------------------------------------------------------------------

    /// 带生命周期参数的 trait，模拟 `TrBuffSegmMut<'a, T>` 的形态。
    pub trait SegmLike<'a> {
        fn len(&self) -> usize;
    }

    impl<'a> SegmLike<'a> for () {
        fn len(&self) -> usize {
            0
        }
    }

    #[gen_may_cancel_future(ReactThing)]
    pub async fn react_thing_async<'a, 'f, S, K>(
        segm: &'f mut S,
        cancel: &'f mut K,
    ) -> usize
    where
        'a: 'f,
        S: 'a + SegmLike<'a>,
        K: TrCancellationToken + Clone,
    {
        let _ = cancel;
        segm.len()
    }
}


#[compio::test]
pub async fn run() {
    use abs_cancel::NonCancellableToken;
    use tests_expanded_::do_thing_async;

    let mut a = 1usize;
    let mut b = 2.0f32;
    let l = 3usize;
    let x = [0usize; 1usize].as_ref().iter();
    let _ = do_thing_async(&mut a, &mut b, l, x, NonCancellableToken::shared_mut()).await;
}

#[compio::test]
pub async fn run_react_thing() {
    use abs_cancel::NonCancellableToken;
    use tests_expanded_::react_thing_async;

    let mut segm = ();
    let _ = react_thing_async(&mut segm, NonCancellableToken::shared_mut()).await;
}
