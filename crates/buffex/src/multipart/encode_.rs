use core::{
    marker::PhantomData,
};

use abs_buff::{
    Demand, TrBuffRead, TrBuffWrite, buffer::{TrAsBufferMut, TrBuffSegmMut, TrBuffSegmRef, TrBuffSegmView}, gen_may_cancel_future, x_deps::{abs_cancel, anylr},
};
use abs_cancel::{TrCancellationToken, TrMayCancel};
use anylr::SomeOf;

use crate::observer::TrObserver;
use super::{
    prefix::{TrMultipartPrefix, U16Prefix},
};

pub enum EncodeError<R, W, T>
where
    R: TrBuffRead<T>,
    W: TrBuffWrite<T>,
{
    /// 数据来源方错误
    Source(<R as TrBuffRead<T>>::Err),

    /// 数据发送方错误
    Target(<W as TrBuffWrite<T>>::Err),

    /// 格式错误
    Invalid,

    /// 调用者主动取消
    Cancelled,
}

/// 根据约定接收长度未知的 multipart 数据，每个数据块带有长度前缀
/// 以 0 长度前缀表示数据末尾。MultipartRecv 会按照这些长度前缀和
/// 末尾提示，将原本的数据流提交给调用者。
/// 默认情况下使用两个字节作为块长度前缀。
pub struct MultipartEncode<'a, R, W, T = u8, P = U16Prefix>
where
    R: TrBuffRead<T> + TrObserver,
    W: TrBuffWrite<T>,
    P: TrMultipartPrefix,
{
    source_: &'a mut R,
    target_: &'a mut W,
    _use_t_: PhantomData<fn() -> T>,
    _use_p_: PhantomData<fn() -> P>,
}

impl<'a, R, W, T, P> MultipartEncode<'a, R, W, T, P>
where
    R: TrBuffRead<T> + TrObserver,
    W: TrBuffWrite<T>,
    P: TrMultipartPrefix,
{
    /// 从一个缓存开始编码 multipart 数据并放入发送端
    pub const fn new(
        source: &'a mut R,
        target: &'a mut W,
    ) -> Self {
        MultipartEncode {
            source_: source,
            target_: target,
            _use_t_: PhantomData,
            _use_p_: PhantomData,
        }
    }
}

impl<'a, R, W, P> MultipartEncode<'a, R, W, u8, P>
where
    R: TrBuffRead<u8> + TrObserver,
    W: TrBuffWrite<u8>,
    P: TrMultipartPrefix,
{
    pub fn start_async<'f>(
        &'f mut self,
    ) -> MultipartEncodeStartAsync<'f, R, W> {
        let prefix_max: usize = <P as TrMultipartPrefix>::PREFIX_MAX();
        let prefix_len: usize = <P as TrMultipartPrefix>::PREFIX_LEN();
        MultipartEncodeStartAsync(
            self.source_,
            self.target_,
            prefix_max,
            prefix_len,
        )
    }
}

#[gen_may_cancel_future(MultipartEncodeStart)]
async fn multipart_send_async_<'f, R, W, K>(
    source: &'f mut R,
    target: &'f mut W,
    prefix_max: usize,
    prefix_len: usize,
    cancel: &'f mut K,
) -> SomeOf<usize, EncodeError<R, W, u8>>
where
    R: TrBuffRead<u8> + TrObserver,
    W: TrBuffWrite<u8>,
    K: TrCancellationToken + Clone,
{
    let mut c = 0usize;
    let mut tgt_tok = cancel.clone();
    let mut src_tok = cancel.clone();
    loop {
        if cancel.is_cancelled() {
            break;
        }
        let data_size = source.data_size() % prefix_max;
        let demand = Demand::between(prefix_len, prefix_len + data_size);
        let mut opt_segm = target
            .write_async(&demand)
            .may_cancel_with(&mut tgt_tok)
            .await;
        if let Option::Some(target_segm) = opt_segm.as_mut().pick_left() {
            let mut prefix_bytes = &mut data_size.to_be_bytes()[..prefix_len];
            // 写入长度前缀
            let mc = target_segm.move_items_from_buff(prefix_bytes.as_mut_slice_uninit());
            debug_assert!(mc == prefix_len);

            let mut cc = 0usize;
            while target_segm.least_count() > 0 || cc < data_size {
                let demand = Demand::less_than(target_segm.least_count() - cc);
                let mut opt_src_segm = source
                    .read_async(&demand)
                    .may_cancel_with(&mut src_tok)
                    .await;
                if let Option::Some(source_segm) = opt_src_segm.as_mut().pick_left() {
                    let mc = source_segm.move_items_to_segm(target_segm);
                    cc += mc;
                    c += mc;
                    continue;
                }
                if let Option::Some(err) = opt_src_segm.pick_right() {
                    return SomeOf::new_both(c, EncodeError::Source(err));
                }
            }
        }
        if let Option::Some(err) = opt_segm.pick_right() {
            return SomeOf::new_both(c, EncodeError::Target(err));
        }
    }
    SomeOf::new_left(0usize)
}
