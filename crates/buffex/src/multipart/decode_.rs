use core::{
    marker::PhantomData,
};

use abs_buff::{
    Demand, TrBuffRead, TrBuffWrite,
    gen_may_cancel_future,
    x_deps::{abs_cancel, anylr},
};
use abs_cancel::TrCancellationToken;
use anylr::SomeOf;

use super::{
    prefix::{TrMultipartPrefix, U16Prefix},
};

pub enum DecodeError<R, W, T>
where
    R: TrBuffRead<T>,
    W: TrBuffWrite<T>,
{
    /// 数据来源方错误
    Source(<R as TrBuffRead<T>>::Err),

    /// 数据接收方错误
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
pub struct MultipartDecode<'a, R, W, T = u8, P = U16Prefix>
where
    R: TrBuffRead<T>,
    W: TrBuffWrite<T>,
    P: TrMultipartPrefix,
{
    source_: &'a mut R,
    target_: &'a mut W,
    _use_t_: PhantomData<fn() -> T>,
    _use_p_: PhantomData<fn() -> P>,
}

impl<'a, R, W, T, P> MultipartDecode<'a, R, W, T, P>
where
    R: TrBuffRead<T>,
    W: TrBuffWrite<T>,
    P: TrMultipartPrefix,
{
    /// 从一个原始源开始读取并解析 multipart 协议的数据并放入接收端
    pub const fn new(
        source: &'a mut R,
        target: &'a mut W,
    ) -> Self {
        MultipartDecode {
            source_: source,
            target_: target,
            _use_t_: PhantomData,
            _use_p_: PhantomData,
        }
    }

    pub fn start_async<'f>(
        &'f mut self,
    ) -> MultipartDecodeStartAsync<'a, 'f, R, W, T, P> {
        MultipartDecodeStartAsync(self)
    }
}

#[gen_may_cancel_future(MultipartDecodeStart)]
async fn multipart_decode_start_async_<'a, 'f, R, W, T, P, K>(
    decode: &'f mut MultipartDecode<'a, R, W, T, P>,
    cancel: &'f mut K,
) -> SomeOf<usize, DecodeError<R, W, T>>
where
    'a: 'f,
    R: TrBuffRead<T>,
    W: TrBuffWrite<T>,
    P: TrMultipartPrefix,
    K: TrCancellationToken + Clone,
{
    let mut c = 0usize;
    loop {
        if cancel.is_cancelled() {
            break;
        }
    }
    SomeOf::new_left(c)
}
