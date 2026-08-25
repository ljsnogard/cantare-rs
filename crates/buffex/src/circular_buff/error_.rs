//! `CircularBuff` 的错误类型。
//!
//! * [`TxError`]——写（生产）端的错误，携带失败时写位置/空间快照；
//! * [`RxError`]——读（消费）端的错误，携带失败时读位置/数据快照；
//! * [`EndError`]——端访问错误：主动端不对外暴露，尝试访问时的错误。

use core::fmt;

/// 写（生产）端错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxError<S> {
    /// 缓冲区已满（或可写空间不足 `Demand` 下限），携带当前写位置。
    Stuffed(S),
    /// 写端已关闭，不再接受数据。
    Closing,
    /// 参数非法（例如 `Demand` 区间非法）。
    Argument,
}

impl<S: fmt::Debug> fmt::Display for TxError<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TxError::Stuffed(wp) => write!(f, "环形缓冲已满（写位置 {wp:?}）"),
            TxError::Closing => write!(f, "写端已关闭"),
            TxError::Argument => write!(f, "参数非法"),
        }
    }
}

impl<S: fmt::Debug> core::error::Error for TxError<S> {}

/// 读（消费）端错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RxError<S> {
    /// 缓冲区已空（或可读数据不足 `Demand` 下限），携带当前读位置。
    Drained(S),
    /// 读端已关闭，不再有数据。
    Closing,
    /// 参数非法。
    Argument,
}

impl<S: fmt::Debug> fmt::Display for RxError<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RxError::Drained(rp) => write!(f, "环形缓冲已空（读位置 {rp:?}）"),
            RxError::Closing => write!(f, "读端已关闭"),
            RxError::Argument => write!(f, "参数非法"),
        }
    }
}

impl<S: fmt::Debug> core::error::Error for RxError<S> {}
