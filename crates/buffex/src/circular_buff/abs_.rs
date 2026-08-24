use abs_buff::{
    TrBuffTryRead, TrBuffTryWrite,
    io::{TrInput, TrOutput},
};

/// 可以观察环形缓冲状态的类型
pub trait TrObserver {
    fn capacity(&self) -> usize;

    fn ready(&self) -> usize;

    fn is_remote_end_closing(&self) -> bool;
}

/// 消费者，读取环形缓冲中数据
pub trait TrConsumer<T>
where
    Self: TrObserver,
{
    type Buff<'f>: TrBuffTryRead<T> where Self: 'f;
    type Err: core::error::Error;

    /// 如果消费者端在环形缓冲被构造时就设置为主动模式，此方法将一直返回错误，
    /// 否则返回一个实现 TrBuffTryRead 的实例
    fn try_as_buff(&mut self) -> Result<Self::Buff<'_>, Self::Err>;
}

/// 生产者，写入数据到环形缓冲
pub trait TrProducer<T>
where
    Self: TrObserver,
{
    type Buff<'f>: TrBuffTryWrite<T> where Self: 'f;
    type Err: core::error::Error;

    /// 如果生产者端在环形缓冲被构造时就设置为主动模式，此方法将一直返回错误，
    /// 否则返回一个实现 TrBuffTryWrite 的实例
    fn try_as_buff(&mut self) -> Result<Self::Buff<'_>, Self::Err>;
}

/// 用于表示生产者端为主动模式时的 trait 约束
pub trait TrDeviceProducer<T>
where
    Self: TrProducer<T>,
{
    type InputDevice: TrInput;
}

/// 用于表示消费者端为主动模式时的 trait 约束
pub trait TrDeviceConsumer<T>
where
    Self: TrConsumer<T>,
{
    type OutputDevice: TrOutput;
}