//! 定义环形缓冲的可扩展接口。大部分情况下，用户不需要使用。

use abs_buff::buffer::{TrBuffSegmMut, TrBuffSegmRef};

pub trait TrCircBuffCore
where
    Self: Send + Sync,
{
    type Data;

    fn advance_read(&self, amount: usize);

    fn advance_write(&self, amount: usize);
}

/// 生产端 hook 收到的事件（消费端完成读取 / 消费者关闭后触发）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerHookEvent {
    /// 缓冲区中的可供生产的数据有变化，携带新的可写容量
    Available(usize),

    /// 消费者已关闭，携带剩余可写容量
    ConsumerClose(usize),
}

/// 消费端 hook 收到的事件（生产端完成写入 / 生产者关闭后触发）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumerHookEvent {
    /// 缓冲区中的可供消费的数据有变化，携带新的可读数据量
    Available(usize),

    /// 生产者端已关闭，携带剩余可读数据量
    ProducerClose(usize),
}

pub enum ReceiverReact {
    /// Receiver has reacted upon the given buffer
    Reacted,

    /// Receiver
    Continue,
}

/// 接收并响应 CircCore 发出的事件通知
pub trait TrConsumer {
    type Data;
    type Buffer<'f>: TrBuffSegmRef<'f, Self::Data> where Self: 'f;

    fn is_passive(&self) -> bool;

    fn check(&mut self, event: ConsumerHookEvent) -> bool;

    fn react_async<'f>(
        &mut self,
        buff: &mut Self::Buffer<'f>,
    ) -> impl Future<Output = ReceiverReact>;
}

pub trait TrProducer {
    type Data;
    type Buffer<'f>: TrBuffSegmMut<'f, Self::Data> where Self: 'f;

    fn is_passive(&self) -> bool;

    fn check(&mut self, event: ProducerHookEvent) -> bool;

    fn react_async<'f>(
        &mut self,
        buff: &mut Self::Buffer<'f>,
    ) -> impl Future<Output = ReceiverReact>;
}

/// 环形缓冲状态的观察者。已由 Producer 和 Consumer 实现。
/// 保留仅为将来外部扩展用。
pub trait TrObserver {
    /// 环形缓冲的容量（单元数）。
    fn capacity(&self) -> usize;

    /// 当前可供本端操作的数据量（生产者视角为可写空间，消费者视角为可读数据）。
    fn ready(&self) -> usize;

    /// 对端是否已关闭（生产者视角：消费者端关闭；消费者视角：生产者端关闭）。
    fn is_remote_end_closing(&self) -> bool;
}
