//! 提供给最终用户、暴露的公共接口 Producer 和 Consumer。
//! 所有对 Circular Buffer 的操作都必须通过这两个实例。
//! 如果有一端在 Circular Buffer 构建时就已经被指定，那么这一端就不会提供给使用者。

use abs_buff::{
    Demand, TrBuffRead, TrBuffWrite, TrBuffTryRead, TrBuffTryWrite,
    x_deps::{abs_cancel, anylr, gen_mcf_macro},
};
use abs_cancel::TrCancellationToken;
use anylr::SomeOf;
use abs_mm::mem_alloc::{TrMalloc, CoreAlloc};
use gen_mcf_macro::gen_may_cancel_future;
use mm_ptr::{Shared, x_deps::abs_mm};

use super::{
    abs_comp::{TrConsumer, TrObserver, TrProducer},
    core_::CircCore,
    error_::{RxError, TxError},
};

pub(super) type CoreRef<P, C, T, A> = Shared<CircCore<P, C, T>, A>;

pub type SpscPair<P, C, T = u8, A = CoreAlloc> =
    (Producer<P, C, T, A>, Consumer<P, C, T, A>);

pub struct Producer<P, C, T, A>
where
    P: TrProducer<T>,
    C: TrConsumer<T>,
    A: TrMalloc + Clone,
{
    core_ref_: CoreRef<P, C, T, A>,
}

pub struct Consumer<P, C, T, A>
where
    P: TrProducer<T>,
    C: TrConsumer<T>,
    A: TrMalloc + Clone,
{
    core_ref_: CoreRef<P, C, T, A>,
}

impl<P, C, T, A> Producer<P, C, T, A>
where
    P: TrProducer<T>,
    C: TrConsumer<T>,
    A: TrMalloc + Clone,
{
    /// 环形缓冲的容量（单元数）。
    pub fn capacity(&self) -> usize {
        self.core_ref_.capacity()
    }

    /// 当前可供本端操作的数据量（生产者视角为可写空间，消费者视角为可读数据）。
    pub fn ready(&self) -> usize {
        todo!()
    }

    /// 对端是否已关闭（生产者视角：消费者端关闭；消费者视角：生产者端关闭）。
    pub fn is_remote_end_closing(&self) -> bool {
        todo!()
    }

    pub fn is_pairing(&self, consumer: &Consumer<P, C, T, A>) -> bool {
        todo!()
    }

    pub fn write_async(&mut self, demand: &Demand<usize>) -> ProducerWriteAsync<'_, P, C, T, A> {
        ProducerWriteAsync(self, demand)
    }

    pub fn try_write(&mut self, demand: &Demand<usize>) -> SomeOf<WrSegm<'_, T>, TxError<T>> {
        todo!()
    }
}

impl<P, C, T, A> Consumer<P, C, T, A>
where
    P: TrProducer<T>,
    C: TrConsumer<T>,
    A: TrMalloc + Clone,
{
    /// 环形缓冲的容量（单元数）。
    pub fn capacity(&self) -> usize {
        self.core_ref_.capacity()
    }

    /// 当前可供本端操作的数据量（生产者视角为可写空间，消费者视角为可读数据）。
    pub fn ready(&self) -> usize {
        todo!()
    }

    /// 对端是否已关闭（生产者视角：消费者端关闭；消费者视角：生产者端关闭）。
    pub fn is_remote_end_closing(&self) -> bool {
        todo!()
    }

    pub fn is_pairing(&self, producer: &Producer<P, C, T, A>) -> bool {
        todo!()
    }

    pub fn read_async(&mut self, demand: &Demand<usize>) -> ConsumerReadAsync<'_, P, C, T, A> {
        ConsumerReadAsync(self, demand)
    }

    pub fn try_read(&mut self, demand: &Demand<usize>) -> SomeOf<WrSegm<'_, T>, TxError<T>> {
        todo!()
    }
}

impl<P, C, T, A> TrObserver for Producer<P, C, T, A>
where
    P: TrProducer<T>,
    C: TrConsumer<T>,
    A: TrMalloc + Clone,
{
    #[inline]
    fn capacity(&self) -> usize {
        Producer::capacity(self)
    }

    #[inline]
    fn ready(&self) -> usize {
        Producer::ready(self)
    }

    #[inline]
    fn is_remote_end_closing(&self) -> bool {
        Producer::is_remote_end_closing(self)
    }
}

impl<P, C, T, A> TrObserver for Consumer<P, C, T, A>
where
    P: TrProducer<T>,
    C: TrConsumer<T>,
    A: TrMalloc + Clone,
{
    #[inline]
    fn capacity(&self) -> usize {
        Consumer::capacity(self)
    }

    #[inline]
    fn ready(&self) -> usize {
        Consumer::ready(self)
    }

    #[inline]
    fn is_remote_end_closing(&self) -> bool {
        Consumer::is_remote_end_closing(self)
    }
}

impl<P, C, T, A> TrBuffWrite<T> for Producer<P, C, T, A>
where
    P: TrProducer<T>,
    C: TrConsumer<T>,
    A: TrMalloc + Clone,
{

}

impl<P, C, T, A> TrBuffTryWrite<T> for Producer<P, C, T, A>
where
    P: TrProducer<T>,
    C: TrConsumer<T>,
    A: TrMalloc + Clone,
{

}

impl<P, C, T, A> TrBuffRead<T> for Consumer<P, C, T, A>
where
    P: TrProducer<T>,
    C: TrConsumer<T>,
    A: TrMalloc + Clone,
{

}

impl<P, C, T, A> TrBuffTryWrite<T> for Consumer<P, C, T, A>
where
    P: TrProducer<T>,
    C: TrConsumer<T>,
    A: TrMalloc + Clone,
{

}
