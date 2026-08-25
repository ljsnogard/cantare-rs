use core::{
    borrow::{Borrow, BorrowMut},
    marker::PhantomData,
    mem::MaybeUninit,
};

use abs_buff::io::{TrInput, TrOutput};
use abs_mm::mem_alloc::{TrMalloc, CoreAlloc};
use mm_ptr::{Shared, x_deps::abs_mm};

use crate::circular_buff::core_;

use super::{
    abs_comp::{TrConsumer, TrProducer},
    circ_buff_::{BuffConsumer, BuffProducer},
    core_::{CircCore, },
    spsc_,
};

pub(super) type CoreRef<TyCore, TyAlloc = CoreAlloc> =
    Shared<TyCore, TyAlloc>;

pub type SpscPair<TyCore, TyAlloc = CoreAlloc> = (
    Producer<TyCore, TyAlloc>,
    Consumer<TyCore, TyAlloc>,
);

pub enum BuilderError<T> {
    SizeTooSmall(T),
    SizeTooBig(T),
}

/// 面向环形缓冲最终用户的 Producer。其主要作用是代理转发用户请求到 CircCore。
pub struct Producer<TyProducer, TyConsumer, TyData, TyAlloc>
where
    TyProducer: TrProducer<TyData>,
    TyConsumer: TrConsumer<TyData>,
    TyAlloc: TrMalloc + Clone,
{
    core_ref_: CoreRef<TyProducer, TyConsumer, TyData, TyAlloc>,
}

/// 面向环形缓冲最终用户的 Consumer。其主要作用是代理转发用户请求到 CircCore
pub struct Consumer<TyProducer, TyConsumer, TyData, TyAlloc>
where
    TyProducer: TrProducer<TyData>,
    TyConsumer: TrConsumer<TyData>,
    TyAlloc: TrMalloc + Clone,
{
    core_ref_: CoreRef<TyProducer, TyConsumer, TyData, TyAlloc>,
}

pub struct BufferBuilder<B, T = u8>
where
    B: BorrowMut<[MaybeUninit<T>]> + Send + Sync,
{
    buffer_: B,
    capacity_: usize,
    _use_t_: PhantomData<fn () -> T>,
}

pub struct PipeIntoOutputBuilder<F, O, B, T>
where
    F: FnOnce() -> O,
    O: TrOutput<T>,
    B: BorrowMut<[MaybeUninit<T>]> + Send + Sync,
{
    builder_: BufferBuilder<B, T>,
    factory_: F,
    _mark_o_: PhantomData<fn () -> O>,
}

pub struct PipeFromInputBuilder<F, I, B, T>
where
    F: FnOnce() -> I,
    I: TrInput<T>,
    B: BorrowMut<[MaybeUninit<T>]> + Send + Sync,
{
    builder_: BufferBuilder<B, T>,
    factory_: F,
    _mark_i_: PhantomData<fn () -> I>,
}

pub struct SpscBuilder<P, C, T, A>
where
    P: TrProducer<T>,
    C: TrConsumer<T>,
    A: TrMalloc + Clone,
{
    core_ref_: CoreRef<P, C, T, A>,
    // _using_o_: PhantomData<(TyProducer, TyConsumer, TyBuff)>,
    // _using_t_: PhantomData<TyBuff>,
}

impl<B, T> BufferBuilder<B, T>
where
    B: BorrowMut<[MaybeUninit<T>]> + Send + Sync,
{
    pub fn from_buffer(buffer: B) -> Result<Self, BuilderError<B>>  {
        let buff = buffer.borrow();
        let size = buff.len();
        if size < core_::MIN_CAPACITY {
            return Result::Err(BuilderError::SizeTooSmall(buffer));
        }
        if size > core_::MAX_CAPACITY {
            return Result::Err(BuilderError::SizeTooBig(buffer));
        }
        Result::Ok(BufferBuilder {
            buffer_: buffer,
            capacity_: size,
            _use_t_: PhantomData,
        })
    }

    pub fn pipe_into_output<F, O>(f: F) -> PipeIntoOutputBuilder<F, O, B, T>
    where
        F: FnOnce() -> O,
        O: TrOutput<T>,
    {
        todo!()
    }

    pub fn pipe_from_input<F, I>(f: F) -> PipeFromInputBuilder<F, I, B, T>
    where
        F: FnOnce() -> I,
        I: TrInput<T>,
    {
        todo!()
    }

    /// 两端不指定的情况下，默认设为被动模式
    pub fn build(self) -> SpscPair<spsc_::Producer<T>, spsc_::Consumer<T>, T, CoreAlloc> {
        todo!()
    }
}

impl<P, C, T, A> SpscBuilder<P, C, T, A>
where
    P: TrProducer<T>,
    C: TrConsumer<T>,
    A: TrMalloc + Clone,
{
    pub(super) fn build_producer() -> Producer<P, C, T, A> {
        todo!()
    }

    pub(super) fn build_consumer() -> Consumer<P, C, T, A> {
        todo!()
    }

    pub(super) fn build_pair() -> SpscPair<P, C, T, A> {
        todo!()
    }
}
