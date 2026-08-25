//! `CircularBuff` 的类型与**公开 API**（集中在本文件）。
//!
//! 完整的设计与使用思路见 [`crate::circular_buff`] 的模块文档。
//!
//! 结构说明：
//!
//! * `CircularBuff<'a, P, C, B, T>`——环形缓冲器本体。`P` / `C` 是**端类型**
//!   （角色标记：被动=可访问，主动=占位），`B` 是存储拥有者，`T` 是元素类型；
//! * 四个端类型（[`PassiveProducer`] / [`PassiveConsumer`] /
//!   [`DeviceProducer`] / [`DeviceConsumer`]）——构建期由 builder 选定的角色
//!   标记，实现 `TrProducer` / `TrConsumer` 以满足 `CircularBuff` 的类型约束；
//!   **真实的访问入口是 `CircularBuff` 本身**（`try_as_buff` / `try_split_io`），
//!   主动端返回错误 / `None`（不对外暴露）。

use core::{
    future,
    marker::PhantomData,
};

use abs_buff::{
    Demand,
    io::{TrInput, TrOutput},
};

use crate::circular_buff::core_::WakeSlot;

use super::{
    abs_comp::{
        ConsumerHookEvent, ProducerHookEvent, ReceiverReact,
        TrConsumer, TrProducer,
    },
    core_::CircCore,
    reclaim_::{ReclSliceMut, ReclSliceRef, ReaderReclaim, WriterReclaim},
};

// ---------------------------------------------------------------------------
// 端类型（角色标记）
// ---------------------------------------------------------------------------

/// 被动生产者，并不对外暴露接口，存放进 CircCore
/// 实现 TrBuffTryWrite + TrProducer
pub struct BuffProducer<T> {
    demand_: Option<Demand<usize>>,
    wake_slot_: WakeSlot,
    _use_t_: PhantomData<fn() -> T>,
}

/// 被动消费者，并不对外暴露接口，存放进 CircCore
/// 实现 TrBuffTryRead + TrConsumer
pub struct BuffConsumer<T> {
    demand_: Option<Demand<usize>>,
    wake_slot_: WakeSlot,
    _use_t_: PhantomData<fn() -> T>,
}

/// 主动生产端，携带输入设备的实际类型，存放进 CircCore
pub struct DeviceProducer<TyInput, T>
where
    TyInput: TrInput<T>,
{
    input_: TyInput,
    _use_t: PhantomData<fn() -> T>,
}

/// 主动消费端，携带输出设备的实际类型，存放进 CircCore
pub struct DeviceConsumer<TyOutput, T>
where
    TyOutput: TrOutput<T>,
{
    output_: TyOutput,
    _use_t_: PhantomData<fn() -> T>,
}

// ---------------------------------------------------------------------------
//
// ---------------------------------------------------------------------------

impl<T> TrProducer for BuffProducer<T> {
    type Data = T;
    type Buffer = for<'a> ReclSliceMut<'a, T, WriterReclaim<'a, CircCore<Self, C, T>>>;

    fn check(&mut self, event: ProducerHookEvent) -> bool {
        let Option::Some(demand) = &mut self.demand_ else {
            return false;
        };
        if let ProducerHookEvent::Available(size) = event {
            let min = demand.min().cloned().unwrap_or(1);
            return size >= min;
        }
        todo!()
    }

    fn react_async(&mut self, _: &mut Self::Buffer) -> impl Future<Output = ReceiverReact> {
        // 被动模式不需要处理传进来的 buff
        future::ready(ReceiverReact::Continue)
    }
}

impl<T> TrConsumer for BuffConsumer<T> {
    type Data = T;
    type Buffer<'f> = ReclSliceRef<'f, T, ReaderReclaim<'f, CircCore<P, Self, T>>>;

    fn check(&mut self, event: ConsumerHookEvent) -> bool {
        let Option::Some(demand) = &mut self.demand_ else {
            return false;
        };
        if let ConsumerHookEvent::Available(size) = event {
            let min = demand.min().cloned().unwrap_or(1);
            return size >= min;
        }
        todo!()
    }

    fn react_async(&mut self, buff: &mut Self::Buffer) -> impl Future<Output = ReceiverReact> {
        future::ready(ReceiverReact::Continue)
    }
}

impl<'a, TyInput, C, T> TrProducer<'a, T> for DeviceProducer<TyInput, C, T>
where
    TyInput: TrInput<T>,
    C: TrConsumer<T>,
{
    type Buffer = ReclSliceMut<'a, T, WriterReclaim<'a, CircCore<Self, C, T>>>;

    fn check(&mut self, event: ProducerHookEvent) -> bool {
        if let ProducerHookEvent::Available(size) = event {
            size > 0
        } else {
            false
        }
    }

    async fn react_async(&mut self, buff: &mut Self::Buffer) -> ReceiverReact {
        while !buff.is_empty() {
            let demand = Demand::less_than(buff.least_count());
            let x = buff
                .as_segm_mut()
                .move_items_from_input_async(&mut self.input_, &demand)
                .await;
            if let Option::Some(err) = x.pick_right() {
                todo!("handle err {err}");
            }
        }
        ReceiverReact::Reacted
    }
}

impl<'a, TyOutput, T> TrConsumer for DeviceConsumer<TyOutput, T>
where
    TyOutput: TrOutput<T>,
{
    type Data = T;
    type Buffer = ReclSliceRef<'a, T, ReaderReclaim<'a, CircCore<P, Self, T>>>;

    fn check(&mut self, e: ConsumerHookEvent) -> bool {
        if let ConsumerHookEvent::Available(size) = e {
            size > 0
        } else {
            false
        }
    }

    async fn react_async(&mut self, buff: &mut Self::Buffer) -> ReceiverReact {
        // 从
        todo!()
    }
}
