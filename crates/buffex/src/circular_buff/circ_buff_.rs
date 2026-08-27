//! `CircularBuff` 的**端类型**：被动标记（[`BuffProducer`] / [`BuffConsumer`]）
//! 与主动设备端（[`DeviceProducer`] / [`DeviceConsumer`]）。
//!
//! 完整的设计与使用思路见 [`crate::circular_buff`] 的模块文档。
//!
//! 端类型存放在核心（`CircCore`）内部，实现
//! `TrProducer` / `TrConsumer`
//! 契约。它们**不对外暴露**——真实的访问入口是构建器产出的
//! `Producer` / `Consumer`
//! 半部（借用核心）。主动端（设备驱动）的半部操作返回错误（不对外访问）。

use core::{
    marker::PhantomData,
    ptr,
    sync::atomic::AtomicPtr,
};

use abs_buff::{
    Demand,
    buffer::{TrBuffSegmMut, TrBuffSegmRef},
    io::{TrInput, TrOutput},
    // x_deps::abs_cancel,
};
// use abs_cancel::{NonCancellableToken, TrMayCancel};
use atomex::AtomexPtrOwned;
use atomic_sync::x_deps::atomex;

use super::{
    abs_comp_::{
        ConsumerHookEvent, ProducerHookEvent, ReceiverReact,
        TrConsumer, TrProducer,
    },
    core_::WakeSlot,
};

// ---------------------------------------------------------------------------
// 端类型
// ---------------------------------------------------------------------------

struct BuffObsv {
    demand_: AtomexPtrOwned<Demand<usize>>,
}

impl BuffObsv {
    pub const fn new() -> Self {
        BuffObsv {
            demand_: AtomexPtrOwned::new(AtomicPtr::new(ptr::null_mut())),
        }
    }

    /// 假定 Demand 指针为空，设置新的 Demand。返回设置是否成功。
    #[inline]
    pub fn try_set_demand(
        &self,
        demand: &Demand<usize>,
    ) -> bool {
        let p = demand as *const _ as *mut Demand<usize>;
        let p = unsafe { core::ptr::NonNull::new_unchecked(p) };
        self.demand_
            .try_spin_init(p)
            .is_ok()
    }

    /// 假定 Demand 指针非空，重新置空。返回此前存储的指针。
    #[inline]
    pub fn try_reset_demand(
        &self,
    ) -> Result<ptr::NonNull<Demand<usize>>, *mut Demand<usize>> {
        self.demand_.try_reset()
    }
}

/// 被动生产端的端类型：存放进核心的 `P` 参数。
///
/// 携带**等待者的需求**（`TrProducer::set_demand` 登记、`check` 裁决）：
/// 可写空间不足 `demand` 下限时**不唤醒**写者（避免 spurious wake；写者被
/// 唤醒后仍会重查条件）。唤醒槽位由核心持有（见 `core_` 的 `producer_wake_`）。
///
/// # 被动端的三态生命周期（STNDBY armed 协议，SPSC）
///
/// 在任意时刻，被动端处于以下三种状态之一：
///
/// 1. **无需求等待调用**（idle）：没有等待者登记需求（`demand` 为 `None`），
///    `TX_STNDBY` 为 0——**不接受 fire**（fire 直接返回）；
/// 2. **有需求正在登记**（arming）：等待者已写入 `demand`，但 armed 位尚未
///    置位——`TX_STNDBY` 仍为 0，**不接受 fire**（等待者注册后会重查条件，
///    不会丢唤醒）；
/// 3. **等待中**（armed / standing by）：`TX_STNDBY` 为 1，**接受 fire**——
///    fire 侧此时才访问 `demand` 判断兴趣（经状态字 Acquire 读与置位 CAS
///    建立 happens-before，见 `core_` 的 `fire_producer`），感兴趣则唤醒
///    等待者。
///
/// 状态迁移：等待者 park 时 `1 → 2 → 3`（写 demand → CAS 置 `TX_STNDBY` →
/// 注册槽位）；完成 / drop 时 `3 → 1`（注销槽位 → CAS 清 `TX_STNDBY` →
/// 清 demand）。因为 `demand` 只在态 3 被 fire 读取、写入先于 armed 置位、
/// 清除后于 armed 清位，它是**普通字段**（无需原子）。
pub struct BufProducer<T> {
    buf_obsv_: BuffObsv,
    wakeslot_: WakeSlot,
    _unuse_t_: PhantomData<fn() -> T>,
}

impl<T> BufProducer<T> {
    pub(super) const fn new() -> Self {
        BufProducer {
            buf_obsv_: BuffObsv::new(),
            wakeslot_: WakeSlot::new(),
            _unuse_t_: PhantomData,
        }
    }

    #[inline]
    pub(super) fn try_set_demand(&self, demand: &Demand<usize>) -> bool {
        self.buf_obsv_.try_set_demand(demand)
    }

    #[inline]
    pub(super) fn try_reset_demand(
        &self,
    ) -> Result<ptr::NonNull<Demand<usize>>, *mut Demand<usize>> {
        self.buf_obsv_.try_reset_demand()
    }

    /// 被动等待者注册 / 注销 waker 的唤醒槽位（`core_` 的等待 future 使用）。
    #[inline]
    pub(super) fn wakeslot(&self) -> &WakeSlot {
        &self.wakeslot_
    }
}

/// 被动消费端的端类型：与 [`BuffProducer`] 对称，存放进核心的 `C` 参数。
///
/// 三态生命周期同 [`BuffProducer`]（armed 位为 `RX_STNDBY`）：fire 仅在
/// **等待中**（态 3）访问 `demand` 判断兴趣。
pub struct BufConsumer<T> {
    buf_obsv_: BuffObsv,
    wakeslot_: WakeSlot,
    _unuse_t_: PhantomData<fn() -> T>,
}

impl<T> BufConsumer<T> {
    pub(super) const fn new() -> Self {
        BufConsumer {
            buf_obsv_: BuffObsv::new(),
            wakeslot_: WakeSlot::new(),
            _unuse_t_: PhantomData,
        }
    }

    #[inline]
    pub(super) fn try_set_demand(&self, demand: &Demand<usize>) -> bool {
        self.buf_obsv_.try_set_demand(demand)
    }

    #[inline]
    pub(super) fn try_reset_demand(
        &self,
    ) -> Result<ptr::NonNull<Demand<usize>>, *mut Demand<usize>> {
        self.buf_obsv_.try_reset_demand()
    }

    /// 被动等待者注册 / 注销 waker 的唤醒槽位（`core_` 的等待 future 使用）。
    #[inline]
    pub(super) fn wakeslot(&self) -> &WakeSlot {
        &self.wakeslot_
    }
}

/// 主动生产端的端类型：携带输入设备的**实际类型**（`TyInput`），随设备一同
/// 存放进核心。构造完成后由 hook 驱动，自动从设备提取数据填入缓冲——因此
/// **不对外暴露**可访问的写半部（主动端的 `Producer` 半部操作返回错误）。
///
/// # 为什么是具体类型而不是类型擦除
///
/// 本类型直接持有 `input_: TyInput`（构建期把设备 move 进核心），核心因此
/// **无需任何类型擦除**即可在内部持有并驱动设备；`TrInput` 带泛型关联类型、
/// 不能直接 `dyn`，具体化是唯一可行路径。代价是核心必须泛型于端类型
/// （`CircCore<P, C, T>`），以及设备必须 `Send + Sync`（随核心跨线程）。
///
/// # 行为
///
/// `react_async`：循环地把 `TrInput` 的数据搬进传入的可写段（经 `abs_buff`
/// 的 `move_items_from_input_async`），直到段满或设备暂无数据。泵由核心在
/// 提交路径上同步轮询到完成（`Waker::noop`，不 spawn）。
pub struct DevProducer<TyInput, T>
where
    TyInput: TrInput<T>,
{
    input_: TyInput,
    _use_t: PhantomData<fn() -> T>,
}

impl<TyInput, T> DevProducer<TyInput, T>
where
    TyInput: TrInput<T>,
{
    pub(super) fn new(input: TyInput) -> Self {
        DevProducer {
            input_: input,
            _use_t: PhantomData,
        }
    }
}

/// 主动消费端的端类型：携带输出设备的**实际类型**（`TyOutput`），与
/// [`DeviceProducer`] 对称存放进核心的 `C` 参数，不对外暴露。
///
/// `react_async`：循环地把传入的可读段数据搬到 `TrOutput`（经
/// `move_items_to_output_async`），直到段空或设备暂时不能接收。
pub struct DevConsumer<TyOutput, T>
where
    TyOutput: TrOutput<T>,
{
    output_: TyOutput,
    _use_t_: PhantomData<fn() -> T>,
}

impl<TyOutput, T> DevConsumer<TyOutput, T>
where
    TyOutput: TrOutput<T>,
{
    pub(super) fn new(output: TyOutput) -> Self {
        DevConsumer {
            output_: output,
            _use_t_: PhantomData,
        }
    }
}

// ---------------------------------------------------------------------------
// 端契约实现
// ---------------------------------------------------------------------------

impl<T> TrProducer for BufProducer<T> {
    type Data = T;

    #[inline]
    fn is_passive(&self) -> bool {
        true
    }

    fn check(&self, event: ProducerHookEvent) -> bool {
        let ProducerHookEvent::Available(free) = event else {
            return true;
        };
        if free == 0 {
            return false;
        }
        let Option::Some(demand_ptr) = self.buf_obsv_.demand_.load() else {
            return false;
        };
        let demand = unsafe { demand_ptr.as_ref() };
        let min = demand.min().copied().unwrap_or(0);
        if free >= min {
            self.wakeslot_.signal();
            true
        } else {
            false
        }
    }

    #[inline]
    async fn react_async<'f, TySegm>(
        &mut self,
        _segm: &mut TySegm,
    ) -> ReceiverReact
    where
        TySegm: TrBuffSegmMut<'f, T>,
    {
        // 被动端由调用者驱动，无反应。
        ReceiverReact::Continue
    }
}

impl<T> TrConsumer for BufConsumer<T> {
    type Data = T;

    #[inline]
    fn is_passive(&self) -> bool {
        true
    }

    fn check(&self, event: ConsumerHookEvent) -> bool {
        let ConsumerHookEvent::Available(ready) = event else {
            return true;
        };
        if ready == 0 {
            return false;
        }
        let Option::Some(demand_ptr) = self.buf_obsv_.demand_.load() else {
            return false;
        };
        let demand = unsafe { demand_ptr.as_ref() };
        let min = demand.min().copied().unwrap_or(0);
        if ready >= min {
            self.wakeslot_.signal();
            true
        } else {
            false
        }
    }

    #[inline]
    async fn react_async<'f, TySegm>(&mut self, _segm: &mut TySegm) -> ReceiverReact
    where
        TySegm: TrBuffSegmRef<'f, T>,
    {
        ReceiverReact::Continue
    }
}

impl<TyInput, T> TrProducer for DevProducer<TyInput, T>
where
    TyInput: TrInput<T>,
{
    type Data = T;

    #[inline]
    fn is_passive(&self) -> bool {
        false
    }

    #[inline]
    fn check(&self, event: ProducerHookEvent) -> bool {
        // 有可写空间即值得泵一轮（参考值；泵循环还会重查状态）。
        matches!(event, ProducerHookEvent::Available(size) if size > 0)
    }

    async fn react_async<'f, TySegm>(
        &mut self,
        segm: &mut TySegm,
    ) -> ReceiverReact
    where
        TySegm: TrBuffSegmMut<'f, T>,
    {
        let mut moved = 0usize;
        while !segm.is_empty() {
            let demand = Demand::less_than(segm.least_count());
            let x = segm
                .as_segm_mut()
                .move_items_from_input_async(&mut self.input_, &demand)
                .await;
            // 设备错误：本轮视为无数据。
            if x.as_ref().pick_right().is_some() {
                break;
            }
            let n = x.pick_left().unwrap_or(0);
            if n == 0 {
                break; // 设备暂无数据
            }
            moved += n;
        }
        if moved > 0 {
            ReceiverReact::Reacted
        } else {
            ReceiverReact::Continue
        }
    }
}

impl<TyOutput, T> TrConsumer for DevConsumer<TyOutput, T>
where
    TyOutput: TrOutput<T>,
{
    type Data = T;

    #[inline]
    fn is_passive(&self) -> bool {
        false
    }

    #[inline]
    fn check(&self, event: ConsumerHookEvent) -> bool {
        // 有数据即值得泵一轮；**ProducerClose 也感兴趣**——写端关闭后必须
        // 把残留数据排空（泵循环会一直搬到空为止）。
        matches!(
            event,
            ConsumerHookEvent::Available(size) if size > 0
        ) || matches!(event, ConsumerHookEvent::ProducerClose(_))
    }

    async fn react_async<'f, TySegm>(&mut self, segm: &mut TySegm) -> ReceiverReact
    where
        TySegm: TrBuffSegmRef<'f, T>,
    {
        let mut moved = 0usize;
        while !segm.is_empty() {
            let demand = Demand::less_than(segm.least_count());
            let x = segm
                .as_segm_ref()
                .move_items_to_output_async(&mut self.output_, &demand)
                .await;
            // 设备错误：本轮视为不能接收。
            if x.as_ref().pick_right().is_some() {
                break;
            }
            let n = x.pick_left().unwrap_or(0);
            if n == 0 {
                break; // 设备暂时不能接收
            }
            moved += n;
        }
        if moved > 0 {
            ReceiverReact::Reacted
        } else {
            ReceiverReact::Continue
        }
    }
}
