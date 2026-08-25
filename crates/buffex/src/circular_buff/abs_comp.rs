//! 环形缓冲的抽象组件：核心提交接口（[`TrCircBuffCore`]）、端类型契约
//! （[`TrProducer`] / [`TrConsumer`]）、事件（[`ProducerHookEvent`] /
//! [`ConsumerHookEvent`]）与观察者（[`TrObserver`]）。
//!
//! 这些 trait 把「环形核心」与「两端」解耦成两层：
//!
//! * [`TrCircBuffCore`] 是**段提交接口**——两段式段（[`super::reclaim_`] 的
//!   `ReclSliceMut` / `ReclSliceRef`）在 drop 时经 `WriterReclaim` /
//!   `ReaderReclaim` 调用它推进读写位置。段层因此只依赖这个窄接口；
//! * [`TrProducer`] / [`TrConsumer`] 是**端契约**——由存放在核心里的端类型
//!   （被动：`BuffProducer` / `BuffConsumer`；
//!   主动：`DeviceProducer` / `DeviceConsumer`）
//!   实现。核心在提交路径上向对端触发事件，主动端经 `react_async` 拿到一段
//!   缓冲区视图完成同步搬运。
//!
//! # 为什么 `react_async` 泛化段参数（而不是关联类型）
//!
//! 若端契约携带 `Buffer<'f>` 关联类型（段），段在 drop 时要提交回核心，就
//! **必须指名核心类型**；而核心又泛型于端类型（`CircCore<P, C, T>`），于是
//! `P::Buffer<'f> = ReclSliceMut<…, WriterReclaim<…, CircCore<Self, C, T>>>`
//! 形成无基例的类型方程——这正是重构前「泛型参数无法稳定」的根源。
//!
//! 解法：端类型**不携带**段类型，`react_async` 的段参数由调用方（核心）按
//! 具体类型传入。段因此可以指名 `CircCore<P, C, T>`（段不在端类型内部，无环）。
//!
//! 大部分情况下用户不需要直接使用本模块；公开 API 见 [`super::builder`] 与
//! `spsc_`。

use abs_buff::{Demand, buffer::{TrBuffSegmMut, TrBuffSegmRef}};

/// 环形核心的「段提交」接口：段 drop 时按已消费量推进读写位置。
///
/// 这是 [`super::reclaim_`] 的 `WriterReclaim` / `ReaderReclaim` 唯一依赖的
/// 接口——段层通过它把消费量提交回环形，而无需指名核心的具体类型。**对端
/// hook 的触发由核心自身在推进位置时完成**（`advance_write` 后触发消费端
/// 事件、`advance_read` 后触发生产端事件），对段层完全透明。
///
/// `Send + Sync` 是该接口的硬性要求：段可能被搬运到其他线程（`abs_buff`
/// 管道），提交路径必须线程安全。
pub trait TrCircBuffCore
where
    Self: Send + Sync,
{
    type Data;

    fn advance_read(&self, amount: usize);

    fn advance_write(&self, amount: usize);
}

/// 生产端 hook 收到的事件（消费端完成读取 / 消费者关闭后触发）。
///
/// 由核心在 `advance_read` / `close_rx` 的提交路径上发给生产端 `P`。携带的
/// 新可写容量只是**参考值**：被动端唤醒等待者后由等待者重新检查条件（防丢失
/// 唤醒）；主动端据此决定是否继续泵设备。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerHookEvent {
    /// 缓冲区中的可供生产的数据有变化，携带新的可写容量
    Available(usize),

    /// 消费者已关闭，携带剩余可写容量
    ConsumerClose(usize),
}

/// 消费端 hook 收到的事件（生产端完成写入 / 生产者关闭后触发）。
///
/// 由核心在 `advance_write` / `close_tx` 的提交路径上发给消费端 `C`。语义与
/// [`ProducerHookEvent`] 对称。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumerHookEvent {
    /// 缓冲区中的可供消费的数据有变化，携带新的可读数据量
    Available(usize),

    /// 生产者端已关闭，携带剩余可读数据量
    ProducerClose(usize),
}

/// 端类型对 `react_async` 的反应结果：告诉核心「这次反应是否推进了数据」。
///
/// * [`ReceiverReact::Reacted`]——本端确实消费 / 生产了数据（例如主动泵搬了
///   一批数据）。核心的泵循环据此继续驱动；
/// * [`ReceiverReact::Continue`]——本次无事可做（被动端、或设备暂时无数据 /
///   无法接收），泵循环停止本轮。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiverReact {
    /// Receiver has reacted upon the given buffer
    Reacted,

    /// Receiver
    Continue,
}

/// 消费端（读侧）契约：由存放在核心中的**端类型**实现。
///
/// # 角色
///
/// 核心 `CircCore<P, C, T>` 持有 `C: TrConsumer`。生产端完成写入 / 关闭后，
/// 核心向 `C` 触发一个 [`ConsumerHookEvent`] 并**先问 [`TrConsumer::check`]
/// 是否感兴趣**——感兴趣才行动：被动端唤醒等待者（重查条件），主动端经
/// [`TrConsumer::react_async`] 把缓冲数据搬到输出设备。
///
/// # 两种实现
///
/// * **被动**（`BuffConsumer`）：`is_passive` 为 `true`，`check` 按
///   [`TrConsumer::set_demand`] 登记的完整需求（`demand.min()`）裁决——不足
///   下限不唤醒，避免 spurious wake；关闭事件例外——EOF 总是唤醒；等待者被
///   唤醒后仍重查条件（见 `spsc_` 的 `Park`）；
/// * **主动**（`DeviceConsumer`）：`is_passive` 为 `false`，`check` 由设备
///   裁决（有数据 / 关闭排空等），核心在事件分发与泵循环中都会调用它。
pub trait TrConsumer {
    type Data;

    fn is_passive(&self) -> bool;

    /// 本端对 `event`（携带当前数据量）是否感兴趣。
    ///
    /// 核心在**事件分发**（提交路径）与**泵循环每轮**调用本方法：返回 `false`
    /// 则不唤醒 / 不继续泵。被动端按 [`TrConsumer::set_demand`] 登记的等待者
    /// 完整需求（`demand.min()`）裁决——不足下限不唤醒，避免 spurious wake；
    /// 关闭事件例外——EOF 总是值得唤醒；主动端由设备裁决。`&mut self`——端
    /// 类型可在裁决时更新内部状态。
    fn check(&mut self, event: ConsumerHookEvent) -> bool;

    /// 登记 / 清除等待者的完整需求（被动端实现；主动端无等待者，默认无操作）。
    ///
    /// 由核心的 `arm_*` / `unpark_*` 调用（等待者 park / 完成时）：**先写
    /// `demand`（普通字段）、再 CAS 置 `STNDBY` armed 位**——fire 侧仅在
    /// armed 时访问 `demand`（三态协议，见 `circ_buff_` 的端类型文档），经
    /// 状态字 Acquire 读与置位 CAS 建立 happens-before（见 `core_` 的
    /// `fire_*`）。`check` 以 `demand.min()` 判兴趣。
    fn set_demand(&mut self, _demand: Option<Demand<usize>>) {}

    /// 对事件作出反应：把 `segm` 中的可读数据搬给本端（设备）。
    ///
    /// `TySegm` 泛型化——端类型**不携带**段类型，避免端类型指名核心类型造成
    /// 的类型级循环（见模块文档）。核心以具体段类型（两段式
    /// `ReclSliceRef`）调用本方法，并把返回的 future 同步轮询到完成。
    fn react_async<'f, TySegm>(
        &mut self,
        segm: &mut TySegm,
    ) -> impl Future<Output = ReceiverReact>
    where
        TySegm: TrBuffSegmRef<'f, Self::Data>,
        Self: 'f;
}

/// 生产端（写侧）契约：由存放在核心中的**端类型**实现。与 [`TrConsumer`]
/// 对称（详见其文档）；`react_async` 把输入设备的数据搬进 `segm`。
pub trait TrProducer {
    type Data;

    fn is_passive(&self) -> bool;

    /// 本端对 `event`（携带当前可写量）是否感兴趣。语义同
    /// [`TrConsumer::check`]（被动端按 [`TrProducer::set_demand`] 登记的完整
    /// 需求裁决）。
    fn check(&mut self, event: ProducerHookEvent) -> bool;

    /// 登记 / 清除等待者的完整需求。语义同 [`TrConsumer::set_demand`]。
    fn set_demand(&mut self, _demand: Option<Demand<usize>>) {}

    fn react_async<'f, TySegm>(
        &mut self,
        segm: &mut TySegm,
    ) -> impl Future<Output = ReceiverReact>
    where
        TySegm: TrBuffSegmMut<'f, Self::Data>,
        Self: 'f;
}

/// 环形缓冲状态的观察者。已由 Producer 和 Consumer 实现。
/// 保留仅为将来外部扩展用。
///
/// 只读查询面：容量、本端可操作量、对端是否关闭——不包含任何写操作。
pub trait TrObserver {
    /// 环形缓冲的容量（单元数）。
    fn capacity(&self) -> usize;

    /// 当前可供本端操作的数据量（生产者视角为可写空间，消费者视角为可读数据）。
    fn ready(&self) -> usize;

    /// 对端是否已关闭（生产者视角：消费者端关闭；消费者视角：生产者端关闭）。
    fn is_remote_end_closing(&self) -> bool;
}
