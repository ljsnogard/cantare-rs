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

use core::{borrow::BorrowMut, marker::PhantomData, mem::MaybeUninit};

use abs_buff::io::{TrInput, TrOutput};

use super::{
    abs_::{TrConsumer, TrDeviceConsumer, TrDeviceProducer, TrObserver, TrProducer},
    core_::RingCore,
    error_::EndError,
    half_::{ConsumerHalf, ProducerHalf},
};

// ---------------------------------------------------------------------------
// 端类型（角色标记）
// ---------------------------------------------------------------------------

/// 被动生产端的端类型（角色标记，零大小）。
///
/// 该标记只表达「生产端为被动模式」这一类型信息。真实的写半部经
/// [`CircularBuff::try_as_buff`]（`TrProducer`）或 [`CircularBuff::try_split_io`]
/// 获得；标记自身的 `TrProducer` 实现不提供半部（见其文档）。
pub struct PassiveProducer<T = u8> {
    _marker: PhantomData<fn() -> T>,
}

/// 被动消费端的端类型（角色标记，零大小）。见 [`PassiveProducer`]。
pub struct PassiveConsumer<T = u8> {
    _marker: PhantomData<fn() -> T>,
}

/// 主动生产端的**占位类型**（角色标记，零大小）：携带输入设备的实际类型
/// `TyInput`。
///
/// # 设计要点：主动端不对外暴露
///
/// 主动生产端由输入设备驱动，不可能再让外部调用者访问：其 `TrProducer`
/// 实现（`try_as_buff`）**永远返回错误**。`TyInput` 经
/// [`TrDeviceProducer`] 的关联类型 `InputDevice` 携带设备类型，核心据此在
/// 内部持有设备（无需类型擦除之外的泛型参数）。
pub struct DeviceProducer<TyInput, T>
where
    TyInput: TrInput<T>,
{
    _input: PhantomData<TyInput>,
    _marker: PhantomData<fn() -> T>,
}

/// 主动消费端的**占位类型**（角色标记，零大小）：携带输出设备的实际类型
/// `TyOutput`。其 `TrConsumer` 实现（`try_as_buff`）永远返回错误。
pub struct DeviceConsumer<TyOutput, T>
where
    TyOutput: TrOutput<T>,
{
    _output: PhantomData<TyOutput>,
    _marker: PhantomData<fn() -> T>,
}

// ---------------------------------------------------------------------------
// CircularBuff
// ---------------------------------------------------------------------------

/// 唤醒式环形缓冲器：构造期定「模式与用途」，核心机制与 hook 见模块文档。
///
/// # 泛型参数
///
/// * `'a`——内部缓冲区（以及主动端设备）的借用期；
/// * `P`——生产端类型：被动 = [`PassiveProducer`]，主动 = [`DeviceProducer`]；
/// * `C`——消费端类型：被动 = [`PassiveConsumer`]，主动 = [`DeviceConsumer`]；
/// * `B`——存储拥有者，要求能提供 `&mut [MaybeUninit<T>]` 视图
///   （实践中为 `&'a mut [MaybeUninit<T>]`，`no_std` 下借用而非拥有）；
/// * `T`——元素类型，默认 `u8`。
///
/// # 设计要点
///
/// * **hook 对调用者透明**：核心内部使用哪个 hook（被动=唤醒，主动=搬运）是
///   构建期由 builder 决定并隐藏的；`P` / `C` 是**端类型**（决定可访问性），
///   不是 hook；
/// * **主动端不对外暴露**：主动端的 `try_as_buff` 永远返回错误、`try_split_io`
///   返回 `None`；设备类型经 `TrDeviceProducer` / `TrDeviceConsumer` 的关联
///   类型携带在端类型里；
/// * **存储统一为 `[MaybeUninit<T>]`**：`B` 只要求 `BorrowMut` 提供
///   `&mut [MaybeUninit<T>]` 视图，内部一律以此视图操作缓冲区。
pub struct CircularBuff<'a, P, C, B, T = u8>
where
    P: TrProducer<T>,
    C: TrConsumer<T>,
    B: BorrowMut<[MaybeUninit<T>]>,
{
    /// 环形核心：位置状态机 + 两个 hook。
    core: RingCore<T>,
    /// 内部缓冲区拥有者（统一以 `[MaybeUninit<T>]` 视图访问）。
    ///
    /// 本字段只用于**持有存储借用**（让 `'a` 的借用关系在类型上成立）；
    /// 实际读写都经核心内部的裸指针（见 `core_` 的安全说明），因此不直接
    /// 读取本字段。
    #[allow(dead_code)]
    buffer_: B,
    /// 生产端类型（角色标记）。
    _producer: PhantomData<P>,
    /// 消费端类型（角色标记）。
    _consumer: PhantomData<C>,
    /// 借用期标记：缓冲区与被动半部共享的借用期 `'a`。
    _marker: PhantomData<&'a mut [MaybeUninit<T>]>,
}

impl<'a, P, C, B, T> CircularBuff<'a, P, C, B, T>
where
    P: TrProducer<T>,
    C: TrConsumer<T>,
    B: BorrowMut<[MaybeUninit<T>]>,
{
    /// 由 builder 构造（内部）。
    pub(super) fn new(core: RingCore<T>, buffer_: B) -> Self {
        CircularBuff {
            core,
            buffer_,
            _producer: PhantomData,
            _consumer: PhantomData,
            _marker: PhantomData,
        }
    }

    // ------------------------------------------------------------------
    // 公开 API（集中在一处）
    // ------------------------------------------------------------------

    /// 环形缓冲容量。
    pub fn capacity(&self) -> usize {
        self.core.capacity()
    }

    /// 当前可读数据量。
    pub fn data_size(&self) -> usize {
        self.core.data_size()
    }

    /// 当前可写空间。
    pub fn free_size(&self) -> usize {
        self.core.free_size()
    }

    /// 写端（生产端）是否已关闭。
    pub fn is_tx_closed(&self) -> bool {
        self.core.is_tx_closed()
    }

    /// 读端（消费端）是否已关闭。
    pub fn is_rx_closed(&self) -> bool {
        self.core.is_rx_closed()
    }

    /// 关闭写端：不再写入，触发消费端 hook（`ProducerClose`）。
    pub fn close_tx(&self) {
        self.core.close_tx();
    }

    /// 关闭读端：不再读取，触发生产端 hook（`ConsumerClose`）。
    pub fn close_rx(&self) {
        self.core.close_rx();
    }

    /// 拆分出写半部与读半部。
    ///
    /// 仅当**两端都是被动模式**时返回两个可用的半部；任一端为主动模式时返回
    /// `None`——主动端由设备驱动、不对外暴露（对应 `RingBuffer` 中「半部
    /// 不存在」的情形，此处以 `None` 表达，主动端的占位类型见
    /// [`DeviceProducer`] / [`DeviceConsumer`]）。
    pub fn try_split_io(&mut self) -> Option<(ProducerHalf<'_, T>, ConsumerHalf<'_, T>)> {
        if self.core.producer_is_passive() && self.core.consumer_is_passive() {
            Some((ProducerHalf::new(&self.core), ConsumerHalf::new(&self.core)))
        } else {
            None
        }
    }
}

// ---------------------------------------------------------------------------
// TrObserver / TrProducer / TrConsumer（CircularBuff 本身即两端的访问入口）
// ---------------------------------------------------------------------------

impl<P, C, B, T> TrObserver for CircularBuff<'_, P, C, B, T>
where
    P: TrProducer<T>,
    C: TrConsumer<T>,
    B: BorrowMut<[MaybeUninit<T>]>,
{
    fn capacity(&self) -> usize {
        CircularBuff::capacity(self)
    }

    fn ready(&self) -> usize {
        CircularBuff::data_size(self)
    }

    fn is_remote_end_closing(&self) -> bool {
        self.core.is_tx_closed() || self.core.is_rx_closed()
    }
}

impl<P, C, B, T> TrProducer<T> for CircularBuff<'_, P, C, B, T>
where
    P: TrProducer<T>,
    C: TrConsumer<T>,
    B: BorrowMut<[MaybeUninit<T>]>,
{
    type Buff<'f> = ProducerHalf<'f, T> where Self: 'f;
    type Err = EndError;

    /// 生产端为被动模式时返回可用的写半部；为主动模式时永远返回错误
    /// （该端由输入设备驱动，不对外暴露）。
    fn try_as_buff(&mut self) -> Result<Self::Buff<'_>, Self::Err> {
        if self.core.producer_is_passive() {
            Ok(ProducerHalf::new(&self.core))
        } else {
            Err(EndError)
        }
    }
}

impl<P, C, B, T> TrConsumer<T> for CircularBuff<'_, P, C, B, T>
where
    P: TrProducer<T>,
    C: TrConsumer<T>,
    B: BorrowMut<[MaybeUninit<T>]>,
{
    type Buff<'f> = ConsumerHalf<'f, T> where Self: 'f;
    type Err = EndError;

    /// 消费端为被动模式时返回可用的读半部；为主动模式时永远返回错误。
    fn try_as_buff(&mut self) -> Result<Self::Buff<'_>, Self::Err> {
        if self.core.consumer_is_passive() {
            Ok(ConsumerHalf::new(&self.core))
        } else {
            Err(EndError)
        }
    }
}

// ---------------------------------------------------------------------------
// 端类型标记的 trait 实现（满足 CircularBuff 的类型约束）
// ---------------------------------------------------------------------------

macro_rules! impl_observer_marker {
    ($ty:ident $(, $lt:lifetime)?) => {
        impl<$($lt,)? T> TrObserver for $ty<T> {
            fn capacity(&self) -> usize {
                0
            }
            fn ready(&self) -> usize {
                0
            }
            fn is_remote_end_closing(&self) -> bool {
                false
            }
        }
    };
}

impl_observer_marker!(PassiveProducer);
impl_observer_marker!(PassiveConsumer);

impl<TyInput, T> TrObserver for DeviceProducer<TyInput, T>
where
    TyInput: TrInput<T>,
{
    fn capacity(&self) -> usize {
        0
    }
    fn ready(&self) -> usize {
        0
    }
    fn is_remote_end_closing(&self) -> bool {
        false
    }
}

impl<TyOutput, T> TrObserver for DeviceConsumer<TyOutput, T>
where
    TyOutput: TrOutput<T>,
{
    fn capacity(&self) -> usize {
        0
    }
    fn ready(&self) -> usize {
        0
    }
    fn is_remote_end_closing(&self) -> bool {
        false
    }
}

impl<T> TrProducer<T> for PassiveProducer<T> {
    type Buff<'f> = ProducerHalf<'f, T> where Self: 'f;
    type Err = EndError;

    /// 角色标记自身没有环形核心：不直接提供半部。真实的被动写半部请通过
    /// `CircularBuff`（`TrProducer::try_as_buff` / `try_split_io`）获得。
    fn try_as_buff(&mut self) -> Result<Self::Buff<'_>, Self::Err> {
        todo!("角色标记不直接提供半部：请通过 CircularBuff 访问")
    }
}

impl<T> TrConsumer<T> for PassiveConsumer<T> {
    type Buff<'f> = ConsumerHalf<'f, T> where Self: 'f;
    type Err = EndError;

    fn try_as_buff(&mut self) -> Result<Self::Buff<'_>, Self::Err> {
        todo!("角色标记不直接提供半部：请通过 CircularBuff 访问")
    }
}

impl<TyInput, T> TrProducer<T> for DeviceProducer<TyInput, T>
where
    TyInput: TrInput<T>,
{
    type Buff<'f> = ProducerHalf<'f, T> where Self: 'f;
    type Err = EndError;

    /// 主动生产端不对外暴露：永远返回错误。
    fn try_as_buff(&mut self) -> Result<Self::Buff<'_>, Self::Err> {
        Err(EndError)
    }
}

impl<TyOutput, T> TrConsumer<T> for DeviceConsumer<TyOutput, T>
where
    TyOutput: TrOutput<T>,
{
    type Buff<'f> = ConsumerHalf<'f, T> where Self: 'f;
    type Err = EndError;

    /// 主动消费端不对外暴露：永远返回错误。
    fn try_as_buff(&mut self) -> Result<Self::Buff<'_>, Self::Err> {
        Err(EndError)
    }
}

impl<TyInput, T> TrDeviceProducer<T> for DeviceProducer<TyInput, T>
where
    TyInput: TrInput<T>,
{
    type InputDevice = TyInput;
}

impl<TyOutput, T> TrDeviceConsumer<T> for DeviceConsumer<TyOutput, T>
where
    TyOutput: TrOutput<T>,
{
    type OutputDevice = TyOutput;
}
