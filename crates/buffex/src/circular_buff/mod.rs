//! # `CircularBuff` —— 构造期定「模式与用途」的唤醒式环形缓冲器
//!
//! `CircularBuff` 是 [`crate::ring_buffer`] 之外、在同一 `crates/buffex` 下新增的
//! 一个环形缓冲器实现。它与旧的 `RingBuffer` 并行存在、互不干扰，避免破坏既有
//! 代码。
//!
//! 它与 `RingBuffer` 最大的不同在于：**在构建期间就决定好生产端与消费端各自的
//! 工作模式（主动 / 被动）与用途（数据从哪里来、到哪里去）**，之后通过统一的
//! **hook** 机制驱动两端联动，因此无需任何异步运行时（`buffex` 是 `no_std` 的，
//! 主动模式不允许 `spawn` 任何异步任务）。
//!
//! # 模式：被动 vs 主动
//!
//! 生产端与消费端各自独立地从两种模式中选择一种：
//!
//! ## 被动模式（passive）——调用者驱动
//!
//! * **被动生产**：对外提供 `TrBuffTryWrite` 接口（`abs_buff`），调用者自行决定
//!   什么时候把数据写入内部缓冲。
//! * **被动消费**：对外提供 `TrBuffTryRead` 接口（`abs_buff`），调用者异步等待
//!   「内部缓冲区就绪」的 future / parker，就绪后自行读取。
//!
//! 被动端的「等待」就是普通异步等待：等待方把自己注册进核心的 waker 槽位
//! （parker），进入 pending 状态；对端操作触发 hook 把它唤醒，等待方重新检查
//! 条件（必要时再次注册），循环直至满足。
//!
//! ## 主动模式（active）——设备驱动
//!
//! * **主动生产**：构建时接收一个 `TrInput`（输入设备）实例。构造完成后立即主动
//!   从该设备提取数据放入内部缓冲等待消费；一旦消费端读取、释放出可写空间，
//!   立即主动从设备获取新的输入数据。
//! * **主动消费**：构建时接收一个 `TrOutput`（输出设备）实例。构造完成后，任何
//!   数据一旦写入缓冲区，立即被搬运到该输出设备。
//!
//! 主动端的「搬运」是**同步**完成的：不 `spawn` 任何任务。在 hook 内部直接把设备
//! 的 `read_async` / `write_async` future 轮询（poll）到完成（用
//! `core::task::Waker::noop()` 自旋驱动，见下文「主动模式的同步驱动」）。
//!
//! # 统一机制：hook
//!
//! 两种模式共享**同一套核心实现**：一个环形状态机（rp / wp / 容量），外加
//! **两个 hook 槽位**。hook 的触发时机固定为「另一端完成数据读取或写入之后」：
//!
//! ```text
//! 生产端完成写入（wp 前进，产生可读数据）  →  触发【消费端 hook】
//!     消费端被动：唤醒等待可读数据的读者（pending future 的 waker / parker）
//!     消费端主动：把缓冲数据搬运到 TrOutput
//!
//! 消费端完成读取（rp 前进，释放可写空间）  →  触发【生产端 hook】
//!     生产端被动：唤醒等待可写空间的写者（pending future 的 waker / parker）
//!     生产端主动：从 TrInput 抽取新数据填充空位
//! ```
//!
//! ## 设计要点一：hook 对调用者透明
//!
//! 核心内部使用哪个 hook（被动=唤醒，主动=搬运）是构建期由 [`builder`]
//! 决定并隐藏的，**不通过 `CircularBuff` 的泛型参数
//! 暴露**，调用者也不参与构造。对四种模式组合，`CircularBuff` 都是同一个类型
//! `CircularBuff<'a, T>`（`T` 默认 `u8`），只有 `'a` 一个生命周期参数。
//!
//! ## 设计要点二：主动端不对外暴露（占位类型）
//!
//! 模式对调用者唯一可见的影响是**可访问性**：使用了主动模式的那一端由设备驱动，
//! 不可能再让外部调用者访问——例如主动消费端不会再提供任何有实际效果的
//! `TrBuffTryRead` 实现。因此 `CircularBuff` 的对外接口（类似
//! [`RingBuffer::try_split_io`](crate::ring_buffer::TrRingBuffer::try_split_io)
//! 的拆分）对主动端返回**占位类型**（[`TxPlaceholder`] / [`RxPlaceholder`]），
//! 而不是一个可用的半部：接口形状保持统一（永远返回两端），但主动端拿到的是
//! 「无实际效果」的占位。这对应了 `RingBuffer` 中「半部不存在」
//! （`try_split_io` 返回 `None`）的情形，只是用占位类型而非 `Option` 来表达。
//!
//! ## 设计要点三：存储统一为 `[MaybeUninit<T>]`
//!
//! 与 `RingBuffer` 的设计理念不同，这里**不引入** `RingStorage` 之类的存储抽象
//! 来兼容支持其他可能的缓冲区存储类型。环形缓冲器内部一律把缓冲区视为
//! `[MaybeUninit<T>]`（`T` 默认 `u8`），内存由调用者在构建时以
//! `&'a mut [MaybeUninit<T>]` 提供（`no_std`、无 alloc，借用而非拥有）。
//!
//! # 主动模式的同步驱动（不 spawn，无运行时依赖）
//!
//! 主动 hook 直接搬运数据，但 `TrInput::read_async` / `TrOutput::write_async`
//! 返回的是 future。在 `no_std`、无异步运行时的环境下，hook 采用**同步轮询
//! （poll-to-completion）**驱动单个 future：
//!
//! * 用 `core::task::Waker::noop()` 构造一个 `Context`（该 waker 永远不会被唤醒，
//!   因为此处不存在执行器）；
//! * 在一个循环里反复 `poll` 该 future，直到返回 `Poll::Ready`；
//! * 期间不 `yield`、不注册任何任务，纯粹占用当前线程——等价于一个微型的
//!   单 future `block_on`。
//!
//! 这样「立即搬运」就是字面意义上的立即：消费端完成读取的同一个调用栈上，
//! hook 就把 `TrInput` 的新数据读进缓冲；生产端完成写入的同一个调用栈上，
//! hook 就把缓冲数据写进 `TrOutput`。
//!
//! 注意：hook 内部的搬运（pump）对核心状态的推进同样会触发 hook（例如输入
//! pump 写入数据后，会触发消费端 hook）。为避免「输入 pump → 输出 pump →
//! 输入 pump → …」的无界递归，pump 采用**迭代收敛**形式：hook 只设置「有待办
//! pump」的标志位，由最外层的 `drive()` 以单层循环把待办 pump 全部执行完毕
//! （见下文「全主动流水线」）。
//!
//! # 全主动流水线（TrInput → 缓冲 → TrOutput）
//!
//! 两端都主动时，数据流自动贯通：
//!
//! 1. **构建期**执行第一轮 `drive()`：输入 pump 从 `TrInput` 读入数据 → 写入
//!    commit 触发消费端 hook → 输出 pump 把数据搬到 `TrOutput` → 读取 commit
//!    触发生产端 hook → 输入 pump 继续补位 → ……直到输入暂时无数据（返回 0 或
//!    非阻塞 EAGAIN）且输出已排空。
//! 2. 此后每当任何一端发生用户操作（若有被动端）或再次进入 `drive()`，流水线
//!    继续推进。
//!
//! 语义上这就是一个同步的 `copy`：数据只在当前线程上流动，栈深度不随数据量
//! 增长（收敛在单层 `drive()` 循环里）。若输入设备是阻塞式的，`drive()` 会阻塞
//! 在 `read_async` 上等待数据——这是无任务模型下「自动搬运」的固有语义。
//!
//! # 构建期决策（builder）
//!
//! 模式与用途的决策全部收敛在构建器里，见 [`builder`]：
//!
//! ```ignore
//! // 被动 × 被动：经典手动管道（两端都可访问）
//! let mut storage = [MaybeUninit::<u8>::uninit(); 4096];
//! let buff = CircularBuffBuilder::with_capacity(4096)
//!     .producer_passive()
//!     .consumer_passive()
//!     .build(&mut storage)?;
//!
//! // 主动生产 × 被动消费：从 TrInput 自动灌入，用户自行读取
//! let buff = CircularBuffBuilder::with_capacity(4096)
//!     .producer_active(&mut input)
//!     .consumer_passive()
//!     .build(&mut storage)?;
//! // 读取端可用（TrBuffTryRead）；生产端对外是 TxPlaceholder（无实际效果）。
//!
//! // 被动生产 × 主动消费：用户自行写入，写后自动搬运到 TrOutput
//! let buff = CircularBuffBuilder::with_capacity(4096)
//!     .producer_passive()
//!     .consumer_active(&mut output)
//!     .build(&mut storage)?;
//!
//! // 主动 × 主动：TrInput → 缓冲 → TrOutput 自动流水线（两端都不可直接访问）
//! let buff = CircularBuffBuilder::with_capacity(4096)
//!     .producer_active(&mut input)
//!     .consumer_active(&mut output)
//!     .build(&mut storage)?;
//! ```
//!
//! 构建器用类型状态（type-state）编码强制「两端模式必须在构建期决定」：
//! `CircularBuffBuilder` → `ProducerSetBuilder` → `ReadyBuilder`，漏设一端无法
//! 编译。构建完成后，模式与设备被**擦除**进 `CircularBuff` 的内部状态
//! （hook），`CircularBuff` 本身的类型对四种组合是统一的。
//!
//! # 核心实现（进行中）
//!
//! 环形核心状态机、hook 槽位的挂载与触发、主动 pump 的同步驱动目前只有类型
//! 骨架（见 [`CircularBuff`] 与 [`TxPlaceholder`] / [`RxPlaceholder`]），
//! `builder` 的 `build` 中留了 `todo!()`。待定的具体细节：
//!
//! * 容量校验（`2..=MAX_CAPACITY`，与 `ring_buffer` 的上限对齐）；
//! * 主动端设备的**类型擦除**机制（`TrInput` / `TrOutput` 带泛型关联类型，不能
//!   直接做 `dyn`；候选：手动 vtable 结构体（设备裸指针 + 单态化的泵函数指针）、
//!   或单线程专用的 `UnsafeCell` 持有，以及相应的 `Send` / `Sync` 取舍）；
//! * 被动模式的等待接口形态（future + parker，复用 `ring_buffer` 的
//!   `DemandSlot` 思路还是独立的槽位）；
//! * 对外拆分接口的形状（返回可用半部 / 占位类型，以及占位类型是否实现
//!   `TrBuffTryWrite` / `TrBuffTryRead` 的永远失败 stub）；
//! * pump 的标志位与 `drive()` 循环的具体布局；
//! * 关闭 / 错误传播（EOF、设备错误如何跨过 hook 通知对端）。
//!
//! # 与 ring_buffer 的关系
//!
//! `CircularBuff` 是新增模块，不改动 [`crate::ring_buffer`] 的任何既有 API 与
//! 实现。两者在「环形状态机」层面思路一致，但职责不同：`RingBuffer` 面向
//! 用户线程 + 运行时（内核）两侧的管道；`CircularBuff` 面向**构造期固定
//! 模式与设备**、由 hook 联动的唤醒式缓冲。

mod abs_;
mod circ_buff_;

pub mod builder;

pub use builder::{CircularBuffBuilder, ProducerSetBuilder, ReadyBuilder};
pub use circ_buff_::{CircularBuff, DeviceConsumer, DeviceProducer};
