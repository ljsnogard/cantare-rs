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
//! 决定并隐藏的：`CircCore<P, C, T, A>` 的泛型参数 `P` / `C` 是**端类型**
//! （决定可访问性），`T` 是元素类型，`A` 是分配器——hook 完全在内部，调用者
//! 不参与构造。
//!
//! ## 设计要点二：主动端不产出半部
//!
//! 模式对调用者唯一可见的影响是**可访问性**：使用了主动模式的那一端由设备驱动，
//! 调用者**根本拿不到它的半部**——`build` 只把**被动端**的半部交给调用者
//! （见 [`BuildOutcome`](builder::BuildOutcome)）：被动 × 被动 → 一对半部；
//! 主动生产 × 被动消费 → 仅消费端半部；被动生产 × 主动消费 → 仅生产端半部；
//! 主动 × 主动 → [`Pipeline`]（流水线 future，无半部）。这对应了
//! `RingBuffer` 中「半部不存在」（
//! [`RingBuffer::try_split_io`](crate::ring_buffer::TrRingBuffer::try_split_io)
//! 返回 `None`）的情形——主动端连「返回错误的空半部」都没有，从类型层面
//! 杜绝调用者持有主动端。
//!
//! 主动端的**端类型**（[`DeviceProducer`] / [`DeviceConsumer`]）携带设备的实际
//! 类型并随设备一同存放进核心——因此**无需任何类型擦除**（`TrInput` 带泛型
//! 关联类型、不能直接 `dyn`）即可在内部持有并驱动设备。
//!
//! ## 设计要点三：缓冲归核心所有（统一 `[MaybeUninit<T>]` 视图）
//!
//! 与 `RingBuffer` 的 `RingStorage` 抽象不同，这里不引入存储抽象层：缓冲由
//! 核心**拥有**（[`mm_ptr::Owned`]，分配器 `A` 默认 `CoreAlloc`），内部一律以
//! `[MaybeUninit<T>]` 视图操作（`T` 默认 `u8`）。`no_std` 下 `alloc` 是 stable
//! crate，`TrMalloc` 抽象用于避免 `allocator_api` nightly 特性。
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
//! 注意：两端全主动时 `build` 返回 [`Pipeline`]（一个 **Future**，见
//! [`Pipeline`]）——交给异步运行时 `spawn` 后，**只要它存活，数据就持续由
//! 两端设备驱动流动**（泵循环 await 设备的 `read_async` / `write_async`，
//! 设备就绪即流动、阻塞即挂起等待），直到一端出错 / 关闭、或调用者用
//! [`PipelineDisconnect`] 请求断开。若不想持有 `Pipeline` future，请让至少
//! 一端保持被动：被动端的每次读写都会自动驱动对端的主动泵。
//!
//! # 构建期决策（builder）
//!
//! 模式与用途的决策全部收敛在构建器里，见 [`builder`]。**两端可以任意顺序
//! 设置**——既可以从生产端开始，也可以从消费端开始，还可以一步同时设置两端，
//! 或两端都不设置（默认双端被动）：
//!
//! ```ignore
//! // 双端被动（默认，不 pipe 任何设备）：经典手动管道（两端都可访问）
//! let (mut tx, mut rx) = CircularBuffBuilder::with_capacity(4096)
//!     .build()?;
//!
//! // 显式双端被动：`producer_passive` / `consumer_passive` 可任意换序
//! let (mut tx, mut rx) = CircularBuffBuilder::with_capacity(4096)
//!     .producer_passive()
//!     .consumer_passive()
//!     .build()?;
//!
//! // 主动生产 × 被动消费：从 TrInput 自动灌入，用户自行读取
//! // （主动生产端不产出半部——只拿到消费端）
//! let mut rx = CircularBuffBuilder::with_capacity(4096)
//!     .pipe_from_input(input)      // 生产端先行
//!     .consumer_passive()
//!     .build()?;
//!
//! // 被动生产 × 主动消费：用户自行写入，写后自动搬运到 TrOutput
//! // （消费端先行、生产端后设——顺序与上例对调；主动消费端不产出半部）
//! let mut tx = CircularBuffBuilder::with_capacity(4096)
//!     .pipe_into_output(output)    // 消费端先行
//!     .producer_passive()          // 生产端后设
//!     .build()?;
//!
//! // 主动 × 主动：TrInput → 缓冲 → TrOutput 流水线——`build` 返回 [`Pipeline`]
//! // （Future）：交给运行时 spawn 后持续由设备驱动流动，断开经
//! // `pipeline.disconnect_handle().request()`
//! let pipeline = CircularBuffBuilder::with_capacity(4096)
//!     .pipe_into_output(output)    // 消费端先行
//!     .pipe_from_input(input)      // 生产端后设
//!     .build()?;
//!
//! // 一步同时设置两端（等价于上例）
//! let pipeline = CircularBuffBuilder::with_capacity(4096)
//!     .pipe_between(input, output)
//!     .build()?;
//! ```
//!
//! 构建器用类型状态（type-state）编码强制「两端模式必须在构建期决定」：
//! `CircularBuffBuilder`（仅容量）→ 任一「单端已定」的中态
//! （[`ProducerSetBuilder`] 生产端已定 / [`ConsumerSetBuilder`] 消费端已定）→
//! [`ReadyBuilder`]（两端已定）→ `build`，漏设一端无法编译。构建完成后，
//! 端类型（`P` / `C`）被确定，hook 在内部挂载；`build` 的返回类型由两端模式
//! 决定（**主动端不产出半部**，见上文「设计要点二」）。
//!
//! 注意：示例中的 `input` / `output` 是实现了 `TrInput` / `TrOutput` 的设备
//! （move 进缓冲）。示例为示意而保持 `ignore`，完整可运行的用法见 `tests_`
//! 模块。
//!
//! # 实现现状（重构已完成）
//!
//! 按「端类型即 hook」的目标重构完成：hook 从独立的擦除对象（旧 `hook_` 的
//! `ActiveInput` / `ActiveOutput`）改为**端类型**——两端以具体类型存放进核心
//! （`CircCore<P, C, T, A>` 的 `P` / `C`），设备因此**无需类型擦除**；段提交
//! 经 `TrCircBuffCore` 窄接口解耦（`reclaim_`）。
//!
//! 模块划分：
//!
//! * `builder`——类型状态构建链（`CircularBuffBuilder → ProducerSetBuilder /
//!   ConsumerSetBuilder → ReadyBuilder`，两端可任意换序，也可 `pipe_between`
//!   一步同时设置或直接 `build` 双端被动），`build` 在堆上装配核心并产出
//!   [`SpscPair`]（拥有型访问模型，无「缓冲聚合体」）；
//! * `spsc_`——公共半部 [`Producer`] /
//!   [`Consumer`]（`Shared<CircCore>` + 异步等待 future）；
//! * `abs_comp_`——**内部**端契约模块（私有，不对外暴露）：`check` /
//!   `react_async` 事件模型（`react_async` 泛化段参数、端类型不携带段类型，
//!   从而解开类型级循环）与 `TrCircBuffCore`；
//!   其中的 trait 仅供核心与端类型内部协作使用，调用者不应实现或调用；
//! * `circ_buff_`——四个端类型（被动 `BuffProducer` / `BuffConsumer`，主动
//!   `DeviceProducer` / `DeviceConsumer` 携带设备）；
//! * `core_`——`CircCore<P, C, T, A>`（自有缓冲 + 原子状态机 + 事件分发 +
//!   同步泵，`pumping` 标志保证泵互斥，无需端锁；**内部**模块，不对外暴露）；
//! * `reclaim_`——两段式段（`ReclSliceMut` / `ReclSliceRef`）+ 泛型提交器。
//!
//! 旧模型文件（`half_` 借用型半部、`segm_` 旧段、`hook_` 擦除 hook、`abs_`
//! 旧 trait）已删除。
//!
//! 仍待定 / 未完成（与重构正交的长期项）：
//!
//! * **设备错误传播**：当前泵把设备错误视为「本轮无数据」，错误如何跨过 hook
//!   通知对端（例如让被动端感知设备失败）待定；
//! * **被动端取消**：`TrMayCancel` 的 `may_cancel_with` 目前忽略取消 token；
//! * **`T ≠ u8` 与主动模式**：设备元素类型与缓冲元素类型一致（`TrInput<T>`），
//!   泛型上自洽；`T` 非平凡类型时的实践（drop 语义、`Send`/`Sync` 边界）待验证；
//! * **发送/共享边界**：核心按 SPSC + 单泵线程约定实现 `Send + Sync`（见
//!   `core_` 的安全说明），多线程流水测试待补；
//! * **`ring_buffer` 去留**：`reclaim_` 已从 `ring_buffer` 复制为自有实现，
//!   将来删除 `ring_buffer` 前需先迁移 `buffex_iroh` 的使用方。
//!
//! # 与 ring_buffer 的关系
//!
//! `CircularBuff` 不改动 [`crate::ring_buffer`] 的任何既有 API 与实现。两者在
//! 「环形状态机」层面思路一致，但职责不同：`RingBuffer` 面向用户线程 + 运行时
//! （内核）两侧的管道；`CircularBuff` 面向**构造期固定模式与设备**、由 hook
//! 联动的唤醒式缓冲。规划上 `ring_buffer` 将来会被 `CircularBuff` 取代。

mod abs_comp_;
mod circ_buff_;
mod core_;
mod error_;
pub mod reclaim_;
mod spsc_;

pub mod builder;

pub use builder::{
    BuilderError, CircularBuffBuilder, ConsumerSetBuilder, ProducerSetBuilder,
    ReadyBuilder,
};
pub use circ_buff_::{
    BuffConsumer, BuffProducer, DeviceConsumer, DeviceProducer,
};
pub use error_::{RxError, TxError};
pub use mm_ptr::x_deps::abs_mm::mem_alloc::CoreAlloc;
pub use reclaim_::{ReclSliceMut, ReclSliceRef};
pub use spsc_::{Consumer, Producer, Pipeline, SpscPair};

#[cfg(test)]
mod tests_;
