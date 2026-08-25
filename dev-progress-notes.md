# buffex/circular_buff 重构 —— 设计分析与类型处置清单

> 本文基于工作树（`164d8fc` refactor WIP 之后）的现状写成，先于任何代码修改。
> 目标：①说清设计意图；②定位「组件循环引用」的根源；③给出本次重构中
> 「可删除 / 需重设计」的处置清单及理由。
>
> 与类型 doc comment（`crates/buffex/src/circular_buff/*.rs`）配套阅读。

## ✅ buffex_iroh 验证（circular_buff 无 spawn 搬运 iroh 数据，已通过）

用 circular_buff 重构 `crates/buffex_iroh` 并通过真实 iroh QUIC 连接测试
（`buffered_streams_roundtrip_over_real_iroh_connection` 与
`try_interface_moves_data_without_spawn`，均多线程 tokio 运行时，库源码
**零 `tokio::spawn`**，外部 `TrBuffTryRead` / `TrBuffTryWrite` 接口不变）：

- **写侧**（`IrohWriter`）：`pipe_into_output(StreamOutput)`，`SendStream` 作主动
  消费端；设备 `write_async` **同步阻塞写**（noop waker 自旋到完成）——段 drop
  提交即泵出，保证送达、无死锁；`shutdown` 冲刷 + 取回流 `finish()`；
- **读侧**（`IrohReader`）：`pipe_from_input(StreamInput)`，`RecvStream` 作主动
  生产端；设备 `read_async` **try-once**（只 poll 一次，无数据返回 0）；
  `read_async` 是循环 future（尝试 + tokio 零延时 yield 等待）；
- **drive 对调用者透明**：半部操作（`try_read` / `try_write` / `try_read_shared`
  及等待 future 的 poll）在**对端为主动**时自动驱动一轮定向泵
  （`CircCore::drive_input` / `drive_output`），适配器与用户都不再手动调用；
- EOF/错误经共享状态合成 `Closing` / `take_error`。

**为什么「对端被动」时绝不能 drive（正确性而非优化）**：被动端意味着用户持有
活段；若在被动侧强行泵，泵会在同一侧构造段（`pump_input` 对可写区 / `pump_output`
对数据区），与用户活段指向同一缓冲内存 → **别名 UB**；即使无活段也会做无谓的
`advance(0)` → spurious 唤醒。故 drive 全部带 guard（`!producer_is_passive()` /
`!consumer_is_passive()`），且只驱动与操作相关的单侧泵（避免 `drive_all` 强制
另一侧被动泵撞活段）。

## ✅ check 机制已启用（事件分发 = check 裁决 + 待办泵收敛）

`TrProducer::check` / `TrConsumer::check` 从死代码变为事件分发的**兴趣裁决**：

- `fire_producer` / `fire_consumer`（提交路径）先 `check(event)`：**被动端按
  恢复的内置 demand（`BuffProducer` / `BuffConsumer` 的 `demand_min` 原子下限，
  `set_demand` 由等待 future 登记 / 清除）裁决——不足下限不唤醒（避免
  spurious wake），关闭事件例外（EOF 总是唤醒）**；主动端由设备裁决
  （`DeviceConsumer::check` 对 `ProducerClose` 也返回 `true`，保证关闭时排空
  残留数据）；
- 泵循环每轮也 `check(Available(...))`——设备可在轮间改变主意；
- `fire_*` 取 `&mut` 调 check 的安全性：`fire_consumer` 只由 `advance_write`
  触发，其调用点（用户写提交、`pump_input` 提交）不持有 `&mut C`；对称地
  `fire_producer` 触发时无人持有 `&mut P`——无活借用重叠；
- **被动端 demand 的同步 = STNDBY armed 协议（无原子，普通字段）**：
  `BuffProducer` / `BuffConsumer` 恢复内置 **`Option<Demand<usize>>`**（不是
  只有下限——`check` 以 `demand.min().unwrap_or(1)` 判兴趣，保留完整需求
  语义）。等待者 park 时先写 demand、再 CAS 置 `TX_STNDBY` / `RX_STNDBY`
  （armed）；fire 侧从状态字 **Acquire 读**到 armed 位才访问 demand（否则
  直接返回），与该 CAS 建立 happens-before，demand 必为当前等待者的值；完成 /
  drop 时先清位再清 demand，armed 期间 demand 恒有效。
  **被动端三态（已写入 `circ_buff_` 端类型 doc）**：①无需求等待调用
  （demand=None、STNDBY=0，不接受 fire）→ ②有需求正在登记（demand 已写入、
  STNDBY 仍 0，不接受 fire；等待者注册后重查）→ ③等待中（STNDBY=1，接受
  fire）；park 迁移 ①→②→③，完成 / drop ③→①；
- **drive 机制保留**：`check → pump → 提交 → 对端 check → …` 的递归仍需
  「待办标志 + 单层 `drive()` 循环 + `PUMPING` 互斥」收敛；半部操作的
  `drive_input` / `drive_output`（读侧空缓冲首拉等）仍需要——check 只裁决
  「是否值得驱动」，不替代驱动本身。

**结论与边界（如实记录）**：验证成立——本地 QUIC 连接上数据正确搬运。固有
语义：①写路径**阻塞当前线程**直到网络写入完成（无任务模型的代价）；②读侧
只在用户操作时拉取（`try_read` 无数据立即 `Drained`，非阻塞；`read_async`
yield 等待）；③流控受限时的写自旋依赖多线程运行时驱动 ACK；④`try-once`
读依赖「数据已在 quinn 接收缓冲」这一前提，适合测试/请求-响应类场景，不适合
「后台持续灌入」类场景（那需要 spawn 或阻塞读）。

## ✅ 执行结果（重构已完成，buffex 恢复编译且 43 个测试全过）

按下文分析与后续讨论的拍板，重构已落地：

- **端契约去 `Buffer` GAT**：`TrProducer` / `TrConsumer` 的 `react_async` 泛化
  段参数（`fn react_async<'f, TySegm>(&mut self, segm: &mut TySegm)`），端类型
  不再携带任何指名核心的类型——类型级循环从根上消失；
- **段提交对接 CircCore**（实现 `TrCircBuffCore`），放弃 ReclCore 想法；
- **拥有型访问模型**：`build` 产出 `SpscPair = (Producer, Consumer)`，各持
  `Shared<CircCore<P, C, T, A>, A>`（`A = CoreAlloc` 默认，`TrMalloc` 抽象、
  不引入 `allocator_api`）；无 `CircularBuff` 聚合体；
- **删除**：`half_.rs`（借用型半部）、`segm_.rs`（旧段）、`hook_` / `abs_`
  （已在前序 WIP 删除）、`builder.rs` 与 `spsc_.rs` 的重复定义、`EndError`；
- **主动端半部**：存在但操作返回 `TxError::Unavailable` / `RxError::Unavailable`
  （替代旧 `try_as_buff` 报错）。

**与讨论的两处偏差**（有意的，均已写入代码文档）：

1. **WakeSlot 放回核心**（`CircCore::producer_wake_` / `consumer_wake_`），而非
   端类型内部。原因：等待 future 在 park 期间持有 `&WakeSlot`，若槽位是端类型
   （`UnsafeCell` 内）的字段，泵对端类型的 `&mut` 访问会与该长期 `&WakeSlot`
   构成别名 UB。放核心（普通字段）则等待方与提交方都经 `&self` 原子访问，无
   别名问题（旧设计即此形态，验证过正确）。
2. **未实现 STNDBY 端锁**：分析（写进 `core_` 模块文档）表明泵只运行在
   `drive()` 内，而 `drive()` 的 `pumping_.swap(true)` 本身就是跨线程互斥
   （并发线程 swap 得 `true` 直接返回、待办标志由最外层循环处理），端类型
   访问天然串行，无需再加锁。`TX_STNDBY` / `RX_STNDBY` 位保留未用，作为将来
   多泵线程支持的扩展点。fire 方「少量尝试后放弃」的语义由「事件被更新事件
   取代 + 等待者注册后重查（`Park`）」保证，符合讨论结论。

---

## 一、设计意图（一句话版）

`CircularBuff` 是在**构建期**固定两端「模式（被动 / 主动）与用途（数据从哪来、
到哪去）」的唤醒式环形缓冲：被动端由调用者驱动（提供 `TrBuffTryWrite` /
`TrBuffTryRead` 半部 + 异步等待），主动端由设备驱动（`TrInput` 灌入 /
`TrOutput` 搬出，hook 内**同步**轮询到完成、不 spawn）。两端共享同一套核心
（原子位置状态机 + 事件分发），全部语义由「另一端完成读写后触发对端 hook」
统一。完整表述见 `mod.rs` 模块文档。

本次重构的**增量目标**：把「hook」从旧模型的独立擦除对象（`hook_` 的
`ActiveInput<T>` = `*mut ()` + fn 指针，`ActiveOutput<T>` 同理）改为**端类型即
hook**——两端以具体类型存放进核心（`CircCore<P, C, T>` 的 `P` / `C`），
主动设备因此**无需类型擦除**（`TrInput` 带 GAT、不能直接 `dyn`，具体化是
唯一可行路径）。代价是核心泛型化，并由此引入下面的类型级循环。

---

## 二、循环引用的根源

### 2.1 类型级循环（本次重构新增，核心矛盾）

三角依赖（A → B 表示「A 的类型定义需要指名 B」）：

```text
CircCore<P, C, T>            （核心泛型于端类型，为了不擦除设备）
   │ 持有 P / C
   ▼
端类型 P: TrProducer / C: TrConsumer
   │ Buffer<'f> 关联类型（段，供 react_async 使用）
   ▼
ReclSliceMut/Ref<'f, T, Writer/ReaderReclaim<'f, CircCore<P, C, T>>>
   │ 段 drop 时要把位置提交回核心
   ▼
CircCore<P, C, T>            ←—— 回到起点
```

即：`P::Buffer<'f> = ReclSliceMut<'f, T, WriterReclaim<'f, CircCore<Self, C, T>>>`——
**端类型的关联类型指名「包含该端自身的核心类型」**。类型方程没有基例
（`CircCore` 的定义要求 `P: TrProducer`，验证 `P::Buffer` 又要求先有
`CircCore`），求解器必然溢出或无法收敛——这正是「泛型参数改来改去
都无法稳定」的直接原因。当前 `circ_buff_.rs` 中的实现（非 GAT 写法、
`C`/`P` 不在作用域）只是这个死结的半成品表达，把 GAT 语法写对也解不开。

旧设计为什么没有这个问题：核心是单参数 `RingCore<T>`，**不泛型于端类型**；
端标记（`PassiveProducer` 等）只是 `CircularBuff` 里的 `PhantomData`；设备以
`*mut ()` + fn 指针擦除进 hook；段只引用具体的 `RingCore<T>`。没有三角形，
自然没有环——代价正是重构想消除的类型擦除。

### 2.2 运行期循环（旧设计已解决，新模型必须保留）

hook → 泵 → 提交 → 对端 hook → … 的无界递归。旧设计用「待办标志
（`input_pending` / `output_pending`）+ 单层 `drive()` 循环 + 重入保护
（`pumping`）」收敛：hook 只置标志，最外层循环把全部待办泵执行完毕。
新模型的事件分发（`check` / `react_async`）同样必须沿用这套收敛，否则
`DeviceProducer` ↔ `DeviceConsumer` 会在提交路径上互相触发到栈溢出。

---

## 三、本次重构中可以删除的类型

| 类型 | 位置 | 为什么删除 |
|---|---|---|
| `segm_` 整模块：`PiecesMut` / `PiecesRef` / `WrSegm` / `RdSegm` / `CommitWrite` / `CommitRead` | `segm_.rs` | 与 `reclaim_` 的两段式段功能完全重复；`Commit*` 直接引用单参数 `CircCore<T>`，新核心 `CircCore<P, C, T>` 下无法编译。`reclaim_` 的 `ReclSliceMut` / `ReclSliceRef` + `WriterReclaim` / `ReaderReclaim` 是替代（提交器泛型化于 `TyCore: TrCircBuffCore`）。依赖方（`half_`、`tests_` 的 `fill_segm` / `take_segm`）迁移后即可删除。 |
| `hook_.rs` 全体：`ActiveInput` / `ActiveOutput` / `ProducerHook` / `ConsumerHook`（及 `block_on`） | 已在 WIP 中删除 | 旧「类型擦除设备 + hook enum」模型，被「端类型即 hook」取代；擦除正是本次重构要消除的。 |
| `abs_.rs` 全体：旧 `TrProducer` / `TrConsumer`（`try_as_buff` 模型）/ `TrDeviceProducer` / `TrDeviceConsumer` | 已在 WIP 中删除 | 被 `abs_comp` 的「事件 + 段视图」模型（`check` / `react_async`）取代；`try_as_buff` 的职责移交给半部。 |
| `EndError` | `error_.rs`（已删，`mod.rs` 仍在导出） | 「主动端不对外暴露」改由 `is_passive()`（+ 端自身实现/半部构造路径）表达，不需要专用错误类型；`mod.rs` 导出需同步清理。 |
| `builder.rs` 的 `Producer` / `Consumer` / `CoreRef` / `SpscPair` | `builder.rs` | 与 `spsc_.rs` 同名重复定义（`CoreRef` 甚至同名异义：`builder` 是 4 参、`spsc_` 是 4 参但结构不同，且 `spsc_.rs` 另有自己的 `CoreRef<P,C,T,A>`）。构建期不应产出两套半部，二选一。 |
| `core_.rs` 的旧泵 / hook 残留：`producer_hook` / `consumer_hook` 字段引用、`producer_wake_slot` / `consumer_wake_slot` 的旧 match 形态、`start` / `drive` / `pump_input` / `pump_output` 的旧实现 | `core_.rs` | 字段已随 `hook_` 删除，方法体仍引用（当前编译错误的主要来源）。须按「端即 hook」重写为：提交 → 对端 `check` → `react_async`；泵标志（`pumping` / `input_pending` / `output_pending`）以原子字段补回。 |
| `reclaim_.rs` 的 `cap_` 字段（`WriterReclaim` / `ReaderReclaim`） | `reclaim_.rs` | 复制自 `ring_buffer`，未使用，可随重构清理。 |

---

## 四、需要重新设计的类型

| 类型 | 位置 | 重新设计点 | 为什么 |
|---|---|---|---|
| `TrProducer` / `TrConsumer`（`Buffer<'f>` GAT） | `abs_comp.rs` | **循环根源所在**。三选一：<br>① `react_async` 改收裸 slice（`&mut [MaybeUninit<T>]` / `&[T]`），删掉 `Buffer<'f>` GAT，泵循环留在核心（旧 `ActiveInput::pump_into` 即此形态）——**推荐**；<br>② `react_async` 对段类型泛型化（`fn react_async<'f, TySegm: TrBuffSegmMut<'f, Self::Data>>(...)`）；<br>③ 拆两层核心：内部状态机不泛型于端类型，段只提交给它。 | 端类型**存放在核心内部**，其关联类型一旦指名核心类型（`CircCore<Self, C, T>`）就构成 §2.1 的无基例方程。原则：**存放在核心里的类型，其关联类型绝不能指名核心自身**。另外 `is_passive` 目前无实现（E0046）。 |
| `CircCore<P, C, T>` | `core_.rs` | ①按新模型重写事件分发（`fire_*` 已按 `check` 改写，泵与槽位未迁移）；②泵标志作为原子字段补回；③**决定 `WakeSlot` 的摆放**；④`Send`/`Sync` 论证随字段变化更新。 | 端字段已落地但其余部分仍是旧模型，方法体引用已删字段。 |
| `WakeSlot` 的归属 | `core_.rs` / `circ_buff_.rs` | 现设计把它放进端类型（`BuffProducer::wake_slot_` / `BuffConsumer::wake_slot_`，在 `UnsafeCell` 内）。等待方（半部）与提交方（核心 `&self`）都要访问它：半部拿 `&WakeSlot` 需要绕过 `UnsafeCell`（构造半部时一次性 unsafe 取出，或核心持锁时借出）。 | 槽位若在端内，半部构造路径需要 unsafe 支撑；若放回核心字段，半部可直接借用、无需 unsafe。建议后者，或前者文档化其 safety。 |
| `half_.rs` 全体：`ProducerHalf` / `ConsumerHalf` / `Park` / `WriteAsync` / `ReadAsync` / `WriteFuture` / `ReadFuture` | `half_.rs` | 全部引用单参数 `CircCore<T>` 与 `core.producer_wake_slot()`（旧模型），须改为新核心参数（`CircCore<P, C, T>` 或泛型 `TyCore: TrCircBuffCore`）。 | 与 `spsc_` 的拥有型半部是两种访问模型，须二选一或统一（见 §五）。 |
| `spsc_.rs` 的 `Producer` / `Consumer` | `spsc_.rs` | 4 个泛型参数（`P/C/T/A`）；`impl TrBuffTryWrite for Consumer`（读端实现写 trait）与 `try_read` 返回 `WrSegm` / `TxError` 是复制粘贴错误；方法多为 `todo!()`。 | 访问模型未定、实现未完成。 |
| `builder.rs` 构建链 | `builder.rs` | 新链是 `from_buffer → pipe_* → build`（返回 `SpscPair`）；`mod.rs` 文档与测试用的是 `with_capacity → producer_passive / consumer_passive / pipe_* → build`（返回 `CircularBuff<'a, P, C, B, T>`），且 `CircularBuffBuilder` / `ProducerSetBuilder` / `ReadyBuilder` 已被删。 | 公开构建 API 以哪套为准需拍板；「类型状态强制两端模式」的设计意图应保留。 |
| `CircularBuff<'a, P, C, B, T>` 本体 | `circ_buff_.rs`（当前缺失） | 新模型下还需要这个聚合体（核心 + 存储借用 + 端类型）吗？还是由 `spsc_` 的 `SpscPair`（堆上核心 + 拥有型半部）取代？ | `mod.rs` 文档与测试仍按前者书写；若选 `spsc_` 模型，`CircularBuff` 与 `BorrowMut` 存储的借用模型一并作废。 |
| `reclaim_.rs` | `reclaim_.rs` | 模块头注释仍写「RingBuffer 专用」（复制残留）；与 `ring_buffer::reclaim_` 是否共用实现待定。 | 方向正确（`TyCore: TrCircBuffCore` 解耦、段不指名核心），保留。 |

---

## 五、建议的破环方向（供下一轮实现）

核心约束不变：**设备以具体类型存放，不做类型擦除**。在此约束下推荐组合：

1. **端契约去段化**（§四第 1 行的选项 ①）：`TrProducer` / `TrConsumer` 只保留
   `Data`、`is_passive`、`check(event)`、`react_async(裸 slice 视图)`。设备实现
   直接调 `TrInput::read_async(slice)` / `TrOutput::write_async(slice)` 并
   `await`；**泵循环（借区、驱动 future、按返回值 `advance_write` / `advance_read`）
   留在核心**——这正是旧 `pump_input` / `pump_output` 的形态，仅把擦除设备换成
   具体端类型。循环消失，因为端类型不再携带任何指名核心的类型。
2. **段只服务于半部**（`reclaim_` + `half_` / `spsc_`）：两段式 `ReclSliceMut` /
   `ReclSliceRef` 保留，供**核心之外的**半部借出区域；其提交器泛型于
   `TyCore: TrCircBuffCore`。半部在核心之外，`ProducerHalf<'s, TyCore, T>` 指名
   核心类型不会构成循环（环只出现在「核心内部类型指名核心」时）。
3. **WakeSlot 放回核心字段**（或经锁一次性借出给半部），避免半部穿透
   `UnsafeCell`。
4. **访问模型与公开 API 二选一**：借用型（`CircularBuff` + 存储借用 + `half_`，
   测试与 mod.rs 文档的现状）或拥有型（`spsc_` 的 `Shared<CircCore, A>` +
   `Producer` / `Consumer`，builder 新链的现状）。前者 no_std 无 alloc 更纯粹，
   与 `mod.rs` 文档「借用而非拥有」一致，建议保留前者、删 `spsc_`（或反向，
   但需同步改写 mod.rs 文档与测试）。

---

## 六、待拍板问题（阻塞代码修改）

1. 端契约选 ① / ② / ③？（影响 `abs_comp` / `circ_buff_` / `core_`）
2. 访问模型选借用型（`CircularBuff` + `half_`）还是拥有型（`spsc_`）？
   （影响 `builder` 公开 API、`CircularBuff` 是否保留）
3. `WakeSlot` 放核心还是端内？（影响半部构造 safety）
4. `is_passive` 保留在端契约里，还是由半部构造路径隐式表达？

## 七、下一步

1. 先按 §五 定设计（回答 §六 四个问题）；
2. 然后才动代码：删 `segm_`、删重复半部、重写 `core_` 泵/槽位、迁移
   `half_`、重写 `builder`、清理 `mod.rs` 导出；
3. 最后补 `is_passive`、`DeviceConsumer::react_async`（当前 `todo!`）、
   `may_cancel_with`（长期项）。
