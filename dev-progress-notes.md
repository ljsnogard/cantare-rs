# Agents.md —— 代理工作守则（务必先读）

本文件记录本仓库（`crates/buffex` 及其周边）中代理必须遵守的硬性约束。
其中两条规则来自一次真实的事故，任何后续改动都不得违反。

---

## 规则一：不准更改 Producer / Consumer 的 SPSC 约束

**不要把 `Producer` / `Consumer` 上 `pub` 公开的方法随便从 `&mut self` 改成 `&self`。**

原因（来自事故教训）：

- `&mut self` 是 SPSC（单生产者 / 单消费者）在**编译期**的强制手段：同一时刻只能有一个
  调用者持有半部的可变借用，两线程并发 `try_read` / `try_write` 根本写不出来。
- 一旦改成 `&self`，借用检查器不再限制并发——两个线程各持 `&Consumer` 并发
  `try_read` 会同时进入 `create_read_segm` → `buffer_view_mut`（`UnsafeCell` 的
  可变视图），造成**缓冲数据竞争（UB）**。SPSC 就从编译期保证降级成了「调用者
  自觉遵守的约定」。
- 事故经过：为了让「泵循环」与被动端操作并发，把 `close` / `read_async` /
  `write_async` / `try_read` / `try_write` 全部改成了 `&self`——这是错误的，已撤销。

正确做法：

- 需要并发访问核心时，用 `Shared`（`CoreRef`）的**克隆**持有独立引用，而不是
  放宽半部的借用签名；
- 半部的公开方法保持 `&mut self`（编译期强制单生产者 / 单消费者）。

## 规则二：不准为 Producer / Consumer 增加公开方法

**不得为 `Producer` / `Consumer` 增加任何新的公开（`pub`）方法，除非先经过
讨论并获得批准。**

原因（来自事故教训）：

- 事故中未经讨论就给 `Consumer` 加了 `input_pump()`、给 `Producer` 加了
  `output_pump()`（把泵循环 future 暴露为半部方法）。这是**未获批准的公开 API
  扩张**，改变了使用面，且方向本身也有问题（泵的驱动 / 归属未经设计确认）。
- 公开 API 一旦添加就难以收回，涉及 `abs_buff` / `buffex_iroh` 等下游使用方。

正确做法：

- 新的公开方法 / 类型 / trait 约束，先**提出设计并等待讨论与批准**，再实现；
- 未经批准，宁可在内部模块（私有）完成实现与验证。

---

## 通用提醒

- 改动前先 `git status` / `git log` 确认当前基线，避免覆盖他人已提交的工作；
- 涉及公开 API、trait 签名、`no_std` / `Send + Sync` 边界的改动尤其谨慎；
- 本文件的规则优先于「看起来更顺手的实现」——有疑问先问，不要自作主张改设计。

---

# CircularBuff 主动端实现设计（当前任务）

> 本文是在前述规则基础上的内部设计记录，不是对外 API 文档。
> 目标是补齐 `CircCore` 中主动端（`DevProducer` / `DevConsumer`）的搬运与唤醒机制。

## 一、目标

在 `crates/buffex` 的 `circular_buff` 中，当核心的 `producer_` / `consumer_`
是主动端类型时，需要完成：

- 主动生产端 `DevProducer` 根据消费端行为（读取提交、关闭）异步拉取 `TrInput`
  数据填充缓冲；
- 主动消费端 `DevConsumer` 根据生产端行为（写入提交、关闭）异步把缓冲数据
  推送到 `TrOutput`；
- 全主动模式下，`Pipeline` 通过两端自己的 `WakeSlot` 相互唤醒，而不是依赖
  spawn 或 `Waker::noop` 忙等。

## 二、硬性约束

1. **no_std，不 spawn**：不能在库内部创建后台异步任务。
2. **禁止 `Waker::noop` 循环忙等**：不能占住 executor 空转。
3. **等待必须使用真实 waker**：
   - 设备未就绪 → 由设备 future 自己的 waker 唤醒；
   - 缓冲背压（满 / 空）→ 由主动端注册到自身 `WakeSlot` 的 waker 唤醒；
   - 触发来源是 `fire_producer` / `fire_consumer`。
4. **不新增公开 API**：所有新泵、park 逻辑都放在内部模块（`core_` /
   `abs_comp_` / `circ_buff_`），不给 `Producer` / `Consumer` 增加公开方法。
5. **SPSC 借用纪律不变**：半部公开方法保持 `&mut self`；核心内部通过
   `UnsafeCell` 和明确的调用路径访问端类型。

## 三、`build_async` / `init_async` 的语义

- `build_async` 是异步的，可以等待，也可以通过 `CancelToken` 取消。
- `init_async` 的目标不是“必须填满整个缓冲才让出 executor”。
- `init_async` 的目标是：
  1. 尽可能把当前可用的设备数据搬进缓冲；
  2. 遇到设备 `Pending`、缓冲满、无进展或取消时，不再继续死等；
  3. 建立主动端“等待后续事件”的内部状态：设置对应的 STNDBY armed 位，
     并让主动端 `WakeSlot` 成为后续泵 park / 被 `fire_*` 唤醒的挂载点。

因此：

- `DevProducer::init_async` 填到“当前能填的”之后，若还有空位但设备未就绪，
  可以返回；之后由消费端读取或 `Pipeline` 继续驱动。
- `DevConsumer::init_async` 同理，尽量排空当前已有数据，然后进入可被
  `fire_consumer` 唤醒的就绪状态。

## 四、总体机制

沿用现有 `fire_*` 模型：

```text
写入提交 advance_write
  → fire_consumer
  → DevConsumer::check 感兴趣
  → signal DevConsumer 自己的 WakeSlot
  → 输出泵被唤醒 → 向 TrOutput 写入

读取提交 advance_read
  → fire_producer
  → DevProducer::check 感兴趣
  → signal DevProducer 自己的 WakeSlot
  → 输入泵被唤醒 → 从 TrInput 读取
```

核心新增两层泵：

1. **同步非阻塞泵**：用于 `try_*`、`advance_*`、`close_tx` 等同步路径。
   每次只对设备 future 做一次 poll；`Pending` 立即放弃，不循环等待。
2. **异步泵**：写在 `DevProducer` / `DevConsumer` 中，用于
   `read_async` / `write_async` / `Pipeline`。
   `await` 设备 future，设备 `Pending` 时由设备自己的 waker 唤醒。
3. **背压 park**：用于 `Pipeline` 等连续泵场景；缓冲满 / 空时，主动端把
   waker 注册到自己的 `WakeSlot`，等待对端提交路径 `fire_*` 唤醒。

## 五、类型改动

### `abs_comp_.rs`（内部 trait）

- 给 `TrProducer` / `TrConsumer` 增加两个内部协作能力：

  ```rust
  // 主动端返回自己的唤醒槽位；被动端返回 None
  fn wakeslot(&self) -> Option<&WakeSlot> {
      None
  }

  // 由“对端”调用的异步泵：
  // 被动端返回 Ready(0)，主动端实现真正的设备搬运。
  type PumpAsync<'f, C>: TrMayCancel<'f, MayCancelOutput = usize>
  where
      Self: 'f,
      C: 'f + TrCircBuffCore<Data = Self::Data>;

  fn pump_async<'f, C>(&'f mut self, core: &'f C) -> Self::PumpAsync<'f, C>
  where
      C: TrCircBuffCore<Data = Self::Data>;
  ```

- `DevProducer` / `DevConsumer` 实现真正的异步泵；
- `BufProducer` / `BufConsumer` 实现为 `Ready(0)` no-op。
- 这是私有模块内部协作，不构成公开 API 扩张。

### `circ_buff_.rs`

- `DevProducer` / `DevConsumer`：
  - 实现 `wakeslot()`；
  - 实现 `pump_async()`，内部包含“从 `TrInput` 拉取”或“向 `TrOutput` 推送”
    的异步循环；
  - 被 `fire_*` 唤醒后，继续执行的逻辑写在这里，而不是写在被动端等待函数里。
- `BufProducer` / `BufConsumer`：
  - `pump_async()` 直接返回 0，表示被动端不主动搬运。
- 调整 `DevProducerInitAsync` / `DevConsumerInitAsync`：
  - 先尽量搬运；
  - 遇到 `Pending` / 满 / 空 / 无进展时停止；
  - 最后建立 armed / wake-slot 就绪状态。
- 不新增公开方法。

### `core_.rs`

新增私有方法：

```rust
fn pump_input_sync(&self) -> usize;
fn pump_output_sync(&self) -> usize;

async fn park_producer(&self);
async fn park_consumer(&self);
```

修改：

- `try_write_`：`Stuffed` 且未终止时，先 `pump_output_sync()` 再重试；
- `try_read_`：`Drained` 且未终止时，先 `pump_input_sync()` 再重试；
- `advance_write`：`fire_consumer` 后调用 `pump_output_sync()`；
- `advance_read`：`fire_producer` 后调用 `pump_input_sync()`；
- `close_tx`：`fire_consumer` 后调用 `pump_output_sync()` 排空残留；
- `core_passive_read_async_`：统一调用 `producer.pump_async(core)`，
  不判断对端是否主动；
- `core_passive_write_async_`：统一调用 `consumer.pump_async(core)`，
  不判断对端是否主动。

### `spsc_.rs`

- `Pipeline` 不再在“双零进展”时无条件 `future::pending()`。
- 根据阻塞点调用 `park_producer()` / `park_consumer()`：
  - 缓冲满 → 输入泵等消费端读，park 到 `DevProducer`；
  - 缓冲空 → 输出泵等生产端写，park 到 `DevConsumer`；
  - 确实只是设备无进展且无背压时，才保持挂起等待设备外部唤醒。

## 六、伪代码

### 同步非阻塞泵

```rust
fn pump_input_sync(&self) -> usize {
    // 仅主动生产 + 被动消费；双主动交给 Pipeline，避免同步递归
    if producer.is_passive() || !consumer.is_passive() {
        return 0;
    }

    let mut total = 0;
    loop {
        let free = self.free_size();
        if free == 0 || self.is_tx_closed() || self.is_rx_closed() {
            break;
        }

        let mut segm = self.create_write_segm(self.wp(), free);
        let producer = unsafe { &mut *self.producer_.get() };

        // 单次 poll；Pending 立即 break，不忙等
        let mut fut = pin!(
            producer
                .react_async(&mut segm)
                .may_cancel_with(NonCancellableToken::shared_mut())
        );
        let outcome = poll_once(fut.as_mut());

        let moved = segm.capacity() - segm.least_count();
        drop(segm); // 提交，触发 fire_consumer

        total += moved;
        if moved == 0 || !matches!(outcome, Poll::Ready(ReceiverReact::Reacted)) {
            break;
        }
    }

    total
}
```

`pump_output_sync` 对称。

### 主动端 park（核心机制）

```rust
async fn park_producer(&self) {
    let Some(slot) = producer.wakeslot() else {
        return;
    };

    let mut waiter = Waiter::new();
    self.set_tx_standby();

    let mut guard = ActiveParkGuard {
        slot,
        waiter: &mut waiter,
        core: self,
        standby: TX_STNDBY,
        registered: false,
    };

    let _ = core::future::poll_fn(|cx| {
        guard.waiter.waker = Some(cx.waker().clone());
        guard.slot.register(guard.waiter);
        guard.registered = true;

        // 注册后重查，避免丢唤醒
        if self.try_write_at(&Demand::at_least(1)).is_ok()
            || self.is_tx_closed()
            || self.is_rx_closed()
        {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
}
```

`park_consumer` 对称。

### 异步泵：写在 `DevProducer` / `DevConsumer` 代码块里

被动端等待函数**不判断对端是主动还是被动**，只统一调用 `pump_async`。

```rust
// DevProducer 内部实现（circ_buff_.rs）
async fn pump_async<C>(&mut self, core: &C) -> usize
where
    C: TrCircBuffCore<Data = T>,
{
    let mut total = 0;

    // try_write_init() 返回 None 表示当前没有可写空间 / 已关闭
    while let Some(mut segm) = core.try_write_init() {
        let r = self
            .react_async(&mut segm)
            .may_cancel_with(NonCancellableToken::shared_mut())
            .await;

        let moved = segm.capacity() - segm.least_count();
        drop(segm); // 提交，触发 fire_consumer

        total += moved;
        if moved == 0 || r == ReceiverReact::Continue {
            break;
        }
    }

    total
}
```

`DevConsumer::pump_async` 对称，使用 `core.try_read_init()` 并调用
`react_async` 把数据写入 `TrOutput`。

被动端实现为 no-op：

```rust
// BufProducer 内部实现
fn pump_async<C>(&mut self, _core: &C) -> Ready<usize> {
    Ready(0)
}
```

因此 `core_passive_read_async_` / `core_passive_write_async_` 中只需要：

```rust
// core_passive_read_async_ 内部
let moved = producer.pump_async(core).may_cancel_with(cancel).await;
if moved > 0 {
    continue; // 主动端已经补入数据，重新尝试读取
}
// 否则进入普通被动 park
```

```rust
// core_passive_write_async_ 内部
let moved = consumer.pump_async(core).may_cancel_with(cancel).await;
if moved > 0 {
    continue; // 主动端已经排出空间，重新尝试写入
}
// 否则进入普通被动 park
```

### Pipeline 背压等待

```rust
loop {
    let in_moved = core.pipe_input_once().await;
    let out_moved = core.pipe_output_once().await;

    if in_moved == 0 && out_moved == 0 {
        if core.is_tx_closed() && core.data_size() == 0 {
            return None;
        }

        if core.free_size() == 0 {
            // 输入泵被“满”挡住：等消费端读，fire_producer 唤醒
            core.park_producer().await;
        } else if core.data_size() == 0 {
            // 输出泵被“空”挡住：等生产端写，fire_consumer 唤醒
            core.park_consumer().await;
        } else {
            // 设备无进展且无背压，只能等设备外部唤醒
            core::future::pending::<()>().await;
        }
    }
}
```

## 七、与旧实现的关系

- 旧的 `block_on` / `Waker::noop` 自旋泵已废弃，不再恢复。
- 旧的“build 必须非阻塞”测试已删除；`build_async` 可以异步等待，但
  `init_async` 不应以“必须填满整个环”为目标。
- 保留“fire 只唤醒、不搬运”的事件模型；搬运由主动泵完成。
- 不改变 `Producer` / `Consumer` 公开 API，不改变 SPSC `&mut self` 约束。

## 八、待实现顺序建议

1. 在 `abs_comp_` 增加内部 `wakeslot()` 与 `PumpAsync` / `pump_async()`；
2. 在 `circ_buff_` 为四个端类型实现 `pump_async()`：被动端 no-op，
   `DevProducer` / `DevConsumer` 实现真正的异步搬运；
3. 在 `core_` 实现同步泵与 `park_producer` / `park_consumer`；
4. 接入 `try_*` / `advance_*` / `close_tx`；
5. 接入 `core_passive_*_async_`：统一调用 `pump_async`，不判断主动/被动；
6. 改造 `Pipeline` 使用 `park_producer` / `park_consumer`；
7. 运行 `circular_buff` 相关测试，确认无自旋、无 spawn、无公开 API 变化。

---

# Unsafe 代码审计与调用上下文

> 本节列出 `circular_buff` 非测试代码中的所有 `unsafe` 位置，并说明：
> 谁会调用它、为什么在当前 SPSC / 生命周期 / armed 协议下是 sound 的。

## 1. `builder.rs`：构建期自引用初始化

```rust
unsafe {
    let init_producer = core.producer_ptr().as_mut().init_async(&core)...
    let init_consumer = core.consumer_ptr().as_mut().init_async(&core)...
}
```

- **调用者**：`ReadyBuilder::build_async()` 内部的 `essential_build_async_`。
- **作用**：同时取得核心内 `P` / `C` 的可变引用和整个 `core` 的共享引用，调用 `init_async`。
- **为什么 sound**：
  - 此时 `core` 还是栈上局部变量，尚未放入 `Shared`，没有第二个引用；
  - `init_async` 只在构建期调用一次；
  - `producer_ptr()` / `consumer_ptr()` 指向 `UnsafeCell` 内始终存在的字段，指针非空；
  - 构建期不存在用户半部、活段或泵并发访问。

## 2. `core_.rs`：端类型 `UnsafeCell` 访问

### 2.1 `on_buf_producer_drop_` / `on_buf_consumer_drop_`

```rust
let p = unsafe { &*self.producer_.get() };
let c = unsafe { &*self.consumer_.get() };
```

- **调用者**：`Producer::drop` / `Consumer::drop`。
- **为什么 sound**：被动半部 drop 时，只有该半部仍引用核心；SPSC 保证没有并发写者 / 读者，也没有泵正在使用该端。

### 2.2 `producer_ptr()` / `consumer_ptr()`

```rust
unsafe { NonNull::new_unchecked(self.producer_.get()) }
```

- **调用者**：builder 的 `init_async` 阶段。
- **为什么 sound**：`UnsafeCell` 字段始终存在，裸指针不可能为 null。

### 2.3 同步泵 `pump_input_sync` / `pump_output_sync`

```rust
let producer = unsafe { &*self.producer_.get() };
let consumer = unsafe { &*self.consumer_.get() };
let producer = unsafe { &mut *self.producer_.get() };
let consumer = unsafe { &mut *self.consumer_.get() };
```

- **调用者**：
  - `try_read_` / `try_write_` 的重试路径；
  - `advance_read` / `advance_write` / `close_tx`。
- **为什么 sound**：
  - 这些调用点都处于“没有同侧活段 / 没有其它泵正在运行”的路径上；
  - 同步泵有门控：只在一端主动、一端被动时工作，避免双主动递归；
  - `&mut P` / `&mut C` 的取得与用户半部借用满足 SPSC。

### 2.4 背压 park `park_producer` / `park_consumer`

```rust
let Some(slot) = unsafe { &*self.producer_.get() }.wakeslot() else { ... };
let Some(slot) = unsafe { &*self.consumer_.get() }.wakeslot() else { ... };
```

- **调用者**：`Pipeline` 在缓冲满 / 空时调用。
- **为什么 sound**：
  - 只读取端类型的 `WakeSlot` 共享引用；
  - Pipeline 是双主动唯一驱动者，没有其他泵并发；
  - `ActiveParkGuard` 保证取消 / drop 时注销。

### 2.5 读段构建 `create_read_segm`

```rust
let a = unsafe { slice::from_raw_parts(base.add(start), first) };
let b = unsafe { slice::from_raw_parts(base, take - first) };
```

- **调用者**：`try_read_`、`pump_output_sync`、`pipe_output_once`、`TrCircBuffCore::try_read_init`。
- **为什么 sound**：
  - `start` / `take` 来自 `try_read_at`，范围已校验；
  - 这些位置是已初始化数据；
  - SPSC 保证不与活写段重叠。

### 2.6 `fire_producer` / `fire_consumer`

```rust
let producer = unsafe { &*self.producer_.get() };
let consumer = unsafe { &*self.consumer_.get() };
```

- **调用者**：`advance_read` / `advance_write` / `close_tx` / `close_rx`。
- **为什么 sound**：
  - fire 只取共享引用调用 `check`；
  - 调用点不在任何 `&mut P` / `&mut C` 借用期间；
  - STNDBY armed 协议保证 demand / slot 访问安全。

### 2.7 `buffer_view_mut`

```rust
unsafe { slice::from_raw_parts_mut(base, self.capacity()) }
```

- **调用者**：`create_write_segm`、`pipe_input_once`、`pipe_output_once`。
- **为什么 sound**：
  - 调用者保证当前没有同区域活段 / 其它泵；
  - 缓冲内存由 `Owned` 持有，生命周期与核心一致。

### 2.8 `pipe_input_once` / `pipe_output_once`

```rust
let producer = unsafe { &mut *self.producer_.get() };
let consumer = unsafe { &mut *self.consumer_.get() };
```

- **调用者**：`Pipeline::pipe_async` 的泵循环。
- **为什么 sound**：
  - Pipeline 是双主动模式唯一驱动者；
  - 每个时刻只有一个方向在借用端类型；
  - 活段在 `await` 前创建、在下次借用前 drop。

### 2.9 `WakeSlot::signal`

```rust
let w = unsafe { &*p };
```

- **调用者**：`check` 内部 / 被动端等待唤醒路径。
- **为什么 sound**：
  - `p` 来自 `swap(null)`，即槽位中确实注册了一个 `Waiter`；
  - `Waiter` 的注册与注销由 `WaitGuard` / `ActiveParkGuard` 保证；
  - swap 先取出指针并清空槽位，再解引用，不会与 deregister 竞争。

### 2.10 `core_passive_read_async_` / `core_passive_write_async_`

```rust
let producer = unsafe { &mut *core.producer_.get() };
let consume = unsafe { &*core.consumer_.get() };
let consumer = unsafe { &mut *core.consumer_.get() };
let producer = unsafe { &*core.producer_.get() };
```

- **调用者**：`Consumer::read_async` / `Producer::write_async`。
- **为什么 sound**：
  - 同一时刻只有一个被动等待 future；
  - `pump_async` 借用对端时，本端被动半部没有活段；
  - 被动 park 只读取本端 demand / slot，不与其他泵重叠。

### 2.11 `unsafe impl Send / Sync for CircCore`

- **为什么 sound**：
  - 共享状态（状态字、WakeSlot）全部是原子；
  - 缓冲与端类型通过 `UnsafeCell` 访问，由 SPSC + 单泵路径保证不并发；
  - 端类型本身满足 `Send + Sync` 时才实现。

## 3. `circ_buff_.rs`：Demand 指针

### 3.1 `BuffObsv::try_set_demand`

```rust
let p = unsafe { core::ptr::NonNull::new_unchecked(p) };
```

- **调用者**：被动等待 future 的 `try_set_demand`。
- **为什么 sound**：`demand` 是活引用，指针非空；存储期间引用生命周期有效。

### 3.2 `BufProducer::check` / `BufConsumer::check`

```rust
let demand = unsafe { demand_ptr.as_ref() };
```

- **调用者**：`fire_producer` / `fire_consumer`。
- **为什么 sound**：STNDBY armed 协议保证：
  - 先写 demand，再置 armed；
  - fire 侧只在 armed 时读 demand；
  - 完成 / drop 时先清 armed 再清 demand。

## 4. `reclaim_.rs`

### 4.1 `unsafe impl Send / Sync for WriterReclaim / ReaderReclaim`

- **为什么 sound**：内部只保存 `&TyCore`，且 `TyCore: Send + Sync + TrCircBuffCore`。

### 4.2 `unsafe fn move_items_from_buff` / `move_items_to_buff`

- **调用者**：测试辅助、abs_buff 管道、主动/被动段搬运。
- **为什么 sound**：这两个方法是 abs_buff 对应 unsafe 能力的透传；
  调用方必须保证源 / 目标不重叠，并正确处理 `MaybeUninit<T>` 的初始化状态。
