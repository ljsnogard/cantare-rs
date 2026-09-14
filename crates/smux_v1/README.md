# smux_v1

流复用协议（stream multiplexing）第一版，采用 **sans-IO** 设计：协议逻辑只依赖
抽象的字节读写 trait，不绑定具体运行时或 socket。

当前处于实验阶段：连接层尚未实现；握手协商已给出完整规范并完成编解码与状态机
实现，扩展条目暂缓。

## 握手协议

握手协商的**规范正文**位于 Rust 源码的模块文档中，随代码一起维护：

| 文件 | 内容 |
| --- | --- |
| `src/handshake/mod.rs` | 协议规范：角色、帧格式、TLV 编码、协商流程、状态机、取消、错误分类、可行性约束 |
| `src/handshake/opts.rs` | 协商项类型与其线格式映射（`BasicOpts`、`NegotiationKey`、`NegotiationValType`、基础条目） |
| `src/handshake/codec.rs` | 私有的纯编解码模块：`build_frame_` / `parse_frame_` 与协商步骤函数（含单元测试） |
| `src/handshake/error.rs` | `HandshakeError` |
| `src/handshake/agent.rs` | `HandshakeAgent` / `HandshakeListener` / `HandshakeEndpoint` 与成帧读写 |

本地查看渲染后的文档：

```sh
cargo doc -p smux_v1 --no-deps --open
```

### 用法

握手对外只暴露两个对象，成功后交付协商规格与归还的 `Tx` / `Rx`：

```rust,ignore
// 发起方
let agent = HandshakeAgent::new(rx, tx, max_size);
let HandshakeEndpoint { opts, tx, rx } = agent
    .initiate_handshake(&invite, |opts| decide(opts))
    .may_cancel_with(cancel)
    .await?;

// 等待方
let listener = HandshakeListener::new(rx, tx, max_size);
let HandshakeEndpoint { opts, tx, rx } = listener
    .accept_handshake(&local, |opts| decide(opts))
    .may_cancel_with(cancel)
    .await?;
```

### 一页摘要

- 角色：**发起方**（先发 `INVITE`）与**等待方**（先收 `INVITE`）。
- 流程：`INVITE → ACCEPT → CONFIRM → CONFIRM`；任一步失败则 `REJECT` 并关闭
  连接。
- 帧格式（大端）：`magic(4B) + 条目区 + 校验尾`。**没有长度字段**，帧的结束
  由校验尾条目确定：头 `0x1C` + 2 字节 `CRC-16/XMODEM`，或头 `0x2C` + 4 字节
  `CRC-32/ISO-HDLC`（只允许这两种）。CRC 覆盖 `crc` 之前的全部字节（含校验尾
  头）。
- 条目区由 TLV 条目组成：`header = (val_type << 4) | key`，其后跟 1/2/4/8 字节
  大端数值。除基础键（`0x00..=0x03`）与校验尾（`0x0C`）外的键一律非法，遇到
  即拒绝。
- `max_size`（单帧总字节数上限）由发送/监听握手的调用方给出，推荐取
  `BasicOpts::max_packet_size`。
- 协商不是自动取小：等待方重复已提及的基础项、用本地值补全未提及项，然后交
  上层决定接受（发 `ACCEPT`）或拒绝（发 `REJECT`）。
- 每帧的校验算法自行声明，不要求同一连接内统一；数据帧校验不属于握手范围。

完整规则、示例与实现注意事项见 `src/handshake/mod.rs` 的模块文档；尚未定稿的
取舍记录在仓库根目录的 `dev-progress-notes.md`。
