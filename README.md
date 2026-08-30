# Cantare

> `Cantare` means "sing" in Italian.  
> This workspace sings a song of some understandings of asynchronous programming in Rust.  
> All crates are experimental.

## Workspace Overview

This repository is a Cargo workspace containing several small, mostly `no_std`-friendly crates.
They are grouped around a few themes:

- **Atomic / synchronization primitives**
- **Cancellation abstractions**
- **Buffered I/O abstractions and adapters**
- **Virtual file system abstractions**
- **Stream multiplexing experiments**
- **A proc-macro helper for async cancellation futures**

## Dependency Tree

The following tree shows the **path dependencies between workspace crates**.
External dependencies (e.g. `anylr`, `mm_ptr`, `abs_art-bridge`, `tokio`, `iroh`) are omitted for clarity.

```text
cantare (workspace)
│
├── foundation
│   ├── atomex                      # atomics extensions
│   ├── abs_iter                    # iteration abstractions
│   ├── abs_cancel                  # cancellation abstractions
│   └── gen_mcf_macro               # proc-macro for async cancel future generation
│
├── synchronization
│   ├── abs_sync
│   │   └── depends on: abs_cancel
│   └── atomic_sync
│       └── depends on: abs_sync, atomex
│
├── buffered-io
│   ├── abs_buff
│   │   └── depends on: abs_iter, abs_cancel, gen_mcf_macro
│   ├── abs_buff_tokio_adapt
│   │   └── depends on: abs_buff
│   ├── abs_buff_stdio_adapt
│   │   └── depends on: abs_buff
│   │       └── (dev-dependency) buffex
│   ├── buffex
│   │   └── depends on: abs_buff, atomic_sync
│   └── buffex_iroh
│       └── depends on: buffex, abs_buff_tokio_adapt
│
├── cancellation
│   ├── cancel_src
│   │   └── depends on: abs_cancel
│   └── gen_mcf_test
│       └── depends on: abs_cancel, gen_mcf_macro
│
├── virtual-fs
│   ├── abs_vfs
│   │   └── depends on: abs_cancel
│   └── mem_vfs
│       └── depends on: abs_vfs, gen_mcf_macro
│
└── mux / networking experiments
    ├── abs_smux
    │   └── depends on: abs_buff (+ external abs_str)
    └── smux_v1                      # SANS-IO stream multiplexing v1 (placeholder)
```

## Crate Descriptions

### Foundation

| Crate | Description |
|---|---|
| `atomex` | Atomics extensions; provides additional atomic types / helpers. |
| `abs_iter` | Abstractions around iteration. |
| `abs_cancel` | Abstractions for cancellation tokens and cancellable async operations. |
| `gen_mcf_macro` | Procedural macro that generates types and code for `Future` + `TrMayCancel` patterns. |

### Synchronization

| Crate | Description |
|---|---|
| `abs_sync` | Abstractions of synchronization primitives (mutex, rwlock, etc.). |
| `atomic_sync` | Atomic-based implementations of the `abs_sync` traits. |

### Buffered I/O

| Crate | Description |
|---|---|
| `abs_buff` | Abstraction of buffered I/O: `TrBuffRead`, `TrBuffWrite`, segments, pipelines. |
| `abs_buff_tokio_adapt` | Adapts tokio `AsyncRead` / `AsyncWrite` into `abs_buff` I/O traits. |
| `abs_buff_stdio_adapt` | Adapts `std::io::{Read, Write}` to `abs_buff` traits via `abs_art-bridge`. |
| `buffex` | Buffer extensions: ring buffer, circular buff, SPSC halves, active/passive pumps. |
| `buffex_iroh` | Iroh (QUIC) stream adapters built on `buffex` and `abs_buff_tokio_adapt`. |

### Cancellation

| Crate | Description |
|---|---|
| `cancel_src` | C#-style `CancellationTokenSource` / `CancellationToken` built on futures-channel. |
| `gen_mcf_test` | Tests / examples for `gen_mcf_macro` generated cancellation futures. |

### Virtual File System

| Crate | Description |
|---|---|
| `abs_vfs` | Abstraction of a virtual file system. |
| `mem_vfs` | In-memory implementation of `abs_vfs`. |

### Stream Multiplexing

| Crate | Description |
|---|---|
| `abs_smux` | Communication abstraction based on a reusable multi-channel connection. |
| `smux_v1` | SANS-IO stream multiplexing protocol, version 1 experiment. |

## Notes

- Most crates target `no_std` where practical.
- Some crates depend on external Git repositories such as `anylr` and `mm_ptr`.
- `abs_smux` references an external `abs_str` crate that is not part of this workspace.
- This is an experimental workspace; APIs may change without notice.
