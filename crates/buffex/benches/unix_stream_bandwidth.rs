//! Bandwidth benchmark: a compio unix socket wrapped in the abs_buff buffer
//! traits (`TrBuffWrite` / `TrBuffRead`), with the ring buffer providing the
//! segments and a single `writev` / `readv` syscall per handoff.
//!
//! Run with:
//!
//! ```sh
//! cargo bench -p buffex --bench unix_stream_bandwidth
//! ```
//!
//! Reports the one-way client→server bandwidth (client send / server recv)
//! and the echo bandwidth (client send + recv through a server loopback).

#[cfg(not(all(feature = "compio", unix)))]
fn main() {
    eprintln!("this benchmark requires the `compio` feature on unix");
}

#[cfg(all(feature = "compio", unix))]
fn main() {
    use std::boxed::Box;
    use std::format;
    use std::time::Instant;
    use std::vec;
    use std::vec::Vec;

    use abs_buff::Demand;
    use abs_buff::x_deps::anylr::SomeOf;
    use abs_buff::{TrBuffRead, TrBuffTryRead, TrBuffTryWrite, TrBuffWrite};
    use abs_cancel::{NonCancellableToken, TrMayCancel};

    use buffex::unix_stream::BufferedUnixStream;

    const RING_CAP: usize = 64 * 1024;
    const TOTAL: usize = 64 * 1024 * 1024; // 64 MiB per direction
    const CHUNK: usize = 16 * 1024;

    fn sock_path() -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!("buffex-bench-{}.sock", std::process::id()));
        let _ = std::fs::remove_file(&path);
        path
    }

    /// Push `total` bytes through `TrBuffWrite::write_async`, filling each
    /// borrowed segment completely.
    async fn send_all(buffered: &mut BufferedUnixStream, total: usize) {
        let mut off = 0usize;
        while off < total {
            let demand = Demand::less_than(core::cmp::min(CHUNK, total - off));
            let x = buffered
                .write_async(&demand)
                .may_cancel_with(NonCancellableToken::shared_mut())
                .await;
            let Some(mut segm) = x.pick_left() else {
                panic!("write_async failed");
            };
            let len = segm.least_count();
            let mut seen = 0usize;
            for dst in segm.iter_slices_mut() {
                for (i, slot) in dst.iter_mut().enumerate() {
                    slot.write((off + seen + i) as u8);
                }
                seen += dst.len();
            }
            drop(segm);
            off += len;
        }
    }

    /// Receive `total` bytes through `TrBuffRead::read_async`, consuming each
    /// segment.
    async fn recv_all(buffered: &mut BufferedUnixStream, total: usize) {
        let mut off = 0usize;
        while off < total {
            let demand = Demand::less_than(core::cmp::min(CHUNK, total - off));
            let x = buffered
                .read_async(&demand)
                .may_cancel_with(NonCancellableToken::shared_mut())
                .await;
            let Some(segm) = x.pick_left() else {
                panic!("read_async failed (peer closed early?)");
            };
            let len = segm.least_count();
            for src in segm.iter_slices() {
                let _ = src;
            }
            drop(segm);
            off += len;
        }
    }

    let mi_bps = |elapsed: std::time::Duration, bytes: usize| -> f64 {
        bytes as f64 / elapsed.as_secs_f64() / (1024.0 * 1024.0)
    };

    compio::runtime::Runtime::new().unwrap().block_on(async {
        // ---- one-way: client sends, server receives ----
        let path = sock_path();
        let listener = compio::net::UnixListener::bind(&path).await.expect("bind");
        let accept = compio::runtime::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            stream
        });
        let stream = compio::net::UnixStream::connect(&path).await.expect("connect");
        let server_stream = accept.await.expect("accept task");

        let server = compio::runtime::spawn(async move {
            let mut buffered = BufferedUnixStream::new(server_stream, RING_CAP);
            let start = Instant::now();
            recv_all(&mut buffered, TOTAL).await;
            let elapsed = start.elapsed();
            buffered.shutdown().await;
            elapsed
        });

        let mut buffered = BufferedUnixStream::new(stream, RING_CAP);
        let start = Instant::now();
        send_all(&mut buffered, TOTAL).await;
        let send_elapsed = start.elapsed();
        buffered.shutdown().await;
        let recv_elapsed = server.await.expect("server task");

        println!(
            "one-way client→server: {} MiB in {:.3}s → send {:.1} MiB/s, recv {:.1} MiB/s",
            TOTAL / (1024 * 1024),
            send_elapsed.as_secs_f64(),
            mi_bps(send_elapsed, TOTAL),
            mi_bps(recv_elapsed, TOTAL),
        );

        // ---- echo: server loops the data back; the client interleaves
        // sends and receives through the *try* interfaces (nothing parks, so
        // the full-duplex backpressure cannot deadlock) ----
        let path = sock_path();
        let listener = compio::net::UnixListener::bind(&path).await.expect("bind");
        let accept = compio::runtime::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            stream
        });
        let stream = compio::net::UnixStream::connect(&path).await.expect("connect");
        let server_stream = accept.await.expect("accept task");

        let server = compio::runtime::spawn(async move {
            let mut buffered = BufferedUnixStream::new(server_stream, RING_CAP);
            let mut scratch = Vec::with_capacity(CHUNK);
            let mut off = 0usize;
            while off < TOTAL {
                let demand = Demand::less_than(CHUNK);
                let x = buffered
                    .read_async(&demand)
                    .may_cancel_with(NonCancellableToken::shared_mut())
                    .await;
                let Some(segm) = x.pick_left() else {
                    panic!("echo server read failed");
                };
                let len = segm.least_count();
                scratch.clear();
                for src in segm.iter_slices() {
                    scratch.extend_from_slice(src);
                }
                drop(segm);

                let wdemand = Demand::less_than(len);
                let wx = buffered
                    .write_async(&wdemand)
                    .may_cancel_with(NonCancellableToken::shared_mut())
                    .await;
                let Some(mut wsegm) = wx.pick_left() else {
                    panic!("echo server write failed");
                };
                let mut seen = 0usize;
                for dst in wsegm.iter_slices_mut() {
                    let copy = core::cmp::min(dst.len(), scratch.len() - seen);
                    for (i, slot) in dst[..copy].iter_mut().enumerate() {
                        slot.write(scratch[seen + i]);
                    }
                    seen += copy;
                }
                drop(wsegm);
                off += len;
            }
            buffered.shutdown().await;
        });

        let mut buffered = BufferedUnixStream::new(stream, RING_CAP);
        let mut sent = 0usize;
        let mut recvd = 0usize;
        let start = Instant::now();
        while sent < TOTAL || recvd < TOTAL {
            if sent < TOTAL {
                let demand = Demand::less_than(core::cmp::min(CHUNK, TOTAL - sent));
                let x = buffered.try_write(&demand);
                if let Some(mut segm) = x.pick_left() {
                    let len = segm.least_count();
                    let mut seen = 0usize;
                    for dst in segm.iter_slices_mut() {
                        for (i, slot) in dst.iter_mut().enumerate() {
                            slot.write((sent + seen + i) as u8);
                        }
                        seen += dst.len();
                    }
                    drop(segm);
                    sent += len;
                }
            }
            if recvd < TOTAL {
                let demand = Demand::less_than(core::cmp::min(CHUNK, TOTAL - recvd));
                let x = buffered.try_read(&demand);
                if let Some(segm) = x.pick_left() {
                    let len = segm.least_count();
                    for src in segm.iter_slices() {
                        let _ = src;
                    }
                    drop(segm);
                    recvd += len;
                }
            }
            if sent < TOTAL || recvd < TOTAL {
                futures_lite::future::yield_now().await;
            }
        }
        let echo_elapsed = start.elapsed();
        buffered.shutdown().await;
        server.await.expect("echo server task");

        println!(
            "echo: {} MiB sent + received in {:.3}s → {:.1} MiB/s round-trip",
            TOTAL / (1024 * 1024),
            echo_elapsed.as_secs_f64(),
            mi_bps(echo_elapsed, TOTAL * 2),
        );
    });
}
