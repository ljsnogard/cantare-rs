//! Real iroh connection tests for the buffered stream adapters.

use abs_buff_tokio_adapt::x_deps::abs_buff::{
    Demand, TrBuffRead, TrBuffTryRead, TrBuffTryWrite, TrBuffWrite,
};
use buffex_iroh::{IrohReader, IrohWriter};
use iroh::{Endpoint, RelayMode, endpoint::presets};

const ALPN: &[u8] = b"buffex-iroh/test/1";
const PAYLOAD: &[u8] = b"hello from client through ring buffer";
const RESPONSE: &[u8] = b"hello from server through ring buffer";

async fn write_all(writer: &mut IrohWriter, data: &[u8]) {
    let mut off = 0usize;
    while off < data.len() {
        let demand = Demand::less_than(data.len() - off);
        let Some(mut segm) = TrBuffWrite::write_async(writer, &demand)
        .await
        .pick_left() else {
            panic!("writer returned an error");
        };

        let n = segm.least_count();
        let mut staging: Vec<std::mem::MaybeUninit<u8>> = data[off..off + n]
            .iter()
            .map(|&b| std::mem::MaybeUninit::new(b))
            .collect();
        // SAFETY: moving plain `u8` bytes into the ring segment is a bitwise
        // copy; the staging buffer owns nothing that needs dropping.
        unsafe {
            segm.move_items_from_buff(&mut staging);
        }
        off += n;
        drop(segm);
    }
}

async fn read_exact(reader: &mut IrohReader, len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        let demand = Demand::less_than(len - out.len());
        let res = TrBuffRead::read_async(reader, &demand).await;
        let right = res.as_ref().pick_right().map(|err| format!("{err:?}"));
        let Some(mut segm) = res.pick_left() else {
            panic!(
                "reader returned an error: {:?}; already read {} bytes",
                right,
                out.len(),
            );
        };

        let n = segm.least_count();
        let mut staging: Vec<std::mem::MaybeUninit<u8>> = Vec::with_capacity(n);
        staging.resize_with(n, std::mem::MaybeUninit::uninit);
        // SAFETY: the ring segment contains plain `u8` bytes; moving them into
        // the staging buffer is a bitwise copy.
        unsafe {
            segm.move_items_to_buff(&mut staging);
        }
        out.extend(
            staging.into_iter().map(|m| unsafe { m.assume_init_read() }),
        );
        drop(segm);
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn buffered_streams_roundtrip_over_real_iroh_connection() {
    let server = Endpoint::builder(presets::N0)
        .relay_mode(RelayMode::Disabled)
        .alpns(vec![ALPN.to_vec()])
        .bind()
        .await
        .expect("bind server");
    let server_addr = server.addr();

    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    let server_future = async move {
        let incoming = server
            .accept()
            .await
            .expect("server should receive an incoming connection");
        let conn = incoming.await.expect("incoming connection handshake");
        let (server_send, server_recv) = conn
            .accept_bi()
            .await
            .expect("server should accept the bidirectional stream");

        // Receive the client payload through TrBuffRead.
        let mut reader = IrohReader::try_new(server_recv, 32).unwrap();
        let got = read_exact(&mut reader, PAYLOAD.len()).await;
        assert_eq!(got, PAYLOAD);
        reader.shutdown().await;

        // Send the response through TrBuffWrite.
        let mut writer = IrohWriter::try_new(server_send, 32).unwrap();
        write_all(&mut writer, RESPONSE).await;
        writer.shutdown().await;

        // Keep the connection alive until the client has finished reading the
        // response; closing the endpoint earlier can abort the stream before
        // the last bytes are delivered to the client.
        let _ = done_rx.await;
        server.close().await;
    };

    let client_future = async move {
        let client = Endpoint::builder(presets::N0)
            .relay_mode(RelayMode::Disabled)
            .bind()
            .await
            .expect("bind client");
        let conn = client
            .connect(server_addr, ALPN)
            .await
            .expect("client should connect to server");
        let (client_send, client_recv) = conn
            .open_bi()
            .await
            .expect("client should open a bidirectional stream");

        // Send the payload through TrBuffWrite.
        let mut writer = IrohWriter::try_new(client_send, 32).unwrap();
        write_all(&mut writer, PAYLOAD).await;
        writer.shutdown().await;

        // Receive the response through TrBuffRead.
        let mut reader = IrohReader::try_new(client_recv, 32).unwrap();
        let got = read_exact(&mut reader, RESPONSE.len()).await;
        assert_eq!(got, RESPONSE);
        reader.shutdown().await;
        let _ = done_tx.send(());
        client.close().await;
    };

    tokio::join!(server_future, client_future);
}


/// 直接经同步接口 `TrBuffTryWrite` / `TrBuffTryRead` 搬运较大数据
/// （多段、多次泵），全程不 spawn——验证无后台任务模型下 try 接口的正确性。
#[tokio::test(flavor = "multi_thread")]
async fn try_interface_moves_data_without_spawn() {
    let payload: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
    let server_payload = payload.clone();

    let server = Endpoint::builder(presets::N0)
        .relay_mode(RelayMode::Disabled)
        .alpns(vec![ALPN.to_vec()])
        .bind()
        .await
        .expect("bind server");
    let server_addr = server.addr();

    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    let server_task = tokio::spawn(async move {
        let payload = server_payload;
        let incoming = server.accept().await.expect("incoming connection");
        let conn = incoming.await.expect("handshake");
        let (server_send, server_recv) =
            conn.accept_bi().await.expect("accept bidirectional stream");

        // 读侧：try_read 循环（内部每次先 drive 拉网络，非阻塞）。
        let mut reader = IrohReader::try_new(server_recv, 32).unwrap();
        let mut got = Vec::new();
        while got.len() < payload.len() {
            if let Some(mut segm) = TrBuffTryRead::try_read(
                &mut reader,
                &Demand::less_than(payload.len() - got.len()),
            )
            .pick_left()
            {
                let n = segm.least_count();
                let mut staging: Vec<std::mem::MaybeUninit<u8>> =
                    Vec::with_capacity(n);
                staging.resize_with(n, std::mem::MaybeUninit::uninit);
                // SAFETY: u8 位拷贝。
                unsafe {
                    segm.move_items_to_buff(&mut staging);
                }
                got.extend(
                    staging
                        .into_iter()
                        .map(|m| unsafe { m.assume_init_read() }),
                );
                drop(segm);
            } else {
                // 暂无数据：让运行时处理网络。
                tokio::task::yield_now().await;
            }
        }
        assert_eq!(got, payload, "server 收到的数据应与客户端一致");
        reader.shutdown().await;

        // 写侧：try_write 循环（段 drop 时泵同步阻塞写）。
        let mut writer = IrohWriter::try_new(server_send, 32).unwrap();
        let mut off = 0usize;
        while off < payload.len() {
            if let Some(mut segm) = TrBuffTryWrite::try_write(
                &mut writer,
                &Demand::less_than(payload.len() - off),
            )
            .pick_left()
            {
                let n = segm.least_count();
                let mut staging: Vec<std::mem::MaybeUninit<u8>> = payload
                    [off..off + n]
                    .iter()
                    .map(|&b| std::mem::MaybeUninit::new(b))
                    .collect();
                // SAFETY: u8 位拷贝。
                unsafe {
                    segm.move_items_from_buff(&mut staging);
                }
                off += n;
                drop(segm);
            } else {
                tokio::task::yield_now().await;
            }
        }
        writer.shutdown().await;

        let _ = done_rx.await;
        server.close().await;
    });

    let client = Endpoint::builder(presets::N0)
        .relay_mode(RelayMode::Disabled)
        .bind()
        .await
        .expect("bind client");
    let conn = client
        .connect(server_addr, ALPN)
        .await
        .expect("client connects");
    let (client_send, client_recv) =
        conn.open_bi().await.expect("open bidirectional stream");

    // 客户端：先写（try_write 循环），再读（try_read 循环）。
    let mut writer = IrohWriter::try_new(client_send, 32).unwrap();
    let mut off = 0usize;
    while off < payload.len() {
        if let Some(mut segm) = TrBuffTryWrite::try_write(
            &mut writer,
            &Demand::less_than(payload.len() - off),
        )
        .pick_left()
        {
            let n = segm.least_count();
            let mut staging: Vec<std::mem::MaybeUninit<u8>> = payload
                [off..off + n]
                .iter()
                .map(|&b| std::mem::MaybeUninit::new(b))
                .collect();
            // SAFETY: u8 位拷贝。
            unsafe {
                segm.move_items_from_buff(&mut staging);
            }
            off += n;
            drop(segm);
        } else {
            tokio::task::yield_now().await;
        }
    }
    writer.shutdown().await;

    let mut reader = IrohReader::try_new(client_recv, 32).unwrap();
    let mut got = Vec::new();
    while got.len() < payload.len() {
        if let Some(mut segm) = TrBuffTryRead::try_read(
            &mut reader,
            &Demand::less_than(payload.len() - got.len()),
        )
        .pick_left()
        {
            let n = segm.least_count();
            let mut staging: Vec<std::mem::MaybeUninit<u8>> =
                Vec::with_capacity(n);
            staging.resize_with(n, std::mem::MaybeUninit::uninit);
            // SAFETY: u8 位拷贝。
            unsafe {
                segm.move_items_to_buff(&mut staging);
            }
            got.extend(
                staging.into_iter().map(|m| unsafe { m.assume_init_read() }),
            );
            drop(segm);
        } else {
            tokio::task::yield_now().await;
        }
    }
    assert_eq!(got, payload, "client 收到的数据应与服务器一致");
    reader.shutdown().await;

    let _ = done_tx.send(());
    server_task.await.expect("server task should succeed");
    client.close().await;
}
