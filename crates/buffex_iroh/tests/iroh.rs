//! Real iroh connection tests for the buffered stream adapters.

use abs_buff::{Demand, TrBuffRead, TrBuffWrite};
use buffex_iroh::{IrohReader, IrohWriter};
use iroh::{Endpoint, RelayMode, endpoint::presets};

const ALPN: &[u8] = b"buffex-iroh/test/1";
const PAYLOAD: &[u8] = b"hello from client through ring buffer";
const RESPONSE: &[u8] = b"hello from server through ring buffer";

async fn write_all(writer: &mut IrohWriter, data: &[u8]) {
    let mut off = 0usize;
    while off < data.len() {
        let Some(mut segm) = TrBuffWrite::write_async(
            writer,
            &Demand::less_than(data.len() - off),
        )
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
        let res =
            TrBuffRead::read_async(reader, &Demand::less_than(len - out.len()))
                .await;
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
    let server_task = tokio::spawn(async move {
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
        let mut reader = IrohReader::new(server_recv, 32);
        let got = read_exact(&mut reader, PAYLOAD.len()).await;
        assert_eq!(got, PAYLOAD);
        reader.shutdown().await;

        // Send the response through TrBuffWrite.
        let mut writer = IrohWriter::new(server_send, 32);
        write_all(&mut writer, RESPONSE).await;
        writer.shutdown().await;

        // Keep the connection alive until the client has finished reading the
        // response; closing the endpoint earlier can abort the stream before
        // the last bytes are delivered to the client.
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
        .expect("client should connect to server");
    let (client_send, client_recv) = conn
        .open_bi()
        .await
        .expect("client should open a bidirectional stream");

    // Send the payload through TrBuffWrite.
    let mut writer = IrohWriter::new(client_send, 32);
    write_all(&mut writer, PAYLOAD).await;
    writer.shutdown().await;

    // Receive the response through TrBuffRead.
    let mut reader = IrohReader::new(client_recv, 32);
    let got = read_exact(&mut reader, RESPONSE.len()).await;
    assert_eq!(got, RESPONSE);
    reader.shutdown().await;
    let _ = done_tx.send(());

    server_task.await.expect("server task should succeed");
    client.close().await;
}
