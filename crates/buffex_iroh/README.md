# buffex_iroh

Thin `TrBuffRead` / `TrBuffWrite` adapters over iroh QUIC streams.

iroh's `RecvStream` / `SendStream` do not expose their internal buffers as
abs_buff segments, so this crate places a `buffex::ring_buffer::RingBuffer`
between the user and the QUIC stream:

- `IrohReader` wraps a `RecvStream`; a background Tokio task reads from the
  stream into the ring, and the user consumes through `TrBuffRead` /
  `TrBuffTryRead`.
- `IrohWriter` wraps a `SendStream`; the user produces through `TrBuffWrite` /
  `TrBuffTryWrite`, and a background Tokio task drains the ring and writes to
  the stream.

Constructors spawn Tokio tasks and therefore must be called from a Tokio
runtime context.

## Example

```rust,ignore
use abs_buff::{Demand, TrBuffRead, TrBuffWrite};
use buffex_iroh::{IrohReader, IrohWriter};
use iroh::endpoint::{RecvStream, SendStream};

async fn copy_recv_to_send(mut recv: RecvStream, mut send: SendStream) {
    let mut reader = IrohReader::new(recv, 8192);
    let mut writer = IrohWriter::new(send, 8192);

    loop {
        let Some(mut rseg) = TrBuffRead::read_async(&mut reader, &Demand::less_than(4096)).await.pick_left() else {
            break;
        };
        // move data from rseg to writer ...
        drop(rseg);
    }
}
```
