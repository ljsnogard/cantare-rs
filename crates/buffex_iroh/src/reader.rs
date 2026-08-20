//! The read-side adapter: [`IrohReader`].

use std::{
    mem::MaybeUninit,
    sync::{Arc, Mutex},
    vec,
    vec::Vec,
};

use abs_buff::{Demand, TrBuffRead, TrBuffTryRead};
use anylr::SomeOf;
use buffex::ring_buffer::RingBuffer;
use iroh::endpoint::{ReadError, RecvStream};
use tokio::task::JoinHandle;

use super::common::{SharedRx, SharedTx, new_shared_ring};

/// Read half of a buffered iroh stream.
///
/// Implements [`TrBuffRead`] / [`TrBuffTryRead`] by delegating to the internal
/// [`RingRx`].
pub struct IrohReader {
    rx: SharedRx,
    error: Arc<Mutex<Option<ReadError>>>,
    _task: JoinHandle<()>,
}

impl IrohReader {
    /// Wrap a QUIC receive stream with a ring buffer of `cap` bytes.
    ///
    /// Spawns a background task that keeps the ring filled from `stream`.
    pub fn new(stream: RecvStream, cap: usize) -> Self {
        let ring = new_shared_ring(cap);
        let (tx, rx) = RingBuffer::try_split_shared(
            ring,
            Arc::strong_count,
            Arc::weak_count,
        )
        .expect("fresh ring must split into a single rx half");
        // The fill task owns the tx half; it must not implicitly close the
        // ring when the task exits. EOF/closure is signalled explicitly via
        // `RingTx::close_rx`.
        let tx = tx.with_auto_close(false);

        let error = Arc::new(Mutex::new(None::<ReadError>));
        let task = tokio::spawn(fill_task(stream, tx, error.clone()));

        Self {
            rx,
            error,
            _task: task,
        }
    }

    /// Report the last background read error, if any.
    pub fn take_error(&self) -> Option<ReadError> {
        self.error.lock().ok().and_then(|mut guard| guard.take())
    }

    /// Close the read side and wait for the background fill task to exit.
    ///
    /// If the task is currently blocked waiting for network data, this may
    /// wait until that read completes or the endpoint is closed.
    pub async fn shutdown(mut self) {
        self.rx.close();
        let _ = (&mut self._task).await;
    }
}

impl Drop for IrohReader {
    fn drop(&mut self) {
        self.rx.close();
    }
}

impl TrBuffRead<u8> for IrohReader {
    type ReadAsync<'f>
        = <SharedRx as TrBuffRead<u8>>::ReadAsync<'f>
    where
        Self: 'f;
    type SegmRef<'a>
        = <SharedRx as TrBuffRead<u8>>::SegmRef<'a>
    where
        Self: 'a;
    type Err = <SharedRx as TrBuffRead<u8>>::Err;

    fn is_drained_closing(&self) -> bool {
        self.rx.is_drained_closing()
    }

    fn read_async<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> Self::ReadAsync<'f> {
        <SharedRx as TrBuffRead<u8>>::read_async(&mut self.rx, demand)
    }
}

impl TrBuffTryRead<u8> for IrohReader {
    fn try_read<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> SomeOf<Self::SegmRef<'f>, Self::Err> {
        <SharedRx as TrBuffTryRead<u8>>::try_read(&mut self.rx, demand)
    }
}

/// Read from the QUIC `RecvStream` into the write side of a ring.
async fn fill_task(
    mut stream: RecvStream,
    mut tx: SharedTx,
    error: Arc<Mutex<Option<ReadError>>>,
) {
    loop {
        if tx.ring().is_rx_closed() {
            return;
        }

        let Some(mut segm) =
            tx.write_at_most_async(usize::MAX).await.pick_left()
        else {
            // The write side has been closed, so there is nothing left to fill.
            return;
        };

        let cap = segm.least_count();
        let mut buf = vec![0u8; cap];
        match stream.read(&mut buf).await {
            Ok(Some(0)) => {
                // No progress; avoid a busy loop and wait for the next event.
                continue;
            }
            Ok(Some(n)) => {
                let mut staging: Vec<MaybeUninit<u8>> =
                    buf[..n].iter().map(|&b| MaybeUninit::new(b)).collect();
                // SAFETY: moving plain `u8` bytes into the ring is a bitwise
                // copy; the staging buffer has nothing that needs dropping.
                unsafe {
                    segm.move_items_from_buff(&mut staging);
                }
            }
            Ok(None) => {
                // Remote finished the stream: close the read side so the
                // user-facing read half eventually reports drained/closing.
                drop(segm);
                tx.close_rx();
                return;
            }
            Err(e) => {
                if let Ok(mut guard) = error.lock() {
                    *guard = Some(e);
                }
                drop(segm);
                tx.close_rx();
                return;
            }
        }
    }
}
