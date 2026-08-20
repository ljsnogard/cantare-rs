//! The write-side adapter: [`IrohWriter`].

use std::{
    mem::MaybeUninit,
    sync::{Arc, Mutex},
    vec::Vec,
};

use abs_buff::{Demand, TrBuffTryWrite, TrBuffWrite};
use anylr::SomeOf;
use buffex::ring_buffer::RingBuffer;
use iroh::endpoint::{SendStream, WriteError};
use tokio::task::JoinHandle;

use super::common::{SharedRx, SharedTx, new_shared_ring};

/// Write half of a buffered iroh stream.
///
/// Implements [`TrBuffWrite`] / [`TrBuffTryWrite`] by delegating to the
/// internal [`RingTx`].
pub struct IrohWriter {
    tx: SharedTx,
    error: Arc<Mutex<Option<WriteError>>>,
    _task: JoinHandle<()>,
}

impl IrohWriter {
    /// Wrap a QUIC send stream with a ring buffer of `cap` bytes.
    ///
    /// Spawns a background task that drains the ring and writes it to
    /// `stream`.
    pub fn new(stream: SendStream, cap: usize) -> Self {
        let ring = new_shared_ring(cap);
        let (tx, rx) = RingBuffer::try_split_shared(
            ring,
            Arc::strong_count,
            Arc::weak_count,
        )
        .expect("fresh ring must split into a single tx half");
        // The flush task owns the rx half; it must not implicitly close the
        // ring when the task exits. Closure is signalled explicitly via
        // `RingRx::close_tx`.
        let rx = rx.with_auto_close(false);

        let error = Arc::new(Mutex::new(None::<WriteError>));
        let task = tokio::spawn(flush_task(stream, rx, error.clone()));

        Self {
            tx,
            error,
            _task: task,
        }
    }

    /// Report the last background write error, if any.
    pub fn take_error(&self) -> Option<WriteError> {
        self.error.lock().ok().and_then(|mut guard| guard.take())
    }

    /// Flush buffered data, finish the QUIC stream, and wait for the
    /// background flush task to exit.
    pub async fn shutdown(mut self) {
        self.tx.close();
        let _ = (&mut self._task).await;
    }
}

impl Drop for IrohWriter {
    fn drop(&mut self) {
        self.tx.close();
    }
}

impl TrBuffWrite<u8> for IrohWriter {
    type WriteAsync<'f>
        = <SharedTx as TrBuffWrite<u8>>::WriteAsync<'f>
    where
        Self: 'f;
    type SegmMut<'a>
        = <SharedTx as TrBuffWrite<u8>>::SegmMut<'a>
    where
        Self: 'a;
    type Err = <SharedTx as TrBuffWrite<u8>>::Err;

    fn is_blocked_closing(&self) -> bool {
        self.tx.is_blocked_closing()
    }

    fn write_async<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> Self::WriteAsync<'f> {
        <SharedTx as TrBuffWrite<u8>>::write_async(&mut self.tx, demand)
    }
}

impl TrBuffTryWrite<u8> for IrohWriter {
    fn try_write<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> SomeOf<Self::SegmMut<'f>, Self::Err> {
        <SharedTx as TrBuffTryWrite<u8>>::try_write(&mut self.tx, demand)
    }
}

/// Drain the read side of a ring and write it to the QUIC `SendStream`.
async fn flush_task(
    mut stream: SendStream,
    mut rx: SharedRx,
    error: Arc<Mutex<Option<WriteError>>>,
) {
    loop {
        if rx.ring().is_tx_closed() && rx.data_size() == 0 {
            let _ = stream.finish();
            return;
        }

        let Some(mut segm) =
            rx.read_at_most_async(usize::MAX).await.pick_left()
        else {
            // The read side has been closed, or the write side has been
            // closed and the ring drained. In the drained case we still need
            // to finish the QUIC stream.
            if rx.ring().is_tx_closed() && rx.data_size() == 0 {
                let _ = stream.finish();
            }
            return;
        };

        let n = segm.least_count();
        let mut staging: Vec<MaybeUninit<u8>> = Vec::with_capacity(n);
        staging.resize_with(n, MaybeUninit::uninit);
        // SAFETY: the ring segment contains plain `u8` bytes; moving them into
        // the staging buffer is a bitwise copy.
        unsafe {
            segm.move_items_to_buff(&mut staging);
        }
        let bytes: Vec<u8> = staging
            .into_iter()
            .map(|m| unsafe { m.assume_init_read() })
            .collect();
        drop(segm);

        if let Err(e) = stream.write_all(&bytes).await {
            if let Ok(mut guard) = error.lock() {
                *guard = Some(e);
            }
            rx.close_tx();
            return;
        }
    }
}
