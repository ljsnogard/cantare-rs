//! Thin `TrBuffRead` / `TrBuffWrite` adapters over iroh QUIC streams.
//!
//! iroh's [`RecvStream`] and [`SendStream`] do not expose their internal
//! buffers through the abs_buff segment model.  This crate places a
//! [`buffex::ring_buffer::RingBuffer`] between the user and the QUIC stream:
//!
//! * [`IrohReader`] wraps a [`RecvStream`].  A background task reads from the
//!   QUIC stream into the ring; the user consumes the buffered data through
//!   [`TrBuffRead`] / [`TrBuffTryRead`].
//! * [`IrohWriter`] wraps a [`SendStream`].  The user produces data through
//!   [`TrBuffWrite`] / [`TrBuffTryWrite`]; a background task drains the ring
//!   and writes it to the QUIC stream.
//!
//! Both constructors spawn a Tokio task, so they must be called from a Tokio
//! runtime context.

mod common;
mod reader;
mod writer;

pub use reader::IrohReader;
pub use writer::IrohWriter;
