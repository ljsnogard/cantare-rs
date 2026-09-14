use core::error::Error;

use abs_buff::{
    x_deps::{abs_cancel, anylr},
    TrBuffRead, TrBuffWrite,
};
use abs_cancel::TrMayCancel;
use anylr::SomeOf;

/// Similar to port in TCP/IP, a tuple of dock defines the packet source and destination.
pub trait TrDock
where
    Self: Clone + Eq + Ord + Sized,
{
    /// A dock representing any remote dock.
    fn wildcard() -> Self;
}

/// Similar to UDP in TCP/IP, a telegrpah can send or receive packets without
/// any handshake to establish a short-living channel. But not like in TCP/IP,
/// a channel and a telegraph sharing a same dock is not allowed.
pub trait TrTelegraph {
    type Data;
    type Dock: TrDock;
    type Err: Error;

    type SendAsync<'f>: TrMayCancel<'f, MayCancelOutput = SomeOf<usize, Self::Err>>
    where
        Self: 'f;

    type RecvAsync<'f>: TrMayCancel<'f, MayCancelOutput = SomeOf<usize, Self::Err>>
    where
        Self: 'f;

    fn local_dock(&self) -> Self::Dock;

    fn send_async<'f, R>(
        &'f mut self,
        remote_dock: Self::Dock,
        packet: &mut R,
    ) -> Self::SendAsync<'f>
        where R: TrBuffRead<Self::Data>;

    fn recv_async<'f, W>(
        &'f mut self,
        remote_dock: Self::Dock,
        buffer: &mut W,
    ) -> Self::RecvAsync<'f>
        where W: TrBuffWrite<Self::Data>;
}

pub trait TrChannel {
    type Data;
    type Dock: TrDock;

    fn local_dock(&self) -> Self::Dock;

    fn remote_dock(&self) -> Self::Dock;
}

pub trait TrChannelHandle {
    type Channel: TrChannel<Data = Self::Data, Dock = Self::Dock>;
    type Data;
    type Dock: TrDock;
    type Err;

    type AcceptAsync<'f>: TrMayCancel<'f, MayCancelOutput = Result<Self::Channel, Self::Err>>
    where
        Self: 'f;

    type RejectAsync<'f>: TrMayCancel<'f, MayCancelOutput = Result<usize, Self::Err>>
    where
        Self: 'f;

    /// 向请求端发送同意建立 channel 的消息及欢迎信息
    fn accept_async<'f, W>(
        &'f mut self,
        welcome: &mut W,
    ) -> Self::AcceptAsync<'f>
    where
        W: TrBuffWrite;

    /// 向请求端发送拒绝建立 channel 的消息及理由
    fn reject_async<'f, R>(
        &'f mut self,
        reason: &mut R,
    ) -> Self::RejectAsync<'f>
    where
        R: TrBuffRead;
}

pub trait TrChannelListener {
    type ChannelHandle: TrChannelHandle<Channel = Self::Channel>;
    type Channel: TrChannel;
    type Dock: TrDock;
    type Err;

    type IncomeAsync<'f>: TrMayCancel<'f, MayCancelOutput =
        Result<Self::ChannelHandle, Self::Err>>
    where
        Self: 'f;

    fn local_dock(&self) -> &Self::Dock;

    fn income_async(&mut self) -> Self::IncomeAsync<'_>;
}

pub trait TrConnection {
    type DockBinding<'f>: TrDockBinding<Data = Self::Data, Dock = Self::Dock>
    where
        Self: 'f;

    type Data;
    type Dock: TrDock;
    type Err: Error;

    type BindAsync<'f>: TrMayCancel<'f, MayCancelOutput =
        Result<Self::DockBinding<'f>, Self::Err>>
    where
        Self: 'f;

    fn bind_async<'f>(
        &'f self,
        local_dock: Self::Dock,
    ) -> Self::BindAsync<'f>;
}

pub trait TrDockBinding {
    type Channel<'f>: TrChannel<Data = Self::Data, Dock = Self::Dock>
    where
        Self: 'f;

    type Data;
    type Dock: TrDock;

    type Err: Error;

    type Listener<'f>: TrChannelListener<Channel= Self::Channel<'f>>
    where
        Self: 'f;

    type Telegraph<'f>: TrTelegraph<Data = Self::Data, Dock = Self::Dock>
    where
        Self: 'f;

    type ListenAsync<'f>: TrMayCancel<'f, MayCancelOutput = Result<Self::Listener<'f>, Self::Err>>
    where
        Self: 'f;

    type OpenTelegraphAsync<'f>: TrMayCancel<'f, MayCancelOutput = Result<Self::Telegraph<'f>, Self::Err>>
    where
        Self: 'f;

    type OpenChannelAsync<'f>: TrMayCancel<'f, MayCancelOutput = Result<Self::Channel<'f>, Self::Err>>
    where
        Self: 'f;

    fn local_dock(&self) -> &Self::Dock;

    /// Listen at the dock owned by this operator.
    fn listen_async(&mut self) -> Self::ListenAsync<'_>;

    /// Create a telegraph for sending and receiving packets.
    fn open_telegraph_async(&mut self) -> Self::OpenTelegraphAsync<'_>;

    /// Initiate a channel to the remote dock
    fn open_channel_async<'f, R>(
        &'f mut self,
        remote_dock: Self::Dock,
        message: &mut R,
    ) -> Self::OpenChannelAsync<'f>
    where
        R: TrBuffRead<Self::Data>;
}
