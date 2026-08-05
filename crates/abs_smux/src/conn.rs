use core::error::Error;

use abs_buff::{
    x_deps::anylr,
    TrBuffRead, TrBuffWrite,
};
use abs_sync::may_cancel::TrMayCancel;
use anylr::SomeOf;

/// Similar to port in TCP/IP, a tuple of dock defines the packet source and destination.
pub trait TrDock
where
    Self: Clone + Eq + Ord + Sized,
{
    /// A dock representing any remote dock.
    fn wildcard() -> Self;
}

/// Similar to UDP in TCP/IP, a telegrpah can send or receive packets without a handshake
/// to establish a channel. But not like in TCP/IP, a channel and a telegraph sharing a same
/// dock is not allowed.
pub trait TrTelegraph {
    type Data;
    type Dock: TrDock;
    type Err: Error;

    fn local_dock(&self) -> Self::Dock;

    fn send_async<'f, R>(
        &'f mut self,
        remote_dock: Self::Dock,
        packet: &mut R,
    ) -> impl TrMayCancel<'f, MayCancelOutput = SomeOf<usize, Self::Err>>
    where
        R: TrBuffRead<Self::Data>;

    fn recv_async<'f, W>(
        &'f mut self,
        remote_dock: Self::Dock,
        buffer: &mut W,
    ) -> impl TrMayCancel<'f, MayCancelOutput = SomeOf<usize, Self::Err>>
    where
        W: TrBuffWrite<Self::Data>;
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

    /// 向请求端发送同意建立 channel 的消息及欢迎信息
    fn accept_async<'a, W>(
        &'a mut self,
        welcome: &mut W,
    ) -> impl TrMayCancel<'a, MayCancelOutput = Result<Self::Channel, Self::Err>>
    where
        W: TrBuffWrite;

    /// 向请求端发送拒绝建立 channel 的消息及理由
    fn reject_async<'a, R>(
        &'a mut self,
        reason: &mut R,
    ) -> impl TrMayCancel<'a, MayCancelOutput = Result<usize, Self::Err>>
    where
        R: TrBuffRead;
}

pub trait TrChannelListener {
    type ChannelHandle: TrChannelHandle<Channel = Self::Channel>;
    type Channel: TrChannel;
    type Dock: TrDock;
    type Err;

    fn local_dock(&self) -> &Self::Dock;

    fn income_async(
        &mut self,
    ) -> impl TrMayCancel<'_, MayCancelOutput = Result<Self::ChannelHandle, Self::Err>>;
}

pub trait TrConnection {
    type DockBinding<'a>: TrDockBinding<Data = Self::Data, Dock = Self::Dock>
    where
        Self: 'a;

    type Data;
    type Dock: TrDock;
    type Err: Error;

    fn bind_async<'a>(
        &'a self,
        local_dock: Self::Dock,
    ) -> impl TrMayCancel<'a, MayCancelOutput = Result<Self::DockBinding<'a>, Self::Err>>;
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

    fn local_dock(&self) -> &Self::Dock;

    /// Listen at the dock owned by this operator.
    fn listen_async(
        &mut self,
    ) -> impl TrMayCancel<'_, MayCancelOutput = Result<Self::Listener<'_>, Self::Err>>;

    /// Create a telegraph for sending and receiving packets.
    fn open_telegraph_async(
        &mut self,
    ) -> impl TrMayCancel<'_, MayCancelOutput = Result<Self::Telegraph<'_>, Self::Err>>;

    /// Initiate a channel to the remote dock
    fn open_channel_async<'a, R>(
        &'a mut self,
        remote_dock: Self::Dock,
        message: &mut R,
    ) -> impl TrMayCancel<'a, MayCancelOutput = Result<Self::Channel<'a>, Self::Err>>
    where
        R: TrBuffRead<Self::Data>;
}
