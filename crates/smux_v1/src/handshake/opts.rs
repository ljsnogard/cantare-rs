use core::time::Duration;

pub struct HandshakeOpts<'a> {
    pub basic_opts: BasicOpts,
    pub ext_opts: &'a [NegotiationExtEntry<'a>],
    pub checksum: &'a [u8],
}

pub struct BasicOpts {
    /// 分片传输过程中最大报文大小（含头部）
    pub max_packet_size: usize,

    /// 单个连接上最大同时活动的 channel 数量
    pub max_channel_count: usize,

    /// 单个 dock 能容纳的最大同时活动的 channel 数量
    pub max_dock_chan_count: usize,

    /// 一个 Channel 在无任何数据交流后的最长存活时间。
    ///
    /// # Discussion
    /// 不断地发送心跳报文（ACK）可以无限地延长 channel 存活时间，直到有一端主动关闭。
    pub max_channel_timeout: Duration,
}

impl BasicOpts {
    pub const DEFAULT: BasicOpts = BasicOpts {
        max_packet_size: 4096usize,
        max_channel_count: 1usize << 32,
        max_dock_chan_count: 1usize << 32,
        max_channel_timeout: Duration::from_secs(30u64),
    };
}

impl Default for BasicOpts {
    fn default() -> Self {
        BasicOpts::DEFAULT
    }
}

/// 用低四位表示协商项的键值
#[repr(u8)]
pub enum NegotiationKey {
    MaxPacketSize     = 0x00,
    MaxChannelCount   = 0x01,
    MaxDockChanCount  = 0x02,
    MaxChannelTimeout = 0x03,
    Checksum          = 0x0C,
    ExtMsg            = 0x0E,
}

impl From<NegotiationKey> for u8 {
    fn from(v: NegotiationKey) -> Self {
        v as u8
    }
}

/// 用高四位表示协商项的值类型
#[repr(u8)]
pub enum NegotiationValType {
    /// The value will be 1 byte u8, or the checksum type will be crc-8
    BeU8  = 0x00,

    /// The value will be 2 byte big endian u16, or the checksum type will be crc-16
    BeU16 = 0x10,

    /// The value will be 4 byte big endian u32, or the checksum type will be crc-32
    BeU32 = 0x20,

    /// The value will be 8 byte big endian u64, or the checksum type will be crc-64
    BeU64 = 0x30,
}

impl From<NegotiationValType> for u8 {
    fn from(v: NegotiationValType) -> Self {
        v as u8
    }
}

pub struct NegotiationSimpleEntry {
    pub opts_key: u8,
    pub val_data: usize,
}

pub struct NegotiationExtEntry<'a> {
    pub len_type: u8,
    pub len_data: usize,
    pub msg_data: &'a [u8],
}

#[cfg(test)]
mod tests_ {

}
