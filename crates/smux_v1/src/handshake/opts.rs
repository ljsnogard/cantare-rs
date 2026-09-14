use core::time::Duration;

#[derive(Debug, Clone)]
pub struct HandshakeOpts<'a> {
    pub basic_opts: BasicOpts,
    pub ext_opts: &'a [NegotiationExtEntry<'a>],
}

/// 基础协商结果
#[derive(Debug, Clone)]
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

    pub fn from_entries<I>(
        mut entries_iter: I,
    ) -> Result<Self, NegotiationBasicEntry>
    where
        I: Iterator<Item: Borrow<NegotiationBasicEntry>>,
    {
        let mut x = BasicOpts::DEFAULT;
        while let Option::Some(entry) = entries_iter.next() {
            let Result::Ok(key) = NegotiationKey::try_from(entry.opts_key) else {
                return Result::Err(entry.borrow().clone());
            };
            let entry = entry.borrow();
            match key {
                NegotiationKey::MaxPacketSize =>
                    x.max_packet_size = entry.val_data,
                NegotiationKey::MaxChannelCount =>
                    x.max_channel_count = entry.val_data,
                NegotiationKey::MaxDockChanCount =>
                    x.max_dock_chan_count = entry.val_data,
                NegotiationKey::MaxChannelTimeout =>
                    x.max_channel_timeout = Duration::from_secs(entry.val_data as u64),
                _ => (),
            }
        }
        Result::Ok(x)
    }
}

impl Default for BasicOpts {
    fn default() -> Self {
        BasicOpts::DEFAULT
    }
}

/// 用低四位表示协商项的种类或名称。
/// `NegotiationKey` 和 `NegotiationValType` 共用同一个字节。
#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum NegotiationKey {
    MaxPacketSize     = 0x00,
    MaxChannelCount   = 0x01,
    MaxDockChanCount  = 0x02,
    MaxChannelTimeout = 0x03,
    Checksum          = 0x0C,
    ExtMsg            = 0x0E,
}

impl NegotiationKey {
    const MASK: u8 = 0x0F;
}

impl TryFrom<u8> for NegotiationKey {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value & NegotiationKey::MASK {
            0x00 => Result::Ok(NegotiationKey::MaxPacketSize),
            0x01 => Result::Ok(NegotiationKey::MaxChannelCount),
            0x02 => Result::Ok(NegotiationKey::MaxDockChanCount),
            0x03 => Result::Ok(NegotiationKey::MaxChannelTimeout),
            0x0C => Result::Ok(NegotiationKey::Checksum),
            0x0E => Result::Ok(NegotiationKey::ExtMsg),
            _ => Result::Err(value),
        }
    }
}

impl From<NegotiationKey> for u8 {
    fn from(v: NegotiationKey) -> Self {
        (v as u8) & NegotiationValType::MASK
    }
}

/// 用高四位表示协商项的值类型。
/// `NegotiationKey` 和 `NegotiationValType` 共用同一个字节。
#[derive(Debug, Clone, Copy)]
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

impl NegotiationValType {
    const MASK: u8 = 0x30;
}

impl TryFrom<u8> for NegotiationValType {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value & NegotiationValType::MASK {
            0x00 => Result::Ok(NegotiationValType::BeU8),
            0x10 => Result::Ok(NegotiationValType::BeU16),
            0x20 => Result::Ok(NegotiationValType::BeU32),
            0x30 => Result::Ok(NegotiationValType::BeU64),
            _ => Result::Err(value),
        }
    }
}

impl From<NegotiationValType> for u8 {
    fn from(v: NegotiationValType) -> Self {
        (v as u8) & NegotiationValType::MASK
    }
}

/// 基础协商事项键值对
#[derive(Debug, Clone)]
pub struct NegotiationBasicEntry {
    pub opts_key: u8,
    pub val_data: usize,
}

/// 扩展协商事项键值对
#[derive(Debug, Clone)]
pub struct NegotiationExtEntry<'a> {
    pub len_type: u8,
    pub len_data: usize,
    pub msg_data: &'a [u8],
}

#[cfg(test)]
mod tests_ {

}
