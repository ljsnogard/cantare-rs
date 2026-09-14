pub mod opts;
pub mod agent;

pub const K_INVITE_MAGIC: [u8; 4] = [95, 27, 0x01, b'i'];
pub const K_ACCEPT_MAGIC: [u8; 4] = [95, 27, 0x01, b'A'];
pub const K_REJECT_MAGIC: [u8; 4] = [95, 27, 0x01, b'J'];
pub const K_CONFRM_MAGIC: [u8; 4] = [95, 27, 0x01, b'c'];
