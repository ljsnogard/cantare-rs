use abs_buff::x_deps::funty;

mod sealed_prefix_ {
    pub trait SealedPrefix_ {}
}

pub trait TrMultipartPrefix
where
    Self: sealed_prefix_::SealedPrefix_,
{
    type Prefix: funty::Unsigned;

    fn PREFIX_LEN() -> usize {
        <Self::Prefix as funty::Integral>::BITS as usize
    }

    fn PREFIX_MAX() -> usize {
        1usize << <Self::Prefix as funty::Integral>::BITS
    }
}

pub struct U32Prefix;
pub struct U16Prefix;
pub struct U8Prefix;

impl TrMultipartPrefix for U32Prefix {
    type Prefix = u32;
}
impl TrMultipartPrefix for U16Prefix {
    type Prefix = u16;
}
impl TrMultipartPrefix for U8Prefix {
    type Prefix = u8;
}

impl sealed_prefix_::SealedPrefix_ for U32Prefix {}
impl sealed_prefix_::SealedPrefix_ for U16Prefix {}
impl sealed_prefix_::SealedPrefix_ for U8Prefix {}
