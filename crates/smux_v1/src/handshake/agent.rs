use abs_buff::{
    Demand, TrBuffRead, TrBuffWrite,
    buffer::{TrBuffSegmRef, TrBuffSegmView},
    gen_may_cancel_future,
    x_deps::abs_cancel,
};
use abs_cancel::{TrMayCancel, TrCancellationToken};

use crate::handshake::{
    K_INVITE_MAGIC, K_CONFRM_MAGIC,
    opts::HandshakeOpts,
};

const HANDSHAKE_CRC: crc::Crc<u16> = crc::Crc::<u16>::new(&crc::CRC_16_XMODEM);

pub enum ListenInvitaionError<R>
where
    R: TrBuffRead<u8>,
{
    ChecksumErr,
    Cancelled,
    InvalidMagic,
    RxErr(R::Err),
}

#[gen_may_cancel_future(ListenInvitation)]
async fn listen_invitation_async_<'f, R, K>(
    buf_read: &'f mut R,
    max_size: usize,
    cancel: &'f mut K,
) -> Result<HandshakeOpts<'f>, ListenInvitaionError<R>>
where
    R: TrBuffRead<u8>,
    K: TrCancellationToken + Clone,
{
    let recv_magic = read_magic_async_::<R, K, { K_INVITE_MAGIC.len() }>(buf_read, cancel)
        .await
        .map_err(ListenInvitaionError::RxErr)?;
    let is_magic_valid = recv_magic
        .iter()
        .eq(K_INVITE_MAGIC.iter());
    if !is_magic_valid {
        return Result::Err(ListenInvitaionError::ChecksumErr)
    }
    Result::Err(ListenInvitaionError::Cancelled)
}

async fn read_magic_async_<'f, R, K, const MAGIC_LEN: usize>(
    buf_read: &'f mut R,
    cancel: &'f mut K
) -> Result<[u8; MAGIC_LEN], R::Err>
where
    R: TrBuffRead<u8>,
    K: TrCancellationToken + Clone,
{
    let magic_demand = Demand::exactly(MAGIC_LEN);
    let mut read_res = buf_read
        .read_async(&magic_demand)
        .may_cancel_with(cancel)
        .await;
    if let Option::Some(segm) = read_res.as_mut().pick_left() {
        return Result::Ok(segm
            .iter_slices()
            .into_iter()
            .flat_map(|s| s.iter())
            .collect()
        );
    }
    if let Option::Some(err) = read_res.pick_right() {
        return Result::Err(err);
    }
    unreachable!()
}
