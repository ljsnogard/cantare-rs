mod buff_;

mod segm_;

pub use buff_::{TrBuffer, TrBufferMut, TrMaybeUninit};
pub use segm_::{
    SegmMut, SegmRef, SegmReclaim,
    TrBuffSegmView, TrBuffSegmMut, TrBuffSegmRef,
};
