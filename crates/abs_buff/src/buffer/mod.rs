mod buff_;

mod segm_;

// 泛型段测试函数：仅在 abs_buff 自身测试或显式启用 `segm-tests` feature 时
// 编译，供依赖方（如 buffex）在测试中验证其段类型的 move_items_* 默认实现。
#[cfg(any(test, feature = "segm-tests"))]
pub mod segm_tests;

pub use buff_::{TrBuffer, TrBufferMut, TrMaybeUninit};
pub use segm_::{
    SegmMut, SegmReclaim, SegmRef, TrBuffSegmMut, TrBuffSegmRef,
    TrBuffSegmView, TrReclaim,
};
