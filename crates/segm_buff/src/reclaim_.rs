use core::{
    ops::AddAssign,
    ptr::NonNull,
};

use abs_buff::TrBuffSegmView;

pub trait TrReclaim<T>: Sized {
    fn reclaim<S: TrBuffSegmView<Item = T>>(&mut self, s: &mut S);
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NoReclaim;

impl<T> TrReclaim<T> for NoReclaim {
    #[inline]
    fn reclaim<S: TrBuffSegmView<Item = T>>(&mut self, s: &mut S) {
        let _ = s;
    }
}

pub struct SegmSelfReclaim(NonNull<usize>);

impl SegmSelfReclaim {
    #[inline]
    pub(super) const fn new(offset_ptr: NonNull<usize>) -> Self {
        SegmSelfReclaim(offset_ptr)
    }
}

impl<T> TrReclaim<T> for SegmSelfReclaim {
    #[inline]
    fn reclaim<S: TrBuffSegmView<Item = T>>(&mut self, s: &mut S) {
        let c = s.capacity();
        #[cfg(test)]
        {
            let offset_ptr = self.0.as_ptr();
            std::println!("SegmSelfReclaim::reclaim: [{:p}] {c}", offset_ptr);
        }
        unsafe {
            let offset_mut: &mut usize = self.0.as_mut();
            offset_mut.add_assign(c);
        }
    }
}
