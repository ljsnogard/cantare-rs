pub trait TrReclaim: Sized {
    fn reclaim(&mut self, amount: usize);
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NoReclaim;

impl TrReclaim for NoReclaim {
    #[inline]
    fn reclaim(&mut self, amount: usize) {
        let _ = amount;
    }
}

pub struct SegmSelfReclaim<'a>(&'a mut usize);

impl<'a> SegmSelfReclaim<'a> {
    #[inline]
    pub(super) const fn new(offset: &'a mut usize) -> Self {
        SegmSelfReclaim(offset)
    }
}

impl TrReclaim for SegmSelfReclaim<'_> {
    #[inline]
    fn reclaim(&mut self, amount: usize) {
        #[cfg(test)]
        {
            let offset_ptr = self.0 as *mut usize;
            std::println!("SegmSelfReclaim::reclaim: [{:p}] {amount}", offset_ptr);
        }
        *self.0 += amount;
    }
}
