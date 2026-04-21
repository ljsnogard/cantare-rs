use core::{
    error::Error,
    mem::{self, MaybeUninit},
    ptr,
};

use abs_sync::may_cancel::TrMayCancel;

use anylr::SomeOf;

/// Unbuffered input device
pub trait TrInput<T = u8> {
    type Err : Error;

    /// Move the data out of the device and into the specified target buffer.
    ///
    /// ## Safety
    ///
    /// - It's the responsibility of the implementation providers to guarantee that, 
    ///   data written into the `target` must be memory-aligned for type `T`;
    ///
    /// - It's the responsibility of the caller to guarantee that, conversion from
    ///   `MaybeUninit<T>` to `T` is sound;
    ///
    /// - For example, if `T: Clone` is satisfied, implementaion provider to move
    ///   a `t` of `T` into `target`, should do `target[0].write(t.clone())`; caller
    ///   should do `let t = target[0].assume_init()`;
    fn read_async<'a>(
        &'a mut self,
        target: &'a mut [MaybeUninit<T>],
    ) -> impl TrMayCancel<'a, MayCancelOutput = SomeOf<usize, Self::Err>>;
}

/// Unbuffered output device
pub trait TrOutput<T = u8> {
    type Err : Error;

    /// Move data from the specified source into this output device
    fn write_async<'a>(
        &'a mut self,
        source: &'a [MaybeUninit<T>],
    ) -> impl TrMayCancel<'a, MayCancelOutput = SomeOf<usize, Self::Err>>;

    /// Clone data from the specified source buffer into this output device 
    fn write_cloned_async<'a>(
        &'a mut self,
        source: &'a [T],
    ) -> impl TrMayCancel<'a, MayCancelOutput = SomeOf<usize, Self::Err>>
    where
        T: Clone,
    {
        if mem::size_of::<T>() == 0 {
            // Handle ZSTs separately, as copying them is unnecessary and UB
            return self.write_async(&[])
        }
        unsafe {
            let src_head = &source[0] as *const T as *const MaybeUninit<T>;
            let slice = ptr::slice_from_raw_parts(src_head, source.len());
            self.write_async(&*slice)
        }
    }
}
