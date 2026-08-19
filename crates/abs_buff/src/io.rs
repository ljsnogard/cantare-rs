use core::{
    error::Error,
    mem::{self, MaybeUninit},
    ops::{Deref, DerefMut, Try},
    ptr,
};

use abs_cancel::TrMayCancel;
use anylr::SomeOf;

use crate::buffer::{TrBuffer, TrBufferMut};

/// A device that will produce data. And the data shall be buffered when taking
/// taking out of them from this device.
pub trait TrInput<T = u8> {
    type ReadAsync<'f>: TrMayCancel<'f, MayCancelOutput = SomeOf<usize, Self::Err>>
    where
        Self: 'f, T: 'f;

    type Err: Error;

    /// Read data from this input device and into the specified target buffer.
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
    fn read_async<'f>(
        &'f mut self,
        target: &'f mut [MaybeUninit<T>],
    ) -> Self::ReadAsync<'f>;
}

/// A device that will consume data. And the data shall be offered with
/// a buffer.
pub trait TrOutput<T = u8> {
    type WriteAsync<'f>: TrMayCancel<'f, MayCancelOutput = SomeOf<usize, Self::Err>>
    where
        Self: 'f, T: 'f;

    type Err: Error;

    /// Move data from the specified source into this output device
    fn write_async<'f>(
        &'f mut self,
        source: &'f [MaybeUninit<T>],
    ) -> Self::WriteAsync<'f>;

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
            return self.write_async(&[]);
        }
        unsafe {
            let src_head = &source[0] as *const T as *const MaybeUninit<T>;
            let slice = ptr::slice_from_raw_parts(src_head, source.len());
            self.write_async(&*slice)
        }
    }
}

pub trait TrSink<T = u8> {
    fn write_async<'f, TyBuff>(
        &'f mut self,
        source: TyBuff,
    ) -> impl TrMayCancel<'f, MayCancelOutput: Try<Output = (usize, TyBuff)>>
    where
        TyBuff: Deref<Target: 'static + TrBuffer>;
}

pub trait TrFlux<T = u8> {
    fn read_async<'f, TyBuffMut>(
        &'f mut self,
        target: TyBuffMut,
    ) -> impl TrMayCancel<'f, MayCancelOutput: Try<Output = (usize, TyBuffMut)>>
    where
        TyBuffMut: DerefMut<Target: 'static + TrBufferMut>;
}
