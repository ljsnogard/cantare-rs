use core::{
    error::Error,
    mem::{self, MaybeUninit},
    ptr,
    slice,
};

use abs_iter::{TrAsSlice, TrAsSliceMut};
use anylr::SomeOf;
use abs_sync::may_cancel::TrMayCancel;

/// A trait specifically abstracted from `MaybeUninit<T>` or types alike.
///
/// # Safety
/// The only reasonable implementation is core::mem::MaybeUninit<T>, which is
/// already included in this crate.
pub unsafe trait TrMaybeUninit {
    type Inner: Sized;

    /// See [core::mem::MaybeUninit::uninit]
    fn uninit() -> Self;

    /// See [core::mem::MaybeUninit::zeroed]
    fn zeroed() -> Self;

    /// See [core::mem::MaybeUninit::as_bytes]
    fn as_bytes(&self) -> &[MaybeUninit<u8>];

    /// See [core::mem::MaybeUninit::as_bytes_mut]
    fn as_bytes_mut(&mut self) -> &mut [MaybeUninit<u8>];

    /// Extracts the value from the `MaybeUninit<T>` container. This is a great way
    /// to ensure that the data will get dropped, because the resulting `T` is
    /// subject to the usual drop handling.
    ///
    /// # Safety
    /// See [core::mem::MaybeUninit::assume_init].
    unsafe fn assume_init(self) -> Self::Inner;

    /// Reads the value from the `MaybeUninit<T>` container. The resulting `T` is subject
    /// to the usual drop handling.
    ///
    /// # Safety
    /// See [core::mem::MaybeUninit::assume_init_read].
    unsafe fn assume_init_read(&self) -> Self::Inner;

    /// Gets a shared reference to the contained value.
    ///
    /// # Safety
    /// See [core::mem::MaybeUninit::assume_init_ref].
    unsafe fn assume_init_ref(&self) -> &Self::Inner;

    /// Gets a mutable reference to the containted value.
    ///
    /// # Safety
    /// See [core::mem::MaybeUninit::assume_init_mut]
    unsafe fn assume_init_mut(&mut self) -> &mut Self::Inner;

    /// Drops the contained value in place.
    ///
    /// # Safety
    /// See [core::mem::MaybeUninit::assume_init_drop]
    unsafe fn assume_init_drop(&mut self);

    /// See [core::mem::MaybeUninit::write]
    fn write(&mut self, value: Self::Inner) -> &mut Self::Inner;
}

/// A continuous memory space that can read and write items.
///
/// The reasonable implementations are already included in this crate. They are
/// `[MaybeUninit<T>; N]`, `MaybeUninit<[T; N]>`, `&mut MaybeUninit<[T; N]>`,
/// and `&mut [MaybeUninit<T>] `
pub trait TrBuffer
where
    Self: TrAsSlice<Elem = Self::Slot>
        + TrAsSliceMut<Elem = Self::Slot>,
{
    type Slot: TrMaybeUninit;

    /// Explicitly declare that the termination of evaluation for
    /// `TrMaybeUninit` be `core::mem::MaybeUninit`.
    fn as_slice_uninit(
        &self,
    ) -> &[MaybeUninit<<Self::Slot as TrMaybeUninit>::Inner>];

    /// Explicitly declare that the termination of evaluation for
    /// `TrMaybeUninit` be `core::mem::MaybeUninit`.
    fn as_mut_slice_uninit(
        &mut self,
    ) -> &mut [MaybeUninit<<Self::Slot as TrMaybeUninit>::Inner>];
}

/// A convenient alias to extract `MaybeUninit<T>` from trait `TrBuffer`.
pub type BufferSlot<B> = <B as TrBuffer>::Slot;

/// A convenient alias to extract the element type from trait `TrBuffer`.
pub type BufferElem<B> = <BufferSlot<B> as TrMaybeUninit>::Inner;

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

impl<T, const N: usize> TrBuffer for [MaybeUninit<T>; N] {
    type Slot = MaybeUninit<T>;

    #[inline]
    fn as_slice_uninit(
        &self,
    ) -> &[MaybeUninit<<Self::Slot as TrMaybeUninit>::Inner>] {
        self.as_ref()
    }

    #[inline]
    fn as_mut_slice_uninit(
        &mut self,
    ) -> &mut [MaybeUninit<<Self::Slot as TrMaybeUninit>::Inner>] {
        self.as_mut()
    }
}

impl<T, const N: usize> TrBuffer for MaybeUninit<[T; N]> {
    type Slot = MaybeUninit<T>;

    #[inline]
    fn as_slice_uninit(
        &self,
    ) -> &[MaybeUninit<<Self::Slot as TrMaybeUninit>::Inner>] {
        unsafe {
            mem::transmute(self.assume_init_ref().as_ref())
        }
    }

    #[inline]
    fn as_mut_slice_uninit(
        &mut self,
    ) -> &mut [MaybeUninit<<Self::Slot as TrMaybeUninit>::Inner>] {
        unsafe {
            mem::transmute(self.assume_init_mut().as_mut())
        }
    }
}

impl<T, const N: usize> TrBuffer for &mut MaybeUninit<[T; N]> {
    type Slot = MaybeUninit<T>;

    #[inline]
    fn as_slice_uninit(
        &self,
    ) -> &[MaybeUninit<<Self::Slot as TrMaybeUninit>::Inner>] {
        unsafe {
            mem::transmute(self.assume_init_ref().as_ref())
        }
    }

    #[inline]
    fn as_mut_slice_uninit(
        &mut self,
    ) -> &mut [MaybeUninit<<Self::Slot as TrMaybeUninit>::Inner>] {
        unsafe {
            mem::transmute(self.assume_init_mut().as_mut())
        }
    }
}

impl<T> TrBuffer for [MaybeUninit<T>] {
    type Slot = MaybeUninit<T>;

    #[inline]
    fn as_slice_uninit(
        &self,
    ) -> &[MaybeUninit<<Self::Slot as TrMaybeUninit>::Inner>] {
        self
    }

    #[inline]
    fn as_mut_slice_uninit(
        &mut self,
    ) -> &mut [MaybeUninit<<Self::Slot as TrMaybeUninit>::Inner>] {
        self
    }
}

impl<T> TrBuffer for &mut [MaybeUninit<T>] {
    type Slot = MaybeUninit<T>;

    #[inline]
    fn as_slice_uninit(
        &self,
    ) -> &[MaybeUninit<<Self::Slot as TrMaybeUninit>::Inner>] {
        self
    }

    #[inline]
    fn as_mut_slice_uninit(
        &mut self,
    ) -> &mut [MaybeUninit<<Self::Slot as TrMaybeUninit>::Inner>] {
        self
    }
}

unsafe impl<T> TrMaybeUninit for MaybeUninit<T> {
    type Inner = T;

    #[inline]
    fn uninit() -> Self {
        MaybeUninit::uninit()
    }

    #[inline]
    fn zeroed() -> Self {
        MaybeUninit::zeroed()
    }

    #[inline]
    fn as_bytes(&self) -> &[MaybeUninit<u8>] {
        // SAFETY: MaybeUninit<u8> is always valid, even for padding bytes
        unsafe {
            slice::from_raw_parts(
                self.as_ptr().cast::<MaybeUninit<u8>>(),
                mem::size_of::<T>()
            )
        }
    }

    #[inline]
    fn as_bytes_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        unsafe {
            slice::from_raw_parts_mut(
                self.as_mut_ptr().cast::<MaybeUninit<u8>>(),
                mem::size_of::<T>(),
            )
        }
    }

    #[inline]
    unsafe fn assume_init(self) -> Self::Inner {
        unsafe { self.assume_init() }
    }

    #[inline]
    unsafe fn assume_init_read(&self) -> Self::Inner {
        unsafe { self.assume_init_read() }
    }

    #[inline]
    unsafe fn assume_init_ref(&self) -> &Self::Inner {
        unsafe { self.assume_init_ref() }
    }

    #[inline]
    unsafe fn assume_init_mut(&mut self) -> &mut Self::Inner {
        unsafe { self.assume_init_mut() }
    }

    #[inline]
    unsafe fn assume_init_drop(&mut self) {
        unsafe { self.assume_init_drop(); }
    }

    #[inline]
    fn write(&mut self, value: Self::Inner) -> &mut Self::Inner {
        MaybeUninit::write(self, value)
    }
}

#[cfg(test)]
mod tests_ {
    #[allow(unused)]
    use super::{MaybeUninit, TrBuffer};

    #[test]
    fn array_as_slice_uninit() {
        const L: usize = 3usize;
        let mut a = [MaybeUninit::<usize>::uninit(); L];
        let s = a.as_slice_uninit();
        assert_eq!(s.len(), L);
        let s = a.as_mut_slice_uninit();
        assert_eq!(s.len(), L);
    }

    #[test]
    fn uninit_array_as_slice_uninit() {
        const L: usize = 3usize;
        let mut a: MaybeUninit<[usize; 3]> = MaybeUninit::uninit();
        let s = a.as_slice_uninit();
        assert_eq!(s.len(), L);
        let s = a.as_mut_slice_uninit();
        assert_eq!(s.len(), L);
    }
}
