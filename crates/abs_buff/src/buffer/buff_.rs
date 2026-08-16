use core::{
    mem::{self, MaybeUninit},
    slice,
};

use abs_iter::{TrAsSlice, TrAsSliceMut};

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
    Self: TrAsSlice<Elem = Self::Slot>,
{
    type Slot: TrMaybeUninit;

    /// Explicitly declare that the termination of evaluation for
    /// `TrMaybeUninit` be `core::mem::MaybeUninit`.
    fn as_slice_uninit(
        &self,
    ) -> &[MaybeUninit<<Self::Slot as TrMaybeUninit>::Inner>];
}

pub trait TrBufferMut
where
    Self: TrBuffer + TrAsSliceMut<Elem = Self::Slot>,
{
    /// Explicitly declare that the termination of evaluation for
    /// `TrMaybeUninit` be `core::mem::MaybeUninit`.
    fn as_mut_slice_uninit(
        &mut self,
    ) -> &mut [MaybeUninit<<Self::Slot as TrMaybeUninit>::Inner>];
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// impl TrBuffer TrBufferMut for `[MaybeUninit<T>; N]`, array of maybe uninit
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

impl<T, const N: usize> TrBuffer for [MaybeUninit<T>; N] {
    type Slot = MaybeUninit<T>;

    #[inline]
    fn as_slice_uninit(
        &self,
    ) -> &[MaybeUninit<<Self::Slot as TrMaybeUninit>::Inner>] {
        self.as_ref()
    }
}

impl<T, const N: usize> TrBufferMut for [MaybeUninit<T>; N] {
    #[inline]
    fn as_mut_slice_uninit(
        &mut self,
    ) -> &mut [MaybeUninit<<Self::Slot as TrMaybeUninit>::Inner>] {
        self.as_mut()
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// impl TrBuffer TrBufferMut for `MaybeUninit<[T; N]>` a maybe uninit array
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

impl<T, const N: usize> TrBuffer for MaybeUninit<[T; N]> {
    type Slot = MaybeUninit<T>;

    #[inline]
    fn as_slice_uninit(
        &self,
    ) -> &[MaybeUninit<<Self::Slot as TrMaybeUninit>::Inner>] {
        unsafe { mem::transmute(self.assume_init_ref().as_ref()) }
    }
}

impl<T, const N: usize> TrBufferMut for MaybeUninit<[T; N]> {
    #[inline]
    fn as_mut_slice_uninit(
        &mut self,
    ) -> &mut [MaybeUninit<<Self::Slot as TrMaybeUninit>::Inner>] {
        unsafe { mem::transmute(self.assume_init_mut().as_mut()) }
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// impl TrBuffer TrBufferMut for `&mut MaybeUninit<[T; N]>`
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

impl<T, const N: usize> TrBuffer for &mut MaybeUninit<[T; N]> {
    type Slot = MaybeUninit<T>;

    #[inline]
    fn as_slice_uninit(
        &self,
    ) -> &[MaybeUninit<<Self::Slot as TrMaybeUninit>::Inner>] {
        unsafe { mem::transmute(self.assume_init_ref().as_ref()) }
    }
}

impl<T, const N: usize> TrBufferMut for &mut MaybeUninit<[T; N]> {
    #[inline]
    fn as_mut_slice_uninit(
        &mut self,
    ) -> &mut [MaybeUninit<<Self::Slot as TrMaybeUninit>::Inner>] {
        unsafe { mem::transmute(self.assume_init_mut().as_mut()) }
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// impl TrBuffer for `&<[MaybeUninit<T>]>`
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

impl<T> TrBuffer for &[MaybeUninit<T>] {
    type Slot = MaybeUninit<T>;

    #[inline]
    fn as_slice_uninit(
        &self,
    ) -> &[MaybeUninit<<Self::Slot as TrMaybeUninit>::Inner>] {
        self
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// impl TrBuffer TrBufferMut for `&mut [MaybeUninit<T>]`
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

impl<T> TrBuffer for &mut [MaybeUninit<T>] {
    type Slot = MaybeUninit<T>;

    fn as_slice_uninit(
        &self,
    ) -> &[MaybeUninit<<Self::Slot as TrMaybeUninit>::Inner>] {
        self
    }
}

impl<T> TrBufferMut for &mut [MaybeUninit<T>] {
    #[inline]
    fn as_mut_slice_uninit(
        &mut self,
    ) -> &mut [MaybeUninit<<Self::Slot as TrMaybeUninit>::Inner>] {
        self
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// impl TrMaybeUninit for `MaybeUninit<T>`
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

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
        // self.as_bytes()
        // SAFETY: MaybeUninit<u8> is always valid, even for padding bytes
        unsafe {
            slice::from_raw_parts(
                self.as_ptr().cast::<MaybeUninit<u8>>(),
                mem::size_of::<T>(),
            )
        }
    }

    #[inline]
    fn as_bytes_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        // self.as_bytes_mut()
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
        unsafe {
            self.assume_init_drop();
        }
    }

    #[inline]
    fn write(&mut self, value: Self::Inner) -> &mut Self::Inner {
        MaybeUninit::write(self, value)
    }
}

#[cfg(test)]
mod tests_ {
    #[allow(unused)]
    use super::*;

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
