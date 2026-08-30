use core::mem::MaybeUninit;

pub trait TrAsBuffer<T> {
    /// Explicitly declare that the termination of evaluation for
    /// `TrMaybeUninit` be `core::mem::MaybeUninit`.
    fn as_slice_uninit(&self) -> &[MaybeUninit<T>];
}

pub trait TrAsBufferMut<T>
where
    Self: TrAsBuffer<T>,
{
    fn as_mut_slice_uninit(&mut self) -> &mut [MaybeUninit<T>];
}


//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// impl TrBuffer TrBufferMut for `[MaybeUninit<T>; N]`, array of maybe uninit
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

impl<T, const N: usize> TrAsBuffer<T> for [T; N]
where
    T: Copy,
{
    #[inline]
    fn as_slice_uninit(&self) -> &[MaybeUninit<T>] {
        let p = self.as_ref().as_ptr() as *const MaybeUninit<T>;
        unsafe { core::slice::from_raw_parts(p, N) }
    }
}

impl<T, const N: usize> TrAsBufferMut<T> for [T; N]
where
    T: Copy,
{
    #[inline]
    fn as_mut_slice_uninit(&mut self) -> &mut [MaybeUninit<T>] {
        let p = self.as_mut().as_ptr() as *mut T as *mut MaybeUninit<T>;
        unsafe { core::slice::from_raw_parts_mut(p, N) }
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// impl TrBuffer for `&<[MaybeUninit<T>]>`
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

impl<T> TrAsBuffer<T> for &[T]
where
    T: Copy,
{
    #[inline]
    fn as_slice_uninit(&self) -> &[MaybeUninit<T>] {
        let len = self.len();
        let data = self.as_ref().as_ptr() as *const MaybeUninit<T>;
        unsafe { core::slice::from_raw_parts(data, len) }
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// impl TrBuffer TrBufferMut for `&mut [MaybeUninit<T>]`
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

impl<T> TrAsBuffer<T> for &mut [T]
where
    T: Copy,
{
    #[inline]
    fn as_slice_uninit(&self) -> &[MaybeUninit<T>] {
        let len = self.len();
        let data = self.as_ref().as_ptr() as *const MaybeUninit<T>;
        unsafe { core::slice::from_raw_parts(data, len) }
    }
}

impl<T> TrAsBufferMut<T> for &mut [T]
where
    T: Copy,
{
    #[inline]
    fn as_mut_slice_uninit(&mut self) -> &mut [MaybeUninit<T>] {
        let len = self.len();
        let data = self.as_mut().as_ptr() as *mut MaybeUninit<T>;
        unsafe { core::slice::from_raw_parts_mut(data, len) }
    }
}
