use core::ops::{Deref, DerefMut};

pub trait TrAcqRefGuard<'a, 'g, T>
where
    'a: 'g,
    Self: 'g + Sized + Deref<Target = T>,
    T: 'a + ?Sized
{}

pub trait TrAcqMutGuard<'a, 'g, T>
where
    'a: 'g,
    Self: 'g + Sized + DerefMut<Target = T> + TrAcqRefGuard<'a, 'g, T>,
    T: 'a + ?Sized,
{}