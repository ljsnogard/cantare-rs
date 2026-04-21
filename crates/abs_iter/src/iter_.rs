use core::{
    borrow::{Borrow, BorrowMut},
    iter::IntoIterator,
};

/// Collections that can provide iterator accessing to the view in the form of
/// `Borrow`) of the its items.
///
/// Arrays `[T; N]`, slices `[T]` and `&[T]`, `Option<T>` and `Result<T>` 
/// implements this trait.
pub trait TrItemsRefView {
    type Item: ?Sized;

    fn items_ref_view(&self) -> impl IntoIterator<Item: Borrow<Self::Item>>;
}

/// Collections that can provide iterator accessing to the view in the form of
/// `BorrowMut`) of the its items.
///
/// Arrays `[T; N]`, slices `[T]` and `&[T]`, `Option<T>` and `Result<T>` 
/// implements this trait.
pub trait TrItemsMutView {
    type Item: ?Sized;

    fn items_mut_view(
        &mut self,
    ) -> impl IntoIterator<Item: BorrowMut<Self::Item>>;
}

// for array [T; N]

impl<T, const N: usize> TrItemsRefView for [T; N] {
    type Item = T;

    fn items_ref_view(&self) -> impl IntoIterator<Item: Borrow<Self::Item>> {
        self.iter()
    }
}

impl<T, const N: usize> TrItemsMutView for [T; N] {
    type Item = T;

    fn items_mut_view(
        &mut self,
    ) -> impl IntoIterator<Item: BorrowMut<Self::Item>> {
        self.iter_mut()
    }
}

// --- for slice [T]

impl<T> TrItemsRefView for [T] {
    type Item = T;

    fn items_ref_view(&self) -> impl IntoIterator<Item: Borrow<Self::Item>> {
        self.iter()
    }
}

impl<T> TrItemsMutView for [T] {
    type Item = T;

    fn items_mut_view(
        &mut self,
    ) -> impl IntoIterator<Item: BorrowMut<Self::Item>> {
        self.iter_mut()
    }
}

// --- for slice &[T]

impl<T> TrItemsRefView for &[T] {
    type Item = T;

    fn items_ref_view(&self) -> impl IntoIterator<Item: Borrow<Self::Item>> {
        self.iter()
    }
}

impl<T> TrItemsMutView for &mut [T] {
    type Item = T;

    fn items_mut_view(
        &mut self,
    ) -> impl IntoIterator<Item: BorrowMut<Self::Item>> {
        self.iter_mut()
    }
}

// --- for Option<T>

impl<T> TrItemsRefView for Option<T> {
    type Item = T;

    fn items_ref_view(&self) -> impl IntoIterator<Item: Borrow<Self::Item>> {
        self.iter()
    }
}

impl<T> TrItemsMutView for Option<T> {
    type Item = T;

    fn items_mut_view(
        &mut self,
    ) -> impl IntoIterator<Item: BorrowMut<Self::Item>> {
        self.iter_mut()
    }
}

// --- for Result<T, E>

impl<T, E> TrItemsRefView for Result<T, E> {
    type Item = T;

    fn items_ref_view(&self) -> impl IntoIterator<Item: Borrow<Self::Item>> {
        self.iter()
    }
}

impl<T, E> TrItemsMutView for Result<T, E> {
    type Item = T;

    fn items_mut_view(
        &mut self,
    ) -> impl IntoIterator<Item: BorrowMut<Self::Item>> {
        self.iter_mut()
    }
}


// for str

impl TrItemsRefView for str {
    type Item = str;

    fn items_ref_view(&self) -> impl IntoIterator<Item: Borrow<Self::Item>> {
        let mut chars = self.char_indices().peekable();

        core::iter::from_fn(move || {
            let (start, _) = chars.next()?;
            let end = chars.peek().map(|(i, _)| *i).unwrap_or(self.len());
            Some(&self[start..end])
        })
    }
}
