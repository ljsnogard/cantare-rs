use core::{
    borrow::{Borrow, BorrowMut},
    iter::IntoIterator,
};

use crate::{
    impl_items_ref_view_simple,
    impl_items_mut_view_simple,
    impl_items_views_simple,
};

/// Collections that can provide iterator accessing to the view in the form of
/// `Borrow`) of the its items.
///
/// Arrays `[T; N]`, slices `[T]` and `&[T]`, `Option<T>` and `Result<T>` 
/// implements this trait.
///
/// This is for things like `for<'a> &'a C: IntoIterator`
pub trait TrItemsRefView {
    type Item: ?Sized;

    fn items_ref_view(&self) -> impl IntoIterator<Item: Borrow<Self::Item>>;
}

/// Collections that can provide iterator accessing to the view in the form of
/// `BorrowMut`) of the its items.
///
/// Arrays `[T; N]`, slices `[T]` and `&[T]`, `Option<T>` and `Result<T>` 
/// implements this trait.
///
/// This is for things like `for<'a> &'a mut C: IntoIterator`
pub trait TrItemsMutView {
    type Item: ?Sized;

    fn items_mut_view(&mut self) -> impl IntoIterator<Item: BorrowMut<Self::Item>>;
}

impl_items_views_simple!(
    impl [<T, const N: usize>]
    for [T; N]
    => T
);
impl_items_views_simple!(
    impl [<T>]
    for [T]
    => T
);
impl_items_ref_view_simple!(
    impl [<T>]
    for &[T]
    => T
);
impl_items_mut_view_simple!(
    impl [<T>]
    for &mut [T]
    => T
);
impl_items_views_simple!(
    impl [<T>]
    for Option<T>
    => T
);
impl_items_views_simple!(
    impl [<T, E>]
    for Result<T, E>
    => T
);

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
