#[macro_export]
macro_rules! impl_items_ref_view_simple {
    (
        impl [$($impl_head:tt)*]
        for $ty:ty
        => $item:ty
    ) => {
        impl $($impl_head)* TrItemsRefView for $ty {
            type Item = $item;

            fn items_ref_view(&self) -> impl IntoIterator<Item: Borrow<Self::Item>> {
                self.iter()
            }
        }
    };
}

#[macro_export]
macro_rules! impl_items_mut_view_simple {
    (
        impl [$($impl_head:tt)*]
        for $ty:ty
        => $item:ty
    ) => {
        impl $($impl_head)* TrItemsMutView for $ty {
            type Item = $item;

            fn items_mut_view(&mut self) -> impl IntoIterator<Item: BorrowMut<Self::Item>> {
                self.iter_mut()
            }
        }
    };
}

#[macro_export]
macro_rules! impl_items_views_simple {
    (
        impl [$($impl_head:tt)*]
        for $ty:ty
        => $item:ty
    ) => {
        impl_items_ref_view_simple!(
            impl [$($impl_head)*]
            for $ty
            => $item
        );

        impl_items_mut_view_simple!(
            impl [$($impl_head)*]
            for $ty
            => $item
        );
    };
}