extern crate alloc;

use alloc::{
    collections::{BTreeSet, LinkedList, VecDeque},
    vec::Vec,
};
use core::{
    alloc::Allocator,
    borrow::{Borrow, BorrowMut},
    iter::IntoIterator,
};

use crate::{TrItemsMutView, TrItemsRefView};

// for BTreeSet

impl<T, A> TrItemsRefView for BTreeSet<T, A>
where
    A: Allocator + Clone,
{
    type Item = T;

    fn items_ref_view(&self) -> impl IntoIterator<Item: Borrow<Self::Item>> {
        self.iter()
    }
}

// for LinkedList

impl<T, A> TrItemsRefView for LinkedList<T, A>
where
    A: Allocator,
{
    type Item = T;

    fn items_ref_view(&self) -> impl IntoIterator<Item: Borrow<Self::Item>> {
        self.iter()
    }
}

impl<T, A> TrItemsMutView for LinkedList<T, A>
where
    A: Allocator,
{
    type Item = T;

    fn items_mut_view(
        &mut self,
    ) -> impl IntoIterator<Item: BorrowMut<Self::Item>> {
        self.iter_mut()
    }
}

// for Vec

impl<T, A> TrItemsRefView for Vec<T, A>
where
    A: Allocator,
{
    type Item = T;

    fn items_ref_view(&self) -> impl IntoIterator<Item: Borrow<Self::Item>> {
        self.iter()
    }
}

impl<T, A> TrItemsMutView for Vec<T, A>
where
    A: Allocator,
{
    type Item = T;

    fn items_mut_view(
        &mut self,
    ) -> impl IntoIterator<Item: BorrowMut<Self::Item>> {
        self.iter_mut()
    }
}

// for VecDeque

impl<T, A> TrItemsRefView for VecDeque<T, A>
where
    A: Allocator,
{
    type Item = T;

    fn items_ref_view(&self) -> impl IntoIterator<Item: Borrow<Self::Item>> {
        self.iter()
    }
}

impl<T, A> TrItemsMutView for VecDeque<T, A>
where
    A: Allocator,
{
    type Item = T;

    fn items_mut_view(
        &mut self,
    ) -> impl IntoIterator<Item: BorrowMut<Self::Item>> {
        self.iter_mut()
    }
}
