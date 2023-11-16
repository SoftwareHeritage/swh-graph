/*
 * Copyright (C) 2023  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

//! Inserts items to a sorted iterator

pub trait Incrementable {
    fn increment(&mut self);
}

macro_rules! impl_incrementable {
    ($type: ty) => {
        impl Incrementable for $type {
            fn increment(&mut self) {
                *self += 1;
            }
        }
    };
}

impl_incrementable!(usize);
impl_incrementable!(u128);
impl_incrementable!(u64);
impl_incrementable!(u32);
impl_incrementable!(u16);
impl_incrementable!(u8);
impl_incrementable!(isize);
impl_incrementable!(i128);
impl_incrementable!(i64);
impl_incrementable!(i32);
impl_incrementable!(i16);
impl_incrementable!(i8);

impl<T: Incrementable> Incrementable for &mut T {
    fn increment(&mut self) {
        (*self).increment()
    }
}

pub trait ToExhaustiveIterator<Item>: Iterator<Item = Item> + Sized {
    /// Returns an iterator such that `.exhaustive().map(f).collect() == min_key..max_key`
    ///
    /// In other words, the resulting iterator will contain, up to `f`, every value
    /// from `min_key` to `max_key` exactly once and in order.
    ///
    /// The original iterator **must** be sorted according to `f` and must not contain
    /// values such that `f` would be outside the `[min_key, max_key)` range.
    ///
    /// `default` is passed missing keys and returns a filler value.
    ///
    /// # Examples
    ///
    /// ```
    /// use swh_graph::utils::exhaustiveiterator::ToExhaustiveIterator;
    ///
    /// assert_eq!(
    ///     [2, 4, 6].into_iter().exhaustive(0, 8, |&v| v, |k| k).collect::<Vec<_>>(),
    ///     vec![0, 1, 2, 3, 4, 5, 6, 7],
    /// );
    /// assert_eq!(
    ///     [1, 4].into_iter().exhaustive(0, 5, |&v| v, |k| k).collect::<Vec<_>>(),
    ///     vec![0, 1, 2, 3, 4],
    /// );
    ///
    /// // Completes a graph's adjacency list
    /// assert_eq!(
    ///     [(1u32, vec![2u32, 4]), (2, vec![4]), (4, vec![1])]
    ///         .into_iter()
    ///         .exhaustive(0, 5, |(node, _neighbors)| *node, |k| (k, vec![]))
    ///         .collect::<Vec<_>>(),
    ///     [(0, vec![]), (1, vec![2, 4]), (2, vec![4]), (3, vec![]), (4, vec![1])],
    /// );
    ///
    /// // No-op if the list is already complete
    /// assert_eq!(
    ///     [(1, vec![2, 4]), (2, vec![4]), (3, vec![]), (4, vec![1])]
    ///         .into_iter()
    ///         .exhaustive(1, 5, |(node, _neighbors)| *node, |k| (k, vec![]))
    ///         .collect::<Vec<_>>(),
    ///     [(1, vec![2, 4]), (2, vec![4]), (3, vec![]), (4, vec![1])],
    /// );
    /// ```
    fn exhaustive<Key: Incrementable + Eq + Ord + Clone + Sized, F, D>(
        mut self,
        min_key: Key,
        max_key: Key,
        key: F,
        default: D,
    ) -> ExhaustiveIterator<Self, Key, F, D>
    where
        F: Fn(&Item) -> Key,
        D: Fn(Key) -> Item,
    {
        let first_item = self.next();
        if let Some(first_item) = first_item.as_ref() {
            assert!(
                key(first_item) >= min_key,
                "exhaustive()'s first value is lower than min_key"
            );
        }
        ExhaustiveIterator {
            key,
            iter: self,
            default,
            current_key: min_key,
            max_key,
            next_item: first_item,
        }
    }
}

impl<Iter: Iterator> ToExhaustiveIterator<Iter::Item> for Iter {}

pub struct ExhaustiveIterator<
    Iter: Iterator + Sized,
    Key: Eq + Clone + Sized,
    F: Fn(&Iter::Item) -> Key,
    D: Fn(Key) -> Iter::Item,
> {
    key: F,
    iter: Iter,
    default: D,
    current_key: Key,
    max_key: Key,
    next_item: Option<Iter::Item>,
}

impl<
        Iter: Iterator,
        Key: Incrementable + Eq + Ord + Clone + Sized,
        F: Fn(&Iter::Item) -> Key,
        D: Fn(Key) -> Iter::Item,
    > Iterator for ExhaustiveIterator<Iter, Key, F, D>
{
    type Item = Iter::Item;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_key == self.max_key {
            assert!(
                self.next_item.is_none(),
                "ExhaustiveIterator's input iterator is too long"
            );
            return None;
        }
        if self.next_item.is_some()
            && self.current_key == (self.key)(self.next_item.as_ref().unwrap())
        {
            // These two lines write the inner iterator's next value to self.next_item,
            // and move the previous self.next_item to the local 'next_item' without
            // violating ownership
            let mut next_item = self.iter.next();
            std::mem::swap(&mut next_item, &mut self.next_item);

            // TODO: see if this check affects performance too much
            if let Some(next_next_item) = self.next_item.as_ref() {
                assert!(
                    (self.key)(next_next_item) > (self.key)(next_item.as_ref().unwrap()),
                    "the iterator is not strictly increasing"
                );
                assert!(
                    (self.key)(next_next_item) < self.max_key,
                    "the iterator returned a value larger than max_key"
                );
            }
            self.current_key.increment();
            next_item
        } else {
            let res = Some((self.default)(self.current_key.clone()));
            self.current_key.increment();
            res
        }
    }
}
