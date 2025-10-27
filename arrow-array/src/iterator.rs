// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Idiomatic iterators for [`Array`](crate::Array)

use std::iter::Skip;
use crate::array::{
    ArrayAccessor, BooleanArray, FixedSizeBinaryArray, GenericBinaryArray, GenericListArray,
    GenericStringArray, PrimitiveArray,
};
use crate::{FixedSizeListArray, GenericListViewArray, MapArray};
use arrow_buffer::NullBuffer;

/// An iterator that returns Some(T) or None, that can be used on any [`ArrayAccessor`]
///
/// # Performance
///
/// [`ArrayIter`] provides an idiomatic way to iterate over an array, however, this
/// comes at the cost of performance. In particular the interleaved handling of
/// the null mask is often sub-optimal.
///
/// If performing an infallible operation, it is typically faster to perform the operation
/// on every index of the array, and handle the null mask separately. For [`PrimitiveArray`]
/// this functionality is provided by [`compute::unary`]
///
/// If performing a fallible operation, it isn't possible to perform the operation independently
/// of the null mask, as this might result in a spurious failure on a null index. However,
/// there are more efficient ways to iterate over just the non-null indices, this functionality
/// is provided by [`compute::try_unary`]
///
/// [`PrimitiveArray`]: crate::PrimitiveArray
/// [`compute::unary`]: https://docs.rs/arrow/latest/arrow/compute/fn.unary.html
/// [`compute::try_unary`]: https://docs.rs/arrow/latest/arrow/compute/fn.try_unary.html
#[derive(Debug, Clone)]
pub struct ArrayIter<T: ArrayAccessor> {
    array: T,
    logical_nulls: Option<NullBuffer>,
    current: usize,
    current_end: usize,
}

impl<T: ArrayAccessor> ArrayIter<T> {
    /// create a new iterator
    pub fn new(array: T) -> Self {
        let len = array.len();
        let logical_nulls = array.logical_nulls().filter(|x| x.null_count() > 0);
        ArrayIter {
            array,
            logical_nulls,
            current: 0,
            current_end: len,
        }
    }

    #[inline]
    fn is_null(&self, idx: usize) -> bool {
        self.logical_nulls
            .as_ref()
            .map(|x| x.is_null(idx))
            .unwrap_or_default()
    }

    /// Get iterator when there are nulls
    ///
    /// # Panics
    /// If `self.logical_nulls` is `None`
    #[inline]
    fn get_iterator_for_nullable(&mut self) -> impl Iterator<Item = <Self as Iterator>::Item> {
        self.logical_nulls
            .as_ref()
            .expect("logical_nulls must exists when this function is called")
            .iter()
            .skip(self.current)
            .take(self.current_end - self.current)
            .map(|is_valid| {
                if is_valid {
                    // Safety:
                    // we are in bounds as i < self.array.len()
                    let value = unsafe { self.array.value_unchecked(self.current) };
                    self.current += 1;
                    Some(value)
                } else {
                    self.current += 1;
                    None
                }
            })
    }

    /// Get iterator when there are no null
    ///
    /// # Panics
    /// If `self.logical_nulls` is `Some(_)`
    #[inline]
    fn get_iterator_for_non_nullable(&mut self) -> impl Iterator<Item = <Self as Iterator>::Item> {
        assert_eq!(
            self.logical_nulls, None,
            "logical_nulls must be None when this function is called"
        );

        // Skip null checks
        (self.current..self.current_end).map(|_| {
            debug_assert!(
                !self.is_null(self.current),
                "Value at index {} is null for non_nullable",
                self.current
            );

            // Safety:
            // we are in bounds as self.current < self.array.len()
            let val = unsafe { self.array.value_unchecked(self.current) };

            self.current += 1;

            Some(val)
        })
    }

    /// Get reverse iterator when there are nulls
    ///
    /// # Panics
    /// If `self.logical_nulls` is `None`
    #[inline]
    fn get_reverse_iterator_for_nullable(
        &mut self,
    ) -> impl Iterator<Item = <Self as Iterator>::Item> {
        self.logical_nulls
            .as_ref()
            .expect("logical_nulls must exists when this function is called")
            .iter()
            .rev()
            .skip(self.array.len() - self.current_end)
            .take(self.current_end - self.current)
            .map(|is_valid| {
                self.current_end -= 1;

                if is_valid {
                    // Safety:
                    // we are in bounds as i < self.array.len()
                    let value = unsafe { self.array.value_unchecked(self.current_end) };
                    Some(value)
                } else {
                    None
                }
            })
    }

    /// Get reverse iterator when there are no null
    ///
    /// # Panics
    /// If `self.logical_nulls` is `Some(_)`
    #[inline]
    fn get_reverse_iterator_for_non_nullable(
        &mut self,
    ) -> impl Iterator<Item = <Self as Iterator>::Item> {
        assert_eq!(
            self.logical_nulls, None,
            "logical_nulls must be None when this function is called"
        );

        // Skip null checks
        (self.current..self.current_end).map(|_| {
            self.current_end -= 1;

            debug_assert!(
                !self.is_null(self.current_end),
                "Value at index {} is null for non_nullable",
                self.current_end
            );

            // Safety:
            // we are in bounds as self.current < self.array.len()
            let val = unsafe { self.array.value_unchecked(self.current_end) };

            Some(val)
        })
    }
}

impl<T: ArrayAccessor> Iterator for ArrayIter<T> {
    type Item = Option<T::Item>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.current == self.current_end {
            None
        } else if self.is_null(self.current) {
            self.current += 1;
            Some(None)
        } else {
            let old = self.current;
            self.current += 1;
            // Safety:
            // we just checked bounds in `self.current_end == self.current`
            // this is safe on the premise that this struct is initialized with
            // current = array.len()
            // and that current_end is ever only decremented
            unsafe { Some(Some(self.array.value_unchecked(old))) }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.array.len() - self.current,
            Some(self.array.len() - self.current),
        )
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        // Check if we can advance to the desired offset
        match self.current.checked_add(n) {
            // Yes, and still within bounds
            Some(new_current) if new_current < self.current_end => {
                self.current = new_current;
            }

            // Either overflow or would exceed current_end
            _ => {
                self.current = self.current_end;
                return None;
            }
        }

        self.next()
    }

    fn last(mut self) -> Option<Self::Item> {
        // If already at the end, return None
        if self.current == self.current_end {
            return None;
        }

        // Go to the one before the last value
        self.current = self.current_end - 1;

        // Return the last value
        self.next()
    }

    fn count(self) -> usize
    where
        Self: Sized,
    {
        self.len()
    }

    fn for_each<F>(mut self, f: F)
    where
        Self: Sized,
        F: FnMut(Self::Item),
    {
        if self.logical_nulls.is_some() {
            self.get_iterator_for_nullable().for_each(f)
        } else {
            self.get_iterator_for_non_nullable().for_each(f)
        }
    }

    fn fold<B, F>(mut self, init: B, f: F) -> B
    where
        Self: Sized,
        F: FnMut(B, Self::Item) -> B,
    {
        if self.logical_nulls.is_some() {
            self.get_iterator_for_nullable().fold(init, f)
        } else {
            self.get_iterator_for_non_nullable().fold(init, f)
        }
    }

    fn all<F>(&mut self, f: F) -> bool
    where
        Self: Sized,
        F: FnMut(Self::Item) -> bool,
    {
        if self.logical_nulls.is_some() {
            self.get_iterator_for_nullable().all(f)
        } else {
            self.get_iterator_for_non_nullable().all(f)
        }
    }

    fn any<F>(&mut self, f: F) -> bool
    where
        Self: Sized,
        F: FnMut(Self::Item) -> bool,
    {
        if self.logical_nulls.is_some() {
            self.get_iterator_for_nullable().any(f)
        } else {
            self.get_iterator_for_non_nullable().any(f)
        }
    }

    fn find_map<B, F>(&mut self, f: F) -> Option<B>
    where
        Self: Sized,
        F: FnMut(Self::Item) -> Option<B>,
    {
        if self.logical_nulls.is_some() {
            self.get_iterator_for_nullable().find_map(f)
        } else {
            self.get_iterator_for_non_nullable().find_map(f)
        }
    }

    fn find<P>(&mut self, predicate: P) -> Option<Self::Item>
    where
        Self: Sized,
        P: FnMut(&Self::Item) -> bool,
    {
        if self.logical_nulls.is_some() {
            self.get_iterator_for_nullable().find(predicate)
        } else {
            self.get_iterator_for_non_nullable().find(predicate)
        }
    }

    fn partition<B, F>(mut self, f: F) -> (B, B)
    where
        Self: Sized,
        B: Default + Extend<Self::Item>,
        F: FnMut(&Self::Item) -> bool,
    {
        if self.logical_nulls.is_some() {
            self.get_iterator_for_nullable().partition(f)
        } else {
            self.get_iterator_for_non_nullable().partition(f)
        }
    }

    fn position<P>(&mut self, predicate: P) -> Option<usize>
    where
        Self: Sized,
        P: FnMut(Self::Item) -> bool,
    {
        if self.logical_nulls.is_some() {
            self.get_iterator_for_nullable().position(predicate)
        } else {
            self.get_iterator_for_non_nullable().position(predicate)
        }
    }

    fn rposition<P>(&mut self, predicate: P) -> Option<usize>
    where
        P: FnMut(Self::Item) -> bool,
        Self: Sized + ExactSizeIterator + DoubleEndedIterator,
    {
        if self.logical_nulls.is_some() {
            self.get_reverse_iterator_for_nullable().position(predicate)
        } else {
            self.get_reverse_iterator_for_non_nullable()
                .position(predicate)
        }
    }
}

impl<T: ArrayAccessor> DoubleEndedIterator for ArrayIter<T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.current_end == self.current {
            None
        } else {
            self.current_end -= 1;
            Some(if self.is_null(self.current_end) {
                None
            } else {
                // Safety:
                // we just checked bounds in `self.current_end == self.current`
                // this is safe on the premise that this struct is initialized with
                // current = array.len()
                // and that current_end is ever only decremented
                unsafe { Some(self.array.value_unchecked(self.current_end)) }
            })
        }
    }

    fn nth_back(&mut self, n: usize) -> Option<Self::Item> {
        // Check if we advance to the one before the desired offset
        match self.current_end.checked_sub(n) {
            // Yes, and still within bounds
            Some(new_offset) if self.current < new_offset => {
                self.current_end = new_offset;
            }

            // Either underflow or would exceed current
            _ => {
                self.current = self.current_end;
                return None;
            }
        }

        self.next_back()
    }

    fn rfold<B, F>(mut self, init: B, f: F) -> B
    where
        Self: Sized,
        F: FnMut(B, Self::Item) -> B,
    {
        if self.logical_nulls.is_some() {
            self.get_reverse_iterator_for_nullable()
                .fold(init, f)
        } else {
            self.get_reverse_iterator_for_non_nullable()
                .fold(init, f)
        }
    }

    fn rfind<P>(&mut self, predicate: P) -> Option<Self::Item>
    where
        Self: Sized,
        P: FnMut(&Self::Item) -> bool,
    {
        if self.logical_nulls.is_some() {
            self.get_reverse_iterator_for_nullable().find(predicate)
        } else {
            self.get_reverse_iterator_for_non_nullable().find(predicate)
        }
    }
}

/// all arrays have known size.
impl<T: ArrayAccessor> ExactSizeIterator for ArrayIter<T> {}

/// an iterator that returns Some(T) or None, that can be used on any PrimitiveArray
pub type PrimitiveIter<'a, T> = ArrayIter<&'a PrimitiveArray<T>>;
/// an iterator that returns Some(T) or None, that can be used on any BooleanArray
pub type BooleanIter<'a> = ArrayIter<&'a BooleanArray>;
/// an iterator that returns Some(T) or None, that can be used on any Utf8Array
pub type GenericStringIter<'a, T> = ArrayIter<&'a GenericStringArray<T>>;
/// an iterator that returns Some(T) or None, that can be used on any BinaryArray
pub type GenericBinaryIter<'a, T> = ArrayIter<&'a GenericBinaryArray<T>>;
/// an iterator that returns Some(T) or None, that can be used on any FixedSizeBinaryArray
pub type FixedSizeBinaryIter<'a> = ArrayIter<&'a FixedSizeBinaryArray>;
/// an iterator that returns Some(T) or None, that can be used on any FixedSizeListArray
pub type FixedSizeListIter<'a> = ArrayIter<&'a FixedSizeListArray>;
/// an iterator that returns Some(T) or None, that can be used on any ListArray
pub type GenericListArrayIter<'a, O> = ArrayIter<&'a GenericListArray<O>>;
/// an iterator that returns Some(T) or None, that can be used on any MapArray
pub type MapArrayIter<'a> = ArrayIter<&'a MapArray>;
/// an iterator that returns Some(T) or None, that can be used on any ListArray
pub type GenericListViewArrayIter<'a, O> = ArrayIter<&'a GenericListViewArray<O>>;
#[cfg(test)]
mod tests {
    use std::fmt::Debug;
    use std::iter::Copied;
    use std::slice::Iter;
    use std::sync::Arc;
    use hashbrown::HashSet;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use crate::array::{ArrayRef, BinaryArray, BooleanArray, Int32Array, StringArray};
    use crate::ArrayAccessor;
    use crate::iterator::ArrayIter;

    #[test]
    fn test_primitive_array_iter_round_trip() {
        let array = Int32Array::from(vec![Some(0), None, Some(2), None, Some(4)]);
        let array = Arc::new(array) as ArrayRef;

        let array = array.as_any().downcast_ref::<Int32Array>().unwrap();

        // to and from iter, with a +1
        let result: Int32Array = array.iter().map(|e| e.map(|e| e + 1)).collect();

        let expected = Int32Array::from(vec![Some(1), None, Some(3), None, Some(5)]);
        assert_eq!(result, expected);

        // check if DoubleEndedIterator is implemented
        let result: Int32Array = array.iter().rev().collect();
        let rev_array = Int32Array::from(vec![Some(4), None, Some(2), None, Some(0)]);
        assert_eq!(result, rev_array);
        // check if ExactSizeIterator is implemented
        let _ = array.iter().rposition(|opt_b| opt_b == Some(1));
    }

    #[test]
    fn test_double_ended() {
        let array = Int32Array::from(vec![Some(0), None, Some(2), None, Some(4)]);
        let mut a = array.iter();
        assert_eq!(a.next(), Some(Some(0)));
        assert_eq!(a.next(), Some(None));
        assert_eq!(a.next_back(), Some(Some(4)));
        assert_eq!(a.next_back(), Some(None));
        assert_eq!(a.next_back(), Some(Some(2)));
        // the two sides have met: None is returned by both
        assert_eq!(a.next_back(), None);
        assert_eq!(a.next(), None);
    }

    #[test]
    fn test_string_array_iter_round_trip() {
        let array = StringArray::from(vec![Some("a"), None, Some("aaa"), None, Some("aaaaa")]);
        let array = Arc::new(array) as ArrayRef;

        let array = array.as_any().downcast_ref::<StringArray>().unwrap();

        // to and from iter, with a +1
        let result: StringArray = array
            .iter()
            .map(|e| {
                e.map(|e| {
                    let mut a = e.to_string();
                    a.push('b');
                    a
                })
            })
            .collect();

        let expected =
            StringArray::from(vec![Some("ab"), None, Some("aaab"), None, Some("aaaaab")]);
        assert_eq!(result, expected);

        // check if DoubleEndedIterator is implemented
        let result: StringArray = array.iter().rev().collect();
        let rev_array = StringArray::from(vec![Some("aaaaa"), None, Some("aaa"), None, Some("a")]);
        assert_eq!(result, rev_array);
        // check if ExactSizeIterator is implemented
        let _ = array.iter().rposition(|opt_b| opt_b == Some("a"));
    }

    #[test]
    fn test_binary_array_iter_round_trip() {
        let array = BinaryArray::from(vec![
            Some(b"a" as &[u8]),
            None,
            Some(b"aaa"),
            None,
            Some(b"aaaaa"),
        ]);

        // to and from iter
        let result: BinaryArray = array.iter().collect();

        assert_eq!(result, array);

        // check if DoubleEndedIterator is implemented
        let result: BinaryArray = array.iter().rev().collect();
        let rev_array = BinaryArray::from(vec![
            Some(b"aaaaa" as &[u8]),
            None,
            Some(b"aaa"),
            None,
            Some(b"a"),
        ]);
        assert_eq!(result, rev_array);

        // check if ExactSizeIterator is implemented
        let _ = array.iter().rposition(|opt_b| opt_b == Some(&[9]));
    }

    #[test]
    fn test_boolean_array_iter_round_trip() {
        let array = BooleanArray::from(vec![Some(true), None, Some(false)]);

        // to and from iter
        let result: BooleanArray = array.iter().collect();

        assert_eq!(result, array);

        // check if DoubleEndedIterator is implemented
        let result: BooleanArray = array.iter().rev().collect();
        let rev_array = BooleanArray::from(vec![Some(false), None, Some(true)]);
        assert_eq!(result, rev_array);

        // check if ExactSizeIterator is implemented
        let _ = array.iter().rposition(|opt_b| opt_b == Some(true));
    }

    // fn assert_same(primitive_arr: Int32Array, source: &[Option<i32>]) {
    //
    //     {
    //         let primitive_iter = primitive_arr.iter();
    //         let source_iter = source.iter();
    //
    //         let mut calls = vec![];
    //
    //         primitive_iter.for_each(|opt_b| {
    //             calls.push(opt_b);
    //         });
    //
    //         source_iter.for_each(|opt_b| {
    //             assert_eq!(calls.pop().unwrap(), *opt_b);
    //         });
    //
    //         assert_eq!(calls.len(), 0);
    //     }
    //
    //     {
    //         let primitive_iter = primitive_arr.iter();
    //         let source_iter = source.iter();
    //         let mut calls = vec![];
    //
    //         primitive_iter.fold(|opt_b| {
    //             calls.push(opt_b);
    //         });
    //
    //         source_iter.for_each(|opt_b| {
    //             assert_eq!(calls.pop().unwrap(), *opt_b);
    //         });
    //
    //         assert_eq!(calls.len(), 0);
    //     }
    // }

    trait SharedBetweenArrayIterAndSliceIter:
    ExactSizeIterator<Item = Option<i32>> + DoubleEndedIterator<Item = Option<i32>> + Clone
    {
    }
    impl<T: ?Sized + Clone + ExactSizeIterator<Item = Option<i32>> + DoubleEndedIterator<Item = Option<i32>>>
    SharedBetweenArrayIterAndSliceIter for T
    {
    }

    fn get_base_int32_iterator_cases() -> impl Iterator<Item = (Int32Array, Vec<Option<i32>>)> {
        let mut rng = StdRng::seed_from_u64(42);

        let no_nulls_and_no_duplicates = (0..20).map(Some).collect::<Vec<Option<i32>>>();
        let no_nulls_random_values = (0..20).map(|_| rng.random::<i32>()).map(Some).collect::<Vec<Option<i32>>>();

        let all_nulls = (0..20).map(|_| None).collect::<Vec<Option<i32>>>();
        let only_start_nulls = (0..20).map(|item| if item < 7 {None} else {Some(item)}).collect::<Vec<Option<i32>>>();
        let only_end_nulls = (0..20).map(|item| if item > 13 {None} else {Some(item)}).collect::<Vec<Option<i32>>>();
        let only_middle_nulls = (0..20).map(|item| if item >= 7 && item <= 13 && rng.random_bool(0.9) {None} else {Some(item)}).collect::<Vec<Option<i32>>>();
        let random_values_with_random_nulls = (0..20).map(|_| {
            if rng.random_bool(0.3) {
                None
            } else {
                Some(rng.random::<i32>())
            }
        }).collect::<Vec<Option<i32>>>();

        [
            no_nulls_and_no_duplicates,
            no_nulls_random_values,
            all_nulls,
            only_start_nulls,
            only_end_nulls,
            only_middle_nulls,
            random_values_with_random_nulls
        ]
          .map(|case| (Int32Array::from(case.clone()), case))
          .into_iter()
    }

    fn get_int32_iterator_cases_with_duplicates() -> impl Iterator<Item = (Int32Array, Vec<Option<i32>>)> {
        let no_nulls_and_some_duplicates = (0..20).map(|item| item % 3).map(Some).collect::<Vec<Option<i32>>>();
        let no_nulls_and_all_same_value = (0..20).map(|_| 17).map(Some).collect::<Vec<Option<i32>>>();
        let no_nulls_and_continues_duplicates = [0, 0, 0, 1, 1, 2, 2, 2, 2, 3].map(Some).into_iter().collect::<Vec<Option<i32>>>();

        let single_null_and_no_duplicates = (0..20).map(|item| if item == 4 {None} else {Some(item)}).collect::<Vec<Option<i32>>>();
        let multiple_nulls_and_no_duplicates = (0..20).map(|item| if item % 3 == 2 {None} else {Some(item)}).collect::<Vec<Option<i32>>>();
        let continues_nulls_and_no_duplicates = [Some(0), Some(1), None, None, Some(2), Some(3), None, Some(4), Some(5), None].into_iter().collect::<Vec<Option<i32>>>();

        [
            no_nulls_and_some_duplicates,
            no_nulls_and_all_same_value,
            no_nulls_and_continues_duplicates,
            single_null_and_no_duplicates,
            multiple_nulls_and_no_duplicates,
            continues_nulls_and_no_duplicates,
        ]
          .map(|case| (Int32Array::from(case.clone()), case))
          .into_iter()
          .chain(get_base_int32_iterator_cases())
    }

    fn setup_and_assert_base_cases(
        setup_iters: impl Fn(&mut dyn SharedBetweenArrayIterAndSliceIter),
        assert_fn: impl Fn(ArrayIter<&Int32Array>, Copied<Iter<Option<i32>>>),
    ) {
        for (array, source) in get_base_int32_iterator_cases() {
            let mut actual = ArrayIter::new(&array);
            let mut expected = source.iter().copied();

            setup_iters(&mut actual);
            setup_iters(&mut expected);

            assert_fn(actual, expected);
        }
    }

    fn setup_and_assert_extended_cases(
        setup_iters: impl Fn(&mut dyn SharedBetweenArrayIterAndSliceIter),
        assert_fn: impl Fn(ArrayIter<&Int32Array>, Copied<Iter<Option<i32>>>),
    ) {
        for (array, source) in get_int32_iterator_cases_with_duplicates() {
            let mut actual = ArrayIter::new(&array);
            let mut expected = source.iter().copied();

            setup_iters(&mut actual);
            setup_iters(&mut expected);

            assert_fn(actual, expected);
        }
    }

    /// Trait representing an operation on a BitIterator
    /// that can be compared against a slice iterator
    trait ArrayIteratorOp {
        /// What the operation returns (e.g. Option<bool> for last/max, usize for count, etc)
        type Output: PartialEq + Debug;

        /// The name of the operation, used for error messages
        const NAME: &'static str;

        /// Get the value of the operation for the provided iterator
        /// This will be either a BitIterator or a slice iterator to make sure they produce the same result
        fn get_value<T: SharedBetweenArrayIterAndSliceIter>(iter: T) -> Self::Output;
    }

    /// Helper function that will assert that the provided operation
    /// produces the same result for both BitIterator and slice iterator
    /// under various consumption patterns (e.g. some calls to next/next_back/consume_all/etc)
    fn assert_array_iterator_cases<O: ArrayIteratorOp>() {
        setup_and_assert_base_cases(
            |_iter: &mut dyn SharedBetweenArrayIterAndSliceIter| {},
            |actual, expected| {
                let current_iterator_values: Vec<Option<i32>> = expected.clone().collect();
                assert_eq!(
                    O::get_value(actual),
                    O::get_value(expected),
                    "Failed on op {} for new iter (left actual, right expected) ({current_iterator_values:?})",
                    O::NAME
                );
            },
        );

        setup_and_assert_extended_cases(
            |iter: &mut dyn SharedBetweenArrayIterAndSliceIter| {
                iter.next();
            },
            |actual, expected| {
                let current_iterator_values: Vec<Option<i32>> = expected.clone().collect();

                assert_eq!(
                    O::get_value(actual),
                    O::get_value(expected),
                    "Failed on op {} for new iter after consuming 1 element from the start (left actual, right expected) ({current_iterator_values:?})",
                    O::NAME
                );
            },
        );

        setup_and_assert_extended_cases(
            |iter: &mut dyn SharedBetweenArrayIterAndSliceIter| {
                iter.next_back();
            },
            |actual, expected| {
                let current_iterator_values: Vec<Option<i32>> = expected.clone().collect();

                assert_eq!(
                    O::get_value(actual),
                    O::get_value(expected),
                    "Failed on op {} for new iter after consuming 1 element from the end (left actual, right expected) ({current_iterator_values:?})",
                    O::NAME
                );
            },
        );

        setup_and_assert_extended_cases(
            |iter: &mut dyn SharedBetweenArrayIterAndSliceIter| {
                iter.next();
                iter.next_back();
            },
            |actual, expected| {
                let current_iterator_values: Vec<Option<i32>> = expected.clone().collect();

                assert_eq!(
                    O::get_value(actual),
                    O::get_value(expected),
                    "Failed on op {} for new iter after consuming 1 element from start and end (left actual, right expected) ({current_iterator_values:?})",
                    O::NAME
                );
            },
        );

        setup_and_assert_extended_cases(
            |iter: &mut dyn SharedBetweenArrayIterAndSliceIter| {
                while iter.len() > 1 {
                    iter.next();
                }
            },
            |actual, expected| {
                let current_iterator_values: Vec<Option<i32>> = expected.clone().collect();

                assert_eq!(
                    O::get_value(actual),
                    O::get_value(expected),
                    "Failed on op {} for new iter after consuming all from the start but 1 (left actual, right expected) ({current_iterator_values:?})",
                    O::NAME
                );
            },
        );

        setup_and_assert_extended_cases(
            |iter: &mut dyn SharedBetweenArrayIterAndSliceIter| {
                while iter.len() > 1 {
                    iter.next_back();
                }
            },
            |actual, expected| {
                let current_iterator_values: Vec<Option<i32>> = expected.clone().collect();

                assert_eq!(
                    O::get_value(actual),
                    O::get_value(expected),
                    "Failed on op {} for new iter after consuming all from the end but 1 (left actual, right expected) ({current_iterator_values:?})",
                    O::NAME
                );
            },
        );

        setup_and_assert_extended_cases(
            |iter: &mut dyn SharedBetweenArrayIterAndSliceIter| {
                while iter.next().is_some() {}
            },
            |actual, expected| {
                let current_iterator_values: Vec<Option<i32>> = expected.clone().collect();

                assert_eq!(
                    O::get_value(actual),
                    O::get_value(expected),
                    "Failed on op {} for new iter after consuming all from the start (left actual, right expected) ({current_iterator_values:?})",
                    O::NAME
                );
            },
        );

        setup_and_assert_extended_cases(
            |iter: &mut dyn SharedBetweenArrayIterAndSliceIter| {
                while iter.next_back().is_some() {}
            },
            |actual, expected| {
                let current_iterator_values: Vec<Option<i32>> = expected.clone().collect();

                assert_eq!(
                    O::get_value(actual),
                    O::get_value(expected),
                    "Failed on op {} for new iter after consuming all from the end (left actual, right expected) ({current_iterator_values:?})",
                    O::NAME
                );
            },
        );

        setup_and_assert_extended_cases(
            |iter: &mut dyn SharedBetweenArrayIterAndSliceIter| {
                // Until last null
                {
                    let last_null_position = iter.clone().rposition(|item| item == None);

                    // move the iterator to the location where there are no nulls anymore
                    if let Some(last_null_position) = last_null_position {
                        iter.nth(last_null_position);
                    }
                }
            },
            |actual, expected| {
                let current_iterator_values: Vec<Option<i32>> = expected.clone().collect();

                assert_eq!(
                    O::get_value(actual),
                    O::get_value(expected),
                    "Failed on op {} for iter that have no nulls left (left actual, right expected) ({current_iterator_values:?})",
                    O::NAME
                );
            },
        );

        setup_and_assert_extended_cases(
            |iter: &mut dyn SharedBetweenArrayIterAndSliceIter| {
                // Until last non null
                {
                    let last_some_position = iter.clone().rposition(|item| item != None);

                    // move the iterator to the location where there are only nulls
                    if let Some(last_some_position) = last_some_position {
                        iter.nth(last_some_position);
                    }
                }
            },
            |actual, expected| {
                let current_iterator_values: Vec<Option<i32>> = expected.clone().collect();

                assert_eq!(
                    O::get_value(actual),
                    O::get_value(expected),
                    "Failed on op {} for iter that only have nulls left (left actual, right expected) ({current_iterator_values:?})",
                    O::NAME
                );
            },
        );

        // TODO - move from the end
    }

    fn test_on_single_case<O: ArrayIteratorOp>(source: Vec<Option<i32>>, array: Int32Array) {
        for i in 0..source.len() {
            let mut actual = ArrayIter::new(&array);
            let mut expected = source.iter().copied();

            // calling nth(0) is the same as calling next()
            // but we want to get to the ith position so we call nth(i - 1)
            if i > 0 {
                actual.nth(i - 1);
                expected.nth(i - 1);
            }

            let current_iterator_values: Vec<Option<i32>> = expected.clone().collect();

            assert_eq!(
                O::get_value(actual),
                O::get_value(expected),
                "Failed on op {} for iter that advanced to i {i} (left actual, right expected) ({current_iterator_values:?})",
                O::NAME
            );
        }
    }

    #[test]
    fn assert_position() {
        for (array, source) in get_int32_iterator_cases_with_duplicates() {
            let all_uniques = source.clone().into_iter().collect::<std::collections::HashSet<_>>();
            for i in 0..source.len() {
                let mut actual = ArrayIter::new(&array);
                let mut expected = source.iter().copied();

                // calling nth(0) is the same as calling next()
                // but we want to get to the ith position so we call nth(i - 1)
                if i > 0 {
                    actual.nth(i - 1);
                    expected.nth(i - 1);
                }

                let current_iterator_values: Vec<Option<i32>> = expected.clone().collect();

                for unique in &all_uniques {
                    let mut actual_calls = vec![];
                    let actual_pos = actual.position(|x| {
                        actual_calls.push(x);
                        x == *unique
                    });
                    let mut expected_calls = vec![];
                    let expected_pos = expected.position(|x| {
                        expected_calls.push(x);
                        x == *unique
                    });

                    assert_eq!(
                        actual_pos,
                        expected_pos,
                        "Failed on position of value {:?} for iter that advanced to i {i} (left actual, right expected) ({current_iterator_values:?})",
                        unique
                    );

                    assert_eq!(
                        actual_calls, expected_calls,
                        "should have the same amount of calls and with the same arguments in position"
                    )
                }
            }
        }
    }

    #[test]
    fn assert_rposition() {
        for (array, source) in get_int32_iterator_cases_with_duplicates() {
            let all_uniques = source.clone().into_iter().collect::<std::collections::HashSet<_>>();
            for i in 0..source.len() {
                let mut actual = ArrayIter::new(&array);
                let mut expected = source.iter().copied();

                // calling nth(0) is the same as calling next()
                // but we want to get to the ith position so we call nth(i - 1)
                if i > 0 {
                    actual.nth(i - 1);
                    expected.nth(i - 1);
                }

                let current_iterator_values: Vec<Option<i32>> = expected.clone().collect();

                for unique in &all_uniques {
                    let mut actual_calls = vec![];
                    let actual_pos = actual.rposition(|x| {
                        actual_calls.push(x);
                        x == *unique
                    });
                    let mut expected_calls = vec![];
                    let expected_pos = expected.rposition(|x| {
                        expected_calls.push(x);
                        x == *unique
                    });

                    assert_eq!(
                        actual_pos,
                        expected_pos,
                        "Failed on position of value {:?} for iter that advanced to i {i} (left actual, right expected) ({current_iterator_values:?})",
                        unique
                    );

                    assert_eq!(
                        actual_calls, expected_calls,
                        "should have the same amount of calls and with the same arguments in position"
                    )
                }
            }
        }
    }

    #[test]
    fn assert_nth() {
        setup_and_assert_base_cases(
            |_| {},
            |actual, expected| {
                {
                    let mut actual = actual.clone();
                    let mut expected = expected.clone();
                    for _ in 0..expected.len() {
                        #[allow(clippy::iter_nth_zero)]
                        let actual_val = actual.nth(0);
                        #[allow(clippy::iter_nth_zero)]
                        let expected_val = expected.nth(0);
                        assert_eq!(actual_val, expected_val, "Failed on nth(0)");
                    }
                }

                {
                    let mut actual = actual.clone();
                    let mut expected = expected.clone();
                    for _ in 0..expected.len() {
                        let actual_val = actual.nth(1);
                        let expected_val = expected.nth(1);
                        assert_eq!(actual_val, expected_val, "Failed on nth(1)");
                    }
                }

                {
                    let mut actual = actual.clone();
                    let mut expected = expected.clone();
                    for _ in 0..expected.len() {
                        let actual_val = actual.nth(2);
                        let expected_val = expected.nth(2);
                        assert_eq!(actual_val, expected_val, "Failed on nth(2)");
                    }
                }
            },
        );
    }

    #[test]
    fn assert_nth_back() {
        setup_and_assert_base_cases(
            |_| {

            },
            |actual, expected| {
                {
                    let mut actual = actual.clone();
                    let mut expected = expected.clone();
                    for _ in 0..expected.len() {
                        #[allow(clippy::iter_nth_zero)]
                        let actual_val = actual.nth_back(0);
                        #[allow(clippy::iter_nth_zero)]
                        let expected_val = expected.nth_back(0);
                        assert_eq!(actual_val, expected_val, "Failed on nth_back(0)");
                    }
                }

                {
                    let mut actual = actual.clone();
                    let mut expected = expected.clone();
                    for _ in 0..expected.len() {
                        let actual_val = actual.nth_back(1);
                        let expected_val = expected.nth_back(1);
                        assert_eq!(actual_val, expected_val, "Failed on nth_back(1)");
                    }
                }

                {
                    let mut actual = actual.clone();
                    let mut expected = expected.clone();
                    for _ in 0..expected.len() {
                        let actual_val = actual.nth_back(2);
                        let expected_val = expected.nth_back(2);
                        assert_eq!(actual_val, expected_val, "Failed on nth_back(2)");
                    }
                }
            },
        );
    }

    #[test]
    fn assert_last() {
        for (array, source) in get_base_int32_iterator_cases() {

            let mut actual_forward = ArrayIter::new(&array);
            let mut expected_forward = source.iter().copied();

            for _ in 0..source.len() + 1 {
                {
                    let actual_forward_clone = actual_forward.clone();
                    let expected_forward_clone = expected_forward.clone();

                    assert_eq!(actual_forward_clone.last(), expected_forward_clone.last());
                }

                actual_forward.next();
                expected_forward.next();
            }


            let mut actual_backward = ArrayIter::new(&array);
            let mut expected_backward = source.iter().copied();
            for _ in 0..source.len() + 1 {
                {
                    assert_eq!(actual_backward.clone().last(), expected_backward.clone().last());
                }

                actual_backward.next_back();
                expected_backward.next_back();
            }
        }
    }

    #[test]
    fn assert_for_each() {
        struct ForEachOp;

        impl ArrayIteratorOp for ForEachOp {
            type Output = Vec<Option<i32>>;
            const NAME: &'static str = "for_each";

            fn get_value<T: SharedBetweenArrayIterAndSliceIter>(iter: T) -> Self::Output {
                let mut items = vec![];

                iter.for_each(|item| {
                    items.push(item);
                });

                items
            }
        }

        assert_array_iterator_cases::<ForEachOp>()
    }

    #[test]
    fn assert_for_each() {
        struct ForEachOp;

        impl ArrayIteratorOp for ForEachOp {
            type Output = Vec<Option<i32>>;
            const NAME: &'static str = "for_each";

            fn get_value<T: SharedBetweenArrayIterAndSliceIter>(iter: T) -> Self::Output {
                let mut items = vec![];

                iter.for_each(|item| {
                    items.push(item);
                });

                items
            }
        }

        assert_array_iterator_cases::<ForEachOp>()
    }

    #[test]
    fn assert_count() {
        struct CountOp;

        impl ArrayIteratorOp for CountOp {
            type Output = usize;
            const NAME: &'static str = "count";

            fn get_value<T: SharedBetweenArrayIterAndSliceIter>(iter: T) -> Self::Output {
                iter.count()
            }
        }

        assert_array_iterator_cases::<CountOp>()
    }
    //
    // #[test]
    // fn assert_bit_iterator_last() {
    //     struct LastOp;
    //
    //     impl ArrayIteratorOp for LastOp {
    //         type Output = Option<bool>;
    //         const NAME: &'static str = "last";
    //
    //         fn get_value<T: SharedBetweenArrayIterAndSliceIter>(iter: T) -> Self::Output {
    //             iter.last()
    //         }
    //     }
    //
    //     assert_array_iterator_cases::<LastOp>()
    // }
    //
    // #[test]
    // fn assert_bit_iterator_max() {
    //     struct MaxOp;
    //
    //     impl ArrayIteratorOp for MaxOp {
    //         type Output = Option<bool>;
    //         const NAME: &'static str = "max";
    //
    //         fn get_value<T: SharedBetweenArrayIterAndSliceIter>(iter: T) -> Self::Output {
    //             iter.max()
    //         }
    //     }
    //
    //     assert_array_iterator_cases::<MaxOp>()
    // }
    //
    // #[test]
    // fn assert_bit_iterator_nth_0() {
    //     struct NthOp<const BACK: bool>;
    //
    //     impl<const BACK: bool> ArrayIteratorOp for NthOp<BACK> {
    //         type Output = Option<bool>;
    //         const NAME: &'static str = if BACK { "nth_back(0)" } else { "nth(0)" };
    //
    //         fn get_value<T: SharedBetweenArrayIterAndSliceIter>(mut iter: T) -> Self::Output {
    //             if BACK { iter.nth_back(0) } else { iter.nth(0) }
    //         }
    //     }
    //
    //     assert_array_iterator_cases::<NthOp<false>>();
    //     assert_array_iterator_cases::<NthOp<true>>();
    // }
    //
    // #[test]
    // fn assert_bit_iterator_nth_1() {
    //     struct NthOp<const BACK: bool>;
    //
    //     impl<const BACK: bool> ArrayIteratorOp for NthOp<BACK> {
    //         type Output = Option<bool>;
    //         const NAME: &'static str = if BACK { "nth_back(1)" } else { "nth(1)" };
    //
    //         fn get_value<T: SharedBetweenArrayIterAndSliceIter>(mut iter: T) -> Self::Output {
    //             if BACK { iter.nth_back(1) } else { iter.nth(1) }
    //         }
    //     }
    //
    //     assert_array_iterator_cases::<NthOp<false>>();
    //     assert_array_iterator_cases::<NthOp<true>>();
    // }
    //
    // #[test]
    // fn assert_bit_iterator_nth_after_end() {
    //     struct NthOp<const BACK: bool>;
    //
    //     impl<const BACK: bool> ArrayIteratorOp for NthOp<BACK> {
    //         type Output = Option<bool>;
    //         const NAME: &'static str = if BACK {
    //             "nth_back(iter.len() + 1)"
    //         } else {
    //             "nth(iter.len() + 1)"
    //         };
    //
    //         fn get_value<T: SharedBetweenArrayIterAndSliceIter>(mut iter: T) -> Self::Output {
    //             if BACK {
    //                 iter.nth_back(iter.len() + 1)
    //             } else {
    //                 iter.nth(iter.len() + 1)
    //             }
    //         }
    //     }
    //
    //     assert_array_iterator_cases::<NthOp<false>>();
    //     assert_array_iterator_cases::<NthOp<true>>();
    // }
    //
    // #[test]
    // fn assert_bit_iterator_nth_len() {
    //     struct NthOp<const BACK: bool>;
    //
    //     impl<const BACK: bool> ArrayIteratorOp for NthOp<BACK> {
    //         type Output = Option<bool>;
    //         const NAME: &'static str = if BACK {
    //             "nth_back(iter.len())"
    //         } else {
    //             "nth(iter.len())"
    //         };
    //
    //         fn get_value<T: SharedBetweenArrayIterAndSliceIter>(mut iter: T) -> Self::Output {
    //             if BACK {
    //                 iter.nth_back(iter.len())
    //             } else {
    //                 iter.nth(iter.len())
    //             }
    //         }
    //     }
    //
    //     assert_array_iterator_cases::<NthOp<false>>();
    //     assert_array_iterator_cases::<NthOp<true>>();
    // }
    //
    // #[test]
    // fn assert_bit_iterator_nth_last() {
    //     struct NthOp<const BACK: bool>;
    //
    //     impl<const BACK: bool> ArrayIteratorOp for NthOp<BACK> {
    //         type Output = Option<bool>;
    //         const NAME: &'static str = if BACK {
    //             "nth_back(iter.len().saturating_sub(1))"
    //         } else {
    //             "nth(iter.len().saturating_sub(1))"
    //         };
    //
    //         fn get_value<T: SharedBetweenArrayIterAndSliceIter>(mut iter: T) -> Self::Output {
    //             if BACK {
    //                 iter.nth_back(iter.len().saturating_sub(1))
    //             } else {
    //                 iter.nth(iter.len().saturating_sub(1))
    //             }
    //         }
    //     }
    //
    //     assert_array_iterator_cases::<NthOp<false>>();
    //     assert_array_iterator_cases::<NthOp<true>>();
    // }
    //
    // #[test]
    // fn assert_bit_iterator_nth_and_reuse() {
    //     setup_and_assert(
    //         |_| {},
    //         |actual, expected| {
    //             {
    //                 let mut actual = actual.clone();
    //                 let mut expected = expected.clone();
    //                 for _ in 0..expected.len() {
    //                     #[allow(clippy::iter_nth_zero)]
    //                     let actual_val = actual.nth(0);
    //                     #[allow(clippy::iter_nth_zero)]
    //                     let expected_val = expected.nth(0);
    //                     assert_eq!(actual_val, expected_val, "Failed on nth(0)");
    //                 }
    //             }
    //
    //             {
    //                 let mut actual = actual.clone();
    //                 let mut expected = expected.clone();
    //                 for _ in 0..expected.len() {
    //                     let actual_val = actual.nth(1);
    //                     let expected_val = expected.nth(1);
    //                     assert_eq!(actual_val, expected_val, "Failed on nth(1)");
    //                 }
    //             }
    //
    //             {
    //                 let mut actual = actual.clone();
    //                 let mut expected = expected.clone();
    //                 for _ in 0..expected.len() {
    //                     let actual_val = actual.nth(2);
    //                     let expected_val = expected.nth(2);
    //                     assert_eq!(actual_val, expected_val, "Failed on nth(2)");
    //                 }
    //             }
    //         },
    //     );
    // }
    //
    // #[test]
    // fn assert_bit_iterator_nth_back_and_reuse() {
    //     setup_and_assert(
    //         |_| {},
    //         |actual, expected| {
    //             {
    //                 let mut actual = actual.clone();
    //                 let mut expected = expected.clone();
    //                 for _ in 0..expected.len() {
    //                     #[allow(clippy::iter_nth_zero)]
    //                     let actual_val = actual.nth_back(0);
    //                     let expected_val = expected.nth_back(0);
    //                     assert_eq!(actual_val, expected_val, "Failed on nth_back(0)");
    //                 }
    //             }
    //
    //             {
    //                 let mut actual = actual.clone();
    //                 let mut expected = expected.clone();
    //                 for _ in 0..expected.len() {
    //                     let actual_val = actual.nth_back(1);
    //                     let expected_val = expected.nth_back(1);
    //                     assert_eq!(actual_val, expected_val, "Failed on nth_back(1)");
    //                 }
    //             }
    //
    //             {
    //                 let mut actual = actual.clone();
    //                 let mut expected = expected.clone();
    //                 for _ in 0..expected.len() {
    //                     let actual_val = actual.nth_back(2);
    //                     let expected_val = expected.nth_back(2);
    //                     assert_eq!(actual_val, expected_val, "Failed on nth_back(2)");
    //                 }
    //             }
    //         },
    //     );
    // }
    //
    // fn assert_position() {
    //     let slice_without_nulls: &[i32];
    //     let slice_with_nulls: &[Option<i32>];
    //     let primitive_arr_without_nulls: Int32Array = Int32Array::from(slice_without_nulls.to_vec());
    //
    //     // TODO - test (for both nulls and not nulls):
    //     //  1. only 1 value exists and the iterator is before it
    //     //  2. more than 1 value exists and the iterator is before all of them
    //     //  3. more than 1 value exists and the iterator is between them
    //     //  4. only 1 value exists and the iterator is after it
    //     //  5. more than 1 value exists and the iterator is after all of them
    //
    // }
    //
    // fn assert_position_only_1_value_exists_and_the_iterator_is_before_it() {
    //     let unique_non_nulls_values = (0..10).map(Some).collect::<Vec<Option<i32>>>();
    //     let unique_non_nulls_values = unique_non_nulls_values.as_slice();
    //     let primitive_array = Int32Array::from(unique_non_nulls_values.to_vec());
    //
    //     // Test for each value the position for initial iterator state
    //     for value in unique_non_nulls_values.iter() {
    //         let mut iter = primitive_array.iter();
    //         let mut unique_non_nulls_values_iter = unique_non_nulls_values.iter();
    //
    //         let actual_position = iter.position(|opt_b| opt_b == *value);
    //         let expected_position = unique_non_nulls_values_iter.position(|opt_b| opt_b == *value);
    //         assert_eq!(position, Some(idx));
    //     }
    // }
}
