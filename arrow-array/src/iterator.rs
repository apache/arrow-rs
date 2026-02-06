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
            self.current_end - self.current,
            Some(self.current_end - self.current),
        )
    }

    #[inline]
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

    #[inline]
    fn last(mut self) -> Option<Self::Item> {
        self.next_back()
    }

    #[inline]
    fn count(self) -> usize
    where
        Self: Sized,
    {
        self.len()
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

    #[inline]
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
    use crate::array::{ArrayRef, BinaryArray, BooleanArray, Int32Array, StringArray};
    use crate::iterator::ArrayIter;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use std::fmt::Debug;
    use std::sync::Arc;

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

    trait SharedBetweenArrayIterAndSliceIter:
        ExactSizeIterator<Item = Option<i32>> + DoubleEndedIterator<Item = Option<i32>> + Clone
    {
    }
    impl<T: Clone + ExactSizeIterator<Item = Option<i32>> + DoubleEndedIterator<Item = Option<i32>>>
        SharedBetweenArrayIterAndSliceIter for T
    {
    }

    fn get_int32_iterator_cases() -> impl Iterator<Item = (Int32Array, Vec<Option<i32>>)> {
        let mut rng = StdRng::seed_from_u64(42);

        let no_nulls_and_no_duplicates = (0..10).map(Some).collect::<Vec<Option<i32>>>();
        let no_nulls_random_values = (0..10)
            .map(|_| rng.random::<i32>())
            .map(Some)
            .collect::<Vec<Option<i32>>>();

        let all_nulls = (0..10).map(|_| None).collect::<Vec<Option<i32>>>();
        let only_start_nulls = (0..10)
            .map(|item| if item < 4 { None } else { Some(item) })
            .collect::<Vec<Option<i32>>>();
        let only_end_nulls = (0..10)
            .map(|item| if item > 8 { None } else { Some(item) })
            .collect::<Vec<Option<i32>>>();
        let only_middle_nulls = (0..10)
            .map(|item| {
                if (4..=8).contains(&item) && rng.random_bool(0.9) {
                    None
                } else {
                    Some(item)
                }
            })
            .collect::<Vec<Option<i32>>>();
        let random_values_with_random_nulls = (0..10)
            .map(|_| {
                if rng.random_bool(0.3) {
                    None
                } else {
                    Some(rng.random::<i32>())
                }
            })
            .collect::<Vec<Option<i32>>>();

        let no_nulls_and_some_duplicates = (0..10)
            .map(|item| item % 3)
            .map(Some)
            .collect::<Vec<Option<i32>>>();
        let no_nulls_and_all_same_value =
            (0..10).map(|_| 9).map(Some).collect::<Vec<Option<i32>>>();
        let no_nulls_and_continues_duplicates = [0, 0, 0, 1, 1, 2, 2, 2, 2, 3]
            .map(Some)
            .into_iter()
            .collect::<Vec<Option<i32>>>();

        let single_null_and_no_duplicates = (0..10)
            .map(|item| if item == 4 { None } else { Some(item) })
            .collect::<Vec<Option<i32>>>();
        let multiple_nulls_and_no_duplicates = (0..10)
            .map(|item| if item % 3 == 2 { None } else { Some(item) })
            .collect::<Vec<Option<i32>>>();
        let continues_nulls_and_no_duplicates = [
            Some(0),
            Some(1),
            None,
            None,
            Some(2),
            Some(3),
            None,
            Some(4),
            Some(5),
            None,
        ]
        .into_iter()
        .collect::<Vec<Option<i32>>>();

        [
            no_nulls_and_no_duplicates,
            no_nulls_random_values,
            no_nulls_and_some_duplicates,
            no_nulls_and_all_same_value,
            no_nulls_and_continues_duplicates,
            all_nulls,
            only_start_nulls,
            only_end_nulls,
            only_middle_nulls,
            random_values_with_random_nulls,
            single_null_and_no_duplicates,
            multiple_nulls_and_no_duplicates,
            continues_nulls_and_no_duplicates,
        ]
        .map(|case| (Int32Array::from(case.clone()), case))
        .into_iter()
    }

    trait SetupIter {
        fn description(&self) -> String;
        fn setup<I: SharedBetweenArrayIterAndSliceIter>(&self, iter: &mut I);
    }

    struct NoSetup;
    impl SetupIter for NoSetup {
        fn description(&self) -> String {
            "no setup".to_string()
        }
        fn setup<I: SharedBetweenArrayIterAndSliceIter>(&self, _iter: &mut I) {
            // none
        }
    }

    fn setup_and_assert_cases_on_single_operation(
        o: &impl ConsumingArrayIteratorOp,
        setup_iterator: impl SetupIter,
    ) {
        for (array, source) in get_int32_iterator_cases() {
            let mut actual = ArrayIter::new(&array);
            let mut expected = source.iter().copied();

            setup_iterator.setup(&mut actual);
            setup_iterator.setup(&mut expected);

            let current_iterator_values: Vec<Option<i32>> = expected.clone().collect();

            assert_eq!(
                o.get_value(actual),
                o.get_value(expected),
                "Failed on op {} for {} (left actual, right expected) ({current_iterator_values:?})",
                o.name(),
                setup_iterator.description(),
            );
        }
    }

    /// Trait representing an operation on a [`ArrayIter`]
    /// that can be compared against a slice iterator
    ///
    /// this is for consuming operations (e.g. `count`, `last`, etc)
    trait ConsumingArrayIteratorOp {
        /// What the operation returns (e.g. Option<i32> for last, usize for count, etc)
        type Output: PartialEq + Debug;

        /// The name of the operation, used for error messages
        fn name(&self) -> String;

        /// Get the value of the operation for the provided iterator
        /// This will be either a [`ArrayIter`] or a slice iterator to make sure they produce the same result
        ///
        /// Example implementation:
        /// 1. for `last` it will be the last value
        /// 2. for `count` it will be the returned length
        fn get_value<T: SharedBetweenArrayIterAndSliceIter>(&self, iter: T) -> Self::Output;
    }

    /// Trait representing an operation on a [`ArrayIter`]
    /// that can be compared against a slice iterator.
    ///
    /// This is for mutating operations (e.g. `position`, `any`, `find`, etc)
    trait MutatingArrayIteratorOp {
        /// What the operation returns (e.g. Option<i32> for last, usize for count, etc)
        type Output: PartialEq + Debug;

        /// The name of the operation, used for error messages
        fn name(&self) -> String;

        /// Get the value of the operation for the provided iterator
        /// This will be either a [`ArrayIter`] or a slice iterator to make sure they produce the same result
        ///
        /// Example implementation:
        /// 1. for `for_each` it will be the iterator element that the function was called with
        /// 2. for `fold` it will be the accumulator and the iterator element from each call, as well as the final result
        fn get_value<T: SharedBetweenArrayIterAndSliceIter>(&self, iter: &mut T) -> Self::Output;
    }

    /// Helper function that will assert that the provided operation
    /// produces the same result for both [`ArrayIter`] and slice iterator
    /// under various consumption patterns (e.g. some calls to next/next_back/consume_all/etc)
    fn assert_array_iterator_cases<O: ConsumingArrayIteratorOp>(o: O) {
        setup_and_assert_cases_on_single_operation(&o, NoSetup);

        struct Next;
        impl SetupIter for Next {
            fn description(&self) -> String {
                "new iter after consuming 1 element from the start".to_string()
            }
            fn setup<I: SharedBetweenArrayIterAndSliceIter>(&self, iter: &mut I) {
                iter.next();
            }
        }
        setup_and_assert_cases_on_single_operation(&o, Next);

        struct NextBack;
        impl SetupIter for NextBack {
            fn description(&self) -> String {
                "new iter after consuming 1 element from the end".to_string()
            }

            fn setup<I: SharedBetweenArrayIterAndSliceIter>(&self, iter: &mut I) {
                iter.next_back();
            }
        }

        setup_and_assert_cases_on_single_operation(&o, NextBack);

        struct NextAndBack;
        impl SetupIter for NextAndBack {
            fn description(&self) -> String {
                "new iter after consuming 1 element from start and end".to_string()
            }

            fn setup<I: SharedBetweenArrayIterAndSliceIter>(&self, iter: &mut I) {
                iter.next();
                iter.next_back();
            }
        }

        setup_and_assert_cases_on_single_operation(&o, NextAndBack);

        struct NextUntilLast;
        impl SetupIter for NextUntilLast {
            fn description(&self) -> String {
                "new iter after consuming all from the start but 1".to_string()
            }
            fn setup<I: SharedBetweenArrayIterAndSliceIter>(&self, iter: &mut I) {
                let len = iter.len();
                if len > 1 {
                    iter.nth(len - 2);
                }
            }
        }
        setup_and_assert_cases_on_single_operation(&o, NextUntilLast);

        struct NextBackUntilFirst;
        impl SetupIter for NextBackUntilFirst {
            fn description(&self) -> String {
                "new iter after consuming all from the end but 1".to_string()
            }

            fn setup<I: SharedBetweenArrayIterAndSliceIter>(&self, iter: &mut I) {
                let len = iter.len();
                if len > 1 {
                    iter.nth_back(len - 2);
                }
            }
        }
        setup_and_assert_cases_on_single_operation(&o, NextBackUntilFirst);

        struct NextFinish;
        impl SetupIter for NextFinish {
            fn description(&self) -> String {
                "new iter after consuming all from the start".to_string()
            }
            fn setup<I: SharedBetweenArrayIterAndSliceIter>(&self, iter: &mut I) {
                iter.nth(iter.len());
            }
        }
        setup_and_assert_cases_on_single_operation(&o, NextFinish);

        struct NextBackFinish;
        impl SetupIter for NextBackFinish {
            fn description(&self) -> String {
                "new iter after consuming all from the end".to_string()
            }
            fn setup<I: SharedBetweenArrayIterAndSliceIter>(&self, iter: &mut I) {
                iter.nth_back(iter.len());
            }
        }
        setup_and_assert_cases_on_single_operation(&o, NextBackFinish);

        struct NextUntilLastNone;
        impl SetupIter for NextUntilLastNone {
            fn description(&self) -> String {
                "new iter that have no nulls left".to_string()
            }
            fn setup<I: SharedBetweenArrayIterAndSliceIter>(&self, iter: &mut I) {
                let last_null_position = iter.clone().rposition(|item| item.is_none());

                // move the iterator to the location where there are no nulls anymore
                if let Some(last_null_position) = last_null_position {
                    iter.nth(last_null_position);
                }
            }
        }
        setup_and_assert_cases_on_single_operation(&o, NextUntilLastNone);

        struct NextUntilLastSome;
        impl SetupIter for NextUntilLastSome {
            fn description(&self) -> String {
                "iter that only have nulls left".to_string()
            }
            fn setup<I: SharedBetweenArrayIterAndSliceIter>(&self, iter: &mut I) {
                let last_some_position = iter.clone().rposition(|item| item.is_some());

                // move the iterator to the location where there are only nulls
                if let Some(last_some_position) = last_some_position {
                    iter.nth(last_some_position);
                }
            }
        }
        setup_and_assert_cases_on_single_operation(&o, NextUntilLastSome);
    }

    /// Helper function that will assert that the provided operation
    /// produces the same result for both [`ArrayIter`] and slice iterator
    /// under various consumption patterns (e.g. some calls to next/next_back/consume_all/etc)
    ///
    /// this is different from [`assert_array_iterator_cases`] as this also check that the state after the call is correct
    /// to make sure we don't leave the iterator in incorrect state
    fn assert_array_iterator_cases_mutate<O: MutatingArrayIteratorOp>(o: O) {
        struct Adapter<O: MutatingArrayIteratorOp> {
            o: O,
        }

        #[derive(Debug, PartialEq)]
        struct AdapterOutput<Value> {
            value: Value,
            /// collect on the iterator after running the operation
            leftover: Vec<Option<i32>>,
        }

        impl<O: MutatingArrayIteratorOp> ConsumingArrayIteratorOp for Adapter<O> {
            type Output = AdapterOutput<O::Output>;

            fn name(&self) -> String {
                self.o.name()
            }

            fn get_value<T: SharedBetweenArrayIterAndSliceIter>(
                &self,
                mut iter: T,
            ) -> Self::Output {
                let value = self.o.get_value(&mut iter);

                // Get the rest of the iterator to make sure we leave the iterator in a valid state
                let leftover: Vec<_> = iter.collect();

                AdapterOutput { value, leftover }
            }
        }

        assert_array_iterator_cases(Adapter { o })
    }

    #[derive(Debug, PartialEq)]
    struct CallTrackingAndResult<Result: Debug + PartialEq, CallArgs: Debug + PartialEq> {
        result: Result,
        calls: Vec<CallArgs>,
    }
    type CallTrackingWithInputType<Result> = CallTrackingAndResult<Result, Option<i32>>;
    type CallTrackingOnly = CallTrackingWithInputType<()>;

    #[test]
    fn assert_position() {
        struct PositionOp {
            reverse: bool,
            number_of_false: usize,
        }

        impl MutatingArrayIteratorOp for PositionOp {
            type Output = CallTrackingWithInputType<Option<usize>>;
            fn name(&self) -> String {
                if self.reverse {
                    format!("rposition with {} false returned", self.number_of_false)
                } else {
                    format!("position with {} false returned", self.number_of_false)
                }
            }

            fn get_value<T: SharedBetweenArrayIterAndSliceIter>(
                &self,
                iter: &mut T,
            ) -> Self::Output {
                let mut items = vec![];

                let mut count = 0;

                let cb = |item| {
                    items.push(item);

                    if count < self.number_of_false {
                        count += 1;
                        false
                    } else {
                        true
                    }
                };

                let position_result = if self.reverse {
                    iter.rposition(cb)
                } else {
                    iter.position(cb)
                };

                CallTrackingAndResult {
                    result: position_result,
                    calls: items,
                }
            }
        }

        for reverse in [false, true] {
            for number_of_false in [0, 1, 2, usize::MAX] {
                assert_array_iterator_cases_mutate(PositionOp {
                    reverse,
                    number_of_false,
                });
            }
        }
    }

    #[test]
    fn assert_nth() {
        for (array, source) in get_int32_iterator_cases() {
            let actual = ArrayIter::new(&array);
            let expected = source.iter().copied();
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
        }
    }

    #[test]
    fn assert_nth_back() {
        for (array, source) in get_int32_iterator_cases() {
            let actual = ArrayIter::new(&array);
            let expected = source.iter().copied();
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
        }
    }

    #[test]
    fn assert_last() {
        for (array, source) in get_int32_iterator_cases() {
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
                    assert_eq!(
                        actual_backward.clone().last(),
                        expected_backward.clone().last()
                    );
                }

                actual_backward.next_back();
                expected_backward.next_back();
            }
        }
    }

    #[test]
    fn assert_for_each() {
        struct ForEachOp;

        impl ConsumingArrayIteratorOp for ForEachOp {
            type Output = CallTrackingOnly;

            fn name(&self) -> String {
                "for_each".to_string()
            }

            fn get_value<T: SharedBetweenArrayIterAndSliceIter>(&self, iter: T) -> Self::Output {
                let mut items = Vec::with_capacity(iter.len());

                iter.for_each(|item| {
                    items.push(item);
                });

                CallTrackingAndResult {
                    calls: items,
                    result: (),
                }
            }
        }

        assert_array_iterator_cases(ForEachOp)
    }

    #[test]
    fn assert_fold() {
        struct FoldOp {
            reverse: bool,
        }

        #[derive(Debug, PartialEq)]
        struct CallArgs {
            acc: Option<i32>,
            item: Option<i32>,
        }

        impl ConsumingArrayIteratorOp for FoldOp {
            type Output = CallTrackingAndResult<Option<i32>, CallArgs>;

            fn name(&self) -> String {
                if self.reverse {
                    "rfold".to_string()
                } else {
                    "fold".to_string()
                }
            }

            fn get_value<T: SharedBetweenArrayIterAndSliceIter>(&self, iter: T) -> Self::Output {
                let mut items = Vec::with_capacity(iter.len());

                let cb = |acc, item| {
                    items.push(CallArgs { item, acc });

                    item.map(|val| val + 100)
                };

                let result = if self.reverse {
                    iter.rfold(Some(1), cb)
                } else {
                    #[allow(clippy::manual_try_fold)]
                    iter.fold(Some(1), cb)
                };

                CallTrackingAndResult {
                    calls: items,
                    result,
                }
            }
        }

        assert_array_iterator_cases(FoldOp { reverse: false });
        assert_array_iterator_cases(FoldOp { reverse: true });
    }

    #[test]
    fn assert_count() {
        struct CountOp;

        impl ConsumingArrayIteratorOp for CountOp {
            type Output = usize;

            fn name(&self) -> String {
                "count".to_string()
            }

            fn get_value<T: SharedBetweenArrayIterAndSliceIter>(&self, iter: T) -> Self::Output {
                iter.count()
            }
        }

        assert_array_iterator_cases(CountOp)
    }

    #[test]
    fn assert_any() {
        struct AnyOp {
            false_count: usize,
        }

        impl MutatingArrayIteratorOp for AnyOp {
            type Output = CallTrackingWithInputType<bool>;

            fn name(&self) -> String {
                format!("any with {} false returned", self.false_count)
            }

            fn get_value<T: SharedBetweenArrayIterAndSliceIter>(
                &self,
                iter: &mut T,
            ) -> Self::Output {
                let mut items = Vec::with_capacity(iter.len());

                let mut count = 0;
                let res = iter.any(|item| {
                    items.push(item);

                    if count < self.false_count {
                        count += 1;
                        false
                    } else {
                        true
                    }
                });

                CallTrackingWithInputType {
                    calls: items,
                    result: res,
                }
            }
        }

        for false_count in [0, 1, 2, usize::MAX] {
            assert_array_iterator_cases_mutate(AnyOp { false_count });
        }
    }

    #[test]
    fn assert_all() {
        struct AllOp {
            true_count: usize,
        }

        impl MutatingArrayIteratorOp for AllOp {
            type Output = CallTrackingWithInputType<bool>;

            fn name(&self) -> String {
                format!("all with {} false returned", self.true_count)
            }

            fn get_value<T: SharedBetweenArrayIterAndSliceIter>(
                &self,
                iter: &mut T,
            ) -> Self::Output {
                let mut items = Vec::with_capacity(iter.len());

                let mut count = 0;
                let res = iter.all(|item| {
                    items.push(item);

                    if count < self.true_count {
                        count += 1;
                        true
                    } else {
                        false
                    }
                });

                CallTrackingWithInputType {
                    calls: items,
                    result: res,
                }
            }
        }

        for true_count in [0, 1, 2, usize::MAX] {
            assert_array_iterator_cases_mutate(AllOp { true_count });
        }
    }

    #[test]
    fn assert_find() {
        struct FindOp {
            reverse: bool,
            false_count: usize,
        }

        impl MutatingArrayIteratorOp for FindOp {
            type Output = CallTrackingWithInputType<Option<Option<i32>>>;

            fn name(&self) -> String {
                if self.reverse {
                    format!("rfind with {} false returned", self.false_count)
                } else {
                    format!("find with {} false returned", self.false_count)
                }
            }

            fn get_value<T: SharedBetweenArrayIterAndSliceIter>(
                &self,
                iter: &mut T,
            ) -> Self::Output {
                let mut items = vec![];

                let mut count = 0;

                let cb = |item: &Option<i32>| {
                    items.push(*item);

                    if count < self.false_count {
                        count += 1;
                        false
                    } else {
                        true
                    }
                };

                let position_result = if self.reverse {
                    iter.rfind(cb)
                } else {
                    iter.find(cb)
                };

                CallTrackingWithInputType {
                    calls: items,
                    result: position_result,
                }
            }
        }

        for reverse in [false, true] {
            for false_count in [0, 1, 2, usize::MAX] {
                assert_array_iterator_cases_mutate(FindOp {
                    reverse,
                    false_count,
                });
            }
        }
    }

    #[test]
    fn assert_find_map() {
        struct FindMapOp {
            number_of_nones: usize,
        }

        impl MutatingArrayIteratorOp for FindMapOp {
            type Output = CallTrackingWithInputType<Option<&'static str>>;

            fn name(&self) -> String {
                format!("find_map with {} None returned", self.number_of_nones)
            }

            fn get_value<T: SharedBetweenArrayIterAndSliceIter>(
                &self,
                iter: &mut T,
            ) -> Self::Output {
                let mut items = vec![];

                let mut count = 0;

                let result = iter.find_map(|item| {
                    items.push(item);

                    if count < self.number_of_nones {
                        count += 1;
                        None
                    } else {
                        Some("found it")
                    }
                });

                CallTrackingAndResult {
                    result,
                    calls: items,
                }
            }
        }

        for number_of_nones in [0, 1, 2, usize::MAX] {
            assert_array_iterator_cases_mutate(FindMapOp { number_of_nones });
        }
    }

    #[test]
    fn assert_partition() {
        struct PartitionOp<F: Fn(usize, &Option<i32>) -> bool> {
            description: &'static str,
            predicate: F,
        }

        #[derive(Debug, PartialEq)]
        struct PartitionResult {
            left: Vec<Option<i32>>,
            right: Vec<Option<i32>>,
        }

        impl<F: Fn(usize, &Option<i32>) -> bool> ConsumingArrayIteratorOp for PartitionOp<F> {
            type Output = CallTrackingWithInputType<PartitionResult>;

            fn name(&self) -> String {
                format!("partition by {}", self.description)
            }

            fn get_value<T: SharedBetweenArrayIterAndSliceIter>(&self, iter: T) -> Self::Output {
                let mut items = vec![];

                let mut index = 0;

                let (left, right) = iter.partition(|item| {
                    items.push(*item);

                    let res = (self.predicate)(index, item);

                    index += 1;
                    res
                });

                CallTrackingAndResult {
                    result: PartitionResult { left, right },
                    calls: items,
                }
            }
        }

        assert_array_iterator_cases(PartitionOp {
            description: "None on one side and Some(*) on the other",
            predicate: |_, item| item.is_none(),
        });

        assert_array_iterator_cases(PartitionOp {
            description: "all true",
            predicate: |_, _| true,
        });

        assert_array_iterator_cases(PartitionOp {
            description: "all false",
            predicate: |_, _| false,
        });

        let random_values = (0..100).map(|_| rand::random_bool(0.5)).collect::<Vec<_>>();
        assert_array_iterator_cases(PartitionOp {
            description: "random",
            predicate: |index, _| random_values[index % random_values.len()],
        });
    }
}
