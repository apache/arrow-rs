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
#[derive(Debug)]
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

    /// Get consuming (won't update `self.current`) iterator when there are nulls
    ///
    /// # Panics
    /// If `self.logical_nulls` is `None`
    #[inline]
    fn get_consuming_iterator_for_nullable(
        &self,
    ) -> impl Iterator<Item = <Self as Iterator>::Item> {
        self.logical_nulls
            .as_ref()
            .expect("logical_nulls must exists when this function is called")
            .iter()
            .skip(self.current)
            .take(self.current_end - self.current)
            .enumerate()
            .map(move |(offset, is_valid)| {
                if is_valid {
                    // Safety:
                    // we are in bounds as i < self.array.len()
                    let value = unsafe { self.array.value_unchecked(self.current + offset) };
                    Some(value)
                } else {
                    None
                }
            })
    }

    /// Get consuming (won't update `self.current`) iterator  when there are no null
    ///
    /// # Panics
    /// If `self.logical_nulls` is `Some(_)`
    #[inline]
    fn get_consuming_iterator_for_non_nullable(
        self,
    ) -> impl Iterator<Item = <Self as Iterator>::Item> {
        assert_eq!(
            self.logical_nulls, None,
            "logical_nulls must be None when this function is called"
        );

        // Skip null checks
        (self.current..self.current_end).map(move |i| {
            debug_assert!(
                !self.is_null(i),
                "Value at index {} is null for non_nullable",
                i
            );

            // Safety:
            // we are in bounds as i < self.array.len()
            Some(unsafe { self.array.value_unchecked(i) })
        })
    }

    /// Get consuming (won't update `self.current_end`) reverse iterator when there are nulls
    ///
    /// # Panics
    /// If `self.logical_nulls` is `None`
    #[inline]
    fn get_consuming_reverse_iterator_for_nullable(
        &self,
    ) -> impl Iterator<Item = <Self as Iterator>::Item> {
        self.logical_nulls
            .as_ref()
            .expect("logical_nulls must exists when this function is called")
            .iter()
            .rev()
            .skip(self.array.len() - self.current_end)
            .take(self.current_end - self.current)
            .enumerate()
            .map(move |(offset, is_valid)| {
                if is_valid {
                    // Safety:
                    // we are in bounds as i < self.array.len()
                    let value = unsafe { self.array.value_unchecked(self.current_end - offset) };
                    Some(value)
                } else {
                    None
                }
            })
    }

    /// Get consuming (won't update `self.current_end`) reverse iterator when there are no null
    ///
    /// # Panics
    /// If `self.logical_nulls` is `Some(_)`
    #[inline]
    fn get_consuming_reverse_iterator_for_non_nullable(
        self,
    ) -> impl Iterator<Item = <Self as Iterator>::Item> {
        assert_eq!(
            self.logical_nulls, None,
            "logical_nulls must be None when this function is called"
        );

        // Skip null checks
        (self.current..self.current_end).rev().map(move |i| {
            debug_assert!(
                !self.is_null(i),
                "Value at index {} is null for non_nullable",
                i
            );

            // Safety:
            // we are in bounds as i < self.array.len()
            Some(unsafe { self.array.value_unchecked(i) })
        })
    }

    /// Get non-consuming (will update `self.current`) iterator when there are nulls
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

    /// Get non-consuming (won't update `self.current`) iterator when there are no null
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

    /// Get non-consuming (will update `self.current_end`) reverse iterator when there are nulls
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

    /// Get non-consuming (won't update `self.current_end`) reverse iterator when there are no null
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
        // Check if we advance to the one before the desired offset
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

    fn for_each<F>(self, f: F)
    where
        Self: Sized,
        F: FnMut(Self::Item),
    {
        if self.logical_nulls.is_some() {
            self.get_consuming_iterator_for_nullable().for_each(f)
        } else {
            self.get_consuming_iterator_for_non_nullable().for_each(f)
        }
    }

    fn fold<B, F>(self, init: B, f: F) -> B
    where
        Self: Sized,
        F: FnMut(B, Self::Item) -> B,
    {
        if self.logical_nulls.is_some() {
            self.get_consuming_iterator_for_nullable().fold(init, f)
        } else {
            self.get_consuming_iterator_for_non_nullable().fold(init, f)
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

    fn partition<B, F>(self, f: F) -> (B, B)
    where
        Self: Sized,
        B: Default + Extend<Self::Item>,
        F: FnMut(&Self::Item) -> bool,
    {
        if self.logical_nulls.is_some() {
            self.get_consuming_iterator_for_nullable().partition(f)
        } else {
            self.get_consuming_iterator_for_non_nullable().partition(f)
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

    fn rfold<B, F>(self, init: B, f: F) -> B
    where
        Self: Sized,
        F: FnMut(B, Self::Item) -> B,
    {
        if self.logical_nulls.is_some() {
            self.get_consuming_reverse_iterator_for_nullable()
                .fold(init, f)
        } else {
            self.get_consuming_reverse_iterator_for_non_nullable()
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
    use std::sync::Arc;

    use crate::array::{ArrayRef, BinaryArray, BooleanArray, Int32Array, StringArray};

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
}
