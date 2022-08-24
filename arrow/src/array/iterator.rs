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

use crate::array::array::ArrayAccessor;
use crate::array::{DecimalArray, FixedSizeBinaryArray};
use crate::datatypes::{Decimal128Type, Decimal256Type};
use std::ops::Range;

use super::{
    BooleanArray, GenericBinaryArray, GenericListArray, GenericStringArray,
    PrimitiveArray,
};

/// an iterator that returns Some(T) or None, that can be used on any [`ArrayAccessor`]
// Note: This implementation is based on std's [Vec]s' [IntoIter].
#[derive(Debug)]
pub struct ArrayIter<T: ArrayAccessor> {
    array: T,
    current: usize,
    current_end: usize,
}

impl<T: ArrayAccessor> ArrayIter<T> {
    /// create a new iterator
    pub fn new(array: T) -> Self {
        let len = array.len();
        ArrayIter {
            array,
            current: 0,
            current_end: len,
        }
    }
}

impl<T: ArrayAccessor> Iterator for ArrayIter<T> {
    type Item = Option<T::Item>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.current == self.current_end {
            None
        } else if self.array.is_null(self.current) {
            self.current += 1;
            Some(None)
        } else {
            let old = self.current;
            self.current += 1;
            // Safety:
            // This is safe as `current` cannot exceed the bounds of the array
            // and we have verified that the value is not null
            unsafe { Some(Some(self.array.value_unchecked(old))) }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.current_end - self.current,
            Some(self.current_end - self.current),
        )
    }
}

impl<T: ArrayAccessor> DoubleEndedIterator for ArrayIter<T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.current_end == self.current {
            None
        } else {
            self.current_end -= 1;
            Some(if self.array.is_null(self.current_end) {
                None
            } else {
                // Safety:
                // This is safe as `current` cannot exceed the bounds of the array
                // and we have verified that the value is not null
                unsafe { Some(self.array.value_unchecked(self.current_end)) }
            })
        }
    }
}

/// all arrays have known size.
impl<T: ArrayAccessor> ExactSizeIterator for ArrayIter<T> {}

/// an iterator that returns Some(T) or None, that can be used on any PrimitiveArray
pub type PrimitiveIter<'a, T> = ArrayIter<&'a PrimitiveArray<T>>;
pub type BooleanIter<'a> = ArrayIter<&'a BooleanArray>;
pub type GenericStringIter<'a, T> = ArrayIter<&'a GenericStringArray<T>>;
pub type GenericBinaryIter<'a, T> = ArrayIter<&'a GenericBinaryArray<T>>;
pub type FixedSizeBinaryIter<'a> = ArrayIter<&'a FixedSizeBinaryArray>;
pub type GenericListArrayIter<'a, O> = ArrayIter<&'a GenericListArray<O>>;

pub type DecimalIter<'a, T> = ArrayIter<&'a DecimalArray<T>>;
/// an iterator that returns `Some(Decimal128)` or `None`, that can be used on a
/// [`super::Decimal128Array`]
pub type Decimal128Iter<'a> = DecimalIter<'a, Decimal128Type>;

/// an iterator that returns `Some(Decimal256)` or `None`, that can be used on a
/// [`super::Decimal256Array`]
pub type Decimal256Iter<'a> = DecimalIter<'a, Decimal256Type>;

/// An iterator over the defined values within an array
///
/// Note: Unlike [`ArrayIter`] this may return values for null indexes, provided
/// doing so is well defined
pub(crate) struct ValuesIter<T: ArrayAccessor> {
    inner: ValuesIterInner<T>,
}

impl<T: ArrayAccessor> ValuesIter<T> {
    pub(crate) fn new(array: T) -> Self {
        let inner = match T::NULLS_DEFINED || array.null_count() == 0 {
            true => ValuesIterInner::Indices(0..array.len(), array),
            false => ValuesIterInner::Array(ArrayIter::new(array)),
        };

        Self { inner }
    }
}

impl<T: ArrayAccessor> Iterator for ValuesIter<T> {
    type Item = Option<T::Item>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.inner {
            ValuesIterInner::Indices(idx, array) => {
                Some(Some(unsafe { array.value_unchecked(idx.next()?) }))
            }
            ValuesIterInner::Array(a) => a.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.inner {
            ValuesIterInner::Indices(idx, _) => idx.size_hint(),
            ValuesIterInner::Array(a) => a.size_hint(),
        }
    }
}

enum ValuesIterInner<T: ArrayAccessor> {
    Indices(Range<usize>, T),
    Array(ArrayIter<T>),
}

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
        let array =
            StringArray::from(vec![Some("a"), None, Some("aaa"), None, Some("aaaaa")]);
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
        let rev_array =
            StringArray::from(vec![Some("aaaaa"), None, Some("aaa"), None, Some("a")]);
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
