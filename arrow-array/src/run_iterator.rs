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

//! Idiomatic iterator for [`RunArray`](crate::Array)

use crate::{array::ArrayAccessor, RunArrayAccessor};

/// An iterator that returns Some(T) or None, that can be used on any [`ArrayAccessor`]
///
/// # Performance
///
/// [`RunArrayIter`] provides an idiomatic way to iterate over an array, however, this
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
pub struct RunArrayIter<T: ArrayAccessor + RunArrayAccessor> {
    array: T,
    current_logical: usize,
    current_physical: usize,
    current_end_logical: usize,
    current_end_physical: usize,
}

impl<T: ArrayAccessor + RunArrayAccessor> RunArrayIter<T> {
    /// create a new iterator
    pub fn new(array: T) -> Self {
        let logical_len = array.len();
        let physical_len: usize = array.physical_len();
        RunArrayIter {
            array,
            current_logical: 0,
            current_physical: 0,
            current_end_logical: logical_len,
            current_end_physical: physical_len,
        }
    }
}

impl<T: ArrayAccessor + RunArrayAccessor> Iterator for RunArrayIter<T> {
    type Item = Option<T::Item>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.current_logical == self.current_end_logical {
            return None;
        }
        // If current logical index is greater than current run end index then increment
        // the physical index.
        match self.array.run_end_index(self.current_physical) {
            None => {
                // The self.current_physical shold not go out of bounds as its
                // kept within the bounds of self.current_logical.
                panic!(
                    "Could not get run end index for physical index {}",
                    self.current_physical
                );
            }
            Some(run_end_index) if self.current_logical >= run_end_index => {
                //As the run_ends is expected to be strictly increasing, there
                // should be at least one logical entry in one physical entry. Because of this
                // reason we dont have to increment the physical index multiple times to get to next
                // logical index.
                self.current_physical += 1;
            }
            _ => {}
        }
        if self.array.is_value_null(self.current_physical) {
            self.current_logical += 1;
            Some(None)
        } else {
            self.current_logical += 1;
            // Safety:
            // The self.current_physical is kept within bounds of self.current_logical.
            // The self.current_logical will not go out of bounds because of the check
            // `self.current_logical = self.current_end_logical` above.
            unsafe { Some(Some(self.array.value_unchecked(self.current_physical))) }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.current_end_logical - self.current_logical,
            Some(self.current_end_logical - self.current_logical),
        )
    }
}

impl<T: ArrayAccessor + RunArrayAccessor> DoubleEndedIterator for RunArrayIter<T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.current_end_logical == self.current_logical {
            None
        } else {
            self.current_end_logical -= 1;
            if self.current_end_physical > 0
                && self.current_end_logical
                    < self
                        .array
                        .run_end_index(self.current_end_physical - 1)
                        .unwrap()
            {
                self.current_end_physical -= 1;
            }
            Some(if self.array.is_value_null(self.current_end_physical) {
                None
            } else {
                // Safety:
                // The check `self.current_end_physical > 0` ensures we don't underflow
                // the variable. Also self.current_end_physical starts with array.len()
                // and decrements based on the bounds of self.current_end_logical.
                unsafe { Some(self.array.value_unchecked(self.current_end_physical)) }
            })
        }
    }
}

/// all arrays have known size.
impl<T: ArrayAccessor + RunArrayAccessor> ExactSizeIterator for RunArrayIter<T> {}

#[cfg(test)]
mod tests {
    use crate::{
        array::{Int32Array, StringArray},
        builder::PrimitiveRunBuilder,
        types::Int32Type,
        Int64RunArray,
    };

    #[test]
    fn test_primitive_array_iter_round_trip() {
        let mut input_vec = vec![
            Some(32),
            Some(32),
            None,
            Some(64),
            Some(64),
            Some(64),
            Some(72),
        ];
        let mut builder = PrimitiveRunBuilder::<Int32Type, Int32Type>::new();
        builder.extend(input_vec.clone().into_iter());
        let ree_array = builder.finish();
        let ree_array = ree_array.downcast_ref::<Int32Array>().unwrap();

        let output_vec: Vec<Option<i32>> = ree_array.into_iter().collect();
        assert_eq!(input_vec, output_vec);

        let rev_output_vec: Vec<Option<i32>> = ree_array.into_iter().rev().collect();
        input_vec.reverse();
        assert_eq!(input_vec, rev_output_vec);
    }

    #[test]
    fn test_double_ended() {
        let input_vec = vec![
            Some(32),
            Some(32),
            None,
            Some(64),
            Some(64),
            Some(64),
            Some(72),
        ];
        let mut builder = PrimitiveRunBuilder::<Int32Type, Int32Type>::new();
        builder.extend(input_vec.clone().into_iter());
        let ree_array = builder.finish();
        let ree_array = ree_array.downcast_ref::<Int32Array>().unwrap();

        let mut iter = ree_array.into_iter();
        assert_eq!(Some(Some(32)), iter.next());
        assert_eq!(Some(Some(72)), iter.next_back());
        assert_eq!(Some(Some(32)), iter.next());
        assert_eq!(Some(Some(64)), iter.next_back());
        assert_eq!(Some(None), iter.next());
        assert_eq!(Some(Some(64)), iter.next_back());
        assert_eq!(Some(Some(64)), iter.next());
        assert_eq!(None, iter.next_back());
        assert_eq!(None, iter.next());
    }

    #[test]
    fn test_string_array_iter_round_trip() {
        let input_vec = vec!["ab", "ab", "ba", "cc", "cc"];
        let input_ree_array: Int64RunArray = input_vec.into_iter().collect();
        let string_ree_array = input_ree_array.downcast_ref::<StringArray>().unwrap();

        // to and from iter, with a +1
        let result: Vec<Option<String>> = string_ree_array
            .into_iter()
            .map(|e| {
                e.map(|e| {
                    let mut a = e.to_string();
                    a.push('b');
                    a
                })
            })
            .collect();

        let result_asref: Vec<Option<&str>> =
            result.iter().map(|f| f.as_deref()).collect();

        let expected_vec = vec![
            Some("abb"),
            Some("abb"),
            Some("bab"),
            Some("ccb"),
            Some("ccb"),
        ];

        assert_eq!(expected_vec, result_asref);
    }
}
