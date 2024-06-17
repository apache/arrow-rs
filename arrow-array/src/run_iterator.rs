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

//! Idiomatic iterator for [`RunArray`](crate::RunArray)

use crate::{array::ArrayAccessor, types::RunEndIndexType, Array, TypedRunArray};
use arrow_buffer::ArrowNativeType;

/// The [`RunArrayIter`] provides an idiomatic way to iterate over the run array.
/// It returns Some(T) if there is a value or None if the value is null.
///
/// The iterator comes with a cost as it has to iterate over three arrays to determine
/// the value to be returned. The run_ends array is used to determine the index of the value.
/// The nulls array is used to determine if the value is null and the values array is used to
/// get the value.
///
/// Unlike other iterators in this crate, [`RunArrayIter`] does not use [`ArrayAccessor`]
/// because the run array accessor does binary search to access each value which is too slow.
/// The run array iterator can determine the next value in constant time.
///
#[derive(Debug)]
pub struct RunArrayIter<'a, R, V>
where
    R: RunEndIndexType,
    V: Sync + Send,
    &'a V: ArrayAccessor,
    <&'a V as ArrayAccessor>::Item: Default,
{
    array: TypedRunArray<'a, R, V>,
    current_front_logical: usize,
    current_front_physical: usize,
    current_back_logical: usize,
    current_back_physical: usize,
}

impl<'a, R, V> RunArrayIter<'a, R, V>
where
    R: RunEndIndexType,
    V: Sync + Send,
    &'a V: ArrayAccessor,
    <&'a V as ArrayAccessor>::Item: Default,
{
    /// create a new iterator
    pub fn new(array: TypedRunArray<'a, R, V>) -> Self {
        let current_front_physical = array.run_array().get_start_physical_index();
        let current_back_physical = array.run_array().get_end_physical_index() + 1;
        RunArrayIter {
            array,
            current_front_logical: array.offset(),
            current_front_physical,
            current_back_logical: array.offset() + array.len(),
            current_back_physical,
        }
    }
}

impl<'a, R, V> Iterator for RunArrayIter<'a, R, V>
where
    R: RunEndIndexType,
    V: Sync + Send,
    &'a V: ArrayAccessor,
    <&'a V as ArrayAccessor>::Item: Default,
{
    type Item = Option<<&'a V as ArrayAccessor>::Item>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.current_front_logical == self.current_back_logical {
            return None;
        }

        // If current logical index is greater than current run end index then increment
        // the physical index.
        let run_ends = self.array.run_ends().values();
        if self.current_front_logical >= run_ends[self.current_front_physical].as_usize() {
            // As the run_ends is expected to be strictly increasing, there
            // should be at least one logical entry in one physical entry. Because of this
            // reason the next value can be accessed by incrementing physical index once.
            self.current_front_physical += 1;
        }
        if self.array.values().is_null(self.current_front_physical) {
            self.current_front_logical += 1;
            Some(None)
        } else {
            self.current_front_logical += 1;
            // Safety:
            // The self.current_physical is kept within bounds of self.current_logical.
            // The self.current_logical will not go out of bounds because of the check
            // `self.current_logical = self.current_end_logical` above.
            unsafe {
                Some(Some(
                    self.array
                        .values()
                        .value_unchecked(self.current_front_physical),
                ))
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.current_back_logical - self.current_front_logical,
            Some(self.current_back_logical - self.current_front_logical),
        )
    }
}

impl<'a, R, V> DoubleEndedIterator for RunArrayIter<'a, R, V>
where
    R: RunEndIndexType,
    V: Sync + Send,
    &'a V: ArrayAccessor,
    <&'a V as ArrayAccessor>::Item: Default,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.current_back_logical == self.current_front_logical {
            return None;
        }

        self.current_back_logical -= 1;

        let run_ends = self.array.run_ends().values();
        if self.current_back_physical > 0
            && self.current_back_logical < run_ends[self.current_back_physical - 1].as_usize()
        {
            // As the run_ends is expected to be strictly increasing, there
            // should be at least one logical entry in one physical entry. Because of this
            // reason the next value can be accessed by decrementing physical index once.
            self.current_back_physical -= 1;
        }
        Some(if self.array.values().is_null(self.current_back_physical) {
            None
        } else {
            // Safety:
            // The check `self.current_end_physical > 0` ensures the value will not underflow.
            // Also self.current_end_physical starts with array.len() and
            // decrements based on the bounds of self.current_end_logical.
            unsafe {
                Some(
                    self.array
                        .values()
                        .value_unchecked(self.current_back_physical),
                )
            }
        })
    }
}

/// all arrays have known size.
impl<'a, R, V> ExactSizeIterator for RunArrayIter<'a, R, V>
where
    R: RunEndIndexType,
    V: Sync + Send,
    &'a V: ArrayAccessor,
    <&'a V as ArrayAccessor>::Item: Default,
{
}

#[cfg(test)]
mod tests {
    use rand::{seq::SliceRandom, thread_rng, Rng};

    use crate::{
        array::{Int32Array, StringArray},
        builder::PrimitiveRunBuilder,
        types::{Int16Type, Int32Type},
        Array, Int64RunArray, PrimitiveArray, RunArray,
    };

    fn build_input_array(size: usize) -> Vec<Option<i32>> {
        // The input array is created by shuffling and repeating
        // the seed values random number of times.
        let mut seed: Vec<Option<i32>> = vec![
            None,
            None,
            None,
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            Some(6),
            Some(7),
            Some(8),
            Some(9),
        ];
        let mut result: Vec<Option<i32>> = Vec::with_capacity(size);
        let mut ix = 0;
        let mut rng = thread_rng();
        // run length can go up to 8. Cap the max run length for smaller arrays to size / 2.
        let max_run_length = 8_usize.min(1_usize.max(size / 2));
        while result.len() < size {
            // shuffle the seed array if all the values are iterated.
            if ix == 0 {
                seed.shuffle(&mut rng);
            }
            // repeat the items between 1 and 8 times. Cap the length for smaller sized arrays
            let num = max_run_length.min(rand::thread_rng().gen_range(1..=max_run_length));
            for _ in 0..num {
                result.push(seed[ix]);
            }
            ix += 1;
            if ix == seed.len() {
                ix = 0
            }
        }
        result.resize(size, None);
        result
    }

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
        builder.extend(input_vec.iter().copied());
        let ree_array = builder.finish();
        let ree_array = ree_array.downcast::<Int32Array>().unwrap();

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
        builder.extend(input_vec);
        let ree_array = builder.finish();
        let ree_array = ree_array.downcast::<Int32Array>().unwrap();

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
    fn test_run_iterator_comprehensive() {
        // Test forward and backward iterator for different array lengths.
        let logical_lengths = vec![1_usize, 2, 3, 4, 15, 16, 17, 63, 64, 65];

        for logical_len in logical_lengths {
            let input_array = build_input_array(logical_len);

            let mut run_array_builder = PrimitiveRunBuilder::<Int32Type, Int32Type>::new();
            run_array_builder.extend(input_array.iter().copied());
            let run_array = run_array_builder.finish();
            let typed_array = run_array.downcast::<Int32Array>().unwrap();

            // test forward iterator
            let mut input_iter = input_array.iter().copied();
            let mut run_array_iter = typed_array.into_iter();
            for _ in 0..logical_len {
                assert_eq!(input_iter.next(), run_array_iter.next());
            }
            assert_eq!(None, run_array_iter.next());

            // test reverse iterator
            let mut input_iter = input_array.iter().rev().copied();
            let mut run_array_iter = typed_array.into_iter().rev();
            for _ in 0..logical_len {
                assert_eq!(input_iter.next(), run_array_iter.next());
            }
            assert_eq!(None, run_array_iter.next());
        }
    }

    #[test]
    fn test_string_array_iter_round_trip() {
        let input_vec = vec!["ab", "ab", "ba", "cc", "cc"];
        let input_ree_array: Int64RunArray = input_vec.into_iter().collect();
        let string_ree_array = input_ree_array.downcast::<StringArray>().unwrap();

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

        let result_asref: Vec<Option<&str>> = result.iter().map(|f| f.as_deref()).collect();

        let expected_vec = vec![
            Some("abb"),
            Some("abb"),
            Some("bab"),
            Some("ccb"),
            Some("ccb"),
        ];

        assert_eq!(expected_vec, result_asref);
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
    fn test_sliced_run_array_iterator() {
        let total_len = 80;
        let input_array = build_input_array(total_len);

        // Encode the input_array to run array
        let mut builder =
            PrimitiveRunBuilder::<Int16Type, Int32Type>::with_capacity(input_array.len());
        builder.extend(input_array.iter().copied());
        let run_array = builder.finish();

        // test for all slice lengths.
        for slice_len in 1..=total_len {
            // test for offset = 0, slice length = slice_len
            let sliced_run_array: RunArray<Int16Type> =
                run_array.slice(0, slice_len).into_data().into();
            let sliced_typed_run_array = sliced_run_array
                .downcast::<PrimitiveArray<Int32Type>>()
                .unwrap();

            // Iterate on sliced typed run array
            let actual: Vec<Option<i32>> = sliced_typed_run_array.into_iter().collect();
            let expected: Vec<Option<i32>> = input_array.iter().take(slice_len).copied().collect();
            assert_eq!(expected, actual);

            // test for offset = total_len - slice_len, length = slice_len
            let sliced_run_array: RunArray<Int16Type> = run_array
                .slice(total_len - slice_len, slice_len)
                .into_data()
                .into();
            let sliced_typed_run_array = sliced_run_array
                .downcast::<PrimitiveArray<Int32Type>>()
                .unwrap();

            // Iterate on sliced typed run array
            let actual: Vec<Option<i32>> = sliced_typed_run_array.into_iter().collect();
            let expected: Vec<Option<i32>> = input_array
                .iter()
                .skip(total_len - slice_len)
                .copied()
                .collect();
            assert_eq!(expected, actual);
        }
    }
}
