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

//! Rewrite variable-length arrays so that null entries point to empty offset
//! ranges, while preserving the original null mask.
//!
//! Some variable-length array types (`Binary`, `Utf8`, `List`, `Map`, ...) can
//! legally hold null entries whose offsets still reference real bytes/values
//! in the underlying buffer. Iterating the child values exposes that data,
//! which is rarely what callers want. This module provides:
//!
//! * [`has_non_empty_nulls`] - cheap check for whether the array contains
//!   any such null entries.
//! * [`cleanup_non_empty_nulls`] - produces an equivalent array where every
//!   null entry has a zero-length offset range.

use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, GenericByteArray, GenericListArray, MapArray, OffsetSizeTrait, UInt32Array,
    cast::AsArray, make_array, new_null_array, types::ByteArrayType,
};
use arrow_buffer::{Buffer, OffsetBuffer};
use arrow_schema::{ArrowError, DataType};

/// Return true if there are no nulls pointing to non-empty values.
///
/// This will return true if [`cleanup_non_empty_nulls`] will do anything.
///
/// This should be called before [`cleanup_non_empty_nulls`] to avoid unnecessary work.
pub fn has_non_empty_nulls(array: &dyn Array) -> Result<bool, ArrowError> {
    Ok(match array.data_type() {
        DataType::Binary => array.as_binary::<i32>().has_non_empty_nulls(),
        DataType::LargeBinary => array.as_binary::<i64>().has_non_empty_nulls(),
        DataType::Utf8 => array.as_string::<i32>().has_non_empty_nulls(),
        DataType::LargeUtf8 => array.as_string::<i64>().has_non_empty_nulls(),
        DataType::List(_) => array.as_list::<i32>().has_non_empty_nulls(),
        DataType::LargeList(_) => array.as_list::<i64>().has_non_empty_nulls(),
        DataType::Map(_, _) => array.as_map().has_non_empty_nulls(),
        dt => {
            return Err(ArrowError::InvalidArgumentError(format!(
                "data type {dt:?} is not supported"
            )));
        }
    })
}

/// Create a new list/map/bytes array with the same nulls as the original list/map/bytes array but all the nulls
/// are pointing to an empty list/map/byte slice.
///
/// Users should first check if [`has_non_empty_nulls`] returns true before calling this method
/// to avoid unnecessary work.
///
/// This is useful when wanting to go over the list/map/bytes values
/// and not wanting to deal with values that are not used by the list/map/bytes.
pub fn cleanup_non_empty_nulls(array: ArrayRef) -> Result<ArrayRef, ArrowError> {
    match array.data_type() {
        DataType::Binary => array.as_binary::<i32>().cleanup_non_empty_nulls(),
        DataType::LargeBinary => array.as_binary::<i64>().cleanup_non_empty_nulls(),
        DataType::Utf8 => array.as_string::<i32>().cleanup_non_empty_nulls(),
        DataType::LargeUtf8 => array.as_string::<i64>().cleanup_non_empty_nulls(),
        DataType::List(_) => array.as_list::<i32>().cleanup_non_empty_nulls(),
        DataType::LargeList(_) => array.as_list::<i64>().cleanup_non_empty_nulls(),
        DataType::Map(_, _) => array.as_map().cleanup_non_empty_nulls(),
        dt => Err(ArrowError::InvalidArgumentError(format!(
            "data type {dt:?} is not supported"
        ))),
    }
}

/// Helper trait to make it easier to implement the cleanup nulls for variable length with offsets
trait VariableLengthArrayExt<OffsetSize: OffsetSizeTrait>: Array + Clone + Sized + 'static {
    /// Get the offsets of the variable length array.
    fn get_offsets(&self) -> &OffsetBuffer<OffsetSize>;

    fn has_non_empty_nulls(&self) -> bool {
        self.get_offsets().has_non_empty_nulls(self.nulls())
    }

    /// Create a new list/map/bytes array with the same nulls as the original list/map/bytes array but all the nulls
    /// are pointing to an empty list/map/byte slice.
    ///
    /// Users should first check if [`Self::has_non_empty_nulls`] returns true before calling this method
    /// to avoid unnecessary work.
    ///
    /// This is useful when wanting to go over the list/map/bytes values
    /// and not wanting to deal with values that are not used by the list/map/bytes.
    fn cleanup_non_empty_nulls(&self) -> Result<ArrayRef, ArrowError> {
        let Some(nulls) = self.nulls().filter(|n| n.null_count() > 0) else {
            // If no nulls, return as is
            return Ok(Arc::new(self.clone()));
        };

        // Find an empty value so we can use the `take` kernel
        let index_of_empty_value_to_reuse: Option<u32> = self
            .get_offsets()
            .lengths()
            .position(|length| length == 0)
            .map(|index| index as u32);

        if let Some(index_of_empty_value) = index_of_empty_value_to_reuse {
            let buffer = {
                let iter = nulls.iter().enumerate().map(|(i, is_valid)| {
                    if is_valid {
                        i as u32
                    } else {
                        index_of_empty_value
                    }
                });

                // SAFETY: upper bound is trusted because `iter` is just map over `nulls`
                unsafe { Buffer::from_trusted_len_iter(iter) }
            };

            let cleanup_array = crate::take::take(
                &self,
                &UInt32Array::new(buffer.into(), None),
                Some(crate::take::TakeOptions {
                    // The indices are derived from the length
                    check_bounds: false,
                }),
            )?;

            let array_data_with_correct_nulls = {
                let builder = cleanup_array
                    .into_data()
                    .into_builder()
                    .nulls(self.nulls().cloned());
                unsafe {
                    // This is safe as we are only updating the nulls
                    builder.build_unchecked()
                }
            };

            let array = make_array(array_data_with_correct_nulls);

            return Ok(array);
        }

        // Create a new list with 1 null item so we can use the `interleave` kernel
        let array_with_null_item = new_null_array(self.data_type(), 1);

        let interleave_indices = nulls
            .iter()
            .enumerate()
            // If the value is null, we want to take the null item from `list_with_null_item`
            .map(|(i, is_valid)| if is_valid { (0, i) } else { (1, 0) })
            .collect::<Vec<_>>();

        let cleaned_up_array = crate::interleave::interleave(
            &[&self, &array_with_null_item],
            interleave_indices.as_slice(),
        )?;

        Ok(cleaned_up_array)
    }
}

impl<OffsetSize: OffsetSizeTrait> VariableLengthArrayExt<OffsetSize>
    for GenericListArray<OffsetSize>
{
    fn get_offsets(&self) -> &OffsetBuffer<OffsetSize> {
        self.offsets()
    }
}

impl VariableLengthArrayExt<i32> for MapArray {
    fn get_offsets(&self) -> &OffsetBuffer<i32> {
        self.offsets()
    }
}

impl<T: ByteArrayType> VariableLengthArrayExt<T::Offset> for GenericByteArray<T> {
    fn get_offsets(&self) -> &OffsetBuffer<T::Offset> {
        self.offsets()
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::types::Int32Type;
    use arrow_array::{
        BinaryArray, Int32Array, LargeBinaryArray, LargeStringArray, StringArray, StructArray,
        UInt32Array,
    };
    use arrow_buffer::{Buffer, NullBuffer, ScalarBuffer};
    use arrow_schema::{Field, Fields};

    use super::*;
    use std::sync::Arc;

    /// Build a `MapArray` with the given offsets/nulls and `Int32` keys/values.
    fn build_map_array(
        offsets: OffsetBuffer<i32>,
        keys: Int32Array,
        values: Int32Array,
        nulls: Option<NullBuffer>,
    ) -> MapArray {
        let entries_fields = Fields::from(vec![
            Field::new("keys", DataType::Int32, false),
            Field::new("values", DataType::Int32, false),
        ]);
        let entries = StructArray::new(
            entries_fields.clone(),
            vec![Arc::new(keys) as ArrayRef, Arc::new(values) as ArrayRef],
            None,
        );
        let field = Arc::new(Field::new(
            "entries",
            DataType::Struct(entries_fields),
            false,
        ));
        MapArray::new(field, offsets, entries, nulls, false)
    }

    // ===== All nulls already point to empty values =====
    // Cleanup should be a no-op (logically equivalent array).

    #[test]
    fn cleanup_when_all_nulls_are_empty_binary() {
        let values = Buffer::from(b"helloworld".as_slice());
        let offsets = OffsetBuffer::<i32>::from_lengths(vec![5, 0, 5]);
        let nulls = NullBuffer::from(vec![true, false, true]);
        let binary = BinaryArray::new(offsets, values, Some(nulls));
        let array: ArrayRef = Arc::new(binary.clone());

        assert!(!has_non_empty_nulls(array.as_ref()).unwrap());

        let cleaned = cleanup_non_empty_nulls(array).unwrap();
        assert_eq!(cleaned.as_binary::<i32>(), &binary);
    }

    #[test]
    fn cleanup_when_all_nulls_are_empty_large_binary() {
        let values = Buffer::from(b"helloworld".as_slice());
        let offsets = OffsetBuffer::<i64>::from_lengths(vec![5, 0, 5]);
        let nulls = NullBuffer::from(vec![true, false, true]);
        let binary = LargeBinaryArray::new(offsets, values, Some(nulls));
        let array: ArrayRef = Arc::new(binary.clone());

        assert!(!has_non_empty_nulls(array.as_ref()).unwrap());

        let cleaned = cleanup_non_empty_nulls(array).unwrap();
        assert_eq!(cleaned.as_binary::<i64>(), &binary);
    }

    #[test]
    fn cleanup_when_all_nulls_are_empty_string() {
        let values = Buffer::from(b"helloworld".as_slice());
        let offsets = OffsetBuffer::<i32>::from_lengths(vec![5, 0, 5]);
        let nulls = NullBuffer::from(vec![true, false, true]);
        let string = StringArray::new(offsets, values, Some(nulls));
        let array: ArrayRef = Arc::new(string.clone());

        assert!(!has_non_empty_nulls(array.as_ref()).unwrap());

        let cleaned = cleanup_non_empty_nulls(array).unwrap();
        assert_eq!(cleaned.as_string::<i32>(), &string);
    }

    #[test]
    fn cleanup_when_all_nulls_are_empty_large_string() {
        let values = Buffer::from(b"helloworld".as_slice());
        let offsets = OffsetBuffer::<i64>::from_lengths(vec![5, 0, 5]);
        let nulls = NullBuffer::from(vec![true, false, true]);
        let string = LargeStringArray::new(offsets, values, Some(nulls));
        let array: ArrayRef = Arc::new(string.clone());

        assert!(!has_non_empty_nulls(array.as_ref()).unwrap());

        let cleaned = cleanup_non_empty_nulls(array).unwrap();
        assert_eq!(cleaned.as_string::<i64>(), &string);
    }

    #[test]
    fn cleanup_when_all_nulls_are_empty_list() {
        let list_values = UInt32Array::from(vec![1, 2, 3, 4, 5]);
        let offsets = OffsetBuffer::<i32>::from_lengths(vec![3, 0, 2]);
        let nulls = NullBuffer::from(vec![true, false, true]);

        let list_field = Arc::new(Field::new("item", list_values.data_type().clone(), false));
        let list = GenericListArray::<i32>::new(
            Arc::clone(&list_field),
            offsets,
            Arc::new(list_values),
            Some(nulls),
        );
        let array: ArrayRef = Arc::new(list.clone());

        assert!(!has_non_empty_nulls(array.as_ref()).unwrap());

        let cleaned = cleanup_non_empty_nulls(array).unwrap();
        assert_eq!(cleaned.as_list::<i32>(), &list);
    }

    #[test]
    fn cleanup_when_all_nulls_are_empty_large_list() {
        let list_values = UInt32Array::from(vec![1, 2, 3, 4, 5]);
        let offsets = OffsetBuffer::<i64>::from_lengths(vec![3, 0, 2]);
        let nulls = NullBuffer::from(vec![true, false, true]);

        let list_field = Arc::new(Field::new("item", list_values.data_type().clone(), false));
        let list = GenericListArray::<i64>::new(
            Arc::clone(&list_field),
            offsets,
            Arc::new(list_values),
            Some(nulls),
        );
        let array: ArrayRef = Arc::new(list.clone());

        assert!(!has_non_empty_nulls(array.as_ref()).unwrap());

        let cleaned = cleanup_non_empty_nulls(array).unwrap();
        assert_eq!(cleaned.as_list::<i64>(), &list);
    }

    #[test]
    fn cleanup_when_all_nulls_are_empty_map() {
        let keys = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let values = Int32Array::from(vec![10, 20, 30, 40, 50]);
        let offsets = OffsetBuffer::<i32>::from_lengths(vec![3, 0, 2]);
        let nulls = NullBuffer::from(vec![true, false, true]);
        let map = build_map_array(offsets, keys, values, Some(nulls));
        let array: ArrayRef = Arc::new(map.clone());

        assert!(!has_non_empty_nulls(array.as_ref()).unwrap());

        let cleaned = cleanup_non_empty_nulls(array).unwrap();
        assert_eq!(cleaned.as_map(), &map);
    }

    // ===== No nulls at all =====
    // Cleanup should return the same array unchanged.

    #[test]
    fn list_null_cleanup_on_list_with_no_nulls() {
        let list_values = UInt32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let offsets = OffsetBuffer::<i32>::from_lengths(vec![3, 4, 2]);

        let list_field = Arc::new(Field::new("item", list_values.data_type().clone(), false));
        let list: ArrayRef = Arc::new(GenericListArray::<i32>::new(
            Arc::clone(&list_field),
            offsets,
            Arc::new(list_values),
            None,
        ));

        let list_with_cleaned_nulls = cleanup_non_empty_nulls(Arc::clone(&list)).unwrap();
        let list_with_cleaned_nulls = list_with_cleaned_nulls.as_list::<i32>();

        assert_eq!(
            list.as_list::<i32>(),
            list_with_cleaned_nulls,
            "should have the same list"
        );
    }

    #[test]
    fn cleanup_when_no_nulls_binary() {
        let binary = BinaryArray::from_iter_values([b"foo".as_slice(), b"bar", b"baz"]);
        let array: ArrayRef = Arc::new(binary.clone());

        assert!(!has_non_empty_nulls(array.as_ref()).unwrap());

        let cleaned = cleanup_non_empty_nulls(array).unwrap();
        assert_eq!(cleaned.as_binary::<i32>(), &binary);
    }

    #[test]
    fn cleanup_when_no_nulls_string() {
        let string = StringArray::from_iter_values(["foo", "bar", "baz"]);
        let array: ArrayRef = Arc::new(string.clone());

        assert!(!has_non_empty_nulls(array.as_ref()).unwrap());

        let cleaned = cleanup_non_empty_nulls(array).unwrap();
        assert_eq!(cleaned.as_string::<i32>(), &string);
    }

    #[test]
    fn cleanup_when_no_nulls_map() {
        let keys = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let values = Int32Array::from(vec![10, 20, 30, 40, 50]);
        let offsets = OffsetBuffer::<i32>::from_lengths(vec![3, 0, 2]);
        let map = build_map_array(offsets, keys, values, None);
        let array: ArrayRef = Arc::new(map.clone());

        assert!(!has_non_empty_nulls(array.as_ref()).unwrap());

        let cleaned = cleanup_non_empty_nulls(array).unwrap();
        assert_eq!(cleaned.as_map(), &map);
    }

    // ===== Non-empty nulls exist, but only outside the sliced range =====
    // `has_non_empty_nulls` and `cleanup_non_empty_nulls` should respect the slice view.

    #[test]
    fn cleanup_when_non_empty_nulls_outside_sliced_binary() {
        // Full array: ["foo", NULL->"bar"(non-empty), "baz", NULL->""(empty), "qux"]
        // Slicing to [2, 5) excludes the problematic null at index 1.
        let values = Buffer::from(b"foobarbazqux".as_slice());
        // offsets:    0   3       6      9    9   12
        let offsets = OffsetBuffer::<i32>::new(ScalarBuffer::<i32>::from(vec![0, 3, 6, 9, 9, 12]));
        let nulls = NullBuffer::from(vec![true, false, true, false, true]);
        let binary = BinaryArray::new(offsets, values, Some(nulls));
        let full: ArrayRef = Arc::new(binary);

        assert!(
            has_non_empty_nulls(full.as_ref()).unwrap(),
            "full array has a non-empty null"
        );

        let sliced = full.slice(2, 3);
        assert!(
            !has_non_empty_nulls(sliced.as_ref()).unwrap(),
            "sliced view has no non-empty nulls"
        );

        let cleaned = cleanup_non_empty_nulls(Arc::clone(&sliced)).unwrap();
        assert_eq!(cleaned.as_binary::<i32>(), sliced.as_binary::<i32>());
    }

    #[test]
    fn cleanup_when_non_empty_nulls_outside_sliced_list() {
        // Same idea as the binary slice test, but for a `ListArray`.
        let list_values = UInt32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let offsets = OffsetBuffer::<i32>::new(ScalarBuffer::<i32>::from(vec![0, 2, 5, 7, 7, 9]));
        let nulls = NullBuffer::from(vec![true, false, true, false, true]);
        let list_field = Arc::new(Field::new("item", list_values.data_type().clone(), false));
        let list = GenericListArray::<i32>::new(
            Arc::clone(&list_field),
            offsets,
            Arc::new(list_values),
            Some(nulls),
        );
        let full: ArrayRef = Arc::new(list);

        assert!(has_non_empty_nulls(full.as_ref()).unwrap());

        let sliced = full.slice(2, 3);
        assert!(!has_non_empty_nulls(sliced.as_ref()).unwrap());

        let cleaned = cleanup_non_empty_nulls(Arc::clone(&sliced)).unwrap();
        assert_eq!(cleaned.as_list::<i32>(), sliced.as_list::<i32>());
    }

    // ===== Some nulls point to non-empty, some point to empty =====

    #[test]
    fn list_cleanup_nulls_with_null_pointing_to_non_empty_list_and_have_empty_list() {
        let list_values = UInt32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let offsets = OffsetBuffer::<i32>::from_lengths(vec![3, 4, 0, 2]);
        let input_nulls = NullBuffer::from(vec![true, false, true, true]);

        let list_field = Arc::new(Field::new("item", list_values.data_type().clone(), false));
        let list: ArrayRef = Arc::new(GenericListArray::<i32>::new(
            Arc::clone(&list_field),
            offsets,
            Arc::new(list_values),
            Some(input_nulls.clone()),
        ));

        assert!(has_non_empty_nulls(list.as_ref()).unwrap());

        let list_with_cleaned_nulls = cleanup_non_empty_nulls(list).unwrap();
        assert!(!has_non_empty_nulls(list_with_cleaned_nulls.as_ref()).unwrap());
        let list_with_cleaned_nulls = list_with_cleaned_nulls.as_list::<i32>();

        let (field, offsets, values, nulls) = list_with_cleaned_nulls.clone().into_parts();

        assert_eq!(field, list_field, "List field should not change");
        assert_eq!(
            offsets.lengths().collect::<Vec<_>>(),
            vec![3, 0, 0, 2],
            "nulls should point to zero length list"
        );
        assert_eq!(values.as_ref(), &UInt32Array::from(vec![1, 2, 3, 8, 9]));
        assert_eq!(nulls, Some(input_nulls), "Nulls should not change");
    }

    #[test]
    fn binary_cleanup_nulls_with_null_pointing_to_non_empty_and_have_empty() {
        // values:    foo bar baz
        // offsets:   0  3   6  6  9
        // nulls:     T  F   T  T
        // Null at idx 1 points to "bar" (non-empty); null at idx 2 is empty already.
        let values = Buffer::from(b"foobarbaz".as_slice());
        let offsets = OffsetBuffer::<i32>::new(ScalarBuffer::<i32>::from(vec![0, 3, 6, 6, 9]));
        let input_nulls = NullBuffer::from(vec![true, false, true, true]);
        let binary = BinaryArray::new(offsets, values, Some(input_nulls.clone()));
        let array: ArrayRef = Arc::new(binary);

        assert!(has_non_empty_nulls(array.as_ref()).unwrap());

        let cleaned = cleanup_non_empty_nulls(array).unwrap();
        assert!(!has_non_empty_nulls(cleaned.as_ref()).unwrap());
        let cleaned = cleaned.as_binary::<i32>();

        // All nulls must now have zero length.
        assert_eq!(
            cleaned.offsets().lengths().collect::<Vec<_>>(),
            vec![3, 0, 0, 3]
        );
        assert_eq!(cleaned.nulls(), Some(&input_nulls));
        assert_eq!(cleaned.value(0), b"foo");
        assert_eq!(cleaned.value(3), b"baz");
    }

    #[test]
    fn map_cleanup_when_null_pointing_to_non_empty_and_have_empty() {
        let keys = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let values = Int32Array::from(vec![10, 20, 30, 40, 50, 60, 70, 80, 90]);
        let offsets = OffsetBuffer::<i32>::from_lengths(vec![3, 4, 0, 2]);
        let input_nulls = NullBuffer::from(vec![true, false, true, true]);
        let map = build_map_array(offsets, keys, values, Some(input_nulls.clone()));
        let array: ArrayRef = Arc::new(map);

        assert!(has_non_empty_nulls(array.as_ref()).unwrap());

        let cleaned = cleanup_non_empty_nulls(array).unwrap();
        assert!(!has_non_empty_nulls(cleaned.as_ref()).unwrap());
        let cleaned = cleaned.as_map();

        assert_eq!(
            cleaned.offsets().lengths().collect::<Vec<_>>(),
            vec![3, 0, 0, 2]
        );
        assert_eq!(cleaned.nulls(), Some(&input_nulls));
        assert_eq!(
            cleaned.keys().as_primitive::<Int32Type>(),
            &Int32Array::from(vec![1, 2, 3, 8, 9]),
        );
        assert_eq!(
            cleaned.values().as_primitive::<Int32Type>(),
            &Int32Array::from(vec![10, 20, 30, 80, 90]),
        );
    }

    // ===== All nulls point to non-empty values =====

    #[test]
    fn list_cleanup_nulls_with_null_pointing_to_non_empty_list_and_does_not_have_empty_list() {
        let list_values = UInt32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let offsets = OffsetBuffer::<i32>::from_lengths(vec![3, 4, 2]);
        let input_nulls = NullBuffer::from(vec![true, false, true]);

        let list_field = Arc::new(Field::new("item", list_values.data_type().clone(), false));
        let list: ArrayRef = Arc::new(GenericListArray::<i32>::new(
            Arc::clone(&list_field),
            offsets,
            Arc::new(list_values),
            Some(input_nulls.clone()),
        ));

        assert!(has_non_empty_nulls(list.as_ref()).unwrap());

        let list_with_cleaned_nulls = cleanup_non_empty_nulls(list).unwrap();
        assert!(!has_non_empty_nulls(list_with_cleaned_nulls.as_ref()).unwrap());
        let list_with_cleaned_nulls = list_with_cleaned_nulls.as_list::<i32>();

        let (field, offsets, values, nulls) = list_with_cleaned_nulls.clone().into_parts();

        assert_eq!(field, list_field, "List field should not change");
        assert_eq!(
            offsets.lengths().collect::<Vec<_>>(),
            vec![3, 0, 2],
            "nulls should point to zero length list"
        );
        assert_eq!(values.as_ref(), &UInt32Array::from(vec![1, 2, 3, 8, 9]));
        assert_eq!(nulls, Some(input_nulls), "Nulls should not change");
    }

    #[test]
    fn binary_cleanup_when_all_nulls_point_to_non_empty_and_no_empty_exists() {
        let values = Buffer::from(b"foobarbaz".as_slice());
        let offsets = OffsetBuffer::<i32>::from_lengths(vec![3, 3, 3]);
        let input_nulls = NullBuffer::from(vec![true, false, true]);
        let binary = BinaryArray::new(offsets, values, Some(input_nulls.clone()));
        let array: ArrayRef = Arc::new(binary);

        assert!(has_non_empty_nulls(array.as_ref()).unwrap());

        let cleaned = cleanup_non_empty_nulls(array).unwrap();
        assert!(!has_non_empty_nulls(cleaned.as_ref()).unwrap());
        let cleaned = cleaned.as_binary::<i32>();

        assert_eq!(
            cleaned.offsets().lengths().collect::<Vec<_>>(),
            vec![3, 0, 3]
        );
        assert_eq!(cleaned.nulls(), Some(&input_nulls));
        assert_eq!(cleaned.value(0), b"foo");
        assert_eq!(cleaned.value(2), b"baz");
    }

    #[test]
    fn map_cleanup_when_all_nulls_point_to_non_empty() {
        let keys = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let values = Int32Array::from(vec![10, 20, 30, 40, 50, 60, 70, 80, 90]);
        let offsets = OffsetBuffer::<i32>::from_lengths(vec![3, 4, 2]);
        let input_nulls = NullBuffer::from(vec![true, false, true]);
        let map = build_map_array(offsets, keys, values, Some(input_nulls.clone()));
        let array: ArrayRef = Arc::new(map);

        assert!(has_non_empty_nulls(array.as_ref()).unwrap());

        let cleaned = cleanup_non_empty_nulls(array).unwrap();
        assert!(!has_non_empty_nulls(cleaned.as_ref()).unwrap());
        let cleaned = cleaned.as_map();

        assert_eq!(
            cleaned.offsets().lengths().collect::<Vec<_>>(),
            vec![3, 0, 2]
        );
        assert_eq!(cleaned.nulls(), Some(&input_nulls));
        // The null at index 1 originally referenced keys/values 4..8;
        // those entries should be dropped from the underlying child arrays.
        assert_eq!(
            cleaned.keys().as_primitive::<Int32Type>(),
            &Int32Array::from(vec![1, 2, 3, 8, 9]),
        );
        assert_eq!(
            cleaned.values().as_primitive::<Int32Type>(),
            &Int32Array::from(vec![10, 20, 30, 80, 90]),
        );
    }

    // ===== Cleanup does not recurse into inner arrays =====

    #[test]
    fn list_cleanup_does_not_recurse_into_inner_string_nulls() {
        // Inner string: the null at index 1 points to the bytes "bar"
        // (a non-empty null at the inner level).
        let inner_values = Buffer::from(b"foobarbazqux".as_slice());
        let inner_offsets =
            OffsetBuffer::<i32>::new(ScalarBuffer::<i32>::from(vec![0, 3, 6, 9, 9, 12]));
        let inner_nulls = NullBuffer::from(vec![true, false, true, true, true]);
        let inner = StringArray::new(inner_offsets, inner_values, Some(inner_nulls));
        assert!(
            has_non_empty_nulls(&inner).unwrap(),
            "sanity: inner string has a non-empty null"
        );

        // Outer list wrapping the inner string. Index 1 is an outer null whose
        // offsets still cover real entries; index 2 is an empty entry so cleanup
        // can use it as the substitute for the cleaned-up outer null.
        let outer_offsets = OffsetBuffer::<i32>::from_lengths(vec![2, 2, 0, 1]);
        let outer_nulls = NullBuffer::from(vec![true, false, true, true]);
        let list_field = Arc::new(Field::new("item", DataType::Utf8, true));
        let list: ArrayRef = Arc::new(GenericListArray::<i32>::new(
            list_field,
            outer_offsets,
            Arc::new(inner),
            Some(outer_nulls),
        ));
        assert!(has_non_empty_nulls(list.as_ref()).unwrap());

        let cleaned = cleanup_non_empty_nulls(list).unwrap();

        // Outer list is fully cleaned at its own level.
        assert!(!has_non_empty_nulls(cleaned.as_ref()).unwrap());

        // The inner string array still has a non-empty null - cleanup did not
        // recurse into it.
        let inner_after = cleaned.as_list::<i32>().values().as_string::<i32>();
        assert!(
            has_non_empty_nulls(inner_after).unwrap(),
            "inner non-empty nulls must be preserved"
        );
    }

    // ===== Empty arrays (length 0) =====
    // Cleanup is a no-op and `has_non_empty_nulls` is false.

    fn assert_cleanup_is_noop_on_empty(array: ArrayRef) {
        assert_eq!(array.len(), 0);
        assert!(!has_non_empty_nulls(array.as_ref()).unwrap());
        let cleaned = cleanup_non_empty_nulls(Arc::clone(&array)).unwrap();
        assert_eq!(cleaned.len(), 0);
        assert!(!has_non_empty_nulls(cleaned.as_ref()).unwrap());
        assert_eq!(cleaned.to_data(), array.to_data());
    }

    #[test]
    fn cleanup_on_empty_binary() {
        let binary = BinaryArray::new(
            OffsetBuffer::<i32>::new_empty(),
            Buffer::from(&[] as &[u8]),
            None,
        );
        assert_cleanup_is_noop_on_empty(Arc::new(binary));
    }

    #[test]
    fn cleanup_on_empty_large_binary() {
        let binary = LargeBinaryArray::new(
            OffsetBuffer::<i64>::new_empty(),
            Buffer::from(&[] as &[u8]),
            None,
        );
        assert_cleanup_is_noop_on_empty(Arc::new(binary));
    }

    #[test]
    fn cleanup_on_empty_string() {
        let string = StringArray::new(
            OffsetBuffer::<i32>::new_empty(),
            Buffer::from(&[] as &[u8]),
            None,
        );
        assert_cleanup_is_noop_on_empty(Arc::new(string));
    }

    #[test]
    fn cleanup_on_empty_large_string() {
        let string = LargeStringArray::new(
            OffsetBuffer::<i64>::new_empty(),
            Buffer::from(&[] as &[u8]),
            None,
        );
        assert_cleanup_is_noop_on_empty(Arc::new(string));
    }

    #[test]
    fn cleanup_on_empty_list() {
        let list_field = Arc::new(Field::new("item", DataType::UInt32, false));
        let list = GenericListArray::<i32>::new(
            list_field,
            OffsetBuffer::<i32>::new_empty(),
            Arc::new(UInt32Array::from(Vec::<u32>::new())),
            None,
        );
        assert_cleanup_is_noop_on_empty(Arc::new(list));
    }

    #[test]
    fn cleanup_on_empty_large_list() {
        let list_field = Arc::new(Field::new("item", DataType::UInt32, false));
        let list = GenericListArray::<i64>::new(
            list_field,
            OffsetBuffer::<i64>::new_empty(),
            Arc::new(UInt32Array::from(Vec::<u32>::new())),
            None,
        );
        assert_cleanup_is_noop_on_empty(Arc::new(list));
    }

    #[test]
    fn cleanup_on_empty_map() {
        let map = build_map_array(
            OffsetBuffer::<i32>::new_empty(),
            Int32Array::from(Vec::<i32>::new()),
            Int32Array::from(Vec::<i32>::new()),
            None,
        );
        assert_cleanup_is_noop_on_empty(Arc::new(map));
    }

    // Empty arrays produced via slicing a non-empty array down to length 0.

    #[test]
    fn cleanup_on_zero_length_slice_of_list_with_non_empty_nulls() {
        // The full array has a non-empty null, but slicing to length 0 hides it.
        let list_values = UInt32Array::from(vec![1, 2, 3, 4, 5]);
        let offsets = OffsetBuffer::<i32>::new(ScalarBuffer::<i32>::from(vec![0, 3, 5]));
        let nulls = NullBuffer::from(vec![true, false]);
        let list_field = Arc::new(Field::new("item", DataType::UInt32, false));
        let full: ArrayRef = Arc::new(GenericListArray::<i32>::new(
            list_field,
            offsets,
            Arc::new(list_values),
            Some(nulls),
        ));
        assert!(has_non_empty_nulls(full.as_ref()).unwrap());

        let sliced = full.slice(0, 0);
        assert_cleanup_is_noop_on_empty(sliced);
    }

    #[test]
    fn cleanup_on_zero_length_slice_of_map_with_non_empty_nulls() {
        let keys = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let values = Int32Array::from(vec![10, 20, 30, 40, 50]);
        let offsets = OffsetBuffer::<i32>::new(ScalarBuffer::<i32>::from(vec![0, 3, 5]));
        let nulls = NullBuffer::from(vec![true, false]);
        let map = build_map_array(offsets, keys, values, Some(nulls));
        let full: ArrayRef = Arc::new(map);
        assert!(has_non_empty_nulls(full.as_ref()).unwrap());

        let sliced = full.slice(0, 0);
        assert_cleanup_is_noop_on_empty(sliced);
    }

    // ===== Unsupported types =====

    #[test]
    fn unsupported_type_returns_error() {
        let array: ArrayRef = Arc::new(UInt32Array::from(vec![1, 2, 3]));
        assert!(has_non_empty_nulls(array.as_ref()).is_err());
        assert!(cleanup_non_empty_nulls(array).is_err());
    }
}
