use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, GenericByteArray, GenericListArray, MapArray, OffsetSizeTrait, UInt32Array, cast::AsArray, make_array, new_null_array, types::ByteArrayType
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
    dt => return Err(ArrowError::InvalidArgumentError(format!("data type {dt:?} is not supported")))
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
    DataType::Binary => array.as_binary::<i32>().cleanup_nulls(),
    DataType::LargeBinary => array.as_binary::<i64>().cleanup_nulls(),
    DataType::Utf8 => array.as_string::<i32>().cleanup_nulls(),
    DataType::LargeUtf8 => array.as_string::<i64>().cleanup_nulls(),
    DataType::List(_) => array.as_list::<i32>().cleanup_nulls(),
    DataType::LargeList(_) => array.as_list::<i64>().cleanup_nulls(),
    DataType::Map(_, _) => array.as_map().cleanup_nulls(),
    dt => Err(ArrowError::InvalidArgumentError(format!("data type {dt:?} is not supported")))
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
    /// Users should first check if [`Self::can_use_list_values_as_is`] returns true before calling this method
    /// to avoid unnecessary work.
    ///
    /// This is useful when wanting to go over the list/map/bytes values
    /// and not wanting to deal with values that are not used by the list/map/bytes.
    fn cleanup_nulls(&self) -> Result<ArrayRef, ArrowError> {
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
            let buffer = unsafe {
                let iter = nulls.iter().enumerate().map(|(i, is_valid)| {
                    if is_valid {
                        i as u32
                    } else {
                        index_of_empty_value
                    }
                });

                // SAFETY: upper bound is trusted because `iter` is just map over `nulls`
                Buffer::from_trusted_len_iter(iter)
            };

            let cleanup_array = crate::take::take(
                &self,
                &UInt32Array::new(buffer.into(), None),
                Some(crate::take::TakeOptions {
                    // The indices are derived from the length
                    check_bounds: false,
                }),
            )?;

            let array_data_with_correct_nulls = unsafe {
                cleanup_array
                    .into_data()
                    .into_builder()
                    .nulls(self.nulls().cloned())
                    // This is safe as we are only updating the nulls
                    .build_unchecked()
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
    use arrow_array::UInt32Array;
    use arrow_buffer::NullBuffer;
    use arrow_schema::Field;

    use super::*;
    use std::sync::Arc;
    
    // All nulls point to empty
    
    #[test]
    fn cleanup_when_all_nulls_are_empty_binary() {
      todo!()
    }
    
    #[test]
    fn cleanup_when_all_nulls_are_empty_large_binary() {
      todo!()
    }
    
    #[test]
    fn cleanup_when_all_nulls_are_empty_string() {
      todo!()
    }
    
    #[test]
    fn cleanup_when_all_nulls_are_empty_large_string() {
      todo!()
    }
    
    #[test]
    fn cleanup_when_all_nulls_are_empty_list() {
      todo!()
    }
    
    #[test]
    fn cleanup_when_all_nulls_are_empty_large_list() {
      todo!()
    }
    
    #[test]
    fn cleanup_when_all_nulls_are_empty_map() {
      todo!()
    }
    
    // TODO - add tests when there are no nulls
    // TODO - add tests when all nulls point to empty but outide of sliced array
    // TODO - add tests when some nulls point to non empty and some point to empty
    // TODO - add tests when all nulls point to non empty

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

        assert_eq!(list.as_list::<i32>(), list_with_cleaned_nulls, "should have the same list");
    }

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

        let list_with_cleaned_nulls = cleanup_non_empty_nulls(list).unwrap();
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

        let list_with_cleaned_nulls = cleanup_non_empty_nulls(list).unwrap();
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
}
