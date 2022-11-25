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

use crate::builder::{FixedSizeListBuilder, PrimitiveBuilder};
use crate::{
    make_array, print_long_array, Array, ArrayAccessor, ArrayRef, ArrowPrimitiveType,
};
use arrow_data::ArrayData;
use arrow_schema::DataType;
use std::any::Any;

/// A list array where each element is a fixed-size sequence of values with the same
/// type whose maximum length is represented by a i32.
///
/// # Example
///
/// ```
/// # use arrow_array::{Array, FixedSizeListArray, Int32Array};
/// # use arrow_data::ArrayData;
/// # use arrow_schema::{DataType, Field};
/// # use arrow_buffer::Buffer;
/// // Construct a value array
/// let value_data = ArrayData::builder(DataType::Int32)
///     .len(9)
///     .add_buffer(Buffer::from_slice_ref(&[0, 1, 2, 3, 4, 5, 6, 7, 8]))
///     .build()
///     .unwrap();
/// let list_data_type = DataType::FixedSizeList(
///     Box::new(Field::new("item", DataType::Int32, false)),
///     3,
/// );
/// let list_data = ArrayData::builder(list_data_type.clone())
///     .len(3)
///     .add_child_data(value_data.clone())
///     .build()
///     .unwrap();
/// let list_array = FixedSizeListArray::from(list_data);
/// let list0 = list_array.value(0);
/// let list1 = list_array.value(1);
/// let list2 = list_array.value(2);
///
/// assert_eq!( &[0, 1, 2], list0.as_any().downcast_ref::<Int32Array>().unwrap().values());
/// assert_eq!( &[3, 4, 5], list1.as_any().downcast_ref::<Int32Array>().unwrap().values());
/// assert_eq!( &[6, 7, 8], list2.as_any().downcast_ref::<Int32Array>().unwrap().values());
/// ```
///
/// For non generic lists, you may wish to consider using
/// [crate::array::FixedSizeBinaryArray]
#[derive(Clone)]
pub struct FixedSizeListArray {
    data: ArrayData,
    values: ArrayRef,
    length: i32,
}

impl FixedSizeListArray {
    /// Returns a reference to the values of this list.
    pub fn values(&self) -> ArrayRef {
        self.values.clone()
    }

    /// Returns a clone of the value type of this list.
    pub fn value_type(&self) -> DataType {
        self.values.data_ref().data_type().clone()
    }

    /// Returns ith value of this list array.
    pub fn value(&self, i: usize) -> ArrayRef {
        self.values
            .slice(self.value_offset(i) as usize, self.value_length() as usize)
    }

    /// Returns the offset for value at index `i`.
    ///
    /// Note this doesn't do any bound checking, for performance reason.
    #[inline]
    pub fn value_offset(&self, i: usize) -> i32 {
        self.value_offset_at(self.data.offset() + i)
    }

    /// Returns the length for an element.
    ///
    /// All elements have the same length as the array is a fixed size.
    #[inline]
    pub const fn value_length(&self) -> i32 {
        self.length
    }

    #[inline]
    const fn value_offset_at(&self, i: usize) -> i32 {
        i as i32 * self.length
    }

    /// Creates a [`FixedSizeListArray`] from an iterator of primitive values
    /// # Example
    /// ```
    /// # use arrow_array::FixedSizeListArray;
    /// # use arrow_array::types::Int32Type;
    ///
    /// let data = vec![
    ///    Some(vec![Some(0), Some(1), Some(2)]),
    ///    None,
    ///    Some(vec![Some(3), None, Some(5)]),
    ///    Some(vec![Some(6), Some(7), Some(45)]),
    /// ];
    /// let list_array = FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(data, 3);
    /// println!("{:?}", list_array);
    /// ```
    pub fn from_iter_primitive<T, P, I>(iter: I, length: i32) -> Self
    where
        T: ArrowPrimitiveType,
        P: IntoIterator<Item = Option<<T as ArrowPrimitiveType>::Native>>,
        I: IntoIterator<Item = Option<P>>,
    {
        let l = length as usize;
        let iter = iter.into_iter();
        let size_hint = iter.size_hint().0;
        let mut builder = FixedSizeListBuilder::with_capacity(
            PrimitiveBuilder::<T>::with_capacity(size_hint * l),
            length,
            size_hint,
        );

        for i in iter {
            match i {
                Some(p) => {
                    for t in p {
                        builder.values().append_option(t);
                    }
                    builder.append(true);
                }
                None => {
                    builder.values().append_nulls(l);
                    builder.append(false)
                }
            }
        }
        builder.finish()
    }
}

impl From<ArrayData> for FixedSizeListArray {
    fn from(data: ArrayData) -> Self {
        assert_eq!(
            data.buffers().len(),
            0,
            "FixedSizeListArray data should not contain a buffer for value offsets"
        );
        assert_eq!(
            data.child_data().len(),
            1,
            "FixedSizeListArray should contain a single child array (values array)"
        );
        let values = make_array(data.child_data()[0].clone());
        let length = match data.data_type() {
            DataType::FixedSizeList(_, len) => {
                if *len > 0 {
                    // check that child data is multiple of length
                    assert_eq!(
                        values.len() % *len as usize,
                        0,
                        "FixedSizeListArray child array length should be a multiple of {}",
                        len
                    );
                }

                *len
            }
            _ => {
                panic!("FixedSizeListArray data should contain a FixedSizeList data type")
            }
        };
        Self {
            data,
            values,
            length,
        }
    }
}

impl From<FixedSizeListArray> for ArrayData {
    fn from(array: FixedSizeListArray) -> Self {
        array.data
    }
}

impl Array for FixedSizeListArray {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data(&self) -> &ArrayData {
        &self.data
    }

    fn into_data(self) -> ArrayData {
        self.into()
    }
}

impl ArrayAccessor for FixedSizeListArray {
    type Item = ArrayRef;

    fn value(&self, index: usize) -> Self::Item {
        FixedSizeListArray::value(self, index)
    }

    unsafe fn value_unchecked(&self, index: usize) -> Self::Item {
        FixedSizeListArray::value(self, index)
    }
}

impl std::fmt::Debug for FixedSizeListArray {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "FixedSizeListArray<{}>\n[\n", self.value_length())?;
        print_long_array(self, f, |array, index, f| {
            std::fmt::Debug::fmt(&array.value(index), f)
        })?;
        write!(f, "]")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Int32Array;
    use arrow_buffer::{bit_util, Buffer};
    use arrow_schema::Field;

    #[test]
    fn test_fixed_size_list_array() {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(9)
            .add_buffer(Buffer::from_slice_ref([0, 1, 2, 3, 4, 5, 6, 7, 8]))
            .build()
            .unwrap();

        // Construct a list array from the above two
        let list_data_type = DataType::FixedSizeList(
            Box::new(Field::new("item", DataType::Int32, false)),
            3,
        );
        let list_data = ArrayData::builder(list_data_type.clone())
            .len(3)
            .add_child_data(value_data.clone())
            .build()
            .unwrap();
        let list_array = FixedSizeListArray::from(list_data);

        let values = list_array.values();
        assert_eq!(&value_data, values.data());
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(3, list_array.len());
        assert_eq!(0, list_array.null_count());
        assert_eq!(6, list_array.value_offset(2));
        assert_eq!(3, list_array.value_length());
        assert_eq!(
            0,
            list_array
                .value(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0)
        );
        for i in 0..3 {
            assert!(list_array.is_valid(i));
            assert!(!list_array.is_null(i));
        }

        // Now test with a non-zero offset
        let list_data = ArrayData::builder(list_data_type)
            .len(3)
            .offset(1)
            .add_child_data(value_data.clone())
            .build()
            .unwrap();
        let list_array = FixedSizeListArray::from(list_data);

        let values = list_array.values();
        assert_eq!(&value_data, values.data());
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(3, list_array.len());
        assert_eq!(0, list_array.null_count());
        assert_eq!(
            3,
            list_array
                .value(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0)
        );
        assert_eq!(6, list_array.value_offset(1));
        assert_eq!(3, list_array.value_length());
    }

    #[test]
    #[should_panic(
        expected = "FixedSizeListArray child array length should be a multiple of 3"
    )]
    // Different error messages, so skip for now
    // https://github.com/apache/arrow-rs/issues/1545
    #[cfg(not(feature = "force_validate"))]
    fn test_fixed_size_list_array_unequal_children() {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from_slice_ref([0, 1, 2, 3, 4, 5, 6, 7]))
            .build()
            .unwrap();

        // Construct a list array from the above two
        let list_data_type = DataType::FixedSizeList(
            Box::new(Field::new("item", DataType::Int32, false)),
            3,
        );
        let list_data = unsafe {
            ArrayData::builder(list_data_type)
                .len(3)
                .add_child_data(value_data)
                .build_unchecked()
        };
        drop(FixedSizeListArray::from(list_data));
    }

    #[test]
    fn test_fixed_size_list_array_slice() {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(10)
            .add_buffer(Buffer::from_slice_ref([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]))
            .build()
            .unwrap();

        // Set null buts for the nested array:
        //  [[0, 1], null, null, [6, 7], [8, 9]]
        // 01011001 00000001
        let mut null_bits: [u8; 1] = [0; 1];
        bit_util::set_bit(&mut null_bits, 0);
        bit_util::set_bit(&mut null_bits, 3);
        bit_util::set_bit(&mut null_bits, 4);

        // Construct a fixed size list array from the above two
        let list_data_type = DataType::FixedSizeList(
            Box::new(Field::new("item", DataType::Int32, false)),
            2,
        );
        let list_data = ArrayData::builder(list_data_type)
            .len(5)
            .add_child_data(value_data.clone())
            .null_bit_buffer(Some(Buffer::from(null_bits)))
            .build()
            .unwrap();
        let list_array = FixedSizeListArray::from(list_data);

        let values = list_array.values();
        assert_eq!(&value_data, values.data());
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(5, list_array.len());
        assert_eq!(2, list_array.null_count());
        assert_eq!(6, list_array.value_offset(3));
        assert_eq!(2, list_array.value_length());

        let sliced_array = list_array.slice(1, 4);
        assert_eq!(4, sliced_array.len());
        assert_eq!(1, sliced_array.offset());
        assert_eq!(2, sliced_array.null_count());

        for i in 0..sliced_array.len() {
            if bit_util::get_bit(&null_bits, sliced_array.offset() + i) {
                assert!(sliced_array.is_valid(i));
            } else {
                assert!(sliced_array.is_null(i));
            }
        }

        // Check offset and length for each non-null value.
        let sliced_list_array = sliced_array
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        assert_eq!(2, sliced_list_array.value_length());
        assert_eq!(6, sliced_list_array.value_offset(2));
        assert_eq!(8, sliced_list_array.value_offset(3));
    }

    #[test]
    #[should_panic(expected = "assertion failed: (offset + length) <= self.len()")]
    fn test_fixed_size_list_array_index_out_of_bound() {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(10)
            .add_buffer(Buffer::from_slice_ref([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]))
            .build()
            .unwrap();

        // Set null buts for the nested array:
        //  [[0, 1], null, null, [6, 7], [8, 9]]
        // 01011001 00000001
        let mut null_bits: [u8; 1] = [0; 1];
        bit_util::set_bit(&mut null_bits, 0);
        bit_util::set_bit(&mut null_bits, 3);
        bit_util::set_bit(&mut null_bits, 4);

        // Construct a fixed size list array from the above two
        let list_data_type = DataType::FixedSizeList(
            Box::new(Field::new("item", DataType::Int32, false)),
            2,
        );
        let list_data = ArrayData::builder(list_data_type)
            .len(5)
            .add_child_data(value_data)
            .null_bit_buffer(Some(Buffer::from(null_bits)))
            .build()
            .unwrap();
        let list_array = FixedSizeListArray::from(list_data);

        list_array.value(10);
    }
}
