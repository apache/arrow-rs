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

use std::any::Any;
use std::fmt;

use num::Integer;

use super::{
    array::print_long_array, make_array, raw_pointer::RawPtrBox, Array, ArrayData,
    ArrayRef, BooleanBufferBuilder, GenericListArrayIter, PrimitiveArray,
};
use crate::array::array::ArrayAccessor;
use crate::{
    buffer::MutableBuffer,
    datatypes::{ArrowNativeType, ArrowPrimitiveType, DataType, Field},
    error::ArrowError,
};

/// trait declaring an offset size, relevant for i32 vs i64 array types.
pub trait OffsetSizeTrait: ArrowNativeType + std::ops::AddAssign + Integer {
    const IS_LARGE: bool;
    const PREFIX: &'static str;
}

impl OffsetSizeTrait for i32 {
    const IS_LARGE: bool = false;
    const PREFIX: &'static str = "";
}

impl OffsetSizeTrait for i64 {
    const IS_LARGE: bool = true;
    const PREFIX: &'static str = "Large";
}

/// Generic struct for a variable-size list array.
///
/// Columnar format in Apache Arrow:
/// <https://arrow.apache.org/docs/format/Columnar.html#variable-size-list-layout>
///
/// For non generic lists, you may wish to consider using [`ListArray`] or [`LargeListArray`]`
pub struct GenericListArray<OffsetSize> {
    data: ArrayData,
    values: ArrayRef,
    value_offsets: RawPtrBox<OffsetSize>,
}

impl<OffsetSize: OffsetSizeTrait> GenericListArray<OffsetSize> {
    /// The data type constructor of list array.
    /// The input is the schema of the child array and
    /// the output is the [`DataType`], List or LargeList.
    pub const DATA_TYPE_CONSTRUCTOR: fn(Box<Field>) -> DataType = if OffsetSize::IS_LARGE
    {
        DataType::LargeList
    } else {
        DataType::List
    };

    /// Returns a reference to the values of this list.
    pub fn values(&self) -> ArrayRef {
        self.values.clone()
    }

    /// Returns a clone of the value type of this list.
    pub fn value_type(&self) -> DataType {
        self.values.data_ref().data_type().clone()
    }

    /// Returns ith value of this list array.
    /// # Safety
    /// Caller must ensure that the index is within the array bounds
    pub unsafe fn value_unchecked(&self, i: usize) -> ArrayRef {
        let end = *self.value_offsets().get_unchecked(i + 1);
        let start = *self.value_offsets().get_unchecked(i);
        self.values
            .slice(start.to_usize().unwrap(), (end - start).to_usize().unwrap())
    }

    /// Returns ith value of this list array.
    pub fn value(&self, i: usize) -> ArrayRef {
        let end = self.value_offsets()[i + 1];
        let start = self.value_offsets()[i];
        self.values
            .slice(start.to_usize().unwrap(), (end - start).to_usize().unwrap())
    }

    /// Returns the offset values in the offsets buffer
    #[inline]
    pub fn value_offsets(&self) -> &[OffsetSize] {
        // Soundness
        //     pointer alignment & location is ensured by RawPtrBox
        //     buffer bounds/offset is ensured by the ArrayData instance.
        unsafe {
            std::slice::from_raw_parts(
                self.value_offsets.as_ptr().add(self.data.offset()),
                self.len() + 1,
            )
        }
    }

    /// Returns the length for value at index `i`.
    #[inline]
    pub fn value_length(&self, i: usize) -> OffsetSize {
        let offsets = self.value_offsets();
        offsets[i + 1] - offsets[i]
    }

    /// constructs a new iterator
    pub fn iter<'a>(&'a self) -> GenericListArrayIter<'a, OffsetSize> {
        GenericListArrayIter::<'a, OffsetSize>::new(self)
    }

    #[inline]
    fn get_type(data_type: &DataType) -> Option<&DataType> {
        match (OffsetSize::IS_LARGE, data_type) {
            (true, DataType::LargeList(child)) | (false, DataType::List(child)) => {
                Some(child.data_type())
            }
            _ => None,
        }
    }

    /// Creates a [`GenericListArray`] from an iterator of primitive values
    /// # Example
    /// ```
    /// # use arrow::array::ListArray;
    /// # use arrow::datatypes::Int32Type;
    /// let data = vec![
    ///    Some(vec![Some(0), Some(1), Some(2)]),
    ///    None,
    ///    Some(vec![Some(3), None, Some(5)]),
    ///    Some(vec![Some(6), Some(7)]),
    /// ];
    /// let list_array = ListArray::from_iter_primitive::<Int32Type, _, _>(data);
    /// println!("{:?}", list_array);
    /// ```
    pub fn from_iter_primitive<T, P, I>(iter: I) -> Self
    where
        T: ArrowPrimitiveType,
        P: AsRef<[Option<<T as ArrowPrimitiveType>::Native>]>
            + IntoIterator<Item = Option<<T as ArrowPrimitiveType>::Native>>,
        I: IntoIterator<Item = Option<P>>,
    {
        let iterator = iter.into_iter();
        let (lower, _) = iterator.size_hint();

        let mut offsets =
            MutableBuffer::new((lower + 1) * std::mem::size_of::<OffsetSize>());
        let mut length_so_far = OffsetSize::zero();
        offsets.push(length_so_far);

        let mut null_buf = BooleanBufferBuilder::new(lower);

        let values: PrimitiveArray<T> = iterator
            .filter_map(|maybe_slice| {
                // regardless of whether the item is Some, the offsets and null buffers must be updated.
                match &maybe_slice {
                    Some(x) => {
                        length_so_far +=
                            OffsetSize::from_usize(x.as_ref().len()).unwrap();
                        null_buf.append(true);
                    }
                    None => null_buf.append(false),
                };
                offsets.push(length_so_far);
                maybe_slice
            })
            .flatten()
            .collect();

        let field = Box::new(Field::new("item", T::DATA_TYPE, true));
        let data_type = Self::DATA_TYPE_CONSTRUCTOR(field);
        let array_data = ArrayData::builder(data_type)
            .len(null_buf.len())
            .add_buffer(offsets.into())
            .add_child_data(values.into_data())
            .null_bit_buffer(Some(null_buf.into()));
        let array_data = unsafe { array_data.build_unchecked() };

        Self::from(array_data)
    }
}

impl<OffsetSize: OffsetSizeTrait> From<ArrayData> for GenericListArray<OffsetSize> {
    fn from(data: ArrayData) -> Self {
        Self::try_new_from_array_data(data).expect(
            "Expected infallable creation of GenericListArray from ArrayDataRef failed",
        )
    }
}

impl<OffsetSize: 'static + OffsetSizeTrait> From<GenericListArray<OffsetSize>>
    for ArrayData
{
    fn from(array: GenericListArray<OffsetSize>) -> Self {
        array.data
    }
}

impl<OffsetSize: OffsetSizeTrait> GenericListArray<OffsetSize> {
    fn try_new_from_array_data(data: ArrayData) -> Result<Self, ArrowError> {
        if data.buffers().len() != 1 {
            return Err(ArrowError::InvalidArgumentError(
                format!("ListArray data should contain a single buffer only (value offsets), had {}",
                        data.len())));
        }

        if data.child_data().len() != 1 {
            return Err(ArrowError::InvalidArgumentError(format!(
                "ListArray should contain a single child array (values array), had {}",
                data.child_data().len()
            )));
        }

        let values = data.child_data()[0].clone();

        if let Some(child_data_type) = Self::get_type(data.data_type()) {
            if values.data_type() != child_data_type {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "[Large]ListArray's child datatype {:?} does not \
                             correspond to the List's datatype {:?}",
                    values.data_type(),
                    child_data_type
                )));
            }
        } else {
            return Err(ArrowError::InvalidArgumentError(format!(
                "[Large]ListArray's datatype must be [Large]ListArray(). It is {:?}",
                data.data_type()
            )));
        }

        let values = make_array(values);
        let value_offsets = data.buffers()[0].as_ptr();
        let value_offsets = unsafe { RawPtrBox::<OffsetSize>::new(value_offsets) };
        Ok(Self {
            data,
            values,
            value_offsets,
        })
    }
}

impl<OffsetSize: OffsetSizeTrait> Array for GenericListArray<OffsetSize> {
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

impl<'a, OffsetSize: OffsetSizeTrait> ArrayAccessor for &'a GenericListArray<OffsetSize> {
    type Item = ArrayRef;

    fn value(&self, index: usize) -> Self::Item {
        GenericListArray::value(self, index)
    }

    unsafe fn value_unchecked(&self, index: usize) -> Self::Item {
        GenericListArray::value(self, index)
    }
}

impl<OffsetSize: OffsetSizeTrait> fmt::Debug for GenericListArray<OffsetSize> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let prefix = OffsetSize::PREFIX;

        write!(f, "{}ListArray\n[\n", prefix)?;
        print_long_array(self, f, |array, index, f| {
            fmt::Debug::fmt(&array.value(index), f)
        })?;
        write!(f, "]")
    }
}

/// A list array where each element is a variable-sized sequence of values with the same
/// type whose memory offsets between elements are represented by a i32.
///
/// # Example
///
/// ```
/// # use arrow::array::{Array, ListArray, Int32Array};
/// # use arrow::datatypes::{DataType, Int32Type};
/// let data = vec![
///    Some(vec![]),
///    None,
///    Some(vec![Some(3), None, Some(5), Some(19)]),
///    Some(vec![Some(6), Some(7)]),
/// ];
/// let list_array = ListArray::from_iter_primitive::<Int32Type, _, _>(data);
///
/// assert_eq!(false, list_array.is_valid(1));
///
/// let list0 = list_array.value(0);
/// let list2 = list_array.value(2);
/// let list3 = list_array.value(3);
///
/// assert_eq!(&[] as &[i32], list0.as_any().downcast_ref::<Int32Array>().unwrap().values());
/// assert_eq!(false, list2.as_any().downcast_ref::<Int32Array>().unwrap().is_valid(1));
/// assert_eq!(&[6, 7], list3.as_any().downcast_ref::<Int32Array>().unwrap().values());
/// ```
pub type ListArray = GenericListArray<i32>;

/// A list array where each element is a variable-sized sequence of values with the same
/// type whose memory offsets between elements are represented by a i64.
/// # Example
///
/// ```
/// # use arrow::array::{Array, LargeListArray, Int32Array};
/// # use arrow::datatypes::{DataType, Int32Type};
/// let data = vec![
///    Some(vec![]),
///    None,
///    Some(vec![Some(3), None, Some(5), Some(19)]),
///    Some(vec![Some(6), Some(7)]),
/// ];
/// let list_array = LargeListArray::from_iter_primitive::<Int32Type, _, _>(data);
///
/// assert_eq!(false, list_array.is_valid(1));
///
/// let list0 = list_array.value(0);
/// let list2 = list_array.value(2);
/// let list3 = list_array.value(3);
///
/// assert_eq!(&[] as &[i32], list0.as_any().downcast_ref::<Int32Array>().unwrap().values());
/// assert_eq!(false, list2.as_any().downcast_ref::<Int32Array>().unwrap().is_valid(1));
/// assert_eq!(&[6, 7], list3.as_any().downcast_ref::<Int32Array>().unwrap().values());
/// ```
pub type LargeListArray = GenericListArray<i64>;

#[cfg(test)]
mod tests {
    use crate::{
        alloc,
        array::ArrayData,
        array::Int32Array,
        buffer::Buffer,
        datatypes::Field,
        datatypes::{Int32Type, ToByteSlice},
        util::bit_util,
    };

    use super::*;

    fn create_from_buffers() -> ListArray {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from(&[0, 1, 2, 3, 4, 5, 6, 7].to_byte_slice()))
            .build()
            .unwrap();

        // Construct a buffer for value offsets, for the nested array:
        //  [[0, 1, 2], [3, 4, 5], [6, 7]]
        let value_offsets = Buffer::from(&[0, 3, 6, 8].to_byte_slice());

        // Construct a list array from the above two
        let list_data_type =
            DataType::List(Box::new(Field::new("item", DataType::Int32, true)));
        let list_data = ArrayData::builder(list_data_type)
            .len(3)
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .build()
            .unwrap();
        ListArray::from(list_data)
    }

    #[test]
    fn test_from_iter_primitive() {
        let data = vec![
            Some(vec![Some(0), Some(1), Some(2)]),
            Some(vec![Some(3), Some(4), Some(5)]),
            Some(vec![Some(6), Some(7)]),
        ];
        let list_array = ListArray::from_iter_primitive::<Int32Type, _, _>(data);

        let another = create_from_buffers();
        assert_eq!(list_array, another)
    }

    #[test]
    fn test_empty_list_array() {
        // Construct an empty value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(0)
            .add_buffer(Buffer::from([]))
            .build()
            .unwrap();

        // Construct an empty offset buffer
        let value_offsets = Buffer::from([]);

        // Construct a list array from the above two
        let list_data_type =
            DataType::List(Box::new(Field::new("item", DataType::Int32, false)));
        let list_data = ArrayData::builder(list_data_type)
            .len(0)
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .build()
            .unwrap();

        let list_array = ListArray::from(list_data);
        assert_eq!(list_array.len(), 0)
    }

    #[test]
    fn test_list_array() {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from_slice_ref(&[0, 1, 2, 3, 4, 5, 6, 7]))
            .build()
            .unwrap();

        // Construct a buffer for value offsets, for the nested array:
        //  [[0, 1, 2], [3, 4, 5], [6, 7]]
        let value_offsets = Buffer::from_slice_ref(&[0, 3, 6, 8]);

        // Construct a list array from the above two
        let list_data_type =
            DataType::List(Box::new(Field::new("item", DataType::Int32, false)));
        let list_data = ArrayData::builder(list_data_type.clone())
            .len(3)
            .add_buffer(value_offsets.clone())
            .add_child_data(value_data.clone())
            .build()
            .unwrap();
        let list_array = ListArray::from(list_data);

        let values = list_array.values();
        assert_eq!(&value_data, values.data());
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(3, list_array.len());
        assert_eq!(0, list_array.null_count());
        assert_eq!(6, list_array.value_offsets()[2]);
        assert_eq!(2, list_array.value_length(2));
        assert_eq!(
            0,
            list_array
                .value(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0)
        );
        assert_eq!(
            0,
            unsafe { list_array.value_unchecked(0) }
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0)
        );
        for i in 0..3 {
            assert!(list_array.is_valid(i));
            assert!(!list_array.is_null(i));
        }

        // Now test with a non-zero offset (skip first element)
        //  [[3, 4, 5], [6, 7]]
        let list_data = ArrayData::builder(list_data_type)
            .len(2)
            .offset(1)
            .add_buffer(value_offsets)
            .add_child_data(value_data.clone())
            .build()
            .unwrap();
        let list_array = ListArray::from(list_data);

        let values = list_array.values();
        assert_eq!(&value_data, values.data());
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(2, list_array.len());
        assert_eq!(0, list_array.null_count());
        assert_eq!(6, list_array.value_offsets()[1]);
        assert_eq!(2, list_array.value_length(1));
        assert_eq!(
            3,
            list_array
                .value(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0)
        );
        assert_eq!(
            3,
            unsafe { list_array.value_unchecked(0) }
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0)
        );
    }

    #[test]
    fn test_large_list_array() {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from_slice_ref(&[0, 1, 2, 3, 4, 5, 6, 7]))
            .build()
            .unwrap();

        // Construct a buffer for value offsets, for the nested array:
        //  [[0, 1, 2], [3, 4, 5], [6, 7]]
        let value_offsets = Buffer::from_slice_ref(&[0i64, 3, 6, 8]);

        // Construct a list array from the above two
        let list_data_type =
            DataType::LargeList(Box::new(Field::new("item", DataType::Int32, false)));
        let list_data = ArrayData::builder(list_data_type.clone())
            .len(3)
            .add_buffer(value_offsets.clone())
            .add_child_data(value_data.clone())
            .build()
            .unwrap();
        let list_array = LargeListArray::from(list_data);

        let values = list_array.values();
        assert_eq!(&value_data, values.data());
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(3, list_array.len());
        assert_eq!(0, list_array.null_count());
        assert_eq!(6, list_array.value_offsets()[2]);
        assert_eq!(2, list_array.value_length(2));
        assert_eq!(
            0,
            list_array
                .value(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0)
        );
        assert_eq!(
            0,
            unsafe { list_array.value_unchecked(0) }
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
        //  [[3, 4, 5], [6, 7]]
        let list_data = ArrayData::builder(list_data_type)
            .len(2)
            .offset(1)
            .add_buffer(value_offsets)
            .add_child_data(value_data.clone())
            .build()
            .unwrap();
        let list_array = LargeListArray::from(list_data);

        let values = list_array.values();
        assert_eq!(&value_data, values.data());
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(2, list_array.len());
        assert_eq!(0, list_array.null_count());
        assert_eq!(6, list_array.value_offsets()[1]);
        assert_eq!(2, list_array.value_length(1));
        assert_eq!(
            3,
            list_array
                .value(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0)
        );
        assert_eq!(
            3,
            unsafe { list_array.value_unchecked(0) }
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0)
        );
    }

    #[test]
    fn test_list_array_slice() {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(10)
            .add_buffer(Buffer::from_slice_ref(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]))
            .build()
            .unwrap();

        // Construct a buffer for value offsets, for the nested array:
        //  [[0, 1], null, null, [2, 3], [4, 5], null, [6, 7, 8], null, [9]]
        let value_offsets = Buffer::from_slice_ref(&[0, 2, 2, 2, 4, 6, 6, 9, 9, 10]);
        // 01011001 00000001
        let mut null_bits: [u8; 2] = [0; 2];
        bit_util::set_bit(&mut null_bits, 0);
        bit_util::set_bit(&mut null_bits, 3);
        bit_util::set_bit(&mut null_bits, 4);
        bit_util::set_bit(&mut null_bits, 6);
        bit_util::set_bit(&mut null_bits, 8);

        // Construct a list array from the above two
        let list_data_type =
            DataType::List(Box::new(Field::new("item", DataType::Int32, false)));
        let list_data = ArrayData::builder(list_data_type)
            .len(9)
            .add_buffer(value_offsets)
            .add_child_data(value_data.clone())
            .null_bit_buffer(Some(Buffer::from(null_bits)))
            .build()
            .unwrap();
        let list_array = ListArray::from(list_data);

        let values = list_array.values();
        assert_eq!(&value_data, values.data());
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(9, list_array.len());
        assert_eq!(4, list_array.null_count());
        assert_eq!(2, list_array.value_offsets()[3]);
        assert_eq!(2, list_array.value_length(3));

        let sliced_array = list_array.slice(1, 6);
        assert_eq!(6, sliced_array.len());
        assert_eq!(1, sliced_array.offset());
        assert_eq!(3, sliced_array.null_count());

        for i in 0..sliced_array.len() {
            if bit_util::get_bit(&null_bits, sliced_array.offset() + i) {
                assert!(sliced_array.is_valid(i));
            } else {
                assert!(sliced_array.is_null(i));
            }
        }

        // Check offset and length for each non-null value.
        let sliced_list_array =
            sliced_array.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(2, sliced_list_array.value_offsets()[2]);
        assert_eq!(2, sliced_list_array.value_length(2));
        assert_eq!(4, sliced_list_array.value_offsets()[3]);
        assert_eq!(2, sliced_list_array.value_length(3));
        assert_eq!(6, sliced_list_array.value_offsets()[5]);
        assert_eq!(3, sliced_list_array.value_length(5));
    }

    #[test]
    fn test_large_list_array_slice() {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(10)
            .add_buffer(Buffer::from_slice_ref(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]))
            .build()
            .unwrap();

        // Construct a buffer for value offsets, for the nested array:
        //  [[0, 1], null, null, [2, 3], [4, 5], null, [6, 7, 8], null, [9]]
        let value_offsets = Buffer::from_slice_ref(&[0i64, 2, 2, 2, 4, 6, 6, 9, 9, 10]);
        // 01011001 00000001
        let mut null_bits: [u8; 2] = [0; 2];
        bit_util::set_bit(&mut null_bits, 0);
        bit_util::set_bit(&mut null_bits, 3);
        bit_util::set_bit(&mut null_bits, 4);
        bit_util::set_bit(&mut null_bits, 6);
        bit_util::set_bit(&mut null_bits, 8);

        // Construct a list array from the above two
        let list_data_type =
            DataType::LargeList(Box::new(Field::new("item", DataType::Int32, false)));
        let list_data = ArrayData::builder(list_data_type)
            .len(9)
            .add_buffer(value_offsets)
            .add_child_data(value_data.clone())
            .null_bit_buffer(Some(Buffer::from(null_bits)))
            .build()
            .unwrap();
        let list_array = LargeListArray::from(list_data);

        let values = list_array.values();
        assert_eq!(&value_data, values.data());
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(9, list_array.len());
        assert_eq!(4, list_array.null_count());
        assert_eq!(2, list_array.value_offsets()[3]);
        assert_eq!(2, list_array.value_length(3));

        let sliced_array = list_array.slice(1, 6);
        assert_eq!(6, sliced_array.len());
        assert_eq!(1, sliced_array.offset());
        assert_eq!(3, sliced_array.null_count());

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
            .downcast_ref::<LargeListArray>()
            .unwrap();
        assert_eq!(2, sliced_list_array.value_offsets()[2]);
        assert_eq!(2, sliced_list_array.value_length(2));
        assert_eq!(4, sliced_list_array.value_offsets()[3]);
        assert_eq!(2, sliced_list_array.value_length(3));
        assert_eq!(6, sliced_list_array.value_offsets()[5]);
        assert_eq!(3, sliced_list_array.value_length(5));
    }

    #[test]
    #[should_panic(expected = "index out of bounds: the len is 10 but the index is 11")]
    fn test_list_array_index_out_of_bound() {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(10)
            .add_buffer(Buffer::from_slice_ref(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]))
            .build()
            .unwrap();

        // Construct a buffer for value offsets, for the nested array:
        //  [[0, 1], null, null, [2, 3], [4, 5], null, [6, 7, 8], null, [9]]
        let value_offsets = Buffer::from_slice_ref(&[0i64, 2, 2, 2, 4, 6, 6, 9, 9, 10]);
        // 01011001 00000001
        let mut null_bits: [u8; 2] = [0; 2];
        bit_util::set_bit(&mut null_bits, 0);
        bit_util::set_bit(&mut null_bits, 3);
        bit_util::set_bit(&mut null_bits, 4);
        bit_util::set_bit(&mut null_bits, 6);
        bit_util::set_bit(&mut null_bits, 8);

        // Construct a list array from the above two
        let list_data_type =
            DataType::LargeList(Box::new(Field::new("item", DataType::Int32, false)));
        let list_data = ArrayData::builder(list_data_type)
            .len(9)
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .null_bit_buffer(Some(Buffer::from(null_bits)))
            .build()
            .unwrap();
        let list_array = LargeListArray::from(list_data);
        assert_eq!(9, list_array.len());

        list_array.value(10);
    }
    #[test]
    #[should_panic(
        expected = "ListArray data should contain a single buffer only (value offsets)"
    )]
    // Different error messages, so skip for now
    // https://github.com/apache/arrow-rs/issues/1545
    #[cfg(not(feature = "force_validate"))]
    fn test_list_array_invalid_buffer_len() {
        let value_data = unsafe {
            ArrayData::builder(DataType::Int32)
                .len(8)
                .add_buffer(Buffer::from_slice_ref(&[0, 1, 2, 3, 4, 5, 6, 7]))
                .build_unchecked()
        };
        let list_data_type =
            DataType::List(Box::new(Field::new("item", DataType::Int32, false)));
        let list_data = unsafe {
            ArrayData::builder(list_data_type)
                .len(3)
                .add_child_data(value_data)
                .build_unchecked()
        };
        drop(ListArray::from(list_data));
    }

    #[test]
    #[should_panic(
        expected = "ListArray should contain a single child array (values array)"
    )]
    // Different error messages, so skip for now
    // https://github.com/apache/arrow-rs/issues/1545
    #[cfg(not(feature = "force_validate"))]
    fn test_list_array_invalid_child_array_len() {
        let value_offsets = Buffer::from_slice_ref(&[0, 2, 5, 7]);
        let list_data_type =
            DataType::List(Box::new(Field::new("item", DataType::Int32, false)));
        let list_data = unsafe {
            ArrayData::builder(list_data_type)
                .len(3)
                .add_buffer(value_offsets)
                .build_unchecked()
        };
        drop(ListArray::from(list_data));
    }

    #[test]
    fn test_list_array_offsets_need_not_start_at_zero() {
        let value_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from_slice_ref(&[0, 1, 2, 3, 4, 5, 6, 7]))
            .build()
            .unwrap();

        let value_offsets = Buffer::from_slice_ref(&[2, 2, 5, 7]);

        let list_data_type =
            DataType::List(Box::new(Field::new("item", DataType::Int32, false)));
        let list_data = ArrayData::builder(list_data_type)
            .len(3)
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .build()
            .unwrap();

        let list_array = ListArray::from(list_data);
        assert_eq!(list_array.value_length(0), 0);
        assert_eq!(list_array.value_length(1), 3);
        assert_eq!(list_array.value_length(2), 2);
    }

    #[test]
    #[should_panic(expected = "memory is not aligned")]
    fn test_primitive_array_alignment() {
        let ptr = alloc::allocate_aligned::<u8>(8);
        let buf = unsafe { Buffer::from_raw_parts(ptr, 8, 8) };
        let buf2 = buf.slice(1);
        let array_data = ArrayData::builder(DataType::Int32)
            .add_buffer(buf2)
            .build()
            .unwrap();
        drop(Int32Array::from(array_data));
    }

    #[test]
    #[should_panic(expected = "memory is not aligned")]
    // Different error messages, so skip for now
    // https://github.com/apache/arrow-rs/issues/1545
    #[cfg(not(feature = "force_validate"))]
    fn test_list_array_alignment() {
        let ptr = alloc::allocate_aligned::<u8>(8);
        let buf = unsafe { Buffer::from_raw_parts(ptr, 8, 8) };
        let buf2 = buf.slice(1);

        let values: [i32; 8] = [0; 8];
        let value_data = unsafe {
            ArrayData::builder(DataType::Int32)
                .add_buffer(Buffer::from_slice_ref(&values))
                .build_unchecked()
        };

        let list_data_type =
            DataType::List(Box::new(Field::new("item", DataType::Int32, false)));
        let list_data = unsafe {
            ArrayData::builder(list_data_type)
                .add_buffer(buf2)
                .add_child_data(value_data)
                .build_unchecked()
        };
        drop(ListArray::from(list_data));
    }

    #[test]
    fn list_array_equality() {
        // test scaffold
        fn do_comparison(
            lhs_data: Vec<Option<Vec<Option<i32>>>>,
            rhs_data: Vec<Option<Vec<Option<i32>>>>,
            should_equal: bool,
        ) {
            let lhs = ListArray::from_iter_primitive::<Int32Type, _, _>(lhs_data.clone());
            let rhs = ListArray::from_iter_primitive::<Int32Type, _, _>(rhs_data.clone());
            assert_eq!(lhs == rhs, should_equal);

            let lhs = LargeListArray::from_iter_primitive::<Int32Type, _, _>(lhs_data);
            let rhs = LargeListArray::from_iter_primitive::<Int32Type, _, _>(rhs_data);
            assert_eq!(lhs == rhs, should_equal);
        }

        do_comparison(
            vec![
                Some(vec![Some(0), Some(1), Some(2)]),
                None,
                Some(vec![Some(3), None, Some(5)]),
                Some(vec![Some(6), Some(7)]),
            ],
            vec![
                Some(vec![Some(0), Some(1), Some(2)]),
                None,
                Some(vec![Some(3), None, Some(5)]),
                Some(vec![Some(6), Some(7)]),
            ],
            true,
        );

        do_comparison(
            vec![
                None,
                None,
                Some(vec![Some(3), None, Some(5)]),
                Some(vec![Some(6), Some(7)]),
            ],
            vec![
                Some(vec![Some(0), Some(1), Some(2)]),
                None,
                Some(vec![Some(3), None, Some(5)]),
                Some(vec![Some(6), Some(7)]),
            ],
            false,
        );

        do_comparison(
            vec![
                None,
                None,
                Some(vec![Some(3), None, Some(5)]),
                Some(vec![Some(6), Some(7)]),
            ],
            vec![
                None,
                None,
                Some(vec![Some(3), None, Some(5)]),
                Some(vec![Some(0), Some(0)]),
            ],
            false,
        );

        do_comparison(
            vec![None, None, Some(vec![Some(1)])],
            vec![None, None, Some(vec![Some(2)])],
            false,
        );
    }
}
