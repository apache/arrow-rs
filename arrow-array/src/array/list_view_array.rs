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

use arrow_buffer::{NullBuffer, ScalarBuffer};
use arrow_data::{ArrayData, ArrayDataBuilder};
use arrow_schema::{ArrowError, DataType, FieldRef};
use std::any::Any;
use std::ops::Add;
use std::sync::Arc;

use crate::array::{make_array, print_long_array};
use crate::iterator::GenericListViewArrayIter;
use crate::{new_empty_array, Array, ArrayAccessor, ArrayRef, FixedSizeListArray, OffsetSizeTrait};

/// A [`GenericListViewArray`] of variable size lists, storing offsets as `i32`.
pub type ListViewArray = GenericListViewArray<i32>;

/// A [`GenericListViewArray`] of variable size lists, storing offsets as `i64`.
pub type LargeListViewArray = GenericListViewArray<i64>;

///
/// Different from [`crate::GenericListArray`] as it stores both an offset and length
/// meaning that take / filter operations can be implemented without copying the underlying data.
///
/// [Variable-size List Layout: ListView Layout]: https://arrow.apache.org/docs/format/Columnar.html#listview-layout
#[derive(Clone)]
pub struct GenericListViewArray<OffsetSize: OffsetSizeTrait> {
    data_type: DataType,
    nulls: Option<NullBuffer>,
    values: ArrayRef,
    value_offsets: ScalarBuffer<OffsetSize>,
    value_sizes: ScalarBuffer<OffsetSize>,
}

impl<OffsetSize: OffsetSizeTrait> GenericListViewArray<OffsetSize> {
    /// The data type constructor of listview array.
    /// The input is the schema of the child array and
    /// the output is the [`DataType`], ListView or LargeListView.
    pub const DATA_TYPE_CONSTRUCTOR: fn(FieldRef) -> DataType = if OffsetSize::IS_LARGE {
        DataType::LargeListView
    } else {
        DataType::ListView
    };

    /// Create a new [`GenericListViewArray`] from the provided parts
    ///
    /// # Errors
    ///
    /// Errors if
    ///
    /// * `offsets.len() != sizes.len()`
    /// * `offsets.len() != nulls.len()`
    /// * `offsets[i] > values.len()`
    /// * `!field.is_nullable() && values.is_nullable()`
    /// * `field.data_type() != values.data_type()`
    /// * `0 <= offsets[i] <= length of the child array`
    /// * `0 <= offsets[i] + size[i] <= length of the child array`
    pub fn try_new(
        field: FieldRef,
        offsets: ScalarBuffer<OffsetSize>,
        sizes: ScalarBuffer<OffsetSize>,
        values: ArrayRef,
        nulls: Option<NullBuffer>,
    ) -> Result<Self, ArrowError> {
        let len = offsets.len();
        if let Some(n) = nulls.as_ref() {
            if n.len() != len {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Incorrect length of null buffer for {}ListViewArray, expected {len} got {}",
                    OffsetSize::PREFIX,
                    n.len(),
                )));
            }
        }
        if len != sizes.len() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Length of offsets buffer and sizes buffer must be equal for {}ListViewArray, got {len} and {}",
                OffsetSize::PREFIX, sizes.len()
            )));
        }

        for (offset, size) in offsets.iter().zip(sizes.iter()) {
            let offset = offset.as_usize();
            let size = size.as_usize();
            if offset.checked_add(size).ok_or_else(|| {
                ArrowError::InvalidArgumentError(format!(
                    "Overflow in offset + size for {}ListViewArray",
                    OffsetSize::PREFIX
                ))
            })? > values.len()
            {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Offset + size for {}ListViewArray must be within the bounds of the child array, got offset: {offset}, size: {size}, child array length: {}",
                    OffsetSize::PREFIX,
                    values.len()
                )));
            }
        }

        if !field.is_nullable() && values.is_nullable() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Non-nullable field of {}ListViewArray {:?} cannot contain nulls",
                OffsetSize::PREFIX,
                field.name()
            )));
        }

        if field.data_type() != values.data_type() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "{}ListViewArray expected data type {} got {} for {:?}",
                OffsetSize::PREFIX,
                field.data_type(),
                values.data_type(),
                field.name()
            )));
        }

        Ok(Self {
            data_type: Self::DATA_TYPE_CONSTRUCTOR(field),
            nulls,
            values,
            value_offsets: offsets,
            value_sizes: sizes,
        })
    }

    /// Create a new [`GenericListViewArray`] from the provided parts
    ///
    /// # Panics
    ///
    /// Panics if [`Self::try_new`] returns an error
    pub fn new(
        field: FieldRef,
        offsets: ScalarBuffer<OffsetSize>,
        sizes: ScalarBuffer<OffsetSize>,
        values: ArrayRef,
        nulls: Option<NullBuffer>,
    ) -> Self {
        Self::try_new(field, offsets, sizes, values, nulls).unwrap()
    }

    /// Create a new [`GenericListViewArray`] of length `len` where all values are null
    pub fn new_null(field: FieldRef, len: usize) -> Self {
        let values = new_empty_array(field.data_type());
        Self {
            data_type: Self::DATA_TYPE_CONSTRUCTOR(field),
            nulls: Some(NullBuffer::new_null(len)),
            value_offsets: ScalarBuffer::from(vec![]),
            value_sizes: ScalarBuffer::from(vec![]),
            values,
        }
    }

    /// Deconstruct this array into its constituent parts
    pub fn into_parts(
        self,
    ) -> (
        FieldRef,
        ScalarBuffer<OffsetSize>,
        ScalarBuffer<OffsetSize>,
        ArrayRef,
        Option<NullBuffer>,
    ) {
        let f = match self.data_type {
            DataType::ListView(f) | DataType::LargeListView(f) => f,
            _ => unreachable!(),
        };
        (
            f,
            self.value_offsets,
            self.value_sizes,
            self.values,
            self.nulls,
        )
    }

    /// Returns a reference to the offsets of this list
    ///
    /// Unlike [`Self::value_offsets`] this returns the [`ScalarBuffer`]
    /// allowing for zero-copy cloning
    #[inline]
    pub fn offsets(&self) -> &ScalarBuffer<OffsetSize> {
        &self.value_offsets
    }

    /// Returns a reference to the values of this list
    #[inline]
    pub fn values(&self) -> &ArrayRef {
        &self.values
    }

    /// Returns a reference to the sizes of this list
    ///
    /// Unlike [`Self::value_sizes`] this returns the [`ScalarBuffer`]
    /// allowing for zero-copy cloning
    #[inline]
    pub fn sizes(&self) -> &ScalarBuffer<OffsetSize> {
        &self.value_sizes
    }

    /// Returns a clone of the value type of this list.
    pub fn value_type(&self) -> DataType {
        self.values.data_type().clone()
    }

    /// Returns ith value of this list view array.
    /// # Safety
    /// Caller must ensure that the index is within the array bounds
    pub unsafe fn value_unchecked(&self, i: usize) -> ArrayRef {
        let offset = self.value_offsets().get_unchecked(i).as_usize();
        let length = self.value_sizes().get_unchecked(i).as_usize();
        self.values.slice(offset, length)
    }

    /// Returns ith value of this list view array.
    /// # Panics
    /// Panics if the index is out of bounds
    pub fn value(&self, i: usize) -> ArrayRef {
        let offset = self.value_offsets()[i].as_usize();
        let length = self.value_sizes()[i].as_usize();
        self.values.slice(offset, length)
    }

    /// Returns the offset values in the offsets buffer
    #[inline]
    pub fn value_offsets(&self) -> &[OffsetSize] {
        &self.value_offsets
    }

    /// Returns the sizes values in the offsets buffer
    #[inline]
    pub fn value_sizes(&self) -> &[OffsetSize] {
        &self.value_sizes
    }

    /// Returns the size for value at index `i`.
    #[inline]
    pub fn value_size(&self, i: usize) -> OffsetSize {
        self.value_sizes[i]
    }

    /// Returns the offset for value at index `i`.
    pub fn value_offset(&self, i: usize) -> OffsetSize {
        self.value_offsets[i]
    }

    /// Constructs a new iterator
    pub fn iter(&self) -> GenericListViewArrayIter<'_, OffsetSize> {
        GenericListViewArrayIter::<'_, OffsetSize>::new(self)
    }

    #[inline]
    fn get_type(data_type: &DataType) -> Option<&DataType> {
        match (OffsetSize::IS_LARGE, data_type) {
            (true, DataType::LargeListView(child)) | (false, DataType::ListView(child)) => {
                Some(child.data_type())
            }
            _ => None,
        }
    }

    /// Returns a zero-copy slice of this array with the indicated offset and length.
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        Self {
            data_type: self.data_type.clone(),
            nulls: self.nulls.as_ref().map(|n| n.slice(offset, length)),
            values: self.values.clone(),
            value_offsets: self.value_offsets.slice(offset, length),
            value_sizes: self.value_sizes.slice(offset, length),
        }
    }
}

impl<OffsetSize: OffsetSizeTrait> ArrayAccessor for &GenericListViewArray<OffsetSize> {
    type Item = ArrayRef;

    fn value(&self, index: usize) -> Self::Item {
        GenericListViewArray::value(self, index)
    }

    unsafe fn value_unchecked(&self, index: usize) -> Self::Item {
        GenericListViewArray::value_unchecked(self, index)
    }
}

impl<OffsetSize: OffsetSizeTrait> Array for GenericListViewArray<OffsetSize> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn to_data(&self) -> ArrayData {
        self.clone().into()
    }

    fn into_data(self) -> ArrayData {
        self.into()
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        Arc::new(self.slice(offset, length))
    }

    fn len(&self) -> usize {
        self.sizes().len()
    }

    fn is_empty(&self) -> bool {
        self.value_sizes.is_empty()
    }

    fn offset(&self) -> usize {
        0
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        self.nulls.as_ref()
    }

    fn get_buffer_memory_size(&self) -> usize {
        let mut size = self.values.get_buffer_memory_size();
        size += self.value_offsets.inner().capacity();
        size += self.value_sizes.inner().capacity();
        if let Some(n) = self.nulls.as_ref() {
            size += n.buffer().capacity();
        }
        size
    }

    fn get_array_memory_size(&self) -> usize {
        let mut size = std::mem::size_of::<Self>() + self.values.get_array_memory_size();
        size += self.value_offsets.inner().capacity();
        size += self.value_sizes.inner().capacity();
        if let Some(n) = self.nulls.as_ref() {
            size += n.buffer().capacity();
        }
        size
    }
}

impl<OffsetSize: OffsetSizeTrait> std::fmt::Debug for GenericListViewArray<OffsetSize> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let prefix = OffsetSize::PREFIX;
        write!(f, "{prefix}ListViewArray\n[\n")?;
        print_long_array(self, f, |array, index, f| {
            std::fmt::Debug::fmt(&array.value(index), f)
        })?;
        write!(f, "]")
    }
}

impl<OffsetSize: OffsetSizeTrait> From<GenericListViewArray<OffsetSize>> for ArrayData {
    fn from(array: GenericListViewArray<OffsetSize>) -> Self {
        let len = array.len();
        let builder = ArrayDataBuilder::new(array.data_type)
            .len(len)
            .nulls(array.nulls)
            .buffers(vec![
                array.value_offsets.into_inner(),
                array.value_sizes.into_inner(),
            ])
            .child_data(vec![array.values.to_data()]);

        unsafe { builder.build_unchecked() }
    }
}

impl<OffsetSize: OffsetSizeTrait> From<ArrayData> for GenericListViewArray<OffsetSize> {
    fn from(data: ArrayData) -> Self {
        Self::try_new_from_array_data(data)
            .expect("Expected infallible creation of GenericListViewArray from ArrayDataRef failed")
    }
}

impl<OffsetSize: OffsetSizeTrait> From<FixedSizeListArray> for GenericListViewArray<OffsetSize> {
    fn from(value: FixedSizeListArray) -> Self {
        let (field, size) = match value.data_type() {
            DataType::FixedSizeList(f, size) => (f, *size as usize),
            _ => unreachable!(),
        };
        let mut acc = 0_usize;
        let iter = std::iter::repeat(size).take(value.len());
        let mut sizes = Vec::with_capacity(iter.size_hint().0);
        let mut offsets = Vec::with_capacity(iter.size_hint().0);

        for size in iter {
            offsets.push(OffsetSize::usize_as(acc));
            acc = acc.add(size);
            sizes.push(OffsetSize::usize_as(size));
        }
        let sizes = ScalarBuffer::from(sizes);
        let offsets = ScalarBuffer::from(offsets);
        Self {
            data_type: Self::DATA_TYPE_CONSTRUCTOR(field.clone()),
            nulls: value.nulls().cloned(),
            values: value.values().clone(),
            value_offsets: offsets,
            value_sizes: sizes,
        }
    }
}

impl<OffsetSize: OffsetSizeTrait> GenericListViewArray<OffsetSize> {
    fn try_new_from_array_data(data: ArrayData) -> Result<Self, ArrowError> {
        if data.buffers().len() != 2 {
            return Err(ArrowError::InvalidArgumentError(format!(
                "ListViewArray data should contain two buffers (value offsets & value sizes), had {}",
                data.buffers().len()
            )));
        }

        if data.child_data().len() != 1 {
            return Err(ArrowError::InvalidArgumentError(format!(
                "ListViewArray should contain a single child array (values array), had {}",
                data.child_data().len()
            )));
        }

        let values = data.child_data()[0].clone();

        if let Some(child_data_type) = Self::get_type(data.data_type()) {
            if values.data_type() != child_data_type {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "{}ListViewArray's child datatype {:?} does not \
                             correspond to the List's datatype {:?}",
                    OffsetSize::PREFIX,
                    values.data_type(),
                    child_data_type
                )));
            }
        } else {
            return Err(ArrowError::InvalidArgumentError(format!(
                "{}ListViewArray's datatype must be {}ListViewArray(). It is {:?}",
                OffsetSize::PREFIX,
                OffsetSize::PREFIX,
                data.data_type()
            )));
        }

        let values = make_array(values);
        // ArrayData is valid, and verified type above
        let value_offsets = ScalarBuffer::new(data.buffers()[0].clone(), data.offset(), data.len());
        let value_sizes = ScalarBuffer::new(data.buffers()[1].clone(), data.offset(), data.len());

        Ok(Self {
            data_type: data.data_type().clone(),
            nulls: data.nulls().cloned(),
            values,
            value_offsets,
            value_sizes,
        })
    }
}

#[cfg(test)]
mod tests {
    use arrow_buffer::{bit_util, BooleanBuffer, Buffer, ScalarBuffer};
    use arrow_schema::Field;

    use crate::builder::{FixedSizeListBuilder, Int32Builder};
    use crate::cast::AsArray;
    use crate::types::Int32Type;
    use crate::{Int32Array, Int64Array};

    use super::*;

    #[test]
    fn test_empty_list_view_array() {
        // Construct an empty value array
        let vec: Vec<i32> = vec![];
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        let sizes = ScalarBuffer::from(vec![]);
        let offsets = ScalarBuffer::from(vec![]);
        let values = Int32Array::from(vec);
        let list_array = LargeListViewArray::new(field, offsets, sizes, Arc::new(values), None);

        assert_eq!(list_array.len(), 0)
    }

    #[test]
    fn test_list_view_array() {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from_slice_ref([0, 1, 2, 3, 4, 5, 6, 7]))
            .build()
            .unwrap();

        let field = Arc::new(Field::new("item", DataType::Int32, true));
        let sizes = ScalarBuffer::from(vec![3i32, 3, 2]);
        let offsets = ScalarBuffer::from(vec![0i32, 3, 6]);
        let values = Int32Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7]);
        let list_array = ListViewArray::new(field, offsets, sizes, Arc::new(values), None);

        let values = list_array.values();
        assert_eq!(value_data, values.to_data());
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(3, list_array.len());
        assert_eq!(0, list_array.null_count());
        assert_eq!(6, list_array.value_offsets()[2]);
        assert_eq!(2, list_array.value_sizes()[2]);
        assert_eq!(2, list_array.value_size(2));
        assert_eq!(0, list_array.value(0).as_primitive::<Int32Type>().value(0));
        assert_eq!(
            0,
            unsafe { list_array.value_unchecked(0) }
                .as_primitive::<Int32Type>()
                .value(0)
        );
        for i in 0..3 {
            assert!(list_array.is_valid(i));
            assert!(!list_array.is_null(i));
        }
    }

    #[test]
    fn test_large_list_view_array() {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from_slice_ref([0, 1, 2, 3, 4, 5, 6, 7]))
            .build()
            .unwrap();

        let field = Arc::new(Field::new("item", DataType::Int32, true));
        let sizes = ScalarBuffer::from(vec![3i64, 3, 2]);
        let offsets = ScalarBuffer::from(vec![0i64, 3, 6]);
        let values = Int32Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7]);
        let list_array = LargeListViewArray::new(field, offsets, sizes, Arc::new(values), None);

        let values = list_array.values();
        assert_eq!(value_data, values.to_data());
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(3, list_array.len());
        assert_eq!(0, list_array.null_count());
        assert_eq!(6, list_array.value_offsets()[2]);
        assert_eq!(2, list_array.value_sizes()[2]);
        assert_eq!(2, list_array.value_size(2));
        assert_eq!(0, list_array.value(0).as_primitive::<Int32Type>().value(0));
        assert_eq!(
            0,
            unsafe { list_array.value_unchecked(0) }
                .as_primitive::<Int32Type>()
                .value(0)
        );
        for i in 0..3 {
            assert!(list_array.is_valid(i));
            assert!(!list_array.is_null(i));
        }
    }

    #[test]
    fn test_list_view_array_slice() {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(10)
            .add_buffer(Buffer::from_slice_ref([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]))
            .build()
            .unwrap();

        // 01011001 00000001
        let mut null_bits: [u8; 2] = [0; 2];
        bit_util::set_bit(&mut null_bits, 0);
        bit_util::set_bit(&mut null_bits, 3);
        bit_util::set_bit(&mut null_bits, 4);
        bit_util::set_bit(&mut null_bits, 6);
        bit_util::set_bit(&mut null_bits, 8);
        let buffer = BooleanBuffer::new(Buffer::from(null_bits), 0, 9);
        let null_buffer = NullBuffer::new(buffer);

        let field = Arc::new(Field::new("item", DataType::Int32, true));
        let sizes = ScalarBuffer::from(vec![2, 0, 0, 2, 2, 0, 3, 0, 1]);
        let offsets = ScalarBuffer::from(vec![0, 2, 2, 2, 4, 6, 6, 9, 9]);
        let values = Int32Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let list_array =
            ListViewArray::new(field, offsets, sizes, Arc::new(values), Some(null_buffer));

        let values = list_array.values();
        assert_eq!(value_data, values.to_data());
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(9, list_array.len());
        assert_eq!(4, list_array.null_count());
        assert_eq!(2, list_array.value_offsets()[3]);
        assert_eq!(2, list_array.value_sizes()[3]);
        assert_eq!(2, list_array.value_size(3));

        let sliced_array = list_array.slice(1, 6);
        assert_eq!(6, sliced_array.len());
        assert_eq!(3, sliced_array.null_count());

        for i in 0..sliced_array.len() {
            if bit_util::get_bit(&null_bits, 1 + i) {
                assert!(sliced_array.is_valid(i));
            } else {
                assert!(sliced_array.is_null(i));
            }
        }

        // Check offset and length for each non-null value.
        let sliced_list_array = sliced_array
            .as_any()
            .downcast_ref::<ListViewArray>()
            .unwrap();
        assert_eq!(2, sliced_list_array.value_offsets()[2]);
        assert_eq!(2, sliced_list_array.value_sizes()[2]);
        assert_eq!(2, sliced_list_array.value_size(2));

        assert_eq!(4, sliced_list_array.value_offsets()[3]);
        assert_eq!(2, sliced_list_array.value_sizes()[3]);
        assert_eq!(2, sliced_list_array.value_size(3));

        assert_eq!(6, sliced_list_array.value_offsets()[5]);
        assert_eq!(3, sliced_list_array.value_sizes()[5]);
        assert_eq!(3, sliced_list_array.value_size(5));
    }

    #[test]
    fn test_large_list_view_array_slice() {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(10)
            .add_buffer(Buffer::from_slice_ref([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]))
            .build()
            .unwrap();

        // 01011001 00000001
        let mut null_bits: [u8; 2] = [0; 2];
        bit_util::set_bit(&mut null_bits, 0);
        bit_util::set_bit(&mut null_bits, 3);
        bit_util::set_bit(&mut null_bits, 4);
        bit_util::set_bit(&mut null_bits, 6);
        bit_util::set_bit(&mut null_bits, 8);
        let buffer = BooleanBuffer::new(Buffer::from(null_bits), 0, 9);
        let null_buffer = NullBuffer::new(buffer);

        // Construct a large list view array from the above two
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        let sizes = ScalarBuffer::from(vec![2i64, 0, 0, 2, 2, 0, 3, 0, 1]);
        let offsets = ScalarBuffer::from(vec![0i64, 2, 2, 2, 4, 6, 6, 9, 9]);
        let values = Int32Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let list_array =
            LargeListViewArray::new(field, offsets, sizes, Arc::new(values), Some(null_buffer));

        let values = list_array.values();
        assert_eq!(value_data, values.to_data());
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(9, list_array.len());
        assert_eq!(4, list_array.null_count());
        assert_eq!(2, list_array.value_offsets()[3]);
        assert_eq!(2, list_array.value_sizes()[3]);
        assert_eq!(2, list_array.value_size(3));

        let sliced_array = list_array.slice(1, 6);
        assert_eq!(6, sliced_array.len());
        assert_eq!(3, sliced_array.null_count());

        for i in 0..sliced_array.len() {
            if bit_util::get_bit(&null_bits, 1 + i) {
                assert!(sliced_array.is_valid(i));
            } else {
                assert!(sliced_array.is_null(i));
            }
        }

        // Check offset and length for each non-null value.
        let sliced_list_array = sliced_array
            .as_any()
            .downcast_ref::<LargeListViewArray>()
            .unwrap();
        assert_eq!(2, sliced_list_array.value_offsets()[2]);
        assert_eq!(2, sliced_list_array.value_size(2));
        assert_eq!(2, sliced_list_array.value_sizes()[2]);

        assert_eq!(4, sliced_list_array.value_offsets()[3]);
        assert_eq!(2, sliced_list_array.value_size(3));
        assert_eq!(2, sliced_list_array.value_sizes()[3]);

        assert_eq!(6, sliced_list_array.value_offsets()[5]);
        assert_eq!(3, sliced_list_array.value_size(5));
        assert_eq!(2, sliced_list_array.value_sizes()[3]);
    }

    #[test]
    #[should_panic(expected = "index out of bounds: the len is 9 but the index is 10")]
    fn test_list_view_array_index_out_of_bound() {
        // 01011001 00000001
        let mut null_bits: [u8; 2] = [0; 2];
        bit_util::set_bit(&mut null_bits, 0);
        bit_util::set_bit(&mut null_bits, 3);
        bit_util::set_bit(&mut null_bits, 4);
        bit_util::set_bit(&mut null_bits, 6);
        bit_util::set_bit(&mut null_bits, 8);
        let buffer = BooleanBuffer::new(Buffer::from(null_bits), 0, 9);
        let null_buffer = NullBuffer::new(buffer);

        // Construct a buffer for value offsets, for the nested array:
        //  [[0, 1], null, null, [2, 3], [4, 5], null, [6, 7, 8], null, [9]]
        // Construct a list array from the above two
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        let sizes = ScalarBuffer::from(vec![2i32, 0, 0, 2, 2, 0, 3, 0, 1]);
        let offsets = ScalarBuffer::from(vec![0i32, 2, 2, 2, 4, 6, 6, 9, 9]);
        let values = Int32Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let list_array =
            ListViewArray::new(field, offsets, sizes, Arc::new(values), Some(null_buffer));

        assert_eq!(9, list_array.len());
        list_array.value(10);
    }
    #[test]
    #[should_panic(
        expected = "ListViewArray data should contain two buffers (value offsets & value sizes), had 0"
    )]
    #[cfg(not(feature = "force_validate"))]
    fn test_list_view_array_invalid_buffer_len() {
        let value_data = unsafe {
            ArrayData::builder(DataType::Int32)
                .len(8)
                .add_buffer(Buffer::from_slice_ref([0, 1, 2, 3, 4, 5, 6, 7]))
                .build_unchecked()
        };
        let list_data_type =
            DataType::ListView(Arc::new(Field::new("item", DataType::Int32, false)));
        let list_data = unsafe {
            ArrayData::builder(list_data_type)
                .len(3)
                .add_child_data(value_data)
                .build_unchecked()
        };
        drop(ListViewArray::from(list_data));
    }

    #[test]
    #[should_panic(
        expected = "ListViewArray data should contain two buffers (value offsets & value sizes), had 1"
    )]
    #[cfg(not(feature = "force_validate"))]
    fn test_list_view_array_invalid_child_array_len() {
        let value_offsets = Buffer::from_slice_ref([0, 2, 5, 7]);
        let list_data_type =
            DataType::ListView(Arc::new(Field::new("item", DataType::Int32, false)));
        let list_data = unsafe {
            ArrayData::builder(list_data_type)
                .len(3)
                .add_buffer(value_offsets)
                .build_unchecked()
        };
        drop(ListViewArray::from(list_data));
    }

    #[test]
    fn test_list_view_array_offsets_need_not_start_at_zero() {
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        let sizes = ScalarBuffer::from(vec![0i32, 0, 3]);
        let offsets = ScalarBuffer::from(vec![2i32, 2, 5]);
        let values = Int32Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7]);
        let list_array = ListViewArray::new(field, offsets, sizes, Arc::new(values), None);

        assert_eq!(list_array.value_size(0), 0);
        assert_eq!(list_array.value_size(1), 0);
        assert_eq!(list_array.value_size(2), 3);
    }

    #[test]
    #[should_panic(expected = "Memory pointer is not aligned with the specified scalar type")]
    #[cfg(not(feature = "force_validate"))]
    fn test_list_view_array_alignment() {
        let offset_buf = Buffer::from_slice_ref([0_u64]);
        let offset_buf2 = offset_buf.slice(1);

        let size_buf = Buffer::from_slice_ref([0_u64]);
        let size_buf2 = size_buf.slice(1);

        let values: [i32; 8] = [0; 8];
        let value_data = unsafe {
            ArrayData::builder(DataType::Int32)
                .add_buffer(Buffer::from_slice_ref(values))
                .build_unchecked()
        };

        let list_data_type =
            DataType::ListView(Arc::new(Field::new("item", DataType::Int32, false)));
        let list_data = unsafe {
            ArrayData::builder(list_data_type)
                .add_buffer(offset_buf2)
                .add_buffer(size_buf2)
                .add_child_data(value_data)
                .build_unchecked()
        };
        drop(ListViewArray::from(list_data));
    }

    #[test]
    fn test_empty_offsets() {
        let f = Arc::new(Field::new("element", DataType::Int32, true));
        let string = ListViewArray::from(
            ArrayData::builder(DataType::ListView(f.clone()))
                .buffers(vec![Buffer::from(&[]), Buffer::from(&[])])
                .add_child_data(ArrayData::new_empty(&DataType::Int32))
                .build()
                .unwrap(),
        );
        assert_eq!(string.value_offsets(), &[]);
        assert_eq!(string.value_sizes(), &[]);

        let string = LargeListViewArray::from(
            ArrayData::builder(DataType::LargeListView(f))
                .buffers(vec![Buffer::from(&[]), Buffer::from(&[])])
                .add_child_data(ArrayData::new_empty(&DataType::Int32))
                .build()
                .unwrap(),
        );
        assert_eq!(string.len(), 0);
        assert_eq!(string.value_offsets(), &[]);
        assert_eq!(string.value_sizes(), &[]);
    }

    #[test]
    fn test_try_new() {
        let offsets = ScalarBuffer::from(vec![0, 1, 4, 5]);
        let sizes = ScalarBuffer::from(vec![1, 3, 1, 0]);
        let values = Int32Array::new(vec![1, 2, 3, 4, 5].into(), None);
        let values = Arc::new(values) as ArrayRef;

        let field = Arc::new(Field::new("element", DataType::Int32, false));
        ListViewArray::new(
            field.clone(),
            offsets.clone(),
            sizes.clone(),
            values.clone(),
            None,
        );

        let nulls = NullBuffer::new_null(4);
        ListViewArray::new(
            field.clone(),
            offsets,
            sizes.clone(),
            values.clone(),
            Some(nulls),
        );

        let nulls = NullBuffer::new_null(4);
        let offsets = ScalarBuffer::from(vec![0, 1, 2, 3, 4]);
        let sizes = ScalarBuffer::from(vec![1, 1, 1, 1, 0]);
        let err = LargeListViewArray::try_new(
            field,
            offsets.clone(),
            sizes.clone(),
            values.clone(),
            Some(nulls),
        )
        .unwrap_err();

        assert_eq!(
            err.to_string(),
            "Invalid argument error: Incorrect length of null buffer for LargeListViewArray, expected 5 got 4"
        );

        let field = Arc::new(Field::new("element", DataType::Int64, false));
        let err = LargeListViewArray::try_new(
            field.clone(),
            offsets.clone(),
            sizes.clone(),
            values.clone(),
            None,
        )
        .unwrap_err();

        assert_eq!(
            err.to_string(),
            "Invalid argument error: LargeListViewArray expected data type Int64 got Int32 for \"element\""
        );

        let nulls = NullBuffer::new_null(7);
        let values = Int64Array::new(vec![0; 7].into(), Some(nulls));
        let values = Arc::new(values);

        let err = LargeListViewArray::try_new(
            field,
            offsets.clone(),
            sizes.clone(),
            values.clone(),
            None,
        )
        .unwrap_err();

        assert_eq!(
            err.to_string(),
            "Invalid argument error: Non-nullable field of LargeListViewArray \"element\" cannot contain nulls"
        );
    }

    #[test]
    fn test_from_fixed_size_list() {
        let mut builder = FixedSizeListBuilder::new(Int32Builder::new(), 3);
        builder.values().append_slice(&[1, 2, 3]);
        builder.append(true);
        builder.values().append_slice(&[0, 0, 0]);
        builder.append(false);
        builder.values().append_slice(&[4, 5, 6]);
        builder.append(true);
        let list: ListViewArray = builder.finish().into();
        let values: Vec<_> = list
            .iter()
            .map(|x| x.map(|x| x.as_primitive::<Int32Type>().values().to_vec()))
            .collect();
        assert_eq!(values, vec![Some(vec![1, 2, 3]), None, Some(vec![4, 5, 6])]);
        let offsets = list.value_offsets();
        assert_eq!(offsets, &[0, 3, 6]);
        let sizes = list.value_sizes();
        assert_eq!(sizes, &[3, 3, 3]);
    }

    #[test]
    fn test_list_view_array_overlap_lists() {
        let value_data = unsafe {
            ArrayData::builder(DataType::Int32)
                .len(8)
                .add_buffer(Buffer::from_slice_ref([0, 1, 2, 3, 4, 5, 6, 7]))
                .build_unchecked()
        };
        let list_data_type =
            DataType::ListView(Arc::new(Field::new("item", DataType::Int32, false)));
        let list_data = unsafe {
            ArrayData::builder(list_data_type)
                .len(2)
                .add_buffer(Buffer::from_slice_ref([0, 3])) // offsets
                .add_buffer(Buffer::from_slice_ref([5, 5])) // sizes
                .add_child_data(value_data)
                .build_unchecked()
        };
        let array = ListViewArray::from(list_data);

        assert_eq!(array.len(), 2);
        assert_eq!(array.value_size(0), 5);
        assert_eq!(array.value_size(1), 5);

        let values: Vec<_> = array
            .iter()
            .map(|x| x.map(|x| x.as_primitive::<Int32Type>().values().to_vec()))
            .collect();
        assert_eq!(
            values,
            vec![Some(vec![0, 1, 2, 3, 4]), Some(vec![3, 4, 5, 6, 7])]
        );
    }

    #[test]
    fn test_list_view_array_incomplete_offsets() {
        let value_data = unsafe {
            ArrayData::builder(DataType::Int32)
                .len(50)
                .add_buffer(Buffer::from_slice_ref((0..50).collect::<Vec<i32>>()))
                .build_unchecked()
        };
        let list_data_type =
            DataType::ListView(Arc::new(Field::new("item", DataType::Int32, false)));
        let list_data = unsafe {
            ArrayData::builder(list_data_type)
                .len(3)
                .add_buffer(Buffer::from_slice_ref([0, 5, 10])) // offsets
                .add_buffer(Buffer::from_slice_ref([0, 5, 10])) // sizes
                .add_child_data(value_data)
                .build_unchecked()
        };
        let array = ListViewArray::from(list_data);

        assert_eq!(array.len(), 3);
        assert_eq!(array.value_size(0), 0);
        assert_eq!(array.value_size(1), 5);
        assert_eq!(array.value_size(2), 10);

        let values: Vec<_> = array
            .iter()
            .map(|x| x.map(|x| x.as_primitive::<Int32Type>().values().to_vec()))
            .collect();
        assert_eq!(
            values,
            vec![
                Some(vec![]),
                Some(vec![5, 6, 7, 8, 9]),
                Some(vec![10, 11, 12, 13, 14, 15, 16, 17, 18, 19])
            ]
        );
    }

    #[test]
    fn test_list_view_array_empty_lists() {
        let value_data = unsafe {
            ArrayData::builder(DataType::Int32)
                .len(0)
                .add_buffer(Buffer::from_slice_ref::<i32, &[_; 0]>(&[]))
                .build_unchecked()
        };
        let list_data_type =
            DataType::ListView(Arc::new(Field::new("item", DataType::Int32, false)));
        let list_data = unsafe {
            ArrayData::builder(list_data_type)
                .len(3)
                .add_buffer(Buffer::from_slice_ref([0, 0, 0])) // offsets
                .add_buffer(Buffer::from_slice_ref([0, 0, 0])) // sizes
                .add_child_data(value_data)
                .build_unchecked()
        };
        let array = ListViewArray::from(list_data);

        assert_eq!(array.len(), 3);
        assert_eq!(array.value_size(0), 0);
        assert_eq!(array.value_size(1), 0);
        assert_eq!(array.value_size(2), 0);

        let values: Vec<_> = array
            .iter()
            .map(|x| x.map(|x| x.as_primitive::<Int32Type>().values().to_vec()))
            .collect();
        assert_eq!(values, vec![Some(vec![]), Some(vec![]), Some(vec![])]);
    }
}
