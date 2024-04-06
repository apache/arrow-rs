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

use crate::array::{get_view_offsets, get_view_sizes, make_array, print_long_array};
use crate::builder::{GenericListViewBuilder, PrimitiveBuilder};
use crate::iterator::GenericListViewArrayIter;
use crate::{
    new_empty_array, Array, ArrayAccessor, ArrayRef, ArrowPrimitiveType, FixedSizeListArray,
    OffsetSizeTrait,
};
use arrow_buffer::{NullBuffer, ScalarBuffer};
use arrow_data::{ArrayData, ArrayDataBuilder};
use arrow_schema::{ArrowError, DataType, FieldRef};
use std::any::Any;
use std::ops::Add;
use std::sync::Arc;

/// A [`GenericListViewArray`] of variable size lists, storing offsets as `i32`.
///
// See [`ListViewBuilder`](crate::builder::ListViewBuilder) for how to construct a [`ListViewArray`]
pub type ListViewArray = GenericListViewArray<i32>;

/// A [`GenericListViewArray`] of variable size lists, storing offsets as `i64`.
///
// See [`LargeListViewArray`](crate::builder::LargeListViewBuilder) for how to construct a [`LargeListViewArray`]
pub type LargeListViewArray = GenericListViewArray<i64>;

///
/// Different than [`crate::GenericListArray`] as it stores both an offset and length
/// meaning that take / filter operations can be implemented without copying the underlying data.
///
/// [Variable-size List Layout: ListView Layout]: https://arrow.apache.org/docs/format/Columnar.html#listview-layout
pub struct GenericListViewArray<OffsetSize: OffsetSizeTrait> {
    data_type: DataType,
    nulls: Option<NullBuffer>,
    values: ArrayRef,
    value_offsets: ScalarBuffer<OffsetSize>,
    value_sizes: ScalarBuffer<OffsetSize>,
    len: usize,
}

impl<OffsetSize: OffsetSizeTrait> Clone for GenericListViewArray<OffsetSize> {
    fn clone(&self) -> Self {
        Self {
            data_type: self.data_type.clone(),
            nulls: self.nulls.clone(),
            values: self.values.clone(),
            value_offsets: self.value_offsets.clone(),
            value_sizes: self.value_sizes.clone(),
            len: self.len,
        }
    }
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
    /// * `offsets.last() > values.len()`
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
        for i in 0..offsets.len() {
            let length = values.len();
            let offset = offsets[i].as_usize();
            let size = sizes[i].as_usize();
            if offset > length {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Invalid offset value for {}ListViewArray, offset: {}, length: {}",
                    OffsetSize::PREFIX,
                    offset,
                    length
                )));
            }
            if offset + size > values.len() {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Invalid offset and size values for {}ListViewArray, offset: {}, size: {}, values.len(): {}",
                    OffsetSize::PREFIX, offset, size, values.len()
                )));
            }
        }

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
                "Length of offsets buffer and sizes buffer must be equal for {}ListViewArray, got {} and {}",
                OffsetSize::PREFIX, len, sizes.len()
            )));
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
            len,
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
            value_offsets: ScalarBuffer::new_zeroed(len),
            value_sizes: ScalarBuffer::new_zeroed(len),
            values,
            len: 0usize,
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
        let end = self.value_offsets().get_unchecked(i + 1).as_usize();
        let start = self.value_offsets().get_unchecked(i).as_usize();
        self.values.slice(start, end - start)
    }

    /// Returns ith value of this list view array.
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

    /// Returns the length for value at index `i`.
    #[inline]
    pub fn value_length(&self, i: usize) -> OffsetSize {
        self.value_sizes[i]
    }

    /// constructs a new iterator
    pub fn iter<'a>(&'a self) -> GenericListViewArrayIter<'a, OffsetSize> {
        GenericListViewArrayIter::<'a, OffsetSize>::new(self)
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
            len: length,
        }
    }

    /// Creates a [`GenericListViewArray`] from an iterator of primitive values
    ///
    pub fn from_iter_primitive<T, P, I>(iter: I) -> Self
    where
        T: ArrowPrimitiveType,
        P: IntoIterator<Item = Option<<T as ArrowPrimitiveType>::Native>>,
        I: IntoIterator<Item = Option<P>>,
    {
        let iter = iter.into_iter();
        let size_hint = iter.size_hint().0;
        let mut builder =
            GenericListViewBuilder::with_capacity(PrimitiveBuilder::<T>::new(), size_hint);

        for i in iter {
            match i {
                Some(p) => {
                    let mut size = 0usize;
                    for t in p {
                        builder.values().append_option(t);
                        size = size.add(1);
                    }
                    builder.append(true, size);
                }
                None => builder.append(false, 0),
            }
        }
        builder.finish()
    }
}

impl<'a, OffsetSize: OffsetSizeTrait> ArrayAccessor for &'a GenericListViewArray<OffsetSize> {
    type Item = ArrayRef;

    fn value(&self, index: usize) -> Self::Item {
        GenericListViewArray::value(self, index)
    }

    unsafe fn value_unchecked(&self, index: usize) -> Self::Item {
        GenericListViewArray::value(self, index)
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
        self.len
    }

    fn is_empty(&self) -> bool {
        self.value_offsets.len() <= 1
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
        if let Some(n) = self.nulls.as_ref() {
            size += n.buffer().capacity();
        }
        size
    }

    fn get_array_memory_size(&self) -> usize {
        let mut size = std::mem::size_of::<Self>() + self.values.get_array_memory_size();
        size += self.value_offsets.inner().capacity();
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
        let iter = std::iter::repeat(size).take(value.len());
        let mut offsets = Vec::with_capacity(iter.size_hint().0);
        offsets.push(OffsetSize::usize_as(0));
        let mut acc = 0_usize;
        let iter = std::iter::repeat(size).take(value.len());
        let mut sizes = Vec::with_capacity(iter.size_hint().0);
        for size in iter {
            acc = acc.checked_add(size).expect("usize overflow");
            offsets.push(OffsetSize::usize_as(acc));
            sizes.push(OffsetSize::usize_as(size));
        }
        OffsetSize::from_usize(acc).expect("offset overflow");
        let sizes = ScalarBuffer::from(sizes);
        let offsets = ScalarBuffer::from(offsets);
        Self {
            data_type: Self::DATA_TYPE_CONSTRUCTOR(field.clone()),
            nulls: value.nulls().cloned(),
            values: value.values().clone(),
            value_offsets: offsets,
            value_sizes: sizes,
            len: value.len(),
        }
    }
}

impl<OffsetSize: OffsetSizeTrait> GenericListViewArray<OffsetSize> {
    fn try_new_from_array_data(data: ArrayData) -> Result<Self, ArrowError> {
        if data.buffers().len() != 2 {
            return Err(ArrowError::InvalidArgumentError(format!(
                "ListViewArray data should contain two buffer (value offsets & value size), had {}",
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
                    "[Large]ListViewArray's child datatype {:?} does not \
                             correspond to the List's datatype {:?}",
                    values.data_type(),
                    child_data_type
                )));
            }
        } else {
            return Err(ArrowError::InvalidArgumentError(format!(
                "[Large]ListViewArray's datatype must be [Large]ListViewArray(). It is {:?}",
                data.data_type()
            )));
        }

        let values = make_array(values);
        // SAFETY:
        // ArrayData is valid, and verified type above
        let value_offsets = get_view_offsets(&data);
        let value_sizes = get_view_sizes(&data);

        Ok(Self {
            data_type: data.data_type().clone(),
            nulls: data.nulls().cloned(),
            values,
            value_offsets,
            value_sizes,
            len: data.len(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::{FixedSizeListBuilder, Int32Builder, ListViewBuilder};
    use crate::cast::AsArray;
    use crate::types::Int32Type;
    use crate::{Int32Array, Int64Array};
    use arrow_buffer::{bit_util, Buffer, ScalarBuffer};
    use arrow_schema::DataType::LargeListView;
    use arrow_schema::Field;

    fn create_from_buffers() -> ListViewArray {
        //  [[0, 1, 2], [3, 4, 5], [6, 7]]
        let values = Int32Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7]);
        let offsets = ScalarBuffer::from(vec![0, 3, 6]);
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        let sizes = ScalarBuffer::from(vec![3, 3, 2]);

        ListViewArray::new(field, offsets, sizes, Arc::new(values), None)
    }

    #[test]
    fn test_from_iter_primitive() {
        let data = vec![
            Some(vec![Some(0), Some(1), Some(2)]),
            Some(vec![Some(3), Some(4), Some(5)]),
            Some(vec![Some(6), Some(7)]),
        ];
        let list_array = ListViewArray::from_iter_primitive::<Int32Type, _, _>(data);
        let another = create_from_buffers();
        assert_eq!(list_array, another)
    }

    #[test]
    fn test_empty_list_view_array() {
        // Construct an empty value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(0)
            .add_buffer(Buffer::from([]))
            .build()
            .unwrap();

        // Construct an empty offset buffer
        let value_offsets = Buffer::from([]);
        // Construct an empty size buffer
        let value_sizes = Buffer::from([]);

        // Construct a list view array from the above two
        let list_data_type =
            DataType::ListView(Arc::new(Field::new("item", DataType::Int32, false)));
        let list_data = ArrayData::builder(list_data_type)
            .len(0)
            .add_buffer(value_offsets)
            .add_buffer(value_sizes)
            .add_child_data(value_data)
            .build()
            .unwrap();

        let list_array = ListViewArray::from(list_data);
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

        // Construct a buffer for value offsets, for the nested array:
        //  [[0, 1, 2], [3, 4, 5], [6, 7]]
        let value_offsets = Buffer::from_slice_ref([0, 3, 6, 8]);
        // Construct a buffer for value sizes, for the nested array:
        let value_sizes = Buffer::from_slice_ref([3, 3, 2, 0]);

        // Construct a list view array from the above two
        let list_data_type =
            DataType::ListView(Arc::new(Field::new("item", DataType::Int32, false)));
        let list_data = ArrayData::builder(list_data_type.clone())
            .len(3)
            .add_buffer(value_offsets.clone())
            .add_buffer(value_sizes.clone())
            .add_child_data(value_data.clone())
            .build()
            .unwrap();
        let list_array = ListViewArray::from(list_data);

        let values = list_array.values();
        assert_eq!(value_data, values.to_data());
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(3, list_array.len());
        assert_eq!(0, list_array.null_count());
        assert_eq!(6, list_array.value_offsets()[2]);
        assert_eq!(2, list_array.value_sizes()[2]);
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
            .add_buffer(value_sizes)
            .add_child_data(value_data.clone())
            .build()
            .unwrap();
        let list_array = ListViewArray::from(list_data);

        let values = list_array.values();
        assert_eq!(value_data, values.to_data());
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(2, list_array.len());
        assert_eq!(0, list_array.null_count());
        assert_eq!(6, list_array.value_offsets()[1]);
        assert_eq!(2, list_array.value_sizes()[1]);
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
    fn test_large_list_view_array() {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from_slice_ref([0, 1, 2, 3, 4, 5, 6, 7]))
            .build()
            .unwrap();

        // Construct a buffer for value offsets, for the nested array:
        //  [[0, 1, 2], [3, 4, 5], [6, 7]]
        let value_offsets = Buffer::from_slice_ref([0i64, 3, 6, 8]);

        // Construct a buffer for value sizes, for the nested array:
        let value_sizes = Buffer::from_slice_ref([3i64, 3, 2, 0]);

        // Construct a list view array from the above two
        let list_data_type = LargeListView(Arc::new(Field::new("item", DataType::Int32, false)));
        let list_data = ArrayData::builder(list_data_type.clone())
            .len(3)
            .add_buffer(value_offsets.clone())
            .add_buffer(value_sizes.clone())
            .add_child_data(value_data.clone())
            .build()
            .unwrap();
        let list_array = LargeListViewArray::from(list_data);

        let values = list_array.values();
        assert_eq!(value_data, values.to_data());
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(3, list_array.len());
        assert_eq!(0, list_array.null_count());
        assert_eq!(6, list_array.value_offsets()[2]);
        assert_eq!(2, list_array.value_sizes()[2]);
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
            .add_buffer(value_sizes)
            .add_child_data(value_data.clone())
            .build()
            .unwrap();
        let list_array = LargeListViewArray::from(list_data);

        let values = list_array.values();
        assert_eq!(value_data, values.to_data());
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(2, list_array.len());
        assert_eq!(0, list_array.null_count());
        assert_eq!(6, list_array.value_offsets()[1]);
        assert_eq!(2, list_array.value_sizes()[1]);
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
    fn test_list_view_array_slice() {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(10)
            .add_buffer(Buffer::from_slice_ref([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]))
            .build()
            .unwrap();

        // Construct a buffer for value offsets, for the nested array:
        //  [[0, 1], null, null, [2, 3], [4, 5], null, [6, 7, 8], null, [9]]
        let value_offsets = Buffer::from_slice_ref([0, 2, 2, 2, 4, 6, 6, 9, 9, 10]);
        // Construct a buffer for value sizes
        let value_sizes = Buffer::from_slice_ref([2, 0, 0, 2, 2, 0, 3, 0, 1]);
        // 01011001 00000001
        let mut null_bits: [u8; 2] = [0; 2];
        bit_util::set_bit(&mut null_bits, 0);
        bit_util::set_bit(&mut null_bits, 3);
        bit_util::set_bit(&mut null_bits, 4);
        bit_util::set_bit(&mut null_bits, 6);
        bit_util::set_bit(&mut null_bits, 8);

        // Construct a list view array from the above two
        let list_data_type =
            DataType::ListView(Arc::new(Field::new("item", DataType::Int32, false)));
        let list_data = ArrayData::builder(list_data_type)
            .len(9)
            .add_buffer(value_offsets)
            .add_buffer(value_sizes)
            .add_child_data(value_data.clone())
            .null_bit_buffer(Some(Buffer::from(null_bits)))
            .build()
            .unwrap();
        let list_array = ListViewArray::from(list_data);

        let values = list_array.values();
        assert_eq!(value_data, values.to_data());
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(9, list_array.len());
        assert_eq!(4, list_array.null_count());
        assert_eq!(2, list_array.value_offsets()[3]);
        assert_eq!(2, list_array.value_sizes()[3]);
        assert_eq!(2, list_array.value_length(3));

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
        assert_eq!(2, sliced_list_array.value_length(2));

        assert_eq!(4, sliced_list_array.value_offsets()[3]);
        assert_eq!(2, sliced_list_array.value_sizes()[3]);
        assert_eq!(2, sliced_list_array.value_length(3));

        assert_eq!(6, sliced_list_array.value_offsets()[5]);
        assert_eq!(3, sliced_list_array.value_sizes()[5]);
        assert_eq!(3, sliced_list_array.value_length(5));
    }

    #[test]
    fn test_large_list_view_array_slice() {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(10)
            .add_buffer(Buffer::from_slice_ref([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]))
            .build()
            .unwrap();

        // Construct a buffer for value offsets, for the nested array:
        //  [[0, 1], null, null, [2, 3], [4, 5], null, [6, 7, 8], null, [9]]
        let value_offsets = Buffer::from_slice_ref([0i64, 2, 2, 2, 4, 6, 6, 9, 9, 10]);
        // Construct a buffer for value sizes
        let value_sizes = Buffer::from_slice_ref([2i64, 0, 0, 2, 2, 0, 3, 0, 1]);
        // 01011001 00000001
        let mut null_bits: [u8; 2] = [0; 2];
        bit_util::set_bit(&mut null_bits, 0);
        bit_util::set_bit(&mut null_bits, 3);
        bit_util::set_bit(&mut null_bits, 4);
        bit_util::set_bit(&mut null_bits, 6);
        bit_util::set_bit(&mut null_bits, 8);

        // Construct a list view array from the above two
        let list_data_type = LargeListView(Arc::new(Field::new("item", DataType::Int32, false)));
        let list_data = ArrayData::builder(list_data_type)
            .len(9)
            .add_buffer(value_offsets)
            .add_buffer(value_sizes)
            .add_child_data(value_data.clone())
            .null_bit_buffer(Some(Buffer::from(null_bits)))
            .build()
            .unwrap();
        let list_array = LargeListViewArray::from(list_data);

        let values = list_array.values();
        assert_eq!(value_data, values.to_data());
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(9, list_array.len());
        assert_eq!(4, list_array.null_count());
        assert_eq!(2, list_array.value_offsets()[3]);
        assert_eq!(2, list_array.value_sizes()[3]);
        assert_eq!(2, list_array.value_length(3));

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
        assert_eq!(2, sliced_list_array.value_length(2));
        assert_eq!(2, sliced_list_array.value_sizes()[2]);

        assert_eq!(4, sliced_list_array.value_offsets()[3]);
        assert_eq!(2, sliced_list_array.value_length(3));
        assert_eq!(2, sliced_list_array.value_sizes()[3]);

        assert_eq!(6, sliced_list_array.value_offsets()[5]);
        assert_eq!(3, sliced_list_array.value_length(5));
        assert_eq!(2, sliced_list_array.value_sizes()[3]);
    }

    #[test]
    #[should_panic(expected = "index out of bounds: the len is 10 but the index is 10")]
    fn test_list_view_array_index_out_of_bound() {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(10)
            .add_buffer(Buffer::from_slice_ref([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]))
            .build()
            .unwrap();

        // Construct a buffer for value offsets, for the nested array:
        //  [[0, 1], null, null, [2, 3], [4, 5], null, [6, 7, 8], null, [9]]
        let value_offsets = Buffer::from_slice_ref([0i64, 2, 2, 2, 4, 6, 6, 9, 9, 10]);
        // Construct a buffer for value sizes
        let value_sizes = Buffer::from_slice_ref([2i64, 0, 0, 2, 2, 0, 3, 0, 1]);
        // 01011001 00000001
        let mut null_bits: [u8; 2] = [0; 2];
        bit_util::set_bit(&mut null_bits, 0);
        bit_util::set_bit(&mut null_bits, 3);
        bit_util::set_bit(&mut null_bits, 4);
        bit_util::set_bit(&mut null_bits, 6);
        bit_util::set_bit(&mut null_bits, 8);

        // Construct a list array from the above two
        let list_data_type = LargeListView(Arc::new(Field::new("item", DataType::Int32, false)));
        let list_data = ArrayData::builder(list_data_type)
            .len(9)
            .add_buffer(value_offsets)
            .add_buffer(value_sizes)
            .add_child_data(value_data)
            .null_bit_buffer(Some(Buffer::from(null_bits)))
            .build()
            .unwrap();
        let list_array = LargeListViewArray::from(list_data);
        assert_eq!(9, list_array.len());

        list_array.value(10);
    }
    #[test]
    #[should_panic(
        expected = "ListViewArray data should contain two buffer (value offsets & value size), had 0"
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
        expected = "ListViewArray data should contain two buffer (value offsets & value size), had 1"
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
    #[should_panic(
        expected = "[Large]ListViewArray's datatype must be [Large]ListViewArray(). It is ListView"
    )]
    fn test_from_array_data_validation() {
        let mut builder = ListViewBuilder::new(Int32Builder::new());
        builder.values().append_value(1);
        builder.append(true, 0);
        let array = builder.finish();
        let _ = LargeListViewArray::from(array.into_data());
    }

    #[test]
    fn test_list_view_array_offsets_need_not_start_at_zero() {
        let value_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from_slice_ref([0, 1, 2, 3, 4, 5, 6, 7]))
            .build()
            .unwrap();

        let value_offsets = Buffer::from_slice_ref([2, 2, 5, 7]);
        let value_sizes = Buffer::from_slice_ref([0, 0, 3, 2]);

        let list_data_type =
            DataType::ListView(Arc::new(Field::new("item", DataType::Int32, false)));
        let list_data = ArrayData::builder(list_data_type)
            .len(3)
            .add_buffer(value_offsets)
            .add_buffer(value_sizes)
            .add_child_data(value_data)
            .build()
            .unwrap();

        let list_array = ListViewArray::from(list_data);
        assert_eq!(list_array.value_length(0), 0);
        assert_eq!(list_array.value_length(1), 0);
        assert_eq!(list_array.value_length(2), 3);
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
    fn list_array_equality() {
        // test scaffold
        fn do_comparison(
            lhs_data: Vec<Option<Vec<Option<i32>>>>,
            rhs_data: Vec<Option<Vec<Option<i32>>>>,
            should_equal: bool,
        ) {
            let lhs = ListViewArray::from_iter_primitive::<Int32Type, _, _>(lhs_data.clone());
            let rhs = ListViewArray::from_iter_primitive::<Int32Type, _, _>(rhs_data.clone());
            assert_eq!(lhs == rhs, should_equal);

            let lhs = LargeListViewArray::from_iter_primitive::<Int32Type, _, _>(lhs_data);
            let rhs = LargeListViewArray::from_iter_primitive::<Int32Type, _, _>(rhs_data);
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
        assert_eq!(string.value_offsets(), &[0]);
        assert_eq!(string.value_sizes(), &[0]);

        let string = LargeListViewArray::from(
            ArrayData::builder(DataType::LargeListView(f))
                .buffers(vec![Buffer::from(&[]), Buffer::from(&[])])
                .add_child_data(ArrayData::new_empty(&DataType::Int32))
                .build()
                .unwrap(),
        );
        assert_eq!(string.len(), 0);
        assert_eq!(string.value_offsets(), &[0]);
        assert_eq!(string.value_sizes(), &[0]);
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
        assert_eq!(values, vec![Some(vec![1, 2, 3]), None, Some(vec![4, 5, 6])])
    }
}
