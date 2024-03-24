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

use crate::array::{get_offsets, get_sizes, make_array, print_long_array};
use crate::builder::{GenericListViewBuilder, PrimitiveBuilder};
use crate::{new_empty_array, Array, ArrayAccessor, ArrayRef, ArrowPrimitiveType, OffsetSizeTrait};
use arrow_buffer::{NullBuffer, OffsetBuffer, SizeBuffer};
use arrow_data::{ArrayData, ArrayDataBuilder};
use arrow_schema::{ArrowError, DataType, FieldRef};
use std::any::Any;
use std::ops::Add;
use std::sync::Arc;
use crate::iterator::GenericListViewArrayIter;

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
    value_offsets: OffsetBuffer<OffsetSize>,
    value_sizes: SizeBuffer<OffsetSize>,
}


impl<OffsetSize: OffsetSizeTrait> Clone for GenericListViewArray<OffsetSize> {
    fn clone(&self) -> Self {
        Self {
            data_type: self.data_type.clone(),
            nulls: self.nulls.clone(),
            values: self.values.clone(),
            value_offsets: self.value_offsets.clone(),
            value_sizes: self.value_sizes.clone(),
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

    pub fn try_new(
        field: FieldRef,
        offsets: OffsetBuffer<OffsetSize>,
        sizes: SizeBuffer<OffsetSize>,
        values: ArrayRef,
        nulls: Option<NullBuffer>,
    ) -> Result<Self, ArrowError> {
        let len = offsets.len();
        if  len != sizes.len() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Length of offsets buffer and sizes buffer must be equal for {}ListViewArray, got {} and {}",
                  OffsetSize::PREFIX, len, sizes.len()
            )));
        }

        if let Some(n) = nulls.as_ref() {
            if n.len() != len {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Incorrect length of null buffer for {}ListViewArray, expected {len} got {}",
                    OffsetSize::PREFIX,
                    n.len(),
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
        offsets: OffsetBuffer<OffsetSize>,
        sizes: SizeBuffer<OffsetSize>,
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
            value_offsets: OffsetBuffer::new_zeroed(len),
            value_sizes: SizeBuffer::new_zeroed(len),
            values,
        }
    }

    /// Deconstruct this array into its constituent parts
    pub fn into_parts(
        self,
    ) -> (
        FieldRef,
        OffsetBuffer<OffsetSize>,
        SizeBuffer<OffsetSize>,
        ArrayRef,
        Option<NullBuffer>,
    ) {
        let f = match self.data_type {
            DataType::ListView(f) | DataType::LargeListView(f) => f,
            _ => unreachable!(),
        };
        (f, self.value_offsets, self.value_sizes, self.values,  self.nulls)
    }

    /// Returns a reference to the offsets of this list
    ///
    /// Unlike [`Self::value_offsets`] this returns the [`OffsetBuffer`]
    /// allowing for zero-copy cloning
    #[inline]
    pub fn offsets(&self) -> &OffsetBuffer<OffsetSize> {
        &self.value_offsets
    }

    /// Returns a reference to the values of this list
    #[inline]
    pub fn values(&self) -> &ArrayRef {
        &self.values
    }

    /// Returns a reference to the sizes of this list
    ///
    /// Unlike [`Self::value_sizes`] this returns the [`SizeBuffer`]
    /// allowing for zero-copy cloning
    #[inline]
    pub fn sizes(&self) -> &SizeBuffer<OffsetSize> {
        &self.value_sizes
    }


    /// Returns a clone of the value type of this list.
    pub fn value_type(&self) -> DataType {
        self.values.data_type().clone()
    }

    /// Returns ith value of this list array.
    /// # Safety
    /// Caller must ensure that the index is within the array bounds
    pub unsafe fn value_unchecked(&self, i: usize) -> ArrayRef {
        let end = self.value_offsets().get_unchecked(i + 1).as_usize();
        let start = self.value_offsets().get_unchecked(i).as_usize();
        self.values.slice(start, end - start)
    }

    /// Returns ith value of this list array.
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
        let offsets = self.value_offsets();
        offsets[i + 1] - offsets[i]
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
        }
    }

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
                    //todo: remove size variable
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
        self.value_offsets.len() - 1
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
        size += self.value_offsets.inner().inner().capacity();
        if let Some(n) = self.nulls.as_ref() {
            size += n.buffer().capacity();
        }
        size
    }

    fn get_array_memory_size(&self) -> usize {
        let mut size = std::mem::size_of::<Self>() + self.values.get_array_memory_size();
        size += self.value_offsets.inner().inner().capacity();
        if let Some(n) = self.nulls.as_ref() {
            size += n.buffer().capacity();
        }
        size
    }
}

impl <OffsetSize: OffsetSizeTrait> std::fmt::Debug for GenericListViewArray<OffsetSize> {
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
            .buffers(vec![array.value_offsets.into_inner().into_inner(), array.value_sizes.into_inner().into_inner()])
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
        let value_offsets = unsafe { get_offsets(&data) };
        let value_sizes = unsafe { get_sizes(&data) };

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
    use super::*;
    use crate::types::Int32Type;
    use crate::Int32Array;
    use arrow_buffer::{Buffer, ScalarBuffer};
    use arrow_schema::Field;

    fn create_from_buffers() -> ListViewArray {
        //  [[0, 1, 2], [3, 4, 5], [6, 7]]
        let values = Int32Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7]);
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, 3, 6]));
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        let sizes = SizeBuffer::new(ScalarBuffer::from(vec![3, 3, 2]));

        ListViewArray::new(field, offsets, sizes,Arc::new(values),  None)
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
    fn test_empty_list_array() {
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
        let list_data_type = DataType::ListView(Arc::new(Field::new("item", DataType::Int32, false)));
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


}