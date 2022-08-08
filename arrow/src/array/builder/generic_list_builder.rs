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
use std::sync::Arc;

use crate::array::ArrayData;
use crate::array::ArrayRef;
use crate::array::GenericListArray;
use crate::array::OffsetSizeTrait;
use crate::datatypes::Field;

use super::{ArrayBuilder, BufferBuilder, NullBufferBuilder};

///  Array builder for [`GenericListArray`]
#[derive(Debug)]
pub struct GenericListBuilder<OffsetSize: OffsetSizeTrait, T: ArrayBuilder> {
    offsets_builder: BufferBuilder<OffsetSize>,
    null_buffer_builder: NullBufferBuilder,
    values_builder: T,
}

impl<OffsetSize: OffsetSizeTrait, T: ArrayBuilder> GenericListBuilder<OffsetSize, T> {
    /// Creates a new [`GenericListBuilder`] from a given values array builder
    pub fn new(values_builder: T) -> Self {
        let capacity = values_builder.len();
        Self::with_capacity(values_builder, capacity)
    }

    /// Creates a new [`GenericListBuilder`] from a given values array builder
    /// `capacity` is the number of items to pre-allocate space for in this builder
    pub fn with_capacity(values_builder: T, capacity: usize) -> Self {
        let mut offsets_builder = BufferBuilder::<OffsetSize>::new(capacity + 1);
        offsets_builder.append(OffsetSize::zero());
        Self {
            offsets_builder,
            null_buffer_builder: NullBufferBuilder::new(capacity),
            values_builder,
        }
    }
}

impl<OffsetSize: OffsetSizeTrait, T: ArrayBuilder> ArrayBuilder
    for GenericListBuilder<OffsetSize, T>
where
    T: 'static,
{
    /// Returns the builder as a non-mutable `Any` reference.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns the builder as a mutable `Any` reference.
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    /// Returns the boxed builder as a box of `Any`.
    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    /// Returns the number of array slots in the builder
    fn len(&self) -> usize {
        self.null_buffer_builder.len()
    }

    /// Returns whether the number of array slots is zero
    fn is_empty(&self) -> bool {
        self.null_buffer_builder.is_empty()
    }

    /// Builds the array and reset this builder.
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}

impl<OffsetSize: OffsetSizeTrait, T: ArrayBuilder> GenericListBuilder<OffsetSize, T>
where
    T: 'static,
{
    /// Returns the child array builder as a mutable reference.
    ///
    /// This mutable reference can be used to append values into the child array builder,
    /// but you must call [`append`](#method.append) to delimit each distinct list value.
    pub fn values(&mut self) -> &mut T {
        &mut self.values_builder
    }

    /// Returns the child array builder as an immutable reference
    pub fn values_ref(&self) -> &T {
        &self.values_builder
    }

    /// Finish the current variable-length list array slot
    #[inline]
    pub fn append(&mut self, is_valid: bool) {
        self.offsets_builder
            .append(OffsetSize::from_usize(self.values_builder.len()).unwrap());
        self.null_buffer_builder.append(is_valid);
    }

    /// Builds the [`GenericListArray`] and reset this builder.
    pub fn finish(&mut self) -> GenericListArray<OffsetSize> {
        let len = self.len();
        let values_arr = self
            .values_builder
            .as_any_mut()
            .downcast_mut::<T>()
            .unwrap()
            .finish();
        let values_data = values_arr.data();

        let offset_buffer = self.offsets_builder.finish();
        let null_bit_buffer = self.null_buffer_builder.finish();
        self.offsets_builder.append(OffsetSize::zero());
        let field = Box::new(Field::new(
            "item",
            values_data.data_type().clone(),
            true, // TODO: find a consistent way of getting this
        ));
        let data_type = GenericListArray::<OffsetSize>::DATA_TYPE_CONSTRUCTOR(field);
        let array_data_builder = ArrayData::builder(data_type)
            .len(len)
            .add_buffer(offset_buffer)
            .add_child_data(values_data.clone())
            .null_bit_buffer(null_bit_buffer);

        let array_data = unsafe { array_data_builder.build_unchecked() };

        GenericListArray::<OffsetSize>::from(array_data)
    }

    /// Returns the current offsets buffer as a slice
    pub fn offsets_slice(&self) -> &[OffsetSize] {
        self.offsets_builder.as_slice()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::builder::ListBuilder;
    use crate::array::{Array, Int32Array, Int32Builder};
    use crate::buffer::Buffer;
    use crate::datatypes::DataType;

    fn _test_generic_list_array_builder<O: OffsetSizeTrait>() {
        let values_builder = Int32Builder::new(10);
        let mut builder = GenericListBuilder::<O, _>::new(values_builder);

        //  [[0, 1, 2], [3, 4, 5], [6, 7]]
        builder.values().append_value(0);
        builder.values().append_value(1);
        builder.values().append_value(2);
        builder.append(true);
        builder.values().append_value(3);
        builder.values().append_value(4);
        builder.values().append_value(5);
        builder.append(true);
        builder.values().append_value(6);
        builder.values().append_value(7);
        builder.append(true);
        let list_array = builder.finish();

        let values = list_array.values().data().buffers()[0].clone();
        assert_eq!(Buffer::from_slice_ref(&[0, 1, 2, 3, 4, 5, 6, 7]), values);
        assert_eq!(
            Buffer::from_slice_ref(&[0, 3, 6, 8].map(|n| O::from_usize(n).unwrap())),
            list_array.data().buffers()[0].clone()
        );
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(3, list_array.len());
        assert_eq!(0, list_array.null_count());
        assert_eq!(O::from_usize(6).unwrap(), list_array.value_offsets()[2]);
        assert_eq!(O::from_usize(2).unwrap(), list_array.value_length(2));
        for i in 0..3 {
            assert!(list_array.is_valid(i));
            assert!(!list_array.is_null(i));
        }
    }

    #[test]
    fn test_list_array_builder() {
        _test_generic_list_array_builder::<i32>()
    }

    #[test]
    fn test_large_list_array_builder() {
        _test_generic_list_array_builder::<i64>()
    }

    fn _test_generic_list_array_builder_nulls<O: OffsetSizeTrait>() {
        let values_builder = Int32Builder::new(10);
        let mut builder = GenericListBuilder::<O, _>::new(values_builder);

        //  [[0, 1, 2], null, [3, null, 5], [6, 7]]
        builder.values().append_value(0);
        builder.values().append_value(1);
        builder.values().append_value(2);
        builder.append(true);
        builder.append(false);
        builder.values().append_value(3);
        builder.values().append_null();
        builder.values().append_value(5);
        builder.append(true);
        builder.values().append_value(6);
        builder.values().append_value(7);
        builder.append(true);

        let list_array = builder.finish();

        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(4, list_array.len());
        assert_eq!(1, list_array.null_count());
        assert_eq!(O::from_usize(3).unwrap(), list_array.value_offsets()[2]);
        assert_eq!(O::from_usize(3).unwrap(), list_array.value_length(2));
    }

    #[test]
    fn test_list_array_builder_nulls() {
        _test_generic_list_array_builder_nulls::<i32>()
    }

    #[test]
    fn test_large_list_array_builder_nulls() {
        _test_generic_list_array_builder_nulls::<i64>()
    }

    #[test]
    fn test_list_array_builder_finish() {
        let values_builder = Int32Array::builder(5);
        let mut builder = ListBuilder::new(values_builder);

        builder.values().append_slice(&[1, 2, 3]);
        builder.append(true);
        builder.values().append_slice(&[4, 5, 6]);
        builder.append(true);

        let mut arr = builder.finish();
        assert_eq!(2, arr.len());
        assert!(builder.is_empty());

        builder.values().append_slice(&[7, 8, 9]);
        builder.append(true);
        arr = builder.finish();
        assert_eq!(1, arr.len());
        assert!(builder.is_empty());
    }

    #[test]
    fn test_list_list_array_builder() {
        let primitive_builder = Int32Builder::new(10);
        let values_builder = ListBuilder::new(primitive_builder);
        let mut builder = ListBuilder::new(values_builder);

        //  [[[1, 2], [3, 4]], [[5, 6, 7], null, [8]], null, [[9, 10]]]
        builder.values().values().append_value(1);
        builder.values().values().append_value(2);
        builder.values().append(true);
        builder.values().values().append_value(3);
        builder.values().values().append_value(4);
        builder.values().append(true);
        builder.append(true);

        builder.values().values().append_value(5);
        builder.values().values().append_value(6);
        builder.values().values().append_value(7);
        builder.values().append(true);
        builder.values().append(false);
        builder.values().values().append_value(8);
        builder.values().append(true);
        builder.append(true);

        builder.append(false);

        builder.values().values().append_value(9);
        builder.values().values().append_value(10);
        builder.values().append(true);
        builder.append(true);

        let list_array = builder.finish();

        assert_eq!(4, list_array.len());
        assert_eq!(1, list_array.null_count());
        assert_eq!(
            Buffer::from_slice_ref(&[0, 2, 5, 5, 6]),
            list_array.data().buffers()[0].clone()
        );

        assert_eq!(6, list_array.values().data().len());
        assert_eq!(1, list_array.values().data().null_count());
        assert_eq!(
            Buffer::from_slice_ref(&[0, 2, 4, 7, 7, 8, 10]),
            list_array.values().data().buffers()[0].clone()
        );

        assert_eq!(10, list_array.values().data().child_data()[0].len());
        assert_eq!(0, list_array.values().data().child_data()[0].null_count());
        assert_eq!(
            Buffer::from_slice_ref(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
            list_array.values().data().child_data()[0].buffers()[0].clone()
        );
    }
}
