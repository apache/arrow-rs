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

//! Contains the `NullArray` type.

use crate::builder::NullBuilder;
use crate::{Array, ArrayRef};
use arrow_buffer::buffer::NullBuffer;
use arrow_data::{ArrayData, ArrayDataBuilder};
use arrow_schema::DataType;
use std::any::Any;
use std::sync::Arc;

/// An array of [null values](https://arrow.apache.org/docs/format/Columnar.html#null-layout)
///
/// A `NullArray` is a simplified array where all values are null.
///
/// # Example: Create an array
///
/// ```
/// use arrow_array::{Array, NullArray};
///
/// let array = NullArray::new(10);
///
/// assert!(array.is_nullable());
/// assert_eq!(array.len(), 10);
/// assert_eq!(array.null_count(), 0);
/// assert_eq!(array.logical_null_count(), 10);
/// assert_eq!(array.logical_nulls().unwrap().null_count(), 10);
/// ```
#[derive(Clone)]
pub struct NullArray {
    len: usize,
}

impl NullArray {
    /// Create a new [`NullArray`] of the specified length
    ///
    /// *Note*: Use [`crate::array::new_null_array`] if you need an array of some
    /// other [`DataType`].
    ///
    pub fn new(length: usize) -> Self {
        Self { len: length }
    }

    /// Returns a zero-copy slice of this array with the indicated offset and length.
    pub fn slice(&self, offset: usize, len: usize) -> Self {
        assert!(
            offset.saturating_add(len) <= self.len,
            "the length + offset of the sliced BooleanBuffer cannot exceed the existing length"
        );

        Self { len }
    }

    /// Returns a new null array builder
    ///
    /// Note that the `capacity` parameter to this function is _deprecated_. It
    /// now does nothing, and will be removed in a future version.
    pub fn builder(_capacity: usize) -> NullBuilder {
        NullBuilder::new()
    }
}

/// SAFETY: Correctly implements the contract of Arrow Arrays
unsafe impl Array for NullArray {
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
        &DataType::Null
    }

    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        Arc::new(self.slice(offset, length))
    }

    fn len(&self) -> usize {
        self.len
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn offset(&self) -> usize {
        0
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        None
    }

    fn logical_nulls(&self) -> Option<NullBuffer> {
        (self.len != 0).then(|| NullBuffer::new_null(self.len))
    }

    fn is_nullable(&self) -> bool {
        !self.is_empty()
    }

    fn logical_null_count(&self) -> usize {
        self.len
    }

    fn get_buffer_memory_size(&self) -> usize {
        0
    }

    fn get_array_memory_size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

impl From<ArrayData> for NullArray {
    fn from(data: ArrayData) -> Self {
        assert_eq!(
            data.data_type(),
            &DataType::Null,
            "NullArray data type should be Null"
        );
        assert_eq!(
            data.buffers().len(),
            0,
            "NullArray data should contain 0 buffers"
        );
        assert!(
            data.nulls().is_none(),
            "NullArray data should not contain a null buffer, as no buffers are required"
        );
        Self { len: data.len() }
    }
}

impl From<NullArray> for ArrayData {
    fn from(array: NullArray) -> Self {
        let builder = ArrayDataBuilder::new(DataType::Null).len(array.len);
        unsafe { builder.build_unchecked() }
    }
}

impl std::fmt::Debug for NullArray {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "NullArray({})", self.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Int64Array, StructArray, make_array};
    use arrow_data::transform::MutableArrayData;
    use arrow_schema::Field;

    #[test]
    fn test_null_array() {
        let null_arr = NullArray::new(32);

        assert_eq!(null_arr.len(), 32);
        assert_eq!(null_arr.null_count(), 0);
        assert_eq!(null_arr.logical_null_count(), 32);
        assert_eq!(null_arr.logical_nulls().unwrap().null_count(), 32);
        assert!(null_arr.is_valid(0));
        assert!(null_arr.is_nullable());
    }

    #[test]
    fn test_null_array_slice() {
        let array1 = NullArray::new(32);

        let array2 = array1.slice(8, 16);
        assert_eq!(array2.len(), 16);
        assert_eq!(array2.null_count(), 0);
        assert_eq!(array2.logical_null_count(), 16);
        assert_eq!(array2.logical_nulls().unwrap().null_count(), 16);
        assert!(array2.is_valid(0));
        assert!(array2.is_nullable());
    }

    #[test]
    fn test_debug_null_array() {
        let array = NullArray::new(1024 * 1024);
        assert_eq!(format!("{array:?}"), "NullArray(1048576)");
    }

    #[test]
    fn test_null_array_with_parent_null_buffer() {
        let null_array = NullArray::new(1);
        let int_array = Int64Array::from(vec![42]);

        let fields = vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Null, true),
        ];

        let struct_array_data = ArrayData::builder(DataType::Struct(fields.into()))
            .len(1)
            .add_child_data(int_array.to_data())
            .add_child_data(null_array.to_data())
            .build()
            .unwrap();

        let mut mutable = MutableArrayData::new(vec![&struct_array_data], true, 1);

        // Simulate a NULL value in the parent array, for instance, if array being queried by
        // invalid index
        mutable.extend_nulls(1);
        let data = mutable.freeze();

        let struct_array = Arc::new(StructArray::from(data.clone()));
        assert!(make_array(data) == struct_array);
    }
}
