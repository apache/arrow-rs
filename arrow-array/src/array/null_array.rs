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

use crate::Array;
use arrow_data::ArrayData;
use arrow_schema::DataType;
use std::any::Any;

/// An Array where all elements are nulls
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
/// assert_eq!(array.len(), 10);
/// assert_eq!(array.null_count(), 10);
/// ```
#[derive(Clone)]
pub struct NullArray {
    data: ArrayData,
}

impl NullArray {
    /// Create a new [`NullArray`] of the specified length
    ///
    /// *Note*: Use [`crate::array::new_null_array`] if you need an array of some
    /// other [`DataType`].
    ///
    pub fn new(length: usize) -> Self {
        let array_data = ArrayData::builder(DataType::Null).len(length);
        let array_data = unsafe { array_data.build_unchecked() };
        NullArray::from(array_data)
    }
}

impl Array for NullArray {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data(&self) -> &ArrayData {
        &self.data
    }

    fn into_data(self) -> ArrayData {
        self.into()
    }

    /// Returns whether the element at `index` is null.
    /// All elements of a `NullArray` are always null.
    fn is_null(&self, _index: usize) -> bool {
        true
    }

    /// Returns whether the element at `index` is valid.
    /// All elements of a `NullArray` are always invalid.
    fn is_valid(&self, _index: usize) -> bool {
        false
    }

    /// Returns the total number of null values in this array.
    /// The null count of a `NullArray` always equals its length.
    fn null_count(&self) -> usize {
        self.data_ref().len()
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
            data.null_buffer().is_none(),
            "NullArray data should not contain a null buffer, as no buffers are required"
        );
        Self { data }
    }
}

impl From<NullArray> for ArrayData {
    fn from(array: NullArray) -> Self {
        array.data
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

    #[test]
    fn test_null_array() {
        let null_arr = NullArray::new(32);

        assert_eq!(null_arr.len(), 32);
        assert_eq!(null_arr.null_count(), 32);
        assert!(!null_arr.is_valid(0));
    }

    #[test]
    fn test_null_array_slice() {
        let array1 = NullArray::new(32);

        let array2 = array1.slice(8, 16);
        assert_eq!(array2.len(), 16);
        assert_eq!(array2.null_count(), 16);
        assert_eq!(array2.offset(), 8);
    }

    #[test]
    fn test_debug_null_array() {
        let array = NullArray::new(1024 * 1024);
        assert_eq!(format!("{:?}", array), "NullArray(1048576)");
    }
}
