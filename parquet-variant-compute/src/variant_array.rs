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

//! [`VariantArray`] implementation

use arrow::array::{Array, ArrayData, ArrayRef, AsArray, StructArray};
use arrow::buffer::NullBuffer;
use arrow_schema::{ArrowError, DataType};
use parquet_variant::Variant;
use std::any::Any;
use std::sync::Arc;

/// An array of Parquet [`Variant`] values
///
/// A [`VariantArray`] wraps an Arrow [`StructArray`] that stores the underlying
/// `metadata` and `value` fields, and adds convenience methods to access
/// the `Variant`s
///
/// See [`VariantArrayBuilder`] for constructing a `VariantArray`.
///
/// [`VariantArrayBuilder`]: crate::VariantArrayBuilder
///
/// # Specification
///
/// 1. This code follows the conventions for storing variants in Arrow `StructArray`
///    defined by [Extension Type for Parquet Variant arrow] and this [document].
///    At the time of this writing, this is not yet a standardized Arrow extension type.
///
/// [Extension Type for Parquet Variant arrow]: https://github.com/apache/arrow/issues/46908
/// [document]: https://docs.google.com/document/d/1pw0AWoMQY3SjD7R4LgbPvMjG_xSCtXp3rZHkVp9jpZ4/edit?usp=sharing
#[derive(Debug)]
pub struct VariantArray {
    /// StructArray of up to three fields:
    ///
    /// 1. A required field named `metadata` which is binary, large_binary, or
    ///    binary_view
    ///
    /// 2. An optional field named `value` that is binary, large_binary, or
    ///    binary_view
    ///
    /// 3. An optional field named `typed_value` which can be any primitive type
    ///    or be a list, large_list, list_view or struct
    ///
    /// NOTE: It is also permissible for the metadata field to be
    /// Dictionary-Encoded, preferably (but not required) with an index type of
    /// int8.
    inner: StructArray,

    /// Reference to the metadata column of inner
    metadata_ref: ArrayRef,

    /// Reference to the value column of inner
    value_ref: ArrayRef,
}

impl VariantArray {
    /// Creates a new `VariantArray` from a [`StructArray`].
    ///
    /// # Arguments
    /// - `inner` - The underlying [`StructArray`] that contains the variant data.
    ///
    /// # Returns
    /// - A new instance of `VariantArray`.
    ///
    /// # Errors:
    /// - If the `StructArray` does not contain the required fields
    ///
    /// # Current support
    /// This structure does not (yet) support the full Arrow Variant Array specification.
    ///
    /// Only `StructArrays` with `metadata` and `value` fields that are
    /// [`BinaryViewArray`] are supported. Shredded values are not currently supported
    /// nor are using types other than `BinaryViewArray`
    ///
    /// [`BinaryViewArray`]: arrow::array::BinaryViewArray
    pub fn try_new(inner: ArrayRef) -> Result<Self, ArrowError> {
        let Some(inner) = inner.as_struct_opt() else {
            return Err(ArrowError::InvalidArgumentError(
                "Invalid VariantArray: requires StructArray as input".to_string(),
            ));
        };
        // Ensure the StructArray has a metadata field of BinaryView

        let Some(metadata_field) = VariantArray::find_metadata_field(inner) else {
            return Err(ArrowError::InvalidArgumentError(
                "Invalid VariantArray: StructArray must contain a 'metadata' field".to_string(),
            ));
        };
        if metadata_field.data_type() != &DataType::BinaryView {
            return Err(ArrowError::NotYetImplemented(format!(
                "VariantArray 'metadata' field must be BinaryView, got {}",
                metadata_field.data_type()
            )));
        }
        let Some(value_field) = VariantArray::find_value_field(inner) else {
            return Err(ArrowError::InvalidArgumentError(
                "Invalid VariantArray: StructArray must contain a 'value' field".to_string(),
            ));
        };
        if value_field.data_type() != &DataType::BinaryView {
            return Err(ArrowError::NotYetImplemented(format!(
                "VariantArray 'value' field must be BinaryView, got {}",
                value_field.data_type()
            )));
        }

        Ok(Self {
            inner: inner.clone(),
            metadata_ref: metadata_field,
            value_ref: value_field,
        })
    }

    /// Returns a reference to the underlying [`StructArray`].
    pub fn inner(&self) -> &StructArray {
        &self.inner
    }

    /// Returns the inner [`StructArray`], consuming self
    pub fn into_inner(self) -> StructArray {
        self.inner
    }

    /// Return the [`Variant`] instance stored at the given row
    ///
    /// Panics if the index is out of bounds.
    ///
    /// Note: Does not do deep validation of the [`Variant`], so it is up to the
    /// caller to ensure that the metadata and value were constructed correctly.
    pub fn value(&self, index: usize) -> Variant {
        let metadata = self.metadata_field().as_binary_view().value(index);
        let value = self.value_field().as_binary_view().value(index);
        Variant::new(metadata, value)
    }

    fn find_metadata_field(array: &StructArray) -> Option<ArrayRef> {
        array.column_by_name("metadata").cloned()
    }

    fn find_value_field(array: &StructArray) -> Option<ArrayRef> {
        array.column_by_name("value").cloned()
    }

    /// Return a reference to the metadata field of the [`StructArray`]
    pub fn metadata_field(&self) -> &ArrayRef {
        // spec says fields order is not guaranteed, so we search by name
        &self.metadata_ref
    }

    /// Return a reference to the value field of the `StructArray`
    pub fn value_field(&self) -> &ArrayRef {
        // spec says fields order is not guaranteed, so we search by name
        &self.value_ref
    }
}

impl Array for VariantArray {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn to_data(&self) -> ArrayData {
        self.inner.to_data()
    }

    fn into_data(self) -> ArrayData {
        self.inner.into_data()
    }

    fn data_type(&self) -> &DataType {
        self.inner.data_type()
    }

    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        let slice = self.inner.slice(offset, length);
        let met = self.metadata_ref.slice(offset, length);
        let val = self.value_ref.slice(offset, length);
        Arc::new(Self {
            inner: slice,
            metadata_ref: met,
            value_ref: val,
        })
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn offset(&self) -> usize {
        self.inner.offset()
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        self.inner.nulls()
    }

    fn get_buffer_memory_size(&self) -> usize {
        self.inner.get_buffer_memory_size()
    }

    fn get_array_memory_size(&self) -> usize {
        self.inner.get_array_memory_size()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::array::{BinaryArray, BinaryViewArray};
    use arrow_schema::{Field, Fields};

    #[test]
    fn invalid_not_a_struct_array() {
        let array = make_binary_view_array();
        // Should fail because the input is not a StructArray
        let err = VariantArray::try_new(array);
        assert_eq!(
            err.unwrap_err().to_string(),
            "Invalid argument error: Invalid VariantArray: requires StructArray as input"
        );
    }

    #[test]
    fn invalid_missing_metadata() {
        let fields = Fields::from(vec![Field::new("value", DataType::BinaryView, true)]);
        let array = StructArray::new(fields, vec![make_binary_view_array()], None);
        // Should fail because the StructArray does not contain a 'metadata' field
        let err = VariantArray::try_new(Arc::new(array));
        assert_eq!(
            err.unwrap_err().to_string(),
            "Invalid argument error: Invalid VariantArray: StructArray must contain a 'metadata' field"
        );
    }

    #[test]
    fn invalid_missing_value() {
        let fields = Fields::from(vec![Field::new("metadata", DataType::BinaryView, false)]);
        let array = StructArray::new(fields, vec![make_binary_view_array()], None);
        // Should fail because the StructArray does not contain a 'value' field
        let err = VariantArray::try_new(Arc::new(array));
        assert_eq!(
            err.unwrap_err().to_string(),
            "Invalid argument error: Invalid VariantArray: StructArray must contain a 'value' field"
        );
    }

    #[test]
    fn invalid_metadata_field_type() {
        let fields = Fields::from(vec![
            Field::new("metadata", DataType::Binary, true), // Not yet supported
            Field::new("value", DataType::BinaryView, true),
        ]);
        let array = StructArray::new(
            fields,
            vec![make_binary_array(), make_binary_view_array()],
            None,
        );
        let err = VariantArray::try_new(Arc::new(array));
        assert_eq!(
            err.unwrap_err().to_string(),
            "Not yet implemented: VariantArray 'metadata' field must be BinaryView, got Binary"
        );
    }

    #[test]
    fn invalid_value_field_type() {
        let fields = Fields::from(vec![
            Field::new("metadata", DataType::BinaryView, true),
            Field::new("value", DataType::Binary, true), // Not yet supported
        ]);
        let array = StructArray::new(
            fields,
            vec![make_binary_view_array(), make_binary_array()],
            None,
        );
        let err = VariantArray::try_new(Arc::new(array));
        assert_eq!(
            err.unwrap_err().to_string(),
            "Not yet implemented: VariantArray 'value' field must be BinaryView, got Binary"
        );
    }

    fn make_binary_view_array() -> ArrayRef {
        Arc::new(BinaryViewArray::from(vec![b"test" as &[u8]]))
    }

    fn make_binary_array() -> ArrayRef {
        Arc::new(BinaryArray::from(vec![b"test" as &[u8]]))
    }
}
