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

use arrow::array::{Array, ArrayData, ArrayRef, AsArray, BinaryViewArray, StructArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::Int32Type;
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
    /// Reference to the underlying StructArray
    inner: StructArray,

    /// how is this variant array shredded?
    shredding_state: ShreddingState,
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
    /// # Requirements of the `StructArray`
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
    ///
    /// Currently, only [`BinaryViewArray`] are supported.
    ///
    /// [`BinaryViewArray`]: arrow::array::BinaryViewArray
    pub fn try_new(inner: ArrayRef) -> Result<Self, ArrowError> {
        let Some(inner) = inner.as_struct_opt() else {
            return Err(ArrowError::InvalidArgumentError(
                "Invalid VariantArray: requires StructArray as input".to_string(),
            ));
        };

        // Note the specification allows for any order so we must search by name

        // Ensure the StructArray has a metadata field of BinaryView
        let Some(metadata_field) = inner.column_by_name("metadata") else {
            return Err(ArrowError::InvalidArgumentError(
                "Invalid VariantArray: StructArray must contain a 'metadata' field".to_string(),
            ));
        };
        let Some(metadata) = metadata_field.as_binary_view_opt() else {
            return Err(ArrowError::NotYetImplemented(format!(
                "VariantArray 'metadata' field must be BinaryView, got {}",
                metadata_field.data_type()
            )));
        };

        // Find the value field, if present
        let value = inner
            .column_by_name("value")
            .map(|v| {
                let Some(binary_view) = v.as_binary_view_opt() else {
                    return Err(ArrowError::NotYetImplemented(format!(
                        "VariantArray 'value' field must be BinaryView, got {}",
                        v.data_type()
                    )));
                };
                Ok(binary_view)
            })
            .transpose()?;

        // Find the typed_value field, if present
        let typed_value = inner.column_by_name("typed_value");

        // Note these clones are cheap, they just bump the ref count
        let inner = inner.clone();
        let shredding_state =
            ShreddingState::try_new(metadata.clone(), value.cloned(), typed_value.cloned())?;

        Ok(Self {
            inner,
            shredding_state,
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

    /// Return the shredding state of this `VariantArray`
    pub fn shredding_state(&self) -> &ShreddingState {
        &self.shredding_state
    }

    /// Return the [`Variant`] instance stored at the given row
    ///
    /// Consistently with other Arrow arrays types, this API requires you to
    /// check for nulls first using [`Self::is_valid`].
    ///
    /// # Panics
    /// * if the index is out of bounds
    /// * if the array value is null
    ///
    /// If this is a shredded variant but has no value at the shredded location, it
    /// will return [`Variant::Null`].
    ///
    ///
    /// # Performance Note
    ///
    /// This is certainly not the most efficient way to access values in a
    /// `VariantArray`, but it is useful for testing and debugging.
    ///
    /// Note: Does not do deep validation of the [`Variant`], so it is up to the
    /// caller to ensure that the metadata and value were constructed correctly.
    pub fn value(&self, index: usize) -> Variant {
        match &self.shredding_state {
            ShreddingState::Unshredded { metadata, value } => {
                Variant::new(metadata.value(index), value.value(index))
            }
            ShreddingState::FullyShredded { typed_value, .. } => {
                if typed_value.is_null(index) {
                    Variant::Null
                } else {
                    typed_value_to_variant(typed_value, index)
                }
            }
            ShreddingState::PartiallyShredded {
                metadata,
                value,
                typed_value,
            } => {
                if typed_value.is_null(index) {
                    Variant::new(metadata.value(index), value.value(index))
                } else {
                    typed_value_to_variant(typed_value, index)
                }
            }
        }
    }

    /// Return a reference to the metadata field of the [`StructArray`]
    pub fn metadata_field(&self) -> &BinaryViewArray {
        self.shredding_state.metadata_field()
    }

    /// Return a reference to the value field of the `StructArray`
    pub fn value_field(&self) -> Option<&BinaryViewArray> {
        self.shredding_state.value_field()
    }

    /// Return a reference to the typed_value field of the `StructArray`, if present
    pub fn typed_value_field(&self) -> Option<&ArrayRef> {
        self.shredding_state.typed_value_field()
    }
}

/// Variant arrays can be shredded in one of three states, encoded here
#[derive(Debug)]
pub enum ShreddingState {
    /// This variant has no typed_value field
    Unshredded {
        metadata: BinaryViewArray,
        value: BinaryViewArray,
    },
    /// This variant has a typed_value field and no value field
    /// meaning it is fully shredded (aka the value is stored in typed_value)
    FullyShredded {
        metadata: BinaryViewArray,
        typed_value: ArrayRef,
    },
    /// This variant has both a value field and a typed_value field
    /// meaning it is partially shredded: first the typed_value is used, and
    /// if that is null, the value field is used.
    PartiallyShredded {
        metadata: BinaryViewArray,
        value: BinaryViewArray,
        typed_value: ArrayRef,
    },
}

impl ShreddingState {
    /// try to create a new `ShreddingState` from the given fields
    pub fn try_new(
        metadata: BinaryViewArray,
        value: Option<BinaryViewArray>,
        typed_value: Option<ArrayRef>,
    ) -> Result<Self, ArrowError> {
        match (metadata, value, typed_value) {
            (metadata, Some(value), Some(typed_value)) => Ok(Self::PartiallyShredded {
                metadata,
                value,
                typed_value,
            }),
            (metadata, Some(value), None) => Ok(Self::Unshredded { metadata, value }),
            (metadata, None, Some(typed_value)) => Ok(Self::FullyShredded {
                metadata,
                typed_value,
            }),
            (_metadata_field, None, None) => Err(ArrowError::InvalidArgumentError(String::from(
                "VariantArray has neither value nor typed_value field",
            ))),
        }
    }

    /// Return a reference to the metadata field
    pub fn metadata_field(&self) -> &BinaryViewArray {
        match self {
            ShreddingState::Unshredded { metadata, .. } => metadata,
            ShreddingState::FullyShredded { metadata, .. } => metadata,
            ShreddingState::PartiallyShredded { metadata, .. } => metadata,
        }
    }

    /// Return a reference to the value field, if present
    pub fn value_field(&self) -> Option<&BinaryViewArray> {
        match self {
            ShreddingState::Unshredded { value, .. } => Some(value),
            ShreddingState::FullyShredded { .. } => None,
            ShreddingState::PartiallyShredded { value, .. } => Some(value),
        }
    }

    /// Return a reference to the typed_value field, if present
    pub fn typed_value_field(&self) -> Option<&ArrayRef> {
        match self {
            ShreddingState::Unshredded { .. } => None,
            ShreddingState::FullyShredded { typed_value, .. } => Some(typed_value),
            ShreddingState::PartiallyShredded { typed_value, .. } => Some(typed_value),
        }
    }

    /// Slice all the underlying arrays
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        match self {
            ShreddingState::Unshredded { metadata, value } => ShreddingState::Unshredded {
                metadata: metadata.slice(offset, length),
                value: value.slice(offset, length),
            },
            ShreddingState::FullyShredded {
                metadata,
                typed_value,
            } => ShreddingState::FullyShredded {
                metadata: metadata.slice(offset, length),
                typed_value: typed_value.slice(offset, length),
            },
            ShreddingState::PartiallyShredded {
                metadata,
                value,
                typed_value,
            } => ShreddingState::PartiallyShredded {
                metadata: metadata.slice(offset, length),
                value: value.slice(offset, length),
                typed_value: typed_value.slice(offset, length),
            },
        }
    }
}

/// returns the non-null element at index as a Variant
fn typed_value_to_variant(typed_value: &ArrayRef, index: usize) -> Variant {
    match typed_value.data_type() {
        DataType::Int32 => {
            let typed_value = typed_value.as_primitive::<Int32Type>();
            Variant::from(typed_value.value(index))
        }
        // todo other types here
        _ => {
            // We shouldn't panic in production code, but this is a
            // placeholder until we implement more types
            // TODO tickets: XXXX
            debug_assert!(false, "Unsupported typed_value type: {:?}", typed_value.data_type());
            Variant::Null
        }
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
        let inner = self.inner.slice(offset, length);
        let shredding_state = self.shredding_state.slice(offset, length);
        Arc::new(Self {
            inner,
            shredding_state,
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
            "Invalid argument error: VariantArray has neither value nor typed_value field"
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
