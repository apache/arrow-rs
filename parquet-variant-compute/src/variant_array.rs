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
                v.as_binary_view_opt().ok_or_else(|| {
                    ArrowError::NotYetImplemented(format!(
                        "VariantArray 'value' field must be BinaryView, got {}",
                        v.data_type()
                    ))
                })
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
    pub fn value(&self, index: usize) -> Variant<'_, '_> {
        match &self.shredding_state {
            ShreddingState::Unshredded { metadata, value } => {
                Variant::new(metadata.value(index), value.value(index))
            }
            ShreddingState::Typed { typed_value, .. } => {
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
            ShreddingState::AllNull { .. } => Variant::Null,
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

/// Represents the shredding state of a [`VariantArray`]
///
/// [`VariantArray`]s can be shredded according to the [Parquet Variant
/// Shredding Spec]. Shredding means that the actual value is stored in a typed
/// `typed_field` instead of the generic `value` field.
///
/// Both value and typed_value are optional fields used together to encode a
/// single value. Values in the two fields must be interpreted according to the
/// following table (see [Parquet Variant Shredding Spec] for more details):
///
/// | value | typed_value | Meaning |
/// |----------|--------------|---------|
/// | null     | null         | The value is missing; only valid for shredded object fields |
/// | non-null | null         | The value is present and may be any type, including `null` |
/// | null     | non-null     | The value is present and is the shredded type |
/// | non-null | non-null     | The value is present and is a partially shredded object |
///
/// [Parquet Variant Shredding Spec]: https://github.com/apache/parquet-format/blob/master/VariantShredding.md#value-shredding
#[derive(Debug)]
pub enum ShreddingState {
    /// This variant has no typed_value field
    Unshredded {
        metadata: BinaryViewArray,
        value: BinaryViewArray,
    },
    /// This variant has a typed_value field and no value field
    /// meaning it is the shredded type
    Typed {
        metadata: BinaryViewArray,
        typed_value: ArrayRef,
    },
    /// Partially shredded:
    /// * value is an object
    /// * typed_value is a shredded object.
    ///
    /// Note the spec says "Writers must not produce data where both value and
    /// typed_value are non-null, unless the Variant value is an object."
    PartiallyShredded {
        metadata: BinaryViewArray,
        value: BinaryViewArray,
        typed_value: ArrayRef,
    },
    /// All values are null, only metadata is present
    AllNull { metadata: BinaryViewArray },
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
            (metadata, None, Some(typed_value)) => Ok(Self::Typed {
                metadata,
                typed_value,
            }),
            (metadata, None, None) => Ok(Self::AllNull { metadata }),
        }
    }

    /// Return a reference to the metadata field
    pub fn metadata_field(&self) -> &BinaryViewArray {
        match self {
            ShreddingState::Unshredded { metadata, .. } => metadata,
            ShreddingState::Typed { metadata, .. } => metadata,
            ShreddingState::PartiallyShredded { metadata, .. } => metadata,
            ShreddingState::AllNull { metadata } => metadata,
        }
    }

    /// Return a reference to the value field, if present
    pub fn value_field(&self) -> Option<&BinaryViewArray> {
        match self {
            ShreddingState::Unshredded { value, .. } => Some(value),
            ShreddingState::Typed { .. } => None,
            ShreddingState::PartiallyShredded { value, .. } => Some(value),
            ShreddingState::AllNull { .. } => None,
        }
    }

    /// Return a reference to the typed_value field, if present
    pub fn typed_value_field(&self) -> Option<&ArrayRef> {
        match self {
            ShreddingState::Unshredded { .. } => None,
            ShreddingState::Typed { typed_value, .. } => Some(typed_value),
            ShreddingState::PartiallyShredded { typed_value, .. } => Some(typed_value),
            ShreddingState::AllNull { .. } => None,
        }
    }

    /// Slice all the underlying arrays
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        match self {
            ShreddingState::Unshredded { metadata, value } => ShreddingState::Unshredded {
                metadata: metadata.slice(offset, length),
                value: value.slice(offset, length),
            },
            ShreddingState::Typed {
                metadata,
                typed_value,
            } => ShreddingState::Typed {
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
            ShreddingState::AllNull { metadata } => ShreddingState::AllNull {
                metadata: metadata.slice(offset, length),
            },
        }
    }
}

/// returns the non-null element at index as a Variant
fn typed_value_to_variant(typed_value: &ArrayRef, index: usize) -> Variant<'_, '_> {
    match typed_value.data_type() {
        DataType::Int32 => {
            let typed_value = typed_value.as_primitive::<Int32Type>();
            Variant::from(typed_value.value(index))
        }
        // todo other types here (note this is very similar to cast_to_variant.rs)
        // so it would be great to figure out how to share this code
        _ => {
            // We shouldn't panic in production code, but this is a
            // placeholder until we implement more types
            // TODO tickets: XXXX
            debug_assert!(
                false,
                "Unsupported typed_value type: {:?}",
                typed_value.data_type()
            );
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
    fn all_null_missing_value_and_typed_value() {
        let fields = Fields::from(vec![Field::new("metadata", DataType::BinaryView, false)]);
        let array = StructArray::new(fields, vec![make_binary_view_array()], None);
        // Should succeed and create an AllNull variant when neither value nor typed_value are present
        let variant_array = VariantArray::try_new(Arc::new(array)).unwrap();

        // Verify the shredding state is AllNull
        assert!(matches!(
            variant_array.shredding_state(),
            ShreddingState::AllNull { .. }
        ));
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

    #[test]
    fn all_null_shredding_state() {
        let metadata = BinaryViewArray::from(vec![b"test" as &[u8]]);
        let shredding_state = ShreddingState::try_new(metadata.clone(), None, None).unwrap();

        assert!(matches!(shredding_state, ShreddingState::AllNull { .. }));

        // Verify metadata is preserved correctly
        if let ShreddingState::AllNull { metadata: m } = shredding_state {
            assert_eq!(m.len(), metadata.len());
            assert_eq!(m.value(0), metadata.value(0));
        }
    }

    #[test]
    fn all_null_variant_array_construction() {
        let metadata = BinaryViewArray::from(vec![b"test" as &[u8]; 3]);
        let nulls = NullBuffer::from(vec![false, false, false]); // all null

        let fields = Fields::from(vec![Field::new("metadata", DataType::BinaryView, false)]);
        let struct_array = StructArray::new(fields, vec![Arc::new(metadata)], Some(nulls));

        let variant_array = VariantArray::try_new(Arc::new(struct_array)).unwrap();

        // Verify the shredding state is AllNull
        assert!(matches!(
            variant_array.shredding_state(),
            ShreddingState::AllNull { .. }
        ));

        // Verify all values are null
        assert_eq!(variant_array.len(), 3);
        assert!(!variant_array.is_valid(0));
        assert!(!variant_array.is_valid(1));
        assert!(!variant_array.is_valid(2));

        // Verify that value() returns Variant::Null for all indices
        for i in 0..variant_array.len() {
            assert!(
                !variant_array.is_valid(i),
                "Expected value at index {} to be null",
                i
            );
        }
    }
}
