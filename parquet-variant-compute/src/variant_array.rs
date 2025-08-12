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
use arrow_schema::{ArrowError, DataType, Field, FieldRef, Fields};
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

    /// The metadata column of this variant
    metadata: BinaryViewArray,

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

        // Note these clones are cheap, they just bump the ref count
        Ok(Self {
            inner: inner.clone(),
            metadata: metadata.clone(),
            shredding_state: ShreddingState::try_new(inner)?,
        })
    }

    #[allow(unused)]
    pub(crate) fn from_parts(
        metadata: BinaryViewArray,
        value: Option<BinaryViewArray>,
        typed_value: Option<ArrayRef>,
        nulls: Option<NullBuffer>,
    ) -> Self {
        let mut builder =
            StructArrayBuilder::new().with_field("metadata", Arc::new(metadata.clone()));
        if let Some(value) = value.clone() {
            builder = builder.with_field("value", Arc::new(value));
        }
        if let Some(typed_value) = typed_value.clone() {
            builder = builder.with_field("typed_value", typed_value);
        }
        if let Some(nulls) = nulls {
            builder = builder.with_nulls(nulls);
        }

        // This would be a lot simpler if ShreddingState were just a pair of Option... we already
        // have everything we need.
        let inner = builder.build();
        let shredding_state = ShreddingState::try_new(&inner).unwrap(); // valid by construction
        Self {
            inner,
            metadata,
            shredding_state,
        }
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
            ShreddingState::Unshredded { value } => {
                Variant::new(self.metadata.value(index), value.value(index))
            }
            ShreddingState::PerfectlyShredded { typed_value, .. } => {
                if typed_value.is_null(index) {
                    Variant::Null
                } else {
                    typed_value_to_variant(typed_value, index)
                }
            }
            ShreddingState::ImperfectlyShredded { value, typed_value } => {
                if typed_value.is_null(index) {
                    Variant::new(self.metadata.value(index), value.value(index))
                } else {
                    typed_value_to_variant(typed_value, index)
                }
            }
        }
    }

    /// Return a reference to the metadata field of the [`StructArray`]
    pub fn metadata_field(&self) -> &BinaryViewArray {
        &self.metadata
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

/// One shredded field of a partially or prefectly shredded variant. For example, suppose the
/// shredding schema for variant `v` treats it as an object with a single field `a`, where `a` is
/// itself a struct with the single field `b` of type INT. Then the physical layout of the column
/// is:
///
/// ```text
/// v: VARIANT {
///     metadata: BINARY,
///     value: BINARY,
///     typed_value: STRUCT {
///         a: SHREDDED_VARIANT_FIELD {
///             value: BINARY,
///             typed_value: STRUCT {
///                 a: SHREDDED_VARIANT_FIELD {
///                     value: BINARY,
///                     typed_value: INT,
///                 },
///             },
///         },
///     },
/// }
/// ```
///
/// In the above, each row of `v.value` is either a variant value (shredding failed, `v` was not an
/// object at all) or a variant object (partial shredding, `v` was an object but included unexpected
/// fields other than `a`), or is NULL (perfect shredding, `v` was an object containing only the
/// single expected field `a`).
///
/// A similar story unfolds for each `v.typed_value.a.value` -- a variant value if shredding failed
/// (`v:a` was not an object at all), or a variant object (`v:a` was an object with unexpected
/// additional fields), or NULL (`v:a` was an object containing only the single expected field `b`).
///
/// Finally, `v.typed_value.a.typed_value.b.value` is either NULL (`v:a.b` was an integer) or else a
/// variant value.
pub struct ShreddedVariantFieldArray {
    shredding_state: ShreddingState,
}

#[allow(unused)]
impl ShreddedVariantFieldArray {
    /// Creates a new `ShreddedVariantFieldArray` from a [`StructArray`].
    ///
    /// # Arguments
    /// - `inner` - The underlying [`StructArray`] that contains the variant data.
    ///
    /// # Returns
    /// - A new instance of `ShreddedVariantFieldArray`.
    ///
    /// # Errors:
    /// - If the `StructArray` does not contain the required fields
    ///
    /// # Requirements of the `StructArray`
    ///
    /// 1. An optional field named `value` that is binary, large_binary, or
    ///    binary_view
    ///
    /// 2. An optional field named `typed_value` which can be any primitive type
    ///    or be a list, large_list, list_view or struct
    ///
    /// Currently, only `value` columns of type [`BinaryViewArray`] are supported.
    pub fn try_new(inner: ArrayRef) -> Result<Self, ArrowError> {
        let Some(inner) = inner.as_struct_opt() else {
            return Err(ArrowError::InvalidArgumentError(
                "Invalid VariantArray: requires StructArray as input".to_string(),
            ));
        };

        // Note this clone is cheap, it just bumps the ref count
        Ok(Self {
            shredding_state: ShreddingState::try_new(inner)?,
        })
    }

    /// Return the shredding state of this `VariantArray`
    pub fn shredding_state(&self) -> &ShreddingState {
        &self.shredding_state
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
    // TODO: add missing state where there is neither value nor typed_value
    // Missing { metadata: BinaryViewArray },
    /// This variant has no typed_value field
    Unshredded { value: BinaryViewArray },
    /// This variant has a typed_value field and no value field
    /// meaning it is the shredded type
    PerfectlyShredded { typed_value: ArrayRef },
    /// Imperfectly shredded: Shredded values reside in `typed_value` while those that failed to
    /// shred reside in `value`. Missing field values are NULL in both columns, while NULL primitive
    /// values have NULL `typed_value` and `Variant::Null` in `value`.
    ///
    /// NOTE: A partially shredded struct is a special kind of imperfect shredding, where
    /// `typed_value` and `value` are both non-NULL. The `typed_value` is a struct containing the
    /// subset of fields for which shredding was attempted (each field will then have its own value
    /// and/or typed_value sub-fields that indicate how shredding actually turned out). Meanwhile,
    /// the `value` is a variant object containing the subset of fields for which shredding was
    /// not even attempted.
    ImperfectlyShredded {
        value: BinaryViewArray,
        typed_value: ArrayRef,
    },
}

impl ShreddingState {
    /// try to create a new `ShreddingState` from the given fields
    pub fn try_new(inner: &StructArray) -> Result<Self, ArrowError> {
        // Note the specification allows for any order so we must search by name

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
            .transpose()?
            .cloned();

        // Find the typed_value field, if present
        let typed_value = inner.column_by_name("typed_value").cloned();

        match (value, typed_value) {
            (Some(value), Some(typed_value)) => {
                Ok(Self::ImperfectlyShredded { value, typed_value })
            }
            (Some(value), None) => Ok(Self::Unshredded { value }),
            (None, Some(typed_value)) => Ok(Self::PerfectlyShredded { typed_value }),
            (None, None) => Err(ArrowError::InvalidArgumentError(String::from(
                "VariantArray has neither value nor typed_value field",
            ))),
        }
    }

    /// Return a reference to the value field, if present
    pub fn value_field(&self) -> Option<&BinaryViewArray> {
        match self {
            ShreddingState::Unshredded { value, .. } => Some(value),
            ShreddingState::PerfectlyShredded { .. } => None,
            ShreddingState::ImperfectlyShredded { value, .. } => Some(value),
        }
    }

    /// Return a reference to the typed_value field, if present
    pub fn typed_value_field(&self) -> Option<&ArrayRef> {
        match self {
            ShreddingState::Unshredded { .. } => None,
            ShreddingState::PerfectlyShredded { typed_value, .. } => Some(typed_value),
            ShreddingState::ImperfectlyShredded { typed_value, .. } => Some(typed_value),
        }
    }

    /// Slice all the underlying arrays
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        match self {
            ShreddingState::Unshredded { value } => ShreddingState::Unshredded {
                value: value.slice(offset, length),
            },
            ShreddingState::PerfectlyShredded { typed_value } => {
                ShreddingState::PerfectlyShredded {
                    typed_value: typed_value.slice(offset, length),
                }
            }
            ShreddingState::ImperfectlyShredded { value, typed_value } => {
                ShreddingState::ImperfectlyShredded {
                    value: value.slice(offset, length),
                    typed_value: typed_value.slice(offset, length),
                }
            }
        }
    }
}

/// Builds struct arrays from component fields
///
/// TODO: move to arrow crate
#[derive(Debug, Default, Clone)]
pub struct StructArrayBuilder {
    fields: Vec<FieldRef>,
    arrays: Vec<ArrayRef>,
    nulls: Option<NullBuffer>,
}

impl StructArrayBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    /// Add an array to this struct array as a field with the specified name.
    pub fn with_field(mut self, field_name: &str, array: ArrayRef) -> Self {
        let field = Field::new(field_name, array.data_type().clone(), true);
        self.fields.push(Arc::new(field));
        self.arrays.push(array);
        self
    }

    /// Set the null buffer for this struct array.
    pub fn with_nulls(mut self, nulls: NullBuffer) -> Self {
        self.nulls = Some(nulls);
        self
    }

    pub fn build(self) -> StructArray {
        let Self {
            fields,
            arrays,
            nulls,
        } = self;
        StructArray::new(Fields::from(fields), arrays, nulls)
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
        let metadata = self.metadata.slice(offset, length);
        let shredding_state = self.shredding_state.slice(offset, length);
        Arc::new(Self {
            inner,
            metadata,
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
