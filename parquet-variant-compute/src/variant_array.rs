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

use crate::VariantArrayBuilder;
use crate::type_conversion::{
    generic_conversion_single_value, generic_conversion_single_value_with_result,
    primitive_conversion_single_value,
};
use arrow::array::{Array, ArrayRef, AsArray, BinaryViewArray, StructArray};
use arrow::buffer::NullBuffer;
use arrow::compute::cast;
use arrow::datatypes::{
    Date32Type, Decimal32Type, Decimal64Type, Decimal128Type, Float16Type, Float32Type,
    Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, Time64MicrosecondType,
    TimestampMicrosecondType, TimestampNanosecondType,
};
use arrow::error::Result;
use arrow_schema::extension::ExtensionType;
use arrow_schema::{ArrowError, DataType, Field, FieldRef, Fields, TimeUnit};
use chrono::{DateTime, NaiveTime};
use parquet_variant::{
    Uuid, Variant, VariantDecimal4, VariantDecimal8, VariantDecimal16, VariantDecimalType as _,
};

use std::borrow::Cow;
use std::sync::Arc;

/// Arrow Variant [`ExtensionType`].
///
/// Represents the canonical Arrow Extension Type for storing variants.
/// See [`VariantArray`] for more examples of using this extension type.
pub struct VariantType;

impl ExtensionType for VariantType {
    const NAME: &'static str = "arrow.parquet.variant";

    // Variants extension metadata is an empty string
    // <https://github.com/apache/arrow/blob/d803afcc43f5d132506318fd9e162d33b2c3d4cd/docs/source/format/CanonicalExtensions.rst?plain=1#L473>
    type Metadata = &'static str;

    fn metadata(&self) -> &Self::Metadata {
        &""
    }

    fn serialize_metadata(&self) -> Option<String> {
        Some(String::new())
    }

    fn deserialize_metadata(_metadata: Option<&str>) -> Result<Self::Metadata> {
        Ok("")
    }

    fn supports_data_type(&self, data_type: &DataType) -> Result<()> {
        if matches!(data_type, DataType::Struct(_)) {
            Ok(())
        } else {
            Err(ArrowError::InvalidArgumentError(format!(
                "VariantType only supports StructArray, got {data_type}"
            )))
        }
    }

    fn try_new(data_type: &DataType, _metadata: Self::Metadata) -> Result<Self> {
        Self.supports_data_type(data_type)?;
        Ok(Self)
    }
}

/// An array of Parquet [`Variant`] values
///
/// A [`VariantArray`] wraps an Arrow [`StructArray`] that stores the underlying
/// `metadata` and `value` fields, and adds convenience methods to access
/// the [`Variant`]s.
///
/// See [`VariantArrayBuilder`] for constructing `VariantArray` row by row.
///
/// See the examples below from converting between `VariantArray` and
/// `StructArray`.
///
/// [`VariantArrayBuilder`]: crate::VariantArrayBuilder
///
/// # Documentation
///
/// At the time of this writing, Variant has been accepted as an official
/// extension type but not been published to the [official list of extension
/// types] on the Apache Arrow website. See the [Extension Type for Parquet
/// Variant arrow] ticket for more details.
///
/// [Extension Type for Parquet Variant arrow]: https://github.com/apache/arrow/issues/46908
/// [official list of extension types]: https://arrow.apache.org/docs/format/CanonicalExtensions.html
///
/// # Example: Check if a [`StructArray`] has the [`VariantType`] extension
///
/// Arrow Arrays only provide [`DataType`], but the extension type information
/// is stored on a [`Field`]. Thus, you must have access to the [`Schema`] or
/// [`Field`] to check for the extension type.
///
/// [`Schema`]: arrow_schema::Schema
/// ```
/// # use arrow::array::StructArray;
/// # use arrow_schema::{Schema, Field, DataType};
/// # use parquet_variant::Variant;
/// # use parquet_variant_compute::{VariantArrayBuilder, VariantArray, VariantType};
/// # fn get_variant_array() -> VariantArray {
/// #   let mut builder = VariantArrayBuilder::new(10);
/// #   builder.append_variant(Variant::from("such wow"));
/// #   builder.build()
/// # }
/// # fn get_schema() -> Schema {
/// #   Schema::new(vec![
/// #     Field::new("id", DataType::Int32, false),
/// #     get_variant_array().field("var"),
/// #   ])
/// # }
/// let schema = get_schema();
/// assert_eq!(schema.fields().len(), 2);
/// // first field is not a Variant
/// assert!(schema.field(0).try_extension_type::<VariantType>().is_err());
/// // second field is a Variant
/// assert!(schema.field(1).try_extension_type::<VariantType>().is_ok());
/// ```
///
/// # Example: Constructing the correct [`Field`] for a [`VariantArray`]
///
/// You can construct the correct [`Field`] for a [`VariantArray`] using the
/// [`VariantArray::field`] method.
///
/// ```
/// # use arrow_schema::{Schema, Field, DataType};
/// # use parquet_variant::Variant;
/// # use parquet_variant_compute::{VariantArrayBuilder, VariantArray, VariantType};
/// # fn get_variant_array() -> VariantArray {
/// #   let mut builder = VariantArrayBuilder::new(10);
/// #   builder.append_variant(Variant::from("such wow"));
/// #   builder.build()
/// # }
/// let variant_array = get_variant_array();
/// // First field is an integer id, second field is a variant
/// let schema = Schema::new(vec![
///   Field::new("id", DataType::Int32, false),
///   // call VariantArray::field to get the correct Field
///   variant_array.field("var"),
/// ]);
/// ```
///
/// You can also construct the [`Field`] using [`VariantType`] directly
///
/// ```
/// # use arrow_schema::{Schema, Field, DataType};
/// # use parquet_variant::Variant;
/// # use parquet_variant_compute::{VariantArrayBuilder, VariantArray, VariantType};
/// # fn get_variant_array() -> VariantArray {
/// #   let mut builder = VariantArrayBuilder::new(10);
/// #   builder.append_variant(Variant::from("such wow"));
/// #   builder.build()
/// # }
/// # let variant_array = get_variant_array();
/// // The DataType of a VariantArray varies depending on how it is shredded
/// let data_type = variant_array.data_type().clone();
/// // First field is an integer id, second field is a variant
/// let schema = Schema::new(vec![
///   Field::new("id", DataType::Int32, false),
///   Field::new("var", data_type, false)
///     // Add extension metadata to the field using `VariantType`
///     .with_extension_type(VariantType),
/// ]);
/// ```
///
/// # Example: Converting a [`VariantArray`] to a [`StructArray`]
///
/// ```
/// # use arrow::array::StructArray;
/// # use parquet_variant::Variant;
/// # use parquet_variant_compute::VariantArrayBuilder;
/// // Create Variant Array
/// let mut builder = VariantArrayBuilder::new(10);
/// builder.append_variant(Variant::from("such wow"));
/// let variant_array = builder.build();
/// // convert to StructArray
/// let struct_array: StructArray = variant_array.into();
/// ```
///
/// # Example: Converting a [`StructArray`] to a [`VariantArray`]
///
/// ```
/// # use arrow::array::StructArray;
/// # use parquet_variant::Variant;
/// # use parquet_variant_compute::{VariantArrayBuilder, VariantArray};
/// # fn get_struct_array() -> StructArray {
/// #   let mut builder = VariantArrayBuilder::new(10);
/// #   builder.append_variant(Variant::from("such wow"));
/// #   builder.build().into()
/// # }
/// let struct_array: StructArray = get_struct_array();
/// // try and create a VariantArray from it
/// let variant_array = VariantArray::try_new(&struct_array).unwrap();
/// assert_eq!(variant_array.value(0), Variant::from("such wow"));
/// ```
///
#[derive(Debug, Clone, PartialEq)]
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
    pub fn try_new(inner: &dyn Array) -> Result<Self> {
        // Workaround lack of support for Binary
        // https://github.com/apache/arrow-rs/issues/8387
        let inner = cast_to_binary_view_arrays(inner)?;

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
            shredding_state: ShreddingState::try_from(inner)?,
        })
    }

    pub(crate) fn from_parts(
        metadata: BinaryViewArray,
        value: Option<BinaryViewArray>,
        typed_value: Option<ArrayRef>,
        nulls: Option<NullBuffer>,
    ) -> Self {
        let mut builder =
            StructArrayBuilder::new().with_field("metadata", Arc::new(metadata.clone()), false);
        if let Some(value) = value.clone() {
            builder = builder.with_field("value", Arc::new(value), true);
        }
        if let Some(typed_value) = typed_value.clone() {
            builder = builder.with_field("typed_value", typed_value, true);
        }
        if let Some(nulls) = nulls {
            builder = builder.with_nulls(nulls);
        }

        Self {
            inner: builder.build(),
            metadata,
            shredding_state: ShreddingState::new(value, typed_value),
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
    /// This is a convenience wrapper that calls [`VariantArray::try_value`] and unwraps the `Result`.
    /// Use `try_value` if you need to handle conversion errors gracefully.
    ///
    /// # Panics
    /// * if the index is out of bounds
    /// * if the array value is null
    /// * if `try_value` returns an error.
    pub fn value(&self, index: usize) -> Variant<'_, '_> {
        self.try_value(index).unwrap()
    }

    /// Return the [`Variant`] instance stored at the given row
    ///
    /// Note: This method does not check for nulls and the value is arbitrary
    /// (but still well-defined) if [`is_null`](Self::is_null) returns true for the index.
    ///
    /// # Panics
    ///
    /// Panics if
    /// * the index is out of bounds
    /// * the array value is null
    ///
    /// # Errors
    ///
    /// Errors if
    /// - the data in `typed_value` cannot be interpreted as a valid `Variant`
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
    pub fn try_value(&self, index: usize) -> Result<Variant<'_, '_>> {
        match (self.typed_value_field(), self.value_field()) {
            // Always prefer typed_value, if available
            (Some(typed_value), value) if typed_value.is_valid(index) => {
                typed_value_to_variant(typed_value, value, index)
            }
            // Otherwise fall back to value, if available
            (_, Some(value)) if value.is_valid(index) => {
                Ok(Variant::new(self.metadata.value(index), value.value(index)))
            }
            // It is technically invalid for neither value nor typed_value fields to be available,
            // but the spec specifically requires readers to return Variant::Null in this case.
            _ => Ok(Variant::Null),
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

    /// Return a field to represent this VariantArray in a `Schema` with
    /// a particular name
    pub fn field(&self, name: impl Into<String>) -> Field {
        Field::new(
            name.into(),
            self.data_type().clone(),
            self.inner.is_nullable(),
        )
        .with_extension_type(VariantType)
    }

    /// Returns a new DataType representing this VariantArray's inner type
    pub fn data_type(&self) -> &DataType {
        self.inner.data_type()
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let inner = self.inner.slice(offset, length);
        let metadata = self.metadata.slice(offset, length);
        let shredding_state = self.shredding_state.slice(offset, length);
        Self {
            inner,
            metadata,
            shredding_state,
        }
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn nulls(&self) -> Option<&NullBuffer> {
        self.inner.nulls()
    }

    /// Is the element at index null?
    pub fn is_null(&self, index: usize) -> bool {
        self.nulls().is_some_and(|n| n.is_null(index))
    }

    /// Is the element at index valid (not null)?
    pub fn is_valid(&self, index: usize) -> bool {
        !self.is_null(index)
    }

    /// Returns an iterator over the values in this array
    pub fn iter(&self) -> VariantArrayIter<'_> {
        VariantArrayIter::new(self)
    }
}

impl From<VariantArray> for StructArray {
    fn from(variant_array: VariantArray) -> Self {
        variant_array.into_inner()
    }
}

impl From<VariantArray> for ArrayRef {
    fn from(variant_array: VariantArray) -> Self {
        Arc::new(variant_array.into_inner())
    }
}

impl<'m, 'v> FromIterator<Option<Variant<'m, 'v>>> for VariantArray {
    fn from_iter<T: IntoIterator<Item = Option<Variant<'m, 'v>>>>(iter: T) -> Self {
        let iter = iter.into_iter();

        let mut b = VariantArrayBuilder::new(iter.size_hint().0);
        b.extend(iter);
        b.build()
    }
}

impl<'m, 'v> FromIterator<Variant<'m, 'v>> for VariantArray {
    fn from_iter<T: IntoIterator<Item = Variant<'m, 'v>>>(iter: T) -> Self {
        Self::from_iter(iter.into_iter().map(Some))
    }
}

/// An iterator over [`VariantArray`]
///
/// This iterator returns `Option<Option<Variant<'a, 'a>>>` where:
/// - `None` indicates the end of iteration
/// - `Some(None)` indicates a null value at this position
/// - `Some(Some(variant))` indicates a valid variant value
///
/// # Example
///
/// ```
/// # use parquet_variant::Variant;
/// # use parquet_variant_compute::VariantArrayBuilder;
/// let mut builder = VariantArrayBuilder::new(10);
/// builder.append_variant(Variant::from(42));
/// builder.append_null();
/// builder.append_variant(Variant::from("hello"));
/// let array = builder.build();
///
/// let values = array.iter().collect::<Vec<_>>();
/// assert_eq!(values.len(), 3);
/// assert_eq!(values[0], Some(Variant::from(42)));
/// assert_eq!(values[1], None);
/// assert_eq!(values[2], Some(Variant::from("hello")));
/// ```
#[derive(Debug)]
pub struct VariantArrayIter<'a> {
    array: &'a VariantArray,
    head_i: usize,
    tail_i: usize,
}

impl<'a> VariantArrayIter<'a> {
    /// Creates a new iterator over the given [`VariantArray`]
    pub fn new(array: &'a VariantArray) -> Self {
        Self {
            array,
            head_i: 0,
            tail_i: array.len(),
        }
    }

    fn value_opt(&self, i: usize) -> Option<Variant<'a, 'a>> {
        self.array.is_valid(i).then(|| self.array.value(i))
    }
}

impl<'a> Iterator for VariantArrayIter<'a> {
    type Item = Option<Variant<'a, 'a>>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.head_i == self.tail_i {
            return None;
        }

        let out = self.value_opt(self.head_i);

        self.head_i += 1;

        Some(out)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remainder = self.tail_i - self.head_i;

        (remainder, Some(remainder))
    }
}

impl<'a> DoubleEndedIterator for VariantArrayIter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.head_i == self.tail_i {
            return None;
        }

        self.tail_i -= 1;

        Some(self.value_opt(self.tail_i))
    }
}

impl<'a> ExactSizeIterator for VariantArrayIter<'a> {}

/// One shredded field of a partially or perfectly shredded variant. For example, suppose the
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
/// variant value (which could be `Variant::Null`).
#[derive(Debug)]
pub struct ShreddedVariantFieldArray {
    /// Reference to the underlying StructArray
    inner: StructArray,
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
    pub fn try_new(inner: &dyn Array) -> Result<Self> {
        let Some(inner_struct) = inner.as_struct_opt() else {
            return Err(ArrowError::InvalidArgumentError(
                "Invalid ShreddedVariantFieldArray: requires StructArray as input".to_string(),
            ));
        };

        // Note this clone is cheap, it just bumps the ref count
        Ok(Self {
            inner: inner_struct.clone(),
            shredding_state: ShreddingState::try_from(inner_struct)?,
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

    /// Returns a reference to the underlying [`StructArray`].
    pub fn inner(&self) -> &StructArray {
        &self.inner
    }

    pub(crate) fn from_parts(
        value: Option<BinaryViewArray>,
        typed_value: Option<ArrayRef>,
        nulls: Option<NullBuffer>,
    ) -> Self {
        let mut builder = StructArrayBuilder::new();
        if let Some(value) = value.clone() {
            builder = builder.with_field("value", Arc::new(value), true);
        }
        if let Some(typed_value) = typed_value.clone() {
            builder = builder.with_field("typed_value", typed_value, true);
        }
        if let Some(nulls) = nulls {
            builder = builder.with_nulls(nulls);
        }

        Self {
            inner: builder.build(),
            shredding_state: ShreddingState::new(value, typed_value),
        }
    }

    /// Returns the inner [`StructArray`], consuming self
    pub fn into_inner(self) -> StructArray {
        self.inner
    }

    pub fn data_type(&self) -> &DataType {
        self.inner.data_type()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn offset(&self) -> usize {
        self.inner.offset()
    }

    pub fn nulls(&self) -> Option<&NullBuffer> {
        // According to the shredding spec, ShreddedVariantFieldArray should be
        // physically non-nullable - SQL NULL is inferred by both value and
        // typed_value being physically NULL
        None
    }
    /// Is the element at index null?
    pub fn is_null(&self, index: usize) -> bool {
        self.nulls().is_some_and(|n| n.is_null(index))
    }

    /// Is the element at index valid (not null)?
    pub fn is_valid(&self, index: usize) -> bool {
        !self.is_null(index)
    }
}

impl From<ShreddedVariantFieldArray> for ArrayRef {
    fn from(array: ShreddedVariantFieldArray) -> Self {
        Arc::new(array.into_inner())
    }
}

impl From<ShreddedVariantFieldArray> for StructArray {
    fn from(array: ShreddedVariantFieldArray) -> Self {
        array.into_inner()
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
/// | value    | typed_value  | Meaning |
/// |----------|--------------|---------|
/// | NULL     | NULL         | The value is missing; only valid for shredded object fields |
/// | non-NULL | NULL         | The value is present and may be any type, including [`Variant::Null`] |
/// | NULL     | non-NULL     | The value is present and is the shredded type |
/// | non-NULL | non-NULL     | The value is present and is a partially shredded object |
///
///
/// Applying the above rules to entire columns, we obtain the following:
///
/// | value  | typed_value  | Meaning |
/// |--------|-------------|---------|
/// | --     | --          | **Missing**: The value is always missing; only valid for shredded object fields |
/// | exists | --          | **Unshredded**: If present, the value may be any type, including [`Variant::Null`]
/// | --     | exists      | **Perfectly shredded**: If present, the value is always the shredded type |
/// | exists | exists      | **Imperfectly shredded**: The value might (not) be present and might (not) be the shredded type |
///
/// NOTE: Partial shredding is a row-wise situation that can arise under imperfect shredding (a
/// column-wise situation): When both columns exist (imperfect shredding) and the typed_value column
/// is a struct, then both columns can be non-NULL for the same row if value is a variant object
/// (partial shredding).
///
/// [Parquet Variant Shredding Spec]: https://github.com/apache/parquet-format/blob/master/VariantShredding.md#value-shredding
#[derive(Debug, Clone, PartialEq)]
pub struct ShreddingState {
    value: Option<BinaryViewArray>,
    typed_value: Option<ArrayRef>,
}

impl ShreddingState {
    /// Create a new `ShreddingState` from the given `value` and `typed_value` fields
    ///
    /// Note you can create a `ShreddingState` from a &[`StructArray`] using
    /// `ShreddingState::try_from(&struct_array)`, for example:
    ///
    /// ```no_run
    /// # use arrow::array::StructArray;
    /// # use parquet_variant_compute::ShreddingState;
    /// # fn get_struct_array() -> StructArray {
    /// #   unimplemented!()
    /// # }
    /// let struct_array: StructArray = get_struct_array();
    /// let shredding_state = ShreddingState::try_from(&struct_array).unwrap();
    /// ```
    pub fn new(value: Option<BinaryViewArray>, typed_value: Option<ArrayRef>) -> Self {
        Self { value, typed_value }
    }

    /// Return a reference to the value field, if present
    pub fn value_field(&self) -> Option<&BinaryViewArray> {
        self.value.as_ref()
    }

    /// Return a reference to the typed_value field, if present
    pub fn typed_value_field(&self) -> Option<&ArrayRef> {
        self.typed_value.as_ref()
    }

    /// Returns a borrowed version of this shredding state
    pub fn borrow(&self) -> BorrowedShreddingState<'_> {
        BorrowedShreddingState {
            value: self.value_field(),
            typed_value: self.typed_value_field(),
        }
    }

    /// Slice all the underlying arrays
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        Self {
            value: self.value.as_ref().map(|v| v.slice(offset, length)),
            typed_value: self.typed_value.as_ref().map(|tv| tv.slice(offset, length)),
        }
    }
}

/// Similar to [`ShreddingState`] except it holds borrowed references of the target arrays. Useful
/// for avoiding clone operations when the caller does not need a self-standing shredding state.
#[derive(Clone, Debug)]
pub struct BorrowedShreddingState<'a> {
    value: Option<&'a BinaryViewArray>,
    typed_value: Option<&'a ArrayRef>,
}

impl<'a> BorrowedShreddingState<'a> {
    /// Create a new `BorrowedShreddingState` from the given `value` and `typed_value` fields
    ///
    /// Note you can create a `BorrowedShreddingState` from a &[`StructArray`] using
    /// `BorrowedShreddingState::try_from(&struct_array)`, for example:
    ///
    /// ```no_run
    /// # use arrow::array::StructArray;
    /// # use parquet_variant_compute::BorrowedShreddingState;
    /// # fn get_struct_array() -> StructArray {
    /// #   unimplemented!()
    /// # }
    /// let struct_array: StructArray = get_struct_array();
    /// let shredding_state = BorrowedShreddingState::try_from(&struct_array).unwrap();
    /// ```
    pub fn new(value: Option<&'a BinaryViewArray>, typed_value: Option<&'a ArrayRef>) -> Self {
        Self { value, typed_value }
    }

    /// Return a reference to the value field, if present
    pub fn value_field(&self) -> Option<&'a BinaryViewArray> {
        self.value
    }

    /// Return a reference to the typed_value field, if present
    pub fn typed_value_field(&self) -> Option<&'a ArrayRef> {
        self.typed_value
    }
}

impl<'a> TryFrom<&'a StructArray> for BorrowedShreddingState<'a> {
    type Error = ArrowError;

    fn try_from(inner_struct: &'a StructArray) -> Result<Self> {
        // The `value` column need not exist, but if it does it must be a binary view.
        let value = if let Some(value_col) = inner_struct.column_by_name("value") {
            let Some(binary_view) = value_col.as_binary_view_opt() else {
                return Err(ArrowError::NotYetImplemented(format!(
                    "VariantArray 'value' field must be BinaryView, got {}",
                    value_col.data_type()
                )));
            };
            Some(binary_view)
        } else {
            None
        };
        let typed_value = inner_struct.column_by_name("typed_value");
        Ok(BorrowedShreddingState::new(value, typed_value))
    }
}

impl TryFrom<&StructArray> for ShreddingState {
    type Error = ArrowError;

    fn try_from(inner_struct: &StructArray) -> Result<Self> {
        Ok(BorrowedShreddingState::try_from(inner_struct)?.into())
    }
}

impl From<BorrowedShreddingState<'_>> for ShreddingState {
    fn from(state: BorrowedShreddingState<'_>) -> Self {
        ShreddingState {
            value: state.value_field().cloned(),
            typed_value: state.typed_value_field().cloned(),
        }
    }
}

/// Builds struct arrays from component fields
///
/// TODO: move to arrow crate
#[derive(Debug, Default, Clone)]
pub(crate) struct StructArrayBuilder {
    fields: Vec<FieldRef>,
    arrays: Vec<ArrayRef>,
    nulls: Option<NullBuffer>,
}

impl StructArrayBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    /// Add an array to this struct array as a field with the specified name.
    pub fn with_field(mut self, field_name: &str, array: ArrayRef, nullable: bool) -> Self {
        let field = Field::new(field_name, array.data_type().clone(), nullable);
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
fn typed_value_to_variant<'a>(
    typed_value: &'a ArrayRef,
    value: Option<&BinaryViewArray>,
    index: usize,
) -> Result<Variant<'a, 'a>> {
    let data_type = typed_value.data_type();
    if value.is_some_and(|v| !matches!(data_type, DataType::Struct(_)) && v.is_valid(index)) {
        // Only a partially shredded struct is allowed to have values for both columns
        panic!("Invalid variant, conflicting value and typed_value");
    }
    match data_type {
        DataType::Null => Ok(Variant::Null),
        DataType::Boolean => {
            let boolean_array = typed_value.as_boolean();
            let value = boolean_array.value(index);
            Ok(Variant::from(value))
        }
        // 16-byte FixedSizeBinary alway corresponds to a UUID; all other sizes are illegal.
        DataType::FixedSizeBinary(16) => {
            let array = typed_value.as_fixed_size_binary();
            let value = array.value(index);
            Ok(Uuid::from_slice(value).unwrap().into()) // unwrap is safe: slice is always 16 bytes
        }
        DataType::BinaryView => {
            let array = typed_value.as_binary_view();
            let value = array.value(index);
            Ok(Variant::from(value))
        }
        DataType::Utf8 => {
            let array = typed_value.as_string::<i32>();
            let value = array.value(index);
            Ok(Variant::from(value))
        }
        DataType::LargeUtf8 => {
            let array = typed_value.as_string::<i64>();
            let value = array.value(index);
            Ok(Variant::from(value))
        }
        DataType::Utf8View => {
            let array = typed_value.as_string_view();
            let value = array.value(index);
            Ok(Variant::from(value))
        }
        DataType::Int8 => {
            primitive_conversion_single_value!(Int8Type, typed_value, index)
        }
        DataType::Int16 => {
            primitive_conversion_single_value!(Int16Type, typed_value, index)
        }
        DataType::Int32 => {
            primitive_conversion_single_value!(Int32Type, typed_value, index)
        }
        DataType::Int64 => {
            primitive_conversion_single_value!(Int64Type, typed_value, index)
        }
        DataType::Float16 => {
            primitive_conversion_single_value!(Float16Type, typed_value, index)
        }
        DataType::Float32 => {
            primitive_conversion_single_value!(Float32Type, typed_value, index)
        }
        DataType::Float64 => {
            primitive_conversion_single_value!(Float64Type, typed_value, index)
        }
        DataType::Decimal32(_, s) => {
            generic_conversion_single_value_with_result!(
                Decimal32Type,
                as_primitive,
                |v| VariantDecimal4::try_new(v, *s as u8),
                typed_value,
                index
            )
        }
        DataType::Decimal64(_, s) => {
            generic_conversion_single_value_with_result!(
                Decimal64Type,
                as_primitive,
                |v| VariantDecimal8::try_new(v, *s as u8),
                typed_value,
                index
            )
        }
        DataType::Decimal128(_, s) => {
            generic_conversion_single_value_with_result!(
                Decimal128Type,
                as_primitive,
                |v| VariantDecimal16::try_new(v, *s as u8),
                typed_value,
                index
            )
        }
        DataType::Date32 => {
            generic_conversion_single_value!(
                Date32Type,
                as_primitive,
                Date32Type::to_naive_date,
                typed_value,
                index
            )
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            generic_conversion_single_value_with_result!(
                Time64MicrosecondType,
                as_primitive,
                |v| NaiveTime::from_num_seconds_from_midnight_opt(
                    (v / 1_000_000) as u32,
                    (v % 1_000_000) as u32 * 1000
                )
                .ok_or_else(|| format!("Invalid microsecond from midnight: {}", v)),
                typed_value,
                index
            )
        }
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => {
            generic_conversion_single_value!(
                TimestampMicrosecondType,
                as_primitive,
                |v| DateTime::from_timestamp_micros(v).unwrap(),
                typed_value,
                index
            )
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            generic_conversion_single_value!(
                TimestampMicrosecondType,
                as_primitive,
                |v| DateTime::from_timestamp_micros(v).unwrap().naive_utc(),
                typed_value,
                index
            )
        }
        DataType::Timestamp(TimeUnit::Nanosecond, Some(_)) => {
            generic_conversion_single_value!(
                TimestampNanosecondType,
                as_primitive,
                DateTime::from_timestamp_nanos,
                typed_value,
                index
            )
        }
        DataType::Timestamp(TimeUnit::Nanosecond, None) => {
            generic_conversion_single_value!(
                TimestampNanosecondType,
                as_primitive,
                |v| DateTime::from_timestamp_nanos(v).naive_utc(),
                typed_value,
                index
            )
        }
        // todo other types here (note this is very similar to cast_to_variant.rs)
        // so it would be great to figure out how to share this code
        _ => {
            // We shouldn't panic in production code, but this is a
            // placeholder until we implement more types
            // https://github.com/apache/arrow-rs/issues/8091
            debug_assert!(
                false,
                "Unsupported typed_value type: {}",
                typed_value.data_type()
            );
            Ok(Variant::Null)
        }
    }
}

/// Workaround for lack of direct support for BinaryArray
/// <https://github.com/apache/arrow-rs/issues/8387>
///
/// The values are read as
/// * `StructArray<metadata: Binary, value: Binary>`
///
/// but VariantArray needs them as
/// * `StructArray<metadata: BinaryView, value: BinaryView>`
///
/// So cast them to get the right type.
fn cast_to_binary_view_arrays(array: &dyn Array) -> Result<ArrayRef> {
    let new_type = canonicalize_and_verify_data_type(array.data_type())?;
    if let Cow::Borrowed(_) = new_type {
        if let Some(array) = array.as_struct_opt() {
            return Ok(Arc::new(array.clone())); // bypass the unnecessary cast
        }
    }
    cast(array, new_type.as_ref())
}

/// Recursively visits a data type, ensuring that it only contains data types that can legally
/// appear in a (possibly shredded) variant array. It also replaces Binary fields with BinaryView,
/// since that's what comes back from the parquet reader and what the variant code expects to find.
fn canonicalize_and_verify_data_type(data_type: &DataType) -> Result<Cow<'_, DataType>> {
    use DataType::*;

    // helper macros
    macro_rules! fail {
        () => {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Illegal shredded value type: {data_type}"
            )))
        };
    }
    macro_rules! borrow {
        () => {
            Cow::Borrowed(data_type)
        };
    }

    let new_data_type = match data_type {
        // Primitive arrow types that have a direct variant counterpart are allowed
        Null | Boolean => borrow!(),
        Int8 | Int16 | Int32 | Int64 | Float32 | Float64 => borrow!(),

        // Unsigned integers and half-float are not allowed
        UInt8 | UInt16 | UInt32 | UInt64 | Float16 => fail!(),

        // Most decimal types are allowed, with restrictions on precision and scale
        //
        // NOTE: arrow-parquet reads widens 32- and 64-bit decimals to 128-bit, but the variant spec
        // requires using the narrowest decimal type for a given precision. Fix those up first.
        Decimal64(p, s) | Decimal128(p, s)
            if VariantDecimal4::is_valid_precision_and_scale(p, s) =>
        {
            Cow::Owned(Decimal32(*p, *s))
        }
        Decimal128(p, s) if VariantDecimal8::is_valid_precision_and_scale(p, s) => {
            Cow::Owned(Decimal64(*p, *s))
        }
        Decimal32(p, s) if VariantDecimal4::is_valid_precision_and_scale(p, s) => borrow!(),
        Decimal64(p, s) if VariantDecimal8::is_valid_precision_and_scale(p, s) => borrow!(),
        Decimal128(p, s) if VariantDecimal16::is_valid_precision_and_scale(p, s) => borrow!(),
        Decimal32(..) | Decimal64(..) | Decimal128(..) | Decimal256(..) => fail!(),

        // Only micro and nano timestamps are allowed
        Timestamp(TimeUnit::Microsecond | TimeUnit::Nanosecond, _) => borrow!(),
        Timestamp(TimeUnit::Millisecond | TimeUnit::Second, _) => fail!(),

        // Only 32-bit dates and 64-bit microsecond time are allowed.
        Date32 | Time64(TimeUnit::Microsecond) => borrow!(),
        Date64 | Time32(_) | Time64(_) | Duration(_) | Interval(_) => fail!(),

        // Binary and string are allowed. Force Binary/LargeBinary to BinaryView because that's what the parquet
        // reader returns and what the rest of the variant code expects.
        Binary | LargeBinary => Cow::Owned(BinaryView),
        BinaryView | Utf8 | LargeUtf8 | Utf8View => borrow!(),

        // UUID maps to 16-byte fixed-size binary; no other width is allowed
        FixedSizeBinary(16) => borrow!(),
        FixedSizeBinary(_) | FixedSizeList(..) => fail!(),

        // We can _possibly_ allow (some of) these some day?
        ListView(_) | LargeList(_) | LargeListView(_) => {
            fail!()
        }

        // Lists and struct are allowed, maps and unions are not
        List(field) => match canonicalize_and_verify_field(field)? {
            Cow::Borrowed(_) => borrow!(),
            Cow::Owned(new_field) => Cow::Owned(DataType::List(new_field)),
        },
        // Struct is used by the internal layout, and can also represent a shredded variant object.
        Struct(fields) => {
            // Avoid allocation unless at least one field changes, to avoid unnecessary deep cloning
            // of the data type. Even if some fields change, the others are shallow arc clones.
            let mut new_fields = std::collections::HashMap::new();
            for (i, field) in fields.iter().enumerate() {
                if let Cow::Owned(new_field) = canonicalize_and_verify_field(field)? {
                    new_fields.insert(i, new_field);
                }
            }

            if new_fields.is_empty() {
                borrow!()
            } else {
                let new_fields = fields
                    .iter()
                    .enumerate()
                    .map(|(i, field)| new_fields.remove(&i).unwrap_or_else(|| field.clone()));
                Cow::Owned(DataType::Struct(new_fields.collect()))
            }
        }
        Map(..) | Union(..) => fail!(),

        // We can _possibly_ support (some of) these some day?
        Dictionary(..) | RunEndEncoded(..) => fail!(),
    };
    Ok(new_data_type)
}

fn canonicalize_and_verify_field(field: &Arc<Field>) -> Result<Cow<'_, Arc<Field>>> {
    let Cow::Owned(new_data_type) = canonicalize_and_verify_data_type(field.data_type())? else {
        return Ok(Cow::Borrowed(field));
    };
    let new_field = field.as_ref().clone().with_data_type(new_data_type);
    Ok(Cow::Owned(Arc::new(new_field)))
}

#[cfg(test)]
mod test {
    use crate::VariantArrayBuilder;
    use std::str::FromStr;

    use super::*;
    use arrow::array::{
        BinaryViewArray, Decimal32Array, Decimal64Array, Decimal128Array, Int32Array,
        Time64MicrosecondArray,
    };
    use arrow_schema::{Field, Fields};
    use parquet_variant::{EMPTY_VARIANT_METADATA_BYTES, ShortString};

    #[test]
    fn invalid_not_a_struct_array() {
        let array = make_binary_view_array();
        // Should fail because the input is not a StructArray
        let err = VariantArray::try_new(&array);
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
        let err = VariantArray::try_new(&array);
        assert_eq!(
            err.unwrap_err().to_string(),
            "Invalid argument error: Invalid VariantArray: StructArray must contain a 'metadata' field"
        );
    }

    #[test]
    fn all_null_missing_value_and_typed_value() {
        let fields = Fields::from(vec![Field::new("metadata", DataType::BinaryView, false)]);
        let array = StructArray::new(fields, vec![make_binary_view_array()], None);

        // NOTE: By strict spec interpretation, this case (top-level variant with null/null)
        // should be invalid, but we currently allow it and treat it as Variant::Null.
        // This is a pragmatic decision to handle missing data gracefully.
        let variant_array = VariantArray::try_new(&array).unwrap();

        // Verify the shredding state is AllNull
        assert!(matches!(
            variant_array.shredding_state(),
            ShreddingState {
                value: None,
                typed_value: None
            }
        ));

        // Verify that value() returns Variant::Null (compensating for spec violation)
        for i in 0..variant_array.len() {
            if variant_array.is_valid(i) {
                assert_eq!(variant_array.value(i), parquet_variant::Variant::Null);
            }
        }
    }

    #[test]
    fn invalid_metadata_field_type() {
        let fields = Fields::from(vec![
            Field::new("metadata", DataType::Int32, true), // not supported
            Field::new("value", DataType::BinaryView, true),
        ]);
        let array = StructArray::new(
            fields,
            vec![make_int32_array(), make_binary_view_array()],
            None,
        );
        let err = VariantArray::try_new(&array);
        assert_eq!(
            err.unwrap_err().to_string(),
            "Not yet implemented: VariantArray 'metadata' field must be BinaryView, got Int32"
        );
    }

    #[test]
    fn invalid_value_field_type() {
        let fields = Fields::from(vec![
            Field::new("metadata", DataType::BinaryView, true),
            Field::new("value", DataType::Int32, true), // Not yet supported
        ]);
        let array = StructArray::new(
            fields,
            vec![make_binary_view_array(), make_int32_array()],
            None,
        );
        let err = VariantArray::try_new(&array);
        assert_eq!(
            err.unwrap_err().to_string(),
            "Not yet implemented: VariantArray 'value' field must be BinaryView, got Int32"
        );
    }

    fn make_binary_view_array() -> ArrayRef {
        Arc::new(BinaryViewArray::from(vec![b"test" as &[u8]]))
    }

    fn make_int32_array() -> ArrayRef {
        Arc::new(Int32Array::from(vec![1]))
    }

    #[test]
    fn all_null_shredding_state() {
        // Verify the shredding state is AllNull
        assert!(matches!(
            ShreddingState::new(None, None),
            ShreddingState {
                value: None,
                typed_value: None
            }
        ));
    }

    #[test]
    fn all_null_variant_array_construction() {
        let metadata = BinaryViewArray::from(vec![b"test" as &[u8]; 3]);
        let nulls = NullBuffer::from(vec![false, false, false]); // all null

        let fields = Fields::from(vec![Field::new("metadata", DataType::BinaryView, false)]);
        let struct_array = StructArray::new(fields, vec![Arc::new(metadata)], Some(nulls));

        let variant_array = VariantArray::try_new(&struct_array).unwrap();

        // Verify the shredding state is AllNull
        assert!(matches!(
            variant_array.shredding_state(),
            ShreddingState {
                value: None,
                typed_value: None
            }
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
                "Expected value at index {i} to be null"
            );
        }
    }

    #[test]
    fn value_field_present_but_all_null_should_be_unshredded() {
        // This test demonstrates the issue: when a value field exists in schema
        // but all its values are null, it should remain Unshredded, not AllNull
        let metadata = BinaryViewArray::from(vec![b"test" as &[u8]; 3]);

        // Create a value field with all null values
        let value_nulls = NullBuffer::from(vec![false, false, false]); // all null
        let value_array = BinaryViewArray::from_iter_values(vec![""; 3]);
        let value_data = value_array
            .to_data()
            .into_builder()
            .nulls(Some(value_nulls))
            .build()
            .unwrap();
        let value = BinaryViewArray::from(value_data);

        let fields = Fields::from(vec![
            Field::new("metadata", DataType::BinaryView, false),
            Field::new("value", DataType::BinaryView, true), // Field exists in schema
        ]);
        let struct_array = StructArray::new(
            fields,
            vec![Arc::new(metadata), Arc::new(value)],
            None, // struct itself is not null, just the value field is all null
        );

        let variant_array = VariantArray::try_new(&struct_array).unwrap();

        // This should be Unshredded, not AllNull, because value field exists in schema
        assert!(matches!(
            variant_array.shredding_state(),
            ShreddingState {
                value: Some(_),
                typed_value: None
            }
        ));
    }

    #[test]
    fn test_variant_array_iterable() {
        let mut b = VariantArrayBuilder::new(6);

        b.append_null();
        b.append_variant(Variant::from(1_i8));
        b.append_variant(Variant::Null);
        b.append_variant(Variant::from(2_i32));
        b.append_variant(Variant::from(3_i64));
        b.append_null();

        let v = b.build();

        let variants = v.iter().collect::<Vec<_>>();

        assert_eq!(
            variants,
            vec![
                None,
                Some(Variant::Int8(1)),
                Some(Variant::Null),
                Some(Variant::Int32(2)),
                Some(Variant::Int64(3)),
                None,
            ]
        );
    }

    #[test]
    fn test_variant_array_iter_double_ended() {
        let mut b = VariantArrayBuilder::new(5);

        b.append_variant(Variant::from(0_i32));
        b.append_null();
        b.append_variant(Variant::from(2_i32));
        b.append_null();
        b.append_variant(Variant::from(4_i32));

        let array = b.build();
        let mut iter = array.iter();

        assert_eq!(iter.next(), Some(Some(Variant::from(0_i32))));
        assert_eq!(iter.next(), Some(None));

        assert_eq!(iter.next_back(), Some(Some(Variant::from(4_i32))));
        assert_eq!(iter.next_back(), Some(None));
        assert_eq!(iter.next_back(), Some(Some(Variant::from(2_i32))));

        assert_eq!(iter.next_back(), None);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_variant_array_iter_reverse() {
        let mut b = VariantArrayBuilder::new(5);

        b.append_variant(Variant::from("a"));
        b.append_null();
        b.append_variant(Variant::from("aaa"));
        b.append_null();
        b.append_variant(Variant::from("aaaaa"));

        let array = b.build();

        let result: Vec<_> = array.iter().rev().collect();
        assert_eq!(
            result,
            vec![
                Some(Variant::from("aaaaa")),
                None,
                Some(Variant::from("aaa")),
                None,
                Some(Variant::from("a")),
            ]
        );
    }

    #[test]
    fn test_variant_array_iter_empty() {
        let v = VariantArrayBuilder::new(0).build();
        let mut i = v.iter();
        assert!(i.next().is_none());
        assert!(i.next_back().is_none());
    }

    #[test]
    fn test_from_variant_opts_into_variant_array() {
        let v = vec![None, Some(Variant::Null), Some(Variant::BooleanFalse), None];

        let variant_array = VariantArray::from_iter(v);

        assert_eq!(variant_array.len(), 4);

        assert!(variant_array.is_null(0));

        assert!(!variant_array.is_null(1));
        assert_eq!(variant_array.value(1), Variant::Null);

        assert!(!variant_array.is_null(2));
        assert_eq!(variant_array.value(2), Variant::BooleanFalse);

        assert!(variant_array.is_null(3));
    }

    #[test]
    fn test_from_variants_into_variant_array() {
        let v = vec![
            Variant::Null,
            Variant::BooleanFalse,
            Variant::ShortString(ShortString::try_new("norm").unwrap()),
        ];

        let variant_array = VariantArray::from_iter(v);

        assert_eq!(variant_array.len(), 3);

        assert!(!variant_array.is_null(0));
        assert_eq!(variant_array.value(0), Variant::Null);

        assert!(!variant_array.is_null(1));
        assert_eq!(variant_array.value(1), Variant::BooleanFalse);

        assert!(!variant_array.is_null(2));
        assert_eq!(
            variant_array.value(2),
            Variant::ShortString(ShortString::try_new("norm").unwrap())
        );
    }

    #[test]
    fn test_variant_equality() {
        let v_iter = [None, Some(Variant::BooleanFalse), Some(Variant::Null), None];
        let v = VariantArray::from_iter(v_iter.clone());

        {
            let v_copy = v.clone();
            assert_eq!(v, v_copy);
        }

        {
            let v_iter_reversed = v_iter.iter().cloned().rev();
            let v_reversed = VariantArray::from_iter(v_iter_reversed);

            assert_ne!(v, v_reversed);
        }

        {
            let v_sliced = v.slice(0, 1);
            assert_ne!(v, v_sliced);
        }
    }

    macro_rules! invalid_variant_array_test {
        ($fn_name: ident, $invalid_typed_value: expr, $error_msg: literal) => {
            #[test]
            fn $fn_name() {
                let metadata = BinaryViewArray::from_iter_values(std::iter::repeat_n(
                    EMPTY_VARIANT_METADATA_BYTES,
                    1,
                ));
                let invalid_typed_value = $invalid_typed_value;

                let struct_array = StructArrayBuilder::new()
                    .with_field("metadata", Arc::new(metadata), false)
                    .with_field("typed_value", Arc::new(invalid_typed_value), true)
                    .build();

                let array: VariantArray = VariantArray::try_new(&struct_array)
                    .expect("should create variant array")
                    .into();

                let result = array.try_value(0);
                assert!(result.is_err());
                let error = result.unwrap_err();
                assert!(matches!(error, ArrowError::CastError(_)));

                let expected: &str = $error_msg;
                assert!(
                    error.to_string().contains($error_msg),
                    "error `{}` did not contain `{}`",
                    error,
                    expected
                )
            }
        };
    }

    invalid_variant_array_test!(
        test_variant_array_invalide_time,
        Time64MicrosecondArray::from(vec![Some(86401000000)]),
        "Cast error: Cast failed at index 0 (array type: Time64(µs)): Invalid microsecond from midnight: 86401000000"
    );

    invalid_variant_array_test!(
        test_variant_array_invalid_decimal32,
        Decimal32Array::from(vec![Some(1234567890)]),
        "Cast error: Cast failed at index 0 (array type: Decimal32(9, 2)): Invalid argument error: 1234567890 is wider than max precision 9"
    );

    invalid_variant_array_test!(
        test_variant_array_invalid_decimal64,
        Decimal64Array::from(vec![Some(1234567890123456789)]),
        "Cast error: Cast failed at index 0 (array type: Decimal64(18, 6)): Invalid argument error: 1234567890123456789 is wider than max precision 18"
    );

    invalid_variant_array_test!(
        test_variant_array_invalid_decimal128,
        Decimal128Array::from(vec![Some(
            i128::from_str("123456789012345678901234567890123456789").unwrap()
        ),]),
        "Cast error: Cast failed at index 0 (array type: Decimal128(38, 10)): Invalid argument error: 123456789012345678901234567890123456789 is wider than max precision 38"
    );
}
