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

//! Module for shredding VariantArray with a given schema.

use crate::variant_array::{ShreddedVariantFieldArray, StructArrayBuilder};
use crate::variant_to_arrow::{
    ArrayVariantToArrowRowBuilder, PrimitiveVariantToArrowRowBuilder,
    make_primitive_variant_to_arrow_row_builder,
};
use crate::{VariantArray, VariantValueArrayBuilder};
use arrow::array::{ArrayRef, BinaryViewArray, NullBufferBuilder};
use arrow::buffer::NullBuffer;
use arrow::compute::CastOptions;
use arrow::datatypes::{DataType, Field, FieldRef, Fields, TimeUnit};
use arrow::error::{ArrowError, Result};
use indexmap::IndexMap;
use parquet_variant::{Variant, VariantBuilderExt, VariantPath, VariantPathElement};
use std::collections::BTreeMap;
use std::sync::Arc;

/// Shreds the input binary variant using a target shredding schema derived from the requested data type.
///
/// For example, requesting `DataType::Int64` would produce an output variant array with the schema:
///
/// ```text
/// {
///    metadata: BINARY,
///    value: BINARY,
///    typed_value: LONG,
/// }
/// ```
///
/// Similarly, requesting `DataType::Struct` with two integer fields `a` and `b` would produce an
/// output variant array with the schema:
///
/// ```text
/// {
///   metadata: BINARY,
///   value: BINARY,
///   typed_value: {
///     a: {
///       value: BINARY,
///       typed_value: INT,
///     },
///     b: {
///       value: BINARY,
///       typed_value: INT,
///     },
///   }
/// }
/// ```
///
/// See [`ShreddedSchemaBuilder`] for a convenient way to build the `as_type`
/// value passed to this function.
pub fn shred_variant(array: &VariantArray, as_type: &DataType) -> Result<VariantArray> {
    if array.typed_value_field().is_some() {
        return Err(ArrowError::InvalidArgumentError(
            "Input is already shredded".to_string(),
        ));
    }

    if array.value_field().is_none() {
        // all-null case -- nothing to do.
        return Ok(array.clone());
    };

    let cast_options = CastOptions::default();
    let mut builder = make_variant_to_shredded_variant_arrow_row_builder(
        as_type,
        &cast_options,
        array.len(),
        true,
    )?;
    for i in 0..array.len() {
        if array.is_null(i) {
            builder.append_null()?;
        } else {
            builder.append_value(array.value(i))?;
        }
    }
    let (value, typed_value, nulls) = builder.finish()?;
    Ok(VariantArray::from_parts(
        array.metadata_field().clone(),
        Some(value),
        Some(typed_value),
        nulls,
    ))
}

pub(crate) fn make_variant_to_shredded_variant_arrow_row_builder<'a>(
    data_type: &'a DataType,
    cast_options: &'a CastOptions,
    capacity: usize,
    top_level: bool,
) -> Result<VariantToShreddedVariantRowBuilder<'a>> {
    let builder = match data_type {
        DataType::Struct(fields) => {
            let typed_value_builder = VariantToShreddedObjectVariantRowBuilder::try_new(
                fields,
                cast_options,
                capacity,
                top_level,
            )?;
            VariantToShreddedVariantRowBuilder::Object(typed_value_builder)
        }
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::FixedSizeList(..) => {
            let typed_value_builder = VariantToShreddedArrayVariantRowBuilder::try_new(
                data_type,
                cast_options,
                capacity,
            )?;
            VariantToShreddedVariantRowBuilder::Array(typed_value_builder)
        }
        // Supported shredded primitive types, see Variant shredding spec:
        // https://github.com/apache/parquet-format/blob/master/VariantShredding.md#shredded-value-types
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal32(..)
        | DataType::Decimal64(..)
        | DataType::Decimal128(..)
        | DataType::Date32
        | DataType::Time64(TimeUnit::Microsecond)
        | DataType::Timestamp(TimeUnit::Microsecond | TimeUnit::Nanosecond, _)
        | DataType::Binary
        | DataType::BinaryView
        | DataType::Utf8
        | DataType::Utf8View
        | DataType::FixedSizeBinary(16) // UUID
        => {
            let builder =
                make_primitive_variant_to_arrow_row_builder(data_type, cast_options, capacity)?;
            let typed_value_builder =
                VariantToShreddedPrimitiveVariantRowBuilder::new(builder, capacity, top_level);
            VariantToShreddedVariantRowBuilder::Primitive(typed_value_builder)
        }
        DataType::FixedSizeBinary(_) => {
            return Err(ArrowError::InvalidArgumentError(format!("{data_type} is not a valid variant shredding type. Only FixedSizeBinary(16) for UUID is supported.")))
        }
        _ => {
            return Err(ArrowError::InvalidArgumentError(format!("{data_type} is not a valid variant shredding type")))
        }
    };
    Ok(builder)
}

pub(crate) enum VariantToShreddedVariantRowBuilder<'a> {
    Primitive(VariantToShreddedPrimitiveVariantRowBuilder<'a>),
    Array(VariantToShreddedArrayVariantRowBuilder<'a>),
    Object(VariantToShreddedObjectVariantRowBuilder<'a>),
}

impl<'a> VariantToShreddedVariantRowBuilder<'a> {
    pub fn append_null(&mut self) -> Result<()> {
        use VariantToShreddedVariantRowBuilder::*;
        match self {
            Primitive(b) => b.append_null(),
            Array(b) => b.append_null(),
            Object(b) => b.append_null(),
        }
    }

    pub fn append_value(&mut self, value: Variant<'_, '_>) -> Result<bool> {
        use VariantToShreddedVariantRowBuilder::*;
        match self {
            Primitive(b) => b.append_value(value),
            Array(b) => b.append_value(value),
            Object(b) => b.append_value(value),
        }
    }

    pub fn finish(self) -> Result<(BinaryViewArray, ArrayRef, Option<NullBuffer>)> {
        use VariantToShreddedVariantRowBuilder::*;
        match self {
            Primitive(b) => b.finish(),
            Array(b) => b.finish(),
            Object(b) => b.finish(),
        }
    }
}

/// A top-level variant shredder -- appending NULL produces typed_value=NULL and value=Variant::Null
pub(crate) struct VariantToShreddedPrimitiveVariantRowBuilder<'a> {
    value_builder: VariantValueArrayBuilder,
    typed_value_builder: PrimitiveVariantToArrowRowBuilder<'a>,
    nulls: NullBufferBuilder,
    top_level: bool,
}

impl<'a> VariantToShreddedPrimitiveVariantRowBuilder<'a> {
    pub(crate) fn new(
        typed_value_builder: PrimitiveVariantToArrowRowBuilder<'a>,
        capacity: usize,
        top_level: bool,
    ) -> Self {
        Self {
            value_builder: VariantValueArrayBuilder::new(capacity),
            typed_value_builder,
            nulls: NullBufferBuilder::new(capacity),
            top_level,
        }
    }

    fn append_null(&mut self) -> Result<()> {
        // Only the top-level struct that represents the variant can be nullable; object fields and
        // array elements are non-nullable.
        self.nulls.append(!self.top_level);
        self.value_builder.append_null();
        self.typed_value_builder.append_null()
    }

    fn append_value(&mut self, value: Variant<'_, '_>) -> Result<bool> {
        self.nulls.append_non_null();
        if self.typed_value_builder.append_value(&value)? {
            self.value_builder.append_null();
        } else {
            self.value_builder.append_value(value);
        }
        Ok(true)
    }

    fn finish(mut self) -> Result<(BinaryViewArray, ArrayRef, Option<NullBuffer>)> {
        Ok((
            self.value_builder.build()?,
            self.typed_value_builder.finish()?,
            self.nulls.finish(),
        ))
    }
}

pub(crate) struct VariantToShreddedArrayVariantRowBuilder<'a> {
    value_builder: VariantValueArrayBuilder,
    typed_value_builder: ArrayVariantToArrowRowBuilder<'a>,
}

impl<'a> VariantToShreddedArrayVariantRowBuilder<'a> {
    fn try_new(
        data_type: &'a DataType,
        cast_options: &'a CastOptions,
        capacity: usize,
    ) -> Result<Self> {
        Ok(Self {
            value_builder: VariantValueArrayBuilder::new(capacity),
            typed_value_builder: ArrayVariantToArrowRowBuilder::try_new(
                data_type,
                cast_options,
                capacity,
            )?,
        })
    }

    fn append_null(&mut self) -> Result<()> {
        self.value_builder.append_value(Variant::Null);
        self.typed_value_builder.append_null();
        Ok(())
    }

    fn append_value(&mut self, variant: Variant<'_, '_>) -> Result<bool> {
        // If the variant is not an array, typed_value must be null.
        // If the variant is an array, value must be null.
        match variant {
            Variant::List(list) => {
                self.value_builder.append_null();
                self.typed_value_builder.append_value(list)?;
                Ok(true)
            }
            other => {
                self.value_builder.append_value(other);
                self.typed_value_builder.append_null();
                Ok(false)
            }
        }
    }

    fn finish(self) -> Result<(BinaryViewArray, ArrayRef, Option<NullBuffer>)> {
        Ok((
            self.value_builder.build()?,
            self.typed_value_builder.finish()?,
            // All elements of an array must be present (not missing) because
            // the array Variant encoding does not allow missing elements
            None,
        ))
    }
}

pub(crate) struct VariantToShreddedObjectVariantRowBuilder<'a> {
    value_builder: VariantValueArrayBuilder,
    typed_value_builders: IndexMap<&'a str, VariantToShreddedVariantRowBuilder<'a>>,
    typed_value_nulls: NullBufferBuilder,
    nulls: NullBufferBuilder,
    top_level: bool,
}

impl<'a> VariantToShreddedObjectVariantRowBuilder<'a> {
    fn try_new(
        fields: &'a Fields,
        cast_options: &'a CastOptions,
        capacity: usize,
        top_level: bool,
    ) -> Result<Self> {
        let typed_value_builders = fields.iter().map(|field| {
            let builder = make_variant_to_shredded_variant_arrow_row_builder(
                field.data_type(),
                cast_options,
                capacity,
                false,
            )?;
            Ok((field.name().as_str(), builder))
        });
        Ok(Self {
            value_builder: VariantValueArrayBuilder::new(capacity),
            typed_value_builders: typed_value_builders.collect::<Result<_>>()?,
            typed_value_nulls: NullBufferBuilder::new(capacity),
            nulls: NullBufferBuilder::new(capacity),
            top_level,
        })
    }

    fn append_null(&mut self) -> Result<()> {
        // Only the top-level struct that represents the variant can be nullable; object fields and
        // array elements are non-nullable.
        self.nulls.append(!self.top_level);
        self.value_builder.append_null();
        self.typed_value_nulls.append_null();
        for (_, typed_value_builder) in &mut self.typed_value_builders {
            typed_value_builder.append_null()?;
        }
        Ok(())
    }

    fn append_value(&mut self, value: Variant<'_, '_>) -> Result<bool> {
        let Variant::Object(ref obj) = value else {
            // Not an object => fall back
            self.nulls.append_non_null();
            self.value_builder.append_value(value);
            self.typed_value_nulls.append_null();
            for (_, typed_value_builder) in &mut self.typed_value_builders {
                typed_value_builder.append_null()?;
            }
            return Ok(false);
        };

        // Route the object's fields by name as either shredded or unshredded
        let mut builder = self.value_builder.builder_ext(value.metadata());
        let mut object_builder = builder.try_new_object()?;
        let mut seen = std::collections::HashSet::new();
        let mut partially_shredded = false;
        for (field_name, value) in obj.iter() {
            match self.typed_value_builders.get_mut(field_name) {
                Some(typed_value_builder) => {
                    typed_value_builder.append_value(value)?;
                    seen.insert(field_name);
                }
                None => {
                    object_builder.insert_bytes(field_name, value);
                    partially_shredded = true;
                }
            }
        }

        // Handle missing fields
        for (field_name, typed_value_builder) in &mut self.typed_value_builders {
            if !seen.contains(field_name) {
                typed_value_builder.append_null()?;
            }
        }

        // Only emit the value if it captured any unshredded object fields
        if partially_shredded {
            object_builder.finish();
        } else {
            drop(object_builder);
            self.value_builder.append_null();
        }

        self.typed_value_nulls.append_non_null();
        self.nulls.append_non_null();
        Ok(true)
    }

    fn finish(mut self) -> Result<(BinaryViewArray, ArrayRef, Option<NullBuffer>)> {
        let mut builder = StructArrayBuilder::new();
        for (field_name, typed_value_builder) in self.typed_value_builders {
            let (value, typed_value, nulls) = typed_value_builder.finish()?;
            let array =
                ShreddedVariantFieldArray::from_parts(Some(value), Some(typed_value), nulls);
            builder = builder.with_field(field_name, ArrayRef::from(array), false);
        }
        if let Some(nulls) = self.typed_value_nulls.finish() {
            builder = builder.with_nulls(nulls);
        }
        Ok((
            self.value_builder.build()?,
            Arc::new(builder.build()),
            self.nulls.finish(),
        ))
    }
}

/// Field configuration captured by the builder (data type + nullability).
#[derive(Clone)]
pub struct ShreddingField {
    data_type: DataType,
    nullable: bool,
}

impl ShreddingField {
    fn new(data_type: DataType, nullable: bool) -> Self {
        Self {
            data_type,
            nullable,
        }
    }

    fn null() -> Self {
        Self::new(DataType::Null, true)
    }
}

/// Convenience conversion to allow passing either `FieldRef`, `DataType`, or `(DataType, bool)`.
pub trait IntoShreddingField {
    fn into_shredding_field(self) -> ShreddingField;
}

impl IntoShreddingField for FieldRef {
    fn into_shredding_field(self) -> ShreddingField {
        ShreddingField::new(self.data_type().clone(), self.is_nullable())
    }
}

impl IntoShreddingField for &DataType {
    fn into_shredding_field(self) -> ShreddingField {
        ShreddingField::new(self.clone(), true)
    }
}

impl IntoShreddingField for DataType {
    fn into_shredding_field(self) -> ShreddingField {
        ShreddingField::new(self, true)
    }
}

impl IntoShreddingField for (&DataType, bool) {
    fn into_shredding_field(self) -> ShreddingField {
        ShreddingField::new(self.0.clone(), self.1)
    }
}

impl IntoShreddingField for (DataType, bool) {
    fn into_shredding_field(self) -> ShreddingField {
        ShreddingField::new(self.0, self.1)
    }
}

/// Builder for constructing a variant shredding schema.
///
/// The builder pattern makes it easy to incrementally define which fields
/// should be shredded and with what types. Fields are nullable by default; pass
/// a `(data_type, nullable)` pair or a `FieldRef` to control nullability.
///
/// Note: this builder currently only supports struct fields. List support
/// will be added in the future.
///
/// # Example
///
/// ```
/// use std::sync::Arc;
/// use arrow::datatypes::{DataType, Field, TimeUnit};
/// use parquet_variant::{VariantPath, VariantPathElement};
/// use parquet_variant_compute::ShreddedSchemaBuilder;
///
/// // Define the shredding schema using the builder
/// let shredding_type = ShreddedSchemaBuilder::default()
///     // store the "time" field as a separate UTC timestamp
///     .with_path("time", (&DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())), true))
///     // store hostname as non-nullable Utf8
///     .with_path("hostname", (&DataType::Utf8, false))
///     // pass a FieldRef directly
///     .with_path(
///         "metadata.trace_id",
///         Arc::new(Field::new("trace_id", DataType::FixedSizeBinary(16), false)),
///     )
///     // field name with a dot: use VariantPath to avoid splitting
///     .with_path(
///         VariantPath::from_iter([VariantPathElement::from("metrics.cpu")]),
///         &DataType::Float64,
///     )
///     .build();
///
/// // The shredding_type can now be passed to shred_variant:
/// // let shredded = shred_variant(&input, &shredding_type)?;
/// ```
#[derive(Default, Clone)]
pub struct ShreddedSchemaBuilder {
    root: VariantSchemaNode,
}

impl ShreddedSchemaBuilder {
    /// Create a new empty schema builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a typed path into the schema using dot notation (or any
    /// [`VariantPath`] convertible).
    ///
    /// The path uses dot notation to specify nested fields.
    /// For example, "a.b.c" will create a nested structure.
    ///
    /// # Arguments
    ///
    /// * `path` - Anything convertible to [`VariantPath`] (e.g., a `&str`)
    /// * `field` - Anything convertible via [`IntoShreddingField`] (e.g. `FieldRef`,
    ///   `&DataType`, or `(&DataType, bool)` to control nullability)
    pub fn with_path<'a, P, F>(mut self, path: P, field: F) -> Self
    where
        P: Into<VariantPath<'a>>,
        F: IntoShreddingField,
    {
        let path: VariantPath<'a> = path.into();
        self.root.insert_path(&path, field.into_shredding_field());
        self
    }

    /// Build the final [`DataType`].
    pub fn build(self) -> DataType {
        let shredding_type = self.root.to_shredding_type();
        match shredding_type {
            Some(shredding_type) => shredding_type,
            None => DataType::Null,
        }
    }
}

/// Internal tree node structure for building variant schemas.
#[derive(Clone)]
enum VariantSchemaNode {
    /// A leaf node with a primitive/scalar type (and nullability)
    Leaf(ShreddingField),
    /// An inner struct node with nested fields
    Struct(BTreeMap<String, VariantSchemaNode>),
}

impl Default for VariantSchemaNode {
    fn default() -> Self {
        Self::Leaf(ShreddingField::null())
    }
}

impl VariantSchemaNode {
    /// Insert a path into this node with the given data type.
    fn insert_path(&mut self, path: &VariantPath<'_>, field: ShreddingField) {
        self.insert_path_elements(path, field);
    }

    fn insert_path_elements(&mut self, segments: &[VariantPathElement<'_>], field: ShreddingField) {
        let Some((head, tail)) = segments.split_first() else {
            *self = Self::Leaf(field);
            return;
        };

        match head {
            VariantPathElement::Field { name } => {
                // Ensure this node is a Struct node
                let children = match self {
                    Self::Struct(children) => children,
                    _ => {
                        *self = Self::Struct(BTreeMap::new());
                        match self {
                            Self::Struct(children) => children,
                            _ => unreachable!(),
                        }
                    }
                };

                children
                    .entry(name.to_string())
                    .or_default()
                    .insert_path_elements(tail, field);
            }
            VariantPathElement::Index { .. } => {
                // List support to be added later; reject for now
                unreachable!("List paths are not supported yet");
            }
        }
    }

    /// Convert this node to a shredding type.
    ///
    /// Returns the [`DataType`] for passing to [`shred_variant`].
    fn to_shredding_type(&self) -> Option<DataType> {
        match self {
            Self::Leaf(field) => Some(field.data_type.clone()),
            Self::Struct(children) => {
                let child_fields: Vec<_> = children
                    .iter()
                    .filter_map(|(name, child)| child.to_shredding_field(name))
                    .collect();
                if child_fields.is_empty() {
                    None
                } else {
                    Some(DataType::Struct(Fields::from(child_fields)))
                }
            }
        }
    }

    fn to_shredding_field(&self, name: &str) -> Option<FieldRef> {
        match self {
            Self::Leaf(field) => Some(Arc::new(Field::new(
                name,
                field.data_type.clone(),
                field.nullable,
            ))),
            Self::Struct(_) => self
                .to_shredding_type()
                .map(|data_type| Arc::new(Field::new(name, data_type, true))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::VariantArrayBuilder;
    use crate::arrow_to_variant::ListLikeArray;
    use arrow::array::{
        Array, BinaryViewArray, FixedSizeBinaryArray, Float64Array, GenericListArray,
        GenericListViewArray, Int64Array, ListArray, OffsetSizeTrait, PrimitiveArray, StringArray,
    };
    use arrow::datatypes::{
        ArrowPrimitiveType, DataType, Field, Fields, Int64Type, TimeUnit, UnionFields, UnionMode,
    };
    use parquet_variant::{
        BuilderSpecificState, EMPTY_VARIANT_METADATA_BYTES, ObjectBuilder, ReadOnlyMetadataBuilder,
        Variant, VariantBuilder, VariantPath, VariantPathElement,
    };
    use std::sync::Arc;
    use uuid::Uuid;

    #[derive(Clone)]
    enum VariantValue<'a> {
        Value(Variant<'a, 'a>),
        List(Vec<VariantValue<'a>>),
        Object(Vec<(&'a str, VariantValue<'a>)>),
        Null,
    }

    impl<'a, T> From<T> for VariantValue<'a>
    where
        T: Into<Variant<'a, 'a>>,
    {
        fn from(value: T) -> Self {
            Self::Value(value.into())
        }
    }

    #[derive(Clone)]
    enum VariantRow<'a> {
        Value(VariantValue<'a>),
        List(Vec<VariantValue<'a>>),
        Object(Vec<(&'a str, VariantValue<'a>)>),
        Null,
    }

    fn build_variant_array(rows: Vec<VariantRow<'static>>) -> VariantArray {
        let mut builder = VariantArrayBuilder::new(rows.len());

        fn append_variant_value<B: VariantBuilderExt>(builder: &mut B, value: VariantValue) {
            match value {
                VariantValue::Value(v) => builder.append_value(v),
                VariantValue::List(values) => {
                    let mut list = builder.new_list();
                    for v in values {
                        append_variant_value(&mut list, v);
                    }
                    list.finish();
                }
                VariantValue::Object(fields) => {
                    let mut object = builder.new_object();
                    for (name, value) in fields {
                        append_variant_field(&mut object, name, value);
                    }
                    object.finish();
                }
                VariantValue::Null => builder.append_null(),
            }
        }

        fn append_variant_field<'a, S: BuilderSpecificState>(
            object: &mut ObjectBuilder<'_, S>,
            name: &'a str,
            value: VariantValue<'a>,
        ) {
            match value {
                VariantValue::Value(v) => {
                    object.insert(name, v);
                }
                VariantValue::List(values) => {
                    let mut list = object.new_list(name);
                    for v in values {
                        append_variant_value(&mut list, v);
                    }
                    list.finish();
                }
                VariantValue::Object(fields) => {
                    let mut nested = object.new_object(name);
                    for (field_name, v) in fields {
                        append_variant_field(&mut nested, field_name, v);
                    }
                    nested.finish();
                }
                VariantValue::Null => {
                    object.insert(name, Variant::Null);
                }
            }
        }

        rows.into_iter().for_each(|row| match row {
            VariantRow::Value(value) => append_variant_value(&mut builder, value),
            VariantRow::List(values) => {
                let mut list = builder.new_list();
                for value in values {
                    append_variant_value(&mut list, value);
                }
                list.finish();
            }
            VariantRow::Object(fields) => {
                let mut object = builder.new_object();
                for (name, value) in fields {
                    append_variant_field(&mut object, name, value);
                }
                object.finish();
            }
            VariantRow::Null => builder.append_null(),
        });
        builder.build()
    }

    trait TestListLikeArray: ListLikeArray {
        type OffsetSize: OffsetSizeTrait;
        fn value_offsets(&self) -> Option<&[Self::OffsetSize]>;
        fn value_size(&self, index: usize) -> Self::OffsetSize;
    }

    impl<O: OffsetSizeTrait> TestListLikeArray for GenericListArray<O> {
        type OffsetSize = O;

        fn value_offsets(&self) -> Option<&[Self::OffsetSize]> {
            Some(GenericListArray::value_offsets(self))
        }

        fn value_size(&self, index: usize) -> Self::OffsetSize {
            GenericListArray::value_length(self, index)
        }
    }

    impl<O: OffsetSizeTrait> TestListLikeArray for GenericListViewArray<O> {
        type OffsetSize = O;

        fn value_offsets(&self) -> Option<&[Self::OffsetSize]> {
            Some(GenericListViewArray::value_offsets(self))
        }

        fn value_size(&self, index: usize) -> Self::OffsetSize {
            GenericListViewArray::value_size(self, index)
        }
    }

    fn downcast_list_like_array<O: OffsetSizeTrait>(
        array: &VariantArray,
    ) -> &dyn TestListLikeArray<OffsetSize = O> {
        let typed_value = array.typed_value_field().unwrap();
        if let Some(list) = typed_value.as_any().downcast_ref::<GenericListArray<O>>() {
            list
        } else if let Some(list_view) = typed_value
            .as_any()
            .downcast_ref::<GenericListViewArray<O>>()
        {
            list_view
        } else {
            panic!(
                "Expected list-like typed_value with matching offset type, got {}",
                typed_value.data_type()
            );
        }
    }

    fn assert_list_structure<O: OffsetSizeTrait>(
        array: &VariantArray,
        expected_len: usize,
        expected_offsets: &[O],
        expected_sizes: &[Option<O>],
        expected_fallbacks: &[Option<Variant<'static, 'static>>],
    ) {
        assert_eq!(array.len(), expected_len);

        let fallbacks = (array.value_field().unwrap(), Some(array.metadata_field()));
        let array = downcast_list_like_array::<O>(array);

        assert_eq!(
            array.value_offsets().unwrap(),
            expected_offsets,
            "list offsets mismatch"
        );
        assert_eq!(
            array.len(),
            expected_sizes.len(),
            "expected_sizes should match array length"
        );
        assert_eq!(
            array.len(),
            expected_fallbacks.len(),
            "expected_fallbacks should match array length"
        );
        assert_eq!(
            array.len(),
            fallbacks.0.len(),
            "fallbacks value field should match array length"
        );

        // Validate per-row shredding outcomes for the list array
        for (idx, (expected_size, expected_fallback)) in expected_sizes
            .iter()
            .zip(expected_fallbacks.iter())
            .enumerate()
        {
            match expected_size {
                Some(len) => {
                    // Successfully shredded: typed list value present, no fallback value
                    assert!(array.is_valid(idx));
                    assert_eq!(array.value_size(idx), *len);
                    assert!(fallbacks.0.is_null(idx));
                }
                None => {
                    // Unable to shred: typed list value absent, fallback should carry the variant
                    assert!(array.is_null(idx));
                    assert_eq!(array.value_size(idx), O::zero());
                    match expected_fallback {
                        Some(expected_variant) => {
                            assert!(fallbacks.0.is_valid(idx));
                            let metadata_bytes = fallbacks
                                .1
                                .filter(|m| m.is_valid(idx))
                                .map(|m| m.value(idx))
                                .filter(|bytes| !bytes.is_empty())
                                .unwrap_or(EMPTY_VARIANT_METADATA_BYTES);
                            assert_eq!(
                                Variant::new(metadata_bytes, fallbacks.0.value(idx)),
                                expected_variant.clone()
                            );
                        }
                        None => unreachable!(),
                    }
                }
            }
        }
    }

    fn assert_list_structure_and_elements<T: ArrowPrimitiveType, O: OffsetSizeTrait>(
        array: &VariantArray,
        expected_len: usize,
        expected_offsets: &[O],
        expected_sizes: &[Option<O>],
        expected_fallbacks: &[Option<Variant<'static, 'static>>],
        expected_shredded_elements: (&[Option<T::Native>], &[Option<Variant<'static, 'static>>]),
    ) {
        assert_list_structure(
            array,
            expected_len,
            expected_offsets,
            expected_sizes,
            expected_fallbacks,
        );
        let array = downcast_list_like_array::<O>(array);

        // Validate the shredded state of list elements (typed values and fallbacks)
        let (expected_values, expected_fallbacks) = expected_shredded_elements;
        assert_eq!(
            expected_values.len(),
            expected_fallbacks.len(),
            "expected_values and expected_fallbacks should be aligned"
        );

        // Validate the shredded primitive values for list elements
        let element_array = ShreddedVariantFieldArray::try_new(array.values().as_ref()).unwrap();
        let element_values = element_array
            .typed_value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .unwrap();
        assert_eq!(element_values.len(), expected_values.len());
        for (idx, expected_value) in expected_values.iter().enumerate() {
            match expected_value {
                Some(value) => {
                    assert!(element_values.is_valid(idx));
                    assert_eq!(element_values.value(idx), *value);
                }
                None => assert!(element_values.is_null(idx)),
            }
        }

        // Validate fallback variants for list elements that could not be shredded
        let element_fallbacks = element_array.value_field().unwrap();
        assert_eq!(element_fallbacks.len(), expected_fallbacks.len());
        for (idx, expected_fallback) in expected_fallbacks.iter().enumerate() {
            match expected_fallback {
                Some(expected_variant) => {
                    assert!(element_fallbacks.is_valid(idx));
                    assert_eq!(
                        Variant::new(EMPTY_VARIANT_METADATA_BYTES, element_fallbacks.value(idx)),
                        expected_variant.clone()
                    );
                }
                None => assert!(element_fallbacks.is_null(idx)),
            }
        }
    }

    #[test]
    fn test_already_shredded_input_error() {
        // Create a VariantArray that already has typed_value_field
        // First create a valid VariantArray, then extract its parts to construct a shredded one
        let temp_array = VariantArray::from_iter(vec![Some(Variant::from("test"))]);
        let metadata = temp_array.metadata_field().clone();
        let value = temp_array.value_field().unwrap().clone();
        let typed_value = Arc::new(Int64Array::from(vec![42])) as ArrayRef;

        let shredded_array =
            VariantArray::from_parts(metadata, Some(value), Some(typed_value), None);

        let result = shred_variant(&shredded_array, &DataType::Int64);
        assert!(matches!(
            result.unwrap_err(),
            ArrowError::InvalidArgumentError(_)
        ));
    }

    #[test]
    fn test_all_null_input() {
        // Create VariantArray with no value field (all null case)
        let metadata = BinaryViewArray::from_iter_values([&[1u8, 0u8]]); // minimal valid metadata
        let all_null_array = VariantArray::from_parts(metadata, None, None, None);
        let result = shred_variant(&all_null_array, &DataType::Int64).unwrap();

        // Should return array with no value/typed_value fields
        assert!(result.value_field().is_none());
        assert!(result.typed_value_field().is_none());
    }

    #[test]
    fn test_invalid_fixed_size_binary_shredding() {
        let mock_uuid_1 = Uuid::new_v4();

        let input = VariantArray::from_iter([Some(Variant::from(mock_uuid_1)), None]);

        // shred_variant only supports FixedSizeBinary(16). Any other length will err.
        let err = shred_variant(&input, &DataType::FixedSizeBinary(17)).unwrap_err();

        assert_eq!(
            err.to_string(),
            "Invalid argument error: FixedSizeBinary(17) is not a valid variant shredding type. Only FixedSizeBinary(16) for UUID is supported."
        );
    }

    #[test]
    fn test_uuid_shredding() {
        let mock_uuid_1 = Uuid::new_v4();
        let mock_uuid_2 = Uuid::new_v4();

        let input = VariantArray::from_iter([
            Some(Variant::from(mock_uuid_1)),
            None,
            Some(Variant::from(false)),
            Some(Variant::from(mock_uuid_2)),
        ]);

        let variant_array = shred_variant(&input, &DataType::FixedSizeBinary(16)).unwrap();

        // // inspect the typed_value Field and make sure it contains the canonical Uuid extension type
        // let typed_value_field = variant_array
        //     .inner()
        //     .fields()
        //     .into_iter()
        //     .find(|f| f.name() == "typed_value")
        //     .unwrap();

        // assert!(
        //     typed_value_field
        //         .try_extension_type::<extension::Uuid>()
        //         .is_ok()
        // );

        // probe the downcasted typed_value array to make sure uuids are shredded correctly
        let uuids = variant_array
            .typed_value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();

        assert_eq!(uuids.len(), 4);

        assert!(!uuids.is_null(0));

        let got_uuid_1: &[u8] = uuids.value(0);
        assert_eq!(got_uuid_1, mock_uuid_1.as_bytes());

        assert!(uuids.is_null(1));
        assert!(uuids.is_null(2));

        assert!(!uuids.is_null(3));

        let got_uuid_2: &[u8] = uuids.value(3);
        assert_eq!(got_uuid_2, mock_uuid_2.as_bytes());
    }

    #[test]
    fn test_primitive_shredding_comprehensive() {
        // Test mixed scenarios in a single array
        let input = VariantArray::from_iter(vec![
            Some(Variant::from(42i64)),   // successful shred
            Some(Variant::from("hello")), // failed shred (string)
            Some(Variant::from(100i64)),  // successful shred
            None,                         // array-level null
            Some(Variant::Null),          // variant null
            Some(Variant::from(3i8)),     // successful shred (int8->int64 conversion)
        ]);

        let result = shred_variant(&input, &DataType::Int64).unwrap();

        // Verify structure
        let metadata_field = result.metadata_field();
        let value_field = result.value_field().unwrap();
        let typed_value_field = result
            .typed_value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        // Check specific outcomes for each row
        assert_eq!(result.len(), 6);

        // Row 0: 42 -> should shred successfully
        assert!(!result.is_null(0));
        assert!(value_field.is_null(0)); // value should be null when shredded
        assert!(!typed_value_field.is_null(0));
        assert_eq!(typed_value_field.value(0), 42);

        // Row 1: "hello" -> should fail to shred
        assert!(!result.is_null(1));
        assert!(!value_field.is_null(1)); // value should contain original
        assert!(typed_value_field.is_null(1)); // typed_value should be null
        assert_eq!(
            Variant::new(metadata_field.value(1), value_field.value(1)),
            Variant::from("hello")
        );

        // Row 2: 100 -> should shred successfully
        assert!(!result.is_null(2));
        assert!(value_field.is_null(2));
        assert_eq!(typed_value_field.value(2), 100);

        // Row 3: array null -> should be null in result
        assert!(result.is_null(3));

        // Row 4: Variant::Null -> should not shred (it's a null variant, not an integer)
        assert!(!result.is_null(4));
        assert!(!value_field.is_null(4)); // should contain Variant::Null
        assert_eq!(
            Variant::new(metadata_field.value(4), value_field.value(4)),
            Variant::Null
        );
        assert!(typed_value_field.is_null(4));

        // Row 5: 3i8 -> should shred successfully (int8->int64 conversion)
        assert!(!result.is_null(5));
        assert!(value_field.is_null(5)); // value should be null when shredded
        assert!(!typed_value_field.is_null(5));
        assert_eq!(typed_value_field.value(5), 3);
    }

    #[test]
    fn test_primitive_different_target_types() {
        let input = VariantArray::from_iter(vec![
            Variant::from(42i32),
            Variant::from(3.15f64),
            Variant::from("not_a_number"),
        ]);

        // Test Int32 target
        let result_int32 = shred_variant(&input, &DataType::Int32).unwrap();
        let typed_value_int32 = result_int32
            .typed_value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(typed_value_int32.value(0), 42);
        assert!(typed_value_int32.is_null(1)); // float doesn't convert to int32
        assert!(typed_value_int32.is_null(2)); // string doesn't convert to int32

        // Test Float64 target
        let result_float64 = shred_variant(&input, &DataType::Float64).unwrap();
        let typed_value_float64 = result_float64
            .typed_value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(typed_value_float64.value(0), 42.0); // int converts to float
        assert_eq!(typed_value_float64.value(1), 3.15);
        assert!(typed_value_float64.is_null(2)); // string doesn't convert
    }

    #[test]
    fn test_invalid_shredded_types_rejected() {
        let input = VariantArray::from_iter([Variant::from(42)]);

        let invalid_types = vec![
            DataType::UInt8,
            DataType::Float16,
            DataType::Decimal256(38, 10),
            DataType::Date64,
            DataType::Time32(TimeUnit::Second),
            DataType::Time64(TimeUnit::Nanosecond),
            DataType::Timestamp(TimeUnit::Millisecond, None),
            DataType::LargeBinary,
            DataType::LargeUtf8,
            DataType::FixedSizeBinary(17),
            DataType::Union(
                UnionFields::from_fields(vec![
                    Field::new("int_field", DataType::Int32, false),
                    Field::new("str_field", DataType::Utf8, true),
                ]),
                UnionMode::Dense,
            ),
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Int32, true),
                    ])),
                    false,
                )),
                false,
            ),
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            DataType::RunEndEncoded(
                Arc::new(Field::new("run_ends", DataType::Int32, false)),
                Arc::new(Field::new("values", DataType::Utf8, true)),
            ),
        ];

        for data_type in invalid_types {
            let err = shred_variant(&input, &data_type).unwrap_err();
            assert!(
                matches!(err, ArrowError::InvalidArgumentError(_)),
                "expected InvalidArgumentError for {:?}, got {:?}",
                data_type,
                err
            );
        }
    }

    #[test]
    fn test_array_shredding_as_list() {
        let input = build_variant_array(vec![
            // Row 0: List of ints should shred entirely into typed_value
            VariantRow::List(vec![
                VariantValue::from(1i64),
                VariantValue::from(2i64),
                VariantValue::from(3i64),
            ]),
            // Row 1: Contains incompatible types so values fall back
            VariantRow::List(vec![
                VariantValue::from(1i64),
                VariantValue::from("two"),
                VariantValue::from(Variant::Null),
            ]),
            // Row 2: Not a list -> entire row falls back
            VariantRow::Value(VariantValue::from("not a list")),
            // Row 3: Array-level null propagates
            VariantRow::Null,
            // Row 4: Empty list exercises zero-length offsets
            VariantRow::List(vec![]),
        ]);
        let list_schema = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let result = shred_variant(&input, &list_schema).unwrap();
        assert_eq!(result.len(), 5);

        assert_list_structure_and_elements::<Int64Type, i32>(
            &result,
            5,
            &[0, 3, 6, 6, 6, 6],
            &[Some(3), Some(3), None, None, Some(0)],
            &[
                None,
                None,
                Some(Variant::from("not a list")),
                Some(Variant::Null),
                None,
            ],
            (
                &[Some(1), Some(2), Some(3), Some(1), None, None],
                &[
                    None,
                    None,
                    None,
                    None,
                    Some(Variant::from("two")),
                    Some(Variant::Null),
                ],
            ),
        );
    }

    #[test]
    fn test_array_shredding_as_large_list() {
        let input = build_variant_array(vec![
            // Row 0: List of ints shreds to typed_value
            VariantRow::List(vec![VariantValue::from(1i64), VariantValue::from(2i64)]),
            // Row 1: Not a list -> entire row falls back
            VariantRow::Value(VariantValue::from("not a list")),
            // Row 2: Empty list
            VariantRow::List(vec![]),
        ]);
        let list_schema = DataType::LargeList(Arc::new(Field::new("item", DataType::Int64, true)));
        let result = shred_variant(&input, &list_schema).unwrap();
        assert_eq!(result.len(), 3);

        assert_list_structure_and_elements::<Int64Type, i64>(
            &result,
            3,
            &[0, 2, 2, 2],
            &[Some(2), None, Some(0)],
            &[None, Some(Variant::from("not a list")), None],
            (&[Some(1), Some(2)], &[None, None]),
        );
    }

    #[test]
    fn test_array_shredding_as_list_view() {
        let input = build_variant_array(vec![
            // Row 0: Standard list
            VariantRow::List(vec![
                VariantValue::from(1i64),
                VariantValue::from(2i64),
                VariantValue::from(3i64),
            ]),
            // Row 1: List with incompatible types -> element fallback
            VariantRow::List(vec![
                VariantValue::from(1i64),
                VariantValue::from("two"),
                VariantValue::from(Variant::Null),
            ]),
            // Row 2: Not a list -> top-level fallback
            VariantRow::Value(VariantValue::from("not a list")),
            // Row 3: Top-level Null
            VariantRow::Null,
            // Row 4: Empty list
            VariantRow::List(vec![]),
        ]);
        let list_schema = DataType::ListView(Arc::new(Field::new("item", DataType::Int64, true)));
        let result = shred_variant(&input, &list_schema).unwrap();
        assert_eq!(result.len(), 5);

        assert_list_structure_and_elements::<Int64Type, i32>(
            &result,
            5,
            &[0, 3, 6, 6, 6],
            &[Some(3), Some(3), None, None, Some(0)],
            &[
                None,
                None,
                Some(Variant::from("not a list")),
                Some(Variant::Null),
                None,
            ],
            (
                &[Some(1), Some(2), Some(3), Some(1), None, None],
                &[
                    None,
                    None,
                    None,
                    None,
                    Some(Variant::from("two")),
                    Some(Variant::Null),
                ],
            ),
        );
    }

    #[test]
    fn test_array_shredding_as_large_list_view() {
        let input = build_variant_array(vec![
            // Row 0: List of ints shreds to typed_value
            VariantRow::List(vec![VariantValue::from(1i64), VariantValue::from(2i64)]),
            // Row 1: Not a list -> entire row falls back
            VariantRow::Value(VariantValue::from("fallback")),
            // Row 2: Empty list
            VariantRow::List(vec![]),
        ]);
        let list_schema =
            DataType::LargeListView(Arc::new(Field::new("item", DataType::Int64, true)));
        let result = shred_variant(&input, &list_schema).unwrap();
        assert_eq!(result.len(), 3);

        assert_list_structure_and_elements::<Int64Type, i64>(
            &result,
            3,
            &[0, 2, 2],
            &[Some(2), None, Some(0)],
            &[None, Some(Variant::from("fallback")), None],
            (&[Some(1), Some(2)], &[None, None]),
        );
    }

    #[test]
    fn test_array_shredding_as_fixed_size_list() {
        let input = build_variant_array(vec![VariantRow::List(vec![
            VariantValue::from(1i64),
            VariantValue::from(2i64),
            VariantValue::from(3i64),
        ])]);
        let list_schema =
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int64, true)), 2);
        let err = shred_variant(&input, &list_schema).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Not yet implemented: Converting unshredded variant arrays to arrow fixed-size lists"
        );
    }

    #[test]
    fn test_array_shredding_with_array_elements() {
        let input = build_variant_array(vec![
            // Row 0: [[1, 2], [3, 4], []] - clean nested lists
            VariantRow::List(vec![
                VariantValue::List(vec![VariantValue::from(1i64), VariantValue::from(2i64)]),
                VariantValue::List(vec![VariantValue::from(3i64), VariantValue::from(4i64)]),
                VariantValue::List(vec![]),
            ]),
            // Row 1: [[5, "bad", null], "not a list inner", null] - inner fallbacks
            VariantRow::List(vec![
                VariantValue::List(vec![
                    VariantValue::from(5i64),
                    VariantValue::from("bad"),
                    VariantValue::from(Variant::Null),
                ]),
                VariantValue::from("not a list inner"),
                VariantValue::Null,
            ]),
            // Row 2: "not a list" - top-level fallback
            VariantRow::Value(VariantValue::from("not a list")),
            // Row 3: null row
            VariantRow::Null,
        ]);
        let inner_field = Arc::new(Field::new("item", DataType::Int64, true));
        let inner_list_schema = DataType::List(inner_field);
        let list_schema = DataType::List(Arc::new(Field::new(
            "item",
            inner_list_schema.clone(),
            true,
        )));
        let result = shred_variant(&input, &list_schema).unwrap();
        assert_eq!(result.len(), 4);

        let typed_value = result
            .typed_value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();

        assert_list_structure::<i32>(
            &result,
            4,
            &[0, 3, 6, 6, 6],
            &[Some(3), Some(3), None, None],
            &[
                None,
                None,
                Some(Variant::from("not a list")),
                Some(Variant::Null),
            ],
        );

        let outer_elements =
            ShreddedVariantFieldArray::try_new(typed_value.values().as_ref()).unwrap();
        assert_eq!(outer_elements.len(), 6);
        let outer_values = outer_elements
            .typed_value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let outer_fallbacks = outer_elements.value_field().unwrap();

        let outer_metadata = BinaryViewArray::from_iter_values(std::iter::repeat_n(
            EMPTY_VARIANT_METADATA_BYTES,
            outer_elements.len(),
        ));
        let outer_variant = VariantArray::from_parts(
            outer_metadata,
            Some(outer_fallbacks.clone()),
            Some(Arc::new(outer_values.clone())),
            None,
        );

        assert_list_structure_and_elements::<Int64Type, i32>(
            &outer_variant,
            outer_elements.len(),
            &[0, 2, 4, 4, 7, 7, 7],
            &[Some(2), Some(2), Some(0), Some(3), None, None],
            &[
                None,
                None,
                None,
                None,
                Some(Variant::from("not a list inner")),
                Some(Variant::Null),
            ],
            (
                &[Some(1), Some(2), Some(3), Some(4), Some(5), None, None],
                &[
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(Variant::from("bad")),
                    Some(Variant::Null),
                ],
            ),
        );
    }

    #[test]
    fn test_array_shredding_with_object_elements() {
        let input = build_variant_array(vec![
            // Row 0: [{"id": 1, "name": "Alice"}, {"id": null}] fully shards
            VariantRow::List(vec![
                VariantValue::Object(vec![
                    ("id", VariantValue::from(1i64)),
                    ("name", VariantValue::from("Alice")),
                ]),
                VariantValue::Object(vec![("id", VariantValue::from(Variant::Null))]),
            ]),
            // Row 1: "not a list" -> fallback
            VariantRow::Value(VariantValue::from("not a list")),
            // Row 2: Null row
            VariantRow::Null,
        ]);

        // Target schema is List<Struct<id:int64,name:utf8>>
        let object_fields = Fields::from(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]);
        let list_schema = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(object_fields),
            true,
        )));
        let result = shred_variant(&input, &list_schema).unwrap();
        assert_eq!(result.len(), 3);

        assert_list_structure::<i32>(
            &result,
            3,
            &[0, 2, 2, 2],
            &[Some(2), None, None],
            &[None, Some(Variant::from("not a list")), Some(Variant::Null)],
        );

        // Validate nested struct fields for each element
        let typed_value = result
            .typed_value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let element_array =
            ShreddedVariantFieldArray::try_new(typed_value.values().as_ref()).unwrap();
        assert_eq!(element_array.len(), 2);
        let element_objects = element_array
            .typed_value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .unwrap();

        // Id field [1, Variant::Null]
        let id_field =
            ShreddedVariantFieldArray::try_new(element_objects.column_by_name("id").unwrap())
                .unwrap();
        let id_values = id_field.value_field().unwrap();
        let id_typed_values = id_field
            .typed_value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(id_values.is_null(0));
        assert_eq!(id_typed_values.value(0), 1);
        // null is stored as Variant::Null in values
        assert!(id_values.is_valid(1));
        assert_eq!(
            Variant::new(EMPTY_VARIANT_METADATA_BYTES, id_values.value(1)),
            Variant::Null
        );
        assert!(id_typed_values.is_null(1));

        // Name field ["Alice", null]
        let name_field =
            ShreddedVariantFieldArray::try_new(element_objects.column_by_name("name").unwrap())
                .unwrap();
        let name_values = name_field.value_field().unwrap();
        let name_typed_values = name_field
            .typed_value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(name_values.is_null(0));
        assert_eq!(name_typed_values.value(0), "Alice");
        // No value provided, both value and typed_value are null
        assert!(name_values.is_null(1));
        assert!(name_typed_values.is_null(1));
    }

    #[test]
    fn test_object_shredding_comprehensive() {
        let input = build_variant_array(vec![
            // Row 0: Fully shredded object
            VariantRow::Object(vec![
                ("score", VariantValue::from(95.5f64)),
                ("age", VariantValue::from(30i64)),
            ]),
            // Row 1: Partially shredded object (extra email field)
            VariantRow::Object(vec![
                ("score", VariantValue::from(87.2f64)),
                ("age", VariantValue::from(25i64)),
                ("email", VariantValue::from("bob@example.com")),
            ]),
            // Row 2: Missing field (no score)
            VariantRow::Object(vec![("age", VariantValue::from(35i64))]),
            // Row 3: Type mismatch (score is string, age is string)
            VariantRow::Object(vec![
                ("score", VariantValue::from("ninety-five")),
                ("age", VariantValue::from("thirty")),
            ]),
            // Row 4: Non-object
            VariantRow::Value(VariantValue::from("not an object")),
            // Row 5: Empty object
            VariantRow::Object(vec![]),
            // Row 6: Null
            VariantRow::Null,
            // Row 7: Object with only "wrong" fields
            VariantRow::Object(vec![("foo", VariantValue::from(10))]),
            // Row 8: Object with one "right" and one "wrong" field
            VariantRow::Object(vec![
                ("score", VariantValue::from(66.67f64)),
                ("foo", VariantValue::from(10)),
            ]),
        ]);

        // Create target schema: struct<score: float64, age: int64>
        // Both types are supported for shredding
        let target_schema = ShreddedSchemaBuilder::default()
            .with_path("score", &DataType::Float64)
            .with_path("age", &DataType::Int64)
            .build();

        let result = shred_variant(&input, &target_schema).unwrap();

        // Verify structure
        assert!(result.value_field().is_some());
        assert!(result.typed_value_field().is_some());
        assert_eq!(result.len(), 9);

        let metadata = result.metadata_field();

        let value = result.value_field().unwrap();
        let typed_value = result
            .typed_value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .unwrap();

        // Extract score and age fields from typed_value struct
        let score_field =
            ShreddedVariantFieldArray::try_new(typed_value.column_by_name("score").unwrap())
                .unwrap();
        let age_field =
            ShreddedVariantFieldArray::try_new(typed_value.column_by_name("age").unwrap()).unwrap();

        let score_value = score_field
            .value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryViewArray>()
            .unwrap();
        let score_typed_value = score_field
            .typed_value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let age_value = age_field
            .value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryViewArray>()
            .unwrap();
        let age_typed_value = age_field
            .typed_value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        // Set up exhaustive checking of all shredded columns and their nulls/values
        struct ShreddedValue<'m, 'v, T> {
            value: Option<Variant<'m, 'v>>,
            typed_value: Option<T>,
        }
        struct ShreddedStruct<'m, 'v> {
            score: ShreddedValue<'m, 'v, f64>,
            age: ShreddedValue<'m, 'v, i64>,
        }
        fn get_value<'m, 'v>(
            i: usize,
            metadata: &'m BinaryViewArray,
            value: &'v BinaryViewArray,
        ) -> Variant<'m, 'v> {
            Variant::new(metadata.value(i), value.value(i))
        }
        let expect = |i, expected_result: Option<ShreddedValue<ShreddedStruct>>| {
            match expected_result {
                Some(ShreddedValue {
                    value: expected_value,
                    typed_value: expected_typed_value,
                }) => {
                    assert!(result.is_valid(i));
                    match expected_value {
                        Some(expected_value) => {
                            assert!(value.is_valid(i));
                            assert_eq!(expected_value, get_value(i, metadata, value));
                        }
                        None => {
                            assert!(value.is_null(i));
                        }
                    }
                    match expected_typed_value {
                        Some(ShreddedStruct {
                            score: expected_score,
                            age: expected_age,
                        }) => {
                            assert!(typed_value.is_valid(i));
                            assert!(score_field.is_valid(i)); // non-nullable
                            assert!(age_field.is_valid(i)); // non-nullable
                            match expected_score.value {
                                Some(expected_score_value) => {
                                    assert!(score_value.is_valid(i));
                                    assert_eq!(
                                        expected_score_value,
                                        get_value(i, metadata, score_value)
                                    );
                                }
                                None => {
                                    assert!(score_value.is_null(i));
                                }
                            }
                            match expected_score.typed_value {
                                Some(expected_score) => {
                                    assert!(score_typed_value.is_valid(i));
                                    assert_eq!(expected_score, score_typed_value.value(i));
                                }
                                None => {
                                    assert!(score_typed_value.is_null(i));
                                }
                            }
                            match expected_age.value {
                                Some(expected_age_value) => {
                                    assert!(age_value.is_valid(i));
                                    assert_eq!(
                                        expected_age_value,
                                        get_value(i, metadata, age_value)
                                    );
                                }
                                None => {
                                    assert!(age_value.is_null(i));
                                }
                            }
                            match expected_age.typed_value {
                                Some(expected_age) => {
                                    assert!(age_typed_value.is_valid(i));
                                    assert_eq!(expected_age, age_typed_value.value(i));
                                }
                                None => {
                                    assert!(age_typed_value.is_null(i));
                                }
                            }
                        }
                        None => {
                            assert!(typed_value.is_null(i));
                        }
                    }
                }
                None => {
                    assert!(result.is_null(i));
                }
            };
        };

        // Row 0: Fully shredded - both fields shred successfully
        expect(
            0,
            Some(ShreddedValue {
                value: None,
                typed_value: Some(ShreddedStruct {
                    score: ShreddedValue {
                        value: None,
                        typed_value: Some(95.5),
                    },
                    age: ShreddedValue {
                        value: None,
                        typed_value: Some(30),
                    },
                }),
            }),
        );

        // Row 1: Partially shredded - value contains extra email field
        let mut builder = VariantBuilder::new();
        builder
            .new_object()
            .with_field("email", "bob@example.com")
            .finish();
        let (m, v) = builder.finish();
        let expected_value = Variant::new(&m, &v);

        expect(
            1,
            Some(ShreddedValue {
                value: Some(expected_value),
                typed_value: Some(ShreddedStruct {
                    score: ShreddedValue {
                        value: None,
                        typed_value: Some(87.2),
                    },
                    age: ShreddedValue {
                        value: None,
                        typed_value: Some(25),
                    },
                }),
            }),
        );

        // Row 2: Fully shredded -- missing score field
        expect(
            2,
            Some(ShreddedValue {
                value: None,
                typed_value: Some(ShreddedStruct {
                    score: ShreddedValue {
                        value: None,
                        typed_value: None,
                    },
                    age: ShreddedValue {
                        value: None,
                        typed_value: Some(35),
                    },
                }),
            }),
        );

        // Row 3: Type mismatches - both score and age are strings
        expect(
            3,
            Some(ShreddedValue {
                value: None,
                typed_value: Some(ShreddedStruct {
                    score: ShreddedValue {
                        value: Some(Variant::from("ninety-five")),
                        typed_value: None,
                    },
                    age: ShreddedValue {
                        value: Some(Variant::from("thirty")),
                        typed_value: None,
                    },
                }),
            }),
        );

        // Row 4: Non-object - falls back to value field
        expect(
            4,
            Some(ShreddedValue {
                value: Some(Variant::from("not an object")),
                typed_value: None,
            }),
        );

        // Row 5: Empty object
        expect(
            5,
            Some(ShreddedValue {
                value: None,
                typed_value: Some(ShreddedStruct {
                    score: ShreddedValue {
                        value: None,
                        typed_value: None,
                    },
                    age: ShreddedValue {
                        value: None,
                        typed_value: None,
                    },
                }),
            }),
        );

        // Row 6: Null
        expect(6, None);

        // Helper to correctly create a variant object using a row's existing metadata
        let object_with_foo_field = |i| {
            use parquet_variant::{ParentState, ValueBuilder, VariantMetadata};
            let metadata = VariantMetadata::new(metadata.value(i));
            let mut metadata_builder = ReadOnlyMetadataBuilder::new(&metadata);
            let mut value_builder = ValueBuilder::new();
            let state = ParentState::variant(&mut value_builder, &mut metadata_builder);
            ObjectBuilder::new(state, false)
                .with_field("foo", 10)
                .finish();
            (metadata, value_builder.into_inner())
        };

        // Row 7: Object with only a "wrong" field
        let (m, v) = object_with_foo_field(7);
        expect(
            7,
            Some(ShreddedValue {
                value: Some(Variant::new_with_metadata(m, &v)),
                typed_value: Some(ShreddedStruct {
                    score: ShreddedValue {
                        value: None,
                        typed_value: None,
                    },
                    age: ShreddedValue {
                        value: None,
                        typed_value: None,
                    },
                }),
            }),
        );

        // Row 8: Object with one "wrong" and one "right" field
        let (m, v) = object_with_foo_field(8);
        expect(
            8,
            Some(ShreddedValue {
                value: Some(Variant::new_with_metadata(m, &v)),
                typed_value: Some(ShreddedStruct {
                    score: ShreddedValue {
                        value: None,
                        typed_value: Some(66.67),
                    },
                    age: ShreddedValue {
                        value: None,
                        typed_value: None,
                    },
                }),
            }),
        );
    }

    #[test]
    fn test_object_shredding_with_array_field() {
        let input = build_variant_array(vec![
            // Row 0: Object with well-typed scores list
            VariantRow::Object(vec![(
                "scores",
                VariantValue::List(vec![VariantValue::from(10i64), VariantValue::from(20i64)]),
            )]),
            // Row 1: Object whose scores list contains incompatible type
            VariantRow::Object(vec![(
                "scores",
                VariantValue::List(vec![
                    VariantValue::from("oops"),
                    VariantValue::from(Variant::Null),
                ]),
            )]),
            // Row 2: Object missing the scores field entirely
            VariantRow::Object(vec![]),
            // Row 3: Non-object fallback
            VariantRow::Value(VariantValue::from("not an object")),
            // Row 4: Top-level Null
            VariantRow::Null,
        ]);
        let list_field = Arc::new(Field::new("item", DataType::Int64, true));
        let inner_list_schema = DataType::List(list_field);
        let schema = DataType::Struct(Fields::from(vec![Field::new(
            "scores",
            inner_list_schema.clone(),
            true,
        )]));

        let result = shred_variant(&input, &schema).unwrap();
        assert_eq!(result.len(), 5);

        // Access base value/typed_value columns
        let value_field = result.value_field().unwrap();
        let typed_struct = result
            .typed_value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .unwrap();

        // Validate base value fallbacks for non-object rows
        assert!(value_field.is_null(0));
        assert!(value_field.is_null(1));
        assert!(value_field.is_null(2));
        assert!(value_field.is_valid(3));
        assert_eq!(
            Variant::new(result.metadata_field().value(3), value_field.value(3)),
            Variant::from("not an object")
        );
        assert!(value_field.is_null(4));

        // Typed struct should only be null for the fallback row
        assert!(typed_struct.is_valid(0));
        assert!(typed_struct.is_valid(1));
        assert!(typed_struct.is_valid(2));
        assert!(typed_struct.is_null(3));
        assert!(typed_struct.is_null(4));

        // Drill into the scores field on the typed struct
        let scores_field =
            ShreddedVariantFieldArray::try_new(typed_struct.column_by_name("scores").unwrap())
                .unwrap();
        assert_list_structure_and_elements::<Int64Type, i32>(
            &VariantArray::from_parts(
                BinaryViewArray::from_iter_values(std::iter::repeat_n(
                    EMPTY_VARIANT_METADATA_BYTES,
                    scores_field.len(),
                )),
                Some(scores_field.value_field().unwrap().clone()),
                Some(scores_field.typed_value_field().unwrap().clone()),
                None,
            ),
            scores_field.len(),
            &[0i32, 2, 4, 4, 4, 4],
            &[Some(2), Some(2), None, None, None],
            &[
                None,
                None,
                Some(Variant::Null),
                Some(Variant::Null),
                Some(Variant::Null),
            ],
            (
                &[Some(10), Some(20), None, None],
                &[None, None, Some(Variant::from("oops")), Some(Variant::Null)],
            ),
        );
    }

    #[test]
    fn test_object_different_schemas() {
        // Create object with multiple fields
        let input = build_variant_array(vec![VariantRow::Object(vec![
            ("id", VariantValue::from(123i32)),
            ("age", VariantValue::from(25i64)),
            ("score", VariantValue::from(95.5f64)),
        ])]);

        // Test with schema containing only id field
        let schema1 = ShreddedSchemaBuilder::default()
            .with_path("id", &DataType::Int32)
            .build();
        let result1 = shred_variant(&input, &schema1).unwrap();
        let value_field1 = result1.value_field().unwrap();
        assert!(!value_field1.is_null(0)); // should contain {"age": 25, "score": 95.5}

        // Test with schema containing id and age fields
        let schema2 = ShreddedSchemaBuilder::default()
            .with_path("id", &DataType::Int32)
            .with_path("age", &DataType::Int64)
            .build();
        let result2 = shred_variant(&input, &schema2).unwrap();
        let value_field2 = result2.value_field().unwrap();
        assert!(!value_field2.is_null(0)); // should contain {"score": 95.5}

        // Test with schema containing all fields
        let schema3 = ShreddedSchemaBuilder::default()
            .with_path("id", &DataType::Int32)
            .with_path("age", &DataType::Int64)
            .with_path("score", &DataType::Float64)
            .build();
        let result3 = shred_variant(&input, &schema3).unwrap();
        let value_field3 = result3.value_field().unwrap();
        assert!(value_field3.is_null(0)); // fully shredded, no remaining fields
    }

    #[test]
    fn test_uuid_shredding_in_objects() {
        let mock_uuid_1 = Uuid::new_v4();
        let mock_uuid_2 = Uuid::new_v4();
        let mock_uuid_3 = Uuid::new_v4();

        let input = build_variant_array(vec![
            // Row 0: Fully shredded object with both UUID fields
            VariantRow::Object(vec![
                ("id", VariantValue::from(mock_uuid_1)),
                ("session_id", VariantValue::from(mock_uuid_2)),
            ]),
            // Row 1: Partially shredded object - UUID fields plus extra field
            VariantRow::Object(vec![
                ("id", VariantValue::from(mock_uuid_2)),
                ("session_id", VariantValue::from(mock_uuid_3)),
                ("name", VariantValue::from("test_user")),
            ]),
            // Row 2: Missing UUID field (no session_id)
            VariantRow::Object(vec![("id", VariantValue::from(mock_uuid_1))]),
            // Row 3: Type mismatch - id is UUID but session_id is a string
            VariantRow::Object(vec![
                ("id", VariantValue::from(mock_uuid_3)),
                ("session_id", VariantValue::from("not-a-uuid")),
            ]),
            // Row 4: Object with non-UUID value in id field
            VariantRow::Object(vec![
                ("id", VariantValue::from(12345i64)),
                ("session_id", VariantValue::from(mock_uuid_1)),
            ]),
            // Row 5: Null
            VariantRow::Null,
        ]);

        let target_schema = ShreddedSchemaBuilder::default()
            .with_path("id", DataType::FixedSizeBinary(16))
            .with_path("session_id", DataType::FixedSizeBinary(16))
            .build();

        let result = shred_variant(&input, &target_schema).unwrap();

        assert!(result.value_field().is_some());
        assert!(result.typed_value_field().is_some());
        assert_eq!(result.len(), 6);

        let metadata = result.metadata_field();
        let value = result.value_field().unwrap();
        let typed_value = result
            .typed_value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .unwrap();

        // Extract id and session_id fields from typed_value struct
        let id_field =
            ShreddedVariantFieldArray::try_new(typed_value.column_by_name("id").unwrap()).unwrap();
        let session_id_field =
            ShreddedVariantFieldArray::try_new(typed_value.column_by_name("session_id").unwrap())
                .unwrap();

        let id_value = id_field
            .value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryViewArray>()
            .unwrap();
        let id_typed_value = id_field
            .typed_value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let session_id_value = session_id_field
            .value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryViewArray>()
            .unwrap();
        let session_id_typed_value = session_id_field
            .typed_value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();

        // Row 0: Fully shredded - both UUID fields shred successfully
        assert!(result.is_valid(0));

        assert!(value.is_null(0)); // fully shredded, no remaining fields
        assert!(id_value.is_null(0));
        assert!(session_id_value.is_null(0));

        assert!(typed_value.is_valid(0));
        assert!(id_typed_value.is_valid(0));
        assert!(session_id_typed_value.is_valid(0));

        assert_eq!(id_typed_value.value(0), mock_uuid_1.as_bytes());
        assert_eq!(session_id_typed_value.value(0), mock_uuid_2.as_bytes());

        // Row 1: Partially shredded - value contains extra name field
        assert!(result.is_valid(1));

        assert!(value.is_valid(1)); // contains unshredded "name" field
        assert!(typed_value.is_valid(1));

        assert!(id_value.is_null(1));
        assert!(id_typed_value.is_valid(1));
        assert_eq!(id_typed_value.value(1), mock_uuid_2.as_bytes());

        assert!(session_id_value.is_null(1));
        assert!(session_id_typed_value.is_valid(1));
        assert_eq!(session_id_typed_value.value(1), mock_uuid_3.as_bytes());

        // Verify the value field contains the name field
        let row_1_variant = Variant::new(metadata.value(1), value.value(1));
        let Variant::Object(obj) = row_1_variant else {
            panic!("Expected object");
        };

        assert_eq!(obj.get("name"), Some(Variant::from("test_user")));

        // Row 2: Missing session_id field
        assert!(result.is_valid(2));

        assert!(value.is_null(2)); // fully shredded, no extra fields
        assert!(typed_value.is_valid(2));

        assert!(id_value.is_null(2));
        assert!(id_typed_value.is_valid(2));
        assert_eq!(id_typed_value.value(2), mock_uuid_1.as_bytes());

        assert!(session_id_value.is_null(2));
        assert!(session_id_typed_value.is_null(2)); // missing field

        // Row 3: Type mismatch - session_id is a string, not UUID
        assert!(result.is_valid(3));

        assert!(value.is_null(3)); // no extra fields
        assert!(typed_value.is_valid(3));

        assert!(id_value.is_null(3));
        assert!(id_typed_value.is_valid(3));
        assert_eq!(id_typed_value.value(3), mock_uuid_3.as_bytes());

        assert!(session_id_value.is_valid(3)); // type mismatch, stored in value
        assert!(session_id_typed_value.is_null(3));
        let session_id_variant = Variant::new(metadata.value(3), session_id_value.value(3));
        assert_eq!(session_id_variant, Variant::from("not-a-uuid"));

        // Row 4: Type mismatch - id is int64, not UUID
        assert!(result.is_valid(4));

        assert!(value.is_null(4)); // no extra fields
        assert!(typed_value.is_valid(4));

        assert!(id_value.is_valid(4)); // type mismatch, stored in value
        assert!(id_typed_value.is_null(4));
        let id_variant = Variant::new(metadata.value(4), id_value.value(4));
        assert_eq!(id_variant, Variant::from(12345i64));

        assert!(session_id_value.is_null(4));
        assert!(session_id_typed_value.is_valid(4));
        assert_eq!(session_id_typed_value.value(4), mock_uuid_1.as_bytes());

        // Row 5: Null
        assert!(result.is_null(5));
    }

    #[test]
    fn test_spec_compliance() {
        let input = VariantArray::from_iter(vec![Variant::from(42i64), Variant::from("hello")]);

        let result = shred_variant(&input, &DataType::Int64).unwrap();

        // Test field access by name (not position)
        let inner_struct = result.inner();
        assert!(inner_struct.column_by_name("metadata").is_some());
        assert!(inner_struct.column_by_name("value").is_some());
        assert!(inner_struct.column_by_name("typed_value").is_some());

        // Test metadata preservation
        assert_eq!(result.metadata_field().len(), input.metadata_field().len());
        // The metadata should be the same reference (cheap clone)
        // Note: BinaryViewArray doesn't have a .values() method, so we compare the arrays directly
        assert_eq!(result.metadata_field().len(), input.metadata_field().len());

        // Test output structure correctness
        assert_eq!(result.len(), input.len());
        assert!(result.value_field().is_some());
        assert!(result.typed_value_field().is_some());

        // For primitive shredding, verify that value and typed_value are never both non-null
        // (This rule applies to primitives; for objects, both can be non-null for partial shredding)
        let value_field = result.value_field().unwrap();
        let typed_value_field = result
            .typed_value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        for i in 0..result.len() {
            if !result.is_null(i) {
                let value_is_null = value_field.is_null(i);
                let typed_value_is_null = typed_value_field.is_null(i);
                // For primitive shredding, at least one should be null
                assert!(
                    value_is_null || typed_value_is_null,
                    "Row {}: both value and typed_value are non-null for primitive shredding",
                    i
                );
            }
        }
    }

    #[test]
    fn test_variant_schema_builder_simple() {
        let shredding_type = ShreddedSchemaBuilder::default()
            .with_path("a", &DataType::Int64)
            .with_path("b", &DataType::Float64)
            .build();

        assert_eq!(
            shredding_type,
            DataType::Struct(Fields::from(vec![
                Field::new("a", DataType::Int64, true),
                Field::new("b", DataType::Float64, true),
            ]))
        );
    }

    #[test]
    fn test_variant_schema_builder_nested() {
        let shredding_type = ShreddedSchemaBuilder::default()
            .with_path("a", &DataType::Int64)
            .with_path("b.c", &DataType::Utf8)
            .with_path("b.d", &DataType::Float64)
            .build();

        assert_eq!(
            shredding_type,
            DataType::Struct(Fields::from(vec![
                Field::new("a", DataType::Int64, true),
                Field::new(
                    "b",
                    DataType::Struct(Fields::from(vec![
                        Field::new("c", DataType::Utf8, true),
                        Field::new("d", DataType::Float64, true),
                    ])),
                    true
                ),
            ]))
        );
    }

    #[test]
    fn test_variant_schema_builder_with_path_variant_path_arg() {
        let path = VariantPath::from_iter([VariantPathElement::from("a.b")]);
        let shredding_type = ShreddedSchemaBuilder::default()
            .with_path(path, &DataType::Int64)
            .build();

        match shredding_type {
            DataType::Struct(fields) => {
                assert_eq!(fields.len(), 1);
                assert_eq!(fields[0].name(), "a.b");
                assert_eq!(fields[0].data_type(), &DataType::Int64);
            }
            _ => panic!("expected struct data type"),
        }
    }

    #[test]
    fn test_variant_schema_builder_custom_nullability() {
        let shredding_type = ShreddedSchemaBuilder::default()
            .with_path(
                "foo",
                Arc::new(Field::new("should_be_renamed", DataType::Utf8, false)),
            )
            .with_path("bar", (&DataType::Int64, false))
            .build();

        let DataType::Struct(fields) = shredding_type else {
            panic!("expected struct data type");
        };

        let foo = fields.iter().find(|f| f.name() == "foo").unwrap();
        assert_eq!(foo.data_type(), &DataType::Utf8);
        assert!(!foo.is_nullable());

        let bar = fields.iter().find(|f| f.name() == "bar").unwrap();
        assert_eq!(bar.data_type(), &DataType::Int64);
        assert!(!bar.is_nullable());
    }

    #[test]
    fn test_variant_schema_builder_with_shred_variant() {
        let input = build_variant_array(vec![
            VariantRow::Object(vec![
                ("time", VariantValue::from(1234567890i64)),
                ("hostname", VariantValue::from("server1")),
                ("extra", VariantValue::from(42)),
            ]),
            VariantRow::Object(vec![
                ("time", VariantValue::from(9876543210i64)),
                ("hostname", VariantValue::from("server2")),
            ]),
            VariantRow::Null,
        ]);

        let shredding_type = ShreddedSchemaBuilder::default()
            .with_path("time", &DataType::Int64)
            .with_path("hostname", &DataType::Utf8)
            .build();

        let result = shred_variant(&input, &shredding_type).unwrap();

        assert_eq!(
            result.data_type(),
            &DataType::Struct(Fields::from(vec![
                Field::new("metadata", DataType::BinaryView, false),
                Field::new("value", DataType::BinaryView, true),
                Field::new(
                    "typed_value",
                    DataType::Struct(Fields::from(vec![
                        Field::new(
                            "hostname",
                            DataType::Struct(Fields::from(vec![
                                Field::new("value", DataType::BinaryView, true),
                                Field::new("typed_value", DataType::Utf8, true),
                            ])),
                            false,
                        ),
                        Field::new(
                            "time",
                            DataType::Struct(Fields::from(vec![
                                Field::new("value", DataType::BinaryView, true),
                                Field::new("typed_value", DataType::Int64, true),
                            ])),
                            false,
                        ),
                    ])),
                    true,
                ),
            ]))
        );

        assert_eq!(result.len(), 3);
        assert!(result.typed_value_field().is_some());

        let typed_value = result
            .typed_value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .unwrap();

        let time_field =
            ShreddedVariantFieldArray::try_new(typed_value.column_by_name("time").unwrap())
                .unwrap();
        let hostname_field =
            ShreddedVariantFieldArray::try_new(typed_value.column_by_name("hostname").unwrap())
                .unwrap();

        let time_typed = time_field
            .typed_value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let hostname_typed = hostname_field
            .typed_value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();

        // Row 0
        assert!(!result.is_null(0));
        assert_eq!(time_typed.value(0), 1234567890);
        assert_eq!(hostname_typed.value(0), "server1");

        // Row 1
        assert!(!result.is_null(1));
        assert_eq!(time_typed.value(1), 9876543210);
        assert_eq!(hostname_typed.value(1), "server2");

        // Row 2
        assert!(result.is_null(2));
    }

    #[test]
    fn test_variant_schema_builder_conflicting_path() {
        let shredding_type = ShreddedSchemaBuilder::default()
            .with_path("a", &DataType::Int64)
            .with_path("a", &DataType::Float64)
            .build();

        assert_eq!(
            shredding_type,
            DataType::Struct(Fields::from(
                vec![Field::new("a", DataType::Float64, true),]
            ))
        );
    }

    #[test]
    fn test_variant_schema_builder_root_path() {
        let path = VariantPath::new(vec![]);
        let shredding_type = ShreddedSchemaBuilder::default()
            .with_path(path, &DataType::Int64)
            .build();

        assert_eq!(shredding_type, DataType::Int64);
    }

    #[test]
    fn test_variant_schema_builder_empty_path() {
        let shredding_type = ShreddedSchemaBuilder::default()
            .with_path("", &DataType::Int64)
            .build();

        assert_eq!(shredding_type, DataType::Int64);
    }

    #[test]
    fn test_variant_schema_builder_default() {
        let shredding_type = ShreddedSchemaBuilder::default().build();
        assert_eq!(shredding_type, DataType::Null);
    }
}
