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

use arrow::array::{ArrayRef, AsArray as _, NullBufferBuilder};
use arrow::datatypes;
use arrow::datatypes::{ArrowPrimitiveType, FieldRef};
use arrow::error::{ArrowError, Result};
use parquet_variant::{Variant, VariantObject, VariantPath};

use std::sync::Arc;

#[allow(unused)]
pub(crate) fn make_shredding_row_builder(
    //metadata: &BinaryViewArray,
    path: VariantPath<'_>,
    data_type: Option<&datatypes::DataType>,
) -> Result<Box<dyn VariantShreddingRowBuilder>> {
    todo!() // wire it all up!
}

/// Builder for shredding variant values into strongly typed Arrow arrays.
///
/// Useful for variant_get kernels that need to extract specific paths from variant values, possibly
/// with casting of leaf values to specific types.
#[allow(unused)]
pub(crate) trait VariantShreddingRowBuilder {
    fn append_null(&mut self) -> Result<()>;

    fn append_value(&mut self, value: &Variant<'_, '_>) -> Result<bool>;

    fn finish(&mut self) -> Result<ArrayRef>;
}

/// A thin wrapper whose only job is to extract a specific path from a variant value and pass the
/// result to a nested builder.
#[allow(unused)]
struct VariantPathRowBuilder<'a, T: VariantShreddingRowBuilder> {
    builder: T,
    path: VariantPath<'a>,
}

impl<T: VariantShreddingRowBuilder> VariantShreddingRowBuilder for VariantPathRowBuilder<'_, T> {
    fn append_null(&mut self) -> Result<()> {
        self.builder.append_null()
    }

    fn append_value(&mut self, value: &Variant<'_, '_>) -> Result<bool> {
        if let Some(v) = value.get_path(&self.path) {
            self.builder.append_value(&v)
        } else {
            self.builder.append_null()?;
            Ok(false)
        }
    }
    fn finish(&mut self) -> Result<ArrayRef> {
        self.builder.finish()
    }
}

/// Helper trait for converting `Variant` values to arrow primitive values.
#[allow(unused)]
trait VariantAsPrimitive<T: ArrowPrimitiveType> {
    fn as_primitive(&self) -> Option<T::Native>;
}
impl VariantAsPrimitive<datatypes::Int32Type> for Variant<'_, '_> {
    fn as_primitive(&self) -> Option<i32> {
        self.as_int32()
    }
}
impl VariantAsPrimitive<datatypes::Float64Type> for Variant<'_, '_> {
    fn as_primitive(&self) -> Option<f64> {
        self.as_f64()
    }
}

/// Builder for shredding variant values to primitive values
#[allow(unused)]
struct PrimitiveVariantShreddingRowBuilder<T: ArrowPrimitiveType> {
    builder: arrow::array::PrimitiveBuilder<T>,
}

impl<T> VariantShreddingRowBuilder for PrimitiveVariantShreddingRowBuilder<T>
where
    T: ArrowPrimitiveType,
    for<'m, 'v> Variant<'m, 'v>: VariantAsPrimitive<T>,
{
    fn append_null(&mut self) -> Result<()> {
        self.builder.append_null();
        Ok(())
    }

    fn append_value(&mut self, value: &Variant<'_, '_>) -> Result<bool> {
        if let Some(v) = value.as_primitive() {
            self.builder.append_value(v);
            Ok(true)
        } else {
            self.builder.append_null(); // TODO: handle casting failure
            Ok(false)
        }
    }

    fn finish(&mut self) -> Result<ArrayRef> {
        Ok(Arc::new(self.builder.finish()))
    }
}

/// Builder for appending raw binary variant values to a BinaryViewArray. It copies the bytes
/// as-is, without any decoding.
#[allow(unused)]
struct BinaryVariantRowBuilder {
    nulls: NullBufferBuilder,
}

impl VariantShreddingRowBuilder for BinaryVariantRowBuilder {
    fn append_null(&mut self) -> Result<()> {
        self.nulls.append_null();
        Ok(())
    }
    fn append_value(&mut self, _value: &Variant<'_, '_>) -> Result<bool> {
        // We need a way to convert a Variant directly to bytes. In particular, we want to just copy
        // across the underlying value byte slice of a `Variant::Object` or `Variant::List`, without
        // any interaction with a `VariantMetadata` (because we will just reuse the existing one).
        //
        // One could _probably_ emulate this with parquet_variant::VariantBuilder, but it would do a
        // lot of unnecessary work and would also create a new metadata column we don't need.
        todo!()
    }

    fn finish(&mut self) -> Result<ArrayRef> {
        // What `finish` does will depend strongly on how `append_value` ends up working. But
        // ultimately we'll create and return a `VariantArray` instance.
        todo!()
    }
}

/// Builder that extracts a struct. Casting failures produce NULL or error according to options.
#[allow(unused)]
struct StructVariantShreddingRowBuilder {
    nulls: NullBufferBuilder,
    field_builders: Vec<(FieldRef, Box<dyn VariantShreddingRowBuilder>)>,
}

impl VariantShreddingRowBuilder for StructVariantShreddingRowBuilder {
    fn append_null(&mut self) -> Result<()> {
        for (_, builder) in &mut self.field_builders {
            builder.append_null()?;
        }
        self.nulls.append_null();
        Ok(())
    }

    fn append_value(&mut self, value: &Variant<'_, '_>) -> Result<bool> {
        // Casting failure if it's not even an object.
        let Variant::Object(value) = value else {
            // TODO: handle casting failure
            self.append_null()?;
            return Ok(false);
        };

        // Process each field. If the field is missing, it becomes NULL. If the field is present,
        // the child builder handles it from there, and a failed cast could produce NULL or error.
        //
        // TODO: This loop costs `O(m lg n)` where `m` is the number of fields in this builder and
        // `n` is the number of fields in the variant object we're probing. Given that `m` and `n`
        // could both be large -- indepentently of each other -- we should consider doing something
        // more clever that bounds the cost to O(m + n).
        for (field, builder) in &mut self.field_builders {
            match value.get(field.name()) {
                None => builder.append_null()?,
                Some(v) => {
                    builder.append_value(&v)?;
                }
            }
        }
        self.nulls.append_non_null();
        Ok(true)
    }

    fn finish(&mut self) -> Result<ArrayRef> {
        let mut fields = Vec::with_capacity(self.field_builders.len());
        let mut arrays = Vec::with_capacity(self.field_builders.len());
        for (field, mut builder) in std::mem::take(&mut self.field_builders) {
            fields.push(field);
            arrays.push(builder.finish()?);
        }
        Ok(Arc::new(arrow::array::StructArray::try_new(
            fields.into(),
            arrays,
            self.nulls.finish(),
        )?))
    }
}

/// Used for actual shredding of binary variant values into shredded variant values
#[allow(unused)]
struct ShreddedVariantRowBuilder {
    metadata: arrow::array::BinaryViewArray,
    nulls: NullBufferBuilder,
    value_builder: BinaryVariantRowBuilder,
    typed_value_builder: Box<dyn VariantShreddingRowBuilder>,
}

impl VariantShreddingRowBuilder for ShreddedVariantRowBuilder {
    fn append_null(&mut self) -> Result<()> {
        self.nulls.append_null();
        self.value_builder.append_value(&Variant::Null)?;
        self.typed_value_builder.append_null()
    }

    fn append_value(&mut self, value: &Variant<'_, '_>) -> Result<bool> {
        self.nulls.append_non_null();
        if self.typed_value_builder.append_value(value)? {
            // spec: (value: NULL, typed_value: non-NULL => value is present and shredded)
            self.value_builder.append_null()?;
        } else {
            // spec: (value: non-NULL, typed_value: NULL => value is present and unshredded)
            self.value_builder.append_value(value)?;
        }
        Ok(true)
    }

    fn finish(&mut self) -> Result<ArrayRef> {
        let value = self.value_builder.finish()?;
        let Some(value) = value.as_byte_view_opt() else {
            return Err(ArrowError::InvalidArgumentError(
                "Invalid VariantArray: value builder must produce a BinaryViewArray".to_string(),
            ));
        };
        Ok(Arc::new(crate::VariantArray::from_parts(
            self.metadata.clone(),
            Some(value.clone()), // TODO: How to consume an ArrayRef directly?
            Some(self.typed_value_builder.finish()?),
            self.nulls.finish(),
        )))
    }
}

/// Like VariantShreddingRowBuilder, but for (partially shredded) structs which need special
/// handling on a per-field basis.
#[allow(unused)]
struct VariantShreddingStructRowBuilder {
    metadata: arrow::array::BinaryViewArray,
    nulls: NullBufferBuilder,
    value_builder: BinaryVariantRowBuilder,
    typed_value_field_builders: Vec<(FieldRef, Box<dyn VariantShreddingRowBuilder>)>,
    typed_value_nulls: NullBufferBuilder,
}

#[allow(unused)]
impl VariantShreddingStructRowBuilder {
    fn append_null_typed_value(&mut self) -> Result<()> {
        for (_, builder) in &mut self.typed_value_field_builders {
            builder.append_null()?;
        }
        self.typed_value_nulls.append_null();
        Ok(())
    }

    // Co-iterate over all fields of both the input value object and the target `typed_value`
    // schema, effectively performing full outer merge join by field name.
    //
    // NOTE: At most one of the two options can be empty.
    fn merge_join_fields<'a>(
        &'a mut self,
        _value: &'a VariantObject<'a, 'a>,
    ) -> impl Iterator<
        Item = (
            Option<&'a Variant<'a, 'a>>,
            &'a str,
            Option<&'a mut dyn VariantShreddingRowBuilder>,
        ),
    > {
        std::iter::empty()
    }
}

impl VariantShreddingRowBuilder for VariantShreddingStructRowBuilder {
    fn append_null(&mut self) -> Result<()> {
        self.append_null_typed_value()?;
        self.value_builder.append_null()?;
        self.nulls.append_null();
        Ok(())
    }

    #[allow(unused_assignments)]
    fn append_value(&mut self, value: &Variant<'_, '_>) -> Result<bool> {
        // If it's not even an object, just append the whole thing to the child value builder.
        let Variant::Object(value) = value else {
            self.append_null_typed_value()?;
            self.value_builder.append_value(value)?;
            self.nulls.append_non_null();
            return Ok(false);
        };

        let mut found_value_field = false;
        for (input_value_field, _field_name, typed_value_builder) in self.merge_join_fields(value) {
            match (input_value_field, typed_value_builder) {
                (Some(input_value_field), Some(typed_value_builder)) => {
                    // The field is part of the shredding schema, so the output `value` object must
                    // NOT include it. The child builder handles any field shredding failure.
                    typed_value_builder.append_value(input_value_field)?;
                }
                (Some(_input_value_field), None) => {
                    // The field is not part of the shredding schema, so copy the field's value
                    // bytes over unchanged to the output `value` object.
                    found_value_field = true;
                    todo!()
                }
                (None, Some(typed_value_builder)) => {
                    // The field is part of the shredding schema, but the input does not have it.
                    typed_value_builder.append_null()?;
                }
                // NOTE: Every row of an outer join must include at least one of left or right side.
                (None, None) => unreachable!(),
            }
        }

        // Finish the value builder, if non-empty.
        if found_value_field {
            #[allow(unreachable_code)]
            self.value_builder.append_value(todo!())?;
        } else {
            self.value_builder.append_null()?;
        }

        // The typed_value row is valid even if all its fields are NULL.
        self.typed_value_nulls.append_non_null();
        Ok(true)
    }

    fn finish(&mut self) -> Result<ArrayRef> {
        let mut fields = Vec::with_capacity(self.typed_value_field_builders.len());
        let mut arrays = Vec::with_capacity(self.typed_value_field_builders.len());
        for (field, mut builder) in std::mem::take(&mut self.typed_value_field_builders) {
            fields.push(field);
            arrays.push(builder.finish()?);
        }
        let typed_value = Arc::new(arrow::array::StructArray::try_new(
            fields.into(),
            arrays,
            self.typed_value_nulls.finish(),
        )?);

        let value = self.value_builder.finish()?;
        let Some(value) = value.as_byte_view_opt() else {
            return Err(ArrowError::InvalidArgumentError(
                "Invalid VariantArray: value builder must produce a BinaryViewArray".to_string(),
            ));
        };
        Ok(Arc::new(crate::VariantArray::from_parts(
            self.metadata.clone(),
            Some(value.clone()), // TODO: How to consume an ArrayRef directly?
            Some(typed_value),
            self.nulls.finish(),
        )))
    }
}
