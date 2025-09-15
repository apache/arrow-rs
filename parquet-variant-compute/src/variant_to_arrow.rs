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

use arrow::array::{ArrayRef, PrimitiveBuilder};
use arrow::compute::CastOptions;
use arrow::datatypes;
use arrow::datatypes::ArrowPrimitiveType;
use arrow::error::{ArrowError, Result};
use parquet_variant::{Variant, VariantPath};

use crate::type_conversion::VariantAsPrimitive;
use crate::VariantArrayBuilder;

use std::sync::Arc;

pub(crate) fn make_variant_to_arrow_row_builder<'a>(
    //metadata: &BinaryViewArray,
    path: VariantPath<'a>,
    data_type: Option<&'a datatypes::DataType>,
    cast_options: &'a CastOptions,
) -> Result<Box<dyn VariantToArrowRowBuilder + 'a>> {
    use datatypes::{
        Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
    };

    let builder = match data_type {
        // If no data type was requested, build an unshredded VariantArray.
        None => VariantToBinaryVariantArrowRowBuilder::new(16).with_path(path),
        Some(datatypes::DataType::Int8) => {
            VariantToPrimitiveArrowRowBuilder::<Int8Type>::new(cast_options).with_path(path)
        }
        Some(datatypes::DataType::Int16) => {
            VariantToPrimitiveArrowRowBuilder::<Int16Type>::new(cast_options).with_path(path)
        }
        Some(datatypes::DataType::Int32) => {
            VariantToPrimitiveArrowRowBuilder::<Int32Type>::new(cast_options).with_path(path)
        }
        Some(datatypes::DataType::Int64) => {
            VariantToPrimitiveArrowRowBuilder::<Int64Type>::new(cast_options).with_path(path)
        }
        Some(datatypes::DataType::Float16) => {
            VariantToPrimitiveArrowRowBuilder::<Float16Type>::new(cast_options).with_path(path)
        }
        Some(datatypes::DataType::Float32) => {
            VariantToPrimitiveArrowRowBuilder::<Float32Type>::new(cast_options).with_path(path)
        }
        Some(datatypes::DataType::Float64) => {
            VariantToPrimitiveArrowRowBuilder::<Float64Type>::new(cast_options).with_path(path)
        }
        _ => {
            return Err(ArrowError::NotYetImplemented(format!(
                "variant_get with path={:?} and data_type={:?} not yet implemented",
                path, data_type
            )));
        }
    };
    Ok(builder)
}

/// Builder for converting variant values into strongly typed Arrow arrays.
///
/// Useful for variant_get kernels that need to extract specific paths from variant values, possibly
/// with casting of leaf values to specific types.
pub(crate) trait VariantToArrowRowBuilder {
    fn append_null(&mut self) -> Result<()>;

    fn append_value(&mut self, value: &Variant<'_, '_>) -> Result<bool>;

    fn finish(&mut self) -> Result<ArrayRef>;
}

/// A thin wrapper whose only job is to extract a specific path from a variant value and pass the
/// result to a nested builder.
struct VariantPathRowBuilder<'a, T: VariantToArrowRowBuilder> {
    builder: T,
    path: VariantPath<'a>,
}

trait VariantToArrowRowBuilderWithPath<'a>: VariantToArrowRowBuilder {
    fn with_path(self, path: VariantPath<'a>) -> Box<dyn VariantToArrowRowBuilder + 'a>;
}

impl<'a, T: VariantToArrowRowBuilder + 'a> VariantToArrowRowBuilderWithPath<'a> for T {
    fn with_path(self, path: VariantPath<'a>) -> Box<dyn VariantToArrowRowBuilder + 'a> {
        if path.is_empty() {
            Box::new(self)
        } else {
            Box::new(VariantPathRowBuilder {
                builder: self,
                path,
            })
        }
    }
}

impl<T: VariantToArrowRowBuilder> VariantToArrowRowBuilder for VariantPathRowBuilder<'_, T> {
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

/// Helper function to get a user-friendly type name
fn get_type_name<T: ArrowPrimitiveType>() -> &'static str {
    match std::any::type_name::<T>() {
        "arrow_array::types::Int32Type" => "Int32",
        "arrow_array::types::Int16Type" => "Int16",
        "arrow_array::types::Int8Type" => "Int8",
        "arrow_array::types::Int64Type" => "Int64",
        "arrow_array::types::UInt32Type" => "UInt32",
        "arrow_array::types::UInt16Type" => "UInt16",
        "arrow_array::types::UInt8Type" => "UInt8",
        "arrow_array::types::UInt64Type" => "UInt64",
        "arrow_array::types::Float32Type" => "Float32",
        "arrow_array::types::Float64Type" => "Float64",
        "arrow_array::types::Float16Type" => "Float16",
        _ => "Unknown",
    }
}

/// Builder for converting variant values to primitive values
struct VariantToPrimitiveArrowRowBuilder<'a, T: ArrowPrimitiveType> {
    builder: arrow::array::PrimitiveBuilder<T>,
    cast_options: &'a CastOptions<'a>,
}

impl<'a, T: ArrowPrimitiveType> VariantToPrimitiveArrowRowBuilder<'a, T> {
    fn new(cast_options: &'a CastOptions<'a>) -> Self {
        Self {
            builder: PrimitiveBuilder::<T>::new(),
            cast_options,
        }
    }
}

impl<'a, T> VariantToArrowRowBuilder for VariantToPrimitiveArrowRowBuilder<'a, T>
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
            if !self.cast_options.safe {
                // Unsafe casting: return error on conversion failure
                return Err(ArrowError::CastError(format!(
                    "Failed to extract primitive of type {} from variant {:?} at path VariantPath([])",
                    get_type_name::<T>(),
                    value
                )));
            }
            // Safe casting: append null on conversion failure
            self.builder.append_null();
            Ok(false)
        }
    }

    fn finish(&mut self) -> Result<ArrayRef> {
        Ok(Arc::new(self.builder.finish()))
    }
}

/// Builder for creating VariantArray output (for path extraction without type conversion)
struct VariantToBinaryVariantArrowRowBuilder {
    builder: VariantArrayBuilder,
}

impl VariantToBinaryVariantArrowRowBuilder {
    fn new(capacity: usize) -> Self {
        Self {
            builder: VariantArrayBuilder::new(capacity),
        }
    }
}

impl VariantToArrowRowBuilder for VariantToBinaryVariantArrowRowBuilder {
    fn append_null(&mut self) -> Result<()> {
        self.builder.append_null();
        Ok(())
    }

    fn append_value(&mut self, value: &Variant<'_, '_>) -> Result<bool> {
        // TODO: We need a way to convert a Variant directly to bytes. In particular, we want to
        // just copy across the underlying value byte slice of a `Variant::Object` or
        // `Variant::List`, without any interaction with a `VariantMetadata` (because the shredding
        // spec requires us to reuse the existing metadata when unshredding).
        //
        // One could _probably_ emulate this with parquet_variant::VariantBuilder, but it would do a
        // lot of unnecessary work and would also create a new metadata column we don't need.
        self.builder.append_variant(value.clone());
        Ok(true)
    }

    fn finish(&mut self) -> Result<ArrayRef> {
        // VariantArrayBuilder::build takes ownership, so we need to replace it
        let builder = std::mem::replace(&mut self.builder, VariantArrayBuilder::new(0));
        Ok(Arc::new(builder.build()))
    }
}
