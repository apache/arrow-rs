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

use arrow::array::ArrayRef;
use arrow::datatypes;
use arrow::datatypes::ArrowPrimitiveType;
use arrow::error::{ArrowError, Result};
use parquet_variant::{Variant, VariantPath};

use crate::VariantArrayBuilder;

use std::sync::Arc;

pub(crate) fn make_shredding_row_builder<'a>(
    //metadata: &BinaryViewArray,
    path: VariantPath<'a>,
    data_type: Option<&'a datatypes::DataType>,
) -> Result<Box<dyn VariantShreddingRowBuilder + 'a>> {
    use arrow::array::PrimitiveBuilder;
    use datatypes::Int32Type;
    
    // support non-empty paths (field access) and some empty path cases
    if path.is_empty() {
        return match data_type {
            Some(datatypes::DataType::Int32) => {
                // Return PrimitiveInt32Builder for type conversion
                let builder = PrimitiveVariantShreddingRowBuilder {
                    builder: PrimitiveBuilder::<Int32Type>::new(),
                };
                Ok(Box::new(builder))
            }
            None => {
                // Return VariantArrayBuilder for VariantArray output
                let builder = VariantArrayShreddingRowBuilder::new(16);
                Ok(Box::new(builder))
            }
            _ => {
                // only Int32 supported for empty paths
                Err(ArrowError::NotYetImplemented(format!(
                    "variant_get with empty path and data_type={:?} not yet implemented",
                    data_type
                )))
            }
        };
    }

    // Non-empty paths: field access functionality
    // Helper macro to reduce duplication when wrapping builders with path functionality
    macro_rules! wrap_with_path {
        ($inner_builder:expr) => {
            Ok(Box::new(VariantPathRowBuilder {
                builder: $inner_builder,
                path,
            }) as Box<dyn VariantShreddingRowBuilder + 'a>)
        };
    }

    match data_type {
        Some(datatypes::DataType::Int32) => {
            // Create a primitive builder and wrap it with path functionality
            let inner_builder = PrimitiveVariantShreddingRowBuilder {
                builder: PrimitiveBuilder::<Int32Type>::new(),
            };
            wrap_with_path!(inner_builder)
        }
        None => {
            // Create a variant array builder and wrap it with path functionality
            let inner_builder = VariantArrayShreddingRowBuilder::new(16);
            wrap_with_path!(inner_builder)
        }
        _ => {
            // only Int32 and VariantArray supported
            Err(ArrowError::NotYetImplemented(format!(
                "variant_get with path={:?} and data_type={:?} not yet implemented",
                path, data_type
            )))
        }
    }
}

/// Builder for shredding variant values into strongly typed Arrow arrays.
///
/// Useful for variant_get kernels that need to extract specific paths from variant values, possibly
/// with casting of leaf values to specific types.
pub(crate) trait VariantShreddingRowBuilder {
    fn append_null(&mut self) -> Result<()>;

    fn append_value(&mut self, value: &Variant<'_, '_>) -> Result<bool>;

    fn finish(&mut self) -> Result<ArrayRef>;
}

/// A thin wrapper whose only job is to extract a specific path from a variant value and pass the
/// result to a nested builder.
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
            // append null on conversion failure (safe casting behavior)
            // This matches the default CastOptions::safe = true behavior
            // TODO: In future steps, respect CastOptions for safe vs unsafe casting
            self.builder.append_null();
            Ok(false)
        }
    }

    fn finish(&mut self) -> Result<ArrayRef> {
        Ok(Arc::new(self.builder.finish()))
    }
}

/// Builder for creating VariantArray output (for path extraction without type conversion)
struct VariantArrayShreddingRowBuilder {
    builder: VariantArrayBuilder,
}

impl VariantArrayShreddingRowBuilder {
    fn new(capacity: usize) -> Self {
        Self {
            builder: VariantArrayBuilder::new(capacity),
        }
    }
}

impl VariantShreddingRowBuilder for VariantArrayShreddingRowBuilder {
    fn append_null(&mut self) -> Result<()> {
        self.builder.append_null();
        Ok(())
    }

    fn append_value(&mut self, value: &Variant<'_, '_>) -> Result<bool> {
        self.builder.append_variant(value.clone());
        Ok(true)
    }

    fn finish(&mut self) -> Result<ArrayRef> {
        // VariantArrayBuilder::build takes ownership, so we need to replace it
        let builder = std::mem::replace(&mut self.builder, VariantArrayBuilder::new(0));
        Ok(Arc::new(builder.build()))
    }
}


