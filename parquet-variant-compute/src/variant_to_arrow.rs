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

use arrow::array::{ArrayRef, BinaryViewArray, NullBufferBuilder, PrimitiveBuilder};
use arrow::compute::CastOptions;
use arrow::datatypes::{self, ArrowPrimitiveType, DataType};
use arrow::error::{ArrowError, Result};
use parquet_variant::{Variant, VariantPath};

use crate::type_conversion::PrimitiveFromVariant;
use crate::{VariantArray, VariantValueArrayBuilder};

use std::sync::Arc;

/// Builder for converting variant values to primitive Arrow arrays. It is used by both
/// `VariantToArrowRowBuilder` (below) and `VariantToShreddedPrimitiveVariantRowBuilder` (in
/// `shred_variant.rs`).
pub(crate) enum PrimitiveVariantToArrowRowBuilder<'a> {
    Int8(VariantToPrimitiveArrowRowBuilder<'a, datatypes::Int8Type>),
    Int16(VariantToPrimitiveArrowRowBuilder<'a, datatypes::Int16Type>),
    Int32(VariantToPrimitiveArrowRowBuilder<'a, datatypes::Int32Type>),
    Int64(VariantToPrimitiveArrowRowBuilder<'a, datatypes::Int64Type>),
    UInt8(VariantToPrimitiveArrowRowBuilder<'a, datatypes::UInt8Type>),
    UInt16(VariantToPrimitiveArrowRowBuilder<'a, datatypes::UInt16Type>),
    UInt32(VariantToPrimitiveArrowRowBuilder<'a, datatypes::UInt32Type>),
    UInt64(VariantToPrimitiveArrowRowBuilder<'a, datatypes::UInt64Type>),
    Float16(VariantToPrimitiveArrowRowBuilder<'a, datatypes::Float16Type>),
    Float32(VariantToPrimitiveArrowRowBuilder<'a, datatypes::Float32Type>),
    Float64(VariantToPrimitiveArrowRowBuilder<'a, datatypes::Float64Type>),
}

/// Builder for converting variant values into strongly typed Arrow arrays.
///
/// Useful for variant_get kernels that need to extract specific paths from variant values, possibly
/// with casting of leaf values to specific types.
pub(crate) enum VariantToArrowRowBuilder<'a> {
    Primitive(PrimitiveVariantToArrowRowBuilder<'a>),
    BinaryVariant(VariantToBinaryVariantArrowRowBuilder),

    // Path extraction wrapper - contains a boxed enum for any of the above
    WithPath(VariantPathRowBuilder<'a>),
}

impl<'a> PrimitiveVariantToArrowRowBuilder<'a> {
    pub fn append_null(&mut self) -> Result<()> {
        use PrimitiveVariantToArrowRowBuilder::*;
        match self {
            Int8(b) => b.append_null(),
            Int16(b) => b.append_null(),
            Int32(b) => b.append_null(),
            Int64(b) => b.append_null(),
            UInt8(b) => b.append_null(),
            UInt16(b) => b.append_null(),
            UInt32(b) => b.append_null(),
            UInt64(b) => b.append_null(),
            Float16(b) => b.append_null(),
            Float32(b) => b.append_null(),
            Float64(b) => b.append_null(),
        }
    }

    pub fn append_value(&mut self, value: &Variant<'_, '_>) -> Result<bool> {
        use PrimitiveVariantToArrowRowBuilder::*;
        match self {
            Int8(b) => b.append_value(value),
            Int16(b) => b.append_value(value),
            Int32(b) => b.append_value(value),
            Int64(b) => b.append_value(value),
            UInt8(b) => b.append_value(value),
            UInt16(b) => b.append_value(value),
            UInt32(b) => b.append_value(value),
            UInt64(b) => b.append_value(value),
            Float16(b) => b.append_value(value),
            Float32(b) => b.append_value(value),
            Float64(b) => b.append_value(value),
        }
    }

    pub fn finish(self) -> Result<ArrayRef> {
        use PrimitiveVariantToArrowRowBuilder::*;
        match self {
            Int8(b) => b.finish(),
            Int16(b) => b.finish(),
            Int32(b) => b.finish(),
            Int64(b) => b.finish(),
            UInt8(b) => b.finish(),
            UInt16(b) => b.finish(),
            UInt32(b) => b.finish(),
            UInt64(b) => b.finish(),
            Float16(b) => b.finish(),
            Float32(b) => b.finish(),
            Float64(b) => b.finish(),
        }
    }
}

impl<'a> VariantToArrowRowBuilder<'a> {
    pub fn append_null(&mut self) -> Result<()> {
        use VariantToArrowRowBuilder::*;
        match self {
            Primitive(b) => b.append_null(),
            BinaryVariant(b) => b.append_null(),
            WithPath(path_builder) => path_builder.append_null(),
        }
    }

    pub fn append_value(&mut self, value: Variant<'_, '_>) -> Result<bool> {
        use VariantToArrowRowBuilder::*;
        match self {
            Primitive(b) => b.append_value(&value),
            BinaryVariant(b) => b.append_value(value),
            WithPath(path_builder) => path_builder.append_value(value),
        }
    }

    pub fn finish(self) -> Result<ArrayRef> {
        use VariantToArrowRowBuilder::*;
        match self {
            Primitive(b) => b.finish(),
            BinaryVariant(b) => b.finish(),
            WithPath(path_builder) => path_builder.finish(),
        }
    }
}

/// Creates a primitive row builder, returning Err if the requested data type is not primitive.
pub(crate) fn make_primitive_variant_to_arrow_row_builder<'a>(
    data_type: &'a DataType,
    cast_options: &'a CastOptions,
    capacity: usize,
) -> Result<PrimitiveVariantToArrowRowBuilder<'a>> {
    use PrimitiveVariantToArrowRowBuilder::*;

    let builder = match data_type {
        DataType::Int8 => Int8(VariantToPrimitiveArrowRowBuilder::new(
            cast_options,
            capacity,
        )),
        DataType::Int16 => Int16(VariantToPrimitiveArrowRowBuilder::new(
            cast_options,
            capacity,
        )),
        DataType::Int32 => Int32(VariantToPrimitiveArrowRowBuilder::new(
            cast_options,
            capacity,
        )),
        DataType::Int64 => Int64(VariantToPrimitiveArrowRowBuilder::new(
            cast_options,
            capacity,
        )),
        DataType::UInt8 => UInt8(VariantToPrimitiveArrowRowBuilder::new(
            cast_options,
            capacity,
        )),
        DataType::UInt16 => UInt16(VariantToPrimitiveArrowRowBuilder::new(
            cast_options,
            capacity,
        )),
        DataType::UInt32 => UInt32(VariantToPrimitiveArrowRowBuilder::new(
            cast_options,
            capacity,
        )),
        DataType::UInt64 => UInt64(VariantToPrimitiveArrowRowBuilder::new(
            cast_options,
            capacity,
        )),
        DataType::Float16 => Float16(VariantToPrimitiveArrowRowBuilder::new(
            cast_options,
            capacity,
        )),
        DataType::Float32 => Float32(VariantToPrimitiveArrowRowBuilder::new(
            cast_options,
            capacity,
        )),
        DataType::Float64 => Float64(VariantToPrimitiveArrowRowBuilder::new(
            cast_options,
            capacity,
        )),
        _ if data_type.is_primitive() => {
            return Err(ArrowError::NotYetImplemented(format!(
                "Primitive data_type {data_type:?} not yet implemented"
            )));
        }
        _ => {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Not a primitive type: {data_type:?}"
            )));
        }
    };
    Ok(builder)
}

pub(crate) fn make_variant_to_arrow_row_builder<'a>(
    metadata: &BinaryViewArray,
    path: VariantPath<'a>,
    data_type: Option<&'a DataType>,
    cast_options: &'a CastOptions,
    capacity: usize,
) -> Result<VariantToArrowRowBuilder<'a>> {
    use VariantToArrowRowBuilder::*;

    let mut builder = match data_type {
        // If no data type was requested, build an unshredded VariantArray.
        None => BinaryVariant(VariantToBinaryVariantArrowRowBuilder::new(
            metadata.clone(),
            capacity,
        )),
        Some(DataType::Struct(_)) => {
            return Err(ArrowError::NotYetImplemented(
                "Converting unshredded variant objects to arrow structs".to_string(),
            ));
        }
        Some(
            DataType::List(_)
            | DataType::LargeList(_)
            | DataType::ListView(_)
            | DataType::LargeListView(_)
            | DataType::FixedSizeList(..),
        ) => {
            return Err(ArrowError::NotYetImplemented(
                "Converting unshredded variant arrays to arrow lists".to_string(),
            ));
        }
        Some(data_type) => {
            let builder =
                make_primitive_variant_to_arrow_row_builder(data_type, cast_options, capacity)?;
            Primitive(builder)
        }
    };

    // Wrap with path extraction if needed
    if !path.is_empty() {
        builder = WithPath(VariantPathRowBuilder {
            builder: Box::new(builder),
            path,
        })
    };

    Ok(builder)
}

/// A thin wrapper whose only job is to extract a specific path from a variant value and pass the
/// result to a nested builder.
pub(crate) struct VariantPathRowBuilder<'a> {
    builder: Box<VariantToArrowRowBuilder<'a>>,
    path: VariantPath<'a>,
}

impl<'a> VariantPathRowBuilder<'a> {
    fn append_null(&mut self) -> Result<()> {
        self.builder.append_null()
    }

    fn append_value(&mut self, value: Variant<'_, '_>) -> Result<bool> {
        if let Some(v) = value.get_path(&self.path) {
            self.builder.append_value(v)
        } else {
            self.builder.append_null()?;
            Ok(false)
        }
    }

    fn finish(self) -> Result<ArrayRef> {
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
pub(crate) struct VariantToPrimitiveArrowRowBuilder<'a, T: PrimitiveFromVariant> {
    builder: arrow::array::PrimitiveBuilder<T>,
    cast_options: &'a CastOptions<'a>,
}

impl<'a, T: PrimitiveFromVariant> VariantToPrimitiveArrowRowBuilder<'a, T> {
    fn new(cast_options: &'a CastOptions<'a>, capacity: usize) -> Self {
        Self {
            builder: PrimitiveBuilder::<T>::with_capacity(capacity),
            cast_options,
        }
    }
}

impl<'a, T: PrimitiveFromVariant> VariantToPrimitiveArrowRowBuilder<'a, T> {
    fn append_null(&mut self) -> Result<()> {
        self.builder.append_null();
        Ok(())
    }

    fn append_value(&mut self, value: &Variant<'_, '_>) -> Result<bool> {
        if let Some(v) = T::from_variant(value) {
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

    fn finish(mut self) -> Result<ArrayRef> {
        Ok(Arc::new(self.builder.finish()))
    }
}

/// Builder for creating VariantArray output (for path extraction without type conversion)
pub(crate) struct VariantToBinaryVariantArrowRowBuilder {
    metadata: BinaryViewArray,
    builder: VariantValueArrayBuilder,
    nulls: NullBufferBuilder,
}

impl VariantToBinaryVariantArrowRowBuilder {
    fn new(metadata: BinaryViewArray, capacity: usize) -> Self {
        Self {
            metadata,
            builder: VariantValueArrayBuilder::new(capacity),
            nulls: NullBufferBuilder::new(capacity),
        }
    }
}

impl VariantToBinaryVariantArrowRowBuilder {
    fn append_null(&mut self) -> Result<()> {
        self.builder.append_null();
        self.nulls.append_null();
        Ok(())
    }

    fn append_value(&mut self, value: Variant<'_, '_>) -> Result<bool> {
        self.builder.append_value(value);
        self.nulls.append_non_null();
        Ok(true)
    }

    fn finish(mut self) -> Result<ArrayRef> {
        let variant_array = VariantArray::from_parts(
            self.metadata,
            Some(self.builder.build()?),
            None, // no typed_value column
            self.nulls.finish(),
        );

        Ok(ArrayRef::from(variant_array))
    }
}
