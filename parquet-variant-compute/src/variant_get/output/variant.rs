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

use crate::variant_get::output::OutputBuilder;
use crate::{type_conversion::primitive_conversion_array, VariantArray, VariantArrayBuilder};
use arrow::array::{Array, ArrayRef, AsArray, BinaryViewArray};
use arrow::datatypes::{
    Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type,
    UInt32Type, UInt64Type, UInt8Type,
};
use arrow_schema::{ArrowError, DataType};
use parquet_variant::{Variant, VariantPath};
use std::sync::Arc;

macro_rules! cast_partially_shredded_primitive {
    ($typed_value:expr, $variant_array:expr, $arrow_type:ty, $data_type:expr) => {{
        let mut array_builder = VariantArrayBuilder::new($variant_array.len());
        let primitive_array = $typed_value.as_primitive::<$arrow_type>();
        for i in 0..$variant_array.len() {
            if $variant_array.is_null(i) {
                array_builder.append_null();
            } else if $typed_value.is_null(i) {
                // fall back to the value (variant) field
                // (TODO could copy the variant bytes directly)
                let value = $variant_array.value(i);
                array_builder.append_variant(value);
            } else {
                // otherwise we have a typed value, so we can use it directly
                let value = primitive_array.value(i);
                array_builder.append_variant(Variant::from(value));
            }
        }
        Ok(Arc::new(array_builder.build()))
    }};
}

/// Outputs VariantArrays
pub(super) struct VariantOutputBuilder<'a> {
    /// What path to extract
    path: VariantPath<'a>,
}

impl<'a> VariantOutputBuilder<'a> {
    pub(super) fn new(path: VariantPath<'a>) -> Self {
        Self { path }
    }
}

impl OutputBuilder for VariantOutputBuilder<'_> {
    fn partially_shredded(
        &self,
        variant_array: &VariantArray,
        // TODO(perf): can reuse the metadata field here to avoid re-creating it
        _metadata: &BinaryViewArray,
        _value_field: &BinaryViewArray,
        typed_value: &ArrayRef,
    ) -> arrow::error::Result<ArrayRef> {
        // TODO(perf): avoid builders entirely (and write the raw variant directly as we know the metadata is the same)
        match typed_value.data_type() {
            DataType::Int8 => cast_partially_shredded_primitive!(
                typed_value,
                variant_array,
                Int8Type,
                DataType::Int8
            ),

            DataType::Int16 => cast_partially_shredded_primitive!(
                typed_value,
                variant_array,
                Int16Type,
                DataType::Int16
            ),

            DataType::Int32 => cast_partially_shredded_primitive!(
                typed_value,
                variant_array,
                Int32Type,
                DataType::Int32
            ),

            DataType::Int64 => cast_partially_shredded_primitive!(
                typed_value,
                variant_array,
                Int64Type,
                DataType::Int64
            ),

            DataType::UInt8 => cast_partially_shredded_primitive!(
                typed_value,
                variant_array,
                UInt8Type,
                DataType::UInt8
            ),

            DataType::UInt16 => cast_partially_shredded_primitive!(
                typed_value,
                variant_array,
                UInt16Type,
                DataType::Int16
            ),

            DataType::UInt32 => cast_partially_shredded_primitive!(
                typed_value,
                variant_array,
                UInt32Type,
                DataType::UInt32
            ),

            DataType::UInt64 => cast_partially_shredded_primitive!(
                typed_value,
                variant_array,
                UInt64Type,
                DataType::UInt64
            ),

            DataType::Float16 => cast_partially_shredded_primitive!(
                typed_value,
                variant_array,
                Float16Type,
                DataType::Float16
            ),

            DataType::Float32 => cast_partially_shredded_primitive!(
                typed_value,
                variant_array,
                Float32Type,
                DataType::Float32
            ),

            DataType::Float64 => cast_partially_shredded_primitive!(
                typed_value,
                variant_array,
                Float64Type,
                DataType::Float64
            ),

            dt => {
                // https://github.com/apache/arrow-rs/issues/8086
                Err(ArrowError::NotYetImplemented(format!(
                    "variant_get partially shredded with typed_value={dt} is not implemented yet",
                )))
            }
        }
    }

    fn typed(
        &self,
        variant_array: &VariantArray,
        // TODO(perf): can reuse the metadata field here to avoid re-creating it
        _metadata: &BinaryViewArray,
        typed_value: &ArrayRef,
    ) -> arrow::error::Result<ArrayRef> {
        // TODO(perf): avoid builders entirely (and write the raw variant directly as we know the metadata is the same)
        let mut array_builder = VariantArrayBuilder::new(variant_array.len());
        match typed_value.data_type() {
            DataType::Int8 => primitive_conversion_array!(Int8Type, typed_value, array_builder),
            DataType::Int16 => primitive_conversion_array!(Int16Type, typed_value, array_builder),
            DataType::Int32 => primitive_conversion_array!(Int32Type, typed_value, array_builder),
            DataType::Int64 => primitive_conversion_array!(Int64Type, typed_value, array_builder),
            DataType::UInt8 => primitive_conversion_array!(UInt8Type, typed_value, array_builder),
            DataType::UInt16 => primitive_conversion_array!(UInt16Type, typed_value, array_builder),
            DataType::UInt32 => primitive_conversion_array!(UInt32Type, typed_value, array_builder),
            DataType::UInt64 => primitive_conversion_array!(UInt64Type, typed_value, array_builder),
            DataType::Float16 => {
                primitive_conversion_array!(Float16Type, typed_value, array_builder)
            }
            DataType::Float32 => {
                primitive_conversion_array!(Float32Type, typed_value, array_builder)
            }
            DataType::Float64 => {
                primitive_conversion_array!(Float64Type, typed_value, array_builder)
            }
            dt => {
                // https://github.com/apache/arrow-rs/issues/8087
                return Err(ArrowError::NotYetImplemented(format!(
                    "variant_get perfectly shredded with typed_value={dt} is not implemented yet",
                )));
            }
        }
        Ok(Arc::new(array_builder.build()))
    }

    fn unshredded(
        &self,
        variant_array: &VariantArray,
        _metadata: &BinaryViewArray,
        _value_field: &BinaryViewArray,
    ) -> arrow::error::Result<ArrayRef> {
        let mut builder = VariantArrayBuilder::new(variant_array.len());
        for i in 0..variant_array.len() {
            let new_variant = variant_array.value(i);

            // TODO: perf?
            let Some(new_variant) = new_variant.get_path(&self.path) else {
                // path not found, append null
                builder.append_null();
                continue;
            };

            // TODO: we're decoding the value and doing a copy into a variant value
            // again. This can be much faster by using the _metadata and _value_field
            // to avoid decoding the entire variant:
            //
            // 1) reuse the metadata arrays as is
            //
            // 2) Create a new BinaryViewArray that uses the same underlying buffers
            // that the original variant used, but whose views points to a new
            // offset for the new path
            builder.append_variant(new_variant);
        }

        Ok(Arc::new(builder.build()))
    }

    fn all_null(
        &self,
        variant_array: &VariantArray,
        _metadata: &BinaryViewArray,
    ) -> arrow::error::Result<ArrayRef> {
        // For all-null case, simply create a VariantArray with all null values
        let mut builder = VariantArrayBuilder::new(variant_array.len());
        for _i in 0..variant_array.len() {
            builder.append_null();
        }
        Ok(Arc::new(builder.build()))
    }
}
