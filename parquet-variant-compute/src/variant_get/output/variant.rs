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
use crate::{VariantArray, VariantArrayBuilder};
use arrow::array::{Array, ArrayRef, AsArray, BinaryViewArray};
use arrow::datatypes::Int32Type;
use arrow_schema::{ArrowError, DataType};
use parquet_variant::{Variant, VariantPath};
use std::sync::Arc;

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

impl<'a> OutputBuilder for VariantOutputBuilder<'a> {
    fn partially_shredded(
        &self,
        variant_array: &VariantArray,
        // TODO(perf): can reuse the metadata field here to avoid re-creating it
        _metadata: &BinaryViewArray,
        _value_field: &BinaryViewArray,
        typed_value: &ArrayRef,
    ) -> arrow::error::Result<ArrayRef> {
        // in this case dispatch on the typed_value and
        // TODO macro'ize this using downcast! to handle all other primitive types
        // TODO(perf): avoid builders entirely (and write the raw variant directly as we know the metadata is the same)
        let mut array_builder = VariantArrayBuilder::new(variant_array.len());
        match typed_value.data_type() {
            DataType::Int32 => {
                let primitive_array = typed_value.as_primitive::<Int32Type>();
                for i in 0..variant_array.len() {
                    if variant_array.is_null(i) {
                        array_builder.append_null();
                        continue;
                    }

                    if typed_value.is_null(i) {
                        // fall back to the value (variant) field
                        // (TODO could copy the variant bytes directly)
                        let value = variant_array.value(i);
                        array_builder.append_variant(value);
                        continue;
                    }

                    // otherwise we have a typed value, so we can use it directly
                    let int_value = primitive_array.value(i);
                    array_builder.append_variant(Variant::from(int_value));
                }
            }
            dt => {
                return Err(ArrowError::NotYetImplemented(format!(
                    "variant_get fully_shredded with typed_value={dt} is not implemented yet",
                )));
            }
        };
        Ok(Arc::new(array_builder.build()))
    }

    fn fully_shredded(
        &self,
        variant_array: &VariantArray,
        // TODO(perf): can reuse the metadata field here to avoid re-creating it
        _metadata: &BinaryViewArray,
        typed_value: &ArrayRef,
    ) -> arrow::error::Result<ArrayRef> {
        // in this case dispatch on the typed_value and
        // TODO macro'ize this using downcast! to handle all other primitive types
        // TODO(perf): avoid builders entirely (and write the raw variant directly as we know the metadata is the same)
        let mut array_builder = VariantArrayBuilder::new(variant_array.len());
        match typed_value.data_type() {
            DataType::Int32 => {
                let primitive_array = typed_value.as_primitive::<Int32Type>();
                for i in 0..variant_array.len() {
                    if primitive_array.is_null(i) {
                        array_builder.append_null();
                        continue;
                    }

                    let int_value = primitive_array.value(i);
                    array_builder.append_variant(Variant::from(int_value));
                }
            }
            dt => {
                return Err(ArrowError::NotYetImplemented(format!(
                    "variant_get fully_shredded with typed_value={dt} is not implemented yet",
                )));
            }
        };
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
}
