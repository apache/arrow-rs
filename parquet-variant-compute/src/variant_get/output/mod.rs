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

mod primitive;
mod variant;

use crate::variant_get::output::primitive::PrimitiveOutputBuilder;
use crate::variant_get::output::variant::VariantOutputBuilder;
use crate::variant_get::GetOptions;
use crate::VariantArray;
use arrow::array::{ArrayRef, BinaryViewArray};
use arrow::datatypes::Int32Type;
use arrow::error::Result;
use arrow_schema::{ArrowError, DataType};

/// This trait represents something that gets the output of the variant_get kernel.
///
/// For example, there are specializations for writing the output as a VariantArray,
/// or as a specific type (e.g. Int32Array).
///
/// See [`instantiate_output_builder`] to create an instance of this trait.
pub(crate) trait OutputBuilder {
    /// create output for a shredded variant array
    fn partially_shredded(
        &self,
        variant_array: &VariantArray,
        metadata: &BinaryViewArray,
        value_field: &BinaryViewArray,
        typed_value: &ArrayRef,
    ) -> Result<ArrayRef>;

    /// output for a perfectly shredded variant array
    fn fully_shredded(
        &self,
        variant_array: &VariantArray,
        metadata: &BinaryViewArray,
        typed_value: &ArrayRef,
    ) -> Result<ArrayRef>;

    /// write out an unshredded variant array
    fn unshredded(
        &self,
        variant_array: &VariantArray,
        metadata: &BinaryViewArray,
        value_field: &BinaryViewArray,
    ) -> Result<ArrayRef>;
}

pub(crate) fn instantiate_output_builder<'a>(
    options: GetOptions<'a>,
) -> Result<Box<dyn OutputBuilder + 'a>> {
    let GetOptions {
        as_type,
        path,
        cast_options,
    } = options;

    let Some(as_type) = as_type else {
        return Ok(Box::new(VariantOutputBuilder::new(path)));
    };

    // handle typed output
    match as_type.data_type() {
        DataType::Int32 => Ok(Box::new(PrimitiveOutputBuilder::<Int32Type>::new(
            path,
            as_type,
            cast_options,
        ))),
        dt => Err(ArrowError::NotYetImplemented(format!(
            "variant_get with as_type={dt} is not implemented yet",
        ))),
    }
}
