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

use crate::VariantArray;
use crate::variant_get::output::OutputBuilder;
use arrow::error::Result;

use arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, AsArray, BinaryViewArray, NullBufferBuilder,
    PrimitiveArray, new_null_array,
};
use arrow::compute::{CastOptions, cast_with_options};
use arrow::datatypes::{Int16Type, Int32Type};
use arrow_schema::{ArrowError, FieldRef};
use parquet_variant::{Variant, VariantPath};
use std::marker::PhantomData;
use std::sync::Arc;

/// Trait for Arrow primitive types that can be used in the output builder
///
/// This just exists to add a generic way to convert from Variant to the primitive type
pub(super) trait ArrowPrimitiveVariant: ArrowPrimitiveType {
    /// Try to extract the primitive value from a Variant, returning None if it
    /// cannot be converted
    ///
    /// TODO: figure out how to handle coercion/casting
    fn from_variant(variant: &Variant) -> Option<Self::Native>;
}

/// Outputs Primitive arrays
pub(super) struct PrimitiveOutputBuilder<'a, T: ArrowPrimitiveVariant> {
    /// What path to extract
    path: VariantPath<'a>,
    /// Returned output type
    as_type: FieldRef,
    /// Controls the casting behavior (e.g. error vs substituting null on cast error).
    cast_options: CastOptions<'a>,
    /// Phantom data for the primitive type
    _phantom: PhantomData<T>,
}

impl<'a, T: ArrowPrimitiveVariant> PrimitiveOutputBuilder<'a, T> {
    pub(super) fn new(
        path: VariantPath<'a>,
        as_type: FieldRef,
        cast_options: CastOptions<'a>,
    ) -> Self {
        Self {
            path,
            as_type,
            cast_options,
            _phantom: PhantomData,
        }
    }
}

impl<T: ArrowPrimitiveVariant> OutputBuilder for PrimitiveOutputBuilder<'_, T> {
    fn partially_shredded(
        &self,
        variant_array: &VariantArray,
        _metadata: &BinaryViewArray,
        _value_field: &BinaryViewArray,
        typed_value: &ArrayRef,
    ) -> arrow::error::Result<ArrayRef> {
        // build up the output array element by element
        let mut nulls = NullBufferBuilder::new(variant_array.len());
        let mut values = Vec::with_capacity(variant_array.len());
        let typed_value =
            cast_with_options(typed_value, self.as_type.data_type(), &self.cast_options)?;
        // downcast to the primitive array (e.g. Int32Array, Float64Array, etc)
        let typed_value = typed_value.as_primitive::<T>();

        for i in 0..variant_array.len() {
            if variant_array.is_null(i) {
                nulls.append_null();
                values.push(T::default_value()); // not used, placeholder
                continue;
            }

            // if the typed value is null, decode the variant and extract the value
            if typed_value.is_null(i) {
                // TODO follow path
                // https://github.com/apache/arrow-rs/issues/8086
                let variant = variant_array.value(i);
                let Some(value) = T::from_variant(&variant) else {
                    if self.cast_options.safe {
                        // safe mode: append null if we can't convert
                        nulls.append_null();
                        values.push(T::default_value()); // not used, placeholder
                        continue;
                    } else {
                        return Err(ArrowError::CastError(format!(
                            "Failed to extract primitive of type {} from variant {:?} at path {:?}",
                            self.as_type.data_type(),
                            variant,
                            self.path
                        )));
                    }
                };

                nulls.append_non_null();
                values.push(value)
            } else {
                // otherwise we have a typed value, so we can use it directly
                nulls.append_non_null();
                values.push(typed_value.value(i));
            }
        }

        let nulls = nulls.finish();
        let array = PrimitiveArray::<T>::new(values.into(), nulls)
            .with_data_type(self.as_type.data_type().clone());
        Ok(Arc::new(array))
    }

    fn typed(
        &self,
        _variant_array: &VariantArray,
        _metadata: &BinaryViewArray,
        typed_value: &ArrayRef,
    ) -> arrow::error::Result<ArrayRef> {
        // if the types match exactly, we can just return the typed_value
        if typed_value.data_type() == self.as_type.data_type() {
            Ok(typed_value.clone())
        } else {
            // TODO: try to cast the typed_value to the desired type?
            // https://github.com/apache/arrow-rs/issues/8086
            Err(ArrowError::NotYetImplemented(format!(
                "variant_get fully_shredded as {:?} with typed_value={:?} is not implemented yet",
                self.as_type.data_type(),
                typed_value.data_type()
            )))
        }
    }

    fn unshredded(
        &self,
        _variant_array: &VariantArray,
        _metadata: &BinaryViewArray,
        _value_field: &BinaryViewArray,
    ) -> Result<ArrayRef> {
        Err(ArrowError::NotYetImplemented(String::from(
            "variant_get unshredded to primitive types is not implemented yet",
        )))
    }

    fn all_null(
        &self,
        variant_array: &VariantArray,
        _metadata: &BinaryViewArray,
    ) -> Result<ArrayRef> {
        // For all-null case, create a primitive array with all null values
        Ok(Arc::new(new_null_array(
            self.as_type.data_type(),
            variant_array.len(),
        )))
    }
}

impl ArrowPrimitiveVariant for Int32Type {
    fn from_variant(variant: &Variant) -> Option<Self::Native> {
        variant.as_int32()
    }
}

impl ArrowPrimitiveVariant for Int16Type {
    fn from_variant(variant: &Variant) -> Option<Self::Native> {
        variant.as_int16()
    }
}
