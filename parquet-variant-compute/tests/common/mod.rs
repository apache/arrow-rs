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

use arrow::array::{Array, ArrayRef, BinaryViewArray, StructArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::Field;
use arrow::error::Result;
use parquet_variant::{Variant, VariantBuilder, VariantBuilderExt};
use parquet_variant_compute::{DecodedVariant, VariantArray, VariantRow, VariantRowDecoder};
use std::sync::Arc;

pub fn struct_array(fields: Vec<(&str, ArrayRef)>, nulls: Option<NullBuffer>) -> StructArray {
    let (fields, arrays): (Vec<_>, Vec<_>) = fields
        .into_iter()
        .map(|(name, array)| {
            (
                Arc::new(Field::new(name, array.data_type().clone(), true)),
                array,
            )
        })
        .unzip();
    StructArray::new(fields.into(), arrays, nulls)
}

pub fn input(metadata: &[u8], value: &[Option<&[u8]>], typed: ArrayRef) -> VariantArray {
    VariantArray::try_new(&struct_array(
        vec![
            (
                "metadata",
                Arc::new(BinaryViewArray::from_iter_values(std::iter::repeat_n(
                    metadata,
                    value.len(),
                ))),
            ),
            ("value", Arc::new(BinaryViewArray::from(value.to_vec()))),
            ("typed_value", typed),
        ],
        None,
    ))
    .unwrap()
}

// A consumer-owned writer: typed integers are narrowed; residual scalars retain their encoding.
pub fn write_row(row: VariantRow<'_>, output: &mut impl VariantBuilderExt) -> Result<()> {
    match row.decode()? {
        DecodedVariant::Null | DecodedVariant::Missing => output.append_null(),
        DecodedVariant::Typed(value) => output.append_value(match value {
            Variant::Int32(v) if i8::try_from(v).is_ok() => Variant::Int8(v as i8),
            Variant::Int64(v) if i8::try_from(v).is_ok() => Variant::Int8(v as i8),
            Variant::Float(v) if v.is_nan() => Variant::Float(f32::from_bits(0x7fc00000)),
            Variant::Double(v) if v.is_nan() => Variant::Double(f64::from_bits(0x7ff8000000000000)),
            value => value,
        }),
        DecodedVariant::Residual { value, .. } => output.append_value(value),
    }
    Ok(())
}

pub fn rewrite(array: &VariantArray) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let decoder = VariantRowDecoder::try_new(array)?;
    (0..array.len())
        .map(|index| {
            let mut output = VariantBuilder::new();
            write_row(decoder.row(index)?, &mut output)?;
            Ok(output.finish())
        })
        .collect()
}
