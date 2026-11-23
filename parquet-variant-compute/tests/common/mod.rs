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

use arrow::array::{Array, ArrayRef, BinaryViewArray, Int64Array, ListArray, StructArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::Field;
use arrow::error::Result;
use parquet_variant::{ObjectFieldBuilder, Variant, VariantBuilder, VariantBuilderExt};
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

// Narrow typed integers and canonicalize typed NaNs; preserve residual scalars.
// Rebuild containers with a fresh dictionary.
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
        DecodedVariant::Object(object) => {
            let mut object_out = output.try_new_object()?;
            for field in object.fields() {
                let (name, row) = field?;
                write_row(row, &mut ObjectFieldBuilder::new(name, &mut object_out))?;
            }
            object_out.finish();
        }
        DecodedVariant::List(list) => {
            let mut list_out = output.try_new_list()?;
            for row in list.elements() {
                write_row(row?, &mut list_out)?;
            }
            list_out.finish();
        }
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

pub fn nested_input(rows: usize, partial: bool) -> VariantArray {
    let mut residual =
        VariantBuilder::new().with_field_names(["unused", "z", "r", "a", "child", "items"]);
    let mut object = residual.new_object();
    let mut list = object.new_list("r");
    let mut child = list.new_object();
    child.insert("child", 2000i64);
    child.finish();
    list.finish();
    object.finish();
    let (metadata, bytes) = residual.finish();
    let integers = Arc::new(struct_array(
        vec![("typed_value", Arc::new(Int64Array::from(vec![1000; rows])))],
        None,
    ));
    let child = Arc::new(struct_array(
        vec![(
            "typed_value",
            Arc::new(struct_array(vec![("child", integers.clone())], None)),
        )],
        None,
    ));
    let items = ListArray::new(
        Arc::new(Field::new_list_field(integers.data_type().clone(), true)),
        OffsetBuffer::new((0..=rows as i32).collect::<Vec<_>>().into()),
        integers.clone(),
        None,
    );
    let items = Arc::new(struct_array(vec![("typed_value", Arc::new(items))], None));
    let typed = Arc::new(struct_array(
        vec![("z", integers), ("a", child), ("items", items)],
        None,
    ));
    input(
        &metadata,
        &vec![partial.then_some(bytes.as_slice()); rows],
        typed,
    )
}
