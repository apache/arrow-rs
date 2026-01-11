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

use crate::{VariantArrayBuilder, VariantType};
use arrow_array::{Array, StructArray};
use arrow_data::ArrayData;
use arrow_json::{DecoderFactory, StructMode};
use arrow_schema::extension::ExtensionType;
use arrow_schema::{ArrowError, DataType, FieldRef};
use parquet_variant::{ObjectFieldBuilder, Variant, VariantBuilderExt};

use arrow_json::reader::ArrayDecoder;
use arrow_json::reader::{Tape, TapeElement};

/// An [`ArrayDecoder`] implementation that decodes JSON values into a Variant array.
///
/// This decoder converts JSON tape elements (parsed JSON tokens) into Parquet Variant
/// format, preserving the full structure of arbitrary JSON including nested objects,
/// arrays, and primitive types.
///
/// This decoder is typically used indirectly via [`VariantArrayDecoderFactory`] when
/// reading JSON data into Variant columns.
#[derive(Default)]
pub struct VariantArrayDecoder;

impl ArrayDecoder for VariantArrayDecoder {
    fn decode(&mut self, tape: &Tape<'_>, pos: &[u32]) -> Result<ArrayData, ArrowError> {
        let mut array_builder = VariantArrayBuilder::new(pos.len());
        for p in pos {
            variant_from_tape_element(&mut array_builder, *p, tape)?;
        }
        let variant_struct_array = StructArray::from(array_builder.build());
        Ok(variant_struct_array.into_data())
    }
}

/// A [`DecoderFactory`] that integrates with the Arrow JSON reader to automatically
/// decode JSON values into Variant arrays when the target field is registered as a
/// [`VariantType`] extension type.
///
/// # Example
///
/// ```ignore
/// use arrow_json::reader::ReaderBuilder;
/// use arrow_json::StructMode;
/// use std::sync::Arc;
///
/// let builder = ReaderBuilder::new(Arc::new(schema));
/// let reader = builder
///     .with_struct_mode(StructMode::ObjectOnly)
///     .with_decoder_factory(Arc::new(VariantArrayDecoderFactory))
///     .build(json_input)?;
/// ```
#[derive(Default, Debug)]
#[allow(unused)]
pub struct VariantArrayDecoderFactory;

impl DecoderFactory for VariantArrayDecoderFactory {
    fn make_custom_decoder<'a>(
        &self,
        field: Option<FieldRef>,
        _data_type: DataType,
        _coerce_primitive: bool,
        _strict_mode: bool,
        _is_nullable: bool,
        _struct_mode: StructMode,
    ) -> Result<Option<Box<dyn ArrayDecoder>>, ArrowError> {
        let field = match field {
            Some(inner_field) => inner_field,
            None => return Ok(None),
        };
        if field.extension_type_name() == Some(VariantType::NAME)
            && field.try_extension_type::<VariantType>().is_ok()
        {
            return Ok(Some(Box::new(VariantArrayDecoder)))
        }
        Ok(None)
    }
}

fn variant_from_tape_element(
    builder: &mut impl VariantBuilderExt,
    mut p: u32,
    tape: &Tape,
) -> Result<u32, ArrowError> {
    match tape.get(p) {
        TapeElement::StartObject(end_idx) => {
            let mut object_builder = builder.try_new_object()?;
            p += 1;
            while p < end_idx {
                // Read field name
                let field_name = match tape.get(p) {
                    TapeElement::String(s) => tape.get_string(s),
                    _ => return Err(tape.error(p, "field name")),
                };

                let mut field_builder = ObjectFieldBuilder::new(field_name, &mut object_builder);
                p = tape.next(p, "field value")?;
                p = variant_from_tape_element(&mut field_builder, p, tape)?;
            }
            object_builder.finish();
        }
        TapeElement::EndObject(_u32) => {
            return Err(ArrowError::JsonError(
                "unexpected end of object".to_string(),
            ));
        }
        TapeElement::StartList(end_idx) => {
            let mut list_builder = builder.try_new_list()?;
            p += 1;
            while p < end_idx {
                p = variant_from_tape_element(&mut list_builder, p, tape)?;
            }
            list_builder.finish();
        }
        TapeElement::EndList(_u32) => {
            return Err(ArrowError::JsonError("unexpected end of list".to_string()));
        }
        TapeElement::String(idx) => builder.append_value(tape.get_string(idx)),
        TapeElement::Number(idx) => {
            let s = tape.get_string(idx);
            builder.append_value(parse_number(s)?)
        }
        TapeElement::I64(i) => {
            return Err(ArrowError::JsonError(format!(
                "I64 tape element not supported: {i}"
            )));
        }
        TapeElement::I32(i) => {
            return Err(ArrowError::JsonError(format!(
                "I32 tape element not supported: {i}"
            )));
        }
        TapeElement::F64(f) => {
            return Err(ArrowError::JsonError(format!(
                "F64 tape element not supported: {f}"
            )));
        }
        TapeElement::F32(f) => {
            return Err(ArrowError::JsonError(format!(
                "F32 tape element not supported: {f}"
            )));
        }
        TapeElement::True => builder.append_value(true),
        TapeElement::False => builder.append_value(false),
        TapeElement::Null => builder.append_value(Variant::Null),
    }
    p += 1;
    Ok(p)
}

fn parse_number<'a, 'b>(s: &'a str) -> Result<Variant<'a, 'b>, ArrowError> {
    if let Ok(v) = lexical_core::parse(s.as_bytes()) {
        return Ok(Variant::Int64(v));
    }

    match lexical_core::parse(s.as_bytes()) {
        Ok(v) => Ok(Variant::Double(v)),
        Err(_) => Err(ArrowError::JsonError(format!(
            "failed to parse {s} as number"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use crate::VariantArray;

    use super::*;
    use arrow_array::Int32Array;
    use arrow_json::StructMode;
    use arrow_json::reader::ReaderBuilder;
    use arrow_schema::{DataType, Field, Schema};
    use parquet_variant::VariantBuilder;
    use std::io::Cursor;
    use std::sync::Arc;

    #[test]
    fn test_variant() {
        let do_test = |json_input: &str, ids: Vec<i32>, variants: Vec<Option<Variant>>| {
            let variant_array = VariantArrayBuilder::new(0).build();

            let struct_field = Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                // call VariantArray::field to get the correct Field
                variant_array.field("var"),
            ]);

            let builder = ReaderBuilder::new(Arc::new(struct_field.clone()));
            let result = builder
                .with_struct_mode(StructMode::ObjectOnly)
                .with_decoder_factory(Arc::new(VariantArrayDecoderFactory))
                .build(Cursor::new(json_input.as_bytes()))
                .unwrap()
                .next()
                .unwrap()
                .unwrap();

            assert_eq!(result.num_columns(), 2);
            let int_array = arrow_array::array::Int32Array::from(ids);
            assert_eq!(
                result
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap(),
                &int_array
            );

            let result_variant_array: VariantArray =
                VariantArray::try_new(result.column(1)).unwrap();
            let values = result_variant_array.iter().collect::<Vec<_>>();

            assert_eq!(values, variants);
        };

        do_test(
            "{\"id\": 1, \"var\": \"a\"}\n{\"id\": 2, \"var\": \"b\"}",
            vec![1, 2],
            vec![Some(Variant::from("a")), Some(Variant::from("b"))],
        );

        let mut builder = VariantBuilder::new();
        let mut object_builder = builder.new_object();
        object_builder.insert("int64", Variant::Int64(1));
        object_builder.insert("double", Variant::Double(1.0));
        object_builder.insert("null", Variant::Null);
        object_builder.insert("true", Variant::BooleanTrue);
        object_builder.insert("false", Variant::BooleanFalse);
        object_builder.insert("string", Variant::from("a"));
        object_builder.finish();
        let (metadata, value) = builder.finish();
        let variant = Variant::try_new(&metadata, &value).unwrap();

        do_test(
            "{\"id\": 1, \"var\": {\"int64\": 1, \"double\": 1.0, \"null\": null, \"true\": true, \"false\": false, \"string\": \"a\"}}",
            vec![1],
            vec![Some(variant)],
        );

        // nested structs
        let mut builder = VariantBuilder::new();
        let mut object_builder = builder.new_object();
        {
            let mut list_builder = object_builder.new_list("somelist");
            {
                let mut nested_object_builder = list_builder.new_object();
                nested_object_builder.insert("num", Variant::Int64(2));
                nested_object_builder.finish();
            }
            {
                let mut nested_object_builder = list_builder.new_object();
                nested_object_builder.insert("num", Variant::Int64(3));
                nested_object_builder.finish();
            }
            list_builder.finish();
            object_builder.insert("scalar", Variant::from("a"));
        }
        object_builder.finish();

        let (metadata, value) = builder.finish();
        let variant = Variant::try_new(&metadata, &value).unwrap();

        do_test(
            "{\"id\": 1, \"var\": {\"somelist\": [{\"num\": 2}, {\"num\": 3}], \"scalar\": \"a\"}}",
            vec![1],
            vec![Some(variant)],
        );

        let mut builder = VariantBuilder::new();
        let mut list_builder = builder.new_list();
        list_builder.append_value(Variant::Int64(1000000000000));
        list_builder.append_value(Variant::Double(std::f64::consts::E));
        list_builder.finish();
        let (metadata, value) = builder.finish();
        let variant = Variant::try_new(&metadata, &value).unwrap();

        do_test(
            "{\"id\": 1, \"var\": [1000000000000, 2.718281828459045]}",
            vec![1],
            vec![Some(variant)],
        );
    }
}
