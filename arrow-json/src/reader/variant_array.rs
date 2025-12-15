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

use arrow_array::{Array, StructArray};
use parquet_variant::{ObjectFieldBuilder, Variant, VariantBuilder, VariantBuilderExt};
use parquet_variant_compute::VariantArrayBuilder;
use arrow_data::ArrayData;
use arrow_schema::ArrowError;

use crate::reader::ArrayDecoder;
use crate::reader::tape::{Tape, TapeElement};

#[derive(Default)]
pub struct VariantArrayDecoder {}

impl ArrayDecoder for VariantArrayDecoder {
    fn decode(&mut self, tape: &Tape<'_>, pos: &[u32]) -> Result<ArrayData, ArrowError> {
        let mut array_builder = VariantArrayBuilder::new(pos.len());
        for p in pos {
            let mut builder = VariantBuilder::new();
            variant_from_tape_element(&mut builder, *p, tape)?;
            let (metadata, value) = builder.finish();
            array_builder.append_value(Variant::new(&metadata, &value));
        }
        let variant_struct_array: StructArray = array_builder.build().into();
        Ok(variant_struct_array.into_data())
    }
}

fn variant_from_tape_element(builder: &mut impl VariantBuilderExt, mut p: u32, tape: &Tape) -> Result<u32, ArrowError> {
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
        TapeElement::EndObject(_u32) => unreachable!(),
        TapeElement::StartList(end_idx) => {
            let mut list_builder = builder.try_new_list()?;
            p+= 1;
            while p < end_idx {
                p = variant_from_tape_element(&mut list_builder, p, tape)?;
            }
            list_builder.finish();
        }
        TapeElement::EndList(_u32) => unreachable!(),
        TapeElement::String(idx) => builder.append_value(tape.get_string(idx)),
        TapeElement::Number(idx) => {
            let s = tape.get_string(idx);
            builder.append_value(parse_number(s)?)
        },
        TapeElement::I64(i) => builder.append_value(i),
        TapeElement::I32(i) => builder.append_value(i),
        TapeElement::F64(f) => builder.append_value(f),
        TapeElement::F32(f) => builder.append_value(f),
        TapeElement::True => builder.append_value(true),
        TapeElement::False => builder.append_value(false),
        TapeElement::Null => builder.append_value(Variant::Null),
    }
    p += 1;
    Ok(p)
}

fn parse_number<'a, 'b>(s: &'a str) -> Result<Variant<'a, 'b>, ArrowError> {
    match lexical_core::parse::<i64>(s.as_bytes()) {
        Ok(v) => Ok(Variant::from(v)),
        Err(_) => {
            match lexical_core::parse::<f64>(s.as_bytes()) {
                Ok(v) => Ok(Variant::from(v)),
                Err(_) => Err(ArrowError::JsonError(format!("failed to parse {s} as number"))),
            }
        }
    }
}