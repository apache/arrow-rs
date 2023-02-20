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

use crate::raw::tape::{Tape, TapeElement};
use crate::raw::{make_decoder, tape_error, ArrayDecoder};
use arrow_array::builder::BooleanBufferBuilder;
use arrow_data::{ArrayData, ArrayDataBuilder};
use arrow_schema::{ArrowError, DataType, Field};

pub struct StructArrayDecoder {
    data_type: DataType,
    decoders: Vec<Box<dyn ArrayDecoder>>,
    is_nullable: bool,
}

impl StructArrayDecoder {
    pub fn new(
        data_type: DataType,
        coerce_primitive: bool,
        is_nullable: bool,
    ) -> Result<Self, ArrowError> {
        let decoders = struct_fields(&data_type)
            .iter()
            .map(|f| {
                make_decoder(f.data_type().clone(), coerce_primitive, f.is_nullable())
            })
            .collect::<Result<Vec<_>, ArrowError>>()?;

        Ok(Self {
            data_type,
            decoders,
            is_nullable,
        })
    }
}

impl ArrayDecoder for StructArrayDecoder {
    fn decode(&mut self, tape: &Tape<'_>, pos: &[u32]) -> Result<ArrayData, ArrowError> {
        let fields = struct_fields(&self.data_type);
        let mut child_pos: Vec<_> =
            (0..fields.len()).map(|_| vec![0; pos.len()]).collect();

        let mut null_count = 0;
        let mut nulls = self
            .is_nullable
            .then(|| BooleanBufferBuilder::new(pos.len()));

        for (row, p) in pos.iter().enumerate() {
            let end_idx = match (tape.get(*p), nulls.as_mut()) {
                (TapeElement::StartObject(end_idx), None) => end_idx,
                (TapeElement::StartObject(end_idx), Some(nulls)) => {
                    nulls.append(true);
                    end_idx
                }
                (TapeElement::Null, Some(nulls)) => {
                    nulls.append(false);
                    null_count += 1;
                    continue;
                }
                (d, _) => return Err(tape_error(d, "{")),
            };

            let mut cur_idx = *p + 1;
            while cur_idx < end_idx {
                // Read field name
                let field_name = match tape.get(cur_idx) {
                    TapeElement::String(s) => tape.get_string(s),
                    d => return Err(tape_error(d, "field name")),
                };

                // Update child pos if match found
                if let Some(field_idx) =
                    fields.iter().position(|x| x.name() == field_name)
                {
                    child_pos[field_idx][row] = cur_idx + 1;
                }

                // Advance to next field
                cur_idx = tape
                    .next(cur_idx + 1)
                    .map_err(|d| tape_error(d, "field value"))?;
            }
        }

        let child_data = self
            .decoders
            .iter_mut()
            .zip(child_pos)
            .map(|(d, pos)| d.decode(tape, &pos))
            .collect::<Result<Vec<_>, ArrowError>>()?;

        // Sanity check
        child_data
            .iter()
            .for_each(|x| assert_eq!(x.len(), pos.len()));

        let data = ArrayDataBuilder::new(self.data_type.clone())
            .len(pos.len())
            .null_count(null_count)
            .null_bit_buffer(nulls.as_mut().map(|x| x.finish()))
            .child_data(child_data);

        // Safety
        // Validated lengths above
        Ok(unsafe { data.build_unchecked() })
    }
}

fn struct_fields(data_type: &DataType) -> &[Field] {
    match &data_type {
        DataType::Struct(f) => f,
        _ => unreachable!(),
    }
}
