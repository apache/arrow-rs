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

use crate::reader::tape::{Tape, TapeElement};
use crate::reader::{make_decoder, ArrayDecoder};
use crate::StructMode;
use arrow_array::builder::{BooleanBufferBuilder, BufferBuilder};
use arrow_buffer::buffer::NullBuffer;
use arrow_buffer::ArrowNativeType;
use arrow_data::{ArrayData, ArrayDataBuilder};
use arrow_schema::{ArrowError, DataType};

pub struct MapArrayDecoder {
    data_type: DataType,
    keys: Box<dyn ArrayDecoder>,
    values: Box<dyn ArrayDecoder>,
    is_nullable: bool,
}

impl MapArrayDecoder {
    pub fn new(
        data_type: DataType,
        coerce_primitive: bool,
        strict_mode: bool,
        is_nullable: bool,
        struct_mode: StructMode,
    ) -> Result<Self, ArrowError> {
        let fields = match &data_type {
            DataType::Map(_, true) => {
                return Err(ArrowError::NotYetImplemented(
                    "Decoding MapArray with sorted fields".to_string(),
                ))
            }
            DataType::Map(f, _) => match f.data_type() {
                DataType::Struct(fields) if fields.len() == 2 => fields,
                d => {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "MapArray must contain struct with two fields, got {d}"
                    )))
                }
            },
            _ => unreachable!(),
        };

        let keys = make_decoder(
            fields[0].data_type().clone(),
            coerce_primitive,
            strict_mode,
            fields[0].is_nullable(),
            struct_mode,
        )?;
        let values = make_decoder(
            fields[1].data_type().clone(),
            coerce_primitive,
            strict_mode,
            fields[1].is_nullable(),
            struct_mode,
        )?;

        Ok(Self {
            data_type,
            keys,
            values,
            is_nullable,
        })
    }
}

impl ArrayDecoder for MapArrayDecoder {
    fn decode(&mut self, tape: &Tape<'_>, pos: &[u32]) -> Result<ArrayData, ArrowError> {
        let s = match &self.data_type {
            DataType::Map(f, _) => match f.data_type() {
                s @ DataType::Struct(_) => s,
                _ => unreachable!(),
            },
            _ => unreachable!(),
        };

        let mut offsets = BufferBuilder::<i32>::new(pos.len() + 1);
        offsets.append(0);

        let mut key_pos = Vec::with_capacity(pos.len());
        let mut value_pos = Vec::with_capacity(pos.len());

        let mut nulls = self
            .is_nullable
            .then(|| BooleanBufferBuilder::new(pos.len()));

        for p in pos.iter().copied() {
            let end_idx = match (tape.get(p), nulls.as_mut()) {
                (TapeElement::StartObject(end_idx), None) => end_idx,
                (TapeElement::StartObject(end_idx), Some(nulls)) => {
                    nulls.append(true);
                    end_idx
                }
                (TapeElement::Null, Some(nulls)) => {
                    nulls.append(false);
                    p + 1
                }
                _ => return Err(tape.error(p, "{")),
            };

            let mut cur_idx = p + 1;
            while cur_idx < end_idx {
                let key = cur_idx;
                let value = tape.next(key, "map key")?;
                cur_idx = tape.next(value, "map value")?;

                key_pos.push(key);
                value_pos.push(value);
            }

            let offset = i32::from_usize(key_pos.len()).ok_or_else(|| {
                ArrowError::JsonError(format!("offset overflow decoding {}", self.data_type))
            })?;
            offsets.append(offset)
        }

        assert_eq!(key_pos.len(), value_pos.len());

        let key_data = self.keys.decode(tape, &key_pos)?;
        let value_data = self.values.decode(tape, &value_pos)?;

        let struct_data = ArrayDataBuilder::new(s.clone())
            .len(key_pos.len())
            .child_data(vec![key_data, value_data]);

        // Safety:
        // Valid by construction
        let struct_data = unsafe { struct_data.build_unchecked() };

        let nulls = nulls.as_mut().map(|x| NullBuffer::new(x.finish()));

        let builder = ArrayDataBuilder::new(self.data_type.clone())
            .len(pos.len())
            .buffers(vec![offsets.finish()])
            .nulls(nulls)
            .child_data(vec![struct_data]);

        // Safety:
        // Valid by construction
        Ok(unsafe { builder.build_unchecked() })
    }
}
