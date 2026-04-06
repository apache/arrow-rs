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

use std::sync::Arc;

use arrow_array::builder::{BooleanBufferBuilder, BufferBuilder};
use arrow_array::{ArrayRef, MapArray, StructArray};
use arrow_buffer::buffer::NullBuffer;
use arrow_buffer::{ArrowNativeType, OffsetBuffer, ScalarBuffer};
use arrow_schema::{ArrowError, DataType, FieldRef, Fields};

use crate::reader::tape::{Tape, TapeElement};
use crate::reader::{ArrayDecoder, DecoderContext};

pub struct MapArrayDecoder {
    entries_field: FieldRef,
    key_value_fields: Fields,
    ordered: bool,
    keys: Box<dyn ArrayDecoder>,
    values: Box<dyn ArrayDecoder>,
    ignore_type_conflicts: bool,
    is_nullable: bool,
}

impl MapArrayDecoder {
    pub fn new(
        ctx: &DecoderContext,
        data_type: &DataType,
        is_nullable: bool,
    ) -> Result<Self, ArrowError> {
        let (entries_field, ordered) = match data_type {
            DataType::Map(_, true) => {
                return Err(ArrowError::NotYetImplemented(
                    "Decoding MapArray with sorted fields".to_string(),
                ));
            }
            DataType::Map(f, ordered) => (f.clone(), *ordered),
            _ => unreachable!(),
        };

        let key_value_fields = match entries_field.data_type() {
            DataType::Struct(fields) if fields.len() == 2 => fields.clone(),
            d => {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "MapArray must contain struct with two fields, got {d}"
                )));
            }
        };

        let keys = ctx.make_decoder(
            key_value_fields[0].data_type(),
            key_value_fields[0].is_nullable(),
        )?;
        let values = ctx.make_decoder(
            key_value_fields[1].data_type(),
            key_value_fields[1].is_nullable(),
        )?;

        Ok(Self {
            entries_field,
            key_value_fields,
            ordered,
            keys,
            values,
            ignore_type_conflicts: ctx.ignore_type_conflicts(),
            is_nullable,
        })
    }
}

impl ArrayDecoder for MapArrayDecoder {
    fn decode(&mut self, tape: &Tape<'_>, pos: &[u32]) -> Result<ArrayRef, ArrowError> {
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
                (_, Some(nulls)) if self.ignore_type_conflicts => {
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
                ArrowError::JsonError("offset overflow decoding MapArray".to_string())
            })?;
            offsets.append(offset)
        }

        assert_eq!(key_pos.len(), value_pos.len());

        let key_array = self.keys.decode(tape, &key_pos)?;
        let value_array = self.values.decode(tape, &value_pos)?;

        // SAFETY: fields/arrays match the schema, lengths are equal, no nulls
        let entries = unsafe {
            StructArray::new_unchecked_with_length(
                self.key_value_fields.clone(),
                vec![key_array, value_array],
                None,
                key_pos.len(),
            )
        };

        let nulls = nulls.as_mut().map(|x| NullBuffer::new(x.finish()));
        // SAFETY: offsets are built monotonically starting from 0
        let offsets = unsafe { OffsetBuffer::new_unchecked(ScalarBuffer::from(offsets.finish())) };

        let array = MapArray::try_new(
            self.entries_field.clone(),
            offsets,
            entries,
            nulls,
            self.ordered,
        )?;
        Ok(Arc::new(array))
    }
}
