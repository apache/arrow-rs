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
use arrow_array::builder::BufferBuilder;
use arrow_array::OffsetSizeTrait;
use arrow_data::{ArrayData, ArrayDataBuilder};
use arrow_schema::{ArrowError, DataType};
use std::marker::PhantomData;

pub struct ListArrayDecoder<O> {
    data_type: DataType,
    decoder: Box<dyn ArrayDecoder>,
    phantom: PhantomData<O>,
}

impl<O: OffsetSizeTrait> ListArrayDecoder<O> {
    pub fn new(data_type: DataType) -> Result<Self, ArrowError> {
        let field_type = match &data_type {
            DataType::List(f) if !O::IS_LARGE => f.data_type().clone(),
            DataType::LargeList(f) if O::IS_LARGE => f.data_type().clone(),
            _ => unreachable!(),
        };
        let decoder = make_decoder(field_type)?;

        Ok(Self {
            data_type,
            decoder,
            phantom: Default::default(),
        })
    }
}

impl<O: OffsetSizeTrait> ArrayDecoder for ListArrayDecoder<O> {
    fn decode(&mut self, tape: &Tape<'_>, pos: &[u32]) -> Result<ArrayData, ArrowError> {
        let mut child_pos = Vec::with_capacity(pos.len());
        let mut offsets = BufferBuilder::<O>::new(pos.len() + 1);
        offsets.append(O::from_usize(0).unwrap());

        for p in pos {
            // TODO: Handle nulls
            let end_idx = match tape.get(*p) {
                TapeElement::StartList(end_idx) => end_idx,
                d => return Err(tape_error(d, "[")),
            };

            let mut cur_idx = *p + 1;
            while cur_idx < end_idx {
                child_pos.push(cur_idx);

                // Advance to next field
                cur_idx = match tape.get(cur_idx) {
                    TapeElement::String(_)
                    | TapeElement::Number(_)
                    | TapeElement::True
                    | TapeElement::False
                    | TapeElement::Null => cur_idx + 1,
                    TapeElement::StartList(end_idx) => end_idx + 1,
                    TapeElement::StartObject(end_idx) => end_idx + 1,
                    d => return Err(tape_error(d, "list value")),
                }
            }

            let offset = O::from_usize(child_pos.len())
                .ok_or_else(|| ArrowError::JsonError(format!("offset overflow")))?;
            offsets.append(offset)
        }

        let child_data = self.decoder.decode(tape, &child_pos).unwrap();

        let data = ArrayDataBuilder::new(self.data_type.clone())
            .len(pos.len())
            .add_buffer(offsets.finish())
            .child_data(vec![child_data]);

        // Safety
        // Validated lengths above
        Ok(unsafe { data.build_unchecked() })
    }
}
