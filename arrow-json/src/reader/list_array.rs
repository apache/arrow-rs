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
use arrow_array::OffsetSizeTrait;
use arrow_buffer::buffer::NullBuffer;
use arrow_data::{ArrayData, ArrayDataBuilder};
use arrow_schema::{ArrowError, DataType};
use std::marker::PhantomData;

pub struct ListArrayDecoder<O> {
    data_type: DataType,
    decoder: Box<dyn ArrayDecoder>,
    phantom: PhantomData<O>,
    is_nullable: bool,
}

impl<O: OffsetSizeTrait> ListArrayDecoder<O> {
    pub fn new(
        data_type: DataType,
        coerce_primitive: bool,
        strict_mode: bool,
        is_nullable: bool,
        struct_mode: StructMode,
    ) -> Result<Self, ArrowError> {
        let field = match &data_type {
            DataType::List(f) if !O::IS_LARGE => f,
            DataType::LargeList(f) if O::IS_LARGE => f,
            _ => unreachable!(),
        };
        let decoder = make_decoder(
            field.data_type().clone(),
            coerce_primitive,
            strict_mode,
            field.is_nullable(),
            struct_mode,
        )?;

        Ok(Self {
            data_type,
            decoder,
            phantom: Default::default(),
            is_nullable,
        })
    }
}

impl<O: OffsetSizeTrait> ArrayDecoder for ListArrayDecoder<O> {
    fn decode(&mut self, tape: &Tape<'_>, pos: &[u32]) -> Result<ArrayData, ArrowError> {
        let mut child_pos = Vec::with_capacity(pos.len());
        let mut offsets = BufferBuilder::<O>::new(pos.len() + 1);
        offsets.append(O::from_usize(0).unwrap());

        let mut nulls = self
            .is_nullable
            .then(|| BooleanBufferBuilder::new(pos.len()));

        for p in pos {
            let end_idx = match (tape.get(*p), nulls.as_mut()) {
                (TapeElement::StartList(end_idx), None) => end_idx,
                (TapeElement::StartList(end_idx), Some(nulls)) => {
                    nulls.append(true);
                    end_idx
                }
                (TapeElement::Null, Some(nulls)) => {
                    nulls.append(false);
                    *p + 1
                }
                _ => return Err(tape.error(*p, "[")),
            };

            let mut cur_idx = *p + 1;
            while cur_idx < end_idx {
                child_pos.push(cur_idx);

                // Advance to next field
                cur_idx = tape.next(cur_idx, "list value")?;
            }

            let offset = O::from_usize(child_pos.len()).ok_or_else(|| {
                ArrowError::JsonError(format!("offset overflow decoding {}", self.data_type))
            })?;
            offsets.append(offset)
        }

        let child_data = self.decoder.decode(tape, &child_pos)?;
        let nulls = nulls.as_mut().map(|x| NullBuffer::new(x.finish()));

        let data = ArrayDataBuilder::new(self.data_type.clone())
            .len(pos.len())
            .nulls(nulls)
            .add_buffer(offsets.finish())
            .child_data(vec![child_data]);

        // Safety
        // Validated lengths above
        Ok(unsafe { data.build_unchecked() })
    }
}
