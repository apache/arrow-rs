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

use std::marker::PhantomData;
use std::sync::Arc;

use arrow_array::builder::BooleanBufferBuilder;
use arrow_array::{ArrayRef, GenericListArray, GenericListViewArray, OffsetSizeTrait};
use arrow_buffer::buffer::NullBuffer;
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{ArrowError, DataType, FieldRef};

use crate::reader::tape::{Tape, TapeElement};
use crate::reader::{ArrayDecoder, DecoderContext};

pub type ListArrayDecoder<O> = ListLikeArrayDecoder<O, false>;
pub type ListViewArrayDecoder<O> = ListLikeArrayDecoder<O, true>;

pub struct ListLikeArrayDecoder<O, const IS_VIEW: bool> {
    field: FieldRef,
    decoder: Box<dyn ArrayDecoder>,
    phantom: PhantomData<O>,
    is_nullable: bool,
}

impl<O: OffsetSizeTrait, const IS_VIEW: bool> ListLikeArrayDecoder<O, IS_VIEW> {
    pub fn new(
        ctx: &DecoderContext,
        data_type: &DataType,
        is_nullable: bool,
    ) -> Result<Self, ArrowError> {
        let field = match (IS_VIEW, data_type) {
            (false, DataType::List(f)) if !O::IS_LARGE => f,
            (false, DataType::LargeList(f)) if O::IS_LARGE => f,
            (true, DataType::ListView(f)) if !O::IS_LARGE => f,
            (true, DataType::LargeListView(f)) if O::IS_LARGE => f,
            _ => unreachable!(),
        };
        let decoder = ctx.make_decoder(field.data_type(), field.is_nullable())?;

        Ok(Self {
            field: field.clone(),
            decoder,
            phantom: Default::default(),
            is_nullable,
        })
    }
}

impl<O: OffsetSizeTrait, const IS_VIEW: bool> ArrayDecoder for ListLikeArrayDecoder<O, IS_VIEW> {
    fn decode(&mut self, tape: &Tape<'_>, pos: &[u32]) -> Result<ArrayRef, ArrowError> {
        let mut child_pos = Vec::with_capacity(pos.len());
        let mut offsets = Vec::with_capacity(pos.len() + 1);
        offsets.push(O::from_usize(0).unwrap());

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
                ArrowError::JsonError(format!("offset overflow decoding {}ListArray", O::PREFIX))
            })?;
            offsets.push(offset);
        }

        let values = self.decoder.decode(tape, &child_pos)?;
        let nulls = nulls.as_mut().map(|x| NullBuffer::new(x.finish()));

        if IS_VIEW {
            let mut sizes = Vec::with_capacity(offsets.len() - 1);
            for i in 1..offsets.len() {
                sizes.push(offsets[i] - offsets[i - 1]);
            }
            offsets.pop();
            let array = GenericListViewArray::<O>::try_new(
                self.field.clone(),
                ScalarBuffer::from(offsets),
                ScalarBuffer::from(sizes),
                values,
                nulls,
            )?;
            Ok(Arc::new(array))
        } else {
            // SAFETY: offsets are built monotonically starting from 0
            let offsets = unsafe { OffsetBuffer::<O>::new_unchecked(ScalarBuffer::from(offsets)) };

            let array = GenericListArray::<O>::try_new(self.field.clone(), offsets, values, nulls)?;
            Ok(Arc::new(array))
        }
    }
}
