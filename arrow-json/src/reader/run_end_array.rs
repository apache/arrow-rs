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
use std::ops::Range;
use std::slice::from_ref;
use std::sync::Arc;

use arrow_array::types::RunEndIndexType;
use arrow_array::{ArrayRef, RunArray, UInt32Array, new_empty_array};
use arrow_buffer::{ArrowNativeType, RunEndBuffer, ScalarBuffer};
use arrow_ord::partition::partition;
use arrow_schema::{ArrowError, DataType};
use arrow_select::take::take;

use crate::reader::tape::Tape;
use crate::reader::{ArrayDecoder, DecoderContext};

pub struct RunEndEncodedArrayDecoder<R> {
    data_type: DataType,
    decoder: Box<dyn ArrayDecoder>,
    phantom: PhantomData<R>,
}

impl<R: RunEndIndexType> RunEndEncodedArrayDecoder<R> {
    pub fn new(
        ctx: &DecoderContext,
        data_type: &DataType,
        is_nullable: bool,
    ) -> Result<Self, ArrowError> {
        let values_field = match data_type {
            DataType::RunEndEncoded(_, v) => v,
            _ => unreachable!(),
        };
        let decoder = ctx.make_decoder(
            values_field.data_type(),
            values_field.is_nullable() || is_nullable,
        )?;

        Ok(Self {
            data_type: data_type.clone(),
            decoder,
            phantom: Default::default(),
        })
    }
}

impl<R: RunEndIndexType + Send> ArrayDecoder for RunEndEncodedArrayDecoder<R> {
    fn decode(&mut self, tape: &Tape<'_>, pos: &[u32]) -> Result<ArrayRef, ArrowError> {
        let len = pos.len();
        if len == 0 {
            return Ok(new_empty_array(&self.data_type));
        }

        let flat_array = self.decoder.decode(tape, pos)?;

        let partitions = partition(from_ref(&flat_array))?;
        let size = partitions.len();
        let mut run_ends = Vec::with_capacity(size);
        let mut value_indices = Vec::with_capacity(size);

        for Range { start, end } in partitions.ranges() {
            let run_end = R::Native::from_usize(end).ok_or_else(|| {
                ArrowError::JsonError(format!(
                    "Run end value {end} exceeds {:?} range",
                    R::DATA_TYPE
                ))
            })?;
            run_ends.push(run_end);
            value_indices.push(start);
        }

        let indices = UInt32Array::from_iter_values(value_indices.into_iter().map(|i| i as u32));
        let values = take(flat_array.as_ref(), &indices, None)?;

        // SAFETY: run_ends are strictly increasing with the last value equal to len
        let run_ends = unsafe { RunEndBuffer::new_unchecked(ScalarBuffer::from(run_ends), 0, len) };

        // SAFETY: run_ends are valid and values has the same length as run_ends
        let array =
            unsafe { RunArray::<R>::new_unchecked(self.data_type.clone(), run_ends, values) };
        Ok(Arc::new(array))
    }
}
