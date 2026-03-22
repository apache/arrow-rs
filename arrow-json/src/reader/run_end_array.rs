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

use crate::reader::tape::Tape;
use crate::reader::{ArrayDecoder, DecoderContext};
use arrow_array::types::RunEndIndexType;
use arrow_array::{ArrayRef, RunArray, make_array, new_empty_array};
use arrow_buffer::{ArrowNativeType, RunEndBuffer, ScalarBuffer};
use arrow_data::ArrayData;
use arrow_data::transform::MutableArrayData;
use arrow_schema::{ArrowError, DataType};
use std::marker::PhantomData;
use std::sync::Arc;

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

        let flat_data = self.decoder.decode(tape, pos)?.to_data();

        let mut run_end_values: Vec<R::Native> = Vec::new();
        let mut mutable = MutableArrayData::new(vec![&flat_data], false, len);

        let mut run_start = 0;
        for i in 1..len {
            if !same_run(&flat_data, run_start, i) {
                let run_end = R::Native::from_usize(i).ok_or_else(|| {
                    ArrowError::JsonError(format!(
                        "Run end value {i} exceeds {:?} range",
                        R::DATA_TYPE
                    ))
                })?;
                run_end_values.push(run_end);
                mutable.extend(0, run_start, run_start + 1);
                run_start = i;
            }
        }
        let run_end = R::Native::from_usize(len).ok_or_else(|| {
            ArrowError::JsonError(format!(
                "Run end value {len} exceeds {:?} range",
                R::DATA_TYPE
            ))
        })?;
        run_end_values.push(run_end);
        mutable.extend(0, run_start, run_start + 1);

        let values = make_array(mutable.freeze());
        // SAFETY: run_ends are strictly increasing with the last value equal to len
        let run_ends = unsafe {
            RunEndBuffer::new_unchecked(ScalarBuffer::from(run_end_values), 0, len)
        };

        // SAFETY: run_ends are valid and values has the same length as run_ends
        let array =
            unsafe { RunArray::<R>::new_unchecked(self.data_type.clone(), run_ends, values) };
        Ok(Arc::new(array))
    }
}

fn same_run(data: &ArrayData, i: usize, j: usize) -> bool {
    let null_i = data.is_null(i);
    let null_j = data.is_null(j);
    if null_i != null_j {
        return false;
    }
    if null_i {
        return true;
    }
    data.slice(i, 1) == data.slice(j, 1)
}
