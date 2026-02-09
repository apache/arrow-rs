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
use arrow_array::Array;
use arrow_data::transform::MutableArrayData;
use arrow_data::{ArrayData, ArrayDataBuilder};
use arrow_schema::{ArrowError, DataType};

pub struct RunEndEncodedArrayDecoder {
    data_type: DataType,
    decoder: Box<dyn ArrayDecoder>,
}

impl RunEndEncodedArrayDecoder {
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
        })
    }
}

impl ArrayDecoder for RunEndEncodedArrayDecoder {
    fn decode(&mut self, tape: &Tape<'_>, pos: &[u32]) -> Result<ArrayData, ArrowError> {
        let run_ends_type = match &self.data_type {
            DataType::RunEndEncoded(r, _) => r.data_type(),
            _ => unreachable!(),
        };

        let len = pos.len();
        if len == 0 {
            let empty_run_ends = new_empty_run_ends(run_ends_type);
            let empty_values = self.decoder.decode(tape, &[])?;
            let data = ArrayDataBuilder::new(self.data_type.clone())
                .len(0)
                .add_child_data(empty_run_ends)
                .add_child_data(empty_values);
            // Safety:
            // Valid by construction
            return Ok(unsafe { data.build_unchecked() });
        }

        let flat_data = self.decoder.decode(tape, pos)?;

        let mut run_ends: Vec<i64> = Vec::new();
        let mut mutable = MutableArrayData::new(vec![&flat_data], false, len);

        let mut run_start = 0;
        for i in 1..len {
            if !same_run(&flat_data, run_start, i) {
                run_ends.push(i as i64);
                mutable.extend(0, run_start, run_start + 1);
                run_start = i;
            }
        }
        run_ends.push(len as i64);
        mutable.extend(0, run_start, run_start + 1);

        let values_data = mutable.freeze();
        let run_ends_data = build_run_ends_array(run_ends_type, run_ends)?;

        let data = ArrayDataBuilder::new(self.data_type.clone())
            .len(len)
            .add_child_data(run_ends_data)
            .add_child_data(values_data);

        // Safety:
        // Valid by construction
        Ok(unsafe { data.build_unchecked() })
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

fn build_run_ends_array(dt: &DataType, run_ends: Vec<i64>) -> Result<ArrayData, ArrowError> {
    match dt {
        DataType::Int16 => {
            let values = run_ends
                .iter()
                .map(|&v| {
                    i16::try_from(v).map_err(|_| {
                        ArrowError::JsonError(format!("Run end value {v} exceeds Int16 range"))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(arrow_array::Int16Array::from(values).into_data())
        }
        DataType::Int32 => {
            let values = run_ends
                .iter()
                .map(|&v| {
                    i32::try_from(v).map_err(|_| {
                        ArrowError::JsonError(format!("Run end value {v} exceeds Int32 range"))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(arrow_array::Int32Array::from(values).into_data())
        }
        DataType::Int64 => Ok(arrow_array::Int64Array::from(run_ends).into_data()),
        _ => unreachable!(),
    }
}

fn new_empty_run_ends(dt: &DataType) -> ArrayData {
    match dt {
        DataType::Int16 => arrow_array::Int16Array::from(Vec::<i16>::new()).into_data(),
        DataType::Int32 => arrow_array::Int32Array::from(Vec::<i32>::new()).into_data(),
        DataType::Int64 => arrow_array::Int64Array::from(Vec::<i64>::new()).into_data(),
        _ => unreachable!(),
    }
}
