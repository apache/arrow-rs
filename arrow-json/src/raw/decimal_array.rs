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

use arrow_array::builder::PrimitiveBuilder;
use arrow_array::types::DecimalType;
use arrow_array::Array;
use arrow_cast::parse::parse_decimal;
use arrow_data::ArrayData;
use arrow_schema::ArrowError;

use crate::raw::tape::{Tape, TapeElement};
use crate::raw::{tape_error, ArrayDecoder};

pub struct DecimalArrayDecoder<D: DecimalType> {
    precision: u8,
    scale: i8,
    // Invariant and Send
    phantom: PhantomData<fn(D) -> D>,
}

impl<D: DecimalType> DecimalArrayDecoder<D> {
    pub fn new(precision: u8, scale: i8) -> Self {
        Self {
            precision,
            scale,
            phantom: PhantomData,
        }
    }
}

impl<D> ArrayDecoder for DecimalArrayDecoder<D>
where
    D: DecimalType,
{
    fn decode(&mut self, tape: &Tape<'_>, pos: &[u32]) -> Result<ArrayData, ArrowError> {
        let mut builder = PrimitiveBuilder::<D>::with_capacity(pos.len());

        for p in pos {
            match tape.get(*p) {
                TapeElement::Null => builder.append_null(),
                TapeElement::String(idx) => {
                    let s = tape.get_string(idx);
                    let value = parse_decimal::<D>(s, self.precision, self.scale)?;
                    builder.append_value(value)
                }
                TapeElement::Number(idx) => {
                    let s = tape.get_string(idx);
                    let value = parse_decimal::<D>(s, self.precision, self.scale)?;
                    builder.append_value(value)
                }
                d => return Err(tape_error(d, "decimal")),
            }
        }

        Ok(builder
            .finish()
            .with_precision_and_scale(self.precision, self.scale)?
            .into_data())
    }
}
