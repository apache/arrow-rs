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

use crate::reader::tape::{Tape, TapeElement};
use crate::reader::ArrayDecoder;

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
                TapeElement::I64(high) => match tape.get(*p + 1) {
                    TapeElement::I32(low) => {
                        let val = (((high as i64) << 32) | (low as u32) as i64).to_string();
                        let value = parse_decimal::<D>(&val, self.precision, self.scale)?;
                        builder.append_value(value)
                    }
                    _ => unreachable!(),
                },
                TapeElement::I32(val) => {
                    let s = val.to_string();
                    let value = parse_decimal::<D>(&s, self.precision, self.scale)?;
                    builder.append_value(value)
                }
                TapeElement::F64(high) => match tape.get(*p + 1) {
                    TapeElement::F32(low) => {
                        let val = f64::from_bits(((high as u64) << 32) | low as u64).to_string();
                        let value = parse_decimal::<D>(&val, self.precision, self.scale)?;
                        builder.append_value(value)
                    }
                    _ => unreachable!(),
                },
                TapeElement::F32(val) => {
                    let s = f32::from_bits(val).to_string();
                    let value = parse_decimal::<D>(&s, self.precision, self.scale)?;
                    builder.append_value(value)
                }
                _ => return Err(tape.error(*p, "decimal")),
            }
        }

        Ok(builder
            .finish()
            .with_precision_and_scale(self.precision, self.scale)?
            .into_data())
    }
}
