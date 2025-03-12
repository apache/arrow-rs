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

use arrow_array::builder::GenericStringBuilder;
use arrow_array::{Array, GenericStringArray, OffsetSizeTrait};
use arrow_data::ArrayData;
use arrow_schema::ArrowError;
use std::marker::PhantomData;
use std::io::Write;

use crate::reader::tape::{Tape, TapeElement};
use crate::reader::ArrayDecoder;

const TRUE: &str = "true";
const FALSE: &str = "false";

pub struct StringArrayDecoder<O: OffsetSizeTrait> {
    coerce_primitive: bool,
    phantom: PhantomData<O>,
    number_buffer: Vec<u8>,
}

impl<O: OffsetSizeTrait> StringArrayDecoder<O> {
    pub fn new(coerce_primitive: bool) -> Self {
        Self {
            coerce_primitive,
            phantom: Default::default(),
            number_buffer: Vec::with_capacity(32),
        }
    }

    fn write_number<T: std::fmt::Display>(&mut self, n: T) -> &str {
        self.number_buffer.clear();
        write!(&mut self.number_buffer, "{}", n).unwrap();
        // SAFETY: We only write ASCII characters (digits, signs, decimal points,
        // exponent symbols) into `number_buffer`, which are guaranteed valid UTF-8.
        unsafe { std::str::from_utf8_unchecked(&self.number_buffer) }
    }
}

impl<O: OffsetSizeTrait> ArrayDecoder for StringArrayDecoder<O> {
    fn decode(&mut self, tape: &Tape<'_>, pos: &[u32]) -> Result<ArrayData, ArrowError> {
        let coerce_primitive = self.coerce_primitive;

        let mut data_capacity = 0;
        for p in pos {
            match tape.get(*p) {
                TapeElement::String(idx) => {
                    data_capacity += tape.get_string(idx).len();
                }
                TapeElement::Null => {}
                TapeElement::True if coerce_primitive => {
                    data_capacity += TRUE.len();
                }
                TapeElement::False if coerce_primitive => {
                    data_capacity += FALSE.len();
                }
                TapeElement::Number(idx) if coerce_primitive => {
                    data_capacity += tape.get_string(idx).len();
                }
                TapeElement::I64(_)
                | TapeElement::I32(_)
                | TapeElement::F64(_)
                | TapeElement::F32(_)
                    if coerce_primitive =>
                {
                    // An arbitrary estimate
                    data_capacity += 10;
                }
                _ => {
                    return Err(tape.error(*p, "string"));
                }
            }
        }

        if O::from_usize(data_capacity).is_none() {
            return Err(ArrowError::JsonError(format!(
                "offset overflow decoding {}",
                GenericStringArray::<O>::DATA_TYPE
            )));
        }

        let mut builder = GenericStringBuilder::<O>::with_capacity(pos.len(), data_capacity);

        for p in pos {
            match tape.get(*p) {
                TapeElement::String(idx) => {
                    builder.append_value(tape.get_string(idx));
                }
                TapeElement::Null => builder.append_null(),
                TapeElement::True if coerce_primitive => {
                    builder.append_value(TRUE);
                }
                TapeElement::False if coerce_primitive => {
                    builder.append_value(FALSE);
                }
                TapeElement::Number(idx) if coerce_primitive => {
                    let s = self.write_number(tape.get_string(idx));
                    builder.append_value(s);
                }
                TapeElement::I64(high) if coerce_primitive => match tape.get(p + 1) {
                    TapeElement::I32(low) => {
                        let val = ((high as i64) << 32) | (low as u32) as i64;
                        let s = self.write_number(val);
                        builder.append_value(s);
                    }
                    _ => unreachable!(),
                },
                TapeElement::I32(n) if coerce_primitive => {
                    let s = self.write_number(n);
                    builder.append_value(s);
                }
                TapeElement::F32(n) if coerce_primitive => {
                    let s = self.write_number(n);
                    builder.append_value(s);
                }
                TapeElement::F64(high) if coerce_primitive => match tape.get(p + 1) {
                    TapeElement::F32(low) => {
                        let val = f64::from_bits(((high as u64) << 32) | low as u64);
                        let s = self.write_number(val);
                        builder.append_value(s);
                    }
                    _ => unreachable!(),
                },
                _ => unreachable!(),
            }
        }

        Ok(builder.finish().into_data())
    }
}
