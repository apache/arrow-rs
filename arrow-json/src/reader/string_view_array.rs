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

use arrow_array::builder::GenericByteViewBuilder;
use arrow_array::types::StringViewType;
use arrow_array::Array;
use arrow_data::ArrayData;
use arrow_schema::ArrowError;

use crate::reader::tape::{Tape, TapeElement};
use crate::reader::ArrayDecoder;

const TRUE: &str = "true";
const FALSE: &str = "false";

pub struct StringViewArrayDecoder {
    coerce_primitive: bool,
}

impl StringViewArrayDecoder {
    pub fn new(coerce_primitive: bool) -> Self {
        Self { coerce_primitive }
    }
}

impl ArrayDecoder for StringViewArrayDecoder {
    fn decode(&mut self, tape: &Tape<'_>, pos: &[u32]) -> Result<ArrayData, ArrowError> {
        let coerce = self.coerce_primitive;
        let mut data_capacity = 0;
        for &p in pos {
            match tape.get(p) {
                TapeElement::String(idx) => {
                    data_capacity += tape.get_string(idx).len();
                }
                TapeElement::Null => { /* 不增加容量 */ }
                TapeElement::True if coerce => {
                    data_capacity += TRUE.len();
                }
                TapeElement::False if coerce => {
                    data_capacity += FALSE.len();
                }
                TapeElement::Number(idx) if coerce => {
                    data_capacity += tape.get_string(idx).len();
                }
                TapeElement::I64(_) if coerce => {
                    data_capacity += 10;
                }
                TapeElement::I32(_) if coerce => {
                    data_capacity += 10;
                }
                TapeElement::F32(_) if coerce => {
                    data_capacity += 10;
                }
                TapeElement::F64(_) if coerce => {
                    data_capacity += 10;
                }
                _ => {
                    return Err(tape.error(p, "string"));
                }
            }
        }

        let mut builder = GenericByteViewBuilder::<StringViewType>::with_capacity(data_capacity);

        for &p in pos {
            match tape.get(p) {
                TapeElement::String(idx) => {
                    builder.append_value(tape.get_string(idx));
                }
                TapeElement::Null => {
                    builder.append_null();
                }
                TapeElement::True if coerce => {
                    builder.append_value(TRUE);
                }
                TapeElement::False if coerce => {
                    builder.append_value(FALSE);
                }
                TapeElement::Number(idx) if coerce => {
                    builder.append_value(tape.get_string(idx));
                }
                TapeElement::I64(high) if coerce => match tape.get(p + 1) {
                    TapeElement::I32(low) => {
                        let val = ((high as i64) << 32) | (low as u32) as i64;
                        builder.append_value(val.to_string());
                    }
                    _ => unreachable!(),
                },
                TapeElement::I32(n) if coerce => {
                    builder.append_value(n.to_string());
                }
                TapeElement::F32(n) if coerce => {
                    builder.append_value(n.to_string());
                }
                TapeElement::F64(high) if coerce => match tape.get(p + 1) {
                    TapeElement::F32(low) => {
                        let val = f64::from_bits(((high as u64) << 32) | low as u64);
                        builder.append_value(val.to_string());
                    }
                    _ => unreachable!(),
                },
                _ => unreachable!(),
            }
        }

        let array = builder.finish();
        Ok(array.into_data())
    }
}
