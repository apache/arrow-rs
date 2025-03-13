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
use std::fmt::Write;

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
            // note that StringView is different that StringArray in that only
            // "long" strings (longer than 12 bytes) are stored in the buffer.
            // "short" strings are inlined into a fixed length structure.
            match tape.get(p) {
                TapeElement::String(idx) => {
                    let s = tape.get_string(idx);
                    // Only increase capacity if the string length is greater than 12 bytes
                    if s.len() > 12 {
                        data_capacity += s.len();
                    }
                }
                TapeElement::Null => {
                    // Do not increase capacity for null values
                }
                // For booleans, do not increase capacity (both "true" and "false" are less than
                // 12 bytes)
                TapeElement::True if coerce => {}
                TapeElement::False if coerce => {}
                // For Number, use the same strategy as for strings
                TapeElement::Number(idx) if coerce => {
                    let s = tape.get_string(idx);
                    if s.len() > 12 {
                        data_capacity += s.len();
                    }
                }
                // For I64, only add capacity if the absolute value is greater than 999,999,999,999
                // (the largest number that can fit in 12 bytes)
                TapeElement::I64(_) if coerce => {
                    match tape.get(p + 1) {
                        TapeElement::I32(_) => {
                            let high = match tape.get(p) {
                                TapeElement::I64(h) => h,
                                _ => unreachable!(),
                            };
                            let low = match tape.get(p + 1) {
                                TapeElement::I32(l) => l,
                                _ => unreachable!(),
                            };
                            let val = ((high as i64) << 32) | (low as u32) as i64;
                            if val.abs() > 999_999_999_999 {
                                // Only allocate capacity based on the string representation if the number is large
                                data_capacity += val.to_string().len();
                            }
                        }
                        _ => unreachable!(),
                    }
                }
                // For I32, do not increase capacity (the longest string representation is <= 12 bytes)
                TapeElement::I32(_) if coerce => {}
                // For F32 and F64, keep the existing estimate
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
        // Temporary buffer to avoid per-iteration allocation for numeric types
        let mut tmp_buf = String::new();

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
                        tmp_buf.clear();
                        // Reuse the temporary buffer instead of allocating a new String
                        write!(&mut tmp_buf, "{}", val).unwrap();
                        builder.append_value(&tmp_buf);
                    }
                    _ => unreachable!(),
                },
                TapeElement::I32(n) if coerce => {
                    tmp_buf.clear();
                    write!(&mut tmp_buf, "{}", n).unwrap();
                    builder.append_value(&tmp_buf);
                }
                TapeElement::F32(n) if coerce => {
                    tmp_buf.clear();
                    write!(&mut tmp_buf, "{}", n).unwrap();
                    builder.append_value(&tmp_buf);
                }
                TapeElement::F64(high) if coerce => match tape.get(p + 1) {
                    TapeElement::F32(low) => {
                        let val = f64::from_bits(((high as u64) << 32) | (low as u64));
                        tmp_buf.clear();
                        write!(&mut tmp_buf, "{}", val).unwrap();
                        builder.append_value(&tmp_buf);
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
