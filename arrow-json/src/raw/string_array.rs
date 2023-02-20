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

use crate::raw::tape::{Tape, TapeElement};
use crate::raw::{tape_error, ArrayDecoder};

const TRUE: &str = "true";
const FALSE: &str = "false";

#[derive(Default)]
pub struct StringArrayDecoder<O: OffsetSizeTrait> {
    phantom: PhantomData<O>,
}

impl<O: OffsetSizeTrait> ArrayDecoder for StringArrayDecoder<O> {
    fn decode(&mut self, tape: &Tape<'_>, pos: &[u32]) -> Result<ArrayData, ArrowError> {
        let coerce_primitive = tape.coerce_primitive();

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
                d => return Err(tape_error(d, "string")),
            }
        }

        if O::from_usize(data_capacity).is_none() {
            return Err(ArrowError::JsonError(format!(
                "offset overflow decoding {}",
                GenericStringArray::<O>::DATA_TYPE
            )));
        }

        let mut builder =
            GenericStringBuilder::<O>::with_capacity(pos.len(), data_capacity);

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
                    builder.append_value(tape.get_string(idx));
                }
                _ => unreachable!(),
            }
        }

        Ok(builder.finish().into_data())
    }
}
