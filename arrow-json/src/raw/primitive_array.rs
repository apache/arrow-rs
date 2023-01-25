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

use num::NumCast;
use std::marker::PhantomData;

use arrow_array::builder::PrimitiveBuilder;
use arrow_array::{Array, ArrowPrimitiveType};
use arrow_cast::parse::Parser;
use arrow_data::ArrayData;
use arrow_schema::{ArrowError, DataType};

use crate::raw::tape::{Tape, TapeElement};
use crate::raw::{tape_error, ArrayDecoder};

pub struct PrimitiveArrayDecoder<P: ArrowPrimitiveType> {
    data_type: DataType,
    // Invariant and Send
    phantom: PhantomData<fn(P) -> P>,
}

impl<P: ArrowPrimitiveType> PrimitiveArrayDecoder<P> {
    pub fn new(data_type: DataType) -> Self {
        Self {
            data_type,
            phantom: Default::default(),
        }
    }
}

impl<P> ArrayDecoder for PrimitiveArrayDecoder<P>
where
    P: ArrowPrimitiveType + Parser,
    P::Native: NumCast,
{
    fn decode(&mut self, tape: &Tape<'_>, pos: &[u32]) -> Result<ArrayData, ArrowError> {
        let mut builder = PrimitiveBuilder::<P>::with_capacity(pos.len())
            .with_data_type(self.data_type.clone());

        for p in pos {
            match tape.get(*p) {
                TapeElement::Null => builder.append_null(),
                TapeElement::String(idx) => {
                    let s = tape.get_string(idx);
                    let value = P::parse(s).ok_or_else(|| {
                        ArrowError::JsonError(format!(
                            "failed to parse \"{s}\" as {}",
                            self.data_type
                        ))
                    })?;

                    builder.append_value(value)
                }
                TapeElement::Number(idx) => {
                    let s = tape.get_string(idx);
                    let value = lexical_core::parse::<f64>(s.as_bytes())
                        .ok()
                        .and_then(NumCast::from)
                        .ok_or_else(|| {
                            ArrowError::JsonError(format!(
                                "failed to parse {s} as {}",
                                self.data_type
                            ))
                        })?;

                    builder.append_value(value)
                }
                d => return Err(tape_error(d, "primitive")),
            }
        }

        Ok(builder.finish().into_data())
    }
}
