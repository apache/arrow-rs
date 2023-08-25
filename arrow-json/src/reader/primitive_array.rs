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
use half::f16;

use crate::reader::tape::{Tape, TapeElement};
use crate::reader::ArrayDecoder;

/// A trait for JSON-specific primitive parsing logic
///
/// According to the specification unquoted fields should be parsed as a double-precision
/// floating point numbers, including scientific representation such as `2e3`
///
/// In practice, it is common to serialize numbers outside the range of an `f64` and expect
/// them to round-trip correctly. As such when parsing integers we first parse as the integer
/// and fallback to parsing as a floating point if this fails
trait ParseJsonNumber: Sized {
    fn parse(s: &[u8]) -> Option<Self>;
}

macro_rules! primitive_parse {
    ($($t:ty),+) => {
        $(impl ParseJsonNumber for $t {
            fn parse(s: &[u8]) -> Option<Self> {
                match lexical_core::parse::<Self>(s) {
                    Ok(f) => Some(f),
                    Err(_) => lexical_core::parse::<f64>(s).ok().and_then(NumCast::from),
                }
            }
        })+
    };
}

primitive_parse!(i8, i16, i32, i64, u8, u16, u32, u64);

impl ParseJsonNumber for f16 {
    fn parse(s: &[u8]) -> Option<Self> {
        lexical_core::parse::<f32>(s).ok().map(f16::from_f32)
    }
}

impl ParseJsonNumber for f32 {
    fn parse(s: &[u8]) -> Option<Self> {
        lexical_core::parse::<Self>(s).ok()
    }
}

impl ParseJsonNumber for f64 {
    fn parse(s: &[u8]) -> Option<Self> {
        lexical_core::parse::<Self>(s).ok()
    }
}

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
    P::Native: ParseJsonNumber,
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
                    let value =
                        ParseJsonNumber::parse(s.as_bytes()).ok_or_else(|| {
                            ArrowError::JsonError(format!(
                                "failed to parse {s} as {}",
                                self.data_type
                            ))
                        })?;

                    builder.append_value(value)
                }
                _ => return Err(tape.error(*p, "primitive")),
            }
        }

        Ok(builder.finish().into_data())
    }
}
