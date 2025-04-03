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

use chrono::TimeZone;
use std::marker::PhantomData;

use arrow_array::builder::PrimitiveBuilder;
use arrow_array::types::ArrowTimestampType;
use arrow_array::Array;
use arrow_cast::parse::string_to_datetime;
use arrow_data::ArrayData;
use arrow_schema::{ArrowError, DataType, TimeUnit};

use crate::reader::tape::{Tape, TapeElement};
use crate::reader::ArrayDecoder;

/// A specialized [`ArrayDecoder`] for timestamps
pub struct TimestampArrayDecoder<P: ArrowTimestampType, Tz: TimeZone> {
    data_type: DataType,
    timezone: Tz,
    // Invariant and Send
    phantom: PhantomData<fn(P) -> P>,
}

impl<P: ArrowTimestampType, Tz: TimeZone> TimestampArrayDecoder<P, Tz> {
    pub fn new(data_type: DataType, timezone: Tz) -> Self {
        Self {
            data_type,
            timezone,
            phantom: Default::default(),
        }
    }
}

impl<P, Tz> ArrayDecoder for TimestampArrayDecoder<P, Tz>
where
    P: ArrowTimestampType,
    Tz: TimeZone + Send,
{
    fn decode(&mut self, tape: &Tape<'_>, pos: &[u32]) -> Result<ArrayData, ArrowError> {
        let mut builder =
            PrimitiveBuilder::<P>::with_capacity(pos.len()).with_data_type(self.data_type.clone());

        for p in pos {
            match tape.get(*p) {
                TapeElement::Null => builder.append_null(),
                TapeElement::String(idx) => {
                    let s = tape.get_string(idx);
                    let date = string_to_datetime(&self.timezone, s).map_err(|e| {
                        ArrowError::JsonError(format!(
                            "failed to parse \"{s}\" as {}: {}",
                            self.data_type, e
                        ))
                    })?;

                    let value = match P::UNIT {
                        TimeUnit::Second => date.timestamp(),
                        TimeUnit::Millisecond => date.timestamp_millis(),
                        TimeUnit::Microsecond => date.timestamp_micros(),
                        TimeUnit::Nanosecond => date.timestamp_nanos_opt().ok_or_else(|| {
                            ArrowError::ParseError(format!(
                                "{} would overflow 64-bit signed nanoseconds",
                                date.to_rfc3339(),
                            ))
                        })?,
                    };
                    builder.append_value(value)
                }
                TapeElement::Number(idx) => {
                    let s = tape.get_string(idx);
                    let b = s.as_bytes();
                    let value = lexical_core::parse::<i64>(b)
                        .or_else(|_| lexical_core::parse::<f64>(b).map(|x| x as i64))
                        .map_err(|_| {
                            ArrowError::JsonError(format!(
                                "failed to parse {s} as {}",
                                self.data_type
                            ))
                        })?;

                    builder.append_value(value)
                }
                TapeElement::I32(v) => builder.append_value(v as i64),
                TapeElement::I64(high) => match tape.get(p + 1) {
                    TapeElement::I32(low) => {
                        builder.append_value(((high as i64) << 32) | (low as u32) as i64)
                    }
                    _ => unreachable!(),
                },
                _ => return Err(tape.error(*p, "primitive")),
            }
        }

        Ok(builder.finish().into_data())
    }
}
