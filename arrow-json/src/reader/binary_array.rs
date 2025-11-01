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

use arrow_array::builder::{
    BinaryViewBuilder, FixedSizeBinaryBuilder, GenericBinaryBuilder, GenericStringBuilder,
};
use arrow_array::{Array, GenericStringArray, OffsetSizeTrait};
use arrow_data::ArrayData;
use arrow_schema::ArrowError;
use std::marker::PhantomData;

use crate::reader::ArrayDecoder;
use crate::reader::tape::{Tape, TapeElement};

/// Decode a hex-encoded string into bytes
fn decode_hex_string(hex_string: &str) -> Result<Vec<u8>, ArrowError> {
    let mut decoded = Vec::with_capacity(hex_string.len() / 2);
    for substr in hex_string.as_bytes().chunks(2) {
        let str = std::str::from_utf8(substr).map_err(|e| {
            ArrowError::JsonError(format!("invalid utf8 in hex encoded binary data: {e}"))
        })?;
        let byte = u8::from_str_radix(str, 16).map_err(|e| {
            ArrowError::JsonError(format!("invalid hex encoding in binary data: {e}"))
        })?;
        decoded.push(byte);
    }
    Ok(decoded)
}

#[derive(Default)]
pub struct BinaryArrayDecoder<O: OffsetSizeTrait> {
    phantom: PhantomData<O>,
}

impl<O: OffsetSizeTrait> ArrayDecoder for BinaryArrayDecoder<O> {
    fn decode(&mut self, tape: &Tape<'_>, pos: &[u32]) -> Result<ArrayData, ArrowError> {
        let data_capacity = estimate_data_capacity(tape, pos)?;

        if O::from_usize(data_capacity).is_none() {
            return Err(ArrowError::JsonError(format!(
                "offset overflow decoding {}",
                GenericStringArray::<O>::DATA_TYPE
            )));
        }

        let mut builder = GenericBinaryBuilder::<O>::with_capacity(pos.len(), data_capacity);

        GenericStringBuilder::<O>::with_capacity(pos.len(), data_capacity);

        for p in pos {
            match tape.get(*p) {
                TapeElement::String(idx) => {
                    let string = tape.get_string(idx);
                    let decoded = decode_hex_string(string)?;
                    builder.append_value(&decoded);
                }
                TapeElement::Null => builder.append_null(),
                _ => unreachable!(),
            }
        }

        Ok(builder.finish().into_data())
    }
}

#[derive(Default)]
pub struct FixedSizeBinaryArrayDecoder {
    len: i32,
}

impl FixedSizeBinaryArrayDecoder {
    pub fn new(len: i32) -> Self {
        Self { len }
    }
}

impl ArrayDecoder for FixedSizeBinaryArrayDecoder {
    fn decode(&mut self, tape: &Tape<'_>, pos: &[u32]) -> Result<ArrayData, ArrowError> {
        let mut builder = FixedSizeBinaryBuilder::with_capacity(pos.len(), self.len);

        for p in pos {
            match tape.get(*p) {
                TapeElement::String(idx) => {
                    let string = tape.get_string(idx);
                    let decoded = decode_hex_string(string)?;
                    builder.append_value(&decoded)?;
                }
                TapeElement::Null => builder.append_null(),
                _ => unreachable!(),
            }
        }

        Ok(builder.finish().into_data())
    }
}

#[derive(Default)]
pub struct BinaryViewDecoder {}

impl ArrayDecoder for BinaryViewDecoder {
    fn decode(&mut self, tape: &Tape<'_>, pos: &[u32]) -> Result<ArrayData, ArrowError> {
        let data_capacity = estimate_data_capacity(tape, pos)?;
        let mut builder = BinaryViewBuilder::with_capacity(data_capacity);

        for p in pos {
            match tape.get(*p) {
                TapeElement::String(idx) => {
                    let string = tape.get_string(idx);
                    let decoded = decode_hex_string(string)?;
                    builder.append_value(&decoded);
                }
                TapeElement::Null => builder.append_null(),
                _ => unreachable!(),
            }
        }

        Ok(builder.finish().into_data())
    }
}

fn estimate_data_capacity(tape: &Tape<'_>, pos: &[u32]) -> Result<usize, ArrowError> {
    let mut data_capacity = 0;
    for p in pos {
        match tape.get(*p) {
            TapeElement::String(idx) => {
                let string_len = tape.get_string(idx).len();
                // two hex characters represent one byte
                let decoded_len = string_len / 2;
                data_capacity += decoded_len;
            }
            TapeElement::Null => {}
            _ => {
                return Err(tape.error(*p, "binary data encoded as string"));
            }
        }
    }
    Ok(data_capacity)
}
