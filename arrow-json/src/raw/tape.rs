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

use arrow_schema::ArrowError;
use std::fmt::{Display, Formatter};

/// A tape encoding inspired by simdjson
///
/// <https://github.com/simdjson/simdjson/blob/master/doc/tape.md>
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum TapeElement {
    StartObject(u32),
    EndObject(u32),
    String(u32),
    Number(u32),
    True,
    False,
    Null,
}

impl Display for TapeElement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TapeElement::StartObject(_) => write!(f, "{{"),
            TapeElement::EndObject(_) => write!(f, "}}"),
            TapeElement::String(_) => write!(f, "string"),
            TapeElement::Number(_) => write!(f, "number"),
            TapeElement::True => write!(f, "true"),
            TapeElement::False => write!(f, "false"),
            TapeElement::Null => write!(f, "null"),
        }
    }
}

impl TapeElement {
    fn close_object(&mut self, idx: u32) {
        match self {
            Self::StartObject(x) => *x = idx,
            _ => unreachable!(),
        }
    }
}

/// A decoded JSON tape based on <https://github.com/simdjson/simdjson/blob/master/doc/tape.md>
///
/// The first element is always [`TapeElement::Null`]
pub struct Tape<'a> {
    elements: &'a [TapeElement],
    strings: &'a str,
    string_offsets: &'a [usize],
    num_rows: usize,
}

impl<'a> Tape<'a> {
    /// Returns the string for the given string index
    pub fn get_string(&self, idx: u32) -> &'a str {
        let end_offset = self.string_offsets[idx as usize + 1];
        let start_offset = self.string_offsets[idx as usize];
        &self.strings[start_offset..end_offset]
    }

    /// Returns the tape element at `idx`
    pub fn get(&self, idx: u32) -> TapeElement {
        self.elements[idx as usize]
    }

    /// Returns the number of rows
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }
}

/// States based on <https://www.json.org/json-en.html>
#[derive(Debug, Copy, Clone)]
enum DecoderState {
    /// Decoding an object
    ///
    /// Consists of (index of start [`TapeElement`], expecting comma)
    Object(u32, bool),
    String,
    AnyValue,
    Number,
    Colon,
    Escape,
    /// A unicode escape sequence,
    ///
    /// Consists of a `(low surrogate, high surrogate, decoded length)`
    Unicode(u16, u16, u8),
    /// A boolean or null literal
    ///
    /// Consists of `(literal, decoded length)`
    Literal(Literal, u8),
}

#[derive(Debug, Copy, Clone)]
enum Literal {
    Null,
    True,
    False,
}

impl Literal {
    fn element(&self) -> TapeElement {
        match self {
            Literal::Null => TapeElement::Null,
            Literal::True => TapeElement::True,
            Literal::False => TapeElement::False,
        }
    }

    fn bytes(&self) -> &'static [u8] {
        match self {
            Literal::Null => "null",
            Literal::True => "true",
            Literal::False => "false",
        }
        .as_bytes()
    }
}

/// Implements a state machine for decoding JSON to a tape
pub struct TapeDecoder {
    elements: Vec<TapeElement>,

    num_rows: usize,

    /// A buffer of parsed string data
    ///
    /// Note: if part way through a record, i.e. `stack` is not empty,
    /// this may contain truncated UTF-8 data
    bytes: Vec<u8>,

    /// Offsets into `data`
    offsets: Vec<usize>,

    /// A stack of [`DecoderState`]
    stack: Vec<DecoderState>,
}

impl TapeDecoder {
    /// Create a new [`TapeDecoder`] with the provided batch size
    pub fn new(batch_size: usize, num_fields: usize) -> Self {
        let tokens_per_row = 2 + num_fields * 2;
        let mut offsets = Vec::with_capacity(batch_size * (num_fields * 2) + 1);
        offsets.push(0);

        let mut elements = Vec::with_capacity(batch_size * tokens_per_row);
        elements.push(TapeElement::Null);

        Self {
            offsets,
            elements,
            num_rows: 0,
            bytes: Vec::with_capacity(num_fields * 2 * 8),
            stack: Vec::with_capacity(10),
        }
    }

    pub fn decode(&mut self, buf: &[u8]) -> Result<usize, ArrowError> {
        let mut iter = buf.iter();

        while iter.len() != 0 {
            match self.stack.last_mut() {
                // Start of row
                None => {
                    // Skip over leading whitespace
                    for b in &mut iter {
                        match *b {
                            b' ' | b'\n' | b'\r' | b'\t' => {}
                            b'{' => {
                                let idx = self.elements.len() as u32;
                                self.stack.push(DecoderState::Object(idx, false));
                                self.elements.push(TapeElement::StartObject(u32::MAX));
                                break;
                            }
                            _ => return Err(err(*b, "trimming leading whitespace")),
                        }
                    }
                }
                // Decoding an object
                Some(DecoderState::Object(start_idx, needs_comma)) => {
                    for b in &mut iter {
                        match *b {
                            b' ' | b'\n' | b'\r' | b'\t' => {}
                            b',' if *needs_comma => *needs_comma = false,
                            b'"' if !*needs_comma => {
                                *needs_comma = true;
                                self.stack.push(DecoderState::AnyValue);
                                self.stack.push(DecoderState::Colon);
                                self.stack.push(DecoderState::String);
                                break;
                            }
                            b'}' => {
                                let start_idx = *start_idx;
                                let end_idx = self.elements.len() as u32;
                                self.elements[start_idx as usize].close_object(end_idx);
                                self.elements.push(TapeElement::EndObject(start_idx));
                                self.stack.pop();
                                self.num_rows += self.stack.is_empty() as usize;
                                break;
                            }
                            _ => return Err(err(*b, "parsing object")),
                        }
                    }
                }
                // Decoding a string
                Some(DecoderState::String) => {
                    let end_pos = iter
                        .as_slice()
                        .iter()
                        .position(|b| matches!(b, b'\\' | b'"'));

                    match end_pos {
                        Some(x) => self.bytes.extend((&mut iter).take(x)),
                        None => {
                            self.bytes.extend_from_slice(iter.as_slice());
                            return Ok(buf.len());
                        }
                    }

                    match iter.next().unwrap() {
                        b'\\' => {
                            self.stack.push(DecoderState::Escape);
                        }
                        b'"' => {
                            let idx = self.offsets.len() - 1;
                            self.elements.push(TapeElement::String(idx as _));
                            self.offsets.push(self.bytes.len());
                            self.stack.pop();
                        }
                        b => unreachable!("{}", b),
                    }
                }
                Some(DecoderState::AnyValue) => {
                    for b in &mut iter {
                        match b {
                            b' ' | b'\n' | b'\r' | b'\t' => continue,
                            b'"' => {
                                self.stack.pop();
                                self.stack.push(DecoderState::String);
                            }
                            b'-' | b'0'..=b'9' => {
                                self.stack.pop();
                                self.stack.push(DecoderState::Number);
                                self.bytes.push(*b);
                            }
                            b'n' => {
                                self.stack.pop();
                                self.stack.push(DecoderState::Literal(Literal::Null, 1));
                            }
                            b'f' => {
                                self.stack.pop();
                                self.stack.push(DecoderState::Literal(Literal::False, 1));
                            }
                            b't' => {
                                self.stack.pop();
                                self.stack.push(DecoderState::Literal(Literal::True, 1));
                            }
                            b'{' => {
                                self.stack.pop();
                                let idx = self.elements.len() as u32;
                                self.stack.push(DecoderState::Object(idx, false));
                                self.elements.push(TapeElement::StartObject(u32::MAX));
                                break;
                            }
                            _ => return Err(err(*b, "parsing value")),
                        }
                        break;
                    }
                }
                Some(DecoderState::Number) => {
                    let end_pos = iter.as_slice().iter().position(|b| {
                        !matches!(b, b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E')
                    });

                    match end_pos {
                        Some(x) => {
                            self.stack.pop();
                            let idx = self.offsets.len() - 1;
                            self.elements.push(TapeElement::Number(idx as _));
                            self.bytes.extend((&mut iter).take(x));
                            self.offsets.push(self.bytes.len());
                        }
                        None => {
                            self.bytes.extend_from_slice(iter.as_slice());
                            return Ok(buf.len());
                        }
                    }
                }
                Some(DecoderState::Colon) => {
                    for b in &mut iter {
                        match b {
                            b' ' | b'\n' | b'\r' | b'\t' => {}
                            b':' => {
                                self.stack.pop();
                                break;
                            }
                            _ => return Err(err(*b, "parsing colon")),
                        }
                    }
                }
                Some(DecoderState::Literal(literal, idx)) => {
                    let bytes = literal.bytes();
                    let expected = bytes.iter().skip(*idx as usize);
                    for (expected, b) in expected.zip(&mut iter) {
                        match b == expected {
                            true => *idx += 1,
                            false => return Err(err(*b, "parsing literal")),
                        }
                    }
                    if *idx == bytes.len() as u8 {
                        let element = literal.element();
                        self.stack.pop();
                        self.elements.push(element);
                    }
                }
                Some(DecoderState::Escape) => {
                    let v = match iter.next().unwrap() {
                        b'u' => {
                            self.stack.pop();
                            self.stack.push(DecoderState::Unicode(0, 0, 0));
                            continue;
                        }
                        b'"' => b'"',
                        b'\\' => b'\\',
                        b'/' => b'/',
                        b'b' => 8,  // BS
                        b'f' => 12, // FF
                        b'n' => b'\n',
                        b'r' => b'\r',
                        b't' => b'\t',
                        b => return Err(err(*b, "parsing escape sequence")),
                    };

                    self.stack.pop();
                    self.bytes.push(v);
                }
                // Parse single low UTF-16 character
                Some(DecoderState::Unicode(high, low, idx)) => {
                    let write_char = |c: char, out: &mut Vec<u8>| {
                        let mut t = [0; 4];
                        out.extend_from_slice(c.encode_utf8(&mut t).as_bytes());
                    };

                    while *idx < 4 {
                        match iter.next() {
                            Some(b) => {
                                *high = *high << 4 | parse_unicode_escape(*b)? as u16;
                                *idx += 1;
                            }
                            None => return Ok(buf.len()),
                        }
                    }

                    if *idx == 4 {
                        if let Some(c) = char::from_u32(*high as u32) {
                            write_char(c, &mut self.bytes);
                            self.stack.pop();
                            continue;
                        }

                        match iter.next() {
                            Some(b'\\') => *idx += 1,
                            Some(b) => {
                                return Err(err(*b, "reading surrogate pair escape"))
                            }
                            None => return Ok(buf.len()),
                        }
                    }

                    if *idx == 5 {
                        match iter.next() {
                            Some(b'u') => *idx += 1,
                            Some(b) => {
                                return Err(err(*b, "reading surrogate pair unicode"))
                            }
                            None => return Ok(buf.len()),
                        }
                    }

                    while *idx < 10 {
                        match iter.next() {
                            Some(b) => {
                                *low = *low << 4 | parse_unicode_escape(*b)? as u16;
                                *idx += 1;
                            }
                            None => return Ok(buf.len()),
                        }
                    }

                    let n = (((*high - 0xD800) as u32) << 10 | (*low - 0xDC00) as u32)
                        + 0x1_0000;

                    let c = char::from_u32(n).ok_or_else(|| {
                        ArrowError::JsonError(format!("Invalid codepoint {}", n))
                    })?;
                    write_char(c, &mut self.bytes);
                    self.stack.pop();
                }
            }
        }

        Ok(buf.len() - iter.len())
    }

    /// Finishes the current [`Tape`]
    pub fn finish(&self) -> Result<Tape<'_>, ArrowError> {
        assert!(self.stack.is_empty());
        assert!(self.offsets.len() < u32::MAX as usize, "offset overflow");
        assert!(self.elements.len() < u32::MAX as usize, "elements overflow");

        // Sanity check
        assert_eq!(
            self.offsets.last().copied().unwrap_or_default(),
            self.bytes.len()
        );

        let strings = std::str::from_utf8(&self.bytes).map_err(|_| {
            ArrowError::JsonError("Encountered non-UTF-8 data".to_string())
        })?;

        Ok(Tape {
            strings,
            elements: &self.elements,
            string_offsets: &self.offsets,
            num_rows: self.num_rows,
        })
    }

    /// Clears this [`TapeDecoder`] in preparation to read the next batch
    pub fn clear(&mut self) {
        assert!(self.stack.is_empty());

        self.num_rows = 0;
        self.bytes.clear();
        self.elements.clear();
        self.elements.push(TapeElement::Null);
        self.offsets.clear();
        self.offsets.push(0);
    }

    /// Returns the number of read rows
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }
}

fn err(b: u8, ctx: &str) -> ArrowError {
    ArrowError::JsonError(format!("Encountered unexpected {} whilst {ctx}", b as char))
}

fn parse_unicode_escape(b: u8) -> Result<u8, ArrowError> {
    let digit = char::from(b)
        .to_digit(16)
        .ok_or_else(|| err(b, "unicode escape"))?;
    Ok(digit as u8)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sizes() {
        assert_eq!(std::mem::size_of::<DecoderState>(), 8);
        assert_eq!(std::mem::size_of::<TapeElement>(), 8);
    }

    #[test]
    fn test_basic() {
        let a = r#"
        {"hello": "world", "foo": 2, "bar": 45}

        {"foo": "bar"}

        {"fiz": null}

        {"a": true, "b": false, "c": null}

        {"a": "", "": "a"}

        {"a": "b", "object": {"nested": "hello", "foo": 23}, "b": {}, "c": {"foo": null }}
        "#;
        let mut decoder = TapeDecoder::new(16, 2);
        decoder.decode(a.as_bytes()).unwrap();

        let finished = decoder.finish().unwrap();
        assert_eq!(
            finished.elements,
            &[
                TapeElement::Null,
                TapeElement::StartObject(8),
                TapeElement::String(0),
                TapeElement::String(1),
                TapeElement::String(2),
                TapeElement::Number(3),
                TapeElement::String(4),
                TapeElement::Number(5),
                TapeElement::EndObject(1),
                TapeElement::StartObject(12),
                TapeElement::String(6),
                TapeElement::String(7),
                TapeElement::EndObject(9),
                TapeElement::StartObject(16),
                TapeElement::String(8),
                TapeElement::Null,
                TapeElement::EndObject(13),
                TapeElement::StartObject(24),
                TapeElement::String(9),
                TapeElement::True,
                TapeElement::String(10),
                TapeElement::False,
                TapeElement::String(11),
                TapeElement::Null,
                TapeElement::EndObject(17),
                TapeElement::StartObject(30),
                TapeElement::String(12),
                TapeElement::String(13),
                TapeElement::String(14),
                TapeElement::String(15),
                TapeElement::EndObject(25),
                TapeElement::StartObject(49),
                TapeElement::String(16),
                TapeElement::String(17),
                TapeElement::String(18),
                TapeElement::StartObject(40),
                TapeElement::String(19),
                TapeElement::String(20),
                TapeElement::String(21),
                TapeElement::Number(22),
                TapeElement::EndObject(35),
                TapeElement::String(23),
                TapeElement::StartObject(43),
                TapeElement::EndObject(42),
                TapeElement::String(24),
                TapeElement::StartObject(48),
                TapeElement::String(25),
                TapeElement::Null,
                TapeElement::EndObject(45),
                TapeElement::EndObject(31)
            ]
        );

        assert_eq!(
            finished.strings,
            "helloworldfoo2bar45foobarfizabcaaabobjectnestedhellofoo23bcfoo"
        );
        assert_eq!(
            &finished.string_offsets,
            &[
                0, 5, 10, 13, 14, 17, 19, 22, 25, 28, 29, 30, 31, 32, 32, 32, 33, 34, 35,
                41, 47, 52, 55, 57, 58, 59, 62
            ]
        )
    }
}
