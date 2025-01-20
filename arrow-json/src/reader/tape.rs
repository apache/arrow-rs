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

use crate::reader::serializer::TapeSerializer;
use arrow_schema::ArrowError;
use serde::Serialize;
use std::fmt::Write;

/// We decode JSON to a flattened tape representation,
/// allowing for efficient traversal of the JSON data
///
/// This approach is inspired by [simdjson]
///
/// Uses `u32` for offsets to ensure `TapeElement` is 64-bits. A future
/// iteration may increase this to a custom `u56` type.
///
/// [simdjson]: https://github.com/simdjson/simdjson/blob/master/doc/tape.md
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum TapeElement {
    /// The start of an object, i.e. `{`
    ///
    /// Contains the offset of the corresponding [`Self::EndObject`]
    StartObject(u32),
    /// The end of an object, i.e. `}`
    ///
    /// Contains the offset of the corresponding [`Self::StartObject`]
    EndObject(u32),
    /// The start of a list , i.e. `[`
    ///
    /// Contains the offset of the corresponding [`Self::EndList`]
    StartList(u32),
    /// The end of a list , i.e. `]`
    ///
    /// Contains the offset of the corresponding [`Self::StartList`]
    EndList(u32),
    /// A string value
    ///
    /// Contains the offset into the [`Tape`] string data
    String(u32),
    /// A numeric value
    ///
    /// Contains the offset into the [`Tape`] string data
    Number(u32),

    /// The high bits of a i64
    ///
    /// Followed by [`Self::I32`] containing the low bits
    I64(i32),

    /// A 32-bit signed integer
    ///
    /// May be preceded by [`Self::I64`] containing high bits
    I32(i32),

    /// The high bits of a 64-bit float
    ///
    /// Followed by [`Self::F32`] containing the low bits
    F64(u32),

    /// A 32-bit float or the low-bits of a 64-bit float if preceded by [`Self::F64`]
    F32(u32),

    /// A true literal
    True,
    /// A false literal
    False,
    /// A null literal
    Null,
}

/// A decoded JSON tape
///
/// String and numeric data is stored alongside an array of [`TapeElement`]
///
/// The first element is always [`TapeElement::Null`]
///
/// This approach to decoding JSON is inspired by [simdjson]
///
/// [simdjson]: https://github.com/simdjson/simdjson/blob/master/doc/tape.md
#[derive(Debug)]
pub struct Tape<'a> {
    elements: &'a [TapeElement],
    strings: &'a str,
    string_offsets: &'a [usize],
    num_rows: usize,
}

impl<'a> Tape<'a> {
    /// Returns the string for the given string index
    #[inline]
    pub fn get_string(&self, idx: u32) -> &'a str {
        let end_offset = self.string_offsets[idx as usize + 1];
        let start_offset = self.string_offsets[idx as usize];
        // SAFETY:
        // Verified offsets
        unsafe { self.strings.get_unchecked(start_offset..end_offset) }
    }

    /// Returns the tape element at `idx`
    pub fn get(&self, idx: u32) -> TapeElement {
        self.elements[idx as usize]
    }

    /// Returns the index of the next field at the same level as `cur_idx`
    ///
    /// Return an error if `cur_idx` is not the start of a field
    pub fn next(&self, cur_idx: u32, expected: &str) -> Result<u32, ArrowError> {
        match self.get(cur_idx) {
            TapeElement::String(_)
            | TapeElement::Number(_)
            | TapeElement::True
            | TapeElement::False
            | TapeElement::Null
            | TapeElement::I32(_)
            | TapeElement::F32(_) => Ok(cur_idx + 1),
            TapeElement::I64(_) | TapeElement::F64(_) => Ok(cur_idx + 2),
            TapeElement::StartList(end_idx) => Ok(end_idx + 1),
            TapeElement::StartObject(end_idx) => Ok(end_idx + 1),
            TapeElement::EndObject(_) | TapeElement::EndList(_) => {
                Err(self.error(cur_idx, expected))
            }
        }
    }

    /// Returns the number of rows
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    /// Serialize the tape element at index `idx` to `out` returning the next field index
    fn serialize(&self, out: &mut String, idx: u32) -> u32 {
        match self.get(idx) {
            TapeElement::StartObject(end) => {
                out.push('{');
                let mut cur_idx = idx + 1;
                while cur_idx < end {
                    cur_idx = self.serialize(out, cur_idx);
                    out.push_str(": ");
                    cur_idx = self.serialize(out, cur_idx);
                }
                out.push('}');
                return end + 1;
            }
            TapeElement::EndObject(_) => out.push('}'),
            TapeElement::StartList(end) => {
                out.push('[');
                let mut cur_idx = idx + 1;
                while cur_idx < end {
                    cur_idx = self.serialize(out, cur_idx);
                    if cur_idx < end {
                        out.push_str(", ");
                    }
                }
                out.push(']');
                return end + 1;
            }
            TapeElement::EndList(_) => out.push(']'),
            TapeElement::String(s) => {
                out.push('"');
                out.push_str(self.get_string(s));
                out.push('"')
            }
            TapeElement::Number(n) => out.push_str(self.get_string(n)),
            TapeElement::True => out.push_str("true"),
            TapeElement::False => out.push_str("false"),
            TapeElement::Null => out.push_str("null"),
            TapeElement::I64(high) => match self.get(idx + 1) {
                TapeElement::I32(low) => {
                    let val = (high as i64) << 32 | (low as u32) as i64;
                    let _ = write!(out, "{val}");
                    return idx + 2;
                }
                _ => unreachable!(),
            },
            TapeElement::I32(val) => {
                let _ = write!(out, "{val}");
            }
            TapeElement::F64(high) => match self.get(idx + 1) {
                TapeElement::F32(low) => {
                    let val = f64::from_bits((high as u64) << 32 | low as u64);
                    let _ = write!(out, "{val}");
                    return idx + 2;
                }
                _ => unreachable!(),
            },
            TapeElement::F32(val) => {
                let _ = write!(out, "{}", f32::from_bits(val));
            }
        }
        idx + 1
    }

    /// Returns an error reading index `idx`
    pub fn error(&self, idx: u32, expected: &str) -> ArrowError {
        let mut out = String::with_capacity(64);
        self.serialize(&mut out, idx);
        ArrowError::JsonError(format!("expected {expected} got {out}"))
    }
}

/// States based on <https://www.json.org/json-en.html>
#[derive(Debug, Copy, Clone)]
enum DecoderState {
    /// Decoding an object
    ///
    /// Contains index of start [`TapeElement::StartObject`]
    Object(u32),
    /// Decoding a list
    ///
    /// Contains index of start [`TapeElement::StartList`]
    List(u32),
    String,
    Value,
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

impl DecoderState {
    fn as_str(&self) -> &'static str {
        match self {
            DecoderState::Object(_) => "object",
            DecoderState::List(_) => "list",
            DecoderState::String => "string",
            DecoderState::Value => "value",
            DecoderState::Number => "number",
            DecoderState::Colon => "colon",
            DecoderState::Escape => "escape",
            DecoderState::Unicode(_, _, _) => "unicode literal",
            DecoderState::Literal(d, _) => d.as_str(),
        }
    }
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

    fn as_str(&self) -> &'static str {
        match self {
            Literal::Null => "null",
            Literal::True => "true",
            Literal::False => "false",
        }
    }

    fn bytes(&self) -> &'static [u8] {
        self.as_str().as_bytes()
    }
}

/// Evaluates to the next element in the iterator or breaks the current loop
macro_rules! next {
    ($next:ident) => {
        match $next.next() {
            Some(b) => b,
            None => break,
        }
    };
}

/// Implements a state machine for decoding JSON to a tape
pub struct TapeDecoder {
    elements: Vec<TapeElement>,

    /// The number of rows decoded, including any in progress if `!stack.is_empty()`
    cur_row: usize,

    /// Number of rows to read per batch
    batch_size: usize,

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
    /// and an estimated number of fields in each row
    pub fn new(batch_size: usize, num_fields: usize) -> Self {
        let tokens_per_row = 2 + num_fields * 2;
        let mut offsets = Vec::with_capacity(batch_size * (num_fields * 2) + 1);
        offsets.push(0);

        let mut elements = Vec::with_capacity(batch_size * tokens_per_row);
        elements.push(TapeElement::Null);

        Self {
            offsets,
            elements,
            batch_size,
            cur_row: 0,
            bytes: Vec::with_capacity(num_fields * 2 * 8),
            stack: Vec::with_capacity(10),
        }
    }

    pub fn decode(&mut self, buf: &[u8]) -> Result<usize, ArrowError> {
        let mut iter = BufIter::new(buf);

        while !iter.is_empty() {
            let state = match self.stack.last_mut() {
                Some(l) => l,
                None => {
                    iter.skip_whitespace();
                    if iter.is_empty() || self.cur_row >= self.batch_size {
                        break;
                    }

                    // Start of row
                    self.cur_row += 1;
                    self.stack.push(DecoderState::Value);
                    self.stack.last_mut().unwrap()
                }
            };

            match state {
                // Decoding an object
                DecoderState::Object(start_idx) => {
                    iter.advance_until(|b| !json_whitespace(b) && b != b',');
                    match next!(iter) {
                        b'"' => {
                            self.stack.push(DecoderState::Value);
                            self.stack.push(DecoderState::Colon);
                            self.stack.push(DecoderState::String);
                        }
                        b'}' => {
                            let start_idx = *start_idx;
                            let end_idx = self.elements.len() as u32;
                            self.elements[start_idx as usize] = TapeElement::StartObject(end_idx);
                            self.elements.push(TapeElement::EndObject(start_idx));
                            self.stack.pop();
                        }
                        b => return Err(err(b, "parsing object")),
                    }
                }
                // Decoding a list
                DecoderState::List(start_idx) => {
                    iter.advance_until(|b| !json_whitespace(b) && b != b',');
                    match iter.peek() {
                        Some(b']') => {
                            iter.next();
                            let start_idx = *start_idx;
                            let end_idx = self.elements.len() as u32;
                            self.elements[start_idx as usize] = TapeElement::StartList(end_idx);
                            self.elements.push(TapeElement::EndList(start_idx));
                            self.stack.pop();
                        }
                        Some(_) => self.stack.push(DecoderState::Value),
                        None => break,
                    }
                }
                // Decoding a string
                DecoderState::String => {
                    let s = iter.advance_until(|b| matches!(b, b'\\' | b'"'));
                    self.bytes.extend_from_slice(s);

                    match next!(iter) {
                        b'\\' => self.stack.push(DecoderState::Escape),
                        b'"' => {
                            let idx = self.offsets.len() - 1;
                            self.elements.push(TapeElement::String(idx as _));
                            self.offsets.push(self.bytes.len());
                            self.stack.pop();
                        }
                        b => unreachable!("{}", b),
                    }
                }
                state @ DecoderState::Value => {
                    iter.skip_whitespace();
                    *state = match next!(iter) {
                        b'"' => DecoderState::String,
                        b @ b'-' | b @ b'0'..=b'9' => {
                            self.bytes.push(b);
                            DecoderState::Number
                        }
                        b'n' => DecoderState::Literal(Literal::Null, 1),
                        b'f' => DecoderState::Literal(Literal::False, 1),
                        b't' => DecoderState::Literal(Literal::True, 1),
                        b'[' => {
                            let idx = self.elements.len() as u32;
                            self.elements.push(TapeElement::StartList(u32::MAX));
                            DecoderState::List(idx)
                        }
                        b'{' => {
                            let idx = self.elements.len() as u32;
                            self.elements.push(TapeElement::StartObject(u32::MAX));
                            DecoderState::Object(idx)
                        }
                        b => return Err(err(b, "parsing value")),
                    };
                }
                DecoderState::Number => {
                    let s = iter.advance_until(|b| {
                        !matches!(b, b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E')
                    });
                    self.bytes.extend_from_slice(s);

                    if !iter.is_empty() {
                        self.stack.pop();
                        let idx = self.offsets.len() - 1;
                        self.elements.push(TapeElement::Number(idx as _));
                        self.offsets.push(self.bytes.len());
                    }
                }
                DecoderState::Colon => {
                    iter.skip_whitespace();
                    match next!(iter) {
                        b':' => self.stack.pop(),
                        b => return Err(err(b, "parsing colon")),
                    };
                }
                DecoderState::Literal(literal, idx) => {
                    let bytes = literal.bytes();
                    let expected = bytes.iter().skip(*idx as usize).copied();
                    for (expected, b) in expected.zip(&mut iter) {
                        match b == expected {
                            true => *idx += 1,
                            false => return Err(err(b, "parsing literal")),
                        }
                    }
                    if *idx == bytes.len() as u8 {
                        let element = literal.element();
                        self.stack.pop();
                        self.elements.push(element);
                    }
                }
                DecoderState::Escape => {
                    let v = match next!(iter) {
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
                        b => return Err(err(b, "parsing escape sequence")),
                    };

                    self.stack.pop();
                    self.bytes.push(v);
                }
                // Parse a unicode escape sequence
                DecoderState::Unicode(high, low, idx) => loop {
                    match *idx {
                        0..=3 => *high = *high << 4 | parse_hex(next!(iter))? as u16,
                        4 => {
                            if let Some(c) = char::from_u32(*high as u32) {
                                write_char(c, &mut self.bytes);
                                self.stack.pop();
                                break;
                            }

                            match next!(iter) {
                                b'\\' => {}
                                b => return Err(err(b, "parsing surrogate pair escape")),
                            }
                        }
                        5 => match next!(iter) {
                            b'u' => {}
                            b => return Err(err(b, "parsing surrogate pair unicode")),
                        },
                        6..=9 => *low = *low << 4 | parse_hex(next!(iter))? as u16,
                        _ => {
                            let c = char_from_surrogate_pair(*low, *high)?;
                            write_char(c, &mut self.bytes);
                            self.stack.pop();
                            break;
                        }
                    }
                    *idx += 1;
                },
            }
        }

        Ok(buf.len() - iter.len())
    }

    /// Writes any type that implements [`Serialize`] into this [`TapeDecoder`]
    pub fn serialize<S: Serialize>(&mut self, rows: &[S]) -> Result<(), ArrowError> {
        if let Some(b) = self.stack.last() {
            return Err(ArrowError::JsonError(format!(
                "Cannot serialize to tape containing partial decode state {}",
                b.as_str()
            )));
        }

        let mut serializer =
            TapeSerializer::new(&mut self.elements, &mut self.bytes, &mut self.offsets);

        rows.iter()
            .try_for_each(|row| row.serialize(&mut serializer))
            .map_err(|e| ArrowError::JsonError(e.to_string()))?;

        self.cur_row += rows.len();

        Ok(())
    }

    /// Finishes the current [`Tape`]
    pub fn finish(&self) -> Result<Tape<'_>, ArrowError> {
        if let Some(b) = self.stack.last() {
            return Err(ArrowError::JsonError(format!(
                "Truncated record whilst reading {}",
                b.as_str()
            )));
        }

        if self.offsets.len() >= u32::MAX as usize {
            return Err(ArrowError::JsonError(format!("Encountered more than {} bytes of string data, consider using a smaller batch size", u32::MAX)));
        }

        if self.offsets.len() >= u32::MAX as usize {
            return Err(ArrowError::JsonError(format!(
                "Encountered more than {} JSON elements, consider using a smaller batch size",
                u32::MAX
            )));
        }

        // Sanity check
        assert_eq!(
            self.offsets.last().copied().unwrap_or_default(),
            self.bytes.len()
        );

        let strings = std::str::from_utf8(&self.bytes)
            .map_err(|_| ArrowError::JsonError("Encountered non-UTF-8 data".to_string()))?;

        for offset in self.offsets.iter().copied() {
            if !strings.is_char_boundary(offset) {
                return Err(ArrowError::JsonError(
                    "Encountered truncated UTF-8 sequence".to_string(),
                ));
            }
        }

        Ok(Tape {
            strings,
            elements: &self.elements,
            string_offsets: &self.offsets,
            num_rows: self.cur_row,
        })
    }

    /// Clears this [`TapeDecoder`] in preparation to read the next batch
    pub fn clear(&mut self) {
        assert!(self.stack.is_empty());

        self.cur_row = 0;
        self.bytes.clear();
        self.elements.clear();
        self.elements.push(TapeElement::Null);
        self.offsets.clear();
        self.offsets.push(0);
    }
}

/// A wrapper around a slice iterator that provides some helper functionality
struct BufIter<'a>(std::slice::Iter<'a, u8>);

impl<'a> BufIter<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self(buf.iter())
    }

    fn as_slice(&self) -> &'a [u8] {
        self.0.as_slice()
    }

    fn is_empty(&self) -> bool {
        self.0.len() == 0
    }

    fn peek(&self) -> Option<u8> {
        self.0.as_slice().first().copied()
    }

    fn advance(&mut self, skip: usize) {
        for _ in 0..skip {
            self.0.next();
        }
    }

    fn advance_until<F: FnMut(u8) -> bool>(&mut self, f: F) -> &[u8] {
        let s = self.as_slice();
        match s.iter().copied().position(f) {
            Some(x) => {
                self.advance(x);
                &s[..x]
            }
            None => {
                self.advance(s.len());
                s
            }
        }
    }

    fn skip_whitespace(&mut self) {
        self.advance_until(|b| !json_whitespace(b));
    }
}

impl Iterator for BufIter<'_> {
    type Item = u8;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().copied()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl ExactSizeIterator for BufIter<'_> {}

/// Returns an error for a given byte `b` and context `ctx`
fn err(b: u8, ctx: &str) -> ArrowError {
    ArrowError::JsonError(format!(
        "Encountered unexpected '{}' whilst {ctx}",
        b as char
    ))
}

/// Creates a character from an UTF-16 surrogate pair
fn char_from_surrogate_pair(low: u16, high: u16) -> Result<char, ArrowError> {
    let n = (((high - 0xD800) as u32) << 10 | (low - 0xDC00) as u32) + 0x1_0000;
    char::from_u32(n)
        .ok_or_else(|| ArrowError::JsonError(format!("Invalid UTF-16 surrogate pair {n}")))
}

/// Writes `c` as UTF-8 to `out`
fn write_char(c: char, out: &mut Vec<u8>) {
    let mut t = [0; 4];
    out.extend_from_slice(c.encode_utf8(&mut t).as_bytes());
}

/// Evaluates to true if `b` is a valid JSON whitespace character
#[inline]
fn json_whitespace(b: u8) -> bool {
    matches!(b, b' ' | b'\n' | b'\r' | b'\t')
}

/// Parse a hex character to `u8`
fn parse_hex(b: u8) -> Result<u8, ArrowError> {
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

        {"a": ["", "foo", ["bar", "c"]], "b": {"1": []}, "c": {"2": [1, 2, 3]} }
        "#;
        let mut decoder = TapeDecoder::new(16, 2);
        decoder.decode(a.as_bytes()).unwrap();

        let finished = decoder.finish().unwrap();
        assert_eq!(
            finished.elements,
            &[
                TapeElement::Null,
                TapeElement::StartObject(8), // {"hello": "world", "foo": 2, "bar": 45}
                TapeElement::String(0),      // "hello"
                TapeElement::String(1),      // "world"
                TapeElement::String(2),      // "foo"
                TapeElement::Number(3),      // 2
                TapeElement::String(4),      // "bar"
                TapeElement::Number(5),      // 45
                TapeElement::EndObject(1),
                TapeElement::StartObject(12), // {"foo": "bar"}
                TapeElement::String(6),       // "foo"
                TapeElement::String(7),       // "bar"
                TapeElement::EndObject(9),
                TapeElement::StartObject(16), // {"fiz": null}
                TapeElement::String(8),       // "fiz
                TapeElement::Null,            // null
                TapeElement::EndObject(13),
                TapeElement::StartObject(24), // {"a": true, "b": false, "c": null}
                TapeElement::String(9),       // "a"
                TapeElement::True,            // true
                TapeElement::String(10),      // "b"
                TapeElement::False,           // false
                TapeElement::String(11),      // "c"
                TapeElement::Null,            // null
                TapeElement::EndObject(17),
                TapeElement::StartObject(30), // {"a": "", "": "a"}
                TapeElement::String(12),      // "a"
                TapeElement::String(13),      // ""
                TapeElement::String(14),      // ""
                TapeElement::String(15),      // "a"
                TapeElement::EndObject(25),
                TapeElement::StartObject(49), // {"a": "b", "object": {"nested": "hello", "foo": 23}, "b": {}, "c": {"foo": null }}
                TapeElement::String(16),      // "a"
                TapeElement::String(17),      // "b"
                TapeElement::String(18),      // "object"
                TapeElement::StartObject(40), // {"nested": "hello", "foo": 23}
                TapeElement::String(19),      // "nested"
                TapeElement::String(20),      // "hello"
                TapeElement::String(21),      // "foo"
                TapeElement::Number(22),      // 23
                TapeElement::EndObject(35),
                TapeElement::String(23),      // "b"
                TapeElement::StartObject(43), // {}
                TapeElement::EndObject(42),
                TapeElement::String(24),      // "c"
                TapeElement::StartObject(48), // {"foo": null }
                TapeElement::String(25),      // "foo"
                TapeElement::Null,            // null
                TapeElement::EndObject(45),
                TapeElement::EndObject(31),
                TapeElement::StartObject(75), // {"a": ["", "foo", ["bar", "c"]], "b": {"1": []}, "c": {"2": [1, 2, 3]} }
                TapeElement::String(26),      // "a"
                TapeElement::StartList(59),   // ["", "foo", ["bar", "c"]]
                TapeElement::String(27),      // ""
                TapeElement::String(28),      // "foo"
                TapeElement::StartList(58),   // ["bar", "c"]
                TapeElement::String(29),      // "bar"
                TapeElement::String(30),      // "c"
                TapeElement::EndList(55),
                TapeElement::EndList(52),
                TapeElement::String(31),      // "b"
                TapeElement::StartObject(65), // {"1": []}
                TapeElement::String(32),      // "1"
                TapeElement::StartList(64),   // []
                TapeElement::EndList(63),
                TapeElement::EndObject(61),
                TapeElement::String(33),      // "c"
                TapeElement::StartObject(74), // {"2": [1, 2, 3]}
                TapeElement::String(34),      // "2"
                TapeElement::StartList(73),   // [1, 2, 3]
                TapeElement::Number(35),      // 1
                TapeElement::Number(36),      // 2
                TapeElement::Number(37),      // 3
                TapeElement::EndList(69),
                TapeElement::EndObject(67),
                TapeElement::EndObject(50)
            ]
        );

        assert_eq!(
            finished.strings,
            "helloworldfoo2bar45foobarfizabcaaabobjectnestedhellofoo23bcfooafoobarcb1c2123"
        );
        assert_eq!(
            &finished.string_offsets,
            &[
                0, 5, 10, 13, 14, 17, 19, 22, 25, 28, 29, 30, 31, 32, 32, 32, 33, 34, 35, 41, 47,
                52, 55, 57, 58, 59, 62, 63, 63, 66, 69, 70, 71, 72, 73, 74, 75, 76, 77
            ]
        )
    }

    #[test]
    fn test_invalid() {
        // Test invalid
        let mut decoder = TapeDecoder::new(16, 2);
        let err = decoder.decode(b"hello").unwrap_err().to_string();
        assert_eq!(
            err,
            "Json error: Encountered unexpected 'h' whilst parsing value"
        );

        let mut decoder = TapeDecoder::new(16, 2);
        let err = decoder.decode(b"{\"hello\": }").unwrap_err().to_string();
        assert_eq!(
            err,
            "Json error: Encountered unexpected '}' whilst parsing value"
        );

        let mut decoder = TapeDecoder::new(16, 2);
        let err = decoder
            .decode(b"{\"hello\": [ false, tru ]}")
            .unwrap_err()
            .to_string();
        assert_eq!(
            err,
            "Json error: Encountered unexpected ' ' whilst parsing literal"
        );

        let mut decoder = TapeDecoder::new(16, 2);
        let err = decoder
            .decode(b"{\"hello\": \"\\ud8\"}")
            .unwrap_err()
            .to_string();
        assert_eq!(
            err,
            "Json error: Encountered unexpected '\"' whilst unicode escape"
        );

        // Missing surrogate pair
        let mut decoder = TapeDecoder::new(16, 2);
        let err = decoder
            .decode(b"{\"hello\": \"\\ud83d\"}")
            .unwrap_err()
            .to_string();
        assert_eq!(
            err,
            "Json error: Encountered unexpected '\"' whilst parsing surrogate pair escape"
        );

        // Test truncation
        let mut decoder = TapeDecoder::new(16, 2);
        decoder.decode(b"{\"he").unwrap();
        let err = decoder.finish().unwrap_err().to_string();
        assert_eq!(err, "Json error: Truncated record whilst reading string");

        let mut decoder = TapeDecoder::new(16, 2);
        decoder.decode(b"{\"hello\" : ").unwrap();
        let err = decoder.finish().unwrap_err().to_string();
        assert_eq!(err, "Json error: Truncated record whilst reading value");

        let mut decoder = TapeDecoder::new(16, 2);
        decoder.decode(b"{\"hello\" : [").unwrap();
        let err = decoder.finish().unwrap_err().to_string();
        assert_eq!(err, "Json error: Truncated record whilst reading list");

        let mut decoder = TapeDecoder::new(16, 2);
        decoder.decode(b"{\"hello\" : tru").unwrap();
        let err = decoder.finish().unwrap_err().to_string();
        assert_eq!(err, "Json error: Truncated record whilst reading true");

        let mut decoder = TapeDecoder::new(16, 2);
        decoder.decode(b"{\"hello\" : nu").unwrap();
        let err = decoder.finish().unwrap_err().to_string();
        assert_eq!(err, "Json error: Truncated record whilst reading null");

        // Test invalid UTF-8
        let mut decoder = TapeDecoder::new(16, 2);
        decoder.decode(b"{\"hello\" : \"world\xFF\"}").unwrap();
        let err = decoder.finish().unwrap_err().to_string();
        assert_eq!(err, "Json error: Encountered non-UTF-8 data");

        let mut decoder = TapeDecoder::new(16, 2);
        decoder.decode(b"{\"\xe2\" : \"\x96\xa1\"}").unwrap();
        let err = decoder.finish().unwrap_err().to_string();
        assert_eq!(err, "Json error: Encountered truncated UTF-8 sequence");
    }
}
