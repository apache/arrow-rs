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

/// A tape encoding inspired by [simdjson]
///
/// Uses `u32` for offsets to ensure `TapeElement` is 64-bits. A future
/// iteration may increase this to a custom `u56` type.
///
/// [simdjson]: https://github.com/simdjson/simdjson/blob/master/doc/tape.md
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum TapeElement {
    StartObject(u32),
    EndObject(u32),
    StartList(u32),
    EndList(u32),
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
            TapeElement::StartList(_) => write!(f, "["),
            TapeElement::EndList(_) => write!(f, "]"),
            TapeElement::String(_) => write!(f, "string"),
            TapeElement::Number(_) => write!(f, "number"),
            TapeElement::True => write!(f, "true"),
            TapeElement::False => write!(f, "false"),
            TapeElement::Null => write!(f, "null"),
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
    /// Contains index of start [`TapeElement::StartObject`]
    Object(u32),
    /// Decoding a list
    ///
    /// Contains index of start [`TapeElement::StartList`]
    List(u32),
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

    num_rows: usize,

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
            num_rows: 0,
            bytes: Vec::with_capacity(num_fields * 2 * 8),
            stack: Vec::with_capacity(10),
        }
    }

    pub fn decode(&mut self, buf: &[u8]) -> Result<usize, ArrowError> {
        let mut iter = BufIter::new(buf);

        while !iter.is_empty() {
            match self.stack.last_mut() {
                // Start of row
                None => {
                    // Skip over leading whitespace
                    iter.skip_whitespace();
                    match next!(iter) {
                        b'{' => {
                            let idx = self.elements.len() as u32;
                            self.stack.push(DecoderState::Object(idx));
                            self.elements.push(TapeElement::StartObject(u32::MAX));
                        }
                        b => return Err(err(b, "trimming leading whitespace")),
                    }
                }
                // Decoding an object
                Some(DecoderState::Object(start_idx)) => {
                    iter.advance_until(|b| !json_whitespace(b) && b != b',');
                    match next!(iter) {
                        b'"' => {
                            self.stack.push(DecoderState::AnyValue);
                            self.stack.push(DecoderState::Colon);
                            self.stack.push(DecoderState::String);
                        }
                        b'}' => {
                            let start_idx = *start_idx;
                            let end_idx = self.elements.len() as u32;
                            self.elements[start_idx as usize] =
                                TapeElement::StartObject(end_idx);
                            self.elements.push(TapeElement::EndObject(start_idx));
                            self.stack.pop();
                            self.num_rows += self.stack.is_empty() as usize;
                            if self.num_rows >= self.batch_size {
                                break;
                            }
                        }
                        b => return Err(err(b, "parsing object")),
                    }
                }
                // Decoding a list
                Some(DecoderState::List(start_idx)) => {
                    iter.advance_until(|b| !json_whitespace(b) && b != b',');
                    match iter.peek() {
                        Some(b']') => {
                            iter.next();
                            let start_idx = *start_idx;
                            let end_idx = self.elements.len() as u32;
                            self.elements[start_idx as usize] =
                                TapeElement::StartList(end_idx);
                            self.elements.push(TapeElement::EndList(start_idx));
                            self.stack.pop();
                        }
                        Some(_) => self.stack.push(DecoderState::AnyValue),
                        None => break,
                    }
                }
                // Decoding a string
                Some(DecoderState::String) => {
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
                Some(state @ DecoderState::AnyValue) => {
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
                Some(DecoderState::Number) => {
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
                Some(DecoderState::Colon) => {
                    iter.skip_whitespace();
                    match next!(iter) {
                        b':' => self.stack.pop(),
                        b => return Err(err(b, "parsing colon")),
                    };
                }
                Some(DecoderState::Literal(literal, idx)) => {
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
                Some(DecoderState::Escape) => {
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
                Some(DecoderState::Unicode(high, low, idx)) => loop {
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
                                b => return Err(err(b, "reading surrogate pair escape")),
                            }
                        }
                        5 => match next!(iter) {
                            b'u' => {}
                            b => return Err(err(b, "reading surrogate pair unicode")),
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

    /// Finishes the current [`Tape`]
    pub fn finish(&self) -> Result<Tape<'_>, ArrowError> {
        if let Some(b) = self.stack.last() {
            return Err(ArrowError::JsonError(format!("Record truncated: {:?}", b)));
        }

        if self.offsets.len() >= u32::MAX as usize {
            return Err(ArrowError::JsonError(format!("Encountered more than {} bytes of string data, consider using a smaller batch size", u32::MAX)));
        }

        if self.offsets.len() >= u32::MAX as usize {
            return Err(ArrowError::JsonError(format!("Encountered more than {} JSON elements, consider using a smaller batch size", u32::MAX)));
        }

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

impl<'a> Iterator for BufIter<'a> {
    type Item = u8;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().copied()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl<'a> ExactSizeIterator for BufIter<'a> {}

/// Returns an error for a given byte `b` and context `ctx`
fn err(b: u8, ctx: &str) -> ArrowError {
    ArrowError::JsonError(format!("Encountered unexpected {} whilst {ctx}", b as char))
}

/// Creates a character from an UTF-16 surrogate pair
fn char_from_surrogate_pair(low: u16, high: u16) -> Result<char, ArrowError> {
    let n = (((high - 0xD800) as u32) << 10 | (low - 0xDC00) as u32) + 0x1_0000;
    char::from_u32(n).ok_or_else(|| {
        ArrowError::JsonError(format!("Invalid UTF-16 surrogate pair {}", n))
    })
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
                0, 5, 10, 13, 14, 17, 19, 22, 25, 28, 29, 30, 31, 32, 32, 32, 33, 34, 35,
                41, 47, 52, 55, 57, 58, 59, 62, 63, 63, 66, 69, 70, 71, 72, 73, 74, 75,
                76, 77
            ]
        )
    }
}
