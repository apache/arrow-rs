// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Module for parsing JSON strings as Variant

use arrow_schema::ArrowError;
use memchr::memchr2;
use parquet_variant::{MAX_NESTING_DEPTH, ObjectFieldBuilder, Variant, VariantBuilderExt};
use serde_json::{Number, Value};
use std::borrow::Cow;
use std::collections::BTreeMap;

/// Converts a JSON string to Variant using a [`VariantBuilderExt`], such as
/// [`VariantBuilder`].
///
/// The resulting `value` and `metadata` buffers can be
/// extracted using `builder.finish()`
///
/// Integers use the smallest fitting integer encoding; integer `-0` is zero.
/// Other numbers use `Double`.
///
/// # Arguments
/// * `json` - The JSON string to parse as Variant.
///
/// # Returns
///
/// * `Ok(())` if successful
/// * `Err` with error details if the conversion fails
///
/// [`VariantBuilder`]: parquet_variant::VariantBuilder
///
/// ```rust
/// # use parquet_variant::VariantBuilder;
/// # use parquet_variant_json::{JsonToVariant, VariantToJson};
///
/// let mut variant_builder = VariantBuilder::new();
/// let person_string = "{\"name\":\"Alice\", \"age\":30, ".to_string()
/// + "\"email\":\"alice@example.com\", \"is_active\": true, \"score\": 95.7,"
/// + "\"additional_info\": null}";
/// variant_builder.append_json(&person_string)?;
///
/// let (metadata, value) = variant_builder.finish();
///
/// let variant = parquet_variant::Variant::try_new(&metadata, &value)?;
///
/// let json_result = variant.to_json_string()?;
/// let json_value = variant.to_json_value()?;
///
/// let mut buffer = Vec::new();
/// variant.to_json(&mut buffer)?;
/// let buffer_result = String::from_utf8(buffer)?;
/// assert_eq!(json_result, "{\"additional_info\":null,\"age\":30,".to_string() +
/// "\"email\":\"alice@example.com\",\"is_active\":true,\"name\":\"Alice\",\"score\":95.7}");
/// assert_eq!(json_result, buffer_result);
/// assert_eq!(json_result, serde_json::to_string(&json_value)?);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub trait JsonToVariant {
    /// Create a Variant from a JSON string
    fn append_json(&mut self, json: &str) -> Result<(), ArrowError>;
}

impl<T: VariantBuilderExt> JsonToVariant for T {
    fn append_json(&mut self, json: &str) -> Result<(), ArrowError> {
        let depth = self.container_depth();
        let mut parser = JsonParser::new(json, depth);
        if parser.may_exceed_field_budget() {
            return parser.buffer_value(0)?.append(self);
        }
        match parser.parse(self) {
            Ok(()) => Ok(()),
            Err(DirectParseError::Syntax(error)) => Err(error),
            Err(DirectParseError::UnstableFieldIds) => {
                JsonParser::new(json, depth).buffer_value(0)?.append(self)
            }
            // A duplicate (or a missing dictionary key in a field that a duplicate
            // overwrites) can fail the direct append. The builder has rolled back.
            Err(DirectParseError::Append(error)) => {
                let mut parser = JsonParser::new(json, depth);
                let value = parser.buffer_value(0)?;
                if parser.saw_duplicate {
                    value.append(self)
                } else {
                    Err(error)
                }
            }
        }
    }
}

fn variant_from_number<'m, 'v>(n: &Number) -> Result<Variant<'m, 'v>, ArrowError> {
    if let Some(i) = n.as_i64() {
        Ok(variant_from_i64(i))
    } else {
        n.as_f64().map(Variant::from).ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!("Failed to parse {n} as number"))
        })
    }
}

fn variant_from_i64(i: i64) -> Variant<'static, 'static> {
    if i as i8 as i64 == i {
        (i as i8).into()
    } else if i as i16 as i64 == i {
        (i as i16).into()
    } else if i as i32 as i64 == i {
        (i as i32).into()
    } else {
        i.into()
    }
}

/// Parses a JSON number whose grammar has already been checked.
fn variant_from_float_text(value: &str) -> Result<Variant<'static, 'static>, ArrowError> {
    match lexical_core::parse::<f64>(value.as_bytes()) {
        Ok(float) if float.is_finite() => Ok(float.into()),
        _ => Err(ArrowError::InvalidArgumentError(format!(
            "Failed to parse {value} as finite number"
        ))),
    }
}

fn first_json_control(bytes: &[u8]) -> Option<usize> {
    const SUB: u64 = 0x2020_2020_2020_2020;
    const HIGH_BITS: u64 = 0x8080_8080_8080_8080;
    let (chunks, remainder) = bytes.as_chunks::<8>();
    let remainder_start = chunks.len() * 8;
    for (chunk_index, chunk) in chunks.iter().enumerate() {
        let word = u64::from_ne_bytes(*chunk);
        // The word test may overreport a hit; locate and confirm the control byte.
        if word.wrapping_sub(SUB) & !word & HIGH_BITS != 0
            && let Some(position) = chunk.iter().position(|byte| *byte < 0x20)
        {
            return Some(chunk_index * 8 + position);
        }
    }
    remainder
        .iter()
        .position(|byte| *byte < 0x20)
        .map(|position| remainder_start + position)
}

// Bound the dictionary width introduced by a single streaming parse. For wide
// documents, keep the legacy sorted object traversal so rebuilding a sparse
// projection does not repeatedly search a large unsorted dictionary.
const DIRECT_OBJECT_FIELD_BUDGET: usize = 64;

struct JsonParser<'a> {
    input: &'a str,
    bytes: &'a [u8],
    offset: usize,
    container_limit: usize,
    saw_duplicate: bool,
}

enum DirectParseError {
    Syntax(ArrowError),
    Append(ArrowError),
    UnstableFieldIds,
}

impl From<ArrowError> for DirectParseError {
    fn from(error: ArrowError) -> Self {
        Self::Syntax(error)
    }
}

enum JsonScalar<'a> {
    Value(Variant<'static, 'static>),
    String(Cow<'a, str>),
}

impl JsonScalar<'_> {
    fn append(self, builder: &mut impl VariantBuilderExt) -> Result<(), ArrowError> {
        match self {
            Self::Value(value) => builder.try_append_value(value),
            Self::String(value) => builder.try_append_value(value.as_ref()),
        }
    }
}

enum BufferedValue<'a> {
    Scalar(JsonScalar<'a>),
    Array(Vec<BufferedValue<'a>>),
    Object {
        fields: BTreeMap<Cow<'a, str>, BufferedValue<'a>>,
        duplicate: Option<Cow<'a, str>>,
    },
}

impl BufferedValue<'_> {
    fn append(self, builder: &mut impl VariantBuilderExt) -> Result<(), ArrowError> {
        match self {
            Self::Scalar(value) => value.append(builder)?,
            Self::Array(values) => {
                let mut list = builder.try_new_list()?;
                for value in values {
                    value.append(&mut list)?;
                }
                list.finish();
            }
            Self::Object { fields, duplicate } => {
                let mut object = builder.try_new_object()?;
                if object.validates_unique_fields()
                    && let Some(key) = duplicate
                {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Duplicate field name: {key}"
                    )));
                }
                for (key, value) in fields {
                    value.append(&mut ObjectFieldBuilder::new(key.as_ref(), &mut object))?;
                }
                object.finish();
            }
        }
        Ok(())
    }
}

impl<'a> JsonParser<'a> {
    fn new(input: &'a str, parent_depth: usize) -> Self {
        Self {
            input,
            bytes: input.as_bytes(),
            offset: 0,
            container_limit: MAX_NESTING_DEPTH.saturating_sub(parent_depth),
            saw_duplicate: false,
        }
    }

    fn may_exceed_field_budget(&mut self) -> bool {
        // The shortest field, "":0, needs four bytes. Count separators before
        // writing to avoid repeating a possibly large prefix at the cutoff.
        // Colons inside strings can overestimate, but never hide a wide object.
        if self.bytes.len() <= DIRECT_OBJECT_FIELD_BUDGET * 4 {
            return false;
        }
        self.skip_whitespace();
        matches!(self.peek(), Some(b'{' | b'['))
            && memchr::memchr_iter(b':', &self.bytes[self.offset..])
                .nth(DIRECT_OBJECT_FIELD_BUDGET)
                .is_some()
    }

    fn parse(&mut self, builder: &mut impl VariantBuilderExt) -> Result<(), DirectParseError> {
        self.parse_value(builder, 0)?;
        debug_assert_eq!(self.offset, self.bytes.len());
        Ok(())
    }

    fn parse_value(
        &mut self,
        builder: &mut impl VariantBuilderExt,
        depth: usize,
    ) -> Result<(), DirectParseError> {
        self.skip_whitespace();
        match self.peek() {
            Some(b'[') => {
                self.check_container_depth(depth)?;
                self.offset += 1;
                let mut list = builder.try_new_list().map_err(DirectParseError::Append)?;
                self.array_items(depth, |parser| parser.parse_value(&mut list, depth + 1))?;
                list.finish();
            }
            Some(b'{') => {
                self.check_container_depth(depth)?;
                self.offset += 1;
                let mut object = builder.try_new_object().map_err(DirectParseError::Append)?;
                if !object.has_stable_field_ids() {
                    return Err(DirectParseError::UnstableFieldIds);
                }
                object = object.with_validate_unique_fields(true);
                self.object_fields(depth, |parser, key| {
                    let mut field = ObjectFieldBuilder::new(key.as_ref(), &mut object);
                    parser.parse_value(&mut field, depth + 1)
                })?;
                object.finish();
            }
            _ => {
                let value = self.parse_scalar()?;
                self.ensure_root_end(depth)?;
                value.append(builder).map_err(DirectParseError::Append)?;
            }
        }
        Ok(())
    }

    fn parse_scalar(&mut self) -> Result<JsonScalar<'a>, ArrowError> {
        let value = match self.peek() {
            Some(b'n') => {
                self.parse_literal(b"null")?;
                Variant::Null
            }
            Some(b't') => {
                self.parse_literal(b"true")?;
                Variant::BooleanTrue
            }
            Some(b'f') => {
                self.parse_literal(b"false")?;
                Variant::BooleanFalse
            }
            Some(b'"') => return self.parse_string().map(JsonScalar::String),
            Some(b'-' | b'0'..=b'9') => match self.parse_number()? {
                (_, Some(integer)) => variant_from_i64(integer),
                (text, None) => variant_from_float_text(text)?,
            },
            Some(_) => return self.error("expected a JSON value"),
            None => return self.error("expected a JSON value, found end of input"),
        };
        Ok(JsonScalar::Value(value))
    }

    fn array_items<E: From<ArrowError>>(
        &mut self,
        depth: usize,
        mut item: impl FnMut(&mut Self) -> Result<(), E>,
    ) -> Result<(), E> {
        self.skip_whitespace();
        if !self.consume(b']') {
            loop {
                item(self)?;
                self.skip_whitespace();
                if self.consume(b']') {
                    break;
                }
                self.expect(b',', "expected ',' or ']' after array element")?;
            }
        }
        self.ensure_root_end(depth)?;
        Ok(())
    }

    fn object_fields<E: From<ArrowError>>(
        &mut self,
        depth: usize,
        mut field: impl FnMut(&mut Self, Cow<'a, str>) -> Result<(), E>,
    ) -> Result<(), E> {
        self.skip_whitespace();
        if !self.consume(b'}') {
            loop {
                if self.peek() != Some(b'"') {
                    return Err(self.format_error("expected a string object key").into());
                }
                let key = self.parse_string()?;
                self.skip_whitespace();
                self.expect(b':', "expected ':' after object key")?;
                field(self, key)?;
                self.skip_whitespace();
                if self.consume(b'}') {
                    break;
                }
                self.expect(b',', "expected ',' or '}' after object field")?;
                self.skip_whitespace();
            }
        }
        self.ensure_root_end(depth)?;
        Ok(())
    }

    fn buffer_value(&mut self, depth: usize) -> Result<BufferedValue<'a>, ArrowError> {
        self.skip_whitespace();
        match self.peek() {
            Some(b'[') => {
                self.check_container_depth(depth)?;
                self.offset += 1;
                let mut items = Vec::new();
                self.array_items::<ArrowError>(depth, |parser| {
                    items.push(parser.buffer_value(depth + 1)?);
                    Ok(())
                })?;
                Ok(BufferedValue::Array(items))
            }
            Some(b'{') => {
                self.check_container_depth(depth)?;
                self.offset += 1;
                let mut fields = BTreeMap::new();
                let mut duplicate = None;
                self.object_fields::<ArrowError>(depth, |parser, key| {
                    let value = parser.buffer_value(depth + 1)?;
                    match fields.entry(key) {
                        std::collections::btree_map::Entry::Vacant(entry) => {
                            entry.insert(value);
                        }
                        std::collections::btree_map::Entry::Occupied(mut entry) => {
                            parser.saw_duplicate = true;
                            if duplicate.is_none() {
                                duplicate = Some(entry.key().clone());
                            }
                            entry.insert(value);
                        }
                    }
                    Ok(())
                })?;
                Ok(BufferedValue::Object { fields, duplicate })
            }
            _ => {
                let value = self.parse_scalar()?;
                self.ensure_root_end(depth)?;
                Ok(BufferedValue::Scalar(value))
            }
        }
    }

    fn parse_string(&mut self) -> Result<Cow<'a, str>, ArrowError> {
        let token_start = self.offset;
        self.offset += 1;
        let content_start = self.offset;
        let mut escaped = false;

        while let Some(byte) = self.peek() {
            if self.offset - content_start >= 32 && byte != b'"' && byte != b'\\' {
                let remaining = &self.bytes[self.offset..];
                let scanned = memchr2(b'"', b'\\', remaining).unwrap_or(remaining.len());
                if let Some(control_at) = first_json_control(&remaining[..scanned]) {
                    self.offset += control_at;
                    return self.error("unescaped control character in string");
                }
                self.offset += scanned;
                continue;
            }
            match byte {
                b'"' => {
                    let content_end = self.offset;
                    self.offset += 1;
                    if escaped {
                        let value =
                            serde_json::from_slice::<String>(&self.bytes[token_start..self.offset])
                                .map_err(|error| {
                                    ArrowError::InvalidArgumentError(format!(
                                        "JSON format error in string at byte {token_start}: {error}"
                                    ))
                                })?;
                        return Ok(Cow::Owned(value));
                    }
                    let value = self
                        .input
                        .get(content_start..content_end)
                        .ok_or_else(|| self.format_error("invalid string boundary"))?;
                    return Ok(Cow::Borrowed(value));
                }
                b'\\' => {
                    escaped = true;
                    self.offset += 1;
                    if self.peek().is_none() {
                        return self.error("unterminated string escape");
                    }
                    self.offset += 1;
                }
                0..=0x1f => return self.error("unescaped control character in string"),
                _ => self.offset += 1,
            }
        }
        self.error("unterminated string")
    }

    fn parse_number(&mut self) -> Result<(&'a str, Option<i64>), ArrowError> {
        let start = self.offset;
        let negative = self.consume(b'-');
        let mut integer = Some(0_i64);

        match self.peek() {
            Some(b'0') => self.offset += 1,
            Some(b'1'..=b'9') => {
                while let Some(digit @ b'0'..=b'9') = self.peek() {
                    if let Some(value) = integer {
                        integer = value.checked_mul(10).and_then(|value| {
                            if negative {
                                value.checked_sub(i64::from(digit - b'0'))
                            } else {
                                value.checked_add(i64::from(digit - b'0'))
                            }
                        });
                    }
                    self.offset += 1;
                }
            }
            _ => return self.error("expected digit in number"),
        }

        if self.consume(b'.') {
            integer = None;
            let fraction_start = self.offset;
            while matches!(self.peek(), Some(b'0'..=b'9')) {
                self.offset += 1;
            }
            if self.offset == fraction_start {
                return self.error("expected digit after decimal point");
            }
        }

        if matches!(self.peek(), Some(b'e' | b'E')) {
            integer = None;
            self.offset += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.offset += 1;
            }
            let exponent_start = self.offset;
            while matches!(self.peek(), Some(b'0'..=b'9')) {
                self.offset += 1;
            }
            if self.offset == exponent_start {
                return self.error("expected digit in exponent");
            }
        }

        let text = self
            .input
            .get(start..self.offset)
            .ok_or_else(|| self.format_error("invalid number boundary"))?;
        Ok((text, integer))
    }

    fn parse_literal(&mut self, literal: &[u8]) -> Result<(), ArrowError> {
        if self.bytes[self.offset..].starts_with(literal) {
            self.offset += literal.len();
            Ok(())
        } else {
            self.error("invalid literal")
        }
    }

    fn check_container_depth(&self, depth: usize) -> Result<(), ArrowError> {
        if depth >= self.container_limit {
            return self.error("recursion limit exceeded");
        }
        Ok(())
    }

    fn ensure_root_end(&mut self, depth: usize) -> Result<(), ArrowError> {
        if depth != 0 {
            return Ok(());
        }
        self.skip_whitespace();
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            self.error("trailing characters")
        }
    }

    fn skip_whitespace(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\n' | b'\r' | b'\t')) {
            self.offset += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.offset).copied()
    }

    fn consume(&mut self, expected: u8) -> bool {
        if self.peek() == Some(expected) {
            self.offset += 1;
            true
        } else {
            false
        }
    }

    fn expect(&mut self, expected: u8, message: &str) -> Result<(), ArrowError> {
        if self.consume(expected) {
            Ok(())
        } else {
            self.error(message)
        }
    }

    fn error<T>(&self, message: &str) -> Result<T, ArrowError> {
        Err(self.format_error(message))
    }

    fn format_error(&self, message: impl std::fmt::Display) -> ArrowError {
        ArrowError::InvalidArgumentError(format!(
            "JSON format error at byte {}: {message}",
            self.offset
        ))
    }
}

/// Appends an already parsed [`Value`] to a Variant builder.
///
/// Non-integer [`Number`] values use `Double`.
pub fn append_json(json: &Value, builder: &mut impl VariantBuilderExt) -> Result<(), ArrowError> {
    match json {
        Value::Null => builder.try_append_value(Variant::Null)?,
        Value::Bool(b) => builder.try_append_value(*b)?,
        Value::Number(n) => {
            builder.try_append_value(variant_from_number(n)?)?;
        }
        Value::String(s) => builder.try_append_value(s.as_str())?,
        Value::Array(arr) => {
            let mut list_builder = builder.try_new_list()?;
            for val in arr {
                append_json(val, &mut list_builder)?;
            }
            list_builder.finish();
        }
        Value::Object(obj) => {
            let mut obj_builder = builder.try_new_object()?;
            for (key, value) in obj {
                let mut field_builder = ObjectFieldBuilder::new(key, &mut obj_builder);
                append_json(value, &mut field_builder)?;
            }
            obj_builder.finish();
        }
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::VariantToJson;
    use arrow_schema::ArrowError;
    use parquet_variant::{
        ShortString, Variant, VariantBuilder, VariantDecimal4, VariantDecimal8, VariantDecimal16,
    };

    #[test]
    fn test_first_json_control_across_word_boundaries() {
        let mut bytes = [b'x'; 64];
        for position in 0..bytes.len() {
            for control in 0..0x20 {
                bytes[position] = control;
                assert_eq!(first_json_control(&bytes), Some(position));
                bytes[position] = b'x';
            }
        }
        bytes[7] = 0x80;
        bytes[8] = 0xff;
        assert_eq!(first_json_control(&bytes), None);
    }

    struct JsonToVariantTest<'a> {
        json: &'a str,
        expected: Variant<'a, 'a>,
    }

    impl JsonToVariantTest<'_> {
        fn run(self) -> Result<(), ArrowError> {
            let mut variant_builder = VariantBuilder::new();
            variant_builder.append_json(self.json)?;
            let (metadata, value) = variant_builder.finish();
            let variant = Variant::try_new(&metadata, &value)?;
            assert_eq!(variant, self.expected);
            Ok(())
        }
    }

    #[test]
    fn test_json_to_variant_null() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: "null",
            expected: Variant::Null,
        }
        .run()
    }

    #[test]
    fn test_json_to_variant_boolean_true() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: "true",
            expected: Variant::BooleanTrue,
        }
        .run()
    }

    #[test]
    fn test_json_to_variant_boolean_false() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: "false",
            expected: Variant::BooleanFalse,
        }
        .run()
    }

    #[test]
    fn test_json_to_variant_int8_positive() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: "  127 ",
            expected: Variant::Int8(127),
        }
        .run()
    }

    #[test]
    fn test_json_to_variant_int8_negative() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: "  -128 ",
            expected: Variant::Int8(-128),
        }
        .run()
    }

    #[test]
    fn test_json_to_variant_int16() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: "  27134  ",
            expected: Variant::Int16(27134),
        }
        .run()
    }

    #[test]
    fn test_json_to_variant_int32() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: " -32767431  ",
            expected: Variant::Int32(-32767431),
        }
        .run()
    }

    #[test]
    fn test_json_to_variant_int64() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: "92842754201389",
            expected: Variant::Int64(92842754201389),
        }
        .run()
    }

    #[test]
    fn test_json_to_variant_number_boundaries() -> Result<(), ArrowError> {
        for (json, expected) in [
            ("-9223372036854775808", Variant::Int64(i64::MIN)),
            ("9223372036854775807", Variant::Int64(i64::MAX)),
            ("9223372036854775808", Variant::Double(2_f64.powi(63))),
            ("-9223372036854775809", Variant::Double(-2_f64.powi(63))),
            ("18446744073709551615", Variant::Double(2_f64.powi(64))),
            ("-0", Variant::Int8(0)),
            ("1.23", Variant::Double(1.23)),
        ] {
            JsonToVariantTest { json, expected }.run()?;
        }
        Ok(())
    }

    #[ignore]
    #[test]
    fn test_json_to_variant_decimal4_basic() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: "1.23",
            expected: Variant::from(VariantDecimal4::try_new(123, 2)?),
        }
        .run()
    }

    #[ignore]
    #[test]
    fn test_json_to_variant_decimal4_large_positive() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: "99999999.9",
            expected: Variant::from(VariantDecimal4::try_new(999999999, 1)?),
        }
        .run()
    }

    #[ignore]
    #[test]
    fn test_json_to_variant_decimal4_large_negative() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: "-99999999.9",
            expected: Variant::from(VariantDecimal4::try_new(-999999999, 1)?),
        }
        .run()
    }

    #[ignore]
    #[test]
    fn test_json_to_variant_decimal4_small_positive() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: "0.999999999",
            expected: Variant::from(VariantDecimal4::try_new(999999999, 9)?),
        }
        .run()
    }

    #[ignore]
    #[test]
    fn test_json_to_variant_decimal4_tiny_positive() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: "0.000000001",
            expected: Variant::from(VariantDecimal4::try_new(1, 9)?),
        }
        .run()
    }

    #[ignore]
    #[test]
    fn test_json_to_variant_decimal4_small_negative() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: "-0.999999999",
            expected: Variant::from(VariantDecimal4::try_new(-999999999, 9)?),
        }
        .run()
    }

    #[ignore]
    #[test]
    fn test_json_to_variant_decimal8_positive() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: "999999999.0",
            expected: Variant::from(VariantDecimal8::try_new(9999999990, 1)?),
        }
        .run()
    }

    #[ignore]
    #[test]
    fn test_json_to_variant_decimal8_negative() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: "-999999999.0",
            expected: Variant::from(VariantDecimal8::try_new(-9999999990, 1)?),
        }
        .run()
    }

    #[ignore]
    #[test]
    fn test_json_to_variant_decimal8_high_precision() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: "0.999999999999999999",
            expected: Variant::from(VariantDecimal8::try_new(999999999999999999, 18)?),
        }
        .run()
    }

    #[ignore]
    #[test]
    fn test_json_to_variant_decimal8_large_with_scale() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: "9999999999999999.99",
            expected: Variant::from(VariantDecimal8::try_new(999999999999999999, 2)?),
        }
        .run()
    }

    #[ignore]
    #[test]
    fn test_json_to_variant_decimal8_large_negative_with_scale() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: "-9999999999999999.99",
            expected: Variant::from(VariantDecimal8::try_new(-999999999999999999, 2)?),
        }
        .run()
    }

    #[ignore]
    #[test]
    fn test_json_to_variant_decimal16_large_integer() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: "9999999999999999999", // integer larger than i64
            expected: Variant::from(VariantDecimal16::try_new(9999999999999999999, 0)?),
        }
        .run()
    }

    #[ignore]
    #[test]
    fn test_json_to_variant_decimal16_high_precision() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: "0.9999999999999999999",
            expected: Variant::from(VariantDecimal16::try_new(9999999999999999999, 19)?),
        }
        .run()
    }

    #[ignore]
    #[test]
    fn test_json_to_variant_decimal16_max_value() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: "79228162514264337593543950335", // 2 ^ 96 - 1
            expected: Variant::from(VariantDecimal16::try_new(79228162514264337593543950335, 0)?),
        }
        .run()
    }

    #[ignore]
    #[test]
    fn test_json_to_variant_decimal16_max_scale() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: "7.9228162514264337593543950335", // using scale higher than this falls into double
            // since the max scale is 28.
            expected: Variant::from(VariantDecimal16::try_new(
                79228162514264337593543950335,
                28,
            )?),
        }
        .run()
    }

    #[test]
    fn test_json_to_variant_double_precision() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: "0.79228162514264337593543950335",
            // The old serde fallback rounded this one ULP higher.
            expected: Variant::Double(f64::from_bits(0x3fe9_5a5e_fea6_b347)),
        }
        .run()
    }

    #[test]
    fn test_json_to_variant_double_scientific_positive() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: "15e-1",
            expected: Variant::Double(15e-1f64),
        }
        .run()
    }

    #[test]
    fn test_json_to_variant_double_scientific_negative() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: "-15e-1",
            expected: Variant::Double(-15e-1f64),
        }
        .run()
    }

    #[test]
    fn test_json_to_variant_rounds_floats_correctly() -> Result<(), ArrowError> {
        // Without float_roundtrip, serde_json misrounds the first two values.
        for (json, bits) in [
            ("0.9999999999999999", 0x3fef_ffff_ffff_ffff),
            ("2.2250738585072011e-308", 0x000f_ffff_ffff_ffff),
            ("-0.0", (-0.0_f64).to_bits()),
        ] {
            let mut builder = VariantBuilder::new();
            builder.append_json(json)?;
            let (metadata, value) = builder.finish();
            let Variant::Double(actual) = Variant::try_new(&metadata, &value)? else {
                panic!("expected a Double for {json}")
            };
            assert_eq!(actual.to_bits(), bits, "{json}");
        }
        Ok(())
    }

    #[test]
    fn test_json_to_variant_short_string() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: "\"harsh\"",
            expected: Variant::ShortString(ShortString::try_new("harsh")?),
        }
        .run()
    }

    #[test]
    fn test_json_to_variant_short_string_max_length() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: &format!("\"{}\"", "a".repeat(63)),
            expected: Variant::ShortString(ShortString::try_new(&"a".repeat(63))?),
        }
        .run()
    }

    #[test]
    fn test_json_to_variant_long_string() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: &format!("\"{}\"", "a".repeat(64)),
            expected: Variant::String(&"a".repeat(64)),
        }
        .run()
    }

    #[test]
    fn test_json_to_variant_very_long_string() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: &format!("\"{}\"", "b".repeat(100000)),
            expected: Variant::String(&"b".repeat(100000)),
        }
        .run()
    }

    #[test]
    fn test_json_to_variant_array_simple() -> Result<(), ArrowError> {
        let mut variant_builder = VariantBuilder::new();
        let mut list_builder = variant_builder.new_list();
        list_builder.append_value(Variant::Int8(127));
        list_builder.append_value(Variant::Int16(128));
        list_builder.append_value(Variant::Int32(-32767431));
        list_builder.finish();
        let (metadata, value) = variant_builder.finish();
        let variant = Variant::try_new(&metadata, &value)?;

        JsonToVariantTest {
            json: "[127, 128, -32767431]",
            expected: variant,
        }
        .run()
    }

    #[test]
    fn test_json_to_variant_array_with_object() -> Result<(), ArrowError> {
        let mut variant_builder = VariantBuilder::new();
        let mut list_builder = variant_builder.new_list();
        let mut object_builder_inner = list_builder.new_object();
        object_builder_inner.insert("age", Variant::Int8(32));
        object_builder_inner.finish();
        list_builder.append_value(Variant::Int16(128));
        list_builder.append_value(Variant::BooleanFalse);
        list_builder.finish();
        let (metadata, value) = variant_builder.finish();
        let variant = Variant::try_new(&metadata, &value)?;

        JsonToVariantTest {
            json: "[{\"age\": 32}, 128, false]",
            expected: variant,
        }
        .run()
    }

    #[test]
    fn test_json_to_variant_array_large_u16_offset() -> Result<(), ArrowError> {
        // u16 offset - 128 i8's + 1 "true" = 257 bytes
        let mut variant_builder = VariantBuilder::new();
        let mut list_builder = variant_builder.new_list();
        for _ in 0..128 {
            list_builder.append_value(Variant::Int8(1));
        }
        list_builder.append_value(Variant::BooleanTrue);
        list_builder.finish();
        let (metadata, value) = variant_builder.finish();
        let variant = Variant::try_new(&metadata, &value)?;

        JsonToVariantTest {
            json: &format!("[{} true]", "1, ".repeat(128)),
            expected: variant,
        }
        .run()
    }

    #[test]
    fn test_json_to_variant_array_nested_large() -> Result<(), ArrowError> {
        // verify u24, and large_size
        let mut variant_builder = VariantBuilder::new();
        let mut list_builder = variant_builder.new_list();
        for _ in 0..256 {
            let mut list_builder_inner = list_builder.new_list();
            for _ in 0..255 {
                list_builder_inner.append_value(Variant::Null);
            }
            list_builder_inner.finish();
        }
        list_builder.finish();
        let (metadata, value) = variant_builder.finish();
        let variant = Variant::try_new(&metadata, &value)?;
        let intermediate = format!("[{}]", vec!["null"; 255].join(", "));
        let json = format!("[{}]", vec![intermediate; 256].join(", "));
        JsonToVariantTest {
            json: json.as_str(),
            expected: variant,
        }
        .run()
    }

    #[test]
    fn test_json_to_variant_object_simple() -> Result<(), ArrowError> {
        let mut variant_builder = VariantBuilder::new();
        let mut object_builder = variant_builder.new_object();
        object_builder.insert("a", Variant::Int8(3));
        object_builder.insert("b", Variant::Int8(2));
        object_builder.finish();
        let (metadata, value) = variant_builder.finish();
        let variant = Variant::try_new(&metadata, &value)?;
        JsonToVariantTest {
            json: "{\"b\": 2, \"a\": 1, \"a\": 3}",
            expected: variant,
        }
        .run()
    }

    #[test]
    fn test_json_to_variant_object_complex() -> Result<(), ArrowError> {
        let mut variant_builder = VariantBuilder::new();
        let mut object_builder = variant_builder.new_object();
        let mut inner_list_builder = object_builder.new_list("booleans");
        inner_list_builder.append_value(Variant::BooleanTrue);
        inner_list_builder.append_value(Variant::BooleanFalse);
        inner_list_builder.finish();
        object_builder.insert("null", Variant::Null);
        let mut inner_list_builder = object_builder.new_list("numbers");
        inner_list_builder.append_value(Variant::Int8(4));
        inner_list_builder.append_value(Variant::Double(-3e0));
        inner_list_builder.append_value(Variant::Double(1001e-3));
        inner_list_builder.finish();
        object_builder.finish();
        let (metadata, value) = variant_builder.finish();
        let variant = Variant::try_new(&metadata, &value)?;
        JsonToVariantTest {
            json: "{\"numbers\": [4, -3e0, 1001e-3], \"null\": null, \"booleans\": [true, false]}",
            expected: variant,
        }
        .run()
    }

    #[test]
    fn test_json_to_variant_object_very_large() -> Result<(), ArrowError> {
        // 256 elements (keys: 000-255) - each element is an object of 256 elements (240-495) - each
        // element a list of numbers from 0-127
        let keys: Vec<String> = (0..=255).map(|n| format!("{n:03}")).collect();
        let innermost_list: String = format!(
            "[{}]",
            (0..=127)
                .map(|n| format!("{n}"))
                .collect::<Vec<_>>()
                .join(",")
        );
        let inner_keys: Vec<String> = (240..=495).map(|n| format!("{n}")).collect();
        let inner_object = format!(
            "{{{}:{}}}",
            inner_keys
                .iter()
                .map(|k| format!("\"{k}\""))
                .collect::<Vec<String>>()
                .join(format!(":{innermost_list},").as_str()),
            innermost_list
        );
        let json = format!(
            "{{{}:{}}}",
            keys.iter()
                .map(|k| format!("\"{k}\""))
                .collect::<Vec<String>>()
                .join(format!(":{inner_object},").as_str()),
            inner_object
        );
        // Manually verify raw JSON value size
        let mut variant_builder = VariantBuilder::new();
        variant_builder.append_json(&json)?;
        let (metadata, value) = variant_builder.finish();
        let v = Variant::try_new(&metadata, &value)?;
        let output_string = v.to_json_string()?;
        assert_eq!(output_string, json);
        // Verify metadata size = 1 + 2 + 2 * 497 + 3 * 496
        assert_eq!(metadata.len(), 2485);
        // Verify value size.
        // Size of innermost_list: 1 + 1 + 2*(128 + 1) + 2*128 = 516
        // Size of inner object: 1 + 4 + 2*256 + 3*(256 + 1) + 256 * 516 = 133384
        // Size of json: 1 + 4 + 2*256 + 4*(256 + 1) + 256 * 133384 = 34147849
        assert_eq!(value.len(), 34147849);

        let mut variant_builder = VariantBuilder::new();
        let mut object_builder = variant_builder.new_object();
        keys.iter().for_each(|key| {
            let mut inner_object_builder = object_builder.new_object(key);
            inner_keys.iter().for_each(|inner_key| {
                let mut list_builder = inner_object_builder.new_list(inner_key);
                for i in 0..=127 {
                    list_builder.append_value(Variant::Int8(i));
                }
                list_builder.finish();
            });
            inner_object_builder.finish();
        });
        object_builder.finish();
        let (metadata, value) = variant_builder.finish();
        let variant = Variant::try_new(&metadata, &value)?;

        JsonToVariantTest {
            json: &json,
            expected: variant,
        }
        .run()
    }

    #[test]
    fn test_json_to_variant_unicode() -> Result<(), ArrowError> {
        let json = "{\"爱\":\"अ\",\"a\":1}";
        let mut variant_builder = VariantBuilder::new();
        variant_builder.append_json(json)?;
        let (metadata, value) = variant_builder.finish();
        let v = Variant::try_new(&metadata, &value)?;
        let output_string = v.to_json_string()?;
        assert_eq!(output_string, "{\"a\":1,\"爱\":\"अ\"}");
        let mut variant_builder = VariantBuilder::new();
        let mut object_builder = variant_builder.new_object();
        object_builder.insert("a", Variant::Int8(1));
        object_builder.insert("爱", Variant::ShortString(ShortString::try_new("अ")?));
        object_builder.finish();
        let (metadata, value) = variant_builder.finish();
        let variant = Variant::try_new(&metadata, &value)?;

        assert_eq!(
            value,
            &[
                2u8, 2u8, 0u8, 1u8, 0u8, 2u8, 6u8, 12u8, 1u8, 13u8, 0xe0u8, 0xa4u8, 0x85u8
            ]
        );
        assert_eq!(
            metadata,
            &[17u8, 2u8, 0u8, 1u8, 4u8, 97u8, 0xe7u8, 0x88u8, 0xb1u8]
        );
        JsonToVariantTest {
            json,
            expected: variant,
        }
        .run()
    }

    #[test]
    fn test_json_to_variant_rejects_invalid_json() -> Result<(), ArrowError> {
        let long_control = format!("\"{}\u{001f}\"", "x".repeat(64));
        let invalid = [
            "",
            "nul",
            "True",
            "[",
            "{",
            "[1,]",
            "[1 2]",
            "[1,,2]",
            "{\"a\":1,}",
            "{\"a\" 1}",
            "{\"a\":1 \"b\":2}",
            r#"{"a":0,"a":[1,],"a":2}"#,
            concat!("{", "a:1}"),
            "01",
            "-01",
            "+1",
            ".1",
            "1.",
            "1e",
            "1e+",
            "true false",
            "\"unterminated",
            "\"bad\\xescape\"",
            "\"bad\\u12x4\"",
            "\"lone high surrogate: \\ud800\"",
            "\"lone low surrogate: \\udc00\"",
            "\"control \u{0001} byte\"",
            &long_control,
            "1e400",
            "1x",
            "[]x",
            "{}x",
        ];

        for json in invalid {
            let mut builder = VariantBuilder::new();
            assert!(builder.append_json(json).is_err(), "accepted {json:?}");
            builder.append_json("2")?;
            let (metadata, value) = builder.finish();
            assert_eq!(
                Variant::try_new(&metadata, &value)?,
                Variant::Int8(2),
                "{json:?}"
            );
        }
        Ok(())
    }

    #[test]
    fn test_json_to_variant_limits_nesting_depth() -> Result<(), ArrowError> {
        // An empty innermost container still counts toward the limit.
        for (innermost, containers) in [("0", 0), ("[]", 1), ("{}", 1)] {
            let depth = MAX_NESTING_DEPTH - containers;
            let nested = |n| format!("{}{innermost}{}", "[".repeat(n), "]".repeat(n));
            let mut builder = VariantBuilder::new();
            builder.append_json(&nested(depth))?;
            let (metadata, value) = builder.finish();
            Variant::try_new(&metadata, &value)?;

            let error = VariantBuilder::new()
                .append_json(&nested(depth + 1))
                .unwrap_err()
                .to_string();
            assert!(error.contains("recursion limit"), "{innermost}: {error}");
        }
        Ok(())
    }

    #[test]
    fn test_nested_json_counts_its_parent_container() -> Result<(), ArrowError> {
        let nested = |depth| format!("{}[]{}", "[".repeat(depth), "]".repeat(depth));
        let mut builder = VariantBuilder::new();
        let mut list = builder.new_list();
        assert!(list.append_json(&nested(MAX_NESTING_DEPTH - 1)).is_err());
        list.append_json(&nested(MAX_NESTING_DEPTH - 2))?;
        list.finish();
        let (metadata, value) = builder.finish();
        Variant::try_new(&metadata, &value)?;

        let mut builder = VariantBuilder::new();
        let mut object = builder.new_object();
        let mut list = object.new_list("items");
        assert!(list.append_json(&nested(MAX_NESTING_DEPTH - 2)).is_err());
        list.append_json(&nested(MAX_NESTING_DEPTH - 3))?;
        list.finish();
        object.finish();
        let (metadata, value) = builder.finish();
        Variant::try_new(&metadata, &value)?;
        Ok(())
    }

    #[test]
    fn test_json_parse_failure_preserves_existing_dictionary_order() -> Result<(), ArrowError> {
        fn build(first: &str, fail: bool) -> Result<(Vec<u8>, Vec<u8>), ArrowError> {
            let mut builder = VariantBuilder::new();
            let mut list = builder.new_list();
            list.append_json(first)?;
            if fail {
                assert!(list.append_json(r#"{"A":0,}"#).is_err());
            }
            list.append_json("1")?;
            list.finish();
            Ok(builder.finish())
        }

        for first in [r#"{"a":0,"b":1}"#, r#"{"b":0,"a":1}"#] {
            assert_eq!(build(first, true)?, build(first, false)?);
        }
        Ok(())
    }

    #[test]
    fn test_wide_json_preserves_sorted_dictionary() -> Result<(), ArrowError> {
        use parquet_variant::VariantMetadata;

        let fields = 4096;
        let name = |id| format!("field_{id:026}");
        let entry = |id| format!("\"{}\":{id}", name(id));
        let flat = (0..fields).rev().map(entry).collect::<Vec<_>>().join(",");
        let nested = (0..fields)
            .step_by(2)
            .map(|id| format!("{{{},{}}}", entry(id + 1), entry(id)))
            .collect::<Vec<_>>()
            .join(",");
        for json in [format!("{{{flat}}}"), format!("[{nested}]")] {
            let mut builder = VariantBuilder::new();
            builder.append_json(&json)?;
            let (metadata, value) = builder.finish();
            let dictionary = VariantMetadata::try_new(&metadata)?;
            assert!(dictionary.is_sorted());
            assert_eq!(dictionary.len(), fields);
            assert_eq!(dictionary.get(0)?, name(0));
            assert_eq!(dictionary.get(fields - 1)?, name(fields - 1));
            let variant = Variant::try_new(&metadata, &value)?;
            let object = variant.get_list_element(0).unwrap_or(variant);
            assert_eq!(object.get_object_field(&name(0)), Some(Variant::Int8(0)));
        }
        Ok(())
    }

    #[test]
    fn test_wide_json_budget_and_duplicate_validation() -> Result<(), ArrowError> {
        let fields = (0..DIRECT_OBJECT_FIELD_BUDGET)
            .rev()
            .map(|id| format!("\"field_{id:02}\":0"))
            .collect::<Vec<_>>()
            .join(",");
        let at_budget = format!("{{{fields}}}");
        let over_budget = format!("{{{fields},\"field_00\":1}}");
        assert!(!JsonParser::new(&at_budget, 0).may_exceed_field_budget());
        assert!(JsonParser::new(&over_budget, 0).may_exceed_field_budget());

        let mut builder = VariantBuilder::new().with_validate_unique_fields(true);
        let mut list = builder.new_list();
        list.append_json("1")?;
        let error = list.append_json(&over_budget).unwrap_err().to_string();
        assert!(error.contains("Duplicate field name: field_00"), "{error}");
        list.append_json("2")?;
        list.finish();
        let (metadata, value) = builder.finish();
        let value = Variant::try_new(&metadata, &value)?;
        assert_eq!(value.get_list_element(0), Some(Variant::Int8(1)));
        assert_eq!(value.get_list_element(1), Some(Variant::Int8(2)));
        assert_eq!(value.get_list_element(2), None);
        Ok(())
    }

    #[test]
    fn test_json_duplicate_does_not_encode_overwritten_payload() -> Result<(), ArrowError> {
        fn encode(json: &str) -> Result<(Vec<u8>, Vec<u8>), ArrowError> {
            let mut builder = VariantBuilder::new();
            builder.append_json(json)?;
            Ok(builder.finish())
        }

        let json = r#"{"a":{"discarded":"overwritten payload"},"\u0061":0}"#;
        assert_eq!(encode(json)?, encode(r#"{"a":0}"#)?);
        Ok(())
    }

    #[test]
    fn test_json_duplicates_with_unstable_field_ids() -> Result<(), ArrowError> {
        use parquet_variant::{ListBuilder, MetadataBuilder, ParentState, ValueBuilder};

        #[derive(Debug, Default)]
        struct FreshIds(Vec<String>);

        impl MetadataBuilder for FreshIds {
            fn try_upsert_field_name(&mut self, name: &str) -> Result<u32, ArrowError> {
                let id = u32::try_from(self.0.len()).unwrap();
                self.0.push(name.to_owned());
                Ok(id)
            }

            fn field_name(&self, id: usize) -> &str {
                &self.0[id]
            }

            fn num_field_names(&self) -> usize {
                self.0.len()
            }

            fn truncate_field_names(&mut self, size: usize) {
                self.0.truncate(size);
            }

            fn finish(&mut self) -> usize {
                unreachable!("the test encodes the dictionary separately")
            }
        }

        for strict in [false, true] {
            let mut values = ValueBuilder::new();
            let mut dictionary = FreshIds::default();
            let mut list =
                ListBuilder::new(ParentState::variant(&mut values, &mut dictionary), strict);
            let parsed = list.append_json(r#"{"a":1,"\u0061":2}"#);
            if strict {
                assert!(
                    parsed
                        .unwrap_err()
                        .to_string()
                        .contains("Duplicate field name: a")
                );
                list.append_json("0")?;
            } else {
                parsed?;
            }
            list.finish();

            let expected_names: &[&str] = if strict { &[] } else { &["a"] };
            assert_eq!(dictionary.0, expected_names);
            let mut builder =
                VariantBuilder::new().with_field_names(dictionary.0.iter().map(String::as_str));
            builder.append_value(Variant::Null);
            let (metadata, _) = builder.finish();
            let values = values.into_inner();
            let item = Variant::try_new(&metadata, &values)?
                .get_list_element(0)
                .expect("one list element");
            if strict {
                assert_eq!(item, Variant::Int8(0));
            } else {
                assert_eq!(item.get_object_field("a"), Some(Variant::Int8(2)));
            }
        }
        Ok(())
    }

    #[test]
    fn test_json_to_variant_duplicate_keys_respect_validation() -> Result<(), ArrowError> {
        let mut builder = VariantBuilder::new().with_validate_unique_fields(true);
        let error = builder
            .append_json(r#"{"a":1,"a":2}"#)
            .unwrap_err()
            .to_string();
        assert!(error.contains("Duplicate field name: a"), "{error}");

        builder.append_json(r#"{"a":3}"#)?;
        let (metadata, value) = builder.finish();
        let variant = Variant::try_new(&metadata, &value)?;
        assert_eq!(variant.to_json_string()?, r#"{"a":3}"#);
        Ok(())
    }

    #[test]
    fn test_json_parser_matches_serde_for_non_numeric_semantics() -> Result<(), ArrowError> {
        let long_escaped = format!("\"{}\\n\\u00e9{}\"", "x".repeat(40), "y".repeat(40));
        let long_multibyte = format!("\"{}\u{e9}{}\"", "x".repeat(31), "\u{1f600}".repeat(10));
        let cases = [
            "null",
            "true",
            r#""plain""#,
            r#""escaped\ntext\t\"quote\"""#,
            r#""\uD834\uDD1E""#,
            r#"[null,true,false,"text",["nested"]]"#,
            r#"{"z":null,"a":[true,{"unicode":"\u2764"}]}"#,
            r#"{"duplicate":"first","duplicate":"last"}"#,
            r#"{"line\nkey":"quote: \"; slash: \\; unicode: \u2764"}"#,
            &long_escaped,
            &long_multibyte,
        ];

        for json in cases {
            let expected: Value = serde_json::from_str(json).unwrap();
            let mut builder = VariantBuilder::new();
            builder.append_json(json)?;
            let (metadata, value) = builder.finish();
            let actual = Variant::try_new(&metadata, &value)?.to_json_value()?;
            assert_eq!(actual, expected, "mismatch for {json}");
        }
        Ok(())
    }
}
