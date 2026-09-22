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

//! Module for parsing JSON strings as Variant

use arrow_schema::ArrowError;
use parquet_variant::{
    ObjectFieldBuilder, Variant, VariantBuilderExt, VariantDecimal4, VariantDecimal8,
    VariantDecimal16,
};
use serde_json::{Number, Value};
use std::borrow::Cow;

const MAX_JSON_DEPTH: usize = 128;

/// Converts a JSON string to Variant using a [`VariantBuilderExt`], such as
/// [`VariantBuilder`].
///
/// The resulting `value` and `metadata` buffers can be
/// extracted using `builder.finish()`
///
/// Integers use the smallest fitting integer encoding. Fixed-point numbers use the smallest
/// fitting Variant decimal encoding, while exponent notation and values outside the Variant
/// decimal range use `Double`.
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
        JsonParser::new(json).parse(self)
    }
}

fn variant_from_number<'m, 'v>(n: &Number) -> Result<Variant<'m, 'v>, ArrowError> {
    if let Some(i) = n.as_i64() {
        // Find minimum Integer width to fit
        if i as i8 as i64 == i {
            Ok((i as i8).into())
        } else if i as i16 as i64 == i {
            Ok((i as i16).into())
        } else if i as i32 as i64 == i {
            Ok((i as i32).into())
        } else {
            Ok(i.into())
        }
    } else {
        n.as_f64().map(Variant::from).ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!("Failed to parse {n} as number"))
        })
    }
}

fn variant_from_number_text(value: &str) -> Result<Variant<'static, 'static>, ArrowError> {
    if !value.contains(['.', 'e', 'E'])
        && let Ok(integer) = value.parse::<i64>()
    {
        return Ok(if integer as i8 as i64 == integer {
            (integer as i8).into()
        } else if integer as i16 as i64 == integer {
            (integer as i16).into()
        } else if integer as i32 as i64 == integer {
            (integer as i32).into()
        } else {
            integer.into()
        });
    }

    if let Some(decimal) = decimal_from_json_number(value) {
        return Ok(decimal);
    }

    let number = value.parse::<f64>().map_err(|error| {
        ArrowError::InvalidArgumentError(format!("Failed to parse {value} as number: {error}"))
    })?;
    if !number.is_finite() {
        return Err(ArrowError::InvalidArgumentError(format!(
            "Failed to parse {value} as finite number"
        )));
    }
    Ok(number.into())
}

/// Parses fixed-point JSON numbers and integers wider than `i64` without losing precision.
/// Scientific notation remains a floating-point value, matching the Variant JSON semantics.
fn decimal_from_json_number(value: &str) -> Option<Variant<'static, 'static>> {
    if value.contains(['e', 'E']) {
        return None;
    }

    let (negative, unsigned) = value
        .strip_prefix('-')
        .map_or((false, value), |value| (true, value));
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    if whole.is_empty()
        || !whole.bytes().all(|digit| digit.is_ascii_digit())
        || !fraction.bytes().all(|digit| digit.is_ascii_digit())
    {
        return None;
    }

    let scale = u8::try_from(fraction.len()).ok()?;
    let coefficient =
        whole
            .bytes()
            .chain(fraction.bytes())
            .try_fold(0_i128, |coefficient, digit| {
                coefficient
                    .checked_mul(10)?
                    .checked_add(i128::from(digit - b'0'))
            })?;
    let coefficient = if negative {
        coefficient.checked_neg()?
    } else {
        coefficient
    };

    i32::try_from(coefficient)
        .ok()
        .and_then(|coefficient| VariantDecimal4::try_new(coefficient, scale).ok())
        .map(Variant::from)
        .or_else(|| {
            i64::try_from(coefficient)
                .ok()
                .and_then(|coefficient| VariantDecimal8::try_new(coefficient, scale).ok())
                .map(Variant::from)
        })
        .or_else(|| {
            VariantDecimal16::try_new(coefficient, scale)
                .ok()
                .map(Variant::from)
        })
}

struct JsonParser<'a> {
    input: &'a str,
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> JsonParser<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            input,
            bytes: input.as_bytes(),
            offset: 0,
        }
    }

    fn parse(&mut self, builder: &mut impl VariantBuilderExt) -> Result<(), ArrowError> {
        self.skip_whitespace();
        self.parse_value(builder, 0)?;
        debug_assert_eq!(self.offset, self.bytes.len());
        Ok(())
    }

    fn parse_value(
        &mut self,
        builder: &mut impl VariantBuilderExt,
        depth: usize,
    ) -> Result<(), ArrowError> {
        if depth > MAX_JSON_DEPTH {
            return self.error("recursion limit exceeded");
        }

        self.skip_whitespace();
        match self.peek() {
            Some(b'n') => {
                self.parse_literal(b"null")?;
                self.ensure_root_end(depth)?;
                builder.try_append_value(Variant::Null)?;
            }
            Some(b't') => {
                self.parse_literal(b"true")?;
                self.ensure_root_end(depth)?;
                builder.try_append_value(true)?;
            }
            Some(b'f') => {
                self.parse_literal(b"false")?;
                self.ensure_root_end(depth)?;
                builder.try_append_value(false)?;
            }
            Some(b'"') => {
                let value = self.parse_string()?;
                self.ensure_root_end(depth)?;
                builder.try_append_value(value.as_ref())?;
            }
            Some(b'[') => self.parse_array(builder, depth)?,
            Some(b'{') => self.parse_object(builder, depth)?,
            Some(b'-' | b'0'..=b'9') => {
                let number = self.parse_number()?;
                let number = variant_from_number_text(number)?;
                self.ensure_root_end(depth)?;
                builder.try_append_value(number)?;
            }
            Some(_) => return self.error("expected a JSON value"),
            None => return self.error("expected a JSON value, found end of input"),
        }
        Ok(())
    }

    fn parse_array(
        &mut self,
        builder: &mut impl VariantBuilderExt,
        depth: usize,
    ) -> Result<(), ArrowError> {
        self.offset += 1;
        let mut list = builder.try_new_list()?;
        self.skip_whitespace();
        if self.consume(b']') {
            self.ensure_root_end(depth)?;
            list.finish();
            return Ok(());
        }

        loop {
            self.parse_value(&mut list, depth + 1)?;
            self.skip_whitespace();
            if self.consume(b']') {
                self.ensure_root_end(depth)?;
                list.finish();
                return Ok(());
            }
            self.expect(b',', "expected ',' or ']' after array element")?;
            self.skip_whitespace();
        }
    }

    fn parse_object(
        &mut self,
        builder: &mut impl VariantBuilderExt,
        depth: usize,
    ) -> Result<(), ArrowError> {
        self.offset += 1;
        let mut object = builder.try_new_object()?;
        self.skip_whitespace();
        if self.consume(b'}') {
            self.ensure_root_end(depth)?;
            object.finish();
            return Ok(());
        }

        loop {
            if self.peek() != Some(b'"') {
                return self.error("expected a string object key");
            }
            let key = self.parse_string()?;
            self.skip_whitespace();
            self.expect(b':', "expected ':' after object key")?;
            {
                let mut field = ObjectFieldBuilder::new(key.as_ref(), &mut object);
                self.parse_value(&mut field, depth + 1)?;
            }
            self.skip_whitespace();
            if self.consume(b'}') {
                self.ensure_root_end(depth)?;
                object.finish();
                return Ok(());
            }
            self.expect(b',', "expected ',' or '}' after object field")?;
            self.skip_whitespace();
        }
    }

    fn parse_string(&mut self) -> Result<Cow<'a, str>, ArrowError> {
        let token_start = self.offset;
        self.offset += 1;
        let content_start = self.offset;
        let mut escaped = false;

        while let Some(byte) = self.peek() {
            match byte {
                b'"' => {
                    let content_end = self.offset;
                    self.offset += 1;
                    if escaped {
                        let value =
                            serde_json::from_slice::<String>(&self.bytes[token_start..self.offset])
                                .map_err(|error| self.format_error(error.to_string()))?;
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

    fn parse_number(&mut self) -> Result<&'a str, ArrowError> {
        let start = self.offset;
        self.consume(b'-');

        match self.peek() {
            Some(b'0') => self.offset += 1,
            Some(b'1'..=b'9') => {
                self.offset += 1;
                while matches!(self.peek(), Some(b'0'..=b'9')) {
                    self.offset += 1;
                }
            }
            _ => return self.error("expected digit in number"),
        }

        if self.consume(b'.') {
            let fraction_start = self.offset;
            while matches!(self.peek(), Some(b'0'..=b'9')) {
                self.offset += 1;
            }
            if self.offset == fraction_start {
                return self.error("expected digit after decimal point");
            }
        }

        if matches!(self.peek(), Some(b'e' | b'E')) {
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

        self.input
            .get(start..self.offset)
            .ok_or_else(|| self.format_error("invalid number boundary"))
    }

    fn parse_literal(&mut self, literal: &[u8]) -> Result<(), ArrowError> {
        if self.bytes[self.offset..].starts_with(literal) {
            self.offset += literal.len();
            Ok(())
        } else {
            self.error("invalid literal")
        }
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
/// Unlike [`JsonToVariant::append_json`], this function cannot recover the original numeric
/// lexeme. Non-integer [`Number`] values therefore use `Double`; use the string API when exact
/// fixed-point decimal semantics are required.
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
    fn test_json_to_variant_decimal4_basic() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: "1.23",
            expected: Variant::from(VariantDecimal4::try_new(123, 2)?),
        }
        .run()
    }

    #[test]
    fn test_json_to_variant_decimal4_large_positive() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: "99999999.9",
            expected: Variant::from(VariantDecimal4::try_new(999999999, 1)?),
        }
        .run()
    }

    #[test]
    fn test_json_to_variant_decimal4_large_negative() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: "-99999999.9",
            expected: Variant::from(VariantDecimal4::try_new(-999999999, 1)?),
        }
        .run()
    }

    #[test]
    fn test_json_to_variant_decimal4_small_positive() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: "0.999999999",
            expected: Variant::from(VariantDecimal4::try_new(999999999, 9)?),
        }
        .run()
    }

    #[test]
    fn test_json_to_variant_decimal4_tiny_positive() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: "0.000000001",
            expected: Variant::from(VariantDecimal4::try_new(1, 9)?),
        }
        .run()
    }

    #[test]
    fn test_json_to_variant_decimal4_small_negative() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: "-0.999999999",
            expected: Variant::from(VariantDecimal4::try_new(-999999999, 9)?),
        }
        .run()
    }

    #[test]
    fn test_json_to_variant_decimal8_positive() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: "999999999.0",
            expected: Variant::from(VariantDecimal8::try_new(9999999990, 1)?),
        }
        .run()
    }

    #[test]
    fn test_json_to_variant_decimal8_negative() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: "-999999999.0",
            expected: Variant::from(VariantDecimal8::try_new(-9999999990, 1)?),
        }
        .run()
    }

    #[test]
    fn test_json_to_variant_decimal8_high_precision() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: "0.999999999999999999",
            expected: Variant::from(VariantDecimal8::try_new(999999999999999999, 18)?),
        }
        .run()
    }

    #[test]
    fn test_json_to_variant_decimal8_large_with_scale() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: "9999999999999999.99",
            expected: Variant::from(VariantDecimal8::try_new(999999999999999999, 2)?),
        }
        .run()
    }

    #[test]
    fn test_json_to_variant_decimal8_large_negative_with_scale() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: "-9999999999999999.99",
            expected: Variant::from(VariantDecimal8::try_new(-999999999999999999, 2)?),
        }
        .run()
    }

    #[test]
    fn test_json_to_variant_decimal16_large_integer() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: "9999999999999999999", // integer larger than i64
            expected: Variant::from(VariantDecimal16::try_new(9999999999999999999, 0)?),
        }
        .run()
    }

    #[test]
    fn test_json_to_variant_decimal16_high_precision() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: "0.9999999999999999999",
            expected: Variant::from(VariantDecimal16::try_new(9999999999999999999, 19)?),
        }
        .run()
    }

    #[test]
    fn test_json_to_variant_decimal16_29_digit_value() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: "79228162514264337593543950335",
            expected: Variant::from(VariantDecimal16::try_new(79228162514264337593543950335, 0)?),
        }
        .run()
    }

    #[test]
    fn test_json_to_variant_decimal16_scale_28() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: "7.9228162514264337593543950335",
            expected: Variant::from(VariantDecimal16::try_new(
                79228162514264337593543950335,
                28,
            )?),
        }
        .run()
    }

    #[test]
    fn test_json_to_variant_nested_decimals() -> Result<(), ArrowError> {
        let mut variant_builder = VariantBuilder::new();
        let mut object_builder = variant_builder.new_object();
        object_builder.insert("large", VariantDecimal16::try_new(9999999999999999999, 0)?);
        let mut list_builder = object_builder.new_list("values");
        list_builder.append_value(VariantDecimal4::try_new(123, 2)?);
        list_builder.append_value(VariantDecimal8::try_new(9999999990, 1)?);
        list_builder.append_value(1.5e2_f64);
        list_builder.finish();
        object_builder.finish();
        let (metadata, value) = variant_builder.finish();

        JsonToVariantTest {
            json: r#"{"large":9999999999999999999,"values":[1.23,999999999.0,1.5e2]}"#,
            expected: Variant::try_new(&metadata, &value)?,
        }
        .run()
    }

    #[test]
    fn test_json_to_variant_double_precision() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: "0.100000000000000000000000000000000000000",
            expected: Variant::Double(0.1_f64),
        }
        .run()
    }

    #[test]
    fn test_json_to_variant_decimal16_max_precision_and_scale() -> Result<(), ArrowError> {
        JsonToVariantTest {
            json: "0.99999999999999999999999999999999999999",
            expected: Variant::from(VariantDecimal16::try_new(
                99999999999999999999999999999999999999,
                38,
            )?),
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
    fn test_json_to_variant_escaped_strings() -> Result<(), ArrowError> {
        let json = r#"{"line\nkey":"quote: \"; slash: \\; unicode: \u2764"}"#;
        let mut variant_builder = VariantBuilder::new();
        variant_builder.append_json(json)?;
        let (metadata, value) = variant_builder.finish();
        let variant = Variant::try_new(&metadata, &value)?;
        assert_eq!(
            variant.to_json_string()?,
            "{\"line\\nkey\":\"quote: \\\"; slash: \\\\; unicode: ❤\"}"
        );
        Ok(())
    }

    #[test]
    fn test_json_to_variant_rejects_invalid_json() {
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
        ];

        for json in invalid {
            let mut builder = VariantBuilder::new();
            assert!(builder.append_json(json).is_err(), "accepted {json:?}");
        }
    }

    #[test]
    fn test_json_to_variant_rejects_non_finite_double() {
        let mut builder = VariantBuilder::new();
        let error = builder.append_json("1e400").unwrap_err().to_string();
        assert!(error.contains("finite number"), "{error}");
    }

    #[test]
    fn test_json_to_variant_limits_nesting_depth() {
        let accepted = format!(
            "{}0{}",
            "[".repeat(MAX_JSON_DEPTH),
            "]".repeat(MAX_JSON_DEPTH)
        );
        let mut builder = VariantBuilder::new();
        builder.append_json(&accepted).unwrap();

        let rejected = format!(
            "{}0{}",
            "[".repeat(MAX_JSON_DEPTH + 1),
            "]".repeat(MAX_JSON_DEPTH + 1)
        );
        let mut builder = VariantBuilder::new();
        let error = builder.append_json(&rejected).unwrap_err().to_string();
        assert!(error.contains("recursion limit"), "{error}");
    }

    #[test]
    fn test_json_to_variant_error_does_not_modify_builder() -> Result<(), ArrowError> {
        for invalid in ["1x", "true false", "[]x", "{}x", "[1,]", "{\"a\":1,}"] {
            let mut builder = VariantBuilder::new();
            assert!(
                builder.append_json(invalid).is_err(),
                "accepted {invalid:?}"
            );
            builder.append_json("2")?;
            let (metadata, value) = builder.finish();
            assert_eq!(Variant::try_new(&metadata, &value)?, Variant::Int8(2));
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
        let cases = [
            "null",
            "true",
            r#""plain""#,
            r#""escaped\ntext\t\"quote\"""#,
            r#""\uD834\uDD1E""#,
            r#"[null,true,false,"text",["nested"]]"#,
            r#"{"z":null,"a":[true,{"unicode":"\u2764"}]}"#,
            r#"{"duplicate":"first","duplicate":"last"}"#,
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

    #[test]
    fn test_string_and_value_apis_document_numeric_difference() -> Result<(), ArrowError> {
        let mut text_builder = VariantBuilder::new();
        text_builder.append_json("1.23")?;
        let (text_metadata, text_value) = text_builder.finish();
        assert_eq!(
            Variant::try_new(&text_metadata, &text_value)?,
            Variant::from(VariantDecimal4::try_new(123, 2)?)
        );

        let parsed: Value = serde_json::from_str("1.23").unwrap();
        let mut value_builder = VariantBuilder::new();
        append_json(&parsed, &mut value_builder)?;
        let (value_metadata, value_value) = value_builder.finish();
        assert_eq!(
            Variant::try_new(&value_metadata, &value_value)?,
            Variant::Double(1.23)
        );
        Ok(())
    }
}
