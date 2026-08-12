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

//! Array support for the canonical `arrow.big_decimal` extension type.
//!
//! Rust represents extension types as metadata on an Arrow [`Field`]. This
//! module supplies the value codec and a typed wrapper over the physical
//! [`BinaryViewArray`]. Use [`BigDecimal`] from `arrow_schema::extension` when
//! constructing the corresponding field.
//!
//! [`BigDecimalArray`] is a validated logical view and deliberately does not
//! implement [`Array`]. Record batches contain its [`BinaryViewArray`] storage,
//! with the logical extension carried by [`Field`] metadata. Use
//! [`BigDecimalArray::into_storage`] when writing and
//! [`BigDecimalArray::try_from_array`] to recover the view after reading.
//!
//! [`Field`]: arrow_schema::Field

use std::fmt::{Display, Formatter};
use std::str::FromStr;

use arrow_buffer::i256;
use arrow_schema::extension::BigDecimal;
use arrow_schema::{ArrowError, Field};
use num_bigint::{BigInt, BigUint};
use num_integer::Integer;
use num_traits::{Signed, ToPrimitive, Zero};

use crate::builder::{ArrayBuilder, BinaryViewBuilder};
use crate::types::{Decimal128Type, Decimal256Type, validate_decimal_precision_and_scale};
use crate::{Array, BinaryViewArray, Decimal128Array, Decimal256Array};

/// The number of header and scale bytes before a finite magnitude.
pub const BIG_DECIMAL_PREFIX_LENGTH: usize = 3;

/// Numeric categories encoded in the first byte of a BigDecimal value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum BigDecimalClass {
    /// Negative signaling NaN.
    NegativeSignalingNaN = 0x00,
    /// Negative quiet NaN.
    NegativeQuietNaN = 0x01,
    /// Negative infinity.
    NegativeInfinity = 0x02,
    /// Negative finite number.
    NegativeFinite = 0x03,
    /// Positive finite number, including positive zero.
    PositiveFinite = 0x04,
    /// Positive infinity.
    PositiveInfinity = 0x05,
    /// Positive quiet NaN.
    PositiveQuietNaN = 0x06,
    /// Positive signaling NaN.
    PositiveSignalingNaN = 0x07,
}

impl BigDecimalClass {
    /// Returns true for either finite class.
    pub fn is_finite(self) -> bool {
        matches!(self, Self::NegativeFinite | Self::PositiveFinite)
    }

    /// Returns true for an infinity class.
    pub fn is_infinite(self) -> bool {
        matches!(self, Self::NegativeInfinity | Self::PositiveInfinity)
    }

    /// Returns true for any quiet or signaling NaN class.
    pub fn is_nan(self) -> bool {
        matches!(
            self,
            Self::NegativeSignalingNaN
                | Self::NegativeQuietNaN
                | Self::PositiveQuietNaN
                | Self::PositiveSignalingNaN
        )
    }

    /// Returns true for a negative class.
    pub fn is_negative(self) -> bool {
        matches!(
            self,
            Self::NegativeSignalingNaN
                | Self::NegativeQuietNaN
                | Self::NegativeInfinity
                | Self::NegativeFinite
        )
    }
}

impl TryFrom<u8> for BigDecimalClass {
    type Error = ArrowError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x00 => Ok(Self::NegativeSignalingNaN),
            0x01 => Ok(Self::NegativeQuietNaN),
            0x02 => Ok(Self::NegativeInfinity),
            0x03 => Ok(Self::NegativeFinite),
            0x04 => Ok(Self::PositiveFinite),
            0x05 => Ok(Self::PositiveInfinity),
            0x06 => Ok(Self::PositiveQuietNaN),
            0x07 => Ok(Self::PositiveSignalingNaN),
            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Invalid BigDecimal class byte: 0x{value:02X}"
            ))),
        }
    }
}

/// A decoded BigDecimal value.
///
/// Finite values represent `magnitude * 10^-scale`, with the sign carried by
/// [`Self::class`]. This preserves distinct encodings of equal mathematical
/// values, including negative zero and negative scales.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BigDecimalValue {
    class: BigDecimalClass,
    scale: i16,
    magnitude: Vec<u8>,
}

impl BigDecimalValue {
    /// Constructs a value from an unsigned little-endian magnitude and
    /// validates class-specific invariants.
    ///
    /// Finite magnitudes must contain at least one byte and use their minimal
    /// representation. Non-finite values must pass an empty magnitude.
    pub fn try_new(
        class: BigDecimalClass,
        scale: i16,
        magnitude: impl AsRef<[u8]>,
    ) -> Result<Self, ArrowError> {
        let magnitude = magnitude.as_ref();
        if !class.is_finite() && (scale != 0 || !magnitude.is_empty()) {
            return Err(ArrowError::InvalidArgumentError(
                "Non-finite BigDecimal values must have scale zero and no magnitude".to_owned(),
            ));
        }
        if class.is_finite() && magnitude.is_empty() {
            return Err(ArrowError::InvalidArgumentError(
                "Finite BigDecimal must have at least one magnitude byte".to_owned(),
            ));
        }
        if class.is_finite() && magnitude.len() > 1 && magnitude.last() == Some(&0) {
            return Err(ArrowError::InvalidArgumentError(
                "BigDecimal magnitude must use the minimal little-endian encoding".to_owned(),
            ));
        }
        Ok(Self {
            class,
            scale,
            magnitude: magnitude.to_vec(),
        })
    }

    /// Constructs a finite value from a minimal unsigned little-endian
    /// magnitude. Zero is represented by `[0]`.
    pub fn finite(
        magnitude: impl AsRef<[u8]>,
        scale: i16,
        negative: bool,
    ) -> Result<Self, ArrowError> {
        let class = if negative {
            BigDecimalClass::NegativeFinite
        } else {
            BigDecimalClass::PositiveFinite
        };
        Self::try_new(class, scale, magnitude)
    }

    /// Constructs a non-finite value.
    pub fn non_finite(class: BigDecimalClass) -> Result<Self, ArrowError> {
        if class.is_finite() {
            return Err(ArrowError::InvalidArgumentError(
                "Expected a non-finite BigDecimal class".to_owned(),
            ));
        }
        Self::try_new(class, 0, [])
    }

    /// Returns the numeric class.
    pub fn class(&self) -> BigDecimalClass {
        self.class
    }

    /// Returns the row-level scale.
    pub fn scale(&self) -> i16 {
        self.scale
    }

    /// Returns the minimal unsigned little-endian magnitude, or `None` for a
    /// non-finite value. Finite zero returns `[0]`.
    pub fn magnitude_le_bytes(&self) -> Option<&[u8]> {
        self.class.is_finite().then_some(&self.magnitude)
    }

    /// Encodes this value using layout version 1.
    pub fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(BIG_DECIMAL_PREFIX_LENGTH + 16);
        encoded.push(self.class as u8);
        encoded.extend_from_slice(&self.scale.to_le_bytes());
        if self.class.is_finite() {
            encoded.extend_from_slice(&self.magnitude);
        }
        encoded
    }

    /// Decodes and validates a layout version 1 value.
    pub fn decode(encoded: &[u8]) -> Result<Self, ArrowError> {
        if encoded.len() < BIG_DECIMAL_PREFIX_LENGTH {
            return Err(ArrowError::InvalidArgumentError(format!(
                "BigDecimal value is shorter than {BIG_DECIMAL_PREFIX_LENGTH} bytes"
            )));
        }

        let class = BigDecimalClass::try_from(encoded[0])?;
        let scale = i16::from_le_bytes([encoded[1], encoded[2]]);
        let magnitude = &encoded[BIG_DECIMAL_PREFIX_LENGTH..];

        Self::try_new(class, scale, magnitude)
    }
}

impl Display for BigDecimalValue {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        let literal = match self.class {
            BigDecimalClass::NegativeSignalingNaN => return formatter.write_str("-sNaN"),
            BigDecimalClass::NegativeQuietNaN => return formatter.write_str("-NaN"),
            BigDecimalClass::NegativeInfinity => return formatter.write_str("-Infinity"),
            BigDecimalClass::PositiveInfinity => return formatter.write_str("Infinity"),
            BigDecimalClass::PositiveQuietNaN => return formatter.write_str("NaN"),
            BigDecimalClass::PositiveSignalingNaN => return formatter.write_str("sNaN"),
            BigDecimalClass::NegativeFinite | BigDecimalClass::PositiveFinite => {
                BigUint::from_bytes_le(&self.magnitude).to_str_radix(10)
            }
        };

        if self.class.is_negative() {
            formatter.write_str("-")?;
        }
        if self.scale == 0 {
            return formatter.write_str(&literal);
        }
        if self.scale < 0 {
            return write!(formatter, "{literal}E{}", -i32::from(self.scale));
        }

        let scale = self.scale as usize;
        if literal.len() > scale {
            let split = literal.len() - scale;
            write!(formatter, "{}.{}", &literal[..split], &literal[split..])
        } else {
            write!(
                formatter,
                "0.{}{literal}",
                "0".repeat(scale - literal.len())
            )
        }
    }
}

impl FromStr for BigDecimalValue {
    type Err = ArrowError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let original = value;
        let class = match value {
            "-sNaN" => Some(BigDecimalClass::NegativeSignalingNaN),
            "-NaN" => Some(BigDecimalClass::NegativeQuietNaN),
            "-Infinity" => Some(BigDecimalClass::NegativeInfinity),
            "Infinity" => Some(BigDecimalClass::PositiveInfinity),
            "NaN" => Some(BigDecimalClass::PositiveQuietNaN),
            "sNaN" => Some(BigDecimalClass::PositiveSignalingNaN),
            _ => None,
        };
        if let Some(class) = class {
            return Self::non_finite(class);
        }

        let (negative, value) = match value.strip_prefix('-') {
            Some(value) => (true, value),
            None => (false, value),
        };
        if value.is_empty() {
            return Err(parse_error("BigDecimal has no digits"));
        }

        let exponent_index = value.bytes().position(|byte| matches!(byte, b'e' | b'E'));
        let (mantissa, exponent) = match exponent_index {
            Some(index) => {
                let exponent = &value[index + 1..];
                if exponent.is_empty() || exponent.bytes().any(|byte| matches!(byte, b'e' | b'E')) {
                    return Err(parse_error("Invalid BigDecimal exponent"));
                }
                let exponent = exponent
                    .parse::<i32>()
                    .map_err(|_| parse_error("Invalid BigDecimal exponent"))?;
                (&value[..index], exponent)
            }
            None => (value, 0),
        };

        let mut pieces = mantissa.split('.');
        let integer = pieces.next().unwrap_or_default();
        let fraction = pieces.next().unwrap_or_default();
        if pieces.next().is_some() || (integer.is_empty() && fraction.is_empty()) {
            return Err(parse_error("Invalid BigDecimal decimal point"));
        }
        if !integer.bytes().all(|byte| byte.is_ascii_digit())
            || !fraction.bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(parse_error("BigDecimal contains a non-digit"));
        }

        let scale = i64::try_from(fraction.len()).unwrap_or(i64::MAX) - i64::from(exponent);
        let scale = i16::try_from(scale)
            .map_err(|_| parse_error("BigDecimal scale is outside the int16 range"))?;
        let digits = format!("{integer}{fraction}");
        let magnitude = BigUint::parse_bytes(digits.as_bytes(), 10)
            .ok_or_else(|| parse_error("BigDecimal has no digits"))?;
        let mut magnitude = magnitude.to_bytes_le();
        if magnitude.is_empty() {
            magnitude.push(0);
        }
        let value = Self::finite(magnitude, scale, negative)?;
        if value.to_string() != original {
            return Err(parse_error("BigDecimal string is not canonical"));
        }
        Ok(value)
    }
}

fn parse_error(message: &str) -> ArrowError {
    ArrowError::ParseError(message.to_owned())
}

/// A validated logical view over a physical [`BinaryViewArray`].
///
/// This type does not implement [`Array`]. In arrow-rs, an extension type is
/// represented by [`Field`] metadata and the corresponding record-batch column
/// remains its physical storage array. Use [`Self::storage`] or
/// [`Self::into_storage`] when constructing a batch, and
/// [`Self::try_from_array`] to recover this view from a batch column and field.
#[derive(Debug, Clone)]
pub struct BigDecimalArray {
    storage: BinaryViewArray,
    extension: BigDecimal,
}

impl BigDecimalArray {
    /// Creates an array and validates every non-null encoded value.
    pub fn try_new(storage: BinaryViewArray, extension: BigDecimal) -> Result<Self, ArrowError> {
        let array = Self { storage, extension };
        array.validate_full()?;
        Ok(array)
    }

    /// Recovers a BigDecimal view from an arbitrary physical array and its
    /// schema field.
    ///
    /// This validates the field's extension metadata, downcasts the physical
    /// array to [`BinaryViewArray`], and validates every non-null value. The
    /// cloned storage shares its underlying buffers with `array`.
    pub fn try_from_array(array: &dyn Array, field: &Field) -> Result<Self, ArrowError> {
        let extension = field.try_extension_type::<BigDecimal>()?;
        let storage = array
            .as_any()
            .downcast_ref::<BinaryViewArray>()
            .ok_or_else(|| {
                ArrowError::InvalidArgumentError(format!(
                    "BigDecimal storage array mismatch, expected BinaryViewArray, found {}",
                    array.data_type()
                ))
            })?;
        Self::try_new(storage.clone(), extension)
    }

    /// Returns the physical storage array.
    pub fn storage(&self) -> &BinaryViewArray {
        &self.storage
    }

    /// Consumes this wrapper and returns its physical storage.
    pub fn into_storage(self) -> BinaryViewArray {
        self.storage
    }

    /// Returns the extension type, including its metadata identity.
    pub fn extension_type(&self) -> &BigDecimal {
        &self.extension
    }

    /// Returns the number of rows.
    pub fn len(&self) -> usize {
        self.storage.len()
    }

    /// Returns true if the array has no rows.
    pub fn is_empty(&self) -> bool {
        self.storage.is_empty()
    }

    /// Returns true if the row at `index` is null.
    ///
    /// # Panics
    ///
    /// Panics if `index` is outside this array.
    pub fn is_null(&self, index: usize) -> bool {
        self.storage.is_null(index)
    }

    /// Returns the encoded bytes at `index`, or `None` for a null row.
    ///
    /// # Panics
    ///
    /// Panics if `index` is outside this array.
    pub fn encoded_value(&self, index: usize) -> Option<&[u8]> {
        (!self.storage.is_null(index)).then(|| self.storage.value(index))
    }

    /// Decodes the value at `index`, or returns `None` for a null row.
    ///
    /// # Panics
    ///
    /// Panics if `index` is outside this array.
    pub fn value(&self, index: usize) -> Result<Option<BigDecimalValue>, ArrowError> {
        self.encoded_value(index)
            .map(BigDecimalValue::decode)
            .transpose()
    }

    /// Validates all encoded values.
    ///
    /// Informational extension metadata is not used to reject row values.
    pub fn validate_full(&self) -> Result<(), ArrowError> {
        for index in 0..self.storage.len() {
            if let Some(encoded) = self.encoded_value(index) {
                BigDecimalValue::decode(encoded).map_err(|error| {
                    ArrowError::InvalidArgumentError(format!(
                        "Invalid BigDecimal at row {index}: {error}"
                    ))
                })?;
            }
        }
        Ok(())
    }

    /// Converts every finite row exactly to a [`Decimal128Array`].
    ///
    /// This returns an error for a non-finite row, a rescaling operation that
    /// would discard non-zero digits, or a value outside the requested
    /// precision or Decimal128 integer range.
    pub fn to_decimal128(&self, precision: u8, scale: i8) -> Result<Decimal128Array, ArrowError> {
        validate_decimal_precision_and_scale::<Decimal128Type>(precision, scale)?;
        let mut output = Vec::with_capacity(self.len());
        for index in 0..self.len() {
            let Some(value) = self.value(index)? else {
                output.push(None);
                continue;
            };
            let coefficient = rescale_exact(&value, scale, precision)
                .map_err(|error| conversion_error(index, "Decimal128", error))?;
            let coefficient = coefficient.to_i128().ok_or_else(|| {
                conversion_error(
                    index,
                    "Decimal128",
                    "coefficient is outside the signed 128-bit range",
                )
            })?;
            output.push(Some(coefficient));
        }
        Decimal128Array::from(output).with_precision_and_scale(precision, scale)
    }

    /// Converts every finite row exactly to a [`Decimal256Array`].
    ///
    /// This returns an error for a non-finite row, a rescaling operation that
    /// would discard non-zero digits, or a value outside the requested
    /// precision or Decimal256 integer range.
    pub fn to_decimal256(&self, precision: u8, scale: i8) -> Result<Decimal256Array, ArrowError> {
        validate_decimal_precision_and_scale::<Decimal256Type>(precision, scale)?;
        let mut output = Vec::with_capacity(self.len());
        for index in 0..self.len() {
            let Some(value) = self.value(index)? else {
                output.push(None);
                continue;
            };
            let coefficient = rescale_exact(&value, scale, precision)
                .map_err(|error| conversion_error(index, "Decimal256", error))?;
            let coefficient = coefficient.to_string().parse::<i256>().map_err(|_| {
                conversion_error(
                    index,
                    "Decimal256",
                    "coefficient is outside the signed 256-bit range",
                )
            })?;
            output.push(Some(coefficient));
        }
        Decimal256Array::from(output).with_precision_and_scale(precision, scale)
    }
}

fn rescale_exact(
    value: &BigDecimalValue,
    target_scale: i8,
    precision: u8,
) -> Result<BigInt, &'static str> {
    if !value.class.is_finite() {
        return Err("non-finite values cannot be represented by an Arrow decimal");
    }

    let mut coefficient = BigInt::from(BigUint::from_bytes_le(&value.magnitude));
    if value.class.is_negative() {
        coefficient = -coefficient;
    }

    let scale_delta = i32::from(target_scale) - i32::from(value.scale);
    let factor = || BigInt::from(10_u8).pow(scale_delta.unsigned_abs());
    if scale_delta > 0 {
        coefficient *= factor();
    } else if scale_delta < 0 {
        let (quotient, remainder) = coefficient.div_rem(&factor());
        if !remainder.is_zero() {
            return Err("conversion would lose non-zero fractional digits");
        }
        coefficient = quotient;
    }

    let digits = coefficient.abs().to_str_radix(10).len();
    if digits > usize::from(precision) {
        return Err("coefficient exceeds the requested decimal precision");
    }
    Ok(coefficient)
}

fn conversion_error(index: usize, target: &str, message: &str) -> ArrowError {
    ArrowError::InvalidArgumentError(format!(
        "Cannot convert BigDecimal row {index} to {target}: {message}"
    ))
}

/// Builder for a [`BigDecimalArray`].
pub struct BigDecimalBuilder {
    storage: BinaryViewBuilder,
    extension: BigDecimal,
}

impl BigDecimalBuilder {
    /// Creates an empty builder.
    pub fn new(extension: BigDecimal) -> Self {
        Self::with_capacity(extension, 1024)
    }

    /// Creates a builder with capacity for `capacity` rows.
    pub fn with_capacity(extension: BigDecimal, capacity: usize) -> Self {
        Self {
            storage: BinaryViewBuilder::with_capacity(capacity),
            extension,
        }
    }

    /// Appends a decoded value after encoding it.
    pub fn append_value(&mut self, value: &BigDecimalValue) -> Result<(), ArrowError> {
        self.storage.try_append_value(value.encode())
    }

    /// Appends already encoded layout version 1 bytes after validation.
    pub fn append_encoded(&mut self, encoded: &[u8]) -> Result<(), ArrowError> {
        BigDecimalValue::decode(encoded)?;
        self.storage.try_append_value(encoded)
    }

    /// Appends a null row.
    pub fn append_null(&mut self) {
        self.storage.append_null();
    }

    /// Returns the number of appended rows.
    pub fn len(&self) -> usize {
        self.storage.len()
    }

    /// Returns true if no rows have been appended.
    pub fn is_empty(&self) -> bool {
        self.storage.is_empty()
    }

    /// Builds the array and resets this builder.
    ///
    /// Values are validated before they are appended, so this does not rescan
    /// or decode the completed storage.
    pub fn finish(&mut self) -> Result<BigDecimalArray, ArrowError> {
        Ok(BigDecimalArray {
            storage: self.storage.finish(),
            extension: self.extension.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::extension::BigDecimalMetadata;

    use super::*;

    fn finite(magnitude: u64, scale: i16, negative: bool) -> BigDecimalValue {
        let mut magnitude = BigUint::from(magnitude).to_bytes_le();
        if magnitude.is_empty() {
            magnitude.push(0);
        }
        BigDecimalValue::finite(magnitude, scale, negative).unwrap()
    }

    #[test]
    fn golden_encodings() {
        let cases = [
            (
                BigDecimalValue::non_finite(BigDecimalClass::NegativeSignalingNaN).unwrap(),
                vec![0x00, 0x00, 0x00],
            ),
            (
                BigDecimalValue::non_finite(BigDecimalClass::NegativeQuietNaN).unwrap(),
                vec![0x01, 0x00, 0x00],
            ),
            (
                BigDecimalValue::non_finite(BigDecimalClass::NegativeInfinity).unwrap(),
                vec![0x02, 0x00, 0x00],
            ),
            (finite(123, 0, true), vec![0x03, 0x00, 0x00, 0x7B]),
            (finite(0, 0, false), vec![0x04, 0x00, 0x00, 0x00]),
            (finite(123, 0, false), vec![0x04, 0x00, 0x00, 0x7B]),
            (
                finite(1_000_000, 0, false),
                vec![0x04, 0x00, 0x00, 0x40, 0x42, 0x0F],
            ),
            (finite(1_234, 2, false), vec![0x04, 0x02, 0x00, 0xD2, 0x04]),
            (finite(12, 4, false), vec![0x04, 0x04, 0x00, 0x0C]),
            (finite(1_200, 0, false), vec![0x04, 0x00, 0x00, 0xB0, 0x04]),
            (finite(12, -2, false), vec![0x04, 0xFE, 0xFF, 0x0C]),
            (
                BigDecimalValue::non_finite(BigDecimalClass::PositiveInfinity).unwrap(),
                vec![0x05, 0x00, 0x00],
            ),
            (
                BigDecimalValue::non_finite(BigDecimalClass::PositiveQuietNaN).unwrap(),
                vec![0x06, 0x00, 0x00],
            ),
            (
                BigDecimalValue::non_finite(BigDecimalClass::PositiveSignalingNaN).unwrap(),
                vec![0x07, 0x00, 0x00],
            ),
        ];

        for (value, encoded) in cases {
            assert_eq!(value.encode(), encoded);
            assert_eq!(BigDecimalValue::decode(&encoded).unwrap(), value);
        }
    }

    #[test]
    fn textual_round_trip_preserves_scale_and_sign() {
        for literal in [
            "-sNaN",
            "-NaN",
            "-Infinity",
            "-123",
            "-0",
            "0",
            "12.34",
            "0.0012",
            "12E2",
            "Infinity",
            "NaN",
            "sNaN",
        ] {
            let value: BigDecimalValue = literal.parse().unwrap();
            assert_eq!(value.to_string(), literal);
        }
    }

    #[test]
    fn rejects_non_canonical_text() {
        for literal in [
            "-qNaN",
            "+qNaN",
            "qNaN",
            "-Inf",
            "+Infinity",
            "Inf",
            "+Inf",
            "+NaN",
            "+sNaN",
            "+1",
            "01",
            ".5",
            "5.",
            "1e2",
            "1E+2",
            "1E-2",
        ] {
            assert!(literal.parse::<BigDecimalValue>().is_err(), "{literal}");
        }
    }

    #[test]
    fn rejects_invalid_encodings() {
        for encoded in [
            vec![],
            vec![0x04, 0x00, 0x00],
            vec![0x04, 0x00, 0x00, 0x01, 0x00],
            vec![0x05, 0x01, 0x00],
            vec![0x05, 0x00, 0x00, 0x00],
            vec![0x08, 0x00, 0x00],
        ] {
            assert!(BigDecimalValue::decode(&encoded).is_err(), "{encoded:?}");
        }
    }

    #[test]
    fn builder_and_informational_metadata() {
        let metadata =
            BigDecimalMetadata::try_new(Some(4), Some(2), Some(false), Some(false)).unwrap();
        let mut builder = BigDecimalBuilder::new(BigDecimal::new(metadata));

        for value in [
            finite(1_234, 2, false),
            finite(12_345, 2, false),
            finite(1, 3, false),
            BigDecimalValue::non_finite(BigDecimalClass::PositiveInfinity).unwrap(),
            BigDecimalValue::non_finite(BigDecimalClass::PositiveQuietNaN).unwrap(),
        ] {
            builder.append_value(&value).unwrap();
        }
        builder.append_null();

        let array = builder.finish().unwrap();
        assert_eq!(array.len(), 6);
        assert_eq!(array.value(0).unwrap(), Some(finite(1_234, 2, false)));
        assert!(array.value(5).unwrap().is_none());
        array.validate_full().unwrap();
    }

    #[test]
    fn metadata_does_not_override_equivalent_rows() {
        let metadata = BigDecimalMetadata::try_new(Some(3), Some(1), None, None).unwrap();
        let mut builder = BigDecimalBuilder::new(BigDecimal::new(metadata));

        for literal in ["1200", "12E2", "5", "5.00"] {
            builder
                .append_value(&literal.parse::<BigDecimalValue>().unwrap())
                .unwrap();
        }
        builder.finish().unwrap().validate_full().unwrap();
    }

    #[test]
    fn array_rejects_invalid_storage() {
        let storage = BinaryViewArray::from_iter_values([&[0x04, 0x00, 0x00][..]]);
        assert!(BigDecimalArray::try_new(storage, BigDecimal::default()).is_err());
    }

    #[test]
    fn exact_decimal128_conversion() {
        let mut builder = BigDecimalBuilder::new(BigDecimal::default());
        builder.append_value(&finite(1_234, 2, false)).unwrap();
        builder.append_value(&finite(12, -2, false)).unwrap();
        builder.append_value(&finite(7, 1, true)).unwrap();
        builder.append_null();
        let array = builder.finish().unwrap();

        let converted = array.to_decimal128(8, 2).unwrap();
        assert_eq!(converted.value(0), 1_234);
        assert_eq!(converted.value(1), 120_000);
        assert_eq!(converted.value(2), -70);
        assert!(converted.is_null(3));
    }

    #[test]
    fn exact_decimal256_conversion() {
        let magnitude =
            BigUint::parse_bytes(b"12345678901234567890123456789012345678901234567890", 10)
                .unwrap()
                .to_bytes_le();
        let mut builder = BigDecimalBuilder::new(BigDecimal::default());
        builder
            .append_value(&BigDecimalValue::finite(magnitude, 0, false).unwrap())
            .unwrap();
        let array = builder.finish().unwrap();

        let converted = array.to_decimal256(50, 0).unwrap();
        assert_eq!(
            converted.value(0).to_string(),
            "12345678901234567890123456789012345678901234567890"
        );
    }

    #[test]
    fn decimal_conversion_rejects_loss_overflow_and_non_finite() {
        let mut lossy = BigDecimalBuilder::new(BigDecimal::default());
        lossy.append_value(&finite(12, 2, false)).unwrap();
        assert!(lossy.finish().unwrap().to_decimal128(3, 1).is_err());

        let mut overflow = BigDecimalBuilder::new(BigDecimal::default());
        overflow.append_value(&finite(1_234, 0, false)).unwrap();
        assert!(overflow.finish().unwrap().to_decimal128(3, 0).is_err());

        let mut non_finite = BigDecimalBuilder::new(BigDecimal::default());
        non_finite
            .append_value(&BigDecimalValue::non_finite(BigDecimalClass::PositiveInfinity).unwrap())
            .unwrap();
        assert!(non_finite.finish().unwrap().to_decimal256(10, 0).is_err());
    }
}
