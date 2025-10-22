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
use std::fmt;

/// Trait for variant decimal types, enabling generic code across Decimal4/8/16
///
/// This trait provides a common interface for the three variant decimal types,
/// allowing generic functions and data structures to work with any decimal width.
/// It is modeled after Arrow's `DecimalType` trait but adapted for variant semantics.
///
/// # Example
///
/// ```
/// # use parquet_variant::{VariantDecimal4, VariantDecimal8, VariantDecimalType};
/// #
/// fn extract_scale<D: VariantDecimalType>(decimal: D) -> u8 {
///     decimal.scale()
/// }
///
/// let dec4 = VariantDecimal4::try_new(12345, 2).unwrap();
/// let dec8 = VariantDecimal8::try_new(67890, 3).unwrap();
///
/// assert_eq!(extract_scale(dec4), 2);
/// assert_eq!(extract_scale(dec8), 3);
/// ```
pub trait VariantDecimalType: Into<super::Variant<'static, 'static>> {
    /// The underlying signed integer type (i32, i64, or i128)
    type Native;

    /// Maximum number of significant digits this decimal type can represent (9, 18, or 38)
    const MAX_PRECISION: u8;
    /// The largest positive unscaled value that fits in [`Self::MAX_PRECISION`] digits.
    const MAX_UNSCALED_VALUE: Self::Native;

    /// True if the given precision and scale are valid for this variant decimal type.
    ///
    /// NOTE: By a strict reading of the "decimal table" in the [variant spec], one might conclude that
    /// each decimal type has both lower and upper bounds on precision (i.e. Decimal16 with precision 5
    /// is invalid because Decimal4 "covers" it). But the variant shredding integration tests
    /// specifically expect such cases to succeed, so we only enforce the upper bound here.
    ///
    /// [shredding spec]: https://github.com/apache/parquet-format/blob/master/VariantEncoding.md#encoding-types
    ///
    /// # Example
    /// ```
    /// # use parquet_variant::{VariantDecimal4, VariantDecimalType};
    /// #
    /// assert!(VariantDecimal4::is_valid_precision_and_scale(&5, &2));
    /// assert!(!VariantDecimal4::is_valid_precision_and_scale(&10, &2)); // too wide
    /// assert!(!VariantDecimal4::is_valid_precision_and_scale(&5, &-1)); // negative scale
    /// assert!(!VariantDecimal4::is_valid_precision_and_scale(&5, &7)); // scale too big
    /// ```
    fn is_valid_precision_and_scale(precision: &u8, scale: &i8) -> bool {
        (1..=Self::MAX_PRECISION).contains(precision) && (0..=*precision as i8).contains(scale)
    }

    /// Creates a new decimal value from the given unscaled integer and scale, failing if the
    /// integer's width, or the requested scale, exceeds `MAX_PRECISION`.
    ///
    /// NOTE: For compatibility with arrow decimal types, negative scale is allowed as long
    /// as the rescaled value fits in the available precision.
    ///
    /// # Example
    ///
    /// ```
    /// # use parquet_variant::{VariantDecimal4, VariantDecimalType};
    /// #
    /// // Valid: 123.45 (5 digits, scale 2)
    /// let d = VariantDecimal4::try_new(12345, 2).unwrap();
    /// assert_eq!(d.integer(), 12345);
    /// assert_eq!(d.scale(), 2);
    ///
    /// VariantDecimal4::try_new(123, 10).expect_err("scale exceeds MAX_PRECISION");
    /// VariantDecimal4::try_new(1234567890, 10).expect_err("value's width exceeds MAX_PRECISION");
    /// ```
    fn try_new(integer: Self::Native, scale: u8) -> Result<Self, ArrowError>;

    /// Attempts to convert an unscaled arrow decimal value to the indicated variant decimal type.
    ///
    /// Unlike [`Self::try_new`], this function accepts a signed scale, and attempts to rescale
    /// negative-scale values to their equivalent (larger) scale-0 values. For example, a decimal
    /// value of 123 with scale -2 becomes 12300 with scale 0.
    ///
    /// Fails if rescaling fails, or for any of the reasons [`Self::try_new`] could fail.
    fn try_new_with_signed_scale(integer: Self::Native, scale: i8) -> Result<Self, ArrowError>;

    /// Returns the unscaled integer value
    fn integer(&self) -> Self::Native;

    /// Returns the scale (number of digits after the decimal point)
    fn scale(&self) -> u8;
}

/// Implements the complete variant decimal type: methods, Display, and VariantDecimalType trait
macro_rules! impl_variant_decimal {
    ($struct_name:ident, $native:ty) => {
        impl $struct_name {
            /// Attempts to create a new instance of this decimal type, failing if the value is too
            /// wide or the scale is too large.
            pub fn try_new(integer: $native, scale: u8) -> Result<Self, ArrowError> {
                let max_precision = Self::MAX_PRECISION;
                if scale > max_precision {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Scale {scale} is larger than max precision {max_precision}",
                    )));
                }
                if !(-Self::MAX_UNSCALED_VALUE..=Self::MAX_UNSCALED_VALUE).contains(&integer) {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "{integer} is wider than max precision {max_precision}",
                    )));
                }

                Ok(Self { integer, scale })
            }

            /// Returns the unscaled integer value of the decimal.
            ///
            /// For example, if the decimal is `123.45`, this will return `12345`.
            pub fn integer(&self) -> $native {
                self.integer
            }

            /// Returns the scale of the decimal (how many digits after the decimal point).
            ///
            /// For example, if the decimal is `123.45`, this will return `2`.
            pub fn scale(&self) -> u8 {
                self.scale
            }
        }

        impl VariantDecimalType for $struct_name {
            type Native = $native;
            const MAX_PRECISION: u8 = Self::MAX_PRECISION;
            const MAX_UNSCALED_VALUE: $native = <$native>::pow(10, Self::MAX_PRECISION as u32) - 1;

            fn try_new(integer: $native, scale: u8) -> Result<Self, ArrowError> {
                Self::try_new(integer, scale)
            }

            fn try_new_with_signed_scale(integer: $native, scale: i8) -> Result<Self, ArrowError> {
                let (integer, scale) = if scale < 0 {
                    let multiplier = <$native>::checked_pow(10, -scale as u32);
                    let Some(rescaled) = multiplier.and_then(|m| integer.checked_mul(m)) else {
                        return Err(ArrowError::InvalidArgumentError(format!(
                            "Overflow when rescaling {integer} with scale {scale}"
                        )));
                    };
                    (rescaled, 0u8)
                } else {
                    (integer, scale as u8)
                };
                Self::try_new(integer, scale)
            }

            fn integer(&self) -> $native {
                self.integer()
            }

            fn scale(&self) -> u8 {
                self.scale()
            }
        }

        impl fmt::Display for $struct_name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let integer = if self.scale == 0 {
                    self.integer
                } else {
                    let divisor = <$native>::pow(10, self.scale as u32);
                    let remainder = self.integer % divisor;
                    if remainder != 0 {
                        // Track the sign explicitly, in case the quotient is zero
                        let sign = if self.integer < 0 { "-" } else { "" };
                        // Format an unsigned remainder with leading zeros and strip trailing zeros
                        let remainder =
                            format!("{:0width$}", remainder.abs(), width = self.scale as usize);
                        let remainder = remainder.trim_end_matches('0');
                        let quotient = (self.integer / divisor).abs();
                        return write!(f, "{sign}{quotient}.{remainder}");
                    }
                    self.integer / divisor
                };
                write!(f, "{integer}")
            }
        }
    };
}

/// Represents a 4-byte decimal value in the Variant format.
///
/// This struct stores a decimal number using a 32-bit signed integer for the coefficient
/// and an 8-bit unsigned integer for the scale (number of decimal places). Its precision is limited to 9 digits.
///
/// For valid precision and scale values, see the Variant specification:
/// <https://github.com/apache/parquet-format/blob/87f2c8bf77eefb4c43d0ebaeea1778bd28ac3609/VariantEncoding.md?plain=1#L418-L420>
///
/// # Example: Create a VariantDecimal4
/// ```
/// # use parquet_variant::VariantDecimal4;
/// // Create a value representing the decimal 123.4567
/// let decimal = VariantDecimal4::try_new(1234567, 4).expect("Failed to create decimal");
/// ```
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VariantDecimal4 {
    integer: i32,
    scale: u8,
}

impl VariantDecimal4 {
    /// Maximum number of significant digits (9 for 4-byte decimals)
    pub const MAX_PRECISION: u8 = arrow_schema::DECIMAL32_MAX_PRECISION;
}

impl_variant_decimal!(VariantDecimal4, i32);

/// Represents an 8-byte decimal value in the Variant format.
///
/// This struct stores a decimal number using a 64-bit signed integer for the coefficient
/// and an 8-bit unsigned integer for the scale (number of decimal places). Its precision is between 10 and 18 digits.
///
/// For valid precision and scale values, see the Variant specification:
///
/// <https://github.com/apache/parquet-format/blob/87f2c8bf77eefb4c43d0ebaeea1778bd28ac3609/VariantEncoding.md?plain=1#L418-L420>
///
/// # Example: Create a VariantDecimal8
/// ```
/// # use parquet_variant::VariantDecimal8;
/// // Create a value representing the decimal 123456.78
/// let decimal = VariantDecimal8::try_new(12345678, 2).expect("Failed to create decimal");
/// ```
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VariantDecimal8 {
    integer: i64,
    scale: u8,
}

impl VariantDecimal8 {
    /// Maximum number of significant digits (18 for 8-byte decimals)
    pub const MAX_PRECISION: u8 = arrow_schema::DECIMAL64_MAX_PRECISION;
}

impl_variant_decimal!(VariantDecimal8, i64);

/// Represents an 16-byte decimal value in the Variant format.
///
/// This struct stores a decimal number using a 128-bit signed integer for the coefficient
/// and an 8-bit unsigned integer for the scale (number of decimal places). Its precision is between 19 and 38 digits.
///
/// For valid precision and scale values, see the Variant specification:
///
/// <https://github.com/apache/parquet-format/blob/87f2c8bf77eefb4c43d0ebaeea1778bd28ac3609/VariantEncoding.md?plain=1#L418-L420>
///
/// # Example: Create a VariantDecimal16
/// ```
/// # use parquet_variant::VariantDecimal16;
/// // Create a value representing the decimal 12345678901234567.890
/// let decimal = VariantDecimal16::try_new(12345678901234567890, 3).unwrap();
/// ```
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VariantDecimal16 {
    integer: i128,
    scale: u8,
}

impl VariantDecimal16 {
    /// Maximum number of significant digits (38 for 16-byte decimals)
    pub const MAX_PRECISION: u8 = arrow_schema::DECIMAL128_MAX_PRECISION;
}

impl_variant_decimal!(VariantDecimal16, i128);

// Infallible conversion from a narrower decimal type to a wider one
macro_rules! impl_from_decimal_for_decimal {
    ($from_ty:ty, $for_ty:ty) => {
        impl From<$from_ty> for $for_ty {
            fn from(decimal: $from_ty) -> Self {
                Self {
                    integer: decimal.integer.into(),
                    scale: decimal.scale,
                }
            }
        }
    };
}

impl_from_decimal_for_decimal!(VariantDecimal4, VariantDecimal8);
impl_from_decimal_for_decimal!(VariantDecimal4, VariantDecimal16);
impl_from_decimal_for_decimal!(VariantDecimal8, VariantDecimal16);

// Fallible conversion from a wider decimal type to a narrower one
macro_rules! impl_try_from_decimal_for_decimal {
    ($from_ty:ty, $for_ty:ty) => {
        impl TryFrom<$from_ty> for $for_ty {
            type Error = ArrowError;

            fn try_from(decimal: $from_ty) -> Result<Self, ArrowError> {
                let Ok(integer) = decimal.integer.try_into() else {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Value {} is wider than max precision {}",
                        decimal.integer,
                        Self::MAX_PRECISION
                    )));
                };
                Self::try_new(integer, decimal.scale)
            }
        }
    };
}

impl_try_from_decimal_for_decimal!(VariantDecimal8, VariantDecimal4);
impl_try_from_decimal_for_decimal!(VariantDecimal16, VariantDecimal4);
impl_try_from_decimal_for_decimal!(VariantDecimal16, VariantDecimal8);

// Fallible conversion from a decimal's underlying integer type
macro_rules! impl_try_from_int_for_decimal {
    ($from_ty:ty, $for_ty:ty) => {
        impl TryFrom<$from_ty> for $for_ty {
            type Error = ArrowError;

            fn try_from(integer: $from_ty) -> Result<Self, ArrowError> {
                Self::try_new(integer, 0)
            }
        }
    };
}

impl_try_from_int_for_decimal!(i32, VariantDecimal4);
impl_try_from_int_for_decimal!(i64, VariantDecimal8);
impl_try_from_int_for_decimal!(i128, VariantDecimal16);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_variant_decimal_invalid_precision() {
        // Test precision validation for Decimal4
        let decimal4_too_large = VariantDecimal4::try_new(1_000_000_000_i32, 2);
        assert!(
            decimal4_too_large.is_err(),
            "Decimal4 precision overflow should fail"
        );
        assert!(
            decimal4_too_large
                .unwrap_err()
                .to_string()
                .contains("wider than max precision")
        );

        let decimal4_too_small = VariantDecimal4::try_new(-1_000_000_000_i32, 2);
        assert!(
            decimal4_too_small.is_err(),
            "Decimal4 precision underflow should fail"
        );
        assert!(
            decimal4_too_small
                .unwrap_err()
                .to_string()
                .contains("wider than max precision")
        );

        // Test valid edge cases for Decimal4
        let decimal4_max_valid = VariantDecimal4::try_new(999_999_999_i32, 2);
        assert!(
            decimal4_max_valid.is_ok(),
            "Decimal4 max valid value should succeed"
        );

        let decimal4_min_valid = VariantDecimal4::try_new(-999_999_999_i32, 2);
        assert!(
            decimal4_min_valid.is_ok(),
            "Decimal4 min valid value should succeed"
        );

        // Test precision validation for Decimal8
        let decimal8_too_large = VariantDecimal8::try_new(1_000_000_000_000_000_000_i64, 2);
        assert!(
            decimal8_too_large.is_err(),
            "Decimal8 precision overflow should fail"
        );
        assert!(
            decimal8_too_large
                .unwrap_err()
                .to_string()
                .contains("wider than max precision")
        );

        let decimal8_too_small = VariantDecimal8::try_new(-1_000_000_000_000_000_000_i64, 2);
        assert!(
            decimal8_too_small.is_err(),
            "Decimal8 precision underflow should fail"
        );
        assert!(
            decimal8_too_small
                .unwrap_err()
                .to_string()
                .contains("wider than max precision")
        );

        // Test valid edge cases for Decimal8
        let decimal8_max_valid = VariantDecimal8::try_new(999_999_999_999_999_999_i64, 2);
        assert!(
            decimal8_max_valid.is_ok(),
            "Decimal8 max valid value should succeed"
        );

        let decimal8_min_valid = VariantDecimal8::try_new(-999_999_999_999_999_999_i64, 2);
        assert!(
            decimal8_min_valid.is_ok(),
            "Decimal8 min valid value should succeed"
        );

        // Test precision validation for Decimal16
        let decimal16_too_large =
            VariantDecimal16::try_new(100000000000000000000000000000000000000_i128, 2);
        assert!(
            decimal16_too_large.is_err(),
            "Decimal16 precision overflow should fail"
        );
        assert!(
            decimal16_too_large
                .unwrap_err()
                .to_string()
                .contains("wider than max precision")
        );

        let decimal16_too_small =
            VariantDecimal16::try_new(-100000000000000000000000000000000000000_i128, 2);
        assert!(
            decimal16_too_small.is_err(),
            "Decimal16 precision underflow should fail"
        );
        assert!(
            decimal16_too_small
                .unwrap_err()
                .to_string()
                .contains("wider than max precision")
        );

        // Test valid edge cases for Decimal16
        let decimal16_max_valid =
            VariantDecimal16::try_new(99999999999999999999999999999999999999_i128, 2);
        assert!(
            decimal16_max_valid.is_ok(),
            "Decimal16 max valid value should succeed"
        );

        let decimal16_min_valid =
            VariantDecimal16::try_new(-99999999999999999999999999999999999999_i128, 2);
        assert!(
            decimal16_min_valid.is_ok(),
            "Decimal16 min valid value should succeed"
        );
    }

    #[test]
    fn test_variant_decimal_invalid_scale() {
        // Test invalid scale for Decimal4 (scale > 9)
        let decimal4_invalid_scale = VariantDecimal4::try_new(123_i32, 10);
        assert!(
            decimal4_invalid_scale.is_err(),
            "Decimal4 with scale > 9 should fail"
        );
        assert!(
            decimal4_invalid_scale
                .unwrap_err()
                .to_string()
                .contains("larger than max precision")
        );

        let decimal4_invalid_scale_large = VariantDecimal4::try_new(123_i32, 20);
        assert!(
            decimal4_invalid_scale_large.is_err(),
            "Decimal4 with scale > 9 should fail"
        );

        // Test valid scale edge case for Decimal4
        let decimal4_valid_scale = VariantDecimal4::try_new(123_i32, 9);
        assert!(
            decimal4_valid_scale.is_ok(),
            "Decimal4 with scale = 9 should succeed"
        );

        // Test invalid scale for Decimal8 (scale > 18)
        let decimal8_invalid_scale = VariantDecimal8::try_new(123_i64, 19);
        assert!(
            decimal8_invalid_scale.is_err(),
            "Decimal8 with scale > 18 should fail"
        );
        assert!(
            decimal8_invalid_scale
                .unwrap_err()
                .to_string()
                .contains("larger than max precision")
        );

        let decimal8_invalid_scale_large = VariantDecimal8::try_new(123_i64, 25);
        assert!(
            decimal8_invalid_scale_large.is_err(),
            "Decimal8 with scale > 18 should fail"
        );

        // Test valid scale edge case for Decimal8
        let decimal8_valid_scale = VariantDecimal8::try_new(123_i64, 18);
        assert!(
            decimal8_valid_scale.is_ok(),
            "Decimal8 with scale = 18 should succeed"
        );

        // Test invalid scale for Decimal16 (scale > 38)
        let decimal16_invalid_scale = VariantDecimal16::try_new(123_i128, 39);
        assert!(
            decimal16_invalid_scale.is_err(),
            "Decimal16 with scale > 38 should fail"
        );
        assert!(
            decimal16_invalid_scale
                .unwrap_err()
                .to_string()
                .contains("larger than max precision")
        );

        let decimal16_invalid_scale_large = VariantDecimal16::try_new(123_i128, 50);
        assert!(
            decimal16_invalid_scale_large.is_err(),
            "Decimal16 with scale > 38 should fail"
        );

        // Test valid scale edge case for Decimal16
        let decimal16_valid_scale = VariantDecimal16::try_new(123_i128, 38);
        assert!(
            decimal16_valid_scale.is_ok(),
            "Decimal16 with scale = 38 should succeed"
        );
    }

    #[test]
    fn test_variant_decimal4_display() {
        // Test zero scale (integers)
        let d = VariantDecimal4::try_new(42, 0).unwrap();
        assert_eq!(d.to_string(), "42");

        let d = VariantDecimal4::try_new(-42, 0).unwrap();
        assert_eq!(d.to_string(), "-42");

        // Test basic decimal formatting
        let d = VariantDecimal4::try_new(12345, 2).unwrap();
        assert_eq!(d.to_string(), "123.45");

        let d = VariantDecimal4::try_new(-12345, 2).unwrap();
        assert_eq!(d.to_string(), "-123.45");

        // Test trailing zeros are trimmed
        let d = VariantDecimal4::try_new(12300, 2).unwrap();
        assert_eq!(d.to_string(), "123");

        let d = VariantDecimal4::try_new(-12300, 2).unwrap();
        assert_eq!(d.to_string(), "-123");

        // Test leading zeros in decimal part
        let d = VariantDecimal4::try_new(1005, 3).unwrap();
        assert_eq!(d.to_string(), "1.005");

        let d = VariantDecimal4::try_new(-1005, 3).unwrap();
        assert_eq!(d.to_string(), "-1.005");

        // Test number smaller than scale (leading zero before decimal)
        let d = VariantDecimal4::try_new(123, 4).unwrap();
        assert_eq!(d.to_string(), "0.0123");

        let d = VariantDecimal4::try_new(-123, 4).unwrap();
        assert_eq!(d.to_string(), "-0.0123");

        // Test zero
        let d = VariantDecimal4::try_new(0, 0).unwrap();
        assert_eq!(d.to_string(), "0");

        let d = VariantDecimal4::try_new(0, 3).unwrap();
        assert_eq!(d.to_string(), "0");

        // Test max scale
        let d = VariantDecimal4::try_new(123456789, 9).unwrap();
        assert_eq!(d.to_string(), "0.123456789");

        let d = VariantDecimal4::try_new(-123456789, 9).unwrap();
        assert_eq!(d.to_string(), "-0.123456789");

        // Test max precision
        let d = VariantDecimal4::try_new(999999999, 0).unwrap();
        assert_eq!(d.to_string(), "999999999");

        let d = VariantDecimal4::try_new(-999999999, 0).unwrap();
        assert_eq!(d.to_string(), "-999999999");

        // Test trailing zeros with mixed decimal places
        let d = VariantDecimal4::try_new(120050, 4).unwrap();
        assert_eq!(d.to_string(), "12.005");

        let d = VariantDecimal4::try_new(-120050, 4).unwrap();
        assert_eq!(d.to_string(), "-12.005");
    }

    #[test]
    fn test_variant_decimal8_display() {
        // Test zero scale (integers)
        let d = VariantDecimal8::try_new(42, 0).unwrap();
        assert_eq!(d.to_string(), "42");

        let d = VariantDecimal8::try_new(-42, 0).unwrap();
        assert_eq!(d.to_string(), "-42");

        // Test basic decimal formatting
        let d = VariantDecimal8::try_new(1234567890, 3).unwrap();
        assert_eq!(d.to_string(), "1234567.89");

        let d = VariantDecimal8::try_new(-1234567890, 3).unwrap();
        assert_eq!(d.to_string(), "-1234567.89");

        // Test trailing zeros are trimmed
        let d = VariantDecimal8::try_new(123000000, 6).unwrap();
        assert_eq!(d.to_string(), "123");

        let d = VariantDecimal8::try_new(-123000000, 6).unwrap();
        assert_eq!(d.to_string(), "-123");

        // Test leading zeros in decimal part
        let d = VariantDecimal8::try_new(100005, 6).unwrap();
        assert_eq!(d.to_string(), "0.100005");

        let d = VariantDecimal8::try_new(-100005, 6).unwrap();
        assert_eq!(d.to_string(), "-0.100005");

        // Test number smaller than scale
        let d = VariantDecimal8::try_new(123, 10).unwrap();
        assert_eq!(d.to_string(), "0.0000000123");

        let d = VariantDecimal8::try_new(-123, 10).unwrap();
        assert_eq!(d.to_string(), "-0.0000000123");

        // Test zero
        let d = VariantDecimal8::try_new(0, 0).unwrap();
        assert_eq!(d.to_string(), "0");

        let d = VariantDecimal8::try_new(0, 10).unwrap();
        assert_eq!(d.to_string(), "0");

        // Test max scale
        let d = VariantDecimal8::try_new(123456789012345678, 18).unwrap();
        assert_eq!(d.to_string(), "0.123456789012345678");

        let d = VariantDecimal8::try_new(-123456789012345678, 18).unwrap();
        assert_eq!(d.to_string(), "-0.123456789012345678");

        // Test max precision
        let d = VariantDecimal8::try_new(999999999999999999, 0).unwrap();
        assert_eq!(d.to_string(), "999999999999999999");

        let d = VariantDecimal8::try_new(-999999999999999999, 0).unwrap();
        assert_eq!(d.to_string(), "-999999999999999999");

        // Test complex trailing zeros
        let d = VariantDecimal8::try_new(1200000050000, 10).unwrap();
        assert_eq!(d.to_string(), "120.000005");

        let d = VariantDecimal8::try_new(-1200000050000, 10).unwrap();
        assert_eq!(d.to_string(), "-120.000005");
    }

    #[test]
    fn test_variant_decimal16_display() {
        // Test zero scale (integers)
        let d = VariantDecimal16::try_new(42, 0).unwrap();
        assert_eq!(d.to_string(), "42");

        let d = VariantDecimal16::try_new(-42, 0).unwrap();
        assert_eq!(d.to_string(), "-42");

        // Test basic decimal formatting
        let d = VariantDecimal16::try_new(123456789012345, 4).unwrap();
        assert_eq!(d.to_string(), "12345678901.2345");

        let d = VariantDecimal16::try_new(-123456789012345, 4).unwrap();
        assert_eq!(d.to_string(), "-12345678901.2345");

        // Test trailing zeros are trimmed
        let d = VariantDecimal16::try_new(12300000000, 8).unwrap();
        assert_eq!(d.to_string(), "123");

        let d = VariantDecimal16::try_new(-12300000000, 8).unwrap();
        assert_eq!(d.to_string(), "-123");

        // Test leading zeros in decimal part
        let d = VariantDecimal16::try_new(10000005, 8).unwrap();
        assert_eq!(d.to_string(), "0.10000005");

        let d = VariantDecimal16::try_new(-10000005, 8).unwrap();
        assert_eq!(d.to_string(), "-0.10000005");

        // Test number smaller than scale
        let d = VariantDecimal16::try_new(123, 20).unwrap();
        assert_eq!(d.to_string(), "0.00000000000000000123");

        let d = VariantDecimal16::try_new(-123, 20).unwrap();
        assert_eq!(d.to_string(), "-0.00000000000000000123");

        // Test zero
        let d = VariantDecimal16::try_new(0, 0).unwrap();
        assert_eq!(d.to_string(), "0");

        let d = VariantDecimal16::try_new(0, 20).unwrap();
        assert_eq!(d.to_string(), "0");

        // Test max scale
        let d = VariantDecimal16::try_new(12345678901234567890123456789012345678_i128, 38).unwrap();
        assert_eq!(d.to_string(), "0.12345678901234567890123456789012345678");

        let d =
            VariantDecimal16::try_new(-12345678901234567890123456789012345678_i128, 38).unwrap();
        assert_eq!(d.to_string(), "-0.12345678901234567890123456789012345678");

        // Test max precision integer
        let d = VariantDecimal16::try_new(99999999999999999999999999999999999999_i128, 0).unwrap();
        assert_eq!(d.to_string(), "99999999999999999999999999999999999999");

        let d = VariantDecimal16::try_new(-99999999999999999999999999999999999999_i128, 0).unwrap();
        assert_eq!(d.to_string(), "-99999999999999999999999999999999999999");

        // Test complex trailing zeros
        let d = VariantDecimal16::try_new(12000000000000050000000000000_i128, 25).unwrap();
        assert_eq!(d.to_string(), "1200.000000000005");

        let d = VariantDecimal16::try_new(-12000000000000050000000000000_i128, 25).unwrap();
        assert_eq!(d.to_string(), "-1200.000000000005");

        // Test large integer that would overflow i64 but fits in i128
        let large_int = 12345678901234567890123456789_i128;
        let d = VariantDecimal16::try_new(large_int, 0).unwrap();
        assert_eq!(d.to_string(), "12345678901234567890123456789");

        let d = VariantDecimal16::try_new(-large_int, 0).unwrap();
        assert_eq!(d.to_string(), "-12345678901234567890123456789");
    }
}
