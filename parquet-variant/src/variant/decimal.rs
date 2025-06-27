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

// Macro to format decimal values, using only integer arithmetic to avoid floating point precision loss
macro_rules! format_decimal {
    ($f:expr, $integer:expr, $scale:expr, $int_type:ty) => {{
        let integer = if $scale == 0 {
            $integer
        } else {
            let divisor = (10 as $int_type).pow($scale as u32);
            let remainder = $integer % divisor;
            if remainder != 0 {
                // Track the sign explicitly, in case the quotient is zero
                let sign = if $integer < 0 { "-" } else { "" };
                // Format an unsigned remainder with leading zeros and strip (unnecessary) trailing zeros.
                let remainder = format!("{:0width$}", remainder.abs(), width = $scale as usize);
                let remainder = remainder.trim_end_matches('0');
                let quotient = $integer / divisor;
                return write!($f, "{}{}.{}", sign, quotient.abs(), remainder);
            }
            $integer / divisor
        };
        write!($f, "{}", integer)
    }};
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
    const MAX_PRECISION: u32 = 9;
    const MAX_UNSCALED_VALUE: u32 = 10_u32.pow(Self::MAX_PRECISION) - 1;

    pub fn try_new(integer: i32, scale: u8) -> Result<Self, ArrowError> {
        // Validate that scale doesn't exceed precision
        if scale as u32 > Self::MAX_PRECISION {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Scale {} of a 4-byte decimal cannot exceed the max precision {}",
                scale,
                Self::MAX_PRECISION,
            )));
        }

        // Validate that the integer value fits within the precision
        if integer.unsigned_abs() > Self::MAX_UNSCALED_VALUE {
            return Err(ArrowError::InvalidArgumentError(format!(
                "{} is too large to store in a 4-byte decimal with max precision {}",
                integer,
                Self::MAX_PRECISION
            )));
        }

        Ok(VariantDecimal4 { integer, scale })
    }

    /// Returns the underlying value of the decimal.
    ///
    /// For example, if the decimal is `123.4567`, this will return `1234567`.
    pub fn integer(&self) -> i32 {
        self.integer
    }

    /// Returns the scale of the decimal (how many digits after the decimal point).
    ///
    /// For example, if the decimal is `123.4567`, this will return `4`.
    pub fn scale(&self) -> u8 {
        self.scale
    }
}

impl fmt::Display for VariantDecimal4 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        format_decimal!(f, self.integer, self.scale, i32)
    }
}

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
    pub(crate) integer: i64,
    pub(crate) scale: u8,
}

impl VariantDecimal8 {
    const MAX_PRECISION: u32 = 18;
    const MAX_UNSCALED_VALUE: u64 = 10_u64.pow(Self::MAX_PRECISION) - 1;

    pub fn try_new(integer: i64, scale: u8) -> Result<Self, ArrowError> {
        // Validate that scale doesn't exceed precision
        if scale as u32 > Self::MAX_PRECISION {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Scale {} of an 8-byte decimal cannot exceed the max precision {}",
                scale,
                Self::MAX_PRECISION,
            )));
        }

        // Validate that the integer value fits within the precision
        if integer.unsigned_abs() > Self::MAX_UNSCALED_VALUE {
            return Err(ArrowError::InvalidArgumentError(format!(
                "{} is too large to store in an 8-byte decimal with max precision {}",
                integer,
                Self::MAX_PRECISION
            )));
        }

        Ok(VariantDecimal8 { integer, scale })
    }

    /// Returns the underlying value of the decimal.
    ///
    /// For example, if the decimal is `123456.78`, this will return `12345678`.
    pub fn integer(&self) -> i64 {
        self.integer
    }

    /// Returns the scale of the decimal (how many digits after the decimal point).
    ///
    /// For example, if the decimal is `123456.78`, this will return `2`.
    pub fn scale(&self) -> u8 {
        self.scale
    }
}

impl fmt::Display for VariantDecimal8 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        format_decimal!(f, self.integer, self.scale, i64)
    }
}

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
    pub(crate) integer: i128,
    pub(crate) scale: u8,
}

impl VariantDecimal16 {
    const MAX_PRECISION: u32 = 38;
    const MAX_UNSCALED_VALUE: u128 = 10_u128.pow(Self::MAX_PRECISION) - 1;

    pub fn try_new(integer: i128, scale: u8) -> Result<Self, ArrowError> {
        // Validate that scale doesn't exceed precision
        if scale as u32 > Self::MAX_PRECISION {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Scale {} of a 16-byte decimal cannot exceed the max precision {}",
                scale,
                Self::MAX_PRECISION,
            )));
        }

        // Validate that the integer value fits within the precision
        if integer.unsigned_abs() > Self::MAX_UNSCALED_VALUE {
            return Err(ArrowError::InvalidArgumentError(format!(
                "{} is too large to store in a 16-byte decimal with max precision {}",
                integer,
                Self::MAX_PRECISION
            )));
        }

        Ok(VariantDecimal16 { integer, scale })
    }

    /// Returns the underlying value of the decimal.
    ///
    /// For example, if the decimal is `12345678901234567.890`, this will return `12345678901234567890`.
    pub fn integer(&self) -> i128 {
        self.integer
    }

    /// Returns the scale of the decimal (how many digits after the decimal point).
    ///
    /// For example, if the decimal is `12345678901234567.890`, this will return `3`.
    pub fn scale(&self) -> u8 {
        self.scale
    }
}

impl fmt::Display for VariantDecimal16 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        format_decimal!(f, self.integer, self.scale, i128)
    }
}

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
        assert!(decimal4_too_large
            .unwrap_err()
            .to_string()
            .contains("too large"));

        let decimal4_too_small = VariantDecimal4::try_new(-1_000_000_000_i32, 2);
        assert!(
            decimal4_too_small.is_err(),
            "Decimal4 precision underflow should fail"
        );
        assert!(decimal4_too_small
            .unwrap_err()
            .to_string()
            .contains("too large"));

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
        assert!(decimal8_too_large
            .unwrap_err()
            .to_string()
            .contains("too large"));

        let decimal8_too_small = VariantDecimal8::try_new(-1_000_000_000_000_000_000_i64, 2);
        assert!(
            decimal8_too_small.is_err(),
            "Decimal8 precision underflow should fail"
        );
        assert!(decimal8_too_small
            .unwrap_err()
            .to_string()
            .contains("too large"));

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
        assert!(decimal16_too_large
            .unwrap_err()
            .to_string()
            .contains("too large"));

        let decimal16_too_small =
            VariantDecimal16::try_new(-100000000000000000000000000000000000000_i128, 2);
        assert!(
            decimal16_too_small.is_err(),
            "Decimal16 precision underflow should fail"
        );
        assert!(decimal16_too_small
            .unwrap_err()
            .to_string()
            .contains("too large"));

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
        assert!(decimal4_invalid_scale
            .unwrap_err()
            .to_string()
            .contains("cannot exceed the max precision"));

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
        assert!(decimal8_invalid_scale
            .unwrap_err()
            .to_string()
            .contains("cannot exceed the max precision"));

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
        assert!(decimal16_invalid_scale
            .unwrap_err()
            .to_string()
            .contains("cannot exceed the max precision"));

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
