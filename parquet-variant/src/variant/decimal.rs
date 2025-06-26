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

/// Represents a 4-byte decimal value in the Variant format.
///
/// This struct stores a decimal number using a 32-bit signed integer for the coefficient
/// and an 8-bit unsigned integer for the scale (number of decimal places). Its precision is limited to 9 digits.
///
/// For valid precision and scale values, see the Variant specification:
/// <https://github.com/apache/parquet-format/blob/87f2c8bf77eefb4c43d0ebaeea1778bd28ac3609/VariantEncoding.md?plain=1#L418-L420>
///
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VariantDecimal4 {
    pub(crate) integer: i32,
    pub(crate) scale: u8,
}

impl VariantDecimal4 {
    pub(crate) const MAX_PRECISION: u32 = 9;
    pub(crate) const MAX_UNSCALED_VALUE: u32 = 10_u32.pow(Self::MAX_PRECISION) - 1;

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
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VariantDecimal8 {
    pub(crate) integer: i64,
    pub(crate) scale: u8,
}

impl VariantDecimal8 {
    pub(crate) const MAX_PRECISION: u32 = 18;
    pub(crate) const MAX_UNSCALED_VALUE: u64 = 10_u64.pow(Self::MAX_PRECISION) - 1;

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
}
