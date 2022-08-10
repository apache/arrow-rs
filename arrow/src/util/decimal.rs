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

//! Decimal related utils

use crate::datatypes::{
    DataType, DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE, DECIMAL256_MAX_PRECISION,
    DECIMAL256_MAX_SCALE, DECIMAL_DEFAULT_SCALE,
};
use crate::error::{ArrowError, Result};
use num::bigint::BigInt;
use num::Signed;
use std::cmp::{min, Ordering};

#[derive(Debug)]
pub struct BasicDecimal<const BYTE_WIDTH: usize> {
    precision: usize,
    scale: usize,
    value: [u8; BYTE_WIDTH],
}

impl<const BYTE_WIDTH: usize> BasicDecimal<BYTE_WIDTH> {
    #[allow(clippy::type_complexity)]
    const MAX_PRECISION_SCALE_CONSTRUCTOR_DEFAULT_TYPE: (
        usize,
        usize,
        fn(usize, usize) -> DataType,
        DataType,
    ) = match BYTE_WIDTH {
        16 => (
            DECIMAL128_MAX_PRECISION,
            DECIMAL128_MAX_SCALE,
            DataType::Decimal128,
            DataType::Decimal128(DECIMAL128_MAX_PRECISION, DECIMAL_DEFAULT_SCALE),
        ),
        32 => (
            DECIMAL256_MAX_PRECISION,
            DECIMAL256_MAX_SCALE,
            DataType::Decimal256,
            DataType::Decimal256(DECIMAL256_MAX_PRECISION, DECIMAL_DEFAULT_SCALE),
        ),
        _ => panic!("invalid byte width"),
    };

    pub const MAX_PRECISION: usize = Self::MAX_PRECISION_SCALE_CONSTRUCTOR_DEFAULT_TYPE.0;
    pub const MAX_SCALE: usize = Self::MAX_PRECISION_SCALE_CONSTRUCTOR_DEFAULT_TYPE.1;
    pub const TYPE_CONSTRUCTOR: fn(usize, usize) -> DataType =
        Self::MAX_PRECISION_SCALE_CONSTRUCTOR_DEFAULT_TYPE.2;
    pub const DEFAULT_TYPE: DataType =
        Self::MAX_PRECISION_SCALE_CONSTRUCTOR_DEFAULT_TYPE.3;

    /// Tries to create a decimal value from precision, scale and bytes.
    /// If the length of bytes isn't same as the bit width of this decimal,
    /// returning an error. The bytes should be stored in little-endian order.
    ///
    /// Safety:
    /// This method doesn't validate if the decimal value represented by the bytes
    /// can be fitted into the specified precision.
    pub fn try_new_from_bytes(
        precision: usize,
        scale: usize,
        bytes: &[u8; BYTE_WIDTH],
    ) -> Result<Self>
    where
        Self: Sized,
    {
        if precision > Self::MAX_PRECISION {
            return Err(ArrowError::InvalidArgumentError(format!(
                "precision {} is greater than max {}",
                precision,
                Self::MAX_PRECISION
            )));
        }
        if scale > Self::MAX_SCALE {
            return Err(ArrowError::InvalidArgumentError(format!(
                "scale {} is greater than max {}",
                scale,
                Self::MAX_SCALE
            )));
        }

        if precision < scale {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Precision {} is less than scale {}",
                precision, scale
            )));
        }

        if bytes.len() == BYTE_WIDTH {
            Ok(Self::new(precision, scale, bytes))
        } else {
            Err(ArrowError::InvalidArgumentError(format!(
                "Input to Decimal{} must be {} bytes",
                BYTE_WIDTH * 8,
                BYTE_WIDTH
            )))
        }
    }

    /// Creates a decimal value from precision, scale, and bytes.
    ///
    /// Safety:
    /// This method doesn't check if the length of bytes is compatible with this decimal.
    /// Use `try_new_from_bytes` for safe constructor.
    pub fn new(precision: usize, scale: usize, bytes: &[u8]) -> Self {
        Self {
            precision,
            scale,
            value: bytes.try_into().unwrap(),
        }
    }
    /// Returns the raw bytes of the integer representation of the decimal.
    pub fn raw_value(&self) -> &[u8] {
        &self.value
    }

    /// Returns the precision of the decimal.
    pub fn precision(&self) -> usize {
        self.precision
    }

    /// Returns the scale of the decimal.
    pub fn scale(&self) -> usize {
        self.scale
    }

    /// Returns the string representation of the decimal.
    /// If the string representation cannot be fitted with the precision of the decimal,
    /// the string will be truncated.
    #[allow(clippy::inherent_to_string)]
    pub fn to_string(&self) -> String {
        let raw_bytes = self.raw_value();
        let integer = BigInt::from_signed_bytes_le(raw_bytes);
        let value_str = integer.to_string();
        let (sign, rest) =
            value_str.split_at(if integer >= BigInt::from(0) { 0 } else { 1 });
        let bound = min(self.precision(), rest.len()) + sign.len();
        let value_str = &value_str[0..bound];

        if self.scale() == 0 {
            value_str.to_string()
        } else if rest.len() > self.scale() {
            // Decimal separator is in the middle of the string
            let (whole, decimal) = value_str.split_at(value_str.len() - self.scale());
            format!("{}.{}", whole, decimal)
        } else {
            // String has to be padded
            format!("{}0.{:0>width$}", sign, rest, width = self.scale())
        }
    }
}

impl<const BYTE_WIDTH: usize> PartialOrd for BasicDecimal<BYTE_WIDTH> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        assert_eq!(
            self.scale, other.scale,
            "Cannot compare two Decimals with different scale: {}, {}",
            self.scale, other.scale
        );
        Some(singed_cmp_le_bytes(&self.value, &other.value))
    }
}

impl<const BYTE_WIDTH: usize> Ord for BasicDecimal<BYTE_WIDTH> {
    fn cmp(&self, other: &Self) -> Ordering {
        assert_eq!(
            self.scale, other.scale,
            "Cannot compare two Decimals with different scale: {}, {}",
            self.scale, other.scale
        );
        singed_cmp_le_bytes(&self.value, &other.value)
    }
}

impl<const BYTE_WIDTH: usize> PartialEq<Self> for BasicDecimal<BYTE_WIDTH> {
    fn eq(&self, other: &Self) -> bool {
        assert_eq!(
            self.scale, other.scale,
            "Cannot compare two Decimals with different scale: {}, {}",
            self.scale, other.scale
        );
        self.value.eq(&other.value)
    }
}

impl<const BYTE_WIDTH: usize> Eq for BasicDecimal<BYTE_WIDTH> {}

/// Represents a decimal value with precision and scale.
/// The decimal value could represented by a signed 128-bit integer.
pub type Decimal128 = BasicDecimal<16>;

impl Decimal128 {
    /// Creates `Decimal128` from an `i128` value.
    #[allow(dead_code)]
    pub(crate) fn new_from_i128(precision: usize, scale: usize, value: i128) -> Self {
        Decimal128 {
            precision,
            scale,
            value: value.to_le_bytes(),
        }
    }

    /// Returns `i128` representation of the decimal.
    pub fn as_i128(&self) -> i128 {
        i128::from_le_bytes(self.value)
    }
}

impl From<Decimal128> for i128 {
    fn from(decimal: Decimal128) -> Self {
        decimal.as_i128()
    }
}

/// Represents a decimal value with precision and scale.
/// The decimal value could be represented by a signed 256-bit integer.
pub type Decimal256 = BasicDecimal<32>;

impl Decimal256 {
    /// Constructs a `Decimal256` value from a `BigInt`.
    pub fn from_big_int(
        num: &BigInt,
        precision: usize,
        scale: usize,
    ) -> Result<Decimal256> {
        let mut bytes = if num.is_negative() {
            [255_u8; 32]
        } else {
            [0; 32]
        };
        let num_bytes = &num.to_signed_bytes_le();
        bytes[0..num_bytes.len()].clone_from_slice(num_bytes);
        Decimal256::try_new_from_bytes(precision, scale, &bytes)
    }

    /// Constructs a `BigInt` from this `Decimal256` value.
    pub(crate) fn to_big_int(&self) -> BigInt {
        BigInt::from_signed_bytes_le(&self.value)
    }
}

// compare two signed integer which are encoded with little endian.
// left bytes and right bytes must have the same length.
#[inline]
pub(crate) fn singed_cmp_le_bytes(left: &[u8], right: &[u8]) -> Ordering {
    assert_eq!(
        left.len(),
        right.len(),
        "Can't compare bytes array with different len: {}, {}",
        left.len(),
        right.len()
    );
    assert_ne!(left.len(), 0, "Can't compare bytes array of length 0");
    let len = left.len();
    // the sign bit is 1, the value is negative
    let left_negative = left[len - 1] >= 0x80_u8;
    let right_negative = right[len - 1] >= 0x80_u8;
    if left_negative != right_negative {
        return match left_negative {
            true => {
                // left is negative value
                // right is positive value
                Ordering::Less
            }
            false => Ordering::Greater,
        };
    }
    for i in 0..len {
        let l_byte = left[len - 1 - i];
        let r_byte = right[len - 1 - i];
        match l_byte.cmp(&r_byte) {
            Ordering::Less => {
                return Ordering::Less;
            }
            Ordering::Greater => {
                return Ordering::Greater;
            }
            Ordering::Equal => {}
        }
    }
    Ordering::Equal
}

#[cfg(test)]
mod tests {
    use super::*;
    use num::{BigInt, Num};
    use rand::random;

    #[test]
    fn decimal_128_to_string() {
        let mut value = Decimal128::new_from_i128(5, 2, 100);
        assert_eq!(value.to_string(), "1.00");

        value = Decimal128::new_from_i128(5, 3, 100);
        assert_eq!(value.to_string(), "0.100");
    }

    #[test]
    fn decimal_invalid_precision_scale() {
        let bytes = 100_i128.to_le_bytes();
        let err = Decimal128::try_new_from_bytes(5, 6, &bytes);
        assert!(err.is_err());
    }

    #[test]
    fn decimal_128_from_bytes() {
        let mut bytes = 100_i128.to_le_bytes();
        let value = Decimal128::try_new_from_bytes(5, 2, &bytes).unwrap();
        assert_eq!(value.to_string(), "1.00");

        bytes = (-1_i128).to_le_bytes();
        let value = Decimal128::try_new_from_bytes(5, 2, &bytes).unwrap();
        assert_eq!(value.to_string(), "-0.01");

        bytes = i128::MAX.to_le_bytes();
        let value = Decimal128::try_new_from_bytes(38, 2, &bytes).unwrap();
        assert_eq!(value.to_string(), "170141183460469231731687303715884105.72");

        bytes = i128::MIN.to_le_bytes();
        let value = Decimal128::try_new_from_bytes(38, 2, &bytes).unwrap();
        assert_eq!(
            value.to_string(),
            "-170141183460469231731687303715884105.72"
        );

        // Truncated
        bytes = 12345_i128.to_le_bytes();
        let value = Decimal128::try_new_from_bytes(3, 2, &bytes).unwrap();
        assert_eq!(value.to_string(), "1.23");

        bytes = (-12345_i128).to_le_bytes();
        let value = Decimal128::try_new_from_bytes(3, 2, &bytes).unwrap();
        assert_eq!(value.to_string(), "-1.23");
    }

    #[test]
    fn decimal_256_from_bytes() {
        let mut bytes = [0_u8; 32];
        bytes[0..16].clone_from_slice(&100_i128.to_le_bytes());
        let value = Decimal256::try_new_from_bytes(5, 2, &bytes).unwrap();
        assert_eq!(value.to_string(), "1.00");

        bytes[0..16].clone_from_slice(&i128::MAX.to_le_bytes());
        let value = Decimal256::try_new_from_bytes(40, 4, &bytes).unwrap();
        assert_eq!(
            value.to_string(),
            "17014118346046923173168730371588410.5727"
        );

        // i128 maximum + 1
        bytes[0..16].clone_from_slice(&0_i128.to_le_bytes());
        bytes[15] = 128;
        let value = Decimal256::try_new_from_bytes(40, 4, &bytes).unwrap();
        assert_eq!(
            value.to_string(),
            "17014118346046923173168730371588410.5728"
        );

        // smaller than i128 minimum
        bytes = [255; 32];
        bytes[31] = 128;
        let value = Decimal256::try_new_from_bytes(76, 4, &bytes).unwrap();
        assert_eq!(
            value.to_string(),
            "-574437317700748313234121683441537667865831564552201235664496608164256541.5731"
        );

        bytes = [255; 32];
        let value = Decimal256::try_new_from_bytes(5, 2, &bytes).unwrap();
        assert_eq!(value.to_string(), "-0.01");
    }

    fn i128_func(value: impl Into<i128>) -> i128 {
        value.into()
    }

    #[test]
    fn decimal_128_to_i128() {
        let value = Decimal128::new_from_i128(5, 2, 100);
        let integer = i128_func(value);
        assert_eq!(integer, 100);
    }

    #[test]
    fn bigint_to_decimal256() {
        let num = BigInt::from_str_radix("123456789", 10).unwrap();
        let value = Decimal256::from_big_int(&num, 30, 2).unwrap();
        assert_eq!(value.to_string(), "1234567.89");

        let num = BigInt::from_str_radix("-5744373177007483132341216834415376678658315645522012356644966081642565415731", 10).unwrap();
        let value = Decimal256::from_big_int(&num, 76, 4).unwrap();
        assert_eq!(value.to_string(), "-574437317700748313234121683441537667865831564552201235664496608164256541.5731");
    }

    #[test]
    fn test_lt_cmp_byte() {
        for _i in 0..100 {
            let left = random::<i128>();
            let right = random::<i128>();
            let result = singed_cmp_le_bytes(
                left.to_le_bytes().as_slice(),
                right.to_le_bytes().as_slice(),
            );
            assert_eq!(left.cmp(&right), result);
        }
        for _i in 0..100 {
            let left = random::<i32>();
            let right = random::<i32>();
            let result = singed_cmp_le_bytes(
                left.to_le_bytes().as_slice(),
                right.to_le_bytes().as_slice(),
            );
            assert_eq!(left.cmp(&right), result);
        }
    }

    #[test]
    fn compare_decimal128() {
        let v1 = -100_i128;
        let v2 = 10000_i128;
        let right = Decimal128::new_from_i128(20, 3, v2);
        for v in v1..v2 {
            let left = Decimal128::new_from_i128(20, 3, v);
            assert!(left < right);
        }

        for _i in 0..100 {
            let left = random::<i128>();
            let right = random::<i128>();
            let left_decimal = Decimal128::new_from_i128(38, 2, left);
            let right_decimal = Decimal128::new_from_i128(38, 2, right);
            assert_eq!(left < right, left_decimal < right_decimal);
            assert_eq!(left == right, left_decimal == right_decimal)
        }
    }

    #[test]
    fn compare_decimal256() {
        let v1 = -100_i128;
        let v2 = 10000_i128;
        let right = Decimal256::from_big_int(&BigInt::from(v2), 75, 2).unwrap();
        for v in v1..v2 {
            let left = Decimal256::from_big_int(&BigInt::from(v), 75, 2).unwrap();
            assert!(left < right);
        }

        for _i in 0..100 {
            let left = random::<i128>();
            let right = random::<i128>();
            let left_decimal =
                Decimal256::from_big_int(&BigInt::from(left), 75, 2).unwrap();
            let right_decimal =
                Decimal256::from_big_int(&BigInt::from(right), 75, 2).unwrap();
            assert_eq!(left < right, left_decimal < right_decimal);
            assert_eq!(left == right, left_decimal == right_decimal)
        }
    }
}
