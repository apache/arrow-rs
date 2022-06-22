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

use crate::error::{ArrowError, Result};
use num::bigint::BigInt;
use std::cmp::{min, Ordering};

pub trait BasicDecimal: PartialOrd + Ord + PartialEq + Eq {
    /// The bit-width of the internal representation.
    const BIT_WIDTH: usize;

    /// Tries to create a decimal value from precision, scale and bytes.
    /// If the length of bytes isn't same as the bit width of this decimal,
    /// returning an error. The bytes should be stored in little-endian order.
    ///
    /// Safety:
    /// This method doesn't validate if the decimal value represented by the bytes
    /// can be fitted into the specified precision.
    fn try_new_from_bytes(precision: usize, scale: usize, bytes: &[u8]) -> Result<Self>
    where
        Self: Sized,
    {
        if precision < scale {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Precision {} is less than scale {}",
                precision, scale
            )));
        }

        if bytes.len() == Self::BIT_WIDTH / 8 {
            Ok(Self::new(precision, scale, bytes))
        } else {
            Err(ArrowError::InvalidArgumentError(format!(
                "Input to Decimal{} must be {} bytes",
                Self::BIT_WIDTH,
                Self::BIT_WIDTH / 8
            )))
        }
    }

    /// Creates a decimal value from precision, scale, and bytes.
    ///
    /// Safety:
    /// This method doesn't check if the length of bytes is compatible with this decimal.
    /// Use `try_new_from_bytes` for safe constructor.
    fn new(precision: usize, scale: usize, bytes: &[u8]) -> Self;

    /// Returns the raw bytes of the integer representation of the decimal.
    fn raw_value(&self) -> &[u8];

    /// Returns the precision of the decimal.
    fn precision(&self) -> usize;

    /// Returns the scale of the decimal.
    fn scale(&self) -> usize;

    /// Returns the string representation of the decimal.
    /// If the string representation cannot be fitted with the precision of the decimal,
    /// the string will be truncated.
    fn to_string(&self) -> String {
        let raw_bytes = self.raw_value();
        let integer = BigInt::from_signed_bytes_le(raw_bytes);
        let value_str = integer.to_string();
        let (sign, rest) =
            value_str.split_at(if integer >= BigInt::from(0) { 0 } else { 1 });

        if self.scale() == 0 {
            let bound = min(self.precision(), rest.len()) + sign.len();
            let value_str = &value_str[0..bound];
            value_str.to_string()
        } else if rest.len() > self.scale() {
            // Decimal separator is in the middle of the string
            let bound = min(self.precision(), rest.len()) + sign.len();
            let value_str = &value_str[0..bound];
            let (whole, decimal) = value_str.split_at(value_str.len() - self.scale());
            format!("{}.{}", whole, decimal)
        } else {
            // String has to be padded
            format!("{}0.{:0>width$}", sign, rest, width = self.scale())
        }
    }
}

/// Represents a decimal value with precision and scale.
/// The decimal value could represented by a signed 128-bit integer.
#[derive(Debug)]
pub struct Decimal128 {
    #[allow(dead_code)]
    precision: usize,
    scale: usize,
    value: [u8; 16],
}

impl Decimal128 {
    /// Creates `Decimal128` from an `i128` value.
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
#[derive(Debug)]
pub struct Decimal256 {
    #[allow(dead_code)]
    precision: usize,
    scale: usize,
    value: [u8; 32],
}

macro_rules! def_decimal {
    ($ty:ident, $bit:expr) => {
        impl BasicDecimal for $ty {
            const BIT_WIDTH: usize = $bit;

            fn new(precision: usize, scale: usize, bytes: &[u8]) -> Self {
                $ty {
                    precision,
                    scale,
                    value: bytes.try_into().unwrap(),
                }
            }

            fn raw_value(&self) -> &[u8] {
                &self.value
            }

            fn precision(&self) -> usize {
                self.precision
            }

            fn scale(&self) -> usize {
                self.scale
            }
        }

        impl PartialOrd for $ty {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                assert_eq!(
                    self.scale, other.scale,
                    "Cannot compare two Decimals with different scale: {}, {}",
                    self.scale, other.scale
                );
                self.value.partial_cmp(&other.value)
            }
        }

        impl Ord for $ty {
            fn cmp(&self, other: &Self) -> Ordering {
                assert_eq!(
                    self.scale, other.scale,
                    "Cannot compare two Decimals with different scale: {}, {}",
                    self.scale, other.scale
                );
                self.value.cmp(&other.value)
            }
        }

        impl PartialEq<Self> for $ty {
            fn eq(&self, other: &Self) -> bool {
                assert_eq!(
                    self.scale, other.scale,
                    "Cannot compare two Decimals with different scale: {}, {}",
                    self.scale, other.scale
                );
                self.value.eq(&other.value)
            }
        }

        impl Eq for $ty {}
    };
}

def_decimal!(Decimal128, 128);
def_decimal!(Decimal256, 256);

#[cfg(test)]
mod tests {
    use crate::util::decimal::{BasicDecimal, Decimal128, Decimal256};

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
        let mut bytes = vec![0; 32];
        bytes[0..16].clone_from_slice(&100_i128.to_le_bytes());
        let value = Decimal256::try_new_from_bytes(5, 2, bytes.as_slice()).unwrap();
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
        bytes = vec![255; 32];
        bytes[31] = 128;
        let value = Decimal256::try_new_from_bytes(79, 4, &bytes).unwrap();
        assert_eq!(
            value.to_string(),
            "-5744373177007483132341216834415376678658315645522012356644966081642565415.7313"
        );

        bytes = vec![255; 32];
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
}
