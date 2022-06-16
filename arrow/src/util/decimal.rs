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

use std::cmp::Ordering;

/// Represents a decimal value with precision and scale.
/// The decimal value is represented by a signed 128-bit integer.
#[derive(Debug)]
pub struct Decimal128 {
    #[allow(dead_code)]
    precision: usize,
    scale: usize,
    value: i128,
}

impl PartialOrd for Decimal128 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        assert_eq!(
            self.scale, other.scale,
            "Cannot compare two Decimal128 with different scale: {}, {}",
            self.scale, other.scale
        );
        self.value.partial_cmp(&other.value)
    }
}

impl Ord for Decimal128 {
    fn cmp(&self, other: &Self) -> Ordering {
        assert_eq!(
            self.scale, other.scale,
            "Cannot compare two Decimal128 with different scale: {}, {}",
            self.scale, other.scale
        );
        self.value.cmp(&other.value)
    }
}

impl PartialEq<Self> for Decimal128 {
    fn eq(&self, other: &Self) -> bool {
        assert_eq!(
            self.scale, other.scale,
            "Cannot compare two Decimal128 with different scale: {}, {}",
            self.scale, other.scale
        );
        self.value.eq(&other.value)
    }
}

impl Eq for Decimal128 {}

impl Decimal128 {
    pub fn new_from_bytes(precision: usize, scale: usize, bytes: &[u8]) -> Self {
        let as_array = bytes.try_into();
        let value = match as_array {
            Ok(v) if bytes.len() == 16 => i128::from_le_bytes(v),
            _ => panic!("Input to Decimal128 is not 128bit integer."),
        };

        Decimal128 {
            precision,
            scale,
            value,
        }
    }

    pub fn new_from_i128(precision: usize, scale: usize, value: i128) -> Self {
        Decimal128 {
            precision,
            scale,
            value,
        }
    }

    pub fn as_i128(&self) -> i128 {
        self.value
    }

    pub fn as_string(&self) -> String {
        let value_str = self.value.to_string();

        if self.scale == 0 {
            value_str
        } else {
            let (sign, rest) = value_str.split_at(if self.value >= 0 { 0 } else { 1 });

            if rest.len() > self.scale {
                // Decimal separator is in the middle of the string
                let (whole, decimal) = value_str.split_at(value_str.len() - self.scale);
                format!("{}.{}", whole, decimal)
            } else {
                // String has to be padded
                format!("{}0.{:0>width$}", sign, rest, width = self.scale)
            }
        }
    }
}

impl From<Decimal128> for i128 {
    fn from(decimal: Decimal128) -> Self {
        decimal.as_i128()
    }
}

#[cfg(test)]
mod tests {
    use crate::util::decimal::Decimal128;

    #[test]
    fn decimal_128_to_string() {
        let mut value = Decimal128::new_from_i128(5, 2, 100);
        assert_eq!(value.as_string(), "1.00");

        value = Decimal128::new_from_i128(5, 3, 100);
        assert_eq!(value.as_string(), "0.100");
    }

    #[test]
    fn decimal_128_from_bytes() {
        let bytes = 100_i128.to_le_bytes();
        let value = Decimal128::new_from_bytes(5, 2, &bytes);
        assert_eq!(value.as_string(), "1.00");
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
