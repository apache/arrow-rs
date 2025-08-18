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

//! IEEE 754 total order comparison functions for floating point types.
//!
//! According to the IEEE 754 specification, the total order is:
//! -NaN < -Infinity < -x < -0 < +0 < +x < +Infinity < +NaN
//! where x represents any finite non-zero value.
//!
//! Within NaN values, the order is determined by the payload bits.

use half::f16;
use std::cmp::Ordering;

/// Converts a float to its total order representation.
/// This allows for bitwise comparison that respects IEEE 754 total order.
#[inline]
fn total_order_f32(f: f32) -> u32 {
    let bits = f.to_bits();
    if bits & 0x8000_0000 != 0 {
        // Negative number: flip all bits except sign
        !bits
    } else {
        // Positive number: flip only sign bit
        bits | 0x8000_0000
    }
}

/// Converts a double to its total order representation.
#[inline]
fn total_order_f64(f: f64) -> u64 {
    let bits = f.to_bits();
    if bits & 0x8000_0000_0000_0000 != 0 {
        // Negative number: flip all bits except sign
        !bits
    } else {
        // Positive number: flip only sign bit
        bits | 0x8000_0000_0000_0000
    }
}

/// Converts a f16 to its total order representation.
#[inline]
fn total_order_f16(f: f16) -> u16 {
    let bits = f.to_bits();
    if bits & 0x8000 != 0 {
        // Negative number: flip all bits except sign
        !bits
    } else {
        // Positive number: flip only sign bit
        bits | 0x8000
    }
}

/// Compare two f32 values using IEEE 754 total order.
pub fn compare_f32(a: f32, b: f32) -> Ordering {
    total_order_f32(a).cmp(&total_order_f32(b))
}

/// Compare two f64 values using IEEE 754 total order.
pub fn compare_f64(a: f64, b: f64) -> Ordering {
    total_order_f64(a).cmp(&total_order_f64(b))
}

/// Compare two f16 values using IEEE 754 total order.
pub fn compare_f16(a: f16, b: f16) -> Ordering {
    total_order_f16(a).cmp(&total_order_f16(b))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_f32_total_order() {
        // Test the order: -NaN < -Inf < -1.0 < -0.0 < +0.0 < 1.0 < +Inf < +NaN
        let neg_nan = f32::from_bits(0xffc00000);
        let neg_inf = f32::NEG_INFINITY;
        let neg_one = -1.0_f32;
        let neg_zero = -0.0_f32;
        let pos_zero = 0.0_f32;
        let pos_one = 1.0_f32;
        let pos_inf = f32::INFINITY;
        let pos_nan = f32::from_bits(0x7fc00000);

        assert!(compare_f32(neg_nan, neg_inf) == Ordering::Less);
        assert!(compare_f32(neg_inf, neg_one) == Ordering::Less);
        assert!(compare_f32(neg_one, neg_zero) == Ordering::Less);
        assert!(compare_f32(neg_zero, pos_zero) == Ordering::Less);
        assert!(compare_f32(pos_zero, pos_one) == Ordering::Less);
        assert!(compare_f32(pos_one, pos_inf) == Ordering::Less);
        assert!(compare_f32(pos_inf, pos_nan) == Ordering::Less);
    }

    #[test]
    fn test_f64_total_order() {
        // Test the order: -NaN < -Inf < -1.0 < -0.0 < +0.0 < 1.0 < +Inf < +NaN
        let neg_nan = f64::from_bits(0xfff8000000000000);
        let neg_inf = f64::NEG_INFINITY;
        let neg_one = -1.0_f64;
        let neg_zero = -0.0_f64;
        let pos_zero = 0.0_f64;
        let pos_one = 1.0_f64;
        let pos_inf = f64::INFINITY;
        let pos_nan = f64::from_bits(0x7ff8000000000000);

        assert!(compare_f64(neg_nan, neg_inf) == Ordering::Less);
        assert!(compare_f64(neg_inf, neg_one) == Ordering::Less);
        assert!(compare_f64(neg_one, neg_zero) == Ordering::Less);
        assert!(compare_f64(neg_zero, pos_zero) == Ordering::Less);
        assert!(compare_f64(pos_zero, pos_one) == Ordering::Less);
        assert!(compare_f64(pos_one, pos_inf) == Ordering::Less);
        assert!(compare_f64(pos_inf, pos_nan) == Ordering::Less);
    }

    #[test]
    fn test_f16_total_order() {
        // Test the order: -NaN < -Inf < -1.0 < -0.0 < +0.0 < 1.0 < +Inf < +NaN
        let neg_nan = f16::from_bits(0xfe00);
        let neg_inf = f16::NEG_INFINITY;
        let neg_one = f16::from_f32(-1.0);
        let neg_zero = f16::NEG_ZERO;
        let pos_zero = f16::ZERO;
        let pos_one = f16::from_f32(1.0);
        let pos_inf = f16::INFINITY;
        let pos_nan = f16::from_bits(0x7e00);

        assert!(compare_f16(neg_nan, neg_inf) == Ordering::Less);
        assert!(compare_f16(neg_inf, neg_one) == Ordering::Less);
        assert!(compare_f16(neg_one, neg_zero) == Ordering::Less);
        assert!(compare_f16(neg_zero, pos_zero) == Ordering::Less);
        assert!(compare_f16(pos_zero, pos_one) == Ordering::Less);
        assert!(compare_f16(pos_one, pos_inf) == Ordering::Less);
        assert!(compare_f16(pos_inf, pos_nan) == Ordering::Less);
    }
}
