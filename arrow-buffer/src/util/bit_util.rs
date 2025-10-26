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

//! Utils for working with bits

/// Returns the nearest number that is `>=` than `num` and is a multiple of 64
#[inline]
pub fn round_upto_multiple_of_64(num: usize) -> usize {
    num.checked_next_multiple_of(64)
        .expect("failed to round upto multiple of 64")
}

/// Returns the nearest multiple of `factor` that is `>=` than `num`. Here `factor` must
/// be a power of 2.
pub fn round_upto_power_of_2(num: usize, factor: usize) -> usize {
    debug_assert!(factor > 0 && factor.is_power_of_two());
    num.checked_add(factor - 1)
        .expect("failed to round to next highest power of 2")
        & !(factor - 1)
}

/// Returns whether bit at position `i` in `data` is set or not
#[inline]
pub fn get_bit(data: &[u8], i: usize) -> bool {
    data[i / 8] & (1 << (i % 8)) != 0
}

/// Returns whether bit at position `i` in `data` is set or not.
///
/// # Safety
///
/// Note this doesn't do any bound checking, for performance reason. The caller is
/// responsible to guarantee that `i` is within bounds.
#[inline]
pub unsafe fn get_bit_raw(data: *const u8, i: usize) -> bool {
    unsafe { (*data.add(i / 8) & (1 << (i % 8))) != 0 }
}

/// Sets bit at position `i` for `data` to 1
#[inline]
pub fn set_bit(data: &mut [u8], i: usize) {
    data[i / 8] |= 1 << (i % 8);
}

/// Sets bit at position `i` for `data`
///
/// # Safety
///
/// Note this doesn't do any bound checking, for performance reason. The caller is
/// responsible to guarantee that `i` is within bounds.
#[inline]
pub unsafe fn set_bit_raw(data: *mut u8, i: usize) {
    unsafe {
        *data.add(i / 8) |= 1 << (i % 8);
    }
}

/// Sets bit at position `i` for `data` to 0
#[inline]
pub fn unset_bit(data: &mut [u8], i: usize) {
    data[i / 8] &= !(1 << (i % 8));
}

/// Sets bit at position `i` for `data` to 0
///
/// # Safety
///
/// Note this doesn't do any bound checking, for performance reason. The caller is
/// responsible to guarantee that `i` is within bounds.
#[inline]
pub unsafe fn unset_bit_raw(data: *mut u8, i: usize) {
    unsafe {
        *data.add(i / 8) &= !(1 << (i % 8));
    }
}

/// Returns the ceil of `value`/`divisor`
#[inline]
pub fn ceil(value: usize, divisor: usize) -> usize {
    value.div_ceil(divisor)
}

/// Read up to 8 bits from a byte slice starting at a given bit offset.
///
/// # Arguments
///
/// * `slice` - The byte slice to read from
/// * `number_of_bits_to_read` - Number of bits to read (must be < 8)
/// * `bit_offset` - Starting bit offset within the first byte (must be < 8)
///
/// # Returns
///
/// A `u8` containing the requested bits in the least significant positions
///
/// # Panics
/// - Panics if `number_of_bits_to_read` is 0 or >= 8
/// - Panics if `bit_offset` is >= 8
/// - Panics if `slice` is empty or too small to read the requested bits
///
#[inline]
pub(crate) fn read_up_to_byte_from_offset(
    slice: &[u8],
    number_of_bits_to_read: usize,
    bit_offset: usize,
) -> u8 {
    assert!(number_of_bits_to_read < 8, "can read up to 8 bits only");
    assert!(bit_offset < 8, "bit offset must be less than 8");
    assert_ne!(
        number_of_bits_to_read, 0,
        "number of bits to read must be greater than 0"
    );
    assert_ne!(slice.len(), 0, "slice must not be empty");

    let number_of_bytes_to_read = ceil(number_of_bits_to_read + bit_offset, 8);

    // number of bytes to read
    assert!(slice.len() >= number_of_bytes_to_read, "slice is too small");

    let mut bits = slice[0] >> bit_offset;
    for (i, &byte) in slice
        .iter()
        .take(number_of_bytes_to_read)
        .enumerate()
        .skip(1)
    {
        bits |= byte << (i * 8 - bit_offset);
    }

    bits & ((1 << number_of_bits_to_read) - 1)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    #[test]
    fn test_round_upto_multiple_of_64() {
        assert_eq!(0, round_upto_multiple_of_64(0));
        assert_eq!(64, round_upto_multiple_of_64(1));
        assert_eq!(64, round_upto_multiple_of_64(63));
        assert_eq!(64, round_upto_multiple_of_64(64));
        assert_eq!(128, round_upto_multiple_of_64(65));
        assert_eq!(192, round_upto_multiple_of_64(129));
    }

    #[test]
    #[should_panic(expected = "failed to round upto multiple of 64")]
    fn test_round_upto_multiple_of_64_panic() {
        let _ = round_upto_multiple_of_64(usize::MAX);
    }

    #[test]
    #[should_panic(expected = "failed to round to next highest power of 2")]
    fn test_round_upto_panic() {
        let _ = round_upto_power_of_2(usize::MAX, 2);
    }

    #[test]
    fn test_get_bit() {
        // 00001101
        assert!(get_bit(&[0b00001101], 0));
        assert!(!get_bit(&[0b00001101], 1));
        assert!(get_bit(&[0b00001101], 2));
        assert!(get_bit(&[0b00001101], 3));

        // 01001001 01010010
        assert!(get_bit(&[0b01001001, 0b01010010], 0));
        assert!(!get_bit(&[0b01001001, 0b01010010], 1));
        assert!(!get_bit(&[0b01001001, 0b01010010], 2));
        assert!(get_bit(&[0b01001001, 0b01010010], 3));
        assert!(!get_bit(&[0b01001001, 0b01010010], 4));
        assert!(!get_bit(&[0b01001001, 0b01010010], 5));
        assert!(get_bit(&[0b01001001, 0b01010010], 6));
        assert!(!get_bit(&[0b01001001, 0b01010010], 7));
        assert!(!get_bit(&[0b01001001, 0b01010010], 8));
        assert!(get_bit(&[0b01001001, 0b01010010], 9));
        assert!(!get_bit(&[0b01001001, 0b01010010], 10));
        assert!(!get_bit(&[0b01001001, 0b01010010], 11));
        assert!(get_bit(&[0b01001001, 0b01010010], 12));
        assert!(!get_bit(&[0b01001001, 0b01010010], 13));
        assert!(get_bit(&[0b01001001, 0b01010010], 14));
        assert!(!get_bit(&[0b01001001, 0b01010010], 15));
    }

    pub fn seedable_rng() -> StdRng {
        StdRng::seed_from_u64(42)
    }

    #[test]
    fn test_get_bit_raw() {
        const NUM_BYTE: usize = 10;
        let mut buf = [0; NUM_BYTE];
        let mut expected = vec![];
        let mut rng = seedable_rng();
        for i in 0..8 * NUM_BYTE {
            let b = rng.random_bool(0.5);
            expected.push(b);
            if b {
                set_bit(&mut buf[..], i)
            }
        }

        let raw_ptr = buf.as_ptr();
        for (i, b) in expected.iter().enumerate() {
            unsafe {
                assert_eq!(*b, get_bit_raw(raw_ptr, i));
            }
        }
    }

    #[test]
    fn test_set_bit() {
        let mut b = [0b00000010];
        set_bit(&mut b, 0);
        assert_eq!([0b00000011], b);
        set_bit(&mut b, 1);
        assert_eq!([0b00000011], b);
        set_bit(&mut b, 7);
        assert_eq!([0b10000011], b);
    }

    #[test]
    fn test_unset_bit() {
        let mut b = [0b11111101];
        unset_bit(&mut b, 0);
        assert_eq!([0b11111100], b);
        unset_bit(&mut b, 1);
        assert_eq!([0b11111100], b);
        unset_bit(&mut b, 7);
        assert_eq!([0b01111100], b);
    }

    #[test]
    fn test_set_bit_raw() {
        const NUM_BYTE: usize = 10;
        let mut buf = vec![0; NUM_BYTE];
        let mut expected = vec![];
        let mut rng = seedable_rng();
        for i in 0..8 * NUM_BYTE {
            let b = rng.random_bool(0.5);
            expected.push(b);
            if b {
                unsafe {
                    set_bit_raw(buf.as_mut_ptr(), i);
                }
            }
        }

        let raw_ptr = buf.as_ptr();
        for (i, b) in expected.iter().enumerate() {
            unsafe {
                assert_eq!(*b, get_bit_raw(raw_ptr, i));
            }
        }
    }

    #[test]
    fn test_unset_bit_raw() {
        const NUM_BYTE: usize = 10;
        let mut buf = vec![255; NUM_BYTE];
        let mut expected = vec![];
        let mut rng = seedable_rng();
        for i in 0..8 * NUM_BYTE {
            let b = rng.random_bool(0.5);
            expected.push(b);
            if !b {
                unsafe {
                    unset_bit_raw(buf.as_mut_ptr(), i);
                }
            }
        }

        let raw_ptr = buf.as_ptr();
        for (i, b) in expected.iter().enumerate() {
            unsafe {
                assert_eq!(*b, get_bit_raw(raw_ptr, i));
            }
        }
    }

    #[test]
    fn test_get_set_bit_roundtrip() {
        const NUM_BYTES: usize = 10;
        const NUM_SETS: usize = 10;

        let mut buffer: [u8; NUM_BYTES * 8] = [0; NUM_BYTES * 8];
        let mut v = HashSet::new();
        let mut rng = seedable_rng();
        for _ in 0..NUM_SETS {
            let offset = rng.random_range(0..8 * NUM_BYTES);
            v.insert(offset);
            set_bit(&mut buffer[..], offset);
        }
        for i in 0..NUM_BYTES * 8 {
            assert_eq!(v.contains(&i), get_bit(&buffer[..], i));
        }
    }

    #[test]
    fn test_ceil() {
        assert_eq!(ceil(0, 1), 0);
        assert_eq!(ceil(1, 1), 1);
        assert_eq!(ceil(1, 2), 1);
        assert_eq!(ceil(1, 8), 1);
        assert_eq!(ceil(7, 8), 1);
        assert_eq!(ceil(8, 8), 1);
        assert_eq!(ceil(9, 8), 2);
        assert_eq!(ceil(9, 9), 1);
        assert_eq!(ceil(10000000000, 10), 1000000000);
        assert_eq!(ceil(10, 10000000000), 1);
        assert_eq!(ceil(10000000000, 1000000000), 10);
    }

    #[test]
    fn test_read_up_to() {
        let all_ones = &[0b10111001, 0b10001100];

        for (bit_offset, expected) in [
            (0, 0b00000001),
            (1, 0b00000000),
            (2, 0b00000000),
            (3, 0b00000001),
            (4, 0b00000001),
            (5, 0b00000001),
            (6, 0b00000000),
            (7, 0b00000001),
        ] {
            let result = read_up_to_byte_from_offset(all_ones, 1, bit_offset);
            assert_eq!(
                result, expected,
                "failed at bit_offset {bit_offset}. result, expected:\n{result:08b}\n{expected:08b}"
            );
        }

        for (bit_offset, expected) in [
            (0, 0b00000001),
            (1, 0b00000000),
            (2, 0b00000010),
            (3, 0b00000011),
            (4, 0b00000011),
            (5, 0b00000001),
            (6, 0b00000010),
            (7, 0b00000001),
        ] {
            let result = read_up_to_byte_from_offset(all_ones, 2, bit_offset);
            assert_eq!(
                result, expected,
                "failed at bit_offset {bit_offset}. result, expected:\n{result:08b}\n{expected:08b}"
            );
        }

        for (bit_offset, expected) in [
            (0, 0b00111001),
            (1, 0b00011100),
            (2, 0b00101110),
            (3, 0b00010111),
            (4, 0b00001011),
            (5, 0b00100101),
            (6, 0b00110010),
            (7, 0b00011001),
        ] {
            let result = read_up_to_byte_from_offset(all_ones, 6, bit_offset);
            assert_eq!(
                result, expected,
                "failed at bit_offset {bit_offset}. result, expected:\n{result:08b}\n{expected:08b}"
            );
        }

        for (bit_offset, expected) in [
            (0, 0b00111001),
            (1, 0b01011100),
            (2, 0b00101110),
            (3, 0b00010111),
            (4, 0b01001011),
            (5, 0b01100101),
            (6, 0b00110010),
            (7, 0b00011001),
        ] {
            let result = read_up_to_byte_from_offset(all_ones, 7, bit_offset);
            assert_eq!(
                result, expected,
                "failed at bit_offset {bit_offset}. result, expected:\n{result:08b}\n{expected:08b}"
            );
        }
    }
}
