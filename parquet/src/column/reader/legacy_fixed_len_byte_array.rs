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

//! Temporary reader mitigation for [arrow-rs #11261], fixed by #11262.
//! These historical layouts are not additional Parquet encodings.
//!
//! [arrow-rs #11261]: https://github.com/apache/arrow-rs/issues/11261
//!
//! The ordinary decoder checks canonical PLAIN sizes first. Only a size mismatch
//! or the nonconforming FLBA/DELTA_LENGTH_BYTE_ARRAY combination reaches this
//! module. Repairs happen once per page; normal value decoding and skipping do
//! not need to know about the historical representation. Producer strings and
//! embedded Arrow metadata are never used to select a repair.
//!
//! Canonical PLAIN has `N * W` bytes; the historical representation has exactly
//! `N * (W + 4)` bytes with every little-endian length prefix equal to `W`. These
//! sizes are disjoint for nonempty pages. Empty canonical pages bypass repair.
//!
//! To retire the mitigation, remove this module and its calls in `decoder`,
//! reject noncanonical PLAIN sizes, and stop including DELTA_LENGTH_BYTE_ARRAY
//! in the V1 physical-value-count predicate in `GenericColumnReader`. Preserve
//! canonical size validation and physical-value counting: these also detect
//! corruption and are not legacy-only behavior. Remove the legacy fixture tests
//! separately, without removing the canonical validation tests.

use bytes::Bytes;

use crate::data_type::Int32Type;
use crate::encodings::decoding::{Decoder, DeltaBitPackDecoder};
use crate::errors::{ParquetError, Result};

/// Remove BYTE_ARRAY length prefixes from a noncanonical PLAIN FLBA section.
/// The caller has checked `expected_len = num_values * type_length` for overflow
/// and established that the input does not have that canonical length.
#[cold]
pub(super) fn decode_length_prefixed_plain(
    data: Bytes,
    num_values: usize,
    type_length: usize,
    expected_len: usize,
    page: &str,
) -> Result<Bytes> {
    let stride = type_length.checked_add(4);
    if stride.and_then(|s| num_values.checked_mul(s)) != Some(data.len()) {
        return Err(general_err!(
            "Invalid FIXED_LEN_BYTE_ARRAY {} payload length: expected {} bytes \
             ({} values of {} bytes), got {}",
            page,
            expected_len,
            num_values,
            type_length,
            data.len()
        ));
    }
    // The checked size above guarantees a nonzero, representable stride.
    let stride = stride.unwrap();
    for value in data.chunks_exact(stride) {
        let length = u32::from_le_bytes(value[..4].try_into().unwrap()) as usize;
        if length != type_length {
            return Err(general_err!(
                "Invalid FIXED_LEN_BYTE_ARRAY {} payload length prefix: expected {}, got {}",
                page,
                type_length,
                length
            ));
        }
    }

    // Validate the entire page before allocating or exposing any legacy values.
    let mut values = Vec::with_capacity(expected_len);
    for value in data.chunks_exact(stride) {
        values.extend_from_slice(&value[4..]);
    }
    Ok(values.into())
}

/// Validate the BYTE_ARRAY-only delta-length representation and return its
/// contiguous fixed-width values. The caller supplies the overflow-checked
/// canonical payload size and passes the returned value section to PLAIN.
#[cold]
pub(super) fn decode_delta_length(
    data: Bytes,
    num_values: usize,
    type_length: usize,
    expected_len: usize,
) -> Result<Bytes> {
    let mut decoder = DeltaBitPackDecoder::<Int32Type>::new();
    decoder.set_data(data.clone(), num_values)?;
    if decoder.values_left() != num_values {
        return Err(general_err!(
            "Invalid FIXED_LEN_BYTE_ARRAY DELTA_LENGTH_BYTE_ARRAY value count: expected {}, got {}",
            num_values,
            decoder.values_left()
        ));
    }
    // Bound scratch space independently of untrusted page counts.
    let mut lengths = [0_i32; 128];
    let mut remaining = num_values;
    while remaining != 0 {
        let count = remaining.min(lengths.len());
        let read = decoder.get(&mut lengths[..count])?;
        if read != count {
            return Err(general_err!(
                "Truncated FLBA DELTA_LENGTH_BYTE_ARRAY lengths"
            ));
        }
        for &length in &lengths[..count] {
            if usize::try_from(length).ok() != Some(type_length) {
                return Err(general_err!(
                    "Invalid FIXED_LEN_BYTE_ARRAY DELTA_LENGTH_BYTE_ARRAY value length: expected {}, got {}",
                    type_length,
                    length
                ));
            }
        }
        remaining -= count;
    }
    let offset = decoder.get_offset();
    if data.len().checked_sub(offset) != Some(expected_len) {
        return Err(general_err!(
            "Invalid FIXED_LEN_BYTE_ARRAY DELTA_LENGTH_BYTE_ARRAY payload length: expected {} value bytes",
            expected_len
        ));
    }
    Ok(data.slice(offset..))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::basic::Encoding;
    use crate::column::reader::decoder::{
        normalize_fixed_len_byte_array_data, normalize_fixed_len_byte_array_payload,
    };
    use crate::encodings::encoding::{DeltaBitPackEncoder, Encoder};
    use rand::{prelude::*, rngs::StdRng};

    #[test]
    fn length_prefixed_plain() {
        let mut rng = StdRng::seed_from_u64(11261);
        for page in ["dictionary page", "PLAIN data page"] {
            for width in [0, 1, 4, 8, 16, 32] {
                for count in [0, 1, 2, 33] {
                    let raw: Bytes = (0..width * count).map(|_| rng.random::<u8>()).collect();
                    let mut legacy = vec![];
                    for i in 0..count {
                        legacy.extend_from_slice(&(width as u32).to_le_bytes());
                        legacy.extend_from_slice(&raw[i * width..(i + 1) * width]);
                    }
                    assert_eq!(
                        normalize_fixed_len_byte_array_payload(legacy.into(), count, width, page)
                            .unwrap(),
                        raw
                    );
                }
            }
            // An exact aggregate size is insufficient: every prefix must match.
            for data in [
                b"\x03\0\0\0abc\x05\0\0\0defgh".as_slice(),
                b"\x04\0\0\0abcd\xff\xff\xff\xffefgh",
                b"\x04\0\0\0abcd\x04\0\0\0efg",
                b"\x04\0\0\0abcd\x04\0\0\0efghi",
            ] {
                assert!(
                    normalize_fixed_len_byte_array_payload(
                        Bytes::copy_from_slice(data),
                        2,
                        4,
                        page,
                    )
                    .is_err()
                );
            }
        }
    }

    #[test]
    fn data_pages() {
        let length_prefixed = |raw: &[u8]| {
            let mut legacy = vec![];
            for value in raw.as_chunks::<4>().0 {
                legacy.extend_from_slice(&4_u32.to_le_bytes());
                legacy.extend_from_slice(value);
            }
            legacy
        };
        super::super::tests::check_fixed_len_byte_array_pages(
            |raw| {
                let mut lengths = DeltaBitPackEncoder::<Int32Type>::new();
                lengths.put(&vec![4; raw.len() / 4]).unwrap();
                let mut delta = lengths.flush_buffer().unwrap().to_vec();
                delta.extend_from_slice(raw);
                vec![
                    (Encoding::PLAIN, length_prefixed(raw)),
                    (Encoding::DELTA_LENGTH_BYTE_ARRAY, delta),
                ]
            },
            |raw| {
                let mut bad_prefix = length_prefixed(raw);
                if raw.is_empty() {
                    // All-null pages must have an empty PLAIN value section.
                    bad_prefix.extend_from_slice(&4_u32.to_le_bytes());
                } else {
                    bad_prefix[0] = 3;
                }
                vec![bad_prefix]
            },
        );
    }

    #[test]
    fn delta_length() {
        let encode = |lengths: &[i32], values: &[u8]| {
            let mut encoder = DeltaBitPackEncoder::<Int32Type>::new();
            encoder.put(lengths).unwrap();
            let mut data = encoder.flush_buffer().unwrap().to_vec();
            data.extend_from_slice(values);
            Bytes::from(data)
        };
        let normalize = |data, count| {
            normalize_fixed_len_byte_array_data(data, count, 4, Encoding::DELTA_LENGTH_BYTE_ARRAY)
        };
        for count in [0, 1, 2, 129, 2051] {
            let values = b"abcd".repeat(count);
            let data = encode(&vec![4; count], &values);
            let (encoding, result) = normalize(data.clone(), count).unwrap();
            assert_eq!(encoding, Encoding::PLAIN);
            assert_eq!(result.as_ref(), values);
            assert_eq!(
                result.as_ptr(),
                data.as_ptr().wrapping_add(data.len() - result.len())
            );
        }
        for (lengths, values, count) in [
            (vec![3, 5], b"abcdefgh".as_slice(), 2),
            (vec![4, -1], b"abcdefgh", 2),
            (vec![4, 4], b"abcdefgh", 1),
            (vec![4, 4], b"abcdefg", 2),
            (vec![4, 4], b"abcdefghi", 2),
            (vec![], b"x", 0),
        ] {
            assert!(normalize(encode(&lengths, values), count).is_err());
        }
        assert!(normalize(Bytes::new(), 1).is_err());
        assert!(normalize(Bytes::new(), usize::MAX).is_err());
    }
}
