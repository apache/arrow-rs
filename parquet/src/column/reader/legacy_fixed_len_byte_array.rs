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
//! The ordinary decoder checks canonical PLAIN sizes before selecting a repair.
//! A bounded prefix probe can rule out the historical layout, but a match never
//! authorizes repair without an independent physical count. Repairs happen once
//! per page; normal value decoding does not need to know the historical layout.
//! Producer strings and embedded Arrow metadata never select a repair.
//!
//! Canonical PLAIN has `N * W` bytes; the historical representation has exactly
//! `N * (W + 4)` bytes with every little-endian length prefix equal to `W`. These
//! sizes are disjoint for nonempty pages with the same independently established
//! `N`. Without that count, the representations can overlap.
//!
//! To retire the mitigation, remove this module, the compatibility hook in
//! `decoder::prepare_v1_fixed_len_byte_array`, and the repair calls in the two
//! normalization helpers there. Replace noncanonical PLAIN repair with a length
//! error and reject FLBA/DELTA_LENGTH_BYTE_ARRAY instead of normalizing it.
//! No changes to the column reader's page setup, read/skip loops, byte-budget
//! validation or canonical tests are required. Keep physical-value validation:
//! it detects corruption independently of legacy support. Remove legacy fixture
//! tests separately.

use bytes::Bytes;

use super::decoder::{PreparedFixedLenByteArrayPage, normalize_fixed_len_byte_array_data};
use crate::basic::Encoding;
use crate::data_type::Int32Type;
use crate::encodings::decoding::{Decoder, DeltaBitPackDecoder};
use crate::errors::{ParquetError, Result};

/// Handle nullable V1 pages that may need compatibility processing. The caller
/// supplies a lazy, independent count; only ambiguous PLAIN or degenerate pages
/// invoke it. Returns `true` if preparation is complete; `false` leaves `page`
/// unchanged for the caller's ordinary canonical path. Work in place so that
/// ambiguous but canonical pages do not need additional buffer-reference clones.
pub(super) fn prepare_v1_page(
    page: &mut PreparedFixedLenByteArrayPage,
    num_levels: usize,
    type_length: usize,
    count_values: &impl Fn() -> Result<usize>,
) -> Result<bool> {
    let (num_values, deferred) = match page.encoding {
        Encoding::PLAIN
            if num_levels != 0
                && type_length != 0
                && may_be_length_prefixed_plain(&page.data, type_length) =>
        {
            // Even all prefixes matching is not permission to repair. The
            // normalizer must first compare the independently counted canonical size.
            (count_values()?, false)
        }
        Encoding::DELTA_LENGTH_BYTE_ARRAY => {
            if num_levels == 0 || type_length == 0 {
                (count_values()?, false)
            } else {
                let declared = delta_length_value_count(page.data.clone())?;
                if declared > num_levels {
                    return Err(general_err!(
                        "DELTA_LENGTH_BYTE_ARRAY has more values than levels"
                    ));
                }
                (declared, true)
            }
        }
        _ => return Ok(false),
    };
    let (encoding, data) = normalize_fixed_len_byte_array_data(
        std::mem::take(&mut page.data),
        num_values,
        type_length,
        page.encoding,
    )?;
    page.encoding = encoding;
    page.validate_count = deferred;
    page.data = data;
    Ok(true)
}

/// A negative result rules out the supported historical PLAIN layout. A positive
/// result only requests an independent count: even every prefix matching could
/// be an ordinary canonical payload. Bound the probe independently of page size.
fn may_be_length_prefixed_plain(data: &[u8], type_length: usize) -> bool {
    let Some(stride) = type_length.checked_add(4) else {
        return false;
    };
    !data.is_empty()
        && data.len().is_multiple_of(stride)
        && data
            .chunks_exact(stride)
            .take(5)
            .all(|value| u32::from_le_bytes(value[..4].try_into().unwrap()) as usize == type_length)
}

/// Read the declared number of lengths, not an independent physical count.
/// The caller must still check lengths, payload bytes and actual definition levels.
fn delta_length_value_count(data: Bytes) -> Result<usize> {
    let mut decoder = DeltaBitPackDecoder::<Int32Type>::new();
    decoder.set_data(data, 0)?;
    Ok(decoder.values_left())
}

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
    use crate::column::reader::tests::{
        check_incremental_fixed_len_byte_array_pages, nullable_fixed_page, nullable_fixed_reader,
    };
    use crate::encodings::encoding::{DeltaBitPackEncoder, Encoder};
    use rand::{prelude::*, rngs::StdRng};

    #[test]
    fn ordinary_pages_do_not_request_compatibility_counting() {
        for encoding in [
            Encoding::PLAIN,
            Encoding::RLE_DICTIONARY,
            Encoding::DELTA_BYTE_ARRAY,
        ] {
            let mut page = PreparedFixedLenByteArrayPage {
                encoding,
                data: Bytes::from_static(b"abcdefghijkl"),
                validate_count: false,
            };
            assert!(
                !prepare_v1_page(&mut page, 3, 4, &|| {
                    panic!("ordinary page must not be pre-scanned")
                })
                .unwrap()
            );
            assert_eq!(page.encoding, encoding);
            assert_eq!(page.data.as_ref(), b"abcdefghijkl");
            assert!(!page.validate_count);
        }
    }

    #[test]
    fn matching_prefixes_require_an_independent_count() {
        let calls = std::cell::Cell::new(0);
        let raw = Bytes::from(4_u32.to_le_bytes().repeat(10));
        let mut page = PreparedFixedLenByteArrayPage {
            encoding: Encoding::PLAIN,
            data: raw.clone(),
            validate_count: false,
        };
        assert!(
            prepare_v1_page(&mut page, 20, 4, &|| {
                calls.set(calls.get() + 1);
                Ok(10) // ten present values, not twenty logical levels
            })
            .unwrap()
        );
        assert_eq!(calls.get(), 1);
        assert_eq!(page.data, raw);
        assert_eq!(page.encoding, Encoding::PLAIN);
        assert!(!page.validate_count);

        // Five matching prefixes do not permit accepting a bad sixth prefix.
        let mut malformed = 4_u32.to_le_bytes().repeat(12);
        malformed[40..44].fill(0xff);
        let mut reader = nullable_fixed_reader(vec![nullable_fixed_page(
            &[1; 6],
            Encoding::PLAIN,
            &malformed,
        )]);
        let mut values = vec![];
        assert!(
            reader
                .read_records(1, Some(&mut vec![]), None, &mut values)
                .is_err()
        );
        assert!(values.is_empty());
    }

    #[test]
    fn incremental_delta_length_count_validation() {
        check_incremental_fixed_len_byte_array_pages(Encoding::DELTA_LENGTH_BYTE_ARRAY, |count| {
            let mut lengths = DeltaBitPackEncoder::<Int32Type>::new();
            lengths.put(&vec![4; count]).unwrap();
            let mut payload = lengths.flush_buffer().unwrap().to_vec();
            payload.extend_from_slice(&b"abcd".repeat(count));
            payload
        });

        for lengths in [vec![], vec![4; 5], vec![3, 5]] {
            let mut encoder = DeltaBitPackEncoder::<Int32Type>::new();
            encoder.put(&lengths).unwrap();
            let mut payload = encoder.flush_buffer().unwrap().to_vec();
            payload.extend(std::iter::repeat_n(
                b'x',
                lengths.iter().sum::<i32>() as usize,
            ));
            let def = if lengths.is_empty() {
                [0; 4]
            } else {
                [1, 0, 1, 0]
            };
            let mut reader = nullable_fixed_reader(vec![nullable_fixed_page(
                &def,
                Encoding::DELTA_LENGTH_BYTE_ARRAY,
                &payload,
            )]);
            let result = reader.read_records(1, Some(&mut vec![]), None, &mut vec![]);
            if lengths.is_empty() {
                assert_eq!(result.unwrap(), (1, 0, 1));
                assert_eq!(reader.skip_records(3).unwrap(), 3);
            } else {
                // Invalid declared counts or individual lengths remain eager errors.
                assert!(result.is_err());
            }
        }
    }

    #[test]
    fn prefix_probe_only_rules_out_legacy() {
        for width in [0, 1, 2, 3, 4, 16, 32] {
            for count in [0, 1, 5, 6] {
                let mut data = vec![];
                for _ in 0..count {
                    data.extend_from_slice(&(width as u32).to_le_bytes());
                    data.extend(std::iter::repeat_n(0x55, width));
                }
                assert_eq!(may_be_length_prefixed_plain(&data, width), count != 0);
                if !data.is_empty() {
                    let mut truncated = data.clone();
                    truncated.pop();
                    assert!(!may_be_length_prefixed_plain(&truncated, width));
                    data[0] ^= 1;
                    assert!(!may_be_length_prefixed_plain(&data, width));
                }
            }
        }
        assert!(!may_be_length_prefixed_plain(&[], usize::MAX));
        let mut data = 4_u32.to_le_bytes().repeat(12);
        data[40..44].fill(0xff);
        assert!(may_be_length_prefixed_plain(&data, 4)); // probe is bounded
    }

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
