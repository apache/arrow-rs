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

use super::{column, encode};
use bytes::Bytes;
use parquet::DecodeResult;
use parquet::bloom_filter::{Sbbf, SbbfPushDecoder};
use parquet::errors::Result;
use std::fmt::Debug;
use std::ops::Range;

/// Expect that the [`DecodeResult`] is a [`DecodeResult::Data`] and return the corresponding element
fn expect_data<T: Debug>(result: Result<DecodeResult<T>>) -> T {
    match result.expect("Expected Ok(DecodeResult::Data(T))") {
        DecodeResult::Data(data) => data,
        result => panic!("Expected DecodeResult::Data, got {result:?}"),
    }
}

/// Expect that the [`DecodeResult`] is a [`DecodeResult::NeedsData`] and return the corresponding ranges
fn expect_needs_data<T: Debug>(result: Result<DecodeResult<T>>) -> Vec<Range<u64>> {
    match result.expect("Expected Ok(DecodeResult::NeedsData(ranges))") {
        DecodeResult::NeedsData(ranges) => ranges,
        result => panic!("Expected DecodeResult::NeedsData, got {result:?}"),
    }
}

#[test]
fn push_decoder_requests_exact_ranges() {
    // Three blocks are valid even though the writer normally uses powers of two.
    let mut filter = Sbbf::new(&[0; 96]);
    filter.insert("present");
    let mut serialized = Vec::new();
    filter.write(&mut serialized).unwrap();
    let bytes = Bytes::from(serialized);
    let header_length = (bytes.len() - 96) as u64;
    let offset = 13;
    assert!(Sbbf::from_bytes(&bytes).unwrap().check("present"));

    for length in [Some(bytes.len() as i32), None] {
        let mut decoder = SbbfPushDecoder::try_new(&column(Some(offset as i64), length))
            .unwrap()
            .unwrap();
        if length.is_some() {
            let range = offset..offset + bytes.len() as u64;
            assert_eq!(expect_needs_data(decoder.try_decode()), vec![range.clone()]);
            decoder.push_range(range, bytes.clone()).unwrap();
        } else {
            let prefix = offset..offset + 20;
            assert_eq!(
                expect_needs_data(decoder.try_decode()),
                vec![prefix.clone()]
            );
            decoder.push_range(prefix, bytes.slice(..20)).unwrap();
            // The legacy prefix overlaps the bitset; resume at the decoded header end.
            let bitset = offset + header_length..offset + bytes.len() as u64;
            assert_eq!(
                expect_needs_data(decoder.try_decode()),
                vec![bitset.clone()]
            );
            decoder
                .push_range(bitset, bytes.slice(header_length as usize..))
                .unwrap();
        }
        let loaded = expect_data(decoder.try_decode());
        assert_eq!(loaded.num_blocks(), 3);
        assert!(loaded.check("present"));
    }
}

#[test]
fn push_decoder_rejects_invalid_headers_and_bitsets() {
    let mut canonical = Vec::new();
    Sbbf::new(&[0; 32]).write(&mut canonical).unwrap();
    assert_eq!(encode(32, 32).as_ref(), canonical.as_slice());

    let mut unsupported = encode(32, 32).to_vec();
    unsupported[3] = 0x2c; // Unknown algorithm union variant.
    // Deserialization and push decoding must enforce the same header/payload contract.
    let malformed = [(-1, 32), (0, 0), (33, 33), (64, 32), (32, 64)]
        .into_iter()
        .map(|(declared, actual)| encode(declared, actual))
        .chain([Bytes::from_static(&[0]), Bytes::from(unsupported)]);
    for bytes in malformed {
        let range = 0..bytes.len() as u64;
        let mut decoder = SbbfPushDecoder::try_new(&column(Some(0), Some(bytes.len() as i32)))
            .unwrap()
            .unwrap();
        assert_eq!(expect_needs_data(decoder.try_decode()), vec![range.clone()]);
        decoder.push_range(range, bytes.clone()).unwrap();
        assert_eq!(
            decoder.try_decode().unwrap_err().to_string(),
            Sbbf::from_bytes(&bytes).unwrap_err().to_string()
        );
    }
    // Without an advertised length, reject invalid sizes before requesting the bitset.
    for declared in [-1, 0, 33] {
        let bytes = encode(declared, 32);
        let mut decoder = SbbfPushDecoder::try_new(&column(Some(0), None))
            .unwrap()
            .unwrap();
        assert_eq!(expect_needs_data(decoder.try_decode()), vec![0..20]);
        decoder.push_range(0..20, bytes.slice(..20)).unwrap();
        assert!(decoder.try_decode().is_err());
    }
}

#[test]
fn push_decoder_extended_headers_require_an_advertised_length() {
    let canonical = encode(32, 32);
    let header_length = canonical.len() - 32;
    let mut bytes = canonical[..header_length - 1].to_vec();
    // Unknown binary field extends the header beyond the legacy prefix.
    bytes.extend_from_slice(&[0x18, 24]);
    bytes.extend_from_slice(&[0; 24]);
    bytes.push(0);
    bytes.extend_from_slice(&canonical[header_length..]);
    let bytes = Bytes::from(bytes);
    assert_eq!(Sbbf::from_bytes(&bytes).unwrap().num_blocks(), 1);

    // An advertised length lets the parser skip the complete unknown field.
    let mut known = SbbfPushDecoder::try_new(&column(Some(0), Some(bytes.len() as i32)))
        .unwrap()
        .unwrap();
    let range = 0..bytes.len() as u64;
    assert_eq!(expect_needs_data(known.try_decode()), vec![range.clone()]);
    known.push_range(range, bytes.clone()).unwrap();
    assert_eq!(expect_data(known.try_decode()).num_blocks(), 1);

    // The legacy 20-byte prefix cannot decode this header or request a bitset.
    let mut legacy = SbbfPushDecoder::try_new(&column(Some(0), None))
        .unwrap()
        .unwrap();
    assert_eq!(expect_needs_data(legacy.try_decode()), vec![0..20]);
    legacy.push_range(0..20, bytes.slice(..20)).unwrap();
    assert!(legacy.try_decode().is_err());
}
