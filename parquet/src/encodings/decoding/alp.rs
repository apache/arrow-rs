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

use std::marker::PhantomData;
use std::ops::Range;

use bytes::Bytes;

use crate::basic::Encoding;
use crate::data_type::DataType;
use crate::encodings::decoding::Decoder;
use crate::errors::{ParquetError, Result};

const ALP_HEADER_SIZE: usize = 8;
const ALP_VERSION: u8 = 1;
const ALP_COMPRESSION_MODE: u8 = 0;
const ALP_INTEGER_ENCODING_FOR_BIT_PACK: u8 = 0;
const ALP_MAX_LOG_VECTOR_SIZE: u8 = 16;

/// Page-level ALP header (version 1, 8 bytes).
///
/// Layout in bytes:
/// - `[0]` `version`
/// - `[1]` `compression_mode`
/// - `[2]` `integer_encoding`
/// - `[3]` `log_vector_size`
/// - `[4..8]` `num_elements` (little-endian `i32`)
///
/// This mirrors the C++ `AlpHeader` in `arrow/util/alp/alp_wrapper.cc`.
#[derive(Debug, Clone, Copy)]
struct AlpHeader {
    version: u8,
    compression_mode: u8,
    integer_encoding: u8,
    log_vector_size: u8,
    num_elements: i32,
}

impl AlpHeader {
    fn num_elements_usize(&self) -> usize {
        // `num_elements` is serialized as i32, matching Parquet page header `num_values`.
        // Parsing rejects negative values, so this conversion is safe.
        self.num_elements as usize
    }

    /// Returns `2 ^ log_vector_size`.
    fn vector_size(&self) -> usize {
        1usize << self.log_vector_size
    }

    /// Number of vectors in the page.
    ///
    /// All vectors are full-sized except possibly the last one.
    fn num_vectors(&self) -> usize {
        if self.num_elements == 0 {
            0
        } else {
            self.num_elements_usize().div_ceil(self.vector_size())
        }
    }

    /// Number of logical values in `vector_index`.
    fn vector_num_elements(&self, vector_index: usize) -> u16 {
        let vector_size = self.vector_size();
        let num_full_vectors = self.num_elements_usize() / vector_size;
        let remainder = self.num_elements_usize() % vector_size;
        if vector_index < num_full_vectors {
            vector_size as u16
        } else if vector_index == num_full_vectors && remainder > 0 {
            remainder as u16
        } else {
            0
        }
    }
}

/// Per-vector ALP metadata (4 bytes), equivalent to C++ `AlpEncodedVectorInfo`.
///
/// Layout in bytes:
/// - `[0]` `exponent`
/// - `[1]` `factor`
/// - `[2..4]` `num_exceptions` (little-endian `u16`)
#[derive(Debug, Clone, Copy)]
struct AlpEncodedVectorInfo {
    exponent: u8,
    factor: u8,
    num_exceptions: u16,
}

impl AlpEncodedVectorInfo {
    const STORED_SIZE: usize = 4;
}

/// Per-vector FOR metadata for a given exact integer type (`u32` for `f32`,
/// `u64` for `f64`), equivalent to C++ `AlpEncodedForVectorInfo<T>`.
#[derive(Debug, Clone, Copy)]
struct AlpEncodedForVectorInfo<Exact: AlpExact> {
    frame_of_reference: Exact,
    bit_width: u8,
}

impl<Exact: AlpExact> AlpEncodedForVectorInfo<Exact> {
    /// Serialized size of FOR metadata (`Exact::WIDTH + 1` for `bit_width`).
    fn stored_size() -> usize {
        Exact::WIDTH + 1
    }

    /// Number of bytes used for bit-packed encoded integers.
    fn get_bit_packed_size(&self, num_elements: u16) -> usize {
        (self.bit_width as usize * num_elements as usize).div_ceil(8)
    }

    /// Data-only size:
    /// `[packed_values][exception_positions][exception_values]`
    fn get_data_stored_size(&self, num_elements: u16, num_exceptions: u16) -> usize {
        let bit_packed_size = self.get_bit_packed_size(num_elements);
        bit_packed_size
            + num_exceptions as usize * std::mem::size_of::<u16>()
            + num_exceptions as usize * Exact::WIDTH
    }
}

/// Parsed view of one vector's metadata and data slices.
///
/// Note:
/// - `packed_values` is expressed as a range into the vector data section
/// - exceptions are copied into owned vectors for safety and simplicity
#[derive(Debug)]
struct AlpEncodedVectorView<Exact: AlpExact> {
    num_elements: u16,
    alp_info: AlpEncodedVectorInfo,
    for_info: AlpEncodedForVectorInfo<Exact>,
    packed_values: Range<usize>,
    exception_positions: Vec<u16>,
    exception_values: Vec<u8>,
}

/// Parsed ALP layout for a page with a concrete exact integer type.
///
/// `Exact = u32` for `f32` pages and `Exact = u64` for `f64` pages.
#[derive(Debug)]
struct AlpPageLayout<Exact: AlpExact> {
    header: AlpHeader,
    offsets: Vec<u32>,
    vectors: Vec<AlpEncodedVectorView<Exact>>,
}

/// Type-erased page layout used by `AlpDecoder<T>` so we can keep one decoder
/// implementation while preserving exact-width FOR storage.
#[derive(Debug)]
enum AlpPageLayoutAny {
    F32(AlpPageLayout<u32>),
    F64(AlpPageLayout<u64>),
}

impl AlpPageLayoutAny {
    fn num_vectors(&self) -> usize {
        match self {
            Self::F32(layout) => layout.vectors.len(),
            Self::F64(layout) => layout.vectors.len(),
        }
    }

    fn num_offsets(&self) -> usize {
        match self {
            Self::F32(layout) => layout.offsets.len(),
            Self::F64(layout) => layout.offsets.len(),
        }
    }

    fn parsed_values(&self) -> usize {
        match self {
            Self::F32(layout) => layout.vectors.iter().map(|v| v.num_elements as usize).sum(),
            Self::F64(layout) => layout.vectors.iter().map(|v| v.num_elements as usize).sum(),
        }
    }

    fn total_exceptions(&self) -> usize {
        match self {
            Self::F32(layout) => layout
                .vectors
                .iter()
                .map(|v| v.alp_info.num_exceptions as usize)
                .sum(),
            Self::F64(layout) => layout
                .vectors
                .iter()
                .map(|v| v.alp_info.num_exceptions as usize)
                .sum(),
        }
    }

    fn total_packed_bytes(&self) -> usize {
        match self {
            Self::F32(layout) => layout
                .vectors
                .iter()
                .map(|v| v.packed_values.end - v.packed_values.start)
                .sum(),
            Self::F64(layout) => layout
                .vectors
                .iter()
                .map(|v| v.packed_values.end - v.packed_values.start)
                .sum(),
        }
    }

    fn total_exception_bytes(&self) -> usize {
        match self {
            Self::F32(layout) => layout.vectors.iter().map(|v| v.exception_values.len()).sum(),
            Self::F64(layout) => layout.vectors.iter().map(|v| v.exception_values.len()).sum(),
        }
    }

    fn sum_for_xor(&self) -> u64 {
        match self {
            Self::F32(layout) => layout
                .vectors
                .iter()
                .fold(0u64, |acc, v| acc ^ v.for_info.frame_of_reference.to_u64()),
            Self::F64(layout) => layout
                .vectors
                .iter()
                .fold(0u64, |acc, v| acc ^ v.for_info.frame_of_reference.to_u64()),
        }
    }

    fn sum_positions(&self) -> usize {
        match self {
            Self::F32(layout) => layout
                .vectors
                .iter()
                .flat_map(|v| v.exception_positions.iter())
                .map(|v| *v as usize)
                .sum(),
            Self::F64(layout) => layout
                .vectors
                .iter()
                .flat_map(|v| v.exception_positions.iter())
                .map(|v| *v as usize)
                .sum(),
        }
    }
}

/// Exact integer type used by FOR reconstruction.
///
/// This mirrors C++:
/// - `float`  -> `uint32_t`
/// - `double` -> `uint64_t`
///
/// Why unsigned (not `i32`/`i64`)?
/// - FOR computes and stores deltas (`value - frame_of_reference`) that are
///   encoded with bitpacking and are naturally treated as non-negative bit
///   patterns.
/// - Using unsigned exact-width integers avoids signed-overflow edge cases in
///   the FOR stage and matches the C++ implementation's arithmetic model.
/// - Signed interpretation is still applied later during ALP decimal
///   reconstruction (after inverse FOR), so we preserve behavior while keeping
///   FOR metadata compact and byte-compatible.
trait AlpExact: Copy + std::fmt::Debug {
    const WIDTH: usize;
    fn from_le_slice(slice: &[u8]) -> Self;
    fn to_u64(self) -> u64;
}

impl AlpExact for u32 {
    const WIDTH: usize = 4;

    fn from_le_slice(slice: &[u8]) -> Self {
        u32::from_le_bytes([slice[0], slice[1], slice[2], slice[3]])
    }

    fn to_u64(self) -> u64 {
        self as u64
    }
}

impl AlpExact for u64 {
    const WIDTH: usize = 8;

    fn from_le_slice(slice: &[u8]) -> Self {
        u64::from_le_bytes([
            slice[0], slice[1], slice[2], slice[3], slice[4], slice[5], slice[6], slice[7],
        ])
    }

    fn to_u64(self) -> u64 {
        self
    }
}

/// Parse and validate a full ALP page body.
///
/// The caller provides `Exact` (`u32` or `u64`) based on the physical type.
/// Parsing validates:
/// - header fields and version
/// - offsets section bounds and monotonicity
/// - per-vector metadata and data section sizes
fn parse_alp_page_layout<Exact: AlpExact>(data: &[u8]) -> Result<AlpPageLayout<Exact>> {
    if data.len() < ALP_HEADER_SIZE {
        return Err(general_err!(
            "Invalid ALP page: expected at least {} bytes for header, got {}",
            ALP_HEADER_SIZE,
            data.len()
        ));
    }

    let header = AlpHeader {
        version: data[0],
        compression_mode: data[1],
        integer_encoding: data[2],
        log_vector_size: data[3],
        num_elements: i32::from_le_bytes([data[4], data[5], data[6], data[7]]),
    };

    if header.version != ALP_VERSION {
        return Err(general_err!(
            "Invalid ALP page: unsupported version {}, expected {}",
            header.version,
            ALP_VERSION
        ));
    }

    if header.compression_mode != ALP_COMPRESSION_MODE {
        return Err(general_err!(
            "Invalid ALP page: unsupported compression mode {}",
            header.compression_mode
        ));
    }

    if header.integer_encoding != ALP_INTEGER_ENCODING_FOR_BIT_PACK {
        return Err(general_err!(
            "Invalid ALP page: unsupported integer encoding {}",
            header.integer_encoding
        ));
    }

    if header.log_vector_size > ALP_MAX_LOG_VECTOR_SIZE {
        return Err(general_err!(
            "Invalid ALP page: log_vector_size {} exceeds max {}",
            header.log_vector_size,
            ALP_MAX_LOG_VECTOR_SIZE
        ));
    }
    if header.num_elements < 0 {
        return Err(general_err!(
            "Invalid ALP page: num_elements {} must be >= 0",
            header.num_elements
        ));
    }

    let num_vectors = header.num_vectors();

    let offsets_len = num_vectors
        .checked_mul(std::mem::size_of::<u32>())
        .ok_or_else(|| general_err!("Invalid ALP page: offsets length overflow"))?;
    let offsets_end = ALP_HEADER_SIZE
        .checked_add(offsets_len)
        .ok_or_else(|| general_err!("Invalid ALP page: header + offsets length overflow"))?;

    if data.len() < offsets_end {
        return Err(general_err!(
            "Invalid ALP page: expected at least {} bytes for {} offsets, got {}",
            offsets_end,
            num_vectors,
            data.len()
        ));
    }

    let body_len = data.len() - ALP_HEADER_SIZE;
    let offsets_section_size = num_vectors * std::mem::size_of::<u32>();
    let mut offsets = Vec::with_capacity(num_vectors);
    for i in 0..num_vectors {
        let start = ALP_HEADER_SIZE + i * 4;
        let offset = u32::from_le_bytes([
            data[start],
            data[start + 1],
            data[start + 2],
            data[start + 3],
        ]);
        if offset as usize >= body_len {
            return Err(general_err!(
                "Invalid ALP page: vector offset {} out of bounds for body length {}",
                offset,
                body_len
            ));
        }
        if (offset as usize) < offsets_section_size {
            return Err(general_err!(
                "Invalid ALP page: vector offset {} points into offsets section {}",
                offset,
                offsets_section_size
            ));
        }
        offsets.push(offset);
    }

    let body = &data[ALP_HEADER_SIZE..];
    let mut vectors = Vec::with_capacity(num_vectors);
    for (vector_idx, vector_offset) in offsets.iter().enumerate() {
        let vector_start = *vector_offset as usize;
        let vector_end = if vector_idx + 1 < offsets.len() {
            offsets[vector_idx + 1] as usize
        } else {
            body_len
        };

        if vector_end < vector_start {
            return Err(general_err!(
                "Invalid ALP page: vector offsets are not monotonic at index {}",
                vector_idx
            ));
        }

        let vector_num_elements = header.vector_num_elements(vector_idx);
        let vector_view = parse_vector_view(
            &body[vector_start..vector_end],
            vector_num_elements,
        )?;
        vectors.push(vector_view);
    }

    Ok(AlpPageLayout {
        header,
        offsets,
        vectors,
    })
}

/// Parse one vector:
/// `[AlpEncodedVectorInfo][AlpEncodedForVectorInfo][data-only section]`
///
/// Data-only section layout:
/// `[packed_values][exception_positions][exception_values]`
fn parse_vector_view<Exact: AlpExact>(
    vector_bytes: &[u8],
    num_elements: u16,
) -> Result<AlpEncodedVectorView<Exact>> {
    let metadata_size = AlpEncodedVectorInfo::STORED_SIZE
        + AlpEncodedForVectorInfo::<Exact>::stored_size();
    if vector_bytes.len() < metadata_size {
        return Err(general_err!(
            "Invalid ALP page: vector metadata too short, expected at least {} bytes, got {}",
            metadata_size,
            vector_bytes.len()
        ));
    }

    let alp_info = AlpEncodedVectorInfo {
        exponent: vector_bytes[0],
        factor: vector_bytes[1],
        num_exceptions: u16::from_le_bytes([vector_bytes[2], vector_bytes[3]]),
    };

    let for_start = AlpEncodedVectorInfo::STORED_SIZE;
    let for_end = for_start + Exact::WIDTH;
    let frame_of_reference = Exact::from_le_slice(&vector_bytes[for_start..for_end]);
    let bit_width = vector_bytes[for_end];
    if bit_width as usize > Exact::WIDTH * 8 {
        return Err(general_err!(
            "Invalid ALP page: bit width {} exceeds {}",
            bit_width,
            Exact::WIDTH * 8
        ));
    }

    let for_info = AlpEncodedForVectorInfo::<Exact> {
        frame_of_reference,
        bit_width,
    };

    let data_size = for_info.get_data_stored_size(num_elements, alp_info.num_exceptions);
    if vector_bytes.len() < metadata_size + data_size {
        return Err(general_err!(
            "Invalid ALP page: vector data too short, expected at least {} bytes, got {}",
            metadata_size + data_size,
            vector_bytes.len()
        ));
    }

    let data = &vector_bytes[metadata_size..metadata_size + data_size];
    let packed_size = for_info.get_bit_packed_size(num_elements);
    let positions_size = alp_info.num_exceptions as usize * std::mem::size_of::<u16>();
    let values_size = alp_info.num_exceptions as usize * Exact::WIDTH;

    let packed_start = 0;
    let packed_end = packed_start + packed_size;
    let positions_start = packed_end;
    let positions_end = positions_start + positions_size;
    let values_start = positions_end;
    let values_end = values_start + values_size;

    let mut exception_positions = Vec::with_capacity(alp_info.num_exceptions as usize);
    for chunk in data[positions_start..positions_end].chunks_exact(2) {
        exception_positions.push(u16::from_le_bytes([chunk[0], chunk[1]]));
    }
    let exception_values = data[values_start..values_end].to_vec();

    Ok(AlpEncodedVectorView {
        num_elements,
        alp_info,
        for_info,
        packed_values: packed_start..packed_end,
        exception_positions,
        exception_values,
    })
}

pub(crate) struct AlpDecoder<T: DataType> {
    num_values: usize,
    layout: Option<AlpPageLayoutAny>,
    _marker: PhantomData<T>,
}

impl<T: DataType> AlpDecoder<T> {
    pub(crate) fn new() -> Self {
        Self {
            num_values: 0,
            layout: None,
            _marker: PhantomData,
        }
    }
}

impl<T: DataType> Decoder<T> for AlpDecoder<T> {
    fn set_data(&mut self, data: Bytes, num_values: usize) -> Result<()> {
        // Keep exact-width FOR values per page type, matching C++ layout choices.
        let layout = match std::mem::size_of::<T::T>() {
            4 => AlpPageLayoutAny::F32(parse_alp_page_layout::<u32>(data.as_ref())?),
            8 => AlpPageLayoutAny::F64(parse_alp_page_layout::<u64>(data.as_ref())?),
            type_size => {
                return Err(general_err!(
                    "Invalid ALP page: exact type size {} is unsupported",
                    type_size
                ))
            }
        };
        let header_num_elements = match &layout {
            AlpPageLayoutAny::F32(layout) => layout.header.num_elements,
            AlpPageLayoutAny::F64(layout) => layout.header.num_elements,
        };
        if header_num_elements as usize != num_values {
            return Err(general_err!(
                "Invalid ALP page: header num_elements {} does not match page num_values {}",
                header_num_elements,
                num_values
            ));
        }

        self.num_values = num_values;
        self.layout = Some(layout);
        Ok(())
    }

    fn get(&mut self, _buffer: &mut [T::T]) -> Result<usize> {
        // Layout parsing succeeds in `set_data`; value decode is implemented next.
        let num_vectors = self.layout.as_ref().map(|layout| layout.num_vectors()).unwrap_or(0);
        let num_offsets = self.layout.as_ref().map(|layout| layout.num_offsets()).unwrap_or(0);
        let parsed_values = self
            .layout
            .as_ref()
            .map(|layout| layout.parsed_values())
            .unwrap_or(0);
        let total_exceptions = self
            .layout
            .as_ref()
            .map(|layout| layout.total_exceptions())
            .unwrap_or(0);
        let total_packed_bytes = self
            .layout
            .as_ref()
            .map(|layout| layout.total_packed_bytes())
            .unwrap_or(0);
        let total_exception_bytes = self
            .layout
            .as_ref()
            .map(|layout| layout.total_exception_bytes())
            .unwrap_or(0);
        let sum_for = self.layout.as_ref().map(|layout| layout.sum_for_xor()).unwrap_or(0);
        let sum_positions = self
            .layout
            .as_ref()
            .map(|layout| layout.sum_positions())
            .unwrap_or(0);
        Err(nyi_err!(
            "Encoding ALP page layout parsed ({} vectors, {} offsets, {} values, {} exceptions, {} packed bytes, {} exception bytes, for-xor {}, pos-sum {}), value decoding is not implemented",
            num_vectors,
            num_offsets,
            parsed_values,
            total_exceptions,
            total_packed_bytes,
            total_exception_bytes,
            sum_for,
            sum_positions
        ))
    }

    fn values_left(&self) -> usize {
        self.num_values
    }

    fn encoding(&self) -> Encoding {
        Encoding::ALP
    }

    fn skip(&mut self, num_values: usize) -> Result<usize> {
        let skipped = num_values.min(self.num_values);
        self.num_values -= skipped;
        Ok(skipped)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_alp_page_bytes(
        version: u8,
        compression_mode: u8,
        integer_encoding: u8,
        log_vector_size: u8,
        num_elements: i32,
        offsets: &[u32],
        body_tail_len: usize,
    ) -> Vec<u8> {
        let mut out = Vec::with_capacity(ALP_HEADER_SIZE + offsets.len() * 4 + body_tail_len);
        out.push(version);
        out.push(compression_mode);
        out.push(integer_encoding);
        out.push(log_vector_size);
        out.extend_from_slice(&num_elements.to_le_bytes());
        for offset in offsets {
            out.extend_from_slice(&offset.to_le_bytes());
        }
        out.extend(std::iter::repeat_n(0u8, body_tail_len));
        out
    }

    #[test]
    fn test_parse_alp_page_layout_valid() {
        // num_elements=4 with vector_size=4 -> one vector, one offset entry.
        let data = make_alp_page_bytes(1, 0, 0, 2, 4, &[4], 13);
        let parsed = parse_alp_page_layout::<u64>(&data).unwrap();
        assert_eq!(parsed.header.version, 1);
        assert_eq!(parsed.header.num_elements, 4);
        assert_eq!(parsed.offsets, vec![4]);
    }

    #[test]
    fn test_parse_alp_page_layout_short_header() {
        let err = parse_alp_page_layout::<u64>(&[0, 1, 2]).unwrap_err();
        assert!(
            err.to_string()
                .contains("Invalid ALP page: expected at least 8 bytes for header")
        );
    }

    #[test]
    fn test_parse_alp_page_layout_invalid_log_vector_size() {
        let data = make_alp_page_bytes(1, 0, 0, 17, 1, &[4], 8);
        let err = parse_alp_page_layout::<u64>(&data).unwrap_err();
        assert!(
            err.to_string()
                .contains("Invalid ALP page: log_vector_size 17 exceeds max 16")
        );
    }

    #[test]
    fn test_parse_alp_page_layout_invalid_integer_encoding() {
        let data = make_alp_page_bytes(1, 0, 1, 2, 1, &[4], 8);
        let err = parse_alp_page_layout::<u64>(&data).unwrap_err();
        assert!(
            err.to_string()
                .contains("Invalid ALP page: unsupported integer encoding 1")
        );
    }

    #[test]
    fn test_parse_alp_page_layout_negative_num_elements() {
        let data = make_alp_page_bytes(1, 0, 0, 2, -1, &[4], 8);
        let err = parse_alp_page_layout::<u64>(&data).unwrap_err();
        assert!(
            err.to_string()
                .contains("Invalid ALP page: num_elements -1 must be >= 0")
        );
    }

    #[test]
    fn test_parse_alp_page_layout_parses_vector_view_data_only_f64() {
        let mut vector = Vec::new();

        // AlpEncodedVectorInfo: exponent=2, factor=0, num_exceptions=1
        vector.push(2);
        vector.push(0);
        vector.extend_from_slice(&1u16.to_le_bytes());

        // AlpEncodedForVectorInfo<f64>: frame_of_reference=10, bit_width=0
        vector.extend_from_slice(&10u64.to_le_bytes());
        vector.push(0);

        // Packed values: bit_width=0 and num_elements=1 -> 0 bytes.
        // Exception positions (1 * u16)
        vector.extend_from_slice(&0u16.to_le_bytes());
        // Exception values (1 * f64 bytes)
        vector.extend_from_slice(&42.5_f64.to_le_bytes());

        let offsets = [4u32];
        let mut page = make_alp_page_bytes(1, 0, 0, 0, 1, &offsets, 0);
        page.extend_from_slice(&vector);

        let parsed = parse_alp_page_layout::<u64>(&page).unwrap();
        assert_eq!(parsed.vectors.len(), 1);
        assert_eq!(parsed.vectors[0].num_elements, 1);
        assert_eq!(parsed.vectors[0].alp_info.num_exceptions, 1);
        assert_eq!(parsed.vectors[0].for_info.bit_width, 0);
        assert_eq!(parsed.vectors[0].exception_positions, vec![0]);
        assert_eq!(
            parsed.vectors[0].exception_values,
            42.5_f64.to_le_bytes().to_vec()
        );
    }
}
