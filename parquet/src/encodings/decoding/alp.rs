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
use crate::util::bit_util::{BitReader, FromBytes};

const ALP_HEADER_SIZE: usize = 8;
const ALP_VERSION: u8 = 1;
const ALP_COMPRESSION_MODE: u8 = 0;
const ALP_INTEGER_ENCODING_FOR_BIT_PACK: u8 = 0;
const ALP_MAX_LOG_VECTOR_SIZE: u8 = 16;
const ALP_MAX_EXPONENT_F32: u8 = 10;
const ALP_MAX_EXPONENT_F64: u8 = 18;

/// Page-level ALP header (version 1, 8 bytes).
///
/// Layout in bytes:
/// - `[0]` `version`
/// - `[1]` `compression_mode`
/// - `[2]` `integer_encoding`
/// - `[3]` `log_vector_size`
/// - `[4..8]` `num_elements` (little-endian `i32`)
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
        self.num_elements as usize
    }

    fn vector_size(&self) -> usize {
        1usize << self.log_vector_size
    }

    fn num_vectors(&self) -> usize {
        if self.num_elements == 0 {
            0
        } else {
            self.num_elements_usize().div_ceil(self.vector_size())
        }
    }

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
#[derive(Debug, Clone, Copy)]
struct AlpEncodedVectorInfo {
    exponent: u8,
    factor: u8,
    num_exceptions: u16,
}

impl AlpEncodedVectorInfo {
    const STORED_SIZE: usize = 4;
}

/// Per-vector FOR metadata for exact integer type (`u32` for `f32`, `u64` for `f64`).
#[derive(Debug, Clone, Copy)]
struct AlpEncodedForVectorInfo<Exact: AlpExact> {
    frame_of_reference: Exact,
    bit_width: u8,
}

impl<Exact: AlpExact> AlpEncodedForVectorInfo<Exact> {
    fn stored_size() -> usize {
        Exact::WIDTH + 1
    }

    fn get_bit_packed_size(&self, num_elements: u16) -> usize {
        (self.bit_width as usize * num_elements as usize).div_ceil(8)
    }

    fn get_data_stored_size(&self, num_elements: u16, num_exceptions: u16) -> usize {
        let bit_packed_size = self.get_bit_packed_size(num_elements);
        bit_packed_size
            + num_exceptions as usize * std::mem::size_of::<u16>()
            + num_exceptions as usize * Exact::WIDTH
    }
}

/// Parsed view of one vector's metadata and data slices.
///
/// `packed_values` is a zero-copy range into page body bytes.
/// Exception positions/values are copied for straightforward decode handling.
#[derive(Debug)]
struct AlpEncodedVectorView<Exact: AlpExact> {
    num_elements: u16,
    alp_info: AlpEncodedVectorInfo,
    for_info: AlpEncodedForVectorInfo<Exact>,
    packed_values: Range<usize>,
    exception_positions: Vec<u16>,
    exception_values: Vec<Exact>,
}

/// Parsed ALP page layout for one exact integer width (`u32` for float pages,
/// `u64` for double pages).
#[derive(Debug)]
struct AlpPageLayout {
    header: AlpHeader,
    body: Bytes,
    offsets: Vec<u32>,
}

/// Exact integer type used by FOR reconstruction.
///
/// This mirrors C++:
/// - `float`  -> `uint32_t`
/// - `double` -> `uint64_t`
///
/// Why unsigned (not `i32`/`i64`)?
/// - FOR stores non-negative deltas optimized for bitpacking.
/// - Unsigned arithmetic avoids signed-overflow edge cases in FOR stage.
/// - Signed interpretation is applied later during decimal reconstruction.
pub(super) trait AlpExact: Copy + std::fmt::Debug {
    const WIDTH: usize;
    type Signed: Copy;
    fn from_le_slice(slice: &[u8]) -> Self;
    fn zero() -> Self;
    fn wrapping_add(self, rhs: Self) -> Self;
    fn reinterpret_as_signed(self) -> Self::Signed;
}

impl AlpExact for u32 {
    const WIDTH: usize = 4;
    type Signed = i32;

    fn from_le_slice(slice: &[u8]) -> Self {
        u32::from_le_bytes([slice[0], slice[1], slice[2], slice[3]])
    }

    fn zero() -> Self {
        0
    }

    fn wrapping_add(self, rhs: Self) -> Self {
        self.wrapping_add(rhs)
    }

    fn reinterpret_as_signed(self) -> Self::Signed {
        i32::from_ne_bytes(self.to_ne_bytes())
    }
}

impl AlpExact for u64 {
    const WIDTH: usize = 8;
    type Signed = i64;

    fn from_le_slice(slice: &[u8]) -> Self {
        u64::from_le_bytes([
            slice[0], slice[1], slice[2], slice[3], slice[4], slice[5], slice[6], slice[7],
        ])
    }

    fn zero() -> Self {
        0
    }

    fn wrapping_add(self, rhs: Self) -> Self {
        self.wrapping_add(rhs)
    }

    fn reinterpret_as_signed(self) -> Self::Signed {
        i64::from_ne_bytes(self.to_ne_bytes())
    }
}

const ALP_I64_POW10: [i64; 19] = [
    1,
    10,
    100,
    1_000,
    10_000,
    100_000,
    1_000_000,
    10_000_000,
    100_000_000,
    1_000_000_000,
    10_000_000_000,
    100_000_000_000,
    1_000_000_000_000,
    10_000_000_000_000,
    100_000_000_000_000,
    1_000_000_000_000_000,
    10_000_000_000_000_000,
    100_000_000_000_000_000,
    1_000_000_000_000_000_000,
];

const ALP_NEG_POW10_F32: [f32; 11] = [
    1.0,
    0.1,
    0.01,
    0.001,
    0.0001,
    0.00001,
    0.000001,
    0.0000001,
    0.00000001,
    0.000000001,
    0.0000000001,
];

const ALP_NEG_POW10_F64: [f64; 19] = [
    1.0,
    0.1,
    0.01,
    0.001,
    0.0001,
    0.00001,
    0.000001,
    0.0000001,
    0.00000001,
    0.000000001,
    0.0000000001,
    0.00000000001,
    0.000000000001,
    0.0000000000001,
    0.00000000000001,
    0.000000000000001,
    0.0000000000000001,
    0.00000000000000001,
    0.000000000000000001,
];

pub(super) trait AlpFloat: Copy + Default {
    type Exact: AlpExact + FromBytes;

    /// Precompute vector-level ALP decimal scale for:
    /// `value = encoded * 10^(factor) * 10^(-exponent)`.
    ///
    /// Preconditions are validated during page parse.
    fn decode_scale(exponent: u8, factor: u8) -> Self;

    /// Decode one signed exact integer using a precomputed scale.
    fn decode_value(signed_encoded: <Self::Exact as AlpExact>::Signed, scale: Self) -> Self;

    fn from_exact_bits(bits: Self::Exact) -> Self;
}

impl AlpFloat for f32 {
    type Exact = u32;

    fn decode_scale(exponent: u8, factor: u8) -> Self {
        debug_assert!(exponent <= ALP_MAX_EXPONENT_F32);
        debug_assert!(factor <= exponent);
        (ALP_I64_POW10[factor as usize] as f32) * ALP_NEG_POW10_F32[exponent as usize]
    }

    fn decode_value(signed_encoded: i32, scale: Self) -> Self {
        (signed_encoded as f32) * scale
    }

    fn from_exact_bits(bits: Self::Exact) -> Self {
        f32::from_bits(bits)
    }
}

impl AlpFloat for f64 {
    type Exact = u64;

    fn decode_scale(exponent: u8, factor: u8) -> Self {
        debug_assert!(exponent <= ALP_MAX_EXPONENT_F64);
        debug_assert!(factor <= exponent);
        (ALP_I64_POW10[factor as usize] as f64) * ALP_NEG_POW10_F64[exponent as usize]
    }

    fn decode_value(signed_encoded: i64, scale: Self) -> Self {
        (signed_encoded as f64) * scale
    }

    fn from_exact_bits(bits: Self::Exact) -> Self {
        f64::from_bits(bits)
    }
}

/// Parse and validate a full ALP-encoded page body.
///
/// Validation includes:
/// - header fields/version/encoding
/// - non-negative `num_elements`
/// - offsets bounds + monotonicity
/// - per-vector metadata/data section lengths
fn parse_alp_page_layout<Exact: AlpExact>(data: Bytes) -> Result<AlpPageLayout> {
    let data_ref = data.as_ref();
    if data_ref.len() < ALP_HEADER_SIZE {
        return Err(general_err!(
            "Invalid ALP page: expected at least {} bytes for header, got {}",
            ALP_HEADER_SIZE,
            data_ref.len()
        ));
    }

    let header = AlpHeader {
        version: data_ref[0],
        compression_mode: data_ref[1],
        integer_encoding: data_ref[2],
        log_vector_size: data_ref[3],
        num_elements: i32::from_le_bytes([data_ref[4], data_ref[5], data_ref[6], data_ref[7]]),
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

    if data_ref.len() < offsets_end {
        return Err(general_err!(
            "Invalid ALP page: expected at least {} bytes for {} offsets, got {}",
            offsets_end,
            num_vectors,
            data_ref.len()
        ));
    }

    let body = data.slice(ALP_HEADER_SIZE..);
    let body_ref = body.as_ref();
    let body_len = body_ref.len();
    let offsets_section_size = num_vectors * std::mem::size_of::<u32>();

    let mut offsets = Vec::with_capacity(num_vectors);
    for i in 0..num_vectors {
        let start = ALP_HEADER_SIZE + i * 4;
        let offset = u32::from_le_bytes([
            data_ref[start],
            data_ref[start + 1],
            data_ref[start + 2],
            data_ref[start + 3],
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
        parse_vector_view::<Exact>(body_ref, vector_start, vector_end, vector_num_elements)?;
    }

    Ok(AlpPageLayout {
        header,
        body,
        offsets,
    })
}

/// Parse a single vector section:
/// `[AlpInfo][ForInfo][PackedValues][ExceptionPositions][ExceptionValues]`.
fn parse_vector_view<Exact: AlpExact>(
    body: &[u8],
    vector_start: usize,
    vector_end: usize,
    num_elements: u16,
) -> Result<AlpEncodedVectorView<Exact>> {
    let vector_bytes = &body[vector_start..vector_end];

    let metadata_size =
        AlpEncodedVectorInfo::STORED_SIZE + AlpEncodedForVectorInfo::<Exact>::stored_size();
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

    let max_exponent = if Exact::WIDTH == 4 {
        ALP_MAX_EXPONENT_F32
    } else {
        ALP_MAX_EXPONENT_F64
    };

    if alp_info.exponent > max_exponent {
        return Err(general_err!(
            "Invalid ALP page: exponent {} exceeds max {}",
            alp_info.exponent,
            max_exponent
        ));
    }

    if alp_info.factor > alp_info.exponent {
        return Err(general_err!(
            "Invalid ALP page: factor {} exceeds exponent {}",
            alp_info.factor,
            alp_info.exponent
        ));
    }

    if alp_info.num_exceptions > num_elements {
        return Err(general_err!(
            "Invalid ALP page: num_exceptions {} exceeds vector num_elements {}",
            alp_info.num_exceptions,
            num_elements
        ));
    }

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
        let position = u16::from_le_bytes([chunk[0], chunk[1]]);
        if position >= num_elements {
            return Err(general_err!(
                "Invalid ALP page: exception position {} out of bounds for vector length {}",
                position,
                num_elements
            ));
        }
        exception_positions.push(position);
    }

    let packed_values =
        (vector_start + metadata_size + packed_start)..(vector_start + metadata_size + packed_end);

    let mut exception_values = Vec::with_capacity(alp_info.num_exceptions as usize);
    for chunk in data[values_start..values_end].chunks_exact(Exact::WIDTH) {
        exception_values.push(Exact::from_le_slice(chunk));
    }

    Ok(AlpEncodedVectorView {
        num_elements,
        alp_info,
        for_info,
        packed_values,
        exception_positions,
        exception_values,
    })
}

/// Decode bit-packed deltas into exact integers.
fn bit_unpack_integers<Exact: AlpExact + FromBytes>(
    packed_values: &[u8],
    bit_width: u8,
    num_elements: u16,
) -> Result<Vec<Exact>> {
    if bit_width as usize > Exact::WIDTH * 8 {
        return Err(general_err!(
            "Invalid ALP page: bit width {} exceeds {}",
            bit_width,
            Exact::WIDTH * 8
        ));
    }

    if bit_width == 0 {
        return Ok(vec![Exact::zero(); num_elements as usize]);
    }

    let mut out = vec![Exact::zero(); num_elements as usize];
    let mut reader = BitReader::new(Bytes::copy_from_slice(packed_values));
    let read = reader.get_batch::<Exact>(&mut out, bit_width as usize);
    if read != out.len() {
        return Err(general_err!(
            "Invalid ALP page: bit unpack read {} values, expected {}",
            read,
            out.len()
        ));
    }

    Ok(out)
}

/// Apply inverse FOR: `decoded = delta + frame_of_reference`.
fn inverse_for<Exact: AlpExact>(deltas: &mut [Exact], frame_of_reference: Exact) {
    for value in deltas {
        *value = value.wrapping_add(frame_of_reference);
    }
}

/// Decode one vector into output floating values:
/// bit-unpack -> inverse FOR -> decimal decode -> patch exceptions.
fn decode_vector_values<Value: AlpFloat>(
    body: &[u8],
    vector: &AlpEncodedVectorView<Value::Exact>,
) -> Result<Vec<Value>> {
    let mut exact_values = bit_unpack_integers(
        &body[vector.packed_values.clone()],
        vector.for_info.bit_width,
        vector.num_elements,
    )?;
    inverse_for(&mut exact_values, vector.for_info.frame_of_reference);

    let scale = Value::decode_scale(vector.alp_info.exponent, vector.alp_info.factor);

    let mut out = Vec::with_capacity(vector.num_elements as usize);
    for exact_value in exact_values {
        let signed_value = exact_value.reinterpret_as_signed();
        out.push(Value::decode_value(signed_value, scale));
    }

    if vector.exception_positions.len() != vector.exception_values.len() {
        return Err(general_err!(
            "Invalid ALP page: exception positions ({}) and values ({}) length mismatch",
            vector.exception_positions.len(),
            vector.exception_values.len()
        ));
    }

    for (pos, value_bits) in vector
        .exception_positions
        .iter()
        .zip(vector.exception_values.iter())
    {
        let pos = *pos as usize;
        if pos >= out.len() {
            return Err(general_err!(
                "Invalid ALP page: exception position {} out of bounds for vector length {}",
                pos,
                out.len()
            ));
        }
        out[pos] = Value::from_exact_bits(*value_bits);
    }

    Ok(out)
}

fn decode_page_values<Value: AlpFloat>(layout: &AlpPageLayout) -> Result<Vec<Value>> {
    let mut out = Vec::with_capacity(layout.header.num_elements_usize());
    for (vector_idx, vector_offset) in layout.offsets.iter().enumerate() {
        let vector_start = *vector_offset as usize;
        let vector_end = if vector_idx + 1 < layout.offsets.len() {
            layout.offsets[vector_idx + 1] as usize
        } else {
            layout.body.len()
        };
        let vector_num_elements = layout.header.vector_num_elements(vector_idx);
        let vector = parse_vector_view::<Value::Exact>(
            layout.body.as_ref(),
            vector_start,
            vector_end,
            vector_num_elements,
        )?;
        out.extend_from_slice(&decode_vector_values::<Value>(
            layout.body.as_ref(),
            &vector,
        )?);
    }
    Ok(out)
}

/// Decoder for ALP-encoded floating-point pages (`f32`/`f64`).
///
/// Current behavior:
/// - `set_data` parses + validates page metadata and stores ALP layout state.
/// - `get` lazily decodes the full page once, then copies from the decoded buffer.
/// - `skip` advances the decoded cursor.
pub(crate) struct AlpDecoder<T: DataType>
where
    T::T: AlpFloat,
    <T::T as AlpFloat>::Exact: Send,
{
    layout: Option<AlpPageLayout>,
    decoded_values: Vec<T::T>,
    current_offset: usize,
    needs_decode: bool,
    num_values: usize,
    _marker: PhantomData<T>,
}

impl<T: DataType> AlpDecoder<T>
where
    T::T: AlpFloat,
    <T::T as AlpFloat>::Exact: Send,
{
    pub(crate) fn new() -> Self {
        Self {
            layout: None,
            decoded_values: Vec::new(),
            current_offset: 0,
            needs_decode: false,
            num_values: 0,
            _marker: PhantomData,
        }
    }

    fn ensure_decoded(&mut self) -> Result<()> {
        if !self.needs_decode {
            return Ok(());
        }

        let layout = self.layout.take().ok_or_else(|| {
            general_err!("Invalid ALP decoder state: set_data must be called before get/skip")
        })?;

        self.decoded_values = decode_page_values::<T::T>(&layout)?;
        self.needs_decode = false;

        Ok(())
    }
}

impl<T: DataType> Decoder<T> for AlpDecoder<T>
where
    T::T: AlpFloat,
    <T::T as AlpFloat>::Exact: Send,
{
    fn set_data(&mut self, data: Bytes, num_values: usize) -> Result<()> {
        let layout = parse_alp_page_layout::<<T::T as AlpFloat>::Exact>(data)?;

        if layout.header.num_elements_usize() != num_values {
            return Err(general_err!(
                "Invalid ALP page: header num_elements {} does not match page num_values {}",
                layout.header.num_elements,
                num_values
            ));
        }

        self.layout = Some(layout);
        self.decoded_values.clear();
        self.current_offset = 0;
        self.needs_decode = num_values > 0;
        self.num_values = num_values;
        Ok(())
    }

    fn get(&mut self, buffer: &mut [T::T]) -> Result<usize> {
        let target = buffer.len().min(self.num_values);
        if target == 0 {
            return Ok(0);
        }

        self.ensure_decoded()?;
        let end = self.current_offset + target;
        buffer[..target].copy_from_slice(&self.decoded_values[self.current_offset..end]);
        self.current_offset = end;
        self.num_values -= target;
        Ok(target)
    }

    fn values_left(&self) -> usize {
        self.num_values
    }

    fn encoding(&self) -> Encoding {
        Encoding::ALP
    }

    fn skip(&mut self, num_values: usize) -> Result<usize> {
        let to_skip = num_values.min(self.num_values);
        if to_skip == 0 {
            return Ok(0);
        }

        self.ensure_decoded()?;
        self.current_offset += to_skip;
        self.num_values -= to_skip;
        Ok(to_skip)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_type::FloatType;

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

    fn make_vector_u32(
        exponent: u8,
        factor: u8,
        num_exceptions: u16,
        frame_of_reference: u32,
        bit_width: u8,
        packed_values: &[u8],
        exception_positions: &[u16],
        exception_values: &[u32],
    ) -> Vec<u8> {
        assert_eq!(num_exceptions as usize, exception_positions.len());
        assert_eq!(num_exceptions as usize, exception_values.len());

        let mut out = Vec::new();
        out.push(exponent);
        out.push(factor);
        out.extend_from_slice(&num_exceptions.to_le_bytes());
        out.extend_from_slice(&frame_of_reference.to_le_bytes());
        out.push(bit_width);
        out.extend_from_slice(packed_values);
        for position in exception_positions {
            out.extend_from_slice(&position.to_le_bytes());
        }
        for value in exception_values {
            out.extend_from_slice(&value.to_le_bytes());
        }
        out
    }

    fn make_vector_u64(
        exponent: u8,
        factor: u8,
        num_exceptions: u16,
        frame_of_reference: u64,
        bit_width: u8,
        packed_values: &[u8],
        exception_positions: &[u16],
        exception_values: &[u64],
    ) -> Vec<u8> {
        assert_eq!(num_exceptions as usize, exception_positions.len());
        assert_eq!(num_exceptions as usize, exception_values.len());

        let mut out = Vec::new();
        out.push(exponent);
        out.push(factor);
        out.extend_from_slice(&num_exceptions.to_le_bytes());
        out.extend_from_slice(&frame_of_reference.to_le_bytes());
        out.push(bit_width);
        out.extend_from_slice(packed_values);
        for position in exception_positions {
            out.extend_from_slice(&position.to_le_bytes());
        }
        for value in exception_values {
            out.extend_from_slice(&value.to_le_bytes());
        }
        out
    }

    fn make_page_from_vectors(
        log_vector_size: u8,
        num_elements: i32,
        vectors: &[Vec<u8>],
    ) -> Vec<u8> {
        let vector_size = 1usize << log_vector_size;
        let expected_num_vectors = if num_elements <= 0 {
            0
        } else {
            (num_elements as usize).div_ceil(vector_size)
        };
        assert_eq!(vectors.len(), expected_num_vectors);

        let offsets_section_size = vectors.len() * std::mem::size_of::<u32>();
        let mut offsets = Vec::with_capacity(vectors.len());
        let mut running_offset = offsets_section_size as u32;
        for vector in vectors {
            offsets.push(running_offset);
            running_offset += vector.len() as u32;
        }

        let mut page = make_alp_page_bytes(1, 0, 0, log_vector_size, num_elements, &offsets, 0);
        for vector in vectors {
            page.extend_from_slice(vector);
        }
        page
    }

    #[test]
    fn test_parse_alp_page_layout_valid() {
        let data = make_alp_page_bytes(1, 0, 0, 2, 4, &[4], 13);
        let parsed = parse_alp_page_layout::<u64>(Bytes::from(data)).unwrap();
        assert_eq!(parsed.header.version, 1);
        assert_eq!(parsed.header.num_elements, 4);
        assert_eq!(parsed.offsets, vec![4]);
    }

    #[test]
    fn test_parse_alp_page_layout_short_header() {
        let err = parse_alp_page_layout::<u64>(Bytes::from_static(&[0, 1, 2])).unwrap_err();
        assert!(
            err.to_string()
                .contains("Invalid ALP page: expected at least 8 bytes for header")
        );
    }

    #[test]
    fn test_parse_alp_page_layout_invalid_log_vector_size() {
        let data = make_alp_page_bytes(1, 0, 0, 17, 1, &[4], 8);
        let err = parse_alp_page_layout::<u64>(Bytes::from(data)).unwrap_err();
        assert!(
            err.to_string()
                .contains("Invalid ALP page: log_vector_size 17 exceeds max 16")
        );
    }

    #[test]
    fn test_parse_alp_page_layout_invalid_integer_encoding() {
        let data = make_alp_page_bytes(1, 0, 1, 2, 1, &[4], 8);
        let err = parse_alp_page_layout::<u64>(Bytes::from(data)).unwrap_err();
        assert!(
            err.to_string()
                .contains("Invalid ALP page: unsupported integer encoding 1")
        );
    }

    #[test]
    fn test_parse_alp_page_layout_negative_num_elements() {
        let data = make_alp_page_bytes(1, 0, 0, 2, -1, &[4], 8);
        let err = parse_alp_page_layout::<u64>(Bytes::from(data)).unwrap_err();
        assert!(
            err.to_string()
                .contains("Invalid ALP page: num_elements -1 must be >= 0")
        );
    }

    #[test]
    fn test_parse_alp_page_layout_invalid_exponent_f32() {
        let vector = make_vector_u32(11, 0, 0, 0, 0, &[], &[], &[]);
        let page = make_page_from_vectors(0, 1, &[vector]);
        let err = parse_alp_page_layout::<u32>(Bytes::from(page)).unwrap_err();
        assert!(
            err.to_string()
                .contains("Invalid ALP page: exponent 11 exceeds max 10")
        );
    }

    #[test]
    fn test_parse_alp_page_layout_invalid_factor_f32() {
        let vector = make_vector_u32(0, 11, 0, 0, 0, &[], &[], &[]);
        let page = make_page_from_vectors(0, 1, &[vector]);
        let err = parse_alp_page_layout::<u32>(Bytes::from(page)).unwrap_err();
        assert!(
            err.to_string()
                .contains("Invalid ALP page: factor 11 exceeds exponent 0")
        );
    }

    #[test]
    fn test_parse_alp_page_layout_factor_exceeds_exponent() {
        let vector = make_vector_u32(2, 3, 0, 0, 0, &[], &[], &[]);
        let page = make_page_from_vectors(0, 1, &[vector]);
        let err = parse_alp_page_layout::<u32>(Bytes::from(page)).unwrap_err();
        assert!(
            err.to_string()
                .contains("Invalid ALP page: factor 3 exceeds exponent 2")
        );
    }

    #[test]
    fn test_parse_alp_page_layout_invalid_num_exceptions() {
        let vector = make_vector_u32(0, 0, 2, 0, 0, &[], &[0, 0], &[0, 0]);
        let page = make_page_from_vectors(0, 1, &[vector]);
        let err = parse_alp_page_layout::<u32>(Bytes::from(page)).unwrap_err();
        assert!(
            err.to_string()
                .contains("Invalid ALP page: num_exceptions 2 exceeds vector num_elements 1")
        );
    }

    #[test]
    fn test_parse_alp_page_layout_invalid_exception_position() {
        let vector = make_vector_u32(0, 0, 1, 0, 0, &[], &[1], &[123]);
        let page = make_page_from_vectors(0, 1, &[vector]);
        let err = parse_alp_page_layout::<u32>(Bytes::from(page)).unwrap_err();
        assert!(
            err.to_string().contains(
                "Invalid ALP page: exception position 1 out of bounds for vector length 1"
            )
        );
    }

    #[test]
    fn test_parse_alp_page_layout_non_monotonic_offsets() {
        let data = make_alp_page_bytes(1, 0, 0, 1, 3, &[12, 8], 12);
        let err = parse_alp_page_layout::<u64>(Bytes::from(data)).unwrap_err();
        assert!(
            err.to_string()
                .contains("Invalid ALP page: vector offsets are not monotonic at index 0")
        );
    }

    #[test]
    fn test_parse_alp_page_layout_truncated_vector_data() {
        let mut vector = Vec::new();
        vector.push(0);
        vector.push(0);
        vector.extend_from_slice(&0u16.to_le_bytes());
        vector.extend_from_slice(&10u32.to_le_bytes());
        vector.push(1);
        let page = make_page_from_vectors(1, 2, &[vector]);
        let err = parse_alp_page_layout::<u32>(Bytes::from(page)).unwrap_err();
        assert!(
            err.to_string()
                .contains("Invalid ALP page: vector data too short")
        );
    }

    #[test]
    fn test_parse_alp_page_layout_parses_vector_view_data_only_f64() {
        let mut vector = Vec::new();

        vector.push(2);
        vector.push(0);
        vector.extend_from_slice(&1u16.to_le_bytes());

        vector.extend_from_slice(&10u64.to_le_bytes());
        vector.push(0);

        vector.extend_from_slice(&0u16.to_le_bytes());
        vector.extend_from_slice(&42.5_f64.to_le_bytes());

        let offsets = [4u32];
        let mut page = make_alp_page_bytes(1, 0, 0, 0, 1, &offsets, 0);
        page.extend_from_slice(&vector);

        let parsed = parse_alp_page_layout::<u64>(Bytes::from(page)).unwrap();
        assert_eq!(parsed.offsets, vec![4]);
        let vector_start = parsed.offsets[0] as usize;
        let vector_end = parsed.body.len();
        let parsed_vector = parse_vector_view::<u64>(
            parsed.body.as_ref(),
            vector_start,
            vector_end,
            parsed.header.vector_num_elements(0),
        )
        .unwrap();
        assert_eq!(parsed_vector.num_elements, 1);
        assert_eq!(parsed_vector.alp_info.num_exceptions, 1);
        assert_eq!(parsed_vector.for_info.bit_width, 0);
        assert_eq!(parsed_vector.exception_positions, vec![0]);
        assert_eq!(parsed_vector.exception_values, vec![42.5_f64.to_bits()]);
    }

    #[test]
    fn test_bit_unpack_integers_width_zero() {
        let unpacked = bit_unpack_integers::<u32>(&[], 0, 3).unwrap();
        assert_eq!(unpacked, vec![0, 0, 0]);
    }

    #[test]
    fn test_bit_unpack_integers_width_two() {
        let unpacked = bit_unpack_integers::<u32>(&[0b0010_0111], 2, 3).unwrap();
        assert_eq!(unpacked, vec![3, 1, 2]);
    }

    #[test]
    fn test_inverse_for() {
        let mut decoded = vec![0u32, 3, 2];
        inverse_for(&mut decoded, 10);
        assert_eq!(decoded, vec![10, 13, 12]);
    }

    #[test]
    fn test_decode_page_values_f32_no_exceptions() {
        let vector = make_vector_u32(0, 0, 0, 10, 2, &[0b1110_0100], &[], &[]);
        let page = make_page_from_vectors(2, 4, &[vector]);
        let layout = parse_alp_page_layout::<u32>(Bytes::from(page)).unwrap();
        let decoded = decode_page_values::<f32>(&layout).unwrap();
        assert_eq!(decoded, vec![10.0, 11.0, 12.0, 13.0]);
    }

    #[test]
    fn test_decode_page_values_f64_multi_vector_with_exceptions() {
        let vector0 = make_vector_u64(0, 0, 1, 10, 1, &[0b0000_0010], &[1], &[42.5f64.to_bits()]);
        let vector1 = make_vector_u64(0, 0, 0, 7, 0, &[], &[], &[]);
        let page = make_page_from_vectors(1, 3, &[vector0, vector1]);
        let layout = parse_alp_page_layout::<u64>(Bytes::from(page)).unwrap();
        let decoded = decode_page_values::<f64>(&layout).unwrap();
        assert_eq!(decoded, vec![10.0, 42.5, 7.0]);
    }

    #[test]
    fn test_decode_page_values_f32_edge_values_via_exceptions() {
        let edge_values = [
            f32::NAN.to_bits(),
            (-0.0f32).to_bits(),
            f32::INFINITY.to_bits(),
        ];
        let vector = make_vector_u32(0, 0, 3, 0, 0, &[], &[0, 1, 2], &edge_values);
        let page = make_page_from_vectors(2, 3, &[vector]);
        let layout = parse_alp_page_layout::<u32>(Bytes::from(page)).unwrap();
        let decoded = decode_page_values::<f32>(&layout).unwrap();

        assert!(decoded[0].is_nan());
        assert_eq!(decoded[1], -0.0);
        assert!(decoded[1].is_sign_negative());
        assert_eq!(decoded[2], f32::INFINITY);
    }

    #[test]
    fn test_alp_decoder_get_across_vectors() {
        let vector0 = make_vector_u32(0, 0, 0, 10, 1, &[0b0000_0010], &[], &[]);
        let vector1 = make_vector_u32(0, 0, 0, 20, 1, &[0b0000_0010], &[], &[]);
        let page = make_page_from_vectors(1, 4, &[vector0, vector1]);

        let mut decoder = AlpDecoder::<FloatType>::new();
        decoder.set_data(Bytes::from(page), 4).unwrap();

        let mut first = [0.0f32; 3];
        let read = decoder.get(&mut first).unwrap();
        assert_eq!(read, 3);
        assert_eq!(first, [10.0, 11.0, 20.0]);
        assert_eq!(decoder.values_left(), 1);

        let mut second = [0.0f32; 2];
        let read = decoder.get(&mut second).unwrap();
        assert_eq!(read, 1);
        assert_eq!(second[0], 21.0);
        assert_eq!(decoder.values_left(), 0);
    }

    #[test]
    fn test_alp_decoder_skip_across_vectors() {
        let vector0 = make_vector_u32(0, 0, 0, 10, 1, &[0b0000_0010], &[], &[]);
        let vector1 = make_vector_u32(0, 0, 0, 20, 1, &[0b0000_0010], &[], &[]);
        let page = make_page_from_vectors(1, 4, &[vector0, vector1]);

        let mut decoder = AlpDecoder::<FloatType>::new();
        decoder.set_data(Bytes::from(page), 4).unwrap();

        let skipped = decoder.skip(3).unwrap();
        assert_eq!(skipped, 3);
        assert_eq!(decoder.values_left(), 1);

        let mut out = [0.0f32; 1];
        let read = decoder.get(&mut out).unwrap();
        assert_eq!(read, 1);
        assert_eq!(out[0], 21.0);
        assert_eq!(decoder.values_left(), 0);
    }
}
