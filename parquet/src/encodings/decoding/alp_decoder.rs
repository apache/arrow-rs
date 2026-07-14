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

use std::ops::Range;

use bytes::Bytes;

use crate::basic::Encoding;
use crate::data_type::DataType;
use crate::encodings::alp::{
    ALP_COMPRESSION_MODE, ALP_DEFAULT_LOG_VECTOR_SIZE, ALP_HEADER_SIZE,
    ALP_INTEGER_ENCODING_FOR_BIT_PACK, ALP_MAX_EXPONENT_F32, ALP_MAX_EXPONENT_F64,
    ALP_MAX_LOG_VECTOR_SIZE, ALP_MIN_LOG_VECTOR_SIZE, AlpExact, AlpFloat, AlpHeader, AlpInfo,
    ForInfo,
};
use crate::encodings::decoding::Decoder;
use crate::errors::{ParquetError, Result};
use crate::util::bit_util::BitReader;

/// Parsed view of one vector's metadata and data sections.
///
/// Each data section is described by its start offset into the page body; the
/// section bytes themselves stay in the body and are decoded lazily when the
/// vector is decoded. Section lengths are fully determined by the fixed-size
/// metadata at the front of the vector (`bit_width` for `packed_values`,
/// `num_exceptions` for both exception sections), so only the start offset is
/// stored.
#[derive(Debug, Clone, Copy)]
struct AlpEncodedVectorView<Exact: AlpExact> {
    num_elements: u16,
    alp_info: AlpInfo,
    for_info: ForInfo<Exact>,
    packed_values: usize,
    exception_positions: usize,
    exception_values: usize,
}

impl<Exact: AlpExact> AlpEncodedVectorView<Exact> {
    fn expected_stored_size(&self) -> usize {
        AlpInfo::STORED_SIZE
            + ForInfo::<Exact>::stored_size()
            + self
                .for_info
                .get_data_stored_size(self.num_elements, self.alp_info.num_exceptions)
    }

    /// Byte range of the bit-packed values section in the page body.
    fn packed_values_range(&self) -> Range<usize> {
        let len = self.for_info.get_bit_packed_size(self.num_elements);
        self.packed_values..self.packed_values + len
    }

    /// Byte range of the exception positions section (`u16` each) in the page body.
    fn exception_positions_range(&self) -> Range<usize> {
        let len = self.alp_info.num_exceptions as usize * std::mem::size_of::<u16>();
        self.exception_positions..self.exception_positions + len
    }

    /// Byte range of the exception values section (`Exact::WIDTH` each) in the page body.
    fn exception_values_range(&self) -> Range<usize> {
        let len = self.alp_info.num_exceptions as usize * Exact::WIDTH;
        self.exception_values..self.exception_values + len
    }
}

/// Parse and validate the 7-byte ALP page header: compression mode, integer
/// encoding, and vector-size range.
fn parse_alp_page_header(data: &[u8]) -> Result<AlpHeader> {
    let header = AlpHeader::deserialize(data)?;

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
    if header.vector_size < (1usize << ALP_MIN_LOG_VECTOR_SIZE) {
        return Err(general_err!(
            "Invalid ALP page: log_vector_size {} below min {}",
            header.vector_size.trailing_zeros(),
            ALP_MIN_LOG_VECTOR_SIZE
        ));
    }
    if header.vector_size > (1usize << ALP_MAX_LOG_VECTOR_SIZE) {
        return Err(general_err!(
            "Invalid ALP page: log_vector_size {} exceeds max {}",
            header.vector_size.trailing_zeros(),
            ALP_MAX_LOG_VECTOR_SIZE
        ));
    }

    Ok(header)
}

/// Read the little-endian `u32` vector offset at index `idx` from the offsets
/// section at the start of the page body.
fn read_offset(body: &[u8], idx: usize) -> Result<usize> {
    let start = idx * std::mem::size_of::<u32>();
    let bytes = body
        .get(start..start + std::mem::size_of::<u32>())
        .ok_or_else(|| general_err!("Invalid ALP page: offset index {} out of bounds", idx))?;
    Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize)
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

    let metadata_size = AlpInfo::STORED_SIZE + ForInfo::<Exact>::stored_size();
    if vector_bytes.len() < metadata_size {
        return Err(general_err!(
            "Invalid ALP page: vector metadata too short, expected at least {} bytes, got {}",
            metadata_size,
            vector_bytes.len()
        ));
    }

    let alp_info = AlpInfo {
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

    let for_start = AlpInfo::STORED_SIZE;
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

    let for_info = ForInfo::<Exact> {
        frame_of_reference,
        bit_width,
    };

    let data_size = for_info.get_data_stored_size(num_elements, alp_info.num_exceptions);
    let expected_size = metadata_size + data_size;
    if vector_bytes.len() < expected_size {
        return Err(general_err!(
            "Invalid ALP page: vector data too short, expected at least {} bytes, got {}",
            expected_size,
            vector_bytes.len()
        ));
    }
    if vector_bytes.len() > expected_size {
        return Err(general_err!(
            "Invalid ALP page: vector data too long, expected {} bytes, got {}",
            expected_size,
            vector_bytes.len()
        ));
    }

    let data = &vector_bytes[metadata_size..expected_size];
    let packed_size = for_info.get_bit_packed_size(num_elements);
    let positions_size = alp_info.num_exceptions as usize * std::mem::size_of::<u16>();

    // Section offsets relative to the start of the data section: packed values
    // first, then exception positions, then exception values.
    let positions_start = packed_size;
    let values_start = positions_start + positions_size;

    // Validate exception positions without materializing them. They are decoded
    // straight from the body when the vector is decoded; here we only enforce
    // that every position is in range so the whole page is validated up front.
    for chunk in data[positions_start..values_start].chunks_exact(2) {
        let position = u16::from_le_bytes([chunk[0], chunk[1]]);
        if position >= num_elements {
            return Err(general_err!(
                "Invalid ALP page: exception position {} out of bounds for vector length {}",
                position,
                num_elements
            ));
        }
    }

    // Store each section's start offset into the page body. Lengths are derived
    // from the vector metadata at decode time, so no end offset is stored.
    let data_start = vector_start + metadata_size;
    let packed_values = data_start;
    let exception_positions = data_start + positions_start;
    let exception_values = data_start + values_start;

    Ok(AlpEncodedVectorView {
        num_elements,
        alp_info,
        for_info,
        packed_values,
        exception_positions,
        exception_values,
    })
}

/// Live decode state for the one vector currently being consumed.
///
/// Holds the bit position inside that vector's packed values plus the
/// vector-level constants needed to turn each packed integer back into a float.
/// `delivered` is the vector-local index of the next element to produce, so
/// exception patches (which use vector-local positions) land in the right place
/// even when a vector is split across several `get`/`skip` calls.
struct CurrentVector<Value: AlpFloat> {
    reader: BitReader,
    bit_width: u8,
    frame_of_reference: Value::Exact,
    scale: Value::Scale,
    /// Number of this vector's elements not yet delivered or skipped.
    remaining: usize,
    /// Vector-local index of the next element to produce.
    delivered: usize,
    exception_positions: Bytes,
    exception_values: Bytes,
}

/// Largest slice decoded in one unpack-then-decode pass: the canonical ALP
/// vector size - 1024.
///
/// The unpack scratch is sized to `min(vector_size, this)`, so vectors at the
/// default size or smaller are decoded whole, while larger (non-default) vectors
/// are decoded in canonical-vector-sized tiles.
///
/// Bounding the tile to one canonical vector keeps the scratch L1-resident, which
/// is what makes the staged unpack-then-decode beat an in-place decode.
const DECODE_TILE_CAP: usize = 1 << ALP_DEFAULT_LOG_VECTOR_SIZE;

/// Decode the next `out.len()` elements of the current vector into `out`,
/// patching any exceptions whose vector-local position falls in the
/// just-produced sub-range.
///
/// Deltas are bulk-unpacked a tile at a time into the caller-provided `scratch`
/// via `get_batch` (which dispatches to the SIMD-friendly fixed-width `unpack`
/// kernels), then the inverse FOR and decimal decode run as one branchless,
/// state-free loop over that contiguous tile so the compiler can autovectorize
/// it.
fn decode_range<Value: AlpFloat>(
    cur: &mut CurrentVector<Value>,
    scratch: &mut [Value::Exact],
    out: &mut [Value],
) -> Result<()> {
    let frame_of_reference = cur.frame_of_reference;
    if cur.bit_width == 0 {
        // Every packed delta is zero, so all values share `frame_of_reference`.
        let signed = frame_of_reference.reinterpret_as_signed();
        out.fill(Value::decode_value(signed, cur.scale));
    } else {
        let bit_width = cur.bit_width as usize;
        let scale = cur.scale;
        for chunk in out.chunks_mut(scratch.len()) {
            let deltas = &mut scratch[..chunk.len()];
            let unpacked = cur.reader.get_batch::<Value::Exact>(deltas, bit_width);
            if unpacked != chunk.len() {
                return Err(general_err!(
                    "Invalid ALP page: not enough packed bits to decode vector"
                ));
            }
            for (slot, &delta) in chunk.iter_mut().zip(deltas.iter()) {
                let signed = delta
                    .wrapping_add(frame_of_reference)
                    .reinterpret_as_signed();
                *slot = Value::decode_value(signed, scale);
            }
        }
    }

    // Patch exceptions landing in `[delivered, delivered + out.len())`. Positions
    // were validated in bounds when the vector was parsed, and patching is a
    // positional overwrite, so it is independent of exception ordering.
    let lo = cur.delivered;
    let hi = cur.delivered + out.len();
    for (pos_chunk, value_chunk) in cur
        .exception_positions
        .chunks_exact(std::mem::size_of::<u16>())
        .zip(cur.exception_values.chunks_exact(Value::Exact::WIDTH))
    {
        let pos = u16::from_le_bytes([pos_chunk[0], pos_chunk[1]]) as usize;
        if (lo..hi).contains(&pos) {
            out[pos - lo] = Value::from_exact_bits(Value::Exact::from_le_slice(value_chunk));
        }
    }

    cur.delivered = hi;
    cur.remaining -= out.len();
    Ok(())
}

/// Decoder for ALP-encoded floating-point pages (`f32`/`f64`).
///
/// Values are decoded directly into the caller's output buffer, one vector at a
/// time: `set_data` validates the page header, and each vector is parsed and
/// decoded on demand as the read cursor reaches it.
pub(crate) struct AlpDecoder<T: DataType>
where
    T::T: AlpFloat,
    <T::T as AlpFloat>::Exact: Send,
{
    /// Validated page header; drives vector sizing and count.
    header: AlpHeader,
    /// Page body following the 7-byte header: `[offsets][vector data...]`.
    body: Bytes,
    /// Index of the next vector to parse once `current` is exhausted.
    next_vector_idx: usize,
    /// Body offset where the next vector must begin; offsets are validated to be
    /// contiguous as vectors are parsed.
    expected_next_offset: usize,
    /// Live decode state for the vector currently being consumed.
    current: Option<CurrentVector<T::T>>,
    /// Reused unpack buffer, sized once per page to `min(vector_size, cap)` and
    /// shared across all vectors; holds bulk-unpacked deltas for one tile.
    scratch: Vec<<T::T as AlpFloat>::Exact>,
    /// Number of values still to be read from the page.
    num_values: usize,
}

impl<T: DataType> AlpDecoder<T>
where
    T::T: AlpFloat,
    <T::T as AlpFloat>::Exact: Send,
{
    pub(crate) fn new() -> Self {
        Self {
            // Benign placeholder header; replaced on the first `set_data`. The
            // cursor never consults it while `num_values == 0`.
            header: AlpHeader {
                compression_mode: 0,
                integer_encoding: 0,
                vector_size: 1,
                num_elements: 0,
            },
            body: Bytes::new(),
            next_vector_idx: 0,
            expected_next_offset: 0,
            current: None,
            scratch: Vec::new(),
            num_values: 0,
        }
    }

    /// Parse the next vector on demand and install it as `current`: validate its
    /// offset is contiguous with the previous vector, parse its metadata, build a
    /// live bit reader over its packed values, and slice out its exception
    /// sections.
    fn load_current_vector(&mut self) -> Result<()> {
        let idx = self.next_vector_idx;
        let num_vectors = self.header.num_vectors();
        debug_assert!(idx < num_vectors, "load_current_vector with no vector left");

        let body_ref = self.body.as_ref();
        let start = read_offset(body_ref, idx)?;
        if start != self.expected_next_offset {
            return Err(general_err!(
                "Invalid ALP page: vector offset {} at index {} does not match expected {}",
                start,
                idx,
                self.expected_next_offset
            ));
        }
        let end = if idx + 1 < num_vectors {
            read_offset(body_ref, idx + 1)?
        } else {
            body_ref.len()
        };
        if end < start || end > body_ref.len() {
            return Err(general_err!(
                "Invalid ALP page: vector offset {} out of bounds at index {}",
                end,
                idx
            ));
        }

        let count = self.header.vector_num_elements(idx);
        let view = parse_vector_view::<<T::T as AlpFloat>::Exact>(body_ref, start, end, count)?;

        // The next vector must start exactly where this one ends.
        self.expected_next_offset = start + view.expected_stored_size();

        let packed = self.body.slice(view.packed_values_range());
        let exception_positions = self.body.slice(view.exception_positions_range());
        let exception_values = self.body.slice(view.exception_values_range());
        self.current = Some(CurrentVector {
            reader: BitReader::new(packed),
            bit_width: view.for_info.bit_width,
            frame_of_reference: view.for_info.frame_of_reference,
            scale: <T::T as AlpFloat>::decode_scale(view.alp_info.exponent, view.alp_info.factor),
            remaining: count as usize,
            delivered: 0,
            exception_positions,
            exception_values,
        });
        self.next_vector_idx += 1;
        Ok(())
    }
}

impl<T: DataType> Decoder<T> for AlpDecoder<T>
where
    T::T: AlpFloat,
    <T::T as AlpFloat>::Exact: Send,
{
    /// Validate the page header and reset the read cursor. Vectors are parsed
    /// and decoded lazily, on the first `get`/`skip`.
    fn set_data(&mut self, data: Bytes, num_values: usize) -> Result<()> {
        let header = parse_alp_page_header(data.as_ref())?;

        // `num_values` is an upper bound, not an exact count: only non-null values
        // are encoded, and the caller passes the level count when the value count
        // is not known up front. The header carries the authoritative count.
        if header.num_elements > num_values {
            return Err(general_err!(
                "Invalid ALP page: header num_elements {} exceeds page num_values {}",
                header.num_elements,
                num_values
            ));
        }
        let num_values = header.num_elements;

        let offsets_section_size = header
            .num_vectors()
            .checked_mul(std::mem::size_of::<u32>())
            .ok_or_else(|| general_err!("Invalid ALP page: offsets length overflow"))?;

        // `parse_alp_page_header` guarantees `data.len() >= ALP_HEADER_SIZE`.
        let body = data.slice(ALP_HEADER_SIZE..);
        if body.len() < offsets_section_size {
            return Err(general_err!(
                "Invalid ALP page: expected at least {} bytes for {} offsets, got {}",
                offsets_section_size,
                header.num_vectors(),
                body.len()
            ));
        }

        // Size the reused unpack buffer to one vector, capped to stay L1-resident
        // and to the page's value count so tiny pages don't over-allocate. The
        // `.max(1)` keeps a non-empty tile so the `chunks_mut` decode never hits a
        // zero stride (an empty page returns before any decode anyway).
        let tile = header
            .vector_size
            .min(DECODE_TILE_CAP)
            .min(num_values)
            .max(1);
        self.scratch.clear();
        self.scratch.resize(tile, Default::default());

        self.header = header;
        self.body = body;
        self.next_vector_idx = 0;
        self.expected_next_offset = offsets_section_size;
        self.current = None;
        self.num_values = num_values;
        Ok(())
    }

    /// Read up to `buffer.len()` values, decoding each vector on demand directly
    /// into `buffer`.
    fn get(&mut self, buffer: &mut [T::T]) -> Result<usize> {
        let target = buffer.len().min(self.num_values);
        if target == 0 {
            return Ok(0);
        }

        let mut written = 0;
        while written < target {
            if self.current.as_ref().is_none_or(|c| c.remaining == 0) {
                self.load_current_vector()?;
            }
            let cur = self.current.as_mut().unwrap();
            let n = cur.remaining.min(target - written);
            decode_range::<T::T>(cur, &mut self.scratch, &mut buffer[written..written + n])?;
            written += n;
            self.num_values -= n;
        }
        Ok(target)
    }

    fn values_left(&self) -> usize {
        self.num_values
    }

    fn encoding(&self) -> Encoding {
        Encoding::ALP
    }

    /// Skip up to `num_values` values, advancing the per-vector cursor (and the
    /// underlying bit reader) without decoding.
    fn skip(&mut self, num_values: usize) -> Result<usize> {
        let to_skip = num_values.min(self.num_values);
        if to_skip == 0 {
            return Ok(0);
        }

        let mut skipped = 0;
        while skipped < to_skip {
            if self.current.as_ref().is_none_or(|c| c.remaining == 0) {
                self.load_current_vector()?;
            }
            let cur = self.current.as_mut().unwrap();
            let n = cur.remaining.min(to_skip - skipped);
            if cur.bit_width != 0 {
                cur.reader.skip(n, cur.bit_width as usize);
            }
            cur.delivered += n;
            cur.remaining -= n;
            skipped += n;
            self.num_values -= n;
        }
        Ok(to_skip)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_type::{DoubleType, FloatType};
    use crate::encodings::alp::{
        ALP_NEG_POW10_F32, ALP_NEG_POW10_F64, ALP_POW10_F32, ALP_POW10_F64,
    };

    fn make_alp_page_bytes(
        compression_mode: u8,
        integer_encoding: u8,
        log_vector_size: u8,
        num_elements: i32,
        offsets: &[u32],
        body_tail_len: usize,
    ) -> Vec<u8> {
        let mut out = Vec::with_capacity(ALP_HEADER_SIZE + offsets.len() * 4 + body_tail_len);
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

    trait AppendLeBytes {
        fn append_le_bytes(self, out: &mut Vec<u8>);
    }

    impl AppendLeBytes for u32 {
        fn append_le_bytes(self, out: &mut Vec<u8>) {
            out.extend_from_slice(&self.to_le_bytes());
        }
    }

    impl AppendLeBytes for u64 {
        fn append_le_bytes(self, out: &mut Vec<u8>) {
            out.extend_from_slice(&self.to_le_bytes());
        }
    }

    struct VectorSpec<'a, Exact> {
        exponent: u8,
        factor: u8,
        frame_of_reference: Exact,
        bit_width: u8,
        packed_values: &'a [u8],
        exception_positions: &'a [u16],
        exception_values: &'a [Exact],
    }

    fn make_vector<Exact: AppendLeBytes + Copy>(spec: VectorSpec<'_, Exact>) -> Vec<u8> {
        let num_exceptions = spec.exception_positions.len();
        assert_eq!(num_exceptions, spec.exception_values.len());
        assert!(u16::try_from(num_exceptions).is_ok());

        let mut out = Vec::new();
        out.push(spec.exponent);
        out.push(spec.factor);
        out.extend_from_slice(&(num_exceptions as u16).to_le_bytes());
        spec.frame_of_reference.append_le_bytes(&mut out);
        out.push(spec.bit_width);
        out.extend_from_slice(spec.packed_values);
        for position in spec.exception_positions {
            out.extend_from_slice(&position.to_le_bytes());
        }
        for value in spec.exception_values {
            value.append_le_bytes(&mut out);
        }
        out
    }

    /// Decode a full page through the streaming decoder.
    fn decode_page<T: DataType>(page: Vec<u8>, num_values: usize) -> Vec<T::T>
    where
        T::T: AlpFloat,
        <T::T as AlpFloat>::Exact: Send,
    {
        let mut decoder = AlpDecoder::<T>::new();
        decoder.set_data(Bytes::from(page), num_values).unwrap();
        let mut out = vec![T::T::default(); num_values];
        assert_eq!(decoder.get(&mut out).unwrap(), num_values);
        out
    }

    /// Drive the streaming decoder to its first error. Header errors surface
    /// from `set_data`; per-vector and offset errors surface from `get`.
    fn decode_err<T: DataType>(page: Vec<u8>, num_values: usize) -> ParquetError
    where
        T::T: AlpFloat,
        <T::T as AlpFloat>::Exact: Send,
    {
        let mut decoder = AlpDecoder::<T>::new();
        match decoder.set_data(Bytes::from(page), num_values) {
            Err(e) => e,
            Ok(()) => {
                let mut out = vec![T::T::default(); num_values];
                decoder.get(&mut out).unwrap_err()
            }
        }
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

        let mut page = make_alp_page_bytes(0, 0, log_vector_size, num_elements, &offsets, 0);
        for vector in vectors {
            page.extend_from_slice(vector);
        }
        page
    }

    #[test]
    fn test_alp_header_serialize_deserialize_round_trip() {
        let header = AlpHeader {
            compression_mode: ALP_COMPRESSION_MODE,
            integer_encoding: ALP_INTEGER_ENCODING_FOR_BIT_PACK,
            vector_size: 8,
            num_elements: 1234,
        };
        let bytes = header.serialize().unwrap();
        let parsed = AlpHeader::deserialize(&bytes).unwrap();
        assert_eq!(parsed.compression_mode, header.compression_mode);
        assert_eq!(parsed.integer_encoding, header.integer_encoding);
        assert_eq!(parsed.vector_size, header.vector_size);
        assert_eq!(parsed.num_elements, header.num_elements);
    }

    #[test]
    fn test_alp_header_serialize_rejects_overflow() {
        let header = AlpHeader {
            compression_mode: 0,
            integer_encoding: 0,
            vector_size: 8,
            num_elements: i32::MAX as usize + 1,
        };
        let err = header.serialize().unwrap_err();
        assert!(err.to_string().contains("exceeds i32::MAX"));
    }

    #[test]
    fn test_alp_header_serialize_rejects_non_power_of_two_vector_size() {
        let header = AlpHeader {
            compression_mode: 0,
            integer_encoding: 0,
            vector_size: 100,
            num_elements: 4,
        };
        let err = header.serialize().unwrap_err();
        assert!(err.to_string().contains("is not a power of two"));
    }

    #[test]
    fn test_alp_header_deserialize_short_header() {
        let err = AlpHeader::deserialize(&[0, 1, 2]).unwrap_err();
        assert!(
            err.to_string()
                .contains("Invalid ALP page: expected at least 7 bytes for header")
        );
    }

    #[test]
    fn test_alp_header_deserialize_negative_num_elements() {
        let mut bytes = [0u8; ALP_HEADER_SIZE];
        bytes[2] = ALP_MIN_LOG_VECTOR_SIZE;
        bytes[3..7].copy_from_slice(&(-1i32).to_le_bytes());
        let err = AlpHeader::deserialize(&bytes).unwrap_err();
        assert!(
            err.to_string()
                .contains("Invalid ALP page: num_elements -1 must be >= 0")
        );
    }

    #[test]
    fn test_alp_header_deserialize_log_vector_size_overflow() {
        let mut bytes = [0u8; ALP_HEADER_SIZE];
        bytes[2] = 64; // 1 << 64 overflows usize
        let err = AlpHeader::deserialize(&bytes).unwrap_err();
        assert!(
            err.to_string()
                .contains("too large to represent a vector size")
        );
    }

    #[test]
    fn test_decode_valid_page() {
        let data = make_alp_page_bytes(0, 0, 3, 4, &[4], 13);
        let decoded = decode_page::<DoubleType>(data, 4);
        assert_eq!(decoded, vec![0.0_f64; 4]);
    }

    #[test]
    fn test_set_data_rejects_small_vector_size() {
        let data = make_alp_page_bytes(0, 0, 2, 1, &[4], 8);
        let err = decode_err::<DoubleType>(data, 1);
        assert!(
            err.to_string()
                .contains("Invalid ALP page: log_vector_size 2 below min 3")
        );
    }

    #[test]
    fn test_set_data_rejects_big_vector_size() {
        let data = make_alp_page_bytes(0, 0, 16, 1, &[4], 8);
        let err = decode_err::<DoubleType>(data, 1);
        assert!(
            err.to_string()
                .contains("Invalid ALP page: log_vector_size 16 exceeds max 15")
        );
    }

    #[test]
    fn test_set_data_rejects_integer_encoding() {
        let data = make_alp_page_bytes(0, 1, 3, 1, &[4], 8);
        let err = decode_err::<DoubleType>(data, 1);
        assert!(
            err.to_string()
                .contains("Invalid ALP page: unsupported integer encoding 1")
        );
    }

    #[test]
    fn test_decode_rejects_invalid_exponent_f32() {
        let vector = make_vector(VectorSpec {
            exponent: 11,
            factor: 0,
            frame_of_reference: 0u32,
            bit_width: 0,
            packed_values: &[],
            exception_positions: &[],
            exception_values: &[],
        });
        let page = make_page_from_vectors(3, 1, &[vector]);
        let err = decode_err::<FloatType>(page, 1);
        assert!(
            err.to_string()
                .contains("Invalid ALP page: exponent 11 exceeds max 10")
        );
    }

    #[test]
    fn test_decode_rejects_invalid_factor_f32() {
        let vector = make_vector(VectorSpec {
            exponent: 0,
            factor: 11,
            frame_of_reference: 0u32,
            bit_width: 0,
            packed_values: &[],
            exception_positions: &[],
            exception_values: &[],
        });
        let page = make_page_from_vectors(3, 1, &[vector]);
        let err = decode_err::<FloatType>(page, 1);
        assert!(
            err.to_string()
                .contains("Invalid ALP page: factor 11 exceeds exponent 0")
        );
    }

    #[test]
    fn test_decode_rejects_factor_exceeds_exponent() {
        let vector = make_vector(VectorSpec {
            exponent: 2,
            factor: 3,
            frame_of_reference: 0u32,
            bit_width: 0,
            packed_values: &[],
            exception_positions: &[],
            exception_values: &[],
        });
        let page = make_page_from_vectors(3, 1, &[vector]);
        let err = decode_err::<FloatType>(page, 1);
        assert!(
            err.to_string()
                .contains("Invalid ALP page: factor 3 exceeds exponent 2")
        );
    }

    #[test]
    fn test_decode_rejects_invalid_num_exceptions() {
        let vector = make_vector(VectorSpec {
            exponent: 0,
            factor: 0,
            frame_of_reference: 0u32,
            bit_width: 0,
            packed_values: &[],
            exception_positions: &[0, 0],
            exception_values: &[0, 0],
        });
        let page = make_page_from_vectors(3, 1, &[vector]);
        let err = decode_err::<FloatType>(page, 1);
        assert!(
            err.to_string()
                .contains("Invalid ALP page: num_exceptions 2 exceeds vector num_elements 1")
        );
    }

    #[test]
    fn test_decode_rejects_invalid_exception_position() {
        let vector = make_vector(VectorSpec {
            exponent: 0,
            factor: 0,
            frame_of_reference: 0u32,
            bit_width: 0,
            packed_values: &[],
            exception_positions: &[1],
            exception_values: &[123],
        });
        let page = make_page_from_vectors(3, 1, &[vector]);
        let err = decode_err::<FloatType>(page, 1);
        assert!(
            err.to_string().contains(
                "Invalid ALP page: exception position 1 out of bounds for vector length 1"
            )
        );
    }

    #[test]
    fn test_decode_rejects_non_contiguous_offset() {
        let data = make_alp_page_bytes(0, 0, 3, 9, &[12, 8], 12);
        let err = decode_err::<DoubleType>(data, 9);
        assert!(
            err.to_string().contains(
                "Invalid ALP page: vector offset 12 at index 0 does not match expected 8"
            )
        );
    }

    #[test]
    fn test_decode_rejects_truncated_vector() {
        let mut vector = Vec::new();
        vector.push(0);
        vector.push(0);
        vector.extend_from_slice(&0u16.to_le_bytes());
        vector.extend_from_slice(&10u32.to_le_bytes());
        vector.push(1);
        let page = make_page_from_vectors(3, 2, &[vector]);
        let err = decode_err::<FloatType>(page, 2);
        assert!(
            err.to_string()
                .contains("Invalid ALP page: vector data too short")
        );
    }

    #[test]
    fn test_decode_rejects_vector_too_long() {
        let mut vector = make_vector(VectorSpec {
            exponent: 0,
            factor: 0,
            frame_of_reference: 0u32,
            bit_width: 0,
            packed_values: &[],
            exception_positions: &[],
            exception_values: &[],
        });
        vector.push(0xAB);

        let page = make_page_from_vectors(3, 1, &[vector]);
        let err = decode_err::<FloatType>(page, 1);
        assert!(
            err.to_string()
                .contains("Invalid ALP page: vector data too long, expected 9 bytes, got 10")
        );
    }

    #[test]
    fn test_decode_rejects_unclaimed_bytes() {
        let vector = make_vector(VectorSpec {
            exponent: 0,
            factor: 0,
            frame_of_reference: 0u32,
            bit_width: 0,
            packed_values: &[],
            exception_positions: &[],
            exception_values: &[],
        });

        // offsets section is 4 bytes for one vector, so offset=5 leaves one
        // unclaimed byte between offsets and vector data.
        let mut page = make_alp_page_bytes(0, 0, 3, 1, &[5], 0);
        page.push(0);
        page.extend_from_slice(&vector);

        let err = decode_err::<FloatType>(page, 1);
        assert!(
            err.to_string()
                .contains("Invalid ALP page: vector offset 5 at index 0 does not match expected 4")
        );
    }

    #[test]
    fn test_parse_vector_view_exception_sections() {
        let mut vector = Vec::new();

        vector.push(2);
        vector.push(0);
        vector.extend_from_slice(&1u16.to_le_bytes());

        vector.extend_from_slice(&10u64.to_le_bytes());
        vector.push(0);

        vector.extend_from_slice(&0u16.to_le_bytes());
        vector.extend_from_slice(&42.5_f64.to_le_bytes());

        let body = Bytes::from(vector);
        let view = parse_vector_view::<u64>(body.as_ref(), 0, body.len(), 1).unwrap();
        assert_eq!(view.num_elements, 1);
        assert_eq!(view.alp_info.num_exceptions, 1);
        assert_eq!(view.for_info.bit_width, 0);

        let positions: Vec<u16> = body[view.exception_positions_range()]
            .chunks_exact(2)
            .map(|c| u16::from_le_bytes([c[0], c[1]]))
            .collect();
        let values: Vec<u64> = body[view.exception_values_range()]
            .chunks_exact(8)
            .map(|c| u64::from_le_bytes(c.try_into().unwrap()))
            .collect();
        assert_eq!(positions, vec![0]);
        assert_eq!(values, vec![42.5_f64.to_bits()]);
    }

    #[test]
    fn test_decode_page_values_f32_no_exceptions() {
        let vector = make_vector(VectorSpec {
            exponent: 0,
            factor: 0,
            frame_of_reference: 10u32,
            bit_width: 2,
            packed_values: &[0b1110_0100],
            exception_positions: &[],
            exception_values: &[],
        });
        let page = make_page_from_vectors(3, 4, &[vector]);
        let decoded = decode_page::<FloatType>(page, 4);
        assert_eq!(decoded, vec![10.0, 11.0, 12.0, 13.0]);
    }

    #[test]
    fn test_decode_page_values_f64_multi_vector_with_exceptions() {
        let vector0 = make_vector(VectorSpec {
            exponent: 0,
            factor: 0,
            frame_of_reference: 10u64,
            bit_width: 1,
            packed_values: &[0b0000_0010],
            exception_positions: &[1],
            exception_values: &[42.5f64.to_bits()],
        });
        let vector1 = make_vector(VectorSpec {
            exponent: 0,
            factor: 0,
            frame_of_reference: 7u64,
            bit_width: 0,
            packed_values: &[],
            exception_positions: &[],
            exception_values: &[],
        });
        let page = make_page_from_vectors(3, 9, &[vector0, vector1]);
        let decoded = decode_page::<DoubleType>(page, 9);
        assert_eq!(
            decoded,
            vec![10.0, 42.5, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 7.0]
        );
    }

    #[test]
    fn test_decode_page_values_f32_edge_values_via_exceptions() {
        let edge_values = [
            f32::NAN.to_bits(),
            (-0.0f32).to_bits(),
            f32::INFINITY.to_bits(),
        ];
        let vector = make_vector(VectorSpec {
            exponent: 0,
            factor: 0,
            frame_of_reference: 0u32,
            bit_width: 0,
            packed_values: &[],
            exception_positions: &[0, 1, 2],
            exception_values: &edge_values,
        });
        let page = make_page_from_vectors(3, 3, &[vector]);
        let decoded = decode_page::<FloatType>(page, 3);

        assert!(decoded[0].is_nan());
        assert_eq!(decoded[1], -0.0);
        assert!(decoded[1].is_sign_negative());
        assert_eq!(decoded[2], f32::INFINITY);
    }

    #[test]
    fn test_decode_page_values_f32_two_step_decimal_multiply() {
        let encoded = 1_970_570_984_i32;
        let vector = make_vector(VectorSpec {
            exponent: 1,
            factor: 1,
            frame_of_reference: encoded as u32,
            bit_width: 0,
            packed_values: &[],
            exception_positions: &[],
            exception_values: &[],
        });
        let page = make_page_from_vectors(3, 1, &[vector]);
        let decoded = decode_page::<FloatType>(page, 1);

        let expected_two_step = ((encoded as f32) * ALP_POW10_F32[1]) * ALP_NEG_POW10_F32[1];
        let one_step_scale = ALP_POW10_F32[1] * ALP_NEG_POW10_F32[1];
        let expected_one_step = (encoded as f32) * one_step_scale;

        assert_eq!(decoded[0].to_bits(), expected_two_step.to_bits());
        assert_ne!(decoded[0].to_bits(), expected_one_step.to_bits());
    }

    #[test]
    fn test_decode_page_values_f64_two_step_decimal_multiply() {
        let encoded = -3_900_047_474_048_127_703_i64;
        let vector = make_vector(VectorSpec {
            exponent: 1,
            factor: 1,
            frame_of_reference: encoded as u64,
            bit_width: 0,
            packed_values: &[],
            exception_positions: &[],
            exception_values: &[],
        });
        let page = make_page_from_vectors(3, 1, &[vector]);
        let decoded = decode_page::<DoubleType>(page, 1);

        let expected_two_step = ((encoded as f64) * ALP_POW10_F64[1]) * ALP_NEG_POW10_F64[1];
        let one_step_scale = ALP_POW10_F64[1] * ALP_NEG_POW10_F64[1];
        let expected_one_step = (encoded as f64) * one_step_scale;

        assert_eq!(decoded[0].to_bits(), expected_two_step.to_bits());
        assert_ne!(decoded[0].to_bits(), expected_one_step.to_bits());
    }

    #[test]
    fn test_alp_decoder_get_across_vectors() {
        let vector0 = make_vector(VectorSpec {
            exponent: 0,
            factor: 0,
            frame_of_reference: 10u32,
            bit_width: 1,
            packed_values: &[0b0000_0010],
            exception_positions: &[],
            exception_values: &[],
        });
        let vector1 = make_vector(VectorSpec {
            exponent: 0,
            factor: 0,
            frame_of_reference: 20u32,
            bit_width: 1,
            packed_values: &[0b0000_0010],
            exception_positions: &[],
            exception_values: &[],
        });
        let page = make_page_from_vectors(3, 12, &[vector0, vector1]);

        let mut decoder = AlpDecoder::<FloatType>::new();
        decoder.set_data(Bytes::from(page), 12).unwrap();

        let mut first = [0.0f32; 9];
        let read = decoder.get(&mut first).unwrap();
        assert_eq!(read, 9);
        assert_eq!(
            first,
            [10.0, 11.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 20.0]
        );
        assert_eq!(decoder.values_left(), 3);

        let mut second = [0.0f32; 3];
        let read = decoder.get(&mut second).unwrap();
        assert_eq!(read, 3);
        assert_eq!(second, [21.0, 20.0, 20.0]);
        assert_eq!(decoder.values_left(), 0);
    }

    #[test]
    fn test_alp_decoder_skip_across_vectors() {
        let vector0 = make_vector(VectorSpec {
            exponent: 0,
            factor: 0,
            frame_of_reference: 10u32,
            bit_width: 1,
            packed_values: &[0b0000_0010],
            exception_positions: &[],
            exception_values: &[],
        });
        let vector1 = make_vector(VectorSpec {
            exponent: 0,
            factor: 0,
            frame_of_reference: 20u32,
            bit_width: 1,
            packed_values: &[0b0000_0010],
            exception_positions: &[],
            exception_values: &[],
        });
        let page = make_page_from_vectors(3, 12, &[vector0, vector1]);

        let mut decoder = AlpDecoder::<FloatType>::new();
        decoder.set_data(Bytes::from(page), 12).unwrap();

        let skipped = decoder.skip(9).unwrap();
        assert_eq!(skipped, 9);
        assert_eq!(decoder.values_left(), 3);

        let mut out = [0.0f32; 1];
        let read = decoder.get(&mut out).unwrap();
        assert_eq!(read, 1);
        assert_eq!(out[0], 21.0);
        assert_eq!(decoder.values_left(), 2);
    }

    /// Microbenchmark comparing three decode inner loops on identical packed
    /// data, decoding into an 8 MB output to expose memory traffic:
    /// 1. scalar `get_value` per element;
    /// 2. bulk `get_batch` into a stack scratch tile, then a fused
    ///    inverse-FOR + decimal-decode pass writing into the output;
    /// 3. in-place: `get_batch` straight into the output (viewed as packed
    ///    integers), then decode each slot over its own bytes — no scratch.
    ///
    /// Ignored by default; run with:
    /// `cargo test -p parquet --release decode_inner_loop -- --ignored --nocapture`
    #[test]
    #[ignore = "microbenchmark"]
    fn bench_decode_inner_loop_f64() {
        use crate::util::bit_util::BitWriter;
        use std::time::Instant;

        let n = 1024usize; // values per vector
        let vectors = 1024usize; // -> 1M values, 8 MB output, well past L2
        let total = n * vectors;
        let bit_width = 12usize;
        let base = 1_000_000u64;
        let scale = <f64 as AlpFloat>::decode_scale(4, 2);

        let mut writer = BitWriter::new(n * 8);
        for i in 0..n as u64 {
            let delta = i.wrapping_mul(2_654_435_761) & ((1 << bit_width) - 1);
            writer.put_value(delta, bit_width);
        }
        writer.flush();
        let packed = Bytes::from(writer.consume());

        let iters = 30usize;
        let mut out = vec![0.0f64; total];
        let mut sink = 0.0f64;

        // 1. scalar get_value
        let t0 = Instant::now();
        for _ in 0..iters {
            for chunk in out.chunks_mut(n) {
                let mut reader = BitReader::new(packed.clone());
                for slot in chunk.iter_mut() {
                    let delta = reader.get_value::<u64>(bit_width).unwrap();
                    let signed = delta.wrapping_add(base) as i64;
                    *slot = ((signed as f64) * scale.0) * scale.1;
                }
            }
            sink += out[total - 1];
        }
        let scalar = t0.elapsed();

        // 2. batched into a stack scratch tile
        let t1 = Instant::now();
        for _ in 0..iters {
            let mut scratch = [0u64; 1024];
            for chunk in out.chunks_mut(n) {
                let len = chunk.len();
                let mut reader = BitReader::new(packed.clone());
                reader.get_batch::<u64>(&mut scratch[..len], bit_width);
                for (slot, &delta) in chunk.iter_mut().zip(scratch[..len].iter()) {
                    let signed = delta.wrapping_add(base) as i64;
                    *slot = ((signed as f64) * scale.0) * scale.1;
                }
            }
            sink += out[total - 1];
        }
        let scratch_t = t1.elapsed();

        // 3. in-place, no scratch
        let t2 = Instant::now();
        for _ in 0..iters {
            for chunk in out.chunks_mut(n) {
                let mut reader = BitReader::new(packed.clone());
                // SAFETY: f64 and u64 share size and alignment, so the output
                // chunk can hold the unpacked integers and then be overwritten
                // with the decoded float bit patterns.
                let int_out: &mut [u64] = unsafe {
                    std::slice::from_raw_parts_mut(chunk.as_mut_ptr() as *mut u64, chunk.len())
                };
                reader.get_batch::<u64>(int_out, bit_width);
                for slot in int_out.iter_mut() {
                    let signed = slot.wrapping_add(base) as i64;
                    *slot = (((signed as f64) * scale.0) * scale.1).to_bits();
                }
            }
            sink += out[total - 1];
        }
        let inplace = t2.elapsed();

        let total_vals = (iters * total) as f64;
        let ns = |d: std::time::Duration| d.as_nanos() as f64 / total_vals;
        println!(
            "f64 decode ({} MB out)  scalar: {:.3}  scratch: {:.3}  in-place: {:.3} ns/val  |  \
             scratch {:.2}x scalar, in-place {:.2}x scalar, in-place {:.2}x scratch  (sink={sink})",
            total * 8 / (1 << 20),
            ns(scalar),
            ns(scratch_t),
            ns(inplace),
            scalar.as_nanos() as f64 / scratch_t.as_nanos() as f64,
            scalar.as_nanos() as f64 / inplace.as_nanos() as f64,
            scratch_t.as_nanos() as f64 / inplace.as_nanos() as f64,
        );
    }

    /// End-to-end decoder throughput on a real multi-vector page, across a few
    /// `vector_size` values, exercising the production path: `set_data` (scratch
    /// sizing), per-vector `load_current_vector`, and the tiled `decode_range`
    /// over the reused buffer.
    ///
    /// `cargo test -p parquet --release decode_end_to_end -- --ignored --nocapture`
    #[test]
    #[ignore = "microbenchmark"]
    fn bench_decode_end_to_end() {
        use crate::util::bit_util::BitWriter;
        use std::time::Instant;

        fn pack(count: usize, bit_width: usize) -> Vec<u8> {
            let mut w = BitWriter::new(count * 8);
            for i in 0..count as u64 {
                let delta = i.wrapping_mul(2_654_435_761) & ((1u64 << bit_width) - 1);
                w.put_value(delta, bit_width);
            }
            w.flush();
            w.consume()
        }

        let total = 1usize << 20; // ~1M values
        let bit_width = 12usize;
        let iters = 50usize;

        for &log_vs in &[8u8, 10, 13] {
            let vector_size = 1usize << log_vs;
            let full_vectors = total / vector_size;
            let packed = pack(vector_size, bit_width);
            let vectors: Vec<Vec<u8>> = (0..full_vectors)
                .map(|_| {
                    make_vector(VectorSpec {
                        exponent: 4,
                        factor: 2,
                        frame_of_reference: 1_000_000u64,
                        bit_width: bit_width as u8,
                        packed_values: &packed,
                        exception_positions: &[],
                        exception_values: &[],
                    })
                })
                .collect();
            let n = full_vectors * vector_size;
            let page = Bytes::from(make_page_from_vectors(log_vs, n as i32, &vectors));

            let mut out = vec![0.0f64; n];
            let mut decoder = AlpDecoder::<DoubleType>::new();
            let mut sink = 0.0f64;
            let t = Instant::now();
            for _ in 0..iters {
                decoder.set_data(page.clone(), n).unwrap();
                decoder.get(&mut out).unwrap();
                sink += out[n - 1];
            }
            let el = t.elapsed();
            println!(
                "vector_size {:>5} ({:>4} vectors):  {:.3} ns/val  (sink={sink})",
                vector_size,
                full_vectors,
                el.as_nanos() as f64 / (iters * n) as f64,
            );
        }
    }

    #[test]
    fn test_alp_decoder_get_full_read() {
        let vector0 = make_vector(VectorSpec {
            exponent: 0,
            factor: 0,
            frame_of_reference: 10u32,
            bit_width: 1,
            packed_values: &[0b0000_0010],
            exception_positions: &[],
            exception_values: &[],
        });
        let vector1 = make_vector(VectorSpec {
            exponent: 0,
            factor: 0,
            frame_of_reference: 20u32,
            bit_width: 1,
            packed_values: &[0b0000_0010],
            exception_positions: &[],
            exception_values: &[],
        });
        let page = make_page_from_vectors(3, 12, &[vector0, vector1]);

        let mut decoder = AlpDecoder::<FloatType>::new();
        decoder.set_data(Bytes::from(page), 12).unwrap();

        let mut out = [0.0f32; 12];
        let read = decoder.get(&mut out).unwrap();
        assert_eq!(read, 12);
        assert_eq!(
            out,
            [
                10.0, 11.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 20.0, 21.0, 20.0, 20.0
            ]
        );
        assert_eq!(decoder.values_left(), 0);

        // The exhausted decoder yields nothing more.
        let mut extra = [0.0f32; 1];
        assert_eq!(decoder.get(&mut extra).unwrap(), 0);
    }
}
