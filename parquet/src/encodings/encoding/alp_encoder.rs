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

//! ALP (Adaptive Lossless floating-Point) encoder.
//!
//! Spec: <https://github.com/apache/parquet-format/blob/master/Encodings.md#adaptive-lossless-floating-point-alp--10>
//!
//! Values are buffered until the page is flushed, then encoded a vector at a
//! time into the page layout that [`AlpDecoder`] reads back:
//!
//! ```text
//! [AlpHeader][offsets][vector 0][vector 1]...[vector N-1]
//! ```
//!
//! [`AlpDecoder`]: crate::encodings::decoding::alp_decoder::AlpDecoder

use std::cmp::Reverse;

use bytes::Bytes;

use crate::basic::Encoding;
use crate::data_type::DataType;
use crate::encodings::alp::{
    ALP_COMPRESSION_MODE, ALP_DEFAULT_LOG_VECTOR_SIZE, ALP_HEADER_SIZE,
    ALP_INTEGER_ENCODING_FOR_BIT_PACK, AlpExact, AlpFloat, AlpHeader, AlpInfo, ForInfo,
};
use crate::encodings::encoding::Encoder;
use crate::errors::{ParquetError, Result};
use crate::util::bit_util::{BitWriter, num_required_bits};

/// Vectors are written at the spec's default size, the ALP paper's 1024.
const VECTOR_SIZE: usize = 1 << ALP_DEFAULT_LOG_VECTOR_SIZE;

/// Values sampled from a vector when estimating a candidate's encoded size.
const SAMPLES_PER_VECTOR: usize = 256;

/// Vectors sampled from the first page to build the column chunk's candidate set.
const SAMPLE_VECTORS: usize = 8;

/// Candidate `(exponent, factor)` pairs carried forward from the sampling pass.
const MAX_COMBINATIONS: usize = 5;

/// Consecutive non-improving candidates after which per-vector selection stops.
const SAMPLING_EARLY_EXIT_THRESHOLD: usize = 4;

/// Bits of overhead an exception costs: the value itself plus its `u16` position.
fn exception_bits<F: AlpFloat>() -> u64 {
    (F::Exact::WIDTH as u64 * 8) + 16
}

/// One ALP decimal-encoding candidate: `encoded = round(value * 10^e * 10^-f)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ExponentAndFactor {
    exponent: u8,
    factor: u8,
}

/// A candidate together with how often it won across the sampled vectors.
#[derive(Debug, Clone, Copy)]
struct Combination {
    params: ExponentAndFactor,
    num_appearances: u64,
    estimated_size_bits: u64,
}

/// Sort key ranking candidates, larger is better: more appearances as a vector's
/// best candidate, then smaller estimated size, then larger exponent, then
/// larger factor.
///
/// The last two are tie-breaks taken from the ALP paper (Afroozeh et al., SIGMOD
/// 2023, section 3.1.2), which prefers higher exponents and factors without
/// justifying it further.
fn rank(c: &Combination) -> (u64, Reverse<u64>, u8, u8) {
    (
        c.num_appearances,
        Reverse(c.estimated_size_bits),
        c.params.exponent,
        c.params.factor,
    )
}

fn is_better(c1: &Combination, c2: &Combination) -> bool {
    rank(c1) > rank(c2)
}

/// Estimate, in bits, what `params` would cost on `sample`.
///
/// Encodes each value, checks whether it round-trips, and prices the result as
/// `num_values * bit_width + num_exceptions * exception_bits`, where `bit_width`
/// covers the FOR range of the values that did round-trip.
///
/// Returns `None` when `penalize_exceptions` is set and fewer than two values
/// round-trip: such a candidate encodes almost everything as an exception, and
/// its FOR range is meaningless.
fn estimate_size_bits<F: AlpFloat>(
    sample: &[F],
    params: ExponentAndFactor,
    penalize_exceptions: bool,
) -> Option<u64> {
    let encode_scale = F::encode_scale(params.exponent, params.factor);
    let decode_scale = F::decode_scale(params.exponent, params.factor);

    let mut num_exceptions = 0u64;
    let mut min = None;
    let mut max = None;

    for &value in sample {
        let encoded = value.encode_value(encode_scale);
        if F::decode_value(encoded, decode_scale) == value {
            min = Some(min.map_or(encoded, |m: <F::Exact as AlpExact>::Signed| m.min(encoded)));
            max = Some(max.map_or(encoded, |m: <F::Exact as AlpExact>::Signed| m.max(encoded)));
        } else {
            num_exceptions += 1;
        }
    }

    let num_values = sample.len() as u64;
    let num_non_exceptions = num_values - num_exceptions;
    if penalize_exceptions && num_non_exceptions < 2 {
        return None;
    }

    // With nothing to frame, there is no packed section at all: every value is
    // stored as an exception.
    let (Some(min), Some(max)) = (min, max) else {
        return Some(num_values * exception_bits::<F>());
    };

    let range =
        F::Exact::reinterpret_from_signed(max).wrapping_sub(F::Exact::reinterpret_from_signed(min));
    let bit_width = u64::from(num_required_bits(range.to_u64()));

    Some(num_values * bit_width + num_exceptions * exception_bits::<F>())
}

/// Sample every `n`th value so the whole span is represented, capped at
/// [`SAMPLES_PER_VECTOR`] values.
fn sample_values<F: AlpFloat>(values: &[F], out: &mut Vec<F>) {
    out.clear();
    let stride = values.len().div_ceil(SAMPLES_PER_VECTOR).max(1);
    out.extend(values.iter().step_by(stride).copied());
}

/// Build the column chunk's candidate set: the top [`MAX_COMBINATIONS`] pairs by
/// how often they win across sampled vectors.
///
/// This is the first level of the ALP paper's two-level sampling. Each sampled
/// vector is searched exhaustively (66 pairs for `f32`, 190 for `f64`); the
/// winners are tallied, and the most frequent are carried forward so that each
/// vector only has to choose among a handful of candidates.
fn build_preset<F: AlpFloat>(values: &[F]) -> Vec<ExponentAndFactor> {
    let num_vectors = values.len().div_ceil(VECTOR_SIZE);
    let vector_stride = num_vectors.div_ceil(SAMPLE_VECTORS).max(1);

    let mut sample = Vec::with_capacity(SAMPLES_PER_VECTOR);
    let mut tally: Vec<Combination> = Vec::new();

    for vector in values.chunks(VECTOR_SIZE).step_by(vector_stride) {
        sample_values(vector, &mut sample);

        // Start from the worst case - every value an exception, unpacked - so
        // that any candidate that manages to encode anything beats it.
        let mut best = Combination {
            params: ExponentAndFactor {
                exponent: F::MAX_EXPONENT,
                factor: F::MAX_EXPONENT,
            },
            num_appearances: 0,
            estimated_size_bits: sample.len() as u64
                * (exception_bits::<F>() + F::Exact::WIDTH as u64 * 8),
        };

        for exponent in 0..=F::MAX_EXPONENT {
            for factor in 0..=exponent {
                let params = ExponentAndFactor { exponent, factor };
                let Some(estimated_size_bits) = estimate_size_bits(&sample, params, true) else {
                    continue;
                };
                let candidate = Combination {
                    params,
                    num_appearances: 0,
                    estimated_size_bits,
                };
                if is_better(&candidate, &best) {
                    best = candidate;
                }
            }
        }

        match tally.iter_mut().find(|c| c.params == best.params) {
            Some(existing) => existing.num_appearances += 1,
            None => tally.push(Combination {
                num_appearances: 1,
                ..best
            }),
        }
    }

    // Size estimates from different vectors are not comparable with each other,
    // so only the win counts rank the candidate set.
    for combination in tally.iter_mut() {
        combination.estimated_size_bits = 0;
    }
    tally.sort_by_key(|c| Reverse(rank(c)));
    tally.truncate(MAX_COMBINATIONS);

    if tally.is_empty() {
        // An empty page has nothing to sample; any valid pair will do.
        return vec![ExponentAndFactor {
            exponent: 0,
            factor: 0,
        }];
    }
    tally.into_iter().map(|c| c.params).collect()
}

/// Pick the candidate that encodes `vector` smallest.
///
/// This is the second level of the two-level sampling: only the chunk's
/// candidates are tried, against a sample of the vector rather than all of it.
fn select_params<F: AlpFloat>(
    vector: &[F],
    preset: &[ExponentAndFactor],
    sample: &mut Vec<F>,
) -> ExponentAndFactor {
    if preset.len() == 1 {
        return preset[0];
    }

    sample_values(vector, sample);

    let mut best = preset[0];
    let mut best_size_bits = u64::MAX;
    let mut worse_in_a_row = 0;

    for &params in preset {
        let Some(size_bits) = estimate_size_bits(sample, params, false) else {
            continue;
        };
        if size_bits >= best_size_bits {
            worse_in_a_row += 1;
            if worse_in_a_row == SAMPLING_EARLY_EXIT_THRESHOLD {
                break;
            }
            continue;
        }
        best = params;
        best_size_bits = size_bits;
        worse_in_a_row = 0;
    }
    best
}

/// Reusable per-vector buffers, kept across vectors and pages so encoding a page
/// does not allocate per vector.
struct Scratch<F: AlpFloat> {
    /// Decimal-encoded integers, overwritten in place with their FOR deltas.
    encoded: Vec<<F::Exact as AlpExact>::Signed>,
    /// Per-value exception flag (`1` = round-trip failed). A byte, not a bitset,
    /// so the map loop's per-lane store stays vectorizable.
    exc_mask: Vec<u8>,
    exception_positions: Vec<u16>,
    exception_values: Vec<F>,
    sample: Vec<F>,
}

impl<F: AlpFloat> Scratch<F> {
    fn new() -> Self {
        Self {
            encoded: Vec::new(),
            exc_mask: Vec::new(),
            exception_positions: Vec::new(),
            exception_values: Vec::new(),
            sample: Vec::new(),
        }
    }

    fn estimated_memory_size(&self) -> usize {
        self.encoded.capacity() * std::mem::size_of::<<F::Exact as AlpExact>::Signed>()
            + self.exc_mask.capacity()
            + self.exception_positions.capacity() * std::mem::size_of::<u16>()
            + self.exception_values.capacity() * std::mem::size_of::<F>()
            + self.sample.capacity() * std::mem::size_of::<F>()
    }
}

/// Encode one vector and append it to `out`:
/// `[AlpInfo][ForInfo][PackedValues][ExceptionPositions][ExceptionValues]`.
fn encode_vector<F: AlpFloat>(
    values: &[F],
    params: ExponentAndFactor,
    scratch: &mut Scratch<F>,
    out: &mut Vec<u8>,
) -> Result<()> {
    let encode_scale = F::encode_scale(params.exponent, params.factor);
    let decode_scale = F::decode_scale(params.exponent, params.factor);

    let Scratch {
        encoded,
        exc_mask,
        exception_positions,
        exception_values,
        ..
    } = scratch;

    let zero = F::Exact::default().reinterpret_as_signed();
    let n_values = values.len();
    encoded.resize(n_values, zero);
    exc_mask.resize(n_values, 0);
    exception_values.clear();

    // Comparing the *decoded* value to the input (not the input to itself) flags
    // -0.0, which encodes to +0.0.
    for ((&value, enc_slot), mask_slot) in values
        .iter()
        .zip(encoded.iter_mut())
        .zip(exc_mask.iter_mut())
    {
        let encoded_value = value.encode_value(encode_scale);
        *enc_slot = encoded_value;
        *mask_slot = u8::from(F::decode_value(encoded_value, decode_scale) != value);
    }

    // Branchless compaction: always write the index, advance only when flagged.
    exception_positions.resize(n_values, 0);
    let mut num_exceptions_usize = 0usize;
    for (idx, &is_exception) in exc_mask.iter().enumerate() {
        exception_positions[num_exceptions_usize] = idx as u16;
        num_exceptions_usize += usize::from(is_exception);
    }
    exception_positions.truncate(num_exceptions_usize);

    let num_exceptions = u16::try_from(exception_positions.len()).map_err(|_| {
        general_err!(
            "Invalid ALP vector: {} exceptions exceeds u16::MAX",
            exception_positions.len()
        )
    })?;

    // Fill each exception's packed slot with a real value (not the sentinel) so
    // the FOR range - and thus the bit width - stays tight; the true value goes
    // in the exception section.
    let placeholder = first_non_exception_value::<F>(encoded, exception_positions);
    for &position in exception_positions.iter() {
        exception_values.push(values[position as usize]);
        encoded[position as usize] = placeholder;
    }

    let min = encoded.iter().copied().min().unwrap_or(zero);
    let max = encoded.iter().copied().max().unwrap_or(zero);

    let frame_of_reference = F::Exact::reinterpret_from_signed(min);
    let range = F::Exact::reinterpret_from_signed(max).wrapping_sub(frame_of_reference);
    let bit_width = num_required_bits(range.to_u64());

    let alp_info = AlpInfo {
        exponent: params.exponent,
        factor: params.factor,
        num_exceptions,
    };
    let for_info = ForInfo::<F::Exact> {
        frame_of_reference,
        bit_width,
    };
    alp_info.extend_serialized(out);
    for_info.extend_serialized(out);

    // PackedValues: FOR deltas, LSB-first, as the spec requires. A zero bit
    // width means every value equals the frame of reference, so nothing is
    // stored.
    if bit_width > 0 {
        let mut writer = BitWriter::new_from_buf(std::mem::take(out));
        for &encoded_value in encoded.iter() {
            let delta =
                F::Exact::reinterpret_from_signed(encoded_value).wrapping_sub(frame_of_reference);
            writer.put_value(delta.to_u64(), bit_width as usize);
        }
        // Pads to a byte boundary, giving exactly the ceil(n * bit_width / 8)
        // bytes the decoder derives from the metadata.
        *out = writer.consume();
    }

    for &position in exception_positions.iter() {
        out.extend_from_slice(&position.to_le_bytes());
    }
    for &value in exception_values.iter() {
        value.to_exact_bits().extend_le_bytes(out);
    }

    Ok(())
}

/// The encoded integer of the first value that is not an exception, or zero if
/// every value is one. `exception_positions` is ascending, so the first index it
/// skips over is the first non-exception.
fn first_non_exception_value<F: AlpFloat>(
    encoded: &[<F::Exact as AlpExact>::Signed],
    exception_positions: &[u16],
) -> <F::Exact as AlpExact>::Signed {
    let mut candidate = 0usize;
    for &position in exception_positions {
        if position as usize != candidate {
            break;
        }
        candidate += 1;
    }
    encoded
        .get(candidate)
        .copied()
        .unwrap_or_else(|| F::Exact::default().reinterpret_as_signed())
}

/// Encode `values` as one ALP page.
fn encode_page<F: AlpFloat>(
    values: &[F],
    preset: &[ExponentAndFactor],
    scratch: &mut Scratch<F>,
) -> Result<Vec<u8>> {
    let header = AlpHeader {
        compression_mode: ALP_COMPRESSION_MODE,
        integer_encoding: ALP_INTEGER_ENCODING_FOR_BIT_PACK,
        vector_size: VECTOR_SIZE,
        num_elements: values.len(),
    };
    let num_vectors = header.num_vectors();

    let mut page = Vec::with_capacity(ALP_HEADER_SIZE + num_vectors * 4 + values.len() * 4);
    page.extend_from_slice(&header.serialize()?);

    // Offsets are only known once each vector is encoded, so leave room and
    // backfill.
    let offsets_start = page.len();
    page.resize(offsets_start + num_vectors * std::mem::size_of::<u32>(), 0);

    for (idx, vector) in values.chunks(VECTOR_SIZE).enumerate() {
        // Offsets are relative to the start of the page body, which follows the
        // fixed-size header.
        let offset = u32::try_from(page.len() - ALP_HEADER_SIZE)
            .map_err(|_| general_err!("Invalid ALP page: body exceeds u32 offset range"))?;
        let offset_at = offsets_start + idx * std::mem::size_of::<u32>();
        page[offset_at..offset_at + 4].copy_from_slice(&offset.to_le_bytes());

        let params = select_params(vector, preset, &mut scratch.sample);
        encode_vector(vector, params, scratch, &mut page)?;
    }

    Ok(page)
}

/// A page encoded incrementally, one vector at a time.
///
/// Once the column chunk's preset is known (after the first page), later pages do
/// not need all their values at once: each vector is encoded and appended to
/// `body` as soon as its [`VECTOR_SIZE`] values arrive, and the raw floats are
/// dropped. Only a sub-vector remainder is held in `carry` between `put`s. The
/// header and offset array - which depend on the final vector count - are
/// prepended in [`StreamingPage::finish`].
///
/// The bytes produced are identical to encoding the same values in one pass with
/// [`encode_page`]: same vectors, same per-vector parameters, same contiguous
/// offsets. What changes is peak memory - the whole page of raw floats is never
/// resident - and that the encode work is spread across `put`s.
struct StreamingPage<F: AlpFloat> {
    /// Encoded vectors, concatenated; becomes the page body after the offsets.
    body: Vec<u8>,
    /// Body-relative start offset of each completed vector.
    vector_offsets: Vec<u32>,
    /// Values not yet forming a complete vector, carried across `put`s.
    carry: Vec<F>,
    /// Total values appended to this page so far.
    count: usize,
}

impl<F: AlpFloat> StreamingPage<F> {
    fn new() -> Self {
        Self {
            body: Vec::new(),
            vector_offsets: Vec::new(),
            carry: Vec::new(),
            count: 0,
        }
    }

    fn estimated_memory_size(&self) -> usize {
        self.body.capacity()
            + self.vector_offsets.capacity() * std::mem::size_of::<u32>()
            + self.carry.capacity() * std::mem::size_of::<F>()
    }

    /// Encode one complete vector and append it to `body`.
    fn push_vector(
        &mut self,
        vector: &[F],
        preset: &[ExponentAndFactor],
        scratch: &mut Scratch<F>,
    ) -> Result<()> {
        let body_start = u32::try_from(self.body.len())
            .map_err(|_| general_err!("Invalid ALP page: body exceeds u32 offset range"))?;
        self.vector_offsets.push(body_start);
        let params = select_params(vector, preset, &mut scratch.sample);
        encode_vector(vector, params, scratch, &mut self.body)
    }

    /// Buffer `values`, encoding and dropping every complete vector they form.
    fn put(
        &mut self,
        mut values: &[F],
        preset: &[ExponentAndFactor],
        scratch: &mut Scratch<F>,
    ) -> Result<()> {
        self.count += values.len();

        // Finish a vector left partially filled by a previous `put`.
        if !self.carry.is_empty() {
            let need = VECTOR_SIZE - self.carry.len();
            if values.len() < need {
                self.carry.extend_from_slice(values);
                return Ok(());
            }
            let (head, tail) = values.split_at(need);
            self.carry.extend_from_slice(head);
            // Move the carry out so the vector slice does not alias `self`; the
            // allocation is handed back, cleared, for the next partial to reuse.
            let mut vector = std::mem::take(&mut self.carry);
            self.push_vector(&vector, preset, scratch)?;
            vector.clear();
            self.carry = vector;
            values = tail;
        }

        // Encode whole vectors straight from the input, no copy.
        let mut chunks = values.chunks_exact(VECTOR_SIZE);
        for vector in chunks.by_ref() {
            self.push_vector(vector, preset, scratch)?;
        }
        self.carry.extend_from_slice(chunks.remainder());
        Ok(())
    }

    /// Encode the trailing partial vector, then assemble `[header][offsets][body]`
    /// and reset for the next page.
    fn finish(
        &mut self,
        preset: &[ExponentAndFactor],
        scratch: &mut Scratch<F>,
    ) -> Result<Vec<u8>> {
        if !self.carry.is_empty() {
            let mut vector = std::mem::take(&mut self.carry);
            self.push_vector(&vector, preset, scratch)?;
            vector.clear();
            self.carry = vector;
        }

        let num_vectors = self.vector_offsets.len();
        let offsets_section = num_vectors * std::mem::size_of::<u32>();
        let header = AlpHeader {
            compression_mode: ALP_COMPRESSION_MODE,
            integer_encoding: ALP_INTEGER_ENCODING_FOR_BIT_PACK,
            vector_size: VECTOR_SIZE,
            num_elements: self.count,
        };

        let mut page = Vec::with_capacity(ALP_HEADER_SIZE + offsets_section + self.body.len());
        page.extend_from_slice(&header.serialize()?);
        for &body_start in &self.vector_offsets {
            // Offsets are measured from the start of the offset array: the array's
            // own size plus the vector's position within the body.
            let page_offset = u32::try_from(offsets_section + body_start as usize)
                .map_err(|_| general_err!("Invalid ALP page: body exceeds u32 offset range"))?;
            page.extend_from_slice(&page_offset.to_le_bytes());
        }
        page.extend_from_slice(&self.body);

        self.body.clear();
        self.vector_offsets.clear();
        self.count = 0;
        Ok(page)
    }
}

/// Encoder for ALP-encoded floating-point pages (`f32`/`f64`).
///
/// The first page is buffered whole: ALP samples it to choose the column chunk's
/// candidate `(exponent, factor)` set, which needs all of the page's data. Once
/// that preset is fixed, later pages are encoded incrementally, a vector at a
/// time, without ever holding the whole page of raw floats (see [`StreamingPage`]).
pub struct AlpEncoder<T: DataType>
where
    T::T: AlpFloat,
{
    /// Values buffered for the first page, until the preset is built. Empty once
    /// streaming begins.
    values: Vec<T::T>,
    /// Candidate `(exponent, factor)` pairs for this column chunk, sampled once
    /// from the first page and reused for the rest of the chunk. `None` until the
    /// first page is flushed; its presence is what switches `put` to streaming.
    preset: Option<Vec<ExponentAndFactor>>,
    scratch: Scratch<T::T>,
    /// The page built incrementally, used for every page after the first.
    streaming: StreamingPage<T::T>,
}

impl<T: DataType> AlpEncoder<T>
where
    T::T: AlpFloat,
{
    pub(crate) fn new() -> Self {
        Self {
            values: Vec::new(),
            preset: None,
            scratch: Scratch::new(),
            streaming: StreamingPage::new(),
        }
    }

    /// Values buffered for the current page: the first page lives in `values`,
    /// later pages in the streaming buffer. Used only for size estimates.
    fn current_page_len(&self) -> usize {
        if self.preset.is_none() {
            self.values.len()
        } else {
            self.streaming.count
        }
    }
}

impl<T: DataType> Encoder<T> for AlpEncoder<T>
where
    T::T: AlpFloat,
{
    fn put(&mut self, values: &[T::T]) -> Result<()> {
        let Self {
            values: buffer,
            preset,
            scratch,
            streaming,
        } = self;
        match preset.as_deref() {
            // First page: buffer until the preset can be built on flush.
            None => buffer.extend_from_slice(values),
            // Later pages: encode incrementally against the fixed preset.
            Some(preset) => streaming.put(values, preset, scratch)?,
        }
        Ok(())
    }

    fn encoding(&self) -> Encoding {
        Encoding::ALP
    }

    fn estimated_data_encoded_size(&self) -> usize {
        // Encoded size is not known until the parameters are chosen. In the
        // worst case, almost every value is an exception while two encodable
        // values force a full-width packed FOR range. Bound both the packed
        // placeholder/value and the raw exception value by the exact type's
        // width, plus the exception position. Bounding by value count (not the
        // running encoded size) keeps the writer's page-boundary decisions
        // identical whether the page is buffered or streamed.
        let len = self.current_page_len();
        let num_vectors = len.div_ceil(VECTOR_SIZE);
        ALP_HEADER_SIZE
            + num_vectors
                * (std::mem::size_of::<u32>()
                    + AlpInfo::STORED_SIZE
                    + ForInfo::<<T::T as AlpFloat>::Exact>::stored_size())
            + len * (2 * <T::T as AlpFloat>::Exact::WIDTH + std::mem::size_of::<u16>())
    }

    fn estimated_memory_size(&self) -> usize {
        self.values.capacity() * std::mem::size_of::<T::T>()
            + self.preset.as_ref().map_or(0, |p| {
                p.capacity() * std::mem::size_of::<ExponentAndFactor>()
            })
            + self.scratch.estimated_memory_size()
            + self.streaming.estimated_memory_size()
    }

    fn flush_buffer(&mut self) -> Result<Bytes> {
        let Self {
            values,
            preset,
            scratch,
            streaming,
        } = self;

        // The first flush builds the preset from the whole buffered page and
        // encodes it in one pass; that also arms streaming for later pages.
        let page = match preset {
            None => {
                let built = build_preset(values);
                let page = encode_page(values, &built, scratch)?;
                values.clear();
                *preset = Some(built);
                page
            }
            Some(preset) => streaming.finish(preset.as_slice(), scratch)?,
        };
        Ok(page.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_type::{DoubleType, FloatType};
    use crate::encodings::decoding::Decoder;
    use crate::encodings::decoding::alp_decoder::AlpDecoder;

    /// Encode `values` and read them back through the decoder.
    fn roundtrip<T: DataType>(values: &[T::T]) -> Vec<T::T>
    where
        T::T: AlpFloat,
        <T::T as AlpFloat>::Exact: Send,
    {
        let mut encoder = AlpEncoder::<T>::new();
        encoder.put(values).unwrap();
        let page = encoder.flush_buffer().unwrap();

        let mut decoder = AlpDecoder::<T>::new();
        decoder.set_data(page, values.len()).unwrap();
        let mut out = vec![T::T::default(); values.len()];
        assert_eq!(decoder.get(&mut out).unwrap(), values.len());
        out
    }

    /// Assert bit-for-bit equality, so that NaN and -0.0 are held to the same
    /// standard as every other value.
    fn assert_bits_eq<F: AlpFloat + std::fmt::Debug>(actual: &[F], expected: &[F]) {
        assert_eq!(actual.len(), expected.len(), "length mismatch");
        for (idx, (a, e)) in actual.iter().zip(expected.iter()).enumerate() {
            assert_eq!(
                a.to_exact_bits(),
                e.to_exact_bits(),
                "value mismatch at {idx}: expected {e:?}, got {a:?}"
            );
        }
    }

    #[test]
    fn test_roundtrip_f64_decimals() {
        let values: Vec<f64> = (0..500).map(|i| (i as f64) * 0.01 + 1.23).collect();
        assert_bits_eq(&roundtrip::<DoubleType>(&values), &values);
    }

    #[test]
    fn test_roundtrip_f32_decimals() {
        let values: Vec<f32> = (0..500).map(|i| (i as f32) * 0.5 + 1.25).collect();
        assert_bits_eq(&roundtrip::<FloatType>(&values), &values);
    }

    /// Spans several vectors, including a short trailing one.
    #[test]
    fn test_roundtrip_multiple_vectors() {
        let values: Vec<f64> = (0..2600).map(|i| (i as f64) * 0.001).collect();
        assert_bits_eq(&roundtrip::<DoubleType>(&values), &values);
    }

    /// The values ALP cannot represent must survive verbatim through the
    /// exception path - including -0.0, which compares equal to +0.0 and would
    /// be silently lost by a naive round-trip check.
    #[test]
    fn test_roundtrip_exceptions() {
        let values = vec![
            1.5f64,
            f64::NAN,
            2.5,
            f64::INFINITY,
            -0.0,
            f64::NEG_INFINITY,
            3.5,
            0.0,
            f64::MAX,
            f64::MIN,
        ];
        let decoded = roundtrip::<DoubleType>(&values);
        assert_bits_eq(&decoded, &values);
        assert!(decoded[1].is_nan());
        assert!(decoded[4].is_sign_negative());
    }

    /// Every value identical means a zero FOR range, so `bit_width` is 0 and no
    /// packed section is written at all.
    #[test]
    fn test_roundtrip_all_identical() {
        let values = vec![42.42f64; 3000];
        assert_bits_eq(&roundtrip::<DoubleType>(&values), &values);
    }

    #[test]
    fn test_roundtrip_all_exceptions() {
        let values = vec![f64::NAN; 100];
        let decoded = roundtrip::<DoubleType>(&values);
        assert!(decoded.iter().all(|v| v.is_nan()));
    }

    #[test]
    fn test_roundtrip_single_value() {
        assert_bits_eq(&roundtrip::<DoubleType>(&[3.25]), &[3.25]);
    }

    #[test]
    fn test_roundtrip_empty() {
        let mut encoder = AlpEncoder::<DoubleType>::new();
        let page = encoder.flush_buffer().unwrap();
        assert_eq!(page.len(), ALP_HEADER_SIZE);

        let mut decoder = AlpDecoder::<DoubleType>::new();
        decoder.set_data(page, 0).unwrap();
        assert_eq!(decoder.values_left(), 0);
    }

    /// The encoder must be reusable across pages, and the candidate set chosen on
    /// the first page must still produce valid pages for later ones.
    #[test]
    fn test_roundtrip_multiple_pages() {
        let mut encoder = AlpEncoder::<DoubleType>::new();

        for page_idx in 0..3 {
            let values: Vec<f64> = (0..1500)
                .map(|i| (i as f64) * 0.01 + (page_idx as f64))
                .collect();
            encoder.put(&values).unwrap();
            let page = encoder.flush_buffer().unwrap();

            let mut decoder = AlpDecoder::<DoubleType>::new();
            decoder.set_data(page, values.len()).unwrap();
            let mut out = vec![0.0f64; values.len()];
            assert_eq!(decoder.get(&mut out).unwrap(), values.len());
            assert_bits_eq(&out, &values);
        }
    }

    /// Two-decimal data must encode with no exceptions and pack far below the 64
    /// bits an unencoded double takes.
    ///
    /// The parameter to check is `exponent - factor`, not `exponent`: the encoded
    /// integers depend only on the effective scale `10^(exponent - factor)`, so
    /// (3, 1) and (2, 0) encode identically and tie on estimated size. The ALP
    /// paper's tie-break then prefers the larger exponent and factor, which is
    /// why the pair chosen here is not simply (2, 0).
    #[test]
    fn test_selects_decimal_parameters() {
        let values: Vec<f64> = (0..1024).map(|i| (i as f64) * 0.01).collect();

        let mut encoder = AlpEncoder::<DoubleType>::new();
        encoder.put(&values).unwrap();
        let page = encoder.flush_buffer().unwrap();

        // [header][one u32 offset][exponent][factor][num_exceptions]...
        let exponent = page[ALP_HEADER_SIZE + 4];
        let factor = page[ALP_HEADER_SIZE + 5];
        let num_exceptions =
            u16::from_le_bytes([page[ALP_HEADER_SIZE + 6], page[ALP_HEADER_SIZE + 7]]);

        assert_eq!(
            exponent - factor,
            2,
            "two-decimal data should encode at an effective scale of 10^2, \
             got exponent {exponent} factor {factor}"
        );
        assert_eq!(num_exceptions, 0, "two-decimal data should not except");

        let plain_size = values.len() * std::mem::size_of::<f64>();
        assert!(
            page.len() * 4 < plain_size,
            "expected at least 4x compression, got {} bytes vs {plain_size} plain",
            page.len()
        );
    }

    /// A single exception must not blow up the frame of reference: its slot is
    /// filled with a real encoded value, so the bit width stays tight.
    #[test]
    fn test_exception_placeholder_keeps_bit_width_tight() {
        let mut values: Vec<f64> = (0..1024).map(|i| (i as f64) * 0.01).collect();
        values[500] = f64::NAN;

        let mut encoder = AlpEncoder::<DoubleType>::new();
        encoder.put(&values).unwrap();
        let page = encoder.flush_buffer().unwrap();

        // [header][offset][AlpInfo(4)][frame_of_reference(8)][bit_width(1)]
        let bit_width = page[ALP_HEADER_SIZE + 4 + AlpInfo::STORED_SIZE + 8];
        assert!(
            bit_width <= 17,
            "one exception should not widen the frame; got bit_width {bit_width}"
        );

        assert_bits_eq(&roundtrip::<DoubleType>(&values), &values);
    }

    /// Streaming pages (every page after the first) must be byte-for-byte
    /// identical to encoding the same values in one pass with the same preset.
    #[test]
    fn test_streaming_matches_buffered() {
        let page1: Vec<f64> = (0..3000).map(|i| (i as f64) * 0.01 + 1.23).collect();
        let page2: Vec<f64> = (0..3000).map(|i| (i as f64) * 0.03 - 7.0).collect();

        let mut encoder = AlpEncoder::<DoubleType>::new();
        encoder.put(&page1).unwrap();
        let _ = encoder.flush_buffer().unwrap(); // builds and caches the preset

        // Feed page 2 in irregular chunks that cross vector boundaries.
        for chunk in page2.chunks(997) {
            encoder.put(chunk).unwrap();
        }
        let streamed = encoder.flush_buffer().unwrap();

        // The encoder built its preset from page 1; reproduce it to encode page 2
        // in a single pass as the reference.
        let preset = build_preset(&page1);
        let mut scratch = Scratch::<f64>::new();
        let reference = encode_page(&page2, &preset, &mut scratch).unwrap();

        assert_eq!(
            streamed.as_ref(),
            reference.as_slice(),
            "streamed page differs from single-pass encoding"
        );
    }

    /// The carry logic must reassemble vectors correctly no matter how `put` calls
    /// land relative to vector boundaries: sizes below, at, and above a vector.
    #[test]
    fn test_streaming_irregular_puts() {
        let page1: Vec<f64> = (0..2048).map(|i| (i as f64) * 0.01).collect();
        let page2: Vec<f64> = (0..5000).map(|i| (i as f64) * 0.01 + 100.0).collect();

        let mut encoder = AlpEncoder::<DoubleType>::new();
        encoder.put(&page1).unwrap();
        let _ = encoder.flush_buffer().unwrap();

        let sizes = [1usize, 1023, 2, 1024, 1025, 7, 900, 118];
        let (mut offset, mut i) = (0usize, 0usize);
        while offset < page2.len() {
            let n = sizes[i % sizes.len()].min(page2.len() - offset);
            encoder.put(&page2[offset..offset + n]).unwrap();
            offset += n;
            i += 1;
        }
        let page = encoder.flush_buffer().unwrap();

        let mut decoder = AlpDecoder::<DoubleType>::new();
        decoder.set_data(page, page2.len()).unwrap();
        let mut out = vec![0.0f64; page2.len()];
        assert_eq!(decoder.get(&mut out).unwrap(), page2.len());
        assert_bits_eq(&out, &page2);
    }

    /// A streaming page ending exactly on a vector boundary must finish with an
    /// empty carry buffer.
    #[test]
    fn test_streaming_page_ends_on_vector_boundary() {
        // Flushing a throwaway first page builds the preset and switches the
        // encoder to streaming. Only the second page is under test.
        let preset_page: Vec<f64> = (0..100).map(|i| (i as f64) * 0.01).collect();
        let values: Vec<f64> = (0..2 * VECTOR_SIZE).map(|i| (i as f64) * 0.01).collect();

        let mut encoder = AlpEncoder::<DoubleType>::new();
        encoder.put(&preset_page).unwrap();
        let _ = encoder.flush_buffer().unwrap();

        let empty_page_estimate = encoder.estimated_data_encoded_size();
        encoder.put(&values).unwrap();
        // The size estimate for a streamed page comes from the running count.
        assert!(encoder.estimated_data_encoded_size() > empty_page_estimate);
        // The 2048 values streamed through as two complete vectors, so this
        // flush reaches `StreamingPage::finish` with an empty carry: the page
        // must assemble correctly without the trailing-partial-vector encode
        // that every other streaming test ends with.
        let page = encoder.flush_buffer().unwrap();

        let mut decoder = AlpDecoder::<DoubleType>::new();
        decoder.set_data(page, values.len()).unwrap();
        let mut out = vec![0.0f64; values.len()];
        assert_eq!(decoder.get(&mut out).unwrap(), values.len());
        assert_bits_eq(&out, &values);
    }

    /// The memory estimate must track the buffers in both encoder states: the
    /// buffered first page and the streaming state after it.
    #[test]
    fn test_estimated_memory_size_tracks_buffers() {
        let mut encoder = AlpEncoder::<DoubleType>::new();
        // A fresh encoder has allocated nothing.
        assert_eq!(encoder.estimated_memory_size(), 0);

        let values: Vec<f64> = (0..1500).map(|i| (i as f64) * 0.01).collect();
        // First-page state: only the raw-value buffer is live (scratch and
        // streaming are untouched), so the estimate must cover its bytes.
        encoder.put(&values).unwrap();
        assert!(encoder.estimated_memory_size() >= values.len() * std::mem::size_of::<f64>());

        // The flushed first-page buffer and encoding scratch space keep their
        // capacities, and the preset is retained. Capture that full baseline.
        let _ = encoder.flush_buffer().unwrap();
        let after_flush = encoder.estimated_memory_size();

        // Streaming state: the streaming-page buffers now hold encoded vectors,
        // which must be counted on top of that baseline.
        encoder.put(&values).unwrap();
        assert!(encoder.estimated_memory_size() > after_flush);
    }

    /// The size estimate must include packed placeholders as well as raw
    /// exception values when both sections coexist at their widest.
    #[test]
    fn test_estimated_data_size_covers_wide_packed_values_with_exceptions() {
        let mut values = vec![f64::NAN; VECTOR_SIZE];
        // Both positions are sampled when building the preset. At exponent and
        // factor zero their integer range is exactly 2^63, forcing a 64-bit
        // packed section; every other value remains an exception.
        values[0] = f64::ENCODING_LOWER_LIMIT;
        values[4] = 1024.0;

        let mut encoder = AlpEncoder::<DoubleType>::new();
        encoder.put(&values).unwrap();
        let estimated_size = encoder.estimated_data_encoded_size();
        let page = encoder.flush_buffer().unwrap();

        let vector_start = ALP_HEADER_SIZE + std::mem::size_of::<u32>();
        let num_exceptions = u16::from_le_bytes([page[vector_start + 2], page[vector_start + 3]]);
        let bit_width = page[vector_start + AlpInfo::STORED_SIZE + <f64 as AlpFloat>::Exact::WIDTH];
        assert_eq!(num_exceptions as usize, VECTOR_SIZE - 2);
        assert_eq!(bit_width, 64);
        assert!(
            estimated_size >= page.len(),
            "estimated {estimated_size} bytes, encoded {} bytes",
            page.len()
        );
    }

    /// Fill the preset to `MAX_COMBINATIONS` and include a vector no candidate
    /// can encode, exercising the all-exception size estimate and the early
    /// exit of the per-vector candidate search.
    #[test]
    fn test_full_preset_with_all_exception_vector() {
        // Nine vectors are sampled at stride two, so the even-indexed ones
        // decide the preset: four distinct decimal scales, plus the all-NaN
        // ninth vector, which no candidate can encode and which therefore
        // contributes the worst-case fallback pair as the fifth candidate.
        let scales = [0.01, 0.01, 0.001, 0.01, 0.0001, 0.01, 0.00001, 0.01];
        let mut values = Vec::with_capacity(9 * VECTOR_SIZE);
        for scale in scales {
            values.extend((0..VECTOR_SIZE).map(|i| (i as f64) * scale));
        }
        values.extend(std::iter::repeat_n(f64::NAN, VECTOR_SIZE));

        let preset = build_preset(&values);
        assert_eq!(preset.len(), MAX_COMBINATIONS);

        let nan_vector = &values[8 * VECTOR_SIZE..];
        let mut sample = Vec::new();
        sample_values(nan_vector, &mut sample);
        let candidate_costs: Vec<_> = preset
            .iter()
            .map(|&params| estimate_size_bits(&sample, params, false))
            .collect();
        assert!(candidate_costs.windows(2).all(|costs| costs[0] == costs[1]));

        // Encoding the NaN vector prices all five candidates at the same
        // all-exception size, so the per-vector search stops early after
        // `SAMPLING_EARLY_EXIT_THRESHOLD` non-improving candidates. The
        // round-trip proves the page survives both paths losslessly.
        assert_bits_eq(&roundtrip::<DoubleType>(&values), &values);
    }
}
