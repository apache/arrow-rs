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
//! Based on the draft Parquet spec: <https://github.com/apache/parquet-format/pull/557>
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
/// justifying it further. They are kept so the candidate set matches the
/// reference implementations.
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

    let range = F::Exact::reinterpret_from_signed(max).wrapping_sub(F::Exact::reinterpret_from_signed(min));
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
    exception_positions: Vec<u16>,
    exception_values: Vec<F>,
    sample: Vec<F>,
}

impl<F: AlpFloat> Scratch<F> {
    fn new() -> Self {
        Self {
            encoded: Vec::new(),
            exception_positions: Vec::new(),
            exception_values: Vec::new(),
            sample: Vec::new(),
        }
    }

    fn estimated_memory_size(&self) -> usize {
        self.encoded.capacity() * std::mem::size_of::<<F::Exact as AlpExact>::Signed>()
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
        exception_positions,
        exception_values,
        ..
    } = scratch;
    encoded.clear();
    exception_positions.clear();
    exception_values.clear();

    // A value is an exception when it does not survive the round trip. Values
    // ALP cannot represent at all (NaN, the infinities, -0.0) encode to a
    // sentinel whose round trip is guaranteed to mismatch, so they need no
    // separate test here. Note that `-0.0 == 0.0`, so a naive equality check
    // against the *input* would let -0.0 through and silently lose its sign.
    for (idx, &value) in values.iter().enumerate() {
        let encoded_value = value.encode_value(encode_scale);
        encoded.push(encoded_value);
        if F::decode_value(encoded_value, decode_scale) != value {
            exception_positions.push(idx as u16);
        }
    }

    let num_exceptions = u16::try_from(exception_positions.len()).map_err(|_| {
        general_err!(
            "Invalid ALP vector: {} exceptions exceeds u16::MAX",
            exception_positions.len()
        )
    })?;

    // An exception still occupies a slot in the packed section. Filling it with
    // a real encoded value - rather than leaving the sentinel there - keeps the
    // FOR range tight, which is what the bit width is derived from. The true
    // value is written verbatim into the exception section.
    let placeholder = first_non_exception_value::<F>(encoded, exception_positions);
    for &position in exception_positions.iter() {
        exception_values.push(values[position as usize]);
        encoded[position as usize] = placeholder;
    }

    let zero = F::Exact::default().reinterpret_as_signed();
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
    // stored. This is the stage a FastLanes-ordered `integer_encoding` would
    // replace.
    if bit_width > 0 {
        let mut writer = BitWriter::new_from_buf(std::mem::take(out));
        for &encoded_value in encoded.iter() {
            let delta = F::Exact::reinterpret_from_signed(encoded_value)
                .wrapping_sub(frame_of_reference);
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

/// Encoder for ALP-encoded floating-point pages (`f32`/`f64`).
///
/// Values are buffered and encoded on flush, because ALP chooses its decimal
/// parameters per vector by sampling the values, and the candidate set for the
/// column chunk is derived from the first page's data.
pub struct AlpEncoder<T: DataType>
where
    T::T: AlpFloat,
{
    /// Values buffered for the page currently being built.
    values: Vec<T::T>,
    /// Candidate `(exponent, factor)` pairs for this column chunk, sampled once
    /// from the first page and reused for the rest of the chunk.
    preset: Option<Vec<ExponentAndFactor>>,
    scratch: Scratch<T::T>,
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
        }
    }
}

impl<T: DataType> Encoder<T> for AlpEncoder<T>
where
    T::T: AlpFloat,
{
    fn put(&mut self, values: &[T::T]) -> Result<()> {
        self.values.extend_from_slice(values);
        Ok(())
    }

    fn encoding(&self) -> Encoding {
        Encoding::ALP
    }

    fn estimated_data_encoded_size(&self) -> usize {
        // Encoded size is not known until the parameters are chosen, so bound it
        // by the unencoded size: a vector never encodes larger than storing every
        // value as an exception.
        let num_vectors = self.values.len().div_ceil(VECTOR_SIZE);
        ALP_HEADER_SIZE
            + num_vectors
                * (std::mem::size_of::<u32>()
                    + AlpInfo::STORED_SIZE
                    + ForInfo::<<T::T as AlpFloat>::Exact>::stored_size())
            + self.values.len()
                * (<T::T as AlpFloat>::Exact::WIDTH + std::mem::size_of::<u16>())
    }

    fn estimated_memory_size(&self) -> usize {
        self.values.capacity() * std::mem::size_of::<T::T>()
            + self.preset.as_ref().map_or(0, |p| {
                p.capacity() * std::mem::size_of::<ExponentAndFactor>()
            })
            + self.scratch.estimated_memory_size()
    }

    fn flush_buffer(&mut self) -> Result<Bytes> {
        let values = &self.values;
        let preset = self.preset.get_or_insert_with(|| build_preset(values));
        let page = encode_page(&self.values, preset, &mut self.scratch)?;
        self.values.clear();
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
}

#[cfg(test)]
mod conformance {
    use super::*;
    use crate::column::page::Page;
    use crate::data_type::FloatType;
    use crate::encodings::decoding::Decoder;
    use crate::encodings::decoding::alp_decoder::AlpDecoder;
    use crate::file::reader::{FileReader, SerializedFileReader};

    /// Re-encode the values from a page written by the arrow-cpp ALP
    /// implementation and require our bytes to match theirs exactly.
    ///
    /// The parameter search is explicitly non-normative - the spec says any
    /// valid (exponent, factor) pair decodes correctly - so this is not a
    /// conformance requirement in itself. It is a much sharper test than one:
    /// byte identity pins the decimal parameters, the exception set, the
    /// placeholder substitution, the frame of reference, the bit width and the
    /// packing order all at once, against an independent implementation.
    #[test]
    fn test_matches_cpp_reference_page() {
        let path = format!(
            "{}/alp_float_arade.parquet",
            arrow::util::test_util::parquet_test_data()
        );
        let reader = SerializedFileReader::new(std::fs::File::open(&path).unwrap()).unwrap();
        let mut page_reader = reader
            .get_row_group(0)
            .unwrap()
            .get_column_page_reader(0)
            .unwrap();

        let mut pages_checked = 0;
        while let Some(page) = page_reader.get_next_page().unwrap() {
            let Page::DataPage {
                buf,
                num_values,
                encoding: Encoding::ALP,
                ..
            } = &page
            else {
                continue;
            };
            let num_values = *num_values as usize;

            // A v1 data page stores its definition levels ahead of the encoded
            // values: a 4-byte length, then that many bytes.
            let def_len = u32::from_le_bytes(buf.as_ref()[0..4].try_into().unwrap()) as usize;
            let cpp_page = buf.slice(4 + def_len..);

            let mut decoder = AlpDecoder::<FloatType>::new();
            decoder.set_data(cpp_page.clone(), num_values).unwrap();
            let mut values = vec![0.0f32; num_values];
            assert_eq!(decoder.get(&mut values).unwrap(), num_values);

            let mut encoder = AlpEncoder::<FloatType>::new();
            encoder.put(&values).unwrap();
            let our_page = encoder.flush_buffer().unwrap();

            assert_eq!(
                our_page.as_ref(),
                cpp_page.as_ref(),
                "re-encoded page differs from the arrow-cpp reference page"
            );
            pages_checked += 1;
        }
        assert!(pages_checked > 0, "no ALP data page found in the test file");
    }
}
