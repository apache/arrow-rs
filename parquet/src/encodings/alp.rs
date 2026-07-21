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

//! ALP (Adaptive Lossless floating-Point) Encoding
//!
//! Based on the draft Parquet spec: <https://github.com/apache/parquet-format/pull/557>
//!
//! # Page layout
//!
//! An ALP-encoded page consists of a fixed-size header, an offset array
//! locating each vector inside the body, and the vector data itself:
//!
//! ```text
//! +-------------+-----------------------------+--------------------------------------+
//! |   Header    |        Offset Array         |            Vector Data               |
//! |  (7 bytes)  |   (num_vectors * 4 bytes)   |            (variable)                |
//! +-------------+------+------+-----+---------+----------+----------+-----+----------+
//! | Page Header | off0 | off1 | ... | off N-1 | Vector 0 | Vector 1 | ... | Vec N-1  |
//! |  (7 bytes)  | (4B) | (4B) |     |  (4B)   |(variable)|(variable)|     |(variable)|
//! +-------------+------+------+-----+---------+----------+----------+-----+----------+
//! ```
//!
//! Each vector entry has the form
//! `[AlpInfo][ForInfo][PackedValues][ExceptionPositions][ExceptionValues]`.

use crate::errors::{ParquetError, Result};
use crate::util::bit_util::{FromBitpacked, FromBytes};

pub(crate) const ALP_HEADER_SIZE: usize = 7;
pub(crate) const ALP_COMPRESSION_MODE: u8 = 0;
pub(crate) const ALP_INTEGER_ENCODING_FOR_BIT_PACK: u8 = 0;
pub(crate) const ALP_MIN_LOG_VECTOR_SIZE: u8 = 3;
pub(crate) const ALP_MAX_LOG_VECTOR_SIZE: u8 = 15;
/// Spec-recommended default `log_vector_size`: 1024-value vectors, the canonical
/// ALP/FastLanes vector size.
pub(crate) const ALP_DEFAULT_LOG_VECTOR_SIZE: u8 = 10;
pub(crate) const ALP_MAX_EXPONENT_F32: u8 = 10;
pub(crate) const ALP_MAX_EXPONENT_F64: u8 = 18;

/// Page-level ALP header (7 bytes).
///
/// ```text
/// Byte:    0              1               2              3    4    5    6
/// +----------------+---------------+--------------+----+----+----+----+
/// | compression    | integer       | log_vector   |     num_elements  |
/// | _mode          | _encoding     | _size        |     (int32 LE)    |
/// +----------------+---------------+--------------+----+----+----+----+
/// ```
///
/// Layout in bytes:
/// - `[0]` `compression_mode`
/// - `[1]` `integer_encoding`
/// - `[2]` `log_vector_size`
/// - `[3..7]` `num_elements` (little-endian `i32`)
///
/// The fields hold the *decoded* values used throughout the decoder, not the
/// raw on-disk encoding:
/// - `num_elements` is stored on disk as an `i32`, kept in memory as a `usize`.
/// - vector size is stored on disk as a `u8` `log_vector_size`, kept in memory
///   as the actual `vector_size` (`1 << log_vector_size`) `usize`.
///
/// Each conversion happens once, in [`AlpHeader::deserialize`] and
/// [`AlpHeader::serialize`], so the rest of the decoder computes offsets and
/// sizes in `usize`. Those methods reject only what the target type cannot
/// represent; spec-level validity, such as the allowed vector-size range, is
/// enforced by the page parser.
#[derive(Debug, Clone, Copy)]
pub(crate) struct AlpHeader {
    pub(crate) compression_mode: u8,
    pub(crate) integer_encoding: u8,
    pub(crate) vector_size: usize,
    pub(crate) num_elements: usize,
}

impl AlpHeader {
    /// Parse a 7-byte page header from its little-endian on-disk form,
    /// converting each field to its in-memory type:
    /// - `log_vector_size` (`u8`) is expanded to `vector_size` with an
    ///   overflow-checked shift.
    /// - `num_elements` (`i32`) is checked for non-negativity.
    pub(crate) fn deserialize(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < ALP_HEADER_SIZE {
            return Err(general_err!(
                "Invalid ALP page: expected at least {} bytes for header, got {}",
                ALP_HEADER_SIZE,
                bytes.len()
            ));
        }

        let log_vector_size = bytes[2];
        let vector_size = 1usize
            .checked_shl(u32::from(log_vector_size))
            .ok_or_else(|| {
                general_err!(
                    "Invalid ALP page: log_vector_size {} too large to represent a vector size",
                    log_vector_size
                )
            })?;

        let num_elements_i32 = i32::from_le_bytes([bytes[3], bytes[4], bytes[5], bytes[6]]);
        let num_elements = usize::try_from(num_elements_i32).map_err(|_| {
            general_err!(
                "Invalid ALP page: num_elements {} must be >= 0",
                num_elements_i32
            )
        })?;

        Ok(Self {
            compression_mode: bytes[0],
            integer_encoding: bytes[1],
            vector_size,
            num_elements,
        })
    }

    /// Serialize this header into its 7-byte little-endian on-disk form.
    ///
    /// Converts the in-memory values back to the on-disk encoding, rejecting
    /// what cannot be represented: `vector_size` must be a power of two (its log
    /// is the on-disk field), and `num_elements` must fit in an `i32`.
    /// Counterpart to [`AlpHeader::deserialize`]; consumed by the ALP encoder.
    pub(crate) fn serialize(&self) -> Result<[u8; ALP_HEADER_SIZE]> {
        if !self.vector_size.is_power_of_two() {
            return Err(general_err!(
                "Invalid ALP page: vector_size {} is not a power of two",
                self.vector_size
            ));
        }
        let log_vector_size = self.vector_size.trailing_zeros() as u8;

        let num_elements = i32::try_from(self.num_elements).map_err(|_| {
            general_err!(
                "Invalid ALP page: num_elements {} exceeds i32::MAX",
                self.num_elements
            )
        })?;

        let mut out = [0u8; ALP_HEADER_SIZE];
        out[0] = self.compression_mode;
        out[1] = self.integer_encoding;
        out[2] = log_vector_size;
        out[3..7].copy_from_slice(&num_elements.to_le_bytes());
        Ok(out)
    }

    /// `vector_size` is always `1 << log_vector_size` (see [`AlpHeader::deserialize`]),
    /// so the division a `div_ceil` would emit is a shift. `vector_size` is a
    /// runtime value, so the compiler cannot see that on its own.
    pub(crate) fn num_vectors(&self) -> usize {
        debug_assert!(self.vector_size.is_power_of_two());
        (self.num_elements + self.vector_size - 1) >> self.vector_size.trailing_zeros()
    }

    /// Number of elements in vector `vector_index`: a full vector, the short
    /// trailing remainder, or zero past the end of the page.
    pub(crate) fn vector_num_elements(&self, vector_index: usize) -> u16 {
        let start = vector_index.saturating_mul(self.vector_size);
        let remaining = self.num_elements.saturating_sub(start);
        remaining.min(self.vector_size) as u16
    }
}

/// Per-vector ALP metadata (4 bytes), equivalent to C++ `AlpEncodedVectorInfo`.
///
/// ##### AlpInfo (4 bytes, both types)
///
/// ```text
///  Byte:    0           1          2       3
///        +----------+----------+---------+---------+
///        | exponent |  factor  |  num_exceptions   |
///        |  (uint8) | (uint8)  |   (uint16 LE)     |
///        +----------+----------+---------+---------+
/// ```
#[derive(Debug, Clone, Copy)]
pub(crate) struct AlpInfo {
    pub(crate) exponent: u8,
    pub(crate) factor: u8,
    pub(crate) num_exceptions: u16,
}

impl AlpInfo {
    pub(crate) const STORED_SIZE: usize = 4;

    /// Append this vector's ALP metadata in its on-disk little-endian form.
    pub(crate) fn extend_serialized(&self, out: &mut Vec<u8>) {
        out.push(self.exponent);
        out.push(self.factor);
        out.extend_from_slice(&self.num_exceptions.to_le_bytes());
    }
}

/// Per-vector FOR metadata for exact integer type (`u32` for `f32`, `u64` for `f64`).
///
/// Frame of reference (FOR) encoding
///
/// ###### ForInfo for FLOAT (5 bytes) / DOUBLE (9 bytes)
///
/// ```text
/// Byte:    0    1    2    3       4
/// +----+----+----+----+-----------+
/// | frame_of_reference | bit_width |
/// |    (int32 LE)      |  (uint8)  |
/// +----+----+----+----+-----------+
/// ```
#[derive(Debug, Clone, Copy)]
pub(crate) struct ForInfo<Exact: AlpExact> {
    pub(crate) frame_of_reference: Exact,
    pub(crate) bit_width: u8,
}

impl<Exact: AlpExact> ForInfo<Exact> {
    pub(crate) fn stored_size() -> usize {
        Exact::WIDTH + 1
    }

    /// Append this vector's FOR metadata in its on-disk little-endian form.
    pub(crate) fn extend_serialized(&self, out: &mut Vec<u8>) {
        self.frame_of_reference.extend_le_bytes(out);
        out.push(self.bit_width);
    }

    pub(crate) fn get_bit_packed_size(&self, num_elements: u16) -> usize {
        (self.bit_width as usize * num_elements as usize).div_ceil(8)
    }

    pub(crate) fn get_data_stored_size(&self, num_elements: u16, num_exceptions: u16) -> usize {
        let bit_packed_size = self.get_bit_packed_size(num_elements);
        bit_packed_size
            + num_exceptions as usize * std::mem::size_of::<u16>()
            + num_exceptions as usize * Exact::WIDTH
    }
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
pub(crate) trait AlpExact: Copy + std::fmt::Debug + PartialEq + FromBitpacked + Default {
    const WIDTH: usize;
    type Signed: Copy + Ord + std::fmt::Debug + Send;
    fn from_le_slice(slice: &[u8]) -> Self;
    fn wrapping_add(self, rhs: Self) -> Self;
    fn wrapping_sub(self, rhs: Self) -> Self;
    fn reinterpret_as_signed(self) -> Self::Signed;
    fn reinterpret_from_signed(signed: Self::Signed) -> Self;
    /// Widen to `u64` for bit-packing, which is `u64`-oriented throughout.
    fn to_u64(self) -> u64;
    fn extend_le_bytes(self, out: &mut Vec<u8>);
}

impl AlpExact for u32 {
    const WIDTH: usize = 4;
    type Signed = i32;

    fn from_le_slice(slice: &[u8]) -> Self {
        u32::from_le_bytes([slice[0], slice[1], slice[2], slice[3]])
    }

    fn wrapping_add(self, rhs: Self) -> Self {
        self.wrapping_add(rhs)
    }

    fn wrapping_sub(self, rhs: Self) -> Self {
        self.wrapping_sub(rhs)
    }

    fn reinterpret_as_signed(self) -> Self::Signed {
        i32::from_ne_bytes(self.to_ne_bytes())
    }

    fn reinterpret_from_signed(signed: Self::Signed) -> Self {
        u32::from_ne_bytes(signed.to_ne_bytes())
    }

    fn to_u64(self) -> u64 {
        u64::from(self)
    }

    fn extend_le_bytes(self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.to_le_bytes());
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

    fn wrapping_add(self, rhs: Self) -> Self {
        self.wrapping_add(rhs)
    }

    fn wrapping_sub(self, rhs: Self) -> Self {
        self.wrapping_sub(rhs)
    }

    fn reinterpret_as_signed(self) -> Self::Signed {
        i64::from_ne_bytes(self.to_ne_bytes())
    }

    fn reinterpret_from_signed(signed: Self::Signed) -> Self {
        u64::from_ne_bytes(signed.to_ne_bytes())
    }

    fn to_u64(self) -> u64 {
        self
    }

    fn extend_le_bytes(self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.to_le_bytes());
    }
}
pub(crate) const ALP_POW10_F32: [f32; 11] = [
    1.0,
    10.0,
    100.0,
    1000.0,
    10000.0,
    100000.0,
    1000000.0,
    10000000.0,
    100000000.0,
    1000000000.0,
    10000000000.0,
];

pub(crate) const ALP_POW10_F64: [f64; 19] = [
    1.0,
    10.0,
    100.0,
    1000.0,
    10000.0,
    100000.0,
    1000000.0,
    10000000.0,
    100000000.0,
    1000000000.0,
    10000000000.0,
    100000000000.0,
    1000000000000.0,
    10000000000000.0,
    100000000000000.0,
    1000000000000000.0,
    10000000000000000.0,
    100000000000000000.0,
    1000000000000000000.0,
];

pub(crate) const ALP_NEG_POW10_F32: [f32; 11] = [
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

pub(crate) const ALP_NEG_POW10_F64: [f64; 19] = [
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

pub(crate) trait AlpFloat: Copy + Default + PartialEq + std::ops::Mul<Output = Self> {
    type Exact: AlpExact + FromBytes;
    type Scale: Copy + Send;

    /// Largest `exponent` this type admits: 10 for `f32`, 18 for `f64`.
    const MAX_EXPONENT: u8;

    /// Rounding magic number: `2^22 + 2^23` (`f32`) or `2^51 + 2^52` (`f64`).
    const MAGIC_NUMBER: Self;

    /// Bounds outside which the scaled value cannot reach the exact integer
    /// type: `i32` for `f32`, `i64` for `f64`.
    const ENCODING_UPPER_LIMIT: Self;
    const ENCODING_LOWER_LIMIT: Self;

    /// [`AlpFloat::ENCODING_UPPER_LIMIT`] as the exact signed integer. Stands in
    /// for values ALP cannot represent, so that the round-trip check that
    /// follows fails and the value is recorded as an exception.
    const ENCODING_SENTINEL: <Self::Exact as AlpExact>::Signed;

    /// Precompute vector-level ALP decimal scale constants for:
    /// `value = (encoded * 10^(factor)) * 10^(-exponent)`.
    ///
    /// Preconditions are validated during page parse.
    fn decode_scale(exponent: u8, factor: u8) -> Self::Scale;

    /// Decode one signed exact integer using a precomputed two-step scale.
    fn decode_value(signed_encoded: <Self::Exact as AlpExact>::Signed, scale: Self::Scale) -> Self;

    fn from_exact_bits(bits: Self::Exact) -> Self;

    fn to_exact_bits(self) -> Self::Exact;

    /// Precompute vector-level ALP decimal scale constants for the encode
    /// direction: `encoded = fast_round((value * 10^(exponent)) * 10^(-factor))`.
    fn encode_scale(exponent: u8, factor: u8) -> Self::Scale;

    /// Apply a scale as the same two separate multiplications the decode side
    /// uses. The spec requires two steps rather than one multiplication by a
    /// combined constant, so that every implementation produces bit-identical
    /// results.
    fn apply_scale(self, scale: Self::Scale) -> Self;

    /// True for values ALP cannot turn into an exact integer: NaN, the
    /// infinities, anything scaled past the exact integer type, and `-0.0`
    /// (which would come back as `+0.0` and lose its sign).
    fn is_impossible_to_encode(self) -> bool;

    /// Round to the nearest integer by the "magic number" technique: adding and
    /// then subtracting the magic number lands the value in a binade where the
    /// unit in the last place is exactly 1.0, so the fractional bits fall off.
    ///
    /// The sign branch is load-bearing, not a micro-optimization: negative
    /// values must use `- magic + magic` to land in the corresponding negative
    /// binade. Nor can the `+ magic - magic` be algebraically cancelled - the
    /// rounding *is* the intermediate loss of precision.
    fn fast_round(self) -> <Self::Exact as AlpExact>::Signed;

    /// Encode one value with a precomputed [`AlpFloat::encode_scale`].
    ///
    /// Values ALP cannot represent map to [`AlpFloat::ENCODING_SENTINEL`], whose
    /// round trip is guaranteed to mismatch, so the caller's `decode == value`
    /// check records them as exceptions without a separate test.
    fn encode_value(self, scale: Self::Scale) -> <Self::Exact as AlpExact>::Signed {
        let scaled = self.apply_scale(scale);
        if scaled.is_impossible_to_encode() {
            return Self::ENCODING_SENTINEL;
        }
        scaled.fast_round()
    }
}

impl AlpFloat for f32 {
    type Exact = u32;
    type Scale = (f32, f32);

    const MAX_EXPONENT: u8 = ALP_MAX_EXPONENT_F32;
    const MAGIC_NUMBER: Self = 12582912.0; // 2^22 + 2^23
    const ENCODING_UPPER_LIMIT: Self = 2147483520.0;
    const ENCODING_LOWER_LIMIT: Self = -2147483520.0;
    const ENCODING_SENTINEL: i32 = 2147483520;

    fn decode_scale(exponent: u8, factor: u8) -> Self::Scale {
        debug_assert!(exponent <= ALP_MAX_EXPONENT_F32);
        debug_assert!(factor <= exponent);
        (
            ALP_POW10_F32[factor as usize],
            ALP_NEG_POW10_F32[exponent as usize],
        )
    }

    fn decode_value(signed_encoded: i32, scale: Self::Scale) -> Self {
        ((signed_encoded as f32) * scale.0) * scale.1
    }

    fn from_exact_bits(bits: Self::Exact) -> Self {
        f32::from_bits(bits)
    }

    fn to_exact_bits(self) -> Self::Exact {
        self.to_bits()
    }

    fn encode_scale(exponent: u8, factor: u8) -> Self::Scale {
        debug_assert!(exponent <= ALP_MAX_EXPONENT_F32);
        debug_assert!(factor <= exponent);
        (
            ALP_POW10_F32[exponent as usize],
            ALP_NEG_POW10_F32[factor as usize],
        )
    }

    fn apply_scale(self, scale: Self::Scale) -> Self {
        (self * scale.0) * scale.1
    }

    fn is_impossible_to_encode(self) -> bool {
        // The infinities need no separate test: they fall outside the limits.
        self.is_nan()
            || !(Self::ENCODING_LOWER_LIMIT..=Self::ENCODING_UPPER_LIMIT).contains(&self)
            || (self == 0.0 && self.is_sign_negative())
    }

    fn fast_round(self) -> i32 {
        let rounded = if self >= 0.0 {
            (self + Self::MAGIC_NUMBER) - Self::MAGIC_NUMBER
        } else {
            (self - Self::MAGIC_NUMBER) + Self::MAGIC_NUMBER
        };
        rounded as i32
    }
}

impl AlpFloat for f64 {
    type Exact = u64;
    type Scale = (f64, f64);

    const MAX_EXPONENT: u8 = ALP_MAX_EXPONENT_F64;
    const MAGIC_NUMBER: Self = 6755399441055744.0; // 2^51 + 2^52
    const ENCODING_UPPER_LIMIT: Self = 9223372036854774784.0;
    const ENCODING_LOWER_LIMIT: Self = -9223372036854774784.0;
    const ENCODING_SENTINEL: i64 = 9223372036854774784;

    fn decode_scale(exponent: u8, factor: u8) -> Self::Scale {
        debug_assert!(exponent <= ALP_MAX_EXPONENT_F64);
        debug_assert!(factor <= exponent);
        (
            ALP_POW10_F64[factor as usize],
            ALP_NEG_POW10_F64[exponent as usize],
        )
    }

    fn decode_value(signed_encoded: i64, scale: Self::Scale) -> Self {
        ((signed_encoded as f64) * scale.0) * scale.1
    }

    fn from_exact_bits(bits: Self::Exact) -> Self {
        f64::from_bits(bits)
    }

    fn to_exact_bits(self) -> Self::Exact {
        self.to_bits()
    }

    fn encode_scale(exponent: u8, factor: u8) -> Self::Scale {
        debug_assert!(exponent <= ALP_MAX_EXPONENT_F64);
        debug_assert!(factor <= exponent);
        (
            ALP_POW10_F64[exponent as usize],
            ALP_NEG_POW10_F64[factor as usize],
        )
    }

    fn apply_scale(self, scale: Self::Scale) -> Self {
        (self * scale.0) * scale.1
    }

    fn is_impossible_to_encode(self) -> bool {
        // The infinities need no separate test: they fall outside the limits.
        self.is_nan()
            || !(Self::ENCODING_LOWER_LIMIT..=Self::ENCODING_UPPER_LIMIT).contains(&self)
            || (self == 0.0 && self.is_sign_negative())
    }

    fn fast_round(self) -> i64 {
        let rounded = if self >= 0.0 {
            (self + Self::MAGIC_NUMBER) - Self::MAGIC_NUMBER
        } else {
            (self - Self::MAGIC_NUMBER) + Self::MAGIC_NUMBER
        };
        rounded as i64
    }
}
