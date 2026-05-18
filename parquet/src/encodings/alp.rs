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

use crate::util::bit_util::{FromBitpacked, FromBytes};

pub(crate) const ALP_HEADER_SIZE: usize = 7;
pub(crate) const ALP_COMPRESSION_MODE: u8 = 0;
pub(crate) const ALP_INTEGER_ENCODING_FOR_BIT_PACK: u8 = 0;
pub(crate) const ALP_MIN_LOG_VECTOR_SIZE: u8 = 3;
pub(crate) const ALP_MAX_LOG_VECTOR_SIZE: u8 = 15;
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
#[derive(Debug, Clone, Copy)]
pub(crate) struct AlpHeader {
    pub(crate) compression_mode: u8,
    pub(crate) integer_encoding: u8,
    pub(crate) log_vector_size: u8,
    pub(crate) num_elements: i32,
}

impl AlpHeader {
    pub(crate) fn num_elements_usize(&self) -> usize {
        self.num_elements as usize
    }

    fn vector_size(&self) -> usize {
        1usize << self.log_vector_size
    }

    pub(crate) fn num_vectors(&self) -> usize {
        if self.num_elements == 0 {
            0
        } else {
            self.num_elements_usize().div_ceil(self.vector_size())
        }
    }

    pub(crate) fn vector_num_elements(&self, vector_index: usize) -> u16 {
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
pub(crate) trait AlpExact: Copy + std::fmt::Debug + FromBitpacked {
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

pub(crate) trait AlpFloat: Copy + Default {
    type Exact: AlpExact + FromBytes;
    type Scale: Copy;

    /// Precompute vector-level ALP decimal scale constants for:
    /// `value = (encoded * 10^(factor)) * 10^(-exponent)`.
    ///
    /// Preconditions are validated during page parse.
    fn decode_scale(exponent: u8, factor: u8) -> Self::Scale;

    /// Decode one signed exact integer using a precomputed two-step scale.
    fn decode_value(signed_encoded: <Self::Exact as AlpExact>::Signed, scale: Self::Scale) -> Self;

    fn from_exact_bits(bits: Self::Exact) -> Self;
}

impl AlpFloat for f32 {
    type Exact = u32;
    type Scale = (f32, f32);

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
}

impl AlpFloat for f64 {
    type Exact = u64;
    type Scale = (f64, f64);

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
}
