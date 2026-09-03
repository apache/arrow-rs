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

//! Wire format shared by the PFOR encoder and decoder.
//!
//! PFOR (Patched Frame of Reference) compresses an integer column in fixed-size vectors. Each
//! vector subtracts a frame of reference from its values, bit-packs the residuals at a width
//! chosen by a cost model, and stores the values that do not fit at that width separately, as
//! exceptions patched back in on the way out.
//!
//! A page is laid out as
//!
//! ```text
//! [header: 7 bytes] [offset array: num_vectors * 4 bytes] [vector 0] [vector 1] ...
//! ```
//!
//! and each vector as
//!
//! ```text
//! [info block] [start value, only when differencing] [packed residuals]
//! [exception positions] [exception values]
//! ```
//!
//! Every multi-byte field is little-endian.

use crate::errors::{ParquetError, Result};
use crate::util::bit_util::FromBitpacked;

/// Number of elements compressed together as a unit, by default.
pub const DEFAULT_VECTOR_SIZE: usize = 1 << DEFAULT_LOG_VECTOR_SIZE;

/// `log2` of [`DEFAULT_VECTOR_SIZE`].
pub const DEFAULT_LOG_VECTOR_SIZE: u8 = 10;

/// Smallest vector the page header can describe, as a log.
pub const MIN_LOG_VECTOR_SIZE: u8 = 3;

/// Largest vector the page header can describe, as a log.
pub const MAX_LOG_VECTOR_SIZE: u8 = 15;

/// Largest vector the format allows, in elements.
pub const MAX_VECTOR_SIZE: usize = 1 << MAX_LOG_VECTOR_SIZE;

/// Width of one entry in the per-page offset array.
pub const OFFSET_SIZE: usize = std::mem::size_of::<u32>();

/// Width of one exception position.
pub const POSITION_SIZE: usize = std::mem::size_of::<u16>();

/// Page header size in bytes: `packing_mode`, `log_vector_size` and `value_byte_width` one byte
/// each, then `num_elements` as a four-byte little-endian signed integer.
pub const HEADER_SIZE: usize = 3 + std::mem::size_of::<i32>();

/// Packing mode: frame of reference plus bit-packing, currently the only mode.
pub const PACKING_MODE_FOR_BIT_PACK: u8 = 0;

/// Mask selecting the bit width out of the info block's width byte.
pub const BIT_WIDTH_MASK: u8 = 0x7F;

/// Bit of the info block's width byte that marks a differenced vector.
pub const DELTA_FLAG: u8 = 0x80;

/// Bits an exception costs on the wire: its position plus a full-width value.
pub const fn exception_bits(byte_width: usize) -> i64 {
    ((POSITION_SIZE + byte_width) * 8) as i64
}

/// The integer types PFOR encodes, and the arithmetic the codec needs from them.
///
/// PFOR does all of its arithmetic in the unsigned counterpart of the value type. Subtracting the
/// frame wraps rather than overflows, which is what lets a frame sit above the minimum: a value
/// below the frame wraps to a huge residual, fails the width test like any value above the window,
/// and is patched from the exception list. Nothing here is exposed outside the crate.
pub trait PforInt: Copy + Default + Ord + Send + std::fmt::Debug + FromBitpacked + 'static {
    /// Width of one value on the wire, in bytes: 4 for INT32, 8 for INT64.
    const BYTE_WIDTH: usize;

    /// Widest bit width the type can pack at, and the largest the width byte may hold.
    const MAX_BIT_WIDTH: u8;

    /// Bytes the info block occupies, start value excluded: frame, width byte, exception count.
    const INFO_SIZE: usize = Self::BYTE_WIDTH + 1 + std::mem::size_of::<u16>();

    /// Reinterpret as unsigned. Bit-preserving.
    /// Mask of the bits a residual can occupy, which is the type's own width.
    const RESIDUAL_MASK: u64 = low_mask(Self::MAX_BIT_WIDTH);

    fn to_bits(self) -> u64;

    /// Reinterpret from unsigned, taking the low [`Self::BYTE_WIDTH`] bytes. Bit-preserving.
    fn from_bits(bits: u64) -> Self;

    /// Read one value from the first [`Self::BYTE_WIDTH`] bytes of `bytes`, little-endian.
    ///
    /// # Panics
    ///
    /// Panics if `bytes` is shorter than [`Self::BYTE_WIDTH`]. Callers check the length of a
    /// vector's sections against the buffer before reading any of them.
    fn read_le(bytes: &[u8]) -> Self;

    /// Append this value to `out`, little-endian.
    fn write_le(self, out: &mut Vec<u8>);

    /// `self - frame_bits`, in the type's own width.
    ///
    /// PFOR's residual test is a single unsigned comparison, and it only works if the subtraction
    /// wraps at the column's width: on an INT32 column a value below the frame has to wrap to a
    /// 32-bit residual. Wrapping at 64 bits instead puts it beyond every width the type has, which
    /// makes even the full-width candidate look like it needs exceptions -- and that candidate
    /// costing no more than the values unpacked is what bounds the size of a page.
    #[inline]
    fn residual_from(self, frame_bits: u64) -> u64 {
        self.to_bits().wrapping_sub(frame_bits) & Self::RESIDUAL_MASK
    }
}

impl PforInt for i32 {
    const BYTE_WIDTH: usize = 4;
    const MAX_BIT_WIDTH: u8 = 32;

    #[inline]
    fn to_bits(self) -> u64 {
        self as u32 as u64
    }

    #[inline]
    fn from_bits(bits: u64) -> Self {
        bits as u32 as i32
    }

    #[inline]
    fn read_le(bytes: &[u8]) -> Self {
        i32::from_le_bytes(bytes[..4].try_into().unwrap())
    }

    #[inline]
    fn write_le(self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.to_le_bytes());
    }
}

impl PforInt for i64 {
    const BYTE_WIDTH: usize = 8;
    const MAX_BIT_WIDTH: u8 = 64;

    #[inline]
    fn to_bits(self) -> u64 {
        self as u64
    }

    #[inline]
    fn from_bits(bits: u64) -> Self {
        bits as i64
    }

    #[inline]
    fn read_le(bytes: &[u8]) -> Self {
        i64::from_le_bytes(bytes[..8].try_into().unwrap())
    }

    #[inline]
    fn write_le(self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.to_le_bytes());
    }
}

/// Bits needed to hold `value`, i.e. the position of its highest set bit. Zero needs none.
#[inline]
pub fn bits_required(value: u64) -> u8 {
    (u64::BITS - value.leading_zeros()) as u8
}

/// Mask of the low `bit_width` bits, saturating at all ones.
#[inline]
pub const fn low_mask(bit_width: u8) -> u64 {
    if bit_width >= 64 {
        u64::MAX
    } else {
        (1u64 << bit_width) - 1
    }
}

/// Bytes needed to hold `bits`, rounded up.
#[inline]
pub fn bytes_for_bits(bits: usize) -> usize {
    bits.div_ceil(8)
}

/// Accept only the vector sizes the page header can describe.
///
/// A size outside this range would encode a page that the decoder then rejects, so refuse it up
/// front.
pub fn validate_vector_size(vector_size: usize) -> Result<u8> {
    if !vector_size.is_power_of_two()
        || !((1 << MIN_LOG_VECTOR_SIZE)..=MAX_VECTOR_SIZE).contains(&vector_size)
    {
        return Err(general_err!(
            "PFOR vector_size must be a power of two in [{}, {}]: {}",
            1 << MIN_LOG_VECTOR_SIZE,
            MAX_VECTOR_SIZE,
            vector_size
        ));
    }
    Ok(vector_size.trailing_zeros() as u8)
}

/// The per-page header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PforHeader {
    /// How the residuals are laid out. Always [`PACKING_MODE_FOR_BIT_PACK`] today.
    pub packing_mode: u8,
    /// `log2` of the number of elements per vector.
    pub log_vector_size: u8,
    /// Width of one value on the wire, in bytes.
    pub value_byte_width: u8,
    /// Total number of values the page holds, nulls excluded.
    pub num_elements: i32,
}

impl PforHeader {
    /// Number of elements per vector.
    pub fn vector_size(&self) -> usize {
        1usize << self.log_vector_size
    }

    /// Number of vectors the page holds.
    pub fn num_vectors(&self) -> usize {
        (self.num_elements as usize).div_ceil(self.vector_size())
    }

    /// Append this header to `out`.
    pub fn write(&self, out: &mut Vec<u8>) {
        out.push(self.packing_mode);
        out.push(self.log_vector_size);
        out.push(self.value_byte_width);
        out.extend_from_slice(&self.num_elements.to_le_bytes());
    }

    /// Read and validate a header from the front of `src`.
    ///
    /// Every field is checked here rather than where it is used, because each of them sizes a read
    /// further down: the packing mode selects the layout, the byte width has to match the column's
    /// type, the vector size divides the page into vectors, and the element count bounds the
    /// output.
    pub fn read<T: PforInt>(src: &[u8]) -> Result<Self> {
        if src.len() < HEADER_SIZE {
            return Err(general_err!(
                "PFOR compressed buffer too small for header: {} < {}",
                src.len(),
                HEADER_SIZE
            ));
        }
        let header = PforHeader {
            packing_mode: src[0],
            log_vector_size: src[1],
            value_byte_width: src[2],
            num_elements: i32::from_le_bytes(src[3..7].try_into().unwrap()),
        };
        if header.packing_mode != PACKING_MODE_FOR_BIT_PACK {
            return Err(general_err!(
                "PFOR unsupported packing mode: {}",
                header.packing_mode
            ));
        }
        if header.value_byte_width as usize != T::BYTE_WIDTH {
            return Err(general_err!(
                "PFOR value_byte_width mismatch: {} vs expected {}",
                header.value_byte_width,
                T::BYTE_WIDTH
            ));
        }
        if header.log_vector_size < MIN_LOG_VECTOR_SIZE
            || header.log_vector_size > MAX_LOG_VECTOR_SIZE
        {
            return Err(general_err!(
                "PFOR invalid log_vector_size: {}",
                header.log_vector_size
            ));
        }
        if header.num_elements < 0 {
            return Err(general_err!(
                "PFOR invalid num_elements: {}",
                header.num_elements
            ));
        }
        Ok(header)
    }
}

/// The per-vector info block.
///
/// For INT32 it is 7 bytes, for INT64 11:
///
/// ```text
/// [frame of reference: 4 or 8 bytes] [width byte] [exception count: 2 bytes]
/// ```
///
/// The width byte holds the bit width in bits 0..6 and the differencing flag in bit 7. A vector
/// with the flag set packs the backward differences of its values rather than the values, and
/// carries one extra full-width field after this block -- the vector's first value -- so that it
/// still decodes without reading the vector before it.
///
/// Seven bits for the width, not six: the range is 0..64 inclusive, and 64 does not fit in six. A
/// six-bit field stored an INT64 vector whose differences need the full 64 bits as width 0, and
/// since such a vector has no exceptions either, the decoder read it as a constant vector and
/// filled the output with the frame of reference -- silently, with no error and no size mismatch.
///
/// The exception count is unsigned: a vector holds up to [`MAX_VECTOR_SIZE`] elements and every
/// one of them can be an exception, so a count of 32768 has to be representable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PforVectorInfo<T> {
    /// Subtracted from every element before packing; added back on decode.
    pub frame_of_reference: T,
    /// Bits each packed residual occupies. Zero means the vector packs nothing at all.
    pub bit_width: u8,
    /// Number of values that did not fit at `bit_width` and travel as exceptions.
    pub num_exceptions: u16,
    /// Whether the packed residuals are backward differences rather than values.
    pub is_delta: bool,
}

impl<T: PforInt> PforVectorInfo<T> {
    /// Bytes this info occupies on the wire, start value included.
    ///
    /// Not a constant: the start value is only present on a differenced vector. Paying for it
    /// unconditionally would be free at a 1024-value vector and ruinous at the smallest one the
    /// format allows, where eight bytes across eight values is a byte per value.
    pub fn stored_bytes(&self) -> usize {
        T::INFO_SIZE + if self.is_delta { T::BYTE_WIDTH } else { 0 }
    }

    /// Append this info to `out`.
    ///
    /// # Panics
    ///
    /// Panics on a width above the type's maximum. Such a width would lose its high bits to the
    /// mask and set the differencing flag on the way out, turning an encoder bug into a page that
    /// decodes to the wrong values with no error anywhere. [`Self::read`] rejects it on the way
    /// in; this catches it on the way out, where the bug is.
    pub fn write(&self, out: &mut Vec<u8>) {
        assert!(
            self.bit_width <= T::MAX_BIT_WIDTH,
            "PFOR bit_width {} exceeds the {} the type allows",
            self.bit_width,
            T::MAX_BIT_WIDTH
        );
        self.frame_of_reference.write_le(out);
        out.push((self.bit_width & BIT_WIDTH_MASK) | if self.is_delta { DELTA_FLAG } else { 0 });
        out.extend_from_slice(&self.num_exceptions.to_le_bytes());
    }

    /// Read an info block from the front of `src`.
    pub fn read(src: &[u8]) -> Result<Self> {
        if src.len() < T::INFO_SIZE {
            return Err(general_err!(
                "PFOR vector info buffer too small: {} < {}",
                src.len(),
                T::INFO_SIZE
            ));
        }
        let frame_of_reference = T::read_le(src);
        let width_byte = src[T::BYTE_WIDTH];
        let info = PforVectorInfo {
            frame_of_reference,
            bit_width: width_byte & BIT_WIDTH_MASK,
            num_exceptions: u16::from_le_bytes(
                src[T::BYTE_WIDTH + 1..T::BYTE_WIDTH + 3]
                    .try_into()
                    .unwrap(),
            ),
            is_delta: width_byte & DELTA_FLAG != 0,
        };
        if info.bit_width > T::MAX_BIT_WIDTH {
            return Err(general_err!(
                "PFOR bit_width out of range: {}",
                info.bit_width
            ));
        }
        // A count above the largest vector the format allows cannot be honest, whatever this
        // vector's own element count turns out to be, and rejecting it here keeps the check off
        // the per-vector path.
        if info.num_exceptions as usize > MAX_VECTOR_SIZE {
            return Err(general_err!(
                "PFOR num_exceptions exceeds the maximum vector size: {}",
                info.num_exceptions
            ));
        }
        Ok(info)
    }
}

/// Check the whole offset array before any of it steers a read.
///
/// The offsets are byte counts from the start of the offset array, and the vectors they point at
/// were written back to back in order, so a well-formed page has the first offset landing just
/// past the array and the rest strictly increasing. Checking the chain as a whole, before decoding
/// any of it, keeps a page whose offsets overlap or run backwards from decoding part-way and
/// emitting values built out of the wrong bytes.
pub fn validate_offsets(offsets: &[u8], num_vectors: usize, payload_size: usize) -> Result<()> {
    let offset_array_size = num_vectors * OFFSET_SIZE;
    let mut previous = 0usize;
    for v in 0..num_vectors {
        let offset = read_offset(offsets, v);
        if v == 0 {
            if offset != offset_array_size {
                return Err(general_err!(
                    "PFOR first vector offset {} does not follow the {} byte offset array",
                    offset,
                    offset_array_size
                ));
            }
        } else if offset <= previous {
            return Err(general_err!(
                "PFOR vector {} offset {} does not follow offset {} of the vector before it",
                v,
                offset,
                previous
            ));
        }
        if offset >= payload_size {
            return Err(general_err!(
                "PFOR vector {} offset {} is past the end of the {} byte payload",
                v,
                offset,
                payload_size
            ));
        }
        previous = offset;
    }
    Ok(())
}

/// Read entry `index` of an offset array.
///
/// # Panics
///
/// Panics if `offsets` is shorter than `(index + 1) * OFFSET_SIZE`. Callers check the array
/// against the buffer before reading any of it.
#[inline]
pub fn read_offset(offsets: &[u8], index: usize) -> usize {
    let at = index * OFFSET_SIZE;
    u32::from_le_bytes(offsets[at..at + OFFSET_SIZE].try_into().unwrap()) as usize
}

/// Largest page `num_values` values can compress to, at `vector_size` values per vector.
///
/// A vector never serializes to more than its values occupy unpacked. The cost model minimises
/// `num_elements * bit_width + num_exceptions * exception_bits`, and the full-width candidate
/// scores `num_elements * 8 * byte_width` bits with no exceptions at all, so whatever width it
/// does pick costs no more than that. Bit packing rounds the packed section up to a whole byte,
/// hence the trailing byte. Differencing adds one full-width start value, and the model only
/// chooses it when doing so still comes out cheaper, but the bound has to hold either way.
pub fn max_compressed_size<T: PforInt>(num_values: usize, vector_size: usize) -> Result<usize> {
    validate_vector_size(vector_size)?;
    let num_vectors = num_values.div_ceil(vector_size);
    let max_vector_size = T::INFO_SIZE + T::BYTE_WIDTH + vector_size * T::BYTE_WIDTH + 1;
    Ok(HEADER_SIZE + num_vectors * (OFFSET_SIZE + max_vector_size))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bits_required() {
        assert_eq!(bits_required(0), 0);
        assert_eq!(bits_required(1), 1);
        assert_eq!(bits_required(2), 2);
        assert_eq!(bits_required(255), 8);
        assert_eq!(bits_required(256), 9);
        assert_eq!(bits_required(u64::MAX), 64);
    }

    #[test]
    fn test_low_mask() {
        assert_eq!(low_mask(0), 0);
        assert_eq!(low_mask(1), 1);
        assert_eq!(low_mask(8), 0xFF);
        assert_eq!(low_mask(63), i64::MAX as u64);
        // The shift that builds the mask overflows at the full width, so it is a special case.
        assert_eq!(low_mask(64), u64::MAX);
    }

    #[test]
    fn test_bytes_for_bits() {
        assert_eq!(bytes_for_bits(0), 0);
        assert_eq!(bytes_for_bits(1), 1);
        assert_eq!(bytes_for_bits(8), 1);
        assert_eq!(bytes_for_bits(9), 2);
    }

    #[test]
    fn test_exception_bits() {
        // A position plus a full-width value, in bits.
        assert_eq!(exception_bits(4), 48);
        assert_eq!(exception_bits(8), 80);
    }

    #[test]
    fn test_validate_vector_size() {
        assert_eq!(validate_vector_size(8).unwrap(), 3);
        assert_eq!(validate_vector_size(1024).unwrap(), 10);
        assert_eq!(validate_vector_size(1 << 15).unwrap(), 15);
        // Below the minimum, above the maximum, and not a power of two.
        assert!(validate_vector_size(4).is_err());
        assert!(validate_vector_size(1 << 16).is_err());
        assert!(validate_vector_size(1000).is_err());
        assert!(validate_vector_size(0).is_err());
    }

    #[test]
    fn test_header_round_trip() {
        let header = PforHeader {
            packing_mode: PACKING_MODE_FOR_BIT_PACK,
            log_vector_size: DEFAULT_LOG_VECTOR_SIZE,
            value_byte_width: 4,
            num_elements: 3000,
        };
        let mut buf = Vec::new();
        header.write(&mut buf);

        // The byte layout is part of the format, so it is asserted rather than just round-tripped.
        assert_eq!(buf.len(), HEADER_SIZE);
        assert_eq!(buf, vec![0, 10, 4, 0xB8, 0x0B, 0x00, 0x00]);

        let read = PforHeader::read::<i32>(&buf).unwrap();
        assert_eq!(read, header);
        assert_eq!(read.vector_size(), 1024);
        assert_eq!(read.num_vectors(), 3);
    }

    #[test]
    fn test_header_num_vectors() {
        let header = |num_elements| PforHeader {
            packing_mode: PACKING_MODE_FOR_BIT_PACK,
            log_vector_size: 3,
            value_byte_width: 4,
            num_elements,
        };
        assert_eq!(header(0).num_vectors(), 0);
        assert_eq!(header(1).num_vectors(), 1);
        assert_eq!(header(8).num_vectors(), 1);
        assert_eq!(header(9).num_vectors(), 2);
    }

    #[test]
    fn test_header_rejections() {
        let good = PforHeader {
            packing_mode: PACKING_MODE_FOR_BIT_PACK,
            log_vector_size: DEFAULT_LOG_VECTOR_SIZE,
            value_byte_width: 4,
            num_elements: 10,
        };
        let bytes = |header: &PforHeader| {
            let mut buf = Vec::new();
            header.write(&mut buf);
            buf
        };

        // Truncated, one byte short of a header.
        let buf = bytes(&good);
        assert!(PforHeader::read::<i32>(&buf[..HEADER_SIZE - 1]).is_err());

        // A packing mode this implementation does not have.
        let mut buf = bytes(&good);
        buf[0] = 1;
        assert!(PforHeader::read::<i32>(&buf).is_err());

        // An INT64 page read as INT32.
        let mut buf = bytes(&good);
        buf[2] = 8;
        assert!(PforHeader::read::<i32>(&buf).is_err());
        assert!(PforHeader::read::<i64>(&buf).is_ok());

        // Vector sizes outside [2^3, 2^15].
        for log_vector_size in [0u8, 2, 16, 63, 255] {
            let mut buf = bytes(&good);
            buf[1] = log_vector_size;
            assert!(
                PforHeader::read::<i32>(&buf).is_err(),
                "log_vector_size {log_vector_size} should be rejected"
            );
        }

        // A negative element count, which would otherwise size an allocation.
        let mut buf = bytes(&good);
        buf[3..7].copy_from_slice(&(-1i32).to_le_bytes());
        assert!(PforHeader::read::<i32>(&buf).is_err());
    }

    #[test]
    fn test_vector_info_round_trip_i32() {
        let info = PforVectorInfo::<i32> {
            frame_of_reference: -5,
            bit_width: 9,
            num_exceptions: 3,
            is_delta: false,
        };
        let mut buf = Vec::new();
        info.write(&mut buf);
        assert_eq!(buf.len(), <i32 as PforInt>::INFO_SIZE);
        assert_eq!(buf, vec![0xFB, 0xFF, 0xFF, 0xFF, 9, 3, 0]);
        assert_eq!(PforVectorInfo::<i32>::read(&buf).unwrap(), info);
        assert_eq!(info.stored_bytes(), 7);
    }

    #[test]
    fn test_vector_info_round_trip_i64() {
        let info = PforVectorInfo::<i64> {
            frame_of_reference: 1,
            bit_width: 33,
            num_exceptions: 0,
            is_delta: false,
        };
        let mut buf = Vec::new();
        info.write(&mut buf);
        assert_eq!(buf.len(), <i64 as PforInt>::INFO_SIZE);
        assert_eq!(buf, vec![1, 0, 0, 0, 0, 0, 0, 0, 33, 0, 0]);
        assert_eq!(PforVectorInfo::<i64>::read(&buf).unwrap(), info);
        assert_eq!(info.stored_bytes(), 11);
    }

    #[test]
    fn test_vector_info_delta_flag() {
        let info = PforVectorInfo::<i32> {
            frame_of_reference: 0,
            bit_width: 7,
            num_exceptions: 0,
            is_delta: true,
        };
        let mut buf = Vec::new();
        info.write(&mut buf);
        // The flag rides in bit 7 of the width byte, leaving seven bits of width.
        assert_eq!(buf[4], 7 | DELTA_FLAG);
        let read = PforVectorInfo::<i32>::read(&buf).unwrap();
        assert_eq!(read.bit_width, 7);
        assert!(read.is_delta);
    }

    #[test]
    fn test_vector_info_full_width() {
        // Width 64 is a legal width for INT64 and needs all seven bits of the field. A six-bit
        // field truncates it to 0, which decodes as a constant vector: silent corruption, so it is
        // asserted on both sides.
        let info = PforVectorInfo::<i64> {
            frame_of_reference: 0,
            bit_width: 64,
            num_exceptions: 0,
            is_delta: false,
        };
        let mut buf = Vec::new();
        info.write(&mut buf);
        assert_eq!(buf[8], 64);
        assert_eq!(PforVectorInfo::<i64>::read(&buf).unwrap().bit_width, 64);

        // The same width, with the delta flag beside it.
        let info = PforVectorInfo::<i64> {
            is_delta: true,
            ..info
        };
        let mut buf = Vec::new();
        info.write(&mut buf);
        assert_eq!(buf[8], 64 | DELTA_FLAG);
        let read = PforVectorInfo::<i64>::read(&buf).unwrap();
        assert_eq!(read.bit_width, 64);
        assert!(read.is_delta);
    }

    #[test]
    fn test_vector_info_rejections() {
        // Truncated info block.
        let buf = vec![0u8; <i32 as PforInt>::INFO_SIZE - 1];
        assert!(PforVectorInfo::<i32>::read(&buf).is_err());

        // Width 33 on an INT32 column: nothing can pack to it, and it would size reads past the
        // vector.
        let mut buf = vec![0u8; <i32 as PforInt>::INFO_SIZE];
        buf[4] = 33;
        assert!(PforVectorInfo::<i32>::read(&buf).is_err());
        buf[4] = 32;
        assert!(PforVectorInfo::<i32>::read(&buf).is_ok());

        // More exceptions than the largest vector the format allows.
        let mut buf = vec![0u8; <i32 as PforInt>::INFO_SIZE];
        buf[5..7].copy_from_slice(&((MAX_VECTOR_SIZE + 1) as u16).to_le_bytes());
        assert!(PforVectorInfo::<i32>::read(&buf).is_err());
    }

    #[test]
    fn test_vector_info_stored_bytes_covers_the_start_value() {
        let info = PforVectorInfo::<i32> {
            frame_of_reference: 0,
            bit_width: 3,
            num_exceptions: 2,
            is_delta: false,
        };
        assert_eq!(info.stored_bytes(), <i32 as PforInt>::INFO_SIZE);
        // Differencing puts one full-width start value between the info and the packed residuals,
        // so the offset the packed section starts at moves.
        let info = PforVectorInfo::<i32> {
            is_delta: true,
            ..info
        };
        assert_eq!(info.stored_bytes(), <i32 as PforInt>::INFO_SIZE + 4);

        let info = PforVectorInfo::<i64> {
            frame_of_reference: 0,
            bit_width: 3,
            num_exceptions: 0,
            is_delta: true,
        };
        assert_eq!(info.stored_bytes(), <i64 as PforInt>::INFO_SIZE + 8);
    }

    /// An offset array holding `offsets`, as it appears on the wire.
    fn offset_bytes(offsets: &[u32]) -> Vec<u8> {
        offsets.iter().flat_map(|o| o.to_le_bytes()).collect()
    }

    #[test]
    fn test_validate_offsets_accepts_well_formed() {
        let offsets = offset_bytes(&[8, 20]);
        assert!(validate_offsets(&offsets, 2, 40).is_ok());
        // A page of one vector, whose single offset lands just past the array.
        let offsets = offset_bytes(&[4]);
        assert!(validate_offsets(&offsets, 1, 20).is_ok());
        // No vectors at all: nothing to check, and nothing to reject.
        assert!(validate_offsets(&[], 0, 0).is_ok());
    }

    #[test]
    fn test_validate_offsets_rejections() {
        // The first offset has to land exactly past the offset array, so neither a gap nor an
        // overlap with the array itself is accepted.
        assert!(validate_offsets(&offset_bytes(&[9, 20]), 2, 40).is_err());
        assert!(validate_offsets(&offset_bytes(&[7, 20]), 2, 40).is_err());

        // Equal offsets mean a zero-length vector; a decreasing pair means the second vector
        // starts inside the first. Both would decode values out of the wrong bytes.
        assert!(validate_offsets(&offset_bytes(&[8, 8]), 2, 40).is_err());
        assert!(validate_offsets(&offset_bytes(&[8, 20, 12]), 3, 40).is_err());

        // Past the end of the payload, and exactly at its end -- which leaves the vector no bytes.
        assert!(validate_offsets(&offset_bytes(&[8, 41]), 2, 40).is_err());
        assert!(validate_offsets(&offset_bytes(&[8, 40]), 2, 40).is_err());
    }

    #[test]
    fn test_read_offset() {
        let offsets = offset_bytes(&[8, 0x0102_0304]);
        assert_eq!(read_offset(&offsets, 0), 8);
        assert_eq!(read_offset(&offsets, 1), 0x0102_0304);
    }

    #[test]
    fn test_max_compressed_size() {
        // A page of one 8-value INT32 vector: header, one offset, info, a start value, the values
        // unpacked, and the byte the packed section can round up to.
        assert_eq!(
            max_compressed_size::<i32>(8, 8).unwrap(),
            HEADER_SIZE + 4 + 7 + 4 + 8 * 4 + 1
        );
        // Two vectors' worth, the second of them partial.
        assert_eq!(
            max_compressed_size::<i32>(9, 8).unwrap(),
            HEADER_SIZE + 2 * (4 + 7 + 4 + 8 * 4 + 1)
        );
        assert_eq!(
            max_compressed_size::<i64>(8, 8).unwrap(),
            HEADER_SIZE + 4 + 11 + 8 + 8 * 8 + 1
        );
        // An empty page is a bare header.
        assert_eq!(max_compressed_size::<i32>(0, 8).unwrap(), HEADER_SIZE);
        assert!(max_compressed_size::<i32>(8, 1000).is_err());
    }
}
