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

use std::{cmp, mem::size_of};

use bytes::Bytes;

use crate::data_type::{AsBytes, ByteArray, FixedLenByteArray, Int96};
use crate::errors::{ParquetError, Result};
use crate::util::bit_pack::{unpack8, unpack16, unpack32, unpack64};

#[inline]
fn array_from_slice<const N: usize>(bs: &[u8]) -> Result<[u8; N]> {
    // Need to slice as may be called with zero-padded values
    match bs.get(..N) {
        Some(b) => Ok(b.try_into().unwrap()),
        None => Err(general_err!(
            "error converting value, expected {} bytes got {}",
            N,
            bs.len()
        )),
    }
}

/// Types that can be decoded from plain representations. This includes non-primitive types like
/// `FixedLenByteArray` and also variable length types like `ByteArray`.
pub trait FromBytes: Sized {
    type Buffer: AsMut<[u8]> + Default;
    fn try_from_le_slice(b: &[u8]) -> Result<Self>;
    fn from_le_bytes(bs: Self::Buffer) -> Self;
}

/// Types that can be decoded from bitpacked representations.
///
/// This is implemented for primitive types and bool that can be
/// directly converted from a u64 value. Types like Int96, ByteArray,
/// and FixedLenByteArray that cannot be represented in 64 bits do not
/// implement this trait.
pub trait FromBitpacked {
    /// The maximum number of bits that are allowed to be converted to this type.
    /// This is at most the size of the type in bits, but could be less, for example
    /// for the boolean type.
    const BIT_CAPACITY: usize;
    /// How many values are converted by one call to `unpack_batch`.
    const BATCH_SIZE: usize;
    /// Convert directly from a u64 value by truncation, avoiding byte slice copies.
    fn from_u64(v: u64) -> Self;

    /// Converts multiple bitpacked values from `input` to `output`.
    /// The `output` slice needs to have space for at least `BATCH_SIZE` elements,
    /// otherwise this method will panic.
    fn unpack_batch(input: &[u8], output: &mut [Self], num_bits: usize)
    where
        Self: Sized;
}

macro_rules! from_le_bytes {
    ($($ty: ty),*) => {
        $(
        impl FromBytes for $ty {
            type Buffer = [u8; size_of::<Self>()];
            fn try_from_le_slice(b: &[u8]) -> Result<Self> {
                Ok(Self::from_le_bytes(array_from_slice(b)?))
            }
            fn from_le_bytes(bs: Self::Buffer) -> Self {
                <$ty>::from_le_bytes(bs)
            }
        }
        )*
    };
}

macro_rules! from_bitpacked {
    ($($ty: ty => $unpack: path),*) => {
        $(
            impl FromBitpacked for $ty {
                const BIT_CAPACITY: usize = std::mem::size_of::<$ty>() * 8;
                // this has to match the signature of the unpack* functions
                const BATCH_SIZE: usize = std::mem::size_of::<$ty>() * 8;

                #[inline]
                fn from_u64(v: u64) -> Self {
                    v as _
                }

                #[inline]
                fn unpack_batch(input: &[u8], output: &mut [Self], num_bits: usize) {
                    $unpack(input, (&mut output[..Self::BATCH_SIZE]).try_into().unwrap(), num_bits)
                }
            }
        )*
    }
}

macro_rules! from_bitpacked_delegate {
    ($($ty: ty => $delegate: ty),*) => {
        $(
            impl FromBitpacked for $ty {
                const BIT_CAPACITY: usize = <$delegate as FromBitpacked>::BIT_CAPACITY;
                const BATCH_SIZE: usize = <$delegate as FromBitpacked>::BATCH_SIZE;

                #[inline]
                fn from_u64(v: u64) -> Self {
                    v as _
                }

                #[inline]
                fn unpack_batch(input: &[u8], output: &mut [Self], num_bits: usize) {
                    // Guard against misusages of this macro, due to the const block this will fail
                    // already at compile-time if the types are not compatible.
                    const {
                        assert!(
                            std::mem::size_of::<$ty>() == std::mem::size_of::<$delegate>()
                            && std::mem::align_of::<$ty>() == std::mem::align_of::<$delegate>(),
                            "types need to have the same size and alignment"
                        );
                    }
                    // Safety: ty and delegate have the same size and alignment, and this macro is only used for types that have transmutable bit patterns.
                    let output: &mut [$delegate] = unsafe { std::slice::from_raw_parts_mut(output.as_mut_ptr().cast::<$delegate>(), output.len()) };
                    <$delegate>::unpack_batch(input, output, num_bits);
                }
            }
        )*
    }
}

from_le_bytes! { u8, u16, u32, u64, i8, i16, i32, i64 }
from_bitpacked!(u8 => unpack8, u16 => unpack16, u32 => unpack32, u64 => unpack64);
from_bitpacked_delegate!(i8 => u8, i16 => u16, i32 => u32, i64 => u64);

impl FromBitpacked for bool {
    const BIT_CAPACITY: usize = 1;
    const BATCH_SIZE: usize = <u8 as FromBitpacked>::BATCH_SIZE;

    #[inline]
    fn from_u64(v: u64) -> Self {
        v != 0
    }

    #[inline]
    fn unpack_batch(input: &[u8], output: &mut [Self], num_bits: usize) {
        assert!(num_bits == 1);
        // Safety:
        //   we asserted that we will only decode with a bitwidth of 1,
        //   so the u8 can only be 0 or 1, which are the valid representations of a bool.
        let output: &mut [u8] = unsafe {
            std::slice::from_raw_parts_mut(output.as_mut_ptr().cast::<u8>(), output.len())
        };
        u8::unpack_batch(input, output, num_bits);
    }
}

impl FromBytes for f32 {
    type Buffer = [u8; 4];
    fn try_from_le_slice(b: &[u8]) -> Result<Self> {
        Ok(Self::from_le_bytes(array_from_slice(b)?))
    }
    fn from_le_bytes(bs: Self::Buffer) -> Self {
        f32::from_le_bytes(bs)
    }
}

impl FromBytes for f64 {
    type Buffer = [u8; 8];
    fn try_from_le_slice(b: &[u8]) -> Result<Self> {
        Ok(Self::from_le_bytes(array_from_slice(b)?))
    }
    fn from_le_bytes(bs: Self::Buffer) -> Self {
        f64::from_le_bytes(bs)
    }
}

impl FromBytes for bool {
    type Buffer = [u8; 1];

    fn try_from_le_slice(b: &[u8]) -> Result<Self> {
        Ok(Self::from_le_bytes(array_from_slice(b)?))
    }
    fn from_le_bytes(bs: Self::Buffer) -> Self {
        bs[0] != 0
    }
}

impl FromBytes for Int96 {
    type Buffer = [u8; 12];

    fn try_from_le_slice(b: &[u8]) -> Result<Self> {
        let bs: [u8; 12] = array_from_slice(b)?;
        let mut i = Int96::new();
        i.set_data(
            u32::try_from_le_slice(&bs[0..4])?,
            u32::try_from_le_slice(&bs[4..8])?,
            u32::try_from_le_slice(&bs[8..12])?,
        );
        Ok(i)
    }

    fn from_le_bytes(bs: Self::Buffer) -> Self {
        let mut i = Int96::new();
        i.set_data(
            u32::try_from_le_slice(&bs[0..4]).unwrap(),
            u32::try_from_le_slice(&bs[4..8]).unwrap(),
            u32::try_from_le_slice(&bs[8..12]).unwrap(),
        );
        i
    }
}

impl FromBytes for ByteArray {
    type Buffer = Vec<u8>;

    fn try_from_le_slice(b: &[u8]) -> Result<Self> {
        Ok(b.to_vec().into())
    }
    fn from_le_bytes(bs: Self::Buffer) -> Self {
        bs.into()
    }
}

impl FromBytes for FixedLenByteArray {
    type Buffer = Vec<u8>;

    fn try_from_le_slice(b: &[u8]) -> Result<Self> {
        Ok(b.to_vec().into())
    }
    fn from_le_bytes(bs: Self::Buffer) -> Self {
        bs.into()
    }
}

/// Reads `size` of bytes from `src`, and reinterprets them as type `ty`, in
/// little-endian order.
/// This is copied and modified from byteorder crate.
pub(crate) fn read_num_bytes<T>(size: usize, src: &[u8]) -> T
where
    T: FromBytes,
{
    debug_assert!(size <= src.len());
    let mut buffer = <T as FromBytes>::Buffer::default();
    buffer.as_mut()[..size].copy_from_slice(&src[..size]);
    <T>::from_le_bytes(buffer)
}

/// Returns the ceil of value/divisor.
///
/// This function should be removed after
/// [`int_roundings`](https://github.com/rust-lang/rust/issues/88581) is stable.
#[inline]
pub fn ceil<T: num_integer::Integer>(value: T, divisor: T) -> T {
    num_integer::Integer::div_ceil(&value, &divisor)
}

/// Returns the `num_bits` least-significant bits of `v`
#[inline]
pub fn trailing_bits(v: u64, num_bits: usize) -> u64 {
    if num_bits >= 64 {
        v
    } else {
        v & ((1 << num_bits) - 1)
    }
}

/// Returns the minimum number of bits needed to represent the value 'x'
#[inline]
pub fn num_required_bits(x: u64) -> u8 {
    64 - x.leading_zeros() as u8
}

static BIT_MASK: [u8; 8] = [1, 2, 4, 8, 16, 32, 64, 128];

/// Returns whether bit at position `i` in `data` is set or not
#[inline]
pub fn get_bit(data: &[u8], i: usize) -> bool {
    (data[i >> 3] & BIT_MASK[i & 7]) != 0
}

/// Writes bit packed values to an in-memory buffer.
///
/// `BitWriter` is the dual of [`BitReader`] and can write values that are either
/// byte aligned or packed at arbitrary bit widths. It is primarily used by the
/// Parquet RLE/bit-packing hybrid encoder.
///
/// Bit-packed values are appended to an internal buffer in
/// little-endian bit order: the first value written occupies the
/// least-significant bits of the first byte. Bits that have not yet filled a
/// whole byte are held in an internal accumulator until a byte-aligning
/// operation (such as [`BitWriter::flush`], [`BitWriter::put_aligned`], or
/// [`BitWriter::consume`]) is called.
///
/// Use [`BitWriter::consume`] to take ownership of the underlying buffer once
/// writing is complete.
///
/// [`BitReader`]: crate::util::bit_util::BitReader
pub struct BitWriter {
    /// Output Buffer
    buffer: Vec<u8>,
    /// Accumulator for in progress values
    buffered_values: u64,
    /// Current write offset within `buffered_values`
    bit_offset: u8,
}

impl BitWriter {
    /// Creates a new [`BitWriter`] backed by an internal buffer of the given
    /// initial capacity.
    pub fn new(initial_capacity: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(initial_capacity),
            buffered_values: 0,
            bit_offset: 0,
        }
    }

    /// Creates a new [`BitWriter`] that appends to the existing `buffer`.
    ///
    /// Data written with this writer are appended after existing values.
    pub fn new_from_buf(buffer: Vec<u8>) -> Self {
        Self {
            buffer,
            buffered_values: 0,
            bit_offset: 0,
        }
    }

    /// Flushes any buffered bits to a byte boundary, then consumes this
    /// writer and returns the underlying buffer.
    #[inline]
    pub fn consume(mut self) -> Vec<u8> {
        self.flush();
        self.buffer
    }

    /// Flushes any buffered bits to a byte boundary and returns a borrowed
    /// view of the buffer's contents.
    ///
    /// This is the borrowing equivalent of [`BitWriter::consume`]. The writer
    /// can continue to be used after this call.
    #[inline]
    pub fn flush_buffer(&mut self) -> &[u8] {
        self.flush();
        self.buffer()
    }

    /// Like [`BitWriter::flush_buffer`], but returns mutable access to the
    /// buffer.
    #[inline]
    pub fn flush_buffer_mut(&mut self) -> &mut [u8] {
        self.flush();
        &mut self.buffer
    }

    /// Clears the internal state.
    ///
    /// Truncates the underlying buffer to length 0 (preserving its capacity)
    /// and resets the bit accumulator.
    #[inline]
    pub fn clear(&mut self) {
        self.buffer.clear();
        self.buffered_values = 0;
        self.bit_offset = 0;
    }

    /// Flushes any buffered bits and aligns the writer to the next byte
    /// boundary.
    ///
    /// Any partial byte currently held in the bit accumulator is appended to
    /// the buffer, and the accumulator is reset. Subsequent writes will start
    /// at a byte boundary.
    #[inline]
    pub fn flush(&mut self) {
        let num_bytes = ceil(self.bit_offset, 8);
        let slice = &self.buffered_values.to_le_bytes()[..num_bytes as usize];
        self.buffer.extend_from_slice(slice);
        self.buffered_values = 0;
        self.bit_offset = 0;
    }

    /// Reserves `num_bytes` bytes of zero-filled space at the current
    /// position and returns the byte offset of the start of that region.
    ///
    /// Internally flushes any buffered bits first so the reservation begins
    /// at a byte boundary. Use the returned offset together with
    /// [`BitWriter::write_at`] or [`BitWriter::put_aligned_offset`] to fill
    /// in the reserved bytes once their contents are known (for example, a
    /// length prefix that depends on subsequently encoded data).
    #[inline]
    pub fn skip(&mut self, num_bytes: usize) -> usize {
        self.flush();
        let result = self.buffer.len();
        self.buffer.extend(std::iter::repeat_n(0, num_bytes));
        result
    }

    /// Reserves `num_bytes` bytes at the current position and returns a
    /// mutable slice over them.
    ///
    /// Equivalent to [`BitWriter::skip`], but returns the reserved region
    /// directly so it can be written into. Useful for filling in a header
    /// (such as a length prefix) once the size of the following payload is
    /// known.
    #[inline]
    pub fn get_next_byte_ptr(&mut self, num_bytes: usize) -> &mut [u8] {
        let offset = self.skip(num_bytes);
        &mut self.buffer[offset..offset + num_bytes]
    }

    /// Returns the total number of bytes written so far, including any
    /// partial byte still held in the bit accumulator (rounded up).
    #[inline]
    pub fn bytes_written(&self) -> usize {
        self.buffer.len() + ceil(self.bit_offset, 8) as usize
    }

    /// Returns a borrowed view of the bytes that have been flushed to the
    /// underlying buffer so far.
    ///
    /// Note that bits currently held in the bit accumulator (i.e. not yet
    /// flushed to a byte boundary) are not included. Use
    /// [`BitWriter::flush_buffer`] to also flush pending bits before reading.
    #[inline]
    pub fn buffer(&self) -> &[u8] {
        &self.buffer
    }

    /// Returns the current offset within the output buffer.
    ///
    /// This is the index of the next byte that a byte-aligned write would
    /// land in (excluding any bits currently held in the bit accumulator).
    #[inline]
    pub fn byte_offset(&self) -> usize {
        self.buffer.len()
    }

    /// Overwrites the byte at position `offset` in the underlying buffer
    /// with `value`.
    ///
    /// Typically used together with [`BitWriter::skip`] or
    /// [`BitWriter::get_next_byte_ptr`] to back-fill a previously reserved
    /// byte once its value is known.
    ///
    /// # Panics
    ///
    /// Panics if `offset` is out of bounds for the underlying buffer.
    pub fn write_at(&mut self, offset: usize, value: u8) {
        self.buffer[offset] = value;
    }

    /// Writes the `num_bits` least-significant bits of `v` to the writer in
    /// bit-packed form.
    ///
    /// Values are packed in little-endian bit order: this call appends
    /// `num_bits` bits starting at the current bit position.
    ///
    /// `num_bits` must be no larger than 64.
    #[inline]
    pub fn put_value(&mut self, v: u64, num_bits: usize) {
        debug_assert!(num_bits <= 64);
        let num_bits = num_bits as u8;
        debug_assert_eq!(v.checked_shr(num_bits as u32).unwrap_or(0), 0); // covers case v >> 64

        // Add value to buffered_values
        self.buffered_values |= v << self.bit_offset;
        self.bit_offset += num_bits;
        if let Some(remaining) = self.bit_offset.checked_sub(64) {
            self.buffer
                .extend_from_slice(&self.buffered_values.to_le_bytes());
            self.bit_offset = remaining;

            // Perform checked right shift: v >> offset, where offset < 64, otherwise we
            // shift all bits
            self.buffered_values = v
                .checked_shr((num_bits - self.bit_offset) as u32)
                .unwrap_or(0);
        }
    }

    /// Writes the first `num_bytes` little-endian bytes of `val` to the
    /// writer at the next byte boundary.
    ///
    /// Any buffered bits are first flushed so the value is byte-aligned in
    /// the output. If `T` is wider than `num_bytes`, the high-order bytes
    /// are silently truncated.
    #[inline]
    pub fn put_aligned<T: AsBytes>(&mut self, val: T, num_bytes: usize) {
        self.flush();
        let slice = val.as_bytes();
        let len = num_bytes.min(slice.len());
        self.buffer.extend_from_slice(&slice[..len]);
    }

    /// Writes the first `num_bytes` little-endian bytes of `val` at the
    /// given `offset` in the underlying buffer, overwriting any existing
    /// data in `offset..offset + num_bytes`.
    ///
    /// `offset` is measured from the start of the internal buffer. If `T`
    /// is wider than `num_bytes`, the high-order bytes are silently
    /// truncated.
    ///
    /// Typically used together with [`BitWriter::skip`] to back-fill a
    /// previously reserved region once its contents are known.
    ///
    /// # Panics
    ///
    /// Panics if `offset + min(size_of::<T>(), num_bytes)` is out of bounds
    /// for the underlying buffer.
    #[inline]
    pub fn put_aligned_offset<T: AsBytes>(&mut self, val: T, num_bytes: usize, offset: usize) {
        let slice = val.as_bytes();
        let len = num_bytes.min(slice.len());
        self.buffer[offset..offset + len].copy_from_slice(&slice[..len])
    }

    /// Writes `v` to the buffer in VLQ (variable-length quantity) encoding,
    /// in little-endian byte order.
    ///
    /// Any buffered bits are first flushed so the encoding starts at a byte
    /// boundary. The encoded form is between 1 and [`MAX_VLQ_BYTE_LEN`]
    /// bytes long, depending on the magnitude of `v`.
    #[inline]
    pub fn put_vlq_int(&mut self, mut v: u64) {
        while v & 0xFFFFFFFFFFFFFF80 != 0 {
            self.put_aligned::<u8>(((v & 0x7F) | 0x80) as u8, 1);
            v >>= 7;
        }
        self.put_aligned::<u8>((v & 0x7F) as u8, 1);
    }

    /// Writes `v` to the buffer in zigzag-VLQ encoding, in little-endian
    /// byte order.
    ///
    /// Zigzag-VLQ is a variant of VLQ encoding where negative and positive
    /// numbers are interleaved so that small absolute values produce short
    /// encodings regardless of sign. See the [Protocol Buffers encoding
    /// documentation](https://developers.google.com/protocol-buffers/docs/encoding)
    /// for details.
    ///
    /// As with [`BitWriter::put_vlq_int`], any buffered bits are first
    /// flushed so the encoding starts at a byte boundary.
    #[inline]
    pub fn put_zigzag_vlq_int(&mut self, v: i64) {
        let u: u64 = ((v << 1) ^ (v >> 63)) as u64;
        self.put_vlq_int(u)
    }

    /// Returns an estimate of the heap memory used by this writer, in bytes.
    ///
    /// This reflects the capacity of the underlying buffer rather than the
    /// number of bytes actually written.
    pub fn estimated_memory_size(&self) -> usize {
        self.buffer.capacity() * size_of::<u8>()
    }
}

/// Maximum byte length for a VLQ encoded integer
/// MAX_VLQ_BYTE_LEN = 5 for i32, and MAX_VLQ_BYTE_LEN = 10 for i64
pub const MAX_VLQ_BYTE_LEN: usize = 10;

/// Reads bit packed values from an in-memory buffer.
///
/// `BitReader` is the dual of [`BitWriter`] and reads values that are either
/// byte aligned or packed at arbitrary bit widths. It is primarily used by the
/// Parquet RLE/bit-packing hybrid decoder.
///
/// Reads advance an internal cursor; once the buffer is exhausted, the
/// `get_*` methods return `None` rather than panicking. To rewind, use
/// [`BitReader::reset`] with the same (or a different) buffer.
pub struct BitReader {
    /// The byte buffer to read from, passed in by client
    buffer: Bytes,

    /// Bytes are memcpy'd from `buffer` and values are read from this variable.
    /// This is faster than reading values byte by byte directly from `buffer`
    ///
    /// This is only populated when `self.bit_offset != 0`
    buffered_values: u64,

    ///
    /// End                                         Start
    /// |............|B|B|B|B|B|B|B|B|..............|
    ///                   ^          ^
    ///                 bit_offset   byte_offset
    ///
    /// Current byte offset in `buffer`
    byte_offset: usize,

    /// Current bit offset in `buffered_values`
    bit_offset: usize,
}

impl BitReader {
    /// Creates a new [`BitReader`] that reads from `buffer`, starting at
    /// bit offset 0.
    pub fn new(buffer: Bytes) -> Self {
        BitReader {
            buffer,
            buffered_values: 0,
            byte_offset: 0,
            bit_offset: 0,
        }
    }

    /// Resets this reader to read from the start of `buffer`, discarding any
    /// previous buffer and position.
    ///
    /// This is useful for reusing the same `BitReader` instance across
    /// multiple input buffers without allocation.
    pub fn reset(&mut self, buffer: Bytes) {
        self.buffer = buffer;
        self.buffered_values = 0;
        self.byte_offset = 0;
        self.bit_offset = 0;
    }

    /// Returns the current byte offset, rounded up to the next whole byte.
    ///
    /// This is the index of the next byte that a byte-aligned
    /// read (such as [`BitReader::get_aligned`]) would consume.
    #[inline]
    pub fn get_byte_offset(&self) -> usize {
        self.byte_offset + ceil(self.bit_offset, 8)
    }

    /// Reads a single bit-packed value of `num_bits` bits as a `T` from the
    /// stream.
    ///
    /// The value is read as the low `num_bits` bits of `T`. Bits are consumed
    /// from the stream in little-endian bit order.
    ///
    /// Returns `None` if there are fewer than `num_bits` bits left in the
    /// buffer; otherwise `Some(value)`. On `None` the reader's position is
    /// left unchanged.
    pub fn get_value<T: FromBitpacked>(&mut self, num_bits: usize) -> Option<T> {
        debug_assert!(num_bits <= 64);
        debug_assert!(num_bits <= size_of::<T>() * 8);

        if self.byte_offset * 8 + self.bit_offset + num_bits > self.buffer.len() * 8 {
            return None;
        }

        // If buffer is not byte aligned, `self.buffered_values` will
        // have already been populated
        if self.bit_offset == 0 {
            self.load_buffered_values()
        }

        let mut v =
            trailing_bits(self.buffered_values, self.bit_offset + num_bits) >> self.bit_offset;
        self.bit_offset += num_bits;

        if self.bit_offset >= 64 {
            self.byte_offset += 8;
            self.bit_offset -= 64;

            // If the new bit_offset is not 0, we need to read the next 64-bit chunk
            // to buffered_values and update `v`
            if self.bit_offset != 0 {
                self.load_buffered_values();

                v |= trailing_bits(self.buffered_values, self.bit_offset)
                    .wrapping_shl((num_bits - self.bit_offset) as u32);
            }
        }

        Some(T::from_u64(v))
    }

    /// Reads up to `batch.len()` bit-packed values of `num_bits` each, into
    /// `batch`.
    ///
    /// Equivalent to repeatedly calling [`BitReader::get_value`] with the same
    /// `num_bits`, but faster because it dispatches to SIMD-friendly
    /// fixed-width unpacking routines whenever possible.
    ///
    /// Returns the number of values actually written to `batch`. This will be
    /// less than `batch.len()` if the underlying buffer is exhausted before
    /// `batch` is filled.
    ///
    /// # Panics
    ///
    /// This function panics if
    /// - `num_bits` is larger than the bit-capacity of `T`
    pub fn get_batch<T: FromBitpacked>(&mut self, batch: &mut [T], num_bits: usize) -> usize {
        debug_assert!(num_bits <= size_of::<T>() * 8);

        let mut values_to_read = batch.len();
        let needed_bits = num_bits * values_to_read;
        let remaining_bits = (self.buffer.len() - self.byte_offset) * 8 - self.bit_offset;
        if remaining_bits < needed_bits {
            values_to_read = remaining_bits / num_bits;
        }

        let mut i = 0;

        // First align bit offset to byte offset
        if self.bit_offset != 0 {
            while i < values_to_read && self.bit_offset != 0 {
                batch[i] = self
                    .get_value(num_bits)
                    .expect("expected to have more data");
                i += 1;
            }
        }

        assert_ne!(T::BIT_CAPACITY, 0);
        assert!(num_bits <= T::BIT_CAPACITY);

        // Read directly into output buffer
        while values_to_read - i >= T::BATCH_SIZE {
            T::unpack_batch(&self.buffer[self.byte_offset..], &mut batch[i..], num_bits);
            self.byte_offset += num_bits * T::BATCH_SIZE / 8;
            i += T::BATCH_SIZE;
        }

        // Try to read smaller batches if possible
        if size_of::<T>() > 4 && values_to_read - i >= 32 && num_bits <= 32 {
            let mut out_buf = [0_u32; 32];
            unpack32(&self.buffer[self.byte_offset..], &mut out_buf, num_bits);
            self.byte_offset += 4 * num_bits;

            for out in out_buf {
                batch[i] = T::from_u64(out as u64);
                i += 1;
            }
        }

        if size_of::<T>() > 2 && values_to_read - i >= 16 && num_bits <= 16 {
            let mut out_buf = [0_u16; 16];
            unpack16(&self.buffer[self.byte_offset..], &mut out_buf, num_bits);
            self.byte_offset += 2 * num_bits;

            for out in out_buf {
                batch[i] = T::from_u64(out as u64);
                i += 1;
            }
        }

        if size_of::<T>() > 1 && values_to_read - i >= 8 && num_bits <= 8 {
            let mut out_buf = [0_u8; 8];
            unpack8(&self.buffer[self.byte_offset..], &mut out_buf, num_bits);
            self.byte_offset += num_bits;

            for out in out_buf {
                batch[i] = T::from_u64(out as u64);
                i += 1;
            }
        }

        // Read any trailing values
        while i < values_to_read {
            let value = self
                .get_value(num_bits)
                .expect("expected to have more data");
            batch[i] = value;
            i += 1;
        }

        values_to_read
    }

    /// Skips `num_values` bit-packed values of `num_bits` bits, advancing the
    /// reader past them without decoding.
    ///
    /// Returns the number of values actually skipped (up to `num_values`).
    /// This will be less than `num_values` if the underlying buffer is
    /// exhausted.
    pub fn skip(&mut self, num_values: usize, num_bits: usize) -> usize {
        debug_assert!(num_bits <= 64);

        let needed_bits = num_bits * num_values;
        let remaining_bits = (self.buffer.len() - self.byte_offset) * 8 - self.bit_offset;

        let values_to_read = match remaining_bits < needed_bits {
            true => remaining_bits / num_bits,
            false => num_values,
        };

        let end_bit_offset = self.byte_offset * 8 + values_to_read * num_bits + self.bit_offset;

        self.byte_offset = end_bit_offset / 8;
        self.bit_offset = end_bit_offset % 8;

        if self.bit_offset != 0 {
            self.load_buffered_values()
        }

        values_to_read
    }

    /// Reads up to `num_bytes` bytes from the stream, appending them to `buf`,
    /// and returns the number of bytes actually appended.
    ///
    /// The reader is first advanced to the next byte boundary, so any
    /// in-progress bit-level read is discarded before the bytes are copied.
    pub(crate) fn get_aligned_bytes(&mut self, buf: &mut Vec<u8>, num_bytes: usize) -> usize {
        // Align to byte offset
        self.byte_offset = self.get_byte_offset();
        self.bit_offset = 0;

        let src = &self.buffer[self.byte_offset..];
        let to_read = num_bytes.min(src.len());
        buf.extend_from_slice(&src[..to_read]);

        self.byte_offset += to_read;

        to_read
    }

    /// Reads a `num_bytes`-sized value of type `T` from the stream.
    ///
    /// `T` is interpreted as a little-endian native type. The value is
    /// assumed to be byte aligned, so the reader is first advanced to the
    /// start of the next byte before reading.
    ///
    /// Returns `Some(value)` if there are at least `num_bytes` bytes left in
    /// the buffer after byte-alignment, and `None` otherwise. On `None` the
    /// reader's byte position is still advanced to the alignment boundary.
    pub fn get_aligned<T: FromBytes>(&mut self, num_bytes: usize) -> Option<T> {
        self.byte_offset = self.get_byte_offset();
        self.bit_offset = 0;

        if self.byte_offset + num_bytes > self.buffer.len() {
            return None;
        }

        // Advance byte_offset to next unread byte and read num_bytes
        let v = read_num_bytes::<T>(num_bytes, &self.buffer[self.byte_offset..]);
        self.byte_offset += num_bytes;

        Some(v)
    }

    /// Reads a VLQ-encoded (in little-endian order) integer from the stream.
    ///
    /// The encoded integer must start at the beginning of a byte; the reader
    /// is first advanced to the next byte boundary before decoding.
    ///
    /// Returns `Some(value)` on success, or `None` if the buffer is exhausted
    /// before a complete VLQ value is read.
    ///
    /// # Panics
    ///
    /// Panics if the encoded integer is longer than [`MAX_VLQ_BYTE_LEN`]
    /// bytes (bad input).
    pub fn get_vlq_int(&mut self) -> Option<i64> {
        // Align to byte boundary once, then read bytes directly
        self.byte_offset = self.get_byte_offset();
        self.bit_offset = 0;

        let buf = &self.buffer[self.byte_offset..];
        let mut shift = 0;
        let mut v: i64 = 0;

        for (i, &byte) in buf.iter().enumerate() {
            v |= ((byte & 0x7F) as i64) << shift;
            shift += 7;
            assert!(
                shift <= MAX_VLQ_BYTE_LEN * 7,
                "Num of bytes exceed MAX_VLQ_BYTE_LEN ({MAX_VLQ_BYTE_LEN})"
            );
            if byte & 0x80 == 0 {
                self.byte_offset += i + 1;
                return Some(v);
            }
        }
        None
    }

    /// Reads a zigzag-VLQ-encoded little-endian integer from the
    /// stream.
    ///
    /// Zigzag-VLQ is a variant of VLQ encoding where negative and positive
    /// numbers are interleaved so that small absolute values produce short
    /// encodings regardless of sign. See the [Protocol Buffers encoding
    /// documentation](https://developers.google.com/protocol-buffers/docs/encoding)
    /// for details.
    ///
    /// As with [`BitReader::get_vlq_int`], the encoded integer must start at
    /// the beginning of a byte; the reader is first advanced to the next
    /// byte boundary before decoding.
    ///
    /// Returns `Some(value)` on success, or `None` if the buffer is exhausted
    /// before a complete value is read.
    #[inline]
    pub fn get_zigzag_vlq_int(&mut self) -> Option<i64> {
        self.get_vlq_int().map(|v| {
            let u = v as u64;
            (u >> 1) as i64 ^ -((u & 1) as i64)
        })
    }

    /// Loads up to the next 8 bytes from `self.buffer` at `self.byte_offset`
    /// into `self.buffered_values`.
    ///
    /// Reads fewer than 8 bytes if there are fewer than 8 bytes left
    #[inline]
    fn load_buffered_values(&mut self) {
        let bytes_to_read = cmp::min(self.buffer.len() - self.byte_offset, 8);
        self.buffered_values =
            read_num_bytes::<u64>(bytes_to_read, &self.buffer[self.byte_offset..]);
    }
}

impl From<Vec<u8>> for BitReader {
    #[inline]
    fn from(buffer: Vec<u8>) -> Self {
        BitReader::new(buffer.into())
    }
}

/// Parallel bit extract: for each set bit in `mask`, extract the
/// corresponding bit from `value` and pack them contiguously into the low
/// bits of the return value.
///
/// Equivalent to the x86 BMI2 `PEXT` instruction. When compiled with the
/// `bmi2` target feature enabled (for example `-C target-cpu=x86-64-v3`)
/// this lowers to the hardware `pext` instruction; otherwise it falls back
/// to a portable scalar loop.
///
/// Replace with `value.compress(mask)` when `uint_gather_scatter_bits`
/// is stabilised: <https://github.com/rust-lang/rust/issues/149069>
#[allow(dead_code)]
#[inline]
pub(crate) fn compress(value: u64, mask: u64) -> u64 {
    #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
    {
        // SAFETY: the `bmi2` target feature is statically enabled for this
        // build, so the `pext` instruction is guaranteed to be available.
        unsafe { std::arch::x86_64::_pext_u64(value, mask) }
    }

    #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
    {
        let mut mask = mask;
        let mut result: u64 = 0;
        let mut dest_bit: u64 = 1;
        while mask != 0 {
            let lowest = mask & mask.wrapping_neg();
            if value & lowest != 0 {
                result |= dest_bit;
            }
            dest_bit <<= 1;
            mask ^= lowest;
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::util::test_common::rand_gen::random_numbers;
    use rand::distr::{Distribution, StandardUniform};
    use std::fmt::Debug;

    #[test]
    fn test_compress() {
        // Reference: gather the `mask`-selected bits of `value` into
        // contiguous low bits, least-significant first.
        fn reference(value: u64, mut mask: u64) -> u64 {
            let mut result = 0u64;
            let mut dest = 0u32;
            while mask != 0 {
                let lowest = mask & mask.wrapping_neg();
                result |= (((value & lowest) != 0) as u64) << dest;
                dest += 1;
                mask ^= lowest;
            }
            result
        }

        // Hand-picked edge cases.
        assert_eq!(compress(0b1010, 0b1111), 0b1010);
        assert_eq!(compress(0b1010, 0b1010), 0b11);
        assert_eq!(compress(0b1010, 0b0101), 0);
        assert_eq!(compress(u64::MAX, 0), 0);
        assert_eq!(compress(0, u64::MAX), 0);
        assert_eq!(compress(u64::MAX, u64::MAX), u64::MAX);

        // Randomised cross-check against the reference. On a `bmi2` build
        // this validates the hardware `pext` path; otherwise it exercises
        // the portable fallback.
        let values = random_numbers::<u64>(1024);
        let masks = random_numbers::<u64>(1024);
        for (&value, &mask) in values.iter().zip(masks.iter()) {
            assert_eq!(
                compress(value, mask),
                reference(value, mask),
                "value={value:#x} mask={mask:#x}"
            );
        }
    }

    #[test]
    fn test_ceil() {
        assert_eq!(ceil(0, 1), 0);
        assert_eq!(ceil(1, 1), 1);
        assert_eq!(ceil(1, 2), 1);
        assert_eq!(ceil(1, 8), 1);
        assert_eq!(ceil(7, 8), 1);
        assert_eq!(ceil(8, 8), 1);
        assert_eq!(ceil(9, 8), 2);
        assert_eq!(ceil(9, 9), 1);
        assert_eq!(ceil(10000000000_u64, 10), 1000000000);
        assert_eq!(ceil(10_u64, 10000000000), 1);
        assert_eq!(ceil(10000000000_u64, 1000000000), 10);
    }

    #[test]
    fn test_bit_reader_get_byte_offset() {
        let buffer = vec![255; 10];
        let mut bit_reader = BitReader::from(buffer);
        assert_eq!(bit_reader.get_byte_offset(), 0); // offset (0 bytes, 0 bits)
        bit_reader.get_value::<i32>(6);
        assert_eq!(bit_reader.get_byte_offset(), 1); // offset (0 bytes, 6 bits)
        bit_reader.get_value::<i32>(10);
        assert_eq!(bit_reader.get_byte_offset(), 2); // offset (0 bytes, 16 bits)
        bit_reader.get_value::<i32>(20);
        assert_eq!(bit_reader.get_byte_offset(), 5); // offset (0 bytes, 36 bits)
        bit_reader.get_value::<i32>(30);
        assert_eq!(bit_reader.get_byte_offset(), 9); // offset (8 bytes, 2 bits)
    }

    #[test]
    fn test_bit_reader_get_value() {
        let buffer = vec![255, 0];
        let mut bit_reader = BitReader::from(buffer);
        assert_eq!(bit_reader.get_value::<i32>(1), Some(1));
        assert_eq!(bit_reader.get_value::<i32>(2), Some(3));
        assert_eq!(bit_reader.get_value::<i32>(3), Some(7));
        assert_eq!(bit_reader.get_value::<i32>(4), Some(3));
    }

    #[test]
    fn test_bit_reader_skip() {
        let buffer = vec![255, 0];
        let mut bit_reader = BitReader::from(buffer);
        let skipped = bit_reader.skip(1, 1);
        assert_eq!(skipped, 1);
        assert_eq!(bit_reader.get_value::<i32>(1), Some(1));
        let skipped = bit_reader.skip(2, 2);
        assert_eq!(skipped, 2);
        assert_eq!(bit_reader.get_value::<i32>(2), Some(3));
        let skipped = bit_reader.skip(4, 1);
        assert_eq!(skipped, 4);
        assert_eq!(bit_reader.get_value::<i32>(4), Some(0));
        let skipped = bit_reader.skip(1, 1);
        assert_eq!(skipped, 0);
    }

    #[test]
    fn test_bit_reader_get_value_boundary() {
        let buffer = vec![10, 0, 0, 0, 20, 0, 30, 0, 0, 0, 40, 0];
        let mut bit_reader = BitReader::from(buffer);
        assert_eq!(bit_reader.get_value::<i64>(32), Some(10));
        assert_eq!(bit_reader.get_value::<i64>(16), Some(20));
        assert_eq!(bit_reader.get_value::<i64>(32), Some(30));
        assert_eq!(bit_reader.get_value::<i64>(16), Some(40));
    }

    #[test]
    fn test_bit_reader_skip_boundary() {
        let buffer = vec![10, 0, 0, 0, 20, 0, 30, 0, 0, 0, 40, 0];
        let mut bit_reader = BitReader::from(buffer);
        assert_eq!(bit_reader.get_value::<i64>(32), Some(10));
        assert_eq!(bit_reader.skip(1, 16), 1);
        assert_eq!(bit_reader.get_value::<i64>(32), Some(30));
        assert_eq!(bit_reader.get_value::<i64>(16), Some(40));
    }

    #[test]
    fn test_bit_reader_get_aligned() {
        // 01110101 11001011
        let buffer = Bytes::from(vec![0x75, 0xCB]);
        let mut bit_reader = BitReader::new(buffer.clone());
        assert_eq!(bit_reader.get_value::<i32>(3), Some(5));
        assert_eq!(bit_reader.get_aligned::<i32>(1), Some(203));
        assert_eq!(bit_reader.get_value::<i32>(1), None);
        bit_reader.reset(buffer.clone());
        assert_eq!(bit_reader.get_aligned::<i32>(3), None);
    }

    #[test]
    fn test_bit_reader_get_vlq_int() {
        // 10001001 00000001 11110010 10110101 00000110
        let buffer: Vec<u8> = vec![0x89, 0x01, 0xF2, 0xB5, 0x06];
        let mut bit_reader = BitReader::from(buffer);
        assert_eq!(bit_reader.get_vlq_int(), Some(137));
        assert_eq!(bit_reader.get_vlq_int(), Some(105202));
    }

    #[test]
    fn test_bit_reader_get_zigzag_vlq_int() {
        let buffer: Vec<u8> = vec![0, 1, 2, 3];
        let mut bit_reader = BitReader::from(buffer);
        assert_eq!(bit_reader.get_zigzag_vlq_int(), Some(0));
        assert_eq!(bit_reader.get_zigzag_vlq_int(), Some(-1));
        assert_eq!(bit_reader.get_zigzag_vlq_int(), Some(1));
        assert_eq!(bit_reader.get_zigzag_vlq_int(), Some(-2));
    }

    #[test]
    fn test_num_required_bits() {
        assert_eq!(num_required_bits(0), 0);
        assert_eq!(num_required_bits(1), 1);
        assert_eq!(num_required_bits(2), 2);
        assert_eq!(num_required_bits(4), 3);
        assert_eq!(num_required_bits(8), 4);
        assert_eq!(num_required_bits(10), 4);
        assert_eq!(num_required_bits(12), 4);
        assert_eq!(num_required_bits(16), 5);
        assert_eq!(num_required_bits(u64::MAX), 64);
    }

    #[test]
    fn test_get_bit() {
        // 00001101
        assert!(get_bit(&[0b00001101], 0));
        assert!(!get_bit(&[0b00001101], 1));
        assert!(get_bit(&[0b00001101], 2));
        assert!(get_bit(&[0b00001101], 3));

        // 01001001 01010010
        assert!(get_bit(&[0b01001001, 0b01010010], 0));
        assert!(!get_bit(&[0b01001001, 0b01010010], 1));
        assert!(!get_bit(&[0b01001001, 0b01010010], 2));
        assert!(get_bit(&[0b01001001, 0b01010010], 3));
        assert!(!get_bit(&[0b01001001, 0b01010010], 4));
        assert!(!get_bit(&[0b01001001, 0b01010010], 5));
        assert!(get_bit(&[0b01001001, 0b01010010], 6));
        assert!(!get_bit(&[0b01001001, 0b01010010], 7));
        assert!(!get_bit(&[0b01001001, 0b01010010], 8));
        assert!(get_bit(&[0b01001001, 0b01010010], 9));
        assert!(!get_bit(&[0b01001001, 0b01010010], 10));
        assert!(!get_bit(&[0b01001001, 0b01010010], 11));
        assert!(get_bit(&[0b01001001, 0b01010010], 12));
        assert!(!get_bit(&[0b01001001, 0b01010010], 13));
        assert!(get_bit(&[0b01001001, 0b01010010], 14));
        assert!(!get_bit(&[0b01001001, 0b01010010], 15));
    }

    #[test]
    fn test_skip() {
        let mut writer = BitWriter::new(5);
        let old_offset = writer.skip(1);
        writer.put_aligned(42, 4);
        writer.put_aligned_offset(0x10, 1, old_offset);
        let result = writer.consume();
        assert_eq!(result.as_ref(), [0x10, 42, 0, 0, 0]);

        writer = BitWriter::new(4);
        let result = writer.skip(5);
        assert_eq!(result, 0);
        assert_eq!(writer.buffer(), &[0; 5])
    }

    #[test]
    fn test_get_next_byte_ptr() {
        let mut writer = BitWriter::new(5);
        {
            let first_byte = writer.get_next_byte_ptr(1);
            first_byte[0] = 0x10;
        }
        writer.put_aligned(42, 4);
        let result = writer.consume();
        assert_eq!(result.as_ref(), [0x10, 42, 0, 0, 0]);
    }

    #[test]
    fn test_consume_flush_buffer() {
        let mut writer1 = BitWriter::new(3);
        let mut writer2 = BitWriter::new(3);
        for i in 1..10 {
            writer1.put_value(i, 4);
            writer2.put_value(i, 4);
        }
        let res1 = writer1.flush_buffer();
        let res2 = writer2.consume();
        assert_eq!(res1, &res2[..]);
    }

    #[test]
    fn test_put_get_bool() {
        let len = 8;
        let mut writer = BitWriter::new(len);

        for i in 0..8 {
            writer.put_value(i % 2, 1);
        }

        writer.flush();
        {
            let buffer = writer.buffer();
            assert_eq!(buffer[0], 0b10101010);
        }

        // Write 00110011
        for i in 0..8 {
            match i {
                0 | 1 | 4 | 5 => writer.put_value(false as u64, 1),
                _ => writer.put_value(true as u64, 1),
            }
        }
        writer.flush();
        {
            let buffer = writer.buffer();
            assert_eq!(buffer[0], 0b10101010);
            assert_eq!(buffer[1], 0b11001100);
        }

        let mut reader = BitReader::from(writer.consume());

        for i in 0..8 {
            let val = reader
                .get_value::<u8>(1)
                .expect("get_value() should return OK");
            assert_eq!(val, i % 2);
        }

        for i in 0..8 {
            let val = reader
                .get_value::<bool>(1)
                .expect("get_value() should return OK");
            match i {
                0 | 1 | 4 | 5 => assert!(!val),
                _ => assert!(val),
            }
        }
    }

    #[test]
    fn test_put_value_roundtrip() {
        test_put_value_rand_numbers(32, 2);
        test_put_value_rand_numbers(32, 3);
        test_put_value_rand_numbers(32, 4);
        test_put_value_rand_numbers(32, 5);
        test_put_value_rand_numbers(32, 6);
        test_put_value_rand_numbers(32, 7);
        test_put_value_rand_numbers(32, 8);
        test_put_value_rand_numbers(64, 16);
        test_put_value_rand_numbers(64, 24);
        test_put_value_rand_numbers(64, 32);
    }

    fn test_put_value_rand_numbers(total: usize, num_bits: usize) {
        assert!(num_bits < 64);
        let num_bytes = ceil(num_bits, 8);
        let mut writer = BitWriter::new(num_bytes * total);
        let values: Vec<u64> = random_numbers::<u64>(total)
            .iter()
            .map(|v| v & ((1 << num_bits) - 1))
            .collect();
        (0..total).for_each(|i| writer.put_value(values[i], num_bits));

        let mut reader = BitReader::from(writer.consume());
        (0..total).for_each(|i| {
            let v = reader
                .get_value::<u64>(num_bits)
                .expect("get_value() should return OK");
            assert_eq!(
                v, values[i],
                "[{}]: expected {} but got {}",
                i, values[i], v
            );
        });
    }

    #[test]
    fn test_get_batch() {
        const SIZE: &[usize] = &[1, 31, 32, 33, 128, 129];
        for s in SIZE {
            for i in 0..=64 {
                // `get_batch` is generic over all of these types, so exercise the
                // signed integer types and `bool` alongside the unsigned types rather
                // than relying on the unsigned coverage alone.
                match i {
                    0..=8 => {
                        test_get_batch_helper::<u8>(*s, i);
                        test_get_batch_helper::<i8>(*s, i);
                    }
                    9..=16 => {
                        test_get_batch_helper::<u16>(*s, i);
                        test_get_batch_helper::<i16>(*s, i);
                    }
                    17..=32 => {
                        test_get_batch_helper::<u32>(*s, i);
                        test_get_batch_helper::<i32>(*s, i);
                    }
                    _ => {
                        test_get_batch_helper::<u64>(*s, i);
                        test_get_batch_helper::<i64>(*s, i);
                    }
                }
                // `bool` only supports a bit width of 1.
                if i == 1 {
                    test_get_batch_helper::<bool>(*s, i);
                }
            }
        }
    }

    fn test_get_batch_helper<T>(total: usize, num_bits: usize)
    where
        T: FromBitpacked + Default + Clone + Debug + Eq,
    {
        assert!(num_bits <= 64);
        let num_bytes = ceil(num_bits, 8);
        let mut writer = BitWriter::new(num_bytes * total);

        let mask = match num_bits {
            64 => u64::MAX,
            _ => (1 << num_bits) - 1,
        };

        let values: Vec<u64> = random_numbers::<u64>(total)
            .iter()
            .map(|v| v & mask)
            .collect();

        // Generic values used to check against actual values read from `get_batch`.
        let expected_values: Vec<T> = values.iter().map(|v| T::from_u64(*v)).collect();

        (0..total).for_each(|i| writer.put_value(values[i], num_bits));

        let buf = writer.consume();
        let mut reader = BitReader::from(buf);
        let mut batch = vec![T::default(); values.len()];
        let values_read = reader.get_batch::<T>(&mut batch, num_bits);
        assert_eq!(values_read, values.len());
        for i in 0..batch.len() {
            assert_eq!(
                batch[i],
                expected_values[i],
                "max_num_bits = {}, num_bits = {}, index = {}",
                size_of::<T>() * 8,
                num_bits,
                i
            );
        }
    }

    #[test]
    fn test_put_aligned_roundtrip() {
        test_put_aligned_rand_numbers::<u8>(4, 3);
        test_put_aligned_rand_numbers::<u8>(16, 5);
        test_put_aligned_rand_numbers::<i16>(32, 7);
        test_put_aligned_rand_numbers::<i16>(32, 9);
        test_put_aligned_rand_numbers::<i32>(32, 11);
        test_put_aligned_rand_numbers::<i32>(32, 13);
        test_put_aligned_rand_numbers::<i64>(32, 17);
        test_put_aligned_rand_numbers::<i64>(32, 23);
    }

    fn test_put_aligned_rand_numbers<T>(total: usize, num_bits: usize)
    where
        T: Copy + FromBytes + AsBytes + Debug + PartialEq,
        StandardUniform: Distribution<T>,
    {
        assert!(num_bits <= 32);
        assert!(total % 2 == 0);

        let aligned_value_byte_width = std::mem::size_of::<T>();
        let value_byte_width = ceil(num_bits, 8);
        let mut writer =
            BitWriter::new((total / 2) * (aligned_value_byte_width + value_byte_width));
        let values: Vec<u32> = random_numbers::<u32>(total / 2)
            .iter()
            .map(|v| v & ((1 << num_bits) - 1))
            .collect();
        let aligned_values = random_numbers::<T>(total / 2);

        for i in 0..total {
            let j = i / 2;
            if i % 2 == 0 {
                writer.put_value(values[j] as u64, num_bits);
            } else {
                writer.put_aligned::<T>(aligned_values[j], aligned_value_byte_width)
            }
        }

        let mut reader = BitReader::from(writer.consume());
        for i in 0..total {
            let j = i / 2;
            if i % 2 == 0 {
                let v = reader
                    .get_value::<u64>(num_bits)
                    .expect("get_value() should return OK");
                assert_eq!(
                    v, values[j] as u64,
                    "[{}]: expected {} but got {}",
                    i, values[j], v
                );
            } else {
                let v = reader
                    .get_aligned::<T>(aligned_value_byte_width)
                    .expect("get_aligned() should return OK");
                assert_eq!(
                    v, aligned_values[j],
                    "[{}]: expected {:?} but got {:?}",
                    i, aligned_values[j], v
                );
            }
        }
    }

    #[test]
    fn test_put_vlq_int() {
        let total = 64;
        let mut writer = BitWriter::new(total * 32);
        let values = random_numbers::<u32>(total);
        (0..total).for_each(|i| writer.put_vlq_int(values[i] as u64));

        let mut reader = BitReader::from(writer.consume());
        (0..total).for_each(|i| {
            let v = reader
                .get_vlq_int()
                .expect("get_vlq_int() should return OK");
            assert_eq!(
                v as u32, values[i],
                "[{}]: expected {} but got {}",
                i, values[i], v
            );
        });
    }

    #[test]
    fn test_put_zigzag_vlq_int() {
        let total = 64;
        let mut writer = BitWriter::new(total * 32);
        let values = random_numbers::<i32>(total);
        (0..total).for_each(|i| writer.put_zigzag_vlq_int(values[i] as i64));

        let mut reader = BitReader::from(writer.consume());
        (0..total).for_each(|i| {
            let v = reader
                .get_zigzag_vlq_int()
                .expect("get_zigzag_vlq_int() should return OK");
            assert_eq!(
                v as i32, values[i],
                "[{}]: expected {} but got {}",
                i, values[i], v
            );
        });
    }

    #[test]
    fn test_get_batch_zero_extend() {
        let to_read = vec![0xFF; 4];
        let mut reader = BitReader::from(to_read);

        // Create a non-zeroed output buffer
        let mut output = [u64::MAX; 32];
        reader.get_batch(&mut output, 1);

        for v in output {
            // Values should be read correctly
            assert_eq!(v, 1);
        }
    }
}
