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

use crate::array::PrimitiveArray;
use crate::null_sentinel;
use arrow_array::builder::BufferBuilder;
use arrow_array::{ArrowPrimitiveType, BooleanArray, FixedSizeBinaryArray};
use arrow_buffer::{
    bit_util, i256, ArrowNativeType, BooleanBuffer, Buffer, IntervalDayTime, IntervalMonthDayNano,
    MutableBuffer, NullBuffer,
};
use arrow_data::{ArrayData, ArrayDataBuilder};
use arrow_schema::{DataType, SortOptions};
use half::f16;

pub trait FromSlice {
    fn from_slice(slice: &[u8], invert: bool) -> Self;
}

impl<const N: usize> FromSlice for [u8; N] {
    #[inline]
    fn from_slice(slice: &[u8], invert: bool) -> Self {
        let mut t: Self = slice.try_into().unwrap();
        if invert {
            t.iter_mut().for_each(|o| *o = !*o);
        }
        t
    }
}

/// Encodes a value of a particular fixed width type into bytes according to the rules
/// described on [`super::RowConverter`]
pub trait FixedLengthEncoding: Copy {
    const ENCODED_LEN: usize = 1 + std::mem::size_of::<Self::Encoded>();

    type Encoded: Sized + Copy + FromSlice + AsRef<[u8]> + AsMut<[u8]>;

    fn encode(self) -> Self::Encoded;

    fn decode(encoded: Self::Encoded) -> Self;
}

impl FixedLengthEncoding for bool {
    type Encoded = [u8; 1];

    fn encode(self) -> [u8; 1] {
        [self as u8]
    }

    fn decode(encoded: Self::Encoded) -> Self {
        encoded[0] != 0
    }
}

macro_rules! encode_signed {
    ($n:expr, $t:ty) => {
        impl FixedLengthEncoding for $t {
            type Encoded = [u8; $n];

            fn encode(self) -> [u8; $n] {
                let mut b = self.to_be_bytes();
                // Toggle top "sign" bit to ensure consistent sort order
                b[0] ^= 0x80;
                b
            }

            fn decode(mut encoded: Self::Encoded) -> Self {
                // Toggle top "sign" bit
                encoded[0] ^= 0x80;
                Self::from_be_bytes(encoded)
            }
        }
    };
}

encode_signed!(1, i8);
encode_signed!(2, i16);
encode_signed!(4, i32);
encode_signed!(8, i64);
encode_signed!(16, i128);
encode_signed!(32, i256);

macro_rules! encode_unsigned {
    ($n:expr, $t:ty) => {
        impl FixedLengthEncoding for $t {
            type Encoded = [u8; $n];

            fn encode(self) -> [u8; $n] {
                self.to_be_bytes()
            }

            fn decode(encoded: Self::Encoded) -> Self {
                Self::from_be_bytes(encoded)
            }
        }
    };
}

encode_unsigned!(1, u8);
encode_unsigned!(2, u16);
encode_unsigned!(4, u32);
encode_unsigned!(8, u64);

impl FixedLengthEncoding for f16 {
    type Encoded = [u8; 2];

    fn encode(self) -> [u8; 2] {
        // https://github.com/rust-lang/rust/blob/9c20b2a8cc7588decb6de25ac6a7912dcef24d65/library/core/src/num/f32.rs#L1176-L1260
        let s = self.to_bits() as i16;
        let val = s ^ (((s >> 15) as u16) >> 1) as i16;
        val.encode()
    }

    fn decode(encoded: Self::Encoded) -> Self {
        let bits = i16::decode(encoded);
        let val = bits ^ (((bits >> 15) as u16) >> 1) as i16;
        Self::from_bits(val as u16)
    }
}

impl FixedLengthEncoding for f32 {
    type Encoded = [u8; 4];

    fn encode(self) -> [u8; 4] {
        // https://github.com/rust-lang/rust/blob/9c20b2a8cc7588decb6de25ac6a7912dcef24d65/library/core/src/num/f32.rs#L1176-L1260
        let s = self.to_bits() as i32;
        let val = s ^ (((s >> 31) as u32) >> 1) as i32;
        val.encode()
    }

    fn decode(encoded: Self::Encoded) -> Self {
        let bits = i32::decode(encoded);
        let val = bits ^ (((bits >> 31) as u32) >> 1) as i32;
        Self::from_bits(val as u32)
    }
}

impl FixedLengthEncoding for f64 {
    type Encoded = [u8; 8];

    fn encode(self) -> [u8; 8] {
        // https://github.com/rust-lang/rust/blob/9c20b2a8cc7588decb6de25ac6a7912dcef24d65/library/core/src/num/f32.rs#L1176-L1260
        let s = self.to_bits() as i64;
        let val = s ^ (((s >> 63) as u64) >> 1) as i64;
        val.encode()
    }

    fn decode(encoded: Self::Encoded) -> Self {
        let bits = i64::decode(encoded);
        let val = bits ^ (((bits >> 63) as u64) >> 1) as i64;
        Self::from_bits(val as u64)
    }
}

impl FixedLengthEncoding for IntervalDayTime {
    type Encoded = [u8; 8];

    fn encode(self) -> Self::Encoded {
        let mut out = [0_u8; 8];
        out[..4].copy_from_slice(&self.days.encode());
        out[4..].copy_from_slice(&self.milliseconds.encode());
        out
    }

    fn decode(encoded: Self::Encoded) -> Self {
        Self {
            days: i32::decode(encoded[..4].try_into().unwrap()),
            milliseconds: i32::decode(encoded[4..].try_into().unwrap()),
        }
    }
}

impl FixedLengthEncoding for IntervalMonthDayNano {
    type Encoded = [u8; 16];

    fn encode(self) -> Self::Encoded {
        let mut out = [0_u8; 16];
        out[..4].copy_from_slice(&self.months.encode());
        out[4..8].copy_from_slice(&self.days.encode());
        out[8..].copy_from_slice(&self.nanoseconds.encode());
        out
    }

    fn decode(encoded: Self::Encoded) -> Self {
        Self {
            months: i32::decode(encoded[..4].try_into().unwrap()),
            days: i32::decode(encoded[4..8].try_into().unwrap()),
            nanoseconds: i64::decode(encoded[8..].try_into().unwrap()),
        }
    }
}

/// Returns the total encoded length (including null byte) for a value of type `T::Native`
pub const fn encoded_len<T>(_col: &PrimitiveArray<T>) -> usize
where
    T: ArrowPrimitiveType,
    T::Native: FixedLengthEncoding,
{
    T::Native::ENCODED_LEN
}

/// Fixed width types are encoded as
///
/// - 1 byte `0` if null or `1` if valid
/// - bytes of [`FixedLengthEncoding`]
pub fn encode<T: FixedLengthEncoding>(
    data: &mut [u8],
    offsets: &mut [usize],
    values: &[T],
    nulls: &NullBuffer,
    opts: SortOptions,
) {
    for (value_idx, is_valid) in nulls.iter().enumerate() {
        let offset = &mut offsets[value_idx + 1];
        let end_offset = *offset + T::ENCODED_LEN;
        if is_valid {
            let to_write = &mut data[*offset..end_offset];
            to_write[0] = 1;
            let mut encoded = values[value_idx].encode();
            if opts.descending {
                // Flip bits to reverse order
                encoded.as_mut().iter_mut().for_each(|v| *v = !*v)
            }
            to_write[1..].copy_from_slice(encoded.as_ref())
        } else {
            data[*offset] = null_sentinel(opts);
        }
        *offset = end_offset;
    }
}

/// Encoding for non-nullable primitive arrays.
/// Iterates directly over the `values`, and skips NULLs-checking.
pub fn encode_not_null<T: FixedLengthEncoding>(
    data: &mut [u8],
    offsets: &mut [usize],
    values: &[T],
    opts: SortOptions,
) {
    for (value_idx, val) in values.iter().enumerate() {
        let offset = &mut offsets[value_idx + 1];
        let end_offset = *offset + T::ENCODED_LEN;

        let to_write = &mut data[*offset..end_offset];
        to_write[0] = 1;
        let mut encoded = val.encode();
        if opts.descending {
            // Flip bits to reverse order
            encoded.as_mut().iter_mut().for_each(|v| *v = !*v)
        }
        to_write[1..].copy_from_slice(encoded.as_ref());

        *offset = end_offset;
    }
}

/// Boolean values are encoded as
///
/// - 1 byte `0` if null or `1` if valid
/// - bytes of [`FixedLengthEncoding`]
pub fn encode_boolean(
    data: &mut [u8],
    offsets: &mut [usize],
    values: &BooleanBuffer,
    nulls: &NullBuffer,
    opts: SortOptions,
) {
    for (idx, is_valid) in nulls.iter().enumerate() {
        let offset = &mut offsets[idx + 1];
        let end_offset = *offset + bool::ENCODED_LEN;
        if is_valid {
            let to_write = &mut data[*offset..end_offset];
            to_write[0] = 1;
            let mut encoded = values.value(idx).encode();
            if opts.descending {
                // Flip bits to reverse order
                encoded.as_mut().iter_mut().for_each(|v| *v = !*v)
            }
            to_write[1..].copy_from_slice(encoded.as_ref())
        } else {
            data[*offset] = null_sentinel(opts);
        }
        *offset = end_offset;
    }
}

/// Encoding for non-nullable boolean arrays.
/// Iterates directly over `values`, and skips NULLs-checking.
pub fn encode_boolean_not_null(
    data: &mut [u8],
    offsets: &mut [usize],
    values: &BooleanBuffer,
    opts: SortOptions,
) {
    for (value_idx, val) in values.iter().enumerate() {
        let offset = &mut offsets[value_idx + 1];
        let end_offset = *offset + bool::ENCODED_LEN;

        let to_write = &mut data[*offset..end_offset];
        to_write[0] = 1;
        let mut encoded = val.encode();
        if opts.descending {
            // Flip bits to reverse order
            encoded.as_mut().iter_mut().for_each(|v| *v = !*v)
        }
        to_write[1..].copy_from_slice(encoded.as_ref());

        *offset = end_offset;
    }
}

pub fn encode_fixed_size_binary(
    data: &mut [u8],
    offsets: &mut [usize],
    array: &FixedSizeBinaryArray,
    opts: SortOptions,
) {
    let len = array.value_length() as usize;
    for (offset, maybe_val) in offsets.iter_mut().skip(1).zip(array.iter()) {
        let end_offset = *offset + len + 1;
        if let Some(val) = maybe_val {
            let to_write = &mut data[*offset..end_offset];
            to_write[0] = 1;
            to_write[1..].copy_from_slice(&val[..len]);
            if opts.descending {
                // Flip bits to reverse order
                to_write[1..1 + len].iter_mut().for_each(|v| *v = !*v)
            }
        } else {
            data[*offset] = null_sentinel(opts);
        }
        *offset = end_offset;
    }
}

/// Splits `len` bytes from `src`
#[inline]
fn split_off<'a>(src: &mut &'a [u8], len: usize) -> &'a [u8] {
    let v = &src[..len];
    *src = &src[len..];
    v
}

/// Decodes a `BooleanArray` from rows
pub fn decode_bool(rows: &mut [&[u8]], options: SortOptions) -> BooleanArray {
    let true_val = match options.descending {
        true => !1,
        false => 1,
    };

    let len = rows.len();

    let mut null_count = 0;
    let mut nulls = MutableBuffer::new(bit_util::ceil(len, 64) * 8);
    let mut values = MutableBuffer::new(bit_util::ceil(len, 64) * 8);

    let chunks = len / 64;
    let remainder = len % 64;
    for chunk in 0..chunks {
        let mut null_packed = 0;
        let mut values_packed = 0;

        for bit_idx in 0..64 {
            let i = split_off(&mut rows[bit_idx + chunk * 64], 2);
            let (null, value) = (i[0] == 1, i[1] == true_val);
            null_count += !null as usize;
            null_packed |= (null as u64) << bit_idx;
            values_packed |= (value as u64) << bit_idx;
        }

        nulls.push(null_packed);
        values.push(values_packed);
    }

    if remainder != 0 {
        let mut null_packed = 0;
        let mut values_packed = 0;

        for bit_idx in 0..remainder {
            let i = split_off(&mut rows[bit_idx + chunks * 64], 2);
            let (null, value) = (i[0] == 1, i[1] == true_val);
            null_count += !null as usize;
            null_packed |= (null as u64) << bit_idx;
            values_packed |= (value as u64) << bit_idx;
        }

        nulls.push(null_packed);
        values.push(values_packed);
    }

    let builder = ArrayDataBuilder::new(DataType::Boolean)
        .len(rows.len())
        .null_count(null_count)
        .add_buffer(values.into())
        .null_bit_buffer(Some(nulls.into()));

    // SAFETY:
    // Buffers are the correct length
    unsafe { BooleanArray::from(builder.build_unchecked()) }
}

/// Decodes a single byte from each row, interpreting `0x01` as a valid value
/// and all other values as a null
///
/// Returns the null count and null buffer
pub fn decode_nulls(rows: &[&[u8]]) -> (usize, Buffer) {
    let mut null_count = 0;
    let buffer = MutableBuffer::collect_bool(rows.len(), |idx| {
        let valid = rows[idx][0] == 1;
        null_count += !valid as usize;
        valid
    })
    .into();
    (null_count, buffer)
}

/// Decodes a `ArrayData` from rows based on the provided `FixedLengthEncoding` `T`
///
/// # Safety
///
/// `data_type` must be appropriate native type for `T`
unsafe fn decode_fixed<T: FixedLengthEncoding + ArrowNativeType>(
    rows: &mut [&[u8]],
    data_type: DataType,
    options: SortOptions,
) -> ArrayData {
    let len = rows.len();

    let mut values = BufferBuilder::<T>::new(len);
    let (null_count, nulls) = decode_nulls(rows);

    for row in rows {
        let i = split_off(row, T::ENCODED_LEN);
        let value = T::Encoded::from_slice(&i[1..], options.descending);
        values.append(T::decode(value));
    }

    let builder = ArrayDataBuilder::new(data_type)
        .len(len)
        .null_count(null_count)
        .add_buffer(values.finish())
        .null_bit_buffer(Some(nulls));

    // SAFETY: Buffers correct length
    builder.build_unchecked()
}

/// Decodes a `PrimitiveArray` from rows
pub fn decode_primitive<T: ArrowPrimitiveType>(
    rows: &mut [&[u8]],
    data_type: DataType,
    options: SortOptions,
) -> PrimitiveArray<T>
where
    T::Native: FixedLengthEncoding,
{
    assert!(PrimitiveArray::<T>::is_compatible(&data_type));
    // SAFETY:
    // Validated data type above
    unsafe { decode_fixed::<T::Native>(rows, data_type, options).into() }
}

/// Decodes a `FixedLengthBinary` from rows
pub fn decode_fixed_size_binary(
    rows: &mut [&[u8]],
    size: i32,
    options: SortOptions,
) -> FixedSizeBinaryArray {
    let len = rows.len();

    let mut values = MutableBuffer::new(size as usize * rows.len());
    let (null_count, nulls) = decode_nulls(rows);

    let encoded_len = size as usize + 1;

    for row in rows {
        let i = split_off(row, encoded_len);
        values.extend_from_slice(&i[1..]);
    }

    if options.descending {
        for v in values.as_slice_mut() {
            *v = !*v;
        }
    }

    let builder = ArrayDataBuilder::new(DataType::FixedSizeBinary(size))
        .len(len)
        .null_count(null_count)
        .add_buffer(values.into())
        .null_bit_buffer(Some(nulls));

    // SAFETY: Buffers correct length
    unsafe { builder.build_unchecked().into() }
}
