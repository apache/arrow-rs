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

use super::null_sentinel;
use crate::array::PrimitiveArray;
use arrow_array::builder::BufferBuilder;
use arrow_array::{ArrowPrimitiveType, BooleanArray, FixedSizeBinaryArray};
use arrow_buffer::{
    ArrowNativeType, BooleanBuffer, Buffer, IntervalDayTime, IntervalMonthDayNano, MutableBuffer,
    NullBuffer, bit_util, i256,
};
use arrow_data::{ArrayData, ArrayDataBuilder};
use arrow_schema::DataType;
use half::f16;

pub trait FromSlice {
    fn from_slice(slice: &[u8]) -> Self;
}

impl<const N: usize> FromSlice for [u8; N] {
    #[inline]
    fn from_slice(slice: &[u8]) -> Self {
        let mut t: Self = slice.try_into().unwrap();
        t
    }
}

/// Encodes a value of a particular fixed width type into bytes according to the rules
/// described on [`super::UnorderedRowConverter`]
pub trait FixedLengthEncoding: Copy {
    const ENCODED_LEN: usize = 1 + std::mem::size_of::<Self::Encoded>();

    type Encoded: Sized + Copy + FromSlice + AsRef<[u8]> + AsMut<[u8]>;

    fn encode(self) -> Self::Encoded;

    fn encode_to_box(self) -> Box<[u8]> {
        self.encode().as_ref().to_vec().into_boxed_slice()
    }

    fn encode_to_large(self) -> [u8; 32] {
        let encoded = self.encode();
        let encoded = encoded.as_ref();
        let mut out = [0_u8; 32];
        out[..encoded.len()].copy_from_slice(encoded);

        out
    }

    fn decode(encoded: Self::Encoded) -> Self;
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
// impl FixedLengthEncoding for i32 {
//     type Encoded = [u8; 4];
//
//     fn encode(self) -> [u8; 4] {
//         // (self as u32).swap_bytes()
//
//         let mut b = self.to_be_bytes();
//
//         b[0] ^= 0x80;
//         b
//     }
//
//     fn decode(mut encoded: Self::Encoded) -> Self {
//         encoded[0] ^= 0x80;
//         Self::from_be_bytes(encoded)
//     }
// }
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
) {
    for (value_idx, is_valid) in nulls.iter().enumerate() {
        let offset = &mut offsets[value_idx + 1];
        let end_offset = *offset + T::ENCODED_LEN;
        if is_valid {
            let to_write = &mut data[*offset..end_offset];
            to_write[0] = 1;
            let mut encoded = values[value_idx].encode();
            to_write[1..].copy_from_slice(encoded.as_ref())
        } else {
            data[*offset] = null_sentinel();
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
) {
    for (value_idx, val) in values.iter().enumerate() {
        let offset = &mut offsets[value_idx + 1];
        let end_offset = *offset + T::ENCODED_LEN;

        let to_write = &mut data[*offset..end_offset];
        to_write[0] = 1;
        let mut encoded = val.encode();
        to_write[1..].copy_from_slice(encoded.as_ref());

        *offset = end_offset;
    }
}

/// Encoding for non-nullable primitive arrays.
/// Iterates directly over the `values`, and skips NULLs-checking.
pub fn encode_not_null_double<T: FixedLengthEncoding>(
    data: &mut [u8],
    offsets: &mut [usize],
    values_1: impl Iterator<Item = T>,
    values_2: impl Iterator<Item = T>,
) {
    for (value_idx, (val1, val2)) in values_1.zip(values_2).enumerate() {
        let offset = &mut offsets[value_idx + 1];
        let end_offset = *offset + T::ENCODED_LEN * 2;

        let to_write = &mut data[*offset..end_offset];
        to_write[0] = 1;
        to_write[T::ENCODED_LEN] = 1;

        {
            let mut encoded = val1.encode();
            to_write[1..T::ENCODED_LEN].copy_from_slice(encoded.as_ref());
        }

        {
            let mut encoded = val2.encode();
            to_write[T::ENCODED_LEN + 1..].copy_from_slice(encoded.as_ref());
        }

        *offset = end_offset;
    }
}

pub struct ZipArraySameLength<T, const N: usize> {
    array: [T; N],
}

pub fn zip_array<T: ExactSizeIterator, const N: usize>(array: [T; N]) -> ZipArraySameLength<T, N> {
    assert_ne!(N, 0);

    ZipArraySameLength { array }
}

impl<T: ExactSizeIterator, const N: usize> Iterator for ZipArraySameLength<T, N> {
    type Item = [T::Item; N];

    fn next(&mut self) -> Option<Self::Item> {
        // SAFETY: It is always valid to `assume_init()` an array of `MaybeUninit`s (can be replaced
        // with `MaybeUninit::uninit_array()` once stable).
        let mut result: [std::mem::MaybeUninit<T::Item>; N] = unsafe { std::mem::MaybeUninit::uninit().assume_init() };
        for (item, iterator) in std::iter::zip(&mut result, &mut self.array) {
            item.write(iterator.next()?);
        }
        // SAFETY: We initialized the array above (can be replaced with `MaybeUninit::array_assume_init()`
        // once stable).
        Some(unsafe { std::mem::transmute_copy::<[std::mem::MaybeUninit<T::Item>; N], [T::Item; N]>(&result) })
    }
}

impl<T: ExactSizeIterator, const N: usize> ExactSizeIterator for ZipArraySameLength<T, N> {
    fn len(&self) -> usize {
        self.array[0].len()
    }
}

/// Encoding for non-nullable primitive arrays.
/// Iterates directly over the `values`, and skips NULLs-checking.
pub fn encode_not_null_fixed<const N: usize, T: ArrowPrimitiveType>(
    data: &mut [u8],
    offsets: &mut [usize],
    arrays: [&PrimitiveArray<T>; N],
    // iters: [impl ExactSizeIterator<Item = T>; N],
) where T::Native: FixedLengthEncoding {
    let valid_bits = {
      // Create bitmask where the first N bits are 1s, and the rest are 0s.
      let mut bits = 0u8;
      for i in 0..N {
          bits |= 1 << i;
      }
        bits
    };
    let zip_iter = zip_array::<_, N>(arrays.map(|a| a.values().iter().copied()));
    for (value_idx, array) in zip_iter.enumerate() {
        let offset = &mut offsets[value_idx + 1];
        let end_offset = *offset + 1 + (T::Native::ENCODED_LEN - 1) * N;

        let to_write = &mut data[*offset..end_offset];
        // for i in 0..N {
        //     to_write[i * T::Native::ENCODED_LEN] = 1;
        // }
        to_write[0] = valid_bits;
        for (i, val) in array.iter().enumerate() {
            let mut encoded = val.encode();
            to_write[1 + i * (T::Native::ENCODED_LEN - 1)..(i + 1) * (T::Native::ENCODED_LEN - 1) + 1].copy_from_slice(encoded.as_ref());
        }

        *offset = end_offset;
    }
}
//
// /// Encoding for non-nullable primitive arrays.
// /// Iterates directly over the `values`, and skips NULLs-checking.
// pub fn encode_not_null_four<'a>(
//     data: &'a mut [u8],
//     offsets: &'a mut [usize],
//     values_1: (usize, &'a Buffer),
//     values_2: (usize, &'a Buffer),
//     values_3: (usize, &'a Buffer),
//     values_4: (usize, &'a Buffer),
// ) {
//     let shift_1 = 1;
//     let values_1_slice = values_1.1.as_slice();
//     let shift_2 = shift_1 + values_1.0;
//     let values_2_slice = values_2.1.as_slice();
//     let shift_3 = shift_2 + values_2.0;
//     let values_3_slice = values_3.1.as_slice();
//     let shift_4 = shift_3 + values_3.0;
//     let values_4_slice = values_4.1.as_slice();
//
//     let total_size = shift_4 + values_4.0;
//     for (value_idx, offset) in offsets.iter_mut().skip(1).enumerate()
//     {
//         // let offset = &mut offsets[value_idx + 1];
//
//         // let val1 = values_1_slice.;
//         let end_offset = *offset + 1 + values_1.0 + values_2.0 + values_3.0 + values_4.0;
//
//         let to_write = &mut data[*offset..end_offset];
//
//
//         // let size = std::mem::size_of::<T::Encoded>();
//         // data[*offset..*offset + slice.len()].copy_from_slice(slice.as_slice());
//         //
//         // let slice = [val1, val2, val3, val4].concat();
//
//         // all valid
//         let valid_bits = 0b0000_1111;
//         to_write[0] = valid_bits;
//
//         unsafe { to_write.get_unchecked_mut(1..1 + values_1.0).copy_from_slice(values_1_slice.get_unchecked((value_idx * values_1.0)..(value_idx + 1) * values_1.0)); }
//         let to_write = &mut to_write[1 + values_1.0..];
//         unsafe { to_write.get_unchecked_mut(..values_2.0).copy_from_slice(values_2_slice.get_unchecked((value_idx * values_2.0)..(value_idx + 1) * values_2.0)); }
//         let to_write = &mut to_write[values_2.0..];
//         unsafe { to_write.get_unchecked_mut(..values_3.0).copy_from_slice(values_3_slice.get_unchecked((value_idx * values_3.0)..(value_idx + 1) * values_3.0)); }
//         let to_write = &mut to_write[values_3.0..];
//         unsafe { to_write.get_unchecked_mut(..).copy_from_slice(values_4_slice.get_unchecked((value_idx * values_4.0)..(value_idx + 1) * values_4.0)); }
//         // to_write[1 + values_1.0..1 + values_1.0 + values_2.0].copy_from_slice(&values_2_slice[(value_idx * values_2.0)..(value_idx + 1) * values_2.0]);
//
//         // {
//         //     let mut encoded = val1;
//         //     data[*offset..*offset + slice.len()].copy_from_slice(slice.as_slice());
//         //     *offset += slice.len();
//         // }
//         //
//         // {
//         //     let mut encoded = val2;
//         //     data[*offset..*offset + val2.len()].copy_from_slice(encoded);
//         //     *offset += val2.len();
//         //     // to_write[shift_2..shift_3].copy_from_slice(encoded);
//         // }
//         //
//         // {
//         //     let mut encoded = val3;
//         //     data[*offset..*offset + val3.len()].copy_from_slice(encoded);
//         //     *offset += val3.len();
//         //     // to_write[shift_3..shift_4].copy_from_slice(encoded);
//         // }
//         //
//         // {
//         //     let mut encoded = val4;
//         //     data[*offset..*offset + val4.len()].copy_from_slice(encoded);
//         //     *offset += val4.len();
//         //     // to_write[shift_4..].copy_from_slice(encoded);
//         // }
//
//         *offset = end_offset;
//     }
// }

pub fn encode_fixed_size_binary(
    data: &mut [u8],
    offsets: &mut [usize],
    array: &FixedSizeBinaryArray,
) {
    let len = array.value_length() as usize;
    for (offset, maybe_val) in offsets.iter_mut().skip(1).zip(array.iter()) {
        let end_offset = *offset + len + 1;
        if let Some(val) = maybe_val {
            let to_write = &mut data[*offset..end_offset];
            to_write[0] = 1;
            to_write[1..].copy_from_slice(&val[..len]);
        } else {
            data[*offset] = null_sentinel();
        }
        *offset = end_offset;
    }
}

/// Splits `len` bytes from `src`
#[inline]
pub(super) fn split_off<'a>(src: &mut &'a [u8], len: usize) -> &'a [u8] {
    let v = &src[..len];
    *src = &src[len..];
    v
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
) -> ArrayData {
    let len = rows.len();

    let mut values = BufferBuilder::<T>::new(len);
    let (null_count, nulls) = decode_nulls(rows);

    for row in rows {
        let i = split_off(row, T::ENCODED_LEN);
        let value = T::Encoded::from_slice(&i[1..]);
        values.append(T::decode(value));
    }

    let builder = ArrayDataBuilder::new(data_type)
        .len(len)
        .null_count(null_count)
        .add_buffer(values.finish())
        .null_bit_buffer(Some(nulls));

    // SAFETY: Buffers correct length
    unsafe { builder.build_unchecked() }
}

/// Decodes a `ArrayData` from rows based on the provided `FixedLengthEncoding` `T`
///
/// # Safety
///
/// `data_type` must be appropriate native type for `T`
unsafe fn decode_fixed_four<T: FixedLengthEncoding + ArrowNativeType>(
    rows: &mut [&[u8]],
    data_type1: DataType,
    data_type2: DataType,
    data_type3: DataType,
    data_type4: DataType,
) -> (ArrayData, ArrayData, ArrayData, ArrayData) {
    let len = rows.len();

    let mut values1 = BufferBuilder::<T>::new(len);
    let mut values2 = BufferBuilder::<T>::new(len);
    let mut values3 = BufferBuilder::<T>::new(len);
    let mut values4 = BufferBuilder::<T>::new(len);
    // let (null_count, nulls) = decode_nulls(rows);

    let mut null_count1 = 0;
    let mut null_count2 = 0;
    let mut null_count3 = 0;
    let mut null_count4 = 0;
    let nulls_buffer1 = MutableBuffer::collect_bool(rows.len(), |idx| {
        let valid = rows[idx][0] & 0b00000001 != 0;
        null_count1 += !valid as usize;
        valid
    })
    .into();
    let nulls_buffer2 = MutableBuffer::collect_bool(rows.len(), |idx| {
        let valid = rows[idx][0] & 0b00000010 != 0;
        null_count2 += !valid as usize;
        valid
    })
    .into();
    let nulls_buffer3 = MutableBuffer::collect_bool(rows.len(), |idx| {
        let valid = rows[idx][0] & 0b00000100 != 0;
        null_count3 += !valid as usize;
        valid
    })
    .into();
    let nulls_buffer4 = MutableBuffer::collect_bool(rows.len(), |idx| {
        let valid = rows[idx][0] & 0b00001000 != 0;
        null_count4 += !valid as usize;
        valid
    })
    .into();
    // (null_count, buffer)

    for row in rows {
        let size = std::mem::size_of::<T::Encoded>();
        let i = split_off(row, size * 4 + 1);

        {
            let value = T::Encoded::from_slice(&i[1 + size * 0..1 + size * 1]);
            values1.append(T::decode(value));
        }

        {
            let value = T::Encoded::from_slice(&i[1 + size * 1..1 + size * 2]);
            values2.append(T::decode(value));
        }

        {
            let value = T::Encoded::from_slice(&i[1 + size * 2..1 + size * 3]);
            values3.append(T::decode(value));
        }

        {
            let value = T::Encoded::from_slice(&i[1 + size * 3..1 + size * 4]);
            values4.append(T::decode(value));
        }
    }

    let builder1 = ArrayDataBuilder::new(data_type1)
        .len(len)
        .null_count(null_count1)
        .add_buffer(values1.finish())
        .null_bit_buffer(Some(nulls_buffer1));

    let builder2 = ArrayDataBuilder::new(data_type2)
        .len(len)
        .null_count(null_count2)
        .add_buffer(values2.finish())
        .null_bit_buffer(Some(nulls_buffer2));

    let builder3 = ArrayDataBuilder::new(data_type3)
        .len(len)
        .null_count(null_count3)
        .add_buffer(values3.finish())
        .null_bit_buffer(Some(nulls_buffer3));

    let builder4 = ArrayDataBuilder::new(data_type4)
        .len(len)
        .null_count(null_count4)
        .add_buffer(values4.finish())
        .null_bit_buffer(Some(nulls_buffer4));

    // SAFETY: Buffers correct length
    let array1 = unsafe { builder1.build_unchecked() };
    // SAFETY: Buffers correct length
    let array2 = unsafe { builder2.build_unchecked() };
    // SAFETY: Buffers correct length
    let array3 = unsafe { builder3.build_unchecked() };
    // SAFETY: Buffers correct length
    let array4 = unsafe { builder4.build_unchecked() };

    (array1, array2, array3, array4)
}

/// Decodes a `PrimitiveArray` from rows
pub fn decode_primitive<T: ArrowPrimitiveType>(
    rows: &mut [&[u8]],
    data_type: DataType,
) -> PrimitiveArray<T>
where
    T::Native: FixedLengthEncoding,
{
    assert!(PrimitiveArray::<T>::is_compatible(&data_type));
    // SAFETY:
    // Validated data type above
    unsafe { decode_fixed::<T::Native>(rows, data_type).into() }
}

/// Decodes a `PrimitiveArray` from rows
pub fn decode_primitive4<T: ArrowPrimitiveType>(
    rows: &mut [&[u8]],
    data_type1: DataType,
    data_type2: DataType,
    data_type3: DataType,
    data_type4: DataType,
) -> (
    PrimitiveArray<T>,
    PrimitiveArray<T>,
    PrimitiveArray<T>,
    PrimitiveArray<T>,
)
where
    T::Native: FixedLengthEncoding,
{
    assert!(PrimitiveArray::<T>::is_compatible(&data_type1));
    assert!(PrimitiveArray::<T>::is_compatible(&data_type2));
    assert!(PrimitiveArray::<T>::is_compatible(&data_type3));
    assert!(PrimitiveArray::<T>::is_compatible(&data_type4));
    // SAFETY:
    // Validated data type above
    let (data1, data2, data3, data4) = unsafe { decode_fixed_four::<T::Native>(rows, data_type1, data_type2, data_type3, data_type4) };

    (data1.into(), data2.into(), data3.into(), data4.into())
}

/// Decodes a `FixedLengthBinary` from rows
pub fn decode_fixed_size_binary(rows: &mut [&[u8]], size: i32) -> FixedSizeBinaryArray {
    let len = rows.len();

    let mut values = MutableBuffer::new(size as usize * rows.len());
    let (null_count, nulls) = decode_nulls(rows);

    let encoded_len = size as usize + 1;

    for row in rows {
        let i = split_off(row, encoded_len);
        values.extend_from_slice(&i[1..]);
    }

    let builder = ArrayDataBuilder::new(DataType::FixedSizeBinary(size))
        .len(len)
        .null_count(null_count)
        .add_buffer(values.into())
        .null_bit_buffer(Some(nulls));

    // SAFETY: Buffers correct length
    unsafe { builder.build_unchecked().into() }
}
