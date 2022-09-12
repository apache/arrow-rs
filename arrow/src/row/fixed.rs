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
use crate::compute::SortOptions;
use crate::datatypes::ArrowPrimitiveType;
use crate::row::Rows;
use crate::util::decimal::{Decimal128, Decimal256};
use half::f16;

/// Encodes a value of a particular fixed width type into bytes according to the rules
/// described on [`super::RowConverter`]
pub trait FixedLengthEncoding<const N: usize>: Copy {
    const ENCODED_LEN: usize = 1 + N;

    fn encode(self) -> [u8; N];
}

impl FixedLengthEncoding<1> for bool {
    fn encode(self) -> [u8; 1] {
        [self as u8]
    }
}

macro_rules! encode_signed {
    ($n:expr, $t:ty) => {
        impl FixedLengthEncoding<$n> for $t {
            fn encode(self) -> [u8; $n] {
                let mut b = self.to_be_bytes();
                // Toggle top "sign" bit to ensure consistent sort order
                b[0] ^= 0x80;
                b
            }
        }
    };
}

encode_signed!(1, i8);
encode_signed!(2, i16);
encode_signed!(4, i32);
encode_signed!(8, i64);
encode_signed!(16, i128);

macro_rules! encode_unsigned {
    ($n:expr, $t:ty) => {
        impl FixedLengthEncoding<$n> for $t {
            fn encode(self) -> [u8; $n] {
                self.to_be_bytes()
            }
        }
    };
}

encode_unsigned!(1, u8);
encode_unsigned!(2, u16);
encode_unsigned!(4, u32);
encode_unsigned!(8, u64);

impl FixedLengthEncoding<2> for f16 {
    fn encode(self) -> [u8; 2] {
        // https://github.com/rust-lang/rust/blob/9c20b2a8cc7588decb6de25ac6a7912dcef24d65/library/core/src/num/f32.rs#L1176-L1260
        let s = self.to_bits() as i16;
        let val = s ^ (((s >> 15) as u16) >> 1) as i16;
        val.encode()
    }
}

impl FixedLengthEncoding<4> for f32 {
    fn encode(self) -> [u8; 4] {
        // https://github.com/rust-lang/rust/blob/9c20b2a8cc7588decb6de25ac6a7912dcef24d65/library/core/src/num/f32.rs#L1176-L1260
        let s = self.to_bits() as i32;
        let val = s ^ (((s >> 31) as u32) >> 1) as i32;
        val.encode()
    }
}

impl FixedLengthEncoding<8> for f64 {
    fn encode(self) -> [u8; 8] {
        // https://github.com/rust-lang/rust/blob/9c20b2a8cc7588decb6de25ac6a7912dcef24d65/library/core/src/num/f32.rs#L1176-L1260
        let s = self.to_bits() as i64;
        let val = s ^ (((s >> 63) as u64) >> 1) as i64;
        val.encode()
    }
}

impl FixedLengthEncoding<16> for Decimal128 {
    fn encode(self) -> [u8; 16] {
        let mut val = *self.raw_value();
        // Convert to big endian representation
        val.reverse();
        // Toggle top "sign" bit to ensure consistent sort order
        val[0] ^= 0x80;
        val
    }
}

impl FixedLengthEncoding<32> for Decimal256 {
    fn encode(self) -> [u8; 32] {
        let mut val = *self.raw_value();
        // Convert to big endian representation
        val.reverse();
        // Toggle top "sign" bit to ensure consistent sort order
        val[0] ^= 0x80;
        val
    }
}

/// Returns the total encoded length (including null byte) for a value of type `T::Native`
pub const fn encoded_len<const N: usize, T>(_col: &PrimitiveArray<T>) -> usize
where
    T: ArrowPrimitiveType,
    T::Native: FixedLengthEncoding<N>,
{
    T::Native::ENCODED_LEN
}

/// Fixed width types are encoded as
///
/// - 1 byte `0` if null or `1` if valid
/// - bytes of [`FixedLengthEncoding`]
pub fn encode<
    const N: usize,
    T: FixedLengthEncoding<N>,
    I: IntoIterator<Item = Option<T>>,
>(
    out: &mut Rows,
    i: I,
    opts: SortOptions,
) {
    for (offset, maybe_val) in out.offsets.iter_mut().skip(1).zip(i) {
        let end_offset = *offset + N + 1;
        if let Some(val) = maybe_val {
            let to_write = &mut out.buffer[*offset..end_offset];
            to_write[0] = 1;
            let mut encoded = val.encode();
            if opts.descending {
                // Flip bits to reverse order
                encoded.iter_mut().for_each(|v| *v = !*v)
            }
            to_write[1..].copy_from_slice(&encoded)
        } else if !opts.nulls_first {
            out.buffer[*offset] = 0xFF;
        }
        *offset = end_offset;
    }
}
