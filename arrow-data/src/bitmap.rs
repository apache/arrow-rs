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

//! Defines [Bitmap] for tracking validity bitmaps

use arrow_buffer::bit_util;
use arrow_schema::ArrowError;
use std::mem;

use arrow_buffer::buffer::{buffer_bin_and, buffer_bin_or, Buffer};
use std::ops::{BitAnd, BitOr};

#[derive(Debug, Clone)]
/// Defines a bitmap, which is used to track which values in an Arrow
/// array are null.
///
/// This is called a "validity bitmap" in the Arrow documentation.
pub struct Bitmap {
    pub(crate) bits: Buffer,
}

impl Bitmap {
    pub fn new(num_bits: usize) -> Self {
        let num_bytes = bit_util::ceil(num_bits, 8);
        let len = bit_util::round_upto_multiple_of_64(num_bytes);
        Bitmap {
            bits: Buffer::from(&vec![0xFF; len]),
        }
    }

    /// Return the length of this Bitmap in bits (not bytes)
    pub fn bit_len(&self) -> usize {
        self.bits.len() * 8
    }

    pub fn is_empty(&self) -> bool {
        self.bits.is_empty()
    }

    pub fn is_set(&self, i: usize) -> bool {
        assert!(i < (self.bits.len() << 3));
        unsafe { bit_util::get_bit_raw(self.bits.as_ptr(), i) }
    }

    pub fn buffer(&self) -> &Buffer {
        &self.bits
    }

    pub fn buffer_ref(&self) -> &Buffer {
        &self.bits
    }

    pub fn into_buffer(self) -> Buffer {
        self.bits
    }

    /// Returns the total number of bytes of memory occupied by the buffers owned by this [Bitmap].
    pub fn get_buffer_memory_size(&self) -> usize {
        self.bits.capacity()
    }

    /// Returns the total number of bytes of memory occupied physically by this [Bitmap].
    pub fn get_array_memory_size(&self) -> usize {
        self.bits.capacity() + mem::size_of_val(self)
    }
}

impl<'a, 'b> BitAnd<&'b Bitmap> for &'a Bitmap {
    type Output = Result<Bitmap, ArrowError>;

    fn bitand(self, rhs: &'b Bitmap) -> Result<Bitmap, ArrowError> {
        if self.bits.len() != rhs.bits.len() {
            return Err(ArrowError::ComputeError(
                "Buffers must be the same size to apply Bitwise AND.".to_string(),
            ));
        }
        Ok(Bitmap::from(buffer_bin_and(
            &self.bits,
            0,
            &rhs.bits,
            0,
            self.bit_len(),
        )))
    }
}

impl<'a, 'b> BitOr<&'b Bitmap> for &'a Bitmap {
    type Output = Result<Bitmap, ArrowError>;

    fn bitor(self, rhs: &'b Bitmap) -> Result<Bitmap, ArrowError> {
        if self.bits.len() != rhs.bits.len() {
            return Err(ArrowError::ComputeError(
                "Buffers must be the same size to apply Bitwise OR.".to_string(),
            ));
        }
        Ok(Bitmap::from(buffer_bin_or(
            &self.bits,
            0,
            &rhs.bits,
            0,
            self.bit_len(),
        )))
    }
}

impl From<Buffer> for Bitmap {
    fn from(buf: Buffer) -> Self {
        Self { bits: buf }
    }
}

impl PartialEq for Bitmap {
    fn eq(&self, other: &Self) -> bool {
        // buffer equality considers capacity, but here we want to only compare
        // actual data contents
        let self_len = self.bits.len();
        let other_len = other.bits.len();
        if self_len != other_len {
            return false;
        }
        self.bits.as_slice()[..self_len] == other.bits.as_slice()[..self_len]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitmap_length() {
        assert_eq!(512, Bitmap::new(63 * 8).bit_len());
        assert_eq!(512, Bitmap::new(64 * 8).bit_len());
        assert_eq!(1024, Bitmap::new(65 * 8).bit_len());
    }

    #[test]
    fn test_bitwise_and() {
        let bitmap1 = Bitmap::from(Buffer::from([0b01101010]));
        let bitmap2 = Bitmap::from(Buffer::from([0b01001110]));
        assert_eq!(
            Bitmap::from(Buffer::from([0b01001010])),
            (&bitmap1 & &bitmap2).unwrap()
        );
    }

    #[test]
    fn test_bitwise_or() {
        let bitmap1 = Bitmap::from(Buffer::from([0b01101010]));
        let bitmap2 = Bitmap::from(Buffer::from([0b01001110]));
        assert_eq!(
            Bitmap::from(Buffer::from([0b01101110])),
            (&bitmap1 | &bitmap2).unwrap()
        );
    }

    #[test]
    fn test_bitmap_is_set() {
        let bitmap = Bitmap::from(Buffer::from([0b01001010]));
        assert!(!bitmap.is_set(0));
        assert!(bitmap.is_set(1));
        assert!(!bitmap.is_set(2));
        assert!(bitmap.is_set(3));
        assert!(!bitmap.is_set(4));
        assert!(!bitmap.is_set(5));
        assert!(bitmap.is_set(6));
        assert!(!bitmap.is_set(7));
    }
}
