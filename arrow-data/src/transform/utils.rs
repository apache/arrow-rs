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

use arrow_buffer::{ArrowNativeType, MutableBuffer, bit_util};
use arrow_schema::ArrowError;
use num_integer::Integer;
use num_traits::CheckedAdd;

/// extends the `buffer` to be able to hold `len` bits, setting all bits of the new size to zero.
#[inline]
pub(super) fn resize_for_bits(buffer: &mut MutableBuffer, len: usize) {
    let needed_bytes = bit_util::ceil(len, 8);
    if buffer.len() < needed_bytes {
        buffer.resize(needed_bytes, 0);
    }
}

/// Extends `buffer` with the re-based offsets from `offsets`, returning an error on overflow.
pub(super) fn try_extend_offsets<T: ArrowNativeType + Integer + CheckedAdd>(
    buffer: &mut MutableBuffer,
    mut last_offset: T,
    offsets: &[T],
) -> Result<(), ArrowError> {
    buffer.reserve(std::mem::size_of_val(offsets));
    // Snapshot the length so we can roll back partial writes on overflow.
    let original_len = buffer.len();
    for window in offsets.windows(2) {
        let length = window[1] - window[0];
        match last_offset.checked_add(&length) {
            Some(new_offset) => {
                last_offset = new_offset;
                buffer.push(last_offset);
            }
            None => {
                // Restore the buffer to its state before this call so the
                // caller is not left with a partially-written offset sequence.
                buffer.resize(original_len, 0);
                return Err(ArrowError::InvalidArgumentError(
                    "offset overflow: data exceeds the capacity of the offset type. \
                     Try splitting into smaller batches or using a larger type \
                     (e.g. LargeStringArray / LargeBinaryArray instead of StringArray / BinaryArray)"
                        .to_string(),
                ));
            }
        }
    }
    Ok(())
}

#[inline]
pub(super) unsafe fn get_last_offset<T: ArrowNativeType>(offset_buffer: &MutableBuffer) -> T {
    // JUSTIFICATION
    //  Benefit
    //      20% performance improvement extend of variable sized arrays (see bench `mutable_array`)
    //  Soundness
    //      * offset buffer is always extended in slices of T and aligned accordingly.
    //      * Buffer[0] is initialized with one element, 0, and thus `mutable_offsets.len() - 1` is always valid.
    let (prefix, offsets, suffix) = unsafe { offset_buffer.as_slice().align_to::<T>() };
    debug_assert!(prefix.is_empty() && suffix.is_empty());
    *unsafe { offsets.get_unchecked(offsets.len() - 1) }
}

#[cfg(test)]
mod tests {
    use crate::transform::utils::try_extend_offsets;
    use arrow_buffer::MutableBuffer;

    #[test]
    fn test_overflow_returns_error() {
        let mut buffer = MutableBuffer::new(10);
        let err = try_extend_offsets(&mut buffer, i32::MAX - 4, &[0i32, 5]).unwrap_err();
        assert!(
            err.to_string().contains("offset overflow"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_overflow_restores_buffer() {
        // Pre-populate the buffer with a known-good offset so we can verify
        // it is unchanged after a failed extend.
        let mut buffer = MutableBuffer::new(16);
        buffer.push(0i32);
        buffer.push(10i32);
        let len_before = buffer.len();

        // Offsets [0, 3, i32::MAX]: the second window (3 → i32::MAX) will overflow
        // because last_offset (i32::MAX - 4 + 3 = i32::MAX - 1) + (i32::MAX - 3) overflows.
        // Use a simpler case: start near MAX so the very first window overflows.
        let err = try_extend_offsets(&mut buffer, i32::MAX - 2, &[0i32, 5]).unwrap_err();
        assert!(
            err.to_string().contains("offset overflow"),
            "unexpected error: {err}"
        );
        // Buffer must be exactly as it was before the failed call.
        assert_eq!(
            buffer.len(),
            len_before,
            "buffer length changed after overflow rollback"
        );
    }
}
