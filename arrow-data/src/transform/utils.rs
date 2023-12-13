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

use arrow_buffer::{bit_util, ArrowNativeType, MutableBuffer};
use num::{CheckedAdd, Integer};

/// extends the `buffer` to be able to hold `len` bits, setting all bits of the new size to zero.
#[inline]
pub(super) fn resize_for_bits(buffer: &mut MutableBuffer, len: usize) {
    let needed_bytes = bit_util::ceil(len, 8);
    if buffer.len() < needed_bytes {
        buffer.resize(needed_bytes, 0);
    }
}

pub(super) fn extend_offsets<T: ArrowNativeType + Integer + CheckedAdd>(
    buffer: &mut MutableBuffer,
    mut last_offset: T,
    offsets: &[T],
) {
    buffer.reserve(std::mem::size_of_val(offsets));
    offsets.windows(2).for_each(|offsets| {
        // compute the new offset
        let length = offsets[1] - offsets[0];
        // if you hit this appending to a StringArray / BinaryArray it is because you
        // are trying to add more data than can fit into that type. Try breaking your data into
        // smaller batches or using LargeStringArray / LargeBinaryArray
        last_offset = last_offset.checked_add(&length).expect("offset overflow");
        buffer.push(last_offset);
    });
}

#[inline]
pub(super) unsafe fn get_last_offset<T: ArrowNativeType>(offset_buffer: &MutableBuffer) -> T {
    // JUSTIFICATION
    //  Benefit
    //      20% performance improvement extend of variable sized arrays (see bench `mutable_array`)
    //  Soundness
    //      * offset buffer is always extended in slices of T and aligned accordingly.
    //      * Buffer[0] is initialized with one element, 0, and thus `mutable_offsets.len() - 1` is always valid.
    let (prefix, offsets, suffix) = offset_buffer.as_slice().align_to::<T>();
    debug_assert!(prefix.is_empty() && suffix.is_empty());
    *offsets.get_unchecked(offsets.len() - 1)
}

#[cfg(test)]
mod tests {
    use crate::transform::utils::extend_offsets;
    use arrow_buffer::MutableBuffer;

    #[test]
    #[should_panic(expected = "offset overflow")]
    fn test_overflow() {
        let mut buffer = MutableBuffer::new(10);
        extend_offsets(&mut buffer, i32::MAX - 4, &[0, 5]);
    }
}
