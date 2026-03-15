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

use crate::ArrayData;
use crate::transform::_MutableArrayData;
use arrow_buffer::ArrowNativeType;
use num_integer::Integer;
use num_traits::CheckedAdd;

pub(super) fn build_extend<T: ArrowNativeType + Integer + CheckedAdd>(
    array: &ArrayData,
) -> crate::transform::Extend<'_> {
    let offsets = array.buffer::<T>(0);
    let sizes = array.buffer::<T>(1);
    Box::new(
        move |mutable: &mut _MutableArrayData, index: usize, start: usize, len: usize| {
            let offset_buffer = &mut mutable.buffer1;
            let sizes_buffer = &mut mutable.buffer2;

            // MutableArrayData builds a new independent array, so we must copy the child values
            // that the source elements reference. Since ListView allows a
            // non-contiguous offset layout we can do this efficiently by finding the
            // bounding child range [child_min, child_max) that covers all elements being copied.
            // We then bulk copy that entire range into the output child array, and
            // remap each element's offset to point to its data at the new
            // location. Sizes are unchanged.
            //
            // The copied range may include unreferenced child values in gaps
            // between elements, but that is fine — ListView allows non-contiguous
            // offsets, so the (offset, size) pairs precisely identify each
            // element's data regardless of what else sits in the child array.

            // Find the bounding child range
            let mut child_min = usize::MAX;
            let mut child_max = 0usize;
            for i in start..start + len {
                let size = sizes[i].as_usize();
                if size > 0 {
                    let offset = offsets[i].as_usize();
                    child_min = child_min.min(offset);
                    child_max = child_max.max(offset + size);
                }
            }

            // Copy the child range
            let child_base = if child_max > child_min {
                let base = mutable.child_data[0].len();
                mutable.child_data[0].extend(index, child_min, child_max);
                base
            } else {
                0
            };

            // Remap offsets
            for i in start..start + len {
                let size = sizes[i].as_usize();
                let new_offset = if size > 0 {
                    offsets[i].as_usize() - child_min + child_base
                } else {
                    0
                };
                offset_buffer.push(T::from_usize(new_offset).expect("offset overflow"));
                sizes_buffer.push(sizes[i]);
            }
        },
    )
}

pub(super) fn extend_nulls<T: ArrowNativeType>(mutable: &mut _MutableArrayData, len: usize) {
    let offset_buffer = &mut mutable.buffer1;
    let sizes_buffer = &mut mutable.buffer2;

    // We push 0 as a placeholder for NULL values in both the offsets and sizes
    (0..len).for_each(|_| offset_buffer.push(T::default()));
    (0..len).for_each(|_| sizes_buffer.push(T::default()));
}
