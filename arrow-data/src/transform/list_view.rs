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
        move |mutable: &mut _MutableArrayData, _index: usize, start: usize, len: usize| {
            let offset_buffer = &mut mutable.buffer1;
            let sizes_buffer = &mut mutable.buffer2;

            for &offset in &offsets[start..start + len] {
                offset_buffer.push(offset);
            }

            // sizes
            for &size in &sizes[start..start + len] {
                sizes_buffer.push(size);
            }

            // the beauty of views is that we don't need to copy child_data, we just splat
            // the offsets and sizes.
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
