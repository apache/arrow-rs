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
use arrow_schema::ArrowError;
use num_integer::Integer;
use num_traits::CheckedAdd;

pub(super) fn build_extend<T: ArrowNativeType + Integer + CheckedAdd>(
    array: &ArrayData,
) -> crate::transform::Extend<'_> {
    let offsets = array.buffer::<T>(0);
    let sizes = array.buffer::<T>(1);
    Box::new(
        move |mutable: &mut _MutableArrayData, index: usize, start: usize, len: usize| {
            let mut new_offset = T::usize_as(mutable.child_data[0].len());

            for i in start..start + len {
                mutable.buffer1.push(new_offset);
                mutable.buffer2.push(sizes[i]);
                new_offset = new_offset.checked_add(&sizes[i]).ok_or_else(|| {
                    ArrowError::InvalidArgumentError(
                        "offset overflow: data exceeds the capacity of the offset type. \
                         Try splitting into smaller batches or using a larger type \
                         (e.g. LargeListView instead of ListView)"
                            .to_string(),
                    )
                })?;

                let size = sizes[i].as_usize();
                if size > 0 {
                    let child_start = offsets[i].as_usize();
                    mutable.child_data[0].try_extend(index, child_start, child_start + size)?;
                }
            }
            Ok(())
        },
    )
}

pub(super) fn extend_nulls<T: ArrowNativeType>(
    mutable: &mut _MutableArrayData,
    len: usize,
) -> Result<(), ArrowError> {
    let offset_buffer = &mut mutable.buffer1;
    let sizes_buffer = &mut mutable.buffer2;

    // We push 0 as a placeholder for NULL values in both the offsets and sizes
    (0..len).for_each(|_| offset_buffer.push(T::default()));
    (0..len).for_each(|_| sizes_buffer.push(T::default()));
    Ok(())
}
