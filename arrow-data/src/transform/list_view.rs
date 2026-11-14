// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::ArrayData;
use crate::transform::_MutableArrayData;
use arrow_buffer::ArrowNativeType;
use arrow_schema::ArrowError;
use num_integer::Integer;
use num_traits::CheckedAdd;
use std::mem::size_of;

use super::utils::{try_extend_repeated, try_reserve};

pub(super) fn build_extend<T: ArrowNativeType + Integer + CheckedAdd>(
    array: &ArrayData,
) -> crate::transform::Extend<'_> {
    let offsets = array.buffer::<T>(0);
    let sizes = array.buffer::<T>(1);
    Box::new(
        move |mutable: &mut _MutableArrayData, index: usize, start: usize, len: usize| {
            let additional = len
                .checked_mul(size_of::<T>())
                .ok_or_else(|| ArrowError::MemoryError("list-view buffer size overflow".into()))?;
            try_reserve(&mut mutable.buffer1, additional)?;
            try_reserve(&mut mutable.buffer2, additional)?;

            let mut new_offset = T::usize_as(mutable.child_data[0].len());
            for i in start..start + len {
                let next_offset = new_offset.checked_add(&sizes[i]).ok_or_else(|| {
                    ArrowError::InvalidArgumentError(
                        "offset overflow: data exceeds the capacity of the offset type. \
                         Try splitting into smaller batches or using a larger type \
                         (e.g. LargeListView instead of ListView)"
                            .to_string(),
                    )
                })?;
                // SAFETY: Both buffers were reserved for every value above.
                unsafe {
                    mutable.buffer1.push_unchecked(new_offset);
                    mutable.buffer2.push_unchecked(sizes[i]);
                }
                new_offset = next_offset;

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
    let additional = len
        .checked_mul(size_of::<T>())
        .ok_or_else(|| ArrowError::MemoryError("list-view buffer size overflow".into()))?;
    try_reserve(offset_buffer, additional)?;
    try_reserve(sizes_buffer, additional)?;

    // We push 0 as a placeholder for NULL values in both the offsets and sizes
    try_extend_repeated(offset_buffer, T::default(), len)?;
    try_extend_repeated(sizes_buffer, T::default(), len)?;
    Ok(())
}
