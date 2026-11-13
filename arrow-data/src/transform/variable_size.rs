// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::ArrayData;
use arrow_buffer::{ArrowNativeType, MutableBuffer, ToByteSlice};
use arrow_schema::ArrowError;
use num_integer::Integer;
use num_traits::{AsPrimitive, CheckedAdd};

use super::utils::memory_error;
use super::{
    _MutableArrayData, Extend,
    utils::{get_last_offset, try_extend_offsets, try_reserve},
};

#[inline]
fn extend_offset_values<T: ArrowNativeType + AsPrimitive<usize>>(
    buffer: &mut MutableBuffer,
    offsets: &[T],
    values: &[u8],
    start: usize,
    len: usize,
) -> Result<(), ArrowError> {
    let start_values = offsets[start].as_();
    let end_values: usize = offsets[start + len].as_();
    let new_values = &values[start_values..end_values];
    try_reserve(buffer, new_values.len())?;
    buffer.extend_from_slice(new_values);
    Ok(())
}

pub(super) fn build_extend<T: ArrowNativeType + Integer + CheckedAdd + AsPrimitive<usize>>(
    array: &ArrayData,
) -> Extend<'_> {
    let offsets = array.buffer::<T>(0);
    let values = array.buffers()[1].as_slice();
    Box::new(
        move |mutable: &mut _MutableArrayData, _, start: usize, len: usize| {
            let offset_buffer = &mut mutable.buffer1;
            let values_buffer = &mut mutable.buffer2;

            // this is safe due to how offset is built. See details on `get_last_offset`
            let last_offset = unsafe { get_last_offset(offset_buffer) };

            try_extend_offsets::<T>(offset_buffer, last_offset, &offsets[start..start + len + 1])?;
            // values
            extend_offset_values::<T>(values_buffer, offsets, values, start, len)?;
            Ok(())
        },
    )
}

pub(super) fn extend_nulls<T: ArrowNativeType>(
    mutable: &mut _MutableArrayData,
    len: usize,
) -> Result<(), arrow_schema::ArrowError> {
    let offset_buffer = &mut mutable.buffer1;

    // this is safe due to how offset is built. See details on `get_last_offset`
    let last_offset: T = unsafe { get_last_offset(offset_buffer) };

    offset_buffer
        .try_repeat_slice_n_times(last_offset.to_byte_slice(), len)
        .map_err(memory_error)?;
    Ok(())
}
