// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use super::{_MutableArrayData, Extend};
use crate::ArrayData;
use crate::data::get_fixed_size_binary_width;
use arrow_schema::ArrowError;

pub(super) fn build_extend(array: &ArrayData) -> Extend<'_> {
    let size = get_fixed_size_binary_width(array.data_type());
    let values = &array.buffers()[0].as_slice()[array.offset() * size..];
    Box::new(
        move |mutable: &mut _MutableArrayData, _, start: usize, len: usize| {
            let buffer = &mut mutable.buffer1;
            buffer
                .try_extend_from_slice(&values[start * size..(start + len) * size])
                .map_err(|e| ArrowError::MemoryError(e.to_string()))?;
            Ok(())
        },
    )
}

pub(super) fn extend_nulls(
    mutable: &mut _MutableArrayData,
    len: usize,
) -> Result<(), arrow_schema::ArrowError> {
    let size = get_fixed_size_binary_width(&mutable.data_type);
    let byte_len = len
        .checked_mul(size)
        .ok_or_else(|| ArrowError::MemoryError("buffer size overflow".into()))?;

    let values_buffer = &mut mutable.buffer1;
    values_buffer
        .try_extend_zeros(byte_len)
        .map_err(|e| arrow_schema::ArrowError::MemoryError(e.to_string()))?;
    Ok(())
}
