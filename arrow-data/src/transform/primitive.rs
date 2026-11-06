// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::ArrayData;
use arrow_buffer::ArrowNativeType;
use arrow_schema::ArrowError;
use std::mem::size_of;
use std::ops::Add;

use super::{_MutableArrayData, Extend};

pub(super) fn build_extend<T: ArrowNativeType>(array: &ArrayData) -> Extend<'_> {
    let values = array.buffer::<T>(0);
    Box::new(
        move |mutable: &mut _MutableArrayData, _, start: usize, len: usize| {
            mutable
                .buffer1
                .try_extend_from_slice(&values[start..start + len])
                .map_err(|e| ArrowError::MemoryError(e.to_string()))?;
            Ok(())
        },
    )
}

pub(super) fn build_extend_with_offset<T>(array: &ArrayData, offset: T) -> Extend<'_>
where
    T: ArrowNativeType + Add<Output = T>,
{
    let values = array.buffer::<T>(0);
    Box::new(
        move |mutable: &mut _MutableArrayData, _, start: usize, len: usize| {
            let values = &values[start..start + len];
            let additional = values
                .len()
                .checked_mul(size_of::<T>())
                .ok_or_else(|| ArrowError::MemoryError("buffer size overflow".into()))?;
            mutable
                .buffer1
                .try_reserve(additional)
                .map_err(|e| ArrowError::MemoryError(e.to_string()))?;
            mutable.buffer1.extend(values.iter().map(|x| *x + offset));
            Ok(())
        },
    )
}

pub(super) fn extend_nulls<T: ArrowNativeType>(
    mutable: &mut _MutableArrayData,
    len: usize,
) -> Result<(), arrow_schema::ArrowError> {
    let byte_len = len
        .checked_mul(size_of::<T>())
        .ok_or_else(|| ArrowError::MemoryError("buffer size overflow".into()))?;
    mutable
        .buffer1
        .try_extend_zeros(byte_len)
        .map_err(|e| arrow_schema::ArrowError::MemoryError(e.to_string()))?;
    Ok(())
}
