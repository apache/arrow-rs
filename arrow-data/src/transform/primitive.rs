// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::ArrayData;
use arrow_buffer::ArrowNativeType;
use arrow_schema::ArrowError;
use std::mem::size_of;
use std::ops::Add;

use super::{_MutableArrayData, Extend, utils::memory_error};

pub(super) fn build_extend<T: ArrowNativeType>(array: &ArrayData) -> Extend<'_> {
    let values = array.buffer::<T>(0);
    Box::new(
        move |mutable: &mut _MutableArrayData, _, start: usize, len: usize| {
            mutable
                .buffer1
                .extend_from_slice(&values[start..start + len]);
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
            mutable
                .buffer1
                .extend(values[start..start + len].iter().map(|x| *x + offset));
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
        .map_err(memory_error)?;
    Ok(())
}
