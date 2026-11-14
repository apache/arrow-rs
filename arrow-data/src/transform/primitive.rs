// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::ArrayData;
use arrow_buffer::ArrowNativeType;
use std::mem::size_of;
use std::ops::Add;

use super::{
    _MutableArrayData, Extend,
    utils::{memory_error, try_extend_with},
};

pub(super) fn build_extend<T: ArrowNativeType>(array: &ArrayData) -> Extend<'_> {
    let values = array.buffer::<T>(0);
    Box::new(
        move |mutable: &mut _MutableArrayData, _, start: usize, len: usize| {
            let values = &values[start..start + len];
            mutable
                .buffer1
                .try_extend_from_slice(values)
                .map_err(memory_error)?;
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
            try_extend_with(&mut mutable.buffer1, values.len(), |i| values[i] + offset)
        },
    )
}

pub(super) fn extend_nulls<T: ArrowNativeType>(
    mutable: &mut _MutableArrayData,
    len: usize,
) -> Result<(), arrow_schema::ArrowError> {
    mutable
        .buffer1
        .try_extend_zeros(len * size_of::<T>())
        .map_err(|e| arrow_schema::ArrowError::MemoryError(e.to_string()))?;
    Ok(())
}
