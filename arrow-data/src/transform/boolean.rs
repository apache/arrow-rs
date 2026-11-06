// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use super::{_MutableArrayData, Extend, utils::try_resize_for_bits};
use crate::ArrayData;
use crate::bit_mask::set_bits;
use arrow_schema::ArrowError;

pub(super) fn build_extend(array: &ArrayData) -> Extend<'_> {
    let values = array.buffers()[0].as_slice();
    Box::new(
        move |mutable: &mut _MutableArrayData, _, start: usize, len: usize| {
            let new_len = mutable
                .len
                .checked_add(len)
                .ok_or_else(|| ArrowError::MemoryError("length overflow".into()))?;
            let buffer = &mut mutable.buffer1;
            try_resize_for_bits(buffer, new_len)?;
            set_bits(
                buffer.as_slice_mut(),
                values,
                mutable.len,
                array.offset() + start,
                len,
            );
            Ok(())
        },
    )
}

pub(super) fn extend_nulls(
    mutable: &mut _MutableArrayData,
    len: usize,
) -> Result<(), arrow_schema::ArrowError> {
    let new_len = mutable
        .len
        .checked_add(len)
        .ok_or_else(|| ArrowError::MemoryError("length overflow".into()))?;
    let buffer = &mut mutable.buffer1;
    try_resize_for_bits(buffer, new_len)?;
    Ok(())
}
