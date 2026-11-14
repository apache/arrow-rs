// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use super::{
    _MutableArrayData, Extend,
    utils::{try_extend_repeated, try_reserve},
};
use crate::ArrayData;
use arrow_schema::{ArrowError, DataType};
use std::mem::size_of;

fn nullable_union_field(fields: &arrow_schema::UnionFields) -> Result<(usize, i8), ArrowError> {
    fields
        .iter()
        .enumerate()
        .find_map(|(index, (type_id, field))| field.is_nullable().then_some((index, type_id)))
        .ok_or_else(|| {
            ArrowError::InvalidArgumentError(if fields.is_empty() {
                "Cannot extend a union with no fields with nulls".to_owned()
            } else {
                "Cannot extend a union with no nullable fields with nulls".to_owned()
            })
        })
}

pub(super) fn build_extend_sparse(array: &ArrayData) -> Extend<'_> {
    // `buffer` already applies the parent offset to the type ids
    let type_ids = array.buffer::<i8>(0);
    // but a sparse union's children are not sliced by the parent offset
    let offset = array.offset();

    Box::new(
        move |mutable: &mut _MutableArrayData, index: usize, start: usize, len: usize| {
            // extends type_ids
            mutable
                .buffer1
                .try_extend_from_slice(&type_ids[start..start + len])
                .map_err(|e| ArrowError::MemoryError(e.to_string()))?;

            let child_start = start.checked_add(offset).ok_or_else(|| {
                ArrowError::InvalidArgumentError(format!(
                    "sparse union offset {offset} with start {start} overflows usize"
                ))
            })?;
            let child_end = child_start.checked_add(len).ok_or_else(|| {
                ArrowError::InvalidArgumentError(format!(
                    "sparse union start {child_start} with length {len} overflows usize"
                ))
            })?;
            for child in &mut mutable.child_data {
                child.try_extend(index, child_start, child_end)?;
            }
            Ok(())
        },
    )
}

pub(super) fn build_extend_dense(array: &ArrayData) -> Extend<'_> {
    let type_ids = array.buffer::<i8>(0);
    let offsets = array.buffer::<i32>(1);
    let arrow_schema::DataType::Union(src_fields, _) = array.data_type() else {
        unreachable!();
    };

    Box::new(
        move |mutable: &mut _MutableArrayData, index: usize, start: usize, len: usize| {
            // extends type_ids
            mutable
                .buffer1
                .try_extend_from_slice(&type_ids[start..start + len])
                .map_err(|e| ArrowError::MemoryError(e.to_string()))?;
            let offsets_len = len
                .checked_mul(size_of::<i32>())
                .ok_or_else(|| ArrowError::MemoryError("union offset size overflow".into()))?;
            try_reserve(&mut mutable.buffer2, offsets_len)?;

            for i in start..start + len {
                let type_id = type_ids[i];
                let child_index = src_fields
                    .iter()
                    .position(|(r, _)| r == type_id)
                    .expect("invalid union type ID");
                let src_offset = offsets[i] as usize;
                let child_data = &mut mutable.child_data[child_index];
                let dst_offset = child_data.len();

                // Extend offsets
                // SAFETY: The destination was reserved for every offset above.
                unsafe { mutable.buffer2.push_unchecked(dst_offset as i32) };
                mutable.child_data[child_index].try_extend(index, src_offset, src_offset + 1)?;
            }
            Ok(())
        },
    )
}

pub(super) fn extend_nulls_dense(
    mutable: &mut _MutableArrayData,
    len: usize,
) -> Result<(), ArrowError> {
    let DataType::Union(fields, _) = &mutable.data_type else {
        unreachable!()
    };
    let (child_index, null_type_id) = nullable_union_field(fields)?;

    // Dense: point into a nullable child and extend its values with nulls.
    let child_offset = mutable.child_data[child_index].len();
    let (start, end) = (child_offset as i32, (child_offset + len) as i32);
    let offsets_len = len
        .checked_mul(size_of::<i32>())
        .ok_or_else(|| ArrowError::MemoryError("union offset size overflow".into()))?;
    try_reserve(&mut mutable.buffer1, len)?;
    try_reserve(&mut mutable.buffer2, offsets_len)?;

    mutable.child_data[child_index].try_extend_nulls(len)?;
    try_extend_repeated(&mut mutable.buffer1, null_type_id, len)?;
    for offset in start..end {
        // SAFETY: The offset buffer was reserved for every value above.
        unsafe { mutable.buffer2.push_unchecked(offset) };
    }
    Ok(())
}

pub(super) fn extend_nulls_sparse(
    mutable: &mut _MutableArrayData,
    len: usize,
) -> Result<(), ArrowError> {
    let DataType::Union(fields, _) = &mutable.data_type else {
        unreachable!()
    };
    let (_, null_type_id) = nullable_union_field(fields)?;

    try_reserve(&mut mutable.buffer1, len)?;

    // Sparse: extend nulls in ALL children
    for child in &mut mutable.child_data {
        child.try_extend_nulls(len)?;
    }
    try_extend_repeated(&mut mutable.buffer1, null_type_id, len)?;
    Ok(())
}
