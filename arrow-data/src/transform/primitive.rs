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
