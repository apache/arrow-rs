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

use arrow_array::cast::AsArray;
use arrow_array::types::{BinaryType, ByteArrayType, LargeBinaryType, LargeUtf8Type, Utf8Type};
use arrow_array::{Array, ArrayRef, GenericByteArray};
use arrow_buffer::{ArrowNativeType, Buffer, NullBufferBuilder, OffsetBuffer, ScalarBuffer};
use arrow_schema::{ArrowError, DataType};
use std::sync::Arc;

use crate::coalesce::InProgressArray;

#[derive(Debug)]
pub(crate) struct InProgressByteArray {
    kind: DataType,
    source: Option<ArrayRef>,
    // intermediate byte arrays ((offset,len) array)
    intermeditate_buffers: Vec<((usize, usize), ArrayRef)>,
}

impl InProgressByteArray {
    pub(crate) fn new(kind: DataType) -> Self {
        Self {
            kind,
            source: None,
            intermeditate_buffers: vec![],
        }
    }
}

/// Pre-allocates output buffers in one shot and copies byte ranges from each
/// source array without going through any arrow validation.
fn build_generic<T: ByteArrayType>(
    ranges: &[((usize, usize), ArrayRef)],
    get_array: impl Fn(&ArrayRef) -> &GenericByteArray<T>,
) -> Result<ArrayRef, ArrowError> {
    // Single pass: compute total_bytes and total_rows together, grouping
    // consecutive entries from the same ArrayRef to avoid redundant downcasts.
    let (total_bytes, total_rows) = {
        let mut bytes = 0usize;
        let mut rows = 0usize;
        let mut iter = ranges.iter().peekable();
        while let Some(((offset, len), arr)) = iter.next() {
            let off = get_array(arr).value_offsets();
            bytes += off[offset + len].as_usize() - off[*offset].as_usize();
            rows += len;
            while let Some(((next_off, next_len), next_arr)) = iter.peek() {
                if Arc::ptr_eq(arr, next_arr) {
                    bytes += off[next_off + next_len].as_usize() - off[*next_off].as_usize();
                    rows += next_len;
                    iter.next();
                } else {
                    break;
                }
            }
        }
        (bytes, rows)
    };

    let mut out_values: Vec<u8> = Vec::with_capacity(total_bytes);
    let mut out_offsets: Vec<T::Offset> = Vec::with_capacity(total_rows + 1);
    let mut null_builder = NullBufferBuilder::new(total_rows);
    out_offsets.push(T::Offset::usize_as(0));

    // Copy pass: group consecutive entries from the same ArrayRef so we
    // downcast and fetch value_data/offsets/nulls once per source.
    let mut iter = ranges.iter().peekable();
    while let Some(((offset, len), arr)) = iter.next() {
        let a = get_array(arr);
        let value_data = a.value_data();
        let off = a.value_offsets();
        let source_nulls = a.nulls();

        let mut copy_range = |offset: usize, len: usize| {
            if let Some(nulls) = source_nulls {
                null_builder.append_buffer(&nulls.slice(offset, len));
            } else {
                null_builder.append_n_non_nulls(len);
            }
            let base_byte = off[offset].as_usize();
            let out_base = out_values.len();
            out_values.extend_from_slice(&value_data[base_byte..off[offset + len].as_usize()]);
            for i in offset + 1..=offset + len {
                out_offsets.push(T::Offset::usize_as(out_base + (off[i].as_usize() - base_byte)));
            }
        };

        copy_range(*offset, *len);

        while let Some(((next_off, next_len), next_arr)) = iter.peek() {
            if Arc::ptr_eq(arr, next_arr) {
                let (o, l) = (*next_off, *next_len);
                iter.next();
                copy_range(o, l);
            } else {
                break;
            }
        }
    }

    let nulls = null_builder.finish();
    // SAFETY: offsets are monotonically increasing and in-bounds for out_values;
    // source arrays were already validated on construction.
    let offsets = unsafe { OffsetBuffer::new_unchecked(ScalarBuffer::from(out_offsets)) };
    let values = Buffer::from_vec(out_values);
    let array = unsafe { GenericByteArray::<T>::new_unchecked(offsets, values, nulls) };
    Ok(Arc::new(array))
}

impl InProgressArray for InProgressByteArray {
    fn set_source(&mut self, source: Option<ArrayRef>) {
        self.source = source
    }

    fn copy_rows(&mut self, offset: usize, len: usize) -> Result<(), ArrowError> {
        let source = self.source.as_ref().ok_or_else(|| {
            ArrowError::InvalidArgumentError(
                "Internal Error: InProgressByteArray: source not set".to_string(),
            )
        })?;
        self.intermeditate_buffers
            .push(((offset, len), source.clone()));
        Ok(())
    }

    fn finish(&mut self) -> Result<ArrayRef, ArrowError> {
        let buffers = std::mem::take(&mut self.intermeditate_buffers);

        let data_type = buffers
            .first()
            .map(|(_, arr)| arr.data_type().clone())
            .unwrap_or_else(|| self.kind.clone());

        match data_type {
            DataType::Utf8 => build_generic::<Utf8Type>(&buffers, |a| a.as_string::<i32>()),
            DataType::LargeUtf8 => {
                build_generic::<LargeUtf8Type>(&buffers, |a| a.as_string::<i64>())
            }
            DataType::Binary => build_generic::<BinaryType>(&buffers, |a| a.as_binary::<i32>()),
            DataType::LargeBinary => {
                build_generic::<LargeBinaryType>(&buffers, |a| a.as_binary::<i64>())
            }
            dt => Err(ArrowError::InvalidArgumentError(format!(
                "InProgressByteArray: unsupported type {dt}"
            ))),
        }
    }

    fn size(&self) -> usize {
        self.source
            .as_ref()
            .map_or(0, |s| s.get_array_memory_size())
            + self.intermeditate_buffers.capacity()
                * std::mem::size_of::<((usize, usize), ArrayRef)>()
    }
}
