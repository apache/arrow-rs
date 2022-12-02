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

use crate::compute::SortOptions;
use crate::row::{RowConverter, Rows, SortField};
use arrow_array::builder::BufferBuilder;
use arrow_array::{Array, GenericListArray, OffsetSizeTrait};
use arrow_data::ArrayDataBuilder;
use arrow_schema::ArrowError;
use std::ops::Range;

pub fn compute_lengths<O: OffsetSizeTrait>(
    lengths: &mut [usize],
    rows: &Rows,
    array: &GenericListArray<O>,
) {
    let offsets = array.value_offsets().windows(2);
    lengths
        .iter_mut()
        .zip(offsets)
        .enumerate()
        .for_each(|(idx, (length, offsets))| {
            let start = offsets[0].as_usize();
            let end = offsets[1].as_usize();
            let range = array.is_valid(idx).then_some(start..end);
            *length += encoded_len(rows, range);
        });
}

fn encoded_len(rows: &Rows, range: Option<Range<usize>>) -> usize {
    match range {
        None => 1,
        Some(range) if range.start == range.end => 1,
        Some(range) => {
            let element_count = range.end - range.start;
            let row_bytes = range.map(|i| rows.row(i).as_ref().len()).sum::<usize>();
            let total = (1 + element_count) * std::mem::size_of::<u32>() + row_bytes;
            super::variable::padded_length(Some(total))
        }
    }
}

/// Encodes the provided `GenericListArray` to `out` with the provided `SortOptions`
///
/// `rows` should contain the encoded child elements
pub fn encode<O: OffsetSizeTrait>(
    out: &mut Rows,
    rows: &Rows,
    opts: SortOptions,
    array: &GenericListArray<O>,
) {
    let mut temporary = vec![];
    let offsets = array.value_offsets().windows(2);
    out.offsets
        .iter_mut()
        .skip(1)
        .zip(offsets)
        .enumerate()
        .for_each(|(idx, (offset, offsets))| {
            let start = offsets[0].as_usize();
            let end = offsets[1].as_usize();
            let range = array.is_valid(idx).then_some(start..end);
            let out = &mut out.buffer[*offset..];
            *offset += encode_one(out, &mut temporary, rows, range, opts)
        });
}

#[inline]
fn encode_one(
    out: &mut [u8],
    temporary: &mut Vec<u8>,
    rows: &Rows,
    range: Option<Range<usize>>,
    opts: SortOptions,
) -> usize {
    temporary.clear();

    match range {
        None => super::variable::encode_one(out, None, opts),
        Some(range) if range.start == range.end => {
            super::variable::encode_one(out, Some(&[]), opts)
        }
        Some(range) => {
            for row in range.clone().map(|i| rows.row(i)) {
                temporary.extend_from_slice(row.as_ref());
            }
            for row in range.clone().map(|i| rows.row(i)) {
                let len: u32 = row
                    .as_ref()
                    .len()
                    .try_into()
                    .expect("ListArray or LargeListArray containing a list of more than u32::MAX items is not supported");
                temporary.extend_from_slice(&len.to_be_bytes());
            }
            let row_count: u32 = (range.end - range.start)
                .try_into()
                .expect("lists containing more than u32::MAX elements not supported");
            temporary.extend_from_slice(&row_count.to_be_bytes());
            super::variable::encode_one(out, Some(temporary), opts)
        }
    }
}

/// Decodes a string array from `rows` with the provided `options`
///
/// # Safety
///
/// `rows` must contain valid data for the provided `converter`
pub unsafe fn decode<O: OffsetSizeTrait>(
    converter: &RowConverter,
    rows: &mut [&[u8]],
    field: &SortField,
    validate_utf8: bool,
) -> Result<GenericListArray<O>, ArrowError> {
    let canonical = super::variable::decode_binary::<i64>(rows, field.options);

    let mut offsets = BufferBuilder::<O>::new(rows.len() + 1);
    offsets.append(O::from_usize(0).unwrap());
    let mut current_offset = 0;

    let mut child_rows = Vec::with_capacity(rows.len());
    canonical.value_offsets().windows(2).for_each(|w| {
        let start = w[0] as usize;
        let end = w[1] as usize;
        if start == end {
            // Null or empty list
            offsets.append(O::from_usize(current_offset).unwrap());
            return;
        }

        let row = &canonical.value_data()[start..end];
        let element_count_start = row.len() - 4;
        let element_count =
            u32::from_be_bytes((&row[element_count_start..]).try_into().unwrap())
                as usize;

        let lengths_start = element_count_start - (element_count * 4);
        let mut row_offset = 0;
        row[lengths_start..element_count_start]
            .chunks_exact(4)
            .for_each(|chunk| {
                let len = u32::from_be_bytes(chunk.try_into().unwrap());
                let next_row_offset = row_offset + len as usize;
                child_rows.push(&row[row_offset..next_row_offset]);
                row_offset = next_row_offset;
            });

        current_offset += element_count;
        offsets.append(O::from_usize(current_offset).unwrap());
    });

    let child = converter.convert_raw(&mut child_rows, validate_utf8)?;
    assert_eq!(child.len(), 1);
    let child_data = child[0].data().clone();

    let builder = ArrayDataBuilder::new(field.data_type.clone())
        .len(rows.len())
        .null_count(canonical.null_count())
        .null_bit_buffer(canonical.data().null_buffer().cloned())
        .add_buffer(offsets.finish())
        .add_child_data(child_data);

    Ok(GenericListArray::from(unsafe { builder.build_unchecked() }))
}
