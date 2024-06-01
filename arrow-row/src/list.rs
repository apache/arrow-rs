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

use crate::{null_sentinel, RowConverter, Rows, SortField};
use arrow_array::{Array, GenericListArray, OffsetSizeTrait};
use arrow_buffer::{Buffer, MutableBuffer};
use arrow_data::ArrayDataBuilder;
use arrow_schema::{ArrowError, SortOptions};
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
        Some(range) => {
            1 + range
                .map(|i| super::variable::padded_length(Some(rows.row(i).as_ref().len())))
                .sum::<usize>()
        }
    }
}

/// Encodes the provided `GenericListArray` to `out` with the provided `SortOptions`
///
/// `rows` should contain the encoded child elements
pub fn encode<O: OffsetSizeTrait>(
    data: &mut [u8],
    offsets: &mut [usize],
    rows: &Rows,
    opts: SortOptions,
    array: &GenericListArray<O>,
) {
    offsets
        .iter_mut()
        .skip(1)
        .zip(array.value_offsets().windows(2))
        .enumerate()
        .for_each(|(idx, (offset, offsets))| {
            let start = offsets[0].as_usize();
            let end = offsets[1].as_usize();
            let range = array.is_valid(idx).then_some(start..end);
            let out = &mut data[*offset..];
            *offset += encode_one(out, rows, range, opts)
        });
}

#[inline]
fn encode_one(
    out: &mut [u8],
    rows: &Rows,
    range: Option<Range<usize>>,
    opts: SortOptions,
) -> usize {
    match range {
        None => super::variable::encode_null(out, opts),
        Some(range) if range.start == range.end => super::variable::encode_empty(out, opts),
        Some(range) => {
            let mut offset = 0;
            for i in range {
                let row = rows.row(i);
                offset += super::variable::encode_one(&mut out[offset..], Some(row.data), opts);
            }
            offset += super::variable::encode_empty(&mut out[offset..], opts);
            offset
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
    let opts = field.options;

    let mut values_bytes = 0;

    let mut offset = 0;
    let mut offsets = Vec::with_capacity(rows.len() + 1);
    offsets.push(O::usize_as(0));

    for row in rows.iter_mut() {
        let mut row_offset = 0;
        loop {
            let decoded = super::variable::decode_blocks(&row[row_offset..], opts, |x| {
                values_bytes += x.len();
            });
            if decoded <= 1 {
                offsets.push(O::usize_as(offset));
                break;
            }
            row_offset += decoded;
            offset += 1;
        }
    }
    O::from_usize(offset).expect("overflow");

    let mut null_count = 0;
    let nulls = MutableBuffer::collect_bool(rows.len(), |x| {
        let valid = rows[x][0] != null_sentinel(opts);
        null_count += !valid as usize;
        valid
    });

    let mut values_offsets = Vec::with_capacity(offset);
    let mut values_bytes = Vec::with_capacity(values_bytes);
    for row in rows.iter_mut() {
        let mut row_offset = 0;
        loop {
            let decoded = super::variable::decode_blocks(&row[row_offset..], opts, |x| {
                values_bytes.extend_from_slice(x)
            });
            row_offset += decoded;
            if decoded <= 1 {
                break;
            }
            values_offsets.push(values_bytes.len());
        }
        *row = &row[row_offset..];
    }

    if opts.descending {
        values_bytes.iter_mut().for_each(|o| *o = !*o);
    }

    let mut last_value_offset = 0;
    let mut child_rows: Vec<_> = values_offsets
        .into_iter()
        .map(|offset| {
            let v = &values_bytes[last_value_offset..offset];
            last_value_offset = offset;
            v
        })
        .collect();

    let child = converter.convert_raw(&mut child_rows, validate_utf8)?;
    assert_eq!(child.len(), 1);

    let child_data = child[0].to_data();

    let builder = ArrayDataBuilder::new(field.data_type.clone())
        .len(rows.len())
        .null_count(null_count)
        .null_bit_buffer(Some(nulls.into()))
        .add_buffer(Buffer::from_vec(offsets))
        .add_child_data(child_data);

    Ok(GenericListArray::from(unsafe { builder.build_unchecked() }))
}
