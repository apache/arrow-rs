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

use crate::{LengthTracker, RowConverter, Rows, SortField, fixed, null_sentinel};
use arrow_array::{Array, FixedSizeListArray, GenericListArray, OffsetSizeTrait, new_null_array};
use arrow_buffer::{ArrowNativeType, Buffer, MutableBuffer};
use arrow_data::ArrayDataBuilder;
use arrow_schema::{ArrowError, DataType, SortOptions};
use std::{ops::Range, sync::Arc};

pub fn compute_lengths<O: OffsetSizeTrait>(
    lengths: &mut [usize],
    rows: &Rows,
    array: &GenericListArray<O>,
) {
    let shift = array.value_offsets()[0].as_usize();

    let offsets = array.value_offsets().windows(2);
    lengths
        .iter_mut()
        .zip(offsets)
        .enumerate()
        .for_each(|(idx, (length, offsets))| {
            let start = offsets[0].as_usize() - shift;
            let end = offsets[1].as_usize() - shift;
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
    let shift = array.value_offsets()[0].as_usize();

    offsets
        .iter_mut()
        .skip(1)
        .zip(array.value_offsets().windows(2))
        .enumerate()
        .for_each(|(idx, (offset, offsets))| {
            let start = offsets[0].as_usize() - shift;
            let end = offsets[1].as_usize() - shift;
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

/// Decodes an array from `rows` with the provided `options`
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

    let child = unsafe { converter.convert_raw(&mut child_rows, validate_utf8) }?;
    assert_eq!(child.len(), 1);

    let child_data = child[0].to_data();

    // Since RowConverter flattens certain data types (i.e. Dictionary),
    // we need to use updated data type instead of original field
    let corrected_type = match &field.data_type {
        DataType::List(inner_field) => DataType::List(Arc::new(
            inner_field
                .as_ref()
                .clone()
                .with_data_type(child_data.data_type().clone()),
        )),
        DataType::LargeList(inner_field) => DataType::LargeList(Arc::new(
            inner_field
                .as_ref()
                .clone()
                .with_data_type(child_data.data_type().clone()),
        )),
        _ => unreachable!(),
    };

    let builder = ArrayDataBuilder::new(corrected_type)
        .len(rows.len())
        .null_count(null_count)
        .null_bit_buffer(Some(nulls.into()))
        .add_buffer(Buffer::from_vec(offsets))
        .add_child_data(child_data);

    Ok(GenericListArray::from(unsafe { builder.build_unchecked() }))
}

pub fn compute_lengths_fixed_size_list(
    tracker: &mut LengthTracker,
    rows: &Rows,
    array: &FixedSizeListArray,
) {
    let value_length = array.value_length().as_usize();
    tracker.push_variable((0..array.len()).map(|idx| {
        match array.is_valid(idx) {
            true => {
                1 + ((idx * value_length)..(idx + 1) * value_length)
                    .map(|child_idx| rows.row(child_idx).as_ref().len())
                    .sum::<usize>()
            }
            false => 1,
        }
    }))
}

/// Encodes the provided `FixedSizeListArray` to `out` with the provided `SortOptions`
///
/// `rows` should contain the encoded child elements
pub fn encode_fixed_size_list(
    data: &mut [u8],
    offsets: &mut [usize],
    rows: &Rows,
    opts: SortOptions,
    array: &FixedSizeListArray,
) {
    let null_sentinel = null_sentinel(opts);
    offsets
        .iter_mut()
        .skip(1)
        .enumerate()
        .for_each(|(idx, offset)| {
            let value_length = array.value_length().as_usize();
            match array.is_valid(idx) {
                true => {
                    data[*offset] = 0x01;
                    *offset += 1;
                    for child_idx in (idx * value_length)..(idx + 1) * value_length {
                        let row = rows.row(child_idx);
                        let end_offset = *offset + row.as_ref().len();
                        data[*offset..end_offset].copy_from_slice(row.as_ref());
                        *offset = end_offset;
                    }
                }
                false => {
                    data[*offset] = null_sentinel;
                    *offset += 1;
                }
            };
        })
}

/// Decodes a fixed size list array from `rows` with the provided `options`
///
/// # Safety
///
/// `rows` must contain valid data for the provided `converter`
pub unsafe fn decode_fixed_size_list(
    converter: &RowConverter,
    rows: &mut [&[u8]],
    field: &SortField,
    validate_utf8: bool,
    value_length: usize,
) -> Result<FixedSizeListArray, ArrowError> {
    let list_type = &field.data_type;
    let element_type = match list_type {
        DataType::FixedSizeList(element_field, _) => element_field.data_type(),
        _ => {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Expected FixedSizeListArray, found: {list_type}",
            )));
        }
    };

    let len = rows.len();
    let (null_count, nulls) = fixed::decode_nulls(rows);

    let null_element_encoded = converter.convert_columns(&[new_null_array(element_type, 1)])?;
    let null_element_encoded = null_element_encoded.row(0);
    let null_element_slice = null_element_encoded.as_ref();

    let mut child_rows = Vec::new();
    for row in rows {
        let valid = row[0] == 1;
        let mut row_offset = 1;
        if !valid {
            for _ in 0..value_length {
                child_rows.push(null_element_slice);
            }
        } else {
            for _ in 0..value_length {
                let mut temp_child_rows = vec![&row[row_offset..]];
                unsafe { converter.convert_raw(&mut temp_child_rows, validate_utf8) }?;
                let decoded_bytes = row.len() - row_offset - temp_child_rows[0].len();
                let next_offset = row_offset + decoded_bytes;
                child_rows.push(&row[row_offset..next_offset]);
                row_offset = next_offset;
            }
        }
        *row = &row[row_offset..]; // Update row for the next decoder
    }

    let children = unsafe { converter.convert_raw(&mut child_rows, validate_utf8) }?;
    let child_data = children.iter().map(|c| c.to_data()).collect();
    let builder = ArrayDataBuilder::new(list_type.clone())
        .len(len)
        .null_count(null_count)
        .null_bit_buffer(Some(nulls))
        .child_data(child_data);

    Ok(FixedSizeListArray::from(unsafe {
        builder.build_unchecked()
    }))
}
