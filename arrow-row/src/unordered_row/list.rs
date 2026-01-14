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

use super::{LengthTracker, UnorderedRowConverter, UnorderedRows, fixed, null_sentinel};
use arrow_array::{Array, FixedSizeListArray, GenericListArray, OffsetSizeTrait, new_null_array};
use arrow_buffer::{ArrowNativeType, Buffer, MutableBuffer, NullBuffer};
use arrow_data::ArrayDataBuilder;
use arrow_schema::{ArrowError, DataType, Field};
use std::{ops::Range, sync::Arc};

pub fn compute_lengths<O: OffsetSizeTrait>(
    lengths: &mut [usize],
    rows: &UnorderedRows,
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

fn encoded_len(rows: &UnorderedRows, range: Option<Range<usize>>) -> usize {
    // super::variable::encoded_len(
    //     match range {
    //         None => None,
    //         Some(range) if range.is_empty() => Some(&[]),
    //         Some(range) => Some(rows.data_range(range))
    //     }
    // )
    match range.filter(|r| !r.is_empty()) {
        None =>
            // Only the ctrl byte
            1,
        Some(range) => {
            // Number of items
            super::variable::length_of_encoding_length(range.len()) +
              // ctrl byte for the length type that will be used for all lengths here
              1 +
              // what is the worst case scenerio for how much bytes are needed to encode the length of a row
              // if the range is a single item (this is worst case scenerio as we don't know how much each row will take)
              super::variable::get_number_of_bytes_needed_to_encode(rows.data_range_len(&range)) * range.len() +
              // The bytes themselves
              super::variable::padded_length(Some(rows.data_range(range).len()))
        }
    }
}

/// Encodes the provided `GenericListArray` to `out` with the provided `SortOptions`
///
/// `rows` should contain the encoded child elements
pub fn encode<O: OffsetSizeTrait>(
    data: &mut [u8],
    offsets: &mut [usize],
    rows: &UnorderedRows,
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
          *offset += encode_one(out, rows, range)
      });
}

#[inline]
fn encode_one(out: &mut [u8], rows: &UnorderedRows, range: Option<Range<usize>>) -> usize {

    match range.filter(|r| !r.is_empty()) {
        None => {
            super::variable::encode_empty(out)
        },
        Some(range) => {
            let mut offset = 0;

            // Encode the number of items in the list
            offset += super::variable::encode_len(&mut out[offset..], range.len());

            // Encode the type of the lengths of the rows and the lengths themselves
            // this is used to avoid using more memory than needed for small rows
            offset += super::variable::encode_lengths_with_prefix(
                &mut out[offset..],

                // Encode using the worst case if there is a single row
                // as we don't know the maximum length of the rows without iterating over them
                // so we use the worst case scenario
                rows.data_range_len(&range),
                rows.lengths_from(&range),
            );

            // Encode the whole list in one go
            offset += super::variable::fast_encode_bytes(
                &mut out[offset..],
                rows.data_range(range.clone()),
            );

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
    converter: &UnorderedRowConverter,
    rows: &mut [&[u8]],
    field: &Field,
    validate_utf8: bool,
    list_nulls: Option<NullBuffer>,
) -> Result<GenericListArray<O>, ArrowError> {
    let mut values_bytes = 0;

    let mut offset = 0;
    let mut offsets = Vec::with_capacity(rows.len() + 1);
    offsets.push(O::usize_as(0));

    for row in rows.iter_mut() {
        let mut row_offset = 0;

        let (number_of_items, start_offset) = super::variable::decode_len(&row[row_offset..]);
        row_offset += start_offset;

        offset += number_of_items;
        offsets.push(O::usize_as(offset));

        if number_of_items == 0 {
            continue;
        }

        // TODO - encode the bytes first and then the lengths so we don't have to jump here in memory only to get to the number
        //        of bytes the lengths is using
        // read ctrl byte
        let byte_size = super::variable::get_number_of_bytes_used_to_encode_from_ctrl_byte(row[row_offset]);
        // Skip the ctrl byte
        row_offset += 1;

        // Skip the lengths
        row_offset += byte_size * number_of_items;

        let (number_of_bytes, start_offset) = super::variable::decode_len(&row[row_offset..]);
        row_offset += start_offset;

        values_bytes += number_of_bytes;
    }
    O::from_usize(offset).expect("overflow");

    let mut values_offsets = Vec::with_capacity(offset);
    values_offsets.push(0);
    let mut values_bytes = Vec::with_capacity(values_bytes);
    for row in rows.iter_mut() {
        let mut row_offset = 0;

        // Decode the number of items in the list
        let (number_of_items, start_offset) = super::variable::decode_len(&&row[row_offset..]);
        row_offset += start_offset;

        if number_of_items == 0 {
            *row = &row[row_offset..];
            continue;
        }

        // decode the lengths of the rows
        let mut initial_value_offset = values_bytes.len();
        row_offset += super::variable::decode_lengths_with_prefix(&row[row_offset..], number_of_items, |len: usize| {
            initial_value_offset += len;

            values_offsets.push(initial_value_offset);
        });

        // copy the rows bytes in a single pass
        let decoded = super::variable::decode_blocks(&row[row_offset..], |x| {
            values_bytes.extend_from_slice(x)
        });
        row_offset += decoded;
        *row = &row[row_offset..];
    }

    let mut child_rows: Vec<_> = values_offsets
      .windows(2)
      .map(|start_and_end| {
          let v = &values_bytes[start_and_end[0]..start_and_end[1]];
          v
      })
      .collect();

    let child = unsafe { converter.convert_raw(&mut child_rows, validate_utf8) }?;
    assert_eq!(child.len(), 1);

    let child_data = child[0].to_data();

    // Since RowConverter flattens certain data types (i.e. Dictionary),
    // we need to use updated data type instead of original field
    let corrected_type = match field.data_type() {
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
      .nulls(list_nulls)
      .add_buffer(Buffer::from_vec(offsets))
      .add_child_data(child_data);

    Ok(GenericListArray::from(unsafe { builder.build_unchecked() }))
}

pub fn compute_lengths_fixed_size_list(
    tracker: &mut LengthTracker,
    rows: &UnorderedRows,
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
    rows: &UnorderedRows,
    array: &FixedSizeListArray,
) {
    let null_sentinel = null_sentinel();
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
    converter: &UnorderedRowConverter,
    rows: &mut [&[u8]],
    field: &Field,
    validate_utf8: bool,
    value_length: usize,
    nulls: Option<NullBuffer>,
) -> Result<FixedSizeListArray, ArrowError> {
    let list_type = field.data_type();
    let element_type = match list_type {
        DataType::FixedSizeList(element_field, _) => element_field.data_type(),
        _ => {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Expected FixedSizeListArray, found: {list_type}",
            )));
        }
    };

    let len = rows.len();

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
      .nulls(nulls)
      .child_data(child_data);

    Ok(FixedSizeListArray::from(unsafe {
        builder.build_unchecked()
    }))
}
