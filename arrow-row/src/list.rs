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
use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, GenericListArray, GenericListViewArray, MapArray,
    OffsetSizeTrait, StructArray, new_null_array,
};
use arrow_buffer::{
    ArrowNativeType, BooleanBuffer, MutableBuffer, NullBuffer, OffsetBuffer, ScalarBuffer,
};
use arrow_data::ArrayDataBuilder;
use arrow_schema::{ArrowError, DataType, Fields, SortOptions};
use std::{ops::Range, sync::Arc};

pub(crate) trait GenericListArrayOrMap: Array {
    type Offset: OffsetSizeTrait;

    fn offsets(&self) -> &[Self::Offset];

    unsafe fn from_parts_unchecked(
        data_type: DataType,
        offsets: Vec<Self::Offset>,
        children: Vec<ArrayRef>,
        null_buffer: Option<NullBuffer>,
    ) -> Self
    where
        Self: Sized;
}

impl<O: OffsetSizeTrait> GenericListArrayOrMap for GenericListArray<O> {
    type Offset = O;

    fn offsets(&self) -> &[Self::Offset] {
        self.value_offsets()
    }

    unsafe fn from_parts_unchecked(
        data_type: DataType,
        offsets: Vec<Self::Offset>,
        children: Vec<ArrayRef>,
        null_buffer: Option<NullBuffer>,
    ) -> Self
    where
        Self: Sized,
    {
        let field = match data_type {
            DataType::List(inner_field) | DataType::LargeList(inner_field) => inner_field,
            _ => unreachable!(),
        };

        let child = children
            .into_iter()
            .next()
            .expect("List arrays must have exactly one child array");

        // SAFETY: Caller must ensure offsets are valid and correctly correspond to the children and null buffer
        // the benefit here is to avoid validating that the offsets are monotonically increasing
        let offset_buffer = unsafe { OffsetBuffer::new_unchecked(ScalarBuffer::from(offsets)) };
        GenericListArray::<Self::Offset>::new(field, offset_buffer, child, null_buffer)
    }
}

impl GenericListArrayOrMap for MapArray {
    type Offset = i32;

    fn offsets(&self) -> &[Self::Offset] {
        self.value_offsets()
    }

    unsafe fn from_parts_unchecked(
        data_type: DataType,
        offsets: Vec<Self::Offset>,
        children: Vec<ArrayRef>,
        null_buffer: Option<NullBuffer>,
    ) -> Self
    where
        Self: Sized,
    {
        let DataType::Map(entries_field, ordered) = data_type else {
            unreachable!("data type must be Map for MapArray");
        };

        assert_eq!(
            children.len(),
            2,
            "Map arrays must have exactly two child arrays for keys and values"
        );

        let DataType::Struct(fields) = entries_field.data_type() else {
            unreachable!("Map entry type must be Struct");
        };

        let entries = StructArray::new(
            fields.clone(),
            children,
            // Entries StructArray cannot have NullBuffer since nulls are represented at the Map level
            None,
        );

        // SAFETY: Caller must ensure offsets are valid and correctly correspond to the children and null buffer
        // the benefit here is to avoid validating that the offsets are monotonically increasing
        let offset_buffer = unsafe { OffsetBuffer::new_unchecked(ScalarBuffer::from(offsets)) };

        MapArray::new(entries_field, offset_buffer, entries, null_buffer, ordered)
    }
}

pub(crate) fn compute_lengths<L: GenericListArrayOrMap>(
    lengths: &mut [usize],
    rows: &Rows,
    array: &L,
) {
    let shift = array.offsets()[0].as_usize();

    lengths
        .iter_mut()
        .zip(array.offsets().windows(2))
        .enumerate()
        .for_each(|(idx, (length, offsets))| {
            let start = offsets[0].as_usize() - shift;
            let end = offsets[1].as_usize() - shift;
            let range = array.is_valid(idx).then_some(start..end);
            *length += list_like_element_encoded_len(rows, range);
        });
}

/// Encodes the provided [`GenericListArrayOrMap`] to `out` with the provided `SortOptions`
///
/// `rows` should contain the encoded child elements
pub(crate) fn encode<L: GenericListArrayOrMap>(
    data: &mut [u8],
    offsets: &mut [usize],
    rows: &Rows,
    opts: SortOptions,
    array: &L,
) {
    let shift = array.offsets()[0].as_usize();

    offsets
        .iter_mut()
        .skip(1)
        .zip(array.offsets().windows(2))
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
pub(crate) unsafe fn decode<ListLikeImpl: GenericListArrayOrMap>(
    converter: &RowConverter,
    rows: &mut [&[u8]],
    field: &SortField,
    validate_utf8: bool,
) -> Result<ListLikeImpl, ArrowError> {
    let opts = field.options;

    let mut values_bytes = 0;

    let mut offset = 0;
    let mut offsets = Vec::with_capacity(rows.len() + 1);
    offsets.push(ListLikeImpl::Offset::usize_as(0));

    for row in rows.iter_mut() {
        let mut row_offset = 0;
        loop {
            let decoded = super::variable::decode_blocks(&row[row_offset..], opts, |x| {
                values_bytes += x.len();
            });
            if decoded <= 1 {
                offsets.push(ListLikeImpl::Offset::usize_as(offset));
                break;
            }
            row_offset += decoded;
            offset += 1;
        }
    }
    ListLikeImpl::Offset::from_usize(offset).expect("overflow");

    let mut null_count = 0;
    let nulls = BooleanBuffer::collect_bool(rows.len(), |x| {
        let valid = rows[x][0] != null_sentinel(opts);
        null_count += !valid as usize;
        valid
    });

    let nulls = if null_count > 0 {
        // SAFETY: null_count was computed correctly when building the nulls buffer above and the
        // Perf benefit: avoid computing the null count again
        Some(unsafe { NullBuffer::new_unchecked(nulls, null_count) })
    } else {
        None
    };

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

    let children = unsafe { converter.convert_raw(&mut child_rows, validate_utf8) }?;

    // Since RowConverter flattens certain data types (i.e. Dictionary),
    // we need to use updated data type instead of original field
    let corrected_type = match &field.data_type {
        DataType::List(inner_field) => {
            assert_eq!(children.len(), 1);
            DataType::List(Arc::new(
                inner_field
                    .as_ref()
                    .clone()
                    .with_data_type(children[0].data_type().clone()),
            ))
        }
        DataType::LargeList(inner_field) => {
            assert_eq!(children.len(), 1);
            DataType::LargeList(Arc::new(
                inner_field
                    .as_ref()
                    .clone()
                    .with_data_type(children[0].data_type().clone()),
            ))
        }
        DataType::Map(inner_field, ordered) => {
            let DataType::Struct(entries_field) = inner_field.data_type() else {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Expected Map entry type to be Struct, found: {}",
                    inner_field.data_type()
                )));
            };
            assert_eq!(
                children.len(),
                2,
                "Map arrays must have exactly two child arrays for keys and values"
            );
            let key_field = entries_field[0]
                .as_ref()
                .clone()
                .with_data_type(children[0].data_type().clone());
            let value_field = entries_field[1]
                .as_ref()
                .clone()
                .with_data_type(children[1].data_type().clone());

            let entries_fields = Fields::from(vec![key_field, value_field]);

            DataType::Map(
                Arc::new(
                    inner_field
                        .as_ref()
                        .clone()
                        .with_data_type(DataType::Struct(entries_fields)),
                ),
                *ordered,
            )
        }
        _ => unreachable!(),
    };

    Ok(unsafe { ListLikeImpl::from_parts_unchecked(corrected_type, offsets, children, nulls) })
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

/// Computes the encoded length for a single list/map element given its child rows.
///
/// This is used by list types (List, LargeList, ListView, LargeListView) and by Map to determine
/// the encoded length of a list element/map entry. For null elements, returns 1 (null sentinel only).
/// For valid elements, returns 1 + the sum of padded lengths for each child row.
#[inline]
fn list_like_element_encoded_len(rows: &Rows, range: Option<Range<usize>>) -> usize {
    match range {
        None => 1,
        Some(range) => {
            1 + range
                .map(|i| super::variable::padded_length(Some(rows.row(i).as_ref().len())))
                .sum::<usize>()
        }
    }
}

/// Computes the encoded lengths for a `GenericListViewArray`
///
/// `rows` should contain the encoded child elements
pub fn compute_lengths_list_view<O: OffsetSizeTrait>(
    lengths: &mut [usize],
    rows: &Rows,
    array: &GenericListViewArray<O>,
    shift: usize,
) {
    let offsets = array.value_offsets();
    let sizes = array.value_sizes();

    lengths.iter_mut().enumerate().for_each(|(idx, length)| {
        let size = sizes[idx].as_usize();
        let range = array.is_valid(idx).then(|| {
            // For empty lists (size=0), offset may be arbitrary and could underflow when shifted.
            // Use 0 as start since the range is empty anyway.
            let start = if size > 0 {
                offsets[idx].as_usize() - shift
            } else {
                0
            };
            start..start + size
        });
        *length += list_like_element_encoded_len(rows, range);
    });
}

/// Encodes the provided `GenericListViewArray` to `out` with the provided `SortOptions`
///
/// `rows` should contain the encoded child elements
pub fn encode_list_view<O: OffsetSizeTrait>(
    data: &mut [u8],
    out_offsets: &mut [usize],
    rows: &Rows,
    opts: SortOptions,
    array: &GenericListViewArray<O>,
    shift: usize,
) {
    let offsets = array.value_offsets();
    let sizes = array.value_sizes();

    out_offsets
        .iter_mut()
        .skip(1)
        .enumerate()
        .for_each(|(idx, offset)| {
            let size = sizes[idx].as_usize();
            let range = array.is_valid(idx).then(|| {
                // For empty lists (size=0), offset may be arbitrary and could underflow when shifted.
                // Use 0 as start since the range is empty anyway.
                let start = if size > 0 {
                    offsets[idx].as_usize() - shift
                } else {
                    0
                };
                start..start + size
            });
            let out = &mut data[*offset..];
            *offset += encode_one(out, rows, range, opts)
        });
}

/// Decodes a `GenericListViewArray` from `rows` with the provided `options`
///
/// # Safety
///
/// `rows` must contain valid data for the provided `converter`
pub unsafe fn decode_list_view<O: OffsetSizeTrait>(
    converter: &RowConverter,
    rows: &mut [&[u8]],
    field: &SortField,
    validate_utf8: bool,
) -> Result<GenericListViewArray<O>, ArrowError> {
    let opts = field.options;

    let mut values_bytes = 0;

    let mut child_count = 0usize;
    let mut list_sizes: Vec<O> = Vec::with_capacity(rows.len());

    // First pass: count children and compute sizes
    for row in rows.iter_mut() {
        let mut row_offset = 0;
        let mut list_size = 0usize;
        loop {
            let decoded = super::variable::decode_blocks(&row[row_offset..], opts, |x| {
                values_bytes += x.len();
            });
            if decoded <= 1 {
                list_sizes.push(O::usize_as(list_size));
                break;
            }
            row_offset += decoded;
            child_count += 1;
            list_size += 1;
        }
    }
    O::from_usize(child_count).expect("overflow");

    let mut null_count = 0;
    let nulls = MutableBuffer::collect_bool(rows.len(), |x| {
        let valid = rows[x][0] != null_sentinel(opts);
        null_count += !valid as usize;
        valid
    });

    let mut values_offsets_vec = Vec::with_capacity(child_count);
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
            values_offsets_vec.push(values_bytes.len());
        }
        *row = &row[row_offset..];
    }

    if opts.descending {
        values_bytes.iter_mut().for_each(|o| *o = !*o);
    }

    let mut last_value_offset = 0;
    let mut child_rows: Vec<_> = values_offsets_vec
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

    // Technically ListViews don't have to have offsets follow each other precisely, but can be
    // reused. However, because we cannot preserve that sharing within the row format, this is the
    // best we can do.
    let mut list_offsets: Vec<O> = Vec::with_capacity(rows.len());
    let mut current_offset = O::usize_as(0);
    for size in &list_sizes {
        list_offsets.push(current_offset);
        current_offset += *size;
    }

    // Since RowConverter flattens certain data types (i.e. Dictionary),
    // we need to use updated data type instead of original field
    let corrected_inner_field = match &field.data_type {
        DataType::ListView(inner_field) | DataType::LargeListView(inner_field) => Arc::new(
            inner_field
                .as_ref()
                .clone()
                .with_data_type(child_data.data_type().clone()),
        ),
        _ => unreachable!(),
    };

    // SAFETY: null_count was computed correctly when building the nulls buffer above
    let null_buffer = unsafe {
        NullBuffer::new_unchecked(BooleanBuffer::new(nulls.into(), 0, rows.len()), null_count)
    };

    GenericListViewArray::try_new(
        corrected_inner_field,
        ScalarBuffer::from(list_offsets),
        ScalarBuffer::from(list_sizes),
        child[0].clone(),
        Some(null_buffer).filter(|n| n.null_count() > 0),
    )
}
