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

//! Interleave elements from multiple arrays

use crate::concat::concat;
use crate::dictionary::{merge_dictionary_values, should_merge_dictionary_values};
use arrow_array::ListLikeArray;
use arrow_array::builder::{BooleanBufferBuilder, PrimitiveBuilder};
use arrow_array::cast::AsArray;
use arrow_array::types::*;
use arrow_array::*;
use arrow_buffer::bit_mask::set_bits;
use arrow_buffer::bit_util;
use arrow_buffer::{
    ArrowNativeType, BooleanBuffer, Buffer, MutableBuffer, NullBuffer, OffsetBuffer,
};
use arrow_data::ByteView;
use arrow_data::transform::MutableArrayData;
use arrow_schema::{ArrowError, DataType, FieldRef, Fields};
use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Arc;

macro_rules! primitive_helper {
    ($t:ty, $values:ident, $indices:ident, $data_type:ident) => {
        interleave_primitive::<$t>($values, $indices, $data_type)
    };
}

macro_rules! dict_helper {
    ($t:ty, $values:expr, $indices:expr) => {
        interleave_dictionaries::<$t>($values, $indices)
    };
}

///
/// Takes elements by index from a list of [`Array`], creating a new [`Array`] from those values.
///
/// Each element in `indices` is a pair of `usize` with the first identifying the index
/// of the [`Array`] in `values`, and the second the index of the value within that [`Array`]
///
/// ```text
/// ┌─────────────────┐      ┌─────────┐                                  ┌─────────────────┐
/// │        A        │      │ (0, 0)  │        interleave(               │        A        │
/// ├─────────────────┤      ├─────────┤          [values0, values1],     ├─────────────────┤
/// │        D        │      │ (1, 0)  │          indices                 │        B        │
/// └─────────────────┘      ├─────────┤        )                         ├─────────────────┤
///   values array 0         │ (1, 1)  │      ─────────────────────────▶  │        C        │
///                          ├─────────┤                                  ├─────────────────┤
///                          │ (0, 1)  │                                  │        D        │
///                          └─────────┘                                  └─────────────────┘
/// ┌─────────────────┐       indices
/// │        B        │        array
/// ├─────────────────┤                                                    result
/// │        C        │
/// ├─────────────────┤
/// │        E        │
/// └─────────────────┘
///   values array 1
/// ```
///
/// For selecting values by index from a single array see [`crate::take`]
pub fn interleave(
    values: &[&dyn Array],
    indices: &[(usize, usize)],
) -> Result<ArrayRef, ArrowError> {
    if values.is_empty() {
        return Err(ArrowError::InvalidArgumentError(
            "interleave requires input of at least one array".to_string(),
        ));
    }
    let data_type = values[0].data_type();

    for array in values.iter().skip(1) {
        if array.data_type() != data_type {
            return Err(ArrowError::InvalidArgumentError(format!(
                "It is not possible to interleave arrays of different data types ({} and {})",
                data_type,
                array.data_type()
            )));
        }
    }

    if indices.is_empty() {
        return Ok(new_empty_array(data_type));
    }

    downcast_primitive! {
        data_type => (primitive_helper, values, indices, data_type),
        DataType::Boolean => interleave_boolean(values, indices),
        DataType::Utf8 => interleave_bytes::<Utf8Type>(values, indices),
        DataType::LargeUtf8 => interleave_bytes::<LargeUtf8Type>(values, indices),
        DataType::Binary => interleave_bytes::<BinaryType>(values, indices),
        DataType::LargeBinary => interleave_bytes::<LargeBinaryType>(values, indices),
        DataType::BinaryView => interleave_views::<BinaryViewType>(values, indices),
        DataType::Utf8View => interleave_views::<StringViewType>(values, indices),
        DataType::Dictionary(k, _) => downcast_integer! {
            k.as_ref() => (dict_helper, values, indices),
            _ => unreachable!("illegal dictionary key type {k}")
        },
        DataType::Struct(fields) => interleave_struct(fields, values, indices),
        DataType::List(field) => interleave_list::<i32>(values, indices, field),
        DataType::LargeList(field) => interleave_list::<i64>(values, indices, field),
        DataType::FixedSizeList(field, size) => interleave_fixed_size_list(values, indices, field, *size),
        DataType::Map(field, ordered) => interleave_map(values, indices, field, *ordered),
        DataType::RunEndEncoded(r, _) => match r.data_type() {
            DataType::Int16 => interleave_run_end::<Int16Type>(values, indices),
            DataType::Int32 => interleave_run_end::<Int32Type>(values, indices),
            DataType::Int64 => interleave_run_end::<Int64Type>(values, indices),
            t => unreachable!("illegal run-end type {t}"),
        },
        DataType::ListView(field) => interleave_list_view::<i32>(values, indices, field),
        DataType::LargeListView(field) => interleave_list_view::<i64>(values, indices, field),
        _ => interleave_fallback(values, indices)
    }
}

/// Common functionality for interleaving arrays
///
/// T is the concrete Array type
struct Interleave<'a, T> {
    /// The input arrays downcast to T
    arrays: Vec<&'a T>,
    /// The null buffer of the interleaved output
    nulls: Option<NullBuffer>,
}

impl<'a, T: Array + 'static> Interleave<'a, T> {
    fn new(values: &[&'a dyn Array], indices: &'a [(usize, usize)]) -> Self {
        let mut has_nulls = false;
        let arrays: Vec<&T> = values
            .iter()
            .map(|x| {
                has_nulls = has_nulls || x.null_count() != 0;
                x.as_any().downcast_ref().unwrap()
            })
            .collect();

        let nulls = match has_nulls {
            true => {
                let nulls = BooleanBuffer::collect_bool(indices.len(), |i| {
                    let (a, b) = indices[i];
                    arrays[a].is_valid(b)
                });
                Some(nulls.into())
            }
            false => None,
        };

        Self { arrays, nulls }
    }
}

fn interleave_primitive<T: ArrowPrimitiveType>(
    values: &[&dyn Array],
    indices: &[(usize, usize)],
    data_type: &DataType,
) -> Result<ArrayRef, ArrowError> {
    let interleaved = Interleave::<'_, PrimitiveArray<T>>::new(values, indices);
    let arrays = &interleaved.arrays;
    let len = indices.len();

    let mut output = Vec::with_capacity(len);
    let dst: *mut T::Native = output.as_mut_ptr();
    let mut base = 0;

    // Process 8 elements at a time to issue multiple independent loads
    // and increase memory-level parallelism for random access patterns.
    let (chunks, remainder) = indices.as_chunks::<8>();
    for chunk in chunks {
        let v0 = arrays[chunk[0].0].value(chunk[0].1);
        let v1 = arrays[chunk[1].0].value(chunk[1].1);
        let v2 = arrays[chunk[2].0].value(chunk[2].1);
        let v3 = arrays[chunk[3].0].value(chunk[3].1);
        let v4 = arrays[chunk[4].0].value(chunk[4].1);
        let v5 = arrays[chunk[5].0].value(chunk[5].1);
        let v6 = arrays[chunk[6].0].value(chunk[6].1);
        let v7 = arrays[chunk[7].0].value(chunk[7].1);

        // SAFETY: base+7 < len == output capacity
        debug_assert!(base + 7 < len);
        unsafe {
            dst.add(base).write(v0);
            dst.add(base + 1).write(v1);
            dst.add(base + 2).write(v2);
            dst.add(base + 3).write(v3);
            dst.add(base + 4).write(v4);
            dst.add(base + 5).write(v5);
            dst.add(base + 6).write(v6);
            dst.add(base + 7).write(v7);
        }
        base += 8;
    }

    for idx in remainder {
        // SAFETY: base < len == output capacity
        debug_assert!(base < len);
        unsafe { dst.add(base).write(arrays[idx.0].value(idx.1)) };
        base += 1;
    }

    // SAFETY: all `len` elements have been initialized
    debug_assert_eq!(base, len);
    unsafe { output.set_len(len) };

    let array = PrimitiveArray::<T>::try_new(output.into(), interleaved.nulls)?;
    Ok(Arc::new(array.with_data_type(data_type.clone())))
}

fn interleave_boolean(
    values: &[&dyn Array],
    indices: &[(usize, usize)],
) -> Result<ArrayRef, ArrowError> {
    let interleaved = Interleave::<'_, BooleanArray>::new(values, indices);
    let arrays = &interleaved.arrays;

    let mut values_builder = BooleanBufferBuilder::new(indices.len());

    for &(array_idx, row_idx) in indices {
        values_builder.append(arrays[array_idx].value(row_idx));
    }

    let array = BooleanArray::new(values_builder.build(), interleaved.nulls);
    Ok(Arc::new(array))
}

fn interleave_bytes<T: ByteArrayType>(
    values: &[&dyn Array],
    indices: &[(usize, usize)],
) -> Result<ArrayRef, ArrowError> {
    let interleaved = Interleave::<'_, GenericByteArray<T>>::new(values, indices);

    let mut capacity = 0;
    let mut offsets = Vec::with_capacity(indices.len() + 1);
    offsets.push(T::Offset::from_usize(0).unwrap());
    for (a, b) in indices {
        let o = interleaved.arrays[*a].value_offsets();
        let element_len = o[*b + 1].as_usize() - o[*b].as_usize();
        capacity += element_len;
        offsets.push(
            T::Offset::from_usize(capacity)
                .ok_or_else(|| ArrowError::OffsetOverflowError(capacity))?,
        );
    }

    let mut values = Vec::with_capacity(capacity);
    for (a, b) in indices {
        values.extend_from_slice(interleaved.arrays[*a].value(*b).as_ref());
    }

    // Safety: safe by construction
    let array = unsafe {
        let offsets = OffsetBuffer::new_unchecked(offsets.into());
        GenericByteArray::<T>::new_unchecked(offsets, values.into(), interleaved.nulls)
    };
    Ok(Arc::new(array))
}

fn interleave_dictionaries<K: ArrowDictionaryKeyType>(
    arrays: &[&dyn Array],
    indices: &[(usize, usize)],
) -> Result<ArrayRef, ArrowError> {
    let dictionaries: Vec<_> = arrays.iter().map(|x| x.as_dictionary::<K>()).collect();
    let (should_merge, has_overflow) =
        should_merge_dictionary_values::<K>(&dictionaries, indices.len());
    if !should_merge {
        return if has_overflow {
            interleave_fallback(arrays, indices)
        } else {
            interleave_fallback_dictionary::<K>(&dictionaries, indices)
        };
    }

    let masks: Vec<_> = dictionaries
        .iter()
        .enumerate()
        .map(|(a_idx, dictionary)| {
            let mut key_mask = BooleanBufferBuilder::new_from_buffer(
                MutableBuffer::new_null(dictionary.len()),
                dictionary.len(),
            );

            for (_, key_idx) in indices.iter().filter(|(a, _)| *a == a_idx) {
                key_mask.set_bit(*key_idx, true);
            }
            key_mask.finish()
        })
        .collect();

    let merged = merge_dictionary_values(&dictionaries, Some(&masks))?;

    // Recompute keys
    let mut keys = PrimitiveBuilder::<K>::with_capacity(indices.len());
    for (a, b) in indices {
        let old_keys: &PrimitiveArray<K> = dictionaries[*a].keys();
        match old_keys.is_valid(*b) {
            true => {
                let old_key = old_keys.values()[*b];
                keys.append_value(merged.key_mappings[*a][old_key.as_usize()])
            }
            false => keys.append_null(),
        }
    }
    let array = unsafe { DictionaryArray::new_unchecked(keys.finish(), merged.values) };
    Ok(Arc::new(array))
}

fn interleave_views<T: ByteViewType>(
    values: &[&dyn Array],
    indices: &[(usize, usize)],
) -> Result<ArrayRef, ArrowError> {
    let interleaved = Interleave::<'_, GenericByteViewArray<T>>::new(values, indices);
    let mut buffers = Vec::new();

    // Contains the offsets of start buffer in `buffer_to_new_index`
    let mut offsets = Vec::with_capacity(interleaved.arrays.len() + 1);
    offsets.push(0);
    let mut total_buffers = 0;
    for a in &interleaved.arrays {
        total_buffers += a.data_buffers().len();
        offsets.push(total_buffers);
    }

    // Marks a buffer in `buffer_to_new_index` that has not yet been referenced.
    //
    // The view's buffer index is a signed 32-bit integer in the Arrow specification,
    // so `u32::MAX` can never be a valid buffer index
    const UNASSIGNED: u32 = u32::MAX;

    // contains the mapping from old buffer index to new buffer index
    let mut buffer_to_new_index = vec![UNASSIGNED; total_buffers];

    // Contains the index in `buffers` of each buffer already emitted, keyed by identity
    let mut seen_buffers = BTreeMap::new();

    let views: Vec<u128> = indices
        .iter()
        .map(|(array_idx, value_idx)| {
            let array = interleaved.arrays[*array_idx];
            let view = array.views().get(*value_idx).unwrap();
            let view_len = *view as u32;
            if view_len <= 12 {
                return *view;
            }
            // value is big enough to be in a variadic buffer
            let view = ByteView::from(*view);
            let buffer_to_new_idx = offsets[*array_idx] + view.buffer_index as usize;
            let new_buffer_idx = &mut buffer_to_new_index[buffer_to_new_idx];
            if *new_buffer_idx == UNASSIGNED {
                *new_buffer_idx = push_buffer(
                    &mut seen_buffers,
                    &mut buffers,
                    &array.data_buffers()[view.buffer_index as usize],
                );
            }
            view.with_buffer_index(*new_buffer_idx).as_u128()
        })
        .collect();

    let array = unsafe {
        GenericByteViewArray::<T>::new_unchecked(views.into(), buffers.into(), interleaved.nulls)
    };
    Ok(Arc::new(array))
}

/// Returns the index of `buffer` in `buffers`, adding it if not already present.
///
/// Multiple input arrays, e.g. slices of the same array, may reference the same
/// buffer, so this checks by identity to ensure it is only emitted once
#[inline(never)]
fn push_buffer(
    seen: &mut BTreeMap<(*const u8, usize), u32>,
    buffers: &mut Vec<Buffer>,
    buffer: &Buffer,
) -> u32 {
    *seen
        .entry((buffer.as_ptr(), buffer.len()))
        .or_insert_with(|| {
            buffers.push(buffer.clone());
            (buffers.len() - 1) as u32
        })
}

fn interleave_struct(
    fields: &Fields,
    values: &[&dyn Array],
    indices: &[(usize, usize)],
) -> Result<ArrayRef, ArrowError> {
    let interleaved = Interleave::<'_, StructArray>::new(values, indices);

    if fields.is_empty() {
        let array = StructArray::try_new_with_length(
            fields.clone(),
            vec![],
            interleaved.nulls,
            indices.len(),
        )?;
        return Ok(Arc::new(array));
    }

    let struct_fields_array: Result<Vec<_>, _> = (0..fields.len())
        .map(|i| {
            let field_values: Vec<&dyn Array> = interleaved
                .arrays
                .iter()
                .map(|x| x.column(i).as_ref())
                .collect();
            interleave(&field_values, indices)
        })
        .collect();

    let struct_array =
        StructArray::try_new(fields.clone(), struct_fields_array?, interleaved.nulls)?;
    Ok(Arc::new(struct_array))
}

fn interleave_list_like_primitive_child<L: ListLikeArray, T: ArrowPrimitiveType>(
    interleaved: &Interleave<'_, L>,
    indices: &[(usize, usize)],
    capacity: usize,
    data_type: &DataType,
) -> ArrayRef {
    let child_arrays: Vec<&PrimitiveArray<T>> = interleaved
        .arrays
        .iter()
        .map(|list| list.values().as_primitive::<T>())
        .collect();

    let ranges = indices
        .iter()
        .map(|&(array, row)| (array, interleaved.arrays[array].element_range(row)));
    interleave_primitive_ranges(&child_arrays, ranges, capacity, data_type)
}

/// Copy list child ranges without expanding them into an index per child value.
#[inline]
fn interleave_primitive_ranges<T: ArrowPrimitiveType>(
    arrays: &[&PrimitiveArray<T>],
    ranges: impl Iterator<Item = (usize, Range<usize>)> + Clone,
    capacity: usize,
    data_type: &DataType,
) -> ArrayRef {
    let has_nulls = arrays.iter().any(|a| a.null_count() > 0);
    let mut values: Vec<T::Native> = Vec::with_capacity(capacity);
    for (array, range) in ranges.clone() {
        if !range.is_empty() {
            values.extend_from_slice(&arrays[array].values()[range]);
        }
    }
    let nulls = if has_nulls {
        interleave_nulls_ranges(arrays, ranges, capacity)
    } else {
        None
    };
    Arc::new(PrimitiveArray::<T>::new(values.into(), nulls).with_data_type(data_type.clone()))
}

/// Copy only this array's validity: parent and child masks remain independent.
#[inline]
fn interleave_nulls_ranges<A: Array>(
    arrays: &[&A],
    ranges: impl Iterator<Item = (usize, Range<usize>)>,
    capacity: usize,
) -> Option<NullBuffer> {
    let mut buffer = MutableBuffer::from_len_zeroed(bit_util::ceil(capacity, 8));
    let mut offset = 0;
    for (array, range) in ranges {
        let len = range.len();
        if len > 0 {
            match arrays[array].nulls() {
                Some(nulls) => {
                    set_bits(
                        buffer.as_slice_mut(),
                        nulls.validity(),
                        offset,
                        nulls.offset() + range.start,
                        len,
                    );
                }
                None => {
                    let buffer = buffer.as_slice_mut();
                    (offset..offset + len).for_each(|i| bit_util::set_bit(buffer, i));
                }
            }
        }
        offset += len;
    }
    let nulls = NullBuffer::new(BooleanBuffer::new(buffer.into(), 0, capacity));
    (nulls.null_count() > 0).then_some(nulls)
}

fn can_interleave_struct_ranges(fields: &Fields) -> bool {
    macro_rules! supported {
        ($t:ty) => {
            true
        };
    }
    fields.iter().all(|field| {
        downcast_primitive! {
            field.data_type() => (supported),
            _ => false
        }
    })
}

/// Preserve the list ranges through a Struct of primitive fields. Replaying the
/// ranges avoids both a per-child index vector and a separate range allocation.
/// Even null lists and null structs retain their backing values and child masks.
fn interleave_list_struct_child<O: OffsetSizeTrait>(
    interleaved: &Interleave<'_, GenericListArray<O>>,
    indices: &[(usize, usize)],
    capacity: usize,
    fields: &Fields,
) -> Result<ArrayRef, ArrowError> {
    let arrays: Vec<_> = interleaved
        .arrays
        .iter()
        .map(|list| list.values().as_struct())
        .collect();
    let ranges = indices
        .iter()
        .map(|&(array, row)| (array, interleaved.arrays[array].element_range(row)));
    let nulls = if arrays.iter().any(|a| a.null_count() > 0) {
        interleave_nulls_ranges(&arrays, ranges.clone(), capacity)
    } else {
        None
    };
    let columns = fields
        .iter()
        .enumerate()
        .map(|(i, field)| {
            macro_rules! primitive_field {
                ($t:ty) => {{
                    let children: Vec<_> = arrays
                        .iter()
                        .map(|array| array.column(i).as_primitive::<$t>())
                        .collect();
                    interleave_primitive_ranges(
                        &children,
                        ranges.clone(),
                        capacity,
                        field.data_type(),
                    )
                }};
            }
            downcast_primitive! {
                field.data_type() => (primitive_field),
                _ => unreachable!("checked by can_interleave_struct_ranges")
            }
        })
        .collect();
    Ok(Arc::new(StructArray::try_new_with_length(
        fields.clone(),
        columns,
        nulls,
        capacity,
    )?))
}

/// Interleave child values for non-primitive child types, shared by List and FixedSizeList.
fn interleave_list_like_child<L: ListLikeArray>(
    interleaved: &Interleave<'_, L>,
    indices: &[(usize, usize)],
    capacity: usize,
) -> Result<ArrayRef, ArrowError> {
    let mut child_indices = Vec::with_capacity(capacity);
    for &(array, row) in indices {
        let range = interleaved.arrays[array].element_range(row);
        child_indices.extend(range.map(|i| (array, i)));
    }

    let child_arrays: Vec<&dyn Array> = interleaved
        .arrays
        .iter()
        .map(|list| list.values().as_ref())
        .collect();
    interleave(&child_arrays, &child_indices)
}

fn interleave_list<O: OffsetSizeTrait>(
    values: &[&dyn Array],
    indices: &[(usize, usize)],
    field: &FieldRef,
) -> Result<ArrayRef, ArrowError> {
    let interleaved = Interleave::<'_, GenericListArray<O>>::new(values, indices);

    // Step 1: compute output offsets and total child capacity
    let mut capacity = 0usize;
    let mut offsets = Vec::with_capacity(indices.len() + 1);
    offsets.push(O::from_usize(0).unwrap());
    for (array, row) in indices {
        let o = interleaved.arrays[*array].value_offsets();
        let element_len = o[*row + 1].as_usize() - o[*row].as_usize();
        capacity += element_len;
        offsets.push(
            O::from_usize(capacity).ok_or_else(|| ArrowError::OffsetOverflowError(capacity))?,
        );
    }

    // Step 2: build child values.
    macro_rules! list_primitive_helper {
        ($t:ty) => {
            interleave_list_like_primitive_child::<GenericListArray<O>, $t>(
                &interleaved,
                indices,
                capacity,
                field.data_type(),
            )
        };
    }

    let child_values = downcast_primitive! {
        // For primitive child types, directly copy typed value slices and null bit
        // ranges, avoiding both the intermediate child_indices Vec allocation and
        // MutableArrayData's function pointer indirection.
        field.data_type() => (list_primitive_helper),
        // Short lists offer no bulk-copy advantage over the scalar gather.
        DataType::Struct(fields) if capacity > indices.len() && can_interleave_struct_ranges(fields) => {
            interleave_list_struct_child(&interleaved, indices, capacity, fields)?
        }
        _ => {
            interleave_list_like_child(&interleaved, indices, capacity)?
        }
    };

    let offsets = OffsetBuffer::new(offsets.into());
    let list_array =
        GenericListArray::<O>::new(field.clone(), offsets, child_values, interleaved.nulls);

    Ok(Arc::new(list_array))
}

fn interleave_fixed_size_list(
    values: &[&dyn Array],
    indices: &[(usize, usize)],
    field: &FieldRef,
    size: i32,
) -> Result<ArrayRef, ArrowError> {
    let interleaved = Interleave::<'_, FixedSizeListArray>::new(values, indices);
    let capacity = indices.len() * size as usize;

    macro_rules! fsl_primitive_helper {
        ($t:ty) => {
            interleave_list_like_primitive_child::<FixedSizeListArray, $t>(
                &interleaved,
                indices,
                capacity,
                field.data_type(),
            )
        };
    }

    let interleaved_values = downcast_primitive! {
        field.data_type() => (fsl_primitive_helper),
        _ => {
            interleave_list_like_child(&interleaved, indices, capacity)?
        }
    };

    let array = FixedSizeListArray::try_new_with_length(
        field.clone(),
        size,
        interleaved_values,
        interleaved.nulls,
        indices.len(),
    )?;
    Ok(Arc::new(array))
}

fn interleave_map(
    values: &[&dyn Array],
    indices: &[(usize, usize)],
    field: &FieldRef,
    ordered: bool,
) -> Result<ArrayRef, ArrowError> {
    let interleaved = Interleave::<'_, MapArray>::new(values, indices);

    let mut capacity = 0usize;
    let mut offsets = Vec::with_capacity(indices.len() + 1);
    offsets.push(0i32);
    for &(array, row) in indices {
        let o = interleaved.arrays[array].value_offsets();
        let element_len = (o[row + 1] - o[row]) as usize;
        capacity += element_len;
        offsets
            .push(i32::try_from(capacity).map_err(|_| ArrowError::OffsetOverflowError(capacity))?);
    }

    let mut child_indices = Vec::with_capacity(capacity);
    for &(array, row) in indices {
        let o = interleaved.arrays[array].value_offsets();
        let start = o[row] as usize;
        let end = o[row + 1] as usize;
        child_indices.extend((start..end).map(|i| (array, i)));
    }

    let entries_arrays: Vec<&dyn Array> = interleaved
        .arrays
        .iter()
        .map(|m| m.entries() as &dyn Array)
        .collect();
    let interleaved_entries = interleave(&entries_arrays, &child_indices)?;

    let offsets = OffsetBuffer::new(offsets.into());
    let entries = interleaved_entries.as_struct().clone();
    let array = MapArray::new(field.clone(), offsets, entries, interleaved.nulls, ordered);
    Ok(Arc::new(array))
}

/// Specialized [`interleave`] for [`RunArray`].
fn interleave_run_end<R: RunEndIndexType>(
    values: &[&dyn Array],
    indices: &[(usize, usize)],
) -> Result<ArrayRef, ArrowError> {
    if indices.is_empty() {
        return Ok(new_empty_array(values[0].data_type()));
    }

    let n = indices.len();
    R::Native::from_usize(n).ok_or_else(|| {
        ArrowError::ComputeError(format!(
            "interleave_run_end: output length {n} does not fit run-end type"
        ))
    })?;

    let runs: Vec<&RunArray<R>> = values.iter().map(|a| a.as_run::<R>()).collect();
    let value_arrays: Vec<&dyn Array> = runs.iter().map(|r| r.values().as_ref()).collect();

    // Resolve each (array, logical_row) to (array, physical_row), so we can
    // lookup physical indices by batch.
    let mut phys_pairs: Vec<(usize, usize)> = vec![(0, 0); n];
    let mut grouped: Vec<(Vec<R::Native>, Vec<usize>)> =
        (0..runs.len()).map(|_| (Vec::new(), Vec::new())).collect();
    for (out_pos, &(arr, row)) in indices.iter().enumerate() {
        let row = R::Native::from_usize(row).ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!(
                "interleave_run_end: row index {row} not representable as run-end type {}",
                R::DATA_TYPE
            ))
        })?;
        grouped[arr].0.push(row);
        grouped[arr].1.push(out_pos);
    }
    for (arr_idx, (logical_rows, out_positions)) in grouped.into_iter().enumerate() {
        let phys = runs[arr_idx].get_physical_indices(&logical_rows)?;
        for (p, out_pos) in phys.iter().zip(out_positions.iter()) {
            phys_pairs[*out_pos] = (arr_idx, *p);
        }
    }

    // Coalesce by physical-pair equality only: emit a new run when the
    // (array_idx, physical_idx) pair changes between adjacent output rows.
    // TODO: We could perform an equality check across sources to extend the
    // output run, but we can't call make_comparator from this crate.
    let mut run_ends_buf: Vec<R::Native> = Vec::with_capacity(n);
    let mut dedup_pairs: Vec<(usize, usize)> = Vec::with_capacity(n);
    dedup_pairs.push(phys_pairs[0]);
    for i in 1..n {
        if phys_pairs[i] != phys_pairs[i - 1] {
            run_ends_buf.push(R::Native::from_usize(i).unwrap());
            dedup_pairs.push(phys_pairs[i]);
        }
    }
    run_ends_buf.push(R::Native::from_usize(n).unwrap());

    let taken_values = interleave(&value_arrays, &dedup_pairs)?;
    let run_ends = PrimitiveArray::<R>::from_iter_values(run_ends_buf);

    Ok(Arc::new(RunArray::<R>::try_new(
        &run_ends,
        taken_values.as_ref(),
    )?))
}

fn interleave_list_view<O: OffsetSizeTrait>(
    values: &[&dyn Array],
    indices: &[(usize, usize)],
    field: &FieldRef,
) -> Result<ArrayRef, ArrowError> {
    let interleaved = Interleave::<'_, GenericListViewArray<O>>::new(values, indices);

    // Pick whichever strategy produces fewer child elements:
    // - Per-row copy: total = sum of selected sizes. Better for sparse selections.
    // - Concat + offset adjustment: total = sum of source backing array lengths.
    //   Better when rows share backing elements via overlapping offset/size ranges.
    let concat_cost: usize = interleaved.arrays.iter().map(|lv| lv.values().len()).sum();
    let per_row_cost: usize = indices
        .iter()
        .map(|&(a, r)| interleaved.arrays[a].sizes()[r].as_usize())
        .sum();

    if per_row_cost <= concat_cost {
        interleave_list_view_copy::<O>(&interleaved, indices, field)
    } else {
        interleave_list_view_concat::<O>(&interleaved, indices, field)
    }
}

/// Per-row copy: copies each selected row's child elements into a new flat array.
fn interleave_list_view_copy<O: OffsetSizeTrait>(
    interleaved: &Interleave<'_, GenericListViewArray<O>>,
    indices: &[(usize, usize)],
    field: &FieldRef,
) -> Result<ArrayRef, ArrowError> {
    let mut capacity = 0usize;
    let mut offsets = Vec::with_capacity(indices.len());
    let mut sizes = Vec::with_capacity(indices.len());
    for &(array_idx, row_idx) in indices {
        let list = interleaved.arrays[array_idx];
        let size = list.sizes()[row_idx].as_usize();
        offsets.push(
            O::from_usize(capacity).ok_or_else(|| ArrowError::OffsetOverflowError(capacity))?,
        );
        sizes.push(O::from_usize(size).ok_or_else(|| ArrowError::OffsetOverflowError(size))?);
        capacity += size;
    }

    let child_data: Vec<_> = interleaved
        .arrays
        .iter()
        .map(|list| list.values().to_data())
        .collect();
    let child_data_refs: Vec<_> = child_data.iter().collect();
    let mut mutable_child = MutableArrayData::new(child_data_refs, false, capacity);
    for &(array_idx, row_idx) in indices {
        let list = interleaved.arrays[array_idx];
        let start = list.offsets()[row_idx].as_usize();
        let size = list.sizes()[row_idx].as_usize();
        if size > 0 {
            mutable_child.try_extend(array_idx, start, start + size)?;
        }
    }

    Ok(Arc::new(GenericListViewArray::<O>::new(
        field.clone(),
        offsets.into(),
        sizes.into(),
        make_array(mutable_child.freeze()),
        interleaved.nulls.clone(),
    )))
}

/// Concat backing arrays: concatenates all source value arrays and adjusts offsets.
/// Preserves within-source element sharing.
fn interleave_list_view_concat<O: OffsetSizeTrait>(
    interleaved: &Interleave<'_, GenericListViewArray<O>>,
    indices: &[(usize, usize)],
    field: &FieldRef,
) -> Result<ArrayRef, ArrowError> {
    let child_arrays: Vec<&dyn Array> = interleaved
        .arrays
        .iter()
        .map(|lv| lv.values().as_ref())
        .collect();
    let mut base_offsets = Vec::with_capacity(interleaved.arrays.len());
    let mut running = 0usize;
    for lv in &interleaved.arrays {
        base_offsets.push(running);
        running += lv.values().len();
    }
    let combined_values = concat(&child_arrays)?;

    let mut new_offsets = Vec::with_capacity(indices.len());
    let mut new_sizes = Vec::with_capacity(indices.len());
    for &(array_idx, row_idx) in indices {
        let lv = interleaved.arrays[array_idx];
        let adjusted = lv.offsets()[row_idx].as_usize() + base_offsets[array_idx];
        new_offsets.push(
            O::from_usize(adjusted).ok_or_else(|| ArrowError::OffsetOverflowError(adjusted))?,
        );
        new_sizes.push(lv.sizes()[row_idx]);
    }

    Ok(Arc::new(GenericListViewArray::<O>::new(
        field.clone(),
        new_offsets.into(),
        new_sizes.into(),
        combined_values,
        interleaved.nulls.clone(),
    )))
}

/// Fallback implementation of interleave using [`MutableArrayData`]
fn interleave_fallback(
    values: &[&dyn Array],
    indices: &[(usize, usize)],
) -> Result<ArrayRef, ArrowError> {
    let arrays: Vec<_> = values.iter().map(|x| x.to_data()).collect();
    let arrays: Vec<_> = arrays.iter().collect();
    let mut array_data = MutableArrayData::try_new(arrays, false, indices.len())?;

    let mut cur_array = indices[0].0;
    let mut start_row_idx = indices[0].1;
    let mut end_row_idx = start_row_idx + 1;

    for (array, row) in indices.iter().skip(1).copied() {
        if array == cur_array && row == end_row_idx {
            // subsequent row in same batch
            end_row_idx += 1;
            continue;
        }

        // emit current batch of rows for current buffer
        array_data.try_extend(cur_array, start_row_idx, end_row_idx)?;

        // start new batch of rows
        cur_array = array;
        start_row_idx = row;
        end_row_idx = start_row_idx + 1;
    }

    // emit final batch of rows
    array_data.try_extend(cur_array, start_row_idx, end_row_idx)?;
    Ok(make_array(array_data.freeze()))
}

/// Fallback implementation for interleaving dictionaries when it was determined
/// that the dictionary values should not be merged. This implementation concatenates
/// the value slices and recomputes the resulting dictionary keys.
///
/// # Panics
///
/// This function assumes that the combined dictionary values will not overflow the
/// key type. Callers must verify this condition [`should_merge_dictionary_values`]
/// before calling this function.
fn interleave_fallback_dictionary<K: ArrowDictionaryKeyType>(
    dictionaries: &[&DictionaryArray<K>],
    indices: &[(usize, usize)],
) -> Result<ArrayRef, ArrowError> {
    let relative_offsets: Vec<usize> = dictionaries
        .iter()
        .scan(0usize, |offset, dict| {
            let current = *offset;
            *offset += dict.values().len();
            Some(current)
        })
        .collect();
    let all_values: Vec<&dyn Array> = dictionaries.iter().map(|d| d.values().as_ref()).collect();
    let concatenated_values = concat(&all_values)?;

    let any_nulls = dictionaries.iter().any(|d| d.keys().nulls().is_some());
    let (new_keys, nulls) = if any_nulls {
        let mut has_nulls = false;
        let new_keys: Vec<K::Native> = indices
            .iter()
            .map(|(array, row)| {
                let old_keys = dictionaries[*array].keys();
                if old_keys.is_valid(*row) {
                    let old_key = old_keys.values()[*row].as_usize();
                    K::Native::from_usize(relative_offsets[*array] + old_key)
                        .expect("key overflow should be checked by caller")
                } else {
                    has_nulls = true;
                    K::Native::ZERO
                }
            })
            .collect();

        let nulls = if has_nulls {
            let null_buffer = BooleanBuffer::collect_bool(indices.len(), |i| {
                let (array, row) = indices[i];
                dictionaries[array].keys().is_valid(row)
            });
            Some(NullBuffer::new(null_buffer))
        } else {
            None
        };
        (new_keys, nulls)
    } else {
        let new_keys: Vec<K::Native> = indices
            .iter()
            .map(|(array, row)| {
                let old_key = dictionaries[*array].keys().values()[*row].as_usize();
                K::Native::from_usize(relative_offsets[*array] + old_key)
                    .expect("key overflow should be checked by caller")
            })
            .collect();
        (new_keys, None)
    };

    let keys_array = PrimitiveArray::<K>::new(new_keys.into(), nulls);
    // SAFETY: keys_array is constructed from a valid set of keys.
    let array = unsafe { DictionaryArray::new_unchecked(keys_array, concatenated_values) };
    Ok(Arc::new(array))
}

/// Interleave rows by index from multiple [`RecordBatch`] instances and return a new [`RecordBatch`].
///
/// This function will call [`interleave`] on each array of the [`RecordBatch`] instances and assemble a new [`RecordBatch`].
///
/// # Example
/// ```
/// # use std::sync::Arc;
/// # use arrow_array::{StringArray, Int32Array, RecordBatch, UInt32Array};
/// # use arrow_schema::{DataType, Field, Schema};
/// # use arrow_select::interleave::interleave_record_batch;
///
/// let schema = Arc::new(Schema::new(vec![
///     Field::new("a", DataType::Int32, true),
///     Field::new("b", DataType::Utf8, true),
/// ]));
///
/// let batch1 = RecordBatch::try_new(
///     schema.clone(),
///     vec![
///         Arc::new(Int32Array::from(vec![0, 1, 2])),
///         Arc::new(StringArray::from(vec!["a", "b", "c"])),
///     ],
/// ).unwrap();
///
/// let batch2 = RecordBatch::try_new(
///     schema.clone(),
///     vec![
///         Arc::new(Int32Array::from(vec![3, 4, 5])),
///         Arc::new(StringArray::from(vec!["d", "e", "f"])),
///     ],
/// ).unwrap();
///
/// let indices = vec![(0, 1), (1, 2), (0, 0), (1, 1)];
/// let interleaved = interleave_record_batch(&[&batch1, &batch2], &indices).unwrap();
///
/// let expected = RecordBatch::try_new(
///     schema,
///     vec![
///         Arc::new(Int32Array::from(vec![1, 5, 0, 4])),
///         Arc::new(StringArray::from(vec!["b", "f", "a", "e"])),
///     ],
/// ).unwrap();
/// assert_eq!(interleaved, expected);
/// ```
pub fn interleave_record_batch(
    record_batches: &[&RecordBatch],
    indices: &[(usize, usize)],
) -> Result<RecordBatch, ArrowError> {
    let schema = record_batches[0].schema();
    let columns = (0..schema.fields().len())
        .map(|i| {
            let column_values: Vec<&dyn Array> = record_batches
                .iter()
                .map(|batch| batch.column(i).as_ref())
                .collect();
            interleave(&column_values, indices)
        })
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(schema, columns)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int32RunArray;
    use arrow_array::builder::{
        GenericListBuilder, Int32Builder, PrimitiveBuilder, PrimitiveRunBuilder,
    };
    use arrow_array::types::{Decimal128Type, Int8Type, TimestampMicrosecondType};
    use arrow_buffer::ScalarBuffer;
    use arrow_schema::{Field, TimeUnit};
    use std::collections::HashMap;

    #[test]
    fn test_primitive() {
        let a = Int32Array::from_iter_values([1, 2, 3, 4]);
        let b = Int32Array::from_iter_values([5, 6, 7]);
        let c = Int32Array::from_iter_values([8, 9, 10]);
        let values = interleave(&[&a, &b, &c], &[(0, 3), (0, 3), (2, 2), (2, 0), (1, 1)]).unwrap();
        let v = values.as_primitive::<Int32Type>();
        assert_eq!(v.values(), &[4, 4, 10, 8, 6]);
    }

    #[test]
    fn test_primitive_nulls() {
        let a = Int32Array::from_iter_values([1, 2, 3, 4]);
        let b = Int32Array::from_iter([Some(1), Some(4), None]);
        let values = interleave(&[&a, &b], &[(0, 1), (1, 2), (1, 2), (0, 3), (0, 2)]).unwrap();
        let v: Vec<_> = values.as_primitive::<Int32Type>().into_iter().collect();
        assert_eq!(&v, &[Some(2), None, None, Some(4), Some(3)])
    }

    #[test]
    fn test_interleave_boolean() {
        let a = BooleanArray::from(vec![Some(true), Some(false), None, Some(true)]);
        let b = BooleanArray::from(vec![Some(false), Some(true), Some(true), None]);

        let values = interleave(&[&a, &b], &[(0, 0), (1, 1), (0, 2), (1, 3), (1, 2)]).unwrap();

        let result = values.as_boolean();

        assert_eq!(
            result.iter().collect::<Vec<_>>(),
            vec![Some(true), Some(true), None, None, Some(true),]
        );
    }

    #[test]
    fn test_interleave_boolean_sliced() {
        let original = BooleanArray::from(vec![
            Some(false),
            Some(true),
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
        ]);

        let sliced_array = original.slice(1, 5);
        let sliced = sliced_array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let other = BooleanArray::from(vec![Some(false), Some(false), Some(true)]);

        let values =
            interleave(&[sliced, &other], &[(0, 0), (1, 2), (0, 3), (0, 4), (1, 0)]).unwrap();

        let result = values.as_boolean();

        assert_eq!(
            result.iter().collect::<Vec<_>>(),
            vec![Some(true), Some(true), None, Some(false), Some(false),]
        );
    }

    #[test]
    fn test_interleave_boolean_fallback_matches_specialized() {
        let a = BooleanArray::from_iter(
            (0..1024).map(|i| if i % 10 == 0 { None } else { Some(i % 2 == 0) }),
        );
        let b = BooleanArray::from_iter((0..1024).map(|i| Some(i % 3 == 0)));

        let indices: Vec<_> = (0..1024).map(|i| (i % 2, (i * 37) % 1024)).collect();

        let values: [&dyn Array; 2] = [&a, &b];

        let specialized = interleave_boolean(&values, &indices).unwrap();
        let fallback = interleave_fallback(&values, &indices).unwrap();

        assert_eq!(specialized.as_ref(), fallback.as_ref());
    }

    #[test]
    fn test_primitive_empty() {
        let a = Int32Array::from_iter_values([1, 2, 3, 4]);
        let v = interleave(&[&a], &[]).unwrap();
        assert!(v.is_empty());
        assert_eq!(v.data_type(), &DataType::Int32);
    }

    #[test]
    fn test_strings() {
        let a = StringArray::from_iter_values(["a", "b", "c"]);
        let b = StringArray::from_iter_values(["hello", "world", "foo"]);
        let values = interleave(&[&a, &b], &[(0, 2), (0, 2), (1, 0), (1, 1), (0, 1)]).unwrap();
        let v = values.as_string::<i32>();
        let values: Vec<_> = v.into_iter().collect();
        assert_eq!(
            &values,
            &[
                Some("c"),
                Some("c"),
                Some("hello"),
                Some("world"),
                Some("b")
            ]
        )
    }

    #[test]
    fn test_interleave_dictionary() {
        let a = DictionaryArray::<Int32Type>::from_iter(["a", "b", "c", "a", "b"]);
        let b = DictionaryArray::<Int32Type>::from_iter(["a", "c", "a", "c", "a"]);

        // Should not recompute dictionary
        let values =
            interleave(&[&a, &b], &[(0, 2), (0, 2), (0, 2), (1, 0), (1, 1), (0, 1)]).unwrap();
        let v = values.as_dictionary::<Int32Type>();
        assert_eq!(v.values().len(), 5);

        let vc = v.downcast_dict::<StringArray>().unwrap();
        let collected: Vec<_> = vc.into_iter().map(Option::unwrap).collect();
        assert_eq!(&collected, &["c", "c", "c", "a", "c", "b"]);

        // Should recompute dictionary
        let values = interleave(&[&a, &b], &[(0, 2), (0, 2), (1, 1)]).unwrap();
        let v = values.as_dictionary::<Int32Type>();
        assert_eq!(v.values().len(), 1);

        let vc = v.downcast_dict::<StringArray>().unwrap();
        let collected: Vec<_> = vc.into_iter().map(Option::unwrap).collect();
        assert_eq!(&collected, &["c", "c", "c"]);
    }

    #[test]
    fn test_interleave_dictionary_nulls() {
        let input_1_keys = Int32Array::from_iter_values([0, 2, 1, 3]);
        let input_1_values = StringArray::from(vec![Some("foo"), None, Some("bar"), Some("fiz")]);
        let input_1 = DictionaryArray::new(input_1_keys, Arc::new(input_1_values));
        let input_2: DictionaryArray<Int32Type> = vec![None].into_iter().collect();

        let expected = vec![Some("fiz"), None, None, Some("foo")];

        let values = interleave(
            &[&input_1 as _, &input_2 as _],
            &[(0, 3), (0, 2), (1, 0), (0, 0)],
        )
        .unwrap();
        let dictionary = values.as_dictionary::<Int32Type>();
        let actual: Vec<Option<&str>> = dictionary
            .downcast_dict::<StringArray>()
            .unwrap()
            .into_iter()
            .collect();

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_interleave_dictionary_overflow_same_values() {
        let values: ArrayRef = Arc::new(StringArray::from_iter_values(
            (0..50).map(|i| format!("v{i}")),
        ));

        // With 3 dictionaries of 50 values each, relative_offsets = [0, 50, 100]
        // Accessing key 49 from dict3 gives 100 + 49 = 149 which overflows Int8
        // (max 127).
        // This test case falls back to interleave_fallback because the
        // dictionaries share the same underlying values slice.
        let dict1 = DictionaryArray::<Int8Type>::new(
            Int8Array::from_iter_values([0, 1, 2]),
            values.clone(),
        );
        let dict2 = DictionaryArray::<Int8Type>::new(
            Int8Array::from_iter_values([0, 1, 2]),
            values.clone(),
        );
        let dict3 =
            DictionaryArray::<Int8Type>::new(Int8Array::from_iter_values([49]), values.clone());

        let indices = &[(0, 0), (1, 0), (2, 0)];
        let result = interleave(&[&dict1, &dict2, &dict3], indices).unwrap();

        let dict_result = result.as_dictionary::<Int8Type>();
        let string_result: Vec<_> = dict_result
            .downcast_dict::<StringArray>()
            .unwrap()
            .into_iter()
            .map(|x| x.unwrap())
            .collect();
        assert_eq!(string_result, vec!["v0", "v0", "v49"]);
    }

    fn test_interleave_lists<O: OffsetSizeTrait>() {
        // [[1, 2], null, [3]]
        let mut a = GenericListBuilder::<O, _>::new(Int32Builder::new());
        a.values().append_value(1);
        a.values().append_value(2);
        a.append(true);
        a.append(false);
        a.values().append_value(3);
        a.append(true);
        let a = a.finish();

        // [[4], null, [5, 6, null]]
        let mut b = GenericListBuilder::<O, _>::new(Int32Builder::new());
        b.values().append_value(4);
        b.append(true);
        b.append(false);
        b.values().append_value(5);
        b.values().append_value(6);
        b.values().append_null();
        b.append(true);
        let b = b.finish();

        let values = interleave(&[&a, &b], &[(0, 2), (0, 1), (1, 0), (1, 2), (1, 1)]).unwrap();
        let v = values
            .as_any()
            .downcast_ref::<GenericListArray<O>>()
            .unwrap();

        // [[3], null, [4], [5, 6, null], null]
        let mut expected = GenericListBuilder::<O, _>::new(Int32Builder::new());
        expected.values().append_value(3);
        expected.append(true);
        expected.append(false);
        expected.values().append_value(4);
        expected.append(true);
        expected.values().append_value(5);
        expected.values().append_value(6);
        expected.values().append_null();
        expected.append(true);
        expected.append(false);
        let expected = expected.finish();

        assert_eq!(v, &expected);
    }

    #[test]
    fn test_lists() {
        test_interleave_lists::<i32>();
    }

    #[test]
    fn test_large_lists() {
        test_interleave_lists::<i64>();
    }

    /// One list slot in a `List<Primitive>` fixture: `None` is a null slot,
    /// `Some(items)` is a list whose items may individually be null.
    type ListRow<T> = Option<Vec<Option<<T as ArrowPrimitiveType>::Native>>>;

    /// Build a `List<Primitive>` from row fixtures. The primitive child carries
    /// `data_type` (e.g. its Decimal scale or timezone).
    fn list_of_primitive<O: OffsetSizeTrait, T: ArrowPrimitiveType>(
        data_type: &DataType,
        rows: &[ListRow<T>],
    ) -> GenericListArray<O> {
        let mut builder = GenericListBuilder::<O, _>::new(
            PrimitiveBuilder::<T>::new().with_data_type(data_type.clone()),
        );
        for row in rows {
            match row {
                Some(items) => {
                    items
                        .iter()
                        .for_each(|v| builder.values().append_option(*v));
                    builder.append(true);
                }
                None => builder.append(false),
            }
        }
        builder.finish()
    }

    /// Interleave list fixtures and assert both the result and that the
    /// interleaved primitive child preserves the parameterized `data_type`.
    fn check_interleave_list_primitive<O: OffsetSizeTrait, T: ArrowPrimitiveType>(
        data_type: &DataType,
        inputs: &[&[ListRow<T>]],
        indices: &[(usize, usize)],
        expected: &[ListRow<T>],
    ) {
        let arrays: Vec<_> = inputs
            .iter()
            .map(|rows| list_of_primitive::<O, T>(data_type, rows))
            .collect();
        let refs: Vec<&dyn Array> = arrays.iter().map(|a| a as &dyn Array).collect();

        let values = interleave(&refs, indices).unwrap();
        let v = values
            .as_any()
            .downcast_ref::<GenericListArray<O>>()
            .unwrap();

        assert_eq!(v, &list_of_primitive::<O, T>(data_type, expected));
        // The child's logical type (Decimal precision/scale, Timestamp timezone)
        // must be preserved, not reset to the primitive's default.
        assert_eq!(v.values().data_type(), data_type);
    }

    fn test_interleave_lists_decimal<O: OffsetSizeTrait>() {
        // List<Decimal128(20, 3)>, exercising child-element nulls and null slots.
        check_interleave_list_primitive::<O, Decimal128Type>(
            &DataType::Decimal128(20, 3),
            &[
                &[
                    Some(vec![Some(1), Some(2)]),
                    None,
                    Some(vec![Some(3), None]),
                ], // a
                &[Some(vec![Some(4)]), Some(vec![Some(5), Some(6)])], // b
            ],
            &[(0, 2), (0, 1), (1, 0), (1, 1)],
            &[
                Some(vec![Some(3), None]),
                None,
                Some(vec![Some(4)]),
                Some(vec![Some(5), Some(6)]),
            ],
        );
    }

    #[test]
    fn test_lists_decimal() {
        test_interleave_lists_decimal::<i32>();
        test_interleave_lists_decimal::<i64>();
    }

    fn test_interleave_lists_timestamp_tz<O: OffsetSizeTrait>() {
        // List<Timestamp(Microsecond, "+08:00")>, checking the timezone survives.
        check_interleave_list_primitive::<O, TimestampMicrosecondType>(
            &DataType::Timestamp(TimeUnit::Microsecond, Some("+08:00".into())),
            &[&[Some(vec![Some(1), Some(2)]), Some(vec![Some(3)])]],
            &[(0, 1), (0, 0)],
            &[Some(vec![Some(3)]), Some(vec![Some(1), Some(2)])],
        );
    }

    #[test]
    fn test_lists_timestamp_tz() {
        test_interleave_lists_timestamp_tz::<i32>();
        test_interleave_lists_timestamp_tz::<i64>();
    }

    /// The old list path expands child indices and recursively interleaves the
    /// child array. Keep it as the oracle, including backing values under nulls.
    fn check_list_ranges<O: OffsetSizeTrait>(
        arrays: &[&dyn Array],
        indices: &[(usize, usize)],
    ) -> ArrayRef {
        let actual = interleave(arrays, indices).unwrap();
        let list = actual.as_list::<O>();
        let input = Interleave::<'_, GenericListArray<O>>::new(arrays, indices);
        let mut offsets = vec![0];
        for &(a, row) in indices {
            offsets.push(offsets.last().unwrap() + input.arrays[a].value_length(row).as_usize());
        }
        assert_eq!(list.data_type(), arrays[0].data_type());
        assert_eq!(
            list.value_offsets()
                .iter()
                .map(|o| o.as_usize())
                .collect::<Vec<_>>(),
            offsets
        );
        assert_eq!(
            (0..list.len())
                .map(|i| list.is_valid(i))
                .collect::<Vec<_>>(),
            indices
                .iter()
                .map(|&(a, row)| input.arrays[a].is_valid(row))
                .collect::<Vec<_>>()
        );
        let expected =
            interleave_list_like_child(&input, indices, *offsets.last().unwrap()).unwrap();
        assert_eq!(list.values().as_ref(), expected.as_ref());
        assert_range_validity(list.values().as_ref(), expected.as_ref());
        if let DataType::Struct(_) = expected.data_type() {
            for (a, b) in list
                .values()
                .as_struct()
                .columns()
                .iter()
                .zip(expected.as_struct().columns())
            {
                assert_range_validity(a.as_ref(), b.as_ref());
                // Logical equality alone ignores payloads under nulls, including
                // NaN payloads and signed zero. Compare the physical buffers too.
                assert_eq!(a.to_data().buffers(), b.to_data().buffers());
            }
        } else {
            assert_eq!(
                list.values().to_data().buffers(),
                expected.to_data().buffers()
            );
        }
        actual
    }

    fn assert_range_validity(actual: &dyn Array, expected: &dyn Array) {
        assert_eq!(actual.len(), expected.len());
        assert_eq!(actual.null_count(), expected.null_count());
        for i in 0..actual.len() {
            assert_eq!(actual.is_valid(i), expected.is_valid(i));
        }
    }

    fn sliced_struct_list<O: OffsetSizeTrait>(mode: usize) -> GenericListArray<O> {
        let validity = |period| match mode {
            0 => None,
            1 => Some(NullBuffer::from_iter((0..24).map(|i| i % period != 0))),
            _ => Some(NullBuffer::new_null(24)),
        };
        let bits = [
            0x7ff8000000000001,
            0x7ff0000000000002,
            0x8000000000000000,
            0,
        ];
        let floats = Float64Array::new(
            (0..24)
                .map(|i| f64::from_bits(bits[i % bits.len()]))
                .collect::<Vec<_>>()
                .into(),
            validity(3),
        )
        .slice(1, 20);
        let times = TimestampMicrosecondArray::new(
            (0..24).map(i64::from).collect::<Vec<_>>().into(),
            validity(5),
        )
        .with_timezone("+08:00")
        .slice(2, 20);
        let decimals = Decimal128Array::new(
            (0..24).map(i128::from).collect::<Vec<_>>().into(),
            validity(7),
        )
        .with_precision_and_scale(20, 3)
        .unwrap()
        .slice(3, 20);
        let columns: Vec<ArrayRef> = vec![Arc::new(floats), Arc::new(times), Arc::new(decimals)];
        let fields: Fields = columns
            .iter()
            .enumerate()
            .map(|(i, a)| {
                Field::new(format!("f{i}"), a.data_type().clone(), true).with_metadata(
                    HashMap::from([("field-key".to_owned(), "field-value".to_owned())]),
                )
            })
            .collect();
        let nulls = validity(2).map(|n| n.slice(1, 20));
        let child = StructArray::new(fields, columns, nulls).slice(3, 12);
        let field =
            Field::new("entries", child.data_type().clone(), true).with_metadata(HashMap::from([
                ("list-key".to_owned(), "list-value".to_owned()),
            ]));
        let offsets = OffsetBuffer::new(
            [0, 1, 1, 4, 8, 12]
                .map(|i| O::from_usize(i).unwrap())
                .to_vec()
                .into(),
        );
        let nulls = match mode {
            0 => None,
            1 => Some(NullBuffer::from(vec![true, true, false, true, false])),
            _ => Some(NullBuffer::new_null(5)),
        };
        GenericListArray::new(Arc::new(field), offsets, Arc::new(child), nulls).slice(1, 4)
    }

    fn check_struct_list_slices<O: OffsetSizeTrait>() {
        let a = sliced_struct_list::<O>(0);
        let b = sliced_struct_list::<O>(1);
        let c = sliced_struct_list::<O>(2);
        let arrays: [&dyn Array; 3] = [&a, &b, &c];
        for indices in [
            vec![(1, 1), (0, 3), (2, 2), (1, 3), (0, 0), (1, 1)],
            vec![(0, 0), (1, 0), (2, 0)],
            vec![(0, 1)],
            vec![],
        ] {
            check_list_ranges::<O>(&arrays, &indices);
        }
        // Exercise many range/bitmap boundaries deterministically, without RNG.
        let indices: Vec<_> = (0..257).map(|i| (i % 3, (i * 7 / 3) % 4)).collect();
        check_list_ranges::<O>(&arrays, &indices);
    }

    #[test]
    fn test_list_struct_ranges_sliced() {
        check_struct_list_slices::<i32>();
        check_struct_list_slices::<i64>();
    }

    #[test]
    fn test_list_struct_ranges_hidden_values() {
        let bits = [
            0x8000000000000000,
            0x7ff8000000000001,
            0x7ff0000000000002,
            0,
        ];
        let floats = Float64Array::new(
            bits.map(f64::from_bits).to_vec().into(),
            Some(NullBuffer::from(vec![true, false, true, false])),
        );
        let child = StructArray::new(
            vec![Field::new("value", DataType::Float64, true)].into(),
            vec![Arc::new(floats)],
            Some(NullBuffer::from(vec![false, true, false, true])),
        );
        let list = ListArray::new(
            Arc::new(Field::new_list_field(child.data_type().clone(), true)),
            OffsetBuffer::new(vec![0, 2, 2, 4].into()),
            Arc::new(child),
            Some(NullBuffer::from(vec![false, true, true])),
        );
        let result = check_list_ranges::<i32>(&[&list], &[(0, 2), (0, 0), (0, 1), (0, 0)]);
        let list = result.as_list::<i32>();
        assert_eq!(list.value_offsets(), &[0, 2, 4, 4, 6]);
        assert_eq!(
            list.nulls().unwrap().iter().collect::<Vec<_>>(),
            [true, false, true, false]
        );
        let child = list.values().as_struct();
        assert_eq!(
            child.nulls().unwrap().iter().collect::<Vec<_>>(),
            [false, true, false, true, false, true]
        );
        let values = child.column(0).as_primitive::<Float64Type>();
        assert_eq!(
            values.nulls().unwrap().iter().collect::<Vec<_>>(),
            [true, false, true, false, true, false]
        );
        assert_eq!(
            values
                .values()
                .iter()
                .map(|v| v.to_bits())
                .collect::<Vec<_>>(),
            [bits[2], bits[3], bits[0], bits[1], bits[0], bits[1]]
        );
    }

    fn check_struct_list_empty<O: OffsetSizeTrait>() {
        let child =
            StructArray::new_empty_fields(3, Some(NullBuffer::from(vec![true, false, true])));
        let list = GenericListArray::<O>::new(
            Arc::new(Field::new_list_field(child.data_type().clone(), true)),
            OffsetBuffer::new(
                [0, 0, 2, 3]
                    .map(|i| O::from_usize(i).unwrap())
                    .to_vec()
                    .into(),
            ),
            Arc::new(child),
            None,
        );
        let result = check_list_ranges::<O>(&[&list], &[(0, 2), (0, 1), (0, 1)]);
        assert_eq!(result.as_list::<O>().values().len(), 5);
        check_list_ranges::<O>(&[&list], &[(0, 0), (0, 0)]);
        check_list_ranges::<O>(&[&list], &[]);
    }

    #[test]
    fn test_list_struct_ranges_empty() {
        check_struct_list_empty::<i32>();
        check_struct_list_empty::<i64>();
    }

    #[test]
    fn test_list_struct_ranges_offset_overflow() {
        let child = StructArray::new_empty_fields(i32::MAX as usize, None);
        let list = ListArray::new(
            Arc::new(Field::new_list_field(child.data_type().clone(), false)),
            OffsetBuffer::new(vec![0, i32::MAX].into()),
            Arc::new(child),
            None,
        );
        assert!(matches!(interleave(&[&list], &[(0, 0), (0, 0)]),
            Err(ArrowError::OffsetOverflowError(n)) if n == 2 * i32::MAX as usize));
    }

    #[test]
    fn test_list_struct_ranges_dispatch() {
        use arrow_schema::IntervalUnit;
        let mut types = vec![
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float16,
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal32(8, 2),
            DataType::Decimal64(16, 3),
            DataType::Decimal128(20, 4),
            DataType::Decimal256(40, 5),
            DataType::Date32,
            DataType::Date64,
            DataType::Time32(TimeUnit::Second),
            DataType::Time32(TimeUnit::Millisecond),
            DataType::Time64(TimeUnit::Microsecond),
            DataType::Time64(TimeUnit::Nanosecond),
            DataType::Interval(IntervalUnit::YearMonth),
            DataType::Interval(IntervalUnit::DayTime),
            DataType::Interval(IntervalUnit::MonthDayNano),
        ];
        for unit in [
            TimeUnit::Second,
            TimeUnit::Millisecond,
            TimeUnit::Microsecond,
            TimeUnit::Nanosecond,
        ] {
            types.push(DataType::Timestamp(unit, Some("UTC".into())));
            types.push(DataType::Duration(unit));
        }
        for data_type in types {
            let fields: Fields = vec![Field::new("value", data_type.clone(), true)].into();
            assert!(can_interleave_struct_ranges(&fields), "{data_type}");
            let child = StructArray::new(fields, vec![new_null_array(&data_type, 5)], None);
            let list = ListArray::new(
                Arc::new(Field::new_list_field(child.data_type().clone(), false)),
                OffsetBuffer::new(vec![0, 2, 5].into()),
                Arc::new(child),
                None,
            );
            check_list_ranges::<i32>(&[&list], &[(0, 1), (0, 0), (0, 1)]);
        }
        // is_primitive() alone would accept these unsupported temporal types.
        for data_type in [
            DataType::Time32(TimeUnit::Nanosecond),
            DataType::Time64(TimeUnit::Second),
        ] {
            assert!(!can_interleave_struct_ranges(
                &vec![Field::new("value", data_type, true)].into()
            ));
        }
    }

    #[test]
    fn test_list_struct_ranges_fallback() {
        let nested: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(1)]),
            None,
            Some(vec![]),
        ]));
        let unsupported: Vec<ArrayRef> = vec![
            Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
            Arc::new(StringArray::from(vec![Some("a"), None, Some("b")])),
            Arc::new(DictionaryArray::<Int32Type>::from_iter([
                Some("a"),
                None,
                Some("b"),
            ])),
            nested,
        ];
        for other in unsupported {
            let fields: Fields = vec![
                Field::new("a", DataType::Int64, false),
                Field::new("b", other.data_type().clone(), true),
            ]
            .into();
            assert!(!can_interleave_struct_ranges(&fields));
            let child = StructArray::new(
                fields,
                vec![Arc::new(Int64Array::from(vec![1, 2, 3])), other],
                None,
            );
            let list = ListArray::new(
                Arc::new(Field::new_list_field(child.data_type().clone(), false)),
                OffsetBuffer::new(vec![0, 1, 3].into()),
                Arc::new(child),
                None,
            );
            check_list_ranges::<i32>(&[&list], &[(0, 1), (0, 0), (0, 1)]);
        }
    }

    #[test]
    fn test_list_primitive_ranges_sliced() {
        let a = Int64Array::new(
            (0..24).map(i64::from).collect::<Vec<_>>().into(),
            Some(NullBuffer::from_iter((0..24).map(|i| i % 3 != 0))),
        )
        .slice(1, 20);
        let b = Int64Array::from_iter_values(0..20);
        let lists: Vec<_> = [a, b]
            .into_iter()
            .map(|a| {
                ListArray::new(
                    Arc::new(Field::new_list_field(DataType::Int64, true)),
                    OffsetBuffer::new(vec![0, 1, 2, 9, 20].into()),
                    Arc::new(a),
                    Some(NullBuffer::from(vec![true, false, true, true])),
                )
                .slice(1, 3)
            })
            .collect();
        check_list_ranges::<i32>(&[&lists[0], &lists[1]], &[(0, 2), (1, 1), (0, 1), (0, 0)]);
        let result = check_list_ranges::<i32>(&[&lists[0], &lists[1]], &[(0, 0), (1, 0)]);
        assert!(result.as_list::<i32>().values().nulls().is_none());
    }

    #[test]
    fn test_list_struct_ranges_nonnullable_field() {
        // A nonnullable field may contain nulls hidden by its Struct's mask.
        let nulls = NullBuffer::from(vec![true, false, true, false]);
        let values = Int64Array::new(vec![10, 11, 12, 13].into(), Some(nulls.clone()));
        let child = StructArray::new(
            vec![Field::new("value", DataType::Int64, false)].into(),
            vec![Arc::new(values)],
            Some(nulls),
        )
        .slice(1, 3);
        let list = ListArray::new(
            Arc::new(Field::new_list_field(child.data_type().clone(), true)),
            OffsetBuffer::new(vec![0, 1, 3].into()),
            Arc::new(child),
            None,
        );
        let result = check_list_ranges::<i32>(&[&list], &[(0, 1), (0, 0), (0, 1)]);
        let child = result.as_list::<i32>().values().as_struct();
        assert!(!child.fields()[0].is_nullable());
        assert_eq!(
            child.column(0).as_primitive::<Int64Type>().values(),
            &[12, 13, 11, 12, 13]
        );
    }

    #[test]
    fn test_fixed_size_list_primitive_ranges_sliced() {
        let field = Arc::new(Field::new_list_field(DataType::Int64, true));
        let values = Int64Array::new(
            (0..14).map(i64::from).collect::<Vec<_>>().into(),
            Some(NullBuffer::from_iter((0..14).map(|i| i % 3 != 0))),
        )
        .slice(1, 12);
        let a = FixedSizeListArray::new(
            field.clone(),
            2,
            Arc::new(values),
            Some(NullBuffer::from(vec![true, false, true, false, true, true])),
        )
        .slice(1, 4);
        let b = FixedSizeListArray::new(
            field,
            2,
            Arc::new(Int64Array::from_iter_values(20..28)),
            None,
        );
        let arrays: [&dyn Array; 2] = [&a, &b];
        let indices = [(0, 3), (1, 0), (0, 1), (0, 3)];
        let result = interleave(&arrays, &indices).unwrap();
        let list = result.as_fixed_size_list();
        let input = Interleave::<'_, FixedSizeListArray>::new(&arrays, &indices);
        let expected = interleave_list_like_child(&input, &indices, indices.len() * 2).unwrap();
        assert_eq!(list.nulls(), input.nulls.as_ref());
        assert_eq!(list.values().as_ref(), expected.as_ref());
        assert_range_validity(list.values().as_ref(), expected.as_ref());
        assert_eq!(
            list.values().to_data().buffers(),
            expected.to_data().buffers()
        );
    }

    fn test_interleave_list_views<O: OffsetSizeTrait>() {
        // [[1, 2], null, [3]]
        let mut a = GenericListBuilder::<O, _>::new(Int32Builder::new());
        a.values().append_value(1);
        a.values().append_value(2);
        a.append(true);
        a.append(false);
        a.values().append_value(3);
        a.append(true);
        let a: GenericListViewArray<O> = a.finish().into();

        // [[4], null, [5, 6, null]]
        let mut b = GenericListBuilder::<O, _>::new(Int32Builder::new());
        b.values().append_value(4);
        b.append(true);
        b.append(false);
        b.values().append_value(5);
        b.values().append_value(6);
        b.values().append_null();
        b.append(true);
        let b: GenericListViewArray<O> = b.finish().into();

        let values = interleave(&[&a, &b], &[(0, 2), (0, 1), (1, 0), (1, 2), (1, 1)]).unwrap();
        let v = values
            .as_any()
            .downcast_ref::<GenericListViewArray<O>>()
            .unwrap();

        // [[3], null, [4], [5, 6, null], null]
        let mut expected = GenericListBuilder::<O, _>::new(Int32Builder::new());
        expected.values().append_value(3);
        expected.append(true);
        expected.append(false);
        expected.values().append_value(4);
        expected.append(true);
        expected.values().append_value(5);
        expected.values().append_value(6);
        expected.values().append_null();
        expected.append(true);
        expected.append(false);
        let expected: GenericListViewArray<O> = expected.finish().into();

        assert_eq!(v, &expected);
    }

    #[test]
    fn test_list_views() {
        test_interleave_list_views::<i32>();
    }

    #[test]
    fn test_large_list_views() {
        test_interleave_list_views::<i64>();
    }

    #[test]
    fn test_interleave_list_view_overlapping() {
        let field = Arc::new(Field::new_list_field(DataType::Int64, false));

        // lv_a: 10 rows, two groups of 5 sharing the same backing elements.
        //   rows 0-4 → offset 0, size 5 → [0,1,2,3,4]
        //   rows 5-9 → offset 5, size 5 → [5,6,7,8,9]
        let lv_a = ListViewArray::new(
            Arc::clone(&field),
            ScalarBuffer::from(vec![0i32, 0, 0, 0, 0, 5, 5, 5, 5, 5]),
            ScalarBuffer::from(vec![5i32; 10]),
            Arc::new(Int64Array::from_iter_values(0..10)),
            None,
        );

        // lv_b: 8 rows, two groups of 4 sharing the same backing elements.
        //   rows 0-3 → offset 0, size 3 → [100,101,102]
        //   rows 4-7 → offset 3, size 3 → [103,104,105]
        let lv_b = ListViewArray::new(
            Arc::clone(&field),
            ScalarBuffer::from(vec![0i32, 0, 0, 0, 3, 3, 3, 3]),
            ScalarBuffer::from(vec![3i32; 8]),
            Arc::new(Int64Array::from_iter_values(100..106)),
            None,
        );

        let indices: Vec<(usize, usize)> = vec![
            (0, 0),
            (1, 0),
            (0, 5),
            (1, 4),
            (0, 1),
            (1, 1),
            (0, 6),
            (1, 5),
        ];
        let result = interleave(&[&lv_a as &dyn Array, &lv_b as &dyn Array], &indices).unwrap();
        result
            .to_data()
            .validate_full()
            .expect("result must be valid");

        let result_lv = result.as_list_view::<i32>();
        assert_eq!(result_lv.len(), 8);
        assert_eq!(
            result_lv.value(0).as_primitive::<Int64Type>().values(),
            &[0, 1, 2, 3, 4]
        );
        assert_eq!(
            result_lv.value(1).as_primitive::<Int64Type>().values(),
            &[100, 101, 102]
        );
        assert_eq!(
            result_lv.value(2).as_primitive::<Int64Type>().values(),
            &[5, 6, 7, 8, 9]
        );
        assert_eq!(
            result_lv.value(3).as_primitive::<Int64Type>().values(),
            &[103, 104, 105]
        );

        // Backing elements = sum of source arrays (10 + 6 = 16), not per-row
        // expansion (8 rows × avg ~4 = 32). Overlapping sharing is preserved.
        let total_input_elements = lv_a.values().len() + lv_b.values().len();
        assert_eq!(result_lv.values().len(), total_input_elements);
    }

    #[test]
    fn test_struct_without_nulls() {
        let fields = Fields::from(vec![
            Field::new("number_col", DataType::Int32, false),
            Field::new("string_col", DataType::Utf8, false),
        ]);
        let a = {
            let number_col = Int32Array::from_iter_values([1, 2, 3, 4]);
            let string_col = StringArray::from_iter_values(["a", "b", "c", "d"]);

            StructArray::try_new(
                fields.clone(),
                vec![Arc::new(number_col), Arc::new(string_col)],
                None,
            )
            .unwrap()
        };

        let b = {
            let number_col = Int32Array::from_iter_values([5, 6, 7]);
            let string_col = StringArray::from_iter_values(["hello", "world", "foo"]);

            StructArray::try_new(
                fields.clone(),
                vec![Arc::new(number_col), Arc::new(string_col)],
                None,
            )
            .unwrap()
        };

        let c = {
            let number_col = Int32Array::from_iter_values([8, 9, 10]);
            let string_col = StringArray::from_iter_values(["x", "y", "z"]);

            StructArray::try_new(
                fields.clone(),
                vec![Arc::new(number_col), Arc::new(string_col)],
                None,
            )
            .unwrap()
        };

        let values = interleave(&[&a, &b, &c], &[(0, 3), (0, 3), (2, 2), (2, 0), (1, 1)]).unwrap();
        let values_struct = values.as_struct();
        assert_eq!(values_struct.data_type(), &DataType::Struct(fields));
        assert_eq!(values_struct.null_count(), 0);

        let values_number = values_struct.column(0).as_primitive::<Int32Type>();
        assert_eq!(values_number.values(), &[4, 4, 10, 8, 6]);
        let values_string = values_struct.column(1).as_string::<i32>();
        let values_string: Vec<_> = values_string.into_iter().collect();
        assert_eq!(
            &values_string,
            &[Some("d"), Some("d"), Some("z"), Some("x"), Some("world")]
        );
    }

    #[test]
    fn test_struct_with_nulls_in_values() {
        let fields = Fields::from(vec![
            Field::new("number_col", DataType::Int32, true),
            Field::new("string_col", DataType::Utf8, true),
        ]);
        let a = {
            let number_col = Int32Array::from_iter_values([1, 2, 3, 4]);
            let string_col = StringArray::from_iter_values(["a", "b", "c", "d"]);

            StructArray::try_new(
                fields.clone(),
                vec![Arc::new(number_col), Arc::new(string_col)],
                None,
            )
            .unwrap()
        };

        let b = {
            let number_col = Int32Array::from_iter([Some(1), Some(4), None]);
            let string_col = StringArray::from(vec![Some("hello"), None, Some("foo")]);

            StructArray::try_new(
                fields.clone(),
                vec![Arc::new(number_col), Arc::new(string_col)],
                None,
            )
            .unwrap()
        };

        let values = interleave(&[&a, &b], &[(0, 1), (1, 2), (1, 2), (0, 3), (1, 1)]).unwrap();
        let values_struct = values.as_struct();
        assert_eq!(values_struct.data_type(), &DataType::Struct(fields));

        // The struct itself has no nulls, but the values do
        assert_eq!(values_struct.null_count(), 0);

        let values_number: Vec<_> = values_struct
            .column(0)
            .as_primitive::<Int32Type>()
            .into_iter()
            .collect();
        assert_eq!(values_number, &[Some(2), None, None, Some(4), Some(4)]);

        let values_string = values_struct.column(1).as_string::<i32>();
        let values_string: Vec<_> = values_string.into_iter().collect();
        assert_eq!(
            &values_string,
            &[Some("b"), Some("foo"), Some("foo"), Some("d"), None]
        );
    }

    #[test]
    fn test_struct_with_nulls() {
        let fields = Fields::from(vec![
            Field::new("number_col", DataType::Int32, false),
            Field::new("string_col", DataType::Utf8, false),
        ]);
        let a = {
            let number_col = Int32Array::from_iter_values([1, 2, 3, 4]);
            let string_col = StringArray::from_iter_values(["a", "b", "c", "d"]);

            StructArray::try_new(
                fields.clone(),
                vec![Arc::new(number_col), Arc::new(string_col)],
                None,
            )
            .unwrap()
        };

        let b = {
            let number_col = Int32Array::from_iter_values([5, 6, 7]);
            let string_col = StringArray::from_iter_values(["hello", "world", "foo"]);

            StructArray::try_new(
                fields.clone(),
                vec![Arc::new(number_col), Arc::new(string_col)],
                Some(NullBuffer::from(&[true, false, true])),
            )
            .unwrap()
        };

        let c = {
            let number_col = Int32Array::from_iter_values([8, 9, 10]);
            let string_col = StringArray::from_iter_values(["x", "y", "z"]);

            StructArray::try_new(
                fields.clone(),
                vec![Arc::new(number_col), Arc::new(string_col)],
                None,
            )
            .unwrap()
        };

        let values = interleave(&[&a, &b, &c], &[(0, 3), (0, 3), (2, 2), (1, 1), (2, 0)]).unwrap();
        let values_struct = values.as_struct();
        assert_eq!(values_struct.data_type(), &DataType::Struct(fields));

        let validity: Vec<bool> = {
            let null_buffer = values_struct.nulls().expect("should_have_nulls");

            null_buffer.iter().collect()
        };
        assert_eq!(validity, &[true, true, true, false, true]);
        let values_number = values_struct.column(0).as_primitive::<Int32Type>();
        assert_eq!(values_number.values(), &[4, 4, 10, 6, 8]);
        let values_string = values_struct.column(1).as_string::<i32>();
        let values_string: Vec<_> = values_string.into_iter().collect();
        assert_eq!(
            &values_string,
            &[Some("d"), Some("d"), Some("z"), Some("world"), Some("x"),]
        );
    }

    #[test]
    fn test_struct_empty() {
        let fields = Fields::from(vec![
            Field::new("number_col", DataType::Int32, false),
            Field::new("string_col", DataType::Utf8, false),
        ]);
        let a = {
            let number_col = Int32Array::from_iter_values([1, 2, 3, 4]);
            let string_col = StringArray::from_iter_values(["a", "b", "c", "d"]);

            StructArray::try_new(
                fields.clone(),
                vec![Arc::new(number_col), Arc::new(string_col)],
                None,
            )
            .unwrap()
        };
        let v = interleave(&[&a], &[]).unwrap();
        assert!(v.is_empty());
        assert_eq!(v.data_type(), &DataType::Struct(fields));
    }

    #[test]
    fn interleave_sparse_nulls() {
        let values = StringArray::from_iter_values((0..100).map(|x| x.to_string()));
        let keys = Int32Array::from_iter_values(0..10);
        let dict_a = DictionaryArray::new(keys, Arc::new(values));
        let values = StringArray::new_null(0);
        let keys = Int32Array::new_null(10);
        let dict_b = DictionaryArray::new(keys, Arc::new(values));

        let indices = &[(0, 0), (0, 1), (0, 2), (1, 0)];
        let array = interleave(&[&dict_a, &dict_b], indices).unwrap();

        let expected =
            DictionaryArray::<Int32Type>::from_iter(vec![Some("0"), Some("1"), Some("2"), None]);
        assert_eq!(array.as_ref(), &expected)
    }

    #[test]
    fn test_interleave_views() {
        let values = StringArray::from_iter_values([
            "hello",
            "world_long_string_not_inlined",
            "foo",
            "bar",
            "baz",
        ]);
        let view_a = StringViewArray::from(&values);

        let values = StringArray::from_iter_values([
            "test",
            "data",
            "more_long_string_not_inlined",
            "views",
            "here",
        ]);
        let view_b = StringViewArray::from(&values);

        let indices = &[
            (0, 2), // "foo"
            (1, 0), // "test"
            (0, 4), // "baz"
            (1, 3), // "views"
            (0, 1), // "world_long_string_not_inlined"
        ];

        // Test specialized implementation
        let values = interleave(&[&view_a, &view_b], indices).unwrap();
        let result = values.as_string_view();
        assert_eq!(result.data_buffers().len(), 1);

        let fallback = interleave_fallback(&[&view_a, &view_b], indices).unwrap();
        let fallback_result = fallback.as_string_view();
        // note that fallback_result has 2 buffers, but only one long enough string to warrant a buffer
        assert_eq!(fallback_result.data_buffers().len(), 2);

        // Convert to strings for easier assertion
        let collected: Vec<_> = result.iter().map(|x| x.map(|s| s.to_string())).collect();

        let fallback_collected: Vec<_> = fallback_result
            .iter()
            .map(|x| x.map(|s| s.to_string()))
            .collect();

        assert_eq!(&collected, &fallback_collected);

        assert_eq!(
            &collected,
            &[
                Some("foo".to_string()),
                Some("test".to_string()),
                Some("baz".to_string()),
                Some("views".to_string()),
                Some("world_long_string_not_inlined".to_string()),
            ]
        );
    }

    #[test]
    fn test_interleave_views_with_nulls() {
        let values = StringArray::from_iter([
            Some("hello"),
            None,
            Some("foo_long_string_not_inlined"),
            Some("bar"),
            None,
        ]);
        let view_a = StringViewArray::from(&values);

        let values = StringArray::from_iter([
            Some("test"),
            Some("data_long_string_not_inlined"),
            None,
            None,
            Some("here"),
        ]);
        let view_b = StringViewArray::from(&values);

        let indices = &[
            (0, 1), // null
            (1, 2), // null
            (0, 2), // "foo_long_string_not_inlined"
            (1, 3), // null
            (0, 4), // null
        ];

        // Test specialized implementation
        let values = interleave(&[&view_a, &view_b], indices).unwrap();
        let result = values.as_string_view();
        assert_eq!(result.data_buffers().len(), 1);

        let fallback = interleave_fallback(&[&view_a, &view_b], indices).unwrap();
        let fallback_result = fallback.as_string_view();

        // Convert to strings for easier assertion
        let collected: Vec<_> = result.iter().map(|x| x.map(|s| s.to_string())).collect();

        let fallback_collected: Vec<_> = fallback_result
            .iter()
            .map(|x| x.map(|s| s.to_string()))
            .collect();

        assert_eq!(&collected, &fallback_collected);

        assert_eq!(
            &collected,
            &[
                None,
                None,
                Some("foo_long_string_not_inlined".to_string()),
                None,
                None,
            ]
        );
    }

    #[test]
    fn test_interleave_views_shared_buffers() {
        let long = |i: usize| format!("long_string_not_inlined_{i}");
        let base = StringViewArray::from_iter_values((0..10).map(long));
        assert_eq!(base.data_buffers().len(), 1);

        // Slices share the same buffers
        let a = base.slice(0, 5);
        let b = base.slice(5, 5);
        // A distinct buffer list containing the same underlying buffer
        let c = StringViewArray::try_new(base.views().clone(), base.data_buffers().to_vec(), None)
            .unwrap();

        let indices = &[(0, 1), (1, 2), (2, 3), (0, 4), (1, 0), (2, 9)];
        let values = interleave(&[&a, &b, &c], indices).unwrap();
        let result = values.as_string_view();
        assert_eq!(result.data_buffers().len(), 1);

        let expected: Vec<_> = [1, 7, 3, 4, 5, 9].into_iter().map(long).collect();
        let actual: Vec<_> = result.iter().map(|x| x.unwrap().to_string()).collect();
        assert_eq!(actual, expected);

        // Many slices, exceeding the linear scan threshold
        let slices: Vec<_> = (0..40).map(|i| base.slice(i % 10, 1)).collect();
        let arrays: Vec<&dyn Array> = slices.iter().map(|x| x as &dyn Array).collect();
        let indices: Vec<_> = (0..40).rev().map(|i| (i, 0)).collect();
        let values = interleave(&arrays, &indices).unwrap();
        let result = values.as_string_view();
        assert_eq!(result.data_buffers().len(), 1);

        let expected: Vec<_> = (0..40).rev().map(|i| long(i % 10)).collect();
        let actual: Vec<_> = result.iter().map(|x| x.unwrap().to_string()).collect();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_interleave_views_multiple_buffers() {
        let str1 = "very_long_string_from_first_buffer".as_bytes();
        let str2 = "very_long_string_from_second_buffer".as_bytes();
        let buffer1 = str1.to_vec().into();
        let buffer2 = str2.to_vec().into();

        let view1 = ByteView::new(str1.len() as u32, &str1[..4])
            .with_buffer_index(0)
            .with_offset(0)
            .as_u128();
        let view2 = ByteView::new(str2.len() as u32, &str2[..4])
            .with_buffer_index(1)
            .with_offset(0)
            .as_u128();
        let view_a =
            StringViewArray::try_new(vec![view1, view2].into(), vec![buffer1, buffer2], None)
                .unwrap();

        let str3 = "another_very_long_string_buffer_three".as_bytes();
        let str4 = "different_long_string_in_buffer_four".as_bytes();
        let buffer3 = str3.to_vec().into();
        let buffer4 = str4.to_vec().into();

        let view3 = ByteView::new(str3.len() as u32, &str3[..4])
            .with_buffer_index(0)
            .with_offset(0)
            .as_u128();
        let view4 = ByteView::new(str4.len() as u32, &str4[..4])
            .with_buffer_index(1)
            .with_offset(0)
            .as_u128();
        let view_b =
            StringViewArray::try_new(vec![view3, view4].into(), vec![buffer3, buffer4], None)
                .unwrap();

        let indices = &[
            (0, 0), // String from first buffer of array A
            (1, 0), // String from first buffer of array B
            (0, 1), // String from second buffer of array A
            (1, 1), // String from second buffer of array B
            (0, 0), // String from first buffer of array A again
            (1, 1), // String from second buffer of array B again
        ];

        // Test interleave
        let values = interleave(&[&view_a, &view_b], indices).unwrap();
        let result = values.as_string_view();

        assert_eq!(
            result.data_buffers().len(),
            4,
            "Expected four buffers (two from each input array)"
        );

        let result_strings: Vec<_> = result.iter().map(|x| x.map(|s| s.to_string())).collect();
        assert_eq!(
            result_strings,
            vec![
                Some("very_long_string_from_first_buffer".to_string()),
                Some("another_very_long_string_buffer_three".to_string()),
                Some("very_long_string_from_second_buffer".to_string()),
                Some("different_long_string_in_buffer_four".to_string()),
                Some("very_long_string_from_first_buffer".to_string()),
                Some("different_long_string_in_buffer_four".to_string()),
            ]
        );

        let views = result.views();
        let buffer_indices: Vec<_> = views
            .iter()
            .map(|raw_view| ByteView::from(*raw_view).buffer_index)
            .collect();

        assert_eq!(
            buffer_indices,
            vec![
                0, // First buffer from array A
                1, // First buffer from array B
                2, // Second buffer from array A
                3, // Second buffer from array B
                0, // First buffer from array A (reused)
                3, // Second buffer from array B (reused)
            ]
        );
    }

    #[test]
    fn test_interleave_run_end_encoded_primitive() {
        let mut builder = PrimitiveRunBuilder::<Int32Type, Int32Type>::new();
        builder.extend([1, 1, 2, 2, 2, 3].into_iter().map(Some));
        let a = builder.finish();

        let mut builder = PrimitiveRunBuilder::<Int32Type, Int32Type>::new();
        builder.extend([4, 5, 5, 6, 6, 6].into_iter().map(Some));
        let b = builder.finish();

        let indices = &[(0, 1), (1, 0), (0, 4), (1, 2), (0, 5)];
        let result = interleave(&[&a, &b], indices).unwrap();

        // The result should be a RunEndEncoded array
        assert!(matches!(result.data_type(), DataType::RunEndEncoded(_, _)));

        // Cast to RunArray to access values
        let result_run_array: &Int32RunArray = result.as_any().downcast_ref().unwrap();

        // Verify the logical values by accessing the logical array directly
        let expected = vec![1, 4, 2, 5, 3];
        let mut actual = Vec::new();
        for i in 0..result_run_array.len() {
            let physical_idx = result_run_array.get_physical_index(i);
            let value = result_run_array
                .values()
                .as_primitive::<Int32Type>()
                .value(physical_idx);
            actual.push(value);
        }
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_interleave_run_end_encoded_sliced() {
        let mut builder = PrimitiveRunBuilder::<Int32Type, Int32Type>::new();
        builder.extend([1, 1, 2, 2, 2, 3].into_iter().map(Some));
        let a = builder.finish();
        let a = a.slice(2, 3); // [2, 2, 2]

        let mut builder = PrimitiveRunBuilder::<Int32Type, Int32Type>::new();
        builder.extend([4, 5, 5, 6, 6, 6].into_iter().map(Some));
        let b = builder.finish();
        let b = b.slice(1, 3); // [5, 5, 6]

        let indices = &[(0, 1), (1, 0), (0, 2), (1, 1), (1, 2)];
        let result = interleave(&[&a, &b], indices).unwrap();

        let result = result.as_run::<Int32Type>();
        let result = result.downcast::<Int32Array>().unwrap();

        let expected = vec![2, 5, 2, 5, 6];
        let actual = result.into_iter().flatten().collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_interleave_run_end_encoded_string() {
        let a: Int32RunArray = vec!["hello", "hello", "world", "world", "foo"]
            .into_iter()
            .collect();
        let b: Int32RunArray = vec!["bar", "baz", "baz", "qux"].into_iter().collect();

        let indices = &[(0, 0), (1, 1), (0, 3), (1, 3), (0, 4)];
        let result = interleave(&[&a, &b], indices).unwrap();

        // The result should be a RunEndEncoded array
        assert!(matches!(result.data_type(), DataType::RunEndEncoded(_, _)));

        // Cast to RunArray to access values
        let result_run_array: &Int32RunArray = result.as_any().downcast_ref().unwrap();

        // Verify the logical values by accessing the logical array directly
        let expected = vec!["hello", "baz", "world", "qux", "foo"];
        let mut actual = Vec::new();
        for i in 0..result_run_array.len() {
            let physical_idx = result_run_array.get_physical_index(i);
            let value = result_run_array
                .values()
                .as_string::<i32>()
                .value(physical_idx);
            actual.push(value);
        }
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_interleave_run_end_encoded_with_nulls() {
        let a: Int32RunArray = vec![Some("a"), Some("a"), None, None, Some("b")]
            .into_iter()
            .collect();
        let b: Int32RunArray = vec![None, Some("c"), Some("c"), Some("d")]
            .into_iter()
            .collect();

        let indices = &[(0, 1), (1, 0), (0, 2), (1, 3), (0, 4)];
        let result = interleave(&[&a, &b], indices).unwrap();

        // The result should be a RunEndEncoded array
        assert!(matches!(result.data_type(), DataType::RunEndEncoded(_, _)));

        // Cast to RunArray to access values
        let result_run_array: &Int32RunArray = result.as_any().downcast_ref().unwrap();

        // Verify the logical values by accessing the logical array directly
        let expected = vec![Some("a"), None, None, Some("d"), Some("b")];
        let mut actual = Vec::new();
        for i in 0..result_run_array.len() {
            let physical_idx = result_run_array.get_physical_index(i);
            if result_run_array.values().is_null(physical_idx) {
                actual.push(None);
            } else {
                let value = result_run_array
                    .values()
                    .as_string::<i32>()
                    .value(physical_idx);
                actual.push(Some(value));
            }
        }
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_interleave_run_end_encoded_different_run_types() {
        let mut builder = PrimitiveRunBuilder::<Int16Type, Int32Type>::new();
        builder.extend([1, 1, 2, 3, 3].into_iter().map(Some));
        let a = builder.finish();

        let mut builder = PrimitiveRunBuilder::<Int16Type, Int32Type>::new();
        builder.extend([4, 5, 5, 6].into_iter().map(Some));
        let b = builder.finish();

        let indices = &[(0, 0), (1, 1), (0, 3), (1, 3)];
        let result = interleave(&[&a, &b], indices).unwrap();

        // The result should be a RunEndEncoded array
        assert!(matches!(result.data_type(), DataType::RunEndEncoded(_, _)));

        // Cast to RunArray to access values
        let result_run_array: &RunArray<Int16Type> = result.as_any().downcast_ref().unwrap();

        // Verify the logical values by accessing the logical array directly
        let expected = vec![1, 5, 3, 6];
        let mut actual = Vec::new();
        for i in 0..result_run_array.len() {
            let physical_idx = result_run_array.get_physical_index(i);
            let value = result_run_array
                .values()
                .as_primitive::<Int32Type>()
                .value(physical_idx);
            actual.push(value);
        }
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_interleave_run_end_encoded_mixed_run_lengths() {
        let mut builder = PrimitiveRunBuilder::<Int64Type, Int32Type>::new();
        builder.extend([1, 2, 2, 2, 2, 3, 3, 4].into_iter().map(Some));
        let a = builder.finish();

        let mut builder = PrimitiveRunBuilder::<Int64Type, Int32Type>::new();
        builder.extend([5, 5, 5, 6, 7, 7, 8, 8].into_iter().map(Some));
        let b = builder.finish();

        let indices = &[
            (0, 0), // 1
            (1, 2), // 5
            (0, 3), // 2
            (1, 3), // 6
            (0, 6), // 3
            (1, 6), // 8
            (0, 7), // 4
            (1, 4), // 7
        ];
        let result = interleave(&[&a, &b], indices).unwrap();

        // The result should be a RunEndEncoded array
        assert!(matches!(result.data_type(), DataType::RunEndEncoded(_, _)));

        // Cast to RunArray to access values
        let result_run_array: &RunArray<Int64Type> = result.as_any().downcast_ref().unwrap();

        // Verify the logical values by accessing the logical array directly
        let expected = vec![1, 5, 2, 6, 3, 8, 4, 7];
        let mut actual = Vec::new();
        for i in 0..result_run_array.len() {
            let physical_idx = result_run_array.get_physical_index(i);
            let value = result_run_array
                .values()
                .as_primitive::<Int32Type>()
                .value(physical_idx);
            actual.push(value);
        }
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_interleave_run_end_encoded_empty_runs() {
        let mut builder = PrimitiveRunBuilder::<Int32Type, Int32Type>::new();
        builder.extend(std::iter::once(Some(1)));
        let a = builder.finish();

        let mut builder = PrimitiveRunBuilder::<Int32Type, Int32Type>::new();
        builder.extend([2, 2, 2].into_iter().map(Some));
        let b = builder.finish();

        let indices = &[(0, 0), (1, 1), (1, 2)];
        let result = interleave(&[&a, &b], indices).unwrap();

        // The result should be a RunEndEncoded array
        assert!(matches!(result.data_type(), DataType::RunEndEncoded(_, _)));

        // Cast to RunArray to access values
        let result_run_array: &Int32RunArray = result.as_any().downcast_ref().unwrap();

        // Verify the logical values by accessing the logical array directly
        let expected = vec![1, 2, 2];
        let mut actual = Vec::new();
        for i in 0..result_run_array.len() {
            let physical_idx = result_run_array.get_physical_index(i);
            let value = result_run_array
                .values()
                .as_primitive::<Int32Type>()
                .value(physical_idx);
            actual.push(value);
        }
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_struct_no_fields() {
        let fields = Fields::empty();
        let a = StructArray::try_new_with_length(fields.clone(), vec![], None, 10).unwrap();
        let v = interleave(&[&a], &[(0, 0)]).unwrap();
        assert_eq!(v.len(), 1);
        assert_eq!(v.data_type(), &DataType::Struct(fields));
    }

    #[test]
    fn test_interleave_fallback_dictionary_with_nulls() {
        let input_1_keys = Int32Array::from_iter([Some(0), None, Some(1)]);
        let input_1_values = StringArray::from_iter_values(["foo", "bar"]);
        let dict_a = DictionaryArray::new(input_1_keys, Arc::new(input_1_values));

        let input_2_keys = Int32Array::from_iter([Some(0), Some(1), None]);
        let input_2_values = StringArray::from_iter_values(["baz", "qux"]);
        let dict_b = DictionaryArray::new(input_2_keys, Arc::new(input_2_values));

        let indices = vec![
            (0, 0), // "foo"
            (0, 1), // null
            (1, 0), // "baz"
            (1, 2), // null
            (0, 2), // "bar"
            (1, 1), // "qux"
        ];

        let result =
            interleave_fallback_dictionary::<Int32Type>(&[&dict_a, &dict_b], &indices).unwrap();
        let dict_result = result.as_dictionary::<Int32Type>();

        let string_result = dict_result.downcast_dict::<StringArray>().unwrap();
        let collected: Vec<_> = string_result.into_iter().collect();
        assert_eq!(
            collected,
            vec![
                Some("foo"),
                None,
                Some("baz"),
                None,
                Some("bar"),
                Some("qux")
            ]
        );
    }

    #[test]
    fn test_interleave_string_view_dictionary_overflow_returns_err() {
        // interleaving dictionaries which results in overflowing the key type should
        // surface an error not a panic
        let values_a: StringViewArray = (0..200).map(|i| Some(format!("a{i}"))).collect();
        let keys_a = UInt8Array::from_iter_values(0..200);
        let dict_a = DictionaryArray::<UInt8Type>::new(keys_a, Arc::new(values_a));

        let values_b: StringViewArray = (0..200).map(|i| Some(format!("b{i}"))).collect();
        let keys_b = UInt8Array::from_iter_values(0..200);
        let dict_b = DictionaryArray::<UInt8Type>::new(keys_b, Arc::new(values_b));

        let indices: Vec<_> = (0..200).flat_map(|i| [(0, i), (1, i)]).collect();

        let err = interleave(&[&dict_a, &dict_b], &indices).unwrap_err();
        assert!(matches!(err, ArrowError::DictionaryKeyOverflowError));
    }

    #[test]
    fn test_interleave_nested_dictionary_overflow_returns_err() {
        // same as above, but with the dictionary nested inside a FixedSizeList
        let field = Arc::new(arrow_schema::Field::new(
            "item",
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8View)),
            false,
        ));

        let values_a: StringViewArray = (0..200).map(|i| Some(format!("a{i}"))).collect();
        let keys_a = UInt8Array::from_iter_values(0..200);
        let dict_a = DictionaryArray::<UInt8Type>::new(keys_a, Arc::new(values_a));
        let list_a = FixedSizeListArray::new(field.clone(), 1, Arc::new(dict_a), None);

        let values_b: StringViewArray = (0..200).map(|i| Some(format!("b{i}"))).collect();
        let keys_b = UInt8Array::from_iter_values(0..200);
        let dict_b = DictionaryArray::<UInt8Type>::new(keys_b, Arc::new(values_b));
        let list_b = FixedSizeListArray::new(field, 1, Arc::new(dict_b), None);

        let indices: Vec<_> = (0..200).flat_map(|i| [(0, i), (1, i)]).collect();

        let err = interleave(&[&list_a, &list_b], &indices).unwrap_err();
        assert!(matches!(err, ArrowError::DictionaryKeyOverflowError));
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
    fn test_interleave_bytes_offset_overflow() {
        let indices: Vec<(usize, usize)> = vec![(0, 0); (i32::MAX >> 4) as usize];
        let text = ('a'..='z').collect::<String>();
        let values = StringArray::from(vec![Some(text)]);
        assert!(matches!(
            interleave(&[&values], &indices),
            Err(ArrowError::OffsetOverflowError(_))
        ));
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
    fn test_interleave_list_offset_overflow() {
        // Build a ListArray<i32> with a single row containing many elements
        let mut builder = GenericListBuilder::<i32, _>::new(Int32Builder::new());
        for i in 0..32 {
            builder.values().append_value(i);
        }
        builder.append(true);
        let list = builder.finish();

        // Interleave enough copies to overflow i32 offsets
        let indices: Vec<(usize, usize)> = vec![(0, 0); (i32::MAX as usize / 32) + 1];
        assert!(matches!(
            interleave(&[&list], &indices),
            Err(ArrowError::OffsetOverflowError(_))
        ));
    }

    #[test]
    fn test_interleave_list_view() {
        // `interleave` for ListView falls through to `interleave_fallback`, which uses
        // `MutableArrayData`. `list_view::build_extend` copies offsets/sizes but never
        // extends the child array, so the result contains offsets/sizes that reference
        // positions in the now-absent original child arrays while the child is empty.
        //
        // lv_a: [[1, 2], [3]]   (values=[1,2,3], offsets=[0,2], sizes=[2,1])
        // lv_b: [[4, 5, 6]]     (values=[4,5,6], offsets=[0],   sizes=[3])
        // interleave at [(0,0), (1,0), (0,1)] should produce [[1, 2], [4, 5, 6], [3]]
        let field = Arc::new(Field::new_list_field(DataType::Int64, false));

        let lv_a = ListViewArray::new(
            Arc::clone(&field),
            ScalarBuffer::from(vec![0i32, 2]),
            ScalarBuffer::from(vec![2i32, 1]),
            Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
            None,
        );
        let lv_b = ListViewArray::new(
            field,
            ScalarBuffer::from(vec![0i32]),
            ScalarBuffer::from(vec![3i32]),
            Arc::new(Int64Array::from(vec![4_i64, 5, 6])),
            None,
        );

        let result = interleave(
            &[&lv_a as &dyn Array, &lv_b as &dyn Array],
            &[(0, 0), (1, 0), (0, 1)],
        )
        .unwrap();

        result
            .to_data()
            .validate_full()
            .expect("interleaved ListViewArray must be internally consistent");

        let result_lv = result.as_list_view::<i32>();
        assert_eq!(result_lv.len(), 3);
        assert_eq!(
            result_lv.value(0).as_primitive::<Int64Type>().values(),
            &[1, 2]
        );
        assert_eq!(
            result_lv.value(1).as_primitive::<Int64Type>().values(),
            &[4, 5, 6]
        );
        assert_eq!(
            result_lv.value(2).as_primitive::<Int64Type>().values(),
            &[3]
        );
    }

    #[test]
    fn test_interleave_fixed_size_list() {
        // a: [[1, 2], [3, 4], [5, 6]]
        let field = Arc::new(Field::new("item", DataType::Int32, false));
        let a = FixedSizeListArray::new(
            field.clone(),
            2,
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
            None,
        );
        // b: [[7, 8], [9, 10]]
        let b = FixedSizeListArray::new(
            field.clone(),
            2,
            Arc::new(Int32Array::from(vec![7, 8, 9, 10])),
            None,
        );

        let result = interleave(&[&a, &b], &[(0, 2), (1, 0), (0, 0), (1, 1), (0, 1)]).unwrap();
        let result = result.as_fixed_size_list();
        assert_eq!(result.len(), 5);
        assert_eq!(result.value_length(), 2);

        let values = result.values().as_primitive::<Int32Type>();
        // [[5,6], [7,8], [1,2], [9,10], [3,4]]
        assert_eq!(values.values(), &[5, 6, 7, 8, 1, 2, 9, 10, 3, 4]);
    }

    #[test]
    fn test_interleave_zero_sized_fixed_size_list() {
        let input = FixedSizeListArray::try_new_with_length(
            Field::new_list_field(DataType::Int32, true).into(),
            0,
            Arc::new(Int32Array::new_null(0)),
            None,
            3,
        )
        .unwrap();

        let indices = [(0, 2), (0, 0)];
        let result = interleave(&[&input], &indices).unwrap();

        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_interleave_fixed_size_list_with_nulls() {
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        // a: [[1, 2], null, [5, 6]]
        let a = FixedSizeListArray::new(
            field.clone(),
            2,
            Arc::new(Int32Array::from(vec![1, 2, 0, 0, 5, 6])),
            Some(NullBuffer::from(&[true, false, true])),
        );
        // b: [null, [9, 10]]
        let b = FixedSizeListArray::new(
            field.clone(),
            2,
            Arc::new(Int32Array::from(vec![0, 0, 9, 10])),
            Some(NullBuffer::from(&[false, true])),
        );

        let result = interleave(&[&a, &b], &[(0, 0), (0, 1), (1, 0), (1, 1), (0, 2)]).unwrap();
        let result = result.as_fixed_size_list();
        assert_eq!(result.len(), 5);

        let validity: Vec<bool> = result.nulls().unwrap().iter().collect();
        assert_eq!(validity, &[true, false, false, true, true]);
    }

    fn run_fsl_string_child_test(
        child_data_type: DataType,
        create_child: impl Fn(Vec<&str>) -> ArrayRef,
        extract_values: impl Fn(&ArrayRef) -> Vec<String>,
    ) {
        let field = Arc::new(Field::new("item", child_data_type, false));

        // a: [["a", "b"], ["c", "d"]]
        let a = FixedSizeListArray::new(
            field.clone(),
            2,
            create_child(vec!["a", "b", "c", "d"]),
            None,
        );
        // b: [["x", "y"], ["z", "w"]]
        let b = FixedSizeListArray::new(
            field.clone(),
            2,
            create_child(vec!["x", "y", "z", "w"]),
            None,
        );

        let result = interleave(&[&a, &b], &[(0, 1), (1, 0), (0, 0)]).unwrap();
        let result = result.as_fixed_size_list();
        assert_eq!(result.len(), 3);
        assert_eq!(result.value_length(), 2);

        // Expected: [[c,d], [x,y], [a,b]]
        let values = extract_values(result.values());
        assert_eq!(values, vec!["c", "d", "x", "y", "a", "b"]);
    }

    #[test]
    fn test_interleave_fixed_size_list_string_child() {
        // FixedSizeList<Utf8> — exercises the non-primitive child path
        run_fsl_string_child_test(
            DataType::Utf8,
            |v| Arc::new(StringArray::from(v)),
            |a| {
                a.as_string::<i32>()
                    .iter()
                    .map(|s| s.unwrap().to_owned())
                    .collect()
            },
        );
    }

    #[test]
    fn test_interleave_fixed_size_list_string_view_child() {
        // FixedSizeList<Utf8View> — exercises the non-primitive child path
        run_fsl_string_child_test(
            DataType::Utf8View,
            |v| Arc::new(StringViewArray::from(v)),
            |a| {
                a.as_string_view()
                    .iter()
                    .map(|s| s.unwrap().to_owned())
                    .collect()
            },
        );
    }

    #[test]
    fn test_interleave_map() {
        use arrow_array::builder::MapBuilder;
        use arrow_array::builder::StringBuilder;

        // a: [{k1: 1, k2: 2}, {k3: 3}]
        let mut a_builder = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        a_builder.keys().append_value("k1");
        a_builder.values().append_value(1);
        a_builder.keys().append_value("k2");
        a_builder.values().append_value(2);
        a_builder.append(true).unwrap();
        a_builder.keys().append_value("k3");
        a_builder.values().append_value(3);
        a_builder.append(true).unwrap();
        let a = a_builder.finish();

        // b: [{k4: 4}, {k5: 5, k6: 6, k7: 7}]
        let mut b_builder = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        b_builder.keys().append_value("k4");
        b_builder.values().append_value(4);
        b_builder.append(true).unwrap();
        b_builder.keys().append_value("k5");
        b_builder.values().append_value(5);
        b_builder.keys().append_value("k6");
        b_builder.values().append_value(6);
        b_builder.keys().append_value("k7");
        b_builder.values().append_value(7);
        b_builder.append(true).unwrap();
        let b = b_builder.finish();

        let result = interleave(&[&a, &b], &[(1, 0), (0, 0), (0, 1), (1, 1)]).unwrap();
        let result = result.as_map();
        assert_eq!(result.len(), 4);

        // Row 0: {k4: 4}
        let row0 = result.value(0);
        assert_eq!(row0.len(), 1);
        assert_eq!(row0.column(0).as_string::<i32>().value(0), "k4");
        assert_eq!(row0.column(1).as_primitive::<Int32Type>().value(0), 4);

        // Row 1: {k1: 1, k2: 2}
        let row1 = result.value(1);
        assert_eq!(row1.len(), 2);

        // Row 2: {k3: 3}
        let row2 = result.value(2);
        assert_eq!(row2.len(), 1);
        assert_eq!(row2.column(0).as_string::<i32>().value(0), "k3");

        // Row 3: {k5: 5, k6: 6, k7: 7}
        let row3 = result.value(3);
        assert_eq!(row3.len(), 3);
    }

    #[test]
    fn test_interleave_map_with_nulls() {
        use arrow_array::builder::MapBuilder;
        use arrow_array::builder::StringBuilder;

        // a: [{k1: 1}, null]
        let mut a_builder = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        a_builder.keys().append_value("k1");
        a_builder.values().append_value(1);
        a_builder.append(true).unwrap();
        a_builder.append(false).unwrap();
        let a = a_builder.finish();

        // b: [null, {k2: 2}]
        let mut b_builder = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        b_builder.append(false).unwrap();
        b_builder.keys().append_value("k2");
        b_builder.values().append_value(2);
        b_builder.append(true).unwrap();
        let b = b_builder.finish();

        let result = interleave(&[&a, &b], &[(0, 0), (1, 0), (0, 1), (1, 1)]).unwrap();
        let result = result.as_map();
        assert_eq!(result.len(), 4);

        let validity: Vec<bool> = result.nulls().unwrap().iter().collect();
        assert_eq!(validity, &[true, false, false, true]);

        assert_eq!(result.value(0).len(), 1);
        assert_eq!(result.value(3).len(), 1);
    }
}
