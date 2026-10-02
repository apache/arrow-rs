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
use arrow_data::transform::MutableArrayData;
use arrow_data::{ByteView, MAX_INLINE_VIEW_LEN};
use arrow_schema::{ArrowError, DataType, FieldRef, Fields};
use std::collections::{BTreeMap, HashMap};
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
///
/// To copy selected byte view values into owned buffers, see
/// [`Interleaver::with_compact_byte_views`].
pub fn interleave(
    values: &[&dyn Array],
    indices: &[(usize, usize)],
) -> Result<ArrayRef, ArrowError> {
    Interleaver::default().interleave(values, indices)
}

/// Configurable interleaving of elements from multiple arrays.
///
/// The default configuration has the same behavior as [`interleave`].
#[derive(Debug, Default, Clone)]
pub struct Interleaver {
    compact_byte_views: bool,
    preserve_byte_view_sharing: bool,
}

impl Interleaver {
    /// Creates an interleaver with the same behavior as [`interleave`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Copies selected top-level [`StringViewArray`] and [`BinaryViewArray`]
    /// values into independent buffers when enabled.
    ///
    /// Only selected, non-null, non-inline values need payload storage. No input
    /// buffers are retained. Repeated values are copied for each selected row
    /// unless [`Self::with_preserve_byte_view_sharing`] is enabled.
    /// Other array types, including byte views nested inside other arrays, keep
    /// the behavior of [`interleave`]. This option is disabled by default.
    ///
    /// This is useful when selected rows need independent lifetimes, such as
    /// partitions of a spilling operator. Unlike [`interleave`] followed by
    /// [`GenericByteViewArray::gc`], it avoids an intermediate shared array and
    /// source-buffer remapping tables.
    /// Copying can increase total live memory while inputs remain retained.
    ///
    /// ```
    /// use arrow_array::{Array, StringViewArray};
    /// use arrow_select::interleave::Interleaver;
    ///
    /// let a = StringViewArray::from(vec![Some("a long selected value"), None]);
    /// let b = StringViewArray::from(vec!["another selected value"]);
    /// let result = Interleaver::new()
    ///     .with_compact_byte_views(true)
    ///     .interleave(&[&a, &b], &[(1, 0), (0, 1), (0, 0)])?;
    /// assert_eq!(result.as_ref(), &StringViewArray::from(vec![
    ///     Some("another selected value"), None, Some("a long selected value")
    /// ]) as &dyn Array);
    /// # Ok::<(), arrow_schema::ArrowError>(())
    /// ```
    pub fn with_compact_byte_views(mut self, compact: bool) -> Self {
        self.compact_byte_views = compact;
        self
    }

    /// Preserves existing source-range sharing when compacting byte views.
    ///
    /// Repeated references to the same source byte range share one copy in the
    /// output, including references through different input arrays. Equal values
    /// stored at different addresses are not deduplicated. The output still
    /// retains no input buffers.
    ///
    /// This option only affects [`Self::with_compact_byte_views`]. It is disabled
    /// by default: tracking ranges adds a lookup per non-inline value and
    /// temporary memory proportional to the number of distinct selected ranges.
    /// Enable it for selections with shared long values to avoid copying their
    /// payload repeatedly, for example after a join or dictionary decoding.
    pub fn with_preserve_byte_view_sharing(mut self, preserve: bool) -> Self {
        self.preserve_byte_view_sharing = preserve;
        self
    }

    /// Selects elements using `(array index, row index)` pairs, as in [`interleave`].
    ///
    /// # Errors
    ///
    /// Returns an error for empty inputs, differing input types, or unsupported
    /// sizes. Compact byte views support values and output buffer indices up to
    /// [`i32::MAX`], splitting larger total payloads across multiple buffers.
    ///
    /// # Panics
    ///
    /// Panics if an array or row index is out of bounds.
    pub fn interleave(
        &self,
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

        if self.compact_byte_views {
            match data_type {
                DataType::Utf8View => {
                    return self.interleave_views_compact::<StringViewType>(
                        values,
                        indices,
                        i32::MAX as usize,
                    );
                }
                DataType::BinaryView => {
                    return self.interleave_views_compact::<BinaryViewType>(
                        values,
                        indices,
                        i32::MAX as usize,
                    );
                }
                _ => {}
            }
        }

        downcast_primitive! {
            data_type => (primitive_helper, values, indices, data_type),
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

    fn interleave_views_compact<T: ByteViewType>(
        &self,
        values: &[&dyn Array],
        indices: &[(usize, usize)],
        max_buffer_size: usize,
    ) -> Result<ArrayRef, ArrowError> {
        if self.preserve_byte_view_sharing {
            interleave_views_compact::<T, true>(values, indices, max_buffer_size)
        } else {
            interleave_views_compact::<T, false>(values, indices, max_buffer_size)
        }
    }
}

fn interleave_views_compact<T: ByteViewType, const PRESERVE_SHARING: bool>(
    values: &[&dyn Array],
    indices: &[(usize, usize)],
    max_buffer_size: usize,
) -> Result<ArrayRef, ArrowError> {
    let interleaved = Interleave::<GenericByteViewArray<T>>::new(values, indices);
    let arrays = &interleaved.arrays;
    let mut views = Vec::with_capacity(indices.len());
    let mut block_sizes: Vec<usize> = Vec::new();
    let mut current_size = 0;
    let mut copied =
        HashMap::<(usize, u32), usize, _>::with_hasher(ahash::RandomState::with_seeds(0, 0, 0, 0));
    for &(source, row) in indices {
        let array = arrays[source];
        let raw = array.views()[row];
        if interleaved
            .nulls
            .as_ref()
            .is_some_and(|nulls| nulls.is_null(views.len()))
        {
            views.push(0);
            continue;
        }
        let mut view = ByteView::from(raw);
        if view.length <= MAX_INLINE_VIEW_LEN {
            views.push(raw);
            continue;
        }
        let len = view.length as usize;
        if len > i32::MAX as usize {
            return Err(ArrowError::OffsetOverflowError(len));
        }
        if PRESERVE_SHARING {
            let buffer = &array.data_buffers()[view.buffer_index as usize];
            // Effective addresses identify aliases through differently sliced buffers.
            let address = buffer.as_ptr().wrapping_add(view.offset as usize) as usize;
            match copied.entry((address, view.length)) {
                std::collections::hash_map::Entry::Occupied(entry) => {
                    views.push(views[*entry.get()]);
                    continue;
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(views.len());
                }
            }
        }
        if current_size != 0 && current_size + len > max_buffer_size {
            let next_buffer_index = block_sizes.len() + 1;
            if next_buffer_index > i32::MAX as usize {
                return Err(ArrowError::OffsetOverflowError(next_buffer_index));
            }
            block_sizes.push(current_size);
            current_size = 0;
        }
        if PRESERVE_SHARING {
            view.buffer_index = block_sizes.len() as u32;
            view.offset = current_size as u32;
        }
        current_size += len;
        views.push(view.as_u128());
    }
    drop(copied);
    if current_size != 0 {
        block_sizes.push(current_size);
    }

    let mut buffers: Vec<Vec<u8>> = block_sizes
        .iter()
        .map(|&size| Vec::with_capacity(size))
        .collect();
    if !PRESERVE_SHARING && buffers.len() == 1 {
        let buffer = &mut buffers[0];
        for (raw, &(source, _)) in views.iter_mut().zip(indices) {
            let view = ByteView::from(*raw);
            if view.length <= MAX_INLINE_VIEW_LEN {
                continue;
            }
            *raw = view
                .with_buffer_index(0)
                .with_offset(buffer.len() as u32)
                .as_u128();
            // SAFETY: the first pass checked every source index and retained
            // this non-null source view, whose payload is valid for its array.
            unsafe {
                copy_view_payload(arrays.get_unchecked(source), view, buffer);
            }
        }
    } else {
        let mut current_buffer = 0;
        for (raw, &(source, row)) in views.iter_mut().zip(indices) {
            let mut view = ByteView::from(*raw);
            if view.length <= MAX_INLINE_VIEW_LEN {
                continue;
            }
            // SAFETY: the first pass checked every source index.
            let array = unsafe { arrays.get_unchecked(source) };
            let buffer = if PRESERVE_SHARING {
                let buffer = &mut buffers[view.buffer_index as usize];
                // First occurrences fill consecutive offsets. Repeated ranges point
                // behind the write cursor and have already been copied.
                if view.offset as usize != buffer.len() {
                    continue;
                }
                view = ByteView::from(array.views()[row]);
                buffer
            } else {
                // Keep source views until this pass, avoiding a second random read
                // of the input views. Rewrite the final views in place as we copy.
                if buffers[current_buffer].len() == block_sizes[current_buffer] {
                    current_buffer += 1;
                }
                let buffer = &mut buffers[current_buffer];
                *raw = view
                    .with_buffer_index(current_buffer as u32)
                    .with_offset(buffer.len() as u32)
                    .as_u128();
                buffer
            };
            // SAFETY: view is a non-null source view from array, either retained
            // from the first pass or read above, so its payload range is valid.
            unsafe {
                copy_view_payload(array, view, buffer);
            }
        }
    }
    let buffers: Vec<_> = buffers.into_iter().map(Buffer::from_vec).collect();
    // SAFETY: inline views are unchanged, null views are zero, and every other
    // view addresses its copied source range with the original length and prefix.
    Ok(Arc::new(unsafe {
        GenericByteViewArray::<T>::new_unchecked(views.into(), buffers.into(), interleaved.nulls)
    }))
}

/// Copies the payload addressed by a non-inline source view.
///
/// # Safety
///
/// The view must reference a valid payload range in `array`.
#[inline]
unsafe fn copy_view_payload<T: ByteViewType>(
    array: &GenericByteViewArray<T>,
    view: ByteView,
    output: &mut Vec<u8>,
) {
    let start = view.offset as usize;
    // SAFETY: the caller guarantees that the buffer index and payload range
    // address valid bytes in array.
    let bytes = unsafe {
        array
            .data_buffers()
            .get_unchecked(view.buffer_index as usize)
            .get_unchecked(start..start + view.length as usize)
    };
    output.extend_from_slice(bytes);
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

    let has_child_nulls = child_arrays.iter().any(|a| a.null_count() > 0);

    // Build values buffer by copying contiguous slices
    let mut values: Vec<T::Native> = Vec::with_capacity(capacity);
    for &(array, row) in indices {
        let range = interleaved.arrays[array].element_range(row);
        if !range.is_empty() {
            values.extend_from_slice(&child_arrays[array].values()[range]);
        }
    }

    // Build null buffer. Pre-allocate with 0x00 (all null), then:
    // - Sources with nulls: set_bits copies the source validity bits into the destination range.
    // - Sources without nulls: set the bit range to all 1s directly.
    let nulls = if has_child_nulls {
        let null_byte_len = bit_util::ceil(capacity, 8);
        let mut output_null_buf = MutableBuffer::from_len_zeroed(null_byte_len);

        let mut offset_write = 0;
        let mut output_null_count = 0usize;
        for &(array, row) in indices {
            let range = interleaved.arrays[array].element_range(row);
            let len = range.len();
            if len > 0 {
                match child_arrays[array].nulls() {
                    Some(null_buffer) => {
                        output_null_count += set_bits(
                            output_null_buf.as_slice_mut(),
                            null_buffer.validity(),
                            offset_write,
                            null_buffer.offset() + range.start,
                            len,
                        );
                    }
                    None => {
                        // For a non-nullable source, set the bit range to all 1s directly.
                        let buf = output_null_buf.as_slice_mut();
                        (offset_write..offset_write + len).for_each(|i| bit_util::set_bit(buf, i));
                    }
                }
            }
            offset_write += len;
        }

        if output_null_count > 0 {
            let bool_buf = BooleanBuffer::new(output_null_buf.into(), 0, capacity);
            // SAFETY: null_count is accumulated from set_bits which correctly counts unset bits
            Some(unsafe { NullBuffer::new_unchecked(bool_buf, output_null_count) })
        } else {
            None
        }
    } else {
        None
    };

    Arc::new(PrimitiveArray::<T>::new(values.into(), nulls).with_data_type(data_type.clone()))
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
        BinaryViewBuilder, GenericListBuilder, Int32Builder, PrimitiveBuilder, PrimitiveRunBuilder,
        StringViewBuilder,
    };
    use arrow_array::types::{Decimal128Type, Int8Type, TimestampMicrosecondType};
    use arrow_buffer::ScalarBuffer;
    use arrow_schema::{Field, TimeUnit};

    #[test]
    fn test_compact_sliced_multibuffer_sources() {
        let mut first = StringViewBuilder::new().with_fixed_block_size(32);
        first.append_value("discard before slice");
        first.append_value("first selected long string");
        first.append_null();
        first.append_value("second selected long string");
        first.append_value("discard after slice");
        let first = first.finish().slice(1, 3);

        let mut second = StringViewBuilder::new().with_fixed_block_size(32);
        second.append_value("discard before slice");
        second.append_value("third selected long string");
        second.append_value("short");
        second.append_value("fourth selected long string");
        let second = second.finish().slice(1, 3);
        assert!(first.data_buffers().len() > 1);
        assert!(second.data_buffers().len() > 1);

        let indices = [(1, 2), (0, 1), (0, 2), (1, 1), (0, 0), (1, 2)];
        let ordinary = interleave(&[&first, &second], &indices).unwrap();
        for preserve in [false, true] {
            let compact = Interleaver::new()
                .with_compact_byte_views(true)
                .with_preserve_byte_view_sharing(preserve)
                .interleave(&[&first, &second], &indices)
                .unwrap();
            assert_eq!(compact.as_ref(), ordinary.as_ref());
            let compact = compact.as_string_view();
            assert_eq!(
                compact.iter().collect::<Vec<_>>(),
                vec![
                    Some("fourth selected long string"),
                    None,
                    Some("second selected long string"),
                    Some("short"),
                    Some("first selected long string"),
                    Some("fourth selected long string"),
                ]
            );
            assert_eq!(compact.views()[0] == compact.views()[5], preserve);
        }
    }

    #[test]
    fn test_compact_binary_inline_boundary() {
        let inline = [0xff; 12];
        let external = [0xfe; 13];
        let mut input = BinaryViewBuilder::new();
        input.append_value(inline);
        input.append_value(external);
        input.append_null();
        input.append_value([]);
        let input = input.finish();

        let indices = [(0, 1), (0, 0), (0, 2), (0, 3), (0, 1)];
        for preserve in [false, true] {
            let compact = Interleaver::new()
                .with_compact_byte_views(true)
                .with_preserve_byte_view_sharing(preserve)
                .interleave(&[&input], &indices)
                .unwrap();
            assert_eq!(
                compact.as_ref(),
                interleave(&[&input], &indices).unwrap().as_ref()
            );
            let compact = compact.as_binary_view();
            assert_eq!(compact.value(0), &external);
            assert_eq!(compact.value(1), &inline);
            assert!(compact.is_null(2));
            assert_eq!(compact.value(3), b"");
            assert_eq!(compact.data_buffers().len(), 1);
            let copies = if preserve { 1 } else { 2 };
            assert_eq!(compact.data_buffers()[0].len(), copies * external.len());
            assert_eq!(compact.views()[0] == compact.views()[4], preserve);
        }
    }

    #[test]
    fn test_compact_skips_hidden_null_payload_and_owns_buffers() {
        for preserve in [false, true] {
            let unselected = "u".repeat(32 * 1024);
            let hidden = "n".repeat(32 * 1024);
            let selected = "selected long string";
            let input = StringViewArray::from(vec![unselected.as_str(), selected, hidden.as_str()]);
            let input = StringViewArray::try_new(
                input.views().clone(),
                input.data_buffers().clone(),
                Some(NullBuffer::from(vec![true, true, false])),
            )
            .unwrap();
            let compact = Interleaver::new()
                .with_compact_byte_views(true)
                .with_preserve_byte_view_sharing(preserve)
                .interleave(&[&input], &[(0, 1), (0, 2), (0, 1)])
                .unwrap();
            let compact = compact.as_string_view();
            assert_eq!(compact.data_buffers().len(), 1);
            let copies = if preserve { 1 } else { 2 };
            assert_eq!(compact.data_buffers()[0].len(), copies * selected.len());
            assert_eq!(
                compact.data_buffers()[0].capacity(),
                copies * selected.len()
            );
            assert_eq!(compact.views()[1], 0);
            for output in compact.data_buffers().iter() {
                for source in input.data_buffers().iter() {
                    assert_ne!(
                        output.as_ptr() as usize - output.ptr_offset(),
                        source.as_ptr() as usize - source.ptr_offset()
                    );
                }
            }
            assert_ne!(compact.views().as_ptr(), input.views().as_ptr());
            assert_ne!(
                compact.nulls().unwrap().buffer().as_ptr(),
                input.nulls().unwrap().buffer().as_ptr()
            );
            drop(input);
            assert_eq!(
                compact.iter().collect::<Vec<_>>(),
                vec![Some(selected), None, Some(selected)]
            );
        }
    }

    #[test]
    fn test_compact_repeated_range_copies_payload_once() {
        let value = "x".repeat(1024);
        let input = StringViewArray::from(vec![value.as_str()]);
        let compact = Interleaver::new()
            .with_compact_byte_views(true)
            .with_preserve_byte_view_sharing(true)
            .interleave(&[&input], &vec![(0, 0); 10_000])
            .unwrap();
        let compact = compact.as_string_view();
        assert_eq!(compact.len(), 10_000);
        assert_eq!(compact.data_buffers().len(), 1);
        assert_eq!(compact.data_buffers()[0].len(), 1024);
        assert_eq!(compact.data_buffers()[0].capacity(), 1024);
        assert_ne!(
            compact.data_buffers()[0].as_ptr(),
            input.data_buffers()[0].as_ptr()
        );
        drop(input);
        assert!(compact.iter().all(|actual| actual == Some(value.as_str())));
        assert!(
            compact
                .views()
                .iter()
                .all(|view| *view == compact.views()[0])
        );
        compact.to_data().validate_full().unwrap();
    }

    #[test]
    fn test_compact_sharing_is_opt_in() {
        let input = StringViewArray::from(vec!["long repeated payload"]);
        for interleaver in [
            Interleaver::new().with_compact_byte_views(true),
            Interleaver::new()
                .with_compact_byte_views(true)
                .with_preserve_byte_view_sharing(true)
                .with_preserve_byte_view_sharing(false),
        ] {
            let output = interleaver
                .interleave(&[&input], &[(0, 0), (0, 0), (0, 0)])
                .unwrap();
            let output = output.as_string_view();
            assert_eq!(output.data_buffers()[0].len(), 3 * input.value(0).len());
            assert_eq!(
                output.data_buffers()[0].capacity(),
                3 * input.value(0).len()
            );
            assert_ne!(output.views()[0], output.views()[1]);
            assert_ne!(output.views()[1], output.views()[2]);
            assert!(output.iter().all(|value| value == Some(input.value(0))));
        }
    }

    #[test]
    fn test_compact_aliases_across_sliced_buffers() {
        fn view(buffer: Buffer, offset: u32, len: u32) -> StringViewArray {
            let mut builder = StringViewBuilder::new();
            let block = builder.append_block(buffer);
            builder.try_append_view(block, offset, len).unwrap();
            builder.finish()
        }

        let backing = Buffer::from_vec(
            b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".to_vec(),
        );
        let a = view(backing.clone(), 7, 20);
        let b = view(backing.slice(7), 0, 20);
        let c = view(backing.slice(7), 0, 21);
        let d = view(Buffer::from_vec(backing[7..27].to_vec()), 0, 20);
        let overlap = view(backing.slice(8), 0, 20);
        let compact = Interleaver::new()
            .with_compact_byte_views(true)
            .with_preserve_byte_view_sharing(true)
            .interleave(
                &[&a, &b, &c, &d, &overlap],
                &[(0, 0), (1, 0), (2, 0), (3, 0), (4, 0)],
            )
            .unwrap();
        let compact = compact.as_string_view();
        assert_eq!(compact.views()[0], compact.views()[1]);
        assert_ne!(compact.views()[0], compact.views()[2]);
        assert_ne!(compact.views()[0], compact.views()[3]);
        assert_ne!(compact.views()[0], compact.views()[4]);
        assert_eq!(compact.value(0), compact.value(3));
        assert_eq!(compact.data_buffers()[0].len(), 20 + 21 + 20 + 20);
        assert_eq!(compact.data_buffers()[0].capacity(), 20 + 21 + 20 + 20);
        assert_ne!(compact.data_buffers()[0].as_ptr(), backing.as_ptr());
        assert_eq!(
            compact.iter().collect::<Vec<_>>(),
            vec![
                Some(a.value(0)),
                Some(b.value(0)),
                Some(c.value(0)),
                Some(d.value(0)),
                Some(overlap.value(0))
            ]
        );
        compact.to_data().validate_full().unwrap();
    }

    #[test]
    fn test_compact_splits_buffers_and_reuses_earlier_ranges() {
        let first = "a".repeat(20);
        let second = "b".repeat(13);
        let third = "c".repeat(18);
        let input = StringViewArray::from(vec![
            Some(first.as_str()),
            Some(second.as_str()),
            None,
            Some(third.as_str()),
            Some("inline"),
        ]);
        let indices = [(0, 0), (0, 1), (0, 3), (0, 0), (0, 2), (0, 4), (0, 1)];
        for preserve in [false, true] {
            let compact = if preserve {
                interleave_views_compact::<StringViewType, true>(&[&input], &indices, 32)
            } else {
                interleave_views_compact::<StringViewType, false>(&[&input], &indices, 32)
            }
            .unwrap();
            assert_eq!(
                compact.as_ref(),
                interleave(&[&input], &indices).unwrap().as_ref()
            );
            let compact = compact.as_string_view();
            assert_eq!(compact.data_buffers().len(), if preserve { 2 } else { 4 });
            assert_eq!(compact.data_buffers()[0].len(), 20);
            assert_eq!(compact.data_buffers()[1].len(), 31);
            for buffer in compact.data_buffers().iter() {
                assert!(buffer.len() <= 32);
                assert_eq!(buffer.len(), buffer.capacity());
            }
            assert_eq!(ByteView::from(compact.views()[0]).buffer_index, 0);
            assert_eq!(ByteView::from(compact.views()[1]).buffer_index, 1);
            assert_eq!(ByteView::from(compact.views()[2]).offset, 13);
            assert_eq!(compact.views()[0] == compact.views()[3], preserve);
            assert_eq!(compact.views()[1] == compact.views()[6], preserve);
            assert_eq!(compact.views()[4], 0);
            compact.to_data().validate_full().unwrap();
        }
    }

    #[test]
    fn test_compact_values_at_and_above_buffer_limit() {
        let exact = "x".repeat(32);
        let oversized = "y".repeat(33);
        let input = StringViewArray::from(vec![
            Some(exact.as_str()),
            Some(oversized.as_str()),
            Some("inline"),
            None,
        ]);
        let indices = [
            (0, 2),
            (0, 3),
            (0, 1),
            (0, 2),
            (0, 0),
            (0, 3),
            (0, 1),
            (0, 0),
        ];
        for preserve in [false, true] {
            let compact = if preserve {
                interleave_views_compact::<StringViewType, true>(&[&input], &indices, 32)
            } else {
                interleave_views_compact::<StringViewType, false>(&[&input], &indices, 32)
            }
            .unwrap();
            assert_eq!(
                compact.as_ref(),
                interleave(&[&input], &indices).unwrap().as_ref()
            );
            let compact = compact.as_string_view();
            let expected_sizes = if preserve {
                vec![33, 32]
            } else {
                vec![33, 32, 33, 32]
            };
            assert_eq!(
                compact
                    .data_buffers()
                    .iter()
                    .map(|buffer| buffer.len())
                    .collect::<Vec<_>>(),
                expected_sizes
            );
            for buffer in compact.data_buffers().iter() {
                assert_eq!(buffer.len(), buffer.capacity());
            }
            compact.to_data().validate_full().unwrap();
        }
    }

    #[test]
    fn test_compact_empty_null_and_inline_selections() {
        let input =
            StringViewArray::from(vec![Some("unselected long string"), None, Some("inline")]);
        for preserve in [false, true] {
            for indices in [vec![], vec![(0, 1), (0, 1)], vec![(0, 2), (0, 1)]] {
                let compact = Interleaver::new()
                    .with_compact_byte_views(true)
                    .with_preserve_byte_view_sharing(preserve)
                    .interleave(&[&input], &indices)
                    .unwrap();
                assert_eq!(
                    compact.as_ref(),
                    interleave(&[&input], &indices).unwrap().as_ref()
                );
                assert!(compact.as_string_view().data_buffers().is_empty());
            }
            let empty = StringViewArray::from(Vec::<&str>::new());
            assert!(
                Interleaver::new()
                    .with_compact_byte_views(true)
                    .with_preserve_byte_view_sharing(preserve)
                    .interleave(&[&empty], &[])
                    .unwrap()
                    .is_empty()
            );
        }
    }

    #[test]
    fn test_compact_configuration_and_type_errors() {
        let input = StringViewArray::from(vec!["first long view value", "second long view value"]);
        let indices = [(0, 1), (0, 0), (0, 1)];
        let ordinary = interleave(&[&input], &indices).unwrap();
        for interleaver in [
            Interleaver::default(),
            Interleaver::new().with_preserve_byte_view_sharing(true),
            Interleaver::new()
                .with_compact_byte_views(true)
                .with_compact_byte_views(false),
        ] {
            let output = interleaver.interleave(&[&input], &indices).unwrap();
            assert_eq!(output.as_ref(), ordinary.as_ref());
            assert_eq!(
                output.as_string_view().data_buffers()[0].as_ptr(),
                input.data_buffers()[0].as_ptr()
            );
        }
        let ints = Int32Array::from(vec![1, 2]);
        for preserve in [false, true] {
            for compact in [false, true] {
                let interleaver = Interleaver::new()
                    .with_compact_byte_views(compact)
                    .with_preserve_byte_view_sharing(preserve);
                let output = interleaver.interleave(&[&ints], &indices).unwrap();
                assert_eq!(
                    output.as_ref(),
                    interleave(&[&ints], &indices).unwrap().as_ref()
                );
                assert!(matches!(
                    interleaver.interleave(&[], &[]),
                    Err(ArrowError::InvalidArgumentError(_))
                ));
                assert!(matches!(
                    interleaver.interleave(&[&input, &ints], &[]),
                    Err(ArrowError::InvalidArgumentError(_))
                ));
                assert!(matches!(
                    interleaver.interleave(&[&input, &ints], &[(0, 0)]),
                    Err(ArrowError::InvalidArgumentError(_))
                ));
            }
        }
    }

    #[test]
    fn test_compact_invalid_indices_panic_before_copying() {
        let input = StringViewArray::from(vec!["valid selected long string"]);
        for preserve in [false, true] {
            for invalid in [(1, 0), (usize::MAX, 0), (0, 1), (0, usize::MAX)] {
                let interleaver = Interleaver::new()
                    .with_compact_byte_views(true)
                    .with_preserve_byte_view_sharing(preserve);
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    interleaver.interleave(&[&input], &[(0, 0), invalid])
                }));
                assert!(result.is_err(), "{invalid:?}, preserve={preserve}");
            }
        }
    }

    #[test]
    fn test_compact_option_leaves_nested_views_unchanged() {
        let values = StringViewArray::from(vec![
            "first nested long string",
            "second nested long string",
        ]);
        let pointer = values.data_buffers()[0].as_ptr();
        let input = ListArray::new(
            Arc::new(Field::new_list_field(DataType::Utf8View, false)),
            OffsetBuffer::new(vec![0_i32, 1, 2].into()),
            Arc::new(values),
            None,
        );
        let compact = Interleaver::new()
            .with_compact_byte_views(true)
            .interleave(&[&input], &[(0, 1)])
            .unwrap();
        let ordinary = interleave(&[&input], &[(0, 1)]).unwrap();
        assert_eq!(compact.as_ref(), ordinary.as_ref());
        assert_eq!(
            compact
                .as_list::<i32>()
                .values()
                .as_string_view()
                .data_buffers()[0]
                .as_ptr(),
            pointer
        );
    }

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
