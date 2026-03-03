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

use crate::cast::*;

/// Converts a non-list array to a list array where every element is a single element
/// list. `NULL`s in the original array become `[NULL]` (i.e. output list array
/// contains no nulls since it wraps all input nulls in a single element list).
///
/// For example: `Int32([1, NULL, 2]) -> List<Int32>([[1], [NULL], [2]])`
pub(crate) fn cast_values_to_list<O: OffsetSizeTrait>(
    array: &dyn Array,
    to: &FieldRef,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    if array.len() > O::MAX_OFFSET {
        return Err(ArrowError::ComputeError(format!(
            "Offset overflow when casting from {} to {}",
            array.data_type(),
            to.data_type()
        )));
    }
    let values = cast_with_options(array, to.data_type(), cast_options)?;
    let offsets = OffsetBuffer::from_repeated_length(1, values.len());
    let list = GenericListArray::<O>::try_new(to.clone(), offsets, values, None)?;
    Ok(Arc::new(list))
}

/// Same as [`cast_values_to_list`] but output list view array.
pub(crate) fn cast_values_to_list_view<O: OffsetSizeTrait>(
    array: &dyn Array,
    to: &FieldRef,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    if array.len() > O::MAX_OFFSET {
        return Err(ArrowError::ComputeError(format!(
            "Offset overflow when casting from {} to {}",
            array.data_type(),
            to.data_type()
        )));
    }
    let values = cast_with_options(array, to.data_type(), cast_options)?;
    let offsets = (0..values.len())
        .map(|index| O::usize_as(index))
        .collect::<Vec<O>>();
    let list = GenericListViewArray::<O>::try_new(
        to.clone(),
        offsets.into(),
        vec![O::one(); values.len()].into(),
        values,
        None,
    )?;
    Ok(Arc::new(list))
}

/// Same as [`cast_values_to_list`] but output fixed size list array with element
/// size 1.
pub(crate) fn cast_values_to_fixed_size_list(
    array: &dyn Array,
    to: &FieldRef,
    size: i32,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let values = cast_with_options(array, to.data_type(), cast_options)?;
    let list = FixedSizeListArray::try_new(to.clone(), size, values, None)?;
    Ok(Arc::new(list))
}

/// Cast fixed size list array to inner values type, essentially flattening the
/// lists.
///
/// For example: `FixedSizeList<Int32, 2>([[1, 2], [3, 4]]) -> Int32([1, 2, 3, 4])`
pub(crate) fn cast_single_element_fixed_size_list_to_values(
    array: &dyn Array,
    to: &DataType,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let values = array.as_fixed_size_list().values();
    cast_with_options(values, to, cast_options)
}

fn cast_fixed_size_list_to_list_inner<OffsetSize: OffsetSizeTrait, const IS_LIST_VIEW: bool>(
    array: &dyn Array,
    to: &FieldRef,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let array = array.as_fixed_size_list();
    let DataType::FixedSizeList(inner_field, size) = array.data_type() else {
        unreachable!()
    };
    let array = if to.data_type() != inner_field.data_type() {
        // To transform inner type, can first cast to FSL with new inner type.
        let fsl_to = DataType::FixedSizeList(to.clone(), *size);
        let array = cast_with_options(array, &fsl_to, cast_options)?;
        array.as_fixed_size_list().clone()
    } else {
        array.clone()
    };
    if IS_LIST_VIEW {
        let list: GenericListViewArray<OffsetSize> = array.into();
        Ok(Arc::new(list))
    } else {
        let list: GenericListArray<OffsetSize> = array.into();
        Ok(Arc::new(list))
    }
}

/// Cast fixed size list arrays to list arrays, maintaining the lengths of the inner
/// lists.
///
/// For example: `FixedSizeList<Int32, 2>([[1, 2], [3, 4]]) -> List<Int32>([[1, 2], [3, 4]])`
pub(crate) fn cast_fixed_size_list_to_list<OffsetSize: OffsetSizeTrait>(
    array: &dyn Array,
    to: &FieldRef,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    cast_fixed_size_list_to_list_inner::<OffsetSize, false>(array, to, cast_options)
}

/// Same as [`cast_fixed_size_list_to_list`] but output list view array.
pub(crate) fn cast_fixed_size_list_to_list_view<OffsetSize: OffsetSizeTrait>(
    array: &dyn Array,
    to: &FieldRef,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    cast_fixed_size_list_to_list_inner::<OffsetSize, true>(array, to, cast_options)
}

/// Cast list to fixed size list array. If any inner list size does not match the
/// size of the output fixed size list array, depending on `cast_options` we either
/// output `NULL` for that element (safe) or raise an error.
pub(crate) fn cast_list_to_fixed_size_list<OffsetSize>(
    array: &dyn Array,
    field: &FieldRef,
    size: i32,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    OffsetSize: OffsetSizeTrait,
{
    let array = array.as_list::<OffsetSize>();

    let cap = array.len() * size as usize;

    let mut null_builder = NullBufferBuilder::new(array.len());
    if let Some(nulls) = array.nulls().filter(|b| b.null_count() > 0) {
        null_builder.append_buffer(nulls);
    } else {
        null_builder.append_n_non_nulls(array.len());
    }

    // Whether the resulting array may contain null lists
    let nullable = cast_options.safe || array.null_count() != 0;
    // Nulls in FixedSizeListArray take up space and so we must pad the values
    let values = array.values().to_data();
    let mut mutable = MutableArrayData::new(vec![&values], nullable, cap);
    // The end position in values of the last incorrectly-sized list slice
    let mut last_pos = 0;

    // Need to flag when previous vector(s) are empty/None to distinguish from 'All slices were correct length' cases.
    let is_prev_empty = if array.offsets().len() < 2 {
        false
    } else {
        let first_offset = array.offsets()[0].as_usize();
        let second_offset = array.offsets()[1].as_usize();

        first_offset == 0 && second_offset == 0
    };

    for (idx, w) in array.offsets().windows(2).enumerate() {
        let start_pos = w[0].as_usize();
        let end_pos = w[1].as_usize();
        let len = end_pos - start_pos;

        if len != size as usize {
            if cast_options.safe || array.is_null(idx) {
                if last_pos != start_pos {
                    // Extend with valid slices
                    mutable.extend(0, last_pos, start_pos);
                }
                // Pad this slice with nulls
                mutable.extend_nulls(size as _);
                null_builder.set_bit(idx, false);
                // Set last_pos to the end of this slice's values
                last_pos = end_pos
            } else {
                return Err(ArrowError::CastError(format!(
                    "Cannot cast to FixedSizeList({size}): value at index {idx} has length {len}",
                )));
            }
        }
    }

    let values = match last_pos {
        0 if !is_prev_empty => array.values().slice(0, cap), // All slices were the correct length
        _ => {
            if mutable.len() != cap {
                // Remaining slices were all correct length
                let remaining = cap - mutable.len();
                mutable.extend(0, last_pos, last_pos + remaining)
            }
            make_array(mutable.freeze())
        }
    };

    // Cast the inner values if necessary
    let values = cast_with_options(values.as_ref(), field.data_type(), cast_options)?;

    let array = FixedSizeListArray::try_new(field.clone(), size, values, null_builder.build())?;
    Ok(Arc::new(array))
}

/// Same as [`cast_list_to_fixed_size_list`] but for list view arrays.
pub(crate) fn cast_list_view_to_fixed_size_list<O: OffsetSizeTrait>(
    array: &dyn Array,
    field: &FieldRef,
    size: i32,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let array = array.as_list_view::<O>();

    let mut null_builder = NullBufferBuilder::new(array.len());
    if let Some(nulls) = array.nulls().filter(|b| b.null_count() > 0) {
        null_builder.append_buffer(nulls);
    } else {
        null_builder.append_n_non_nulls(array.len());
    }

    let nullable = cast_options.safe || array.null_count() != 0;
    let values = array.values().to_data();
    let cap = array.len() * size as usize;
    let mut mutable = MutableArrayData::new(vec![&values], nullable, cap);

    for idx in 0..array.len() {
        let offset = array.value_offset(idx).as_usize();
        let len = array.value_size(idx).as_usize();

        if len != size as usize {
            // Nulls in FixedSizeListArray take up space and so we must pad the values
            if cast_options.safe || array.is_null(idx) {
                mutable.extend_nulls(size as _);
                null_builder.set_bit(idx, false);
            } else {
                return Err(ArrowError::CastError(format!(
                    "Cannot cast to FixedSizeList({size}): value at index {idx} has length {len}",
                )));
            }
        } else {
            mutable.extend(0, offset, offset + len);
        }
    }

    let values = make_array(mutable.freeze());
    let values = cast_with_options(values.as_ref(), field.data_type(), cast_options)?;

    let array = FixedSizeListArray::try_new(field.clone(), size, values, null_builder.build())?;
    Ok(Arc::new(array))
}

/// Casting between list arrays of same offset size; we cast only the inner type.
pub(crate) fn cast_list_values<O: OffsetSizeTrait>(
    array: &dyn Array,
    to: &FieldRef,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let list = array.as_list::<O>();
    let values = cast_with_options(list.values(), to.data_type(), cast_options)?;
    Ok(Arc::new(GenericListArray::<O>::try_new(
        to.clone(),
        list.offsets().clone(),
        values,
        list.nulls().cloned(),
    )?))
}

/// Casting between list view arrays of same offset size; we cast only the inner type.
pub(crate) fn cast_list_view_values<O: OffsetSizeTrait>(
    array: &dyn Array,
    to: &FieldRef,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let list = array.as_list_view::<O>();
    let values = cast_with_options(list.values(), to.data_type(), cast_options)?;
    Ok(Arc::new(GenericListViewArray::<O>::try_new(
        to.clone(),
        list.offsets().clone(),
        list.sizes().clone(),
        values,
        list.nulls().cloned(),
    )?))
}

/// Casting between list arrays of different offset size (e.g. List -> LargeList)
pub(crate) fn cast_list<I: OffsetSizeTrait, O: OffsetSizeTrait>(
    array: &dyn Array,
    field: &FieldRef,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let list = array.as_list::<I>();
    let values = list.values();
    let offsets = list.offsets();
    let nulls = list.nulls().cloned();

    if offsets.last().unwrap().as_usize() > O::MAX_OFFSET {
        return Err(ArrowError::ComputeError(format!(
            "Offset overflow when casting from {} to {}",
            array.data_type(),
            field.data_type()
        )));
    }

    // Recursively cast values
    let values = cast_with_options(values, field.data_type(), cast_options)?;
    let offsets: Vec<_> = offsets.iter().map(|x| O::usize_as(x.as_usize())).collect();

    // Safety: valid offsets and checked for overflow
    let offsets = unsafe { OffsetBuffer::new_unchecked(offsets.into()) };

    Ok(Arc::new(GenericListArray::<O>::try_new(
        field.clone(),
        offsets,
        values,
        nulls,
    )?))
}

/// Casting list view arrays to list.
pub(crate) fn cast_list_view_to_list<I, O>(
    array: &dyn Array,
    to: &FieldRef,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    I: OffsetSizeTrait,
    // We need ArrowPrimitiveType here to be able to create indices array for the
    // take kernel.
    O: ArrowPrimitiveType,
    O::Native: OffsetSizeTrait,
{
    let list_view = array.as_list_view::<I>();
    let list_view_offsets = list_view.offsets();
    let sizes = list_view.sizes();

    let mut take_indices: Vec<O::Native> = Vec::with_capacity(list_view.values().len());
    let mut offsets: Vec<O::Native> = Vec::with_capacity(list_view.len() + 1);
    use num_traits::Zero;
    offsets.push(O::Native::zero());

    for i in 0..list_view.len() {
        if list_view.is_null(i) {
            offsets.push(O::Native::usize_as(take_indices.len()));
            continue;
        }

        let offset = list_view_offsets[i].as_usize();
        let size = sizes[i].as_usize();

        for value_index in offset..offset + size {
            take_indices.push(O::Native::usize_as(value_index));
        }

        // Must guard all cases since ListView<i32> can overflow List<i32>
        // e.g. if offsets of [0, 0, 0] and sizes [i32::MAX, i32::MAX, i32::MAX]
        if take_indices.len() > O::Native::MAX_OFFSET {
            return Err(ArrowError::ComputeError(format!(
                "Offset overflow when casting from {} to {}",
                array.data_type(),
                to.data_type()
            )));
        }
        offsets.push(O::Native::usize_as(take_indices.len()));
    }

    // Form a contiguous values array
    let take_indices = PrimitiveArray::<O>::from_iter_values(take_indices);
    let values = arrow_select::take::take(list_view.values(), &take_indices, None)?;
    let values = cast_with_options(&values, to.data_type(), cast_options)?;

    Ok(Arc::new(GenericListArray::<O::Native>::try_new(
        to.clone(),
        OffsetBuffer::new(offsets.into()),
        values,
        list_view.nulls().cloned(),
    )?))
}

/// Casting between list view arrays of different offset size (e.g. ListView -> LargeListView)
pub(crate) fn cast_list_view<I: OffsetSizeTrait, O: OffsetSizeTrait>(
    array: &dyn Array,
    to_field: &FieldRef,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let list_view = array.as_list_view::<I>();

    // Recursively cast values
    let values = cast_with_options(list_view.values(), to_field.data_type(), cast_options)?;

    let offsets = list_view
        .offsets()
        .iter()
        .map(|offset| {
            let offset = offset.as_usize();
            if offset > O::MAX_OFFSET {
                return Err(ArrowError::ComputeError(format!(
                    "Offset overflow when casting from {} to {}",
                    array.data_type(),
                    to_field.data_type()
                )));
            }
            Ok(O::usize_as(offset))
        })
        .collect::<Result<Vec<O>, _>>()?;
    let sizes = list_view
        .sizes()
        .iter()
        .map(|size| {
            let size = size.as_usize();
            if size > O::MAX_OFFSET {
                return Err(ArrowError::ComputeError(format!(
                    "Offset overflow when casting from {} to {}",
                    array.data_type(),
                    to_field.data_type()
                )));
            }
            Ok(O::usize_as(size))
        })
        .collect::<Result<Vec<O>, _>>()?;
    Ok(Arc::new(GenericListViewArray::<O>::try_new(
        to_field.clone(),
        offsets.into(),
        sizes.into(),
        values,
        list_view.nulls().cloned(),
    )?))
}

/// Casting list arrays to list view.
pub(crate) fn cast_list_to_list_view<I: OffsetSizeTrait, O: OffsetSizeTrait>(
    array: &dyn Array,
    to_field: &FieldRef,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let list = array.as_list::<I>();
    let (_field, offsets, values, nulls) = list.clone().into_parts();

    let len = offsets.len() - 1;
    let mut sizes = Vec::with_capacity(len);
    let mut view_offsets = Vec::with_capacity(len);
    for (i, offset) in offsets.iter().enumerate().take(len) {
        let offset = offset.as_usize();
        let size = offsets[i + 1].as_usize() - offset;

        if offset > O::MAX_OFFSET || size > O::MAX_OFFSET {
            return Err(ArrowError::ComputeError(format!(
                "Offset overflow when casting from {} to {}",
                array.data_type(),
                to_field.data_type()
            )));
        }

        view_offsets.push(O::usize_as(offset));
        sizes.push(O::usize_as(size));
    }
    let values = cast_with_options(&values, to_field.data_type(), cast_options)?;
    let array = GenericListViewArray::<O>::new(
        to_field.clone(),
        view_offsets.into(),
        sizes.into(),
        values,
        nulls,
    );
    Ok(Arc::new(array))
}
