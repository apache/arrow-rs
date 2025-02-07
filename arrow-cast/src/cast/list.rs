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

/// Helper function that takes a primitive array and casts to a (generic) list array.
pub(crate) fn cast_values_to_list<O: OffsetSizeTrait>(
    array: &dyn Array,
    to: &FieldRef,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let values = cast_with_options(array, to.data_type(), cast_options)?;
    let offsets = OffsetBuffer::from_lengths(std::iter::repeat(1).take(values.len()));
    let list = GenericListArray::<O>::new(to.clone(), offsets, values, None);
    Ok(Arc::new(list))
}

/// Helper function that takes a primitive array and casts to a fixed size list array.
pub(crate) fn cast_values_to_fixed_size_list(
    array: &dyn Array,
    to: &FieldRef,
    size: i32,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let values = cast_with_options(array, to.data_type(), cast_options)?;
    let list = FixedSizeListArray::new(to.clone(), size, values, None);
    Ok(Arc::new(list))
}

pub(crate) fn cast_single_element_fixed_size_list_to_values(
    array: &dyn Array,
    to: &DataType,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let values = array.as_fixed_size_list().values();
    cast_with_options(values, to, cast_options)
}

pub(crate) fn cast_fixed_size_list_to_list<OffsetSize>(
    array: &dyn Array,
) -> Result<ArrayRef, ArrowError>
where
    OffsetSize: OffsetSizeTrait,
{
    let fixed_size_list: &FixedSizeListArray = array.as_fixed_size_list();
    let list: GenericListArray<OffsetSize> = fixed_size_list.clone().into();
    Ok(Arc::new(list))
}

pub(crate) fn cast_list_to_fixed_size_list<OffsetSize>(
    array: &GenericListArray<OffsetSize>,
    field: &FieldRef,
    size: i32,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    OffsetSize: OffsetSizeTrait,
{
    let cap = array.len() * size as usize;

    // Whether the resulting array may contain null lists
    let nullable = cast_options.safe || array.null_count() != 0;
    let mut nulls = nullable.then(|| {
        let mut buffer = BooleanBufferBuilder::new(array.len());
        match array.nulls() {
            Some(n) => buffer.append_buffer(n.inner()),
            None => buffer.append_n(array.len(), true),
        }
        buffer
    });

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
                nulls.as_mut().unwrap().set_bit(idx, false);
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

    // Construct the FixedSizeListArray
    let nulls = nulls.map(|mut x| x.finish().into());
    let array = FixedSizeListArray::new(field.clone(), size, values, nulls);
    Ok(Arc::new(array))
}

/// Helper function that takes an Generic list container and casts the inner datatype.
pub(crate) fn cast_list_values<O: OffsetSizeTrait>(
    array: &dyn Array,
    to: &FieldRef,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let list = array.as_list::<O>();
    let values = cast_with_options(list.values(), to.data_type(), cast_options)?;
    Ok(Arc::new(GenericListArray::<O>::new(
        to.clone(),
        list.offsets().clone(),
        values,
        list.nulls().cloned(),
    )))
}

/// Cast the container type of List/Largelist array along with the inner datatype
pub(crate) fn cast_list<I: OffsetSizeTrait, O: OffsetSizeTrait>(
    array: &dyn Array,
    field: &FieldRef,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let list = array.as_list::<I>();
    let values = list.values();
    let offsets = list.offsets();
    let nulls = list.nulls().cloned();

    if !O::IS_LARGE && values.len() > i32::MAX as usize {
        return Err(ArrowError::ComputeError(
            "LargeList too large to cast to List".into(),
        ));
    }

    // Recursively cast values
    let values = cast_with_options(values, field.data_type(), cast_options)?;
    let offsets: Vec<_> = offsets.iter().map(|x| O::usize_as(x.as_usize())).collect();

    // Safety: valid offsets and checked for overflow
    let offsets = unsafe { OffsetBuffer::new_unchecked(offsets.into()) };

    Ok(Arc::new(GenericListArray::<O>::new(
        field.clone(),
        offsets,
        values,
        nulls,
    )))
}
