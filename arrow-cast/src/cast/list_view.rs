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

/// Helper function to cast a list view to a list
pub(crate) fn cast_list_view_to_list<O: OffsetSizeTrait>(
    array: &dyn Array,
    to: &FieldRef,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let list_view = array.as_list_view::<O>();
    let list_view_offsets = list_view.offsets();
    let sizes = list_view.sizes();
    let source_values = list_view.values();

    // Construct the indices and offsets for the new list array by iterating over the list view subarrays
    let mut indices = Vec::with_capacity(list_view.values().len());
    let mut offsets = Vec::with_capacity(list_view.len() + 1);
    // Add the offset for the first subarray
    offsets.push(O::usize_as(0));
    for i in 0..list_view.len() {
        // For each subarray, add the indices of the values to take
        let offset = list_view_offsets[i].as_usize();
        let size = sizes[i].as_usize();
        let end = offset + size;
        for j in offset..end {
            indices.push(j as i32);
        }
        // Add the offset for the next subarray
        offsets.push(O::usize_as(indices.len()));
    }

    // Take the values from the source values using the indices, creating a new array
    let values = arrow_select::take::take(source_values, &Int32Array::from(indices), None)?;

    // Cast the values to the target data type
    let values = cast_with_options(&values, to.data_type(), cast_options)?;

    Ok(Arc::new(GenericListArray::<O>::try_new(
        to.clone(),
        OffsetBuffer::new(offsets.into()),
        values,
        list_view.nulls().cloned(),
    )?))
}

pub(crate) fn cast_list_view<I: OffsetSizeTrait, O: OffsetSizeTrait>(
    array: &dyn Array,
    to_field: &FieldRef,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let list_view = array.as_list_view::<I>();
    let (_field, offsets, sizes, values, nulls) = list_view.clone().into_parts();

    // Recursively cast values
    let values = cast_with_options(&values, to_field.data_type(), cast_options)?;

    let new_offsets: Vec<_> = offsets.iter().map(|x| O::usize_as(x.as_usize())).collect();
    let new_sizes: Vec<_> = sizes.iter().map(|x| O::usize_as(x.as_usize())).collect();
    Ok(Arc::new(GenericListViewArray::<O>::try_new(
        to_field.clone(),
        new_offsets.into(),
        new_sizes.into(),
        values,
        nulls,
    )?))
}

pub(crate) fn cast_list_to_list_view<OffsetSize>(array: &dyn Array) -> Result<ArrayRef, ArrowError>
where
    OffsetSize: OffsetSizeTrait,
{
    let list = array.as_list::<OffsetSize>();
    let list_view: GenericListViewArray<OffsetSize> = list.clone().into();
    Ok(Arc::new(list_view))
}
