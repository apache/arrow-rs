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

//! Defines take kernel for [Array]

use std::sync::Arc;

use arrow_array::builder::{BufferBuilder, UInt32Builder};
use arrow_array::cast::AsArray;
use arrow_array::types::*;
use arrow_array::*;
use arrow_buffer::{
    bit_util, ArrowNativeType, BooleanBuffer, Buffer, MutableBuffer, NullBuffer, ScalarBuffer,
};
use arrow_data::{ArrayData, ArrayDataBuilder};
use arrow_schema::{ArrowError, DataType, FieldRef, UnionMode};

use num::{One, Zero};

/// Take elements by index from [Array], creating a new [Array] from those indexes.
///
/// ```text
/// ┌─────────────────┐      ┌─────────┐                              ┌─────────────────┐
/// │        A        │      │    0    │                              │        A        │
/// ├─────────────────┤      ├─────────┤                              ├─────────────────┤
/// │        D        │      │    2    │                              │        B        │
/// ├─────────────────┤      ├─────────┤   take(values, indices)      ├─────────────────┤
/// │        B        │      │    3    │ ─────────────────────────▶   │        C        │
/// ├─────────────────┤      ├─────────┤                              ├─────────────────┤
/// │        C        │      │    1    │                              │        D        │
/// ├─────────────────┤      └─────────┘                              └─────────────────┘
/// │        E        │
/// └─────────────────┘
///    values array          indices array                              result
/// ```
///
/// For selecting values by index from multiple arrays see [`crate::interleave`]
///
/// # Errors
/// This function errors whenever:
/// * An index cannot be casted to `usize` (typically 32 bit architectures)
/// * An index is out of bounds and `options` is set to check bounds.
///
/// # Safety
///
/// When `options` is not set to check bounds, taking indexes after `len` will panic.
///
/// # Examples
/// ```
/// # use arrow_array::{StringArray, UInt32Array, cast::AsArray};
/// # use arrow_select::take::take;
/// let values = StringArray::from(vec!["zero", "one", "two"]);
///
/// // Take items at index 2, and 1:
/// let indices = UInt32Array::from(vec![2, 1]);
/// let taken = take(&values, &indices, None).unwrap();
/// let taken = taken.as_string::<i32>();
///
/// assert_eq!(*taken, StringArray::from(vec!["two", "one"]));
/// ```
pub fn take(
    values: &dyn Array,
    indices: &dyn Array,
    options: Option<TakeOptions>,
) -> Result<ArrayRef, ArrowError> {
    let options = options.unwrap_or_default();
    macro_rules! helper {
        ($t:ty, $values:expr, $indices:expr, $options:expr) => {{
            let indices = indices.as_primitive::<$t>();
            if $options.check_bounds {
                check_bounds($values.len(), indices)?;
            }
            let indices = indices.to_indices();
            take_impl($values, &indices)
        }};
    }
    downcast_integer! {
        indices.data_type() => (helper, values, indices, options),
        d => Err(ArrowError::InvalidArgumentError(format!("Take only supported for integers, got {d:?}")))
    }
}

/// For each [ArrayRef] in the [`Vec<ArrayRef>`], take elements by index and create a new
/// [`Vec<ArrayRef>`] from those indices.
///
/// ```text
/// ┌────────┬────────┐
/// │        │        │           ┌────────┐                                ┌────────┬────────┐
/// │   A    │   1    │           │        │                                │        │        │
/// ├────────┼────────┤           │   0    │                                │   A    │   1    │
/// │        │        │           ├────────┤                                ├────────┼────────┤
/// │   D    │   4    │           │        │                                │        │        │
/// ├────────┼────────┤           │   2    │  take_arrays(values,indices)   │   B    │   2    │
/// │        │        │           ├────────┤                                ├────────┼────────┤
/// │   B    │   2    │           │        │  ───────────────────────────►  │        │        │
/// ├────────┼────────┤           │   3    │                                │   C    │   3    │
/// │        │        │           ├────────┤                                ├────────┼────────┤
/// │   C    │   3    │           │        │                                │        │        │
/// ├────────┼────────┤           │   1    │                                │   D    │   4    │
/// │        │        │           └────────┘                                └────────┼────────┘
/// │   E    │   5    │
/// └────────┴────────┘
///    values arrays             indices array                                      result
/// ```
///
/// # Errors
/// This function errors whenever:
/// * An index cannot be casted to `usize` (typically 32 bit architectures)
/// * An index is out of bounds and `options` is set to check bounds.
///
/// # Safety
///
/// When `options` is not set to check bounds, taking indexes after `len` will panic.
///
/// # Examples
/// ```
/// # use std::sync::Arc;
/// # use arrow_array::{StringArray, UInt32Array, cast::AsArray};
/// # use arrow_select::take::{take, take_arrays};
/// let string_values = Arc::new(StringArray::from(vec!["zero", "one", "two"]));
/// let values = Arc::new(UInt32Array::from(vec![0, 1, 2]));
///
/// // Take items at index 2, and 1:
/// let indices = UInt32Array::from(vec![2, 1]);
/// let taken_arrays = take_arrays(&[string_values, values], &indices, None).unwrap();
/// let taken_string = taken_arrays[0].as_string::<i32>();
/// assert_eq!(*taken_string, StringArray::from(vec!["two", "one"]));
/// let taken_values = taken_arrays[1].as_primitive();
/// assert_eq!(*taken_values, UInt32Array::from(vec![2, 1]));
/// ```
pub fn take_arrays(
    arrays: &[ArrayRef],
    indices: &dyn Array,
    options: Option<TakeOptions>,
) -> Result<Vec<ArrayRef>, ArrowError> {
    arrays
        .iter()
        .map(|array| take(array.as_ref(), indices, options.clone()))
        .collect()
}

/// Verifies that the non-null values of `indices` are all `< len`
fn check_bounds<T: ArrowPrimitiveType>(
    len: usize,
    indices: &PrimitiveArray<T>,
) -> Result<(), ArrowError> {
    if indices.null_count() > 0 {
        indices.iter().flatten().try_for_each(|index| {
            let ix = index
                .to_usize()
                .ok_or_else(|| ArrowError::ComputeError("Cast to usize failed".to_string()))?;
            if ix >= len {
                return Err(ArrowError::ComputeError(format!(
                    "Array index out of bounds, cannot get item at index {ix} from {len} entries"
                )));
            }
            Ok(())
        })
    } else {
        indices.values().iter().try_for_each(|index| {
            let ix = index
                .to_usize()
                .ok_or_else(|| ArrowError::ComputeError("Cast to usize failed".to_string()))?;
            if ix >= len {
                return Err(ArrowError::ComputeError(format!(
                    "Array index out of bounds, cannot get item at index {ix} from {len} entries"
                )));
            }
            Ok(())
        })
    }
}

#[inline(never)]
fn take_impl<IndexType: ArrowPrimitiveType>(
    values: &dyn Array,
    indices: &PrimitiveArray<IndexType>,
) -> Result<ArrayRef, ArrowError> {
    downcast_primitive_array! {
        values => Ok(Arc::new(take_primitive(values, indices)?)),
        DataType::Boolean => {
            let values = values.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(Arc::new(take_boolean(values, indices)))
        }
        DataType::Utf8 => {
            Ok(Arc::new(take_bytes(values.as_string::<i32>(), indices)?))
        }
        DataType::LargeUtf8 => {
            Ok(Arc::new(take_bytes(values.as_string::<i64>(), indices)?))
        }
        DataType::Utf8View => {
            Ok(Arc::new(take_byte_view(values.as_string_view(), indices)?))
        }
        DataType::List(_) => {
            Ok(Arc::new(take_list::<_, Int32Type>(values.as_list(), indices)?))
        }
        DataType::LargeList(_) => {
            Ok(Arc::new(take_list::<_, Int64Type>(values.as_list(), indices)?))
        }
        DataType::FixedSizeList(_, length) => {
            let values = values
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .unwrap();
            Ok(Arc::new(take_fixed_size_list(
                values,
                indices,
                *length as u32,
            )?))
        }
        DataType::Map(_, _) => {
            let list_arr = ListArray::from(values.as_map().clone());
            let list_data = take_list::<_, Int32Type>(&list_arr, indices)?;
            let builder = list_data.into_data().into_builder().data_type(values.data_type().clone());
            Ok(Arc::new(MapArray::from(unsafe { builder.build_unchecked() })))
        }
        DataType::Struct(fields) => {
            let array: &StructArray = values.as_struct();
            let arrays  = array
                .columns()
                .iter()
                .map(|a| take_impl(a.as_ref(), indices))
                .collect::<Result<Vec<ArrayRef>, _>>()?;
            let fields: Vec<(FieldRef, ArrayRef)> =
                fields.iter().cloned().zip(arrays).collect();

            // Create the null bit buffer.
            let is_valid: Buffer = indices
                .iter()
                .map(|index| {
                    if let Some(index) = index {
                        array.is_valid(index.to_usize().unwrap())
                    } else {
                        false
                    }
                })
                .collect();

            Ok(Arc::new(StructArray::from((fields, is_valid))) as ArrayRef)
        }
        DataType::Dictionary(_, _) => downcast_dictionary_array! {
            values => Ok(Arc::new(take_dict(values, indices)?)),
            t => unimplemented!("Take not supported for dictionary type {:?}", t)
        }
        DataType::RunEndEncoded(_, _) => downcast_run_array! {
            values => Ok(Arc::new(take_run(values, indices)?)),
            t => unimplemented!("Take not supported for run type {:?}", t)
        }
        DataType::Binary => {
            Ok(Arc::new(take_bytes(values.as_binary::<i32>(), indices)?))
        }
        DataType::LargeBinary => {
            Ok(Arc::new(take_bytes(values.as_binary::<i64>(), indices)?))
        }
        DataType::BinaryView => {
            Ok(Arc::new(take_byte_view(values.as_binary_view(), indices)?))
        }
        DataType::FixedSizeBinary(size) => {
            let values = values
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            Ok(Arc::new(take_fixed_size_binary(values, indices, *size)?))
        }
        DataType::Null => {
            // Take applied to a null array produces a null array.
            if values.len() >= indices.len() {
                // If the existing null array is as big as the indices, we can use a slice of it
                // to avoid allocating a new null array.
                Ok(values.slice(0, indices.len()))
            } else {
                // If the existing null array isn't big enough, create a new one.
                Ok(new_null_array(&DataType::Null, indices.len()))
            }
        }
        DataType::Union(fields, UnionMode::Sparse) => {
            let mut children = Vec::with_capacity(fields.len());
            let values = values.as_any().downcast_ref::<UnionArray>().unwrap();
            let type_ids = take_native(values.type_ids(), indices);
            for (type_id, _field) in fields.iter() {
                let values = values.child(type_id);
                let values = take_impl(values, indices)?;
                children.push(values);
            }
            let array = UnionArray::try_new(fields.clone(), type_ids, None, children)?;
            Ok(Arc::new(array))
        }
        DataType::Union(fields, UnionMode::Dense) => {
            let values = values.as_any().downcast_ref::<UnionArray>().unwrap();

            let type_ids = <PrimitiveArray<Int8Type>>::new(take_native(values.type_ids(), indices), None);
            let offsets = <PrimitiveArray<Int32Type>>::new(take_native(values.offsets().unwrap(), indices), None);

            let children = fields.iter()
                .map(|(field_type_id, _)| {
                    let mask = BooleanArray::from_unary(&type_ids, |value_type_id| value_type_id == field_type_id);

                    let indices = crate::filter::filter(&offsets, &mask)?;

                    let values = values.child(field_type_id);

                    take_impl(values, indices.as_primitive::<Int32Type>())
                })
                .collect::<Result<_, _>>()?;

            let mut child_offsets = [0; 128];

            let offsets = type_ids.values()
                .iter()
                .map(|&i| {
                    let offset = child_offsets[i as usize];

                    child_offsets[i as usize] += 1;

                    offset
                })
                .collect();

            let (_, type_ids, _) = type_ids.into_parts();

            let array = UnionArray::try_new(fields.clone(), type_ids, Some(offsets), children)?;

            Ok(Arc::new(array))
        }
        t => unimplemented!("Take not supported for data type {:?}", t)
    }
}

/// Options that define how `take` should behave
#[derive(Clone, Debug, Default)]
pub struct TakeOptions {
    /// Perform bounds check before taking indices from values.
    /// If enabled, an `ArrowError` is returned if the indices are out of bounds.
    /// If not enabled, and indices exceed bounds, the kernel will panic.
    pub check_bounds: bool,
}

#[inline(always)]
fn maybe_usize<I: ArrowNativeType>(index: I) -> Result<usize, ArrowError> {
    index
        .to_usize()
        .ok_or_else(|| ArrowError::ComputeError("Cast to usize failed".to_string()))
}

/// `take` implementation for all primitive arrays
///
/// This checks if an `indices` slot is populated, and gets the value from `values`
///  as the populated index.
/// If the `indices` slot is null, a null value is returned.
/// For example, given:
///     values:  [1, 2, 3, null, 5]
///     indices: [0, null, 4, 3]
/// The result is: [1 (slot 0), null (null slot), 5 (slot 4), null (slot 3)]
fn take_primitive<T, I>(
    values: &PrimitiveArray<T>,
    indices: &PrimitiveArray<I>,
) -> Result<PrimitiveArray<T>, ArrowError>
where
    T: ArrowPrimitiveType,
    I: ArrowPrimitiveType,
{
    let values_buf = take_native(values.values(), indices);
    let nulls = take_nulls(values.nulls(), indices);
    Ok(PrimitiveArray::new(values_buf, nulls).with_data_type(values.data_type().clone()))
}

#[inline(never)]
fn take_nulls<I: ArrowPrimitiveType>(
    values: Option<&NullBuffer>,
    indices: &PrimitiveArray<I>,
) -> Option<NullBuffer> {
    match values.filter(|n| n.null_count() > 0) {
        Some(n) => {
            let buffer = take_bits(n.inner(), indices);
            Some(NullBuffer::new(buffer)).filter(|n| n.null_count() > 0)
        }
        None => indices.nulls().cloned(),
    }
}

#[inline(never)]
fn take_native<T: ArrowNativeType, I: ArrowPrimitiveType>(
    values: &[T],
    indices: &PrimitiveArray<I>,
) -> ScalarBuffer<T> {
    match indices.nulls().filter(|n| n.null_count() > 0) {
        Some(n) => indices
            .values()
            .iter()
            .enumerate()
            .map(|(idx, index)| match values.get(index.as_usize()) {
                Some(v) => *v,
                None => match n.is_null(idx) {
                    true => T::default(),
                    false => panic!("Out-of-bounds index {index:?}"),
                },
            })
            .collect(),
        None => indices
            .values()
            .iter()
            .map(|index| values[index.as_usize()])
            .collect(),
    }
}

#[inline(never)]
fn take_bits<I: ArrowPrimitiveType>(
    values: &BooleanBuffer,
    indices: &PrimitiveArray<I>,
) -> BooleanBuffer {
    let len = indices.len();
    let mut output_buffer = MutableBuffer::new_null(len);
    let output_slice = output_buffer.as_slice_mut();

    match indices.nulls().filter(|n| n.null_count() > 0) {
        Some(nulls) => nulls.valid_indices().for_each(|idx| {
            if values.value(indices.value(idx).as_usize()) {
                bit_util::set_bit(output_slice, idx);
            }
        }),
        None => indices.values().iter().enumerate().for_each(|(i, index)| {
            if values.value(index.as_usize()) {
                bit_util::set_bit(output_slice, i);
            }
        }),
    }
    BooleanBuffer::new(output_buffer.into(), 0, indices.len())
}

/// `take` implementation for boolean arrays
fn take_boolean<IndexType: ArrowPrimitiveType>(
    values: &BooleanArray,
    indices: &PrimitiveArray<IndexType>,
) -> BooleanArray {
    let val_buf = take_bits(values.values(), indices);
    let null_buf = take_nulls(values.nulls(), indices);
    BooleanArray::new(val_buf, null_buf)
}

/// `take` implementation for string arrays
fn take_bytes<T: ByteArrayType, IndexType: ArrowPrimitiveType>(
    array: &GenericByteArray<T>,
    indices: &PrimitiveArray<IndexType>,
) -> Result<GenericByteArray<T>, ArrowError> {
    let data_len = indices.len();

    let bytes_offset = (data_len + 1) * std::mem::size_of::<T::Offset>();
    let mut offsets = MutableBuffer::new(bytes_offset);
    offsets.push(T::Offset::default());

    let mut values = MutableBuffer::new(0);

    let nulls;
    if array.null_count() == 0 && indices.null_count() == 0 {
        offsets.extend(indices.values().iter().map(|index| {
            let s: &[u8] = array.value(index.as_usize()).as_ref();
            values.extend_from_slice(s);
            T::Offset::usize_as(values.len())
        }));
        nulls = None
    } else if indices.null_count() == 0 {
        let num_bytes = bit_util::ceil(data_len, 8);

        let mut null_buf = MutableBuffer::new(num_bytes).with_bitset(num_bytes, true);
        let null_slice = null_buf.as_slice_mut();
        offsets.extend(indices.values().iter().enumerate().map(|(i, index)| {
            let index = index.as_usize();
            if array.is_valid(index) {
                let s: &[u8] = array.value(index).as_ref();
                values.extend_from_slice(s.as_ref());
            } else {
                bit_util::unset_bit(null_slice, i);
            }
            T::Offset::usize_as(values.len())
        }));
        nulls = Some(null_buf.into());
    } else if array.null_count() == 0 {
        offsets.extend(indices.values().iter().enumerate().map(|(i, index)| {
            if indices.is_valid(i) {
                let s: &[u8] = array.value(index.as_usize()).as_ref();
                values.extend_from_slice(s);
            }
            T::Offset::usize_as(values.len())
        }));
        nulls = indices.nulls().map(|b| b.inner().sliced());
    } else {
        let num_bytes = bit_util::ceil(data_len, 8);

        let mut null_buf = MutableBuffer::new(num_bytes).with_bitset(num_bytes, true);
        let null_slice = null_buf.as_slice_mut();
        offsets.extend(indices.values().iter().enumerate().map(|(i, index)| {
            // check index is valid before using index. The value in
            // NULL index slots may not be within bounds of array
            let index = index.as_usize();
            if indices.is_valid(i) && array.is_valid(index) {
                let s: &[u8] = array.value(index).as_ref();
                values.extend_from_slice(s);
            } else {
                // set null bit
                bit_util::unset_bit(null_slice, i);
            }
            T::Offset::usize_as(values.len())
        }));
        nulls = Some(null_buf.into())
    }

    T::Offset::from_usize(values.len()).ok_or(ArrowError::ComputeError(format!(
        "Offset overflow for {}BinaryArray: {}",
        T::Offset::PREFIX,
        values.len()
    )))?;

    let array_data = ArrayData::builder(T::DATA_TYPE)
        .len(data_len)
        .add_buffer(offsets.into())
        .add_buffer(values.into())
        .null_bit_buffer(nulls);

    let array_data = unsafe { array_data.build_unchecked() };

    Ok(GenericByteArray::from(array_data))
}

/// `take` implementation for byte view arrays
fn take_byte_view<T: ByteViewType, IndexType: ArrowPrimitiveType>(
    array: &GenericByteViewArray<T>,
    indices: &PrimitiveArray<IndexType>,
) -> Result<GenericByteViewArray<T>, ArrowError> {
    let new_views = take_native(array.views(), indices);
    let new_nulls = take_nulls(array.nulls(), indices);
    // Safety:  array.views was valid, and take_native copies only valid values, and verifies bounds
    Ok(unsafe {
        GenericByteViewArray::new_unchecked(new_views, array.data_buffers().to_vec(), new_nulls)
    })
}

/// `take` implementation for list arrays
///
/// Calculates the index and indexed offset for the inner array,
/// applying `take` on the inner array, then reconstructing a list array
/// with the indexed offsets
fn take_list<IndexType, OffsetType>(
    values: &GenericListArray<OffsetType::Native>,
    indices: &PrimitiveArray<IndexType>,
) -> Result<GenericListArray<OffsetType::Native>, ArrowError>
where
    IndexType: ArrowPrimitiveType,
    OffsetType: ArrowPrimitiveType,
    OffsetType::Native: OffsetSizeTrait,
    PrimitiveArray<OffsetType>: From<Vec<OffsetType::Native>>,
{
    // TODO: Some optimizations can be done here such as if it is
    // taking the whole list or a contiguous sublist
    let (list_indices, offsets, null_buf) =
        take_value_indices_from_list::<IndexType, OffsetType>(values, indices)?;

    let taken = take_impl::<OffsetType>(values.values().as_ref(), &list_indices)?;
    let value_offsets = Buffer::from_vec(offsets);
    // create a new list with taken data and computed null information
    let list_data = ArrayDataBuilder::new(values.data_type().clone())
        .len(indices.len())
        .null_bit_buffer(Some(null_buf.into()))
        .offset(0)
        .add_child_data(taken.into_data())
        .add_buffer(value_offsets);

    let list_data = unsafe { list_data.build_unchecked() };

    Ok(GenericListArray::<OffsetType::Native>::from(list_data))
}

/// `take` implementation for `FixedSizeListArray`
///
/// Calculates the index and indexed offset for the inner array,
/// applying `take` on the inner array, then reconstructing a list array
/// with the indexed offsets
fn take_fixed_size_list<IndexType: ArrowPrimitiveType>(
    values: &FixedSizeListArray,
    indices: &PrimitiveArray<IndexType>,
    length: <UInt32Type as ArrowPrimitiveType>::Native,
) -> Result<FixedSizeListArray, ArrowError> {
    let list_indices = take_value_indices_from_fixed_size_list(values, indices, length)?;
    let taken = take_impl::<UInt32Type>(values.values().as_ref(), &list_indices)?;

    // determine null count and null buffer, which are a function of `values` and `indices`
    let num_bytes = bit_util::ceil(indices.len(), 8);
    let mut null_buf = MutableBuffer::new(num_bytes).with_bitset(num_bytes, true);
    let null_slice = null_buf.as_slice_mut();

    for i in 0..indices.len() {
        let index = indices
            .value(i)
            .to_usize()
            .ok_or_else(|| ArrowError::ComputeError("Cast to usize failed".to_string()))?;
        if !indices.is_valid(i) || values.is_null(index) {
            bit_util::unset_bit(null_slice, i);
        }
    }

    let list_data = ArrayDataBuilder::new(values.data_type().clone())
        .len(indices.len())
        .null_bit_buffer(Some(null_buf.into()))
        .offset(0)
        .add_child_data(taken.into_data());

    let list_data = unsafe { list_data.build_unchecked() };

    Ok(FixedSizeListArray::from(list_data))
}

fn take_fixed_size_binary<IndexType: ArrowPrimitiveType>(
    values: &FixedSizeBinaryArray,
    indices: &PrimitiveArray<IndexType>,
    size: i32,
) -> Result<FixedSizeBinaryArray, ArrowError> {
    let nulls = values.nulls();
    let array_iter = indices
        .values()
        .iter()
        .map(|idx| {
            let idx = maybe_usize::<IndexType::Native>(*idx)?;
            if nulls.map(|n| n.is_valid(idx)).unwrap_or(true) {
                Ok(Some(values.value(idx)))
            } else {
                Ok(None)
            }
        })
        .collect::<Result<Vec<_>, ArrowError>>()?
        .into_iter();

    FixedSizeBinaryArray::try_from_sparse_iter_with_size(array_iter, size)
}

/// `take` implementation for dictionary arrays
///
/// applies `take` to the keys of the dictionary array and returns a new dictionary array
/// with the same dictionary values and reordered keys
fn take_dict<T: ArrowDictionaryKeyType, I: ArrowPrimitiveType>(
    values: &DictionaryArray<T>,
    indices: &PrimitiveArray<I>,
) -> Result<DictionaryArray<T>, ArrowError> {
    let new_keys = take_primitive(values.keys(), indices)?;
    Ok(unsafe { DictionaryArray::new_unchecked(new_keys, values.values().clone()) })
}

/// `take` implementation for run arrays
///
/// Finds physical indices for the given logical indices and builds output run array
/// by taking values in the input run_array.values at the physical indices.
/// The output run array will be run encoded on the physical indices and not on output values.
/// For e.g. an input `RunArray{ run_ends = [2,4,6,8], values=[1,2,1,2] }` and `logical_indices=[2,3,6,7]`
/// would be converted to `physical_indices=[1,1,3,3]` which will be used to build
/// output `RunArray{ run_ends=[2,4], values=[2,2] }`.
fn take_run<T: RunEndIndexType, I: ArrowPrimitiveType>(
    run_array: &RunArray<T>,
    logical_indices: &PrimitiveArray<I>,
) -> Result<RunArray<T>, ArrowError> {
    // get physical indices for the input logical indices
    let physical_indices = run_array.get_physical_indices(logical_indices.values())?;

    // Run encode the physical indices into new_run_ends_builder
    // Keep track of the physical indices to take in take_value_indices
    // `unwrap` is used in this function because the unwrapped values are bounded by the corresponding `::Native`.
    let mut new_run_ends_builder = BufferBuilder::<T::Native>::new(1);
    let mut take_value_indices = BufferBuilder::<I::Native>::new(1);
    let mut new_physical_len = 1;
    for ix in 1..physical_indices.len() {
        if physical_indices[ix] != physical_indices[ix - 1] {
            take_value_indices.append(I::Native::from_usize(physical_indices[ix - 1]).unwrap());
            new_run_ends_builder.append(T::Native::from_usize(ix).unwrap());
            new_physical_len += 1;
        }
    }
    take_value_indices
        .append(I::Native::from_usize(physical_indices[physical_indices.len() - 1]).unwrap());
    new_run_ends_builder.append(T::Native::from_usize(physical_indices.len()).unwrap());
    let new_run_ends = unsafe {
        // Safety:
        // The function builds a valid run_ends array and hence need not be validated.
        ArrayDataBuilder::new(T::DATA_TYPE)
            .len(new_physical_len)
            .null_count(0)
            .add_buffer(new_run_ends_builder.finish())
            .build_unchecked()
    };

    let take_value_indices: PrimitiveArray<I> = unsafe {
        // Safety:
        // The function builds a valid take_value_indices array and hence need not be validated.
        ArrayDataBuilder::new(I::DATA_TYPE)
            .len(new_physical_len)
            .null_count(0)
            .add_buffer(take_value_indices.finish())
            .build_unchecked()
            .into()
    };

    let new_values = take(run_array.values(), &take_value_indices, None)?;

    let builder = ArrayDataBuilder::new(run_array.data_type().clone())
        .len(physical_indices.len())
        .add_child_data(new_run_ends)
        .add_child_data(new_values.into_data());
    let array_data = unsafe {
        // Safety:
        //  This function builds a valid run array and hence can skip validation.
        builder.build_unchecked()
    };
    Ok(array_data.into())
}

/// Takes/filters a list array's inner data using the offsets of the list array.
///
/// Where a list array has indices `[0,2,5,10]`, taking indices of `[2,0]` returns
/// an array of the indices `[5..10, 0..2]` and offsets `[0,5,7]` (5 elements and 2
/// elements)
#[allow(clippy::type_complexity)]
fn take_value_indices_from_list<IndexType, OffsetType>(
    list: &GenericListArray<OffsetType::Native>,
    indices: &PrimitiveArray<IndexType>,
) -> Result<
    (
        PrimitiveArray<OffsetType>,
        Vec<OffsetType::Native>,
        MutableBuffer,
    ),
    ArrowError,
>
where
    IndexType: ArrowPrimitiveType,
    OffsetType: ArrowPrimitiveType,
    OffsetType::Native: OffsetSizeTrait + std::ops::Add + Zero + One,
    PrimitiveArray<OffsetType>: From<Vec<OffsetType::Native>>,
{
    // TODO: benchmark this function, there might be a faster unsafe alternative
    let offsets: &[OffsetType::Native] = list.value_offsets();

    let mut new_offsets = Vec::with_capacity(indices.len());
    let mut values = Vec::new();
    let mut current_offset = OffsetType::Native::zero();
    // add first offset
    new_offsets.push(OffsetType::Native::zero());

    // Initialize null buffer
    let num_bytes = bit_util::ceil(indices.len(), 8);
    let mut null_buf = MutableBuffer::new(num_bytes).with_bitset(num_bytes, true);
    let null_slice = null_buf.as_slice_mut();

    // compute the value indices, and set offsets accordingly
    for i in 0..indices.len() {
        if indices.is_valid(i) {
            let ix = indices
                .value(i)
                .to_usize()
                .ok_or_else(|| ArrowError::ComputeError("Cast to usize failed".to_string()))?;
            let start = offsets[ix];
            let end = offsets[ix + 1];
            current_offset += end - start;
            new_offsets.push(current_offset);

            let mut curr = start;

            // if start == end, this slot is empty
            while curr < end {
                values.push(curr);
                curr += One::one();
            }
            if !list.is_valid(ix) {
                bit_util::unset_bit(null_slice, i);
            }
        } else {
            bit_util::unset_bit(null_slice, i);
            new_offsets.push(current_offset);
        }
    }

    Ok((
        PrimitiveArray::<OffsetType>::from(values),
        new_offsets,
        null_buf,
    ))
}

/// Takes/filters a fixed size list array's inner data using the offsets of the list array.
fn take_value_indices_from_fixed_size_list<IndexType>(
    list: &FixedSizeListArray,
    indices: &PrimitiveArray<IndexType>,
    length: <UInt32Type as ArrowPrimitiveType>::Native,
) -> Result<PrimitiveArray<UInt32Type>, ArrowError>
where
    IndexType: ArrowPrimitiveType,
{
    let mut values = UInt32Builder::with_capacity(length as usize * indices.len());

    for i in 0..indices.len() {
        if indices.is_valid(i) {
            let index = indices
                .value(i)
                .to_usize()
                .ok_or_else(|| ArrowError::ComputeError("Cast to usize failed".to_string()))?;
            let start = list.value_offset(index) as <UInt32Type as ArrowPrimitiveType>::Native;

            // Safety: Range always has known length.
            unsafe {
                values.append_trusted_len_iter(start..start + length);
            }
        } else {
            values.append_nulls(length as usize);
        }
    }

    Ok(values.finish())
}

/// To avoid generating take implementations for every index type, instead we
/// only generate for UInt32 and UInt64 and coerce inputs to these types
trait ToIndices {
    type T: ArrowPrimitiveType;

    fn to_indices(&self) -> PrimitiveArray<Self::T>;
}

macro_rules! to_indices_reinterpret {
    ($t:ty, $o:ty) => {
        impl ToIndices for PrimitiveArray<$t> {
            type T = $o;

            fn to_indices(&self) -> PrimitiveArray<$o> {
                let cast = ScalarBuffer::new(self.values().inner().clone(), 0, self.len());
                PrimitiveArray::new(cast, self.nulls().cloned())
            }
        }
    };
}

macro_rules! to_indices_identity {
    ($t:ty) => {
        impl ToIndices for PrimitiveArray<$t> {
            type T = $t;

            fn to_indices(&self) -> PrimitiveArray<$t> {
                self.clone()
            }
        }
    };
}

macro_rules! to_indices_widening {
    ($t:ty, $o:ty) => {
        impl ToIndices for PrimitiveArray<$t> {
            type T = UInt32Type;

            fn to_indices(&self) -> PrimitiveArray<$o> {
                let cast = self.values().iter().copied().map(|x| x as _).collect();
                PrimitiveArray::new(cast, self.nulls().cloned())
            }
        }
    };
}

to_indices_widening!(UInt8Type, UInt32Type);
to_indices_widening!(Int8Type, UInt32Type);

to_indices_widening!(UInt16Type, UInt32Type);
to_indices_widening!(Int16Type, UInt32Type);

to_indices_identity!(UInt32Type);
to_indices_reinterpret!(Int32Type, UInt32Type);

to_indices_identity!(UInt64Type);
to_indices_reinterpret!(Int64Type, UInt64Type);

/// Take rows by index from [`RecordBatch`] and returns a new [`RecordBatch`] from those indexes.
///
/// This function will call [`take`] on each array of the [`RecordBatch`] and assemble a new [`RecordBatch`].
///
/// # Example
/// ```
/// # use std::sync::Arc;
/// # use arrow_array::{StringArray, Int32Array, UInt32Array, RecordBatch};
/// # use arrow_schema::{DataType, Field, Schema};
/// # use arrow_select::take::take_record_batch;
///
/// let schema = Arc::new(Schema::new(vec![
///     Field::new("a", DataType::Int32, true),
///     Field::new("b", DataType::Utf8, true),
/// ]));
/// let batch = RecordBatch::try_new(
///     schema.clone(),
///     vec![
///         Arc::new(Int32Array::from_iter_values(0..20)),
///         Arc::new(StringArray::from_iter_values(
///             (0..20).map(|i| format!("str-{}", i)),
///         )),
///     ],
/// )
/// .unwrap();
///
/// let indices = UInt32Array::from(vec![1, 5, 10]);
/// let taken = take_record_batch(&batch, &indices).unwrap();
///
/// let expected = RecordBatch::try_new(
///     schema,
///     vec![
///         Arc::new(Int32Array::from(vec![1, 5, 10])),
///         Arc::new(StringArray::from(vec!["str-1", "str-5", "str-10"])),
///     ],
/// )
/// .unwrap();
/// assert_eq!(taken, expected);
/// ```
pub fn take_record_batch(
    record_batch: &RecordBatch,
    indices: &dyn Array,
) -> Result<RecordBatch, ArrowError> {
    let columns = record_batch
        .columns()
        .iter()
        .map(|c| take(c, indices, None))
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(record_batch.schema(), columns)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::*;
    use arrow_buffer::{IntervalDayTime, IntervalMonthDayNano};
    use arrow_schema::{Field, Fields, TimeUnit, UnionFields};

    fn test_take_decimal_arrays(
        data: Vec<Option<i128>>,
        index: &UInt32Array,
        options: Option<TakeOptions>,
        expected_data: Vec<Option<i128>>,
        precision: &u8,
        scale: &i8,
    ) -> Result<(), ArrowError> {
        let output = data
            .into_iter()
            .collect::<Decimal128Array>()
            .with_precision_and_scale(*precision, *scale)
            .unwrap();

        let expected = expected_data
            .into_iter()
            .collect::<Decimal128Array>()
            .with_precision_and_scale(*precision, *scale)
            .unwrap();

        let expected = Arc::new(expected) as ArrayRef;
        let output = take(&output, index, options).unwrap();
        assert_eq!(&output, &expected);
        Ok(())
    }

    fn test_take_boolean_arrays(
        data: Vec<Option<bool>>,
        index: &UInt32Array,
        options: Option<TakeOptions>,
        expected_data: Vec<Option<bool>>,
    ) {
        let output = BooleanArray::from(data);
        let expected = Arc::new(BooleanArray::from(expected_data)) as ArrayRef;
        let output = take(&output, index, options).unwrap();
        assert_eq!(&output, &expected)
    }

    fn test_take_primitive_arrays<T>(
        data: Vec<Option<T::Native>>,
        index: &UInt32Array,
        options: Option<TakeOptions>,
        expected_data: Vec<Option<T::Native>>,
    ) -> Result<(), ArrowError>
    where
        T: ArrowPrimitiveType,
        PrimitiveArray<T>: From<Vec<Option<T::Native>>>,
    {
        let output = PrimitiveArray::<T>::from(data);
        let expected = Arc::new(PrimitiveArray::<T>::from(expected_data)) as ArrayRef;
        let output = take(&output, index, options)?;
        assert_eq!(&output, &expected);
        Ok(())
    }

    fn test_take_primitive_arrays_non_null<T>(
        data: Vec<T::Native>,
        index: &UInt32Array,
        options: Option<TakeOptions>,
        expected_data: Vec<Option<T::Native>>,
    ) -> Result<(), ArrowError>
    where
        T: ArrowPrimitiveType,
        PrimitiveArray<T>: From<Vec<T::Native>>,
        PrimitiveArray<T>: From<Vec<Option<T::Native>>>,
    {
        let output = PrimitiveArray::<T>::from(data);
        let expected = Arc::new(PrimitiveArray::<T>::from(expected_data)) as ArrayRef;
        let output = take(&output, index, options)?;
        assert_eq!(&output, &expected);
        Ok(())
    }

    fn test_take_impl_primitive_arrays<T, I>(
        data: Vec<Option<T::Native>>,
        index: &PrimitiveArray<I>,
        options: Option<TakeOptions>,
        expected_data: Vec<Option<T::Native>>,
    ) where
        T: ArrowPrimitiveType,
        PrimitiveArray<T>: From<Vec<Option<T::Native>>>,
        I: ArrowPrimitiveType,
    {
        let output = PrimitiveArray::<T>::from(data);
        let expected = PrimitiveArray::<T>::from(expected_data);
        let output = take(&output, index, options).unwrap();
        let output = output.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
        assert_eq!(output, &expected)
    }

    // create a simple struct for testing purposes
    fn create_test_struct(values: Vec<Option<(Option<bool>, Option<i32>)>>) -> StructArray {
        let mut struct_builder = StructBuilder::new(
            Fields::from(vec![
                Field::new("a", DataType::Boolean, true),
                Field::new("b", DataType::Int32, true),
            ]),
            vec![
                Box::new(BooleanBuilder::with_capacity(values.len())),
                Box::new(Int32Builder::with_capacity(values.len())),
            ],
        );

        for value in values {
            struct_builder
                .field_builder::<BooleanBuilder>(0)
                .unwrap()
                .append_option(value.and_then(|v| v.0));
            struct_builder
                .field_builder::<Int32Builder>(1)
                .unwrap()
                .append_option(value.and_then(|v| v.1));
            struct_builder.append(value.is_some());
        }
        struct_builder.finish()
    }

    #[test]
    fn test_take_decimal128_non_null_indices() {
        let index = UInt32Array::from(vec![0, 5, 3, 1, 4, 2]);
        let precision: u8 = 10;
        let scale: i8 = 5;
        test_take_decimal_arrays(
            vec![None, Some(3), Some(5), Some(2), Some(3), None],
            &index,
            None,
            vec![None, None, Some(2), Some(3), Some(3), Some(5)],
            &precision,
            &scale,
        )
        .unwrap();
    }

    #[test]
    fn test_take_decimal128() {
        let index = UInt32Array::from(vec![Some(3), None, Some(1), Some(3), Some(2)]);
        let precision: u8 = 10;
        let scale: i8 = 5;
        test_take_decimal_arrays(
            vec![Some(0), Some(1), Some(2), Some(3), Some(4)],
            &index,
            None,
            vec![Some(3), None, Some(1), Some(3), Some(2)],
            &precision,
            &scale,
        )
        .unwrap();
    }

    #[test]
    fn test_take_primitive_non_null_indices() {
        let index = UInt32Array::from(vec![0, 5, 3, 1, 4, 2]);
        test_take_primitive_arrays::<Int8Type>(
            vec![None, Some(3), Some(5), Some(2), Some(3), None],
            &index,
            None,
            vec![None, None, Some(2), Some(3), Some(3), Some(5)],
        )
        .unwrap();
    }

    #[test]
    fn test_take_primitive_non_null_values() {
        let index = UInt32Array::from(vec![Some(3), None, Some(1), Some(3), Some(2)]);
        test_take_primitive_arrays::<Int8Type>(
            vec![Some(0), Some(1), Some(2), Some(3), Some(4)],
            &index,
            None,
            vec![Some(3), None, Some(1), Some(3), Some(2)],
        )
        .unwrap();
    }

    #[test]
    fn test_take_primitive_non_null() {
        let index = UInt32Array::from(vec![0, 5, 3, 1, 4, 2]);
        test_take_primitive_arrays::<Int8Type>(
            vec![Some(0), Some(3), Some(5), Some(2), Some(3), Some(1)],
            &index,
            None,
            vec![Some(0), Some(1), Some(2), Some(3), Some(3), Some(5)],
        )
        .unwrap();
    }

    #[test]
    fn test_take_primitive_nullable_indices_non_null_values_with_offset() {
        let index = UInt32Array::from(vec![Some(0), Some(1), Some(2), Some(3), None, None]);
        let index = index.slice(2, 4);
        let index = index.as_any().downcast_ref::<UInt32Array>().unwrap();

        assert_eq!(
            index,
            &UInt32Array::from(vec![Some(2), Some(3), None, None])
        );

        test_take_primitive_arrays_non_null::<Int64Type>(
            vec![0, 10, 20, 30, 40, 50],
            index,
            None,
            vec![Some(20), Some(30), None, None],
        )
        .unwrap();
    }

    #[test]
    fn test_take_primitive_nullable_indices_nullable_values_with_offset() {
        let index = UInt32Array::from(vec![Some(0), Some(1), Some(2), Some(3), None, None]);
        let index = index.slice(2, 4);
        let index = index.as_any().downcast_ref::<UInt32Array>().unwrap();

        assert_eq!(
            index,
            &UInt32Array::from(vec![Some(2), Some(3), None, None])
        );

        test_take_primitive_arrays::<Int64Type>(
            vec![None, None, Some(20), Some(30), Some(40), Some(50)],
            index,
            None,
            vec![Some(20), Some(30), None, None],
        )
        .unwrap();
    }

    #[test]
    fn test_take_primitive() {
        let index = UInt32Array::from(vec![Some(3), None, Some(1), Some(3), Some(2)]);

        // int8
        test_take_primitive_arrays::<Int8Type>(
            vec![Some(0), None, Some(2), Some(3), None],
            &index,
            None,
            vec![Some(3), None, None, Some(3), Some(2)],
        )
        .unwrap();

        // int16
        test_take_primitive_arrays::<Int16Type>(
            vec![Some(0), None, Some(2), Some(3), None],
            &index,
            None,
            vec![Some(3), None, None, Some(3), Some(2)],
        )
        .unwrap();

        // int32
        test_take_primitive_arrays::<Int32Type>(
            vec![Some(0), None, Some(2), Some(3), None],
            &index,
            None,
            vec![Some(3), None, None, Some(3), Some(2)],
        )
        .unwrap();

        // int64
        test_take_primitive_arrays::<Int64Type>(
            vec![Some(0), None, Some(2), Some(3), None],
            &index,
            None,
            vec![Some(3), None, None, Some(3), Some(2)],
        )
        .unwrap();

        // uint8
        test_take_primitive_arrays::<UInt8Type>(
            vec![Some(0), None, Some(2), Some(3), None],
            &index,
            None,
            vec![Some(3), None, None, Some(3), Some(2)],
        )
        .unwrap();

        // uint16
        test_take_primitive_arrays::<UInt16Type>(
            vec![Some(0), None, Some(2), Some(3), None],
            &index,
            None,
            vec![Some(3), None, None, Some(3), Some(2)],
        )
        .unwrap();

        // uint32
        test_take_primitive_arrays::<UInt32Type>(
            vec![Some(0), None, Some(2), Some(3), None],
            &index,
            None,
            vec![Some(3), None, None, Some(3), Some(2)],
        )
        .unwrap();

        // int64
        test_take_primitive_arrays::<Int64Type>(
            vec![Some(0), None, Some(2), Some(-15), None],
            &index,
            None,
            vec![Some(-15), None, None, Some(-15), Some(2)],
        )
        .unwrap();

        // interval_year_month
        test_take_primitive_arrays::<IntervalYearMonthType>(
            vec![Some(0), None, Some(2), Some(-15), None],
            &index,
            None,
            vec![Some(-15), None, None, Some(-15), Some(2)],
        )
        .unwrap();

        // interval_day_time
        let v1 = IntervalDayTime::new(0, 0);
        let v2 = IntervalDayTime::new(2, 0);
        let v3 = IntervalDayTime::new(-15, 0);
        test_take_primitive_arrays::<IntervalDayTimeType>(
            vec![Some(v1), None, Some(v2), Some(v3), None],
            &index,
            None,
            vec![Some(v3), None, None, Some(v3), Some(v2)],
        )
        .unwrap();

        // interval_month_day_nano
        let v1 = IntervalMonthDayNano::new(0, 0, 0);
        let v2 = IntervalMonthDayNano::new(2, 0, 0);
        let v3 = IntervalMonthDayNano::new(-15, 0, 0);
        test_take_primitive_arrays::<IntervalMonthDayNanoType>(
            vec![Some(v1), None, Some(v2), Some(v3), None],
            &index,
            None,
            vec![Some(v3), None, None, Some(v3), Some(v2)],
        )
        .unwrap();

        // duration_second
        test_take_primitive_arrays::<DurationSecondType>(
            vec![Some(0), None, Some(2), Some(-15), None],
            &index,
            None,
            vec![Some(-15), None, None, Some(-15), Some(2)],
        )
        .unwrap();

        // duration_millisecond
        test_take_primitive_arrays::<DurationMillisecondType>(
            vec![Some(0), None, Some(2), Some(-15), None],
            &index,
            None,
            vec![Some(-15), None, None, Some(-15), Some(2)],
        )
        .unwrap();

        // duration_microsecond
        test_take_primitive_arrays::<DurationMicrosecondType>(
            vec![Some(0), None, Some(2), Some(-15), None],
            &index,
            None,
            vec![Some(-15), None, None, Some(-15), Some(2)],
        )
        .unwrap();

        // duration_nanosecond
        test_take_primitive_arrays::<DurationNanosecondType>(
            vec![Some(0), None, Some(2), Some(-15), None],
            &index,
            None,
            vec![Some(-15), None, None, Some(-15), Some(2)],
        )
        .unwrap();

        // float32
        test_take_primitive_arrays::<Float32Type>(
            vec![Some(0.0), None, Some(2.21), Some(-3.1), None],
            &index,
            None,
            vec![Some(-3.1), None, None, Some(-3.1), Some(2.21)],
        )
        .unwrap();

        // float64
        test_take_primitive_arrays::<Float64Type>(
            vec![Some(0.0), None, Some(2.21), Some(-3.1), None],
            &index,
            None,
            vec![Some(-3.1), None, None, Some(-3.1), Some(2.21)],
        )
        .unwrap();
    }

    #[test]
    fn test_take_preserve_timezone() {
        let index = Int64Array::from(vec![Some(0), None]);

        let input = TimestampNanosecondArray::from(vec![
            1_639_715_368_000_000_000,
            1_639_715_368_000_000_000,
        ])
        .with_timezone("UTC".to_string());
        let result = take(&input, &index, None).unwrap();
        match result.data_type() {
            DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                assert_eq!(tz.clone(), Some("UTC".into()))
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_take_impl_primitive_with_int64_indices() {
        let index = Int64Array::from(vec![Some(3), None, Some(1), Some(3), Some(2)]);

        // int16
        test_take_impl_primitive_arrays::<Int16Type, Int64Type>(
            vec![Some(0), None, Some(2), Some(3), None],
            &index,
            None,
            vec![Some(3), None, None, Some(3), Some(2)],
        );

        // int64
        test_take_impl_primitive_arrays::<Int64Type, Int64Type>(
            vec![Some(0), None, Some(2), Some(-15), None],
            &index,
            None,
            vec![Some(-15), None, None, Some(-15), Some(2)],
        );

        // uint64
        test_take_impl_primitive_arrays::<UInt64Type, Int64Type>(
            vec![Some(0), None, Some(2), Some(3), None],
            &index,
            None,
            vec![Some(3), None, None, Some(3), Some(2)],
        );

        // duration_millisecond
        test_take_impl_primitive_arrays::<DurationMillisecondType, Int64Type>(
            vec![Some(0), None, Some(2), Some(-15), None],
            &index,
            None,
            vec![Some(-15), None, None, Some(-15), Some(2)],
        );

        // float32
        test_take_impl_primitive_arrays::<Float32Type, Int64Type>(
            vec![Some(0.0), None, Some(2.21), Some(-3.1), None],
            &index,
            None,
            vec![Some(-3.1), None, None, Some(-3.1), Some(2.21)],
        );
    }

    #[test]
    fn test_take_impl_primitive_with_uint8_indices() {
        let index = UInt8Array::from(vec![Some(3), None, Some(1), Some(3), Some(2)]);

        // int16
        test_take_impl_primitive_arrays::<Int16Type, UInt8Type>(
            vec![Some(0), None, Some(2), Some(3), None],
            &index,
            None,
            vec![Some(3), None, None, Some(3), Some(2)],
        );

        // duration_millisecond
        test_take_impl_primitive_arrays::<DurationMillisecondType, UInt8Type>(
            vec![Some(0), None, Some(2), Some(-15), None],
            &index,
            None,
            vec![Some(-15), None, None, Some(-15), Some(2)],
        );

        // float32
        test_take_impl_primitive_arrays::<Float32Type, UInt8Type>(
            vec![Some(0.0), None, Some(2.21), Some(-3.1), None],
            &index,
            None,
            vec![Some(-3.1), None, None, Some(-3.1), Some(2.21)],
        );
    }

    #[test]
    fn test_take_bool() {
        let index = UInt32Array::from(vec![Some(3), None, Some(1), Some(3), Some(2)]);
        // boolean
        test_take_boolean_arrays(
            vec![Some(false), None, Some(true), Some(false), None],
            &index,
            None,
            vec![Some(false), None, None, Some(false), Some(true)],
        );
    }

    #[test]
    fn test_take_bool_nullable_index() {
        // indices where the masked invalid elements would be out of bounds
        let index_data = ArrayData::try_new(
            DataType::UInt32,
            6,
            Some(Buffer::from_iter(vec![
                false, true, false, true, false, true,
            ])),
            0,
            vec![Buffer::from_iter(vec![99, 0, 999, 1, 9999, 2])],
            vec![],
        )
        .unwrap();
        let index = UInt32Array::from(index_data);
        test_take_boolean_arrays(
            vec![Some(true), None, Some(false)],
            &index,
            None,
            vec![None, Some(true), None, None, None, Some(false)],
        );
    }

    #[test]
    fn test_take_bool_nullable_index_nonnull_values() {
        // indices where the masked invalid elements would be out of bounds
        let index_data = ArrayData::try_new(
            DataType::UInt32,
            6,
            Some(Buffer::from_iter(vec![
                false, true, false, true, false, true,
            ])),
            0,
            vec![Buffer::from_iter(vec![99, 0, 999, 1, 9999, 2])],
            vec![],
        )
        .unwrap();
        let index = UInt32Array::from(index_data);
        test_take_boolean_arrays(
            vec![Some(true), Some(true), Some(false)],
            &index,
            None,
            vec![None, Some(true), None, Some(true), None, Some(false)],
        );
    }

    #[test]
    fn test_take_bool_with_offset() {
        let index = UInt32Array::from(vec![Some(3), None, Some(1), Some(3), Some(2), None]);
        let index = index.slice(2, 4);
        let index = index
            .as_any()
            .downcast_ref::<PrimitiveArray<UInt32Type>>()
            .unwrap();

        // boolean
        test_take_boolean_arrays(
            vec![Some(false), None, Some(true), Some(false), None],
            index,
            None,
            vec![None, Some(false), Some(true), None],
        );
    }

    fn _test_take_string<'a, K>()
    where
        K: Array + PartialEq + From<Vec<Option<&'a str>>> + 'static,
    {
        let index = UInt32Array::from(vec![Some(3), None, Some(1), Some(3), Some(4)]);

        let array = K::from(vec![
            Some("one"),
            None,
            Some("three"),
            Some("four"),
            Some("five"),
        ]);
        let actual = take(&array, &index, None).unwrap();
        assert_eq!(actual.len(), index.len());

        let actual = actual.as_any().downcast_ref::<K>().unwrap();

        let expected = K::from(vec![Some("four"), None, None, Some("four"), Some("five")]);

        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_take_string() {
        _test_take_string::<StringArray>()
    }

    #[test]
    fn test_take_large_string() {
        _test_take_string::<LargeStringArray>()
    }

    #[test]
    fn test_take_slice_string() {
        let strings = StringArray::from(vec![Some("hello"), None, Some("world"), None, Some("hi")]);
        let indices = Int32Array::from(vec![Some(0), Some(1), None, Some(0), Some(2)]);
        let indices_slice = indices.slice(1, 4);
        let expected = StringArray::from(vec![None, None, Some("hello"), Some("world")]);
        let result = take(&strings, &indices_slice, None).unwrap();
        assert_eq!(result.as_ref(), &expected);
    }

    fn _test_byte_view<T>()
    where
        T: ByteViewType,
        str: AsRef<T::Native>,
        T::Native: PartialEq,
    {
        let index = UInt32Array::from(vec![Some(3), None, Some(1), Some(3), Some(4), Some(2)]);
        let array = {
            // ["hello", "world", null, "large payload over 12 bytes", "lulu"]
            let mut builder = GenericByteViewBuilder::<T>::new();
            builder.append_value("hello");
            builder.append_value("world");
            builder.append_null();
            builder.append_value("large payload over 12 bytes");
            builder.append_value("lulu");
            builder.finish()
        };

        let actual = take(&array, &index, None).unwrap();

        assert_eq!(actual.len(), index.len());

        let expected = {
            // ["large payload over 12 bytes", null, "world", "large payload over 12 bytes", "lulu", null]
            let mut builder = GenericByteViewBuilder::<T>::new();
            builder.append_value("large payload over 12 bytes");
            builder.append_null();
            builder.append_value("world");
            builder.append_value("large payload over 12 bytes");
            builder.append_value("lulu");
            builder.append_null();
            builder.finish()
        };

        assert_eq!(actual.as_ref(), &expected);
    }

    #[test]
    fn test_take_string_view() {
        _test_byte_view::<StringViewType>()
    }

    #[test]
    fn test_take_binary_view() {
        _test_byte_view::<BinaryViewType>()
    }

    macro_rules! test_take_list {
        ($offset_type:ty, $list_data_type:ident, $list_array_type:ident) => {{
            // Construct a value array, [[0,0,0], [-1,-2,-1], [], [2,3]]
            let value_data = Int32Array::from(vec![0, 0, 0, -1, -2, -1, 2, 3]).into_data();
            // Construct offsets
            let value_offsets: [$offset_type; 5] = [0, 3, 6, 6, 8];
            let value_offsets = Buffer::from_slice_ref(&value_offsets);
            // Construct a list array from the above two
            let list_data_type =
                DataType::$list_data_type(Arc::new(Field::new("item", DataType::Int32, false)));
            let list_data = ArrayData::builder(list_data_type.clone())
                .len(4)
                .add_buffer(value_offsets)
                .add_child_data(value_data)
                .build()
                .unwrap();
            let list_array = $list_array_type::from(list_data);

            // index returns: [[2,3], null, [-1,-2,-1], [], [0,0,0]]
            let index = UInt32Array::from(vec![Some(3), None, Some(1), Some(2), Some(0)]);

            let a = take(&list_array, &index, None).unwrap();
            let a: &$list_array_type = a.as_any().downcast_ref::<$list_array_type>().unwrap();

            // construct a value array with expected results:
            // [[2,3], null, [-1,-2,-1], [], [0,0,0]]
            let expected_data = Int32Array::from(vec![
                Some(2),
                Some(3),
                Some(-1),
                Some(-2),
                Some(-1),
                Some(0),
                Some(0),
                Some(0),
            ])
            .into_data();
            // construct offsets
            let expected_offsets: [$offset_type; 6] = [0, 2, 2, 5, 5, 8];
            let expected_offsets = Buffer::from_slice_ref(&expected_offsets);
            // construct list array from the two
            let expected_list_data = ArrayData::builder(list_data_type)
                .len(5)
                // null buffer remains the same as only the indices have nulls
                .nulls(index.nulls().cloned())
                .add_buffer(expected_offsets)
                .add_child_data(expected_data)
                .build()
                .unwrap();
            let expected_list_array = $list_array_type::from(expected_list_data);

            assert_eq!(a, &expected_list_array);
        }};
    }

    macro_rules! test_take_list_with_value_nulls {
        ($offset_type:ty, $list_data_type:ident, $list_array_type:ident) => {{
            // Construct a value array, [[0,null,0], [-1,-2,3], [null], [5,null]]
            let value_data = Int32Array::from(vec![
                Some(0),
                None,
                Some(0),
                Some(-1),
                Some(-2),
                Some(3),
                None,
                Some(5),
                None,
            ])
            .into_data();
            // Construct offsets
            let value_offsets: [$offset_type; 5] = [0, 3, 6, 7, 9];
            let value_offsets = Buffer::from_slice_ref(&value_offsets);
            // Construct a list array from the above two
            let list_data_type =
                DataType::$list_data_type(Arc::new(Field::new("item", DataType::Int32, true)));
            let list_data = ArrayData::builder(list_data_type.clone())
                .len(4)
                .add_buffer(value_offsets)
                .null_bit_buffer(Some(Buffer::from([0b11111111])))
                .add_child_data(value_data)
                .build()
                .unwrap();
            let list_array = $list_array_type::from(list_data);

            // index returns: [[null], null, [-1,-2,3], [2,null], [0,null,0]]
            let index = UInt32Array::from(vec![Some(2), None, Some(1), Some(3), Some(0)]);

            let a = take(&list_array, &index, None).unwrap();
            let a: &$list_array_type = a.as_any().downcast_ref::<$list_array_type>().unwrap();

            // construct a value array with expected results:
            // [[null], null, [-1,-2,3], [5,null], [0,null,0]]
            let expected_data = Int32Array::from(vec![
                None,
                Some(-1),
                Some(-2),
                Some(3),
                Some(5),
                None,
                Some(0),
                None,
                Some(0),
            ])
            .into_data();
            // construct offsets
            let expected_offsets: [$offset_type; 6] = [0, 1, 1, 4, 6, 9];
            let expected_offsets = Buffer::from_slice_ref(&expected_offsets);
            // construct list array from the two
            let expected_list_data = ArrayData::builder(list_data_type)
                .len(5)
                // null buffer remains the same as only the indices have nulls
                .nulls(index.nulls().cloned())
                .add_buffer(expected_offsets)
                .add_child_data(expected_data)
                .build()
                .unwrap();
            let expected_list_array = $list_array_type::from(expected_list_data);

            assert_eq!(a, &expected_list_array);
        }};
    }

    macro_rules! test_take_list_with_nulls {
        ($offset_type:ty, $list_data_type:ident, $list_array_type:ident) => {{
            // Construct a value array, [[0,null,0], [-1,-2,3], null, [5,null]]
            let value_data = Int32Array::from(vec![
                Some(0),
                None,
                Some(0),
                Some(-1),
                Some(-2),
                Some(3),
                Some(5),
                None,
            ])
            .into_data();
            // Construct offsets
            let value_offsets: [$offset_type; 5] = [0, 3, 6, 6, 8];
            let value_offsets = Buffer::from_slice_ref(&value_offsets);
            // Construct a list array from the above two
            let list_data_type =
                DataType::$list_data_type(Arc::new(Field::new("item", DataType::Int32, true)));
            let list_data = ArrayData::builder(list_data_type.clone())
                .len(4)
                .add_buffer(value_offsets)
                .null_bit_buffer(Some(Buffer::from([0b11111011])))
                .add_child_data(value_data)
                .build()
                .unwrap();
            let list_array = $list_array_type::from(list_data);

            // index returns: [null, null, [-1,-2,3], [5,null], [0,null,0]]
            let index = UInt32Array::from(vec![Some(2), None, Some(1), Some(3), Some(0)]);

            let a = take(&list_array, &index, None).unwrap();
            let a: &$list_array_type = a.as_any().downcast_ref::<$list_array_type>().unwrap();

            // construct a value array with expected results:
            // [null, null, [-1,-2,3], [5,null], [0,null,0]]
            let expected_data = Int32Array::from(vec![
                Some(-1),
                Some(-2),
                Some(3),
                Some(5),
                None,
                Some(0),
                None,
                Some(0),
            ])
            .into_data();
            // construct offsets
            let expected_offsets: [$offset_type; 6] = [0, 0, 0, 3, 5, 8];
            let expected_offsets = Buffer::from_slice_ref(&expected_offsets);
            // construct list array from the two
            let mut null_bits: [u8; 1] = [0; 1];
            bit_util::set_bit(&mut null_bits, 2);
            bit_util::set_bit(&mut null_bits, 3);
            bit_util::set_bit(&mut null_bits, 4);
            let expected_list_data = ArrayData::builder(list_data_type)
                .len(5)
                // null buffer must be recalculated as both values and indices have nulls
                .null_bit_buffer(Some(Buffer::from(null_bits)))
                .add_buffer(expected_offsets)
                .add_child_data(expected_data)
                .build()
                .unwrap();
            let expected_list_array = $list_array_type::from(expected_list_data);

            assert_eq!(a, &expected_list_array);
        }};
    }

    fn do_take_fixed_size_list_test<T>(
        length: <Int32Type as ArrowPrimitiveType>::Native,
        input_data: Vec<Option<Vec<Option<T::Native>>>>,
        indices: Vec<<UInt32Type as ArrowPrimitiveType>::Native>,
        expected_data: Vec<Option<Vec<Option<T::Native>>>>,
    ) where
        T: ArrowPrimitiveType,
        PrimitiveArray<T>: From<Vec<Option<T::Native>>>,
    {
        let indices = UInt32Array::from(indices);

        let input_array = FixedSizeListArray::from_iter_primitive::<T, _, _>(input_data, length);

        let output = take_fixed_size_list(&input_array, &indices, length as u32).unwrap();

        let expected = FixedSizeListArray::from_iter_primitive::<T, _, _>(expected_data, length);

        assert_eq!(&output, &expected)
    }

    #[test]
    fn test_take_list() {
        test_take_list!(i32, List, ListArray);
    }

    #[test]
    fn test_take_large_list() {
        test_take_list!(i64, LargeList, LargeListArray);
    }

    #[test]
    fn test_take_list_with_value_nulls() {
        test_take_list_with_value_nulls!(i32, List, ListArray);
    }

    #[test]
    fn test_take_large_list_with_value_nulls() {
        test_take_list_with_value_nulls!(i64, LargeList, LargeListArray);
    }

    #[test]
    fn test_test_take_list_with_nulls() {
        test_take_list_with_nulls!(i32, List, ListArray);
    }

    #[test]
    fn test_test_take_large_list_with_nulls() {
        test_take_list_with_nulls!(i64, LargeList, LargeListArray);
    }

    #[test]
    fn test_take_fixed_size_list() {
        do_take_fixed_size_list_test::<Int32Type>(
            3,
            vec![
                Some(vec![None, Some(1), Some(2)]),
                Some(vec![Some(3), Some(4), None]),
                Some(vec![Some(6), Some(7), Some(8)]),
            ],
            vec![2, 1, 0],
            vec![
                Some(vec![Some(6), Some(7), Some(8)]),
                Some(vec![Some(3), Some(4), None]),
                Some(vec![None, Some(1), Some(2)]),
            ],
        );

        do_take_fixed_size_list_test::<UInt8Type>(
            1,
            vec![
                Some(vec![Some(1)]),
                Some(vec![Some(2)]),
                Some(vec![Some(3)]),
                Some(vec![Some(4)]),
                Some(vec![Some(5)]),
                Some(vec![Some(6)]),
                Some(vec![Some(7)]),
                Some(vec![Some(8)]),
            ],
            vec![2, 7, 0],
            vec![
                Some(vec![Some(3)]),
                Some(vec![Some(8)]),
                Some(vec![Some(1)]),
            ],
        );

        do_take_fixed_size_list_test::<UInt64Type>(
            3,
            vec![
                Some(vec![Some(10), Some(11), Some(12)]),
                Some(vec![Some(13), Some(14), Some(15)]),
                None,
                Some(vec![Some(16), Some(17), Some(18)]),
            ],
            vec![3, 2, 1, 2, 0],
            vec![
                Some(vec![Some(16), Some(17), Some(18)]),
                None,
                Some(vec![Some(13), Some(14), Some(15)]),
                None,
                Some(vec![Some(10), Some(11), Some(12)]),
            ],
        );
    }

    #[test]
    #[should_panic(expected = "index out of bounds: the len is 4 but the index is 1000")]
    fn test_take_list_out_of_bounds() {
        // Construct a value array, [[0,0,0], [-1,-2,-1], [2,3]]
        let value_data = Int32Array::from(vec![0, 0, 0, -1, -2, -1, 2, 3]).into_data();
        // Construct offsets
        let value_offsets = Buffer::from_slice_ref([0, 3, 6, 8]);
        // Construct a list array from the above two
        let list_data_type = DataType::List(Arc::new(Field::new("item", DataType::Int32, false)));
        let list_data = ArrayData::builder(list_data_type)
            .len(3)
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .build()
            .unwrap();
        let list_array = ListArray::from(list_data);

        let index = UInt32Array::from(vec![1000]);

        // A panic is expected here since we have not supplied the check_bounds
        // option.
        take(&list_array, &index, None).unwrap();
    }

    #[test]
    fn test_take_map() {
        let values = Int32Array::from(vec![1, 2, 3, 4]);
        let array =
            MapArray::new_from_strings(vec!["a", "b", "c", "a"].into_iter(), &values, &[0, 3, 4])
                .unwrap();

        let index = UInt32Array::from(vec![0]);

        let result = take(&array, &index, None).unwrap();
        let expected: ArrayRef = Arc::new(
            MapArray::new_from_strings(
                vec!["a", "b", "c"].into_iter(),
                &values.slice(0, 3),
                &[0, 3],
            )
            .unwrap(),
        );
        assert_eq!(&expected, &result);
    }

    #[test]
    fn test_take_struct() {
        let array = create_test_struct(vec![
            Some((Some(true), Some(42))),
            Some((Some(false), Some(28))),
            Some((Some(false), Some(19))),
            Some((Some(true), Some(31))),
            None,
        ]);

        let index = UInt32Array::from(vec![0, 3, 1, 0, 2, 4]);
        let actual = take(&array, &index, None).unwrap();
        let actual: &StructArray = actual.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(index.len(), actual.len());
        assert_eq!(1, actual.null_count());

        let expected = create_test_struct(vec![
            Some((Some(true), Some(42))),
            Some((Some(true), Some(31))),
            Some((Some(false), Some(28))),
            Some((Some(true), Some(42))),
            Some((Some(false), Some(19))),
            None,
        ]);

        assert_eq!(&expected, actual);
    }

    #[test]
    fn test_take_struct_with_null_indices() {
        let array = create_test_struct(vec![
            Some((Some(true), Some(42))),
            Some((Some(false), Some(28))),
            Some((Some(false), Some(19))),
            Some((Some(true), Some(31))),
            None,
        ]);

        let index = UInt32Array::from(vec![None, Some(3), Some(1), None, Some(0), Some(4)]);
        let actual = take(&array, &index, None).unwrap();
        let actual: &StructArray = actual.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(index.len(), actual.len());
        assert_eq!(3, actual.null_count()); // 2 because of indices, 1 because of struct array

        let expected = create_test_struct(vec![
            None,
            Some((Some(true), Some(31))),
            Some((Some(false), Some(28))),
            None,
            Some((Some(true), Some(42))),
            None,
        ]);

        assert_eq!(&expected, actual);
    }

    #[test]
    fn test_take_out_of_bounds() {
        let index = UInt32Array::from(vec![Some(3), None, Some(1), Some(3), Some(6)]);
        let take_opt = TakeOptions { check_bounds: true };

        // int64
        let result = test_take_primitive_arrays::<Int64Type>(
            vec![Some(0), None, Some(2), Some(3), None],
            &index,
            Some(take_opt),
            vec![None],
        );
        assert!(result.is_err());
    }

    #[test]
    #[should_panic(expected = "index out of bounds: the len is 4 but the index is 1000")]
    fn test_take_out_of_bounds_panic() {
        let index = UInt32Array::from(vec![Some(1000)]);

        test_take_primitive_arrays::<Int64Type>(
            vec![Some(0), Some(1), Some(2), Some(3)],
            &index,
            None,
            vec![None],
        )
        .unwrap();
    }

    #[test]
    fn test_null_array_smaller_than_indices() {
        let values = NullArray::new(2);
        let indices = UInt32Array::from(vec![Some(0), None, Some(15)]);

        let result = take(&values, &indices, None).unwrap();
        let expected: ArrayRef = Arc::new(NullArray::new(3));
        assert_eq!(&result, &expected);
    }

    #[test]
    fn test_null_array_larger_than_indices() {
        let values = NullArray::new(5);
        let indices = UInt32Array::from(vec![Some(0), None, Some(15)]);

        let result = take(&values, &indices, None).unwrap();
        let expected: ArrayRef = Arc::new(NullArray::new(3));
        assert_eq!(&result, &expected);
    }

    #[test]
    fn test_null_array_indices_out_of_bounds() {
        let values = NullArray::new(5);
        let indices = UInt32Array::from(vec![Some(0), None, Some(15)]);

        let result = take(&values, &indices, Some(TakeOptions { check_bounds: true }));
        assert_eq!(
            result.unwrap_err().to_string(),
            "Compute error: Array index out of bounds, cannot get item at index 15 from 5 entries"
        );
    }

    #[test]
    fn test_take_dict() {
        let mut dict_builder = StringDictionaryBuilder::<Int16Type>::new();

        dict_builder.append("foo").unwrap();
        dict_builder.append("bar").unwrap();
        dict_builder.append("").unwrap();
        dict_builder.append_null();
        dict_builder.append("foo").unwrap();
        dict_builder.append("bar").unwrap();
        dict_builder.append("bar").unwrap();
        dict_builder.append("foo").unwrap();

        let array = dict_builder.finish();
        let dict_values = array.values().clone();
        let dict_values = dict_values.as_any().downcast_ref::<StringArray>().unwrap();

        let indices = UInt32Array::from(vec![
            Some(0), // first "foo"
            Some(7), // last "foo"
            None,    // null index should return null
            Some(5), // second "bar"
            Some(6), // another "bar"
            Some(2), // empty string
            Some(3), // input is null at this index
        ]);

        let result = take(&array, &indices, None).unwrap();
        let result = result
            .as_any()
            .downcast_ref::<DictionaryArray<Int16Type>>()
            .unwrap();

        let result_values: StringArray = result.values().to_data().into();

        // dictionary values should stay the same
        let expected_values = StringArray::from(vec!["foo", "bar", ""]);
        assert_eq!(&expected_values, dict_values);
        assert_eq!(&expected_values, &result_values);

        let expected_keys = Int16Array::from(vec![
            Some(0),
            Some(0),
            None,
            Some(1),
            Some(1),
            Some(2),
            None,
        ]);
        assert_eq!(result.keys(), &expected_keys);
    }

    fn build_generic_list<S, T>(data: Vec<Option<Vec<T::Native>>>) -> GenericListArray<S>
    where
        S: OffsetSizeTrait + 'static,
        T: ArrowPrimitiveType,
        PrimitiveArray<T>: From<Vec<Option<T::Native>>>,
    {
        GenericListArray::from_iter_primitive::<T, _, _>(
            data.iter()
                .map(|x| x.as_ref().map(|x| x.iter().map(|x| Some(*x)))),
        )
    }

    #[test]
    fn test_take_value_index_from_list() {
        let list = build_generic_list::<i32, Int32Type>(vec![
            Some(vec![0, 1]),
            Some(vec![2, 3, 4]),
            Some(vec![5, 6, 7, 8, 9]),
        ]);
        let indices = UInt32Array::from(vec![2, 0]);

        let (indexed, offsets, null_buf) = take_value_indices_from_list(&list, &indices).unwrap();

        assert_eq!(indexed, Int32Array::from(vec![5, 6, 7, 8, 9, 0, 1]));
        assert_eq!(offsets, vec![0, 5, 7]);
        assert_eq!(null_buf.as_slice(), &[0b11111111]);
    }

    #[test]
    fn test_take_value_index_from_large_list() {
        let list = build_generic_list::<i64, Int32Type>(vec![
            Some(vec![0, 1]),
            Some(vec![2, 3, 4]),
            Some(vec![5, 6, 7, 8, 9]),
        ]);
        let indices = UInt32Array::from(vec![2, 0]);

        let (indexed, offsets, null_buf) =
            take_value_indices_from_list::<_, Int64Type>(&list, &indices).unwrap();

        assert_eq!(indexed, Int64Array::from(vec![5, 6, 7, 8, 9, 0, 1]));
        assert_eq!(offsets, vec![0, 5, 7]);
        assert_eq!(null_buf.as_slice(), &[0b11111111]);
    }

    #[test]
    fn test_take_runs() {
        let logical_array: Vec<i32> = vec![1_i32, 1, 2, 2, 1, 1, 1, 2, 2, 1, 1, 2, 2];

        let mut builder = PrimitiveRunBuilder::<Int32Type, Int32Type>::new();
        builder.extend(logical_array.into_iter().map(Some));
        let run_array = builder.finish();

        let take_indices: PrimitiveArray<Int32Type> =
            vec![7, 2, 3, 7, 11, 4, 6].into_iter().collect();

        let take_out = take_run(&run_array, &take_indices).unwrap();

        assert_eq!(take_out.len(), 7);
        assert_eq!(take_out.run_ends().len(), 7);
        assert_eq!(take_out.run_ends().values(), &[1_i32, 3, 4, 5, 7]);

        let take_out_values = take_out.values().as_primitive::<Int32Type>();
        assert_eq!(take_out_values.values(), &[2, 2, 2, 2, 1]);
    }

    #[test]
    fn test_take_value_index_from_fixed_list() {
        let list = FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
            vec![
                Some(vec![Some(1), Some(2), None]),
                Some(vec![Some(4), None, Some(6)]),
                None,
                Some(vec![None, Some(8), Some(9)]),
            ],
            3,
        );

        let indices = UInt32Array::from(vec![2, 1, 0]);
        let indexed = take_value_indices_from_fixed_size_list(&list, &indices, 3).unwrap();

        assert_eq!(indexed, UInt32Array::from(vec![6, 7, 8, 3, 4, 5, 0, 1, 2]));

        let indices = UInt32Array::from(vec![3, 2, 1, 2, 0]);
        let indexed = take_value_indices_from_fixed_size_list(&list, &indices, 3).unwrap();

        assert_eq!(
            indexed,
            UInt32Array::from(vec![9, 10, 11, 6, 7, 8, 3, 4, 5, 6, 7, 8, 0, 1, 2])
        );
    }

    #[test]
    fn test_take_null_indices() {
        // Build indices with values that are out of bounds, but masked by null mask
        let indices = Int32Array::new(
            vec![1, 2, 400, 400].into(),
            Some(NullBuffer::from(vec![true, true, false, false])),
        );
        let values = Int32Array::from(vec![1, 23, 4, 5]);
        let r = take(&values, &indices, None).unwrap();
        let values = r
            .as_primitive::<Int32Type>()
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(&values, &[Some(23), Some(4), None, None])
    }

    #[test]
    fn test_take_fixed_size_list_null_indices() {
        let indices = Int32Array::from_iter([Some(0), None]);
        let values = Arc::new(Int32Array::from(vec![0, 1, 2, 3]));
        let arr_field = Arc::new(Field::new("item", values.data_type().clone(), true));
        let values = FixedSizeListArray::try_new(arr_field, 2, values, None).unwrap();

        let r = take(&values, &indices, None).unwrap();
        let values = r
            .as_fixed_size_list()
            .values()
            .as_primitive::<Int32Type>()
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(values, &[Some(0), Some(1), None, None])
    }

    #[test]
    fn test_take_bytes_null_indices() {
        let indices = Int32Array::new(
            vec![0, 1, 400, 400].into(),
            Some(NullBuffer::from_iter(vec![true, true, false, false])),
        );
        let values = StringArray::from(vec![Some("foo"), None]);
        let r = take(&values, &indices, None).unwrap();
        let values = r.as_string::<i32>().iter().collect::<Vec<_>>();
        assert_eq!(&values, &[Some("foo"), None, None, None])
    }

    #[test]
    fn test_take_union_sparse() {
        let structs = create_test_struct(vec![
            Some((Some(true), Some(42))),
            Some((Some(false), Some(28))),
            Some((Some(false), Some(19))),
            Some((Some(true), Some(31))),
            None,
        ]);
        let strings = StringArray::from(vec![Some("a"), None, Some("c"), None, Some("d")]);
        let type_ids = [1; 5].into_iter().collect::<ScalarBuffer<i8>>();

        let union_fields = [
            (
                0,
                Arc::new(Field::new("f1", structs.data_type().clone(), true)),
            ),
            (
                1,
                Arc::new(Field::new("f2", strings.data_type().clone(), true)),
            ),
        ]
        .into_iter()
        .collect();
        let children = vec![Arc::new(structs) as Arc<dyn Array>, Arc::new(strings)];
        let array = UnionArray::try_new(union_fields, type_ids, None, children).unwrap();

        let indices = vec![0, 3, 1, 0, 2, 4];
        let index = UInt32Array::from(indices.clone());
        let actual = take(&array, &index, None).unwrap();
        let actual = actual.as_any().downcast_ref::<UnionArray>().unwrap();
        let strings = actual.child(1);
        let strings = strings.as_any().downcast_ref::<StringArray>().unwrap();

        let actual = strings.iter().collect::<Vec<_>>();
        let expected = vec![Some("a"), None, None, Some("a"), Some("c"), Some("d")];
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_take_union_dense() {
        let type_ids = vec![0, 1, 1, 0, 0, 1, 0];
        let offsets = vec![0, 0, 1, 1, 2, 2, 3];
        let ints = vec![10, 20, 30, 40];
        let strings = vec![Some("a"), None, Some("c"), Some("d")];

        let indices = vec![0, 3, 1, 0, 2, 4];

        let taken_type_ids = vec![0, 0, 1, 0, 1, 0];
        let taken_offsets = vec![0, 1, 0, 2, 1, 3];
        let taken_ints = vec![10, 20, 10, 30];
        let taken_strings = vec![Some("a"), None];

        let type_ids = <ScalarBuffer<i8>>::from(type_ids);
        let offsets = <ScalarBuffer<i32>>::from(offsets);
        let ints = UInt32Array::from(ints);
        let strings = StringArray::from(strings);

        let union_fields = [
            (
                0,
                Arc::new(Field::new("f1", ints.data_type().clone(), true)),
            ),
            (
                1,
                Arc::new(Field::new("f2", strings.data_type().clone(), true)),
            ),
        ]
        .into_iter()
        .collect();

        let array = UnionArray::try_new(
            union_fields,
            type_ids,
            Some(offsets),
            vec![Arc::new(ints), Arc::new(strings)],
        )
        .unwrap();

        let index = UInt32Array::from(indices);

        let actual = take(&array, &index, None).unwrap();
        let actual = actual.as_any().downcast_ref::<UnionArray>().unwrap();

        assert_eq!(actual.offsets(), Some(&ScalarBuffer::from(taken_offsets)));
        assert_eq!(actual.type_ids(), &ScalarBuffer::from(taken_type_ids));
        assert_eq!(
            UInt32Array::from(actual.child(0).to_data()),
            UInt32Array::from(taken_ints)
        );
        assert_eq!(
            StringArray::from(actual.child(1).to_data()),
            StringArray::from(taken_strings)
        );
    }

    #[test]
    fn test_take_union_dense_using_builder() {
        let mut builder = UnionBuilder::new_dense();

        builder.append::<Int32Type>("a", 1).unwrap();
        builder.append::<Float64Type>("b", 3.0).unwrap();
        builder.append::<Int32Type>("a", 4).unwrap();
        builder.append::<Int32Type>("a", 5).unwrap();
        builder.append::<Float64Type>("b", 2.0).unwrap();

        let union = builder.build().unwrap();

        let indices = UInt32Array::from(vec![2, 0, 1, 2]);

        let mut builder = UnionBuilder::new_dense();

        builder.append::<Int32Type>("a", 4).unwrap();
        builder.append::<Int32Type>("a", 1).unwrap();
        builder.append::<Float64Type>("b", 3.0).unwrap();
        builder.append::<Int32Type>("a", 4).unwrap();

        let taken = builder.build().unwrap();

        assert_eq!(
            taken.to_data(),
            take(&union, &indices, None).unwrap().to_data()
        );
    }

    #[test]
    fn test_take_union_dense_all_match_issue_6206() {
        let fields = UnionFields::new(vec![0], vec![Field::new("a", DataType::Int64, false)]);
        let ints = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]));

        let array = UnionArray::try_new(
            fields,
            ScalarBuffer::from(vec![0_i8, 0, 0, 0, 0]),
            Some(ScalarBuffer::from_iter(0_i32..5)),
            vec![ints],
        )
        .unwrap();

        let indicies = Int64Array::from(vec![0, 2, 4]);
        let array = take(&array, &indicies, None).unwrap();
        assert_eq!(array.len(), 3);
    }
}
