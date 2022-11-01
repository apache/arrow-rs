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

use arrow_array::builder::{BooleanBufferBuilder, BufferBuilder};
use arrow_array::types::*;
use arrow_array::*;
use arrow_buffer::{ArrowNativeType, Buffer, MutableBuffer};
use arrow_data::transform::MutableArrayData;
use arrow_data::ArrayDataBuilder;
use arrow_schema::{ArrowError, DataType};
use std::sync::Arc;

macro_rules! primitive_helper {
    ($t:ty, $values:ident, $indices:ident, $data_type:ident) => {
        interleave_primitive::<$t>($values, $indices, $data_type)
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
/// For selecting values by index from a single array see [`crate::interleave`]
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
            return Err(ArrowError::InvalidArgumentError(
                format!("It is not possible to interleave arrays of different data types ({} and {})",
              data_type, array.data_type()),
            ));
        }
    }

    if indices.is_empty() {
        return Ok(new_empty_array(data_type));
    }

    downcast_primitive! {
        data_type => (primitive_helper, values, indices, data_type),
        DataType::Utf8 => interleave_bytes::<Utf8Type>(values, indices),
        DataType::LargeUtf8 => interleave_bytes::<LargeUtf8Type>(values, indices),
        DataType::Binary => interleave_bytes::<BinaryType>(values, indices),
        DataType::LargeBinary => interleave_bytes::<LargeBinaryType>(values, indices),
        _ => interleave_fallback(values, indices)
    }
}

/// Common functionality for interleaving arrays
///
/// T is the concrete Array type
struct Interleave<'a, T> {
    /// The input arrays downcast to T
    arrays: Vec<&'a T>,
    /// The number of nulls in the interleaved output
    null_count: usize,
    /// The null buffer of the interleaved output
    nulls: Option<Buffer>,
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

        let mut null_count = 0;
        let nulls = has_nulls.then(|| {
            let mut builder = BooleanBufferBuilder::new(indices.len());
            for (a, b) in indices {
                let v = arrays[*a].is_valid(*b);
                null_count += !v as usize;
                builder.append(v)
            }
            builder.finish()
        });

        Self {
            arrays,
            null_count,
            nulls,
        }
    }
}

fn interleave_primitive<T: ArrowPrimitiveType>(
    values: &[&dyn Array],
    indices: &[(usize, usize)],
    data_type: &DataType,
) -> Result<ArrayRef, ArrowError> {
    let interleaved = Interleave::<'_, PrimitiveArray<T>>::new(values, indices);

    let mut values = BufferBuilder::<T::Native>::new(indices.len());
    for (a, b) in indices {
        let v = interleaved.arrays[*a].value(*b);
        values.append(v)
    }

    let builder = ArrayDataBuilder::new(data_type.clone())
        .len(indices.len())
        .add_buffer(values.finish())
        .null_bit_buffer(interleaved.nulls)
        .null_count(interleaved.null_count);

    let data = unsafe { builder.build_unchecked() };
    Ok(Arc::new(PrimitiveArray::<T>::from(data)))
}

fn interleave_bytes<T: ByteArrayType>(
    values: &[&dyn Array],
    indices: &[(usize, usize)],
) -> Result<ArrayRef, ArrowError> {
    let interleaved = Interleave::<'_, GenericByteArray<T>>::new(values, indices);

    let mut capacity = 0;
    let mut offsets = BufferBuilder::<T::Offset>::new(indices.len() + 1);
    offsets.append(T::Offset::from_usize(0).unwrap());
    for (a, b) in indices {
        let o = interleaved.arrays[*a].value_offsets();
        let element_len = o[*b + 1].as_usize() - o[*b].as_usize();
        capacity += element_len;
        offsets.append(T::Offset::from_usize(capacity).expect("overflow"));
    }

    let mut values = MutableBuffer::new(capacity);
    for (a, b) in indices {
        values.extend_from_slice(interleaved.arrays[*a].value(*b).as_ref());
    }

    let builder = ArrayDataBuilder::new(T::DATA_TYPE)
        .len(indices.len())
        .add_buffer(offsets.finish())
        .add_buffer(values.into())
        .null_bit_buffer(interleaved.nulls)
        .null_count(interleaved.null_count);

    let data = unsafe { builder.build_unchecked() };
    Ok(Arc::new(GenericByteArray::<T>::from(data)))
}

/// Fallback implementation of interleave using [`MutableArrayData`]
fn interleave_fallback(
    values: &[&dyn Array],
    indices: &[(usize, usize)],
) -> Result<ArrayRef, ArrowError> {
    let arrays: Vec<_> = values.iter().map(|x| x.data()).collect();
    let mut array_data = MutableArrayData::new(arrays, false, indices.len());

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
        array_data.extend(cur_array, start_row_idx, end_row_idx);

        // start new batch of rows
        cur_array = array;
        start_row_idx = row;
        end_row_idx = start_row_idx + 1;
    }

    // emit final batch of rows
    array_data.extend(cur_array, start_row_idx, end_row_idx);
    Ok(make_array(array_data.freeze()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::{Int32Builder, ListBuilder};
    use arrow_array::cast::{as_primitive_array, as_string_array};
    use arrow_array::types::Int32Type;
    use arrow_array::{Int32Array, ListArray, StringArray};
    use arrow_schema::DataType;

    #[test]
    fn test_primitive() {
        let a = Int32Array::from_iter_values([1, 2, 3, 4]);
        let b = Int32Array::from_iter_values([5, 6, 7]);
        let c = Int32Array::from_iter_values([8, 9, 10]);
        let values =
            interleave(&[&a, &b, &c], &[(0, 3), (0, 3), (2, 2), (2, 0), (1, 1)]).unwrap();
        let v = as_primitive_array::<Int32Type>(&values);
        assert_eq!(v.values(), &[4, 4, 10, 8, 6]);
    }

    #[test]
    fn test_primitive_nulls() {
        let a = Int32Array::from_iter_values([1, 2, 3, 4]);
        let b = Int32Array::from_iter([Some(1), Some(4), None]);
        let values =
            interleave(&[&a, &b], &[(0, 1), (1, 2), (1, 2), (0, 3), (0, 2)]).unwrap();
        let v: Vec<_> = as_primitive_array::<Int32Type>(&values)
            .into_iter()
            .collect();
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
        let values =
            interleave(&[&a, &b], &[(0, 2), (0, 2), (1, 0), (1, 1), (0, 1)]).unwrap();
        let v = as_string_array(&values);
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
    fn test_lists() {
        // [[1, 2], null, [3]]
        let mut a = ListBuilder::new(Int32Builder::new());
        a.values().append_value(1);
        a.values().append_value(2);
        a.append(true);
        a.append(false);
        a.values().append_value(3);
        a.append(true);
        let a = a.finish();

        // [[4], null, [5, 6, null]]
        let mut b = ListBuilder::new(Int32Builder::new());
        b.values().append_value(4);
        b.append(true);
        b.append(false);
        b.values().append_value(5);
        b.values().append_value(6);
        b.values().append_null();
        b.append(true);
        let b = b.finish();

        let values =
            interleave(&[&a, &b], &[(0, 2), (0, 1), (1, 0), (1, 2), (1, 1)]).unwrap();
        let v = values.as_any().downcast_ref::<ListArray>().unwrap();

        // [[3], null, [4], [5, 6, null], null]
        let mut expected = ListBuilder::new(Int32Builder::new());
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
}
