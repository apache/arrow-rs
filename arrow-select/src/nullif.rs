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

//! Implements the `nullif` function for Arrow arrays.

/*
 * NULLIF Implementation Contract
 *
 * For any ArrayData:
 * len = data.len()            // logical elements
 * offset = data.offset()      // logical starting index into buffers
 *
 * Validity bitmap (if present) is a Buffer B.
 * Invariant:
 *   Logical index i in [0, len) is valid iff get_bit(B, offset + i) == true.
 *
 * For the result of nullif:
 * We will build a fresh ArrayData with offset = 0.
 * For that result:
 *   Logical index i is valid iff get_bit(result_validity, i) == true.
 *   Values buffer is laid out so element 0 is first result value, etc.
 *
 * For nullif semantics:
 * Let V(i) = left is valid at i
 *     C(i) = condition "nullify at i" is true (depends on left, right, type)
 * Then:
 *   result_valid(i) = V(i) & !C(i)
 *   result_value(i) = left_value(i)    // when result_valid(i) == true
 */

use arrow_array::{Array, ArrayRef, BooleanArray, make_array};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer};
use arrow_data::{ArrayData, ArrayDataBuilder};
use arrow_schema::{ArrowError, DataType};

fn get_element_size(data_type: &DataType) -> usize {
    match data_type {
        DataType::Boolean => 1,
        DataType::Int8 | DataType::UInt8 => 1,
        DataType::Int16 | DataType::UInt16 => 2,
        DataType::Int32 | DataType::UInt32 | DataType::Float32 => 4,
        DataType::Int64 | DataType::UInt64 | DataType::Float64 => 8,
        _ => panic!("unsupported"),
    }
}

/// Returns a new array with the same values and the validity bit to false where
/// the corresponding element of`right` is true.
///
/// This can be used to implement SQL `NULLIF`
///
/// # Example
/// ```
/// # use arrow_array::{Int32Array, BooleanArray};
/// # use arrow_array::cast::AsArray;
/// # use arrow_array::types::Int32Type;
/// # use arrow_select::nullif::nullif;
/// // input is [null, 8, 1, 9]
/// let a = Int32Array::from(vec![None, Some(8), Some(1), Some(9)]);
/// // use nullif to set index 1 to null
/// let bool_array = BooleanArray::from(vec![Some(false), Some(true), Some(false), None]);
/// let nulled = nullif(&a, &bool_array).unwrap();
/// // The resulting array is [null, null, 1, 9]
/// assert_eq!(nulled.as_primitive(), &Int32Array::from(vec![None, None, Some(1), Some(9)]));
/// ```
pub fn nullif(left: &dyn Array, right: &BooleanArray) -> Result<ArrayRef, ArrowError> {
    let left_data = left.to_data();

    if left_data.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform comparison operation on arrays of different length".to_string(),
        ));
    }
    let len = left_data.len();

    if len == 0 || left_data.data_type() == &DataType::Null {
        return Ok(make_array(left_data));
    }

    let cond_mask = match right.nulls() {
        Some(nulls) => right.values() & nulls.inner(),
        None => right.values().clone(),
    };
    let cond_offset = cond_mask.offset();

    let validity = if let Some(left_nulls) = left_data.nulls() {
        let left_valid = left_nulls.inner().inner();
        let left_bit_offset = left_nulls.inner().offset();
        compute_nullif_validity(&left_valid, left_bit_offset, &cond_mask.inner(), cond_offset, len)
    } else {
        // left is all valid
        let left_valid_len = (len + 7) / 8;
        let mut left_valid_data = vec![255u8; left_valid_len];
        if len % 8 != 0 {
            let mask = (1u8 << (len % 8)) - 1;
            left_valid_data[left_valid_len - 1] = mask;
        }
        let left_valid = Buffer::from(left_valid_data);
        let left_bit_offset = 0;
        compute_nullif_validity(&left_valid, left_bit_offset, &cond_mask.inner(), cond_offset, len)
    };

    let (null_buffer, _) = compute_null_buffer(&validity, len);
    let nulls = NullBuffer::new(null_buffer);

    let data_without_nulls = copy_array_data_with_offset_zero(&left_data)?;
    let data = ArrayDataBuilder::new(left_data.data_type().clone())
        .len(len)
        .nulls(Some(nulls))
        .offset(0)
        .buffers(data_without_nulls.buffers().to_vec())
        .child_data(data_without_nulls.child_data().to_vec())
        .build()
        .unwrap();

    Ok(make_array(data))
}

/// Computes the null buffer and null count from the validity buffer.
///
/// The null buffer has bits set where validity is 1 (not null).
/// The null count is computed as len - number of set bits in validity.
fn compute_null_buffer(validity: &Buffer, len: usize) -> (BooleanBuffer, usize) {
    let null_buffer = BooleanBuffer::new(validity.clone(), 0, len);
    let null_count = len - validity.count_set_bits_offset(0, len);
    (null_buffer, null_count)
}

/// Computes the NULLIF validity bitmap from the left array's validity and
/// a boolean condition mask.
///
/// Invariants used here:
/// - For any `ArrayData` with `len = L` and `offset = O`, logical index `i`
///   (0 <= i < L) is valid iff `get_bit(validity, O + i)` is true.
/// - Let `V(i)` be the left array's validity at logical index `i`.
/// - Let `C(i)` be the boolean condition at logical index `i`, where `true`
///   means "nullify this position".
/// - NULLIF result validity is defined as:
///       result_valid(i) = V(i) & !C(i)
///   for `i` in `0..len`.
/// - The result array is built with `offset = 0`, so bit `i` in the result
///   validity bitmap corresponds directly to logical index `i`.
fn compute_nullif_validity(
    left_valid: &Buffer,
    left_offset: usize,
    cond_mask: &Buffer,
    cond_offset: usize,
    len: usize,
) -> Buffer {
    left_valid.bitwise_binary(cond_mask, left_offset, cond_offset, len, |l, c| l & !c)
}

/// Creates a new ArrayData with offset=0 by slicing the buffers from the given ArrayData.
/// This ensures the result ArrayData has offset=0 as per the nullif contract.
fn copy_array_data_with_offset_zero(left_data: &ArrayData) -> Result<ArrayData, ArrowError> {
    let len = left_data.len();
    let offset = left_data.offset();
    let data_type = left_data.data_type();
    let mut buffers = Vec::new();
    let mut child_data = Vec::new();

    match data_type {
        DataType::Boolean => {
            let values = left_data.buffers()[0].bitwise_unary(offset, len, |b| b);
            buffers.push(values);
        }
        DataType::Int8 | DataType::UInt8 | DataType::Int16 | DataType::UInt16 |
        DataType::Int32 | DataType::UInt32 | DataType::Float32 |
        DataType::Int64 | DataType::UInt64 | DataType::Float64 => {
            let element_size = get_element_size(data_type);
            let start = offset * element_size;
            let end = start + len * element_size;
            let values = Buffer::from(&left_data.buffers()[0].as_slice()[start..end]);
            buffers.push(values);
        }
        DataType::Utf8 => {
            let offsets_buf = &left_data.buffers()[0];
            let data_buf = &left_data.buffers()[1];
            let offsets_start = offset;
            let offsets_end = offset + len + 1;
            let offsets_slice = &offsets_buf.as_slice()[offsets_start * 4..offsets_end * 4];
            let base_offset = i32::from_le_bytes(offsets_slice[0..4].try_into().unwrap()) as usize;
            let data_end = i32::from_le_bytes(offsets_slice[offsets_slice.len() - 4..].try_into().unwrap()) as usize;
            let data_slice = &data_buf.as_slice()[base_offset..data_end];
            let mut new_offsets = Vec::with_capacity(len + 1);
            for i in 0..=len {
                let old_offset = i32::from_le_bytes(offsets_slice[i * 4..(i + 1) * 4].try_into().unwrap()) as usize;
                new_offsets.push((old_offset - base_offset) as i32);
            }
            let new_offsets_buf = Buffer::from(new_offsets.into_iter().flat_map(|x| x.to_le_bytes()).collect::<Vec<u8>>());
            buffers.push(new_offsets_buf);
            buffers.push(Buffer::from(data_slice));
        }
        DataType::LargeUtf8 => {
            let offsets_buf = &left_data.buffers()[0];
            let data_buf = &left_data.buffers()[1];
            let offsets_start = offset;
            let offsets_end = offset + len + 1;
            let offsets_slice = &offsets_buf.as_slice()[offsets_start * 8..offsets_end * 8];
            let base_offset = i64::from_le_bytes(offsets_slice[0..8].try_into().unwrap()) as usize;
            let data_end = i64::from_le_bytes(offsets_slice[offsets_slice.len() - 8..].try_into().unwrap()) as usize;
            let data_slice = &data_buf.as_slice()[base_offset..data_end];
            let mut new_offsets = Vec::with_capacity(len + 1);
            for i in 0..=len {
                let old_offset = i64::from_le_bytes(offsets_slice[i * 8..(i + 1) * 8].try_into().unwrap()) as usize;
                new_offsets.push((old_offset - base_offset) as i64);
            }
            let new_offsets_buf = Buffer::from(new_offsets.into_iter().flat_map(|x| x.to_le_bytes()).collect::<Vec<u8>>());
            buffers.push(new_offsets_buf);
            buffers.push(Buffer::from(data_slice));
        }
        DataType::Struct(_) => {
            // No buffers for struct
            child_data = left_data.child_data().iter()
                .map(|child| copy_array_data_with_offset_zero(child))
                .collect::<Result<Vec<_>, _>>()?;
        }
        _ => {
            // For other types, copy buffers as is and keep offset (for now, to avoid breaking)
            // TODO: implement slicing for other types like List, Binary, etc.
            buffers = left_data.buffers().to_vec();
            child_data = left_data.child_data().to_vec();
            return Ok(ArrayDataBuilder::new(data_type.clone())
                .len(len)
                .offset(offset)
                .buffers(buffers)
                .child_data(child_data)
                .build()?);
        }
    }

    Ok(ArrayDataBuilder::new(data_type.clone())
        .len(len)
        .offset(0)
        .buffers(buffers)
        .child_data(child_data)
        .build()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_buffer::bit_util::get_bit;
    use arrow_array::builder::{BooleanBuilder, Int32Builder, StructBuilder};
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int32Type;
    use arrow_array::{Int32Array, NullArray, StringArray, StructArray};
    use arrow_data::ArrayData;
    use arrow_schema::{Field, Fields};
    use rand::{Rng, rng};

    #[test]
    fn test_nullif_int_array() {
        let a = Int32Array::from(vec![Some(15), None, Some(8), Some(1), Some(9)]);
        let comp = BooleanArray::from(vec![Some(false), None, Some(true), Some(false), None]);
        let res = nullif(&a, &comp).unwrap();

        let expected = Int32Array::from(vec![
            Some(15),
            None,
            None, // comp true, slot 2 turned into null
            Some(1),
            // Even though comp array / right is null, should still pass through original value
            // comp true, slot 2 turned into null
            Some(9),
        ]);

        let res = res.as_primitive::<Int32Type>();
        assert_eq!(&expected, res);
    }

    #[test]
    fn test_nullif_null_array() {
        assert_eq!(
            nullif(&NullArray::new(0), &BooleanArray::new_null(0))
                .unwrap()
                .as_ref(),
            &NullArray::new(0)
        );

        assert_eq!(
            nullif(
                &NullArray::new(3),
                &BooleanArray::from(vec![Some(false), Some(true), None]),
            )
            .unwrap()
            .as_ref(),
            &NullArray::new(3)
        );
    }

    #[test]
    fn test_nullif_int_array_offset() {
        let a = Int32Array::from(vec![None, Some(15), Some(8), Some(1), Some(9)]);
        let a = a.slice(1, 3); // Some(15), Some(8), Some(1)
        let a = a.as_any().downcast_ref::<Int32Array>().unwrap();
        let comp = BooleanArray::from(vec![
            Some(false),
            Some(false),
            Some(false),
            None,
            Some(true),
            Some(false),
            None,
        ]);
        let comp = comp.slice(2, 3); // Some(false), None, Some(true)
        let comp = comp.as_any().downcast_ref::<BooleanArray>().unwrap();
        let res = nullif(a, comp).unwrap();

        let expected = Int32Array::from(vec![
            Some(15), // False => keep it
            Some(8),  // None => keep it
            None,     // true => None
        ]);
        let res = res.as_primitive::<Int32Type>();
        assert_eq!(&expected, res)
    }

    #[test]
    fn test_nullif_string() {
        let s = StringArray::from_iter([
            Some("hello"),
            None,
            Some("world"),
            Some("a"),
            Some("b"),
            None,
            None,
        ]);
        let select = BooleanArray::from_iter([
            Some(true),
            Some(true),
            Some(false),
            Some(true),
            Some(false),
            Some(false),
            None,
        ]);

        let a = nullif(&s, &select).unwrap();
        let r: Vec<_> = a.as_string::<i32>().iter().collect();
        assert_eq!(
            r,
            vec![None, None, Some("world"), None, Some("b"), None, None]
        );

        let s = s.slice(2, 3);
        let select = select.slice(1, 3);
        let a = nullif(&s, &select).unwrap();
        let r: Vec<_> = a.as_string::<i32>().iter().collect();
        assert_eq!(r, vec![None, Some("a"), None]);
    }

    #[test]
    fn test_nullif_int_large_left_offset() {
        let a = Int32Array::from(vec![
            Some(-1), // 0
            Some(-1),
            Some(-1),
            Some(-1),
            Some(-1),
            Some(-1),
            Some(-1),
            Some(-1),
            Some(-1), // 8
            Some(-1),
            Some(-1),
            Some(-1),
            Some(-1),
            Some(-1),
            Some(-1),
            Some(-1),
            None,     // 16
            Some(15), // 17
            Some(8),
            Some(1),
            Some(9),
        ]);
        let a = a.slice(17, 3); // Some(15), Some(8), Some(1)

        let comp = BooleanArray::from(vec![
            Some(false),
            Some(false),
            Some(false),
            None,
            Some(true),
            Some(false),
            None,
        ]);
        let comp = comp.slice(2, 3); // Some(false), None, Some(true)
        let comp = comp.as_any().downcast_ref::<BooleanArray>().unwrap();
        let res = nullif(&a, comp).unwrap();
        let res = res.as_any().downcast_ref::<Int32Array>().unwrap();

        let expected = Int32Array::from(vec![
            Some(15), // False => keep it
            Some(8),  // None => keep it
            None,     // true => None
        ]);
        assert_eq!(&expected, res)
    }

    #[test]
    fn test_nullif_int_large_right_offset() {
        let a = Int32Array::from(vec![
            None,     // 0
            Some(15), // 1
            Some(8),
            Some(1),
            Some(9),
        ]);
        let a = a.slice(1, 3); // Some(15), Some(8), Some(1)

        let comp = BooleanArray::from(vec![
            Some(false), // 0
            Some(false),
            Some(false),
            Some(false),
            Some(false),
            Some(false),
            Some(false),
            Some(false),
            Some(false), // 8
            Some(false),
            Some(false),
            Some(false),
            Some(false),
            Some(false),
            Some(false),
            Some(false),
            Some(false), // 16
            Some(false), // 17
            Some(false), // 18
            None,
            Some(true),
            Some(false),
            None,
        ]);
        let comp = comp.slice(18, 3); // Some(false), None, Some(true)
        let comp = comp.as_any().downcast_ref::<BooleanArray>().unwrap();
        let res = nullif(&a, comp).unwrap();
        let res = res.as_any().downcast_ref::<Int32Array>().unwrap();

        let expected = Int32Array::from(vec![
            Some(15), // False => keep it
            Some(8),  // None => keep it
            None,     // true => None
        ]);
        assert_eq!(&expected, res)
    }

    #[test]
    fn test_nullif_boolean_offset() {
        let a = BooleanArray::from(vec![
            None,       // 0
            Some(true), // 1
            Some(false),
            Some(true),
            Some(true),
        ]);
        let a = a.slice(1, 3); // Some(true), Some(false), Some(true)

        let comp = BooleanArray::from(vec![
            Some(false), // 0
            Some(false), // 1
            Some(false), // 2
            None,
            Some(true),
            Some(false),
            None,
        ]);
        let comp = comp.slice(2, 3); // Some(false), None, Some(true)
        let comp = comp.as_any().downcast_ref::<BooleanArray>().unwrap();
        let res = nullif(&a, comp).unwrap();
        let res = res.as_any().downcast_ref::<BooleanArray>().unwrap();

        let expected = BooleanArray::from(vec![
            Some(true),  // False => keep it
            Some(false), // None => keep it
            None,        // true => None
        ]);
        assert_eq!(&expected, res)
    }

    struct Foo {
        a: Option<i32>,
        b: Option<bool>,
        /// Whether the entry should be valid.
        is_valid: bool,
    }

    impl Foo {
        fn new_valid(a: i32, b: bool) -> Foo {
            Self {
                a: Some(a),
                b: Some(b),
                is_valid: true,
            }
        }

        fn new_null() -> Foo {
            Self {
                a: None,
                b: None,
                is_valid: false,
            }
        }
    }

    /// Struct Array equality is a bit weird -- we need to have the *child values*
    /// correct even if the enclosing struct indicates it is null. But we
    /// also need the top level is_valid bits to be correct.
    fn create_foo_struct(values: Vec<Foo>) -> StructArray {
        let mut struct_array = StructBuilder::new(
            Fields::from(vec![
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Boolean, true),
            ]),
            vec![
                Box::new(Int32Builder::with_capacity(values.len())),
                Box::new(BooleanBuilder::with_capacity(values.len())),
            ],
        );

        for value in values {
            struct_array
                .field_builder::<Int32Builder>(0)
                .unwrap()
                .append_option(value.a);
            struct_array
                .field_builder::<BooleanBuilder>(1)
                .unwrap()
                .append_option(value.b);
            struct_array.append(value.is_valid);
        }

        struct_array.finish()
    }

    #[test]
    fn test_nullif_struct_slices() {
        let struct_array = create_foo_struct(vec![
            Foo::new_valid(7, true),
            Foo::new_valid(15, false),
            Foo::new_valid(8, true),
            Foo::new_valid(12, false),
            Foo::new_null(),
            Foo::new_null(),
            Foo::new_valid(42, true),
        ]);

        // Some({a: 15, b: false}), Some({a: 8, b: true}), Some({a: 12, b: false}),
        // None, None
        let struct_array = struct_array.slice(1, 5);
        let comp = BooleanArray::from(vec![
            Some(false), // 0
            Some(false), // 1
            Some(false), // 2
            None,
            Some(true),
            Some(false),
            None,
        ]);
        let comp = comp.slice(2, 5); // Some(false), None, Some(true), Some(false), None
        let comp = comp.as_any().downcast_ref::<BooleanArray>().unwrap();
        let res = nullif(&struct_array, comp).unwrap();
        let res = res.as_any().downcast_ref::<StructArray>().unwrap();

        let expected = create_foo_struct(vec![
            // Some(false) -> keep
            Foo::new_valid(15, false),
            // None -> keep
            Foo::new_valid(8, true),
            // Some(true) -> null out. But child values are still there.
            Foo {
                a: Some(12),
                b: Some(false),
                is_valid: false,
            },
            // Some(false) -> keep, but was null
            Foo::new_null(),
            // None -> keep, but was null
            Foo::new_null(),
        ]);

        assert_eq!(&expected, res);
    }

    #[test]
    fn test_nullif_no_nulls() {
        let a = Int32Array::from(vec![Some(15), Some(7), Some(8), Some(1), Some(9)]);
        let comp = BooleanArray::from(vec![Some(false), None, Some(true), Some(false), None]);
        let res = nullif(&a, &comp).unwrap();
        let res = res.as_primitive::<Int32Type>();

        let expected = Int32Array::from(vec![Some(15), Some(7), None, Some(1), Some(9)]);
        assert_eq!(res, &expected);
    }

    #[test]
    fn nullif_empty() {
        let a = Int32Array::from(ArrayData::new_empty(&DataType::Int32));
        let mask = BooleanArray::from(ArrayData::new_empty(&DataType::Boolean));
        let res = nullif(&a, &mask).unwrap();
        assert_eq!(res.as_ref(), &a);
    }

    fn test_nullif(values: &Int32Array, filter: &BooleanArray) {
        let expected: Int32Array = values
            .iter()
            .zip(filter.iter())
            .map(|(a, b)| match b {
                Some(true) => None,
                Some(false) | None => a,
            })
            .collect();

        let r = nullif(values, filter).unwrap();
        let r_data = r.to_data();
        r_data.validate().unwrap();

        assert_eq!(r.as_ref(), &expected);
    }

    #[test]
    fn nullif_fuzz() {
        let mut rng = rng();

        let arrays = [
            Int32Array::from(vec![0; 128]),
            (0..128)
                .map(|_| rng.random_bool(0.5).then_some(0))
                .collect(),
        ];

        for a in arrays {
            let a_slices = [(0, 128), (64, 64), (0, 64), (32, 32), (0, 0), (32, 0)];

            for (a_offset, a_length) in a_slices {
                let a = a.slice(a_offset, a_length);

                for i in 1..65 {
                    let b_start_offset = rng.random_range(0..i);
                    let b_end_offset = rng.random_range(0..i);

                    let b: BooleanArray = (0..a_length + b_start_offset + b_end_offset)
                        .map(|_| rng.random_bool(0.5).then(|| rng.random_bool(0.5)))
                        .collect();
                    let b = b.slice(b_start_offset, a_length);

                    test_nullif(&a, &b);
                }
            }
        }
    }

    #[test]
    fn test_nullif_with_offsets_and_nulls() {
        // Build arrays using builder to ensure correct null count
        let array = Int32Array::from(vec![Some(15), None, Some(8), Some(1), Some(9)]);

        let condition = BooleanArray::from(vec![Some(false), None, Some(true), Some(false), None]);

        // Test on full arrays
        let result_full = nullif(&array, &condition).unwrap();
        let expected_full = Int32Array::from(vec![
            Some(15), // false -> keep
            None,     // null condition -> keep (was null)
            None,     // true -> null
            Some(1),  // false -> keep
            Some(9),  // null condition -> keep
        ]);
        assert_eq!(result_full.as_primitive::<Int32Type>(), &expected_full);

        // Test on sliced arrays
        let array_sliced = array.slice(1, 3); // None, Some(8), Some(1)
        let condition_sliced = condition.slice(1, 3); // None, Some(true), Some(false)
        let result_sliced = nullif(&array_sliced, &condition_sliced).unwrap();

        let expected_sliced = Int32Array::from(vec![
            None,    // null condition -> keep (was null)
            None,    // true -> null
            Some(1), // false -> keep
        ]);
        assert_eq!(result_sliced.as_primitive::<Int32Type>(), &expected_sliced);

        // Test with different condition
        let array2 = Int32Array::from(vec![Some(10), None, Some(20), Some(30), None]);

        let condition2 = BooleanArray::from(vec![Some(false), Some(true), Some(false), Some(true), Some(false)]);

        let result = nullif(&array2, &condition2).unwrap();
        let expected = Int32Array::from(vec![
            Some(10), // false -> keep
            None,     // true -> null (was null)
            Some(20), // false -> keep
            None,     // true -> null
            None,     // false -> keep (was null)
        ]);
        assert_eq!(result.as_primitive::<Int32Type>(), &expected);
    }

    #[test]
    fn test_nullif_null_count_and_offsets_regressions() {
        // Construct a small Int32Array with nulls
        let values = Int32Array::from(vec![Some(1), None, Some(2), Some(1), None, Some(3)]);
        // Construct a "condition" array (equals values for nullif)
        let equals = BooleanArray::from(vec![Some(true), Some(false), None, Some(true), Some(false), None]);

        // Helper to build expected Vec<Option<i32>>
        fn build_expected(values: &[Option<i32>], equals: &[Option<bool>]) -> Vec<Option<i32>> {
            values.iter().zip(equals.iter()).map(|(&v, &e)| {
                if v.is_none() {
                    None
                } else if e == Some(true) {
                    None
                } else {
                    v
                }
            }).collect()
        }

        // Test full arrays
        let result_full = nullif(&values, &equals).unwrap();
        assert_eq!(result_full.len(), values.len());
        let actual_nulls: Vec<Option<i32>> = result_full.as_primitive::<Int32Type>().iter().collect();
        let expected_full = build_expected(&[Some(1), None, Some(2), Some(1), None, Some(3)], &[Some(true), Some(false), None, Some(true), Some(false), None]);
        assert_eq!(actual_nulls, expected_full);
        assert_eq!(result_full.null_count(), actual_nulls.iter().filter(|x| x.is_none()).count());

        // Test sliced arrays: values.slice(1, 4) and equals.slice(1, 4)
        // values[1..5]: None, Some(2), Some(1), None
        // equals[1..5]: Some(false), None, Some(true), Some(false)
        let values_slice1 = values.slice(1, 4);
        let equals_slice1 = equals.slice(1, 4);
        let result_slice1 = nullif(&values_slice1, &equals_slice1).unwrap();
        assert_eq!(result_slice1.len(), 4);
        let actual_slice1: Vec<Option<i32>> = result_slice1.as_primitive::<Int32Type>().iter().collect();
        let expected_slice1 = build_expected(&[None, Some(2), Some(1), None], &[Some(false), None, Some(true), Some(false)]);
        assert_eq!(actual_slice1, expected_slice1);
        assert_eq!(result_slice1.null_count(), actual_slice1.iter().filter(|x| x.is_none()).count());

        // Test another slice: values.slice(2, 3) and equals.slice(2, 3)
        // values[2..5]: Some(2), Some(1), None
        // equals[2..5]: None, Some(true), Some(false)
        let values_slice2 = values.slice(2, 3);
        let equals_slice2 = equals.slice(2, 3);
        let result_slice2 = nullif(&values_slice2, &equals_slice2).unwrap();
        assert_eq!(result_slice2.len(), 3);
        let actual_slice2: Vec<Option<i32>> = result_slice2.as_primitive::<Int32Type>().iter().collect();
        let expected_slice2 = build_expected(&[Some(2), Some(1), None], &[None, Some(true), Some(false)]);
        assert_eq!(actual_slice2, expected_slice2);
        assert_eq!(result_slice2.null_count(), actual_slice2.iter().filter(|x| x.is_none()).count());
    }

    #[test]
    fn test_nullif_boolean_offsets_regression() {
        // Use BooleanArray to reproduce BooleanBuffer slicing issues
        let values = BooleanArray::from(vec![Some(true), None, Some(false), Some(true), None, Some(false), Some(true), Some(false)]);
        let equals = BooleanArray::from(vec![Some(false), Some(true), None, Some(true), Some(false), None, Some(true), Some(false)]);

        // Helper to build expected Vec<Option<bool>>
        fn build_expected_bool(values: &[Option<bool>], equals: &[Option<bool>]) -> Vec<Option<bool>> {
            values.iter().zip(equals.iter()).map(|(&v, &e)| {
                if v.is_none() {
                    None
                } else if e == Some(true) {
                    None
                } else {
                    v
                }
            }).collect()
        }

        // Test slices that cross byte boundaries (e.g., offset=1,len=5; offset=3,len=5; offset=7,len=1)
        // Slice 1: offset=1, len=5 (values[1..6]: None, Some(false), Some(true), None, Some(false))
        // equals[1..6]: Some(true), None, Some(true), Some(false), None
        let values_slice1 = values.slice(1, 5);
        let equals_slice1 = equals.slice(1, 5);
        let result_slice1 = nullif(&values_slice1, &equals_slice1).unwrap();
        assert_eq!(result_slice1.len(), 5);
        let actual_slice1: Vec<Option<bool>> = result_slice1.as_boolean().iter().collect();
        let expected_slice1 = build_expected_bool(&[None, Some(false), Some(true), None, Some(false)], &[Some(true), None, Some(true), Some(false), None]);
        assert_eq!(actual_slice1, expected_slice1);
        assert_eq!(result_slice1.null_count(), actual_slice1.iter().filter(|x| x.is_none()).count());

        // Slice 2: offset=3, len=5 (values[3..8]: Some(true), None, Some(false), Some(true), Some(false))
        // equals[3..8]: Some(true), Some(false), None, Some(true), Some(false)
        let values_slice2 = values.slice(3, 5);
        let equals_slice2 = equals.slice(3, 5);
        let result_slice2 = nullif(&values_slice2, &equals_slice2).unwrap();
        assert_eq!(result_slice2.len(), 5);
        let actual_slice2: Vec<Option<bool>> = result_slice2.as_boolean().iter().collect();
        let expected_slice2 = build_expected_bool(&[Some(true), None, Some(false), Some(true), Some(false)], &[Some(true), Some(false), None, Some(true), Some(false)]);
        assert_eq!(actual_slice2, expected_slice2);
        assert_eq!(result_slice2.null_count(), actual_slice2.iter().filter(|x| x.is_none()).count());

        // Slice 3: offset=7, len=1 (values[7..8]: Some(false))
        // equals[7..8]: Some(false)
        let values_slice3 = values.slice(7, 1);
        let equals_slice3 = equals.slice(7, 1);
        let result_slice3 = nullif(&values_slice3, &equals_slice3).unwrap();
        assert_eq!(result_slice3.len(), 1);
        let actual_slice3: Vec<Option<bool>> = result_slice3.as_boolean().iter().collect();
        let expected_slice3 = build_expected_bool(&[Some(false)], &[Some(false)]);
        assert_eq!(actual_slice3, expected_slice3);
        assert_eq!(result_slice3.null_count(), actual_slice3.iter().filter(|x| x.is_none()).count());
    }

    #[test]
    fn test_nullif_all_null_and_empty() {
        // Test all-null input
        let all_null = Int32Array::from(vec![None, None, None]);
        let condition = BooleanArray::from(vec![Some(true), Some(false), None]);
        let result = nullif(&all_null, &condition).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result.null_count(), 3);
        let actual: Vec<Option<i32>> = result.as_primitive::<Int32Type>().iter().collect();
        assert_eq!(actual, vec![None, None, None]);

        // Test no-null input
        let no_null = Int32Array::from(vec![Some(1), Some(2), Some(3)]);
        let condition = BooleanArray::from(vec![Some(false), Some(true), Some(false)]);
        let result = nullif(&no_null, &condition).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result.null_count(), 1);
        let actual: Vec<Option<i32>> = result.as_primitive::<Int32Type>().iter().collect();
        assert_eq!(actual, vec![Some(1), None, Some(3)]);

        // Test empty arrays
        let empty_values = Int32Array::from(Vec::<Option<i32>>::new());
        let empty_condition = BooleanArray::from(Vec::<Option<bool>>::new());
        let result = nullif(&empty_values, &empty_condition).unwrap();
        assert_eq!(result.len(), 0);
        assert_eq!(result.null_count(), 0);
    }

    #[test]
    fn test_compute_nullif_validity_basic_and_offsets() {
        // left:  0b11110000, 0b00001111
        // cond:  0b10101010, 0b01010101
        let left = Buffer::from(vec![0b11110000u8, 0b00001111u8]);
        let cond = Buffer::from(vec![0b10101010u8, 0b01010101u8]);

        // Cases: (left_offset, cond_offset, len, expected_bits as Vec<bool>)
        let cases = vec![
            // No offset, 8 bits
            (0, 0, 8, vec![
                // left bits: 0 0 0 0 1 1 1 1
                // cond bits: 0 1 0 1 0 1 0 1
                // result: 0&!0=0, 0&!1=0, 0&!0=0, 0&!1=0, 1&!0=1, 1&!1=0, 1&!0=1, 1&!1=0
                // => 0,0,0,0,1,0,1,0
                false, false, false, false, true, false, true, false
            ]),
            // Offsets that make (offset+len)%8==0
            (1, 1, 7, vec![
                // left_offset=1, left bits 1-7: 0,0,0,1,1,1,1
                // cond_offset=1, cond bits 1-7: 1,0,1,0,1,0,1
                // result: 0&!1=0, 0&!0=0, 0&!1=0, 1&!0=1, 1&!1=0, 1&!0=1, 1&!1=0
                // => 0,0,0,1,0,1,0
                false, false, false, true, false, true, false
            ]),
            // A cross-byte case
            (3, 3, 5, vec![
                // left_offset=3, left bits 3-7: 0,1,1,1,1
                // cond_offset=3, cond bits 3-7: 1,0,1,0,1
                // result: 0&!1=0, 1&!0=1, 1&!1=0, 1&!0=1, 1&!1=0
                // => 0,1,0,1,0
                false, true, false, true, false
            ]),
        ];

        for (loff, coff, len, expected) in cases {
            let mask = compute_nullif_validity(&left, loff, &cond, coff, len);
            // Check each bit in the resulting Buffer against expected
            for i in 0..len {
                let bit = get_bit(mask.as_slice(), i);
                assert_eq!(bit, expected[i], "mismatch at bit {} for offsets ({}, {})", i, loff, coff);
            }
        }
    }
}
