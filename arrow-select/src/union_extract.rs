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

//! Defines union_extract kernel for [UnionArray]

use crate::take::take;
use arrow_array::{
    make_array, new_empty_array, new_null_array, Array, ArrayRef, BooleanArray, Int32Array, Scalar,
    UnionArray,
};
use arrow_buffer::{bit_util, BooleanBuffer, MutableBuffer, NullBuffer, ScalarBuffer};
use arrow_data::layout;
use arrow_schema::{ArrowError, DataType, UnionFields};
use std::cmp::Ordering;
use std::sync::Arc;

/// Returns the value of the target field when selected, or NULL otherwise.
/// ```text
/// ┌─────────────────┐                                   ┌─────────────────┐
/// │       A=1       │                                   │        1        │
/// ├─────────────────┤                                   ├─────────────────┤
/// │      A=NULL     │                                   │       NULL      │
/// ├─────────────────┤    union_extract(values, 'A')     ├─────────────────┤
/// │      B='t'      │  ────────────────────────────▶    │       NULL      │
/// ├─────────────────┤                                   ├─────────────────┤
/// │       A=3       │                                   │        3        │
/// ├─────────────────┤                                   ├─────────────────┤
/// │      B=NULL     │                                   │       NULL      │
/// └─────────────────┘                                   └─────────────────┘
///    union array                                              result
/// ```
/// # Errors
///
/// Returns error if target field is not found
///
/// # Examples
/// ```
/// # use std::sync::Arc;
/// # use arrow_schema::{DataType, Field, UnionFields};
/// # use arrow_array::{UnionArray, StringArray, Int32Array};
/// # use arrow_select::union_extract::union_extract;
/// let fields = UnionFields::new(
///     [1, 3],
///     [
///         Field::new("A", DataType::Int32, true),
///         Field::new("B", DataType::Utf8, true)
///     ]
/// );
///
/// let union = UnionArray::try_new(
///     fields,
///     vec![1, 1, 3, 1, 3].into(),
///     None,
///     vec![
///         Arc::new(Int32Array::from(vec![Some(1), None, None, Some(3), Some(0)])),
///         Arc::new(StringArray::from(vec![None, None, Some("t"), Some("."), None]))
///     ]
/// ).unwrap();
///
/// // Extract field A
/// let extracted = union_extract(&union, "A").unwrap();
///
/// assert_eq!(*extracted, Int32Array::from(vec![Some(1), None, None, Some(3), None]));
/// ```
pub fn union_extract(union_array: &UnionArray, target: &str) -> Result<ArrayRef, ArrowError> {
    let fields = match union_array.data_type() {
        DataType::Union(fields, _) => fields,
        _ => unreachable!(),
    };

    let (target_type_id, _) = fields
        .iter()
        .find(|field| field.1.name() == target)
        .ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!("field {target} not found on union"))
        })?;

    match union_array.offsets() {
        Some(_) => extract_dense(union_array, fields, target_type_id),
        None => extract_sparse(union_array, fields, target_type_id),
    }
}

fn extract_sparse(
    union_array: &UnionArray,
    fields: &UnionFields,
    target_type_id: i8,
) -> Result<ArrayRef, ArrowError> {
    let target = union_array.child(target_type_id);

    if fields.len() == 1 // case 1.1: if there is a single field, all type ids are the same, and since union doesn't have a null mask, the result array is exactly the same as it only child
        || union_array.is_empty() // case 1.2: sparse union length and childrens length must match, if the union is empty, so is any children
        || target.null_count() == target.len() || target.data_type().is_null()
    // case 1.3: if all values of the target children are null, regardless of selected type ids, the result will also be completely null
    {
        Ok(Arc::clone(target))
    } else {
        match eq_scalar(union_array.type_ids(), target_type_id) {
            // case 2: all type ids equals our target, and since unions doesn't have a null mask, the result array is exactly the same as our target
            BoolValue::Scalar(true) => Ok(Arc::clone(target)),
            // case 3: none type_id matches our target, the result is a null array
            BoolValue::Scalar(false) => {
                if layout(target.data_type()).can_contain_null_mask {
                    // case 3.1: target array can contain a null mask
                    //SAFETY: The only change to the array data is the addition of a null mask, and if the target data type can contain a null mask was just checked above
                    let data = unsafe {
                        target
                            .into_data()
                            .into_builder()
                            .nulls(Some(NullBuffer::new_null(target.len())))
                            .build_unchecked()
                    };

                    Ok(make_array(data))
                } else {
                    // case 3.2: target can't contain a null mask
                    Ok(new_null_array(target.data_type(), target.len()))
                }
            }
            // case 4: some but not all type_id matches our target
            BoolValue::Buffer(selected) => {
                if layout(target.data_type()).can_contain_null_mask {
                    // case 4.1: target array can contain a null mask
                    let nulls = match target.nulls().filter(|n| n.null_count() > 0) {
                        // case 4.1.1: our target child has nulls and types other than our target are selected, union the masks
                        // the case where n.null_count() == n.len() is cheaply handled at case 1.3
                        Some(nulls) => &selected & nulls.inner(),
                        // case 4.1.2: target child has no nulls, but types other than our target are selected, use the selected mask as a null mask
                        None => selected,
                    };

                    //SAFETY: The only change to the array data is the addition of a null mask, and if the target data type can contain a null mask was just checked above
                    let data = unsafe {
                        assert_eq!(nulls.len(), target.len());

                        target
                            .into_data()
                            .into_builder()
                            .nulls(Some(nulls.into()))
                            .build_unchecked()
                    };

                    Ok(make_array(data))
                } else {
                    // case 4.2: target can't containt a null mask, zip the values that match with a null value
                    Ok(crate::zip::zip(
                        &BooleanArray::new(selected, None),
                        target,
                        &Scalar::new(new_null_array(target.data_type(), 1)),
                    )?)
                }
            }
        }
    }
}

fn extract_dense(
    union_array: &UnionArray,
    fields: &UnionFields,
    target_type_id: i8,
) -> Result<ArrayRef, ArrowError> {
    let target = union_array.child(target_type_id);
    let offsets = union_array.offsets().unwrap();

    if union_array.is_empty() {
        // case 1: the union is empty
        if target.is_empty() {
            // case 1.1: the target is also empty, do a cheap Arc::clone instead of allocating a new empty array
            Ok(Arc::clone(target))
        } else {
            // case 1.2: the target is not empty, allocate a new empty array
            Ok(new_empty_array(target.data_type()))
        }
    } else if target.is_empty() {
        // case 2: the union is not empty but the target is, which implies that none type_id points to it. The result is a null array
        Ok(new_null_array(target.data_type(), union_array.len()))
    } else if target.null_count() == target.len() || target.data_type().is_null() {
        // case 3: since all values on our target are null, regardless of selected type ids and offsets, the result is a null array
        match target.len().cmp(&union_array.len()) {
            // case 3.1: since the target is smaller than the union, allocate a new correclty sized null array
            Ordering::Less => Ok(new_null_array(target.data_type(), union_array.len())),
            // case 3.2: target equals the union len, return it direcly
            Ordering::Equal => Ok(Arc::clone(target)),
            // case 3.3: target len is bigger than the union len, slice it
            Ordering::Greater => Ok(target.slice(0, union_array.len())),
        }
    } else if fields.len() == 1 // case A: since there's a single field, our target, every type id must matches our target
        || fields
            .iter()
            .filter(|(field_type_id, _)| *field_type_id != target_type_id)
            .all(|(sibling_type_id, _)| union_array.child(sibling_type_id).is_empty())
    // case B: since siblings are empty, every type id must matches our target
    {
        // case 4: every type id matches our target
        Ok(extract_dense_all_selected(union_array, target, offsets)?)
    } else {
        match eq_scalar(union_array.type_ids(), target_type_id) {
            // case 4C: all type ids matches our target.
            // Non empty sibling without any selected value may happen after slicing the parent union,
            // since only type_ids and offsets are sliced, not the children
            BoolValue::Scalar(true) => {
                Ok(extract_dense_all_selected(union_array, target, offsets)?)
            }
            BoolValue::Scalar(false) => {
                // case 5: none type_id matches our target, so the result array will be completely null
                // Non empty target without any selected value may happen after slicing the parent union,
                // since only type_ids and offsets are sliced, not the children
                match (target.len().cmp(&union_array.len()), layout(target.data_type()).can_contain_null_mask) {
                    (Ordering::Less, _) // case 5.1A: our target is smaller than the parent union, allocate a new correclty sized null array
                    | (_, false) => { // case 5.1B: target array can't contain a null mask
                        Ok(new_null_array(target.data_type(), union_array.len()))
                    }
                    // case 5.2: target and parent union lengths are equal, and the target can contain a null mask, let's set it to a all-null null-buffer
                    (Ordering::Equal, true) => {
                        //SAFETY: The only change to the array data is the addition of a null mask, and if the target data type can contain a null mask was just checked above
                        let data = unsafe {
                            target
                                .into_data()
                                .into_builder()
                                .nulls(Some(NullBuffer::new_null(union_array.len())))
                                .build_unchecked()
                        };

                        Ok(make_array(data))
                    }
                    // case 5.3: target is bigger than it's parent union and can contain a null mask, let's slice it, and set it's nulls to a all-null null-buffer
                    (Ordering::Greater, true) => {
                        //SAFETY: The only change to the array data is the addition of a null mask, and if the target data type can contain a null mask was just checked above
                        let data = unsafe {
                            target
                                .into_data()
                                .slice(0, union_array.len())
                                .into_builder()
                                .nulls(Some(NullBuffer::new_null(union_array.len())))
                                .build_unchecked()
                        };

                        Ok(make_array(data))
                    }
                }
            }
            BoolValue::Buffer(selected) => {
                //case 6: some type_ids matches our target, but not all. For selected values, take the value pointed by the offset. For unselected, use a valid null
                Ok(take(
                    target,
                    &Int32Array::new(offsets.clone(), Some(selected.into())),
                    None,
                )?)
            }
        }
    }
}

fn extract_dense_all_selected(
    union_array: &UnionArray,
    target: &Arc<dyn Array>,
    offsets: &ScalarBuffer<i32>,
) -> Result<ArrayRef, ArrowError> {
    let sequential =
        target.len() - offsets[0] as usize >= union_array.len() && is_sequential(offsets);

    if sequential && target.len() == union_array.len() {
        // case 1: all offsets are sequential and both lengths match, return the array directly
        Ok(Arc::clone(target))
    } else if sequential && target.len() > union_array.len() {
        // case 2: All offsets are sequential, but our target is bigger than our union, slice it, starting at the first offset
        Ok(target.slice(offsets[0] as usize, union_array.len()))
    } else {
        // case 3: Since offsets are not sequential, take them from the child to a new sequential and correcly sized array
        let indices = Int32Array::try_new(offsets.clone(), None)?;

        Ok(take(target, &indices, None)?)
    }
}

const EQ_SCALAR_CHUNK_SIZE: usize = 512;

/// The result of checking which type_ids matches the target type_id
#[derive(Debug, PartialEq)]
enum BoolValue {
    /// If true, all type_ids matches the target type_id
    /// If false, none type_ids matches the target type_id
    Scalar(bool),
    /// A mask represeting which type_ids matches the target type_id
    Buffer(BooleanBuffer),
}

fn eq_scalar(type_ids: &[i8], target: i8) -> BoolValue {
    eq_scalar_inner(EQ_SCALAR_CHUNK_SIZE, type_ids, target)
}

fn count_first_run(chunk_size: usize, type_ids: &[i8], mut f: impl FnMut(i8) -> bool) -> usize {
    type_ids
        .chunks(chunk_size)
        .take_while(|chunk| chunk.iter().copied().fold(true, |b, v| b & f(v)))
        .map(|chunk| chunk.len())
        .sum()
}

// This is like MutableBuffer::collect_bool(type_ids.len(), |i| type_ids[i] == target) with fast paths for all true or all false values.
fn eq_scalar_inner(chunk_size: usize, type_ids: &[i8], target: i8) -> BoolValue {
    let true_bits = count_first_run(chunk_size, type_ids, |v| v == target);

    let (set_bits, val) = if true_bits == type_ids.len() {
        return BoolValue::Scalar(true);
    } else if true_bits == 0 {
        let false_bits = count_first_run(chunk_size, type_ids, |v| v != target);

        if false_bits == type_ids.len() {
            return BoolValue::Scalar(false);
        } else {
            (false_bits, false)
        }
    } else {
        (true_bits, true)
    };

    // restrict to chunk boundaries
    let set_bits = set_bits - set_bits % 64;

    let mut buffer =
        MutableBuffer::new(bit_util::ceil(type_ids.len(), 8)).with_bitset(set_bits / 8, val);

    buffer.extend(type_ids[set_bits..].chunks(64).map(|chunk| {
        chunk
            .iter()
            .copied()
            .enumerate()
            .fold(0, |packed, (bit_idx, v)| {
                packed | ((v == target) as u64) << bit_idx
            })
    }));

    BoolValue::Buffer(BooleanBuffer::new(buffer.into(), 0, type_ids.len()))
}

const IS_SEQUENTIAL_CHUNK_SIZE: usize = 64;

fn is_sequential(offsets: &[i32]) -> bool {
    is_sequential_generic::<IS_SEQUENTIAL_CHUNK_SIZE>(offsets)
}

fn is_sequential_generic<const N: usize>(offsets: &[i32]) -> bool {
    if offsets.is_empty() {
        return true;
    }

    // fast check this common combination:
    // 1: sequential nulls are represented as a single null value on the values array, pointed by the same offset multiple times
    // 2: valid values offsets increase one by one.
    // example for an union with a single field A with type_id 0:
    // union    = A=7 A=NULL A=NULL A=5 A=9
    // a values = 7 NULL 5 9
    // offsets  = 0 1 1 2 3
    // type_ids = 0 0 0 0 0
    // this also checks if the last chunk/remainder is sequential relative to the first offset
    if offsets[0] + offsets.len() as i32 - 1 != offsets[offsets.len() - 1] {
        return false;
    }

    let chunks = offsets.chunks_exact(N);

    let remainder = chunks.remainder();

    chunks.enumerate().all(|(i, chunk)| {
        let chunk_array = <&[i32; N]>::try_from(chunk).unwrap();

        //checks if values within chunk are sequential
        chunk_array
            .iter()
            .copied()
            .enumerate()
            .fold(true, |acc, (i, offset)| {
                acc & (offset == chunk_array[0] + i as i32)
            })
            && offsets[0] + (i * N) as i32 == chunk_array[0] //checks if chunk is sequential relative to the first offset
    }) && remainder
        .iter()
        .copied()
        .enumerate()
        .fold(true, |acc, (i, offset)| {
            acc & (offset == remainder[0] + i as i32)
        }) //if the remainder is sequential relative to the first offset is checked at the start of the function
}

#[cfg(test)]
mod tests {
    use super::{eq_scalar_inner, is_sequential_generic, union_extract, BoolValue};
    use arrow_array::{new_null_array, Array, Int32Array, NullArray, StringArray, UnionArray};
    use arrow_buffer::{BooleanBuffer, ScalarBuffer};
    use arrow_schema::{ArrowError, DataType, Field, UnionFields, UnionMode};
    use std::sync::Arc;

    #[test]
    fn test_eq_scalar() {
        //multiple all equal chunks, so it's loop and sum logic it's tested
        //multiple chunks after, so it's loop logic it's tested
        const ARRAY_LEN: usize = 64 * 4;

        //so out of 64 boundaries chunks can be generated and checked for
        const EQ_SCALAR_CHUNK_SIZE: usize = 3;

        fn eq_scalar(type_ids: &[i8], target: i8) -> BoolValue {
            eq_scalar_inner(EQ_SCALAR_CHUNK_SIZE, type_ids, target)
        }

        fn cross_check(left: &[i8], right: i8) -> BooleanBuffer {
            BooleanBuffer::collect_bool(left.len(), |i| left[i] == right)
        }

        assert_eq!(eq_scalar(&[], 1), BoolValue::Scalar(true));

        assert_eq!(eq_scalar(&[1], 1), BoolValue::Scalar(true));
        assert_eq!(eq_scalar(&[2], 1), BoolValue::Scalar(false));

        let mut values = [1; ARRAY_LEN];

        assert_eq!(eq_scalar(&values, 1), BoolValue::Scalar(true));
        assert_eq!(eq_scalar(&values, 2), BoolValue::Scalar(false));

        //every subslice should return the same value
        for i in 1..ARRAY_LEN {
            assert_eq!(eq_scalar(&values[..i], 1), BoolValue::Scalar(true));
            assert_eq!(eq_scalar(&values[..i], 2), BoolValue::Scalar(false));
        }

        // test that a single change anywhere is checked for
        for i in 0..ARRAY_LEN {
            values[i] = 2;

            assert_eq!(
                eq_scalar(&values, 1),
                BoolValue::Buffer(cross_check(&values, 1))
            );
            assert_eq!(
                eq_scalar(&values, 2),
                BoolValue::Buffer(cross_check(&values, 2))
            );

            values[i] = 1;
        }
    }

    #[test]
    fn test_is_sequential() {
        /*
        the smallest value that satisfies:
        >1 so the fold logic of a exact chunk executes
        >2 so a >1 non-exact remainder can exist, and it's fold logic executes
         */
        const CHUNK_SIZE: usize = 3;
        //we test arrays of size up to 8 = 2 * CHUNK_SIZE + 2:
        //multiple(2) exact chunks, so the AND logic between them executes
        //a >1(2) remainder, so:
        //    the AND logic between all exact chunks and the remainder executes
        //    the remainder fold logic executes

        fn is_sequential(v: &[i32]) -> bool {
            is_sequential_generic::<CHUNK_SIZE>(v)
        }

        assert!(is_sequential(&[])); //empty
        assert!(is_sequential(&[1])); //single

        assert!(is_sequential(&[1, 2]));
        assert!(is_sequential(&[1, 2, 3]));
        assert!(is_sequential(&[1, 2, 3, 4]));
        assert!(is_sequential(&[1, 2, 3, 4, 5]));
        assert!(is_sequential(&[1, 2, 3, 4, 5, 6]));
        assert!(is_sequential(&[1, 2, 3, 4, 5, 6, 7]));
        assert!(is_sequential(&[1, 2, 3, 4, 5, 6, 7, 8]));

        assert!(!is_sequential(&[8, 7]));
        assert!(!is_sequential(&[8, 7, 6]));
        assert!(!is_sequential(&[8, 7, 6, 5]));
        assert!(!is_sequential(&[8, 7, 6, 5, 4]));
        assert!(!is_sequential(&[8, 7, 6, 5, 4, 3]));
        assert!(!is_sequential(&[8, 7, 6, 5, 4, 3, 2]));
        assert!(!is_sequential(&[8, 7, 6, 5, 4, 3, 2, 1]));

        assert!(!is_sequential(&[0, 2]));
        assert!(!is_sequential(&[1, 0]));

        assert!(!is_sequential(&[0, 2, 3]));
        assert!(!is_sequential(&[1, 0, 3]));
        assert!(!is_sequential(&[1, 2, 0]));

        assert!(!is_sequential(&[0, 2, 3, 4]));
        assert!(!is_sequential(&[1, 0, 3, 4]));
        assert!(!is_sequential(&[1, 2, 0, 4]));
        assert!(!is_sequential(&[1, 2, 3, 0]));

        assert!(!is_sequential(&[0, 2, 3, 4, 5]));
        assert!(!is_sequential(&[1, 0, 3, 4, 5]));
        assert!(!is_sequential(&[1, 2, 0, 4, 5]));
        assert!(!is_sequential(&[1, 2, 3, 0, 5]));
        assert!(!is_sequential(&[1, 2, 3, 4, 0]));

        assert!(!is_sequential(&[0, 2, 3, 4, 5, 6]));
        assert!(!is_sequential(&[1, 0, 3, 4, 5, 6]));
        assert!(!is_sequential(&[1, 2, 0, 4, 5, 6]));
        assert!(!is_sequential(&[1, 2, 3, 0, 5, 6]));
        assert!(!is_sequential(&[1, 2, 3, 4, 0, 6]));
        assert!(!is_sequential(&[1, 2, 3, 4, 5, 0]));

        assert!(!is_sequential(&[0, 2, 3, 4, 5, 6, 7]));
        assert!(!is_sequential(&[1, 0, 3, 4, 5, 6, 7]));
        assert!(!is_sequential(&[1, 2, 0, 4, 5, 6, 7]));
        assert!(!is_sequential(&[1, 2, 3, 0, 5, 6, 7]));
        assert!(!is_sequential(&[1, 2, 3, 4, 0, 6, 7]));
        assert!(!is_sequential(&[1, 2, 3, 4, 5, 0, 7]));
        assert!(!is_sequential(&[1, 2, 3, 4, 5, 6, 0]));

        assert!(!is_sequential(&[0, 2, 3, 4, 5, 6, 7, 8]));
        assert!(!is_sequential(&[1, 0, 3, 4, 5, 6, 7, 8]));
        assert!(!is_sequential(&[1, 2, 0, 4, 5, 6, 7, 8]));
        assert!(!is_sequential(&[1, 2, 3, 0, 5, 6, 7, 8]));
        assert!(!is_sequential(&[1, 2, 3, 4, 0, 6, 7, 8]));
        assert!(!is_sequential(&[1, 2, 3, 4, 5, 0, 7, 8]));
        assert!(!is_sequential(&[1, 2, 3, 4, 5, 6, 0, 8]));
        assert!(!is_sequential(&[1, 2, 3, 4, 5, 6, 7, 0]));

        // checks increments at the chunk boundary
        assert!(!is_sequential(&[1, 2, 3, 5]));
        assert!(!is_sequential(&[1, 2, 3, 5, 6]));
        assert!(!is_sequential(&[1, 2, 3, 5, 6, 7]));
        assert!(!is_sequential(&[1, 2, 3, 4, 5, 6, 8]));
        assert!(!is_sequential(&[1, 2, 3, 4, 5, 6, 8, 9]));
    }

    fn str1() -> UnionFields {
        UnionFields::new(vec![1], vec![Field::new("str", DataType::Utf8, true)])
    }

    fn str1_int3() -> UnionFields {
        UnionFields::new(
            vec![1, 3],
            vec![
                Field::new("str", DataType::Utf8, true),
                Field::new("int", DataType::Int32, true),
            ],
        )
    }

    #[test]
    fn sparse_1_1_single_field() {
        let union = UnionArray::try_new(
            //single field
            str1(),
            ScalarBuffer::from(vec![1, 1]), // non empty, every type id must match
            None,                           //sparse
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])), // not null
            ],
        )
        .unwrap();

        let expected = StringArray::from(vec!["a", "b"]);
        let extracted = union_extract(&union, "str").unwrap();

        assert_eq!(extracted.into_data(), expected.into_data());
    }

    #[test]
    fn sparse_1_2_empty() {
        let union = UnionArray::try_new(
            // multiple fields
            str1_int3(),
            ScalarBuffer::from(vec![]), //empty union
            None,                       // sparse
            vec![
                Arc::new(StringArray::new_null(0)),
                Arc::new(Int32Array::new_null(0)),
            ],
        )
        .unwrap();

        let expected = StringArray::new_null(0);
        let extracted = union_extract(&union, "str").unwrap(); //target type is not Null

        assert_eq!(extracted.into_data(), expected.into_data());
    }

    #[test]
    fn sparse_1_3a_null_target() {
        let union = UnionArray::try_new(
            // multiple fields
            UnionFields::new(
                vec![1, 3],
                vec![
                    Field::new("str", DataType::Utf8, true),
                    Field::new("null", DataType::Null, true), // target type is Null
                ],
            ),
            ScalarBuffer::from(vec![1]), //not empty
            None,                        // sparse
            vec![
                Arc::new(StringArray::new_null(1)),
                Arc::new(NullArray::new(1)), // null data type
            ],
        )
        .unwrap();

        let expected = NullArray::new(1);
        let extracted = union_extract(&union, "null").unwrap();

        assert_eq!(extracted.into_data(), expected.into_data());
    }

    #[test]
    fn sparse_1_3b_null_target() {
        let union = UnionArray::try_new(
            // multiple fields
            str1_int3(),
            ScalarBuffer::from(vec![1]), //not empty
            None,                        // sparse
            vec![
                Arc::new(StringArray::new_null(1)), //all null
                Arc::new(Int32Array::new_null(1)),
            ],
        )
        .unwrap();

        let expected = StringArray::new_null(1);
        let extracted = union_extract(&union, "str").unwrap(); //target type is not Null

        assert_eq!(extracted.into_data(), expected.into_data());
    }

    #[test]
    fn sparse_2_all_types_match() {
        let union = UnionArray::try_new(
            //multiple fields
            str1_int3(),
            ScalarBuffer::from(vec![3, 3]), // all types match
            None,                           //sparse
            vec![
                Arc::new(StringArray::new_null(2)),
                Arc::new(Int32Array::from(vec![1, 4])), // not null
            ],
        )
        .unwrap();

        let expected = Int32Array::from(vec![1, 4]);
        let extracted = union_extract(&union, "int").unwrap();

        assert_eq!(extracted.into_data(), expected.into_data());
    }

    #[test]
    fn sparse_3_1_none_match_target_can_contain_null_mask() {
        let union = UnionArray::try_new(
            //multiple fields
            str1_int3(),
            ScalarBuffer::from(vec![1, 1, 1, 1]), // none match
            None,                                 // sparse
            vec![
                Arc::new(StringArray::new_null(4)),
                Arc::new(Int32Array::from(vec![None, Some(4), None, Some(8)])), // target is not null
            ],
        )
        .unwrap();

        let expected = Int32Array::new_null(4);
        let extracted = union_extract(&union, "int").unwrap();

        assert_eq!(extracted.into_data(), expected.into_data());
    }

    fn str1_union3(union3_datatype: DataType) -> UnionFields {
        UnionFields::new(
            vec![1, 3],
            vec![
                Field::new("str", DataType::Utf8, true),
                Field::new("union", union3_datatype, true),
            ],
        )
    }

    #[test]
    fn sparse_3_2_none_match_cant_contain_null_mask_union_target() {
        let target_fields = str1();
        let target_type = DataType::Union(target_fields.clone(), UnionMode::Sparse);

        let union = UnionArray::try_new(
            //multiple fields
            str1_union3(target_type.clone()),
            ScalarBuffer::from(vec![1, 1]), // none match
            None,                           //sparse
            vec![
                Arc::new(StringArray::new_null(2)),
                //target is not null
                Arc::new(
                    UnionArray::try_new(
                        target_fields.clone(),
                        ScalarBuffer::from(vec![1, 1]),
                        None,
                        vec![Arc::new(StringArray::from(vec!["a", "b"]))],
                    )
                    .unwrap(),
                ),
            ],
        )
        .unwrap();

        let expected = new_null_array(&target_type, 2);
        let extracted = union_extract(&union, "union").unwrap();

        assert_eq!(extracted.into_data(), expected.into_data());
    }

    #[test]
    fn sparse_4_1_1_target_with_nulls() {
        let union = UnionArray::try_new(
            //multiple fields
            str1_int3(),
            ScalarBuffer::from(vec![3, 3, 1, 1]), // multiple selected types
            None,                                 // sparse
            vec![
                Arc::new(StringArray::new_null(4)),
                Arc::new(Int32Array::from(vec![None, Some(4), None, Some(8)])), // target with nulls
            ],
        )
        .unwrap();

        let expected = Int32Array::from(vec![None, Some(4), None, None]);
        let extracted = union_extract(&union, "int").unwrap();

        assert_eq!(extracted.into_data(), expected.into_data());
    }

    #[test]
    fn sparse_4_1_2_target_without_nulls() {
        let union = UnionArray::try_new(
            //multiple fields
            str1_int3(),
            ScalarBuffer::from(vec![1, 3, 3]), // multiple selected types
            None,                              // sparse
            vec![
                Arc::new(StringArray::new_null(3)),
                Arc::new(Int32Array::from(vec![2, 4, 8])), // target without nulls
            ],
        )
        .unwrap();

        let expected = Int32Array::from(vec![None, Some(4), Some(8)]);
        let extracted = union_extract(&union, "int").unwrap();

        assert_eq!(extracted.into_data(), expected.into_data());
    }

    #[test]
    fn sparse_4_2_some_match_target_cant_contain_null_mask() {
        let target_fields = str1();
        let target_type = DataType::Union(target_fields.clone(), UnionMode::Sparse);

        let union = UnionArray::try_new(
            //multiple fields
            str1_union3(target_type),
            ScalarBuffer::from(vec![3, 1]), // some types match, but not all
            None,                           //sparse
            vec![
                Arc::new(StringArray::new_null(2)),
                Arc::new(
                    UnionArray::try_new(
                        target_fields.clone(),
                        ScalarBuffer::from(vec![1, 1]),
                        None,
                        vec![Arc::new(StringArray::from(vec!["a", "b"]))],
                    )
                    .unwrap(),
                ),
            ],
        )
        .unwrap();

        let expected = UnionArray::try_new(
            target_fields,
            ScalarBuffer::from(vec![1, 1]),
            None,
            vec![Arc::new(StringArray::from(vec![Some("a"), None]))],
        )
        .unwrap();
        let extracted = union_extract(&union, "union").unwrap();

        assert_eq!(extracted.into_data(), expected.into_data());
    }

    #[test]
    fn dense_1_1_both_empty() {
        let union = UnionArray::try_new(
            str1_int3(),
            ScalarBuffer::from(vec![]),       //empty union
            Some(ScalarBuffer::from(vec![])), // dense
            vec![
                Arc::new(StringArray::new_null(0)), //empty target
                Arc::new(Int32Array::new_null(0)),
            ],
        )
        .unwrap();

        let expected = StringArray::new_null(0);
        let extracted = union_extract(&union, "str").unwrap();

        assert_eq!(extracted.into_data(), expected.into_data());
    }

    #[test]
    fn dense_1_2_empty_union_target_non_empty() {
        let union = UnionArray::try_new(
            str1_int3(),
            ScalarBuffer::from(vec![]),       //empty union
            Some(ScalarBuffer::from(vec![])), // dense
            vec![
                Arc::new(StringArray::new_null(1)), //non empty target
                Arc::new(Int32Array::new_null(0)),
            ],
        )
        .unwrap();

        let expected = StringArray::new_null(0);
        let extracted = union_extract(&union, "str").unwrap();

        assert_eq!(extracted.into_data(), expected.into_data());
    }

    #[test]
    fn dense_2_non_empty_union_target_empty() {
        let union = UnionArray::try_new(
            str1_int3(),
            ScalarBuffer::from(vec![3, 3]),       //non empty union
            Some(ScalarBuffer::from(vec![0, 1])), // dense
            vec![
                Arc::new(StringArray::new_null(0)), //empty target
                Arc::new(Int32Array::new_null(2)),
            ],
        )
        .unwrap();

        let expected = StringArray::new_null(2);
        let extracted = union_extract(&union, "str").unwrap();

        assert_eq!(extracted.into_data(), expected.into_data());
    }

    #[test]
    fn dense_3_1_null_target_smaller_len() {
        let union = UnionArray::try_new(
            str1_int3(),
            ScalarBuffer::from(vec![3, 3]),       //non empty union
            Some(ScalarBuffer::from(vec![0, 0])), //dense
            vec![
                Arc::new(StringArray::new_null(1)), //smaller target
                Arc::new(Int32Array::new_null(2)),
            ],
        )
        .unwrap();

        let expected = StringArray::new_null(2);
        let extracted = union_extract(&union, "str").unwrap();

        assert_eq!(extracted.into_data(), expected.into_data());
    }

    #[test]
    fn dense_3_2_null_target_equal_len() {
        let union = UnionArray::try_new(
            str1_int3(),
            ScalarBuffer::from(vec![3, 3]),       //non empty union
            Some(ScalarBuffer::from(vec![0, 0])), //dense
            vec![
                Arc::new(StringArray::new_null(2)), //equal len
                Arc::new(Int32Array::new_null(2)),
            ],
        )
        .unwrap();

        let expected = StringArray::new_null(2);
        let extracted = union_extract(&union, "str").unwrap();

        assert_eq!(extracted.into_data(), expected.into_data());
    }

    #[test]
    fn dense_3_3_null_target_bigger_len() {
        let union = UnionArray::try_new(
            str1_int3(),
            ScalarBuffer::from(vec![3, 3]),       //non empty union
            Some(ScalarBuffer::from(vec![0, 0])), //dense
            vec![
                Arc::new(StringArray::new_null(3)), //bigger len
                Arc::new(Int32Array::new_null(3)),
            ],
        )
        .unwrap();

        let expected = StringArray::new_null(2);
        let extracted = union_extract(&union, "str").unwrap();

        assert_eq!(extracted.into_data(), expected.into_data());
    }

    #[test]
    fn dense_4_1a_single_type_sequential_offsets_equal_len() {
        let union = UnionArray::try_new(
            // single field
            str1(),
            ScalarBuffer::from(vec![1, 1]),       //non empty union
            Some(ScalarBuffer::from(vec![0, 1])), //sequential
            vec![
                Arc::new(StringArray::from(vec!["a1", "b2"])), //equal len, non null
            ],
        )
        .unwrap();

        let expected = StringArray::from(vec!["a1", "b2"]);
        let extracted = union_extract(&union, "str").unwrap();

        assert_eq!(extracted.into_data(), expected.into_data());
    }

    #[test]
    fn dense_4_2a_single_type_sequential_offsets_bigger() {
        let union = UnionArray::try_new(
            // single field
            str1(),
            ScalarBuffer::from(vec![1, 1]),       //non empty union
            Some(ScalarBuffer::from(vec![0, 1])), //sequential
            vec![
                Arc::new(StringArray::from(vec!["a1", "b2", "c3"])), //equal len, non null
            ],
        )
        .unwrap();

        let expected = StringArray::from(vec!["a1", "b2"]);
        let extracted = union_extract(&union, "str").unwrap();

        assert_eq!(extracted.into_data(), expected.into_data());
    }

    #[test]
    fn dense_4_3a_single_type_non_sequential() {
        let union = UnionArray::try_new(
            // single field
            str1(),
            ScalarBuffer::from(vec![1, 1]),       //non empty union
            Some(ScalarBuffer::from(vec![0, 2])), //non sequential
            vec![
                Arc::new(StringArray::from(vec!["a1", "b2", "c3"])), //equal len, non null
            ],
        )
        .unwrap();

        let expected = StringArray::from(vec!["a1", "c3"]);
        let extracted = union_extract(&union, "str").unwrap();

        assert_eq!(extracted.into_data(), expected.into_data());
    }

    #[test]
    fn dense_4_1b_empty_siblings_sequential_equal_len() {
        let union = UnionArray::try_new(
            // multiple fields
            str1_int3(),
            ScalarBuffer::from(vec![1, 1]),       //non empty union
            Some(ScalarBuffer::from(vec![0, 1])), //sequential
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])), //equal len, non null
                Arc::new(Int32Array::new_null(0)),           //empty sibling
            ],
        )
        .unwrap();

        let expected = StringArray::from(vec!["a", "b"]);
        let extracted = union_extract(&union, "str").unwrap();

        assert_eq!(extracted.into_data(), expected.into_data());
    }

    #[test]
    fn dense_4_2b_empty_siblings_sequential_bigger_len() {
        let union = UnionArray::try_new(
            // multiple fields
            str1_int3(),
            ScalarBuffer::from(vec![1, 1]),       //non empty union
            Some(ScalarBuffer::from(vec![0, 1])), //sequential
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c"])), //bigger len, non null
                Arc::new(Int32Array::new_null(0)),                //empty sibling
            ],
        )
        .unwrap();

        let expected = StringArray::from(vec!["a", "b"]);
        let extracted = union_extract(&union, "str").unwrap();

        assert_eq!(extracted.into_data(), expected.into_data());
    }

    #[test]
    fn dense_4_3b_empty_sibling_non_sequential() {
        let union = UnionArray::try_new(
            // multiple fields
            str1_int3(),
            ScalarBuffer::from(vec![1, 1]),       //non empty union
            Some(ScalarBuffer::from(vec![0, 2])), //non sequential
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c"])), //non null
                Arc::new(Int32Array::new_null(0)),                //empty sibling
            ],
        )
        .unwrap();

        let expected = StringArray::from(vec!["a", "c"]);
        let extracted = union_extract(&union, "str").unwrap();

        assert_eq!(extracted.into_data(), expected.into_data());
    }

    #[test]
    fn dense_4_1c_all_types_match_sequential_equal_len() {
        let union = UnionArray::try_new(
            // multiple fields
            str1_int3(),
            ScalarBuffer::from(vec![1, 1]),       //all types match
            Some(ScalarBuffer::from(vec![0, 1])), //sequential
            vec![
                Arc::new(StringArray::from(vec!["a1", "b2"])), //equal len
                Arc::new(Int32Array::new_null(2)),             //non empty sibling
            ],
        )
        .unwrap();

        let expected = StringArray::from(vec!["a1", "b2"]);
        let extracted = union_extract(&union, "str").unwrap();

        assert_eq!(extracted.into_data(), expected.into_data());
    }

    #[test]
    fn dense_4_2c_all_types_match_sequential_bigger_len() {
        let union = UnionArray::try_new(
            // multiple fields
            str1_int3(),
            ScalarBuffer::from(vec![1, 1]),       //all types match
            Some(ScalarBuffer::from(vec![0, 1])), //sequential
            vec![
                Arc::new(StringArray::from(vec!["a1", "b2", "b3"])), //bigger len
                Arc::new(Int32Array::new_null(2)),                   //non empty sibling
            ],
        )
        .unwrap();

        let expected = StringArray::from(vec!["a1", "b2"]);
        let extracted = union_extract(&union, "str").unwrap();

        assert_eq!(extracted.into_data(), expected.into_data());
    }

    #[test]
    fn dense_4_3c_all_types_match_non_sequential() {
        let union = UnionArray::try_new(
            // multiple fields
            str1_int3(),
            ScalarBuffer::from(vec![1, 1]),       //all types match
            Some(ScalarBuffer::from(vec![0, 2])), //non sequential
            vec![
                Arc::new(StringArray::from(vec!["a1", "b2", "b3"])),
                Arc::new(Int32Array::new_null(2)), //non empty sibling
            ],
        )
        .unwrap();

        let expected = StringArray::from(vec!["a1", "b3"]);
        let extracted = union_extract(&union, "str").unwrap();

        assert_eq!(extracted.into_data(), expected.into_data());
    }

    #[test]
    fn dense_5_1a_none_match_less_len() {
        let union = UnionArray::try_new(
            // multiple fields
            str1_int3(),
            ScalarBuffer::from(vec![3, 3, 3, 3, 3]), //none matches
            Some(ScalarBuffer::from(vec![0, 0, 0, 1, 1])), // dense
            vec![
                Arc::new(StringArray::from(vec!["a1", "b2", "c3"])), // less len
                Arc::new(Int32Array::from(vec![1, 2])),
            ],
        )
        .unwrap();

        let expected = StringArray::new_null(5);
        let extracted = union_extract(&union, "str").unwrap();

        assert_eq!(extracted.into_data(), expected.into_data());
    }

    #[test]
    fn dense_5_1b_cant_contain_null_mask() {
        let target_fields = str1();
        let target_type = DataType::Union(target_fields.clone(), UnionMode::Sparse);

        let union = UnionArray::try_new(
            // multiple fields
            str1_union3(target_type.clone()),
            ScalarBuffer::from(vec![1, 1, 1, 1, 1]), //none matches
            Some(ScalarBuffer::from(vec![0, 0, 0, 1, 1])), // dense
            vec![
                Arc::new(StringArray::from(vec!["a1", "b2", "c3"])), // less len
                Arc::new(
                    UnionArray::try_new(
                        target_fields.clone(),
                        ScalarBuffer::from(vec![1]),
                        None,
                        vec![Arc::new(StringArray::from(vec!["a"]))],
                    )
                    .unwrap(),
                ), // non empty
            ],
        )
        .unwrap();

        let expected = new_null_array(&target_type, 5);
        let extracted = union_extract(&union, "union").unwrap();

        assert_eq!(extracted.into_data(), expected.into_data());
    }

    #[test]
    fn dense_5_2_none_match_equal_len() {
        let union = UnionArray::try_new(
            // multiple fields
            str1_int3(),
            ScalarBuffer::from(vec![3, 3, 3, 3, 3]), //none matches
            Some(ScalarBuffer::from(vec![0, 0, 0, 1, 1])), // dense
            vec![
                Arc::new(StringArray::from(vec!["a1", "b2", "c3", "d4", "e5"])), // equal len
                Arc::new(Int32Array::from(vec![1, 2])),
            ],
        )
        .unwrap();

        let expected = StringArray::new_null(5);
        let extracted = union_extract(&union, "str").unwrap();

        assert_eq!(extracted.into_data(), expected.into_data());
    }

    #[test]
    fn dense_5_3_none_match_greater_len() {
        let union = UnionArray::try_new(
            // multiple fields
            str1_int3(),
            ScalarBuffer::from(vec![3, 3, 3, 3, 3]), //none matches
            Some(ScalarBuffer::from(vec![0, 0, 0, 1, 1])), // dense
            vec![
                Arc::new(StringArray::from(vec!["a1", "b2", "c3", "d4", "e5", "f6"])), // greater len
                Arc::new(Int32Array::from(vec![1, 2])),                                //non null
            ],
        )
        .unwrap();

        let expected = StringArray::new_null(5);
        let extracted = union_extract(&union, "str").unwrap();

        assert_eq!(extracted.into_data(), expected.into_data());
    }

    #[test]
    fn dense_6_some_matches() {
        let union = UnionArray::try_new(
            // multiple fields
            str1_int3(),
            ScalarBuffer::from(vec![3, 3, 1, 1, 1]), //some matches
            Some(ScalarBuffer::from(vec![0, 1, 0, 1, 2])), // dense
            vec![
                Arc::new(StringArray::from(vec!["a1", "b2", "c3"])), // non null
                Arc::new(Int32Array::from(vec![1, 2])),
            ],
        )
        .unwrap();

        let expected = Int32Array::from(vec![Some(1), Some(2), None, None, None]);
        let extracted = union_extract(&union, "int").unwrap();

        assert_eq!(extracted.into_data(), expected.into_data());
    }

    #[test]
    fn empty_sparse_union() {
        let union = UnionArray::try_new(
            UnionFields::empty(),
            ScalarBuffer::from(vec![]),
            None,
            vec![],
        )
        .unwrap();

        assert_eq!(
            union_extract(&union, "a").unwrap_err().to_string(),
            ArrowError::InvalidArgumentError("field a not found on union".into()).to_string()
        );
    }

    #[test]
    fn empty_dense_union() {
        let union = UnionArray::try_new(
            UnionFields::empty(),
            ScalarBuffer::from(vec![]),
            Some(ScalarBuffer::from(vec![])),
            vec![],
        )
        .unwrap();

        assert_eq!(
            union_extract(&union, "a").unwrap_err().to_string(),
            ArrowError::InvalidArgumentError("field a not found on union".into()).to_string()
        );
    }
}
