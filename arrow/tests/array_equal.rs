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

use arrow::array::{
    make_array, Array, ArrayRef, BooleanArray, Decimal128Array, FixedSizeBinaryArray,
    FixedSizeBinaryBuilder, FixedSizeListBuilder, GenericBinaryArray, GenericStringArray,
    Int32Array, Int32Builder, Int64Builder, ListArray, ListBuilder, NullArray, OffsetSizeTrait,
    StringArray, StringDictionaryBuilder, StructArray, UnionBuilder,
};
use arrow::datatypes::{Int16Type, Int32Type};
use arrow_array::builder::{StringBuilder, StringViewBuilder, StructBuilder};
use arrow_array::{DictionaryArray, FixedSizeListArray, StringViewArray};
use arrow_buffer::{Buffer, ToByteSlice};
use arrow_data::{ArrayData, ArrayDataBuilder};
use arrow_schema::{DataType, Field, Fields};
use std::sync::Arc;

#[test]
fn test_null_equal() {
    let a = NullArray::new(12);
    let b = NullArray::new(12);
    test_equal(&a, &b, true);

    let b = NullArray::new(10);
    test_equal(&a, &b, false);

    // Test the case where offset != 0

    let a_slice = a.slice(2, 3);
    let b_slice = b.slice(1, 3);
    test_equal(&a_slice, &b_slice, true);

    let a_slice = a.slice(5, 4);
    let b_slice = b.slice(3, 3);
    test_equal(&a_slice, &b_slice, false);
}

#[test]
fn test_boolean_equal() {
    let a = BooleanArray::from(vec![false, false, true]);
    let b = BooleanArray::from(vec![false, false, true]);
    test_equal(&a, &b, true);

    let b = BooleanArray::from(vec![false, false, false]);
    test_equal(&a, &b, false);
}

#[test]
fn test_boolean_equal_nulls() {
    let a = BooleanArray::from(vec![Some(false), None, None, Some(true)]);
    let b = BooleanArray::from(vec![Some(false), None, None, Some(true)]);
    test_equal(&a, &b, true);

    let b = BooleanArray::from(vec![None, None, None, Some(true)]);
    test_equal(&a, &b, false);

    let b = BooleanArray::from(vec![Some(true), None, None, Some(true)]);
    test_equal(&a, &b, false);
}

#[test]
fn test_boolean_equal_offset() {
    let a = BooleanArray::from(vec![false, true, false, true, false, false, true]);
    let b = BooleanArray::from(vec![true, false, false, false, true, false, true, true]);
    test_equal(&a, &b, false);

    let a_slice = a.slice(2, 3);
    let b_slice = b.slice(3, 3);
    test_equal(&a_slice, &b_slice, true);

    let a_slice = a.slice(3, 4);
    let b_slice = b.slice(4, 4);
    test_equal(&a_slice, &b_slice, false);

    // Test the optimization cases where null_count == 0 and starts at 0 and len >= size_of(u8)

    // Elements fill in `u8`'s exactly.
    let mut vector = vec![false, false, true, true, true, true, true, true];
    let a = BooleanArray::from(vector.clone());
    let b = BooleanArray::from(vector.clone());
    test_equal(&a, &b, true);

    // Elements fill in `u8`s + suffix bits.
    vector.push(true);
    let a = BooleanArray::from(vector.clone());
    let b = BooleanArray::from(vector);
    test_equal(&a, &b, true);
}

#[test]
fn test_primitive() {
    let cases = vec![
        (
            vec![Some(1), Some(2), Some(3)],
            vec![Some(1), Some(2), Some(3)],
            true,
        ),
        (
            vec![Some(1), Some(2), Some(3)],
            vec![Some(1), Some(2), Some(4)],
            false,
        ),
        (
            vec![Some(1), Some(2), None],
            vec![Some(1), Some(2), None],
            true,
        ),
        (
            vec![Some(1), None, Some(3)],
            vec![Some(1), Some(2), None],
            false,
        ),
        (
            vec![Some(1), None, None],
            vec![Some(1), Some(2), None],
            false,
        ),
    ];

    for (lhs, rhs, expected) in cases {
        let lhs = Int32Array::from(lhs);
        let rhs = Int32Array::from(rhs);
        test_equal(&lhs, &rhs, expected);
    }
}

#[test]
fn test_primitive_slice() {
    let cases = vec![
        (
            vec![Some(1), Some(2), Some(3)],
            (0, 1),
            vec![Some(1), Some(2), Some(3)],
            (0, 1),
            true,
        ),
        (
            vec![Some(1), Some(2), Some(3)],
            (1, 1),
            vec![Some(1), Some(2), Some(3)],
            (2, 1),
            false,
        ),
        (
            vec![Some(1), Some(2), None],
            (1, 1),
            vec![Some(1), None, Some(2)],
            (2, 1),
            true,
        ),
        (
            vec![None, Some(2), None],
            (1, 1),
            vec![None, None, Some(2)],
            (2, 1),
            true,
        ),
        (
            vec![Some(1), None, Some(2), None, Some(3)],
            (2, 2),
            vec![None, Some(2), None, Some(3)],
            (1, 2),
            true,
        ),
        (
            vec![Some(1), Some(2), None, Some(0)],
            (2, 2),
            vec![Some(4), Some(5), Some(0), None],
            (2, 2),
            false,
        ),
    ];

    for (lhs, slice_lhs, rhs, slice_rhs, expected) in cases {
        let lhs = Int32Array::from(lhs);
        let lhs = lhs.slice(slice_lhs.0, slice_lhs.1);
        let rhs = Int32Array::from(rhs);
        let rhs = rhs.slice(slice_rhs.0, slice_rhs.1);

        test_equal(&lhs, &rhs, expected);
    }
}

#[allow(clippy::eq_op)]
fn test_equal(lhs: &dyn Array, rhs: &dyn Array, expected: bool) {
    // equality is symmetric
    assert_eq!(lhs, lhs);
    assert_eq!(rhs, rhs);

    match expected {
        true => {
            assert_eq!(lhs, rhs);
            assert_eq!(rhs, lhs);
        }
        false => {
            assert_ne!(lhs, rhs);
            assert_ne!(rhs, lhs);
        }
    }
}

type OptionString = Option<String>;

fn binary_cases() -> Vec<(Vec<OptionString>, Vec<OptionString>, bool)> {
    let base = vec![
        Some("hello".to_owned()),
        None,
        None,
        Some("world".to_owned()),
        None,
        None,
    ];
    let not_base = vec![
        Some("hello".to_owned()),
        Some("foo".to_owned()),
        None,
        Some("world".to_owned()),
        None,
        None,
    ];
    vec![
        (
            vec![Some("hello".to_owned()), Some("world".to_owned())],
            vec![Some("hello".to_owned()), Some("world".to_owned())],
            true,
        ),
        (
            vec![Some("hello".to_owned()), Some("world".to_owned())],
            vec![Some("hello".to_owned()), Some("arrow".to_owned())],
            false,
        ),
        (base.clone(), base.clone(), true),
        (base, not_base, false),
    ]
}

fn test_generic_string_equal<OffsetSize: OffsetSizeTrait>() {
    let cases = binary_cases();

    for (lhs, rhs, expected) in cases {
        let lhs: GenericStringArray<OffsetSize> = lhs.into_iter().collect();
        let rhs: GenericStringArray<OffsetSize> = rhs.into_iter().collect();
        test_equal(&lhs, &rhs, expected);
    }
}

#[test]
fn test_string_equal() {
    test_generic_string_equal::<i32>()
}

#[test]
fn test_large_string_equal() {
    test_generic_string_equal::<i64>()
}

fn test_generic_binary_equal<OffsetSize: OffsetSizeTrait>() {
    let cases = binary_cases();

    for (lhs, rhs, expected) in cases {
        let lhs = lhs
            .iter()
            .map(|x| x.as_deref().map(|x| x.as_bytes()))
            .collect();
        let rhs = rhs
            .iter()
            .map(|x| x.as_deref().map(|x| x.as_bytes()))
            .collect();
        let lhs = GenericBinaryArray::<OffsetSize>::from_opt_vec(lhs);
        let rhs = GenericBinaryArray::<OffsetSize>::from_opt_vec(rhs);
        test_equal(&lhs, &rhs, expected);
    }
}

#[test]
fn test_binary_equal() {
    test_generic_binary_equal::<i32>()
}

#[test]
fn test_large_binary_equal() {
    test_generic_binary_equal::<i64>()
}

#[test]
fn test_fixed_size_binary_array() {
    let a_input_arg = vec![vec![1, 2], vec![3, 4], vec![5, 6]];
    let a = FixedSizeBinaryArray::try_from_iter(a_input_arg.into_iter()).unwrap();

    let b_input_arg = vec![vec![1, 2], vec![3, 4], vec![5, 6]];
    let b = FixedSizeBinaryArray::try_from_iter(b_input_arg.into_iter()).unwrap();

    test_equal(&a, &b, true);
}

#[test]
fn test_string_view_equal() {
    let a1 = StringViewArray::from(vec!["foo", "very long string over 12 bytes", "bar"]);
    let a2 = StringViewArray::from(vec![
        "a very long string over 12 bytes",
        "foo",
        "very long string over 12 bytes",
        "bar",
    ]);
    test_equal(&a1, &a2.slice(1, 3), true);

    let a1 = StringViewArray::from(vec!["foo", "very long string over 12 bytes", "bar"]);
    let a2 = StringViewArray::from(vec!["foo", "very long string over 12 bytes", "bar"]);
    test_equal(&a1, &a2, true);

    let a1_s = a1.slice(1, 1);
    let a2_s = a2.slice(1, 1);
    test_equal(&a1_s, &a2_s, true);

    let a1_s = a1.slice(2, 1);
    let a2_s = a2.slice(0, 1);
    test_equal(&a1_s, &a2_s, false);

    // test will null value.
    let a1 = StringViewArray::from(vec!["foo", "very long string over 12 bytes", "bar"]);
    let a2 = {
        let mut builder = StringViewBuilder::new();
        builder.append_value("foo");
        builder.append_null();
        builder.append_option(Some("very long string over 12 bytes"));
        builder.append_value("bar");
        builder.finish()
    };
    test_equal(&a1, &a2, false);

    let a1_s = a1.slice(1, 2);
    let a2_s = a2.slice(1, 3);
    test_equal(&a1_s, &a2_s, false);

    let a1_s = a1.slice(1, 2);
    let a2_s = a2.slice(2, 2);
    test_equal(&a1_s, &a2_s, true);
}

#[test]
fn test_string_offset() {
    let a = StringArray::from(vec![Some("a"), None, Some("b")]);
    let a = a.slice(2, 1);
    let b = StringArray::from(vec![Some("b")]);

    test_equal(&a, &b, true);
}

#[test]
fn test_string_offset_larger() {
    let a = StringArray::from(vec![Some("a"), None, Some("b"), None, Some("c")]);
    let b = StringArray::from(vec![None, Some("b"), None, Some("c")]);

    test_equal(&a.slice(2, 2), &b.slice(0, 2), false);
    test_equal(&a.slice(2, 2), &b.slice(1, 2), true);
    test_equal(&a.slice(2, 2), &b.slice(2, 2), false);
}

#[test]
fn test_null() {
    let a = NullArray::new(2);
    let b = NullArray::new(2);
    test_equal(&a, &b, true);

    let b = NullArray::new(1);
    test_equal(&a, &b, false);
}

fn create_list_array<U: AsRef<[i32]>, T: AsRef<[Option<U>]>>(data: T) -> ListArray {
    let mut builder = ListBuilder::new(Int32Builder::with_capacity(10));
    for d in data.as_ref() {
        if let Some(v) = d {
            builder.values().append_slice(v.as_ref());
            builder.append(true);
        } else {
            builder.append(false);
        }
    }
    builder.finish()
}

#[test]
fn test_list_equal() {
    let a = create_list_array([Some(&[1, 2, 3]), Some(&[4, 5, 6])]);
    let b = create_list_array([Some(&[1, 2, 3]), Some(&[4, 5, 6])]);
    test_equal(&a, &b, true);

    let b = create_list_array([Some(&[1, 2, 3]), Some(&[4, 5, 7])]);
    test_equal(&a, &b, false);
}

#[test]
fn test_empty_offsets_list_equal() {
    let empty: Vec<i32> = vec![];
    let values = Int32Array::from(empty);
    let empty_offsets: [u8; 0] = [];

    let a: ListArray = ArrayDataBuilder::new(DataType::List(Arc::new(Field::new(
        "item",
        DataType::Int32,
        true,
    ))))
    .len(0)
    .add_buffer(Buffer::from(&empty_offsets))
    .add_child_data(values.to_data())
    .null_bit_buffer(Some(Buffer::from(&empty_offsets)))
    .build()
    .unwrap()
    .into();

    let b: ListArray = ArrayDataBuilder::new(DataType::List(Arc::new(Field::new(
        "item",
        DataType::Int32,
        true,
    ))))
    .len(0)
    .add_buffer(Buffer::from(&empty_offsets))
    .add_child_data(values.to_data())
    .null_bit_buffer(Some(Buffer::from(&empty_offsets)))
    .build()
    .unwrap()
    .into();

    test_equal(&a, &b, true);

    let c: ListArray = ArrayDataBuilder::new(DataType::List(Arc::new(Field::new(
        "item",
        DataType::Int32,
        true,
    ))))
    .len(0)
    .add_buffer(Buffer::from([0i32, 2, 3, 4, 6, 7, 8].to_byte_slice()))
    .add_child_data(Int32Array::from(vec![1, 2, -1, -2, 3, 4, -3, -4]).into_data())
    .null_bit_buffer(Some(Buffer::from([0b00001001])))
    .build()
    .unwrap()
    .into();

    test_equal(&a, &c, true);
}

// Test the case where null_count > 0
#[test]
fn test_list_null() {
    let a = create_list_array([Some(&[1, 2]), None, None, Some(&[3, 4]), None, None]);
    let b = create_list_array([Some(&[1, 2]), None, None, Some(&[3, 4]), None, None]);
    test_equal(&a, &b, true);

    let b = create_list_array([
        Some(&[1, 2]),
        None,
        Some(&[5, 6]),
        Some(&[3, 4]),
        None,
        None,
    ]);
    test_equal(&a, &b, false);

    let b = create_list_array([Some(&[1, 2]), None, None, Some(&[3, 5]), None, None]);
    test_equal(&a, &b, false);

    // a list where the nullness of values is determined by the list's bitmap
    let c_values = Int32Array::from(vec![1, 2, -1, -2, 3, 4, -3, -4]);
    let c: ListArray = ArrayDataBuilder::new(DataType::List(Arc::new(Field::new(
        "item",
        DataType::Int32,
        true,
    ))))
    .len(6)
    .add_buffer(Buffer::from([0i32, 2, 3, 4, 6, 7, 8].to_byte_slice()))
    .add_child_data(c_values.into_data())
    .null_bit_buffer(Some(Buffer::from([0b00001001])))
    .build()
    .unwrap()
    .into();

    let d_values = Int32Array::from(vec![
        Some(1),
        Some(2),
        None,
        None,
        Some(3),
        Some(4),
        None,
        None,
    ]);
    let d: ListArray = ArrayDataBuilder::new(DataType::List(Arc::new(Field::new(
        "item",
        DataType::Int32,
        true,
    ))))
    .len(6)
    .add_buffer(Buffer::from([0i32, 2, 3, 4, 6, 7, 8].to_byte_slice()))
    .add_child_data(d_values.into_data())
    .null_bit_buffer(Some(Buffer::from([0b00001001])))
    .build()
    .unwrap()
    .into();
    test_equal(&c, &d, true);
}

// Test the case where offset != 0
#[test]
fn test_list_offsets() {
    let a = create_list_array([Some(&[1, 2]), None, None, Some(&[3, 4]), None, None]);
    let b = create_list_array([Some(&[1, 2]), None, None, Some(&[3, 5]), None, None]);

    let a_slice = a.slice(0, 3);
    let b_slice = b.slice(0, 3);
    test_equal(&a_slice, &b_slice, true);

    let a_slice = a.slice(0, 5);
    let b_slice = b.slice(0, 5);
    test_equal(&a_slice, &b_slice, false);

    let a_slice = a.slice(4, 1);
    let b_slice = b.slice(4, 1);
    test_equal(&a_slice, &b_slice, true);
}

fn create_fixed_size_binary_array<U: AsRef<[u8]>, T: AsRef<[Option<U>]>>(
    data: T,
) -> FixedSizeBinaryArray {
    let mut builder = FixedSizeBinaryBuilder::with_capacity(data.as_ref().len(), 5);

    for d in data.as_ref() {
        if let Some(v) = d {
            builder.append_value(v.as_ref()).unwrap();
        } else {
            builder.append_null();
        }
    }
    builder.finish()
}

#[test]
fn test_fixed_size_binary_equal() {
    let a = create_fixed_size_binary_array([Some(b"hello"), Some(b"world")]);
    let b = create_fixed_size_binary_array([Some(b"hello"), Some(b"world")]);
    test_equal(&a, &b, true);

    let b = create_fixed_size_binary_array([Some(b"hello"), Some(b"arrow")]);
    test_equal(&a, &b, false);
}

// Test the case where null_count > 0
#[test]
fn test_fixed_size_binary_null() {
    let a = create_fixed_size_binary_array([Some(b"hello"), None, Some(b"world")]);
    let b = create_fixed_size_binary_array([Some(b"hello"), None, Some(b"world")]);
    test_equal(&a, &b, true);

    let b = create_fixed_size_binary_array([Some(b"hello"), Some(b"world"), None]);
    test_equal(&a, &b, false);

    let b = create_fixed_size_binary_array([Some(b"hello"), None, Some(b"arrow")]);
    test_equal(&a, &b, false);
}

#[test]
fn test_fixed_size_binary_offsets() {
    // Test the case where offset != 0
    let a =
        create_fixed_size_binary_array([Some(b"hello"), None, None, Some(b"world"), None, None]);
    let b =
        create_fixed_size_binary_array([Some(b"hello"), None, None, Some(b"arrow"), None, None]);

    let a_slice = a.slice(0, 3);
    let b_slice = b.slice(0, 3);
    test_equal(&a_slice, &b_slice, true);

    let a_slice = a.slice(0, 5);
    let b_slice = b.slice(0, 5);
    test_equal(&a_slice, &b_slice, false);

    let a_slice = a.slice(4, 1);
    let b_slice = b.slice(4, 1);
    test_equal(&a_slice, &b_slice, true);

    let a_slice = a.slice(3, 1);
    let b_slice = b.slice(3, 1);
    test_equal(&a_slice, &b_slice, false);
}

fn create_decimal_array(data: Vec<Option<i128>>) -> Decimal128Array {
    data.into_iter()
        .collect::<Decimal128Array>()
        .with_precision_and_scale(23, 6)
        .unwrap()
}

#[test]
fn test_decimal_equal() {
    let a = create_decimal_array(vec![Some(8_887_000_000), Some(-8_887_000_000)]);
    let b = create_decimal_array(vec![Some(8_887_000_000), Some(-8_887_000_000)]);
    test_equal(&a, &b, true);

    let b = create_decimal_array(vec![Some(15_887_000_000), Some(-8_887_000_000)]);
    test_equal(&a, &b, false);
}

// Test the case where null_count > 0
#[test]
fn test_decimal_null() {
    let a = create_decimal_array(vec![Some(8_887_000_000), None, Some(-8_887_000_000)]);
    let b = create_decimal_array(vec![Some(8_887_000_000), None, Some(-8_887_000_000)]);
    test_equal(&a, &b, true);

    let b = create_decimal_array(vec![Some(8_887_000_000), Some(-8_887_000_000), None]);
    test_equal(&a, &b, false);

    let b = create_decimal_array(vec![Some(15_887_000_000), None, Some(-8_887_000_000)]);
    test_equal(&a, &b, false);
}

#[test]
fn test_decimal_offsets() {
    // Test the case where offset != 0
    let a = create_decimal_array(vec![
        Some(8_887_000_000),
        None,
        None,
        Some(-8_887_000_000),
        None,
        None,
    ]);
    let b = create_decimal_array(vec![
        None,
        Some(8_887_000_000),
        None,
        None,
        Some(15_887_000_000),
        None,
        None,
    ]);

    let a_slice = a.slice(0, 3);
    let b_slice = b.slice(1, 3);
    test_equal(&a_slice, &b_slice, true);

    let a_slice = a.slice(0, 5);
    let b_slice = b.slice(1, 5);
    test_equal(&a_slice, &b_slice, false);

    let a_slice = a.slice(4, 1);
    let b_slice = b.slice(5, 1);
    test_equal(&a_slice, &b_slice, true);

    let a_slice = a.slice(3, 3);
    let b_slice = b.slice(4, 3);
    test_equal(&a_slice, &b_slice, false);

    let a_slice = a.slice(1, 3);
    let b_slice = b.slice(2, 3);
    test_equal(&a_slice, &b_slice, false);

    let b = create_decimal_array(vec![
        None,
        None,
        None,
        Some(-8_887_000_000),
        Some(-3_000),
        None,
    ]);
    let a_slice = a.slice(1, 3);
    let b_slice = b.slice(1, 3);
    test_equal(&a_slice, &b_slice, true);
}

/// Create a fixed size list of 2 value lengths
fn create_fixed_size_list_array<U: AsRef<[i32]>, T: AsRef<[Option<U>]>>(
    data: T,
) -> FixedSizeListArray {
    let mut builder = FixedSizeListBuilder::new(Int32Builder::with_capacity(10), 3);

    for d in data.as_ref() {
        if let Some(v) = d {
            builder.values().append_slice(v.as_ref());
            builder.append(true);
        } else {
            for _ in 0..builder.value_length() {
                builder.values().append_null();
            }
            builder.append(false);
        }
    }
    builder.finish()
}

#[test]
fn test_fixed_size_list_equal() {
    let a = create_fixed_size_list_array([Some(&[1, 2, 3]), Some(&[4, 5, 6])]);
    let b = create_fixed_size_list_array([Some(&[1, 2, 3]), Some(&[4, 5, 6])]);
    test_equal(&a, &b, true);

    let b = create_fixed_size_list_array([Some(&[1, 2, 3]), Some(&[4, 5, 7])]);
    test_equal(&a, &b, false);
}

// Test the case where null_count > 0
#[test]
fn test_fixed_list_null() {
    let a =
        create_fixed_size_list_array([Some(&[1, 2, 3]), None, None, Some(&[4, 5, 6]), None, None]);
    let b =
        create_fixed_size_list_array([Some(&[1, 2, 3]), None, None, Some(&[4, 5, 6]), None, None]);
    test_equal(&a, &b, true);

    let b = create_fixed_size_list_array([
        Some(&[1, 2, 3]),
        None,
        Some(&[7, 8, 9]),
        Some(&[4, 5, 6]),
        None,
        None,
    ]);
    test_equal(&a, &b, false);

    let b =
        create_fixed_size_list_array([Some(&[1, 2, 3]), None, None, Some(&[3, 6, 9]), None, None]);
    test_equal(&a, &b, false);

    let b = create_fixed_size_list_array([None, Some(&[4, 5, 6]), None, None]);

    test_equal(&a.slice(2, 4), &b, true);
    test_equal(&a.slice(3, 3), &b.slice(1, 3), true);
}

#[test]
fn test_fixed_list_offsets() {
    // Test the case where offset != 0
    let a =
        create_fixed_size_list_array([Some(&[1, 2, 3]), None, None, Some(&[4, 5, 6]), None, None]);
    let b =
        create_fixed_size_list_array([Some(&[1, 2, 3]), None, None, Some(&[3, 6, 9]), None, None]);

    let a_slice = a.slice(0, 3);
    let b_slice = b.slice(0, 3);
    test_equal(&a_slice, &b_slice, true);

    let a_slice = a.slice(0, 5);
    let b_slice = b.slice(0, 5);
    test_equal(&a_slice, &b_slice, false);

    let a_slice = a.slice(4, 1);
    let b_slice = b.slice(4, 1);
    test_equal(&a_slice, &b_slice, true);
}

#[test]
fn test_struct_equal() {
    let strings: ArrayRef = Arc::new(StringArray::from(vec![
        Some("joe"),
        None,
        None,
        Some("mark"),
        Some("doe"),
    ]));
    let ints: ArrayRef = Arc::new(Int32Array::from(vec![
        Some(1),
        Some(2),
        None,
        Some(4),
        Some(5),
    ]));

    let a = StructArray::try_from(vec![("f1", strings.clone()), ("f2", ints.clone())]).unwrap();

    let b = StructArray::try_from(vec![("f1", strings), ("f2", ints)]).unwrap();

    test_equal(&a, &b, true);
}

#[test]
fn test_struct_equal_null() {
    let strings: ArrayRef = Arc::new(StringArray::from(vec![
        Some("joe"),
        None,
        None,
        Some("mark"),
        Some("doe"),
    ]));
    let ints: ArrayRef = Arc::new(Int32Array::from(vec![
        Some(1),
        Some(2),
        None,
        Some(4),
        Some(5),
    ]));
    let ints_non_null: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 0]));

    let a = ArrayData::builder(DataType::Struct(Fields::from(vec![
        Field::new("f1", DataType::Utf8, true),
        Field::new("f2", DataType::Int32, true),
    ])))
    .null_bit_buffer(Some(Buffer::from([0b00001011])))
    .len(5)
    .add_child_data(strings.to_data())
    .add_child_data(ints.to_data())
    .build()
    .unwrap();
    let a = make_array(a);

    let b = ArrayData::builder(DataType::Struct(Fields::from(vec![
        Field::new("f1", DataType::Utf8, true),
        Field::new("f2", DataType::Int32, true),
    ])))
    .null_bit_buffer(Some(Buffer::from([0b00001011])))
    .len(5)
    .add_child_data(strings.to_data())
    .add_child_data(ints_non_null.to_data())
    .build()
    .unwrap();
    let b = make_array(b);

    test_equal(&a, &b, true);

    // test with arrays that are not equal
    let c_ints_non_null: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 0, 4]));
    let c = ArrayData::builder(DataType::Struct(Fields::from(vec![
        Field::new("f1", DataType::Utf8, true),
        Field::new("f2", DataType::Int32, true),
    ])))
    .null_bit_buffer(Some(Buffer::from([0b00001011])))
    .len(5)
    .add_child_data(strings.to_data())
    .add_child_data(c_ints_non_null.to_data())
    .build()
    .unwrap();
    let c = make_array(c);

    test_equal(&a, &c, false);

    // test a nested struct
    let a = ArrayData::builder(DataType::Struct(
        vec![Field::new("f3", a.data_type().clone(), true)].into(),
    ))
    .null_bit_buffer(Some(Buffer::from([0b00011110])))
    .len(5)
    .add_child_data(a.to_data())
    .build()
    .unwrap();
    let a = make_array(a);

    // reconstruct b, but with different data where the first struct is null
    let strings: ArrayRef = Arc::new(StringArray::from(vec![
        Some("joanne"), // difference
        None,
        None,
        Some("mark"),
        Some("doe"),
    ]));
    let b = ArrayData::builder(DataType::Struct(Fields::from(vec![
        Field::new("f1", DataType::Utf8, true),
        Field::new("f2", DataType::Int32, true),
    ])))
    .null_bit_buffer(Some(Buffer::from([0b00001011])))
    .len(5)
    .add_child_data(strings.to_data())
    .add_child_data(ints_non_null.to_data())
    .build()
    .unwrap();

    let b = ArrayData::builder(DataType::Struct(
        vec![Field::new("f3", b.data_type().clone(), true)].into(),
    ))
    .null_bit_buffer(Some(Buffer::from([0b00011110])))
    .len(5)
    .add_child_data(b)
    .build()
    .unwrap();
    let b = make_array(b);

    test_equal(&a, &b, true);
}

#[test]
fn test_struct_equal_null_variable_size() {
    // the string arrays differ, but where the struct array is null
    let strings1: ArrayRef = Arc::new(StringArray::from(vec![
        Some("joe"),
        None,
        None,
        Some("mark"),
        Some("doel"),
    ]));
    let strings2: ArrayRef = Arc::new(StringArray::from(vec![
        Some("joel"),
        None,
        None,
        Some("mark"),
        Some("doe"),
    ]));

    let a = ArrayData::builder(DataType::Struct(
        vec![Field::new("f1", DataType::Utf8, true)].into(),
    ))
    .null_bit_buffer(Some(Buffer::from([0b00001010])))
    .len(5)
    .add_child_data(strings1.to_data())
    .build()
    .unwrap();
    let a = make_array(a);

    let b = ArrayData::builder(DataType::Struct(
        vec![Field::new("f1", DataType::Utf8, true)].into(),
    ))
    .null_bit_buffer(Some(Buffer::from([0b00001010])))
    .len(5)
    .add_child_data(strings2.to_data())
    .build()
    .unwrap();
    let b = make_array(b);

    test_equal(&a, &b, true);

    // test with arrays that are not equal
    let strings3: ArrayRef = Arc::new(StringArray::from(vec![
        Some("mark"),
        None,
        None,
        Some("doe"),
        Some("joe"),
    ]));
    let c = ArrayData::builder(DataType::Struct(
        vec![Field::new("f1", DataType::Utf8, true)].into(),
    ))
    .null_bit_buffer(Some(Buffer::from([0b00001011])))
    .len(5)
    .add_child_data(strings3.to_data())
    .build()
    .unwrap();
    let c = make_array(c);

    test_equal(&a, &c, false);
}

fn create_dictionary_array(values: &[&str], keys: &[Option<&str>]) -> DictionaryArray<Int16Type> {
    let values = StringArray::from(values.to_vec());
    let mut builder =
        StringDictionaryBuilder::<Int16Type>::new_with_dictionary(keys.len(), &values).unwrap();
    for key in keys {
        if let Some(v) = key {
            builder.append(v).unwrap();
        } else {
            builder.append_null()
        }
    }
    builder.finish()
}

#[test]
fn test_dictionary_equal() {
    // (a, b, c), (1, 2, 1, 3) => (a, b, a, c)
    let a = create_dictionary_array(
        &["a", "b", "c"],
        &[Some("a"), Some("b"), Some("a"), Some("c")],
    );
    // different representation (values and keys are swapped), same result
    let b = create_dictionary_array(
        &["a", "c", "b"],
        &[Some("a"), Some("b"), Some("a"), Some("c")],
    );
    test_equal(&a, &b, true);

    // different len
    let b = create_dictionary_array(&["a", "c", "b"], &[Some("a"), Some("b"), Some("a")]);
    test_equal(&a, &b, false);

    // different key
    let b = create_dictionary_array(
        &["a", "c", "b"],
        &[Some("a"), Some("b"), Some("a"), Some("a")],
    );
    test_equal(&a, &b, false);

    // different values, same keys
    let b = create_dictionary_array(
        &["a", "b", "d"],
        &[Some("a"), Some("b"), Some("a"), Some("d")],
    );
    test_equal(&a, &b, false);
}

#[test]
fn test_dictionary_equal_null() {
    // (a, b, c), (1, 2, 1, 3) => (a, b, a, c)
    let a = create_dictionary_array(&["a", "b", "c"], &[Some("a"), None, Some("a"), Some("c")]);

    // equal to self
    test_equal(&a, &a, true);

    // different representation (values and keys are swapped), same result
    let b = create_dictionary_array(&["a", "c", "b"], &[Some("a"), None, Some("a"), Some("c")]);
    test_equal(&a, &b, true);

    // different null position
    let b = create_dictionary_array(&["a", "c", "b"], &[Some("a"), Some("b"), Some("a"), None]);
    test_equal(&a, &b, false);

    // different key
    let b = create_dictionary_array(&["a", "c", "b"], &[Some("a"), None, Some("a"), Some("a")]);
    test_equal(&a, &b, false);

    // different values, same keys
    let b = create_dictionary_array(&["a", "b", "d"], &[Some("a"), None, Some("a"), Some("d")]);
    test_equal(&a, &b, false);
}

#[test]
fn test_non_null_empty_strings() {
    let s1 = StringArray::from(vec![Some(""), Some(""), Some("")]);
    let data = s1.to_data().into_builder().nulls(None).build().unwrap();
    let s2 = StringArray::from(data);

    // s2 is identical to s1 except that it has no validity buffer but since there
    // are no nulls, s1 and s2 are equal
    test_equal(&s1, &s2, true);
}

#[test]
fn test_null_empty_strings() {
    let s1 = StringArray::from(vec![Some(""), None, Some("")]);
    let data = s1.to_data().into_builder().nulls(None).build().unwrap();
    let s2 = StringArray::from(data);

    // s2 is identical to s1 except that it has no validity buffer since string1 has
    // nulls in it, s1 and s2 are not equal
    test_equal(&s1, &s2, false);
}

#[test]
fn test_union_equal_dense() {
    let mut builder = UnionBuilder::new_dense();
    builder.append::<Int32Type>("a", 1).unwrap();
    builder.append::<Int32Type>("b", 2).unwrap();
    builder.append::<Int32Type>("c", 3).unwrap();
    builder.append::<Int32Type>("a", 4).unwrap();
    builder.append_null::<Int32Type>("a").unwrap();
    builder.append::<Int32Type>("a", 6).unwrap();
    builder.append::<Int32Type>("b", 7).unwrap();
    let union1 = builder.build().unwrap();

    builder = UnionBuilder::new_dense();
    builder.append::<Int32Type>("a", 1).unwrap();
    builder.append::<Int32Type>("b", 2).unwrap();
    builder.append::<Int32Type>("c", 3).unwrap();
    builder.append::<Int32Type>("a", 4).unwrap();
    builder.append_null::<Int32Type>("a").unwrap();
    builder.append::<Int32Type>("a", 6).unwrap();
    builder.append::<Int32Type>("b", 7).unwrap();
    let union2 = builder.build().unwrap();

    builder = UnionBuilder::new_dense();
    builder.append::<Int32Type>("a", 1).unwrap();
    builder.append::<Int32Type>("b", 2).unwrap();
    builder.append::<Int32Type>("c", 3).unwrap();
    builder.append::<Int32Type>("a", 5).unwrap();
    builder.append::<Int32Type>("c", 4).unwrap();
    builder.append::<Int32Type>("a", 6).unwrap();
    builder.append::<Int32Type>("b", 7).unwrap();
    let union3 = builder.build().unwrap();

    builder = UnionBuilder::new_dense();
    builder.append::<Int32Type>("a", 1).unwrap();
    builder.append::<Int32Type>("b", 2).unwrap();
    builder.append::<Int32Type>("c", 3).unwrap();
    builder.append::<Int32Type>("a", 4).unwrap();
    builder.append_null::<Int32Type>("c").unwrap();
    builder.append_null::<Int32Type>("b").unwrap();
    builder.append::<Int32Type>("b", 7).unwrap();
    let union4 = builder.build().unwrap();

    test_equal(&union1, &union2, true);
    test_equal(&union1, &union3, false);
    test_equal(&union1, &union4, false);
}

#[test]
fn test_union_equal_sparse() {
    let mut builder = UnionBuilder::new_sparse();
    builder.append::<Int32Type>("a", 1).unwrap();
    builder.append::<Int32Type>("b", 2).unwrap();
    builder.append::<Int32Type>("c", 3).unwrap();
    builder.append::<Int32Type>("a", 4).unwrap();
    builder.append_null::<Int32Type>("a").unwrap();
    builder.append::<Int32Type>("a", 6).unwrap();
    builder.append::<Int32Type>("b", 7).unwrap();
    let union1 = builder.build().unwrap();

    builder = UnionBuilder::new_sparse();
    builder.append::<Int32Type>("a", 1).unwrap();
    builder.append::<Int32Type>("b", 2).unwrap();
    builder.append::<Int32Type>("c", 3).unwrap();
    builder.append::<Int32Type>("a", 4).unwrap();
    builder.append_null::<Int32Type>("a").unwrap();
    builder.append::<Int32Type>("a", 6).unwrap();
    builder.append::<Int32Type>("b", 7).unwrap();
    let union2 = builder.build().unwrap();

    builder = UnionBuilder::new_sparse();
    builder.append::<Int32Type>("a", 1).unwrap();
    builder.append::<Int32Type>("b", 2).unwrap();
    builder.append::<Int32Type>("c", 3).unwrap();
    builder.append::<Int32Type>("a", 5).unwrap();
    builder.append::<Int32Type>("c", 4).unwrap();
    builder.append::<Int32Type>("a", 6).unwrap();
    builder.append::<Int32Type>("b", 7).unwrap();
    let union3 = builder.build().unwrap();

    builder = UnionBuilder::new_sparse();
    builder.append::<Int32Type>("a", 1).unwrap();
    builder.append::<Int32Type>("b", 2).unwrap();
    builder.append::<Int32Type>("c", 3).unwrap();
    builder.append::<Int32Type>("a", 4).unwrap();
    builder.append_null::<Int32Type>("a").unwrap();
    builder.append_null::<Int32Type>("a").unwrap();
    builder.append::<Int32Type>("b", 7).unwrap();
    let union4 = builder.build().unwrap();

    test_equal(&union1, &union2, true);
    test_equal(&union1, &union3, false);
    test_equal(&union1, &union4, false);
}

#[test]
fn test_union_equal_sparse_slice() {
    let mut builder = UnionBuilder::new_sparse();
    builder.append::<Int32Type>("a", 1).unwrap();
    builder.append::<Int32Type>("a", 2).unwrap();
    builder.append::<Int32Type>("b", 3).unwrap();
    let a1 = builder.build().unwrap();

    let mut builder = UnionBuilder::new_sparse();
    builder.append::<Int32Type>("a", 2).unwrap();
    builder.append::<Int32Type>("b", 3).unwrap();
    let a2 = builder.build().unwrap();

    test_equal(&a1.slice(1, 2), &a2, true)
}

#[test]
fn test_boolean_slice() {
    let array = BooleanArray::from(vec![true; 32]);
    let slice = array.slice(4, 12);
    assert_eq!(&slice, &slice);

    let slice = array.slice(8, 12);
    assert_eq!(&slice, &slice);

    let slice = array.slice(8, 24);
    assert_eq!(&slice, &slice);
}

#[test]
fn test_sliced_nullable_boolean_array() {
    let a = BooleanArray::from(vec![None; 32]);
    let b = BooleanArray::from(vec![true; 32]);
    let slice_a = a.slice(1, 12);
    let slice_b = b.slice(1, 12);
    assert_ne!(&slice_a, &slice_b);
}

#[test]
fn list_array_non_zero_nulls() {
    // Tests handling of list arrays with non-empty null ranges
    let mut builder = ListBuilder::new(Int64Builder::with_capacity(10));
    builder.values().append_value(1);
    builder.values().append_value(2);
    builder.values().append_value(3);
    builder.append(true);
    builder.append(false);
    let array1 = builder.finish();

    let mut builder = ListBuilder::new(Int64Builder::with_capacity(10));
    builder.values().append_value(1);
    builder.values().append_value(2);
    builder.values().append_value(3);
    builder.append(true);
    builder.values().append_null();
    builder.values().append_null();
    builder.append(false);
    let array2 = builder.finish();

    assert_eq!(array1, array2);
}

#[test]
fn test_list_different_offsets() {
    let a = ListArray::from_iter_primitive::<Int32Type, _, _>([
        Some([Some(0), Some(0)]),
        Some([Some(1), Some(2)]),
        Some([None, None]),
    ]);
    let b = ListArray::from_iter_primitive::<Int32Type, _, _>([
        Some([Some(1), Some(2)]),
        Some([None, None]),
        Some([None, None]),
    ]);
    let a_slice = a.slice(1, 2);
    let b_slice = b.slice(0, 2);
    assert_eq!(&a_slice, &b_slice);
}

fn make_struct(elements: Vec<Option<(Option<&'static str>, Option<i32>)>>) -> StructArray {
    let mut builder = StructBuilder::new(
        vec![
            Field::new("f1", DataType::Utf8, true),
            Field::new("f2", DataType::Int32, true),
        ],
        vec![
            Box::new(StringBuilder::new()),
            Box::new(Int32Builder::new()),
        ],
    );

    for element in elements {
        match element.and_then(|e| e.0) {
            None => builder
                .field_builder::<StringBuilder>(0)
                .unwrap()
                .append_null(),
            Some(s) => builder
                .field_builder::<StringBuilder>(0)
                .unwrap()
                .append_value(s),
        };

        builder
            .field_builder::<Int32Builder>(1)
            .unwrap()
            .append_option(element.and_then(|e| e.1));

        builder.append(element.is_some());
    }

    builder.finish()
}

#[test]
fn test_struct_equal_slice() {
    let a = make_struct(vec![
        None,
        Some((Some("joe"), Some(1))),
        Some((None, Some(2))),
        Some((None, None)),
        Some((Some("mark"), Some(4))),
        Some((Some("doe"), Some(5))),
    ]);
    let a = a.slice(1, 5);
    let a = a.as_any().downcast_ref::<StructArray>().unwrap();

    let b = make_struct(vec![
        Some((Some("joe"), Some(1))),
        Some((None, Some(2))),
        Some((None, None)),
        Some((Some("mark"), Some(4))),
        Some((Some("doe"), Some(5))),
    ]);
    assert_eq!(a, &b);

    test_equal(&a, &b, true);
}

#[test]
fn test_list_excess_children_equal() {
    let mut a = ListBuilder::new(FixedSizeBinaryBuilder::new(5));
    a.values().append_value(b"11111").unwrap(); // Masked value
    a.append_null();
    a.values().append_value(b"22222").unwrap();
    a.values().append_null();
    a.append(true);
    let a = a.finish();

    let mut b = ListBuilder::new(FixedSizeBinaryBuilder::new(5));
    b.append_null();
    b.values().append_value(b"22222").unwrap();
    b.values().append_null();
    b.append(true);
    let b = b.finish();

    assert_eq!(a.value_offsets(), &[0, 1, 3]);
    assert_eq!(b.value_offsets(), &[0, 0, 2]);
    assert_eq!(a, b);
}
