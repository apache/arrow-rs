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
    make_array, Array, BooleanBuilder, Decimal128Builder, Int32Array, Int32Builder, Int64Array,
    StringArray, StructBuilder, UInt64Array,
};
use arrow_array::Decimal128Array;
use arrow_buffer::{ArrowNativeType, Buffer};
use arrow_data::ArrayData;
use arrow_schema::{DataType, Field, UnionFields, UnionMode};
use std::ptr::NonNull;
use std::sync::Arc;

#[test]
#[should_panic(expected = "Need at least 80 bytes in buffers[0] in array of type Int64, but got 8")]
fn test_buffer_too_small() {
    let buffer = Buffer::from_slice_ref([0i32, 2i32]);
    // should fail as the declared size (10*8 = 80) is larger than the underlying bfufer (8)
    ArrayData::try_new(DataType::Int64, 10, None, 0, vec![buffer], vec![]).unwrap();
}

#[test]
#[should_panic(expected = "Need at least 16 bytes in buffers[0] in array of type Int64, but got 8")]
fn test_buffer_too_small_offset() {
    let buffer = Buffer::from_slice_ref([0i32, 2i32]);
    // should fail -- size is ok, but also has offset
    ArrayData::try_new(DataType::Int64, 1, None, 1, vec![buffer], vec![]).unwrap();
}

#[test]
#[should_panic(expected = "Expected 1 buffers in array of type Int64, got 2")]
fn test_bad_number_of_buffers() {
    let buffer1 = Buffer::from_slice_ref([0i32, 2i32]);
    let buffer2 = Buffer::from_slice_ref([0i32, 2i32]);
    ArrayData::try_new(DataType::Int64, 1, None, 0, vec![buffer1, buffer2], vec![]).unwrap();
}

#[test]
#[should_panic(
    expected = "Need at least 18446744073709551615 bytes in buffers[0] in array of type Int64, but got 8"
)]
fn test_fixed_width_overflow() {
    let buffer = Buffer::from_slice_ref([0i32, 2i32]);
    ArrayData::try_new(DataType::Int64, usize::MAX, None, 0, vec![buffer], vec![]).unwrap();
}

#[test]
#[should_panic(expected = "null_bit_buffer size too small. got 1 needed 2")]
fn test_bitmap_too_small() {
    let buffer = make_i32_buffer(9);
    let null_bit_buffer = Buffer::from([0b11111111]);

    ArrayData::try_new(
        DataType::Int32,
        9,
        Some(null_bit_buffer),
        0,
        vec![buffer],
        vec![],
    )
    .unwrap();
}

// Test creating a dictionary with a non integer type
#[test]
#[should_panic(expected = "Dictionary key type must be integer, but was Utf8")]
fn test_non_int_dictionary() {
    let i32_buffer = Buffer::from_slice_ref([0i32, 2i32]);
    let data_type = DataType::Dictionary(Box::new(DataType::Utf8), Box::new(DataType::Int32));
    let child_data = ArrayData::try_new(
        DataType::Int32,
        1,
        None,
        0,
        vec![i32_buffer.clone()],
        vec![],
    )
    .unwrap();
    ArrayData::try_new(
        data_type,
        1,
        None,
        0,
        vec![i32_buffer.clone(), i32_buffer],
        vec![child_data],
    )
    .unwrap();
}

#[test]
#[should_panic(expected = "Expected LargeUtf8 but child data had Utf8")]
fn test_mismatched_dictionary_types() {
    // test w/ dictionary created with a child array data that has type different than declared
    let string_array: StringArray = vec![Some("foo"), Some("bar")].into_iter().collect();
    let i32_buffer = Buffer::from_slice_ref([0i32, 1i32]);
    // Dict says LargeUtf8 but array is Utf8
    let data_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::LargeUtf8));
    let child_data = string_array.into_data();
    ArrayData::try_new(data_type, 1, None, 0, vec![i32_buffer], vec![child_data]).unwrap();
}

#[test]
fn test_empty_utf8_array_with_empty_offsets_buffer() {
    let data_buffer = Buffer::from(&[]);
    let offsets_buffer = Buffer::from(&[]);
    ArrayData::try_new(
        DataType::Utf8,
        0,
        None,
        0,
        vec![offsets_buffer, data_buffer],
        vec![],
    )
    .unwrap();
}

#[test]
fn test_empty_utf8_array_with_single_zero_offset() {
    let data_buffer = Buffer::from(&[]);
    let offsets_buffer = Buffer::from_slice_ref([0i32]);
    ArrayData::try_new(
        DataType::Utf8,
        0,
        None,
        0,
        vec![offsets_buffer, data_buffer],
        vec![],
    )
    .unwrap();
}

#[test]
#[should_panic(expected = "First offset 1 of Utf8 is larger than values length 0")]
fn test_empty_utf8_array_with_invalid_offset() {
    let data_buffer = Buffer::from(&[]);
    let offsets_buffer = Buffer::from_slice_ref([1i32]);
    ArrayData::try_new(
        DataType::Utf8,
        0,
        None,
        0,
        vec![offsets_buffer, data_buffer],
        vec![],
    )
    .unwrap();
}

#[test]
fn test_empty_utf8_array_with_non_zero_offset() {
    let data_buffer = Buffer::from_slice_ref("abcdef".as_bytes());
    let offsets_buffer = Buffer::from_slice_ref([0i32, 2, 6, 0]);
    ArrayData::try_new(
        DataType::Utf8,
        0,
        None,
        3,
        vec![offsets_buffer, data_buffer],
        vec![],
    )
    .unwrap();
}

#[test]
#[should_panic(expected = "Buffer 0 of LargeUtf8 isn't large enough. Expected 8 bytes got 4")]
fn test_empty_large_utf8_array_with_wrong_type_offsets() {
    let data_buffer = Buffer::from(&[]);
    let offsets_buffer = Buffer::from_slice_ref([0i32]);
    ArrayData::try_new(
        DataType::LargeUtf8,
        0,
        None,
        0,
        vec![offsets_buffer, data_buffer],
        vec![],
    )
    .unwrap();
}

#[test]
#[should_panic(expected = "Buffer 0 of Utf8 isn't large enough. Expected 12 bytes got 8")]
fn test_validate_offsets_i32() {
    let data_buffer = Buffer::from_slice_ref("abcdef".as_bytes());
    let offsets_buffer = Buffer::from_slice_ref([0i32, 2i32]);
    ArrayData::try_new(
        DataType::Utf8,
        2,
        None,
        0,
        vec![offsets_buffer, data_buffer],
        vec![],
    )
    .unwrap();
}

#[test]
#[should_panic(expected = "Buffer 0 of LargeUtf8 isn't large enough. Expected 24 bytes got 16")]
fn test_validate_offsets_i64() {
    let data_buffer = Buffer::from_slice_ref("abcdef".as_bytes());
    let offsets_buffer = Buffer::from_slice_ref([0i64, 2i64]);
    ArrayData::try_new(
        DataType::LargeUtf8,
        2,
        None,
        0,
        vec![offsets_buffer, data_buffer],
        vec![],
    )
    .unwrap();
}

#[test]
#[should_panic(expected = "Error converting offset[0] (-2) to usize for Utf8")]
fn test_validate_offsets_negative_first_i32() {
    let data_buffer = Buffer::from_slice_ref("abcdef".as_bytes());
    let offsets_buffer = Buffer::from_slice_ref([-2i32, 1i32, 3i32]);
    ArrayData::try_new(
        DataType::Utf8,
        2,
        None,
        0,
        vec![offsets_buffer, data_buffer],
        vec![],
    )
    .unwrap();
}

#[test]
#[should_panic(expected = "Error converting offset[2] (-3) to usize for Utf8")]
fn test_validate_offsets_negative_last_i32() {
    let data_buffer = Buffer::from_slice_ref("abcdef".as_bytes());
    let offsets_buffer = Buffer::from_slice_ref([0i32, 2i32, -3i32]);
    ArrayData::try_new(
        DataType::Utf8,
        2,
        None,
        0,
        vec![offsets_buffer, data_buffer],
        vec![],
    )
    .unwrap();
}

#[test]
#[should_panic(expected = "First offset 4 in Utf8 is smaller than last offset 3")]
fn test_validate_offsets_range_too_small() {
    let data_buffer = Buffer::from_slice_ref("abcdef".as_bytes());
    // start offset is larger than end
    let offsets_buffer = Buffer::from_slice_ref([4i32, 2i32, 3i32]);
    ArrayData::try_new(
        DataType::Utf8,
        2,
        None,
        0,
        vec![offsets_buffer, data_buffer],
        vec![],
    )
    .unwrap();
}

#[test]
#[should_panic(expected = "Last offset 10 of Utf8 is larger than values length 6")]
fn test_validate_offsets_range_too_large() {
    let data_buffer = Buffer::from_slice_ref("abcdef".as_bytes());
    // 10 is off the end of the buffer
    let offsets_buffer = Buffer::from_slice_ref([0i32, 2i32, 10i32]);
    ArrayData::try_new(
        DataType::Utf8,
        2,
        None,
        0,
        vec![offsets_buffer, data_buffer],
        vec![],
    )
    .unwrap();
}

#[test]
#[should_panic(expected = "First offset 10 of Utf8 is larger than values length 6")]
fn test_validate_offsets_first_too_large() {
    let data_buffer = Buffer::from_slice_ref("abcdef".as_bytes());
    // 10 is off the end of the buffer
    let offsets_buffer = Buffer::from_slice_ref([10i32, 2i32, 10i32]);
    ArrayData::try_new(
        DataType::Utf8,
        2,
        None,
        0,
        vec![offsets_buffer, data_buffer],
        vec![],
    )
    .unwrap();
}

#[test]
fn test_validate_offsets_first_too_large_skipped() {
    let data_buffer = Buffer::from_slice_ref("abcdef".as_bytes());
    // 10 is off the end of the buffer, but offset starts at 1 so it is skipped
    let offsets_buffer = Buffer::from_slice_ref([10i32, 2i32, 3i32, 4i32]);
    let data = ArrayData::try_new(
        DataType::Utf8,
        2,
        None,
        1,
        vec![offsets_buffer, data_buffer],
        vec![],
    )
    .unwrap();
    let array: StringArray = data.into();
    let expected: StringArray = vec![Some("c"), Some("d")].into_iter().collect();
    assert_eq!(array, expected);
}

#[test]
#[should_panic(expected = "Last offset 8 of Utf8 is larger than values length 6")]
fn test_validate_offsets_last_too_large() {
    let data_buffer = Buffer::from_slice_ref("abcdef".as_bytes());
    // 10 is off the end of the buffer
    let offsets_buffer = Buffer::from_slice_ref([5i32, 7i32, 8i32]);
    ArrayData::try_new(
        DataType::Utf8,
        2,
        None,
        0,
        vec![offsets_buffer, data_buffer],
        vec![],
    )
    .unwrap();
}

/// Test that the list of type `data_type` generates correct offset and size out of bounds errors
fn check_list_view_offsets_sizes<T: ArrowNativeType>(
    data_type: DataType,
    offsets: Vec<T>,
    sizes: Vec<T>,
) {
    let values: Int32Array = [Some(1), Some(2), Some(3), Some(4)].into_iter().collect();
    let offsets_buffer = Buffer::from_slice_ref(offsets);
    let sizes_buffer = Buffer::from_slice_ref(sizes);
    ArrayData::try_new(
        data_type,
        4,
        None,
        0,
        vec![offsets_buffer, sizes_buffer],
        vec![values.into_data()],
    )
    .unwrap();
}

#[test]
#[should_panic(expected = "Size 3 at index 3 is larger than the remaining values for ListView")]
fn test_validate_list_view_offsets_sizes() {
    let field_type = Field::new("f", DataType::Int32, true);
    check_list_view_offsets_sizes::<i32>(
        DataType::ListView(Arc::new(field_type)),
        vec![0, 1, 1, 2],
        vec![1, 1, 1, 3],
    );
}

#[test]
#[should_panic(
    expected = "Size 3 at index 3 is larger than the remaining values for LargeListView"
)]
fn test_validate_large_list_view_offsets_sizes() {
    let field_type = Field::new("f", DataType::Int32, true);
    check_list_view_offsets_sizes::<i64>(
        DataType::LargeListView(Arc::new(field_type)),
        vec![0, 1, 1, 2],
        vec![1, 1, 1, 3],
    );
}

#[test]
#[should_panic(expected = "Error converting offset[1] (-1) to usize for ListView")]
fn test_validate_list_view_negative_offsets() {
    let field_type = Field::new("f", DataType::Int32, true);
    check_list_view_offsets_sizes::<i32>(
        DataType::ListView(Arc::new(field_type)),
        vec![0, -1, 1, 2],
        vec![1, 1, 1, 3],
    );
}

#[test]
#[should_panic(expected = "Error converting size[2] (-1) to usize for ListView")]
fn test_validate_list_view_negative_sizes() {
    let field_type = Field::new("f", DataType::Int32, true);
    check_list_view_offsets_sizes::<i32>(
        DataType::ListView(Arc::new(field_type)),
        vec![0, 1, 1, 2],
        vec![1, 1, -1, 3],
    );
}

#[test]
#[should_panic(expected = "Error converting offset[1] (-1) to usize for LargeListView")]
fn test_validate_large_list_view_negative_offsets() {
    let field_type = Field::new("f", DataType::Int32, true);
    check_list_view_offsets_sizes::<i64>(
        DataType::LargeListView(Arc::new(field_type)),
        vec![0, -1, 1, 2],
        vec![1, 1, 1, 3],
    );
}

#[test]
#[should_panic(expected = "Error converting size[2] (-1) to usize for LargeListView")]
fn test_validate_large_list_view_negative_sizes() {
    let field_type = Field::new("f", DataType::Int32, true);
    check_list_view_offsets_sizes::<i64>(
        DataType::LargeListView(Arc::new(field_type)),
        vec![0, 1, 1, 2],
        vec![1, 1, -1, 3],
    );
}

#[test]
#[should_panic(
    expected = "Values length 4 is less than the length (2) multiplied by the value size (2) for FixedSizeList"
)]
fn test_validate_fixed_size_list() {
    // child has 4 elements,
    let child_array = vec![Some(1), Some(2), Some(3), None]
        .into_iter()
        .collect::<Int32Array>();

    // but claim we have 3 elements for a fixed size of 2
    // 10 is off the end of the buffer
    let field = Field::new("field", DataType::Int32, true);
    ArrayData::try_new(
        DataType::FixedSizeList(Arc::new(field), 2),
        3,
        None,
        0,
        vec![],
        vec![child_array.into_data()],
    )
    .unwrap();
}

#[test]
#[should_panic(expected = "Child type mismatch for Struct")]
fn test_validate_struct_child_type() {
    let field1 = vec![Some(1), Some(2), Some(3), None]
        .into_iter()
        .collect::<Int32Array>();

    // validate the the type of struct fields matches child fields
    ArrayData::try_new(
        DataType::Struct(vec![Field::new("field1", DataType::Int64, true)].into()),
        3,
        None,
        0,
        vec![],
        vec![field1.into_data()],
    )
    .unwrap();
}

#[test]
#[should_panic(
    expected = "child array #0 for field field1 has length smaller than expected for struct array (4 < 6)"
)]
fn test_validate_struct_child_length() {
    // field length only has 4 items, but array claims to have 6
    let field1 = vec![Some(1), Some(2), Some(3), None]
        .into_iter()
        .collect::<Int32Array>();

    ArrayData::try_new(
        DataType::Struct(vec![Field::new("field1", DataType::Int32, true)].into()),
        6,
        None,
        0,
        vec![],
        vec![field1.into_data()],
    )
    .unwrap();
}

/// Test that the array of type `data_type` that has invalid utf8 data errors
fn check_utf8_validation<T: ArrowNativeType>(data_type: DataType) {
    // 0x80 is a utf8 continuation sequence and is not a valid utf8 sequence itself
    let data_buffer = Buffer::from_slice_ref([b'a', b'a', 0x80, 0x00]);
    let offsets: Vec<T> = [0, 2, 3]
        .iter()
        .map(|&v| T::from_usize(v).unwrap())
        .collect();

    let offsets_buffer = Buffer::from_slice_ref(offsets);
    ArrayData::try_new(
        data_type,
        2,
        None,
        0,
        vec![offsets_buffer, data_buffer],
        vec![],
    )
    .unwrap();
}

#[test]
#[should_panic(expected = "Invalid UTF8 sequence at string index 1 (2..3)")]
fn test_validate_utf8_content() {
    check_utf8_validation::<i32>(DataType::Utf8);
}

#[test]
#[should_panic(expected = "Invalid UTF8 sequence at string index 1 (2..3)")]
fn test_validate_large_utf8_content() {
    check_utf8_validation::<i64>(DataType::LargeUtf8);
}

/// Tests that offsets are at valid codepoint boundaries
fn check_utf8_char_boundary<T: ArrowNativeType>(data_type: DataType) {
    let data_buffer = Buffer::from("🙀".as_bytes());
    let offsets: Vec<T> = [0, 1, data_buffer.len()]
        .iter()
        .map(|&v| T::from_usize(v).unwrap())
        .collect();

    let offsets_buffer = Buffer::from_slice_ref(offsets);
    ArrayData::try_new(
        data_type,
        2,
        None,
        0,
        vec![offsets_buffer, data_buffer],
        vec![],
    )
    .unwrap();
}

#[test]
#[should_panic(expected = "incomplete utf-8 byte sequence from index 0")]
fn test_validate_utf8_char_boundary() {
    check_utf8_char_boundary::<i32>(DataType::Utf8);
}

#[test]
#[should_panic(expected = "incomplete utf-8 byte sequence from index 0")]
fn test_validate_large_utf8_char_boundary() {
    check_utf8_char_boundary::<i64>(DataType::LargeUtf8);
}

/// Test that the array of type `data_type` that has invalid indexes (out of bounds)
fn check_index_out_of_bounds_validation<T: ArrowNativeType>(data_type: DataType) {
    let data_buffer = Buffer::from_slice_ref([b'a', b'b', b'c', b'd']);
    // First two offsets are fine, then 5 is out of bounds
    let offsets: Vec<T> = [0, 1, 2, 5, 2]
        .iter()
        .map(|&v| T::from_usize(v).unwrap())
        .collect();

    let offsets_buffer = Buffer::from_slice_ref(offsets);
    ArrayData::try_new(
        data_type,
        4,
        None,
        0,
        vec![offsets_buffer, data_buffer],
        vec![],
    )
    .unwrap();
}

#[test]
#[should_panic(expected = "Offset invariant failure: offset at position 3 out of bounds: 5 > 4")]
fn test_validate_utf8_out_of_bounds() {
    check_index_out_of_bounds_validation::<i32>(DataType::Utf8);
}

#[test]
#[should_panic(expected = "Offset invariant failure: offset at position 3 out of bounds: 5 > 4")]
fn test_validate_large_utf8_out_of_bounds() {
    check_index_out_of_bounds_validation::<i64>(DataType::LargeUtf8);
}

#[test]
#[should_panic(expected = "Offset invariant failure: offset at position 3 out of bounds: 5 > 4")]
fn test_validate_binary_out_of_bounds() {
    check_index_out_of_bounds_validation::<i32>(DataType::Binary);
}

#[test]
#[should_panic(expected = "Offset invariant failure: offset at position 3 out of bounds: 5 > 4")]
fn test_validate_large_binary_out_of_bounds() {
    check_index_out_of_bounds_validation::<i64>(DataType::LargeBinary);
}

// validate that indexes don't go bacwards check indexes that go backwards
fn check_index_backwards_validation<T: ArrowNativeType>(data_type: DataType) {
    let data_buffer = Buffer::from_slice_ref([b'a', b'b', b'c', b'd']);
    // First three offsets are fine, then 1 goes backwards
    let offsets: Vec<T> = [0, 1, 2, 2, 1]
        .iter()
        .map(|&v| T::from_usize(v).unwrap())
        .collect();

    let offsets_buffer = Buffer::from_slice_ref(offsets);
    ArrayData::try_new(
        data_type,
        4,
        None,
        0,
        vec![offsets_buffer, data_buffer],
        vec![],
    )
    .unwrap();
}

#[test]
#[should_panic(expected = "Offset invariant failure: non-monotonic offset at slot 3: 2 > 1")]
fn test_validate_utf8_index_backwards() {
    check_index_backwards_validation::<i32>(DataType::Utf8);
}

#[test]
#[should_panic(expected = "Offset invariant failure: non-monotonic offset at slot 3: 2 > 1")]
fn test_validate_large_utf8_index_backwards() {
    check_index_backwards_validation::<i64>(DataType::LargeUtf8);
}

#[test]
#[should_panic(expected = "Offset invariant failure: non-monotonic offset at slot 3: 2 > 1")]
fn test_validate_binary_index_backwards() {
    check_index_backwards_validation::<i32>(DataType::Binary);
}

#[test]
#[should_panic(expected = "Offset invariant failure: non-monotonic offset at slot 3: 2 > 1")]
fn test_validate_large_binary_index_backwards() {
    check_index_backwards_validation::<i64>(DataType::LargeBinary);
}

#[test]
#[should_panic(expected = "Value at position 1 out of bounds: 3 (should be in [0, 1])")]
fn test_validate_dictionary_index_too_large() {
    let values: StringArray = [Some("foo"), Some("bar")].into_iter().collect();

    // 3 is not a valid index into the values (only 0 and 1)
    let keys: Int32Array = [Some(1), Some(3)].into_iter().collect();

    let data_type = DataType::Dictionary(
        Box::new(keys.data_type().clone()),
        Box::new(values.data_type().clone()),
    );

    ArrayData::try_new(
        data_type,
        2,
        None,
        0,
        vec![keys.into_data().buffers()[0].clone()],
        vec![values.into_data()],
    )
    .unwrap();
}

#[test]
#[should_panic(expected = "Value at position 1 out of bounds: -1 (should be in [0, 1]")]
fn test_validate_dictionary_index_negative() {
    let values: StringArray = [Some("foo"), Some("bar")].into_iter().collect();

    // -1 is not a valid index at all!
    let keys: Int32Array = [Some(1), Some(-1)].into_iter().collect();

    let data_type = DataType::Dictionary(
        Box::new(keys.data_type().clone()),
        Box::new(values.data_type().clone()),
    );

    ArrayData::try_new(
        data_type,
        2,
        None,
        0,
        vec![keys.into_data().buffers()[0].clone()],
        vec![values.into_data()],
    )
    .unwrap();
}

#[test]
fn test_validate_dictionary_index_negative_but_not_referenced() {
    let values: StringArray = [Some("foo"), Some("bar")].into_iter().collect();

    // -1 is not a valid index at all, but the array is length 1
    // so the -1 should not be looked at
    let keys: Int32Array = [Some(1), Some(-1)].into_iter().collect();

    let data_type = DataType::Dictionary(
        Box::new(keys.data_type().clone()),
        Box::new(values.data_type().clone()),
    );

    // Expect this not to panic
    ArrayData::try_new(
        data_type,
        1,
        None,
        0,
        vec![keys.into_data().buffers()[0].clone()],
        vec![values.into_data()],
    )
    .unwrap();
}

#[test]
#[should_panic(
    expected = "Value at position 0 out of bounds: 18446744073709551615 (can not convert to i64)"
)]
fn test_validate_dictionary_index_giant_negative() {
    let values: StringArray = [Some("foo"), Some("bar")].into_iter().collect();

    // -1 is not a valid index at all!
    let keys: UInt64Array = [Some(u64::MAX), Some(1)].into_iter().collect();

    let data_type = DataType::Dictionary(
        Box::new(keys.data_type().clone()),
        Box::new(values.data_type().clone()),
    );

    ArrayData::try_new(
        data_type,
        2,
        None,
        0,
        vec![keys.into_data().buffers()[0].clone()],
        vec![values.into_data()],
    )
    .unwrap();
}

/// Test that the list of type `data_type` generates correct offset out of bounds errors
fn check_list_offsets<T: ArrowNativeType>(data_type: DataType) {
    let values: Int32Array = [Some(1), Some(2), Some(3), Some(4)].into_iter().collect();

    // 5 is an invalid offset into a list of only three values
    let offsets: Vec<T> = [0, 2, 5, 4]
        .iter()
        .map(|&v| T::from_usize(v).unwrap())
        .collect();
    let offsets_buffer = Buffer::from_slice_ref(offsets);

    ArrayData::try_new(
        data_type,
        3,
        None,
        0,
        vec![offsets_buffer],
        vec![values.into_data()],
    )
    .unwrap();
}

#[test]
#[should_panic(expected = "Offset invariant failure: offset at position 2 out of bounds: 5 > 4")]
fn test_validate_list_offsets() {
    let field_type = Field::new("f", DataType::Int32, true);
    check_list_offsets::<i32>(DataType::List(Arc::new(field_type)));
}

#[test]
#[should_panic(expected = "Offset invariant failure: offset at position 2 out of bounds: 5 > 4")]
fn test_validate_large_list_offsets() {
    let field_type = Field::new("f", DataType::Int32, true);
    check_list_offsets::<i64>(DataType::LargeList(Arc::new(field_type)));
}

/// Test that the list of type `data_type` generates correct errors for negative offsets
#[test]
#[should_panic(
    expected = "Offset invariant failure: Could not convert offset -1 to usize at position 2"
)]
fn test_validate_list_negative_offsets() {
    let values: Int32Array = [Some(1), Some(2), Some(3), Some(4)].into_iter().collect();
    let field_type = Field::new("f", values.data_type().clone(), true);
    let data_type = DataType::List(Arc::new(field_type));

    // -1 is an invalid offset any way you look at it
    let offsets: Vec<i32> = vec![0, 2, -1, 4];
    let offsets_buffer = Buffer::from_slice_ref(offsets);

    ArrayData::try_new(
        data_type,
        3,
        None,
        0,
        vec![offsets_buffer],
        vec![values.into_data()],
    )
    .unwrap();
}

/// returns a buffer initialized with some constant value for tests
fn make_i32_buffer(n: usize) -> Buffer {
    Buffer::from_slice_ref(vec![42i32; n])
}

#[test]
#[should_panic(expected = "Expected Int64 but child data had Int32")]
fn test_validate_union_different_types() {
    let field1 = vec![Some(1), Some(2)].into_iter().collect::<Int32Array>();

    let field2 = vec![Some(1), Some(2)].into_iter().collect::<Int32Array>();

    let type_ids = Buffer::from_slice_ref([0i8, 1i8]);

    ArrayData::try_new(
        DataType::Union(
            UnionFields::new(
                vec![0, 1],
                vec![
                    Field::new("field1", DataType::Int32, true),
                    Field::new("field2", DataType::Int64, true), // data is int32
                ],
            ),
            UnionMode::Sparse,
        ),
        2,
        None,
        0,
        vec![type_ids],
        vec![field1.into_data(), field2.into_data()],
    )
    .unwrap();
}

// sparse with wrong sized children
#[test]
#[should_panic(
    expected = "Sparse union child array #1 has length smaller than expected for union array (1 < 2)"
)]
fn test_validate_union_sparse_different_child_len() {
    let field1 = vec![Some(1), Some(2)].into_iter().collect::<Int32Array>();

    // field 2 only has 1 item but array should have 2
    let field2 = vec![Some(1)].into_iter().collect::<Int64Array>();

    let type_ids = Buffer::from_slice_ref([0i8, 1i8]);

    ArrayData::try_new(
        DataType::Union(
            UnionFields::new(
                vec![0, 1],
                vec![
                    Field::new("field1", DataType::Int32, true),
                    Field::new("field2", DataType::Int64, true),
                ],
            ),
            UnionMode::Sparse,
        ),
        2,
        None,
        0,
        vec![type_ids],
        vec![field1.into_data(), field2.into_data()],
    )
    .unwrap();
}

#[test]
#[should_panic(expected = "Expected 2 buffers in array of type Union")]
fn test_validate_union_dense_without_offsets() {
    let field1 = vec![Some(1), Some(2)].into_iter().collect::<Int32Array>();

    let field2 = vec![Some(1)].into_iter().collect::<Int64Array>();

    let type_ids = Buffer::from_slice_ref([0i8, 1i8]);

    ArrayData::try_new(
        DataType::Union(
            UnionFields::new(
                vec![0, 1],
                vec![
                    Field::new("field1", DataType::Int32, true),
                    Field::new("field2", DataType::Int64, true),
                ],
            ),
            UnionMode::Dense,
        ),
        2,
        None,
        0,
        vec![type_ids], // need offsets buffer here too
        vec![field1.into_data(), field2.into_data()],
    )
    .unwrap();
}

#[test]
#[should_panic(expected = "Need at least 8 bytes in buffers[1] in array of type Union")]
fn test_validate_union_dense_with_bad_len() {
    let field1 = vec![Some(1), Some(2)].into_iter().collect::<Int32Array>();

    let field2 = vec![Some(1)].into_iter().collect::<Int64Array>();

    let type_ids = Buffer::from_slice_ref([0i8, 1i8]);
    let offsets = Buffer::from_slice_ref([0i32]); // should have 2 offsets, but only have 1

    ArrayData::try_new(
        DataType::Union(
            UnionFields::new(
                vec![0, 1],
                vec![
                    Field::new("field1", DataType::Int32, true),
                    Field::new("field2", DataType::Int64, true),
                ],
            ),
            UnionMode::Dense,
        ),
        2,
        None,
        0,
        vec![type_ids, offsets],
        vec![field1.into_data(), field2.into_data()],
    )
    .unwrap();
}

#[test]
fn test_try_new_sliced_struct() {
    let mut builder = StructBuilder::new(
        vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Boolean, true),
        ],
        vec![
            Box::new(Int32Builder::with_capacity(5)),
            Box::new(BooleanBuilder::with_capacity(5)),
        ],
    );

    // struct[0] = { a: 10, b: true }
    builder
        .field_builder::<Int32Builder>(0)
        .unwrap()
        .append_option(Some(10));
    builder
        .field_builder::<BooleanBuilder>(1)
        .unwrap()
        .append_option(Some(true));
    builder.append(true);

    // struct[1] = null
    builder
        .field_builder::<Int32Builder>(0)
        .unwrap()
        .append_option(None);
    builder
        .field_builder::<BooleanBuilder>(1)
        .unwrap()
        .append_option(None);
    builder.append(false);

    // struct[2] = { a: null, b: false }
    builder
        .field_builder::<Int32Builder>(0)
        .unwrap()
        .append_option(None);
    builder
        .field_builder::<BooleanBuilder>(1)
        .unwrap()
        .append_option(Some(false));
    builder.append(true);

    // struct[3] = { a: 21, b: null }
    builder
        .field_builder::<Int32Builder>(0)
        .unwrap()
        .append_option(Some(21));
    builder
        .field_builder::<BooleanBuilder>(1)
        .unwrap()
        .append_option(None);
    builder.append(true);

    // struct[4] = { a: 18, b: false }
    builder
        .field_builder::<Int32Builder>(0)
        .unwrap()
        .append_option(Some(18));
    builder
        .field_builder::<BooleanBuilder>(1)
        .unwrap()
        .append_option(Some(false));
    builder.append(true);

    let struct_array = builder.finish();
    let struct_array_slice = struct_array.slice(1, 3);
    assert_eq!(struct_array_slice, struct_array_slice);
}

#[test]
fn test_string_data_from_foreign() {
    let mut strings = "foobarfoobar".to_owned();
    let mut offsets = vec![0_i32, 0, 3, 6, 12];
    let mut bitmap = vec![0b1110_u8];

    let strings_buffer = unsafe {
        Buffer::from_custom_allocation(
            NonNull::new_unchecked(strings.as_mut_ptr()),
            strings.len(),
            Arc::new(strings),
        )
    };
    let offsets_buffer = unsafe {
        Buffer::from_custom_allocation(
            NonNull::new_unchecked(offsets.as_mut_ptr() as *mut u8),
            offsets.len() * std::mem::size_of::<i32>(),
            Arc::new(offsets),
        )
    };
    let null_buffer = unsafe {
        Buffer::from_custom_allocation(
            NonNull::new_unchecked(bitmap.as_mut_ptr()),
            bitmap.len(),
            Arc::new(bitmap),
        )
    };

    let data = ArrayData::try_new(
        DataType::Utf8,
        4,
        Some(null_buffer),
        0,
        vec![offsets_buffer, strings_buffer],
        vec![],
    )
    .unwrap();

    let array = make_array(data);
    let array = array.as_any().downcast_ref::<StringArray>().unwrap();

    let expected = StringArray::from(vec![None, Some("foo"), Some("bar"), Some("foobar")]);

    assert_eq!(array, &expected);
}

#[test]
fn test_decimal_full_validation() {
    let array = Decimal128Array::from(vec![123456_i128]);
    let error = array.validate_decimal_precision(5).unwrap_err();
    assert_eq!(
        "Invalid argument error: 123456 is too large to store in a Decimal128 of precision 5. Max is 99999",
        error.to_string()
    );
}

#[test]
fn test_decimal_validation() {
    let mut builder = Decimal128Builder::with_capacity(4);
    builder.append_value(10000);
    builder.append_value(20000);
    let array = builder.finish();

    array.into_data().validate_full().unwrap();
}

#[test]
#[cfg(not(feature = "force_validate"))]
fn test_sliced_array_child() {
    let values = Int32Array::from_iter_values([1, 2, 3]);
    let values_sliced = values.slice(1, 2);
    let offsets = Buffer::from_iter([1_i32, 3_i32]);

    let list_field = Field::new("element", DataType::Int32, false);
    let data_type = DataType::List(Arc::new(list_field));

    let data = unsafe {
        ArrayData::new_unchecked(
            data_type,
            1,
            None,
            None,
            0,
            vec![offsets],
            vec![values_sliced.into_data()],
        )
    };

    let err = data.validate_values().unwrap_err();
    assert_eq!(err.to_string(), "Invalid argument error: Offset invariant failure: offset at position 1 out of bounds: 3 > 2");
}
