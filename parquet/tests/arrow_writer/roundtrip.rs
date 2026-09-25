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

//! Round-trip tests for Arrow data written to Parquet.

use super::roundtrip_helpers::{
    RoundTripTest, SMALL_SIZE, required_and_optional, roundtrip, values_required,
};

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::ToByteSlice;
use arrow_array::builder::{
    FixedSizeBinaryBuilder, ListBuilder, PrimitiveDictionaryBuilder, StringViewBuilder,
};
use arrow_array::cast::AsArray;
use arrow_array::types::{
    ArrowDictionaryKeyType, Date32Type, Date64Type, Decimal32Type, Decimal64Type, Decimal128Type,
    Decimal256Type, DecimalType, Float16Type, Int8Type, Int16Type, Int32Type, Int64Type,
    Time32MillisecondType, Time64MicrosecondType, UInt8Type, UInt16Type, UInt32Type,
};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, Date32Array, Date64Array, Decimal32Array,
    Decimal64Array, Decimal128Array, Decimal256Array, DictionaryArray, DurationMicrosecondArray,
    DurationMillisecondArray, DurationNanosecondArray, DurationSecondArray, FixedSizeBinaryArray,
    Float16Array, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int32DictionaryArray, Int64Array, LargeBinaryArray, LargeListArray, LargeListViewArray,
    LargeStringArray, ListArray, ListViewArray, NullArray, PrimitiveArray, RecordBatch,
    RecordBatchReader, StringArray, StringViewArray, StructArray, Time32MillisecondArray,
    Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt8DictionaryArray, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_buffer::{ArrowNativeType, Buffer, NullBuffer, i256};
use arrow_data::{ArrayData, ArrayDataBuilder};
use arrow_schema::{DataType as ArrowDataType, Field, Fields, Schema, TimeUnit};
use bytes::Bytes;
use half::f16;
use num_traits::{FromPrimitive, PrimInt, ToPrimitive};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::basic::Type as PhysicalType;
use parquet::errors::Result;
use parquet::file::properties::WriterProperties;

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn i8_single_column() {
    required_and_optional::<Int8Array, _>(0..SMALL_SIZE as i8);
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn i16_single_column() {
    required_and_optional::<Int16Array, _>(0..SMALL_SIZE as i16);
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn i32_single_column() {
    required_and_optional::<Int32Array, _>(0..SMALL_SIZE as i32);
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn i64_single_column() {
    required_and_optional::<Int64Array, _>(0..SMALL_SIZE as i64);
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn u8_single_column() {
    required_and_optional::<UInt8Array, _>(0..SMALL_SIZE as u8);
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn u16_single_column() {
    required_and_optional::<UInt16Array, _>(0..SMALL_SIZE as u16);
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn u32_single_column() {
    required_and_optional::<UInt32Array, _>(0..SMALL_SIZE as u32);
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn u64_single_column() {
    required_and_optional::<UInt64Array, _>(0..SMALL_SIZE as u64);
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn f32_single_column() {
    required_and_optional::<Float32Array, _>((0..SMALL_SIZE).map(|i| i as f32));
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn f64_single_column() {
    required_and_optional::<Float64Array, _>((0..SMALL_SIZE).map(|i| i as f64));
}

// The timestamp array types don't implement From<Vec<T>> because they need the timezone
// argument, and they also doesn't support building from a Vec<Option<T>>, so call
// RoundTripTest manually instead of calling required_and_optional for these tests.

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn timestamp_second_single_column() {
    let raw_values: Vec<_> = (0..SMALL_SIZE as i64).collect();
    let values = Arc::new(TimestampSecondArray::from(raw_values));

    RoundTripTest::new(values).with_nullable(false).run();
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn timestamp_millisecond_single_column() {
    let raw_values: Vec<_> = (0..SMALL_SIZE as i64).collect();
    let values = Arc::new(TimestampMillisecondArray::from(raw_values));

    RoundTripTest::new(values).with_nullable(false).run();
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn timestamp_microsecond_single_column() {
    let raw_values: Vec<_> = (0..SMALL_SIZE as i64).collect();
    let values = Arc::new(TimestampMicrosecondArray::from(raw_values));

    RoundTripTest::new(values).with_nullable(false).run();
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn timestamp_nanosecond_single_column() {
    let raw_values: Vec<_> = (0..SMALL_SIZE as i64).collect();
    let values = Arc::new(TimestampNanosecondArray::from(raw_values));

    RoundTripTest::new(values).with_nullable(false).run();
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn date32_single_column() {
    required_and_optional::<Date32Array, _>(0..SMALL_SIZE as i32);
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn date64_single_column() {
    // Date64 must be a multiple of 86400000, see ARROW-10925
    required_and_optional::<Date64Array, _>((0..(SMALL_SIZE as i64 * 86400000)).step_by(86400000));
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn time32_second_single_column() {
    required_and_optional::<Time32SecondArray, _>(0..SMALL_SIZE as i32);
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn time32_millisecond_single_column() {
    required_and_optional::<Time32MillisecondArray, _>(0..SMALL_SIZE as i32);
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn time64_microsecond_single_column() {
    required_and_optional::<Time64MicrosecondArray, _>(0..SMALL_SIZE as i64);
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn time64_nanosecond_single_column() {
    required_and_optional::<Time64NanosecondArray, _>(0..SMALL_SIZE as i64);
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn duration_second_single_column() {
    required_and_optional::<DurationSecondArray, _>(0..SMALL_SIZE as i64);
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn duration_millisecond_single_column() {
    required_and_optional::<DurationMillisecondArray, _>(0..SMALL_SIZE as i64);
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn duration_microsecond_single_column() {
    required_and_optional::<DurationMicrosecondArray, _>(0..SMALL_SIZE as i64);
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn duration_nanosecond_single_column() {
    required_and_optional::<DurationNanosecondArray, _>(0..SMALL_SIZE as i64);
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn binary_single_column() {
    let one_vec: Vec<u8> = (0..SMALL_SIZE as u8).collect();
    let many_vecs: Vec<_> = std::iter::repeat_n(one_vec, SMALL_SIZE).collect();
    let many_vecs_iter = many_vecs.iter().map(|v| v.as_slice());

    // BinaryArrays can't be built from Vec<Option<&str>>, so only call `values_required`
    values_required::<BinaryArray, _>(many_vecs_iter);
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn binary_view_single_column() {
    let one_vec: Vec<u8> = (0..SMALL_SIZE as u8).collect();
    let many_vecs: Vec<_> = std::iter::repeat_n(one_vec, SMALL_SIZE).collect();
    let many_vecs_iter = many_vecs.iter().map(|v| v.as_slice());

    // BinaryArrays can't be built from Vec<Option<&str>>, so only call `values_required`
    values_required::<BinaryViewArray, _>(many_vecs_iter);
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn large_binary_single_column() {
    let one_vec: Vec<u8> = (0..SMALL_SIZE as u8).collect();
    let many_vecs: Vec<_> = std::iter::repeat_n(one_vec, SMALL_SIZE).collect();
    let many_vecs_iter = many_vecs.iter().map(|v| v.as_slice());

    // LargeBinaryArrays can't be built from Vec<Option<&str>>, so only call `values_required`
    values_required::<LargeBinaryArray, _>(many_vecs_iter);
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn fixed_size_binary_single_column() {
    let mut builder = FixedSizeBinaryBuilder::new(4);
    builder.append_value(b"0123").unwrap();
    builder.append_null();
    builder.append_value(b"8910").unwrap();
    builder.append_value(b"1112").unwrap();
    let array = Arc::new(builder.finish());

    RoundTripTest::new(array).run();
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn string_single_column() {
    let raw_values: Vec<_> = (0..SMALL_SIZE).map(|i| i.to_string()).collect();
    let raw_strs = raw_values.iter().map(|s| s.as_str());

    required_and_optional::<StringArray, _>(raw_strs);
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn large_string_single_column() {
    let raw_values: Vec<_> = (0..SMALL_SIZE).map(|i| i.to_string()).collect();
    let raw_strs = raw_values.iter().map(|s| s.as_str());

    required_and_optional::<LargeStringArray, _>(raw_strs);
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn string_view_single_column() {
    let raw_values: Vec<_> = (0..SMALL_SIZE).map(|i| i.to_string()).collect();
    let raw_strs = raw_values.iter().map(|s| s.as_str());

    required_and_optional::<StringViewArray, _>(raw_strs);
}

#[test]
fn null_list_single_column() {
    let null_field = Field::new_list_field(ArrowDataType::Null, true);
    let list_field = Field::new("emptylist", ArrowDataType::List(Arc::new(null_field)), true);

    let schema = Schema::new(vec![list_field]);

    // Build [[], null, [null, null]]
    let a_values = NullArray::new(2);
    let a_value_offsets = arrow::buffer::Buffer::from([0, 0, 0, 2].to_byte_slice());
    let a_list_data = ArrayData::builder(ArrowDataType::List(Arc::new(Field::new_list_field(
        ArrowDataType::Null,
        true,
    ))))
    .len(3)
    .add_buffer(a_value_offsets)
    .null_bit_buffer(Some(Buffer::from([0b00000101])))
    .add_child_data(a_values.into_data())
    .build()
    .unwrap();

    let a = ListArray::from(a_list_data);

    assert!(a.is_valid(0));
    assert!(!a.is_valid(1));
    assert!(a.is_valid(2));

    assert_eq!(a.value(0).len(), 0);
    assert_eq!(a.value(2).len(), 2);
    assert_eq!(a.value(2).logical_nulls().unwrap().null_count(), 2);

    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();
    roundtrip(batch, None);
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn list_single_column() {
    let a_values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let a_value_offsets = arrow::buffer::Buffer::from([0, 1, 3, 3, 6, 10].to_byte_slice());
    let a_list_data = ArrayData::builder(ArrowDataType::List(Arc::new(Field::new_list_field(
        ArrowDataType::Int32,
        false,
    ))))
    .len(5)
    .add_buffer(a_value_offsets)
    .null_bit_buffer(Some(Buffer::from([0b00011011])))
    .add_child_data(a_values.into_data())
    .build()
    .unwrap();

    assert_eq!(a_list_data.null_count(), 1);

    let a = ListArray::from(a_list_data);
    let values = Arc::new(a);

    RoundTripTest::new(values).run();
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn large_list_single_column() {
    let a_values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let a_value_offsets = arrow::buffer::Buffer::from([0i64, 1, 3, 3, 6, 10].to_byte_slice());
    let a_list_data = ArrayData::builder(ArrowDataType::LargeList(Arc::new(Field::new(
        "large_item",
        ArrowDataType::Int32,
        true,
    ))))
    .len(5)
    .add_buffer(a_value_offsets)
    .add_child_data(a_values.into_data())
    .null_bit_buffer(Some(Buffer::from([0b00011011])))
    .build()
    .unwrap();

    // I think this setup is incorrect because this should pass
    assert_eq!(a_list_data.null_count(), 1);

    let a = LargeListArray::from(a_list_data);
    let values = Arc::new(a);

    RoundTripTest::new(values).run();
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn list_nested_nulls() {
    use arrow::datatypes::Int32Type;
    let data = vec![
        Some(vec![Some(1)]),
        Some(vec![Some(2), Some(3)]),
        None,
        Some(vec![Some(4), Some(5), None]),
        Some(vec![None]),
        Some(vec![Some(6), Some(7)]),
    ];

    let list = ListArray::from_iter_primitive::<Int32Type, _, _>(data.clone());
    RoundTripTest::new(Arc::new(list)).run();

    let list = LargeListArray::from_iter_primitive::<Int32Type, _, _>(data);
    RoundTripTest::new(Arc::new(list)).run();
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn list_utf8_view_selective_padding_roundtrip() {
    let item = Arc::new(Field::new_list_field(ArrowDataType::Utf8View, true));
    let mut builder = ListBuilder::new(StringViewBuilder::new()).with_field(item);
    builder.values().append_value("a");
    builder.values().append_null();
    builder.append(true);
    // The null parent list covers selective padding dropping values below
    // the list definition level while preserving the preceding item null.
    builder.append(false);
    // The long string covers the non-inlined Utf8View buffer path.
    builder.values().append_value("large payload over 12 bytes");
    builder.append(true);

    RoundTripTest::new(Arc::new(builder.finish())).run();
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn struct_single_column() {
    let a_values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let struct_field_a = Arc::new(Field::new("f", ArrowDataType::Int32, false));
    let s = StructArray::from(vec![(struct_field_a, Arc::new(a_values) as ArrayRef)]);

    let values = Arc::new(s);
    RoundTripTest::new(values).with_nullable(false).run();
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn arrow_writer_list() {
    // define schema
    let schema = Schema::new(vec![Field::new(
        "a",
        ArrowDataType::List(Arc::new(Field::new_list_field(ArrowDataType::Int32, false))),
        true,
    )]);

    // create some data
    let a_values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

    // Construct a buffer for value offsets, for the nested array:
    //  [[1], [2, 3], null, [4, 5, 6], [7, 8, 9, 10]]
    let a_value_offsets = arrow::buffer::Buffer::from([0, 1, 3, 3, 6, 10].to_byte_slice());

    // Construct a list array from the above two
    let a_list_data = ArrayData::builder(ArrowDataType::List(Arc::new(Field::new_list_field(
        ArrowDataType::Int32,
        false,
    ))))
    .len(5)
    .add_buffer(a_value_offsets)
    .add_child_data(a_values.into_data())
    .null_bit_buffer(Some(Buffer::from([0b00011011])))
    .build()
    .unwrap();
    let a = ListArray::from(a_list_data);
    assert_eq!(a.null_count(), 1);

    RoundTripTest::new(Arc::new(a))
        .with_schema(Arc::new(schema))
        .run();
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn arrow_writer_list_non_null() {
    // define schema
    let schema = Schema::new(vec![Field::new(
        "a",
        ArrowDataType::List(Arc::new(Field::new_list_field(ArrowDataType::Int32, false))),
        false,
    )]);

    // create some data
    let a_values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

    // Construct a buffer for value offsets, for the nested array:
    //  [[1], [2, 3], [], [4, 5, 6], [7, 8, 9, 10]]
    let a_value_offsets = arrow::buffer::Buffer::from([0, 1, 3, 3, 6, 10].to_byte_slice());

    // Construct a list array from the above two
    let a_list_data = ArrayData::builder(ArrowDataType::List(Arc::new(Field::new_list_field(
        ArrowDataType::Int32,
        false,
    ))))
    .len(5)
    .add_buffer(a_value_offsets)
    .add_child_data(a_values.into_data())
    .build()
    .unwrap();
    let a = ListArray::from(a_list_data);
    assert_eq!(a.null_count(), 0);

    RoundTripTest::new(Arc::new(a))
        .with_schema(Arc::new(schema))
        .run();
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn arrow_writer_list_view() {
    let list_field = Arc::new(Field::new_list_field(ArrowDataType::Int32, false));
    let schema = Schema::new(vec![Field::new(
        "a",
        ArrowDataType::ListView(list_field.clone()),
        true,
    )]);

    //  [[1], [2, 3], null, [4, 5, 6], [7, 8, 9, 10]]
    let a = ListViewArray::new(
        list_field,
        vec![0, 1, 0, 3, 6].into(),
        vec![1, 2, 0, 3, 4].into(),
        Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10])),
        Some(vec![true, true, false, true, true].into()),
    );
    assert_eq!(a.null_count(), 1);

    RoundTripTest::new(Arc::new(a))
        .with_schema(Arc::new(schema))
        .run();
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn arrow_writer_list_view_non_null() {
    let list_field = Arc::new(Field::new_list_field(ArrowDataType::Int32, false));
    let schema = Schema::new(vec![Field::new(
        "a",
        ArrowDataType::ListView(list_field.clone()),
        false,
    )]);

    //  [[1], [2, 3], [], [4, 5, 6], [7, 8, 9, 10]]
    let a = ListViewArray::new(
        list_field,
        vec![0, 1, 0, 3, 6].into(),
        vec![1, 2, 0, 3, 4].into(),
        Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10])),
        None,
    );
    assert_eq!(a.null_count(), 0);

    RoundTripTest::new(Arc::new(a))
        .with_schema(Arc::new(schema))
        .run();
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn arrow_writer_list_view_out_of_order() {
    let list_field = Arc::new(Field::new_list_field(ArrowDataType::Int32, false));
    let schema = Schema::new(vec![Field::new(
        "a",
        ArrowDataType::ListView(list_field.clone()),
        false,
    )]);

    // [[1], [2, 3], [], [7, 8, 9, 10], [4, 5, 6]] - out of order offsets
    let a = ListViewArray::new(
        list_field,
        vec![0, 1, 0, 6, 3].into(),
        vec![1, 2, 0, 4, 3].into(),
        Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10])),
        None,
    );
    assert_eq!(a.null_count(), 0);

    RoundTripTest::new(Arc::new(a))
        .with_schema(Arc::new(schema))
        .run();
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn arrow_writer_large_list_view() {
    let list_field = Arc::new(Field::new_list_field(ArrowDataType::Int32, false));
    let schema = Schema::new(vec![Field::new(
        "a",
        ArrowDataType::LargeListView(list_field.clone()),
        true,
    )]);

    //  [[1], [2, 3], null, [4, 5, 6], [7, 8, 9, 10]]
    let a = LargeListViewArray::new(
        list_field,
        vec![0i64, 1, 0, 3, 6].into(),
        vec![1i64, 2, 0, 3, 4].into(),
        Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10])),
        Some(vec![true, true, false, true, true].into()),
    );
    assert_eq!(a.null_count(), 1);

    RoundTripTest::new(Arc::new(a))
        .with_schema(Arc::new(schema))
        .run();
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn arrow_writer_list_view_with_struct() {
    // Test ListView containing Struct: ListView<Struct<Int32, Utf8>>
    let struct_fields = Fields::from(vec![
        Field::new("id", ArrowDataType::Int32, false),
        Field::new("name", ArrowDataType::Utf8, false),
    ]);
    let struct_type = ArrowDataType::Struct(struct_fields.clone());
    let list_field = Arc::new(Field::new("item", struct_type.clone(), false));

    let schema = Schema::new(vec![Field::new(
        "a",
        ArrowDataType::ListView(list_field.clone()),
        true,
    )]);

    // Create struct values
    let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let name_array = StringArray::from(vec!["a", "b", "c", "d", "e"]);
    let struct_array = StructArray::new(
        struct_fields,
        vec![Arc::new(id_array), Arc::new(name_array)],
        None,
    );

    // Create ListView: [{1, "a"}, {2, "b"}], null, [{3, "c"}, {4, "d"}, {5, "e"}]
    let list_view = ListViewArray::new(
        list_field,
        vec![0, 2, 2].into(), // offsets
        vec![2, 0, 3].into(), // sizes
        Arc::new(struct_array),
        Some(vec![true, false, true].into()),
    );
    assert_eq!(list_view.null_count(), 1);

    RoundTripTest::new(Arc::new(list_view))
        .with_schema(Arc::new(schema))
        .run();
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn arrow_writer_complex() {
    // define schema
    let struct_field_d = Arc::new(Field::new("d", ArrowDataType::Float64, true));
    let struct_field_f = Arc::new(Field::new("f", ArrowDataType::Float32, true));
    let struct_field_g = Arc::new(Field::new_list(
        "g",
        Field::new_list_field(ArrowDataType::Int16, true),
        false,
    ));
    let struct_field_h = Arc::new(Field::new_list(
        "h",
        Field::new_list_field(ArrowDataType::Int16, false),
        true,
    ));
    let struct_field_e = Arc::new(Field::new_struct(
        "e",
        vec![
            struct_field_f.clone(),
            struct_field_g.clone(),
            struct_field_h.clone(),
        ],
        false,
    ));
    let schema = Schema::new(vec![
        Field::new("a", ArrowDataType::Int32, false),
        Field::new("b", ArrowDataType::Int32, true),
        Field::new_struct(
            "c",
            vec![struct_field_d.clone(), struct_field_e.clone()],
            false,
        ),
    ]);

    // create some data
    let a = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let b = Int32Array::from(vec![Some(1), None, None, Some(4), Some(5)]);
    let d = Float64Array::from(vec![None, None, None, Some(1.0), None]);
    let f = Float32Array::from(vec![Some(0.0), None, Some(333.3), None, Some(5.25)]);

    let g_value = Int16Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

    // Construct a buffer for value offsets, for the nested array:
    //  [[1], [2, 3], [], [4, 5, 6], [7, 8, 9, 10]]
    let g_value_offsets = arrow::buffer::Buffer::from([0, 1, 3, 3, 6, 10].to_byte_slice());

    // Construct a list array from the above two
    let g_list_data = ArrayData::builder(struct_field_g.data_type().clone())
        .len(5)
        .add_buffer(g_value_offsets.clone())
        .add_child_data(g_value.to_data())
        .build()
        .unwrap();
    let g = ListArray::from(g_list_data);
    // The difference between g and h is that h has a null bitmap
    let h_list_data = ArrayData::builder(struct_field_h.data_type().clone())
        .len(5)
        .add_buffer(g_value_offsets)
        .add_child_data(g_value.to_data())
        .null_bit_buffer(Some(Buffer::from([0b00011011])))
        .build()
        .unwrap();
    let h = ListArray::from(h_list_data);

    let e = StructArray::from(vec![
        (struct_field_f, Arc::new(f) as ArrayRef),
        (struct_field_g, Arc::new(g) as ArrayRef),
        (struct_field_h, Arc::new(h) as ArrayRef),
    ]);

    let c = StructArray::from(vec![
        (struct_field_d, Arc::new(d) as ArrayRef),
        (struct_field_e, Arc::new(e) as ArrayRef),
    ]);

    // build a record batch
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(a), Arc::new(b), Arc::new(c)],
    )
    .unwrap();

    roundtrip(batch.clone(), Some(SMALL_SIZE / 2));
    roundtrip(batch, Some(SMALL_SIZE / 3));
}

#[test]
fn arrow_writer_complex_mixed() {
    // This test was added while investigating https://github.com/apache/arrow-rs/issues/244.
    // It was subsequently fixed while investigating https://github.com/apache/arrow-rs/issues/245.

    // define schema
    let offset_field = Arc::new(Field::new("offset", ArrowDataType::Int32, false));
    let partition_field = Arc::new(Field::new("partition", ArrowDataType::Int64, true));
    let topic_field = Arc::new(Field::new("topic", ArrowDataType::Utf8, true));
    let schema = Schema::new(vec![Field::new(
        "some_nested_object",
        ArrowDataType::Struct(Fields::from(vec![
            offset_field.clone(),
            partition_field.clone(),
            topic_field.clone(),
        ])),
        false,
    )]);

    // create some data
    let offset = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let partition = Int64Array::from(vec![Some(1), None, None, Some(4), Some(5)]);
    let topic = StringArray::from(vec![Some("A"), None, Some("A"), Some(""), None]);

    let some_nested_object = StructArray::from(vec![
        (offset_field, Arc::new(offset) as ArrayRef),
        (partition_field, Arc::new(partition) as ArrayRef),
        (topic_field, Arc::new(topic) as ArrayRef),
    ]);

    // build a record batch
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(some_nested_object)]).unwrap();

    roundtrip(batch, Some(SMALL_SIZE / 2));
}

#[test]
fn arrow_writer_map() {
    // Note: we are using the JSON Arrow reader for brevity
    let json_content = r#"
        {"stocks":{"long": "$AAA", "short": "$BBB"}}
        {"stocks":{"long": null, "long": "$CCC", "short": null}}
        {"stocks":{"hedged": "$YYY", "long": null, "short": "$D"}}
        "#;
    let entries_struct_type = ArrowDataType::Struct(Fields::from(vec![
        Field::new(
            Field::MAP_KEY_FIELD_DEFAULT_NAME,
            ArrowDataType::Utf8,
            false,
        ),
        Field::new(
            Field::MAP_VALUE_FIELD_DEFAULT_NAME,
            ArrowDataType::Utf8,
            true,
        ),
    ]));
    let stocks_field = Field::new(
        "stocks",
        ArrowDataType::Map(
            Arc::new(Field::new(
                Field::MAP_ENTRIES_FIELD_DEFAULT_NAME,
                entries_struct_type,
                false,
            )),
            false,
        ),
        true,
    );
    let schema = Arc::new(Schema::new(vec![stocks_field]));
    let builder = arrow::json::ReaderBuilder::new(schema).with_batch_size(64);
    let mut reader = builder.build(std::io::Cursor::new(json_content)).unwrap();

    let batch = reader.next().unwrap().unwrap();
    roundtrip(batch, None);
}

#[test]
fn arrow_writer_2_level_struct() {
    // tests writing <struct<struct<primitive>>
    let field_c = Field::new("c", ArrowDataType::Int32, true);
    let field_b = Field::new("b", ArrowDataType::Struct(vec![field_c].into()), true);
    let type_a = ArrowDataType::Struct(vec![field_b.clone()].into());
    let field_a = Field::new("a", type_a, true);
    let schema = Schema::new(vec![field_a.clone()]);

    // create data
    let c = Int32Array::from(vec![Some(1), None, Some(3), None, None, Some(6)]);
    let b_data = ArrayDataBuilder::new(field_b.data_type().clone())
        .len(6)
        .null_bit_buffer(Some(Buffer::from([0b00100111])))
        .add_child_data(c.into_data())
        .build()
        .unwrap();
    let b = StructArray::from(b_data);
    let a_data = ArrayDataBuilder::new(field_a.data_type().clone())
        .len(6)
        .null_bit_buffer(Some(Buffer::from([0b00101111])))
        .add_child_data(b.into_data())
        .build()
        .unwrap();
    let a = StructArray::from(a_data);

    assert_eq!(a.null_count(), 1);
    assert_eq!(a.column(0).null_count(), 2);

    // build a racord batch
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();

    roundtrip(batch, Some(SMALL_SIZE / 2));
}

#[test]
fn arrow_writer_2_level_struct_non_null() {
    // tests writing <struct<struct<primitive>>
    let field_c = Field::new("c", ArrowDataType::Int32, false);
    let type_b = ArrowDataType::Struct(vec![field_c].into());
    let field_b = Field::new("b", type_b.clone(), false);
    let type_a = ArrowDataType::Struct(vec![field_b].into());
    let field_a = Field::new("a", type_a.clone(), false);
    let schema = Schema::new(vec![field_a]);

    // create data
    let c = Int32Array::from(vec![1, 2, 3, 4, 5, 6]);
    let b_data = ArrayDataBuilder::new(type_b)
        .len(6)
        .add_child_data(c.into_data())
        .build()
        .unwrap();
    let b = StructArray::from(b_data);
    let a_data = ArrayDataBuilder::new(type_a)
        .len(6)
        .add_child_data(b.into_data())
        .build()
        .unwrap();
    let a = StructArray::from(a_data);

    assert_eq!(a.null_count(), 0);
    assert_eq!(a.column(0).null_count(), 0);

    // build a racord batch
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();

    roundtrip(batch, Some(SMALL_SIZE / 2));
}

#[test]
fn arrow_writer_2_level_struct_mixed_null() {
    // tests writing <struct<struct<primitive>>
    let field_c = Field::new("c", ArrowDataType::Int32, false);
    let type_b = ArrowDataType::Struct(vec![field_c].into());
    let field_b = Field::new("b", type_b.clone(), true);
    let type_a = ArrowDataType::Struct(vec![field_b].into());
    let field_a = Field::new("a", type_a.clone(), false);
    let schema = Schema::new(vec![field_a]);

    // create data
    let c = Int32Array::from(vec![1, 2, 3, 4, 5, 6]);
    let b_data = ArrayDataBuilder::new(type_b)
        .len(6)
        .null_bit_buffer(Some(Buffer::from([0b00100111])))
        .add_child_data(c.into_data())
        .build()
        .unwrap();
    let b = StructArray::from(b_data);
    // a intentionally has no null buffer, to test that this is handled correctly
    let a_data = ArrayDataBuilder::new(type_a)
        .len(6)
        .add_child_data(b.into_data())
        .build()
        .unwrap();
    let a = StructArray::from(a_data);

    assert_eq!(a.null_count(), 0);
    assert_eq!(a.column(0).null_count(), 2);

    // build a racord batch
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();

    roundtrip(batch, Some(SMALL_SIZE / 2));
}

#[test]
fn arrow_writer_2_level_struct_mixed_null_2() {
    // tests writing <struct<struct<primitive>>, where the primitive columns are non-null.
    let field_c = Field::new("c", ArrowDataType::Int32, false);
    let field_d = Field::new("d", ArrowDataType::FixedSizeBinary(4), false);
    let field_e = Field::new(
        "e",
        ArrowDataType::Dictionary(
            Box::new(ArrowDataType::Int32),
            Box::new(ArrowDataType::Utf8),
        ),
        false,
    );

    let field_b = Field::new(
        "b",
        ArrowDataType::Struct(vec![field_c, field_d, field_e].into()),
        false,
    );
    let type_a = ArrowDataType::Struct(vec![field_b.clone()].into());
    let field_a = Field::new("a", type_a, true);
    let schema = Schema::new(vec![field_a.clone()]);

    // create data
    let c = Int32Array::from_iter_values(0..6);
    let d = FixedSizeBinaryArray::try_from_iter(
        ["aaaa", "bbbb", "cccc", "dddd", "eeee", "ffff"].into_iter(),
    )
    .expect("four byte values");
    let e = Int32DictionaryArray::from_iter(["one", "two", "three", "four", "five", "one"]);
    let b_data = ArrayDataBuilder::new(field_b.data_type().clone())
        .len(6)
        .add_child_data(c.into_data())
        .add_child_data(d.into_data())
        .add_child_data(e.into_data())
        .build()
        .unwrap();
    let b = StructArray::from(b_data);
    let a_data = ArrayDataBuilder::new(field_a.data_type().clone())
        .len(6)
        .null_bit_buffer(Some(Buffer::from([0b00100101])))
        .add_child_data(b.into_data())
        .build()
        .unwrap();
    let a = StructArray::from(a_data);

    assert_eq!(a.null_count(), 3);
    assert_eq!(a.column(0).null_count(), 0);

    // build a record batch
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();

    roundtrip(batch, Some(SMALL_SIZE / 2));
}

/// Test round-trip of Dictionary<UInt32, Utf8View> and
/// Dictionary<UInt32, BinaryView> typed columns.
#[test]
fn arrow_writer_string_view_dictionary() {
    let raw_string_values = vec!["a", "b", "large payload over 12 bytes"];
    let raw_binary_values = vec![
        b"a".to_vec(),
        b"b".to_vec(),
        b"large payload over 12 bytes".to_vec(),
    ];

    let keys = UInt32Array::from(vec![Some(0), None, Some(2), Some(1), None]);

    let string_view_values = Arc::new(StringViewArray::from(raw_string_values));
    let string_dict: ArrayRef =
        Arc::new(DictionaryArray::<UInt32Type>::try_new(keys.clone(), string_view_values).unwrap());

    let binary_view_values = Arc::new(BinaryViewArray::from_iter_values(raw_binary_values));
    let binary_dict: ArrayRef =
        Arc::new(DictionaryArray::<UInt32Type>::try_new(keys, binary_view_values).unwrap());

    RoundTripTest::new(string_dict).run();
    RoundTripTest::new(binary_dict).run();
}

#[test]
fn test_fixed_size_binary_in_dict() {
    fn test_fixed_size_binary_in_dict_inner<K>()
    where
        K: ArrowDictionaryKeyType,
        K::Native: FromPrimitive + ToPrimitive + TryFrom<u8>,
        <<K as arrow_array::ArrowPrimitiveType>::Native as TryFrom<u8>>::Error: std::fmt::Debug,
    {
        let field = Field::new(
            "a",
            ArrowDataType::Dictionary(
                Box::new(K::DATA_TYPE),
                Box::new(ArrowDataType::FixedSizeBinary(4)),
            ),
            false,
        );
        let schema = Schema::new(vec![field]);

        let keys: Vec<K::Native> = vec![
            K::Native::try_from(0u8).unwrap(),
            K::Native::try_from(0u8).unwrap(),
            K::Native::try_from(1u8).unwrap(),
        ];
        let keys = PrimitiveArray::<K>::from_iter_values(keys);
        let values = FixedSizeBinaryArray::try_from_iter(
            vec![vec![0, 0, 0, 0], vec![1, 1, 1, 1]].into_iter(),
        )
        .unwrap();

        let data = DictionaryArray::<K>::new(keys, Arc::new(values));
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(data)]).unwrap();
        roundtrip(batch, None);
    }

    test_fixed_size_binary_in_dict_inner::<UInt8Type>();
    test_fixed_size_binary_in_dict_inner::<UInt16Type>();
    test_fixed_size_binary_in_dict_inner::<UInt32Type>();
    test_fixed_size_binary_in_dict_inner::<UInt16Type>();
    test_fixed_size_binary_in_dict_inner::<Int8Type>();
    test_fixed_size_binary_in_dict_inner::<Int16Type>();
    test_fixed_size_binary_in_dict_inner::<Int32Type>();
    test_fixed_size_binary_in_dict_inner::<Int64Type>();
}

#[test]
fn test_empty_dict() {
    let struct_fields = Fields::from(vec![Field::new(
        "dict",
        ArrowDataType::Dictionary(
            Box::new(ArrowDataType::Int32),
            Box::new(ArrowDataType::Utf8),
        ),
        false,
    )]);

    let schema = Schema::new(vec![Field::new_struct(
        "struct",
        struct_fields.clone(),
        true,
    )]);
    let dictionary = Arc::new(DictionaryArray::new(
        Int32Array::new_null(5),
        Arc::new(StringArray::new_null(0)),
    ));

    let s = StructArray::new(
        struct_fields,
        vec![dictionary],
        Some(NullBuffer::new_null(5)),
    );

    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(s)]).unwrap();
    roundtrip(batch, None);
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn arrow_writer_string_dictionary() {
    // define schema
    #[expect(deprecated)]
    let schema = Arc::new(Schema::new(vec![Field::new_dict(
        "dictionary",
        ArrowDataType::Dictionary(
            Box::new(ArrowDataType::Int32),
            Box::new(ArrowDataType::Utf8),
        ),
        true,
        42,
        true,
    )]));

    // create some data
    let d: Int32DictionaryArray = [Some("alpha"), None, Some("beta"), Some("alpha")]
        .iter()
        .copied()
        .collect();

    // build a record batch
    RoundTripTest::new(Arc::new(d)).with_schema(schema).run();
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn arrow_writer_primitive_dictionary() {
    // define schema
    #[expect(deprecated)]
    let schema = Arc::new(Schema::new(vec![Field::new_dict(
        "dictionary",
        ArrowDataType::Dictionary(
            Box::new(ArrowDataType::UInt8),
            Box::new(ArrowDataType::UInt32),
        ),
        true,
        42,
        true,
    )]));

    // create some data
    let mut builder = PrimitiveDictionaryBuilder::<UInt8Type, UInt32Type>::new();
    builder.append(12345678).unwrap();
    builder.append_null();
    builder.append(22345678).unwrap();
    builder.append(12345678).unwrap();
    let d = builder.finish();

    RoundTripTest::new(Arc::new(d)).with_schema(schema).run();
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn arrow_writer_decimal32_dictionary() {
    let integers = vec![12345, 56789, 34567];

    let keys = UInt8Array::from(vec![Some(0), None, Some(1), Some(2), Some(1)]);

    let values = Decimal32Array::from(integers.clone())
        .with_precision_and_scale(5, 2)
        .unwrap();

    let array = DictionaryArray::new(keys, Arc::new(values));
    RoundTripTest::new(Arc::new(array.clone())).run();

    let values = Decimal32Array::from(integers)
        .with_precision_and_scale(9, 2)
        .unwrap();

    let array = array.with_values(Arc::new(values));
    RoundTripTest::new(Arc::new(array)).run();
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn arrow_writer_decimal64_dictionary() {
    let integers = vec![12345, 56789, 34567];

    let keys = UInt8Array::from(vec![Some(0), None, Some(1), Some(2), Some(1)]);

    let values = Decimal64Array::from(integers.clone())
        .with_precision_and_scale(5, 2)
        .unwrap();

    let array = DictionaryArray::new(keys, Arc::new(values));
    RoundTripTest::new(Arc::new(array.clone())).run();

    let values = Decimal64Array::from(integers)
        .with_precision_and_scale(12, 2)
        .unwrap();

    let array = array.with_values(Arc::new(values));
    RoundTripTest::new(Arc::new(array)).run();
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn arrow_writer_decimal128_dictionary() {
    let integers = vec![12345, 56789, 34567];

    let keys = UInt8Array::from(vec![Some(0), None, Some(1), Some(2), Some(1)]);

    let values = Decimal128Array::from(integers.clone())
        .with_precision_and_scale(5, 2)
        .unwrap();

    let array = DictionaryArray::new(keys, Arc::new(values));
    RoundTripTest::new(Arc::new(array.clone())).run();

    let values = Decimal128Array::from(integers)
        .with_precision_and_scale(12, 2)
        .unwrap();

    let array = array.with_values(Arc::new(values));
    RoundTripTest::new(Arc::new(array)).run();
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn arrow_writer_decimal256_dictionary() {
    let integers = vec![
        i256::from_i128(12345),
        i256::from_i128(56789),
        i256::from_i128(34567),
    ];

    let keys = UInt8Array::from(vec![Some(0), None, Some(1), Some(2), Some(1)]);

    let values = Decimal256Array::from(integers.clone())
        .with_precision_and_scale(5, 2)
        .unwrap();

    let array = DictionaryArray::new(keys, Arc::new(values));
    RoundTripTest::new(Arc::new(array.clone())).run();

    let values = Decimal256Array::from(integers)
        .with_precision_and_scale(12, 2)
        .unwrap();

    let array = array.with_values(Arc::new(values));
    RoundTripTest::new(Arc::new(array)).run();
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn arrow_writer_string_dictionary_unsigned_index() {
    // define schema
    #[expect(deprecated)]
    let schema = Arc::new(Schema::new(vec![Field::new_dict(
        "dictionary",
        ArrowDataType::Dictionary(
            Box::new(ArrowDataType::UInt8),
            Box::new(ArrowDataType::Utf8),
        ),
        true,
        42,
        true,
    )]));

    // create some data
    let d: UInt8DictionaryArray = [Some("alpha"), None, Some("beta"), Some("alpha")]
        .iter()
        .copied()
        .collect();

    RoundTripTest::new(Arc::new(d)).with_schema(schema).run();
}

#[test]
fn test_unsigned_roundtrip() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("uint32", ArrowDataType::UInt32, true),
        Field::new("uint64", ArrowDataType::UInt64, true),
    ]));

    let mut buf = Vec::with_capacity(1024);
    let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), None).unwrap();

    let original = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt32Array::from_iter_values([
                0,
                i32::MAX as u32,
                u32::MAX,
            ])),
            Arc::new(UInt64Array::from_iter_values([
                0,
                i64::MAX as u64,
                u64::MAX,
            ])),
        ],
    )
    .unwrap();

    writer.write(&original).unwrap();
    writer.close().unwrap();

    let mut reader = ParquetRecordBatchReader::try_new(Bytes::from(buf), 1024).unwrap();
    let ret = reader.next().unwrap().unwrap();
    assert_eq!(ret, original);

    // Check they can be downcast to the correct type
    ret.column(0)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap();

    ret.column(1)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
}

#[test]
fn test_float16_roundtrip() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("float16", ArrowDataType::Float16, false),
        Field::new("float16-nullable", ArrowDataType::Float16, true),
    ]));

    let mut buf = Vec::with_capacity(1024);
    let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), None)?;

    let original = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Float16Array::from_iter_values([
                f16::EPSILON,
                f16::MIN,
                f16::MAX,
                f16::NAN,
                f16::INFINITY,
                f16::NEG_INFINITY,
                f16::ONE,
                f16::NEG_ONE,
                f16::ZERO,
                f16::NEG_ZERO,
                f16::E,
                f16::PI,
                f16::FRAC_1_PI,
            ])),
            Arc::new(Float16Array::from(vec![
                None,
                None,
                None,
                Some(f16::NAN),
                Some(f16::INFINITY),
                Some(f16::NEG_INFINITY),
                None,
                None,
                None,
                None,
                None,
                None,
                Some(f16::FRAC_1_PI),
            ])),
        ],
    )?;

    writer.write(&original)?;
    writer.close()?;

    let mut reader = ParquetRecordBatchReader::try_new(Bytes::from(buf), 1024)?;
    let ret = reader.next().unwrap()?;
    assert_eq!(ret, original);

    // Ensure can be downcast to the correct type
    ret.column(0).as_primitive::<Float16Type>();
    ret.column(1).as_primitive::<Float16Type>();

    Ok(())
}

#[test]
fn test_time_utc_roundtrip() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "time_millis",
            ArrowDataType::Time32(TimeUnit::Millisecond),
            true,
        )
        .with_metadata(HashMap::from_iter(vec![(
            "adjusted_to_utc".to_string(),
            String::new(),
        )])),
        Field::new(
            "time_micros",
            ArrowDataType::Time64(TimeUnit::Microsecond),
            true,
        )
        .with_metadata(HashMap::from_iter(vec![(
            "adjusted_to_utc".to_string(),
            String::new(),
        )])),
    ]));

    let mut buf = Vec::with_capacity(1024);
    let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), None)?;

    let original = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Time32MillisecondArray::from(vec![
                Some(-1),
                Some(0),
                Some(86_399_000),
                Some(86_400_000),
                Some(86_401_000),
                None,
            ])),
            Arc::new(Time64MicrosecondArray::from(vec![
                Some(-1),
                Some(0),
                Some(86_399 * 1_000_000),
                Some(86_400 * 1_000_000),
                Some(86_401 * 1_000_000),
                None,
            ])),
        ],
    )?;

    writer.write(&original)?;
    writer.close()?;

    let mut reader = ParquetRecordBatchReader::try_new(Bytes::from(buf), 1024)?;
    let ret = reader.next().unwrap()?;
    assert_eq!(ret, original);

    // Ensure can be downcast to the correct type
    ret.column(0).as_primitive::<Time32MillisecondType>();
    ret.column(1).as_primitive::<Time64MicrosecondType>();

    Ok(())
}

#[test]
fn test_date32_roundtrip() -> Result<()> {
    use arrow_array::Date32Array;

    let schema = Arc::new(Schema::new(vec![Field::new(
        "date32",
        ArrowDataType::Date32,
        false,
    )]));

    let mut buf = Vec::with_capacity(1024);

    let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), None)?;

    let original = RecordBatch::try_new(
        schema,
        vec![Arc::new(Date32Array::from(vec![
            -1_000_000, -100_000, -10_000, -1_000, 0, 1_000, 10_000, 100_000, 1_000_000,
        ]))],
    )?;

    writer.write(&original)?;
    writer.close()?;

    let mut reader = ParquetRecordBatchReader::try_new(Bytes::from(buf), 1024)?;
    let ret = reader.next().unwrap()?;
    assert_eq!(ret, original);

    // Ensure can be downcast to the correct type
    ret.column(0).as_primitive::<Date32Type>();

    Ok(())
}

#[test]
fn test_date64_roundtrip() -> Result<()> {
    use arrow_array::Date64Array;

    let schema = Arc::new(Schema::new(vec![
        Field::new("small-date64", ArrowDataType::Date64, false),
        Field::new("big-date64", ArrowDataType::Date64, false),
        Field::new("invalid-date64", ArrowDataType::Date64, false),
    ]));

    let mut default_buf = Vec::with_capacity(1024);
    let mut coerce_buf = Vec::with_capacity(1024);

    let coerce_props = WriterProperties::builder().set_coerce_types(true).build();

    let mut default_writer = ArrowWriter::try_new(&mut default_buf, schema.clone(), None)?;
    let mut coerce_writer =
        ArrowWriter::try_new(&mut coerce_buf, schema.clone(), Some(coerce_props))?;

    static NUM_MILLISECONDS_IN_DAY: i64 = 1000 * 60 * 60 * 24;

    let original = RecordBatch::try_new(
        schema,
        vec![
            // small-date64
            Arc::new(Date64Array::from(vec![
                -1_000_000 * NUM_MILLISECONDS_IN_DAY,
                -1_000 * NUM_MILLISECONDS_IN_DAY,
                0,
                1_000 * NUM_MILLISECONDS_IN_DAY,
                1_000_000 * NUM_MILLISECONDS_IN_DAY,
            ])),
            // big-date64
            Arc::new(Date64Array::from(vec![
                -10_000_000_000 * NUM_MILLISECONDS_IN_DAY,
                -1_000_000_000 * NUM_MILLISECONDS_IN_DAY,
                0,
                1_000_000_000 * NUM_MILLISECONDS_IN_DAY,
                10_000_000_000 * NUM_MILLISECONDS_IN_DAY,
            ])),
            // invalid-date64
            Arc::new(Date64Array::from(vec![
                -1_000_000 * NUM_MILLISECONDS_IN_DAY + 1,
                -1_000 * NUM_MILLISECONDS_IN_DAY + 1,
                1,
                1_000 * NUM_MILLISECONDS_IN_DAY + 1,
                1_000_000 * NUM_MILLISECONDS_IN_DAY + 1,
            ])),
        ],
    )?;

    default_writer.write(&original)?;
    coerce_writer.write(&original)?;

    default_writer.close()?;
    coerce_writer.close()?;

    let mut default_reader = ParquetRecordBatchReader::try_new(Bytes::from(default_buf), 1024)?;
    let mut coerce_reader = ParquetRecordBatchReader::try_new(Bytes::from(coerce_buf), 1024)?;

    let default_ret = default_reader.next().unwrap()?;
    let coerce_ret = coerce_reader.next().unwrap()?;

    // Roundtrip should be successful when default writer used
    assert_eq!(default_ret, original);

    // Only small-date64 should roundtrip successfully when coerce_types writer is used
    assert_eq!(coerce_ret.column(0), original.column(0));
    assert_ne!(coerce_ret.column(1), original.column(1));
    assert_ne!(coerce_ret.column(2), original.column(2));

    // Ensure both can be downcast to the correct type
    default_ret.column(0).as_primitive::<Date64Type>();
    coerce_ret.column(0).as_primitive::<Date64Type>();

    Ok(())
}

#[test]
fn test_decimal_nullable_struct() {
    let decimals = Decimal256Array::from_iter_values(
        [1, 2, 3, 4, 5, 6, 7, 8].into_iter().map(i256::from_i128),
    );

    let data = ArrayDataBuilder::new(ArrowDataType::Struct(Fields::from(vec![Field::new(
        "decimals",
        decimals.data_type().clone(),
        false,
    )])))
    .len(8)
    .null_bit_buffer(Some(Buffer::from(&[0b11101111])))
    .child_data(vec![decimals.into_data()])
    .build()
    .unwrap();

    let written =
        RecordBatch::try_from_iter([("struct", Arc::new(StructArray::from(data)) as ArrayRef)])
            .unwrap();

    let mut buffer = Vec::with_capacity(1024);
    let mut writer = ArrowWriter::try_new(&mut buffer, written.schema(), None).unwrap();
    writer.write(&written).unwrap();
    writer.close().unwrap();

    let read = ParquetRecordBatchReader::try_new(Bytes::from(buffer), 3)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(&written.slice(0, 3), &read[0]);
    assert_eq!(&written.slice(3, 3), &read[1]);
    assert_eq!(&written.slice(6, 2), &read[2]);
}

#[test]
fn test_int32_nullable_struct() {
    let int32 = Int32Array::from_iter_values([1, 2, 3, 4, 5, 6, 7, 8]);
    let data = ArrayDataBuilder::new(ArrowDataType::Struct(Fields::from(vec![Field::new(
        "int32",
        int32.data_type().clone(),
        false,
    )])))
    .len(8)
    .null_bit_buffer(Some(Buffer::from(&[0b11101111])))
    .child_data(vec![int32.into_data()])
    .build()
    .unwrap();

    let written =
        RecordBatch::try_from_iter([("struct", Arc::new(StructArray::from(data)) as ArrayRef)])
            .unwrap();

    let mut buffer = Vec::with_capacity(1024);
    let mut writer = ArrowWriter::try_new(&mut buffer, written.schema(), None).unwrap();
    writer.write(&written).unwrap();
    writer.close().unwrap();

    let read = ParquetRecordBatchReader::try_new(Bytes::from(buffer), 3)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(&written.slice(0, 3), &read[0]);
    assert_eq!(&written.slice(3, 3), &read[1]);
    assert_eq!(&written.slice(6, 2), &read[2]);
}

#[test]
fn test_decimal_list() {
    let decimals = Decimal128Array::from_iter_values([1, 2, 3, 4, 5, 6, 7, 8]);

    // [[], [1], [2, 3], null, [4], null, [6, 7, 8]]
    let data = ArrayDataBuilder::new(ArrowDataType::List(Arc::new(Field::new_list_field(
        decimals.data_type().clone(),
        false,
    ))))
    .len(7)
    .add_buffer(Buffer::from_iter([0_i32, 0, 1, 3, 3, 4, 5, 8]))
    .null_bit_buffer(Some(Buffer::from(&[0b01010111])))
    .child_data(vec![decimals.into_data()])
    .build()
    .unwrap();

    let written =
        RecordBatch::try_from_iter([("list", Arc::new(ListArray::from(data)) as ArrayRef)])
            .unwrap();

    let mut buffer = Vec::with_capacity(1024);
    let mut writer = ArrowWriter::try_new(&mut buffer, written.schema(), None).unwrap();
    writer.write(&written).unwrap();
    writer.close().unwrap();

    let read = ParquetRecordBatchReader::try_new(Bytes::from(buffer), 3)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(&written.slice(0, 3), &read[0]);
    assert_eq!(&written.slice(3, 3), &read[1]);
    assert_eq!(&written.slice(6, 1), &read[2]);
}

#[test]
fn test_read_dict_fixed_size_binary() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "a",
        ArrowDataType::Dictionary(
            Box::new(ArrowDataType::UInt8),
            Box::new(ArrowDataType::FixedSizeBinary(8)),
        ),
        true,
    )]));
    let keys = UInt8Array::from_iter_values(vec![0, 0, 1]);
    let values = FixedSizeBinaryArray::try_from_iter(
        vec![
            (0u8..8u8).collect::<Vec<u8>>(),
            (24u8..32u8).collect::<Vec<u8>>(),
        ]
        .into_iter(),
    )
    .unwrap();
    let arr = UInt8DictionaryArray::new(keys, Arc::new(values));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

    let mut buffer = Vec::with_capacity(1024);
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let read = ParquetRecordBatchReader::try_new(Bytes::from(buffer), 3)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(read.len(), 1);
    assert_eq!(&batch, &read[0])
}

#[test]
fn test_read_nullable_structs_with_binary_dict_as_first_child_column() {
    // the `StructArrayReader` will check the definition and repetition levels of the first
    // child column in the struct to determine nullability for the struct. If the first
    // column's is being read by `ByteArrayDictionaryReader` we need to ensure that the
    // nullability is interpreted  correctly from the rep/def level buffers managed by the
    // buffers managed by this array reader.

    let struct_fields = Fields::from(vec![
        Field::new(
            "city",
            ArrowDataType::Dictionary(
                Box::new(ArrowDataType::UInt8),
                Box::new(ArrowDataType::Utf8),
            ),
            true,
        ),
        Field::new("name", ArrowDataType::Utf8, true),
    ]);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "items",
        ArrowDataType::Struct(struct_fields.clone()),
        true,
    )]));

    let items_arr = StructArray::new(
        struct_fields,
        vec![
            Arc::new(DictionaryArray::new(
                UInt8Array::from_iter_values(vec![0, 1, 1, 0, 2]),
                Arc::new(StringArray::from_iter_values(vec![
                    "quebec",
                    "fredericton",
                    "halifax",
                ])),
            )),
            Arc::new(StringArray::from_iter_values(vec![
                "albert", "terry", "lance", "", "tim",
            ])),
        ],
        Some(NullBuffer::from_iter(vec![true, true, true, false, true])),
    );

    let batch = RecordBatch::try_new(schema, vec![Arc::new(items_arr)]).unwrap();
    let mut buffer = Vec::with_capacity(1024);
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let read = ParquetRecordBatchReader::try_new(Bytes::from(buffer), 8)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(read.len(), 1);
    assert_eq!(&batch, &read[0])
}

#[test]
fn test_arbitrary_decimal() {
    let values = [1, 2, 3, 4, 5, 6, 7, 8];
    let decimals_19_0 = Decimal128Array::from_iter_values(values)
        .with_precision_and_scale(19, 0)
        .unwrap();
    let decimals_12_0 = Decimal128Array::from_iter_values(values)
        .with_precision_and_scale(12, 0)
        .unwrap();
    let decimals_17_10 = Decimal128Array::from_iter_values(values)
        .with_precision_and_scale(17, 10)
        .unwrap();

    let written = RecordBatch::try_from_iter([
        ("decimal_values_19_0", Arc::new(decimals_19_0) as ArrayRef),
        ("decimal_values_12_0", Arc::new(decimals_12_0) as ArrayRef),
        ("decimal_values_17_10", Arc::new(decimals_17_10) as ArrayRef),
    ])
    .unwrap();

    let mut buffer = Vec::with_capacity(1024);
    let mut writer = ArrowWriter::try_new(&mut buffer, written.schema(), None).unwrap();
    writer.write(&written).unwrap();
    writer.close().unwrap();

    let read = ParquetRecordBatchReader::try_new(Bytes::from(buffer), 8)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(&written.slice(0, 8), &read[0]);
}

fn test_decimal32_roundtrip() {
    let d = |values: Vec<i32>, p: u8| {
        let iter = values.into_iter();
        PrimitiveArray::<Decimal32Type>::from_iter_values(iter)
            .with_precision_and_scale(p, 2)
            .unwrap()
    };

    let d1 = d(vec![1, 2, 3, 4, 5], 9);
    let batch = RecordBatch::try_from_iter([("d1", Arc::new(d1) as ArrayRef)]).unwrap();

    let mut buffer = Vec::with_capacity(1024);
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(buffer)).unwrap();
    let t1 = builder.parquet_schema().columns()[0].physical_type();
    assert_eq!(t1, PhysicalType::INT32);

    let mut reader = builder.build().unwrap();
    assert_eq!(batch.schema(), reader.schema());

    let out = reader.next().unwrap().unwrap();
    assert_eq!(batch, out);
}

fn test_decimal64_roundtrip() {
    // Precision <= 9 -> INT32
    // Precision <= 18 -> INT64

    let d = |values: Vec<i64>, p: u8| {
        let iter = values.into_iter();
        PrimitiveArray::<Decimal64Type>::from_iter_values(iter)
            .with_precision_and_scale(p, 2)
            .unwrap()
    };

    let d1 = d(vec![1, 2, 3, 4, 5], 9);
    let d2 = d(vec![1, 2, 3, 4, 10.pow(10) - 1], 10);
    let d3 = d(vec![1, 2, 3, 4, 10.pow(18) - 1], 18);

    let batch = RecordBatch::try_from_iter([
        ("d1", Arc::new(d1) as ArrayRef),
        ("d2", Arc::new(d2) as ArrayRef),
        ("d3", Arc::new(d3) as ArrayRef),
    ])
    .unwrap();

    let mut buffer = Vec::with_capacity(1024);
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(buffer)).unwrap();
    let t1 = builder.parquet_schema().columns()[0].physical_type();
    assert_eq!(t1, PhysicalType::INT32);
    let t2 = builder.parquet_schema().columns()[1].physical_type();
    assert_eq!(t2, PhysicalType::INT64);
    let t3 = builder.parquet_schema().columns()[2].physical_type();
    assert_eq!(t3, PhysicalType::INT64);

    let mut reader = builder.build().unwrap();
    assert_eq!(batch.schema(), reader.schema());

    let out = reader.next().unwrap().unwrap();
    assert_eq!(batch, out);
}

fn test_decimal_roundtrip<T: DecimalType>() {
    // Precision <= 9 -> INT32
    // Precision <= 18 -> INT64
    // Precision > 18 -> FIXED_LEN_BYTE_ARRAY

    let d = |values: Vec<usize>, p: u8| {
        let iter = values.into_iter().map(T::Native::usize_as);
        PrimitiveArray::<T>::from_iter_values(iter)
            .with_precision_and_scale(p, 2)
            .unwrap()
    };

    let d1 = d(vec![1, 2, 3, 4, 5], 9);
    let d2 = d(vec![1, 2, 3, 4, 10.pow(10) - 1], 10);
    let d3 = d(vec![1, 2, 3, 4, 10.pow(18) - 1], 18);
    let d4 = d(vec![1, 2, 3, 4, 10.pow(19) - 1], 19);

    let batch = RecordBatch::try_from_iter([
        ("d1", Arc::new(d1) as ArrayRef),
        ("d2", Arc::new(d2) as ArrayRef),
        ("d3", Arc::new(d3) as ArrayRef),
        ("d4", Arc::new(d4) as ArrayRef),
    ])
    .unwrap();

    let mut buffer = Vec::with_capacity(1024);
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(buffer)).unwrap();
    let t1 = builder.parquet_schema().columns()[0].physical_type();
    assert_eq!(t1, PhysicalType::INT32);
    let t2 = builder.parquet_schema().columns()[1].physical_type();
    assert_eq!(t2, PhysicalType::INT64);
    let t3 = builder.parquet_schema().columns()[2].physical_type();
    assert_eq!(t3, PhysicalType::INT64);
    let t4 = builder.parquet_schema().columns()[3].physical_type();
    assert_eq!(t4, PhysicalType::FIXED_LEN_BYTE_ARRAY);

    let mut reader = builder.build().unwrap();
    assert_eq!(batch.schema(), reader.schema());

    let out = reader.next().unwrap().unwrap();
    assert_eq!(batch, out);
}

#[test]
fn test_decimal() {
    test_decimal32_roundtrip();
    test_decimal64_roundtrip();
    test_decimal_roundtrip::<Decimal128Type>();
    test_decimal_roundtrip::<Decimal256Type>();
}
