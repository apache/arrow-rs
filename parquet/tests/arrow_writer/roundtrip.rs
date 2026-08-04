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

//! "Round trip" tests for [`ArrowWriter`]: write data to parquet, read it back
//! and verify the exact same values are returned.
//!
//! These tests were moved from `parquet/src/arrow/arrow_writer/mod.rs`.
//! See <https://github.com/apache/arrow-rs/issues/10540>

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::ToByteSlice;
use arrow::datatypes::{
    ArrowDictionaryKeyType, DataType, DataType as ArrowDataType, Field, Int8Type, Int16Type,
    Int32Type, Int64Type, Schema, SchemaRef, UInt8Type, UInt16Type, UInt32Type,
};
use arrow::error::Result as ArrowResult;
use arrow::{array::*, buffer::Buffer};
use arrow_buffer::{IntervalDayTime, IntervalMonthDayNano, NullBuffer, OffsetBuffer, i256};
use arrow_schema::Fields;
use bytes::Bytes;
use half::f16;
use num_traits::{FromPrimitive, ToPrimitive};
use tempfile::tempfile;

use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::arrow::arrow_writer::ArrowWriterOptions;
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use parquet::basic::Encoding;
use parquet::data_type::AsBytes;
use parquet::file::properties::{
    BloomFilterPosition, ReaderProperties, WriterProperties, WriterVersion,
};
use parquet::file::serialized_reader::ReadOptionsBuilder;
use parquet::file::{
    reader::{FileReader, SerializedFileReader},
    statistics::Statistics,
};

/// A dictionary-encoded column written through the deferred-ordering Arrow
/// path must round-trip correctly even with the offset index disabled, when
/// only the chunk-level dictionary/data page offsets are rewritten (there is
/// no offset index to rebuild). Spans multiple data pages so the
/// dictionary-first reordering is exercised.
#[test]
fn dictionary_column_round_trips_with_offset_index_disabled() {
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, true)]));

    // Low cardinality so the column stays dictionary-encoded; enough rows to
    // span several data pages within a single row group.
    let values: Vec<Option<i32>> = (0..50_000).map(|i| Some(i % 8)).collect();
    let array = Int32Array::from(values.clone());
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();

    let props = WriterProperties::builder()
        .set_offset_index_disabled(true)
        .set_data_page_row_count_limit(4096)
        .build();
    let opts = ArrowWriterOptions::new().with_properties(props);

    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new_with_options(&mut buffer, schema.clone(), opts).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let reader = ParquetRecordBatchReader::try_new(Bytes::from(buffer), values.len()).unwrap();
    let read: Vec<RecordBatch> = reader.collect::<ArrowResult<_>>().unwrap();
    let read_values: Vec<Option<i32>> = read
        .iter()
        .flat_map(|b| b.column(0).as_primitive::<Int32Type>().iter())
        .collect();
    assert_eq!(read_values, values);
}

#[test]
fn arrow_writer() {
    // define schema
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, true),
    ]);

    // create some data
    let a = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let b = Int32Array::from(vec![Some(1), None, None, Some(4), Some(5)]);

    // build a record batch
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a), Arc::new(b)]).unwrap();

    roundtrip(batch, Some(SMALL_SIZE / 2));
}

fn get_bytes_after_close(schema: SchemaRef, expected_batch: &RecordBatch) -> Vec<u8> {
    let mut buffer = vec![];

    let mut writer = ArrowWriter::try_new(&mut buffer, schema, None).unwrap();
    writer.write(expected_batch).unwrap();
    writer.close().unwrap();

    buffer
}

fn get_bytes_by_into_inner(schema: SchemaRef, expected_batch: &RecordBatch) -> Vec<u8> {
    let mut writer = ArrowWriter::try_new(Vec::new(), schema, None).unwrap();
    writer.write(expected_batch).unwrap();
    writer.into_inner().unwrap()
}

#[test]
fn roundtrip_bytes() {
    // define schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, true),
    ]));

    // create some data
    let a = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let b = Int32Array::from(vec![Some(1), None, None, Some(4), Some(5)]);

    // build a record batch
    let expected_batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(a), Arc::new(b)]).unwrap();

    for buffer in [
        get_bytes_after_close(schema.clone(), &expected_batch),
        get_bytes_by_into_inner(schema, &expected_batch),
    ] {
        let cursor = Bytes::from(buffer);
        let mut record_batch_reader = ParquetRecordBatchReader::try_new(cursor, 1024).unwrap();

        let actual_batch = record_batch_reader
            .next()
            .expect("No batch found")
            .expect("Unable to get batch");

        assert_eq!(expected_batch.schema(), actual_batch.schema());
        assert_eq!(expected_batch.num_columns(), actual_batch.num_columns());
        assert_eq!(expected_batch.num_rows(), actual_batch.num_rows());
        for i in 0..expected_batch.num_columns() {
            let expected_data = expected_batch.column(i).to_data();
            let actual_data = actual_batch.column(i).to_data();

            assert_eq!(expected_data, actual_data);
        }
    }
}

#[test]
fn arrow_writer_non_null() {
    // define schema
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

    // create some data
    let a = Int32Array::from(vec![1, 2, 3, 4, 5]);

    // build a record batch
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();

    roundtrip(batch, Some(SMALL_SIZE / 2));
}

#[test]
fn arrow_writer_list() {
    // define schema
    let schema = Schema::new(vec![Field::new(
        "a",
        DataType::List(Arc::new(Field::new_list_field(DataType::Int32, false))),
        true,
    )]);

    // create some data
    let a_values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

    // Construct a buffer for value offsets, for the nested array:
    //  [[1], [2, 3], null, [4, 5, 6], [7, 8, 9, 10]]
    let a_value_offsets = arrow::buffer::Buffer::from([0, 1, 3, 3, 6, 10].to_byte_slice());

    // Construct a list array from the above two
    let a_list_data = ArrayData::builder(DataType::List(Arc::new(Field::new_list_field(
        DataType::Int32,
        false,
    ))))
    .len(5)
    .add_buffer(a_value_offsets)
    .add_child_data(a_values.into_data())
    .null_bit_buffer(Some(Buffer::from([0b00011011])))
    .build()
    .unwrap();
    let a = ListArray::from(a_list_data);

    // build a record batch
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();

    assert_eq!(batch.column(0).null_count(), 1);

    // This test fails if the max row group size is less than the batch's length
    // see https://github.com/apache/arrow-rs/issues/518
    roundtrip(batch, None);
}

#[test]
fn arrow_writer_list_non_null() {
    // define schema
    let schema = Schema::new(vec![Field::new(
        "a",
        DataType::List(Arc::new(Field::new_list_field(DataType::Int32, false))),
        false,
    )]);

    // create some data
    let a_values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

    // Construct a buffer for value offsets, for the nested array:
    //  [[1], [2, 3], [], [4, 5, 6], [7, 8, 9, 10]]
    let a_value_offsets = arrow::buffer::Buffer::from([0, 1, 3, 3, 6, 10].to_byte_slice());

    // Construct a list array from the above two
    let a_list_data = ArrayData::builder(DataType::List(Arc::new(Field::new_list_field(
        DataType::Int32,
        false,
    ))))
    .len(5)
    .add_buffer(a_value_offsets)
    .add_child_data(a_values.into_data())
    .build()
    .unwrap();
    let a = ListArray::from(a_list_data);

    // build a record batch
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();

    // This test fails if the max row group size is less than the batch's length
    // see https://github.com/apache/arrow-rs/issues/518
    assert_eq!(batch.column(0).null_count(), 0);

    roundtrip(batch, None);
}

#[test]
fn arrow_writer_list_view() {
    let list_field = Arc::new(Field::new_list_field(DataType::Int32, false));
    let schema = Schema::new(vec![Field::new(
        "a",
        DataType::ListView(list_field.clone()),
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

    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();

    assert_eq!(batch.column(0).null_count(), 1);

    roundtrip(batch, None);
}

#[test]
fn arrow_writer_list_view_non_null() {
    let list_field = Arc::new(Field::new_list_field(DataType::Int32, false));
    let schema = Schema::new(vec![Field::new(
        "a",
        DataType::ListView(list_field.clone()),
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

    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();

    assert_eq!(batch.column(0).null_count(), 0);

    roundtrip(batch, None);
}

#[test]
fn arrow_writer_list_view_out_of_order() {
    let list_field = Arc::new(Field::new_list_field(DataType::Int32, false));
    let schema = Schema::new(vec![Field::new(
        "a",
        DataType::ListView(list_field.clone()),
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

    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();

    roundtrip(batch, None);
}

#[test]
fn arrow_writer_large_list_view() {
    let list_field = Arc::new(Field::new_list_field(DataType::Int32, false));
    let schema = Schema::new(vec![Field::new(
        "a",
        DataType::LargeListView(list_field.clone()),
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

    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();

    assert_eq!(batch.column(0).null_count(), 1);

    roundtrip(batch, None);
}

#[test]
fn arrow_writer_list_view_with_struct() {
    // Test ListView containing Struct: ListView<Struct<Int32, Utf8>>
    let struct_fields = Fields::from(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]);
    let struct_type = DataType::Struct(struct_fields.clone());
    let list_field = Arc::new(Field::new("item", struct_type.clone(), false));

    let schema = Schema::new(vec![Field::new(
        "a",
        DataType::ListView(list_field.clone()),
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

    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(list_view)]).unwrap();

    roundtrip(batch, None);
}

#[test]
fn arrow_writer_binary() {
    let string_field = Field::new("a", DataType::Utf8, false);
    let binary_field = Field::new("b", DataType::Binary, false);
    let schema = Schema::new(vec![string_field, binary_field]);

    let raw_string_values = vec!["foo", "bar", "baz", "quux"];
    let raw_binary_values = [
        b"foo".to_vec(),
        b"bar".to_vec(),
        b"baz".to_vec(),
        b"quux".to_vec(),
    ];
    let raw_binary_value_refs = raw_binary_values
        .iter()
        .map(|x| x.as_slice())
        .collect::<Vec<_>>();

    let string_values = StringArray::from(raw_string_values.clone());
    let binary_values = BinaryArray::from(raw_binary_value_refs);
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(string_values), Arc::new(binary_values)],
    )
    .unwrap();

    roundtrip(batch, Some(SMALL_SIZE / 2));
}

#[test]
fn arrow_writer_binary_view() {
    let string_field = Field::new("a", DataType::Utf8View, false);
    let binary_field = Field::new("b", DataType::BinaryView, false);
    let nullable_string_field = Field::new("a", DataType::Utf8View, true);
    let schema = Schema::new(vec![string_field, binary_field, nullable_string_field]);

    let raw_string_values = vec!["foo", "bar", "large payload over 12 bytes", "lulu"];
    let raw_binary_values = vec![
        b"foo".to_vec(),
        b"bar".to_vec(),
        b"large payload over 12 bytes".to_vec(),
        b"lulu".to_vec(),
    ];
    let nullable_string_values = vec![Some("foo"), None, Some("large payload over 12 bytes"), None];

    let string_view_values = StringViewArray::from(raw_string_values);
    let binary_view_values = BinaryViewArray::from_iter_values(raw_binary_values);
    let nullable_string_view_values = StringViewArray::from(nullable_string_values);
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(string_view_values),
            Arc::new(binary_view_values),
            Arc::new(nullable_string_view_values),
        ],
    )
    .unwrap();

    roundtrip(batch.clone(), Some(SMALL_SIZE / 2));
    roundtrip(batch, None);
}

#[test]
fn arrow_writer_binary_view_long_value() {
    let string_field = Field::new("a", DataType::Utf8View, false);
    let binary_field = Field::new("b", DataType::BinaryView, false);
    let schema = Schema::new(vec![string_field, binary_field]);

    // There is special case validation for long values (greater than 128)
    // 128 encodes as 0x80 0x00 0x00 0x00 in little endian, which should
    // trigger the long-string UTF-8 validation branch in the plain decoder.
    let long = "a".repeat(128);
    let raw_string_values = vec!["foo", long.as_str(), "bar"];
    let raw_binary_values = vec![b"foo".to_vec(), long.as_bytes().to_vec(), b"bar".to_vec()];

    let string_view_values: ArrayRef = Arc::new(StringViewArray::from(raw_string_values));
    let binary_view_values: ArrayRef =
        Arc::new(BinaryViewArray::from_iter_values(raw_binary_values));

    one_column_roundtrip(Arc::clone(&string_view_values), false);
    one_column_roundtrip(Arc::clone(&binary_view_values), false);

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![string_view_values, binary_view_values],
    )
    .unwrap();

    // Disable dictionary to exercise plain encoding paths in the reader.
    for version in [WriterVersion::PARQUET_1_0, WriterVersion::PARQUET_2_0] {
        let props = WriterProperties::builder()
            .set_writer_version(version)
            .set_dictionary_enabled(false)
            .build();
        roundtrip_opts(&batch, props);
    }
}

fn get_decimal_batch(precision: u8, scale: i8) -> RecordBatch {
    let decimal_field = Field::new("a", DataType::Decimal128(precision, scale), false);
    let schema = Schema::new(vec![decimal_field]);

    let decimal_values = vec![10_000, 50_000, 0, -100]
        .into_iter()
        .map(Some)
        .collect::<Decimal128Array>()
        .with_precision_and_scale(precision, scale)
        .unwrap();

    RecordBatch::try_new(Arc::new(schema), vec![Arc::new(decimal_values)]).unwrap()
}

#[test]
fn arrow_writer_decimal() {
    // int32 to store the decimal value
    let batch_int32_decimal = get_decimal_batch(5, 2);
    roundtrip(batch_int32_decimal, Some(SMALL_SIZE / 2));
    // int64 to store the decimal value
    let batch_int64_decimal = get_decimal_batch(12, 2);
    roundtrip(batch_int64_decimal, Some(SMALL_SIZE / 2));
    // fixed_length_byte_array to store the decimal value
    let batch_fixed_len_byte_array_decimal = get_decimal_batch(30, 2);
    roundtrip(batch_fixed_len_byte_array_decimal, Some(SMALL_SIZE / 2));
}

#[test]
fn arrow_writer_complex() {
    // define schema
    let struct_field_d = Arc::new(Field::new("d", DataType::Float64, true));
    let struct_field_f = Arc::new(Field::new("f", DataType::Float32, true));
    let struct_field_g = Arc::new(Field::new_list(
        "g",
        Field::new_list_field(DataType::Int16, true),
        false,
    ));
    let struct_field_h = Arc::new(Field::new_list(
        "h",
        Field::new_list_field(DataType::Int16, false),
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
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, true),
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
    let offset_field = Arc::new(Field::new("offset", DataType::Int32, false));
    let partition_field = Arc::new(Field::new("partition", DataType::Int64, true));
    let topic_field = Arc::new(Field::new("topic", DataType::Utf8, true));
    let schema = Schema::new(vec![Field::new(
        "some_nested_object",
        DataType::Struct(Fields::from(vec![
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
    let entries_struct_type = DataType::Struct(Fields::from(vec![
        Field::new(Field::MAP_KEY_FIELD_DEFAULT_NAME, DataType::Utf8, false),
        Field::new(Field::MAP_VALUE_FIELD_DEFAULT_NAME, DataType::Utf8, true),
    ]));
    let stocks_field = Field::new(
        "stocks",
        DataType::Map(
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
    let field_c = Field::new("c", DataType::Int32, true);
    let field_b = Field::new("b", DataType::Struct(vec![field_c].into()), true);
    let type_a = DataType::Struct(vec![field_b.clone()].into());
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
    let field_c = Field::new("c", DataType::Int32, false);
    let type_b = DataType::Struct(vec![field_c].into());
    let field_b = Field::new("b", type_b.clone(), false);
    let type_a = DataType::Struct(vec![field_b].into());
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
    let field_c = Field::new("c", DataType::Int32, false);
    let type_b = DataType::Struct(vec![field_c].into());
    let field_b = Field::new("b", type_b.clone(), true);
    let type_a = DataType::Struct(vec![field_b].into());
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
    let field_c = Field::new("c", DataType::Int32, false);
    let field_d = Field::new("d", DataType::FixedSizeBinary(4), false);
    let field_e = Field::new(
        "e",
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        false,
    );

    let field_b = Field::new(
        "b",
        DataType::Struct(vec![field_c, field_d, field_e].into()),
        false,
    );
    let type_a = DataType::Struct(vec![field_b.clone()].into());
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
            DataType::Dictionary(
                Box::new(K::DATA_TYPE),
                Box::new(DataType::FixedSizeBinary(4)),
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
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
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
fn arrow_writer_float_nans() {
    let f16_field = Field::new("a", DataType::Float16, false);
    let f32_field = Field::new("b", DataType::Float32, false);
    let f64_field = Field::new("c", DataType::Float64, false);
    let schema = Schema::new(vec![f16_field, f32_field, f64_field]);

    let f16_values = (0..MEDIUM_SIZE)
        .map(|i| {
            Some(if i % 2 == 0 {
                f16::NAN
            } else {
                f16::from_f32(i as f32)
            })
        })
        .collect::<Float16Array>();

    let f32_values = (0..MEDIUM_SIZE)
        .map(|i| Some(if i % 2 == 0 { f32::NAN } else { i as f32 }))
        .collect::<Float32Array>();

    let f64_values = (0..MEDIUM_SIZE)
        .map(|i| Some(if i % 2 == 0 { f64::NAN } else { i as f64 }))
        .collect::<Float64Array>();

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(f16_values),
            Arc::new(f32_values),
            Arc::new(f64_values),
        ],
    )
    .unwrap();

    roundtrip(batch, None);
}

const SMALL_SIZE: usize = 7;

const MEDIUM_SIZE: usize = 63;

// Write the batch to parquet and read it back out, ensuring
// that what comes out is the same as what was written in
fn roundtrip(expected_batch: RecordBatch, max_row_group_size: Option<usize>) -> Vec<Bytes> {
    let mut files = vec![];
    for version in [WriterVersion::PARQUET_1_0, WriterVersion::PARQUET_2_0] {
        let mut props = WriterProperties::builder().set_writer_version(version);

        if let Some(size) = max_row_group_size {
            props = props.set_max_row_group_row_count(Some(size))
        }

        let props = props.build();
        files.push(roundtrip_opts(&expected_batch, props))
    }
    files
}

// Round trip the specified record batch with the specified writer properties,
// to an in-memory file, and validate the arrays using the specified function.
// Returns the in-memory file.
fn roundtrip_opts_with_array_validation<F>(
    expected_batch: &RecordBatch,
    props: WriterProperties,
    validate: F,
) -> Bytes
where
    F: Fn(&ArrayData, &ArrayData),
{
    let mut file = vec![];

    let mut writer = ArrowWriter::try_new(&mut file, expected_batch.schema(), Some(props))
        .expect("Unable to write file");
    writer.write(expected_batch).unwrap();
    writer.close().unwrap();

    let file = Bytes::from(file);
    let mut record_batch_reader = ParquetRecordBatchReader::try_new(file.clone(), 1024).unwrap();

    let actual_batch = record_batch_reader
        .next()
        .expect("No batch found")
        .expect("Unable to get batch");

    assert_eq!(expected_batch.schema(), actual_batch.schema());
    assert_eq!(expected_batch.num_columns(), actual_batch.num_columns());
    assert_eq!(expected_batch.num_rows(), actual_batch.num_rows());
    for i in 0..expected_batch.num_columns() {
        let expected_data = expected_batch.column(i).to_data();
        let actual_data = actual_batch.column(i).to_data();
        validate(&expected_data, &actual_data);
    }

    file
}

fn roundtrip_opts(expected_batch: &RecordBatch, props: WriterProperties) -> Bytes {
    roundtrip_opts_with_array_validation(expected_batch, props, |a, b| {
        a.validate_full().expect("valid expected data");
        b.validate_full().expect("valid actual data");
        assert_eq!(a, b)
    })
}

struct RoundTripOptions {
    values: ArrayRef,
    schema: SchemaRef,
    bloom_filter: bool,
    bloom_filter_ndv: Option<u64>,
    bloom_filter_position: BloomFilterPosition,
}

impl RoundTripOptions {
    fn new(values: ArrayRef, nullable: bool) -> Self {
        let data_type = values.data_type().clone();
        let schema = Schema::new(vec![Field::new("col", data_type, nullable)]);
        Self {
            values,
            schema: Arc::new(schema),
            bloom_filter: false,
            bloom_filter_ndv: None,
            bloom_filter_position: BloomFilterPosition::AfterRowGroup,
        }
    }
}

fn one_column_roundtrip(values: ArrayRef, nullable: bool) -> Vec<Bytes> {
    one_column_roundtrip_with_options(RoundTripOptions::new(values, nullable))
}

fn one_column_roundtrip_with_schema(values: ArrayRef, schema: SchemaRef) -> Vec<Bytes> {
    let mut options = RoundTripOptions::new(values, false);
    options.schema = schema;
    one_column_roundtrip_with_options(options)
}

fn one_column_roundtrip_with_options(options: RoundTripOptions) -> Vec<Bytes> {
    let RoundTripOptions {
        values,
        schema,
        bloom_filter,
        bloom_filter_ndv,
        bloom_filter_position,
    } = options;

    let encodings = match values.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary => {
            vec![
                Encoding::PLAIN,
                Encoding::DELTA_BYTE_ARRAY,
                Encoding::DELTA_LENGTH_BYTE_ARRAY,
            ]
        }
        DataType::Int64
        | DataType::Int32
        | DataType::Int16
        | DataType::Int8
        | DataType::UInt64
        | DataType::UInt32
        | DataType::UInt16
        | DataType::UInt8 => vec![
            Encoding::PLAIN,
            Encoding::DELTA_BINARY_PACKED,
            Encoding::BYTE_STREAM_SPLIT,
        ],
        DataType::Float32 | DataType::Float64 => {
            vec![Encoding::PLAIN, Encoding::BYTE_STREAM_SPLIT]
        }
        _ => vec![Encoding::PLAIN],
    };

    let expected_batch = RecordBatch::try_new(schema, vec![values]).unwrap();

    let row_group_sizes = [1024, SMALL_SIZE, SMALL_SIZE / 2, SMALL_SIZE / 2 + 1, 10];

    let mut files = vec![];
    for dictionary_size in [0, 1, 1024] {
        for encoding in &encodings {
            for version in [WriterVersion::PARQUET_1_0, WriterVersion::PARQUET_2_0] {
                for row_group_size in row_group_sizes {
                    let mut builder = WriterProperties::builder()
                        .set_writer_version(version)
                        .set_max_row_group_row_count(Some(row_group_size))
                        .set_dictionary_enabled(dictionary_size != 0)
                        .set_dictionary_page_size_limit(dictionary_size.max(1))
                        .set_encoding(*encoding)
                        .set_bloom_filter_enabled(bloom_filter)
                        .set_bloom_filter_position(bloom_filter_position);
                    if let Some(ndv) = bloom_filter_ndv {
                        builder = builder.set_bloom_filter_max_ndv(ndv);
                    }
                    let props = builder.build();

                    files.push(roundtrip_opts(&expected_batch, props))
                }
            }
        }
    }
    files
}

fn values_required<A, I>(iter: I) -> Vec<Bytes>
where
    A: From<Vec<I::Item>> + Array + 'static,
    I: IntoIterator,
{
    let raw_values: Vec<_> = iter.into_iter().collect();
    let values = Arc::new(A::from(raw_values));
    one_column_roundtrip(values, false)
}

fn values_optional<A, I>(iter: I) -> Vec<Bytes>
where
    A: From<Vec<Option<I::Item>>> + Array + 'static,
    I: IntoIterator,
{
    let optional_raw_values: Vec<_> = iter
        .into_iter()
        .enumerate()
        .map(|(i, v)| if i % 2 == 0 { None } else { Some(v) })
        .collect();
    let optional_values = Arc::new(A::from(optional_raw_values));
    one_column_roundtrip(optional_values, true)
}

fn required_and_optional<A, I>(iter: I)
where
    A: From<Vec<I::Item>> + From<Vec<Option<I::Item>>> + Array + 'static,
    I: IntoIterator + Clone,
{
    values_required::<A, I>(iter.clone());
    values_optional::<A, I>(iter);
}

fn check_bloom_filter<T: AsBytes>(
    files: Vec<Bytes>,
    file_column: String,
    positive_values: Vec<T>,
    negative_values: Vec<T>,
) {
    files.into_iter().take(1).for_each(|file| {
        let file_reader = SerializedFileReader::new_with_options(
            file,
            ReadOptionsBuilder::new()
                .with_reader_properties(
                    ReaderProperties::builder()
                        .set_read_bloom_filter(true)
                        .build(),
                )
                .build(),
        )
        .expect("Unable to open file as Parquet");
        let metadata = file_reader.metadata();

        // Gets bloom filters from all row groups.
        let mut bloom_filters: Vec<_> = vec![];
        for (ri, row_group) in metadata.row_groups().iter().enumerate() {
            if let Some((column_index, _)) = row_group
                .columns()
                .iter()
                .enumerate()
                .find(|(_, column)| column.column_path().string() == file_column)
            {
                let row_group_reader = file_reader
                    .get_row_group(ri)
                    .expect("Unable to read row group");
                if let Some(sbbf) = row_group_reader.get_column_bloom_filter(column_index) {
                    bloom_filters.push(sbbf.clone());
                } else {
                    panic!("No bloom filter for column named {file_column} found");
                }
            } else {
                panic!("No column named {file_column} found");
            }
        }

        positive_values.iter().for_each(|value| {
            let found = bloom_filters.iter().find(|sbbf| sbbf.check(value));
            assert!(
                found.is_some(),
                "{}",
                format!("Value {:?} should be in bloom filter", value.as_bytes())
            );
        });

        negative_values.iter().for_each(|value| {
            let found = bloom_filters.iter().find(|sbbf| sbbf.check(value));
            assert!(
                found.is_none(),
                "{}",
                format!("Value {:?} should not be in bloom filter", value.as_bytes())
            );
        });
    });
}

#[test]
fn all_null_primitive_single_column() {
    let values = Arc::new(Int32Array::from(vec![None; SMALL_SIZE]));
    one_column_roundtrip(values, true);
}

#[test]
fn null_single_column() {
    let values = Arc::new(NullArray::new(SMALL_SIZE));
    one_column_roundtrip(values, true);
    // null arrays are always nullable, a test with non-nullable nulls fails
}

#[test]
fn bool_single_column() {
    required_and_optional::<BooleanArray, _>(
        [true, false].iter().cycle().copied().take(SMALL_SIZE),
    );
}

#[test]
fn bool_large_single_column() {
    let values = Arc::new(
        [None, Some(true), Some(false)]
            .iter()
            .cycle()
            .copied()
            .take(200_000)
            .collect::<BooleanArray>(),
    );
    let schema = Schema::new(vec![Field::new("col", values.data_type().clone(), true)]);
    let expected_batch = RecordBatch::try_new(Arc::new(schema), vec![values]).unwrap();
    let file = tempfile::tempfile().unwrap();

    let mut writer = ArrowWriter::try_new(file.try_clone().unwrap(), expected_batch.schema(), None)
        .expect("Unable to write file");
    writer.write(&expected_batch).unwrap();
    writer.close().unwrap();
}

#[test]
fn i8_single_column() {
    required_and_optional::<Int8Array, _>(0..SMALL_SIZE as i8);
}

#[test]
fn i16_single_column() {
    required_and_optional::<Int16Array, _>(0..SMALL_SIZE as i16);
}

#[test]
fn i32_single_column() {
    required_and_optional::<Int32Array, _>(0..SMALL_SIZE as i32);
}

#[test]
fn i64_single_column() {
    required_and_optional::<Int64Array, _>(0..SMALL_SIZE as i64);
}

#[test]
fn u8_single_column() {
    required_and_optional::<UInt8Array, _>(0..SMALL_SIZE as u8);
}

#[test]
fn u16_single_column() {
    required_and_optional::<UInt16Array, _>(0..SMALL_SIZE as u16);
}

#[test]
fn u32_single_column() {
    required_and_optional::<UInt32Array, _>(0..SMALL_SIZE as u32);
}

#[test]
fn u64_single_column() {
    required_and_optional::<UInt64Array, _>(0..SMALL_SIZE as u64);
}

#[test]
fn f32_single_column() {
    required_and_optional::<Float32Array, _>((0..SMALL_SIZE).map(|i| i as f32));
}

#[test]
fn f64_single_column() {
    required_and_optional::<Float64Array, _>((0..SMALL_SIZE).map(|i| i as f64));
}

#[test]
fn timestamp_second_single_column() {
    let raw_values: Vec<_> = (0..SMALL_SIZE as i64).collect();
    let values = Arc::new(TimestampSecondArray::from(raw_values));

    one_column_roundtrip(values, false);
}

#[test]
fn timestamp_millisecond_single_column() {
    let raw_values: Vec<_> = (0..SMALL_SIZE as i64).collect();
    let values = Arc::new(TimestampMillisecondArray::from(raw_values));

    one_column_roundtrip(values, false);
}

#[test]
fn timestamp_microsecond_single_column() {
    let raw_values: Vec<_> = (0..SMALL_SIZE as i64).collect();
    let values = Arc::new(TimestampMicrosecondArray::from(raw_values));

    one_column_roundtrip(values, false);
}

#[test]
fn timestamp_nanosecond_single_column() {
    let raw_values: Vec<_> = (0..SMALL_SIZE as i64).collect();
    let values = Arc::new(TimestampNanosecondArray::from(raw_values));

    one_column_roundtrip(values, false);
}

#[test]
fn date32_single_column() {
    required_and_optional::<Date32Array, _>(0..SMALL_SIZE as i32);
}

#[test]
fn date64_single_column() {
    // Date64 must be a multiple of 86400000, see ARROW-10925
    required_and_optional::<Date64Array, _>((0..(SMALL_SIZE as i64 * 86400000)).step_by(86400000));
}

#[test]
fn time32_second_single_column() {
    required_and_optional::<Time32SecondArray, _>(0..SMALL_SIZE as i32);
}

#[test]
fn time32_millisecond_single_column() {
    required_and_optional::<Time32MillisecondArray, _>(0..SMALL_SIZE as i32);
}

#[test]
fn time64_microsecond_single_column() {
    required_and_optional::<Time64MicrosecondArray, _>(0..SMALL_SIZE as i64);
}

#[test]
fn time64_nanosecond_single_column() {
    required_and_optional::<Time64NanosecondArray, _>(0..SMALL_SIZE as i64);
}

#[test]
fn duration_second_single_column() {
    required_and_optional::<DurationSecondArray, _>(0..SMALL_SIZE as i64);
}

#[test]
fn duration_millisecond_single_column() {
    required_and_optional::<DurationMillisecondArray, _>(0..SMALL_SIZE as i64);
}

#[test]
fn duration_microsecond_single_column() {
    required_and_optional::<DurationMicrosecondArray, _>(0..SMALL_SIZE as i64);
}

#[test]
fn duration_nanosecond_single_column() {
    required_and_optional::<DurationNanosecondArray, _>(0..SMALL_SIZE as i64);
}

#[test]
fn interval_year_month_single_column() {
    required_and_optional::<IntervalYearMonthArray, _>(0..SMALL_SIZE as i32);
}

#[test]
fn interval_day_time_single_column() {
    required_and_optional::<IntervalDayTimeArray, _>(vec![
        IntervalDayTime::new(0, 1),
        IntervalDayTime::new(0, 3),
        IntervalDayTime::new(3, -2),
        IntervalDayTime::new(-200, 4),
    ]);
}

#[test]
#[should_panic(
    expected = "Attempting to write an Arrow interval type MonthDayNano to parquet that is not yet implemented"
)]
fn interval_month_day_nano_single_column() {
    required_and_optional::<IntervalMonthDayNanoArray, _>(vec![
        IntervalMonthDayNano::new(0, 1, 5),
        IntervalMonthDayNano::new(0, 3, 2),
        IntervalMonthDayNano::new(3, -2, -5),
        IntervalMonthDayNano::new(-200, 4, -1),
    ]);
}

#[test]
fn binary_single_column() {
    let one_vec: Vec<u8> = (0..SMALL_SIZE as u8).collect();
    let many_vecs: Vec<_> = std::iter::repeat_n(one_vec, SMALL_SIZE).collect();
    let many_vecs_iter = many_vecs.iter().map(|v| v.as_slice());

    // BinaryArrays can't be built from Vec<Option<&str>>, so only call `values_required`
    values_required::<BinaryArray, _>(many_vecs_iter);
}

#[test]
fn binary_view_single_column() {
    let one_vec: Vec<u8> = (0..SMALL_SIZE as u8).collect();
    let many_vecs: Vec<_> = std::iter::repeat_n(one_vec, SMALL_SIZE).collect();
    let many_vecs_iter = many_vecs.iter().map(|v| v.as_slice());

    // BinaryArrays can't be built from Vec<Option<&str>>, so only call `values_required`
    values_required::<BinaryViewArray, _>(many_vecs_iter);
}

#[test]
fn i32_column_bloom_filter_at_end() {
    let array = Arc::new(Int32Array::from_iter(0..SMALL_SIZE as i32));
    let mut options = RoundTripOptions::new(array, false);
    options.bloom_filter = true;
    options.bloom_filter_position = BloomFilterPosition::End;

    let files = one_column_roundtrip_with_options(options);
    check_bloom_filter(
        files,
        "col".to_string(),
        (0..SMALL_SIZE as i32).collect(),
        (SMALL_SIZE as i32 + 1..SMALL_SIZE as i32 + 10).collect(),
    );
}

#[test]
fn i32_column_bloom_filter() {
    let array = Arc::new(Int32Array::from_iter(0..SMALL_SIZE as i32));
    let mut options = RoundTripOptions::new(array, false);
    options.bloom_filter = true;

    let files = one_column_roundtrip_with_options(options);
    check_bloom_filter(
        files,
        "col".to_string(),
        (0..SMALL_SIZE as i32).collect(),
        (SMALL_SIZE as i32 + 1..SMALL_SIZE as i32 + 10).collect(),
    );
}

/// a small NDV means a smaller initial filter.
#[test]
fn i32_column_bloom_filter_fixed_ndv() {
    let array = Arc::new(Int32Array::from_iter(0..SMALL_SIZE as i32));

    // NDV much larger than actual distinct values — tests folding a large filter down
    let mut options = RoundTripOptions::new(array.clone(), false);
    options.bloom_filter = true;
    options.bloom_filter_ndv = Some(1_000_000);

    let files = one_column_roundtrip_with_options(options);
    check_bloom_filter(
        files,
        "col".to_string(),
        (0..SMALL_SIZE as i32).collect(),
        (SMALL_SIZE as i32 + 1..SMALL_SIZE as i32 + 10).collect(),
    );

    // NDV smaller than actual distinct values — tests the underestimate path
    let mut options = RoundTripOptions::new(array, false);
    options.bloom_filter = true;
    options.bloom_filter_ndv = Some(3);

    let files = one_column_roundtrip_with_options(options);
    check_bloom_filter(
        files,
        "col".to_string(),
        (0..SMALL_SIZE as i32).collect(),
        (SMALL_SIZE as i32 + 1..SMALL_SIZE as i32 + 10).collect(),
    );
}

#[test]
fn binary_column_bloom_filter() {
    let one_vec: Vec<u8> = (0..SMALL_SIZE as u8).collect();
    let many_vecs: Vec<_> = std::iter::repeat_n(one_vec, SMALL_SIZE).collect();
    let many_vecs_iter = many_vecs.iter().map(|v| v.as_slice());

    let array = Arc::new(BinaryArray::from_iter_values(many_vecs_iter));
    let mut options = RoundTripOptions::new(array, false);
    options.bloom_filter = true;

    let files = one_column_roundtrip_with_options(options);
    check_bloom_filter(
        files,
        "col".to_string(),
        many_vecs,
        vec![vec![(SMALL_SIZE + 1) as u8]],
    );
}

#[test]
fn empty_string_null_column_bloom_filter() {
    let raw_values: Vec<_> = (0..SMALL_SIZE).map(|i| i.to_string()).collect();
    let raw_strs = raw_values.iter().map(|s| s.as_str());

    let array = Arc::new(StringArray::from_iter_values(raw_strs));
    let mut options = RoundTripOptions::new(array, false);
    options.bloom_filter = true;

    let files = one_column_roundtrip_with_options(options);

    let optional_raw_values: Vec<_> = raw_values
        .iter()
        .enumerate()
        .filter_map(|(i, v)| if i % 2 == 0 { None } else { Some(v.as_str()) })
        .collect();
    // For null slots, empty string should not be in bloom filter.
    check_bloom_filter(files, "col".to_string(), optional_raw_values, vec![""]);
}

#[test]
fn large_binary_single_column() {
    let one_vec: Vec<u8> = (0..SMALL_SIZE as u8).collect();
    let many_vecs: Vec<_> = std::iter::repeat_n(one_vec, SMALL_SIZE).collect();
    let many_vecs_iter = many_vecs.iter().map(|v| v.as_slice());

    // LargeBinaryArrays can't be built from Vec<Option<&str>>, so only call `values_required`
    values_required::<LargeBinaryArray, _>(many_vecs_iter);
}

#[test]
fn fixed_size_binary_single_column() {
    let mut builder = FixedSizeBinaryBuilder::new(4);
    builder.append_value(b"0123").unwrap();
    builder.append_null();
    builder.append_value(b"8910").unwrap();
    builder.append_value(b"1112").unwrap();
    let array = Arc::new(builder.finish());

    one_column_roundtrip(array, true);
}

#[test]
fn string_single_column() {
    let raw_values: Vec<_> = (0..SMALL_SIZE).map(|i| i.to_string()).collect();
    let raw_strs = raw_values.iter().map(|s| s.as_str());

    required_and_optional::<StringArray, _>(raw_strs);
}

#[test]
fn large_string_single_column() {
    let raw_values: Vec<_> = (0..SMALL_SIZE).map(|i| i.to_string()).collect();
    let raw_strs = raw_values.iter().map(|s| s.as_str());

    required_and_optional::<LargeStringArray, _>(raw_strs);
}

#[test]
fn string_view_single_column() {
    let raw_values: Vec<_> = (0..SMALL_SIZE).map(|i| i.to_string()).collect();
    let raw_strs = raw_values.iter().map(|s| s.as_str());

    required_and_optional::<StringViewArray, _>(raw_strs);
}

#[test]
fn null_list_single_column() {
    let null_field = Field::new_list_field(DataType::Null, true);
    let list_field = Field::new("emptylist", DataType::List(Arc::new(null_field)), true);

    let schema = Schema::new(vec![list_field]);

    // Build [[], null, [null, null]]
    let a_values = NullArray::new(2);
    let a_value_offsets = arrow::buffer::Buffer::from([0, 0, 0, 2].to_byte_slice());
    let a_list_data = ArrayData::builder(DataType::List(Arc::new(Field::new_list_field(
        DataType::Null,
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
fn list_single_column() {
    let a_values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let a_value_offsets = arrow::buffer::Buffer::from([0, 1, 3, 3, 6, 10].to_byte_slice());
    let a_list_data = ArrayData::builder(DataType::List(Arc::new(Field::new_list_field(
        DataType::Int32,
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

    one_column_roundtrip(values, true);
}

#[test]
fn large_list_single_column() {
    let a_values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let a_value_offsets = arrow::buffer::Buffer::from([0i64, 1, 3, 3, 6, 10].to_byte_slice());
    let a_list_data = ArrayData::builder(DataType::LargeList(Arc::new(Field::new(
        "large_item",
        DataType::Int32,
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

    one_column_roundtrip(values, true);
}

#[test]
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
    one_column_roundtrip(Arc::new(list), true);

    let list = LargeListArray::from_iter_primitive::<Int32Type, _, _>(data);
    one_column_roundtrip(Arc::new(list), true);
}

#[test]
fn list_utf8_view_selective_padding_roundtrip() {
    let item = Arc::new(Field::new_list_field(DataType::Utf8View, true));
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

    one_column_roundtrip(Arc::new(builder.finish()), true);
}

#[test]
fn struct_single_column() {
    let a_values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let struct_field_a = Arc::new(Field::new("f", DataType::Int32, false));
    let s = StructArray::from(vec![(struct_field_a, Arc::new(a_values) as ArrayRef)]);

    let values = Arc::new(s);
    one_column_roundtrip(values, false);
}

#[test]
fn fallback_flush_data_page() {
    //tests if the Fallback::flush_data_page clears all buffers correctly
    let raw_values: Vec<_> = (0..MEDIUM_SIZE).map(|i| i.to_string()).collect();
    let values = Arc::new(StringArray::from(raw_values));
    let encodings = vec![
        Encoding::DELTA_BYTE_ARRAY,
        Encoding::DELTA_LENGTH_BYTE_ARRAY,
    ];
    let data_type = values.data_type().clone();
    let schema = Arc::new(Schema::new(vec![Field::new("col", data_type, false)]));
    let expected_batch = RecordBatch::try_new(schema, vec![values]).unwrap();

    let row_group_sizes = [1024, SMALL_SIZE, SMALL_SIZE / 2, SMALL_SIZE / 2 + 1, 10];
    let data_page_size_limit: usize = 32;
    let write_batch_size: usize = 16;

    for encoding in &encodings {
        for row_group_size in row_group_sizes {
            let props = WriterProperties::builder()
                .set_writer_version(WriterVersion::PARQUET_2_0)
                .set_max_row_group_row_count(Some(row_group_size))
                .set_dictionary_enabled(false)
                .set_encoding(*encoding)
                .set_data_page_size_limit(data_page_size_limit)
                .set_write_batch_size(write_batch_size)
                .build();

            roundtrip_opts_with_array_validation(&expected_batch, props, |a, b| {
                let string_array_a = StringArray::from(a.clone());
                let string_array_b = StringArray::from(b.clone());
                let vec_a: Vec<&str> = string_array_a.iter().map(|v| v.unwrap()).collect();
                let vec_b: Vec<&str> = string_array_b.iter().map(|v| v.unwrap()).collect();
                assert_eq!(
                    vec_a, vec_b,
                    "failed for encoder: {encoding:?} and row_group_size: {row_group_size:?}"
                );
            });
        }
    }
}

#[test]
fn arrow_writer_string_dictionary() {
    // define schema
    #[allow(deprecated)]
    let schema = Arc::new(Schema::new(vec![Field::new_dict(
        "dictionary",
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
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
    one_column_roundtrip_with_schema(Arc::new(d), schema);
}

#[test]
fn arrow_writer_test_type_compatibility() {
    fn ensure_compatible_write<T1, T2>(array1: T1, array2: T2, expected_result: T1)
    where
        T1: Array + 'static,
        T2: Array + 'static,
    {
        let schema1 = Arc::new(Schema::new(vec![Field::new(
            "a",
            array1.data_type().clone(),
            false,
        )]));

        let file = tempfile().unwrap();
        let mut writer =
            ArrowWriter::try_new(file.try_clone().unwrap(), schema1.clone(), None).unwrap();

        let rb1 = RecordBatch::try_new(schema1.clone(), vec![Arc::new(array1)]).unwrap();
        writer.write(&rb1).unwrap();

        let schema2 = Arc::new(Schema::new(vec![Field::new(
            "a",
            array2.data_type().clone(),
            false,
        )]));
        let rb2 = RecordBatch::try_new(schema2, vec![Arc::new(array2)]).unwrap();
        writer.write(&rb2).unwrap();

        writer.close().unwrap();

        let mut record_batch_reader =
            ParquetRecordBatchReader::try_new(file.try_clone().unwrap(), 1024).unwrap();
        let actual_batch = record_batch_reader.next().unwrap().unwrap();

        let expected_batch =
            RecordBatch::try_new(schema1, vec![Arc::new(expected_result)]).unwrap();
        assert_eq!(actual_batch, expected_batch);
    }

    // check compatibility between native and dictionaries

    ensure_compatible_write(
        DictionaryArray::new(
            UInt8Array::from_iter_values(vec![0]),
            Arc::new(StringArray::from_iter_values(vec!["parquet"])),
        ),
        StringArray::from_iter_values(vec!["barquet"]),
        DictionaryArray::new(
            UInt8Array::from_iter_values(vec![0, 1]),
            Arc::new(StringArray::from_iter_values(vec!["parquet", "barquet"])),
        ),
    );

    ensure_compatible_write(
        StringArray::from_iter_values(vec!["parquet"]),
        DictionaryArray::new(
            UInt8Array::from_iter_values(vec![0]),
            Arc::new(StringArray::from_iter_values(vec!["barquet"])),
        ),
        StringArray::from_iter_values(vec!["parquet", "barquet"]),
    );

    // check compatibility between dictionaries with different key types

    ensure_compatible_write(
        DictionaryArray::new(
            UInt8Array::from_iter_values(vec![0]),
            Arc::new(StringArray::from_iter_values(vec!["parquet"])),
        ),
        DictionaryArray::new(
            UInt16Array::from_iter_values(vec![0]),
            Arc::new(StringArray::from_iter_values(vec!["barquet"])),
        ),
        DictionaryArray::new(
            UInt8Array::from_iter_values(vec![0, 1]),
            Arc::new(StringArray::from_iter_values(vec!["parquet", "barquet"])),
        ),
    );

    // check compatibility between dictionaries with different value types
    ensure_compatible_write(
        DictionaryArray::new(
            UInt8Array::from_iter_values(vec![0]),
            Arc::new(StringArray::from_iter_values(vec!["parquet"])),
        ),
        DictionaryArray::new(
            UInt8Array::from_iter_values(vec![0]),
            Arc::new(LargeStringArray::from_iter_values(vec!["barquet"])),
        ),
        DictionaryArray::new(
            UInt8Array::from_iter_values(vec![0, 1]),
            Arc::new(StringArray::from_iter_values(vec!["parquet", "barquet"])),
        ),
    );

    // check compatibility between a dictionary and a native array with a different type
    ensure_compatible_write(
        DictionaryArray::new(
            UInt8Array::from_iter_values(vec![0]),
            Arc::new(StringArray::from_iter_values(vec!["parquet"])),
        ),
        LargeStringArray::from_iter_values(vec!["barquet"]),
        DictionaryArray::new(
            UInt8Array::from_iter_values(vec![0, 1]),
            Arc::new(StringArray::from_iter_values(vec!["parquet", "barquet"])),
        ),
    );

    // check compatibility for string types

    ensure_compatible_write(
        StringArray::from_iter_values(vec!["parquet"]),
        LargeStringArray::from_iter_values(vec!["barquet"]),
        StringArray::from_iter_values(vec!["parquet", "barquet"]),
    );

    ensure_compatible_write(
        LargeStringArray::from_iter_values(vec!["parquet"]),
        StringArray::from_iter_values(vec!["barquet"]),
        LargeStringArray::from_iter_values(vec!["parquet", "barquet"]),
    );

    ensure_compatible_write(
        StringArray::from_iter_values(vec!["parquet"]),
        StringViewArray::from_iter_values(vec!["barquet"]),
        StringArray::from_iter_values(vec!["parquet", "barquet"]),
    );

    ensure_compatible_write(
        StringViewArray::from_iter_values(vec!["parquet"]),
        StringArray::from_iter_values(vec!["barquet"]),
        StringViewArray::from_iter_values(vec!["parquet", "barquet"]),
    );

    ensure_compatible_write(
        LargeStringArray::from_iter_values(vec!["parquet"]),
        StringViewArray::from_iter_values(vec!["barquet"]),
        LargeStringArray::from_iter_values(vec!["parquet", "barquet"]),
    );

    ensure_compatible_write(
        StringViewArray::from_iter_values(vec!["parquet"]),
        LargeStringArray::from_iter_values(vec!["barquet"]),
        StringViewArray::from_iter_values(vec!["parquet", "barquet"]),
    );

    // check compatibility for binary types

    ensure_compatible_write(
        BinaryArray::from_iter_values(vec![b"parquet"]),
        LargeBinaryArray::from_iter_values(vec![b"barquet"]),
        BinaryArray::from_iter_values(vec![b"parquet", b"barquet"]),
    );

    ensure_compatible_write(
        LargeBinaryArray::from_iter_values(vec![b"parquet"]),
        BinaryArray::from_iter_values(vec![b"barquet"]),
        LargeBinaryArray::from_iter_values(vec![b"parquet", b"barquet"]),
    );

    ensure_compatible_write(
        BinaryArray::from_iter_values(vec![b"parquet"]),
        BinaryViewArray::from_iter_values(vec![b"barquet"]),
        BinaryArray::from_iter_values(vec![b"parquet", b"barquet"]),
    );

    ensure_compatible_write(
        BinaryViewArray::from_iter_values(vec![b"parquet"]),
        BinaryArray::from_iter_values(vec![b"barquet"]),
        BinaryViewArray::from_iter_values(vec![b"parquet", b"barquet"]),
    );

    ensure_compatible_write(
        BinaryViewArray::from_iter_values(vec![b"parquet"]),
        LargeBinaryArray::from_iter_values(vec![b"barquet"]),
        BinaryViewArray::from_iter_values(vec![b"parquet", b"barquet"]),
    );

    ensure_compatible_write(
        LargeBinaryArray::from_iter_values(vec![b"parquet"]),
        BinaryViewArray::from_iter_values(vec![b"barquet"]),
        LargeBinaryArray::from_iter_values(vec![b"parquet", b"barquet"]),
    );

    // check compatibility for list types

    let list_field_metadata = HashMap::from_iter(vec![(
        PARQUET_FIELD_ID_META_KEY.to_string(),
        "1".to_string(),
    )]);
    let list_field = Field::new_list_field(DataType::Int32, false);

    let values1 = Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4]));
    let offsets1 = OffsetBuffer::new(vec![0, 2, 5].into());

    let values2 = Arc::new(Int32Array::from(vec![5, 6, 7, 8, 9]));
    let offsets2 = OffsetBuffer::new(vec![0, 3, 5].into());

    let values_expected = Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]));
    let offsets_expected = OffsetBuffer::new(vec![0, 2, 5, 8, 10].into());

    ensure_compatible_write(
        // when the initial schema has the metadata ...
        ListArray::try_new(
            Arc::new(
                list_field
                    .clone()
                    .with_metadata(list_field_metadata.clone()),
            ),
            offsets1,
            values1,
            None,
        )
        .unwrap(),
        // ... and some intermediate schema doesn't have the metadata
        ListArray::try_new(Arc::new(list_field.clone()), offsets2, values2, None).unwrap(),
        // ... the write will still go through, and the resulting schema will inherit the initial metadata
        ListArray::try_new(
            Arc::new(
                list_field
                    .clone()
                    .with_metadata(list_field_metadata.clone()),
            ),
            offsets_expected,
            values_expected,
            None,
        )
        .unwrap(),
    );
}

#[test]
fn arrow_writer_primitive_dictionary() {
    // define schema
    #[allow(deprecated)]
    let schema = Arc::new(Schema::new(vec![Field::new_dict(
        "dictionary",
        DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::UInt32)),
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

    one_column_roundtrip_with_schema(Arc::new(d), schema);
}

#[test]
fn arrow_writer_decimal32_dictionary() {
    let integers = vec![12345, 56789, 34567];

    let keys = UInt8Array::from(vec![Some(0), None, Some(1), Some(2), Some(1)]);

    let values = Decimal32Array::from(integers.clone())
        .with_precision_and_scale(5, 2)
        .unwrap();

    let array = DictionaryArray::new(keys, Arc::new(values));
    one_column_roundtrip(Arc::new(array.clone()), true);

    let values = Decimal32Array::from(integers)
        .with_precision_and_scale(9, 2)
        .unwrap();

    let array = array.with_values(Arc::new(values));
    one_column_roundtrip(Arc::new(array), true);
}

#[test]
fn arrow_writer_decimal64_dictionary() {
    let integers = vec![12345, 56789, 34567];

    let keys = UInt8Array::from(vec![Some(0), None, Some(1), Some(2), Some(1)]);

    let values = Decimal64Array::from(integers.clone())
        .with_precision_and_scale(5, 2)
        .unwrap();

    let array = DictionaryArray::new(keys, Arc::new(values));
    one_column_roundtrip(Arc::new(array.clone()), true);

    let values = Decimal64Array::from(integers)
        .with_precision_and_scale(12, 2)
        .unwrap();

    let array = array.with_values(Arc::new(values));
    one_column_roundtrip(Arc::new(array), true);
}

#[test]
fn arrow_writer_decimal128_dictionary() {
    let integers = vec![12345, 56789, 34567];

    let keys = UInt8Array::from(vec![Some(0), None, Some(1), Some(2), Some(1)]);

    let values = Decimal128Array::from(integers.clone())
        .with_precision_and_scale(5, 2)
        .unwrap();

    let array = DictionaryArray::new(keys, Arc::new(values));
    one_column_roundtrip(Arc::new(array.clone()), true);

    let values = Decimal128Array::from(integers)
        .with_precision_and_scale(12, 2)
        .unwrap();

    let array = array.with_values(Arc::new(values));
    one_column_roundtrip(Arc::new(array), true);
}

#[test]
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
    one_column_roundtrip(Arc::new(array.clone()), true);

    let values = Decimal256Array::from(integers)
        .with_precision_and_scale(12, 2)
        .unwrap();

    let array = array.with_values(Arc::new(values));
    one_column_roundtrip(Arc::new(array), true);
}

#[test]
fn arrow_writer_string_dictionary_unsigned_index() {
    // define schema
    #[allow(deprecated)]
    let schema = Arc::new(Schema::new(vec![Field::new_dict(
        "dictionary",
        DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
        true,
        42,
        true,
    )]));

    // create some data
    let d: UInt8DictionaryArray = [Some("alpha"), None, Some("beta"), Some("alpha")]
        .iter()
        .copied()
        .collect();

    one_column_roundtrip_with_schema(Arc::new(d), schema);
}

#[test]
fn u32_min_max() {
    // check values roundtrip through parquet
    let src = [
        u32::MIN,
        1,
        (i32::MAX as u32) - 1,
        i32::MAX as u32,
        (i32::MAX as u32) + 1,
        u32::MAX - 1,
        u32::MAX,
    ];
    let values = Arc::new(UInt32Array::from_iter_values(src.iter().cloned()));
    let files = one_column_roundtrip(values, false);

    for file in files {
        // check statistics are valid
        let reader = SerializedFileReader::new(file).unwrap();
        let metadata = reader.metadata();

        let mut row_offset = 0;
        for row_group in metadata.row_groups() {
            assert_eq!(row_group.num_columns(), 1);
            let column = row_group.column(0);

            let num_values = column.num_values() as usize;
            let src_slice = &src[row_offset..row_offset + num_values];
            row_offset += column.num_values() as usize;

            let stats = column.statistics().unwrap();
            if let Statistics::Int32(stats) = stats {
                assert_eq!(
                    *stats.min_opt().unwrap() as u32,
                    *src_slice.iter().min().unwrap()
                );
                assert_eq!(
                    *stats.max_opt().unwrap() as u32,
                    *src_slice.iter().max().unwrap()
                );
            } else {
                panic!("Statistics::Int32 missing")
            }
        }
    }
}

#[test]
fn u64_min_max() {
    // check values roundtrip through parquet
    let src = [
        u64::MIN,
        1,
        (i64::MAX as u64) - 1,
        i64::MAX as u64,
        (i64::MAX as u64) + 1,
        u64::MAX - 1,
        u64::MAX,
    ];
    let values = Arc::new(UInt64Array::from_iter_values(src.iter().cloned()));
    let files = one_column_roundtrip(values, false);

    for file in files {
        // check statistics are valid
        let reader = SerializedFileReader::new(file).unwrap();
        let metadata = reader.metadata();

        let mut row_offset = 0;
        for row_group in metadata.row_groups() {
            assert_eq!(row_group.num_columns(), 1);
            let column = row_group.column(0);

            let num_values = column.num_values() as usize;
            let src_slice = &src[row_offset..row_offset + num_values];
            row_offset += column.num_values() as usize;

            let stats = column.statistics().unwrap();
            if let Statistics::Int64(stats) = stats {
                assert_eq!(
                    *stats.min_opt().unwrap() as u64,
                    *src_slice.iter().min().unwrap()
                );
                assert_eq!(
                    *stats.max_opt().unwrap() as u64,
                    *src_slice.iter().max().unwrap()
                );
            } else {
                panic!("Statistics::Int64 missing")
            }
        }
    }
}

#[test]
fn statistics_null_counts_only_nulls() {
    // check that null-count statistics for "only NULL"-columns are correct
    let values = Arc::new(UInt64Array::from(vec![None, None]));
    let files = one_column_roundtrip(values, true);

    for file in files {
        // check statistics are valid
        let reader = SerializedFileReader::new(file).unwrap();
        let metadata = reader.metadata();
        assert_eq!(metadata.num_row_groups(), 1);
        let row_group = metadata.row_group(0);
        assert_eq!(row_group.num_columns(), 1);
        let column = row_group.column(0);
        let stats = column.statistics().unwrap();
        assert_eq!(stats.null_count_opt(), Some(2));
    }
}

#[test]
fn test_list_of_struct_roundtrip() {
    // define schema
    let int_field = Field::new("a", DataType::Int32, true);
    let int_field2 = Field::new("b", DataType::Int32, true);

    let int_builder = Int32Builder::with_capacity(10);
    let int_builder2 = Int32Builder::with_capacity(10);

    let struct_builder = StructBuilder::new(
        vec![int_field, int_field2],
        vec![Box::new(int_builder), Box::new(int_builder2)],
    );
    let mut list_builder = ListBuilder::new(struct_builder);

    // Construct the following array
    // [{a: 1, b: 2}], [], null, [null, null], [{a: null, b: 3}], [{a: 2, b: null}]

    // [{a: 1, b: 2}]
    let values = list_builder.values();
    values
        .field_builder::<Int32Builder>(0)
        .unwrap()
        .append_value(1);
    values
        .field_builder::<Int32Builder>(1)
        .unwrap()
        .append_value(2);
    values.append(true);
    list_builder.append(true);

    // []
    list_builder.append(true);

    // null
    list_builder.append(false);

    // [null, null]
    let values = list_builder.values();
    values
        .field_builder::<Int32Builder>(0)
        .unwrap()
        .append_null();
    values
        .field_builder::<Int32Builder>(1)
        .unwrap()
        .append_null();
    values.append(false);
    values
        .field_builder::<Int32Builder>(0)
        .unwrap()
        .append_null();
    values
        .field_builder::<Int32Builder>(1)
        .unwrap()
        .append_null();
    values.append(false);
    list_builder.append(true);

    // [{a: null, b: 3}]
    let values = list_builder.values();
    values
        .field_builder::<Int32Builder>(0)
        .unwrap()
        .append_null();
    values
        .field_builder::<Int32Builder>(1)
        .unwrap()
        .append_value(3);
    values.append(true);
    list_builder.append(true);

    // [{a: 2, b: null}]
    let values = list_builder.values();
    values
        .field_builder::<Int32Builder>(0)
        .unwrap()
        .append_value(2);
    values
        .field_builder::<Int32Builder>(1)
        .unwrap()
        .append_null();
    values.append(true);
    list_builder.append(true);

    let array = Arc::new(list_builder.finish());

    one_column_roundtrip(array, true);
}

#[test]
fn test_arrow_writer_nullable() {
    let batch_schema = Schema::new(vec![Field::new("int32", DataType::Int32, false)]);
    let file_schema = Schema::new(vec![Field::new("int32", DataType::Int32, true)]);
    let file_schema = Arc::new(file_schema);

    let batch = RecordBatch::try_new(
        Arc::new(batch_schema),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as _],
    )
    .unwrap();

    let mut buf = Vec::with_capacity(1024);
    let mut writer = ArrowWriter::try_new(&mut buf, file_schema.clone(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let mut read = ParquetRecordBatchReader::try_new(Bytes::from(buf), 1024).unwrap();
    let back = read.next().unwrap().unwrap();
    assert_eq!(back.schema(), file_schema);
    assert_ne!(back.schema(), batch.schema());
    assert_eq!(back.column(0).as_ref(), batch.column(0).as_ref());
}

#[test]
// https://github.com/apache/arrow-rs/issues/6988
fn test_roundtrip_empty_schema() {
    // create empty record batch with empty schema
    let empty_batch = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        vec![],
        &RecordBatchOptions::default().with_row_count(Some(0)),
    )
    .unwrap();

    // write to parquet
    let mut parquet_bytes: Vec<u8> = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut parquet_bytes, empty_batch.schema(), None).unwrap();
    writer.write(&empty_batch).unwrap();
    writer.close().unwrap();

    // read from parquet
    let bytes = Bytes::from(parquet_bytes);
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
    assert_eq!(reader.schema(), &empty_batch.schema());
    let batches: Vec<_> = reader
        .build()
        .unwrap()
        .collect::<ArrowResult<Vec<_>>>()
        .unwrap();
    assert_eq!(batches.len(), 0);
}

#[test]
fn test_arrow_writer_granular_mode_roundtrip() {
    // Granular mode subdivides chunks and writes more pages than the
    // default batched path. Make sure the data we write back is
    // bit-identical to what went in — page-count assertions elsewhere
    // only prove pages were cut, not that the encoded data is correct.
    //
    // Mix value sizes so that the cumulative-byte-budget cutoff
    // lands mid-chunk, exercising both batched and granular paths
    // within the same `write_batch_internal` call.
    let small = "tiny".to_string();
    let big = "x".repeat(64 * 1024);
    let strings: Vec<String> = (0..256)
        .map(|i| {
            if i % 16 == 0 {
                big.clone()
            } else {
                small.clone()
            }
        })
        .collect();

    let schema = Arc::new(Schema::new(vec![Field::new(
        "col",
        ArrowDataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(strings.clone())) as _],
    )
    .unwrap();

    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_data_page_size_limit(16 * 1024)
        .build();
    let mut writer = ArrowWriter::try_new(Vec::new(), schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    let data = Bytes::from(writer.into_inner().unwrap());

    let mut reader = ParquetRecordBatchReader::try_new(data, 1024).unwrap();
    let read = reader.next().unwrap().unwrap();
    assert!(reader.next().is_none(), "expected one batch");
    let col = read
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(col.len(), strings.len());
    for (i, expected) in strings.iter().enumerate() {
        assert_eq!(
            col.value(i),
            expected.as_str(),
            "value mismatch at index {i}"
        );
    }
}

/// Writes a single-column RecordBatch to an in-memory Parquet buffer.
fn write_column_to_bytes(array: ArrayRef) -> Bytes {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "col",
        array.data_type().clone(),
        true,
    )]));
    let buf = get_bytes_after_close(
        schema.clone(),
        &RecordBatch::try_new(schema, vec![array]).unwrap(),
    );
    Bytes::from(buf)
}

/// Reads column 0 from a single-row-group Parquet buffer, projecting it with the given schema.
/// Passing a flat schema when the buffer was written from a REE array lets callers decode
/// the physical values without the run-end encoding wrapper.
fn read_column_with_schema(bytes: Bytes, schema: SchemaRef) -> ArrayRef {
    let opts = parquet::arrow::arrow_reader::ArrowReaderOptions::new().with_schema(schema);
    ParquetRecordBatchReaderBuilder::try_new_with_options(bytes, opts)
        .unwrap()
        .build()
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .column(0)
        .clone()
}

fn ree_write_read_roundtrip(ree: ArrayRef, flat: ArrayRef) {
    let flat_schema = Arc::new(Schema::new(vec![Field::new(
        "col",
        flat.data_type().clone(),
        true,
    )]));
    let ree_bytes = write_column_to_bytes(ree);
    let flat_bytes = write_column_to_bytes(flat.clone());
    assert_eq!(
        ree_bytes, flat_bytes,
        "REE and flat bytes should be identical"
    );

    let decoded_ree = read_column_with_schema(ree_bytes, flat_schema.clone());
    let decoded_flat = read_column_with_schema(flat_bytes, flat_schema);

    assert_eq!(decoded_ree.as_ref(), flat.as_ref());
    assert_eq!(decoded_ree.as_ref(), decoded_flat.as_ref());
}

#[test]
fn ree_string() {
    let ree: ArrayRef = Arc::new(
        [Some("a"), Some("a"), None, Some("b"), Some("b")]
            .into_iter()
            .collect::<Int32RunArray>(),
    );
    let flat: ArrayRef = Arc::new(StringArray::from(vec![
        Some("a"),
        Some("a"),
        None,
        Some("b"),
        Some("b"),
    ]));
    ree_write_read_roundtrip(ree, flat);
}

#[test]
fn ree_int32() {
    let mut b = PrimitiveRunBuilder::<Int32Type, Int32Type>::new();
    for v in [Some(1), Some(1), None, Some(2), Some(2)] {
        b.append_option(v);
    }
    let ree: ArrayRef = Arc::new(b.finish());
    let flat: ArrayRef = Arc::new(Int32Array::from(vec![
        Some(1),
        Some(1),
        None,
        Some(2),
        Some(2),
    ]));
    ree_write_read_roundtrip(ree, flat);
}

#[test]
fn ree_bool() {
    // run_ends [3, 5, 7] → [T,T,T, null,null, F,F]
    let ree: ArrayRef = Arc::new(
        RunArray::try_new(
            &Int32Array::from(vec![3, 5, 7]),
            &BooleanArray::from(vec![Some(true), None, Some(false)]),
        )
        .unwrap(),
    );
    let flat: ArrayRef = Arc::new(BooleanArray::from(vec![
        Some(true),
        Some(true),
        Some(true),
        None,
        None,
        Some(false),
        Some(false),
    ]));
    ree_write_read_roundtrip(ree, flat);
}

#[test]
fn ree_fixed_size_binary() {
    let mk = |vals: &[Option<&[u8]>]| -> FixedSizeBinaryArray {
        let mut b = FixedSizeBinaryBuilder::new(2);
        for v in vals {
            match v {
                Some(x) => b.append_value(x).unwrap(),
                None => b.append_null(),
            }
        }
        b.finish()
    };
    // run_ends [2, 4, 6] → [aa,aa, null,null, bb,bb]
    let ree: ArrayRef = Arc::new(
        RunArray::try_new(
            &Int32Array::from(vec![2, 4, 6]),
            &mk(&[Some(b"aa"), None, Some(b"bb")]),
        )
        .unwrap(),
    );
    let flat: ArrayRef = Arc::new(mk(&[
        Some(b"aa"),
        Some(b"aa"),
        None,
        None,
        Some(b"bb"),
        Some(b"bb"),
    ]));
    ree_write_read_roundtrip(ree, flat);
}

#[test]
fn ree_single_run() {
    let ree: ArrayRef = Arc::new(["x", "x", "x"].into_iter().collect::<Int32RunArray>());
    let flat: ArrayRef = Arc::new(StringArray::from(vec!["x", "x", "x"]));
    ree_write_read_roundtrip(ree, flat);
}

#[test]
fn ree_float32() {
    // run_ends [2, 4, 5] → [1.0, 1.0, null, null, 2.5]
    let ree: ArrayRef = Arc::new(
        RunArray::try_new(
            &Int32Array::from(vec![2, 4, 5]),
            &Float32Array::from(vec![Some(1.0_f32), None, Some(2.5_f32)]),
        )
        .unwrap(),
    );
    let flat: ArrayRef = Arc::new(Float32Array::from(vec![
        Some(1.0_f32),
        Some(1.0_f32),
        None,
        None,
        Some(2.5_f32),
    ]));
    ree_write_read_roundtrip(ree, flat);
}

#[test]
fn ree_sliced() {
    // A sliced (non-zero offset) REE array: verify that get_physical_index
    // correctly accounts for the logical offset when expanding.
    // Full array: run_ends [3, 5, 7] → [a,a,a, b,b, c,c]
    // After slice(2, 5) the logical view is [a, b, b, c, c].
    let full: ArrayRef = Arc::new(
        RunArray::try_new(
            &Int32Array::from(vec![3, 5, 7]),
            &StringArray::from(vec!["a", "b", "c"]),
        )
        .unwrap(),
    );
    let sliced = full.slice(2, 5);
    let flat: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "b", "c", "c"]));
    ree_write_read_roundtrip(sliced, flat);
}
