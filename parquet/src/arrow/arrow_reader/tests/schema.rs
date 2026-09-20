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

//! Schema inference, supplied schemas, type conversions, and column projection.

use super::*;

#[test]
fn test_arrow_reader_all_columns() {
    let file = get_test_file("parquet/generated_simple_numerics/blogs.parquet");

    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let original_schema = Arc::clone(builder.schema());
    let reader = builder.build().unwrap();

    // Verify that the schema was correctly parsed
    assert_eq!(original_schema.fields(), reader.schema().fields());
}

#[test]
fn test_reuse_schema() {
    let file = get_test_file("parquet/alltypes-java.parquet");

    let builder = ParquetRecordBatchReaderBuilder::try_new(file.try_clone().unwrap()).unwrap();
    let expected = builder.metadata;
    let schema = expected.file_metadata().schema_descr_ptr();

    let arrow_options = ArrowReaderOptions::new().with_parquet_schema(schema.clone());
    let builder =
        ParquetRecordBatchReaderBuilder::try_new_with_options(file, arrow_options).unwrap();

    // Verify that the metadata matches
    assert_eq!(expected.as_ref(), builder.metadata.as_ref());
}

#[test]
fn test_arrow_reader_single_column() {
    let file = get_test_file("parquet/generated_simple_numerics/blogs.parquet");

    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let original_schema = Arc::clone(builder.schema());

    let mask = ProjectionMask::leaves(builder.parquet_schema(), [2]);
    let reader = builder.with_projection(mask).build().unwrap();

    // Verify that the schema was correctly parsed
    assert_eq!(1, reader.schema().fields().len());
    assert_eq!(original_schema.fields()[1], reader.schema().fields()[0]);
}

#[test]
fn test_arrow_reader_single_column_by_name() {
    let file = get_test_file("parquet/generated_simple_numerics/blogs.parquet");

    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let original_schema = Arc::clone(builder.schema());

    let mask = ProjectionMask::columns(builder.parquet_schema(), ["blog_id"]);
    let reader = builder.with_projection(mask).build().unwrap();

    // Verify that the schema was correctly parsed
    assert_eq!(1, reader.schema().fields().len());
    assert_eq!(original_schema.fields()[1], reader.schema().fields()[0]);
}

fn get_test_file(file_name: &str) -> File {
    let path = PathBuf::from(arrow::util::test_util::arrow_test_data()).join(file_name);

    File::open(path.as_path()).expect("File not found!")
}

#[cfg_attr(miri, ignore)] // calls native Zstd code unsupported by Miri
#[test]
fn test_read_structs() {
    // This particular test file has columns of struct types where there is
    // a column that has the same name as one of the struct fields
    // (see: ARROW-11452)
    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/nested_structs.rust.parquet");
    let file = File::open(&path).unwrap();
    let record_batch_reader = ParquetRecordBatchReader::try_new(file, 60).unwrap();

    for batch in record_batch_reader {
        batch.unwrap();
    }

    let file = File::open(&path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();

    let mask = ProjectionMask::leaves(builder.parquet_schema(), [3, 8, 10]);
    let projected_reader = builder
        .with_projection(mask)
        .with_batch_size(60)
        .build()
        .unwrap();

    let expected_schema = Schema::new(vec![
        Field::new(
            "roll_num",
            ArrowDataType::Struct(Fields::from(vec![Field::new(
                "count",
                ArrowDataType::UInt64,
                false,
            )])),
            false,
        ),
        Field::new(
            "PC_CUR",
            ArrowDataType::Struct(Fields::from(vec![
                Field::new("mean", ArrowDataType::Int64, false),
                Field::new("sum", ArrowDataType::Int64, false),
            ])),
            false,
        ),
    ]);

    // Tests for #1652 and #1654
    assert_eq!(&expected_schema, projected_reader.schema().as_ref());

    for batch in projected_reader {
        let batch = batch.unwrap();
        assert_eq!(batch.schema().as_ref(), &expected_schema);
    }
}

#[cfg_attr(miri, ignore)] // calls native Zstd code unsupported by Miri
#[test]
// same as test_read_structs but constructs projection mask via column names
fn test_read_structs_by_name() {
    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/nested_structs.rust.parquet");
    let file = File::open(&path).unwrap();
    let record_batch_reader = ParquetRecordBatchReader::try_new(file, 60).unwrap();

    for batch in record_batch_reader {
        batch.unwrap();
    }

    let file = File::open(&path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();

    let mask = ProjectionMask::columns(
        builder.parquet_schema(),
        ["roll_num.count", "PC_CUR.mean", "PC_CUR.sum"],
    );
    let projected_reader = builder
        .with_projection(mask)
        .with_batch_size(60)
        .build()
        .unwrap();

    let expected_schema = Schema::new(vec![
        Field::new(
            "roll_num",
            ArrowDataType::Struct(Fields::from(vec![Field::new(
                "count",
                ArrowDataType::UInt64,
                false,
            )])),
            false,
        ),
        Field::new(
            "PC_CUR",
            ArrowDataType::Struct(Fields::from(vec![
                Field::new("mean", ArrowDataType::Int64, false),
                Field::new("sum", ArrowDataType::Int64, false),
            ])),
            false,
        ),
    ]);

    assert_eq!(&expected_schema, projected_reader.schema().as_ref());

    for batch in projected_reader {
        let batch = batch.unwrap();
        assert_eq!(batch.schema().as_ref(), &expected_schema);
    }
}

// test that we can handle the UNKNOWN logical type annotation on any physical type
#[test]
fn test_unknown_logical_type() {
    let message_type = "message uk {
        OPTIONAL INT32 uki32 (UNKNOWN);
        OPTIONAL INT64 uki64 (UNKNOWN);
        OPTIONAL INT96 uki96 (UNKNOWN);
        OPTIONAL BOOLEAN ukbool (UNKNOWN);
        OPTIONAL FLOAT ukfloat (UNKNOWN);
        OPTIONAL DOUBLE ukdbl (UNKNOWN);
        OPTIONAL BYTE_ARRAY ukbytes (UNKNOWN);
        OPTIONAL FIXED_LEN_BYTE_ARRAY(10) ukflba (UNKNOWN);
    }";

    let schema = Arc::new(parse_message_type(message_type).unwrap());
    let file = tempfile::tempfile().unwrap();

    let mut writer =
        SerializedFileWriter::new(file.try_clone().unwrap(), schema, Default::default()).unwrap();

    let mut row_group_writer = writer.next_row_group().unwrap();

    fn write_nulls<T: DataType>(row_group_writer: &mut SerializedRowGroupWriter<'_, File>) {
        let mut column_writer = row_group_writer.next_column().unwrap().unwrap();
        // write out a bunch of nulls
        column_writer
            .typed::<T>()
            .write_batch(&[], Some(&[0, 0, 0, 0]), None)
            .unwrap();
        column_writer.close().unwrap();
    }

    // INT32
    write_nulls::<Int32Type>(&mut row_group_writer);

    // INT64
    write_nulls::<Int64Type>(&mut row_group_writer);

    // INT96
    write_nulls::<Int96Type>(&mut row_group_writer);

    // BOOLEAN
    write_nulls::<BoolType>(&mut row_group_writer);

    // FLOAT
    write_nulls::<FloatType>(&mut row_group_writer);

    // DOUBLE
    write_nulls::<DoubleType>(&mut row_group_writer);

    // BYTE_ARRAY
    write_nulls::<ByteArrayType>(&mut row_group_writer);

    // FIXED_LEN_BYTE_ARRAY
    write_nulls::<FixedLenByteArrayType>(&mut row_group_writer);

    row_group_writer.close().unwrap();

    writer.close().unwrap();

    let mut reader = ParquetRecordBatchReader::try_new(file, 4).unwrap();
    let batch = reader.next().unwrap().unwrap();

    for col in batch.columns() {
        assert_eq!(col.len(), 4);
        assert_eq!(col.logical_null_count(), 4);
        assert_eq!(*col.data_type(), ArrowDataType::Null);
    }
}

#[test]
fn test_nested_nullability() {
    let message_type = "message nested {
      OPTIONAL Group group {
        REQUIRED INT32 leaf;
      }
    }";

    let file = tempfile::tempfile().unwrap();
    let schema = Arc::new(parse_message_type(message_type).unwrap());

    {
        // Write using low-level parquet API (#1167)
        let mut writer =
            SerializedFileWriter::new(file.try_clone().unwrap(), schema, Default::default())
                .unwrap();

        {
            let mut row_group_writer = writer.next_row_group().unwrap();
            let mut column_writer = row_group_writer.next_column().unwrap().unwrap();

            column_writer
                .typed::<Int32Type>()
                .write_batch(&[34, 76], Some(&[0, 1, 0, 1]), None)
                .unwrap();

            column_writer.close().unwrap();
            row_group_writer.close().unwrap();
        }

        writer.close().unwrap();
    }

    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let mask = ProjectionMask::leaves(builder.parquet_schema(), [0]);

    let reader = builder.with_projection(mask).build().unwrap();

    let expected_schema = Schema::new(vec![Field::new(
        "group",
        ArrowDataType::Struct(vec![Field::new("leaf", ArrowDataType::Int32, false)].into()),
        true,
    )]);

    let batch = reader.into_iter().next().unwrap().unwrap();
    assert_eq!(batch.schema().as_ref(), &expected_schema);
    assert_eq!(batch.num_rows(), 4);
    assert_eq!(batch.column(0).null_count(), 2);
}

#[test]
fn test_null_schema_inference() {
    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/null_list.parquet");
    let file = File::open(path).unwrap();

    let arrow_field = Field::new(
        "emptylist",
        ArrowDataType::List(Arc::new(Field::new_list_field(ArrowDataType::Null, true))),
        true,
    );

    let options = ArrowReaderOptions::new().with_skip_arrow_metadata(true);
    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options).unwrap();
    let schema = builder.schema();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0), &arrow_field);
}

#[test]
fn test_skip_metadata() {
    let col = Arc::new(TimestampNanosecondArray::from_iter_values(vec![0, 1, 2]));
    let field = Field::new("col", col.data_type().clone(), true);

    let schema_without_metadata = Arc::new(Schema::new(vec![field.clone()]));

    let metadata = arrow_schema::Metadata::from([("key".to_string(), "value".to_string())]);

    let schema_with_metadata = Arc::new(Schema::new(vec![field.with_metadata(metadata)]));

    assert_ne!(schema_with_metadata, schema_without_metadata);

    let batch = RecordBatch::try_new(schema_with_metadata.clone(), vec![col as ArrayRef]).unwrap();

    let file = |version: WriterVersion| {
        let props = WriterProperties::builder()
            .set_writer_version(version)
            .build();

        let file = tempfile().unwrap();
        let mut writer =
            ArrowWriter::try_new(file.try_clone().unwrap(), batch.schema(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        file
    };

    let skip_options = ArrowReaderOptions::new().with_skip_arrow_metadata(true);

    let v1_reader = file(WriterVersion::PARQUET_1_0);
    let v2_reader = file(WriterVersion::PARQUET_2_0);

    let arrow_reader =
        ParquetRecordBatchReader::try_new(v1_reader.try_clone().unwrap(), 1024).unwrap();
    assert_eq!(arrow_reader.schema(), schema_with_metadata);

    let reader =
        ParquetRecordBatchReaderBuilder::try_new_with_options(v1_reader, skip_options.clone())
            .unwrap()
            .build()
            .unwrap();
    assert_eq!(reader.schema(), schema_without_metadata);

    let arrow_reader =
        ParquetRecordBatchReader::try_new(v2_reader.try_clone().unwrap(), 1024).unwrap();
    assert_eq!(arrow_reader.schema(), schema_with_metadata);

    let reader = ParquetRecordBatchReaderBuilder::try_new_with_options(v2_reader, skip_options)
        .unwrap()
        .build()
        .unwrap();
    assert_eq!(reader.schema(), schema_without_metadata);
}

fn run_schema_test_with_error<I, F>(value: I, schema: SchemaRef, expected_error: &str)
where
    I: IntoIterator<Item = (F, ArrayRef)>,
    F: AsRef<str>,
{
    let file = write_parquet_from_iter(value);
    let options_with_schema = ArrowReaderOptions::new().with_schema(schema.clone());
    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(
        file.try_clone().unwrap(),
        options_with_schema,
    );
    assert_eq!(builder.err().unwrap().to_string(), expected_error);
}

#[test]
fn test_schema_too_few_columns() {
    run_schema_test_with_error(
        vec![
            ("int64", Arc::new(Int64Array::from(vec![0])) as ArrayRef),
            ("int32", Arc::new(Int32Array::from(vec![0])) as ArrayRef),
        ],
        Arc::new(Schema::new(vec![Field::new(
            "int64",
            ArrowDataType::Int64,
            false,
        )])),
        "Arrow: incompatible arrow schema, expected 2 struct fields got 1",
    );
}

#[test]
fn test_schema_too_many_columns() {
    run_schema_test_with_error(
        vec![("int64", Arc::new(Int64Array::from(vec![0])) as ArrayRef)],
        Arc::new(Schema::new(vec![
            Field::new("int64", ArrowDataType::Int64, false),
            Field::new("int32", ArrowDataType::Int32, false),
        ])),
        "Arrow: incompatible arrow schema, expected 1 struct fields got 2",
    );
}

#[test]
fn test_schema_mismatched_column_names() {
    run_schema_test_with_error(
        vec![("int64", Arc::new(Int64Array::from(vec![0])) as ArrayRef)],
        Arc::new(Schema::new(vec![Field::new(
            "other",
            ArrowDataType::Int64,
            false,
        )])),
        "Arrow: incompatible arrow schema, expected field named int64 got other",
    );
}

#[test]
fn test_schema_incompatible_columns() {
    run_schema_test_with_error(
        vec![
            (
                "col1_invalid",
                Arc::new(Int64Array::from(vec![0])) as ArrayRef,
            ),
            (
                "col2_valid",
                Arc::new(Int32Array::from(vec![0])) as ArrayRef,
            ),
            (
                "col3_invalid",
                Arc::new(Date64Array::from(vec![0])) as ArrayRef,
            ),
        ],
        Arc::new(Schema::new(vec![
            Field::new("col1_invalid", ArrowDataType::Int32, false),
            Field::new("col2_valid", ArrowDataType::Int32, false),
            Field::new("col3_invalid", ArrowDataType::Int32, false),
        ])),
        "Arrow: Incompatible supplied Arrow schema: data type mismatch for field col1_invalid: requested Int32 but found Int64, data type mismatch for field col3_invalid: requested Int32 but found Int64",
    );
}

#[test]
fn test_one_incompatible_nested_column() {
    let nested_fields = Fields::from(vec![
        Field::new("nested1_valid", ArrowDataType::Utf8, false),
        Field::new("nested1_invalid", ArrowDataType::Int64, false),
    ]);
    let nested = StructArray::try_new(
        nested_fields,
        vec![
            Arc::new(StringArray::from(vec!["a"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![0])) as ArrayRef,
        ],
        None,
    )
    .expect("struct array");
    let supplied_nested_fields = Fields::from(vec![
        Field::new("nested1_valid", ArrowDataType::Utf8, false),
        Field::new("nested1_invalid", ArrowDataType::Int32, false),
    ]);
    run_schema_test_with_error(
        vec![
            ("col1", Arc::new(Int64Array::from(vec![0])) as ArrayRef),
            ("col2", Arc::new(Int32Array::from(vec![0])) as ArrayRef),
            ("nested", Arc::new(nested) as ArrayRef),
        ],
        Arc::new(Schema::new(vec![
            Field::new("col1", ArrowDataType::Int64, false),
            Field::new("col2", ArrowDataType::Int32, false),
            Field::new(
                "nested",
                ArrowDataType::Struct(supplied_nested_fields),
                false,
            ),
        ])),
        "Arrow: Incompatible supplied Arrow schema: data type mismatch for field nested: \
        requested Struct(\"nested1_valid\": non-null Utf8, \"nested1_invalid\": non-null Int32) \
        but found Struct(\"nested1_valid\": non-null Utf8, \"nested1_invalid\": non-null Int64)",
    );
}

/// Return parquet data with a single column of utf8 strings
fn utf8_parquet() -> Bytes {
    let input = StringArray::from_iter_values(vec!["foo", "bar", "baz"]);
    let batch = RecordBatch::try_from_iter(vec![("column1", Arc::new(input) as _)]).unwrap();
    let props = None;
    // write parquet file with non nullable strings
    let mut parquet_data = vec![];
    let mut writer = ArrowWriter::try_new(&mut parquet_data, batch.schema(), props).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    Bytes::from(parquet_data)
}

#[test]
fn test_schema_error_bad_types() {
    // verify incompatible schemas error on read
    let parquet_data = utf8_parquet();

    // Ask to read it back with an incompatible schema (int vs string)
    let input_schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "column1",
        arrow::datatypes::DataType::Int32,
        false,
    )]));

    // read it back out
    let reader_options = ArrowReaderOptions::new().with_schema(input_schema.clone());
    let err = ParquetRecordBatchReaderBuilder::try_new_with_options(parquet_data, reader_options)
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "Arrow: Incompatible supplied Arrow schema: data type mismatch for field column1: requested Int32 but found Utf8"
    )
}

#[test]
fn test_schema_error_bad_nullability() {
    // verify incompatible schemas error on read
    let parquet_data = utf8_parquet();

    // Ask to read it back with an incompatible schema (nullability mismatch)
    let input_schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "column1",
        arrow::datatypes::DataType::Utf8,
        true,
    )]));

    // read it back out
    let reader_options = ArrowReaderOptions::new().with_schema(input_schema.clone());
    let err = ParquetRecordBatchReaderBuilder::try_new_with_options(parquet_data, reader_options)
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "Arrow: Incompatible supplied Arrow schema: nullability mismatch for field column1: expected true but found false"
    )
}

#[test]
fn test_read_binary_as_utf8() {
    let file = write_parquet_from_iter(vec![
        (
            "binary_to_utf8",
            Arc::new(BinaryArray::from(vec![
                b"one".as_ref(),
                b"two".as_ref(),
                b"three".as_ref(),
            ])) as ArrayRef,
        ),
        (
            "large_binary_to_large_utf8",
            Arc::new(LargeBinaryArray::from(vec![
                b"one".as_ref(),
                b"two".as_ref(),
                b"three".as_ref(),
            ])) as ArrayRef,
        ),
        (
            "binary_view_to_utf8_view",
            Arc::new(BinaryViewArray::from(vec![
                b"one".as_ref(),
                b"two".as_ref(),
                b"three".as_ref(),
            ])) as ArrayRef,
        ),
    ]);
    let supplied_fields = Fields::from(vec![
        Field::new("binary_to_utf8", ArrowDataType::Utf8, false),
        Field::new(
            "large_binary_to_large_utf8",
            ArrowDataType::LargeUtf8,
            false,
        ),
        Field::new("binary_view_to_utf8_view", ArrowDataType::Utf8View, false),
    ]);

    let options = ArrowReaderOptions::new().with_schema(Arc::new(Schema::new(supplied_fields)));
    let mut arrow_reader =
        ParquetRecordBatchReaderBuilder::try_new_with_options(file.try_clone().unwrap(), options)
            .expect("reader builder with schema")
            .build()
            .expect("reader with schema");

    let batch = arrow_reader.next().unwrap().unwrap();
    assert_eq!(batch.num_columns(), 3);
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(
        batch
            .column(0)
            .as_string::<i32>()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some("one"), Some("two"), Some("three")]
    );

    assert_eq!(
        batch
            .column(1)
            .as_string::<i64>()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some("one"), Some("two"), Some("three")]
    );

    assert_eq!(
        batch.column(2).as_string_view().iter().collect::<Vec<_>>(),
        vec![Some("one"), Some("two"), Some("three")]
    );
}

/// A supplied schema that reads a `Binary` column as a string type must still
/// validate UTF-8. The readers used to enable validation only when the Parquet
/// column was annotated as a string, so a schema hint bypassed it entirely and
/// produced a string array over arbitrary bytes.
#[test]
fn test_read_non_utf8_binary_as_utf8() {
    let file = write_parquet_from_iter(vec![(
        "non_utf8_binary",
        Arc::new(BinaryArray::from(vec![
            b"\xDE\x00\xFF".as_ref(),
            b"\xDE\x01\xAA".as_ref(),
            b"\xDE\x02\xFF".as_ref(),
        ])) as ArrayRef,
    )]);

    for supplied_type in [
        ArrowDataType::Utf8,
        ArrowDataType::LargeUtf8,
        ArrowDataType::Utf8View,
        ArrowDataType::Dictionary(
            Box::new(ArrowDataType::Int32),
            Box::new(ArrowDataType::Utf8),
        ),
    ] {
        let supplied_fields = Fields::from(vec![Field::new(
            "non_utf8_binary",
            supplied_type.clone(),
            false,
        )]);

        let options = ArrowReaderOptions::new().with_schema(Arc::new(Schema::new(supplied_fields)));
        let mut arrow_reader = ParquetRecordBatchReaderBuilder::try_new_with_options(
            file.try_clone().unwrap(),
            options,
        )
        .expect("reader builder with schema")
        .build()
        .expect("reader with schema");

        let err = arrow_reader.next().unwrap().unwrap_err();
        assert!(
            err.to_string().contains("encountered non UTF-8 data"),
            "reading as {supplied_type}: unexpected error: {err}"
        );
    }
}

#[test]
fn test_with_schema() {
    let nested_fields = Fields::from(vec![
        Field::new("utf8_to_dict", ArrowDataType::Utf8, false),
        Field::new("int64_to_ts_nano", ArrowDataType::Int64, false),
    ]);

    let nested_arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec!["a", "a", "a", "b"])) as ArrayRef,
        Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef,
    ];

    let nested = StructArray::try_new(nested_fields, nested_arrays, None).unwrap();

    let file = write_parquet_from_iter(vec![
        (
            "int32_to_ts_second",
            Arc::new(Int32Array::from(vec![0, 1, 2, 3])) as ArrayRef,
        ),
        (
            "date32_to_date64",
            Arc::new(Date32Array::from(vec![0, 1, 2, 3])) as ArrayRef,
        ),
        ("nested", Arc::new(nested) as ArrayRef),
    ]);

    let supplied_nested_fields = Fields::from(vec![
        Field::new(
            "utf8_to_dict",
            ArrowDataType::Dictionary(
                Box::new(ArrowDataType::Int32),
                Box::new(ArrowDataType::Utf8),
            ),
            false,
        ),
        Field::new(
            "int64_to_ts_nano",
            ArrowDataType::Timestamp(
                arrow::datatypes::TimeUnit::Nanosecond,
                Some("+10:00".into()),
            ),
            false,
        ),
    ]);

    let supplied_schema = Arc::new(Schema::new(vec![
        Field::new(
            "int32_to_ts_second",
            ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Second, Some("+01:00".into())),
            false,
        ),
        Field::new("date32_to_date64", ArrowDataType::Date64, false),
        Field::new(
            "nested",
            ArrowDataType::Struct(supplied_nested_fields),
            false,
        ),
    ]));

    let options = ArrowReaderOptions::new().with_schema(supplied_schema.clone());
    let mut arrow_reader =
        ParquetRecordBatchReaderBuilder::try_new_with_options(file.try_clone().unwrap(), options)
            .expect("reader builder with schema")
            .build()
            .expect("reader with schema");

    assert_eq!(arrow_reader.schema(), supplied_schema);
    let batch = arrow_reader.next().unwrap().unwrap();
    assert_eq!(batch.num_columns(), 3);
    assert_eq!(batch.num_rows(), 4);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .expect("downcast to timestamp second")
            .value_as_datetime_with_tz(0, "+01:00".parse().unwrap())
            .map(|v| v.to_string())
            .expect("value as datetime"),
        "1970-01-01 01:00:00 +01:00"
    );
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<Date64Array>()
            .expect("downcast to date64")
            .value_as_date(0)
            .map(|v| v.to_string())
            .expect("value as date"),
        "1970-01-01"
    );

    let nested = batch
        .column(2)
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("downcast to struct");

    let nested_dict = nested
        .column(0)
        .as_any()
        .downcast_ref::<Int32DictionaryArray>()
        .expect("downcast to dictionary");

    assert_eq!(
        nested_dict
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("downcast to string")
            .iter()
            .collect::<Vec<_>>(),
        vec![Some("a"), Some("b")]
    );

    assert_eq!(
        nested_dict.keys().iter().collect::<Vec<_>>(),
        vec![Some(0), Some(0), Some(0), Some(1)]
    );

    assert_eq!(
        nested
            .column(1)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("downcast to timestamp nanosecond")
            .value_as_datetime_with_tz(0, "+10:00".parse().unwrap())
            .map(|v| v.to_string())
            .expect("value as datetime"),
        "1970-01-01 10:00:00.000000001 +10:00"
    );
}

#[test]
fn test_empty_projection() {
    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/alltypes_plain.parquet");
    let file = File::open(path).unwrap();

    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let file_metadata = builder.metadata().file_metadata();
    let expected_rows = file_metadata.num_rows() as usize;

    let mask = ProjectionMask::leaves(builder.parquet_schema(), []);
    let batch_reader = builder
        .with_projection(mask)
        .with_batch_size(2)
        .build()
        .unwrap();

    let mut total_rows = 0;
    for maybe_batch in batch_reader {
        let batch = maybe_batch.unwrap();
        total_rows += batch.num_rows();
        assert_eq!(batch.num_columns(), 0);
        assert!(batch.num_rows() <= 2);
    }

    assert_eq!(total_rows, expected_rows);
}

#[test]
fn test_raw_repetition() {
    const MESSAGE_TYPE: &str = "
        message Log {
          OPTIONAL INT32 eventType;
          REPEATED INT32 category;
          REPEATED group filter {
            OPTIONAL INT32 error;
          }
        }
    ";
    let schema = Arc::new(parse_message_type(MESSAGE_TYPE).unwrap());
    let props = Default::default();

    let mut buf = Vec::with_capacity(1024);
    let mut writer = SerializedFileWriter::new(&mut buf, schema, props).unwrap();
    let mut row_group_writer = writer.next_row_group().unwrap();

    // column 0
    let mut col_writer = row_group_writer.next_column().unwrap().unwrap();
    col_writer
        .typed::<Int32Type>()
        .write_batch(&[1], Some(&[1]), None)
        .unwrap();
    col_writer.close().unwrap();
    // column 1
    let mut col_writer = row_group_writer.next_column().unwrap().unwrap();
    col_writer
        .typed::<Int32Type>()
        .write_batch(&[1, 1], Some(&[1, 1]), Some(&[0, 1]))
        .unwrap();
    col_writer.close().unwrap();
    // column 2
    let mut col_writer = row_group_writer.next_column().unwrap().unwrap();
    col_writer
        .typed::<Int32Type>()
        .write_batch(&[1], Some(&[1]), Some(&[0]))
        .unwrap();
    col_writer.close().unwrap();

    let rg_md = row_group_writer.close().unwrap();
    assert_eq!(rg_md.num_rows(), 1);
    writer.close().unwrap();

    let bytes = Bytes::from(buf);

    let mut no_mask = ParquetRecordBatchReader::try_new(bytes.clone(), 1024).unwrap();
    let full = no_mask.next().unwrap().unwrap();

    assert_eq!(full.num_columns(), 3);

    for idx in 0..3 {
        let b = ParquetRecordBatchReaderBuilder::try_new(bytes.clone()).unwrap();
        let mask = ProjectionMask::leaves(b.parquet_schema(), [idx]);
        let mut reader = b.with_projection(mask).build().unwrap();
        let projected = reader.next().unwrap().unwrap();

        assert_eq!(projected.num_columns(), 1);
        assert_eq!(full.column(idx), projected.column(0));
    }
}
