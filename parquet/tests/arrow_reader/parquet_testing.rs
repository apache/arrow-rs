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

//! Tests with interoperability files in [parquet-testing]
//!
//! [parquet-testing]: https://github.com/apache/parquet-testing

use arrow::util::test_util::parquet_test_data;
#[cfg(feature = "lz4")]
use arrow_array::Float64Array;
use arrow_array::cast::AsArray;
#[cfg(feature = "flate2")]
use arrow_array::types::{Decimal128Type, Float16Type};
#[cfg(any(feature = "flate2", feature = "zstd"))]
use arrow_array::types::{Float32Type, Float64Type};
use arrow_array::{
    Array, ArrayRef, BinaryArray, Int32Array, Int64Array, ListArray, RecordBatch, StringArray,
    types,
};
use arrow_data::ArrayData;
use arrow_schema::{ArrowError, Field, Schema, TimeUnit};
#[cfg(feature = "flate2")]
use arrow_schema::{DataType as ArrowDataType, Fields};
use half::f16;
use parquet::arrow::arrow_reader::{
    ArrowReaderOptions, ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder,
};
use parquet::basic::{LogicalType, Type as PhysicalType};
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

/// The ALP test data file has 8 columns, each containing the same 9032 values.
///
/// The float_plain and double_plain columns are encoded with `PLAIN` + zstd
/// compression, and serve as a reference for the other columns which are
/// encoded with `ALP` encoding.
///
/// Ensure the values in the other column come back the same as the reference columns
///
/// | Column                              | Encoding                  | Rationale / coverage                                                              |
/// |-------------------------------------|---------------------------|-----------------------------------------------------------------------------------|
/// | `float_plain`, `double_plain`       | `PLAIN` + zstd            | In-file reference: readers can bit-compare the ALP columns against these          |
/// | `float_alp_1024`, `double_alp_1024` | `ALP`, 1024-value vectors | The default vector size of 1024 values                                            |
/// | `float_alp_4096`, `double_alp_4096` | `ALP`, 4096-value vectors | Readers must honor `log_vector_size` from the page header rather than assume 1024 |
/// | `float_alp_32`, `double_alp_32`     | `ALP`, 32-value vectors   | Many vectors per page, stresses the per-vector metadata loop                      |
#[test]
#[cfg_attr(miri, ignore)] // Zstd calls native C functions unsupported by Miri
fn test_alp_extended() {
    let alp_extended = PathBuf::from(parquet_test_data()).join("alp_extended.zstd.parquet");
    let file = File::open(alp_extended).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();

    let batches: Vec<_> = reader.into_iter().collect::<Result<Vec<_>, _>>().unwrap();
    let total_rows = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
    assert_eq!(total_rows, 9032);
    batches
        .iter()
        .for_each(|batch| assert_eq!(batch.num_columns(), 8));

    // compare float values to the reference
    let float_plain = column(&batches, "float_plain").unwrap();
    assert_eq!(float_plain, column(&batches, "float_alp_1024").unwrap());
    assert_eq!(float_plain, column(&batches, "float_alp_4096").unwrap());
    assert_eq!(float_plain, column(&batches, "float_alp_32").unwrap());

    // compare double values to the reference
    let double_plain = column(&batches, "double_plain").unwrap();
    assert_eq!(double_plain, column(&batches, "double_alp_1024").unwrap());
    assert_eq!(double_plain, column(&batches, "double_alp_4096").unwrap());
    assert_eq!(double_plain, column(&batches, "double_alp_32").unwrap());
}

fn column(batches: &[RecordBatch], column_name: &str) -> Result<Vec<ArrayRef>, ArrowError> {
    let mut columns = Vec::new();
    for batch in batches {
        let array = batch.column(batch.schema().index_of(column_name)?);
        columns.push(array.clone());
    }
    Ok(columns)
}

#[test]
fn test_int96_from_spark_file_with_provided_schema() {
    // int96_from_spark.parquet was written based on Spark's microsecond timestamps which trade
    // range for resolution compared to a nanosecond timestamp. We must provide a schema with
    // microsecond resolution for the Parquet reader to interpret these values correctly.
    use arrow_schema::DataType::Timestamp;
    let test_data = arrow::util::test_util::parquet_test_data();
    let path = format!("{test_data}/int96_from_spark.parquet");
    let file = File::open(path).unwrap();

    let supplied_schema = Arc::new(Schema::new(vec![Field::new(
        "a",
        Timestamp(TimeUnit::Microsecond, None),
        true,
    )]));
    let options = ArrowReaderOptions::new().with_schema(supplied_schema.clone());

    let mut record_reader = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options)
        .unwrap()
        .build()
        .unwrap();

    let batch = record_reader.next().unwrap().unwrap();
    assert_eq!(batch.num_columns(), 1);
    let column = batch.column(0);
    assert_eq!(column.data_type(), &Timestamp(TimeUnit::Microsecond, None));

    let expected = Arc::new(Int64Array::from(vec![
        Some(1704141296123456),
        Some(1704070800000000),
        Some(253402225200000000),
        Some(1735599600000000),
        None,
        Some(9089380393200000000),
    ]));

    // arrow-rs relies on the chrono library to convert between timestamps and strings, so
    // instead compare as Int64. The underlying type should be a PrimitiveArray of Int64
    // anyway, so this should be a zero-copy non-modifying cast.

    let binding = arrow_cast::cast(batch.column(0), &arrow_schema::DataType::Int64).unwrap();
    let casted_timestamps = binding.as_primitive::<types::Int64Type>();

    assert_eq!(casted_timestamps.len(), expected.len());

    casted_timestamps
        .iter()
        .zip(expected.iter())
        .for_each(|(lhs, rhs)| {
            assert_eq!(lhs, rhs);
        });
}

#[test]
fn test_int96_from_spark_file_without_provided_schema() {
    // int96_from_spark.parquet was written based on Spark's microsecond timestamps which trade
    // range for resolution compared to a nanosecond timestamp. Without a provided schema, some
    // values when read as nanosecond resolution overflow and result in garbage values.
    use arrow_schema::DataType::Timestamp;
    let test_data = arrow::util::test_util::parquet_test_data();
    let path = format!("{test_data}/int96_from_spark.parquet");
    let file = File::open(path).unwrap();

    let mut record_reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();

    let batch = record_reader.next().unwrap().unwrap();
    assert_eq!(batch.num_columns(), 1);
    let column = batch.column(0);
    assert_eq!(column.data_type(), &Timestamp(TimeUnit::Nanosecond, None));

    let expected = Arc::new(Int64Array::from(vec![
        Some(1704141296123456000),  // Reads as nanosecond fine (note 3 extra 0s)
        Some(1704070800000000000),  // Reads as nanosecond fine (note 3 extra 0s)
        Some(-4852191831933722624), // Cannot be represented with nanos timestamp (year 9999)
        Some(1735599600000000000),  // Reads as nanosecond fine (note 3 extra 0s)
        None,
        Some(-4864435138808946688), // Cannot be represented with nanos timestamp (year 290000)
    ]));

    // arrow-rs relies on the chrono library to convert between timestamps and strings, so
    // instead compare as Int64. The underlying type should be a PrimitiveArray of Int64
    // anyway, so this should be a zero-copy non-modifying cast.

    let binding = arrow_cast::cast(batch.column(0), &arrow_schema::DataType::Int64).unwrap();
    let casted_timestamps = binding.as_primitive::<types::Int64Type>();

    assert_eq!(casted_timestamps.len(), expected.len());

    casted_timestamps
        .iter()
        .zip(expected.iter())
        .for_each(|(lhs, rhs)| {
            assert_eq!(lhs, rhs);
        });
}

#[test]
fn test_map_no_value() {
    // File schema:
    // message schema {
    //   required group my_map (MAP) {
    //     repeated group key_value {
    //       required int32 key;
    //       optional int32 value;
    //     }
    //   }
    //   required group my_map_no_v (MAP) {
    //     repeated group key_value {
    //       required int32 key;
    //     }
    //   }
    //   required group my_list (LIST) {
    //     repeated group list {
    //       required int32 element;
    //     }
    //   }
    // }
    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/map_no_value.parquet");
    let file = File::open(path).unwrap();

    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let out = reader.next().unwrap().unwrap();
    assert_eq!(out.num_rows(), 3);
    assert_eq!(out.num_columns(), 3);
    // my_map_no_v and my_list columns should now be equivalent
    let c0 = out.column(1).as_list::<i32>();
    let c1 = out.column(2).as_list::<i32>();
    assert_eq!(c0.len(), c1.len());
    c0.iter().zip(c1.iter()).for_each(|(l, r)| assert_eq!(l, r));
}

#[test]
fn test_read_unknown_logical_type() {
    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/unknown-logical-type.parquet");
    let test_file = File::open(path).unwrap();

    let builder =
        ParquetRecordBatchReaderBuilder::try_new(test_file).expect("Error creating reader builder");

    let schema = builder.metadata().file_metadata().schema_descr();
    assert_eq!(
        schema.column(0).logical_type_ref(),
        Some(&LogicalType::String)
    );
    assert_eq!(
        schema.column(1).logical_type_ref(),
        Some(&LogicalType::_Unknown { field_id: 2555 })
    );
    assert_eq!(schema.column(1).physical_type(), PhysicalType::BYTE_ARRAY);

    let mut reader = builder.build().unwrap();
    let out = reader.next().unwrap().unwrap();
    assert_eq!(out.num_rows(), 3);
    assert_eq!(out.num_columns(), 2);
}

#[test]
fn test_json_and_bson_logical_types() {
    let test_data = arrow::util::test_util::parquet_test_data();

    let json_file = File::open(format!("{test_data}/json.parquet")).unwrap();
    let mut json_reader = ParquetRecordBatchReaderBuilder::try_new(json_file)
        .unwrap()
        .build()
        .unwrap();
    let json_batch = json_reader.next().unwrap().unwrap();
    assert!(json_reader.next().is_none());
    let json = json_batch.column(0).as_string::<i32>();
    assert_eq!(
        json,
        &StringArray::from(vec![
            Some(r#"{"a":1}"#),
            Some(r#"{"a":1,"b":null}"#),
            Some("[1,null,3]"),
            None,
        ])
    );

    let bson_file = File::open(format!("{test_data}/bson.parquet")).unwrap();
    let mut bson_reader = ParquetRecordBatchReaderBuilder::try_new(bson_file)
        .unwrap()
        .build()
        .unwrap();
    let bson_batch = bson_reader.next().unwrap().unwrap();
    assert!(bson_reader.next().is_none());
    let bson = bson_batch.column(0).as_binary::<i32>();
    assert_eq!(
        bson,
        &BinaryArray::from(vec![
            Some(&[12, 0, 0, 0, 16, 97, 0, 1, 0, 0, 0, 0][..]),
            Some(&[15, 0, 0, 0, 16, 97, 0, 1, 0, 0, 0, 10, 98, 0, 0][..]),
            None,
        ])
    );
}

#[test]
fn test_read_decimal_file() {
    use arrow_array::Decimal128Array;
    let testdata = arrow::util::test_util::parquet_test_data();
    let file_variants = vec![
        ("byte_array", 4),
        ("fixed_length", 25),
        ("int32", 4),
        ("int64", 10),
    ];
    for (prefix, target_precision) in file_variants {
        let path = format!("{testdata}/{prefix}_decimal.parquet");
        let file = File::open(path).unwrap();
        let mut record_reader = ParquetRecordBatchReader::try_new(file, 32).unwrap();

        let batch = record_reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 24);
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();

        let expected = 1..25;

        assert_eq!(col.precision(), target_precision);
        assert_eq!(col.scale(), 2);

        for (i, v) in expected.enumerate() {
            assert_eq!(col.value(i), v * 100_i128);
        }
    }
}

#[test]
#[cfg_attr(miri, ignore)] // inline assembly is not supported
fn test_read_float16_nonzeros_file() {
    use arrow_array::Float16Array;
    let testdata = arrow::util::test_util::parquet_test_data();
    // see https://github.com/apache/parquet-testing/pull/40
    let path = format!("{testdata}/float16_nonzeros_and_nans.parquet");
    let file = File::open(path).unwrap();
    let mut record_reader = ParquetRecordBatchReader::try_new(file, 32).unwrap();

    let batch = record_reader.next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 8);
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float16Array>()
        .unwrap();

    let f16_two = f16::ONE + f16::ONE;

    assert_eq!(col.null_count(), 1);
    assert!(col.is_null(0));
    assert_eq!(col.value(1), f16::ONE);
    assert_eq!(col.value(2), -f16_two);
    assert!(col.value(3).is_nan());
    assert_eq!(col.value(4), f16::ZERO);
    assert!(col.value(4).is_sign_positive());
    assert_eq!(col.value(5), f16::NEG_ONE);
    assert_eq!(col.value(6), f16::NEG_ZERO);
    assert!(col.value(6).is_sign_negative());
    assert_eq!(col.value(7), f16_two);
}

#[test]
fn test_read_float16_zeros_file() {
    use arrow_array::Float16Array;
    let testdata = arrow::util::test_util::parquet_test_data();
    // see https://github.com/apache/parquet-testing/pull/40
    let path = format!("{testdata}/float16_zeros_and_nans.parquet");
    let file = File::open(path).unwrap();
    let mut record_reader = ParquetRecordBatchReader::try_new(file, 32).unwrap();

    let batch = record_reader.next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 3);
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float16Array>()
        .unwrap();

    assert_eq!(col.null_count(), 1);
    assert!(col.is_null(0));
    assert_eq!(col.value(1), f16::ZERO);
    assert!(col.value(1).is_sign_positive());
    assert!(col.value(2).is_nan());
}

#[test]
#[cfg(feature = "zstd")]
#[cfg_attr(miri, ignore)] // Zstd calls native C functions unsupported by Miri
fn test_read_float32_float64_byte_stream_split() {
    let path = format!(
        "{}/byte_stream_split.zstd.parquet",
        arrow::util::test_util::parquet_test_data(),
    );
    let file = File::open(path).unwrap();
    let record_reader = ParquetRecordBatchReader::try_new(file, 128).unwrap();

    let mut row_count = 0;
    for batch in record_reader {
        let batch = batch.unwrap();
        row_count += batch.num_rows();
        let f32_col = batch.column(0).as_primitive::<Float32Type>();
        let f64_col = batch.column(1).as_primitive::<Float64Type>();

        // This file contains floats from a standard normal distribution
        for &x in f32_col.values() {
            assert!(x > -10.0);
            assert!(x < 10.0);
        }
        for &x in f64_col.values() {
            assert!(x > -10.0);
            assert!(x < 10.0);
        }
    }
    assert_eq!(row_count, 300);
}

#[test]
#[cfg(feature = "flate2")]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_read_extended_byte_stream_split() {
    let path = format!(
        "{}/byte_stream_split_extended.gzip.parquet",
        arrow::util::test_util::parquet_test_data(),
    );
    let file = File::open(path).unwrap();
    let record_reader = ParquetRecordBatchReader::try_new(file, 128).unwrap();

    let mut row_count = 0;
    for batch in record_reader {
        let batch = batch.unwrap();
        row_count += batch.num_rows();

        // 0,1 are f16
        let f16_col = batch.column(0).as_primitive::<Float16Type>();
        let f16_bss = batch.column(1).as_primitive::<Float16Type>();
        assert_eq!(f16_col.len(), f16_bss.len());
        f16_col
            .iter()
            .zip(f16_bss.iter())
            .for_each(|(l, r)| assert_eq!(l.unwrap(), r.unwrap()));

        // 2,3 are f32
        let f32_col = batch.column(2).as_primitive::<Float32Type>();
        let f32_bss = batch.column(3).as_primitive::<Float32Type>();
        assert_eq!(f32_col.len(), f32_bss.len());
        f32_col
            .iter()
            .zip(f32_bss.iter())
            .for_each(|(l, r)| assert_eq!(l.unwrap(), r.unwrap()));

        // 4,5 are f64
        let f64_col = batch.column(4).as_primitive::<Float64Type>();
        let f64_bss = batch.column(5).as_primitive::<Float64Type>();
        assert_eq!(f64_col.len(), f64_bss.len());
        f64_col
            .iter()
            .zip(f64_bss.iter())
            .for_each(|(l, r)| assert_eq!(l.unwrap(), r.unwrap()));

        // 6,7 are i32
        let i32_col = batch.column(6).as_primitive::<types::Int32Type>();
        let i32_bss = batch.column(7).as_primitive::<types::Int32Type>();
        assert_eq!(i32_col.len(), i32_bss.len());
        i32_col
            .iter()
            .zip(i32_bss.iter())
            .for_each(|(l, r)| assert_eq!(l.unwrap(), r.unwrap()));

        // 8,9 are i64
        let i64_col = batch.column(8).as_primitive::<types::Int64Type>();
        let i64_bss = batch.column(9).as_primitive::<types::Int64Type>();
        assert_eq!(i64_col.len(), i64_bss.len());
        i64_col
            .iter()
            .zip(i64_bss.iter())
            .for_each(|(l, r)| assert_eq!(l.unwrap(), r.unwrap()));

        // 10,11 are FLBA(5)
        let flba_col = batch.column(10).as_fixed_size_binary();
        let flba_bss = batch.column(11).as_fixed_size_binary();
        assert_eq!(flba_col.len(), flba_bss.len());
        flba_col
            .iter()
            .zip(flba_bss.iter())
            .for_each(|(l, r)| assert_eq!(l.unwrap(), r.unwrap()));

        // 12,13 are FLBA(4) (decimal(7,3))
        let dec_col = batch.column(12).as_primitive::<Decimal128Type>();
        let dec_bss = batch.column(13).as_primitive::<Decimal128Type>();
        assert_eq!(dec_col.len(), dec_bss.len());
        dec_col
            .iter()
            .zip(dec_bss.iter())
            .for_each(|(l, r)| assert_eq!(l.unwrap(), r.unwrap()));
    }
    assert_eq!(row_count, 200);
}

#[test]
#[cfg(feature = "flate2")]
fn test_read_incorrect_map_schema_file() {
    let testdata = arrow::util::test_util::parquet_test_data();
    // see https://github.com/apache/parquet-testing/pull/47
    let path = format!("{testdata}/incorrect_map_schema.parquet");
    let file = File::open(path).unwrap();
    let mut record_reader = ParquetRecordBatchReader::try_new(file, 32).unwrap();

    let batch = record_reader.next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);

    let expected_schema = Schema::new(vec![Field::new(
        "my_map",
        ArrowDataType::Map(
            Arc::new(Field::new(
                "key_value",
                ArrowDataType::Struct(Fields::from(vec![
                    Field::new("key", ArrowDataType::Utf8, false),
                    Field::new("value", ArrowDataType::Utf8, true),
                ])),
                false,
            )),
            false,
        ),
        true,
    )]);
    assert_eq!(batch.schema().as_ref(), &expected_schema);

    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.column(0).null_count(), 0);
    assert_eq!(
        batch.column(0).as_map().keys().as_ref(),
        &StringArray::from(vec!["parent", "name"])
    );
    assert_eq!(
        batch.column(0).as_map().values().as_ref(),
        &StringArray::from(vec!["another", "report"])
    );
}

#[test]
#[cfg(feature = "snap")]
fn test_read_maps() {
    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/nested_maps.snappy.parquet");
    let file = File::open(path).unwrap();
    let record_batch_reader = ParquetRecordBatchReader::try_new(file, 60).unwrap();

    for batch in record_batch_reader {
        batch.unwrap();
    }
}

#[test]
fn test_read_null_list() {
    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/null_list.parquet");
    let file = File::open(path).unwrap();
    let mut record_batch_reader = ParquetRecordBatchReader::try_new(file, 60).unwrap();

    let batch = record_batch_reader.next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch.column(0).len(), 1);

    let list = batch
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(list.len(), 1);
    assert!(list.is_valid(0));

    let val = list.value(0);
    assert_eq!(val.len(), 0);
}

#[test]
#[cfg(feature = "lz4")]
fn test_read_lz4_raw() {
    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/lz4_raw_compressed.parquet");
    let file = File::open(path).unwrap();

    let batches = ParquetRecordBatchReader::try_new(file, 1024)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];

    assert_eq!(batch.num_columns(), 3);
    assert_eq!(batch.num_rows(), 4);

    // https://github.com/apache/parquet-testing/pull/18
    let a: &Int64Array = batch.column(0).as_any().downcast_ref().unwrap();
    assert_eq!(
        a.values(),
        &[1593604800, 1593604800, 1593604801, 1593604801]
    );

    let a: &BinaryArray = batch.column(1).as_any().downcast_ref().unwrap();
    let a: Vec<_> = a.iter().flatten().collect();
    assert_eq!(a, &[b"abc", b"def", b"abc", b"def"]);

    let a: &Float64Array = batch.column(2).as_any().downcast_ref().unwrap();
    assert_eq!(a.values(), &[42.000000, 7.700000, 42.125000, 7.700000]);
}

// This test is to ensure backward compatibility, it test 2 files containing the LZ4 CompressionCodec
// but different algorithms: LZ4_HADOOP and LZ4_RAW.
// 1. hadoop_lz4_compressed.parquet -> It is a file with LZ4 CompressionCodec which uses
//    LZ4_HADOOP algorithm for compression.
// 2. non_hadoop_lz4_compressed.parquet -> It is a file with LZ4 CompressionCodec which uses
//    LZ4_RAW algorithm for compression. This fallback is done to keep backward compatibility with
//    older parquet-cpp versions.
//
// For more information, check: https://github.com/apache/arrow-rs/issues/2988
#[test]
#[cfg(feature = "lz4")]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_read_lz4_hadoop_fallback() {
    for file in [
        "hadoop_lz4_compressed.parquet",
        "non_hadoop_lz4_compressed.parquet",
    ] {
        let testdata = arrow::util::test_util::parquet_test_data();
        let path = format!("{testdata}/{file}");
        let file = File::open(path).unwrap();
        let expected_rows = 4;

        let batches = ParquetRecordBatchReader::try_new(file, expected_rows)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];

        assert_eq!(batch.num_columns(), 3);
        assert_eq!(batch.num_rows(), expected_rows);

        let a: &Int64Array = batch.column(0).as_any().downcast_ref().unwrap();
        assert_eq!(
            a.values(),
            &[1593604800, 1593604800, 1593604801, 1593604801]
        );

        let b: &BinaryArray = batch.column(1).as_any().downcast_ref().unwrap();
        let b: Vec<_> = b.iter().flatten().collect();
        assert_eq!(b, &[b"abc", b"def", b"abc", b"def"]);

        let c: &Float64Array = batch.column(2).as_any().downcast_ref().unwrap();
        assert_eq!(c.values(), &[42.0, 7.7, 42.125, 7.7]);
    }
}

#[test]
#[cfg(feature = "lz4")]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_read_lz4_hadoop_large() {
    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/hadoop_lz4_compressed_larger.parquet");
    let file = File::open(path).unwrap();
    let expected_rows = 10000;

    let batches = ParquetRecordBatchReader::try_new(file, expected_rows)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];

    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch.num_rows(), expected_rows);

    let a: &StringArray = batch.column(0).as_any().downcast_ref().unwrap();
    let a: Vec<_> = a.iter().flatten().collect();
    assert_eq!(a[0], "c7ce6bef-d5b0-4863-b199-8ea8c7fb117b");
    assert_eq!(a[1], "e8fb9197-cb9f-4118-b67f-fbfa65f61843");
    assert_eq!(a[expected_rows - 2], "ab52a0cc-c6bb-4d61-8a8f-166dc4b8b13c");
    assert_eq!(a[expected_rows - 1], "85440778-460a-41ac-aa2e-ac3ee41696bf");
}

#[test]
fn test_read_old_nested_list() {
    use arrow::datatypes::DataType;
    use arrow::datatypes::ToByteSlice;

    let testdata = arrow::util::test_util::parquet_test_data();
    // message my_record {
    //     REQUIRED group a (LIST) {
    //         REPEATED group array (LIST) {
    //             REPEATED INT32 array;
    //         }
    //     }
    // }
    // should be read as list<list<int32>>
    let path = format!("{testdata}/old_list_structure.parquet");
    let test_file = File::open(path).unwrap();

    // create expected ListArray
    let a_values = Int32Array::from(vec![1, 2, 3, 4]);

    // Construct a buffer for value offsets, for the nested array: [[1, 2], [3, 4]]
    let a_value_offsets = arrow::buffer::Buffer::from([0, 2, 4].to_byte_slice());

    // Construct a list array from the above two
    let a_list_data = ArrayData::builder(DataType::List(Arc::new(Field::new(
        "array",
        DataType::Int32,
        false,
    ))))
    .len(2)
    .add_buffer(a_value_offsets)
    .add_child_data(a_values.into_data())
    .build()
    .unwrap();
    let a = ListArray::from(a_list_data);

    let builder = ParquetRecordBatchReaderBuilder::try_new(test_file).unwrap();
    let mut reader = builder.build().unwrap();
    let out = reader.next().unwrap().unwrap();
    assert_eq!(out.num_rows(), 1);
    assert_eq!(out.num_columns(), 1);
    // grab first column
    let c0 = out.column(0);
    let c0arr = c0.as_any().downcast_ref::<ListArray>().unwrap();
    // get first row: [[1, 2], [3, 4]]
    let r0 = c0arr.value(0);
    let r0arr = r0.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(r0arr, &a);
}
