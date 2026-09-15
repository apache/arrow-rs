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

//! Compatibility with fixed Parquet files, including historical encodings and nested layouts.

use super::*;

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
