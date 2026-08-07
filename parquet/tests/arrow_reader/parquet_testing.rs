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

use arrow_array::cast::AsArray;
use arrow_array::{Int64Array, types};
use arrow_schema::{Field, Schema, TimeUnit};
use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use std::fs::File;
use std::sync::Arc;

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
