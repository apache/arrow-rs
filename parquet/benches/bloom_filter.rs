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

use arrow_array::{ArrayRef, Decimal128Array, Int32Array, Int8Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use criterion::*;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::arrow::bloom_filter::ArrowSbbf;
use parquet::bloom_filter::Sbbf;
use parquet::file::properties::{ReaderProperties, WriterProperties};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::serialized_reader::ReadOptionsBuilder;
use std::hint;
use std::sync::Arc;
use tempfile::tempfile;

/// Helper function to create an Sbbf from an array for benchmarking
///
/// Writes the given array to a Parquet file with bloom filters enabled,
/// then reads it back and returns the bloom filter for the first column.
fn setup_sbbf(array: ArrayRef, field: Field) -> Sbbf {
    let schema = Arc::new(Schema::new(vec![field.clone()]));
    let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();

    // Write to parquet with bloom filter
    let mut file = tempfile().unwrap();
    let props = WriterProperties::builder()
        .set_bloom_filter_enabled(true)
        .build();
    let mut writer = ArrowWriter::try_new(&mut file, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    // Read bloom filter back
    let options = ReadOptionsBuilder::new()
        .with_reader_properties(
            ReaderProperties::builder()
                .set_read_bloom_filter(true)
                .build(),
        )
        .build();
    let reader = SerializedFileReader::new_with_options(file, options).unwrap();
    let row_group_reader = reader.get_row_group(0).unwrap();

    row_group_reader
        .get_column_bloom_filter(0)
        .expect("Bloom filter should exist")
        .clone()
}

fn bench_integer_types(c: &mut Criterion) {
    // Setup for Int8 benchmarks
    let test_val_i8 = 42i8;
    let int8_array = Arc::new(Int8Array::from(vec![test_val_i8; 1000])) as ArrayRef;
    let int8_field = Field::new("col", DataType::Int8, false);
    let sbbf_int8 = setup_sbbf(int8_array, int8_field);
    let arrow_sbbf_int8 = ArrowSbbf::new(&sbbf_int8, &DataType::Int8);

    // Setup for Int32 benchmarks
    let test_val_i32 = 42i32;
    let int32_array = Arc::new(Int32Array::from(vec![test_val_i32; 1000])) as ArrayRef;
    let int32_field = Field::new("col", DataType::Int32, false);
    let sbbf_int32 = setup_sbbf(int32_array, int32_field);
    let arrow_sbbf_int32 = ArrowSbbf::new(&sbbf_int32, &DataType::Int32);

    c.bench_function("Sbbf::check i8", |b| {
        b.iter(|| {
            let result = sbbf_int8.check(&test_val_i8);
            hint::black_box(result);
        })
    });

    c.bench_function("ArrowSbbf::check i8 (coerce to i32)", |b| {
        b.iter(|| {
            let result = arrow_sbbf_int8.check(&test_val_i8);
            hint::black_box(result);
        })
    });

    c.bench_function("Sbbf::check i32", |b| {
        b.iter(|| {
            let result = sbbf_int32.check(&test_val_i32);
            hint::black_box(result);
        })
    });

    c.bench_function("ArrowSbbf::check i32 (no coercion)", |b| {
        b.iter(|| {
            let result = arrow_sbbf_int32.check(&test_val_i32);
            hint::black_box(result);
        })
    });
}

fn bench_decimal_types(c: &mut Criterion) {
    // Setup for Decimal128 small precision
    let test_val_dec_small = 123456i128;
    let test_bytes_dec_small = test_val_dec_small.to_le_bytes();
    let dec_small_array = Arc::new(
        Decimal128Array::from(vec![test_val_dec_small; 1000])
            .with_precision_and_scale(5, 2)
            .unwrap(),
    ) as ArrayRef;
    let dec_small_field = Field::new("col", DataType::Decimal128(5, 2), false);
    let sbbf_dec_small = setup_sbbf(dec_small_array, dec_small_field);
    let arrow_sbbf_dec_small = ArrowSbbf::new(&sbbf_dec_small, &DataType::Decimal128(5, 2));

    // Setup for Decimal128 medium precision
    let test_val_dec_medium = 123456789012345i128;
    let test_bytes_dec_medium = test_val_dec_medium.to_le_bytes();
    let dec_medium_array = Arc::new(
        Decimal128Array::from(vec![test_val_dec_medium; 1000])
            .with_precision_and_scale(15, 2)
            .unwrap(),
    ) as ArrayRef;
    let dec_medium_field = Field::new("col", DataType::Decimal128(15, 2), false);
    let sbbf_dec_medium = setup_sbbf(dec_medium_array, dec_medium_field);
    let arrow_sbbf_dec_medium = ArrowSbbf::new(&sbbf_dec_medium, &DataType::Decimal128(15, 2));

    // Setup for Decimal128 large precision
    let test_val_dec_large = 123456789012345678901234567890i128;
    let test_bytes_dec_large = test_val_dec_large.to_le_bytes();
    let dec_large_array = Arc::new(
        Decimal128Array::from(vec![test_val_dec_large; 1000])
            .with_precision_and_scale(30, 2)
            .unwrap(),
    ) as ArrayRef;
    let dec_large_field = Field::new("col", DataType::Decimal128(30, 2), false);
    let sbbf_dec_large = setup_sbbf(dec_large_array, dec_large_field);
    let arrow_sbbf_dec_large = ArrowSbbf::new(&sbbf_dec_large, &DataType::Decimal128(30, 2));

    c.bench_function("Sbbf::check Decimal128(5,2)", |b| {
        b.iter(|| {
            let result = sbbf_dec_small.check(&test_bytes_dec_small[..]);
            hint::black_box(result);
        })
    });

    c.bench_function("ArrowSbbf::check Decimal128(5,2) (coerce to i32)", |b| {
        b.iter(|| {
            let result = arrow_sbbf_dec_small.check(&test_bytes_dec_small[..]);
            hint::black_box(result);
        })
    });

    c.bench_function("Sbbf::check Decimal128(15,2)", |b| {
        b.iter(|| {
            let result = sbbf_dec_medium.check(&test_bytes_dec_medium[..]);
            hint::black_box(result);
        })
    });

    c.bench_function("ArrowSbbf::check Decimal128(15,2) (coerce to i64)", |b| {
        b.iter(|| {
            let result = arrow_sbbf_dec_medium.check(&test_bytes_dec_medium[..]);
            hint::black_box(result);
        })
    });

    c.bench_function("Sbbf::check Decimal128(30,2)", |b| {
        b.iter(|| {
            let result = sbbf_dec_large.check(&test_bytes_dec_large[..]);
            hint::black_box(result);
        })
    });

    c.bench_function("ArrowSbbf::check Decimal128(30,2) (no coercion)", |b| {
        b.iter(|| {
            let result = arrow_sbbf_dec_large.check(&test_bytes_dec_large[..]);
            hint::black_box(result);
        })
    });
}

fn config() -> Criterion {
    Criterion::default().noise_threshold(0.05)
}

criterion_group! {
    name = benches_int;
    config = config();
    targets = bench_integer_types
}

criterion_group! {
    name = benches_decimal;
    config = config();
    targets = bench_decimal_types
}

criterion_main!(benches_int, benches_decimal);
