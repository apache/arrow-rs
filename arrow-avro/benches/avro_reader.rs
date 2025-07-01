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

//! Comprehensive benchmarks comparing StringArray vs StringViewArray performance
//!
//! This benchmark suite compares the performance characteristics of StringArray vs
//! StringViewArray across three key dimensions:
//! 1. Array creation performance
//! 2. String value access operations
//! 3. Avro file reading with each array type

use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema};
use arrow_array::{ArrayRef, Int32Array, StringArray, StringViewArray};
use arrow_schema::ArrowError;
use criterion::*;
use tempfile::NamedTempFile;

fn create_test_data(count: usize, str_length: usize) -> Vec<String> {
    (0..count)
        .map(|i| format!("str_{i}") + &"a".repeat(str_length))
        .collect()
}

fn create_avro_test_file(row_count: usize, str_length: usize) -> Result<NamedTempFile, ArrowError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("string_field", DataType::Utf8, false),
        Field::new("int_field", DataType::Int32, false),
    ]));

    let strings = create_test_data(row_count, str_length);
    let string_array = StringArray::from_iter(strings.iter().map(|s| Some(s.as_str())));
    let int_array = Int32Array::from_iter_values(0..row_count as i32);
    let _batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(string_array) as ArrayRef,
            Arc::new(int_array) as ArrayRef,
        ],
    )?;

    let temp_file = NamedTempFile::new()?;

    let mut file = temp_file.reopen()?;

    file.write_all(b"AVRO")?;

    for (i, string) in strings.iter().enumerate().take(row_count) {
        let s = string.as_bytes();
        let len = s.len() as u32;
        file.write_all(&len.to_le_bytes())?;
        file.write_all(s)?;
        file.write_all(&(i as i32).to_le_bytes())?;
    }

    file.flush()?;
    Ok(temp_file)
}

fn read_avro_test_file(
    file_path: &std::path::Path,
    use_utf8view: bool,
) -> Result<RecordBatch, ArrowError> {
    let file = File::open(file_path)?;
    let mut reader = BufReader::new(file);

    let mut header = [0u8; 4];
    reader.read_exact(&mut header)?;

    let mut strings = Vec::new();
    let mut ints = Vec::new();

    loop {
        let mut len_bytes = [0u8; 4];
        if reader.read_exact(&mut len_bytes).is_err() {
            break; // End of file
        }

        let len = u32::from_le_bytes(len_bytes) as usize;
        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf)?;

        let s = String::from_utf8(buf)
            .map_err(|e| ArrowError::ParseError(format!("Invalid UTF-8: {e}")))?;

        strings.push(s);

        let mut int_bytes = [0u8; 4];
        reader.read_exact(&mut int_bytes)?;
        ints.push(i32::from_le_bytes(int_bytes));
    }

    let string_array: ArrayRef = if use_utf8view {
        Arc::new(StringViewArray::from_iter(
            strings.iter().map(|s| Some(s.as_str())),
        ))
    } else {
        Arc::new(StringArray::from_iter(
            strings.iter().map(|s| Some(s.as_str())),
        ))
    };

    let int_array: ArrayRef = Arc::new(Int32Array::from(ints));

    let schema = Arc::new(Schema::new(vec![
        if use_utf8view {
            Field::new("string_field", DataType::Utf8View, false)
        } else {
            Field::new("string_field", DataType::Utf8, false)
        },
        Field::new("int_field", DataType::Int32, false),
    ]));

    RecordBatch::try_new(schema, vec![string_array, int_array])
}

fn bench_array_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_creation");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(5));

    for &str_length in &[10, 100, 1000] {
        let data = create_test_data(10000, str_length);
        let row_count = 1000;

        group.bench_function(format!("string_array_{str_length}_chars"), |b| {
            b.iter(|| {
                let string_array =
                    StringArray::from_iter(data[0..row_count].iter().map(|s| Some(s.as_str())));
                let int_array = Int32Array::from_iter_values(0..row_count as i32);

                let schema = Arc::new(Schema::new(vec![
                    Field::new("string_field", DataType::Utf8, false),
                    Field::new("int_field", DataType::Int32, false),
                ]));

                let batch = RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(string_array) as ArrayRef,
                        Arc::new(int_array) as ArrayRef,
                    ],
                )
                .unwrap();

                std::hint::black_box(batch)
            })
        });

        group.bench_function(format!("string_view_{str_length}_chars"), |b| {
            b.iter(|| {
                let string_array =
                    StringViewArray::from_iter(data[0..row_count].iter().map(|s| Some(s.as_str())));
                let int_array = Int32Array::from_iter_values(0..row_count as i32);

                let schema = Arc::new(Schema::new(vec![
                    Field::new("string_field", DataType::Utf8View, false),
                    Field::new("int_field", DataType::Int32, false),
                ]));

                let batch = RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(string_array) as ArrayRef,
                        Arc::new(int_array) as ArrayRef,
                    ],
                )
                .unwrap();

                std::hint::black_box(batch)
            })
        });
    }

    group.finish();
}

fn bench_string_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_operations");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(5));

    for &str_length in &[10, 100, 1000] {
        let data = create_test_data(10000, str_length);
        let rows = 1000;

        let string_array = StringArray::from_iter(data[0..rows].iter().map(|s| Some(s.as_str())));
        let string_view_array =
            StringViewArray::from_iter(data[0..rows].iter().map(|s| Some(s.as_str())));

        group.bench_function(format!("string_array_value_{str_length}_chars"), |b| {
            b.iter(|| {
                let mut sum_len = 0;
                for i in 0..rows {
                    sum_len += string_array.value(i).len();
                }
                std::hint::black_box(sum_len)
            })
        });

        group.bench_function(format!("string_view_value_{str_length}_chars"), |b| {
            b.iter(|| {
                let mut sum_len = 0;
                for i in 0..rows {
                    sum_len += string_view_array.value(i).len();
                }
                std::hint::black_box(sum_len)
            })
        });
    }

    group.finish();
}

fn bench_avro_reader(c: &mut Criterion) {
    let mut group = c.benchmark_group("avro_reader");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(5));

    for &str_length in &[10, 100, 1000] {
        let row_count = 1000;
        let temp_file = create_avro_test_file(row_count, str_length).unwrap();
        let file_path = temp_file.path();

        group.bench_function(format!("string_array_{str_length}_chars"), |b| {
            b.iter(|| {
                let batch = read_avro_test_file(file_path, false).unwrap();
                std::hint::black_box(batch)
            })
        });

        group.bench_function(format!("string_view_{str_length}_chars"), |b| {
            b.iter(|| {
                let batch = read_avro_test_file(file_path, true).unwrap();
                std::hint::black_box(batch)
            })
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_array_creation,
    bench_string_operations,
    bench_avro_reader
);
criterion_main!(benches);
