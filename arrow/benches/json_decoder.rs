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

//! Benchmarks Serde-to-Arrow decoding with numeric-to-string coercion.

use arrow::datatypes::*;
use arrow::util::bench_util::create_primitive_array;
use arrow_json::ReaderBuilder;
use criterion::{Criterion, criterion_group, criterion_main};
use serde::Serialize;
use std::sync::Arc;

#[global_allocator]
static BENCH_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

const NUM_ROWS: usize = 65536;

fn do_number_to_string_bench<S: Serialize>(
    name: &str,
    c: &mut Criterion,
    schema: Arc<Schema>,
    rows: Vec<S>,
) {
    c.bench_function(name, |b| {
        b.iter(|| {
            let mut decoder = ReaderBuilder::new(schema.clone())
                .with_coerce_primitive(true) // important for coercion
                .build_decoder()
                .expect("Failed to build decoder");

            decoder.serialize(&rows).expect("Failed to serialize rows");

            decoder
                .flush()
                .expect("Failed to flush")
                .expect("No RecordBatch produced")
        })
    });
}

fn bench_i64_to_string(c: &mut Criterion) {
    #[derive(Serialize)]
    struct TestRow {
        val: i64,
    }

    let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Utf8, false)]));

    let a_bunch_of_numbers = create_primitive_array::<Int64Type>(NUM_ROWS, 0.0);

    let rows: Vec<TestRow> = (0..NUM_ROWS)
        .map(|i| TestRow {
            val: a_bunch_of_numbers.value(i),
        })
        .collect();

    do_number_to_string_bench("i64_to_string", c, schema, rows)
}

fn bench_i32_to_string(c: &mut Criterion) {
    #[derive(Serialize)]
    struct TestRow {
        val: i32,
    }

    let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Utf8, false)]));

    let a_bunch_of_numbers = create_primitive_array::<Int32Type>(NUM_ROWS, 0.0);

    let rows: Vec<TestRow> = (0..NUM_ROWS)
        .map(|i| TestRow {
            val: a_bunch_of_numbers.value(i),
        })
        .collect();

    do_number_to_string_bench("i32_to_string", c, schema, rows)
}

fn bench_f32_to_string(c: &mut Criterion) {
    #[derive(Serialize)]
    struct TestRow {
        val: f32,
    }

    let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Utf8, false)]));

    let a_bunch_of_numbers = create_primitive_array::<Float32Type>(NUM_ROWS, 0.0);

    let rows: Vec<TestRow> = (0..NUM_ROWS)
        .map(|i| TestRow {
            val: a_bunch_of_numbers.value(i),
        })
        .collect();

    do_number_to_string_bench("f32_to_string", c, schema, rows)
}

fn bench_f64_to_string(c: &mut Criterion) {
    #[derive(Serialize)]
    struct TestRow {
        val: f64,
    }

    let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Utf8, false)]));

    let a_bunch_of_numbers = create_primitive_array::<Float64Type>(NUM_ROWS, 0.0);

    let rows: Vec<TestRow> = (0..NUM_ROWS)
        .map(|i| TestRow {
            val: a_bunch_of_numbers.value(i),
        })
        .collect();

    do_number_to_string_bench("f64_to_string", c, schema, rows)
}

fn bench_mixed_numbers_to_string(c: &mut Criterion) {
    #[derive(Serialize)]
    struct TestRow {
        val1: f64,
        val2: f32,
        val3: i64,
        val4: i32,
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("val1", DataType::Utf8, false),
        Field::new("val2", DataType::Utf8, false),
        Field::new("val3", DataType::Utf8, false),
        Field::new("val4", DataType::Utf8, false),
    ]));

    let f64_array = create_primitive_array::<Float64Type>(NUM_ROWS, 0.0);
    let f32_array = create_primitive_array::<Float32Type>(NUM_ROWS, 0.0);
    let i64_array = create_primitive_array::<Int64Type>(NUM_ROWS, 0.0);
    let i32_array = create_primitive_array::<Int32Type>(NUM_ROWS, 0.0);

    let rows: Vec<TestRow> = (0..NUM_ROWS)
        .map(|i| TestRow {
            val1: f64_array.value(i),
            val2: f32_array.value(i),
            val3: i64_array.value(i),
            val4: i32_array.value(i),
        })
        .collect();

    do_number_to_string_bench("mixed_numbers_to_string", c, schema, rows)
}

criterion_group!(
    benches,
    bench_f64_to_string,
    bench_f32_to_string,
    bench_i64_to_string,
    bench_i32_to_string,
    bench_mixed_numbers_to_string
);
criterion_main!(benches);
