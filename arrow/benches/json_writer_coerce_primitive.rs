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

use criterion::*;

use arrow::datatypes::*;
use arrow::util::bench_util::create_primitive_array;
use arrow_json::ReaderBuilder;
use serde::Serialize;
use std::sync::Arc;

const NUM_ROWS: usize = 65536;

fn do_bench<S: Serialize>(name: &str, c: &mut Criterion, schema: Arc<Schema>, rows: Vec<S>) {
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
                .expect("No RecordBatch produced");
        })
    });
}

fn bench_i64(c: &mut Criterion) {
    #[derive(Serialize)]
    struct TestRow {
        val: i64,
    }

    // Create schema for string output
    let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Utf8, false)]));

    let i64_array = create_primitive_array::<Int64Type>(NUM_ROWS, 0.0);

    // Create test rows
    let rows: Vec<TestRow> = (0..NUM_ROWS)
        .map(|i| TestRow {
            val: i64_array.value(i),
        })
        .collect();

    do_bench("i64_rows", c, schema, rows)
}

fn bench_i32(c: &mut Criterion) {
    #[derive(Serialize)]
    struct TestRow {
        val: i32,
    }

    // Create schema for string output
    let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Utf8, false)]));

    let i32_array = create_primitive_array::<Int32Type>(NUM_ROWS, 0.0);

    // Create test rows
    let rows: Vec<TestRow> = (0..NUM_ROWS)
        .map(|i| TestRow {
            val: i32_array.value(i),
        })
        .collect();

    do_bench("i32_rows", c, schema, rows)
}

fn bench_f32(c: &mut Criterion) {
    #[derive(Serialize)]
    struct TestRow {
        val: f32,
    }

    // Create schema for string output
    let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Utf8, false)]));

    let f32_array = create_primitive_array::<Float32Type>(NUM_ROWS, 0.0);

    // Create test rows
    let rows: Vec<TestRow> = (0..NUM_ROWS)
        .map(|i| TestRow {
            val: f32_array.value(i),
        })
        .collect();

    do_bench("f32_rows", c, schema, rows)
}

fn bench_f64(c: &mut Criterion) {
    #[derive(Serialize)]
    struct TestRow {
        val: f64,
    }

    // Create schema for string output
    let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Utf8, false)]));

    let f64_array = create_primitive_array::<Float64Type>(NUM_ROWS, 0.0);

    // Create test rows
    let rows: Vec<TestRow> = (0..NUM_ROWS)
        .map(|i| TestRow {
            val: f64_array.value(i),
        })
        .collect();

    do_bench("f64_rows", c, schema, rows)
}

fn bench_mixed(c: &mut Criterion) {
    #[derive(Serialize)]
    struct TestRow {
        val1: f64,
        val2: f32,
        val3: i64,
        val4: i32,
    }

    // Create schema for string output
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

    // Create test rows
    let rows: Vec<TestRow> = (0..NUM_ROWS)
        .map(|i| TestRow {
            val1: f64_array.value(i),
            val2: f32_array.value(i),
            val3: i64_array.value(i),
            val4: i32_array.value(i),
        })
        .collect();

    do_bench("mixed_rows", c, schema, rows)
}

fn criterion_benchmark(c: &mut Criterion) {
    bench_f64(c);
    bench_f32(c);
    bench_i64(c);
    bench_i32(c);
    bench_mixed(c);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
