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

use std::sync::Arc;

use arrow_array::builder::{Int32Builder, ListBuilder, StringBuilder};
use arrow_array::RecordBatch;
use arrow_json::{LineDelimitedWriter, WriterBuilder};
use arrow_json::writer::LineDelimited;
use arrow_schema::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};
use rand::Rng;

#[derive(Debug, Clone, Copy)]
enum TestCaseNulls {
    None,
    All,
}

impl std::fmt::Display for TestCaseNulls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TestCaseNulls::None => write!(f, "none"),
            TestCaseNulls::All => write!(f, "all"),
        }
    }
}

impl TestCaseNulls {
    fn is_nullable(&self) -> bool {
        match self {
            TestCaseNulls::None => false,
            TestCaseNulls::All => true,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum TestCaseNumRows {
    Small,
    Large,
}

impl TestCaseNumRows {
    fn row_count(&self) -> usize {
        match self {
            TestCaseNumRows::Small => 10,
            TestCaseNumRows::Large => 100_000,
        }
    }
}

/// Represents a test case configuration for benchmarking
#[derive(Debug)]
struct TestCase {
    name: String,
    data: RecordBatch,
    explicit_nulls: bool,
    null_generation: TestCaseNulls,
    row_count: TestCaseNumRows,
}

impl TestCase {
    fn name(&self) -> String {
        format!(
            "{}_explicit_nulls:{}_nulls:{}_row_count:{}",
            self.name,
            self.explicit_nulls,
            self.null_generation,
            self.row_count.row_count(),
        )
    }
}

fn create_int32_array(
    nulls: TestCaseNulls,
    num_rows: TestCaseNumRows,
) -> RecordBatch {
    let mut builder = Int32Builder::new();
    for _ in 0..num_rows.row_count() {
        match nulls {
            TestCaseNulls::None => builder.append_value(rand::random::<i32>()),
            TestCaseNulls::All => builder.append_null(),
        }
    }
    let array = builder.finish();
    let schema = Arc::new(Schema::new(vec![Field::new("int", DataType::Int32, nulls.is_nullable())]));
    RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
}

fn create_string_array(
    nulls: TestCaseNulls,
    num_rows: TestCaseNumRows,
) -> RecordBatch {
    let mut rng = rand::thread_rng();
    let mut builder = StringBuilder::new();
    for _ in 0..num_rows.row_count() {
        match nulls {
            TestCaseNulls::None => builder.append_value(rng.gen_range(0..100).to_string()),
            TestCaseNulls::All => builder.append_null(),
        }
    }
    let array = builder.finish();
    let schema = Arc::new(Schema::new(vec![Field::new("string", DataType::Utf8, nulls.is_nullable())]));
    RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
}

fn crete_list_array(
    nulls: TestCaseNulls,
    num_rows: TestCaseNumRows,
) -> RecordBatch {
    // create a list aray with 1-100 elements per row
    let mut rng = rand::thread_rng();
    let mut builder= ListBuilder::new(Int32Builder::new());
    for _ in 0..num_rows.row_count() {
        let len = rng.gen_range(1..100);
        for _ in 0..len {
            match nulls {
                TestCaseNulls::None => builder.values().append_value(rng.gen_range(0..100)),
                TestCaseNulls::All => builder.values().append_null(),
            }
        }
        builder.append(true);
    }
    let array = builder.finish();
    let schema = Arc::new(Schema::new(vec![Field::new_list("list", Field::new("item", DataType::Int32, true), nulls.is_nullable())]));
    RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
}

//------------------------------------------------------------------------------
// Benchmark Definition
//------------------------------------------------------------------------------

fn bench_json_encoding(c: &mut Criterion) {
    // Define all test cases
    let mut cases = vec![];
    for nulls in [TestCaseNulls::None, TestCaseNulls::All] {
        for size in [TestCaseNumRows::Large, TestCaseNumRows::Small] {
            for explicit_nulls in [false, true] {
                cases.push(TestCase {
                    name: "int32".to_string(),
                    data: create_int32_array(nulls, size),
                    explicit_nulls,
                    null_generation: nulls,
                    row_count: size,
                });
                cases.push(TestCase {
                    name: "string".to_string(),
                    data: create_string_array(nulls, size),
                    explicit_nulls,
                    null_generation: nulls,
                    row_count: size,
                });
                cases.push(TestCase {
                    name: "list".to_string(),
                    data: crete_list_array(nulls, size),
                    explicit_nulls,
                    null_generation: nulls,
                    row_count: size,
                });
            }
        }
    }

    let mut group = c.benchmark_group("JSON Encoding");

    // Run benchmarks for each test case
    for test_case in cases {
        group.throughput(Throughput::Elements(test_case.row_count.row_count() as u64));
        group.bench_with_input(
            BenchmarkId::new(test_case.name(), test_case.explicit_nulls),
            &test_case.data,
            |b, batch| {
                // Setup: create a new sink and writer for each iteration
                // Only the write method call is measured
                b.iter_batched_ref(
                    || {
                        // Setup phase - create the writer with a new sink
                        let sink = std::io::sink();
                        
                        if test_case.explicit_nulls {
                            // Create writer with explicit nulls
                            let writer = WriterBuilder::new()
                                .with_explicit_nulls(true)
                                .build::<_, LineDelimited>(sink);
                            writer
                        } else {
                            // Create default writer
                            let writer = LineDelimitedWriter::new(sink);
                            writer
                        }
                    },
                    |writer| {
                        // Only this part is measured - the write operation
                        writer.write(batch)
                    },
                    criterion::BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_json_encoding);
criterion_main!(benches);