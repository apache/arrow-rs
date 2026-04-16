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

//! Benchmarks for writer per-column overhead at high column cardinality.
//!
//! These benchmarks measure the structural cost of creating, writing, and
//! closing a parquet file with many columns while keeping actual data
//! encoding negligible (1 row per column). This isolates overhead such as
//! `WriterProperties` per-column lookups, `GenericColumnWriter` allocation,
//! and metadata assembly.

use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;
use std::io::Empty;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow_array::{Float32Array, RecordBatch};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;

const COLUMN_COUNTS: &[usize] = &[1_000, 5_000, 10_000];

fn make_wide_schema(num_columns: usize) -> SchemaRef {
    let fields: Vec<Field> = (0..num_columns)
        .map(|i| Field::new(format!("c{i}"), DataType::Float32, false))
        .collect();
    Arc::new(Schema::new(fields))
}

fn make_single_row_batch(schema: &SchemaRef) -> RecordBatch {
    let columns: Vec<Arc<dyn arrow_array::Array>> = (0..schema.fields().len())
        .map(|_| Arc::new(Float32Array::from(vec![0.0f32])) as _)
        .collect();
    RecordBatch::try_new(schema.clone(), columns).unwrap()
}

/// Build WriterProperties with a per-column property set for every column,
/// populating the internal HashMap so that per-column lookups are exercised.
fn make_per_column_props(schema: &SchemaRef) -> WriterProperties {
    let mut builder = WriterProperties::builder().set_dictionary_enabled(false);
    for field in schema.fields() {
        builder = builder.set_column_compression(
            ColumnPath::from(field.name().as_str()),
            Compression::UNCOMPRESSED,
        );
    }
    builder.build()
}

fn bench_writer_overhead(c: &mut Criterion) {
    for &num_cols in COLUMN_COUNTS {
        let schema = make_wide_schema(num_cols);
        let batch = make_single_row_batch(&schema);
        let props = make_per_column_props(&schema);

        c.bench_function(&format!("writer_overhead/{num_cols}_cols"), |b| {
            b.iter(|| {
                let mut writer =
                    ArrowWriter::try_new(Empty::default(), schema.clone(), Some(props.clone()))
                        .unwrap();
                writer.write(black_box(&batch)).unwrap();
                black_box(writer.close()).unwrap();
            });
        });
    }
}

criterion_group!(benches, bench_writer_overhead);
criterion_main!(benches);
