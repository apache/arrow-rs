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

#[macro_use]
extern crate criterion;

use criterion::{Criterion, Throughput};
use std::env;
use std::fs::File;

extern crate arrow;
extern crate parquet;

use std::sync::Arc;

use arrow::datatypes::*;
use arrow::{record_batch::RecordBatch, util::data_gen::*};
use parquet::file::properties::WriterProperties;
use parquet::{arrow::ArrowWriter, errors::Result};

fn create_primitive_bench_batch(
    size: usize,
    null_density: f32,
    true_density: f32,
) -> Result<RecordBatch> {
    let fields = vec![
        Field::new("_1", DataType::Int32, true),
        Field::new("_2", DataType::Int64, true),
        Field::new("_3", DataType::UInt32, true),
        Field::new("_4", DataType::UInt64, true),
        Field::new("_5", DataType::Float32, true),
        Field::new("_6", DataType::Float64, true),
        Field::new("_7", DataType::Date64, true),
    ];
    let schema = Schema::new(fields);
    Ok(create_random_batch(
        Arc::new(schema),
        size,
        null_density,
        true_density,
    )?)
}

fn create_primitive_bench_batch_non_null(
    size: usize,
    null_density: f32,
    true_density: f32,
) -> Result<RecordBatch> {
    let fields = vec![
        Field::new("_1", DataType::Int32, false),
        Field::new("_2", DataType::Int64, false),
        Field::new("_3", DataType::UInt32, false),
        Field::new("_4", DataType::UInt64, false),
        Field::new("_5", DataType::Float32, false),
        Field::new("_6", DataType::Float64, false),
        Field::new("_7", DataType::Date64, false),
    ];
    let schema = Schema::new(fields);
    Ok(create_random_batch(
        Arc::new(schema),
        size,
        null_density,
        true_density,
    )?)
}

fn create_string_bench_batch(
    size: usize,
    null_density: f32,
    true_density: f32,
) -> Result<RecordBatch> {
    let fields = vec![
        Field::new("_1", DataType::Utf8, true),
        Field::new("_2", DataType::LargeUtf8, true),
    ];
    let schema = Schema::new(fields);
    Ok(create_random_batch(
        Arc::new(schema),
        size,
        null_density,
        true_density,
    )?)
}

fn create_string_and_binary_view_bench_batch(
    size: usize,
    null_density: f32,
    true_density: f32,
) -> Result<RecordBatch> {
    let fields = vec![
        Field::new("_1", DataType::Utf8View, true),
        Field::new("_2", DataType::BinaryView, true),
    ];
    let schema = Schema::new(fields);
    Ok(create_random_batch(
        Arc::new(schema),
        size,
        null_density,
        true_density,
    )?)
}

fn create_string_dictionary_bench_batch(
    size: usize,
    null_density: f32,
    true_density: f32,
) -> Result<RecordBatch> {
    let fields = vec![Field::new(
        "_1",
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        true,
    )];
    let schema = Schema::new(fields);
    Ok(create_random_batch(
        Arc::new(schema),
        size,
        null_density,
        true_density,
    )?)
}

fn create_string_bench_batch_non_null(
    size: usize,
    null_density: f32,
    true_density: f32,
) -> Result<RecordBatch> {
    let fields = vec![
        Field::new("_1", DataType::Utf8, false),
        Field::new("_2", DataType::LargeUtf8, false),
    ];
    let schema = Schema::new(fields);
    Ok(create_random_batch(
        Arc::new(schema),
        size,
        null_density,
        true_density,
    )?)
}

fn create_bool_bench_batch(
    size: usize,
    null_density: f32,
    true_density: f32,
) -> Result<RecordBatch> {
    let fields = vec![Field::new("_1", DataType::Boolean, true)];
    let schema = Schema::new(fields);
    Ok(create_random_batch(
        Arc::new(schema),
        size,
        null_density,
        true_density,
    )?)
}

fn create_bool_bench_batch_non_null(
    size: usize,
    null_density: f32,
    true_density: f32,
) -> Result<RecordBatch> {
    let fields = vec![Field::new("_1", DataType::Boolean, false)];
    let schema = Schema::new(fields);
    Ok(create_random_batch(
        Arc::new(schema),
        size,
        null_density,
        true_density,
    )?)
}

fn create_list_primitive_bench_batch(
    size: usize,
    null_density: f32,
    true_density: f32,
) -> Result<RecordBatch> {
    let fields = vec![
        Field::new(
            "_1",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        ),
        Field::new(
            "_2",
            DataType::List(Arc::new(Field::new("item", DataType::Boolean, true))),
            true,
        ),
        Field::new(
            "_3",
            DataType::LargeList(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
    ];
    let schema = Schema::new(fields);
    Ok(create_random_batch(
        Arc::new(schema),
        size,
        null_density,
        true_density,
    )?)
}

fn create_list_primitive_bench_batch_non_null(
    size: usize,
    null_density: f32,
    true_density: f32,
) -> Result<RecordBatch> {
    let fields = vec![
        Field::new(
            "_1",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
            false,
        ),
        Field::new(
            "_2",
            DataType::List(Arc::new(Field::new("item", DataType::Boolean, false))),
            false,
        ),
        Field::new(
            "_3",
            DataType::LargeList(Arc::new(Field::new("item", DataType::Utf8, false))),
            false,
        ),
    ];
    let schema = Schema::new(fields);
    Ok(create_random_batch(
        Arc::new(schema),
        size,
        null_density,
        true_density,
    )?)
}

fn _create_nested_bench_batch(
    size: usize,
    null_density: f32,
    true_density: f32,
) -> Result<RecordBatch> {
    let fields = vec![
        Field::new(
            "_1",
            DataType::Struct(Fields::from(vec![
                Field::new("_1", DataType::Int8, true),
                Field::new(
                    "_2",
                    DataType::Struct(Fields::from(vec![
                        Field::new("_1", DataType::Int8, true),
                        Field::new(
                            "_1",
                            DataType::Struct(Fields::from(vec![
                                Field::new("_1", DataType::Int8, true),
                                Field::new("_2", DataType::Utf8, true),
                            ])),
                            true,
                        ),
                        Field::new("_2", DataType::UInt8, true),
                    ])),
                    true,
                ),
            ])),
            true,
        ),
        Field::new(
            "_2",
            DataType::LargeList(Arc::new(Field::new(
                "item",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Struct(Fields::from(vec![
                        Field::new(
                            "_1",
                            DataType::Struct(Fields::from(vec![
                                Field::new("_1", DataType::Int8, true),
                                Field::new("_2", DataType::Int16, true),
                                Field::new("_3", DataType::Int32, true),
                            ])),
                            true,
                        ),
                        Field::new(
                            "_2",
                            DataType::List(Arc::new(Field::new(
                                "",
                                DataType::FixedSizeBinary(2),
                                true,
                            ))),
                            true,
                        ),
                    ])),
                    true,
                ))),
                true,
            ))),
            true,
        ),
    ];
    let schema = Schema::new(fields);
    Ok(create_random_batch(
        Arc::new(schema),
        size,
        null_density,
        true_density,
    )?)
}

#[inline]
fn write_batch(batch: &RecordBatch) -> Result<()> {
    write_batch_with_option(batch, None)
}

#[inline]
fn write_batch_enable_bloom_filter(batch: &RecordBatch) -> Result<()> {
    let option = WriterProperties::builder()
        .set_bloom_filter_enabled(true)
        .build();

    write_batch_with_option(batch, Some(option))
}

#[inline]
fn write_batch_with_option(batch: &RecordBatch, props: Option<WriterProperties>) -> Result<()> {
    let path = env::temp_dir().join("arrow_writer.temp");
    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), props)?;

    writer.write(batch)?;
    writer.close()?;
    Ok(())
}

fn bench_primitive_writer(c: &mut Criterion) {
    let batch = create_primitive_bench_batch(4096, 0.25, 0.75).unwrap();
    let mut group = c.benchmark_group("write_batch primitive");
    group.throughput(Throughput::Bytes(
        batch
            .columns()
            .iter()
            .map(|f| f.get_array_memory_size() as u64)
            .sum(),
    ));
    group.bench_function("4096 values primitive", |b| {
        b.iter(|| write_batch(&batch).unwrap())
    });

    group.bench_function("4096 values primitive with bloom filter", |b| {
        b.iter(|| write_batch_enable_bloom_filter(&batch).unwrap())
    });

    let batch = create_primitive_bench_batch_non_null(4096, 0.25, 0.75).unwrap();
    group.throughput(Throughput::Bytes(
        batch
            .columns()
            .iter()
            .map(|f| f.get_array_memory_size() as u64)
            .sum(),
    ));
    group.bench_function("4096 values primitive non-null", |b| {
        b.iter(|| write_batch(&batch).unwrap())
    });

    group.bench_function("4096 values primitive non-null with bloom filter", |b| {
        b.iter(|| write_batch_enable_bloom_filter(&batch).unwrap())
    });

    let batch = create_bool_bench_batch(4096, 0.25, 0.75).unwrap();
    group.throughput(Throughput::Bytes(
        batch
            .columns()
            .iter()
            .map(|f| f.get_array_memory_size() as u64)
            .sum(),
    ));
    group.bench_function("4096 values bool", |b| {
        b.iter(|| write_batch(&batch).unwrap())
    });

    let batch = create_bool_bench_batch_non_null(4096, 0.25, 0.75).unwrap();
    group.throughput(Throughput::Bytes(
        batch
            .columns()
            .iter()
            .map(|f| f.get_array_memory_size() as u64)
            .sum(),
    ));
    group.bench_function("4096 values bool non-null", |b| {
        b.iter(|| write_batch(&batch).unwrap())
    });

    let batch = create_string_bench_batch(4096, 0.25, 0.75).unwrap();
    group.throughput(Throughput::Bytes(
        batch
            .columns()
            .iter()
            .map(|f| f.get_array_memory_size() as u64)
            .sum(),
    ));
    group.bench_function("4096 values string", |b| {
        b.iter(|| write_batch(&batch).unwrap())
    });

    group.bench_function("4096 values string with bloom filter", |b| {
        b.iter(|| write_batch_enable_bloom_filter(&batch).unwrap())
    });

    let batch = create_string_and_binary_view_bench_batch(4096, 0.25, 0.75).unwrap();
    group.throughput(Throughput::Bytes(
        batch
            .columns()
            .iter()
            .map(|f| f.get_array_memory_size() as u64)
            .sum(),
    ));
    group.bench_function("4096 values string", |b| {
        b.iter(|| write_batch(&batch).unwrap())
    });

    group.bench_function("4096 values string with bloom filter", |b| {
        b.iter(|| write_batch_enable_bloom_filter(&batch).unwrap())
    });

    let batch = create_string_dictionary_bench_batch(4096, 0.25, 0.75).unwrap();
    group.throughput(Throughput::Bytes(
        batch
            .columns()
            .iter()
            .map(|f| f.get_array_memory_size() as u64)
            .sum(),
    ));
    group.bench_function("4096 values string dictionary", |b| {
        b.iter(|| write_batch(&batch).unwrap())
    });

    group.bench_function("4096 values string dictionary with bloom filter", |b| {
        b.iter(|| write_batch_enable_bloom_filter(&batch).unwrap())
    });

    let batch = create_string_bench_batch_non_null(4096, 0.25, 0.75).unwrap();
    group.throughput(Throughput::Bytes(
        batch
            .columns()
            .iter()
            .map(|f| f.get_array_memory_size() as u64)
            .sum(),
    ));
    group.bench_function("4096 values string non-null", |b| {
        b.iter(|| write_batch(&batch).unwrap())
    });

    group.bench_function("4096 values string non-null with bloom filter", |b| {
        b.iter(|| write_batch_enable_bloom_filter(&batch).unwrap())
    });

    group.finish();
}

// This bench triggers a write error, it is ignored for now
fn bench_nested_writer(c: &mut Criterion) {
    let batch = create_list_primitive_bench_batch(4096, 0.25, 0.75).unwrap();
    let mut group = c.benchmark_group("write_batch nested");
    group.throughput(Throughput::Bytes(
        batch
            .columns()
            .iter()
            .map(|f| f.get_array_memory_size() as u64)
            .sum(),
    ));
    group.bench_function("4096 values primitive list", |b| {
        b.iter(|| write_batch(&batch).unwrap())
    });

    let batch = create_list_primitive_bench_batch_non_null(4096, 0.25, 0.75).unwrap();
    group.throughput(Throughput::Bytes(
        batch
            .columns()
            .iter()
            .map(|f| f.get_array_memory_size() as u64)
            .sum(),
    ));
    group.bench_function("4096 values primitive list non-null", |b| {
        b.iter(|| write_batch(&batch).unwrap())
    });

    group.finish();
}

criterion_group!(benches, bench_primitive_writer, bench_nested_writer);
criterion_main!(benches);
