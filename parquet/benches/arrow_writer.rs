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

// Use jemalloc, with page decay disabled, for the writer benchmarks.
//
// Each criterion iteration builds a fresh `ArrowWriter`, so the writer's
// internal encode buffers are allocated and freed every iteration. Whether
// those buffers are served from warm (already-faulted) pages or fresh pages
// depends on the heap state left by previously-run benchmarks in the same
// process. The cold-page case pays a per-page minor fault on every byte
// written, which roughly doubles the measured time for the byte-array writers
// (e.g. `string/parquet_2` swings between ~106ms and ~190ms purely on
// allocation order, with no code change).
//
// The retention policy is pinned, not left to the allocator default, via the
// compiled-in `malloc_conf` symbol below: `dirty_decay_ms:-1,muzzy_decay_ms:-1`
// disables jemalloc's page decay, so freed pages are kept mapped and reused
// warm instead of being returned to the OS. That removes the per-iteration
// fault tax and collapses the order-dependent bimodality, so the numbers
// reflect encoder work. Pinning the setting (rather than relying on an
// allocator's default) keeps the benchmark stable across allocator upgrades.
//
// Gated to Linux (see Cargo.toml): jemalloc does not build on some targets and
// its unprefixed `malloc_conf` symbol is not honored on others; elsewhere the
// bench just uses the default allocator (and `assert_page_decay_disabled` is a
// no-op). Linux is where the canonical benchmark runner runs.
#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// jemalloc reads its options from a symbol named `malloc_conf`. The
// `unprefixed_malloc_on_supported_platforms` feature (see Cargo.toml) is what
// makes jemalloc look for the unprefixed name this defines; without it the
// symbol would be silently ignored. `assert_page_decay_disabled` below guards
// against exactly that, so a config that fails to apply fails loudly instead
// of quietly reintroducing the instability.
#[cfg(target_os = "linux")]
#[allow(non_upper_case_globals)]
#[unsafe(export_name = "malloc_conf")]
pub static malloc_conf: &[u8] = b"dirty_decay_ms:-1,muzzy_decay_ms:-1\0";

/// Assert the `malloc_conf` above actually took effect. If the symbol is ever
/// silently ignored (feature dropped, unsupported platform, renamed symbol,
/// allocator swapped), the byte-array writer benchmarks would quietly become
/// order-dependent again; failing loudly here prevents that.
#[cfg(target_os = "linux")]
fn assert_page_decay_disabled() {
    // SAFETY: reading immutable `opt.*` mallctl values; the type matches
    // jemalloc's `ssize_t`.
    let (dirty, muzzy): (isize, isize) = unsafe {
        (
            tikv_jemalloc_ctl::raw::read(b"opt.dirty_decay_ms\0").unwrap(),
            tikv_jemalloc_ctl::raw::read(b"opt.muzzy_decay_ms\0").unwrap(),
        )
    };
    assert!(
        dirty == -1 && muzzy == -1,
        "malloc_conf did not take effect (dirty_decay_ms={dirty}, muzzy_decay_ms={muzzy}, \
         expected -1/-1); the arrow_writer benchmark would be order-dependent. Ensure the \
         `unprefixed_malloc_on_supported_platforms` jemalloc feature is enabled.",
    );
}

/// On non-Linux targets the benchmark uses the default allocator, so there is
/// no jemalloc page-decay setting to check.
#[cfg(not(target_os = "linux"))]
fn assert_page_decay_disabled() {}

use criterion::{Bencher, Criterion, Throughput};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};

extern crate arrow;
extern crate parquet;

use std::hint::black_box;
use std::io::Empty;
use std::sync::Arc;

use arrow::datatypes::*;
use arrow::util::bench_util::{create_f16_array, create_f32_array, create_f64_array};
use arrow::{record_batch::RecordBatch, util::data_gen::*};
use arrow_array::RecordBatchOptions;
use parquet::errors::Result;
use parquet::file::properties::{CdcOptions, WriterProperties, WriterVersion};

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

fn create_float_bench_batch_with_nans(size: usize, nan_density: f32) -> Result<RecordBatch> {
    let fields = vec![
        Field::new("_1", DataType::Float16, false),
        Field::new("_2", DataType::Float32, false),
        Field::new("_3", DataType::Float64, false),
    ];
    let schema = Schema::new(fields);
    let columns: Vec<arrow_array::ArrayRef> = vec![
        Arc::new(create_f16_array(size, nan_density)),
        Arc::new(create_f32_array(size, nan_density)),
        Arc::new(create_f64_array(size, nan_density)),
    ];
    Ok(RecordBatch::try_new_with_options(
        Arc::new(schema),
        columns,
        &RecordBatchOptions::new().with_match_field_names(false),
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
            DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
            true,
        ),
        Field::new(
            "_2",
            DataType::List(Arc::new(Field::new_list_field(DataType::Boolean, true))),
            true,
        ),
        Field::new(
            "_3",
            DataType::LargeList(Arc::new(Field::new_list_field(DataType::Utf8, true))),
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
            DataType::List(Arc::new(Field::new_list_field(DataType::Int32, false))),
            false,
        ),
        Field::new(
            "_2",
            DataType::List(Arc::new(Field::new_list_field(DataType::Boolean, false))),
            false,
        ),
        Field::new(
            "_3",
            DataType::LargeList(Arc::new(Field::new_list_field(DataType::Utf8, false))),
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

fn create_struct_bench_batch(size: usize, null_density: f32) -> Result<RecordBatch> {
    let fields = vec![Field::new(
        "_1",
        DataType::Struct(Fields::from(vec![
            Field::new("_1", DataType::Int32, false),
            Field::new("_2", DataType::Int64, false),
            Field::new("_3", DataType::Float32, false),
        ])),
        true,
    )];
    let schema = Schema::new(fields);
    Ok(create_random_batch(
        Arc::new(schema),
        size,
        null_density,
        0.75,
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
            DataType::LargeList(Arc::new(Field::new_list_field(
                DataType::List(Arc::new(Field::new_list_field(
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

fn write_batch_with_option(
    bench: &mut Bencher,
    batch: &RecordBatch,
    props: Option<WriterProperties>,
) -> Result<()> {
    let props = props.unwrap_or_default();

    bench.iter(|| {
        let mut file = Empty::default();
        let mut writer =
            ArrowWriter::try_new(&mut file, batch.schema(), Some(props.clone())).unwrap();
        writer.write(black_box(batch)).unwrap();
        black_box(writer.close()).unwrap();
    });

    Ok(())
}

fn create_batches() -> Vec<(&'static str, RecordBatch)> {
    const BATCH_SIZE: usize = 1024 * 1024;

    let mut batches = vec![];

    let batch = create_primitive_bench_batch(BATCH_SIZE, 0.25, 0.75).unwrap();
    batches.push(("primitive", batch));

    let batch = create_primitive_bench_batch_non_null(BATCH_SIZE, 0.25, 0.75).unwrap();
    batches.push(("primitive_non_null", batch));

    let batch = create_bool_bench_batch(BATCH_SIZE, 0.25, 0.75).unwrap();
    batches.push(("bool", batch));

    let batch = create_bool_bench_batch_non_null(BATCH_SIZE, 0.25, 0.75).unwrap();
    batches.push(("bool_non_null", batch));

    let batch = create_string_bench_batch(BATCH_SIZE, 0.25, 0.75).unwrap();
    batches.push(("string", batch));

    let batch = create_string_and_binary_view_bench_batch(BATCH_SIZE, 0.25, 0.75).unwrap();
    batches.push(("string_and_binary_view", batch));

    let batch = create_string_dictionary_bench_batch(BATCH_SIZE, 0.25, 0.75).unwrap();
    batches.push(("string_dictionary", batch));

    let batch = create_string_bench_batch_non_null(BATCH_SIZE, 0.25, 0.75).unwrap();
    batches.push(("string_non_null", batch));

    let batch = create_float_bench_batch_with_nans(BATCH_SIZE, 0.5).unwrap();
    batches.push(("float_with_nans", batch));

    let batch = create_list_primitive_bench_batch(BATCH_SIZE, 0.25, 0.75).unwrap();
    batches.push(("list_primitive", batch));

    let batch = create_list_primitive_bench_batch_non_null(BATCH_SIZE, 0.25, 0.75).unwrap();
    batches.push(("list_primitive_non_null", batch));

    let batch = create_primitive_bench_batch(BATCH_SIZE, 0.99, 0.75).unwrap();
    batches.push(("primitive_sparse_99pct_null", batch));

    let batch = create_list_primitive_bench_batch(BATCH_SIZE, 0.99, 0.75).unwrap();
    batches.push(("list_primitive_sparse_99pct_null", batch));

    let batch = create_primitive_bench_batch(BATCH_SIZE, 1.0, 0.75).unwrap();
    batches.push(("primitive_all_null", batch));

    let batch = create_struct_bench_batch(BATCH_SIZE, 0.0).unwrap();
    batches.push(("struct_non_null", batch));

    let batch = create_struct_bench_batch(BATCH_SIZE, 0.99).unwrap();
    batches.push(("struct_sparse_99pct_null", batch));

    let batch = create_struct_bench_batch(BATCH_SIZE, 1.0).unwrap();
    batches.push(("struct_all_null", batch));

    batches
}

fn create_writer_props() -> Vec<(&'static str, WriterProperties)> {
    let mut props = vec![];

    props.push(("default", Default::default()));

    let prop = WriterProperties::builder()
        .set_bloom_filter_enabled(true)
        .build();
    props.push(("bloom_filter", prop));

    let prop = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .build();
    props.push(("parquet_2", prop));

    let prop = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    props.push(("zstd", prop));

    let prop = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .build();
    props.push(("zstd_parquet_2", prop));

    let prop = WriterProperties::builder()
        .set_content_defined_chunking(Some(CdcOptions::default()))
        .build();
    props.push(("cdc", prop));

    props
}

fn bench_all_writers(c: &mut Criterion) {
    assert_page_decay_disabled();
    let batches = create_batches();
    let props = create_writer_props();

    for (batch_name, batch) in &batches {
        let mut group = c.benchmark_group(*batch_name);
        group.throughput(Throughput::Bytes(
            batch
                .columns()
                .iter()
                .map(|f| f.get_array_memory_size() as u64)
                .sum(),
        ));

        for (prop_name, prop) in &props {
            group.bench_function(*prop_name, |b| {
                write_batch_with_option(b, batch, Some(prop.clone())).unwrap()
            });
        }
        group.finish();
    }
}

criterion_group!(benches, bench_all_writers);
criterion_main!(benches);
