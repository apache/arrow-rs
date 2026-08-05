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

use arrow_array::builder::StringDictionaryBuilder;
use criterion::{Bencher, Criterion, Throughput};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use rand::{RngExt, distr::Alphanumeric};

use std::hint::black_box;
use std::io::Empty;
use std::sync::Arc;

use arrow::datatypes::*;
use arrow::util::bench_util::{create_f16_array, create_f32_array, create_f64_array};
use arrow::{record_batch::RecordBatch, util::data_gen::*};
use arrow_array::{RecordBatchOptions, StringArray};
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
// Creates a DictionaryArray with target cardinality
fn create_low_card_dictionary_bench_batch(size: usize, cardinality: usize) -> Result<RecordBatch> {
    let mut rng = rand::rng();

    // Generate `cardinality` unique random strings.
    let categories: Vec<String> = (0..cardinality)
        .map(|_| {
            let len = rng.random_range(10..25);

            (0..len).map(|_| rng.sample(Alphanumeric) as char).collect()
        })
        .collect();

    let mut builder = StringDictionaryBuilder::<Int32Type>::new();

    for i in 0..size {
        builder.append_value(&categories[i % cardinality]);
    }

    let dict = builder.finish();

    let schema = Schema::new(vec![Field::new(
        "_1",
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        false,
    )]);

    Ok(RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(dict)],
    )?)
}

/// 1 M short, fixed-width 8-byte strings. Exercises the BYTE_ARRAY hot path
/// for the case where individual values are small enough that the byte-budget
/// based sub-batch sizing in `write_batch_internal` should always resolve to
/// the full chunk (no granular splitting, no regression vs. current behavior).
fn create_short_string_bench_batch(size: usize) -> Result<RecordBatch> {
    let array = Arc::new(StringArray::from_iter_values(
        (0..size).map(|i| format!("{i:08}")),
    )) as _;
    Ok(RecordBatch::try_from_iter([("col", array)])?)
}

/// `size` rows of `value_size`-byte strings. Exercises the BYTE_ARRAY path
/// where individual values are large enough that batching the default
/// `write_batch_size` of them would blow the page byte limit by orders of
/// magnitude — the case the page-size fix targets.
fn create_large_string_bench_batch(size: usize, value_size: usize) -> Result<RecordBatch> {
    let value = "x".repeat(value_size);
    let array = Arc::new(StringArray::from_iter_values(
        (0..size).map(|_| value.as_str()),
    )) as _;
    Ok(RecordBatch::try_from_iter([("col", array)])?)
}

/// `size` rows of `value_size`-byte strings sharing a long common prefix and
/// ending in a short distinct suffix — the case `DELTA_BYTE_ARRAY` exists
/// for: consecutive values dedup to a prefix length plus a few suffix bytes.
fn create_large_string_shared_prefix_bench_batch(
    size: usize,
    value_size: usize,
) -> Result<RecordBatch> {
    let prefix = "x".repeat(value_size - 8);
    let array = Arc::new(StringArray::from_iter_values(
        (0..size).map(|i| format!("{prefix}{i:08}")),
    )) as _;
    Ok(RecordBatch::try_from_iter([("col", array)])?)
}

/// `size` rows of `value_size`-byte strings whose leading bytes differ — the
/// adversarial case for `DELTA_BYTE_ARRAY`, where every prefix length is ~0
/// and the encoding stores each value in full.
fn create_large_string_distinct_bench_batch(size: usize, value_size: usize) -> Result<RecordBatch> {
    let filler = "x".repeat(value_size - 8);
    let array = Arc::new(StringArray::from_iter_values(
        (0..size).map(|i| format!("{i:08}{filler}")),
    )) as _;
    Ok(RecordBatch::try_from_iter([("col", array)])?)
}

/// `size` rows of `value_size`-byte strings sharing their first
/// `shared_bytes` bytes and differing thereafter — the realistic sorted-column
/// case (paths, URLs, keys), where prefix deduplication saves part of each
/// value rather than all or none of it.
fn create_string_partial_prefix_bench_batch(
    size: usize,
    value_size: usize,
    shared_bytes: usize,
) -> Result<RecordBatch> {
    let shared = "x".repeat(shared_bytes);
    let tail = "y".repeat(value_size - shared_bytes - 8);
    let array = Arc::new(StringArray::from_iter_values(
        (0..size).map(|i| format!("{shared}{i:08}{tail}")),
    )) as _;
    Ok(RecordBatch::try_from_iter([("col", array)])?)
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
fn create_ree_bench_batch(
    value_dt: DataType,
    size: usize,
    null_pct: Option<u8>,
    true_density: f32,
) -> Result<RecordBatch> {
    const DEFAULT_NULL_PCT: u8 = 10;
    let null_density = null_pct.unwrap_or(DEFAULT_NULL_PCT) as f32 / 100.0;
    let fields = vec![Field::new(
        "_1",
        DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            Arc::new(Field::new("values", value_dt, true)),
        ),
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

fn create_decimal_bench_batch(size: usize, null_density: f32) -> Result<RecordBatch> {
    let fields = vec![
        Field::new("_1", Decimal32Type::DEFAULT_TYPE, false),
        Field::new("_2", Decimal64Type::DEFAULT_TYPE, false),
        Field::new("_3", Decimal128Type::DEFAULT_TYPE, false),
        Field::new("_4", Decimal256Type::DEFAULT_TYPE, false),
    ];
    let schema = Schema::new(fields);
    Ok(create_random_batch(
        Arc::new(schema),
        size,
        null_density,
        0.75,
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

fn create_fsb_bench_batch(size: usize, null_density: f32, len: i32) -> Result<RecordBatch> {
    let fields = vec![Field::new("_1", DataType::FixedSizeBinary(len), true)];
    let schema = Schema::new(fields);
    Ok(create_random_batch(
        Arc::new(schema),
        size,
        null_density,
        0.75,
    )?)
}

fn create_nested_list_bench_batch(size: usize, null_density: f32) -> Result<RecordBatch> {
    // List<List<Int32>> — exercises the nested repetition (non-batched) path
    let fields = vec![Field::new(
        "_1",
        DataType::List(Arc::new(Field::new_list_field(
            DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
            true,
        ))),
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

fn create_list_struct_with_list_batch(size: usize, null_density: f32) -> Result<RecordBatch> {
    // List<Struct<a:Int32, b:Float32, c:List<Int32>>>
    // The struct child contains a nested list, so child_has_no_nested_rep() = false.
    // This exercises the per-slot (non-batched) write path in level computation.
    let fields = vec![Field::new(
        "_1",
        DataType::List(Arc::new(Field::new_list_field(
            DataType::Struct(Fields::from(vec![
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Float32, true),
                Field::new(
                    "c",
                    DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
                    true,
                ),
            ])),
            true,
        ))),
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

    let batch = create_short_string_bench_batch(BATCH_SIZE).unwrap();
    batches.push(("short_string_non_null", batch));

    // 1024 rows × 256 KiB = 256 MiB total. With the default 1 MiB page byte
    // limit, this is the case where the page-size fix kicks in: each value
    // needs its own page, and `write_batch_size = 1024` would otherwise
    // buffer all 256 MiB before the post-write check runs.
    let batch = create_large_string_bench_batch(1024, 256 * 1024).unwrap();
    batches.push(("large_string_non_null", batch));

    let batch = create_string_and_binary_view_bench_batch(BATCH_SIZE, 0.25, 0.75).unwrap();
    batches.push(("string_and_binary_view", batch));

    let batch = create_string_dictionary_bench_batch(BATCH_SIZE, 0.25, 0.75).unwrap();
    batches.push(("string_dictionary", batch));

    let batch = create_low_card_dictionary_bench_batch(BATCH_SIZE, 20).unwrap();
    batches.push(("string_dictionary_low_cardinality_20", batch));

    let batch = create_low_card_dictionary_bench_batch(BATCH_SIZE, 100).unwrap();
    batches.push(("string_dictionary_low_cardinality_100", batch));

    let batch = create_low_card_dictionary_bench_batch(BATCH_SIZE, 400).unwrap();
    batches.push(("string_dictionary_low_cardinality_400", batch));

    let batch = create_string_bench_batch_non_null(BATCH_SIZE, 0.25, 0.75).unwrap();
    batches.push(("string_non_null", batch));

    let batch = create_ree_bench_batch(DataType::Utf8, BATCH_SIZE, None, 0.75).unwrap();
    batches.push(("string_ree", batch));

    let batch = create_ree_bench_batch(DataType::Int32, BATCH_SIZE, None, 0.75).unwrap();
    batches.push(("int32_ree", batch));

    let batch = create_ree_bench_batch(DataType::Boolean, BATCH_SIZE, None, 0.75).unwrap();
    batches.push(("bool_ree", batch));

    let batch =
        create_ree_bench_batch(DataType::FixedSizeBinary(16), BATCH_SIZE, None, 0.75).unwrap();
    batches.push(("fixed_size_binary_ree", batch));

    let batch = create_ree_bench_batch(DataType::Utf8, BATCH_SIZE, Some(95), 0.75).unwrap();
    batches.push(("string_ree_95pct_null", batch));

    let batch = create_ree_bench_batch(DataType::Int32, BATCH_SIZE, Some(95), 0.75).unwrap();
    batches.push(("int32_ree_95pct_null", batch));

    let batch = create_float_bench_batch_with_nans(BATCH_SIZE, 0.5).unwrap();
    batches.push(("float_with_nans", batch));

    let batch = create_decimal_bench_batch(BATCH_SIZE, 0.75).unwrap();
    batches.push(("decimal", batch));

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

    let batch = create_fsb_bench_batch(BATCH_SIZE, 0.9, 16).unwrap();
    batches.push(("fsb", batch));

    let batch = create_nested_list_bench_batch(BATCH_SIZE, 0.25).unwrap();
    batches.push(("list_nested", batch));

    let batch = create_list_struct_with_list_batch(BATCH_SIZE, 0.25).unwrap();
    batches.push(("list_struct_with_list", batch));

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

/// Writes BYTE_ARRAY columns of *small* string values with `DELTA_BYTE_ARRAY`,
/// with `PLAIN` on the same data as a baseline.
///
/// Values here sit far below `data_page_size_limit`, so many share a page and
/// the encoder's previous-value state survives across them. This is the regime
/// `DELTA_BYTE_ARRAY` is actually deployed in, and — unlike the multi-MiB
/// benches below — the one where the shared-prefix scan runs to real depth.
///
/// * `small_string_shared_prefix`: values differing only in a trailing counter,
///   so each scan covers nearly the whole value.
/// * `small_string_partial_prefix`: values sharing their first half, the
///   sorted-column case.
/// * `small_string_distinct`: values differing from byte 0, where the scan
///   stops immediately and prefix deduplication saves nothing.
fn bench_small_delta_byte_array_writers(c: &mut Criterion) {
    const ROWS: usize = 8192;
    const VALUE_SIZE: usize = 1024;

    let shared_prefix = create_large_string_shared_prefix_bench_batch(ROWS, VALUE_SIZE).unwrap();
    let partial_prefix =
        create_string_partial_prefix_bench_batch(ROWS, VALUE_SIZE, VALUE_SIZE / 2).unwrap();
    let distinct = create_large_string_distinct_bench_batch(ROWS, VALUE_SIZE).unwrap();

    let plain = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_encoding(Encoding::PLAIN)
        .build();
    let delta = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_encoding(Encoding::DELTA_BYTE_ARRAY)
        .build();

    for (batch_name, batch) in [
        ("small_string_shared_prefix", &shared_prefix),
        ("small_string_partial_prefix", &partial_prefix),
        ("small_string_distinct", &distinct),
    ] {
        let mut group = c.benchmark_group(batch_name);
        group.throughput(Throughput::Bytes(
            batch
                .columns()
                .iter()
                .map(|f| f.get_array_memory_size() as u64)
                .sum(),
        ));

        for (prop_name, prop) in [("plain", &plain), ("delta_byte_array", &delta)] {
            group.bench_function(prop_name, |b| {
                write_batch_with_option(b, batch, Some((*prop).clone())).unwrap()
            });
        }
        group.finish();
    }
}

/// Writes BYTE_ARRAY columns of large (multi-MiB) string values with
/// `DELTA_BYTE_ARRAY`, with `PLAIN` on the same data as a baseline.
///
/// Two data shapes bracket the encoding's best and worst case:
/// * `large_string_shared_prefix`: values like `xxx…x00000000`,
///   `xxx…x00000001`, … share a long common prefix and differ only in a
///   short suffix — the case `DELTA_BYTE_ARRAY` is designed to handle well,
///   encoding each value as a prefix length plus a few suffix bytes.
/// * `large_string_distinct`: values like `00000000x…xxx`, `00000001x…xxx`, …
///   differ in their leading bytes, so prefix deduplication saves nothing and
///   the encoding stores each value in full.
fn bench_delta_byte_array_writers(c: &mut Criterion) {
    // Each 2 MiB value alone exceeds the default 1 MiB data page size limit.
    let shared = create_large_string_shared_prefix_bench_batch(128, 2 * 1024 * 1024).unwrap();
    let distinct = create_large_string_distinct_bench_batch(128, 2 * 1024 * 1024).unwrap();

    let plain = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_encoding(Encoding::PLAIN)
        .build();
    let delta = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_encoding(Encoding::DELTA_BYTE_ARRAY)
        .build();

    for (batch_name, batch) in [
        ("large_string_shared_prefix", &shared),
        ("large_string_distinct", &distinct),
    ] {
        let mut group = c.benchmark_group(batch_name);
        group.throughput(Throughput::Bytes(
            batch
                .columns()
                .iter()
                .map(|f| f.get_array_memory_size() as u64)
                .sum(),
        ));

        for (prop_name, prop) in [("plain", &plain), ("delta_byte_array", &delta)] {
            group.bench_function(prop_name, |b| {
                write_batch_with_option(b, batch, Some((*prop).clone())).unwrap()
            });
        }
        group.finish();
    }
}

criterion_group!(
    benches,
    bench_all_writers,
    bench_small_delta_byte_array_writers,
    bench_delta_byte_array_writers
);
criterion_main!(benches);
