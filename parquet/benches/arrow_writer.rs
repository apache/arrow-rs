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

use std::cell::OnceCell;
use std::hint::black_box;
use std::io::Empty;
use std::sync::Arc;

use arrow::datatypes::*;
use arrow::util::bench_util::{create_f16_array, create_f32_array, create_f64_array};
use arrow::util::test_util::seedable_rng;
use arrow::{record_batch::RecordBatch, util::data_gen::*};
use arrow_array::builder::{FixedSizeBinaryBuilder, ListBuilder, StringBuilder};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Decimal128Array, DictionaryArray, FixedSizeBinaryArray,
    Float64Array, Int8Array, Int32Array, Int64Array, LargeBinaryArray, ListArray,
    RecordBatchOptions, RunArray, StringArray, StringViewArray, StructArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
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

fn create_primitive_dictionary_bench_batch(
    size: usize,
    null_density: f32,
    true_density: f32,
) -> Result<RecordBatch> {
    let fields = vec![Field::new(
        "_1",
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int32)),
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

fn create_primitive_dictionary_bench_batch_1pct_cardinality(
    size: usize,
    null_density: f32,
) -> Result<RecordBatch> {
    let cardinality = dictionary_cardinality_1pct(size);
    let schema = dictionary_schema(DataType::Int32);
    let keys = dictionary_keys(size, cardinality, null_density);
    let values = Int32Array::from_iter_values((0..cardinality).map(|i| i as i32));
    let array = DictionaryArray::<Int32Type>::new(keys, Arc::new(values));

    Ok(RecordBatch::try_new(
        schema,
        vec![Arc::new(array) as ArrayRef],
    )?)
}

fn create_int64_dictionary_bench_batch(
    size: usize,
    null_density: f32,
    true_density: f32,
) -> Result<RecordBatch> {
    Ok(create_random_batch(
        dictionary_schema(DataType::Int64),
        size,
        null_density,
        true_density,
    )?)
}

fn create_int64_dictionary_bench_batch_1pct_cardinality(
    size: usize,
    null_density: f32,
) -> Result<RecordBatch> {
    let cardinality = dictionary_cardinality_1pct(size);
    let schema = dictionary_schema(DataType::Int64);
    let keys = dictionary_keys(size, cardinality, null_density);
    let values = Int64Array::from_iter_values((0..cardinality).map(|i| i as i64));
    let array = DictionaryArray::<Int32Type>::new(keys, Arc::new(values));

    Ok(RecordBatch::try_new(
        schema,
        vec![Arc::new(array) as ArrayRef],
    )?)
}

fn create_float64_dictionary_bench_batch(
    size: usize,
    null_density: f32,
    true_density: f32,
) -> Result<RecordBatch> {
    Ok(create_random_batch(
        dictionary_schema(DataType::Float64),
        size,
        null_density,
        true_density,
    )?)
}

fn create_float64_dictionary_bench_batch_1pct_cardinality(
    size: usize,
    null_density: f32,
) -> Result<RecordBatch> {
    let cardinality = dictionary_cardinality_1pct(size);
    let schema = dictionary_schema(DataType::Float64);
    let keys = dictionary_keys(size, cardinality, null_density);
    let values = Float64Array::from_iter_values((0..cardinality).map(|i| i as f64));
    let array = DictionaryArray::<Int32Type>::new(keys, Arc::new(values));

    Ok(RecordBatch::try_new(
        schema,
        vec![Arc::new(array) as ArrayRef],
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
    let mut rng = seedable_rng();

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

/// Where nulls fall in a generated batch, for
/// [`create_large_string_nullable_bench_batch`].
#[derive(Clone, Copy)]
enum NullPattern {
    /// One null every `n` rows, spread evenly.
    Every(usize),
    /// `n` nulls in a single run at the end of the batch.
    Trailing(usize),
}

/// `size` rows of `value_size`-byte strings with nulls, sharing a long common
/// prefix when `shared_prefix` is set and differing from their first byte
/// otherwise.
///
/// Nullability is the point. The non-null large-value batches leave the
/// column's definition levels absent, so the writer's byte-budget
/// sub-batching resolves a chunk's value count in O(1) and never inspects
/// levels. Nulls put it on the general path, where the number of values that
/// share a data page is derived from the chunk's level-to-value ratio.
///
/// That ratio is why the density levels chosen at the call sites are not
/// simply "few" and "many". Where a single value already fills the page
/// budget, the derived window spans `ceil(levels / values)` levels, so it
/// covers about `levels / values` values instead of one — an overshoot that
/// is largest when nulls are *sparse* and disappears exactly when the ratio
/// is a whole number, as at one-null-in-two.
fn create_large_string_nullable_bench_batch(
    size: usize,
    value_size: usize,
    shared_prefix: bool,
    nulls: NullPattern,
) -> Result<RecordBatch> {
    let filler = "x".repeat(value_size - 8);
    let is_null = |i: usize| match nulls {
        NullPattern::Every(n) => i % n == n - 1,
        NullPattern::Trailing(n) => i >= size - n,
    };
    let array = Arc::new(StringArray::from_iter((0..size).map(|i| {
        (!is_null(i)).then(|| {
            if shared_prefix {
                format!("{filler}{i:08}")
            } else {
                format!("{i:08}{filler}")
            }
        })
    }))) as _;
    Ok(RecordBatch::try_from_iter([("col", array)])?)
}

/// `size` records of `values_per_record` strings of `value_size` bytes,
/// sharing a long common prefix.
///
/// A repeated column is the third level shape the writer sub-batches against,
/// after absent and flat-nullable levels. Records cannot span data pages, so
/// mini-batches must step whole records; with values this large a single
/// record overruns the page limit on its own.
fn create_list_large_string_bench_batch(
    size: usize,
    values_per_record: usize,
    value_size: usize,
) -> Result<RecordBatch> {
    let prefix = "x".repeat(value_size - 8);
    let mut builder = ListBuilder::new(StringBuilder::new());
    for i in 0..size {
        for j in 0..values_per_record {
            builder
                .values()
                .append_value(format!("{prefix}{:08}", i * values_per_record + j));
        }
        builder.append(true);
    }
    Ok(RecordBatch::try_from_iter([(
        "col",
        Arc::new(builder.finish()) as _,
    )])?)
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

/// Run-end-encoded column whose run values are a low-cardinality
/// `Dictionary<Int32, Utf8>` (run length 8, 16 distinct dictionary entries).
fn create_ree_of_dict_bench_batch(size: usize) -> RecordBatch {
    let run_len = 8usize;
    let num_runs = size / run_len;
    let run_ends = Int32Array::from(
        (1..=num_runs)
            .map(|i| (i * run_len) as i32)
            .collect::<Vec<_>>(),
    );
    let dict_values = StringArray::from((0..16).map(|i| format!("val_{i:04}")).collect::<Vec<_>>());
    let keys = Int32Array::from((0..num_runs).map(|i| (i % 16) as i32).collect::<Vec<_>>());
    let dict = DictionaryArray::<Int32Type>::new(keys, Arc::new(dict_values));
    let ree = RunArray::<Int32Type>::try_new(&run_ends, &dict).unwrap();
    let field = Field::new("_1", ree.data_type().clone(), true);
    let schema = Arc::new(Schema::new(vec![field]));
    RecordBatch::try_new(schema, vec![Arc::new(ree)]).unwrap()
}

/// A run-end column whose values are a sized `Dictionary<Int32, Int64>`.
fn create_ree_of_numeric_dict_bench_batch(size: usize) -> RecordBatch {
    let run_len = 8usize;
    let num_runs = size / run_len;
    let run_ends = Int32Array::from(
        (1..=num_runs)
            .map(|i| (i * run_len) as i32)
            .collect::<Vec<_>>(),
    );
    let dict_values = Int64Array::from((0..16).map(|i| i as i64 * 1_000_000).collect::<Vec<_>>());
    let keys = Int32Array::from((0..num_runs).map(|i| (i % 16) as i32).collect::<Vec<_>>());
    let dict = DictionaryArray::<Int32Type>::new(keys, Arc::new(dict_values));
    let ree = RunArray::<Int32Type>::try_new(&run_ends, &dict).unwrap();
    let field = Field::new("_1", ree.data_type().clone(), true);
    let schema = Arc::new(Schema::new(vec![field]));
    RecordBatch::try_new(schema, vec![Arc::new(ree)]).unwrap()
}

/// A run-end-encoded struct with a dictionary string field and an Int64 field.
fn create_ree_struct_of_dict_bench_batch(size: usize) -> RecordBatch {
    let run_len = 8usize;
    let num_runs = size / run_len;
    let run_ends = Int32Array::from(
        (1..=num_runs)
            .map(|i| (i * run_len) as i32)
            .collect::<Vec<_>>(),
    );
    let dict_values = StringArray::from((0..16).map(|i| format!("tag_{i:04}")).collect::<Vec<_>>());
    let keys = Int32Array::from((0..num_runs).map(|i| (i % 16) as i32).collect::<Vec<_>>());
    let tag = DictionaryArray::<Int32Type>::new(keys, Arc::new(dict_values));
    let val = Int64Array::from((0..num_runs).map(|i| i as i64).collect::<Vec<_>>());
    let fields = Fields::from(vec![
        Field::new("tag", tag.data_type().clone(), true),
        Field::new("val", DataType::Int64, true),
    ]);
    let values = StructArray::new(fields, vec![Arc::new(tag), Arc::new(val)], None);
    let ree = RunArray::<Int32Type>::try_new(&run_ends, &values).unwrap();
    let field = Field::new("_1", ree.data_type().clone(), true);
    let schema = Arc::new(Schema::new(vec![field]));
    RecordBatch::try_new(schema, vec![Arc::new(ree)]).unwrap()
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
            Arc::new(Field::new(
                Field::REE_RUN_ENDS_FIELD_DEFAULT_NAME,
                DataType::Int32,
                false,
            )),
            Arc::new(Field::new(
                Field::REE_VALUES_FIELD_DEFAULT_NAME,
                value_dt,
                true,
            )),
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

/// A single run-end-encoded column with configurable run length and run-end
/// index width. `make_values` produces one value per run, allowing benchmarks
/// to vary value type and distinct-value cardinality.
fn create_run_end_encoded_bench_batch(
    run_ends_type: DataType,
    size: usize,
    run_len: usize,
    make_values: impl Fn(usize) -> ArrayRef,
) -> RecordBatch {
    let num_runs = size.div_ceil(run_len);
    let mut acc = 0usize;
    let run_ends: Vec<i64> = (0..num_runs)
        .map(|_| {
            acc = (acc + run_len).min(size);
            acc as i64
        })
        .collect();
    let values = make_values(num_runs);
    let run_array: ArrayRef = match run_ends_type {
        DataType::Int32 => {
            let ends = Int32Array::from_iter_values(run_ends.iter().map(|&v| v as i32));
            Arc::new(RunArray::<Int32Type>::try_new(&ends, &values).unwrap())
        }
        DataType::Int64 => {
            let ends = Int64Array::from_iter_values(run_ends.iter().copied());
            Arc::new(RunArray::<Int64Type>::try_new(&ends, &values).unwrap())
        }
        other => panic!("unsupported REE run-ends type for bench: {other:?}"),
    };
    let schema = Arc::new(Schema::new(vec![Field::new(
        "_1",
        run_array.data_type().clone(),
        true,
    )]));
    RecordBatch::try_new(schema, vec![run_array]).unwrap()
}

/// Struct-of-leaves run values for the non-leaf REE benches: 16 distinct
/// (int64, utf8) pairs cycled over `n` runs, one in eight struct-null.
fn make_ree_struct_values(n: usize) -> ArrayRef {
    let a = Int64Array::from_iter_values((0..n).map(|i| (i % 16) as i64));
    let b = StringArray::from_iter_values((0..n).map(|i| format!("category_{:02}", i % 16)));
    let fields = Fields::from(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Utf8, true),
    ]);
    let nulls = NullBuffer::from_iter((0..n).map(|i| i % 8 != 7));
    Arc::new(StructArray::new(
        fields,
        vec![Arc::new(a), Arc::new(b)],
        Some(nulls),
    ))
}

/// String-list run values: each run value is a list of 4 strings drawn from
/// 16 distinct lists.
fn make_ree_string_list_values(n: usize) -> ArrayRef {
    let values = StringArray::from_iter_values(
        (0..n * 4).map(|i| format!("category_{:02}_{}", (i / 4) % 16, i % 4)),
    );
    let offsets = OffsetBuffer::from_lengths(std::iter::repeat_n(4usize, n));
    let field = Arc::new(Field::new_list_field(DataType::Utf8, true));
    Arc::new(ListArray::new(field, offsets, Arc::new(values), None))
}

/// A batch of `size` lists of 4 elements over a run-end-encoded child with
/// 256-row runs (16 distinct values, one run in eight null). `null_every`
/// controls the frequency of null list rows.
fn create_list_of_int32_ree_batch(size: usize, null_every: Option<usize>) -> RecordBatch {
    let child_len = size * 4;
    let num_runs = child_len.div_ceil(256);
    let run_ends = Int32Array::from_iter_values(
        (0..num_runs).map(|i| (((i + 1) * 256).min(child_len)) as i32),
    );
    let values =
        Int32Array::from_iter((0..num_runs).map(|i| (i % 8 != 7).then_some((i % 16) as i32)));
    let ree: ArrayRef = Arc::new(RunArray::<Int32Type>::try_new(&run_ends, &values).unwrap());
    let offsets = OffsetBuffer::from_lengths(std::iter::repeat_n(4usize, size));
    let field = Arc::new(Field::new_list_field(ree.data_type().clone(), true));
    let nulls = null_every.map(|n| NullBuffer::from_iter((0..size).map(|i| i % n != n - 1)));
    let list: ArrayRef = Arc::new(ListArray::new(field, offsets, ree, nulls));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "_1",
        list.data_type().clone(),
        true,
    )]));
    RecordBatch::try_new(schema, vec![list]).unwrap()
}

/// A list-of-runs batch whose rows alternate between empty and four values,
/// without null list rows.
fn create_list_of_int32_ree_alternating_empty_batch(size: usize) -> RecordBatch {
    let child_len = size / 2 * 4;
    let num_runs = child_len.div_ceil(256);
    let run_ends = Int32Array::from_iter_values(
        (0..num_runs).map(|i| (((i + 1) * 256).min(child_len)) as i32),
    );
    let values =
        Int32Array::from_iter((0..num_runs).map(|i| (i % 8 != 7).then_some((i % 16) as i32)));
    let ree: ArrayRef = Arc::new(RunArray::<Int32Type>::try_new(&run_ends, &values).unwrap());
    let offsets = OffsetBuffer::from_lengths((0..size).map(|i| if i % 2 == 0 { 0 } else { 4 }));
    let field = Arc::new(Field::new_list_field(ree.data_type().clone(), true));
    let list: ArrayRef = Arc::new(ListArray::new(field, offsets, ree, None));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "_1",
        list.data_type().clone(),
        true,
    )]));
    RecordBatch::try_new(schema, vec![list]).unwrap()
}

/// A uniform list-of-runs column under a nullable struct. Parent validity can
/// divide the child into multiple write ranges even though the list itself has
/// no null or empty rows.
fn create_struct_of_list_of_int32_ree_batch(size: usize, nulls: NullBuffer) -> RecordBatch {
    let list_batch = create_list_of_int32_ree_batch(size, None);
    let list = list_batch.column(0).clone();
    let fields = Fields::from(vec![Field::new("items", list.data_type().clone(), true)]);
    let structs: ArrayRef = Arc::new(StructArray::new(fields, vec![list], Some(nulls)));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "_1",
        structs.data_type().clone(),
        true,
    )]));
    RecordBatch::try_new(schema, vec![structs]).unwrap()
}

/// A batch of `size` outer lists of 2 elements over a run-end-encoded child
/// (128-element runs) whose run values are inner lists of 4 ints.
fn create_list_of_list_int32_ree_batch(size: usize) -> RecordBatch {
    let child_len = size * 2;
    let num_runs = child_len.div_ceil(128);
    let run_ends = Int32Array::from_iter_values(
        (0..num_runs).map(|i| (((i + 1) * 128).min(child_len)) as i32),
    );
    let inner_values =
        Int32Array::from_iter_values((0..num_runs * 4).map(|i| ((i / 4) % 16 * 4 + i % 4) as i32));
    let inner_offsets = OffsetBuffer::from_lengths(std::iter::repeat_n(4usize, num_runs));
    let inner_field = Arc::new(Field::new_list_field(DataType::Int32, true));
    let inner = ListArray::new(inner_field, inner_offsets, Arc::new(inner_values), None);
    let ree: ArrayRef = Arc::new(RunArray::<Int32Type>::try_new(&run_ends, &inner).unwrap());
    let offsets = OffsetBuffer::from_lengths(std::iter::repeat_n(2usize, size));
    let field = Arc::new(Field::new_list_field(ree.data_type().clone(), true));
    let list: ArrayRef = Arc::new(ListArray::new(field, offsets, ree, None));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "_1",
        list.data_type().clone(),
        true,
    )]));
    RecordBatch::try_new(schema, vec![list]).unwrap()
}

/// `RunEndEncoded<List<RunEndEncoded<List<Int32>>>>` with 256-row outer runs,
/// two inner entries per physical outer value, and 128-element inner runs.
fn create_ree_list_ree_list_batch(size: usize) -> RecordBatch {
    let outer_runs = size.div_ceil(256);
    let inner_len = outer_runs * 2;
    let inner_runs = inner_len.div_ceil(128);

    let values = Int32Array::from_iter_values(
        (0..inner_runs * 4).map(|i| ((i / 4) % 16 * 4 + i % 4) as i32),
    );
    let value_offsets = OffsetBuffer::from_lengths(std::iter::repeat_n(4usize, inner_runs));
    let value_field = Arc::new(Field::new_list_field(DataType::Int32, true));
    let value_lists = ListArray::new(value_field, value_offsets, Arc::new(values), None);

    let inner_ends = Int32Array::from_iter_values(
        (0..inner_runs).map(|i| (((i + 1) * 128).min(inner_len)) as i32),
    );
    let inner: ArrayRef =
        Arc::new(RunArray::<Int32Type>::try_new(&inner_ends, &value_lists).unwrap());
    let outer_value_offsets = OffsetBuffer::from_lengths(std::iter::repeat_n(2usize, outer_runs));
    let outer_value_field = Arc::new(Field::new_list_field(inner.data_type().clone(), true));
    let outer_values: ArrayRef = Arc::new(ListArray::new(
        outer_value_field,
        outer_value_offsets,
        inner,
        None,
    ));

    let outer_ends =
        Int32Array::from_iter_values((0..outer_runs).map(|i| (((i + 1) * 256).min(size)) as i32));
    let array: ArrayRef =
        Arc::new(RunArray::<Int32Type>::try_new(&outer_ends, &outer_values).unwrap());
    let schema = Arc::new(Schema::new(vec![Field::new(
        "_1",
        array.data_type().clone(),
        true,
    )]));
    RecordBatch::try_new(schema, vec![array]).unwrap()
}

/// List run values for the non-leaf REE benches: each run value is a list of
/// 8 ints drawn from 16 distinct lists.
fn make_ree_list_values(n: usize) -> ArrayRef {
    let values =
        Int32Array::from_iter_values((0..n * 8).map(|i| ((i / 8) % 16 * 8 + i % 8) as i32));
    let offsets = OffsetBuffer::from_lengths(std::iter::repeat_n(8usize, n));
    let field = Arc::new(Field::new_list_field(DataType::Int32, true));
    Arc::new(ListArray::new(field, offsets, Arc::new(values), None))
}

fn create_string_dictionary_bench_batch_1pct_cardinality(
    size: usize,
    null_density: f32,
) -> Result<RecordBatch> {
    let cardinality = dictionary_cardinality_1pct(size);
    let schema = dictionary_schema(DataType::Utf8);
    let keys = dictionary_keys(size, cardinality, null_density);
    let values = StringArray::from_iter_values((0..cardinality).map(|i| format!("value_{i:08}")));
    let array = DictionaryArray::<Int32Type>::new(keys, Arc::new(values));

    Ok(RecordBatch::try_new(
        schema,
        vec![Arc::new(array) as ArrayRef],
    )?)
}

fn dictionary_schema(value_type: DataType) -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "_1",
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(value_type)),
        true,
    )]))
}

fn dictionary_cardinality_1pct(size: usize) -> usize {
    (size / 100).max(1)
}

fn dictionary_keys(size: usize, cardinality: usize, null_density: f32) -> Int32Array {
    let keys = (0..size)
        .map(|i| (i % cardinality) as i32)
        .collect::<Vec<_>>();
    Int32Array::new(keys.into(), nulls_for_density(size, null_density))
}

fn nulls_for_density(size: usize, null_density: f32) -> Option<NullBuffer> {
    if null_density == 0. {
        return None;
    }

    let null_threshold = (null_density.clamp(0., 1.) * 10_000.) as usize;
    Some(NullBuffer::from(
        (0..size)
            .map(|i| (i * 9973) % 10_000 >= null_threshold)
            .collect::<Vec<_>>(),
    ))
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

/// High-null byte-array columns (Utf8 / LargeUtf8 / Utf8View / BinaryView).
/// Varies sparsity across the supported byte-array representations.
fn create_byte_array_sparse_bench_batch(size: usize, null_density: f32) -> Result<RecordBatch> {
    let fields = vec![
        Field::new("_1", DataType::Utf8, true),
        Field::new("_2", DataType::LargeUtf8, true),
        Field::new("_3", DataType::Utf8View, true),
        Field::new("_4", DataType::BinaryView, true),
    ];
    let schema = Schema::new(fields);
    Ok(create_random_batch(
        Arc::new(schema),
        size,
        null_density,
        0.5,
    )?)
}

/// FixedSizeBinary column mapped to FIXED_LEN_BYTE_ARRAY, with configurable
/// null density.
fn create_fixed_size_binary_bench_batch(
    size: usize,
    byte_width: i32,
    null_density: f32,
) -> Result<RecordBatch> {
    let width = byte_width as usize;
    let nulls = nulls_for_density(size, null_density);
    let array = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
        (0..size).map(|i| {
            nulls.as_ref().is_none_or(|n| n.is_valid(i)).then(|| {
                let mut v = vec![0u8; width];
                let b = (i as u64).to_le_bytes();
                let n = width.min(8);
                v[..n].copy_from_slice(&b[..n]);
                v
            })
        }),
        byte_width,
    )?;
    Ok(RecordBatch::try_from_iter([(
        "col",
        Arc::new(array) as ArrayRef,
    )])?)
}

/// Decimal128(38, 10) column mapped to FIXED_LEN_BYTE_ARRAY(16), with
/// configurable null density. Precision 38 requires fixed-width storage.
fn create_decimal128_bench_batch(size: usize, null_density: f32) -> Result<RecordBatch> {
    let values: Vec<i128> = (0..size).map(|i| i as i128).collect();
    let nulls = nulls_for_density(size, null_density);
    let array = Decimal128Array::new(values.into(), nulls).with_precision_and_scale(38, 10)?;
    Ok(RecordBatch::try_from_iter([(
        "col",
        Arc::new(array) as ArrayRef,
    )])?)
}

fn create_decimal128_dictionary_bench_batch(
    size: usize,
    null_density: f32,
    true_density: f32,
) -> Result<RecordBatch> {
    Ok(create_random_batch(
        dictionary_schema(DataType::Decimal128(38, 10)),
        size,
        null_density,
        true_density,
    )?)
}

fn create_decimal128_dictionary_bench_batch_1pct_cardinality(
    size: usize,
    null_density: f32,
) -> Result<RecordBatch> {
    let cardinality = dictionary_cardinality_1pct(size);
    let keys = dictionary_keys(size, cardinality, null_density);
    let values = Decimal128Array::from_iter_values((0..cardinality).map(|i| i as i128))
        .with_precision_and_scale(38, 10)?;
    let array: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::new(keys, Arc::new(values)));
    Ok(RecordBatch::try_from_iter([("col", array)])?)
}

struct BatchBenchmark {
    name: &'static str,
    logical_rows: u64,
    make_batch: Box<dyn Fn() -> RecordBatch>,
}

impl BatchBenchmark {
    fn new(
        name: &'static str,
        logical_rows: usize,
        make_batch: impl Fn() -> RecordBatch + 'static,
    ) -> Self {
        Self {
            name,
            logical_rows: logical_rows as u64,
            make_batch: Box::new(make_batch),
        }
    }
}

fn create_batches() -> Vec<BatchBenchmark> {
    const BATCH_SIZE: usize = 1024 * 1024;

    let mut batches = vec![];

    macro_rules! push_batch {
        ($name:expr, $batch:expr) => {
            batches.push(BatchBenchmark::new($name, BATCH_SIZE, || $batch));
        };
        ($name:expr, $logical_rows:expr, $batch:expr) => {
            batches.push(BatchBenchmark::new($name, $logical_rows, || $batch));
        };
    }

    push_batch!(
        "primitive",
        create_primitive_bench_batch(BATCH_SIZE, 0.25, 0.75).unwrap()
    );

    push_batch!(
        "primitive_non_null",
        create_primitive_bench_batch_non_null(BATCH_SIZE, 0.25, 0.75).unwrap()
    );

    push_batch!(
        "primitive_dictionary",
        create_primitive_dictionary_bench_batch(BATCH_SIZE, 0.25, 0.75).unwrap()
    );

    push_batch!(
        "primitive_dictionary_1pct_cardinality",
        create_primitive_dictionary_bench_batch_1pct_cardinality(BATCH_SIZE, 0.25).unwrap()
    );

    push_batch!(
        "int64_dictionary",
        create_int64_dictionary_bench_batch(BATCH_SIZE, 0.25, 0.75).unwrap()
    );

    push_batch!(
        "int64_dictionary_1pct_cardinality",
        create_int64_dictionary_bench_batch_1pct_cardinality(BATCH_SIZE, 0.25).unwrap()
    );

    push_batch!(
        "float64_dictionary",
        create_float64_dictionary_bench_batch(BATCH_SIZE, 0.25, 0.75).unwrap()
    );

    push_batch!(
        "float64_dictionary_1pct_cardinality",
        create_float64_dictionary_bench_batch_1pct_cardinality(BATCH_SIZE, 0.25).unwrap()
    );

    push_batch!(
        "bool",
        create_bool_bench_batch(BATCH_SIZE, 0.25, 0.75).unwrap()
    );

    push_batch!(
        "bool_non_null",
        create_bool_bench_batch_non_null(BATCH_SIZE, 0.25, 0.75).unwrap()
    );

    // Dictionary-encoded booleans exercise the native bool-dictionary write path.
    push_batch!("bool_dictionary", {
        let keys = Int8Array::from_iter((0..BATCH_SIZE).map(|i| {
            (i % 10 != 9).then_some((i % 2) as i8) // 10% null keys
        }));
        let values = BooleanArray::from(vec![false, true]);
        let dict: ArrayRef = Arc::new(DictionaryArray::new(keys, Arc::new(values) as ArrayRef));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "_1",
            dict.data_type().clone(),
            true,
        )]));
        RecordBatch::try_new(schema, vec![dict]).unwrap()
    });

    push_batch!(
        "string",
        create_string_bench_batch(BATCH_SIZE, 0.25, 0.75).unwrap()
    );

    push_batch!(
        "short_string_non_null",
        create_short_string_bench_batch(BATCH_SIZE).unwrap()
    );

    // 1024 rows × 256 KiB = 256 MiB total. With the default 1 MiB page byte
    // limit, byte-budget sub-batching avoids admitting all 256 MiB before
    // the post-write check when `write_batch_size = 1024`.
    push_batch!(
        "large_string_non_null",
        1024,
        create_large_string_bench_batch(1024, 256 * 1024).unwrap()
    );

    push_batch!(
        "string_and_binary_view",
        create_string_and_binary_view_bench_batch(BATCH_SIZE, 0.25, 0.75).unwrap()
    );

    push_batch!(
        "string_dictionary",
        create_string_dictionary_bench_batch(BATCH_SIZE, 0.25, 0.75).unwrap()
    );

    push_batch!(
        "string_dictionary_low_cardinality_20",
        create_low_card_dictionary_bench_batch(BATCH_SIZE, 20).unwrap()
    );
    push_batch!(
        "string_dictionary_low_cardinality_100",
        create_low_card_dictionary_bench_batch(BATCH_SIZE, 100).unwrap()
    );
    push_batch!(
        "string_dictionary_low_cardinality_400",
        create_low_card_dictionary_bench_batch(BATCH_SIZE, 400).unwrap()
    );
    push_batch!(
        "string_dictionary_1pct_cardinality",
        create_string_dictionary_bench_batch_1pct_cardinality(BATCH_SIZE, 0.25).unwrap()
    );

    push_batch!(
        "string_non_null",
        create_string_bench_batch_non_null(BATCH_SIZE, 0.25, 0.75).unwrap()
    );

    // Run-end-encoded — high-cardinality / short-run regime (random runs).
    push_batch!(
        "string_ree",
        create_ree_bench_batch(DataType::Utf8, BATCH_SIZE, None, 0.75).unwrap()
    );

    push_batch!(
        "int32_ree",
        create_ree_bench_batch(DataType::Int32, BATCH_SIZE, None, 0.75).unwrap()
    );

    push_batch!(
        "string_ree_of_dict",
        create_ree_of_dict_bench_batch(BATCH_SIZE)
    );

    push_batch!(
        "numeric_ree_of_dict",
        create_ree_of_numeric_dict_bench_batch(BATCH_SIZE)
    );

    push_batch!(
        "ree_struct_of_dict",
        create_ree_struct_of_dict_bench_batch(BATCH_SIZE)
    );

    push_batch!(
        "bool_ree",
        create_ree_bench_batch(DataType::Boolean, BATCH_SIZE, None, 0.75).unwrap()
    );

    push_batch!(
        "decimal",
        create_decimal_bench_batch(BATCH_SIZE, 0.75).unwrap()
    );
    push_batch!(
        "fixed_size_binary_ree",
        create_ree_bench_batch(DataType::FixedSizeBinary(16), BATCH_SIZE, None, 0.75).unwrap()
    );

    push_batch!(
        "string_ree_95pct_null",
        create_ree_bench_batch(DataType::Utf8, BATCH_SIZE, Some(95), 0.75).unwrap()
    );

    push_batch!(
        "int32_ree_95pct_null",
        create_ree_bench_batch(DataType::Int32, BATCH_SIZE, Some(95), 0.75).unwrap()
    );

    // Run-end-encoded batches with 256-row runs and 16 distinct values, across
    // value families and run-end widths.
    push_batch!(
        "string_ree_low_cardinality",
        create_run_end_encoded_bench_batch(DataType::Int32, BATCH_SIZE, 256, |n| {
            Arc::new(StringArray::from_iter_values(
                (0..n).map(|i| format!("category_{:02}", i % 16)),
            ))
        })
    );
    // String run values with Int64 run ends.
    push_batch!(
        "string_ree_int64_run_ends",
        create_run_end_encoded_bench_batch(DataType::Int64, BATCH_SIZE, 256, |n| {
            Arc::new(StringArray::from_iter_values(
                (0..n).map(|i| format!("category_{:02}", i % 16)),
            ))
        })
    );
    // Numeric (Int64) run values.
    push_batch!(
        "int64_ree_low_cardinality",
        create_run_end_encoded_bench_batch(DataType::Int32, BATCH_SIZE, 256, |n| {
            Arc::new(Int64Array::from_iter_values(
                (0..n).map(|i| (i % 16) as i64),
            ))
        })
    );
    // Fixed-length byte-array run values (FLBA path).
    push_batch!(
        "fixed_size_binary_ree_low_cardinality",
        create_run_end_encoded_bench_batch(DataType::Int32, BATCH_SIZE, 256, |n| {
            Arc::new(
                FixedSizeBinaryArray::try_from_iter((0..n).map(|i| {
                    let mut b = [0u8; 16];
                    b[0] = (i % 16) as u8;
                    b
                }))
                .unwrap(),
            )
        })
    );

    // Non-leaf run values: struct-of-leaves with long runs.
    push_batch!(
        "struct_ree",
        create_run_end_encoded_bench_batch(DataType::Int32, BATCH_SIZE, 256, |n| {
            make_ree_struct_values(n)
        })
    );
    // Non-leaf struct run values with short runs (per-run overhead regime).
    push_batch!(
        "struct_ree_short_runs",
        create_run_end_encoded_bench_batch(DataType::Int32, BATCH_SIZE, 8, |n| {
            make_ree_struct_values(n)
        })
    );
    // Non-leaf run values: each run value is a list of 8 ints (block regime).
    push_batch!(
        "list_int32_ree",
        create_run_end_encoded_bench_batch(DataType::Int32, BATCH_SIZE, 256, |n| {
            make_ree_list_values(n)
        })
    );
    push_batch!(
        "list_int32_ree_short_runs",
        create_run_end_encoded_bench_batch(DataType::Int32, BATCH_SIZE, 8, |n| {
            make_ree_list_values(n)
        })
    );
    // Byte-array leaves under run-encoded list values.
    push_batch!(
        "list_string_ree",
        create_run_end_encoded_bench_batch(DataType::Int32, BATCH_SIZE, 256, |n| {
            make_ree_string_list_values(n)
        })
    );
    // Lists whose run-end-encoded child contains lists.
    push_batch!(
        "list_of_list_int32_ree",
        create_list_of_list_int32_ree_batch(BATCH_SIZE)
    );
    push_batch!(
        "ree_list_ree_list",
        create_ree_list_ree_list_batch(BATCH_SIZE)
    );
    // A list whose values child is run-end encoded.
    push_batch!(
        "list_of_int32_ree",
        create_list_of_int32_ree_batch(BATCH_SIZE, None)
    );
    // Alternating null and valid list rows.
    push_batch!(
        "list_of_int32_ree_alt_null",
        create_list_of_int32_ree_batch(BATCH_SIZE, Some(2))
    );
    push_batch!(
        "list_of_int32_ree_alt_empty",
        create_list_of_int32_ree_alternating_empty_batch(BATCH_SIZE)
    );
    push_batch!(
        "struct_of_list_of_int32_ree_alt_null",
        create_struct_of_list_of_int32_ree_batch(
            BATCH_SIZE,
            NullBuffer::from_iter((0..BATCH_SIZE).map(|i| i % 2 == 0)),
        )
    );
    push_batch!(
        "struct_of_list_of_int32_ree_single_null",
        create_struct_of_list_of_int32_ree_batch(
            BATCH_SIZE,
            NullBuffer::from_iter((0..BATCH_SIZE).map(|i| i != BATCH_SIZE / 2)),
        )
    );
    // Alternate ordinary and run-end-encoded input batches.
    push_batch!(
        "struct_of_list_of_int32_ree_fragmented",
        create_struct_of_list_of_int32_ree_batch(
            BATCH_SIZE,
            NullBuffer::from_iter(
                (0..BATCH_SIZE).map(|i| i != 0 && i != BATCH_SIZE / 2 && i + 1 != BATCH_SIZE),
            ),
        )
    );

    push_batch!(
        "float_with_nans",
        create_float_bench_batch_with_nans(BATCH_SIZE, 0.5).unwrap()
    );

    push_batch!(
        "list_primitive",
        create_list_primitive_bench_batch(BATCH_SIZE, 0.25, 0.75).unwrap()
    );

    push_batch!(
        "list_primitive_non_null",
        create_list_primitive_bench_batch_non_null(BATCH_SIZE, 0.25, 0.75).unwrap()
    );

    push_batch!("fsb", create_fsb_bench_batch(BATCH_SIZE, 0.9, 16).unwrap());
    push_batch!(
        "primitive_sparse_99pct_null",
        create_primitive_bench_batch(BATCH_SIZE, 0.99, 0.75).unwrap()
    );

    push_batch!(
        "list_primitive_sparse_99pct_null",
        create_list_primitive_bench_batch(BATCH_SIZE, 0.99, 0.75).unwrap()
    );

    push_batch!(
        "primitive_all_null",
        create_primitive_bench_batch(BATCH_SIZE, 1.0, 0.75).unwrap()
    );

    push_batch!(
        "struct_non_null",
        create_struct_bench_batch(BATCH_SIZE, 0.0).unwrap()
    );

    push_batch!(
        "struct_sparse_99pct_null",
        create_struct_bench_batch(BATCH_SIZE, 0.99).unwrap()
    );

    push_batch!(
        "struct_all_null",
        create_struct_bench_batch(BATCH_SIZE, 1.0).unwrap()
    );

    push_batch!(
        "list_nested",
        create_nested_list_bench_batch(BATCH_SIZE, 0.25).unwrap()
    );

    push_batch!(
        "list_struct_with_list",
        create_list_struct_with_list_batch(BATCH_SIZE, 0.25).unwrap()
    );

    push_batch!(
        "byte_array_sparse_99pct_null",
        create_byte_array_sparse_bench_batch(BATCH_SIZE, 0.99).unwrap()
    );

    push_batch!(
        "fixed_size_binary_non_null",
        create_fixed_size_binary_bench_batch(BATCH_SIZE, 16, 0.0).unwrap()
    );

    push_batch!(
        "decimal128_sparse",
        create_decimal128_bench_batch(BATCH_SIZE, 0.25).unwrap()
    );

    push_batch!(
        "decimal128_dictionary",
        create_decimal128_dictionary_bench_batch(BATCH_SIZE, 0.25, 0.75).unwrap()
    );
    push_batch!(
        "decimal128_dictionary_1pct_cardinality",
        create_decimal128_dictionary_bench_batch_1pct_cardinality(BATCH_SIZE, 0.25).unwrap()
    );

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

    let prop = WriterProperties::builder()
        .set_write_row_group_number_distinct_values(true)
        .build();
    props.push(("number_distinct_values", prop));

    props
}

fn bench_all_writers(c: &mut Criterion) {
    let batches = create_batches();
    let props = create_writer_props();

    for benchmark in &batches {
        let mut group = c.benchmark_group(benchmark.name);

        // Arrow's physical buffer size is not comparable between a dense
        // array and its run-end encoded equivalent. Logical rows are stable
        // across representations and don't require eagerly building input.
        group.throughput(Throughput::Elements(benchmark.logical_rows));

        // Criterion doesn't invoke a benchmark closure for a filtered-out
        // ID. Once any property is selected, retain one input for the whole
        // workload group without including its construction in `b.iter`.
        let batch = OnceCell::new();
        for (prop_name, prop) in &props {
            group.bench_function(*prop_name, |b| {
                let batch = batch.get_or_init(|| (benchmark.make_batch)());
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

    // Nullable and repeated counterparts. Each varies one property of the
    // first nullable case, so a movement can be attributed to that property
    // rather than to some combination:
    //
    // * `_dense` changes only the null density, to a ratio of exactly two
    //   levels per value. Deriving a window from that ratio is exact, so this
    //   case is the one where sub-batching arithmetic cannot go wrong — it
    //   should stay flat when the others move.
    // * `_trailing` changes only where the nulls sit, keeping the count. A
    //   run of nulls at the end leaves every window before it holding values
    //   only, which is the worst placement for `DELTA_BYTE_ARRAY`.
    // * `distinct_nullable` changes only the prefix, removing what
    //   deduplication has to work with.
    // * `medium_string_*` changes only the value size, to a size where
    //   several values share a page budget rather than one overrunning it.
    // * `_list` changes only the level shape, to a repeated column. Records
    //   cannot span pages, so this one is a control: the writer's output for
    //   it is byte for byte identical across the changes it is used to judge.
    //
    // `PLAIN` is measured wherever page count alone drives the result, and
    // omitted where it would only restate a neighbouring case: it does not
    // read the previous value, so prefix, null placement and value size do
    // not change its per-page work.
    let nullable = create_large_string_nullable_bench_batch(
        128,
        2 * 1024 * 1024,
        true,
        NullPattern::Every(16),
    )
    .unwrap();
    let nullable_dense =
        create_large_string_nullable_bench_batch(128, 2 * 1024 * 1024, true, NullPattern::Every(2))
            .unwrap();
    let nullable_trailing = create_large_string_nullable_bench_batch(
        128,
        2 * 1024 * 1024,
        true,
        NullPattern::Trailing(8),
    )
    .unwrap();
    let nullable_distinct = create_large_string_nullable_bench_batch(
        128,
        2 * 1024 * 1024,
        false,
        NullPattern::Every(16),
    )
    .unwrap();
    // 256 KiB against the 1 MiB default limit: several values to a page.
    let nullable_medium =
        create_large_string_nullable_bench_batch(1024, 256 * 1024, true, NullPattern::Every(16))
            .unwrap();
    // 4 values per record, so one record is ~8 MiB and cannot be split.
    let list = create_list_large_string_bench_batch(32, 4, 2 * 1024 * 1024).unwrap();

    let both: &[(&str, &WriterProperties)] = &[("plain", &plain), ("delta_byte_array", &delta)];
    let delta_only: &[(&str, &WriterProperties)] = &[("delta_byte_array", &delta)];

    for (batch_name, batch, props) in [
        ("large_string_shared_prefix", &shared, both),
        ("large_string_distinct", &distinct, both),
        ("large_string_shared_prefix_nullable", &nullable, both),
        (
            "large_string_shared_prefix_nullable_dense",
            &nullable_dense,
            delta_only,
        ),
        (
            "large_string_shared_prefix_nullable_trailing",
            &nullable_trailing,
            delta_only,
        ),
        (
            "large_string_distinct_nullable",
            &nullable_distinct,
            delta_only,
        ),
        (
            "medium_string_shared_prefix_nullable",
            &nullable_medium,
            delta_only,
        ),
        ("large_string_shared_prefix_list", &list, delta_only),
    ] {
        let mut group = c.benchmark_group(batch_name);
        group.throughput(Throughput::Bytes(
            batch
                .columns()
                .iter()
                .map(|f| f.get_array_memory_size() as u64)
                .sum(),
        ));

        for (prop_name, prop) in props {
            group.bench_function(*prop_name, |b| {
                write_batch_with_option(b, batch, Some((*prop).clone())).unwrap()
            });
        }
        group.finish();
    }
}

const NDV_BATCH_SIZE: usize = 65_536;
const NDV_NUM_BATCHES: usize = 10;
const NDV_LOW_CARDINALITY: usize = 100;

// Generates NDV_NUM_BATCHES random batches for high-cardinality cases.
fn ndv_random_batches(schema: Arc<Schema>) -> Vec<RecordBatch> {
    (0..NDV_NUM_BATCHES)
        .map(|_| create_random_batch(schema.clone(), NDV_BATCH_SIZE, 0.0, 0.5).unwrap())
        .collect()
}

// Low-cardinality helpers: cycle through NDV_LOW_CARDINALITY distinct values.
fn make_int32_batches_low() -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int32,
        false,
    )]));
    (0..NDV_NUM_BATCHES)
        .map(|batch_idx| {
            let values: Vec<i32> = (0..NDV_BATCH_SIZE)
                .map(|row| ((batch_idx * NDV_BATCH_SIZE + row) % NDV_LOW_CARDINALITY) as i32)
                .collect();
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from(values)) as _],
            )
            .unwrap()
        })
        .collect()
}

fn make_utf8_batches_low() -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "label",
        DataType::Utf8,
        false,
    )]));
    (0..NDV_NUM_BATCHES)
        .map(|batch_idx| {
            let values: Vec<String> = (0..NDV_BATCH_SIZE)
                .map(|row| {
                    format!(
                        "label_{}",
                        (batch_idx * NDV_BATCH_SIZE + row) % NDV_LOW_CARDINALITY
                    )
                })
                .collect();
            let array = StringArray::from(values.iter().map(|s| s.as_str()).collect::<Vec<_>>());
            RecordBatch::try_new(schema.clone(), vec![Arc::new(array) as _]).unwrap()
        })
        .collect()
}

fn make_large_binary_batches_low() -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "payload",
        DataType::LargeBinary,
        false,
    )]));
    (0..NDV_NUM_BATCHES)
        .map(|batch_idx| {
            let payloads: Vec<Vec<u8>> = (0..NDV_BATCH_SIZE)
                .map(|row| {
                    format!(
                        "payload_{}",
                        (batch_idx * NDV_BATCH_SIZE + row) % NDV_LOW_CARDINALITY
                    )
                    .into_bytes()
                })
                .collect();
            let refs: Vec<&[u8]> = payloads.iter().map(|p| p.as_slice()).collect();
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(LargeBinaryArray::from(refs)) as _],
            )
            .unwrap()
        })
        .collect()
}

// Utf8View is not yet tracked by NDV (skipped in update_distinct_values_seen), so these
// benchmarks measure base write cost only — useful as a no-overhead baseline.
fn make_utf8_view_batches_low() -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "label",
        DataType::Utf8View,
        false,
    )]));
    (0..NDV_NUM_BATCHES)
        .map(|batch_idx| {
            let values: Vec<String> = (0..NDV_BATCH_SIZE)
                .map(|row| {
                    format!(
                        "label_{}",
                        (batch_idx * NDV_BATCH_SIZE + row) % NDV_LOW_CARDINALITY
                    )
                })
                .collect();
            let array = StringViewArray::from_iter_values(values.iter().map(|s| s.as_str()));
            RecordBatch::try_new(schema.clone(), vec![Arc::new(array) as _]).unwrap()
        })
        .collect()
}

fn make_fsb_batches_low(byte_width: i32) -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "id",
        DataType::FixedSizeBinary(byte_width),
        false,
    )]));
    let width = byte_width as usize;
    (0..NDV_NUM_BATCHES)
        .map(|batch_idx| {
            let mut builder = FixedSizeBinaryBuilder::new(byte_width);
            for row in 0..NDV_BATCH_SIZE {
                let distinct_id = (batch_idx * NDV_BATCH_SIZE + row) % NDV_LOW_CARDINALITY;
                let mut buf = vec![0u8; width];
                let encoded = distinct_id.to_le_bytes();
                buf[..encoded.len().min(width)]
                    .copy_from_slice(&encoded[..encoded.len().min(width)]);
                builder.append_value(&buf).unwrap();
            }
            RecordBatch::try_new(schema.clone(), vec![Arc::new(builder.finish()) as _]).unwrap()
        })
        .collect()
}

fn write_ndv_batches(b: &mut criterion::Bencher, batches: &[RecordBatch]) {
    let props = WriterProperties::builder()
        .set_write_row_group_number_distinct_values(true)
        .build();
    let schema = batches[0].schema();
    b.iter(|| {
        let mut file = Empty::default();
        let mut writer =
            ArrowWriter::try_new(&mut file, schema.clone(), Some(props.clone())).unwrap();
        for batch in batches {
            writer.write(black_box(batch)).unwrap();
        }
        black_box(writer.close()).unwrap();
    });
}

fn bench_ndv(c: &mut Criterion) {
    let int32_schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int32,
        false,
    )]));
    let bool_schema = Arc::new(Schema::new(vec![Field::new(
        "flag",
        DataType::Boolean,
        false,
    )]));
    let utf8_schema = Arc::new(Schema::new(vec![Field::new(
        "label",
        DataType::Utf8,
        false,
    )]));
    let large_binary_schema = Arc::new(Schema::new(vec![Field::new(
        "payload",
        DataType::LargeBinary,
        false,
    )]));
    let fsb16_schema = Arc::new(Schema::new(vec![Field::new(
        "id",
        DataType::FixedSizeBinary(16),
        false,
    )]));
    let utf8_view_schema = Arc::new(Schema::new(vec![Field::new(
        "label",
        DataType::Utf8View,
        false,
    )]));

    let cases: Vec<(&str, Vec<RecordBatch>)> = vec![
        ("int32_low_cardinality", make_int32_batches_low()),
        ("int32_high_cardinality", ndv_random_batches(int32_schema)),
        ("bool", ndv_random_batches(bool_schema)),
        ("utf8_low_cardinality", make_utf8_batches_low()),
        ("utf8_high_cardinality", ndv_random_batches(utf8_schema)),
        (
            "large_binary_low_cardinality",
            make_large_binary_batches_low(),
        ),
        (
            "large_binary_high_cardinality",
            ndv_random_batches(large_binary_schema),
        ),
        ("fsb16_low_cardinality", make_fsb_batches_low(16)),
        ("fsb16_high_cardinality", ndv_random_batches(fsb16_schema)),
        ("utf8_view_low_cardinality", make_utf8_view_batches_low()),
        (
            "utf8_view_high_cardinality",
            ndv_random_batches(utf8_view_schema),
        ),
    ];

    for (name, batches) in cases {
        let total_bytes: u64 = batches
            .iter()
            .flat_map(|b| b.columns())
            .map(|c| c.get_array_memory_size() as u64)
            .sum();

        let mut group = c.benchmark_group(format!("ndv/{name}"));
        group.throughput(Throughput::Bytes(total_bytes));
        group.bench_function("ndv_on", |b| write_ndv_batches(b, &batches));
        group.finish();
    }
}

criterion_group!(
    benches,
    bench_all_writers,
    bench_small_delta_byte_array_writers,
    bench_delta_byte_array_writers,
    bench_ndv,
);
criterion_main!(benches);
