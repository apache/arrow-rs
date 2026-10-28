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

//! Criterion benchmark measuring the *memory footprint* of `ArrowWriter` when
//! encoding an Arrow `RecordBatch` to Parquet.
//!
//! Uses a thread-local tracking allocator and two custom criterion measurements:
//! [`PeakMemory`] for the live high-water mark and [`AllocatedBytes`] for
//! cumulative bytes requested. The benchmark quantifies writer memory over and
//! above the input batch and configuration throughout the writer's lifecycle.
//!
//! How the measurement isolates *writer* memory:
//! * The `RecordBatch` is built lazily before `iter_batched`, and writer
//!   properties are cloned in its **setup** closure. Both run *outside* the
//!   measured window. Their live allocations therefore land in the
//!   per-iteration baseline (subtracted out of `PeakMemory`) and are never
//!   counted by `AllocatedBytes`.
//! * `ArrowWriter::try_new`, `writer.write(&batch)`, and `writer.close()` run
//!   inside the measured window, including writer initialization, encoding,
//!   finalization, and destruction.
//! * The output sink is [`std::io::Empty`], which discards serialized bytes, so
//!   the figure reflects the writer's internal working set (column encoders,
//!   dictionary interners, level/encoding buffers, retained page data up to the
//!   row-group flush) rather than the size of the produced file.
//! * `BatchSize::PerIteration` is mandatory: it forces criterion to bracket a
//!   single `routine()` call between `Measurement::start()`/`end()`, so each
//!   sample re-anchors the peak and isolates one complete writer lifecycle.
//!
//! Constraints:
//! * The tracking allocator is **thread-local**, so this only measures the
//!   synchronous, single-threaded `ArrowWriter`. A parallel/async writer would
//!   allocate on other threads and be silently undercounted.

use std::alloc::Layout;
use std::cell::{Cell, OnceCell};
use std::io::Empty;
use std::sync::Arc;

use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use arrow::util::data_gen::create_random_batch;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Decimal128Array, DictionaryArray, FixedSizeBinaryArray,
    Int8Array, Int32Array, Int64Array, ListArray, RunArray, StringArray, StructArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use criterion::measurement::{Measurement, ValueFormatter};
use criterion::{BatchSize, BenchmarkGroup, Criterion, criterion_group, criterion_main};
use parquet::arrow::ArrowWriter;
use parquet::errors::Result;
use parquet::file::properties::{CdcOptions, WriterProperties, WriterVersion};

// ---------------------------------------------------------------------------
// Thread-local tracking allocator
// ---------------------------------------------------------------------------

thread_local! {
    static LIVE_BYTES: Cell<usize> = const { Cell::new(0) };
    static PEAK_BYTES: Cell<usize> = const { Cell::new(0) };
    static ALLOCATED_BYTES: Cell<usize> = const { Cell::new(0) };
}

struct TrackingAllocator {
    inner: std::alloc::System,
}

#[global_allocator]
static GLOBAL: TrackingAllocator = TrackingAllocator {
    inner: std::alloc::System,
};

fn add_live_bytes(size: usize) {
    LIVE_BYTES.with(|live| {
        let new = live.get().saturating_add(size);
        live.set(new);
        PEAK_BYTES.with(|peak| {
            if new > peak.get() {
                peak.set(new);
            }
        });
    });
}

fn subtract_live_bytes(size: usize) {
    LIVE_BYTES.with(|live| {
        live.set(live.get().saturating_sub(size));
    });
}

fn add_allocated_bytes(size: usize) {
    ALLOCATED_BYTES.with(|allocated| {
        allocated.set(allocated.get().saturating_add(size));
    });
}

#[expect(unsafe_code)]
unsafe impl std::alloc::GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { self.inner.alloc(layout) };
        if !ptr.is_null() {
            add_live_bytes(layout.size());
            add_allocated_bytes(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        subtract_live_bytes(layout.size());
        unsafe { self.inner.dealloc(ptr, layout) };
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { self.inner.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            let old_size = layout.size();
            add_allocated_bytes(new_size);
            if new_size > old_size {
                add_live_bytes(new_size - old_size);
            } else {
                subtract_live_bytes(old_size - new_size);
            }
        }
        new_ptr
    }
}

fn reset_peak() {
    PEAK_BYTES.with(|peak| {
        LIVE_BYTES.with(|live| {
            peak.set(live.get());
        });
    });
}

fn peak_bytes() -> usize {
    PEAK_BYTES.with(|peak| peak.get())
}

fn live_bytes() -> usize {
    LIVE_BYTES.with(|live| live.get())
}

fn reset_allocated() {
    ALLOCATED_BYTES.with(|allocated| allocated.set(0));
}

fn allocated_bytes() -> usize {
    ALLOCATED_BYTES.with(|allocated| allocated.get())
}

// ---------------------------------------------------------------------------
// Criterion custom byte measurements
// ---------------------------------------------------------------------------

struct BytesFormatter;

const BYTE_UNITS: &[(u32, &str)] = &[
    (60, "EiB"),
    (50, "PiB"),
    (40, "TiB"),
    (30, "GiB"),
    (20, "MiB"),
    (10, "KiB"),
    (0, "B"),
];

fn bytes_per_unit(exponent: u32) -> f64 {
    (1_u64 << exponent) as f64
}

fn scale_bytes(typical: f64, values: &mut [f64]) -> &'static str {
    for &(exponent, unit) in BYTE_UNITS {
        let scale = bytes_per_unit(exponent);
        if typical >= scale {
            for v in values.iter_mut() {
                *v /= scale;
            }
            return unit;
        }
    }
    unreachable!("BYTE_UNITS contains B")
}

impl ValueFormatter for BytesFormatter {
    fn scale_values(&self, typical: f64, values: &mut [f64]) -> &'static str {
        scale_bytes(typical, values)
    }

    fn scale_throughputs(
        &self,
        typical: f64,
        _throughput: &criterion::Throughput,
        values: &mut [f64],
    ) -> &'static str {
        scale_bytes(typical, values)
    }

    fn scale_for_machines(&self, values: &mut [f64]) -> &'static str {
        // Machine-readable: always bytes
        let _ = values;
        "B"
    }
}

struct PeakMemory;

impl Measurement for PeakMemory {
    type Intermediate = usize;
    type Value = usize;

    fn start(&self) -> Self::Intermediate {
        reset_peak();
        live_bytes()
    }

    fn end(&self, baseline: Self::Intermediate) -> Self::Value {
        peak_bytes().saturating_sub(baseline)
    }

    fn add(&self, v1: &Self::Value, v2: &Self::Value) -> Self::Value {
        *v1 + *v2
    }

    fn zero(&self) -> Self::Value {
        0
    }

    fn to_f64(&self, value: &Self::Value) -> f64 {
        *value as f64
    }

    fn formatter(&self) -> &dyn ValueFormatter {
        &BytesFormatter
    }
}

struct AllocatedBytes;

impl Measurement for AllocatedBytes {
    type Intermediate = ();
    type Value = usize;

    fn start(&self) -> Self::Intermediate {
        reset_allocated();
    }

    fn end(&self, _baseline: Self::Intermediate) -> Self::Value {
        allocated_bytes()
    }

    fn add(&self, v1: &Self::Value, v2: &Self::Value) -> Self::Value {
        *v1 + *v2
    }

    fn zero(&self) -> Self::Value {
        0
    }

    fn to_f64(&self, value: &Self::Value) -> f64 {
        *value as f64
    }

    fn formatter(&self) -> &dyn ValueFormatter {
        &BytesFormatter
    }
}

// ---------------------------------------------------------------------------
// Test data generation for representative writer input shapes.
// ---------------------------------------------------------------------------

const BATCH_SIZE: usize = 1024 * 1024;

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

fn create_bool_dictionary_bench_batch(size: usize) -> Result<RecordBatch> {
    let keys = Int8Array::from_iter((0..size).map(|i| (i % 10 != 9).then_some((i % 2) as i8)));
    let values = BooleanArray::from(vec![false, true]);
    let array: ArrayRef = Arc::new(DictionaryArray::new(keys, Arc::new(values) as ArrayRef));
    Ok(RecordBatch::try_from_iter([("col", array)])?)
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

/// High-null byte-array columns (Utf8 / LargeUtf8 / Utf8View / BinaryView).
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

/// Many short, fixed-width strings — exercises the BYTE_ARRAY dense hot path.
fn create_short_string_bench_batch(size: usize) -> Result<RecordBatch> {
    let array = Arc::new(StringArray::from_iter_values(
        (0..size).map(|i| format!("{i:08}")),
    )) as _;
    Ok(RecordBatch::try_from_iter([("col", array)])?)
}

/// FixedSizeBinary column → FIXED_LEN_BYTE_ARRAY (raw FLBA path).
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

/// Decimal128(38, 10) column → FIXED_LEN_BYTE_ARRAY(16) (converted FLBA path).
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

/// A single run-end-encoded column with long runs of few distinct values —
/// REE's intended regime. `make_values` produces the per-run value array.
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

/// Struct-of-leaves run values for the non-leaf REE cases: 16 distinct
/// (int64, utf8) pairs cycled over `n` runs, one in eight struct-null.
/// Mirrors `arrow_writer.rs`.
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

/// List run values for the non-leaf REE cases: each run value is a list of
/// 8 ints drawn from 16 distinct lists. Mirrors `arrow_writer.rs`.
fn make_ree_list_values(n: usize) -> ArrayRef {
    let values =
        Int32Array::from_iter_values((0..n * 8).map(|i| ((i / 8) % 16 * 8 + i % 8) as i32));
    let offsets = OffsetBuffer::from_lengths(std::iter::repeat_n(8usize, n));
    let field = Arc::new(Field::new_list_field(DataType::Int32, true));
    Arc::new(ListArray::new(field, offsets, Arc::new(values), None))
}

/// A batch of `size` lists of 4 elements over a run-end-encoded child with
/// 256-row runs (16 distinct values, one run in eight null).
/// Mirrors `arrow_writer.rs`.
fn create_list_of_int32_ree_batch(size: usize) -> RecordBatch {
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
    let list: ArrayRef = Arc::new(ListArray::new(field, offsets, ree, None));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "_1",
        list.data_type().clone(),
        true,
    )]));
    RecordBatch::try_new(schema, vec![list]).unwrap()
}

/// A list-of-runs column with null struct-parent rows at the beginning,
/// middle, and end, separating the visible child rows into two ranges.
fn create_fragmented_list_of_int32_ree_batch(size: usize) -> RecordBatch {
    let list_batch = create_list_of_int32_ree_batch(size);
    let list = list_batch.column(0).clone();
    let fields = Fields::from(vec![Field::new("items", list.data_type().clone(), true)]);
    let nulls = NullBuffer::from_iter((0..size).map(|i| i != 0 && i != size / 2 && i + 1 != size));
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
/// Mirrors `arrow_writer.rs`.
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

/// `RunEndEncoded<List<RunEndEncoded<List<Int32>>>>`. Mirrors
/// `arrow_writer.rs`.
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

// ---------------------------------------------------------------------------
// Benchmark driver
// ---------------------------------------------------------------------------

/// Writer property variants to measure. Kept small: `default` for parquet 1.0
/// encoders, `parquet_2` for v2 / DELTA encoder coverage, and `cdc` for the
/// content-defined-chunking path.
fn writer_props() -> Vec<(&'static str, WriterProperties)> {
    vec![
        ("default", WriterProperties::builder().build()),
        (
            "parquet_2",
            WriterProperties::builder()
                .set_writer_version(WriterVersion::PARQUET_2_0)
                .build(),
        ),
        (
            "cdc",
            WriterProperties::builder()
                .set_content_defined_chunking(Some(CdcOptions::default()))
                .build(),
        ),
    ]
}

/// Measure one `(batch, props)` cell. Input construction and the props clone are
/// outside the measured window. Writer construction, `write`, and `close` into a
/// discarding [`Empty`] sink, including writer destruction, are measured.
fn bench_cell<M: Measurement>(
    group: &mut BenchmarkGroup<M>,
    prop_name: &str,
    benchmark: &BatchBenchmark,
    batch: &OnceCell<RecordBatch>,
    props: &WriterProperties,
) {
    group.bench_function(prop_name, |b| {
        // Filtered-out Criterion IDs never invoke this closure, so exact
        // filters build only the selected input instead of every 1M-row batch.
        let batch = batch.get_or_init(|| (benchmark.make_batch)());
        b.iter_batched(
            || props.clone(),
            |props| {
                let mut writer =
                    ArrowWriter::try_new(Empty::default(), batch.schema(), Some(props))
                        .expect("writer construction");
                writer.write(batch).expect("write");
                writer.close().expect("close");
            },
            BatchSize::PerIteration,
        );
    });
}

/// Run-end-encoded column whose run VALUES are a low-cardinality
/// `Dictionary<Int32, Utf8>` (run length 8 -> ~size/8 runs, 16 distinct dict
/// entries). Exercises the REE-of-dictionary path with many logical rows but
/// only `size / 8` physical runs.
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

/// An Int32 run-end array with `Dictionary<Int32, Int64>` values,
/// exercising dictionary mapping at run granularity.
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

/// An Int32 run-end array of structs with `Dictionary<Int32, Utf8>` and Int64
/// fields, exercising dictionary mapping beneath repeated struct records.
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

struct BatchBenchmark {
    name: &'static str,
    make_batch: Box<dyn Fn() -> RecordBatch>,
}

impl BatchBenchmark {
    fn new(name: &'static str, make_batch: impl Fn() -> RecordBatch + 'static) -> Self {
        Self {
            name,
            make_batch: Box::new(make_batch),
        }
    }
}

macro_rules! lazy_batches {
    ($(($name:expr, $batch:expr $(,)?)),* $(,)?) => {
        vec![$(BatchBenchmark::new($name, || $batch)),*]
    };
}

fn create_batches() -> Vec<BatchBenchmark> {
    lazy_batches![
        (
            "primitive_non_null",
            create_primitive_bench_batch_non_null(BATCH_SIZE, 0.0, 0.75).unwrap(),
        ),
        (
            "primitive_nullable",
            create_primitive_bench_batch(BATCH_SIZE, 0.25, 0.75).unwrap(),
        ),
        (
            "primitive_sparse_99pct_null",
            create_primitive_bench_batch(BATCH_SIZE, 0.99, 0.75).unwrap(),
        ),
        (
            "bool_non_null",
            create_bool_bench_batch_non_null(BATCH_SIZE, 0.25, 0.75).unwrap(),
        ),
        (
            "string_non_null",
            create_string_bench_batch_non_null(BATCH_SIZE, 0.0, 0.75).unwrap(),
        ),
        (
            "short_string_non_null",
            create_short_string_bench_batch(BATCH_SIZE).unwrap(),
        ),
        (
            "byte_array_sparse_99pct_null",
            create_byte_array_sparse_bench_batch(BATCH_SIZE, 0.99).unwrap(),
        ),
        (
            "fixed_size_binary_non_null",
            create_fixed_size_binary_bench_batch(BATCH_SIZE, 16, 0.0).unwrap(),
        ),
        (
            "fixed_size_binary_sparse",
            create_fixed_size_binary_bench_batch(BATCH_SIZE, 16, 0.25).unwrap(),
        ),
        (
            "decimal128_non_null",
            create_decimal128_bench_batch(BATCH_SIZE, 0.0).unwrap(),
        ),
        (
            "decimal128_sparse",
            create_decimal128_bench_batch(BATCH_SIZE, 0.25).unwrap(),
        ),
        (
            "int64_dictionary",
            create_int64_dictionary_bench_batch(BATCH_SIZE, 0.25, 0.75).unwrap(),
        ),
        (
            "int64_dictionary_1pct_cardinality",
            create_int64_dictionary_bench_batch_1pct_cardinality(BATCH_SIZE, 0.25).unwrap(),
        ),
        (
            "string_dictionary",
            create_string_dictionary_bench_batch(BATCH_SIZE, 0.25, 0.75).unwrap(),
        ),
        (
            "string_dictionary_1pct_cardinality",
            create_string_dictionary_bench_batch_1pct_cardinality(BATCH_SIZE, 0.25).unwrap(),
        ),
        (
            "decimal128_dictionary",
            create_decimal128_dictionary_bench_batch(BATCH_SIZE, 0.25, 0.75).unwrap(),
        ),
        (
            "decimal128_dictionary_1pct_cardinality",
            create_decimal128_dictionary_bench_batch_1pct_cardinality(BATCH_SIZE, 0.25).unwrap(),
        ),
        (
            "bool_dictionary",
            create_bool_dictionary_bench_batch(BATCH_SIZE).unwrap(),
        ),
        (
            "string_ree_low_cardinality",
            create_run_end_encoded_bench_batch(DataType::Int32, BATCH_SIZE, 256, |n| {
                Arc::new(StringArray::from_iter_values(
                    (0..n).map(|i| format!("category_{:02}", i % 16)),
                ))
            }),
        ),
        (
            "int64_ree_low_cardinality",
            create_run_end_encoded_bench_batch(DataType::Int32, BATCH_SIZE, 256, |n| {
                Arc::new(Int64Array::from_iter_values(
                    (0..n).map(|i| (i % 16) as i64),
                ))
            }),
        ),
        (
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
            }),
        ),
        (
            "string_ree_of_dict",
            create_ree_of_dict_bench_batch(BATCH_SIZE),
        ),
        (
            "numeric_ree_of_dict",
            create_ree_of_numeric_dict_bench_batch(BATCH_SIZE),
        ),
        (
            "ree_struct_of_dict",
            create_ree_struct_of_dict_bench_batch(BATCH_SIZE),
        ),
        (
            "int32_ree_99pct_null",
            create_run_end_encoded_bench_batch(DataType::Int32, BATCH_SIZE, 256, |n| {
                Arc::new(Int32Array::from_iter(
                    (0..n).map(|i| (i % 100 == 0).then_some((i % 16) as i32)),
                ))
            }),
        ),
        (
            "string_ree_99pct_null",
            create_run_end_encoded_bench_batch(DataType::Int32, BATCH_SIZE, 256, |n| {
                Arc::new(StringArray::from_iter((0..n).map(|i| {
                    (i % 100 == 0).then(|| format!("category_{:02}", i % 16))
                })))
            }),
        ),
        (
            "struct_ree",
            create_run_end_encoded_bench_batch(DataType::Int32, BATCH_SIZE, 256, |n| {
                make_ree_struct_values(n)
            }),
        ),
        (
            "list_int32_ree",
            create_run_end_encoded_bench_batch(DataType::Int32, BATCH_SIZE, 256, |n| {
                make_ree_list_values(n)
            }),
        ),
        (
            "list_of_int32_ree",
            create_list_of_int32_ree_batch(BATCH_SIZE),
        ),
        (
            "struct_of_list_of_int32_ree_fragmented",
            create_fragmented_list_of_int32_ree_batch(BATCH_SIZE),
        ),
        (
            "list_of_list_int32_ree",
            create_list_of_list_int32_ree_batch(BATCH_SIZE),
        ),
        (
            "ree_list_ree_list",
            create_ree_list_ree_list_batch(BATCH_SIZE),
        ),
    ]
}

fn add_benches<M: Measurement>(c: &mut Criterion<M>, measurement_name: &str) {
    let batches = create_batches();
    let props = writer_props();

    for benchmark in &batches {
        let mut group = c.benchmark_group(format!(
            "arrow_writer_{measurement_name}/{}",
            benchmark.name
        ));
        let batch = OnceCell::new();
        for (prop_name, prop) in &props {
            bench_cell(&mut group, prop_name, benchmark, &batch, prop);
        }
        group.finish();
    }
}

fn add_peak_memory_benches(c: &mut Criterion<PeakMemory>) {
    add_benches(c, "peak_memory");
}

fn add_allocated_bytes_benches(c: &mut Criterion<AllocatedBytes>) {
    add_benches(c, "allocated_bytes");
}

fn peak_memory_criterion() -> Criterion<PeakMemory> {
    Criterion::default()
        .with_measurement(PeakMemory)
        .sample_size(10)
}

fn allocated_bytes_criterion() -> Criterion<AllocatedBytes> {
    Criterion::default()
        .with_measurement(AllocatedBytes)
        .sample_size(10)
}

criterion_group! {
    name = peak_memory;
    config = peak_memory_criterion();
    targets = add_peak_memory_benches
}

criterion_group! {
    name = cumulative_allocated_bytes;
    config = allocated_bytes_criterion();
    targets = add_allocated_bytes_benches
}
criterion_main!(peak_memory, cumulative_allocated_bytes);
