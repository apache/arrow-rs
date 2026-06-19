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

//! Criterion benchmark measuring memory usage of ListArrayReader for sparse
//! list columns.
//!
//! Uses a global allocator with thread-local memory tracking so that
//! concurrent criterion threads don't interfere with measurements.

use std::alloc::Layout;
use std::cell::Cell;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use criterion::measurement::{Measurement, ValueFormatter};
use criterion::{BenchmarkGroup, Criterion, Throughput, criterion_group, criterion_main};
use parquet::arrow::array_reader::make_byte_array_reader;
use parquet::arrow::array_reader::{
    ArrayReader, ListArrayReader, PrimitiveArrayReader, make_fixed_len_byte_array_reader,
};
use parquet::arrow::arrow_reader::DEFAULT_BATCH_SIZE;
use parquet::basic::Encoding;
use parquet::column::page::PageIterator;
use parquet::data_type::{ByteArrayType, DoubleType, FixedLenByteArrayType, Int32Type};
use parquet::schema::parser::parse_message_type;
use parquet::schema::types::{ColumnDescPtr, SchemaDescriptor};
use parquet::util::{DataPageBuilder, DataPageBuilderImpl, InMemoryPageIterator};
use rand::{Rng, SeedableRng, rngs::StdRng};

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

#[allow(unsafe_code)]
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
// Criterion custom measurements
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
        _throughput: &Throughput,
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
// Test data generation
// ---------------------------------------------------------------------------

const NUM_ROW_GROUPS: usize = 2;
const PAGES_PER_GROUP: usize = 4;
const VALUES_PER_PAGE: usize = 10_000;
const MAX_LIST_LEN: usize = 10;
const BATCH_SIZE: usize = 8192;
const EXPECTED_VALUE_COUNT: usize = NUM_ROW_GROUPS * PAGES_PER_GROUP * VALUES_PER_PAGE;

fn seedable_rng() -> StdRng {
    StdRng::seed_from_u64(42)
}

fn build_int32_list_schema() -> (SchemaDescriptor, ColumnDescPtr) {
    let message_type = "
        message schema {
            OPTIONAL GROUP int32_list (LIST) {
                repeated group list {
                    optional INT32 element;
                }
            }
        }
    ";
    let schema = parse_message_type(message_type)
        .map(|t| SchemaDescriptor::new(Arc::new(t)))
        .unwrap();
    let col_desc = schema.column(0);
    (schema, col_desc)
}

fn build_string_list_schema() -> (SchemaDescriptor, ColumnDescPtr) {
    let message_type = "
        message schema {
            OPTIONAL GROUP string_list (LIST) {
                repeated group list {
                    optional BYTE_ARRAY element (UTF8);
                }
            }
        }
    ";
    let schema = parse_message_type(message_type)
        .map(|t| SchemaDescriptor::new(Arc::new(t)))
        .unwrap();
    let col_desc = schema.column(0);
    (schema, col_desc)
}

const FIXED_BYTE_LEN: usize = 32;

fn build_fixed32_list_schema() -> (SchemaDescriptor, ColumnDescPtr) {
    let message_type = "
        message schema {
            OPTIONAL GROUP fixed32_list (LIST) {
                repeated group list {
                    optional FIXED_LEN_BYTE_ARRAY(32) element;
                }
            }
        }
    ";
    let schema = parse_message_type(message_type)
        .map(|t| SchemaDescriptor::new(Arc::new(t)))
        .unwrap();
    let col_desc = schema.column(0);
    (schema, col_desc)
}

fn build_double_list_schema() -> (SchemaDescriptor, ColumnDescPtr) {
    let message_type = "
        message schema {
            OPTIONAL GROUP double_list (LIST) {
                repeated group list {
                    optional DOUBLE element;
                }
            }
        }
    ";
    let schema = parse_message_type(message_type)
        .map(|t| SchemaDescriptor::new(Arc::new(t)))
        .unwrap();
    let col_desc = schema.column(0);
    (schema, col_desc)
}

fn build_list_pages<T: parquet::data_type::DataType>(
    column_desc: ColumnDescPtr,
    null_density: f32,
    mut gen_value: impl FnMut(&mut StdRng) -> T::T,
) -> impl PageIterator + Clone {
    let max_def_level = column_desc.max_def_level();
    let max_rep_level = column_desc.max_rep_level();
    let mut rng = seedable_rng();
    let mut pages: Vec<Vec<parquet::column::page::Page>> = Vec::new();

    for _ in 0..NUM_ROW_GROUPS {
        let mut column_chunk_pages = Vec::new();
        for _ in 0..PAGES_PER_GROUP {
            let mut values = Vec::new();
            let mut def_levels = Vec::new();
            let mut rep_levels = Vec::new();

            for _ in 0..VALUES_PER_PAGE {
                rep_levels.push(0);
                if rng.random::<f32>() < null_density {
                    def_levels.push(0);
                    continue;
                }
                let len = rng.random_range(0..MAX_LIST_LEN);
                if len == 0 {
                    def_levels.push(1);
                    continue;
                }
                (1..len).for_each(|_| rep_levels.push(1));
                for _ in 0..len {
                    if rng.random::<f32>() < null_density {
                        def_levels.push(2);
                    } else {
                        def_levels.push(3);
                        values.push(gen_value(&mut rng));
                    }
                }
            }

            let mut pb = DataPageBuilderImpl::new(column_desc.clone(), values.len() as u32, true);
            pb.add_rep_levels(max_rep_level, &rep_levels);
            pb.add_def_levels(max_def_level, &def_levels);
            pb.add_values::<T>(Encoding::PLAIN, &values);
            column_chunk_pages.push(pb.consume());
        }
        pages.push(column_chunk_pages);
    }
    InMemoryPageIterator::new(pages)
}

fn drain_reader(mut reader: Box<dyn ArrayReader>) -> usize {
    let mut total_count = 0;
    loop {
        let batch = reader.next_batch(BATCH_SIZE).unwrap();
        let batch_len = batch.len();
        total_count += batch_len;
        if batch_len < BATCH_SIZE {
            break;
        }
    }
    total_count
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

fn null_density_label(null_density: f32) -> &'static str {
    match (null_density * 100.0) as u32 {
        0 => "no NULLs",
        50 => "half NULLs",
        90 => "90pct NULLs",
        99 => "99pct NULLs",
        _ => unreachable!(),
    }
}

fn bench_list_memory<M: Measurement>(
    group: &mut BenchmarkGroup<M>,
    null_density: f32,
    pages: impl PageIterator + Clone + 'static,
    make_reader: fn(Box<dyn PageIterator>, ColumnDescPtr) -> Box<dyn ArrayReader>,
    column_desc: &ColumnDescPtr,
) {
    group.bench_function(null_density_label(null_density), |b| {
        b.iter_batched(
            || make_reader(Box::new(pages.clone()), column_desc.clone()),
            |reader| {
                let count = drain_reader(reader);
                assert_eq!(count, EXPECTED_VALUE_COUNT);
                count
            },
            criterion::BatchSize::PerIteration,
        );
    });
}

fn int32_list_wrapper(
    pages: Box<dyn PageIterator>,
    column_desc: ColumnDescPtr,
) -> Box<dyn ArrayReader> {
    let child: Box<dyn ArrayReader> = Box::new(
        PrimitiveArrayReader::<Int32Type>::new(pages, column_desc, None, DEFAULT_BATCH_SIZE)
            .unwrap(),
    );
    let field = Field::new_list_field(DataType::Int32, true);
    let data_type = DataType::List(Arc::new(field));
    Box::new(ListArrayReader::<i32>::new(child, data_type, 2, 1, true))
}

fn double_list_wrapper(
    pages: Box<dyn PageIterator>,
    column_desc: ColumnDescPtr,
) -> Box<dyn ArrayReader> {
    let child: Box<dyn ArrayReader> = Box::new(
        PrimitiveArrayReader::<DoubleType>::new(pages, column_desc, None, DEFAULT_BATCH_SIZE)
            .unwrap(),
    );
    let field = Field::new_list_field(DataType::Float64, true);
    let data_type = DataType::List(Arc::new(field));
    Box::new(ListArrayReader::<i32>::new(child, data_type, 2, 1, true))
}

fn fixed32_list_wrapper(
    pages: Box<dyn PageIterator>,
    column_desc: ColumnDescPtr,
) -> Box<dyn ArrayReader> {
    let child =
        make_fixed_len_byte_array_reader(pages, column_desc, None, DEFAULT_BATCH_SIZE).unwrap();
    let field = Field::new_list_field(DataType::FixedSizeBinary(FIXED_BYTE_LEN as i32), true);
    let data_type = DataType::List(Arc::new(field));
    Box::new(ListArrayReader::<i32>::new(child, data_type, 2, 1, true))
}

fn string_list_wrapper(
    pages: Box<dyn PageIterator>,
    column_desc: ColumnDescPtr,
) -> Box<dyn ArrayReader> {
    let child = make_byte_array_reader(pages, column_desc, None, DEFAULT_BATCH_SIZE).unwrap();
    let field = Field::new_list_field(DataType::Utf8, true);
    let data_type = DataType::List(Arc::new(field));
    Box::new(ListArrayReader::<i32>::new(child, data_type, 2, 1, true))
}

fn add_benches<M: Measurement>(c: &mut Criterion<M>, measurement_name: &str) {
    let (_schema, int32_desc) = build_int32_list_schema();
    let (_schema, double_desc) = build_double_list_schema();
    let (_schema, fixed32_desc) = build_fixed32_list_schema();
    let (_schema, string_desc) = build_string_list_schema();

    let mut group = c.benchmark_group(format!(
        "arrow_array_reader/ListArray_{measurement_name}/Int32List"
    ));
    for null_density in [0.0, 0.5, 0.9, 0.99] {
        let pages =
            build_list_pages::<Int32Type>(int32_desc.clone(), null_density, |rng| rng.random());
        bench_list_memory(
            &mut group,
            null_density,
            pages,
            int32_list_wrapper,
            &int32_desc,
        );
    }
    group.finish();

    let mut group = c.benchmark_group(format!(
        "arrow_array_reader/ListArray_{measurement_name}/DoubleList"
    ));
    for null_density in [0.0, 0.5, 0.9, 0.99] {
        let pages =
            build_list_pages::<DoubleType>(double_desc.clone(), null_density, |rng| rng.random());
        bench_list_memory(
            &mut group,
            null_density,
            pages,
            double_list_wrapper,
            &double_desc,
        );
    }
    group.finish();

    let mut group = c.benchmark_group(format!(
        "arrow_array_reader/ListArray_{measurement_name}/Fixed32List"
    ));
    for null_density in [0.0, 0.5, 0.9, 0.99] {
        let pages =
            build_list_pages::<FixedLenByteArrayType>(fixed32_desc.clone(), null_density, |rng| {
                let mut buf = vec![0u8; FIXED_BYTE_LEN];
                rng.fill(&mut buf[..]);
                buf.into()
            });
        bench_list_memory(
            &mut group,
            null_density,
            pages,
            fixed32_list_wrapper,
            &fixed32_desc,
        );
    }
    group.finish();

    let mut group = c.benchmark_group(format!(
        "arrow_array_reader/ListArray_{measurement_name}/StringList"
    ));
    for null_density in [0.0, 0.5, 0.9, 0.99] {
        let pages = build_list_pages::<ByteArrayType>(string_desc.clone(), null_density, |rng| {
            let len = rng.random_range(5..50);
            let bytes: Vec<u8> = (0..len).map(|_| rng.random_range(b'a'..=b'z')).collect();
            bytes.into()
        });
        bench_list_memory(
            &mut group,
            null_density,
            pages,
            string_list_wrapper,
            &string_desc,
        );
    }
    group.finish();
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
