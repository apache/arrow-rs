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
use criterion::{BenchmarkId, Criterion};

use arrow_buffer::ScalarBuffer;
use rand::RngExt;

use arrow::compute::{TakeOptions, take, take_record_batch};
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use arrow::util::test_util::seedable_rng;
use arrow::{array::*, util::bench_util::*};
use std::hint;
use std::sync::Arc;

fn create_random_index(size: usize, null_density: f32) -> UInt32Array {
    let mut rng = seedable_rng();
    let mut builder = UInt32Builder::with_capacity(size);
    for _ in 0..size {
        if rng.random::<f32>() < null_density {
            builder.append_null();
        } else {
            let value = rng.random_range::<u32, _>(0u32..size as u32);
            builder.append_value(value);
        }
    }
    builder.finish()
}

fn bench_take(values: &dyn Array, indices: &UInt32Array) {
    hint::black_box(take(values, indices, None).unwrap());
}

fn create_columns(types: &[DataType], size: usize, null_density: f32) -> Vec<ArrayRef> {
    types
        .iter()
        .map(|dt| create_array_for_type(dt, size, null_density))
        .collect()
}

fn make_record_batch(columns: Vec<ArrayRef>) -> RecordBatch {
    let fields: Vec<_> = columns
        .iter()
        .enumerate()
        .map(|(i, col)| Field::new(format!("c{i}"), col.data_type().clone(), true))
        .collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

fn bench_take_record_batch(batch: &RecordBatch, indices: &UInt32Array) {
    hint::black_box(take_record_batch(batch, indices).unwrap());
}

fn bench_take_bounds_check(values: &dyn Array, indices: &UInt32Array) {
    hint::black_box(take(values, indices, Some(TakeOptions { check_bounds: true })).unwrap());
}

fn create_string_run_array(logical_len: usize, physical_len: usize) -> RunArray<Int32Type> {
    let strings = create_string_array_for_runs(physical_len, logical_len, 8);
    let mut builder = GenericByteRunBuilder::<Int32Type, Utf8Type>::new();
    for s in &strings {
        builder.append_value(s.as_str());
    }
    builder.finish()
}

fn create_sparse_union(size: usize) -> UnionArray {
    let mut rng = seedable_rng();
    let type_ids: ScalarBuffer<i8> = (0..size).map(|_| rng.random_range(0_i8..4)).collect();
    let int_array: Int32Array = (0..size).map(|_| Some(rng.random::<i32>())).collect();
    let float_array: Float64Array = (0..size).map(|_| Some(rng.random::<f64>())).collect();
    let string_array = StringArray::from_iter((0..size).map(|i| Some(format!("basic_string:{i}"))));
    let fsb_array = create_fsb_array(size, 0.0, 16);
    let fields = [
        (0, Arc::new(Field::new("a", DataType::Int32, false))),
        (1, Arc::new(Field::new("b", DataType::Float64, false))),
        (2, Arc::new(Field::new("c", DataType::Utf8, false))),
        (
            3,
            Arc::new(Field::new("d", DataType::FixedSizeBinary(16), false)),
        ),
    ]
    .into_iter()
    .collect::<UnionFields>();
    UnionArray::try_new(
        fields,
        type_ids,
        None,
        vec![
            Arc::new(int_array),
            Arc::new(float_array),
            Arc::new(string_array),
            Arc::new(fsb_array),
        ],
    )
    .unwrap()
}

fn create_dense_union(size: usize) -> UnionArray {
    let mut rng = seedable_rng();
    let mut int_vals: Vec<i32> = Vec::new();
    let mut float_vals: Vec<f64> = Vec::new();
    let mut fsb_vals: Vec<[u8; 16]> = Vec::new();
    let mut type_ids = Vec::with_capacity(size);
    let mut offsets = Vec::with_capacity(size);
    for _ in 0..size {
        let tid = rng.random_range(0_i8..3);
        type_ids.push(tid);
        match tid {
            0 => {
                offsets.push(int_vals.len() as i32);
                int_vals.push(rng.random());
            }
            1 => {
                offsets.push(float_vals.len() as i32);
                float_vals.push(rng.random());
            }
            _ => {
                offsets.push(fsb_vals.len() as i32);
                fsb_vals.push(rng.random());
            }
        }
    }
    let type_ids: ScalarBuffer<i8> = type_ids.into_iter().collect();
    let offsets: ScalarBuffer<i32> = offsets.into_iter().collect();
    let int_array: Int32Array = int_vals.into_iter().map(Some).collect();
    let float_array: Float64Array = float_vals.into_iter().map(Some).collect();
    let fsb_array = FixedSizeBinaryArray::try_from_iter(fsb_vals.into_iter()).unwrap();
    let fields = [
        (0, Arc::new(Field::new("a", DataType::Int32, false))),
        (1, Arc::new(Field::new("b", DataType::Float64, false))),
        (
            2,
            Arc::new(Field::new("c", DataType::FixedSizeBinary(16), false)),
        ),
    ]
    .into_iter()
    .collect::<UnionFields>();
    UnionArray::try_new(
        fields,
        type_ids,
        Some(offsets),
        vec![
            Arc::new(int_array),
            Arc::new(float_array),
            Arc::new(fsb_array),
        ],
    )
    .unwrap()
}

fn add_benchmark(c: &mut Criterion) {
    let values = create_primitive_array::<Int32Type>(512, 0.0);
    let indices = create_random_index(512, 0.0);
    c.bench_function("take i32 512", |b| b.iter(|| bench_take(&values, &indices)));

    let values = create_primitive_array::<Int32Type>(1024, 0.0);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take i32 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });
    let indices = create_random_index(1024, 0.5);
    c.bench_function("take i32 null indices 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_primitive_array::<Int32Type>(1024, 0.5);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take i32 null values 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });
    let indices = create_random_index(1024, 0.5);
    c.bench_function("take i32 null values null indices 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_primitive_array::<Int32Type>(512, 0.0);
    let indices = create_random_index(512, 0.0);
    c.bench_function("take check bounds i32 512", |b| {
        b.iter(|| bench_take_bounds_check(&values, &indices))
    });
    let values = create_primitive_array::<Int32Type>(1024, 0.0);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take check bounds i32 1024", |b| {
        b.iter(|| bench_take_bounds_check(&values, &indices))
    });

    let values = create_boolean_array(512, 0.0, 0.5);
    let indices = create_random_index(512, 0.0);
    c.bench_function("take bool 512", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_boolean_array(1024, 0.0, 0.5);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take bool 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let indices = create_random_index(1024, 0.5);
    c.bench_function("take bool null indices 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_boolean_array(1024, 0.5, 0.5);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take bool null values 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_boolean_array(1024, 0.5, 0.5);
    let indices = create_random_index(1024, 0.5);
    c.bench_function("take bool null values null indices 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_string_array::<i32>(512, 0.0);
    let indices = create_random_index(512, 0.0);
    c.bench_function("take str 512", |b| b.iter(|| bench_take(&values, &indices)));

    let values = create_string_array::<i32>(1024, 0.0);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take str 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_string_array::<i32>(512, 0.0);
    let indices = create_random_index(512, 0.5);
    c.bench_function("take str null indices 512", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_string_array::<i32>(1024, 0.0);
    let indices = create_random_index(1024, 0.5);
    c.bench_function("take str null indices 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_string_array::<i32>(1024, 0.5);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take str null values 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_string_array::<i32>(1024, 0.5);
    let indices = create_random_index(1024, 0.5);
    c.bench_function("take str null values null indices 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_string_view_array(512, 0.0);
    let indices = create_random_index(512, 0.0);
    c.bench_function("take stringview 512", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_string_view_array(1024, 0.0);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take stringview 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_string_view_array(512, 0.0);
    let indices = create_random_index(512, 0.5);
    c.bench_function("take stringview null indices 512", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_string_view_array(1024, 0.0);
    let indices = create_random_index(1024, 0.5);
    c.bench_function("take stringview null indices 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_string_view_array(1024, 0.5);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take stringview null values 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_string_view_array(1024, 0.5);
    let indices = create_random_index(1024, 0.5);
    c.bench_function("take stringview null values null indices 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_primitive_list_array::<i32, Int32Type>(512, 0.0, 0.0, 20);
    let indices = create_random_index(512, 0.0);
    c.bench_function("take list i32 512", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_primitive_list_array::<i32, Int32Type>(1024, 0.0, 0.0, 20);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take list i32 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_primitive_list_array::<i32, Int32Type>(1024, 0.5, 0.0, 20);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take list i32 null values 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_primitive_list_array::<i32, Int32Type>(1024, 0.0, 0.0, 20);
    let indices = create_random_index(1024, 0.5);
    c.bench_function("take list i32 null indices 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_primitive_list_array::<i32, Int32Type>(1024, 0.5, 0.5, 20);
    let indices = create_random_index(1024, 0.5);
    c.bench_function("take list i32 null values null indices 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_primitive_list_view_array::<i32, Int32Type>(512, 0.0, 0.0, 20);
    let indices = create_random_index(512, 0.0);
    c.bench_function("take listview i32 512", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_primitive_list_view_array::<i32, Int32Type>(1024, 0.0, 0.0, 20);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take listview i32 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_primitive_list_view_array::<i32, Int32Type>(1024, 0.5, 0.0, 20);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take listview i32 null values 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_primitive_list_view_array::<i32, Int32Type>(1024, 0.0, 0.0, 20);
    let indices = create_random_index(1024, 0.5);
    c.bench_function("take listview i32 null indices 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_primitive_list_view_array::<i32, Int32Type>(1024, 0.5, 0.5, 20);
    let indices = create_random_index(1024, 0.5);
    c.bench_function("take listview i32 null values null indices 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_primitive_run_array::<Int32Type, Int32Type>(1024, 512);
    let indices = create_random_index(1024, 0.0);
    c.bench_function(
        "take primitive run logical len: 1024, physical len: 512, indices: 1024",
        |b| b.iter(|| bench_take(&values, &indices)),
    );

    let values = create_string_run_array(1024, 128);
    let indices = create_random_index(1024, 0.0);
    c.bench_function(
        "take string run logical len: 1024, physical len: 128, indices: 1024",
        |b| b.iter(|| bench_take(&values, &indices)),
    );

    let values = create_string_run_array(1024, 512);
    let indices = create_random_index(1024, 0.0);
    c.bench_function(
        "take string run logical len: 1024, physical len: 512, indices: 1024",
        |b| b.iter(|| bench_take(&values, &indices)),
    );

    let values = create_string_run_array(1024, 512);
    let indices = create_random_index(1024, 0.5);
    c.bench_function(
        "take string run logical len: 1024, physical len: 512, null indices: 1024",
        |b| b.iter(|| bench_take(&values, &indices)),
    );

    let values = create_fsb_array(1024, 0.0, 12);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take fsb value len: 12, indices: 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_fsb_array(1024, 0.5, 12);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take fsb value len: 12, null values, indices: 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_fsb_array(1024, 0.0, 16);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take fsb value optimized len: 16, indices: 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_fsb_array(1024, 0.5, 16);
    let indices = create_random_index(1024, 0.0);
    c.bench_function(
        "take fsb value optimized len: 16, null values, indices: 1024",
        |b| b.iter(|| bench_take(&values, &indices)),
    );

    let types = [
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
        DataType::Boolean,
    ];
    let batch = make_record_batch(create_columns(&types, 1024, 0.0));
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take_record_batch 5 primitive cols no nulls 1024", |b| {
        b.iter(|| bench_take_record_batch(&batch, &indices))
    });

    let types = [
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Utf8View,
        DataType::Binary,
        DataType::LargeBinary,
        DataType::FixedSizeBinary(16),
    ];
    let batch = make_record_batch(create_columns(&types, 1024, 0.0));
    let indices = create_random_index(1024, 0.0);
    c.bench_function(
        "take_record_batch 6 string/binary cols no nulls 1024",
        |b| b.iter(|| bench_take_record_batch(&batch, &indices)),
    );

    let types = [
        DataType::Int32,
        DataType::Utf8,
        DataType::Float64,
        DataType::Boolean,
        DataType::Utf8View,
        DataType::Int64,
        DataType::Binary,
    ];
    let batch = make_record_batch(create_columns(&types, 1024, 0.5));
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take_record_batch 7 mixed cols null values 1024", |b| {
        b.iter(|| bench_take_record_batch(&batch, &indices))
    });

    let types = [
        DataType::Int32,
        DataType::Utf8,
        DataType::Float64,
        DataType::Boolean,
        DataType::Utf8View,
        DataType::Int64,
        DataType::Binary,
    ];
    let batch = make_record_batch(create_columns(&types, 1024, 0.5));
    let indices = create_random_index(1024, 0.5);
    c.bench_function(
        "take_record_batch 7 mixed cols null values null indices 1024",
        |b| b.iter(|| bench_take_record_batch(&batch, &indices)),
    );

    // FixedSizeList — list_size=8 (power-of-two) and list_size=22 (arbitrary/dynamic length)
    let values = create_primitive_fixed_size_list_array::<Int32Type>(1024, 0.0, 0.0, 8);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take fixed_size_list<i32>[8] 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_primitive_fixed_size_list_array::<Int32Type>(1024, 0.5, 0.0, 8);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take fixed_size_list<i32>[8] null values 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_primitive_fixed_size_list_array::<Int32Type>(1024, 0.0, 0.0, 8);
    let indices = create_random_index(1024, 0.5);
    c.bench_function("take fixed_size_list<i32>[8] null indices 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_primitive_fixed_size_list_array::<Int32Type>(1024, 0.0, 0.0, 22);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take fixed_size_list<i32>[22] 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_primitive_fixed_size_list_array::<Int32Type>(1024, 0.5, 0.0, 22);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take fixed_size_list<i32>[22] null values 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_primitive_fixed_size_list_array::<Int32Type>(1024, 0.0, 0.0, 22);
    let indices = create_random_index(1024, 0.5);
    c.bench_function("take fixed_size_list<i32>[22] null indices 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    // Map
    let values = create_string_map_array::<Int32Type>(512, 0.0, 10, 8);
    let indices = create_random_index(512, 0.0);
    c.bench_function("take map<str, i32> 512", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_string_map_array::<Int32Type>(1024, 0.0, 10, 8);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take map<str, i32> 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_string_map_array::<Int32Type>(1024, 0.5, 10, 8);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take map<str, i32> null values 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_string_map_array::<Int32Type>(1024, 0.0, 10, 8);
    let indices = create_random_index(1024, 0.5);
    c.bench_function("take map<str, i32> null indices 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_sparse_union(512);
    let indices = create_random_index(512, 0.0);
    c.bench_function("take sparse union 512", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_sparse_union(1024);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take sparse union 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_dense_union(512);
    let indices = create_random_index(512, 0.0);
    c.bench_function("take dense union 512", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_dense_union(1024);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take dense union 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });
}

// ---------------------------------------------------------------------------
// Repartition index-scan benchmarks
// ---------------------------------------------------------------------------

const REPARTITION_INNER_ITERS: usize = 100;
const REPARTITION_BATCH_SIZES: &[usize] = &[8_192, 16_384];
const REPARTITION_PARTITION_COUNTS: &[usize] = &[4, 8, 16, 32, 64, 128, 256, 512];

fn repartition_fnv1a(mut x: u64) -> u64 {
    const PRIME: u64 = 0x00000100000001B3;
    const BASIS: u64 = 0xcbf29ce484222325;
    let mut h = BASIS;
    for _ in 0..8 {
        h ^= x & 0xFF;
        h = h.wrapping_mul(PRIME);
        x >>= 8;
    }
    h
}

/// `assignment[i]` = which output partition row i belongs to.
fn make_partition_assignment(num_rows: usize, num_partitions: usize) -> Vec<u32> {
    let mask = (num_partitions - 1) as u64;
    let mut out = Vec::with_capacity(num_rows);
    // SAFETY: every element is written before the length is exposed.
    unsafe { out.set_len(num_rows) };
    for i in 0..num_rows {
        out[i] = (repartition_fnv1a(i as u64) & mask) as u32;
    }
    out
}

fn repartition_scan_scalar(assignment: &[u32], p: u32) -> Vec<u32> {
    assignment
        .iter()
        .enumerate()
        .filter(|&(_, &v)| v == p)
        .map(|(i, _)| i as u32)
        .collect()
}

/// Write unconditionally, advance output pointer only on match — no branch per element.
fn repartition_scan_branchless(assignment: &[u32], p: u32) -> Vec<u32> {
    let mut out = Vec::with_capacity(assignment.len());
    // SAFETY: exactly `count` elements are initialised before truncation.
    unsafe { out.set_len(assignment.len()) };
    let mut count = 0usize;
    for (i, &v) in assignment.iter().enumerate() {
        unsafe { *out.get_unchecked_mut(count) = i as u32 };
        count += (v == p) as usize;
    }
    unsafe { out.set_len(count) };
    out
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn repartition_scan_avx2_inner(assignment: &[u32], p: u32) -> Vec<u32> {
    use std::arch::x86_64::*;
    let mut out = Vec::with_capacity(assignment.len());
    let target = _mm256_set1_epi32(p as i32);
    let chunks = assignment.len() / 8;
    for c in 0..chunks {
        let ptr = assignment.as_ptr().add(c * 8) as *const __m256i;
        let data = _mm256_loadu_si256(ptr);
        let eq = _mm256_cmpeq_epi32(data, target);
        let mut mask = _mm256_movemask_ps(_mm256_castsi256_ps(eq)) as u8;
        let base = (c * 8) as u32;
        while mask != 0 {
            let bit = mask.trailing_zeros();
            out.push(base + bit);
            mask &= mask - 1;
        }
    }
    for i in (chunks * 8)..assignment.len() {
        if *assignment.get_unchecked(i) == p {
            out.push(i as u32);
        }
    }
    out
}

fn repartition_scan_simd(assignment: &[u32], p: u32) -> Vec<u32> {
    #[cfg(target_arch = "x86_64")]
    if is_x86_feature_detected!("avx2") {
        return unsafe { repartition_scan_avx2_inner(assignment, p) };
    }
    repartition_scan_scalar(assignment, p)
}

fn repartition_scan_prefetch(assignment: &[u32], p: u32) -> Vec<u32> {
    const DIST: usize = 16;
    let mut out = Vec::with_capacity(assignment.len());
    let len = assignment.len();
    for i in 0..len {
        #[cfg(target_arch = "x86_64")]
        if i + DIST < len {
            unsafe {
                std::arch::x86_64::_mm_prefetch(
                    assignment.as_ptr().add(i + DIST) as *const i8,
                    std::arch::x86_64::_MM_HINT_T0,
                );
            }
        }
        if assignment[i] == p {
            out.push(i as u32);
        }
    }
    out
}

fn bench_repartition_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("repartition/write/index_assignment");
    for &num_rows in REPARTITION_BATCH_SIZES {
        for &num_partitions in REPARTITION_PARTITION_COUNTS {
            let id = format!("rows={num_rows}/partitions={num_partitions}");
            group.bench_with_input(
                BenchmarkId::new("clone_arc", &id),
                &(num_rows, num_partitions),
                |b, &(num_rows, num_partitions)| {
                    b.iter(|| {
                        for _ in 0..REPARTITION_INNER_ITERS {
                            let assignment =
                                Arc::new(make_partition_assignment(num_rows, num_partitions));
                            for _ in 0..num_partitions {
                                hint::black_box(Arc::clone(&assignment));
                            }
                        }
                    })
                },
            );
        }
    }
    group.finish();
}

fn bench_repartition_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("repartition/read/partition_index_scan");
    for &num_rows in REPARTITION_BATCH_SIZES {
        for &num_partitions in REPARTITION_PARTITION_COUNTS {
            let assignment = make_partition_assignment(num_rows, num_partitions);
            let id = format!("rows={num_rows}/partitions={num_partitions}");

            group.bench_with_input(
                BenchmarkId::new("scalar", &id),
                &num_partitions,
                |b, &num_partitions| {
                    b.iter(|| {
                        for _ in 0..REPARTITION_INNER_ITERS {
                            for p in 0..num_partitions as u32 {
                                hint::black_box(repartition_scan_scalar(&assignment, p));
                            }
                        }
                    })
                },
            );

            group.bench_with_input(
                BenchmarkId::new("branchless", &id),
                &num_partitions,
                |b, &num_partitions| {
                    b.iter(|| {
                        for _ in 0..REPARTITION_INNER_ITERS {
                            for p in 0..num_partitions as u32 {
                                hint::black_box(repartition_scan_branchless(&assignment, p));
                            }
                        }
                    })
                },
            );

            group.bench_with_input(
                BenchmarkId::new("simd_avx2", &id),
                &num_partitions,
                |b, &num_partitions| {
                    b.iter(|| {
                        for _ in 0..REPARTITION_INNER_ITERS {
                            for p in 0..num_partitions as u32 {
                                hint::black_box(repartition_scan_simd(&assignment, p));
                            }
                        }
                    })
                },
            );

            group.bench_with_input(
                BenchmarkId::new("prefetch", &id),
                &num_partitions,
                |b, &num_partitions| {
                    b.iter(|| {
                        for _ in 0..REPARTITION_INNER_ITERS {
                            for p in 0..num_partitions as u32 {
                                hint::black_box(repartition_scan_prefetch(&assignment, p));
                            }
                        }
                    })
                },
            );
        }
    }
    group.finish();
}

criterion_group!(benches, add_benchmark, bench_repartition_write, bench_repartition_read);
criterion_main!(benches);
