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
use criterion::Criterion;

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

    let values = create_primitive_list_array::<i32, Int32Type>(1024, 0.0, 0.0, 202);
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
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
