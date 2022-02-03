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
extern crate arrow;

use std::sync::Arc;

use arrow::compute::{filter_record_batch, FilterBuilder, FilterPredicate};
use arrow::record_batch::RecordBatch;
use arrow::util::bench_util::*;

use arrow::array::*;
use arrow::compute::filter;
use arrow::datatypes::{Field, Float32Type, Int32Type, Schema, UInt8Type};

use criterion::{criterion_group, criterion_main, Criterion};

fn bench_filter(data_array: &dyn Array, filter_array: &BooleanArray) {
    criterion::black_box(filter(data_array, filter_array).unwrap());
}

fn bench_built_filter(filter: &FilterPredicate, array: &dyn Array) {
    criterion::black_box(filter.filter(array).unwrap());
}

fn add_benchmark(c: &mut Criterion) {
    let size = 65536;
    let filter_array = create_boolean_array(size, 0.0, 0.5);
    let dense_filter_array = create_boolean_array(size, 0.0, 1.0 - 1.0 / 1024.0);
    let sparse_filter_array = create_boolean_array(size, 0.0, 1.0 / 1024.0);

    let filter = FilterBuilder::new(&filter_array).optimize().build();
    let dense_filter = FilterBuilder::new(&dense_filter_array).optimize().build();
    let sparse_filter = FilterBuilder::new(&sparse_filter_array).optimize().build();

    let data_array = create_primitive_array::<UInt8Type>(size, 0.0);

    c.bench_function("filter optimize (1/2)", |b| {
        b.iter(|| FilterBuilder::new(&filter_array).optimize().build())
    });

    c.bench_function("filter optimize high selectivity (1023/1024)", |b| {
        b.iter(|| FilterBuilder::new(&dense_filter_array).optimize().build())
    });

    c.bench_function("filter optimize low selectivity (1/1024)", |b| {
        b.iter(|| FilterBuilder::new(&sparse_filter_array).optimize().build())
    });

    c.bench_function("filter u8 (1/2)", |b| {
        b.iter(|| bench_filter(&data_array, &filter_array))
    });
    c.bench_function("filter u8 high selectivity (1023/1024)", |b| {
        b.iter(|| bench_filter(&data_array, &dense_filter_array))
    });
    c.bench_function("filter u8 low selectivity (1/1024)", |b| {
        b.iter(|| bench_filter(&data_array, &sparse_filter_array))
    });

    c.bench_function("filter context u8 (1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function("filter context u8 high selectivity (1023/1024)", |b| {
        b.iter(|| bench_built_filter(&dense_filter, &data_array))
    });
    c.bench_function("filter context u8 low selectivity (1/1024)", |b| {
        b.iter(|| bench_built_filter(&sparse_filter, &data_array))
    });

    let data_array = create_primitive_array::<Int32Type>(size, 0.0);
    c.bench_function("filter i32 (1/2)", |b| {
        b.iter(|| bench_filter(&data_array, &filter_array))
    });
    c.bench_function("filter i32 high selectivity (1023/1024)", |b| {
        b.iter(|| bench_filter(&data_array, &dense_filter_array))
    });
    c.bench_function("filter i32 low selectivity (1/1024)", |b| {
        b.iter(|| bench_filter(&data_array, &sparse_filter_array))
    });

    c.bench_function("filter context i32 (1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function("filter context i32 high selectivity (1023/1024)", |b| {
        b.iter(|| bench_built_filter(&dense_filter, &data_array))
    });
    c.bench_function("filter context i32 low selectivity (1/1024)", |b| {
        b.iter(|| bench_built_filter(&sparse_filter, &data_array))
    });

    let data_array = create_primitive_array::<Int32Type>(size, 0.5);
    c.bench_function("filter context i32 w NULLs (1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function(
        "filter context i32 w NULLs high selectivity (1023/1024)",
        |b| b.iter(|| bench_built_filter(&dense_filter, &data_array)),
    );
    c.bench_function("filter context i32 w NULLs low selectivity (1/1024)", |b| {
        b.iter(|| bench_built_filter(&sparse_filter, &data_array))
    });

    let data_array = create_primitive_array::<UInt8Type>(size, 0.5);
    c.bench_function("filter context u8 w NULLs (1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function(
        "filter context u8 w NULLs high selectivity (1023/1024)",
        |b| b.iter(|| bench_built_filter(&dense_filter, &data_array)),
    );
    c.bench_function("filter context u8 w NULLs low selectivity (1/1024)", |b| {
        b.iter(|| bench_built_filter(&sparse_filter, &data_array))
    });

    let data_array = create_primitive_array::<Float32Type>(size, 0.5);
    c.bench_function("filter f32 (1/2)", |b| {
        b.iter(|| bench_filter(&data_array, &filter_array))
    });
    c.bench_function("filter context f32 (1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function("filter context f32 high selectivity (1023/1024)", |b| {
        b.iter(|| bench_built_filter(&dense_filter, &data_array))
    });
    c.bench_function("filter context f32 low selectivity (1/1024)", |b| {
        b.iter(|| bench_built_filter(&sparse_filter, &data_array))
    });

    let data_array = create_string_array::<i32>(size, 0.5);
    c.bench_function("filter context string (1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function("filter context string high selectivity (1023/1024)", |b| {
        b.iter(|| bench_built_filter(&dense_filter, &data_array))
    });
    c.bench_function("filter context string low selectivity (1/1024)", |b| {
        b.iter(|| bench_built_filter(&sparse_filter, &data_array))
    });

    let data_array = create_string_dict_array::<Int32Type>(size, 0.0);
    c.bench_function("filter context string dictionary (1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function(
        "filter context string dictionary high selectivity (1023/1024)",
        |b| b.iter(|| bench_built_filter(&dense_filter, &data_array)),
    );
    c.bench_function(
        "filter context string dictionary low selectivity (1/1024)",
        |b| b.iter(|| bench_built_filter(&sparse_filter, &data_array)),
    );

    let data_array = create_string_dict_array::<Int32Type>(size, 0.5);
    c.bench_function("filter context string dictionary w NULLs (1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function(
        "filter context string dictionary w NULLs high selectivity (1023/1024)",
        |b| b.iter(|| bench_built_filter(&dense_filter, &data_array)),
    );
    c.bench_function(
        "filter context string dictionary w NULLs low selectivity (1/1024)",
        |b| b.iter(|| bench_built_filter(&sparse_filter, &data_array)),
    );

    let data_array = create_primitive_array::<Float32Type>(size, 0.0);

    let field = Field::new("c1", data_array.data_type().clone(), true);
    let schema = Schema::new(vec![field]);

    let batch =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(data_array)]).unwrap();

    c.bench_function("filter single record batch", |b| {
        b.iter(|| filter_record_batch(&batch, &filter_array))
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
