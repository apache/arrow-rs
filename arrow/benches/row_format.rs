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
extern crate core;

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Int64Type, UInt64Type};
use arrow::row::{RowConverter, SortField};
use arrow::util::bench_util::{
    create_primitive_array, create_string_array_with_len, create_string_dict_array,
};
use arrow_array::types::Int32Type;
use criterion::{black_box, Criterion};
use std::sync::Arc;

fn row_bench(c: &mut Criterion) {
    let cols = vec![Arc::new(create_primitive_array::<UInt64Type>(4096, 0.)) as ArrayRef];

    c.bench_function("row_batch 4096 u64(0)", |b| {
        b.iter(|| {
            let mut converter = RowConverter::new(vec![SortField::new(DataType::UInt64)]);
            black_box(converter.convert_columns(&cols))
        });
    });

    let cols = vec![Arc::new(create_primitive_array::<Int64Type>(4096, 0.)) as ArrayRef];

    c.bench_function("row_batch 4096 i64(0)", |b| {
        b.iter(|| {
            let mut converter = RowConverter::new(vec![SortField::new(DataType::Int64)]);
            black_box(converter.convert_columns(&cols))
        });
    });

    let cols =
        vec![Arc::new(create_string_array_with_len::<i32>(4096, 0., 10)) as ArrayRef];

    c.bench_function("row_batch 4096 string(10, 0)", |b| {
        b.iter(|| {
            let mut converter = RowConverter::new(vec![SortField::new(DataType::Utf8)]);
            black_box(converter.convert_columns(&cols))
        });
    });

    let cols =
        vec![Arc::new(create_string_array_with_len::<i32>(4096, 0., 30)) as ArrayRef];

    c.bench_function("row_batch 4096 string(30, 0)", |b| {
        b.iter(|| {
            let mut converter = RowConverter::new(vec![SortField::new(DataType::Utf8)]);
            black_box(converter.convert_columns(&cols))
        });
    });

    let cols =
        vec![Arc::new(create_string_array_with_len::<i32>(4096, 0., 100)) as ArrayRef];

    c.bench_function("row_batch 4096 string(100, 0)", |b| {
        b.iter(|| {
            let mut converter = RowConverter::new(vec![SortField::new(DataType::Utf8)]);
            black_box(converter.convert_columns(&cols))
        });
    });

    let cols =
        vec![Arc::new(create_string_array_with_len::<i32>(4096, 0.5, 100)) as ArrayRef];

    c.bench_function("row_batch 4096 string(100, 0.5)", |b| {
        b.iter(|| {
            let mut converter = RowConverter::new(vec![SortField::new(DataType::Utf8)]);
            black_box(converter.convert_columns(&cols))
        });
    });

    let cols =
        vec![Arc::new(create_string_dict_array::<Int32Type>(4096, 0., 10)) as ArrayRef];

    c.bench_function("row_batch 4096 string_dictionary(10, 0)", |b| {
        b.iter(|| {
            let mut converter = RowConverter::new(vec![SortField::new(DataType::Utf8)]);
            black_box(converter.convert_columns(&cols))
        });
    });

    let cols =
        vec![Arc::new(create_string_dict_array::<Int32Type>(4096, 0., 30)) as ArrayRef];

    c.bench_function("row_batch 4096 string_dictionary(30, 0)", |b| {
        b.iter(|| {
            let mut converter = RowConverter::new(vec![SortField::new(DataType::Utf8)]);
            black_box(converter.convert_columns(&cols))
        });
    });

    let cols =
        vec![Arc::new(create_string_dict_array::<Int32Type>(4096, 0., 100)) as ArrayRef];

    c.bench_function("row_batch 4096 string_dictionary(100, 0)", |b| {
        b.iter(|| {
            let mut converter = RowConverter::new(vec![SortField::new(DataType::Utf8)]);
            black_box(converter.convert_columns(&cols))
        });
    });

    let cols =
        vec![Arc::new(create_string_dict_array::<Int32Type>(4096, 0.5, 100)) as ArrayRef];

    c.bench_function("row_batch 4096 string_dictionary(100, 0.5)", |b| {
        b.iter(|| {
            let mut converter = RowConverter::new(vec![SortField::new(DataType::Utf8)]);
            black_box(converter.convert_columns(&cols))
        });
    });

    let cols = [
        Arc::new(create_string_array_with_len::<i32>(4096, 0.5, 20)) as ArrayRef,
        Arc::new(create_string_array_with_len::<i32>(4096, 0., 30)) as ArrayRef,
        Arc::new(create_string_array_with_len::<i32>(4096, 0., 100)) as ArrayRef,
        Arc::new(create_primitive_array::<Int64Type>(4096, 0.)) as ArrayRef,
    ];

    let fields = [
        SortField::new(DataType::Utf8),
        SortField::new(DataType::Utf8),
        SortField::new(DataType::Utf8),
        SortField::new(DataType::Int64),
    ];

    c.bench_function(
        "row_batch 4096 string(20, 0.5), string(30, 0), string(100, 0), i64(0)",
        |b| {
            b.iter(|| {
                let mut converter = RowConverter::new(fields.to_vec());
                black_box(converter.convert_columns(&cols))
            });
        },
    );

    let cols = [
        Arc::new(create_string_dict_array::<Int32Type>(4096, 0.5, 20)) as ArrayRef,
        Arc::new(create_string_dict_array::<Int32Type>(4096, 0., 30)) as ArrayRef,
        Arc::new(create_string_dict_array::<Int32Type>(4096, 0., 100)) as ArrayRef,
        Arc::new(create_primitive_array::<Int64Type>(4096, 0.)) as ArrayRef,
    ];

    let fields = [
        SortField::new(DataType::Utf8),
        SortField::new(DataType::Utf8),
        SortField::new(DataType::Utf8),
        SortField::new(DataType::Int64),
    ];

    c.bench_function(
        "row_batch 4096 string_dictionary(20, 0.5), string_dictionary(30, 0), string_dictionary(100, 0), i64(0)",
        |b| {
            b.iter(|| {
                let mut converter = RowConverter::new(fields.to_vec());
                black_box(converter.convert_columns(&cols))
            });
        },
    );
}

criterion_group!(benches, row_bench);
criterion_main!(benches);
