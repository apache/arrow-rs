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
use arrow::datatypes::{Int64Type, UInt64Type};
use arrow::row::{RowConverter, SortField};
use arrow::util::bench_util::{
    create_boolean_array, create_dict_from_values, create_primitive_array,
    create_string_array_with_len, create_string_dict_array, create_string_view_array_with_len,
};
use arrow_array::types::Int32Type;
use arrow_array::Array;
use criterion::{black_box, Criterion};
use std::sync::Arc;

fn do_bench(c: &mut Criterion, name: &str, cols: Vec<ArrayRef>) {
    let fields: Vec<_> = cols
        .iter()
        .map(|x| SortField::new(x.data_type().clone()))
        .collect();

    c.bench_function(&format!("convert_columns {name}"), |b| {
        b.iter(|| {
            let converter = RowConverter::new(fields.clone()).unwrap();
            black_box(converter.convert_columns(&cols).unwrap())
        });
    });

    let converter = RowConverter::new(fields).unwrap();
    let rows = converter.convert_columns(&cols).unwrap();
    // using a pre-prepared row converter should be faster than the first time
    c.bench_function(&format!("convert_columns_prepared {name}"), |b| {
        b.iter(|| black_box(converter.convert_columns(&cols).unwrap()));
    });

    c.bench_function(&format!("convert_rows {name}"), |b| {
        b.iter(|| black_box(converter.convert_rows(&rows).unwrap()));
    });
}

fn row_bench(c: &mut Criterion) {
    let cols = vec![Arc::new(create_primitive_array::<UInt64Type>(4096, 0.)) as ArrayRef];
    do_bench(c, "4096 u64(0)", cols);

    let cols = vec![Arc::new(create_primitive_array::<UInt64Type>(4096, 0.3)) as ArrayRef];
    do_bench(c, "4096 u64(0.3)", cols);

    let cols = vec![Arc::new(create_primitive_array::<Int64Type>(4096, 0.)) as ArrayRef];
    do_bench(c, "4096 i64(0)", cols);

    let cols = vec![Arc::new(create_primitive_array::<Int64Type>(4096, 0.3)) as ArrayRef];
    do_bench(c, "4096 i64(0.3)", cols);

    let cols = vec![Arc::new(create_boolean_array(4096, 0., 0.5)) as ArrayRef];
    do_bench(c, "4096 bool(0, 0.5)", cols);

    let cols = vec![Arc::new(create_boolean_array(4096, 0.3, 0.5)) as ArrayRef];
    do_bench(c, "4096 bool(0.3, 0.5)", cols);

    let cols = vec![Arc::new(create_string_array_with_len::<i32>(4096, 0., 10)) as ArrayRef];
    do_bench(c, "4096 string(10, 0)", cols);

    let cols = vec![Arc::new(create_string_array_with_len::<i32>(4096, 0., 30)) as ArrayRef];
    do_bench(c, "4096 string(30, 0)", cols);

    let cols = vec![Arc::new(create_string_array_with_len::<i32>(4096, 0., 100)) as ArrayRef];
    do_bench(c, "4096 string(100, 0)", cols);

    let cols = vec![Arc::new(create_string_array_with_len::<i32>(4096, 0.5, 100)) as ArrayRef];
    do_bench(c, "4096 string(100, 0.5)", cols);

    let cols = vec![Arc::new(create_string_view_array_with_len(4096, 0., 10, false)) as ArrayRef];
    do_bench(c, "4096 string view(10, 0)", cols);

    let cols = vec![Arc::new(create_string_view_array_with_len(4096, 0., 30, false)) as ArrayRef];
    do_bench(c, "4096 string view(30, 0)", cols);

    let cols = vec![Arc::new(create_string_view_array_with_len(4096, 0., 100, false)) as ArrayRef];
    do_bench(c, "4096 string view(100, 0)", cols);

    let cols = vec![Arc::new(create_string_view_array_with_len(4096, 0.5, 100, false)) as ArrayRef];
    do_bench(c, "4096 string view(100, 0.5)", cols);

    let cols = vec![Arc::new(create_string_dict_array::<Int32Type>(4096, 0., 10)) as ArrayRef];
    do_bench(c, "4096 string_dictionary(10, 0)", cols);

    let cols = vec![Arc::new(create_string_dict_array::<Int32Type>(4096, 0., 30)) as ArrayRef];
    do_bench(c, "4096 string_dictionary(30, 0)", cols);

    let cols = vec![Arc::new(create_string_dict_array::<Int32Type>(4096, 0., 100)) as ArrayRef];
    do_bench(c, "4096 string_dictionary(100, 0)", cols.clone());

    let cols = vec![Arc::new(create_string_dict_array::<Int32Type>(4096, 0.5, 100)) as ArrayRef];
    do_bench(c, "4096 string_dictionary(100, 0.5)", cols.clone());

    let values = create_string_array_with_len::<i32>(10, 0., 10);
    let dict = create_dict_from_values::<Int32Type>(4096, 0., &values);
    let cols = vec![Arc::new(dict) as ArrayRef];
    do_bench(c, "4096 string_dictionary_low_cardinality(10, 0)", cols);

    let values = create_string_array_with_len::<i32>(10, 0., 30);
    let dict = create_dict_from_values::<Int32Type>(4096, 0., &values);
    let cols = vec![Arc::new(dict) as ArrayRef];
    do_bench(c, "4096 string_dictionary_low_cardinality(30, 0)", cols);

    let values = create_string_array_with_len::<i32>(10, 0., 100);
    let dict = create_dict_from_values::<Int32Type>(4096, 0., &values);
    let cols = vec![Arc::new(dict) as ArrayRef];
    do_bench(c, "4096 string_dictionary_low_cardinality(100, 0)", cols);

    let cols = vec![
        Arc::new(create_string_array_with_len::<i32>(4096, 0.5, 20)) as ArrayRef,
        Arc::new(create_string_array_with_len::<i32>(4096, 0., 30)) as ArrayRef,
        Arc::new(create_string_array_with_len::<i32>(4096, 0., 100)) as ArrayRef,
        Arc::new(create_primitive_array::<Int64Type>(4096, 0.)) as ArrayRef,
    ];
    do_bench(
        c,
        "4096 string(20, 0.5), string(30, 0), string(100, 0), i64(0)",
        cols,
    );

    let cols = vec![
        Arc::new(create_string_dict_array::<Int32Type>(4096, 0.5, 20)) as ArrayRef,
        Arc::new(create_string_dict_array::<Int32Type>(4096, 0., 30)) as ArrayRef,
        Arc::new(create_string_dict_array::<Int32Type>(4096, 0., 100)) as ArrayRef,
        Arc::new(create_primitive_array::<Int64Type>(4096, 0.)) as ArrayRef,
    ];
    do_bench(c, "4096 4096 string_dictionary(20, 0.5), string_dictionary(30, 0), string_dictionary(100, 0), i64(0)", cols);
}

criterion_group!(benches, row_bench);
criterion_main!(benches);
