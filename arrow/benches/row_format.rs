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
    create_boolean_array, create_boolean_array_with_seed, create_dict_from_values,
    create_f64_array_with_seed, create_primitive_array, create_primitive_array_with_seed,
    create_string_array_with_len, create_string_array_with_len_range_and_prefix_and_seed,
    create_string_dict_array, create_string_view_array_with_len,
    create_string_view_array_with_max_len,
};
use arrow::util::data_gen::create_random_array;
use arrow_array::Array;
use arrow_array::types::{Int8Type, Int32Type};
use arrow_schema::{DataType, Field};
use criterion::Criterion;
use std::{hint, sync::Arc};

fn do_bench(c: &mut Criterion, name: &str, cols: Vec<ArrayRef>) {
    let fields: Vec<_> = cols
        .iter()
        .map(|x| SortField::new(x.data_type().clone()))
        .collect();

    c.bench_function(&format!("convert_columns {name}"), |b| {
        b.iter(|| {
            let converter = RowConverter::new(fields.clone()).unwrap();
            hint::black_box(converter.convert_columns(&cols).unwrap())
        });
    });

    let converter = RowConverter::new(fields).unwrap();
    let rows = converter.convert_columns(&cols).unwrap();
    // using a pre-prepared row converter should be faster than the first time
    c.bench_function(&format!("convert_columns_prepared {name}"), |b| {
        b.iter(|| hint::black_box(converter.convert_columns(&cols).unwrap()));
    });

    c.bench_function(&format!("convert_rows {name}"), |b| {
        b.iter(|| hint::black_box(converter.convert_rows(&rows).unwrap()));
    });

    let mut rows = converter.empty_rows(0, 0);
    c.bench_function(&format!("append_rows {name}"), |b| {
        let cols = cols.clone();
        b.iter(|| {
            rows.clear();
            converter.append(&mut rows, &cols).unwrap();
            hint::black_box(&mut rows);
        });
    });
}

fn bench_iter(c: &mut Criterion) {
    let col = create_string_view_array_with_len(4096, 0., 100, false);
    let converter = RowConverter::new(vec![SortField::new(col.data_type().clone())]).unwrap();
    let rows = converter
        .convert_columns(&[Arc::new(col) as ArrayRef])
        .unwrap();

    c.bench_function("iterate rows", |b| {
        b.iter(|| {
            for r in rows.iter() {
                hint::black_box(r.as_ref());
            }
        })
    });
}

/// A single benchmark with a medium number of columns (around 50) without nested columns for real-world use cases
/// This also makes sure there is a large gap between each value in the column and how it is laid out in the row format.
/// and it is on the edge of not fitting in L3 on some machines
fn run_benchmark_on_medium_amount_and_types_of_columns_without_nesting(
    batch_size: usize,
    c: &mut Criterion,
) {
    let mut seed = 0;

    let mut cols: Vec<ArrayRef> = vec![];

    for nulls in [0.0, 0.1, 0.2, 0.5] {
        seed += 1;
        cols.push(Arc::new(create_primitive_array_with_seed::<Int8Type>(
            batch_size, nulls, seed,
        )) as ArrayRef);
    }

    for nulls in [0.0, 0.1, 0.2, 0.5] {
        seed += 1;
        cols.push(Arc::new(create_primitive_array_with_seed::<Int32Type>(
            batch_size, nulls, seed,
        )) as ArrayRef);
    }

    for nulls in [0.0, 0.1, 0.2, 0.5] {
        seed += 1;
        cols.push(Arc::new(create_primitive_array_with_seed::<Int64Type>(
            batch_size, nulls, seed,
        )) as ArrayRef);
    }

    for _ in 0..10 {
        seed += 1;
        cols.push(Arc::new(create_primitive_array_with_seed::<Int64Type>(
            batch_size, 0.0, seed,
        )) as ArrayRef);
    }

    for nulls in [0.0, 0.1, 0.2, 0.5] {
        seed += 1;
        cols.push(Arc::new(
            create_string_array_with_len_range_and_prefix_and_seed::<i32>(
                batch_size, nulls, 0, 50, "", seed,
            ),
        ));
    }

    for _ in 0..3 {
        seed += 1;
        cols.push(Arc::new(
            create_string_array_with_len_range_and_prefix_and_seed::<i32>(
                batch_size, 0.0, 0, 10, "", seed,
            ),
        ));
    }
    for _ in 0..3 {
        seed += 1;
        cols.push(Arc::new(
            create_string_array_with_len_range_and_prefix_and_seed::<i32>(
                batch_size, 0.0, 10, 20, "", seed,
            ),
        ));
    }
    for _ in 0..3 {
        seed += 1;
        cols.push(Arc::new(
            create_string_array_with_len_range_and_prefix_and_seed::<i32>(
                batch_size, 0.0, 20, 30, "", seed,
            ),
        ));
    }

    for nulls in [0.0, 0.1, 0.2, 0.5] {
        seed += 1;
        cols.push(Arc::new(create_boolean_array_with_seed(
            batch_size, nulls, 0.5, seed,
        )));
    }

    for _ in 0..10 {
        seed += 1;
        cols.push(Arc::new(create_primitive_array_with_seed::<Int64Type>(
            batch_size, 0.0, seed,
        )) as ArrayRef);
    }

    for nulls in [0.0, 0.1, 0.2, 0.5] {
        seed += 1;
        cols.push(Arc::new(create_f64_array_with_seed(batch_size, nulls, seed)) as ArrayRef);
    }

    assert_eq!(cols.len(), 53);
    do_bench(c, format!("{batch_size} 53 columns").as_str(), cols);
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

    let cols = vec![Arc::new(create_string_view_array_with_max_len(4096, 0., 100)) as ArrayRef];
    do_bench(c, "4096 string view(1..100, 0)", cols);

    let cols = vec![Arc::new(create_string_view_array_with_max_len(4096, 0.5, 100)) as ArrayRef];
    do_bench(c, "4096 string view(1..100, 0.5)", cols);

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
    do_bench(
        c,
        "4096 4096 string_dictionary(20, 0.5), string_dictionary(30, 0), string_dictionary(100, 0), i64(0)",
        cols,
    );

    // List

    let cols = vec![
        create_random_array(
            &Field::new(
                "list",
                DataType::List(Arc::new(Field::new_list_field(DataType::UInt64, false))),
                false,
            ),
            4096,
            0.,
            1.0,
        )
        .unwrap(),
    ];
    do_bench(c, "4096 list(0) of u64(0)", cols);

    let cols = vec![
        create_random_array(
            &Field::new(
                "list",
                DataType::LargeList(Arc::new(Field::new_list_field(DataType::UInt64, false))),
                false,
            ),
            4096,
            0.,
            1.0,
        )
        .unwrap(),
    ];
    do_bench(c, "4096 large_list(0) of u64(0)", cols);

    let cols = vec![
        create_random_array(
            &Field::new(
                "list",
                DataType::List(Arc::new(Field::new_list_field(DataType::UInt64, false))),
                false,
            ),
            10,
            0.,
            1.0,
        )
        .unwrap(),
    ];
    do_bench(c, "10 list(0) of u64(0)", cols);

    let cols = vec![
        create_random_array(
            &Field::new(
                "list",
                DataType::LargeList(Arc::new(Field::new_list_field(DataType::UInt64, false))),
                false,
            ),
            10,
            0.,
            1.0,
        )
        .unwrap(),
    ];
    do_bench(c, "10 large_list(0) of u64(0)", cols);

    let cols = vec![
        create_random_array(
            &Field::new(
                "list",
                DataType::List(Arc::new(Field::new_list_field(DataType::UInt64, false))),
                false,
            ),
            4096,
            0.,
            1.0,
        )
        .unwrap()
        .slice(10, 20),
    ];
    do_bench(c, "4096 list(0) sliced to 10 of u64(0)", cols);

    let cols = vec![
        create_random_array(
            &Field::new(
                "list",
                DataType::LargeList(Arc::new(Field::new_list_field(DataType::UInt64, false))),
                false,
            ),
            4096,
            0.,
            1.0,
        )
        .unwrap()
        .slice(10, 20),
    ];
    do_bench(c, "4096 large_list(0) sliced to 10 of u64(0)", cols);

    run_benchmark_on_medium_amount_and_types_of_columns_without_nesting(4096, c);
    run_benchmark_on_medium_amount_and_types_of_columns_without_nesting(8192, c);

    bench_iter(c);
}

criterion_group!(benches, row_bench);
criterion_main!(benches);
