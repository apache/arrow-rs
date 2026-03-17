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
#[macro_use]
extern crate criterion;
use std::sync::Arc;

use criterion::Criterion;

use arrow::array::*;
use arrow::compute::concat;
use arrow::datatypes::*;
use arrow::util::bench_util::*;
use std::hint;

fn bench_concat(v1: &dyn Array, v2: &dyn Array) {
    hint::black_box(concat(&[v1, v2]).unwrap());
}

fn bench_concat_arrays(arrays: &[&dyn Array]) {
    hint::black_box(concat(arrays).unwrap());
}

fn add_benchmark(c: &mut Criterion) {
    let v1 = create_primitive_array::<Int32Type>(1024, 0.0);
    let v2 = create_primitive_array::<Int32Type>(1024, 0.0);
    c.bench_function("concat i32 1024", |b| b.iter(|| bench_concat(&v1, &v2)));

    let v1 = create_primitive_array::<Int32Type>(1024, 0.5);
    let v2 = create_primitive_array::<Int32Type>(1024, 0.5);
    c.bench_function("concat i32 nulls 1024", |b| {
        b.iter(|| bench_concat(&v1, &v2))
    });

    let small_array = create_primitive_array::<Int32Type>(4, 0.0);
    let arrays: Vec<_> = (0..1024).map(|_| &small_array as &dyn Array).collect();
    c.bench_function("concat 1024 arrays i32 4", |b| {
        b.iter(|| bench_concat_arrays(&arrays))
    });

    {
        let input = (0..100)
            .map(|_| create_primitive_array::<Int32Type>(8192, 0.0))
            .collect::<Vec<_>>();
        let arrays: Vec<_> = input.iter().map(|arr| arr as &dyn Array).collect();
        c.bench_function("concat i32 8192 over 100 arrays", |b| {
            b.iter(|| bench_concat_arrays(&arrays))
        });
    }

    {
        let input = (0..100)
            .map(|_| create_primitive_array::<Int32Type>(8192, 0.5))
            .collect::<Vec<_>>();
        let arrays: Vec<_> = input.iter().map(|arr| arr as &dyn Array).collect();
        c.bench_function("concat i32 nulls 8192 over 100 arrays", |b| {
            b.iter(|| bench_concat_arrays(&arrays))
        });
    }

    let v1 = create_boolean_array(1024, 0.0, 0.5);
    let v2 = create_boolean_array(1024, 0.0, 0.5);
    c.bench_function("concat boolean 1024", |b| b.iter(|| bench_concat(&v1, &v2)));

    let v1 = create_boolean_array(1024, 0.5, 0.5);
    let v2 = create_boolean_array(1024, 0.5, 0.5);
    c.bench_function("concat boolean nulls 1024", |b| {
        b.iter(|| bench_concat(&v1, &v2))
    });

    let small_array = create_boolean_array(4, 0.0, 0.5);
    let arrays: Vec<_> = (0..1024).map(|_| &small_array as &dyn Array).collect();
    c.bench_function("concat 1024 arrays boolean 4", |b| {
        b.iter(|| bench_concat_arrays(&arrays))
    });

    {
        let input = (0..100)
            .map(|_| create_boolean_array(8192, 0.0, 0.5))
            .collect::<Vec<_>>();
        let arrays: Vec<_> = input.iter().map(|arr| arr as &dyn Array).collect();
        c.bench_function("concat boolean 8192 over 100 arrays", |b| {
            b.iter(|| bench_concat_arrays(&arrays))
        });
    }

    {
        let input = (0..100)
            .map(|_| create_boolean_array(8192, 0.5, 0.5))
            .collect::<Vec<_>>();
        let arrays: Vec<_> = input.iter().map(|arr| arr as &dyn Array).collect();
        c.bench_function("concat boolean nulls 8192 over 100 arrays", |b| {
            b.iter(|| bench_concat_arrays(&arrays))
        });
    }

    let v1 = create_string_array::<i32>(1024, 0.0);
    let v2 = create_string_array::<i32>(1024, 0.0);
    c.bench_function("concat str 1024", |b| b.iter(|| bench_concat(&v1, &v2)));

    let v1 = create_string_array::<i32>(1024, 0.5);
    let v2 = create_string_array::<i32>(1024, 0.5);
    c.bench_function("concat str nulls 1024", |b| {
        b.iter(|| bench_concat(&v1, &v2))
    });

    let small_array = create_string_array::<i32>(4, 0.0);
    let arrays: Vec<_> = (0..1024).map(|_| &small_array as &dyn Array).collect();
    c.bench_function("concat 1024 arrays str 4", |b| {
        b.iter(|| bench_concat_arrays(&arrays))
    });

    {
        let input = (0..100)
            .map(|_| create_string_array::<i32>(8192, 0.0))
            .collect::<Vec<_>>();
        let arrays: Vec<_> = input.iter().map(|arr| arr as &dyn Array).collect();
        c.bench_function("concat str 8192 over 100 arrays", |b| {
            b.iter(|| bench_concat_arrays(&arrays))
        });
    }

    {
        let input = (0..100)
            .map(|_| create_string_array::<i32>(8192, 0.5))
            .collect::<Vec<_>>();
        let arrays: Vec<_> = input.iter().map(|arr| arr as &dyn Array).collect();
        c.bench_function("concat str nulls 8192 over 100 arrays", |b| {
            b.iter(|| bench_concat_arrays(&arrays))
        });
    }

    // String view arrays
    for null_density in [0.0, 0.2] {
        // Any strings less than 12 characters are stored as prefix only, so specially
        // benchmark cases that have different mixes of lengths.
        for (name, str_len) in [("all_inline", 12), ("", 20), ("", 128)] {
            let array = create_string_view_array_with_len(8192, null_density, str_len, false);
            let arrays = (0..10).map(|_| &array as &dyn Array).collect::<Vec<_>>();
            let id = format!(
                "concat utf8_view {name} max_str_len={str_len} null_density={null_density}"
            );
            c.bench_function(&id, |b| b.iter(|| bench_concat_arrays(&arrays)));
        }
    }

    let v1 = create_string_array_with_len::<i32>(10, 0.0, 20);
    let v1 = create_dict_from_values::<Int32Type>(1024, 0.0, &v1);
    let v2 = create_string_array_with_len::<i32>(10, 0.0, 20);
    let v2 = create_dict_from_values::<Int32Type>(1024, 0.0, &v2);
    c.bench_function("concat str_dict 1024", |b| {
        b.iter(|| bench_concat(&v1, &v2))
    });

    let v1 = create_string_array_with_len::<i32>(1024, 0.0, 20);
    let v1 = create_sparse_dict_from_values::<Int32Type>(1024, 0.0, &v1, 10..20);
    let v2 = create_string_array_with_len::<i32>(1024, 0.0, 20);
    let v2 = create_sparse_dict_from_values::<Int32Type>(1024, 0.0, &v2, 30..40);
    c.bench_function("concat str_dict_sparse 1024", |b| {
        b.iter(|| bench_concat(&v1, &v2))
    });

    let v1 = FixedSizeListArray::try_new(
        Arc::new(Field::new_list_field(DataType::Int32, true)),
        1024,
        Arc::new(create_primitive_array::<Int32Type>(1024 * 1024, 0.0)),
        None,
    )
    .unwrap();
    let v2 = FixedSizeListArray::try_new(
        Arc::new(Field::new_list_field(DataType::Int32, true)),
        1024,
        Arc::new(create_primitive_array::<Int32Type>(1024 * 1024, 0.0)),
        None,
    )
    .unwrap();
    c.bench_function("concat fixed size lists", |b| {
        b.iter(|| bench_concat(&v1, &v2))
    });

    {
        let batch_size = 1024;
        let batch_count = 2;
        let struct_arrays = (0..batch_count)
            .map(|_| {
                let ints = create_primitive_array::<Int32Type>(batch_size, 0.0);
                let string_dict = create_sparse_dict_from_values::<Int32Type>(
                    batch_size,
                    0.0,
                    &create_string_array_with_len::<i32>(20, 0.0, 10),
                    0..10,
                );
                let int_dict = create_sparse_dict_from_values::<UInt16Type>(
                    batch_size,
                    0.0,
                    &create_primitive_array::<Int64Type>(20, 0.0),
                    0..10,
                );
                let fields = vec![
                    Field::new("int_field", ints.data_type().clone(), false),
                    Field::new("strings_dict_field", string_dict.data_type().clone(), false),
                    Field::new("int_dict_field", int_dict.data_type().clone(), false),
                ];

                StructArray::try_new(
                    fields.clone().into(),
                    vec![Arc::new(ints), Arc::new(string_dict), Arc::new(int_dict)],
                    None,
                )
                .unwrap()
            })
            .collect::<Vec<_>>();

        let array_refs = struct_arrays
            .iter()
            .map(|a| a as &dyn Array)
            .collect::<Vec<_>>();

        c.bench_function(
            &format!("concat struct with int32 and dicts size={batch_size} count={batch_count}"),
            |b| b.iter(|| bench_concat_arrays(&array_refs)),
        );
    }
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
