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
use std::ops::Range;

use rand::Rng;

extern crate arrow;

use arrow::datatypes::*;
use arrow::util::test_util::seedable_rng;
use arrow::{array::*, util::bench_util::*};
use arrow_select::interleave::interleave;
use std::hint;
use std::sync::Arc;

fn do_bench(
    c: &mut Criterion,
    prefix: &str,
    len: usize,
    base: &dyn Array,
    slices: &[Range<usize>],
) {
    let arrays: Vec<_> = slices
        .iter()
        .map(|r| base.slice(r.start, r.end - r.start))
        .collect();
    let values: Vec<_> = arrays.iter().map(|x| x.as_ref()).collect();
    bench_values(
        c,
        &format!("interleave {prefix} {len} {slices:?}"),
        len,
        &values,
    );
}

fn bench_values(c: &mut Criterion, name: &str, len: usize, values: &[&dyn Array]) {
    let mut rng = seedable_rng();
    let indices: Vec<_> = (0..len)
        .map(|_| {
            let array_idx = rng.random_range(0..values.len());
            let value_idx = rng.random_range(0..values[array_idx].len());
            (array_idx, value_idx)
        })
        .collect();

    c.bench_function(name, |b| {
        b.iter(|| hint::black_box(interleave(values, &indices).unwrap()))
    });
}

fn add_benchmark(c: &mut Criterion) {
    let i32 = create_primitive_array::<Int32Type>(1024, 0.);
    let i32_opt = create_primitive_array::<Int32Type>(1024, 0.5);
    let string = create_string_array_with_len::<i32>(1024, 0., 20);
    let string_opt = create_string_array_with_len::<i32>(1024, 0.5, 20);
    let values = create_string_array_with_len::<i32>(10, 0.0, 20);
    let dict = create_dict_from_values::<Int32Type>(1024, 0.0, &values);

    let struct_i32_no_nulls_i32_no_nulls = StructArray::new(
        Fields::from(vec![
            Field::new("a", Int32Type::DATA_TYPE, false),
            Field::new("b", Int32Type::DATA_TYPE, false),
        ]),
        vec![
            Arc::new(create_primitive_array::<Int32Type>(1024, 0.)),
            Arc::new(create_primitive_array::<Int32Type>(1024, 0.)),
        ],
        None,
    );

    let struct_string_no_nulls_string_no_nulls = StructArray::new(
        Fields::from(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Utf8, false),
        ]),
        vec![
            Arc::new(create_string_array_with_len::<i32>(1024, 0., 20)),
            Arc::new(create_string_array_with_len::<i32>(1024, 0., 20)),
        ],
        None,
    );

    let struct_i32_no_nulls_string_no_nulls = StructArray::new(
        Fields::from(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]),
        vec![
            Arc::new(create_primitive_array::<Int32Type>(1024, 0.)),
            Arc::new(create_string_array_with_len::<i32>(1024, 0., 20)),
        ],
        None,
    );

    let values = create_string_array_with_len::<i32>(1024, 0.0, 20);
    let sparse_dict = create_sparse_dict_from_values::<Int32Type>(1024, 0.0, &values, 10..20);

    let string_view = create_string_view_array(1024, 0.0);

    // use 8192 as a standard list size for better coverage
    let list_i64 = create_primitive_list_array_with_seed::<i32, Int64Type>(8192, 0.1, 0.1, 20, 42);
    let list_i64_no_nulls =
        create_primitive_list_array_with_seed::<i32, Int64Type>(8192, 0.0, 0.0, 20, 42);

    let cases: &[(&str, &dyn Array)] = &[
        ("i32(0.0)", &i32),
        ("i32(0.5)", &i32_opt),
        ("str(20, 0.0)", &string),
        ("str(20, 0.5)", &string_opt),
        ("dict(20, 0.0)", &dict),
        ("dict_sparse(20, 0.0)", &sparse_dict),
        ("str_view(0.0)", &string_view),
        (
            "struct(i32(0.0), i32(0.0)",
            &struct_i32_no_nulls_i32_no_nulls,
        ),
        (
            "struct(str(20, 0.0), str(20, 0.0))",
            &struct_string_no_nulls_string_no_nulls,
        ),
        (
            "struct(i32(0.0), str(20, 0.0)",
            &struct_i32_no_nulls_string_no_nulls,
        ),
        ("list<i64>(0.1,0.1,20)", &list_i64),
        ("list<i64>(0.0,0.0,20)", &list_i64_no_nulls),
    ];

    for (prefix, base) in cases {
        let slices: &[(usize, &[_])] = &[
            (100, &[0..100, 100..230, 450..1000]),
            (400, &[0..100, 100..230, 450..1000]),
            (1024, &[0..100, 100..230, 450..1000]),
            (1024, &[0..100, 100..230, 450..1000, 0..1000]),
        ];

        for (len, slice) in slices {
            do_bench(c, prefix, *len, *base, slice);
        }
    }

    for len in [100, 1024, 2048] {
        bench_values(
            c,
            &format!("interleave dict_distinct {len}"),
            100,
            &[&dict, &sparse_dict],
        );
    }
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
