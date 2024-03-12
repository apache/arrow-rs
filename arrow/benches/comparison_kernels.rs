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

extern crate arrow;

use arrow::compute::kernels::cmp::*;
use arrow::datatypes::IntervalMonthDayNanoType;
use arrow::util::bench_util::*;
use arrow::{array::*, datatypes::Float32Type, datatypes::Int32Type};
use arrow_string::like::*;
use arrow_string::regexp::regexp_is_match_utf8_scalar;

const SIZE: usize = 65536;

fn bench_like_utf8_scalar(arr_a: &StringArray, value_b: &str) {
    like(arr_a, &StringArray::new_scalar(value_b)).unwrap();
}

fn bench_nlike_utf8_scalar(arr_a: &StringArray, value_b: &str) {
    nlike(arr_a, &StringArray::new_scalar(value_b)).unwrap();
}

fn bench_ilike_utf8_scalar(arr_a: &StringArray, value_b: &str) {
    ilike(arr_a, &StringArray::new_scalar(value_b)).unwrap();
}

fn bench_nilike_utf8_scalar(arr_a: &StringArray, value_b: &str) {
    nilike(arr_a, &StringArray::new_scalar(value_b)).unwrap();
}

fn bench_regexp_is_match_utf8_scalar(arr_a: &StringArray, value_b: &str) {
    regexp_is_match_utf8_scalar(
        criterion::black_box(arr_a),
        criterion::black_box(value_b),
        None,
    )
    .unwrap();
}

fn add_benchmark(c: &mut Criterion) {
    let arr_a = create_primitive_array_with_seed::<Float32Type>(SIZE, 0.0, 42);
    let arr_b = create_primitive_array_with_seed::<Float32Type>(SIZE, 0.0, 43);

    let arr_month_day_nano_a =
        create_primitive_array_with_seed::<IntervalMonthDayNanoType>(SIZE, 0.0, 43);
    let arr_month_day_nano_b =
        create_primitive_array_with_seed::<IntervalMonthDayNanoType>(SIZE, 0.0, 43);

    let arr_string = create_string_array::<i32>(SIZE, 0.0);
    let scalar = Float32Array::from(vec![1.0]);

    c.bench_function("eq Float32", |b| b.iter(|| eq(&arr_a, &arr_b)));
    c.bench_function("eq scalar Float32", |b| {
        b.iter(|| eq(&arr_a, &Scalar::new(&scalar)).unwrap())
    });

    c.bench_function("neq Float32", |b| b.iter(|| neq(&arr_a, &arr_b)));
    c.bench_function("neq scalar Float32", |b| {
        b.iter(|| neq(&arr_a, &Scalar::new(&scalar)).unwrap())
    });

    c.bench_function("lt Float32", |b| b.iter(|| lt(&arr_a, &arr_b)));
    c.bench_function("lt scalar Float32", |b| {
        b.iter(|| lt(&arr_a, &Scalar::new(&scalar)).unwrap())
    });

    c.bench_function("lt_eq Float32", |b| b.iter(|| lt_eq(&arr_a, &arr_b)));
    c.bench_function("lt_eq scalar Float32", |b| {
        b.iter(|| lt_eq(&arr_a, &Scalar::new(&scalar)).unwrap())
    });

    c.bench_function("gt Float32", |b| b.iter(|| gt(&arr_a, &arr_b)));
    c.bench_function("gt scalar Float32", |b| {
        b.iter(|| gt(&arr_a, &Scalar::new(&scalar)).unwrap())
    });

    c.bench_function("gt_eq Float32", |b| b.iter(|| gt_eq(&arr_a, &arr_b)));
    c.bench_function("gt_eq scalar Float32", |b| {
        b.iter(|| gt_eq(&arr_a, &Scalar::new(&scalar)).unwrap())
    });

    let arr_a = create_primitive_array_with_seed::<Int32Type>(SIZE, 0.0, 42);
    let arr_b = create_primitive_array_with_seed::<Int32Type>(SIZE, 0.0, 43);
    let scalar = Int32Array::new_scalar(1);

    c.bench_function("eq Int32", |b| b.iter(|| eq(&arr_a, &arr_b)));
    c.bench_function("eq scalar Int32", |b| {
        b.iter(|| eq(&arr_a, &scalar).unwrap())
    });

    c.bench_function("neq Int32", |b| b.iter(|| neq(&arr_a, &arr_b)));
    c.bench_function("neq scalar Int32", |b| {
        b.iter(|| neq(&arr_a, &scalar).unwrap())
    });

    c.bench_function("lt Int32", |b| b.iter(|| lt(&arr_a, &arr_b)));
    c.bench_function("lt scalar Int32", |b| {
        b.iter(|| lt(&arr_a, &scalar).unwrap())
    });

    c.bench_function("lt_eq Int32", |b| b.iter(|| lt_eq(&arr_a, &arr_b)));
    c.bench_function("lt_eq scalar Int32", |b| {
        b.iter(|| lt_eq(&arr_a, &scalar).unwrap())
    });

    c.bench_function("gt Int32", |b| b.iter(|| gt(&arr_a, &arr_b)));
    c.bench_function("gt scalar Int32", |b| {
        b.iter(|| gt(&arr_a, &scalar).unwrap())
    });

    c.bench_function("gt_eq Int32", |b| b.iter(|| gt_eq(&arr_a, &arr_b)));
    c.bench_function("gt_eq scalar Int32", |b| {
        b.iter(|| gt_eq(&arr_a, &scalar).unwrap())
    });

    c.bench_function("eq MonthDayNano", |b| {
        b.iter(|| eq(&arr_month_day_nano_a, &arr_month_day_nano_b))
    });
    let scalar = IntervalMonthDayNanoArray::new_scalar(123);

    c.bench_function("eq scalar MonthDayNano", |b| {
        b.iter(|| eq(&arr_month_day_nano_b, &scalar).unwrap())
    });

    c.bench_function("like_utf8 scalar equals", |b| {
        b.iter(|| bench_like_utf8_scalar(&arr_string, "xxxx"))
    });

    c.bench_function("like_utf8 scalar contains", |b| {
        b.iter(|| bench_like_utf8_scalar(&arr_string, "%xxxx%"))
    });

    c.bench_function("like_utf8 scalar ends with", |b| {
        b.iter(|| bench_like_utf8_scalar(&arr_string, "xxxx%"))
    });

    c.bench_function("like_utf8 scalar starts with", |b| {
        b.iter(|| bench_like_utf8_scalar(&arr_string, "%xxxx"))
    });

    c.bench_function("like_utf8 scalar complex", |b| {
        b.iter(|| bench_like_utf8_scalar(&arr_string, "%xx_xx%xxx"))
    });

    c.bench_function("nlike_utf8 scalar equals", |b| {
        b.iter(|| bench_nlike_utf8_scalar(&arr_string, "xxxx"))
    });

    c.bench_function("nlike_utf8 scalar contains", |b| {
        b.iter(|| bench_nlike_utf8_scalar(&arr_string, "%xxxx%"))
    });

    c.bench_function("nlike_utf8 scalar ends with", |b| {
        b.iter(|| bench_nlike_utf8_scalar(&arr_string, "xxxx%"))
    });

    c.bench_function("nlike_utf8 scalar starts with", |b| {
        b.iter(|| bench_nlike_utf8_scalar(&arr_string, "%xxxx"))
    });

    c.bench_function("nlike_utf8 scalar complex", |b| {
        b.iter(|| bench_nlike_utf8_scalar(&arr_string, "%xx_xx%xxx"))
    });

    c.bench_function("ilike_utf8 scalar equals", |b| {
        b.iter(|| bench_ilike_utf8_scalar(&arr_string, "xxXX"))
    });

    c.bench_function("ilike_utf8 scalar contains", |b| {
        b.iter(|| bench_ilike_utf8_scalar(&arr_string, "%xxXX%"))
    });

    c.bench_function("ilike_utf8 scalar ends with", |b| {
        b.iter(|| bench_ilike_utf8_scalar(&arr_string, "xXXx%"))
    });

    c.bench_function("ilike_utf8 scalar starts with", |b| {
        b.iter(|| bench_ilike_utf8_scalar(&arr_string, "%XXXx"))
    });

    c.bench_function("ilike_utf8 scalar complex", |b| {
        b.iter(|| bench_ilike_utf8_scalar(&arr_string, "%xx_xX%xXX"))
    });

    c.bench_function("nilike_utf8 scalar equals", |b| {
        b.iter(|| bench_nilike_utf8_scalar(&arr_string, "xxXX"))
    });

    c.bench_function("nilike_utf8 scalar contains", |b| {
        b.iter(|| bench_nilike_utf8_scalar(&arr_string, "%xxXX%"))
    });

    c.bench_function("nilike_utf8 scalar ends with", |b| {
        b.iter(|| bench_nilike_utf8_scalar(&arr_string, "xXXx%"))
    });

    c.bench_function("nilike_utf8 scalar starts with", |b| {
        b.iter(|| bench_nilike_utf8_scalar(&arr_string, "%XXXx"))
    });

    c.bench_function("nilike_utf8 scalar complex", |b| {
        b.iter(|| bench_nilike_utf8_scalar(&arr_string, "%xx_xX%xXX"))
    });

    c.bench_function("egexp_matches_utf8 scalar starts with", |b| {
        b.iter(|| bench_regexp_is_match_utf8_scalar(&arr_string, "^xx"))
    });

    c.bench_function("egexp_matches_utf8 scalar ends with", |b| {
        b.iter(|| bench_regexp_is_match_utf8_scalar(&arr_string, "xx$"))
    });

    let strings = create_string_array::<i32>(20, 0.);
    let dict_arr_a = create_dict_from_values::<Int32Type>(SIZE, 0., &strings);
    let scalar = StringArray::from(vec!["test"]);

    c.bench_function("eq_dyn_utf8_scalar dictionary[10] string[4])", |b| {
        b.iter(|| eq(&dict_arr_a, &Scalar::new(&scalar)))
    });

    c.bench_function(
        "gt_eq_dyn_utf8_scalar scalar dictionary[10] string[4])",
        |b| b.iter(|| gt_eq(&dict_arr_a, &Scalar::new(&scalar))),
    );

    c.bench_function("like_utf8_scalar_dyn dictionary[10] string[4])", |b| {
        b.iter(|| like(&dict_arr_a, &StringArray::new_scalar("test")))
    });

    c.bench_function("ilike_utf8_scalar_dyn dictionary[10] string[4])", |b| {
        b.iter(|| ilike(&dict_arr_a, &StringArray::new_scalar("test")))
    });

    let strings = create_string_array::<i32>(20, 0.);
    let dict_arr_a = create_dict_from_values::<Int32Type>(SIZE, 0., &strings);
    let dict_arr_b = create_dict_from_values::<Int32Type>(SIZE, 0., &strings);

    c.bench_function("eq dictionary[10] string[4])", |b| {
        b.iter(|| eq(&dict_arr_a, &dict_arr_b).unwrap())
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
