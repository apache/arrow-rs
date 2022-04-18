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

use arrow::compute::*;
use arrow::datatypes::{ArrowNumericType, IntervalMonthDayNanoType};
use arrow::util::bench_util::*;
use arrow::{array::*, datatypes::Float32Type, datatypes::Int32Type};

fn bench_eq<T>(arr_a: &PrimitiveArray<T>, arr_b: &PrimitiveArray<T>)
where
    T: ArrowNumericType,
{
    eq(criterion::black_box(arr_a), criterion::black_box(arr_b)).unwrap();
}

fn bench_eq_scalar<T>(arr_a: &PrimitiveArray<T>, value_b: T::Native)
where
    T: ArrowNumericType,
{
    eq_scalar(criterion::black_box(arr_a), criterion::black_box(value_b)).unwrap();
}

fn bench_neq<T>(arr_a: &PrimitiveArray<T>, arr_b: &PrimitiveArray<T>)
where
    T: ArrowNumericType,
{
    neq(criterion::black_box(arr_a), criterion::black_box(arr_b)).unwrap();
}

fn bench_neq_scalar<T>(arr_a: &PrimitiveArray<T>, value_b: T::Native)
where
    T: ArrowNumericType,
{
    neq_scalar(criterion::black_box(arr_a), criterion::black_box(value_b)).unwrap();
}

fn bench_lt<T>(arr_a: &PrimitiveArray<T>, arr_b: &PrimitiveArray<T>)
where
    T: ArrowNumericType,
{
    lt(criterion::black_box(arr_a), criterion::black_box(arr_b)).unwrap();
}

fn bench_lt_scalar<T>(arr_a: &PrimitiveArray<T>, value_b: T::Native)
where
    T: ArrowNumericType,
{
    lt_scalar(criterion::black_box(arr_a), criterion::black_box(value_b)).unwrap();
}

fn bench_lt_eq<T>(arr_a: &PrimitiveArray<T>, arr_b: &PrimitiveArray<T>)
where
    T: ArrowNumericType,
{
    lt_eq(criterion::black_box(arr_a), criterion::black_box(arr_b)).unwrap();
}

fn bench_lt_eq_scalar<T>(arr_a: &PrimitiveArray<T>, value_b: T::Native)
where
    T: ArrowNumericType,
{
    lt_eq_scalar(criterion::black_box(arr_a), criterion::black_box(value_b)).unwrap();
}

fn bench_gt<T>(arr_a: &PrimitiveArray<T>, arr_b: &PrimitiveArray<T>)
where
    T: ArrowNumericType,
{
    gt(criterion::black_box(arr_a), criterion::black_box(arr_b)).unwrap();
}

fn bench_gt_scalar<T>(arr_a: &PrimitiveArray<T>, value_b: T::Native)
where
    T: ArrowNumericType,
{
    gt_scalar(criterion::black_box(arr_a), criterion::black_box(value_b)).unwrap();
}

fn bench_gt_eq<T>(arr_a: &PrimitiveArray<T>, arr_b: &PrimitiveArray<T>)
where
    T: ArrowNumericType,
{
    gt_eq(criterion::black_box(arr_a), criterion::black_box(arr_b)).unwrap();
}

fn bench_gt_eq_scalar<T>(arr_a: &PrimitiveArray<T>, value_b: T::Native)
where
    T: ArrowNumericType,
{
    gt_eq_scalar(criterion::black_box(arr_a), criterion::black_box(value_b)).unwrap();
}

fn bench_like_utf8_scalar(arr_a: &StringArray, value_b: &str) {
    like_utf8_scalar(criterion::black_box(arr_a), criterion::black_box(value_b)).unwrap();
}

fn bench_nlike_utf8_scalar(arr_a: &StringArray, value_b: &str) {
    nlike_utf8_scalar(criterion::black_box(arr_a), criterion::black_box(value_b))
        .unwrap();
}

fn bench_ilike_utf8_scalar(arr_a: &StringArray, value_b: &str) {
    ilike_utf8_scalar(criterion::black_box(arr_a), criterion::black_box(value_b))
        .unwrap();
}

fn bench_regexp_is_match_utf8_scalar(arr_a: &StringArray, value_b: &str) {
    regexp_is_match_utf8_scalar(
        criterion::black_box(arr_a),
        criterion::black_box(value_b),
        None,
    )
    .unwrap();
}

fn bench_dict_eq<T>(arr_a: &DictionaryArray<T>, arr_b: &DictionaryArray<T>)
where
    T: ArrowNumericType,
{
    cmp_dict_utf8::<T, i32, _>(
        criterion::black_box(arr_a),
        criterion::black_box(arr_b),
        |a, b| a == b,
    )
    .unwrap();
}

fn add_benchmark(c: &mut Criterion) {
    let size = 65536;
    let arr_a = create_primitive_array_with_seed::<Float32Type>(size, 0.0, 42);
    let arr_b = create_primitive_array_with_seed::<Float32Type>(size, 0.0, 43);

    let arr_month_day_nano_a =
        create_primitive_array_with_seed::<IntervalMonthDayNanoType>(size, 0.0, 43);
    let arr_month_day_nano_b =
        create_primitive_array_with_seed::<IntervalMonthDayNanoType>(size, 0.0, 43);

    let arr_string = create_string_array::<i32>(size, 0.0);

    c.bench_function("eq Float32", |b| b.iter(|| bench_eq(&arr_a, &arr_b)));
    c.bench_function("eq scalar Float32", |b| {
        b.iter(|| bench_eq_scalar(&arr_a, 1.0))
    });

    c.bench_function("neq Float32", |b| b.iter(|| bench_neq(&arr_a, &arr_b)));
    c.bench_function("neq scalar Float32", |b| {
        b.iter(|| bench_neq_scalar(&arr_a, 1.0))
    });

    c.bench_function("lt Float32", |b| b.iter(|| bench_lt(&arr_a, &arr_b)));
    c.bench_function("lt scalar Float32", |b| {
        b.iter(|| bench_lt_scalar(&arr_a, 1.0))
    });

    c.bench_function("lt_eq Float32", |b| b.iter(|| bench_lt_eq(&arr_a, &arr_b)));
    c.bench_function("lt_eq scalar Float32", |b| {
        b.iter(|| bench_lt_eq_scalar(&arr_a, 1.0))
    });

    c.bench_function("gt Float32", |b| b.iter(|| bench_gt(&arr_a, &arr_b)));
    c.bench_function("gt scalar Float32", |b| {
        b.iter(|| bench_gt_scalar(&arr_a, 1.0))
    });

    c.bench_function("gt_eq Float32", |b| b.iter(|| bench_gt_eq(&arr_a, &arr_b)));
    c.bench_function("gt_eq scalar Float32", |b| {
        b.iter(|| bench_gt_eq_scalar(&arr_a, 1.0))
    });

    c.bench_function("eq MonthDayNano", |b| {
        b.iter(|| bench_eq(&arr_month_day_nano_a, &arr_month_day_nano_b))
    });
    c.bench_function("eq scalar MonthDayNano", |b| {
        b.iter(|| bench_eq_scalar(&arr_month_day_nano_a, 123))
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

    c.bench_function("egexp_matches_utf8 scalar starts with", |b| {
        b.iter(|| bench_regexp_is_match_utf8_scalar(&arr_string, "^xx"))
    });

    c.bench_function("egexp_matches_utf8 scalar ends with", |b| {
        b.iter(|| bench_regexp_is_match_utf8_scalar(&arr_string, "xx$"))
    });

    let dict_arr_a = create_string_dict_array::<Int32Type>(size, 0.0);
    let dict_arr_b = create_string_dict_array::<Int32Type>(size, 0.0);

    c.bench_function("dict eq string", |b| {
        b.iter(|| bench_dict_eq(&dict_arr_a, &dict_arr_b))
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
