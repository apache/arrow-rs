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
use arrow::util::test_util::seedable_rng;
use criterion::Criterion;

extern crate arrow;

use arrow::compute::kernels::cmp::*;
use arrow::util::bench_util::*;
use arrow::{array::*, datatypes::Float32Type, datatypes::Int32Type};
use arrow_buffer::IntervalMonthDayNano;
use arrow_string::like::*;
use arrow_string::regexp::regexp_is_match_utf8_scalar;
use rand::rngs::StdRng;
use rand::Rng;

const SIZE: usize = 65536;

fn bench_like_utf8view_scalar(arr_a: &StringViewArray, value_b: &str) {
    like(arr_a, &StringViewArray::new_scalar(value_b)).unwrap();
}

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

fn make_string_array(size: usize, rng: &mut StdRng) -> impl Iterator<Item = Option<String>> + '_ {
    (0..size).map(|_| {
        let len = rng.gen_range(0..64);
        let bytes = (0..len).map(|_| rng.gen_range(0..128)).collect();
        Some(String::from_utf8(bytes).unwrap())
    })
}

fn add_benchmark(c: &mut Criterion) {
    let arr_a = create_primitive_array_with_seed::<Float32Type>(SIZE, 0.0, 42);
    let arr_b = create_primitive_array_with_seed::<Float32Type>(SIZE, 0.0, 43);

    let arr_month_day_nano_a = create_month_day_nano_array_with_seed(SIZE, 0.0, 43);
    let arr_month_day_nano_b = create_month_day_nano_array_with_seed(SIZE, 0.0, 43);

    let arr_string = create_string_array::<i32>(SIZE, 0.0);

    let scalar = Float32Array::from(vec![1.0]);

    // eq benchmarks

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
    let scalar = IntervalMonthDayNanoArray::new_scalar(IntervalMonthDayNano::new(123, 0, 0));

    c.bench_function("eq scalar MonthDayNano", |b| {
        b.iter(|| eq(&arr_month_day_nano_b, &scalar).unwrap())
    });

    let mut rng = seedable_rng();
    let mut array_gen = make_string_array(1024 * 1024 * 8, &mut rng);
    let string_left = StringArray::from_iter(array_gen);
    let string_view_left = StringViewArray::from_iter(string_left.iter());

    // reference to the same rng to make sure we generate **different** array data,
    // ow. the left and right will be identical
    array_gen = make_string_array(1024 * 1024 * 8, &mut rng);
    let string_right = StringArray::from_iter(array_gen);
    let string_view_right = StringViewArray::from_iter(string_right.iter());

    let scalar = StringArray::new_scalar("xxxx");
    c.bench_function("eq scalar StringArray", |b| {
        b.iter(|| eq(&scalar, &string_left).unwrap())
    });

    c.bench_function("lt scalar StringViewArray", |b| {
        b.iter(|| {
            lt(
                &Scalar::new(StringViewArray::from_iter_values(["xxxx"])),
                &string_view_left,
            )
            .unwrap()
        })
    });

    c.bench_function("lt scalar StringArray", |b| {
        b.iter(|| {
            lt(
                &Scalar::new(StringArray::from_iter_values(["xxxx"])),
                &string_left,
            )
            .unwrap()
        })
    });

    c.bench_function("eq scalar StringViewArray", |b| {
        b.iter(|| eq(&scalar, &string_view_left).unwrap())
    });

    c.bench_function("eq StringArray StringArray", |b| {
        b.iter(|| eq(&string_left, &string_right).unwrap())
    });

    c.bench_function("eq StringViewArray StringViewArray", |b| {
        b.iter(|| eq(&string_view_left, &string_view_right).unwrap())
    });

    // StringArray: LIKE benchmarks

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

    // StringViewArray: LIKE benchmarks
    // Note: since like/nlike share the same implementation, we only benchmark one
    c.bench_function("like_utf8view scalar equals", |b| {
        b.iter(|| bench_like_utf8view_scalar(&string_view_left, "xxxx"))
    });

    c.bench_function("like_utf8view scalar contains", |b| {
        b.iter(|| bench_like_utf8view_scalar(&string_view_left, "%xxxx%"))
    });

    c.bench_function("like_utf8view scalar ends with", |b| {
        b.iter(|| bench_like_utf8view_scalar(&string_view_left, "xxxx%"))
    });

    c.bench_function("like_utf8view scalar starts with", |b| {
        b.iter(|| bench_like_utf8view_scalar(&string_view_left, "%xxxx"))
    });

    c.bench_function("like_utf8view scalar complex", |b| {
        b.iter(|| bench_like_utf8view_scalar(&string_view_left, "%xx_xx%xxx"))
    });

    // StringArray: NOT LIKE benchmarks

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

    // StringArray: ILIKE benchmarks

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

    // StringArray: NOT ILIKE benchmarks

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

    c.bench_function("regexp_matches_utf8 scalar starts with", |b| {
        b.iter(|| bench_regexp_is_match_utf8_scalar(&arr_string, "^xx"))
    });

    c.bench_function("regexp_matches_utf8 scalar ends with", |b| {
        b.iter(|| bench_regexp_is_match_utf8_scalar(&arr_string, "xx$"))
    });

    // DictionaryArray benchmarks

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
