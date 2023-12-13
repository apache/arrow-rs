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
            let array_idx = rng.gen_range(0..values.len());
            let value_idx = rng.gen_range(0..values[array_idx].len());
            (array_idx, value_idx)
        })
        .collect();

    c.bench_function(name, |b| {
        b.iter(|| criterion::black_box(interleave(values, &indices).unwrap()))
    });
}

fn add_benchmark(c: &mut Criterion) {
    let i32 = create_primitive_array::<Int32Type>(1024, 0.);
    let i32_opt = create_primitive_array::<Int32Type>(1024, 0.5);
    let string = create_string_array_with_len::<i32>(1024, 0., 20);
    let string_opt = create_string_array_with_len::<i32>(1024, 0.5, 20);
    let values = create_string_array_with_len::<i32>(10, 0.0, 20);
    let dict = create_dict_from_values::<Int32Type>(1024, 0.0, &values);

    let values = create_string_array_with_len::<i32>(1024, 0.0, 20);
    let sparse_dict = create_sparse_dict_from_values::<Int32Type>(1024, 0.0, &values, 10..20);

    let cases: &[(&str, &dyn Array)] = &[
        ("i32(0.0)", &i32),
        ("i32(0.5)", &i32_opt),
        ("str(20, 0.0)", &string),
        ("str(20, 0.5)", &string_opt),
        ("dict(20, 0.0)", &dict),
        ("dict_sparse(20, 0.0)", &sparse_dict),
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
