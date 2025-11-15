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

use arrow::datatypes::{ArrowNativeType, ArrowPrimitiveType, Float64Type, Int32Type, UInt32Type};
use arrow::util::bench_util::*;
use arrow_array::*;
use arrow_ord::ord::Comparator;
use arrow_schema::SortOptions;
use std::hint;

/// Number of comparisons to perform per benchmark iteration
const NUM_COMPARISONS: usize = 1000;

fn bench_comparator(left: &dyn Array, right: &dyn Array, opts: SortOptions) {
    let comparator = Comparator::try_new(left, right, opts).unwrap();

    // Perform multiple comparisons to get meaningful measurements
    let len = left.len().min(right.len());
    for i in 0..NUM_COMPARISONS {
        let left_idx = i % len;
        let right_idx = (i + 1) % len;
        hint::black_box(comparator.compare(left_idx, right_idx));
    }
}

fn add_benchmark(c: &mut Criterion) {
    let opts = SortOptions::default();

    const ROWS_SMALL: usize = 100;
    const ROWS_LARGE: usize = 1_000_000;

    // i32 benchmarks with 0% nulls - test different sizes
    let arr_i32_100 = create_primitive_array::<Int32Type>(ROWS_SMALL, 0.0);
    c.bench_function("make_comparator i32 100 rows", |b| {
        b.iter(|| bench_comparator(&arr_i32_100, &arr_i32_100, opts))
    });

    let arr_i32_1m = create_primitive_array::<Int32Type>(ROWS_LARGE, 0.0);
    c.bench_function("make_comparator i32 1M rows", |b| {
        b.iter(|| bench_comparator(&arr_i32_1m, &arr_i32_1m, opts))
    });

    // i32 benchmarks with nulls - only for small size
    let arr_i32_50_nulls = create_primitive_array::<Int32Type>(ROWS_SMALL, 0.5);
    c.bench_function("make_comparator i32 nulls 50% 100 rows", |b| {
        b.iter(|| bench_comparator(&arr_i32_50_nulls, &arr_i32_50_nulls, opts))
    });

    let arr_i32_100_nulls = create_primitive_array::<Int32Type>(ROWS_SMALL, 1.0);
    c.bench_function("make_comparator i32 nulls 100% 100 rows", |b| {
        b.iter(|| bench_comparator(&arr_i32_100_nulls, &arr_i32_100_nulls, opts))
    });

    // f64 benchmarks with 0% nulls
    let arr_f64_100 = create_primitive_array::<Float64Type>(ROWS_SMALL, 0.0);
    c.bench_function("make_comparator f64 100 rows", |b| {
        b.iter(|| bench_comparator(&arr_f64_100, &arr_f64_100, opts))
    });

    let arr_f64_1m = create_primitive_array::<Float64Type>(ROWS_LARGE, 0.0);
    c.bench_function("make_comparator f64 1M rows", |b| {
        b.iter(|| bench_comparator(&arr_f64_1m, &arr_f64_1m, opts))
    });

    // utf8 benchmarks with 0% nulls
    let arr_utf8_100 = create_string_array::<i32>(ROWS_SMALL, 0.0);
    c.bench_function("make_comparator utf8 100 rows", |b| {
        b.iter(|| bench_comparator(&arr_utf8_100, &arr_utf8_100, opts))
    });

    let arr_utf8_1m = create_string_array::<i32>(ROWS_LARGE, 0.0);
    c.bench_function("make_comparator utf8 1M rows", |b| {
        b.iter(|| bench_comparator(&arr_utf8_1m, &arr_utf8_1m, opts))
    });

    // utf8view benchmarks with 0% nulls
    let arr_utf8view_100 = create_string_view_array(ROWS_SMALL, 0.0);
    c.bench_function("make_comparator utf8view 100 rows", |b| {
        b.iter(|| bench_comparator(&arr_utf8view_100, &arr_utf8view_100, opts))
    });

    let arr_utf8view_1m = create_string_view_array(ROWS_LARGE, 0.0);
    c.bench_function("make_comparator utf8view 1M rows", |b| {
        b.iter(|| bench_comparator(&arr_utf8view_1m, &arr_utf8view_1m, opts))
    });

    // dictionary(u32, utf8) benchmarks with 0% nulls
    let arr_dict_100 = create_string_dict_array::<UInt32Type>(ROWS_SMALL, 0.0, 10);
    c.bench_function("make_comparator dict(u32,utf8) 100 rows", |b| {
        b.iter(|| bench_comparator(&arr_dict_100, &arr_dict_100, opts))
    });

    let arr_dict_1m = create_string_dict_array::<UInt32Type>(ROWS_LARGE, 0.0, 10);
    c.bench_function("make_comparator dict(u32,utf8) 1M rows", |b| {
        b.iter(|| bench_comparator(&arr_dict_1m, &arr_dict_1m, opts))
    });

    // list(i32) benchmarks with 0% nulls
    // Create lists with variable-length sublists containing i32 values
    let arr_list_100 = create_list_array::<Int32Type>(ROWS_SMALL, 0.0, 5);
    c.bench_function("make_comparator list(i32) 100 rows", |b| {
        b.iter(|| bench_comparator(&arr_list_100, &arr_list_100, opts))
    });

    let arr_list_1m = create_list_array::<Int32Type>(ROWS_LARGE, 0.0, 5);
    c.bench_function("make_comparator list(i32) 1M rows", |b| {
        b.iter(|| bench_comparator(&arr_list_1m, &arr_list_1m, opts))
    });
}

/// Helper function to create a ListArray containing i32 values
fn create_list_array<T: ArrowPrimitiveType>(
    size: usize,
    null_density: f32,
    avg_list_len: usize,
) -> ListArray {
    use arrow::array::ListBuilder;
    use arrow::array::PrimitiveBuilder;
    use rand::Rng;

    let mut rng = arrow::util::test_util::seedable_rng();
    let mut builder = ListBuilder::new(PrimitiveBuilder::<T>::new());

    for i in 0..size {
        if null_density > 0.0 && rng.random::<f32>() < null_density {
            builder.append(false);
        } else {
            let list_len = rng.random_range(0..avg_list_len * 2);
            for _ in 0..list_len {
                builder
                    .values()
                    .append_value(T::Native::from_usize(i).unwrap());
            }
            builder.append(true);
        }
    }

    builder.finish()
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
