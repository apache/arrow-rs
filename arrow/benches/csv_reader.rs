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

use std::io::Cursor;
use std::sync::Arc;

use arrow::util::bench_util::create_string_view_array_with_len;
use criterion::*;
use rand::RngExt;

use arrow::array::*;
use arrow::csv;
use arrow::datatypes::*;
use arrow::util::bench_util::{create_primitive_array, create_string_array_with_len};
use arrow::util::test_util::seedable_rng;

fn do_bench(c: &mut Criterion, name: &str, cols: Vec<ArrayRef>) {
    let batch = RecordBatch::try_from_iter(cols.into_iter().map(|a| ("col", a))).unwrap();

    let mut buf = Vec::with_capacity(1024);
    let mut csv = csv::Writer::new(&mut buf);
    csv.write(&batch).unwrap();
    drop(csv);

    for batch_size in [128, 1024, 4096] {
        c.bench_function(&format!("{name} - {batch_size}"), |b| {
            b.iter(|| {
                let cursor = Cursor::new(buf.as_slice());
                let reader = csv::ReaderBuilder::new(batch.schema())
                    .with_batch_size(batch_size)
                    .with_header(true)
                    .build_buffered(cursor)
                    .unwrap();

                for next in reader {
                    next.unwrap();
                }
            });
        });
    }
}

/// Creates a decimal array of `size` random values with `digits` significant
/// digits (no leading zero) for the given `precision` and `scale`
fn create_decimal_array<T: DecimalType>(
    size: usize,
    null_density: f32,
    digits: u32,
    precision: u8,
    scale: i8,
) -> ArrayRef {
    let mut rng = seedable_rng();
    let ten = T::Native::usize_as(10);
    let values = (0..size).map(|_| {
        if rng.random::<f32>() < null_density {
            return None;
        }
        let mut value = T::Native::usize_as(rng.random_range(1..10));
        for _ in 1..digits {
            value = value
                .mul_wrapping(ten)
                .add_wrapping(T::Native::usize_as(rng.random_range(0..10)));
        }
        Some(value)
    });
    let array = PrimitiveArray::<T>::from_iter(values)
        .with_precision_and_scale(precision, scale)
        .unwrap();
    Arc::new(array)
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut rng = seedable_rng();

    // Single Primitive Column tests
    let values = Int32Array::from_iter_values((0..4096).map(|_| rng.random_range(0..1024)));
    let cols = vec![Arc::new(values) as ArrayRef];
    do_bench(c, "4096 i32_small(0)", cols);

    let values = Int32Array::from_iter_values((0..4096).map(|_| rng.random()));
    let cols = vec![Arc::new(values) as ArrayRef];
    do_bench(c, "4096 i32(0)", cols);

    let values = UInt64Array::from_iter_values((0..4096).map(|_| rng.random_range(0..1024)));
    let cols = vec![Arc::new(values) as ArrayRef];
    do_bench(c, "4096 u64_small(0)", cols);

    let values = UInt64Array::from_iter_values((0..4096).map(|_| rng.random()));
    let cols = vec![Arc::new(values) as ArrayRef];
    do_bench(c, "4096 u64(0)", cols);

    let values = Int64Array::from_iter_values((0..4096).map(|_| rng.random_range(0..1024) - 512));
    let cols = vec![Arc::new(values) as ArrayRef];
    do_bench(c, "4096 i64_small(0)", cols);

    let values = Int64Array::from_iter_values((0..4096).map(|_| rng.random()));
    let cols = vec![Arc::new(values) as ArrayRef];
    do_bench(c, "4096 i64(0)", cols);

    let cols = vec![Arc::new(Float32Array::from_iter_values(
        (0..4096).map(|_| rng.random_range(0..1024000) as f32 / 1000.),
    )) as _];
    do_bench(c, "4096 f32_small(0)", cols);

    let values = Float32Array::from_iter_values((0..4096).map(|_| rng.random()));
    let cols = vec![Arc::new(values) as ArrayRef];
    do_bench(c, "4096 f32(0)", cols);

    let cols = vec![Arc::new(Float64Array::from_iter_values(
        (0..4096).map(|_| rng.random_range(0..1024000) as f64 / 1000.),
    )) as _];
    do_bench(c, "4096 f64_small(0)", cols);

    let values = Float64Array::from_iter_values((0..4096).map(|_| rng.random()));
    let cols = vec![Arc::new(values) as ArrayRef];
    do_bench(c, "4096 f64(0)", cols);

    // Single String Column tests
    let cols = vec![Arc::new(create_string_array_with_len::<i32>(4096, 0., 10)) as ArrayRef];
    do_bench(c, "4096 string(10, 0)", cols);

    let cols = vec![Arc::new(create_string_array_with_len::<i32>(4096, 0., 30)) as ArrayRef];
    do_bench(c, "4096 string(30, 0)", cols);

    let cols = vec![Arc::new(create_string_array_with_len::<i32>(4096, 0., 100)) as ArrayRef];
    do_bench(c, "4096 string(100, 0)", cols);

    let cols = vec![Arc::new(create_string_array_with_len::<i32>(4096, 0.5, 100)) as ArrayRef];
    do_bench(c, "4096 string(100, 0.5)", cols);

    // Single StringView Column tests
    let cols = vec![Arc::new(create_string_view_array_with_len(4096, 0., 10, false)) as ArrayRef];
    do_bench(c, "4096 StringView(10, 0)", cols);

    let cols = vec![Arc::new(create_string_view_array_with_len(4096, 0., 30, false)) as ArrayRef];
    do_bench(c, "4096 StringView(30, 0)", cols);

    let cols = vec![Arc::new(create_string_view_array_with_len(4096, 0., 100, false)) as ArrayRef];
    do_bench(c, "4096 StringView(100, 0)", cols);

    let cols = vec![Arc::new(create_string_view_array_with_len(4096, 0.5, 100, false)) as ArrayRef];
    do_bench(c, "4096 StringView(100, 0.5)", cols);

    // Multi-Column (with String) tests
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
        Arc::new(create_string_array_with_len::<i32>(4096, 0.5, 20)) as ArrayRef,
        Arc::new(create_string_array_with_len::<i32>(4096, 0., 30)) as ArrayRef,
        Arc::new(create_primitive_array::<Float64Type>(4096, 0.)) as ArrayRef,
        Arc::new(create_primitive_array::<Int64Type>(4096, 0.)) as ArrayRef,
    ];
    do_bench(
        c,
        "4096 string(20, 0.5), string(30, 0), f64(0), i64(0)",
        cols,
    );

    // Multi-Column (with StringView) tests
    let cols = vec![
        Arc::new(create_string_view_array_with_len(4096, 0.5, 20, false)) as ArrayRef,
        Arc::new(create_string_view_array_with_len(4096, 0., 30, false)) as ArrayRef,
        Arc::new(create_string_view_array_with_len(4096, 0., 100, false)) as ArrayRef,
        Arc::new(create_primitive_array::<Int64Type>(4096, 0.)) as ArrayRef,
    ];
    do_bench(
        c,
        "4096 StringView(20, 0.5), StringView(30, 0), StringView(100, 0), i64(0)",
        cols,
    );

    let cols = vec![
        Arc::new(create_string_view_array_with_len(4096, 0.5, 20, false)) as ArrayRef,
        Arc::new(create_string_view_array_with_len(4096, 0., 30, false)) as ArrayRef,
        Arc::new(create_primitive_array::<Float64Type>(4096, 0.)) as ArrayRef,
        Arc::new(create_primitive_array::<Int64Type>(4096, 0.)) as ArrayRef,
    ];
    do_bench(
        c,
        "4096 StringView(20, 0.5), StringView(30, 0), f64(0), i64(0)",
        cols,
    );

    // Single Decimal Column tests
    let cols = vec![create_decimal_array::<Decimal32Type>(4096, 0., 9, 9, 2)];
    do_bench(c, "4096 decimal32(9, 2) 9 digits(0)", cols);

    let cols = vec![create_decimal_array::<Decimal64Type>(4096, 0., 18, 18, 2)];
    do_bench(c, "4096 decimal64(18, 2) 18 digits(0)", cols);

    let cols = vec![create_decimal_array::<Decimal128Type>(4096, 0., 5, 10, 2)];
    do_bench(c, "4096 decimal128(10, 2) 5 digits(0)", cols);

    let cols = vec![create_decimal_array::<Decimal128Type>(4096, 0., 10, 10, 2)];
    do_bench(c, "4096 decimal128(10, 2) 10 digits(0)", cols);

    let cols = vec![create_decimal_array::<Decimal128Type>(4096, 0., 18, 18, 6)];
    do_bench(c, "4096 decimal128(18, 6) 18 digits(0)", cols);

    let cols = vec![create_decimal_array::<Decimal128Type>(4096, 0., 38, 38, 10)];
    do_bench(c, "4096 decimal128(38, 10) 38 digits(0)", cols);

    let cols = vec![create_decimal_array::<Decimal256Type>(4096, 0., 76, 76, 10)];
    do_bench(c, "4096 decimal256(76, 10) 76 digits(0)", cols);

    // Multi-Column (with Decimal) tests
    let cols = vec![
        create_decimal_array::<Decimal128Type>(4096, 0., 10, 10, 2),
        create_decimal_array::<Decimal128Type>(4096, 0., 18, 18, 6),
        create_decimal_array::<Decimal128Type>(4096, 0., 10, 10, 1),
    ];
    do_bench(
        c,
        "4096 decimal128(10, 2), decimal128(18, 6), decimal128(10, 1)(0)",
        cols,
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
