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
use rand::distributions::{Distribution, Standard, Uniform};
use rand::Rng;

use chrono::DateTime;
use std::sync::Arc;

extern crate arrow;

use arrow::array::*;
use arrow::compute::cast;
use arrow::datatypes::*;
use arrow::util::bench_util::*;
use arrow::util::test_util::seedable_rng;

fn build_array<T: ArrowPrimitiveType>(size: usize) -> ArrayRef
where
    Standard: Distribution<T::Native>,
{
    let array = create_primitive_array::<T>(size, 0.1);
    Arc::new(array)
}

fn build_utf8_date_array(size: usize, with_nulls: bool) -> ArrayRef {
    use chrono::NaiveDate;

    // use random numbers to avoid spurious compiler optimizations wrt to branching
    let mut rng = seedable_rng();
    let mut builder = StringBuilder::new();
    let range = Uniform::new(0, 737776);

    for _ in 0..size {
        if with_nulls && rng.gen::<f32>() > 0.8 {
            builder.append_null();
        } else {
            let string = NaiveDate::from_num_days_from_ce_opt(rng.sample(range))
                .unwrap()
                .format("%Y-%m-%d")
                .to_string();
            builder.append_value(&string);
        }
    }
    Arc::new(builder.finish())
}

fn build_utf8_date_time_array(size: usize, with_nulls: bool) -> ArrayRef {
    // use random numbers to avoid spurious compiler optimizations wrt to branching
    let mut rng = seedable_rng();
    let mut builder = StringBuilder::new();
    let range = Uniform::new(0, 1608071414123);

    for _ in 0..size {
        if with_nulls && rng.gen::<f32>() > 0.8 {
            builder.append_null();
        } else {
            let string = DateTime::from_timestamp(rng.sample(range), 0)
                .unwrap()
                .format("%Y-%m-%dT%H:%M:%S")
                .to_string();
            builder.append_value(&string);
        }
    }
    Arc::new(builder.finish())
}

fn build_decimal128_array(size: usize, precision: u8, scale: i8) -> ArrayRef {
    let mut rng = seedable_rng();
    let mut builder = Decimal128Builder::with_capacity(size);

    for _ in 0..size {
        builder.append_value(rng.gen_range::<i128, _>(0..1000000000));
    }
    Arc::new(
        builder
            .finish()
            .with_precision_and_scale(precision, scale)
            .unwrap(),
    )
}

fn build_decimal256_array(size: usize, precision: u8, scale: i8) -> ArrayRef {
    let mut rng = seedable_rng();
    let mut builder = Decimal256Builder::with_capacity(size);
    let mut bytes = [0; 32];
    for _ in 0..size {
        let num = rng.gen_range::<i128, _>(0..1000000000);
        bytes[0..16].clone_from_slice(&num.to_le_bytes());
        builder.append_value(i256::from_le_bytes(bytes));
    }
    Arc::new(
        builder
            .finish()
            .with_precision_and_scale(precision, scale)
            .unwrap(),
    )
}

fn build_string_array(size: usize) -> ArrayRef {
    let mut builder = StringBuilder::new();
    for v in 0..size {
        match v % 3 {
            0 => builder.append_value("small"),
            1 => builder.append_value("larger string more than 12 bytes"),
            _ => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

fn build_dict_array(size: usize) -> ArrayRef {
    let values = StringArray::from_iter([
        Some("small"),
        Some("larger string more than 12 bytes"),
        None,
    ]);
    let keys = UInt64Array::from_iter((0..size as u64).map(|v| v % 3));

    Arc::new(DictionaryArray::new(keys, Arc::new(values)))
}

// cast array from specified primitive array type to desired data type
fn cast_array(array: &ArrayRef, to_type: DataType) {
    criterion::black_box(cast(array, &to_type).unwrap());
}

fn add_benchmark(c: &mut Criterion) {
    let i32_array = build_array::<Int32Type>(512);
    let i64_array = build_array::<Int64Type>(512);
    let f32_array = build_array::<Float32Type>(512);
    let f32_utf8_array = cast(&build_array::<Float32Type>(512), &DataType::Utf8).unwrap();

    let f64_array = build_array::<Float64Type>(512);
    let date64_array = build_array::<Date64Type>(512);
    let date32_array = build_array::<Date32Type>(512);
    let time32s_array = build_array::<Time32SecondType>(512);
    let time64ns_array = build_array::<Time64NanosecondType>(512);
    let time_ns_array = build_array::<TimestampNanosecondType>(512);
    let time_ms_array = build_array::<TimestampMillisecondType>(512);
    let utf8_date_array = build_utf8_date_array(512, true);
    let utf8_date_time_array = build_utf8_date_time_array(512, true);

    let decimal128_array = build_decimal128_array(512, 10, 3);
    let decimal256_array = build_decimal256_array(512, 50, 3);
    let string_array = build_string_array(512);
    let wide_string_array = cast(&string_array, &DataType::LargeUtf8).unwrap();

    let dict_array = build_dict_array(10_000);
    let string_view_array = cast(&dict_array, &DataType::Utf8View).unwrap();
    let binary_view_array = cast(&string_view_array, &DataType::BinaryView).unwrap();

    c.bench_function("cast int32 to int32 512", |b| {
        b.iter(|| cast_array(&i32_array, DataType::Int32))
    });
    c.bench_function("cast int32 to uint32 512", |b| {
        b.iter(|| cast_array(&i32_array, DataType::UInt32))
    });
    c.bench_function("cast int32 to float32 512", |b| {
        b.iter(|| cast_array(&i32_array, DataType::Float32))
    });
    c.bench_function("cast int32 to float64 512", |b| {
        b.iter(|| cast_array(&i32_array, DataType::Float64))
    });
    c.bench_function("cast int32 to int64 512", |b| {
        b.iter(|| cast_array(&i32_array, DataType::Int64))
    });
    c.bench_function("cast float32 to int32 512", |b| {
        b.iter(|| cast_array(&f32_array, DataType::Int32))
    });
    c.bench_function("cast float64 to float32 512", |b| {
        b.iter(|| cast_array(&f64_array, DataType::Float32))
    });
    c.bench_function("cast float64 to uint64 512", |b| {
        b.iter(|| cast_array(&f64_array, DataType::UInt64))
    });
    c.bench_function("cast int64 to int32 512", |b| {
        b.iter(|| cast_array(&i64_array, DataType::Int32))
    });
    c.bench_function("cast date64 to date32 512", |b| {
        b.iter(|| cast_array(&date64_array, DataType::Date32))
    });
    c.bench_function("cast date32 to date64 512", |b| {
        b.iter(|| cast_array(&date32_array, DataType::Date64))
    });
    c.bench_function("cast time32s to time32ms 512", |b| {
        b.iter(|| cast_array(&time32s_array, DataType::Time32(TimeUnit::Millisecond)))
    });
    c.bench_function("cast time32s to time64us 512", |b| {
        b.iter(|| cast_array(&time32s_array, DataType::Time64(TimeUnit::Microsecond)))
    });
    c.bench_function("cast time64ns to time32s 512", |b| {
        b.iter(|| cast_array(&time64ns_array, DataType::Time32(TimeUnit::Second)))
    });
    c.bench_function("cast timestamp_ns to timestamp_s 512", |b| {
        b.iter(|| {
            cast_array(
                &time_ns_array,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            )
        })
    });
    c.bench_function("cast timestamp_ms to timestamp_ns 512", |b| {
        b.iter(|| {
            cast_array(
                &time_ms_array,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            )
        })
    });
    c.bench_function("cast utf8 to f32", |b| {
        b.iter(|| cast_array(&f32_utf8_array, DataType::Float32))
    });
    c.bench_function("cast i64 to string 512", |b| {
        b.iter(|| cast_array(&i64_array, DataType::Utf8))
    });
    c.bench_function("cast f32 to string 512", |b| {
        b.iter(|| cast_array(&f32_array, DataType::Utf8))
    });
    c.bench_function("cast f64 to string 512", |b| {
        b.iter(|| cast_array(&f64_array, DataType::Utf8))
    });
    c.bench_function("cast timestamp_ms to i64 512", |b| {
        b.iter(|| cast_array(&time_ms_array, DataType::Int64))
    });
    c.bench_function("cast utf8 to date32 512", |b| {
        b.iter(|| cast_array(&utf8_date_array, DataType::Date32))
    });
    c.bench_function("cast utf8 to date64 512", |b| {
        b.iter(|| cast_array(&utf8_date_time_array, DataType::Date64))
    });

    c.bench_function("cast decimal128 to decimal128 512", |b| {
        b.iter(|| cast_array(&decimal128_array, DataType::Decimal128(30, 5)))
    });
    c.bench_function("cast decimal128 to decimal128 512 lower precision", |b| {
        b.iter(|| cast_array(&decimal128_array, DataType::Decimal128(6, 5)))
    });
    c.bench_function("cast decimal128 to decimal256 512", |b| {
        b.iter(|| cast_array(&decimal128_array, DataType::Decimal256(50, 5)))
    });
    c.bench_function("cast decimal256 to decimal128 512", |b| {
        b.iter(|| cast_array(&decimal256_array, DataType::Decimal128(38, 2)))
    });
    c.bench_function("cast decimal256 to decimal256 512", |b| {
        b.iter(|| cast_array(&decimal256_array, DataType::Decimal256(50, 5)))
    });

    c.bench_function("cast decimal128 to decimal128 512 with same scale", |b| {
        b.iter(|| cast_array(&decimal128_array, DataType::Decimal128(30, 3)))
    });

    c.bench_function(
        "cast decimal128 to decimal128 512 with lower scale (infallible)",
        |b| b.iter(|| cast_array(&decimal128_array, DataType::Decimal128(7, -1))),
    );

    c.bench_function("cast decimal256 to decimal256 512 with same scale", |b| {
        b.iter(|| cast_array(&decimal256_array, DataType::Decimal256(60, 3)))
    });
    c.bench_function("cast dict to string view", |b| {
        b.iter(|| cast_array(&dict_array, DataType::Utf8View))
    });
    c.bench_function("cast string view to dict", |b| {
        b.iter(|| {
            cast_array(
                &string_view_array,
                DataType::Dictionary(Box::new(DataType::UInt64), Box::new(DataType::Utf8)),
            )
        })
    });
    c.bench_function("cast string view to string", |b| {
        b.iter(|| cast_array(&string_view_array, DataType::Utf8))
    });
    c.bench_function("cast string view to wide string", |b| {
        b.iter(|| cast_array(&string_view_array, DataType::LargeUtf8))
    });
    c.bench_function("cast binary view to string", |b| {
        b.iter(|| cast_array(&binary_view_array, DataType::Utf8))
    });
    c.bench_function("cast binary view to wide string", |b| {
        b.iter(|| cast_array(&binary_view_array, DataType::LargeUtf8))
    });
    c.bench_function("cast string to binary view 512", |b| {
        b.iter(|| cast_array(&string_array, DataType::BinaryView))
    });
    c.bench_function("cast wide string to binary view 512", |b| {
        b.iter(|| cast_array(&wide_string_array, DataType::BinaryView))
    });
    c.bench_function("cast string view to binary view", |b| {
        b.iter(|| cast_array(&string_view_array, DataType::BinaryView))
    });
    c.bench_function("cast binary view to string view", |b| {
        b.iter(|| cast_array(&binary_view_array, DataType::Utf8View))
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
