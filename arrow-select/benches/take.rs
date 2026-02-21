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

use criterion::{criterion_group, criterion_main, Criterion};
use std::hint;

use arrow_array::builder::UInt32Builder;
use arrow_array::types::*;
use arrow_array::*;
use rand::distr::{Alphanumeric, Distribution, StandardUniform};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use arrow_select::take::{take, TakeOptions};

fn seedable_rng() -> StdRng {
    StdRng::seed_from_u64(42)
}

fn create_primitive_array<T>(size: usize, null_density: f32) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
    StandardUniform: Distribution<T::Native>,
{
    let mut rng = seedable_rng();
    (0..size)
        .map(|_| {
            if rng.random::<f32>() < null_density {
                None
            } else {
                Some(rng.random())
            }
        })
        .collect()
}

fn create_boolean_array(size: usize, null_density: f32, true_density: f32) -> BooleanArray {
    let mut rng = seedable_rng();
    (0..size)
        .map(|_| {
            if rng.random::<f32>() < null_density {
                None
            } else {
                Some(rng.random::<f32>() < true_density)
            }
        })
        .collect()
}

fn create_string_array(size: usize, null_density: f32) -> StringArray {
    let rng = &mut seedable_rng();
    (0..size)
        .map(|_| {
            if rng.random::<f32>() < null_density {
                None
            } else {
                let str_len = rng.random_range(0..20);
                let value: String = rng
                    .sample_iter(&Alphanumeric)
                    .take(str_len)
                    .map(char::from)
                    .collect();
                Some(value)
            }
        })
        .collect()
}

fn create_string_view_array(size: usize, null_density: f32) -> StringViewArray {
    let rng = &mut seedable_rng();
    (0..size)
        .map(|_| {
            if rng.random::<f32>() < null_density {
                None
            } else {
                let str_len = rng.random_range(0..20);
                let value: String = rng
                    .sample_iter(&Alphanumeric)
                    .take(str_len)
                    .map(char::from)
                    .collect();
                Some(value)
            }
        })
        .collect()
}

fn create_random_index(size: usize, null_density: f32) -> UInt32Array {
    let mut rng = seedable_rng();
    let mut builder = UInt32Builder::with_capacity(size);
    for _ in 0..size {
        if rng.random::<f32>() < null_density {
            builder.append_null();
        } else {
            let value = rng.random_range::<u32, _>(0u32..size as u32);
            builder.append_value(value);
        }
    }
    builder.finish()
}

fn create_fsb_array(size: usize, null_density: f32, value_len: usize) -> FixedSizeBinaryArray {
    let rng = &mut seedable_rng();
    FixedSizeBinaryArray::try_from_sparse_iter_with_size(
        (0..size).map(|_| {
            if rng.random::<f32>() < null_density {
                None
            } else {
                let value: Vec<u8> = rng
                    .sample_iter::<u8, _>(StandardUniform)
                    .take(value_len)
                    .collect();
                Some(value)
            }
        }),
        value_len as i32,
    )
    .unwrap()
}

fn bench_take(values: &dyn Array, indices: &UInt32Array) {
    hint::black_box(take(values, indices, None).unwrap());
}

fn bench_take_bounds_check(values: &dyn Array, indices: &UInt32Array) {
    hint::black_box(
        take(
            values,
            indices,
            Some(TakeOptions {
                check_bounds: true,
            }),
        )
        .unwrap(),
    );
}

fn add_benchmark(c: &mut Criterion) {
    let values = create_primitive_array::<Int32Type>(512, 0.0);
    let indices = create_random_index(512, 0.0);
    c.bench_function("take i32 512", |b| b.iter(|| bench_take(&values, &indices)));

    let values = create_primitive_array::<Int32Type>(1024, 0.0);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take i32 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let indices = create_random_index(1024, 0.5);
    c.bench_function("take i32 null indices 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });


    let values = create_primitive_array::<Int32Type>(1024, 0.5);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take i32 null values 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let indices = create_random_index(1024, 0.5);
    c.bench_function("take i32 null values null indices 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_primitive_array::<Int32Type>(512, 0.0);
    let indices = create_random_index(512, 0.0);
    c.bench_function("take check bounds i32 512", |b| {
        b.iter(|| bench_take_bounds_check(&values, &indices))
    });
    let values = create_primitive_array::<Int32Type>(1024, 0.0);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take check bounds i32 1024", |b| {
        b.iter(|| bench_take_bounds_check(&values, &indices))
    });

    let values = create_boolean_array(512, 0.0, 0.5);
    let indices = create_random_index(512, 0.0);
    c.bench_function("take bool 512", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_boolean_array(1024, 0.0, 0.5);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take bool 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });
    let indices = create_random_index(1024, 0.5);
    c.bench_function("take bool null indices 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_boolean_array(1024, 0.5, 0.5);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take bool null values 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });
    let values = create_boolean_array(1024, 0.5, 0.5);
    let indices = create_random_index(1024, 0.5);
    c.bench_function("take bool null values null indices 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_string_array(512, 0.0);
    let indices = create_random_index(512, 0.0);
    c.bench_function("take str 512", |b| b.iter(|| bench_take(&values, &indices)));

    let values = create_string_array(1024, 0.0);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take str 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });
    let values = create_string_array(512, 0.0);
    let indices = create_random_index(512, 0.5);
    c.bench_function("take str null indices 512", |b| {
        b.iter(|| bench_take(&values, &indices))
    });
    let values = create_string_array(1024, 0.0);
    let indices = create_random_index(1024, 0.5);
    c.bench_function("take str null indices 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });
    let values = create_string_array(1024, 0.5);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take str null values 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });
    let values = create_string_array(1024, 0.5);
    let indices = create_random_index(1024, 0.5);
    c.bench_function("take str null values null indices 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_string_view_array(512, 0.0);
    let indices = create_random_index(512, 0.0);
    c.bench_function("take stringview 512", |b| {
        b.iter(|| bench_take(&values, &indices))
    });
    let values = create_string_view_array(1024, 0.0);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take stringview 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });
    let values = create_string_view_array(512, 0.0);
    let indices = create_random_index(512, 0.5);
    c.bench_function("take stringview null indices 512", |b| {
        b.iter(|| bench_take(&values, &indices))
    });
    let values = create_string_view_array(1024, 0.0);
    let indices = create_random_index(1024, 0.5);
    c.bench_function("take stringview null indices 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });
    let values = create_string_view_array(1024, 0.5);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take stringview null values 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });
    let values = create_string_view_array(1024, 0.5);
    let indices = create_random_index(1024, 0.5);
    c.bench_function("take stringview null values null indices 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_fsb_array(1024, 0.0, 12);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take fsb value len: 12, indices: 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });
    let values = create_fsb_array(1024, 0.5, 12);
    let indices = create_random_index(1024, 0.0);
    c.bench_function(
        "take fsb value len: 12, null values, indices: 1024",
        |b| b.iter(|| bench_take(&values, &indices)),
    );
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
