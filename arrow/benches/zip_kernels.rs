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

use criterion::measurement::WallTime;
use criterion::{BenchmarkGroup, BenchmarkId, Criterion, criterion_group, criterion_main};
use rand::distr::{Distribution, StandardUniform};
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use std::hint;
use std::ops::Range;
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::*;
use arrow::util::bench_util::*;
use arrow_select::zip::zip;

trait InputGenerator {
    fn name(&self) -> &str;

    /// Return an ArrayRef containing a single null value
    fn generate_scalar_with_null_value(&self) -> ArrayRef;

    /// Generate a `number_of_scalars` unique scalars
    fn generate_non_null_scalars(&self, seed: u64, number_of_scalars: usize) -> Vec<ArrayRef>;

    /// Generate array with specified length and null percentage
    fn generate_array(&self, seed: u64, array_length: usize, null_percentage: f32) -> ArrayRef;
}

struct GeneratePrimitive<T: ArrowPrimitiveType> {
    description: String,
    _marker: std::marker::PhantomData<T>,
}

impl<T> InputGenerator for GeneratePrimitive<T>
where
    T: ArrowPrimitiveType,
    StandardUniform: Distribution<T::Native>,
{
    fn name(&self) -> &str {
        self.description.as_str()
    }

    fn generate_scalar_with_null_value(&self) -> ArrayRef {
        new_null_array(&T::DATA_TYPE, 1)
    }

    fn generate_non_null_scalars(&self, seed: u64, number_of_scalars: usize) -> Vec<ArrayRef> {
        let rng = StdRng::seed_from_u64(seed);

        rng.sample_iter::<T::Native, _>(StandardUniform)
            .take(number_of_scalars)
            .map(|v: T::Native| {
                Arc::new(PrimitiveArray::<T>::new_scalar(v).into_inner()) as ArrayRef
            })
            .collect()
    }

    fn generate_array(&self, seed: u64, array_length: usize, null_percentage: f32) -> ArrayRef {
        Arc::new(create_primitive_array_with_seed::<T>(
            array_length,
            null_percentage,
            seed,
        ))
    }
}

struct GenerateBytes<Byte: ByteArrayType> {
    range_length: std::ops::Range<usize>,
    description: String,

    _marker: std::marker::PhantomData<Byte>,
}

impl<Byte> InputGenerator for GenerateBytes<Byte>
where
    Byte: ByteArrayType,
{
    fn name(&self) -> &str {
        self.description.as_str()
    }

    fn generate_scalar_with_null_value(&self) -> ArrayRef {
        new_null_array(&Byte::DATA_TYPE, 1)
    }

    fn generate_non_null_scalars(&self, seed: u64, number_of_scalars: usize) -> Vec<ArrayRef> {
        let array = self.generate_array(seed, number_of_scalars, 0.0);

        (0..number_of_scalars).map(|i| array.slice(i, 1)).collect()
    }

    fn generate_array(&self, seed: u64, array_length: usize, null_percentage: f32) -> ArrayRef {
        let is_binary =
            Byte::DATA_TYPE == DataType::Binary || Byte::DATA_TYPE == DataType::LargeBinary;
        if is_binary {
            Arc::new(create_binary_array_with_len_range_and_prefix_and_seed::<
                Byte::Offset,
            >(
                array_length,
                null_percentage,
                self.range_length.start,
                self.range_length.end - 1,
                &[],
                seed,
            ))
        } else {
            Arc::new(create_string_array_with_len_range_and_prefix_and_seed::<
                Byte::Offset,
            >(
                array_length,
                null_percentage,
                self.range_length.start,
                self.range_length.end - 1,
                "",
                seed,
            ))
        }
    }
}

struct GenerateStringView {
    range: Range<usize>,
    description: String,
    _marker: std::marker::PhantomData<StringViewType>,
}

impl InputGenerator for GenerateStringView {
    fn name(&self) -> &str {
        self.description.as_str()
    }
    fn generate_scalar_with_null_value(&self) -> ArrayRef {
        new_null_array(&DataType::Utf8View, 1)
    }

    fn generate_non_null_scalars(&self, seed: u64, number_of_scalars: usize) -> Vec<ArrayRef> {
        let array = self.generate_array(seed, number_of_scalars, 0.0);
        (0..number_of_scalars).map(|i| array.slice(i, 1)).collect()
    }

    fn generate_array(&self, seed: u64, array_length: usize, null_percentage: f32) -> ArrayRef {
        Arc::new(create_string_view_array_with_len_range_and_seed(
            array_length,
            null_percentage,
            self.range.clone(),
            seed,
        ))
    }
}

fn mask_cases(len: usize) -> Vec<(&'static str, BooleanArray)> {
    vec![
        ("all_true", create_boolean_array(len, 0.0, 1.0)),
        ("99pct_true", create_boolean_array(len, 0.0, 0.99)),
        ("90pct_true", create_boolean_array(len, 0.0, 0.9)),
        ("50pct_true", create_boolean_array(len, 0.0, 0.5)),
        ("10pct_true", create_boolean_array(len, 0.0, 0.1)),
        ("1pct_true", create_boolean_array(len, 0.0, 0.01)),
        ("all_false", create_boolean_array(len, 0.0, 0.0)),
        ("50pct_nulls", create_boolean_array(len, 0.5, 0.5)),
    ]
}

fn bench_zip_on_input_generator(c: &mut Criterion, input_generator: &impl InputGenerator) {
    const ARRAY_LEN: usize = 8192;

    let mut group =
        c.benchmark_group(format!("zip_{ARRAY_LEN}_from_{}", input_generator.name()).as_str());

    let null_scalar = input_generator.generate_scalar_with_null_value();
    let [non_null_scalar_1, non_null_scalar_2]: [_; 2] = input_generator
        .generate_non_null_scalars(42, 2)
        .try_into()
        .unwrap();

    let array_1_10pct_nulls = input_generator.generate_array(42, ARRAY_LEN, 0.1);
    let array_2_10pct_nulls = input_generator.generate_array(18, ARRAY_LEN, 0.1);

    let masks = mask_cases(ARRAY_LEN);

    // Benchmarks for different scalar combinations
    for (description, truthy, falsy) in &[
        ("null_vs_non_null_scalar", &null_scalar, &non_null_scalar_1),
        (
            "non_null_scalar_vs_null_scalar",
            &non_null_scalar_1,
            &null_scalar,
        ),
        ("non_nulls_scalars", &non_null_scalar_1, &non_null_scalar_2),
    ] {
        bench_zip_input_on_all_masks(
            description,
            &mut group,
            &masks,
            &Scalar::new(truthy),
            &Scalar::new(falsy),
        );
    }

    bench_zip_input_on_all_masks(
        "array_vs_non_null_scalar",
        &mut group,
        &masks,
        &array_1_10pct_nulls,
        &non_null_scalar_1,
    );

    bench_zip_input_on_all_masks(
        "non_null_scalar_vs_array",
        &mut group,
        &masks,
        &non_null_scalar_1,
        &array_1_10pct_nulls,
    );

    bench_zip_input_on_all_masks(
        "array_vs_array",
        &mut group,
        &masks,
        &array_1_10pct_nulls,
        &array_2_10pct_nulls,
    );

    group.finish();
}

fn bench_zip_input_on_all_masks(
    description: &str,
    group: &mut BenchmarkGroup<WallTime>,
    masks: &[(&str, BooleanArray)],
    truthy: &impl Datum,
    falsy: &impl Datum,
) {
    for (mask_description, mask) in masks {
        let id = BenchmarkId::new(description, mask_description);
        group.bench_with_input(id, mask, |b, mask| {
            b.iter(|| hint::black_box(zip(mask, truthy, falsy)))
        });
    }
}

fn add_benchmark(c: &mut Criterion) {
    // Primitive
    bench_zip_on_input_generator(
        c,
        &GeneratePrimitive::<Int32Type> {
            description: "i32".to_string(),
            _marker: std::marker::PhantomData,
        },
    );

    // Short strings
    bench_zip_on_input_generator(
        c,
        &GenerateBytes::<GenericStringType<i32>> {
            description: "short strings (3..10)".to_string(),
            range_length: 3..10,
            _marker: std::marker::PhantomData,
        },
    );

    // Long strings
    bench_zip_on_input_generator(
        c,
        &GenerateBytes::<GenericStringType<i32>> {
            description: "long strings (100..400)".to_string(),
            range_length: 100..400,
            _marker: std::marker::PhantomData,
        },
    );

    // Short Bytes
    bench_zip_on_input_generator(
        c,
        &GenerateBytes::<GenericBinaryType<i32>> {
            description: "short bytes (3..10)".to_string(),
            range_length: 3..10,
            _marker: std::marker::PhantomData,
        },
    );

    // Long Bytes
    bench_zip_on_input_generator(
        c,
        &GenerateBytes::<GenericBinaryType<i32>> {
            description: "long bytes (100..400)".to_string(),
            range_length: 100..400,
            _marker: std::marker::PhantomData,
        },
    );

    bench_zip_on_input_generator(
        c,
        &GenerateStringView {
            description: "string_views size (3..10)".to_string(),
            range: 3..10,
            _marker: std::marker::PhantomData,
        },
    );

    bench_zip_on_input_generator(
        c,
        &GenerateStringView {
            description: "string_views size (10..100)".to_string(),
            range: 10..100,
            _marker: std::marker::PhantomData,
        },
    );
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
