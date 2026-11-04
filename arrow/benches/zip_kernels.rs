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
use rand::{RngExt, SeedableRng};
use std::hint;
use std::ops::Range;
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::*;
use arrow::util::bench_util::*;
use arrow_select::zip::zip;

/// Each benchmark cycles through this many arrays and random masks, one per
/// iteration.
const NUM_INPUTS: usize = 64;

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

/// Returns the masks for each mask case: `NUM_INPUTS` of each random case, and
/// one of each constant case.
fn mask_cases(len: usize) -> Vec<(&'static str, Vec<BooleanArray>)> {
    let random = |null_density, true_density| -> Vec<BooleanArray> {
        (0..NUM_INPUTS as u64)
            .map(|seed| create_boolean_array_with_seed(len, null_density, true_density, seed))
            .collect()
    };
    vec![
        ("all_true", vec![create_boolean_array(len, 0.0, 1.0)]),
        ("99pct_true", random(0.0, 0.99)),
        ("90pct_true", random(0.0, 0.9)),
        ("50pct_true", random(0.0, 0.5)),
        (
            "true_then_false",
            vec![BooleanArray::from_iter((0..len).map(|i| i < len / 2))],
        ),
        ("10pct_true", random(0.0, 0.1)),
        ("1pct_true", random(0.0, 0.01)),
        ("all_false", vec![create_boolean_array(len, 0.0, 0.0)]),
        ("50pct_nulls", random(0.5, 0.5)),
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

    let arrays = |first_seed: u64| -> Vec<ArrayRef> {
        (0..NUM_INPUTS as u64)
            .map(|i| input_generator.generate_array(first_seed + i, ARRAY_LEN, 0.1))
            .collect()
    };
    let arrays_1_10pct_nulls = arrays(42);
    let arrays_2_10pct_nulls = arrays(42 + NUM_INPUTS as u64);

    let masks = mask_cases(ARRAY_LEN);

    // Benchmarks for different scalar combinations
    for (description, truthy, falsy) in &[
        ("null_vs_non_null_scalar", &null_scalar, &non_null_scalar_1),
        ("non_nulls_scalars", &non_null_scalar_1, &non_null_scalar_2),
    ] {
        bench_zip_input_on_all_masks(
            description,
            &mut group,
            &masks,
            &[Scalar::new(truthy)],
            &[Scalar::new(falsy)],
        );
    }

    bench_zip_input_on_all_masks(
        "array_vs_non_null_scalar",
        &mut group,
        &masks,
        &arrays_1_10pct_nulls,
        &[Scalar::new(non_null_scalar_1.clone())],
    );

    bench_zip_input_on_all_masks(
        "non_null_scalar_vs_array",
        &mut group,
        &masks,
        &[Scalar::new(non_null_scalar_1.clone())],
        &arrays_1_10pct_nulls,
    );

    bench_zip_input_on_all_masks(
        "array_vs_array",
        &mut group,
        &masks,
        &arrays_1_10pct_nulls,
        &arrays_2_10pct_nulls,
    );

    group.finish();
}

/// Benchmarks `zip` for each mask case. Each iteration takes the next of the
/// case's masks, of `truthy` and of `falsy`, cycling through each list on its
/// own, so a one-element list stays the same.
fn bench_zip_input_on_all_masks(
    description: &str,
    group: &mut BenchmarkGroup<WallTime>,
    mask_cases: &[(&str, Vec<BooleanArray>)],
    truthy: &[impl Datum],
    falsy: &[impl Datum],
) {
    for (mask_description, masks) in mask_cases {
        let id = BenchmarkId::new(description, mask_description);
        let mut masks = masks.iter().cycle();
        let mut truthy = truthy.iter().cycle();
        let mut falsy = falsy.iter().cycle();
        group.bench_function(id, |b| {
            b.iter(|| {
                hint::black_box(zip(
                    masks.next().unwrap(),
                    truthy.next().unwrap(),
                    falsy.next().unwrap(),
                ))
            })
        });
    }
}

fn add_benchmark(c: &mut Criterion) {
    bench_zip_on_input_generator(
        c,
        &GeneratePrimitive::<Int32Type> {
            description: "i32".to_string(),
            _marker: std::marker::PhantomData,
        },
    );

    bench_zip_on_input_generator(
        c,
        &GenerateBytes::<GenericStringType<i32>> {
            description: "strings (3..20)".to_string(),
            range_length: 3..20,
            _marker: std::marker::PhantomData,
        },
    );

    bench_zip_on_input_generator(
        c,
        &GenerateStringView {
            description: "string_views size (3..20)".to_string(),
            range: 3..20,
            _marker: std::marker::PhantomData,
        },
    );
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
