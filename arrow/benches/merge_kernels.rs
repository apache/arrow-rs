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
use std::sync::Arc;
use std::time::Duration;

use arrow::array::*;
use arrow::datatypes::*;
use arrow::util::bench_util::*;
use arrow_select::merge::merge;

trait InputGenerator {
    fn name(&self) -> &str;

    /// Return an ArrayRef containing a single null value
    fn generate_scalar_with_null_value(&self) -> ArrayRef;

    /// Generate a `number_of_scalars` unique scalars
    fn generate_non_null_scalars(&self, seed: u64, number_of_scalars: usize) -> Vec<ArrayRef>;

    /// Generate an array with the specified length and null percentage
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

/// A mask whose set bits come in runs averaging `run` bits, with the unset
/// runs sized to give `true_density`: a CASE over a clustered or sorted
/// column, where the output is long copies from either side.
fn create_clustered_array(len: usize, true_density: f64, run: f64, seed: u64) -> BooleanArray {
    let mut rng = StdRng::seed_from_u64(seed);
    let leave_set = 1.0 / run;
    let leave_unset = true_density / (run * (1.0 - true_density));
    let mut set = rng.random_bool(true_density);
    (0..len)
        .map(|_| {
            let bit = set;
            set = if set {
                !rng.random_bool(leave_set)
            } else {
                rng.random_bool(leave_unset)
            };
            Some(bit)
        })
        .collect()
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

fn run_mask_cases(len: usize) -> Vec<(&'static str, BooleanArray)> {
    [
        ("runs_of_64_50pct_true", 0.5, 64.0),
        ("runs_of_512_10pct_true", 0.1, 512.0),
    ]
    .into_iter()
    .map(|(name, density, run)| (name, create_clustered_array(len, density, run, 7)))
    .collect()
}

/// Which input combinations to run for a type.
#[derive(PartialEq)]
enum Scenarios {
    All,
    /// Only an array against a scalar, the CASE WHEN ... THEN expr ELSE literal shape.
    ScalarSide,
}

fn bench_merge_on_input_generator(
    c: &mut Criterion,
    input_generator: &impl InputGenerator,
    scenarios: Scenarios,
) {
    const ARRAY_LEN: usize = 8192;

    let mut group =
        c.benchmark_group(format!("merge_{ARRAY_LEN}_from_{}", input_generator.name()).as_str());
    // One merge of 8192 rows takes microseconds, so the default 3 s warm up
    // and 5 s of samples per case would make the whole run take over an hour.
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(1));

    let null_scalar = input_generator.generate_scalar_with_null_value();
    let [non_null_scalar_1, non_null_scalar_2]: [_; 2] = input_generator
        .generate_non_null_scalars(42, 2)
        .try_into()
        .unwrap();

    // For simplicity, we generate arrays with length ARRAY_LEN. Not all input values will be used.
    let array_1_10pct_nulls = input_generator.generate_array(42, ARRAY_LEN, 0.1);
    let array_2_10pct_nulls = input_generator.generate_array(18, ARRAY_LEN, 0.1);
    let array_no_nulls = input_generator.generate_array(42, ARRAY_LEN, 0.0);

    let masks = mask_cases(ARRAY_LEN);
    // Runs only matter where one side is a scalar: they decide between
    // copying runs of the array and writing it row by row.
    let scalar_side_masks: Vec<_> = masks
        .iter()
        .cloned()
        .chain(run_mask_cases(ARRAY_LEN))
        .collect();

    bench_merge_input_on_all_masks(
        "array_vs_non_null_scalar",
        &mut group,
        &scalar_side_masks,
        &array_1_10pct_nulls,
        &Scalar::new(non_null_scalar_1.clone()),
    );

    bench_merge_input_on_all_masks(
        "array_no_nulls_vs_non_null_scalar",
        &mut group,
        &scalar_side_masks,
        &array_no_nulls,
        &Scalar::new(non_null_scalar_1.clone()),
    );

    // CASE WHEN ... THEN array END: the ELSE side is NULL.
    bench_merge_input_on_all_masks(
        "array_vs_null_scalar",
        &mut group,
        &scalar_side_masks,
        &array_1_10pct_nulls,
        &Scalar::new(null_scalar.clone()),
    );

    bench_merge_input_on_all_masks(
        "non_null_scalar_vs_array",
        &mut group,
        &scalar_side_masks,
        &Scalar::new(non_null_scalar_1.clone()),
        &array_1_10pct_nulls,
    );

    if scenarios == Scenarios::ScalarSide {
        group.finish();
        return;
    }

    // Benchmarks for different scalar combinations
    for (description, truthy, falsy) in &[
        ("null_vs_non_null_scalar", &null_scalar, &non_null_scalar_1),
        ("non_nulls_scalars", &non_null_scalar_1, &non_null_scalar_2),
    ] {
        bench_merge_input_on_all_masks(
            description,
            &mut group,
            &masks,
            &Scalar::new(truthy),
            &Scalar::new(falsy),
        );
    }

    bench_merge_input_on_all_masks(
        "array_vs_array",
        &mut group,
        &masks,
        &array_1_10pct_nulls,
        &array_2_10pct_nulls,
    );

    group.finish();
}

fn bench_merge_input_on_all_masks(
    description: &str,
    group: &mut BenchmarkGroup<WallTime>,
    masks: &[(&str, BooleanArray)],
    truthy: &impl Datum,
    falsy: &impl Datum,
) {
    for (mask_description, mask) in masks {
        let id = BenchmarkId::new(description, mask_description);
        group.bench_with_input(id, mask, |b, mask| {
            b.iter(|| hint::black_box(merge(mask, truthy, falsy)))
        });
    }
}

fn add_benchmark(c: &mut Criterion) {
    // Primitive
    bench_merge_on_input_generator(
        c,
        &GeneratePrimitive::<Int32Type> {
            description: "i32".to_string(),
            _marker: std::marker::PhantomData,
        },
        Scenarios::All,
    );

    bench_merge_on_input_generator(
        c,
        &GeneratePrimitive::<Decimal128Type> {
            description: "decimal128".to_string(),
            _marker: std::marker::PhantomData,
        },
        Scenarios::ScalarSide,
    );

    bench_merge_on_input_generator(
        c,
        &GenerateBytes::<GenericStringType<i32>> {
            description: "strings (3..20)".to_string(),
            range_length: 3..20,
            _marker: std::marker::PhantomData,
        },
        Scenarios::All,
    );
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
