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

use arrow::array::{Array, ArrayRef, StringArray};
use arrow::util::test_util::seedable_rng;
use criterion::{criterion_group, criterion_main, Criterion};
use parquet_variant::{Variant, VariantBuilder};
use parquet_variant_compute::variant_get::{variant_get, GetOptions};
use parquet_variant_compute::{batch_json_string_to_variant, VariantArray, VariantArrayBuilder};
use rand::distr::Alphanumeric;
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;
use std::fmt::Write;
use std::sync::Arc;
fn benchmark_batch_json_string_to_variant(c: &mut Criterion) {
    let input_array = StringArray::from_iter_values(small_repeated_json_structure(8000));
    let array_ref: ArrayRef = Arc::new(input_array);
    c.bench_function(
        "batch_json_string_to_variant small_repeated_json 8k string",
        |b| {
            b.iter(|| {
                let _ = batch_json_string_to_variant(&array_ref).unwrap();
            });
        },
    );

    let input_array = StringArray::from_iter_values(small_random_json_structure(8000));
    let total_input_bytes = input_array
        .iter()
        .filter_map(|v| v)
        .map(|v| v.len())
        .sum::<usize>();
    let id = format!(
        "batch_json_string_to_variant random_json({} bytes per document)",
        total_input_bytes / input_array.len()
    );
    let array_ref: ArrayRef = Arc::new(input_array);
    c.bench_function(&id, |b| {
        b.iter(|| {
            let _ = batch_json_string_to_variant(&array_ref).unwrap();
        });
    });
}

fn create_primitive_variant(size: usize) -> VariantArray {
    let mut rng = StdRng::seed_from_u64(42);

    let mut variant_builder = VariantArrayBuilder::new(1);

    for _ in 0..size {
        let mut builder = VariantBuilder::new();
        builder.append_value(rng.random::<i64>());
        let (metadata, value) = builder.finish();
        variant_builder.append_variant(Variant::try_new(&metadata, &value).unwrap());
    }

    variant_builder.build()
}

pub fn variant_get_bench(c: &mut Criterion) {
    let variant_array = create_primitive_variant(8192);
    let input: ArrayRef = Arc::new(variant_array);

    let options = GetOptions {
        path: vec![].into(),
        as_type: None,
        cast_options: Default::default(),
    };

    c.bench_function("variant_get_primitive", |b| {
        b.iter(|| variant_get(&input.clone(), options.clone()))
    });
}

criterion_group!(
    benches,
    variant_get_bench,
    benchmark_batch_json_string_to_variant
);
criterion_main!(benches);

/// This function generates a vector of JSON strings, each representing a person
/// with random first name, last name, and age.
///
/// Example:
/// ```json
/// {
///   "first" : random_string_of_1_to_20_characters,
///   "last" : random_string_of_1_to_20_characters,
///   "age": random_value_between_20_and_80,
/// }
/// ```
fn small_repeated_json_structure(count: usize) -> impl Iterator<Item = String> {
    let mut rng = seedable_rng();
    (0..count).map(move |_| {
        let first: String = (0..rng.random_range(1..=20))
            .map(|_| rng.sample(Alphanumeric) as char)
            .collect();
        let last: String = (0..rng.random_range(1..=20))
            .map(|_| rng.sample(Alphanumeric) as char)
            .collect();
        let age: u8 = rng.random_range(20..=80);
        format!("{{\"first\":\"{first}\",\"last\":\"{last}\",\"age\":{age}}}")
    })
}

/// This function generates a vector of JSON strings which have many fields
/// and a random structure (including field names)
fn small_random_json_structure(count: usize) -> impl Iterator<Item = String> {
    let mut generator = RandomJsonGenerator::new();
    (0..count).map(move |_| generator.next().to_string())
}

/// Creates JSON with random structure and fields.
///
/// Each type is created in a random proportion, controlled by the
/// probabilities. The sum of all probabilities should be 1.0.
struct RandomJsonGenerator {
    /// Random number generator
    rng: StdRng,
    /// the probability of generating a null value
    null_probability: f64,
    /// the probably of generating a string value
    string_probability: f64,
    /// the probably of generating a number value
    number_probability: f64,
    /// the probably of generating a boolean value
    boolean_probability: f64,
    /// the probably of generating an object value
    object_probability: f64,
    // probability of generating a JSON array is the remaining probability
    /// The maximum depth of the generated JSON structure
    max_depth: usize,
    /// output buffer
    output_buffer: String,
}

impl RandomJsonGenerator {
    fn new() -> Self {
        let rng = seedable_rng();
        Self {
            rng,
            null_probability: 0.05,
            string_probability: 0.25,
            number_probability: 0.25,
            boolean_probability: 0.10,
            object_probability: 0.10,
            max_depth: 5,
            output_buffer: String::new(),
        }
    }

    fn next(&mut self) -> &str {
        self.output_buffer.clear();
        self.append_random_json(0);
        &self.output_buffer
    }

    /// Appends a random JSON value to the output buffer.
    fn append_random_json(&mut self, current_depth: usize) {
        if current_depth >= self.max_depth {
            write!(&mut self.output_buffer, "\"max_depth reached\"").unwrap();
            return;
        }
        // Generate a random number to determine the type
        let random_value: f64 = self.rng.random();
        if random_value < self.null_probability {
            write!(&mut self.output_buffer, "null").unwrap();
        } else if random_value < self.null_probability + self.string_probability {
            // Generate a random string between 1 and 500 characters
            let length = self.rng.random_range(1..=500);
            let random_string: String = (0..length)
                .map(|_| self.rng.sample(Alphanumeric) as char)
                .collect();
            write!(&mut self.output_buffer, "\"{random_string}\"",).unwrap();
        } else if random_value
            < self.null_probability + self.string_probability + self.number_probability
        {
            // Generate a random number
            let random_number: f64 = self.rng.random_range(-1000.0..1000.0);
            write!(&mut self.output_buffer, "{random_number}",).unwrap();
        } else if random_value
            < self.null_probability
                + self.string_probability
                + self.number_probability
                + self.boolean_probability
        {
            // Generate a random boolean
            let random_boolean: bool = self.rng.random();
            write!(&mut self.output_buffer, "{random_boolean}",).unwrap();
        } else if random_value
            < self.null_probability
                + self.string_probability
                + self.number_probability
                + self.boolean_probability
                + self.object_probability
        {
            // Generate a random object
            let num_fields = self.rng.random_range(1..=10);

            write!(&mut self.output_buffer, "{{").unwrap();
            for i in 0..num_fields {
                let key_length = self.rng.random_range(1..=20);
                let key: String = (0..key_length)
                    .map(|_| self.rng.sample(Alphanumeric) as char)
                    .collect();
                write!(&mut self.output_buffer, "\"{key}\":").unwrap();
                self.append_random_json(current_depth + 1);
                if i < num_fields - 1 {
                    write!(&mut self.output_buffer, ",").unwrap();
                }
            }
            write!(&mut self.output_buffer, "}}").unwrap();
        } else {
            // Generate a random array
            let length = self.rng.random_range(1..=10);
            write!(&mut self.output_buffer, "[").unwrap();
            for i in 0..length {
                self.append_random_json(current_depth + 1);
                if i < length - 1 {
                    write!(&mut self.output_buffer, ",").unwrap();
                }
            }
            write!(&mut self.output_buffer, "]").unwrap();
        }
    }
}
