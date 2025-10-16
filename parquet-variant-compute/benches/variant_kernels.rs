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
use arrow_schema::{DataType, Field, Fields};
use chrono::{DateTime, Utc};
use criterion::{Criterion, criterion_group, criterion_main};
use parquet_variant::{Uuid, Variant, VariantBuilder, VariantBuilderExt};
use parquet_variant_compute::{
    GetOptions, VariantArray, VariantArrayBuilder, json_to_variant, shred_variant, variant_get,
};
use rand::Rng;
use rand::SeedableRng;
use rand::distr::Alphanumeric;
use rand::rngs::StdRng;
use std::fmt::Write;
use std::sync::Arc;

fn json_to_variant_bench(c: &mut Criterion) {
    let input_array = StringArray::from_iter_values(json_repeated_struct(8000));
    let array_ref: ArrayRef = Arc::new(input_array);
    c.bench_function(
        "batch_json_string_to_variant repeated_struct 8k string",
        |b| {
            b.iter(|| {
                let _ = json_to_variant(&array_ref).unwrap();
            });
        },
    );

    let input_array = StringArray::from_iter_values(json_repeated_list(8000));
    let array_ref: ArrayRef = Arc::new(input_array);
    c.bench_function("batch_json_string_to_variant json_list 8k string", |b| {
        b.iter(|| {
            let _ = json_to_variant(&array_ref).unwrap();
        });
    });

    let input_array = StringArray::from_iter_values(random_json_structure(8000));
    let total_input_bytes = input_array
        .iter()
        .flatten() // filter None
        .map(|v| v.len())
        .sum::<usize>();
    let id = format!(
        "batch_json_string_to_variant random_json({} bytes per document)",
        total_input_bytes / input_array.len()
    );
    let array_ref: ArrayRef = Arc::new(input_array);
    c.bench_function(&id, |b| {
        b.iter(|| {
            let _ = json_to_variant(&array_ref).unwrap();
        });
    });

    let input_array = StringArray::from_iter_values(random_json_structure(8000));
    let total_input_bytes = input_array
        .iter()
        .flatten() // filter None
        .map(|v| v.len())
        .sum::<usize>();
    let id = format!(
        "batch_json_string_to_variant random_json({} bytes per document)",
        total_input_bytes / input_array.len()
    );
    let array_ref: ArrayRef = Arc::new(input_array);
    c.bench_function(&id, |b| {
        b.iter(|| {
            let _ = json_to_variant(&array_ref).unwrap();
        });
    });
}

pub fn variant_get_bench(c: &mut Criterion) {
    let variant_array = create_primitive_variant_array(8192);
    let input = ArrayRef::from(variant_array);

    let options = GetOptions {
        path: vec![].into(),
        as_type: None,
        cast_options: Default::default(),
    };

    c.bench_function("variant_get_primitive", |b| {
        b.iter(|| variant_get(&input, options.clone()))
    });
}

pub fn shred_variant_bench(c: &mut Criterion) {
    // This benchmark models shredding semi-structured log entries
    // where each entry has a common set of fields, some optional fields,
    // and some random extra fields.
    //
    let mut generator = VariantLogGenerator {
        rows_per_batch: 8192,
        optional_field_prob: 0.0,
        extra_field_prob: 0.0,
        rng: StdRng::seed_from_u64(42),
    };

    // shred out the common and optional fields
    // leaving the rest in the value field
    let shredding_schema = DataType::Struct(Fields::from(vec![
        Field::new("timestamp", DataType::Utf8, true),
        Field::new("level", DataType::Utf8, true),
        Field::new("message", DataType::Utf8, true),
        Field::new("user_id", DataType::Int64, true),
        Field::new("session_id", DataType::Utf8, true),
    ]));

    // Variants have only required fields
    let variant_array = generator.next().unwrap();
    c.bench_function("shred_variant common fields", |b| {
        b.iter(|| shred_variant(&variant_array, &shredding_schema))
    });

    // Variants with some optional fields
    generator.optional_field_prob = 0.5;
    let variant_array = generator.next().unwrap();
    c.bench_function("shred_variant optional fields", |b| {
        b.iter(|| shred_variant(&variant_array, &shredding_schema))
    });

    // Variants with optional fields and random extra fields
    generator.extra_field_prob = 0.7;
    let variant_array = generator.next().unwrap();
    c.bench_function("shred_variant extra fields", |b| {
        b.iter(|| shred_variant(&variant_array, &shredding_schema))
    });
}

criterion_group!(
    benches,
    variant_get_bench,
    shred_variant_bench,
    json_to_variant_bench
);
criterion_main!(benches);

/// Creates a `VariantArray` with a specified number of Variant::Int64 values each with random value.
fn create_primitive_variant_array(size: usize) -> VariantArray {
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

/// Return an iterator off JSON strings, each representing a person
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
fn json_repeated_struct(count: usize) -> impl Iterator<Item = String> {
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

/// Return a vector of JSON strings, each representing a list of numbers
///
/// Example:
/// ```json
/// [1.0, 2.0, 3.0, 4.0, 5.0],
/// [5.0],
/// [],
/// null,
/// [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0],
/// ```
fn json_repeated_list(count: usize) -> impl Iterator<Item = String> {
    let mut rng = seedable_rng();
    (0..count).map(move |_| {
        let length = rng.random_range(0..=100);
        let mut output = String::new();
        output.push('[');
        for i in 0..length {
            let value: f64 = rng.random_range(0.0..10000.0);
            write!(&mut output, "{value:.1}").unwrap();
            if i < length - 1 {
                output.push(',');
            }
        }

        output.push(']');
        output
    })
}

/// This function generates a vector of JSON strings which have many fields
/// and a random structure (including field names)
fn random_json_structure(count: usize) -> impl Iterator<Item = String> {
    let mut generator = RandomJsonGenerator {
        null_weight: 5,
        string_weight: 25,
        number_weight: 25,
        boolean_weight: 10,
        object_weight: 25,
        array_weight: 25,
        max_fields: 10,
        max_array_length: 10,
        max_depth: 5,
        ..Default::default()
    };
    (0..count).map(move |_| generator.next().to_string())
}

/// Creates JSON with random structure and fields.
///
/// Each type is created in proportion controlled by the
/// weights
#[derive(Debug)]
struct RandomJsonGenerator {
    /// Random number generator
    rng: StdRng,
    /// the probability of generating a null value
    null_weight: usize,
    /// the probability of generating a string value
    string_weight: usize,
    /// the probability of generating a number value
    number_weight: usize,
    /// the probability of generating a boolean value
    boolean_weight: usize,
    /// the probability of generating an object value
    object_weight: usize,
    /// the probability of generating an array value
    array_weight: usize,

    /// The max number of fields in an object
    max_fields: usize,
    /// the max number of elements in an array
    max_array_length: usize,

    /// The maximum depth of the generated JSON structure
    max_depth: usize,
    /// output buffer
    output_buffer: String,
}

impl Default for RandomJsonGenerator {
    fn default() -> Self {
        let rng = seedable_rng();
        Self {
            rng,
            null_weight: 0,
            string_weight: 0,
            number_weight: 0,
            boolean_weight: 0,
            object_weight: 0,
            array_weight: 0,
            max_fields: 1,
            max_array_length: 1,
            max_depth: 1,
            output_buffer: String::new(),
        }
    }
}

impl RandomJsonGenerator {
    // Generate the next random JSON string.
    fn next(&mut self) -> &str {
        self.output_buffer.clear();
        self.append_random_json(0);
        &self.output_buffer
    }

    /// Appends a random JSON value to the output buffer.
    fn append_random_json(&mut self, current_depth: usize) {
        // use destructuring to ensure each field is used
        let Self {
            rng,
            null_weight,
            string_weight,
            number_weight,
            boolean_weight,
            object_weight,
            array_weight,
            max_fields,
            max_array_length,
            max_depth,
            output_buffer,
        } = self;

        if current_depth >= *max_depth {
            write!(output_buffer, "\"max_depth reached\"").unwrap();
            return;
        }

        let total_weight = *null_weight
            + *string_weight
            + *number_weight
            + *boolean_weight
            + *object_weight
            + *array_weight;

        // Generate a random number to determine the type
        let mut random_value: usize = rng.random_range(0..total_weight);

        if random_value <= *null_weight {
            write!(output_buffer, "null").unwrap();
            return;
        }
        random_value -= *null_weight;

        if random_value <= *string_weight {
            // Generate a random string between 1 and 20 characters
            let length = rng.random_range(1..=20);
            let random_string: String = (0..length)
                .map(|_| rng.sample(Alphanumeric) as char)
                .collect();
            write!(output_buffer, "\"{random_string}\"",).unwrap();
            return;
        }
        random_value -= *string_weight;

        if random_value <= *number_weight {
            // 50% chance of generating an integer or a float
            if rng.random_bool(0.5) {
                // Generate a random integer
                let random_integer: i64 = rng.random_range(-1000..1000);
                write!(output_buffer, "{random_integer}",).unwrap();
            } else {
                // Generate a random float
                let random_float: f64 = rng.random_range(-1000.0..1000.0);
                write!(output_buffer, "{random_float}",).unwrap();
            }
            return;
        }
        random_value -= *number_weight;

        if random_value <= *boolean_weight {
            // Generate a random boolean
            let random_boolean: bool = rng.random();
            write!(output_buffer, "{random_boolean}",).unwrap();
            return;
        }
        random_value -= *boolean_weight;

        if random_value <= *object_weight {
            // Generate a random object
            let num_fields = rng.random_range(1..=*max_fields);

            write!(output_buffer, "{{").unwrap();
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
            return;
        }
        random_value -= *object_weight;

        if random_value <= *array_weight {
            // Generate a random array
            let length = rng.random_range(1..=*max_array_length);
            write!(output_buffer, "[").unwrap();
            for i in 0..length {
                self.append_random_json(current_depth + 1);
                if i < length - 1 {
                    write!(&mut self.output_buffer, ",").unwrap();
                }
            }
            write!(&mut self.output_buffer, "]").unwrap();
            return;
        }

        panic!("Random value did not match any type");
    }
}

/// Data generator for VariantArrays that simulate structured log entries.
///
/// Each entry is an object with
/// 1. fields like "timestamp", "level", "message", that always appear
/// 2. Fields like "user_id", "session_id", that appear in some entries
/// 3. Arbitrary extra fields that should be preserved in the value field
///
///
/// Example entries:
/// ```json
/// {
///  "timestamp": "2024-10-01T12:00:00",   -- always present
///  "level": "INFO",                      -- always present
///  "message": "User logged in",          -- always present
///  "user_id": 12345,                     -- optional
///  "session_id": "abcde",                -- optional
///  "extra_field_123": "extra_value_456"  -- arbitrary extra field
/// }
/// ```
struct VariantLogGenerator {
    /// How many rows per batch
    rows_per_batch: usize,
    /// Probability of including optional fields (0 to 1)
    optional_field_prob: f64,
    /// Probability of including extra arbitrary fields (0 to 1)
    extra_field_prob: f64,
    /// Random number generator
    rng: StdRng,
}

impl Iterator for VariantLogGenerator {
    type Item = VariantArray;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.next_array())
    }
}

impl VariantLogGenerator {
    fn next_array(&mut self) -> VariantArray {
        let mut builder = VariantArrayBuilder::new(1000);
        for _ in 0..self.rows_per_batch {
            let mut obj_builder = builder.new_object();

            obj_builder = obj_builder
                .with_field("timestamp", self.random_timestamp())
                .with_field("level", self.random_level())
                .with_field("message", self.random_message());

            // Optional fields
            if self.rng.random::<f64>() < self.optional_field_prob {
                obj_builder = obj_builder.with_field("user_id", self.rng.random_range(1000..9999));
            }
            if self.rng.random::<f64>() < self.optional_field_prob {
                obj_builder = obj_builder.with_field("session_id", Uuid::new_v4());
            }

            // Random extra fields
            if self.rng.random::<f64>() < self.extra_field_prob {
                let num_extra_fields = self.rng.random_range(1..4);
                for _ in 0..num_extra_fields {
                    // totally random field name and value (modeling random logging payloads)
                    let field_name = format!("extra_field_{}", self.rng.random_range(1..100000000));
                    let field_value =
                        format!("extra_value_{}", self.rng.random_range(1..100000000));
                    obj_builder = obj_builder.with_field(field_name.as_str(), field_value.as_str());
                }
            }

            obj_builder.finish();
        }
        builder.build()
    }

    fn random_timestamp(&mut self) -> DateTime<Utc> {
        // random timestamp
        let hour = self.rng.random_range(0..24);
        let minute = self.rng.random_range(0..60);
        let second = self.rng.random_range(0..60);
        let day = self.rng.random_range(1..28);
        let month = self.rng.random_range(1..=12);
        let year = self.rng.random_range(2020..=2024);
        let naive = chrono::NaiveDate::from_ymd_opt(year, month, day)
            .unwrap()
            .and_hms_opt(hour, minute, second)
            .unwrap();
        DateTime::from_naive_utc_and_offset(naive, Utc)
    }

    /// Random level from ["DEBUG", "INFO", "WARN", "ERROR"]
    fn random_level(&mut self) -> &'static str {
        let levels = ["DEBUG", "INFO", "WARN", "ERROR"];
        levels[self.rng.random_range(0..levels.len())]
    }

    /// Generate a random log message
    fn random_message(&mut self) -> &str {
        let messages = [
            "User logged in",
            "User logged out",
            "File not found",
            "Connection established",
            "Error processing request",
            // a few longer messages
            "Database connection timed out after multiple attempts",
            "User attempted to access restricted resource without proper authorization",
            "Scheduled maintenance will occur at midnight UTC",
        ];

        messages[self.rng.random_range(0..messages.len())]
    }
}
