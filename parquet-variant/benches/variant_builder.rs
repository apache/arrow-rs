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

extern crate parquet_variant;

use criterion::*;

use parquet_variant::VariantBuilder;
use rand::{
    distr::{uniform::SampleUniform, Alphanumeric},
    rngs::StdRng,
    Rng, SeedableRng,
};
use std::{hint, ops::Range};

fn random<T: SampleUniform + PartialEq + PartialOrd>(rng: &mut StdRng, range: Range<T>) -> T {
    rng.random_range::<T, _>(range)
}

// generates a string with a 50/50 chance whether it's a short or a long string
fn random_string(rng: &mut StdRng) -> String {
    let len = rng.random_range::<usize, _>(1..128);

    rng.sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

struct RandomStringGenerator {
    cursor: usize,
    table: Vec<String>,
}

impl RandomStringGenerator {
    pub fn new(rng: &mut StdRng, capacity: usize) -> Self {
        let table = (0..capacity)
            .map(|_| random_string(rng))
            .collect::<Vec<_>>();

        Self { cursor: 0, table }
    }

    pub fn next(&mut self) -> &str {
        let this = &self.table[self.cursor];

        self.cursor = (self.cursor + 1) % self.table.len();

        this
    }
}

// Creates an object with field names inserted in reverse lexicographical order
fn bench_object_field_names_reverse_order(c: &mut Criterion) {
    c.bench_function("bench_object_field_names_reverse_order", |b| {
        let mut rng = StdRng::seed_from_u64(42);
        let mut string_table = RandomStringGenerator::new(&mut rng, 117);
        b.iter(|| {
            let mut variant = VariantBuilder::new();
            let mut object_builder = variant.new_object();

            for i in 0..50_000 {
                object_builder.insert(format!("{}", 1000 - i).as_str(), string_table.next());
            }

            object_builder.finish().unwrap();
            hint::black_box(variant.finish());
        })
    });
}

// Creates objects with a homogenous schema (same field names)
/*
    {
        name: String,
        age: i32,
        likes_cilantro: bool,
        comments: Long string
        dishes: Vec<String>
    }
*/
fn bench_object_same_schema(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(42);
    let mut string_table = RandomStringGenerator::new(&mut rng, 117);

    c.bench_function("bench_object_same_schema", |b| {
        b.iter(|| {
            for _ in 0..25_000 {
                let mut variant = VariantBuilder::new();
                let mut object_builder = variant.new_object();
                object_builder.insert("name", string_table.next());
                object_builder.insert("age", random::<u32>(&mut rng, 18..100) as i32);
                object_builder.insert("likes_cilantro", rng.random_bool(0.5));
                object_builder.insert("comments", string_table.next());

                let mut inner_list_builder = object_builder.new_list("dishes");
                inner_list_builder.append_value(string_table.next());
                inner_list_builder.append_value(string_table.next());
                inner_list_builder.append_value(string_table.next());

                inner_list_builder.finish();
                object_builder.finish().unwrap();

                hint::black_box(variant.finish());
            }
        })
    });
}

// Creates a list of objects with the same schema (same field names)
/*
    {
        name: String,
        age: i32,
        likes_cilantro: bool,
        comments: Long string
        dishes: Vec<String>
    }
*/
fn bench_object_list_same_schema(c: &mut Criterion) {
    c.bench_function("bench_object_list_same_schema", |b| {
        let mut rng = StdRng::seed_from_u64(42);
        let mut string_table = RandomStringGenerator::new(&mut rng, 101);

        b.iter(|| {
            let mut variant = VariantBuilder::new();

            let mut list_builder = variant.new_list();

            for _ in 0..25_000 {
                let mut object_builder = list_builder.new_object();
                object_builder.insert("name", string_table.next());
                object_builder.insert("age", random::<u32>(&mut rng, 18..100) as i32);
                object_builder.insert("likes_cilantro", rng.random_bool(0.5));
                object_builder.insert("comments", string_table.next());

                let mut list_builder = object_builder.new_list("dishes");
                list_builder.append_value(string_table.next());
                list_builder.append_value(string_table.next());
                list_builder.append_value(string_table.next());

                list_builder.finish();
                object_builder.finish().unwrap();
            }

            list_builder.finish();
            hint::black_box(variant.finish());
        })
    });
}

// Creates variant objects with an undefined schema (random field names)
// values are randomly generated, with an equal distribution to whether it's a String, Object, or List
fn bench_object_unknown_schema(c: &mut Criterion) {
    c.bench_function("bench_object_unknown_schema", |b| {
        let mut rng = StdRng::seed_from_u64(42);
        let mut string_table = RandomStringGenerator::new(&mut rng, 1001);

        b.iter(|| {
            for _ in 0..200 {
                let mut variant = VariantBuilder::new();
                let mut object_builder = variant.new_object();

                for _num_fields in 0..random::<u8>(&mut rng, 0..100) {
                    if rng.random_bool(0.33) {
                        let key = string_table.next();
                        object_builder.insert(key, key);
                        continue;
                    }

                    if rng.random_bool(0.5) {
                        let mut inner_object_builder = object_builder.new_object("rand_object");

                        for _num_fields in 0..random::<u8>(&mut rng, 0..25) {
                            let key = string_table.next();
                            inner_object_builder.insert(key, key);
                        }
                        inner_object_builder.finish().unwrap();

                        continue;
                    }

                    let mut inner_list_builder = object_builder.new_list("rand_list");

                    for _num_elements in 0..random::<u8>(&mut rng, 0..25) {
                        inner_list_builder.append_value(string_table.next());
                    }

                    inner_list_builder.finish();
                }
                object_builder.finish().unwrap();
                hint::black_box(variant.finish());
            }
        })
    });
}

// Creates a list of variant objects with an undefined schema (random field names)
// values are randomly generated, with an equal distribution to whether it's a String, Object, or List
fn bench_object_list_unknown_schema(c: &mut Criterion) {
    c.bench_function("bench_object_list_unknown_schema", |b| {
        let mut rng = StdRng::seed_from_u64(42);
        let mut string_table = RandomStringGenerator::new(&mut rng, 1001);

        b.iter(|| {
            let mut rng = StdRng::seed_from_u64(42);

            let mut variant = VariantBuilder::new();

            let mut list_builder = variant.new_list();

            for _ in 0..200 {
                let mut object_builder = list_builder.new_object();

                for _num_fields in 0..random::<u8>(&mut rng, 0..100) {
                    let key = string_table.next();

                    if rng.random_bool(0.33) {
                        object_builder.insert(key, key);
                        continue;
                    }

                    if rng.random_bool(0.5) {
                        let mut inner_object_builder = object_builder.new_object("rand_object");

                        for _num_fields in 0..random::<u8>(&mut rng, 0..25) {
                            let key = string_table.next();
                            inner_object_builder.insert(key, key);
                        }
                        inner_object_builder.finish().unwrap();

                        continue;
                    }

                    let mut inner_list_builder = object_builder.new_list("rand_list");

                    for _num_elements in 0..random::<u8>(&mut rng, 0..25) {
                        inner_list_builder.append_value(key);
                    }

                    inner_list_builder.finish();
                }
                object_builder.finish().unwrap();
            }

            list_builder.finish();
            hint::black_box(variant.finish());
        })
    });
}

// Creates objects with a homogenous schema (same field names)
/*
    {
        "id": &[u8],        // Following are common across all objects
        "span_id: &[u8],
        "created": u32,
        "ended": u32,
        "span_name": String,

        "attributees": {
            // following fields are randomized
        }
    }
*/
fn bench_object_partially_same_schema(c: &mut Criterion) {
    c.bench_function("bench_object_partially_same_schema", |b| {
        let mut rng = StdRng::seed_from_u64(42);
        let mut string_table = RandomStringGenerator::new(&mut rng, 117);

        b.iter(|| {
            let mut rng = StdRng::seed_from_u64(42);

            for _ in 0..200 {
                let mut variant = VariantBuilder::new();
                let mut object_builder = variant.new_object();

                object_builder.insert(
                    "id",
                    random::<i128>(&mut rng, 0..i128::MAX)
                        .to_le_bytes()
                        .as_slice(),
                );

                object_builder.insert(
                    "span_id",
                    random::<i128>(&mut rng, 0..i128::MAX)
                        .to_le_bytes()
                        .as_slice(),
                );

                object_builder.insert("created", random::<u32>(&mut rng, 0..u32::MAX) as i32);
                object_builder.insert("ended", random::<u32>(&mut rng, 0..u32::MAX) as i32);
                object_builder.insert("span_name", string_table.next());

                {
                    let mut inner_object_builder = object_builder.new_object("attributes");

                    for _num_fields in 0..random::<u8>(&mut rng, 0..100) {
                        let key = string_table.next();
                        inner_object_builder.insert(key, key);
                    }
                    inner_object_builder.finish().unwrap();
                }

                object_builder.finish().unwrap();
                hint::black_box(variant.finish());
            }
        })
    });
}

// Creates a list of variant objects with a partially homogenous schema (similar field names)
/*
    {
        "id": &[u8],        // Following are common across all objects
        "span_id: &[u8],
        "created": u32,
        "ended": u32,
        "span_name": String,

        "attributees": {
            // following fields are randomized
        }
    }
*/
fn bench_object_list_partially_same_schema(c: &mut Criterion) {
    c.bench_function("bench_object_list_partially_same_schema", |b| {
        let mut rng = StdRng::seed_from_u64(42);
        let mut string_table = RandomStringGenerator::new(&mut rng, 117);

        b.iter(|| {
            let mut variant = VariantBuilder::new();

            let mut list_builder = variant.new_list();

            for _ in 0..100 {
                let mut object_builder = list_builder.new_object();

                object_builder.insert(
                    "id",
                    random::<i128>(&mut rng, 0..i128::MAX)
                        .to_le_bytes()
                        .as_slice(),
                );

                object_builder.insert(
                    "span_id",
                    random::<i128>(&mut rng, 0..i128::MAX)
                        .to_le_bytes()
                        .as_slice(),
                );

                object_builder.insert("created", random::<u32>(&mut rng, 0..u32::MAX) as i32);
                object_builder.insert("ended", random::<u32>(&mut rng, 0..u32::MAX) as i32);
                object_builder.insert("span_name", string_table.next());

                {
                    let mut inner_object_builder = object_builder.new_object("attributes");

                    for _num_fields in 0..random::<u8>(&mut rng, 0..100) {
                        let key = string_table.next();
                        inner_object_builder.insert(key, key);
                    }
                    inner_object_builder.finish().unwrap();
                }

                object_builder.finish().unwrap();
            }

            list_builder.finish();
            hint::black_box(variant.finish());
        })
    });
}

criterion_group!(
    benches,
    bench_object_field_names_reverse_order,
    bench_object_same_schema,
    bench_object_list_same_schema,
    bench_object_unknown_schema,
    bench_object_list_unknown_schema,
    bench_object_partially_same_schema,
    bench_object_list_partially_same_schema
);

criterion_main!(benches);
