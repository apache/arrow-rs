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

use arrow_json::ReaderBuilder;
use arrow_schema::{DataType, Field, Schema};
use criterion::*;
use rand::{thread_rng, Rng};
use serde::Serialize;
use std::sync::Arc;

#[allow(deprecated)]
fn do_bench<R: Serialize>(c: &mut Criterion, name: &str, rows: &[R], schema: &Schema) {
    let schema = Arc::new(schema.clone());
    c.bench_function(name, |b| {
        b.iter(|| {
            let builder = ReaderBuilder::new(schema.clone()).with_batch_size(64);
            let mut decoder = builder.build_decoder().unwrap();
            decoder.serialize(rows)
        })
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut rng = thread_rng();
    let schema = Schema::new(vec![Field::new("i32", DataType::Int32, false)]);
    let v: Vec<i32> = (0..2048).map(|_| rng.gen_range(0..10000)).collect();

    do_bench(c, "small_i32", &v, &schema);
    let v: Vec<i32> = (0..2048).map(|_| rng.gen()).collect();
    do_bench(c, "large_i32", &v, &schema);

    let schema = Schema::new(vec![Field::new("i64", DataType::Int64, false)]);
    let v: Vec<i64> = (0..2048).map(|_| rng.gen_range(0..10000)).collect();
    do_bench(c, "small_i64", &v, &schema);
    let v: Vec<i64> = (0..2048).map(|_| rng.gen_range(0..i32::MAX as _)).collect();
    do_bench(c, "medium_i64", &v, &schema);
    let v: Vec<i64> = (0..2048).map(|_| rng.gen()).collect();
    do_bench(c, "large_i64", &v, &schema);

    let schema = Schema::new(vec![Field::new("f32", DataType::Float32, false)]);
    let v: Vec<f32> = (0..2048).map(|_| rng.gen_range(0.0..10000.)).collect();
    do_bench(c, "small_f32", &v, &schema);
    let v: Vec<f32> = (0..2048).map(|_| rng.gen_range(0.0..f32::MAX)).collect();
    do_bench(c, "large_f32", &v, &schema);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
