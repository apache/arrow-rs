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

use arrow_array::*;
use arrow_cast::cast;
use arrow_schema::{DataType, Field};
use criterion::*;
use std::sync::Arc;

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("string single run, 1_000_000 elements", |b| {
        let source_array = StringArray::from(vec!["a"; 1_000_000]);
        let array_ref = Arc::new(source_array) as ArrayRef;
        let target_type = DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            Arc::new(Field::new("values", DataType::Utf8, true)),
        );
        b.iter(|| cast(&array_ref, &target_type).unwrap());
    });

    c.bench_function("int32 many runs of 10, 1_000_000 elements", |b| {
        let source_array: Int32Array = (0..1_000_000).map(|i| i / 10).collect();
        let array_ref = Arc::new(source_array) as ArrayRef;
        let target_type = DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            Arc::new(Field::new("values", DataType::Int32, true)),
        );
        b.iter(|| cast(&array_ref, &target_type).unwrap());
    });

    c.bench_function("int32 many runs of 1000, 1_000_000 elements", |b| {
        let source_array: Int32Array = (0..1_000_000).map(|i| i / 1000).collect();
        let array_ref = Arc::new(source_array) as ArrayRef;
        let target_type = DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            Arc::new(Field::new("values", DataType::Int32, true)),
        );
        b.iter(|| cast(&array_ref, &target_type).unwrap());
    });

    c.bench_function("int32 no runs, 1_000_000 elements", |b| {
        let source_array: Int32Array = (0..1_000_000).collect();
        let array_ref = Arc::new(source_array) as ArrayRef;
        let target_type = DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            Arc::new(Field::new("values", DataType::Int32, true)),
        );
        b.iter(|| cast(&array_ref, &target_type).unwrap());
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
