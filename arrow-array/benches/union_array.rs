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

use std::{iter::repeat, sync::Arc};

use arrow_array::{Array, ArrayRef, Int32Array, UnionArray};
use arrow_schema::{DataType, Field, UnionFields};
use criterion::*;

fn criterion_benchmark(c: &mut Criterion) {
    let child = Arc::new(Int32Array::new(
        (0..4096).collect(),
        Some(repeat([true, false]).flatten().take(4096).collect()),
    )) as ArrayRef;

    for i in 1..5 {
        c.bench_function(&format!("union logical nulls 4096 {i} children"), |b| {
            let fields = UnionFields::new(
                0..i,
                (0..i).map(|i| Field::new(format!("f{i}"), DataType::Int32, true)),
            );

            let array = UnionArray::try_new(
                fields,
                (0..i).cycle().take(4096).collect(),
                None,
                repeat(child.clone()).take(i as usize).collect(),
            )
            .unwrap();

            b.iter(|| black_box(array.logical_nulls()))
        });
    }

    c.bench_function("single with nulls 4096", |b| {
        let fields = UnionFields::new(
            [1, 3],
            [
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Int32, true),
            ],
        );

        let array = UnionArray::try_new(
            fields,
            repeat([1, 3]).flatten().take(4096).collect(),
            None,
            vec![child.clone(), Arc::new(Int32Array::from_value(-5, 4096))],
        )
        .unwrap();

        b.iter(|| black_box(array.logical_nulls()))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
