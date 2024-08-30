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

use std::{
    iter::{repeat, repeat_with},
    sync::Arc,
};

use arrow_array::{Array, ArrayRef, Int32Array, UnionArray};
use arrow_buffer::{NullBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, UnionFields};
use criterion::*;
use rand::{thread_rng, Rng};

fn array_with_nulls() -> ArrayRef {
    let mut rng = thread_rng();

    let values = ScalarBuffer::from_iter(repeat_with(|| rng.gen()).take(4096));

    // nulls with at least one null and one valid
    let nulls: NullBuffer = [true, false]
        .into_iter()
        .chain(repeat_with(|| rng.gen()))
        .take(4096)
        .collect();

    Arc::new(Int32Array::new(values.clone(), Some(nulls)))
}

fn array_without_nulls() -> ArrayRef {
    let mut rng = thread_rng();

    let values = ScalarBuffer::from_iter(repeat_with(|| rng.gen()).take(4096));

    Arc::new(Int32Array::new(values.clone(), None))
}

fn criterion_benchmark(c: &mut Criterion) {
    for with_nulls in 1..12 {
        for without_nulls in [0, 1, 10] {
            c.bench_function(
                &format!("union logical nulls 4096 {with_nulls} children with nulls, {without_nulls} without nulls"),
                |b| {
                    let type_ids = 0..with_nulls+without_nulls;

                    let fields = UnionFields::new(
                        type_ids.clone(),
                        type_ids.clone().map(|i| Field::new(format!("f{i}"), DataType::Int32, true)),
                    );

                    let array = UnionArray::try_new(
                        fields,
                        type_ids.cycle().take(4096).collect(),
                        None,
                        repeat(array_with_nulls())
                            .take(with_nulls as usize)
                            .chain(repeat(array_without_nulls()).take(without_nulls as usize))
                            .collect(),
                    )
                    .unwrap();

                    b.iter(|| black_box(array.logical_nulls()))
                },
            );
        }
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
