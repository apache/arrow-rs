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

extern crate arrow;
extern crate criterion;

use criterion::*;

use arrow::array::*;
use arrow::csv;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use std::env;
use std::fs::File;
use std::sync::Arc;

fn criterion_benchmark(c: &mut Criterion) {
    let schema = Schema::new(vec![
        Field::new("c1", DataType::Utf8, false),
        Field::new("c2", DataType::Float64, true),
        Field::new("c3", DataType::UInt32, false),
        Field::new("c4", DataType::Boolean, true),
    ]);

    let c1 = StringArray::from(vec![
        "Lorem ipsum dolor sit amet",
        "consectetur adipiscing elit",
        "sed do eiusmod tempor",
    ]);
    let c2 = PrimitiveArray::<Float64Type>::from(vec![
        Some(123.564532),
        None,
        Some(-556132.25),
    ]);
    let c3 = PrimitiveArray::<UInt32Type>::from(vec![3, 2, 1]);
    let c4 = BooleanArray::from(vec![Some(true), Some(false), None]);

    let b = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(c1), Arc::new(c2), Arc::new(c3), Arc::new(c4)],
    )
    .unwrap();
    let path = env::temp_dir().join("bench_write_csv.csv");
    let file = File::create(path).unwrap();
    let mut writer = csv::Writer::new(file);
    let batches = vec![&b, &b, &b, &b, &b, &b, &b, &b, &b, &b, &b];

    c.bench_function("record_batches_to_csv", |b| {
        b.iter(|| {
            #[allow(clippy::unit_arg)]
            criterion::black_box(for batch in &batches {
                writer.write(batch).unwrap()
            });
        });
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
