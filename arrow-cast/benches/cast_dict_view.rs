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

use std::sync::Arc;

use arrow_array::{types::UInt64Type, DictionaryArray, StringArray, UInt64Array};
use arrow_cast::cast;
use arrow_schema::DataType;
use criterion::*;

fn make_dict_array() -> DictionaryArray<UInt64Type> {
    let values = StringArray::from_iter([
        Some("small"),
        Some("larger string more than 12 bytes"),
        None,
    ]);
    let keys = UInt64Array::from_iter((0..10_000).map(|v| v % 3));

    DictionaryArray::new(keys, Arc::new(values))
}

fn criterion_benchmark(c: &mut Criterion) {
    let dict = make_dict_array();
    c.bench_function("dict to view", |b| {
        b.iter(|| {
            cast(&dict, &DataType::Utf8View).unwrap();
        })
    });

    let view_array = cast(&dict, &DataType::Utf8View).unwrap();

    c.bench_function("view to dict", |b| {
        b.iter(|| {
            cast(
                &view_array,
                &DataType::Dictionary(Box::new(DataType::UInt64), Box::new(DataType::Utf8)),
            )
            .unwrap();
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
