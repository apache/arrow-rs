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

use arrow_array::{ArrayRef, DictionaryArray, Int32Array, StringArray};
use arrow_row::{RowConverter, SortField};
use arrow_schema::DataType;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

const CARDINALITIES: &[usize] = &[10, 100, 250, 500, 4_000];
const NUM_ROWS: usize = 8_192;
const CODEC_CALLS: usize = 8;

fn make_dict_array(num_rows: usize, ndv: usize) -> DictionaryArray<arrow_array::types::Int32Type> {
    let mut rng = StdRng::seed_from_u64(42);
    let values: StringArray = (0..ndv).map(|i| Some(format!("k1_{i:08}"))).collect();
    let keys: Int32Array = (0..num_rows)
        .map(|_| Some(rng.random_range(0..ndv as i32)))
        .collect();
    DictionaryArray::try_new(keys, Arc::new(values)).unwrap()
}

fn dict_sort_field() -> SortField {
    SortField::new(DataType::Dictionary(
        Box::new(DataType::Int32),
        Box::new(DataType::Utf8),
    ))
}

fn bench_dict_fresh_values(c: &mut Criterion) {
    let mut group = c.benchmark_group("row_encode/dict_fresh_values");

    for &ndv in CARDINALITIES {
        let dict = make_dict_array(NUM_ROWS, ndv);
        let converter = RowConverter::new(vec![dict_sort_field()]).unwrap();

        group.bench_with_input(BenchmarkId::from_parameter(ndv), &ndv, |b, _| {
            b.iter(|| {
                for _ in 0..CODEC_CALLS {
                    let fresh: ArrayRef = Arc::new(dict.clone());
                    converter.convert_columns(&[fresh]).unwrap();
                }
            })
        });
    }
    group.finish();
}

fn bench_dict_shared_values(c: &mut Criterion) {
    let mut group = c.benchmark_group("row_encode/dict_shared_values");

    for &ndv in CARDINALITIES {
        let dict_ref: ArrayRef = Arc::new(make_dict_array(NUM_ROWS, ndv));
        let converter = RowConverter::new(vec![dict_sort_field()]).unwrap();

        group.bench_with_input(BenchmarkId::from_parameter(ndv), &ndv, |b, _| {
            b.iter(|| {
                for _ in 0..CODEC_CALLS {
                    converter.convert_columns(&[Arc::clone(&dict_ref)]).unwrap();
                }
            })
        });
    }
    group.finish();
}

criterion_group!(benches, bench_dict_fresh_values, bench_dict_shared_values);
criterion_main!(benches);
