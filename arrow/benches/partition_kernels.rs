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

#[macro_use]
extern crate criterion;
use arrow::compute::kernels::sort::{SortColumn, lexsort};
use arrow::util::bench_util::*;
use arrow::{
    array::*,
    datatypes::{Float64Type, UInt8Type},
};
use arrow_ord::partition::partition;
use criterion::Criterion;
use std::hint;
use std::sync::Arc;

fn bench_partition(sorted_columns: &[ArrayRef]) {
    hint::black_box(partition(sorted_columns).unwrap().ranges());
}

fn create_sorted_low_cardinality_data(length: usize) -> Vec<ArrayRef> {
    let arr = Int64Array::from_iter_values(
        std::iter::repeat_n(1, length / 4)
            .chain(std::iter::repeat_n(2, length / 4))
            .chain(std::iter::repeat_n(3, length / 4))
            .chain(std::iter::repeat_n(4, length / 4)),
    );
    lexsort(
        &[SortColumn {
            values: Arc::new(arr),
            options: None,
        }],
        None,
    )
    .unwrap()
}

fn create_sorted_float_data(pow: u32, with_nulls: bool) -> Vec<ArrayRef> {
    let size = 2u64.pow(pow) as usize;
    let null_density = if with_nulls { 0.5 } else { 0.0 };
    lexsort(
        &[
            SortColumn {
                values: Arc::new(create_primitive_array::<Float64Type>(size, null_density)),
                options: None,
            },
            SortColumn {
                values: Arc::new(create_primitive_array::<Float64Type>(size, null_density)),
                options: None,
            },
        ],
        None,
    )
    .unwrap()
}

fn create_sorted_data(pow: u32, with_nulls: bool) -> Vec<ArrayRef> {
    let size = 2u64.pow(pow) as usize;
    let null_density = if with_nulls { 0.5 } else { 0.0 };
    lexsort(
        &[
            SortColumn {
                values: Arc::new(create_primitive_array::<UInt8Type>(size, null_density)),
                options: None,
            },
            SortColumn {
                values: Arc::new(create_primitive_array::<UInt8Type>(size, null_density)),
                options: None,
            },
        ],
        None,
    )
    .unwrap()
}

fn add_benchmark(c: &mut Criterion) {
    let sorted_columns = create_sorted_data(10, false);
    c.bench_function("partition(u8) 2^10", |b| {
        b.iter(|| bench_partition(&sorted_columns))
    });

    let sorted_columns = create_sorted_data(12, false);
    c.bench_function("partition(u8) 2^12", |b| {
        b.iter(|| bench_partition(&sorted_columns))
    });

    let sorted_columns = create_sorted_data(10, true);
    c.bench_function("partition(u8) 2^10 with nulls", |b| {
        b.iter(|| bench_partition(&sorted_columns))
    });

    let sorted_columns = create_sorted_data(12, true);
    c.bench_function("partition(u8) 2^12 with nulls", |b| {
        b.iter(|| bench_partition(&sorted_columns))
    });

    let sorted_columns = create_sorted_float_data(10, false);
    c.bench_function("partition(f64) 2^10", |b| {
        b.iter(|| bench_partition(&sorted_columns))
    });

    let sorted_columns = create_sorted_low_cardinality_data(1024);
    c.bench_function("partition(low cardinality) 1024", |b| {
        b.iter(|| bench_partition(&sorted_columns))
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
