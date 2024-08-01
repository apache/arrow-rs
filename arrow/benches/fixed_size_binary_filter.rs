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

use arrow_array::{make_array, Array, ArrayRef, BooleanArray, FixedSizeBinaryArray};
use arrow_data::transform::MutableArrayData;
use arrow_schema::ArrowError;
use arrow_select::filter::{filter, FilterBuilder, IterationStrategy, SlicesIterator};
use criterion::*;

fn generate_value(value_length: u8, array_length: usize) -> (FixedSizeBinaryArray, BooleanArray) {
    let mut values = vec![];
    let value: Vec<u8> = (0..value_length).collect();
    for _ in 0..array_length {
        values.push(value.as_slice());
    }
    let a = FixedSizeBinaryArray::from(values);
    let filter_value = (0..array_length).map(|i| i % 2 == 0).collect::<Vec<bool>>();
    let b = BooleanArray::from(filter_value);
    (a, b)
}

fn filter_always_default(
    values: &dyn Array,
    predicate: &BooleanArray,
) -> Result<ArrayRef, ArrowError> {
    let predicate = FilterBuilder::new(predicate).build();
    if predicate.get_filter().len() > values.len() {
        return Err(ArrowError::InvalidArgumentError(format!(
            "Filter predicate of length {} is larger than target array of length {}",
            predicate.get_filter().len(),
            values.len()
        )));
    }
    let data = values.to_data();
    // fallback to using MutableArrayData
    let mut mutable = MutableArrayData::new(vec![&data], false, predicate.count());

    match &predicate.get_strategy() {
        IterationStrategy::Slices(slices) => {
            slices
                .iter()
                .for_each(|(start, end)| mutable.extend(0, *start, *end));
        }
        _ => {
            let iter = SlicesIterator::new(predicate.get_filter());
            iter.for_each(|(start, end)| mutable.extend(0, start, end));
        }
    }

    let data = mutable.freeze();
    Ok(make_array(data))
}

fn filter_by_default(a: &FixedSizeBinaryArray, b: &BooleanArray) -> Result<ArrayRef, ArrowError> {
    filter_always_default(a, b)
}

fn filter_special(a: &FixedSizeBinaryArray, b: &BooleanArray) -> Result<ArrayRef, ArrowError> {
    filter(a, b)
}

fn bench_fixed_size_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("fixed_size_binary_array_filter");
    for value_length in [5_u8, 10, 50, 100, 200].iter() {
        let v = generate_value(*value_length, 1024);
        group.bench_with_input(
            BenchmarkId::new("default", value_length),
            value_length,
            |b, _| {
                b.iter(|| black_box(filter_by_default(&v.0, &v.1)));
            },
        );

        group.bench_with_input(
            BenchmarkId::new("special", value_length),
            value_length,
            |b, _| {
                b.iter(|| black_box(filter_special(&v.0, &v.1)));
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_fixed_size_filter);
criterion_main!(benches);
