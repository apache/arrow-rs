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
use criterion::Criterion;
use std::sync::Arc;
extern crate arrow;
use arrow::compute::kernels::partition::lexicographical_partition_ranges;
use arrow::compute::kernels::sort::{lexsort, SortColumn};
use arrow::compute::{sort, SortOptions};
use arrow::datatypes::{ArrowDictionaryKeyType, ArrowNativeType, DataType, Int32Type};
use arrow::util::bench_util::*;
use arrow::util::test_util::seedable_rng;
use arrow::{
    array::*,
    datatypes::{ArrowPrimitiveType, Float64Type, UInt8Type},
};
use rand::distributions::{Distribution, Standard};
use rand::Rng;
use std::iter;

fn create_array<T: ArrowPrimitiveType>(size: usize, with_nulls: bool) -> ArrayRef
where
    Standard: Distribution<T::Native>,
{
    let null_density = if with_nulls { 0.5 } else { 0.0 };
    let array = create_primitive_array::<T>(size, null_density);
    Arc::new(array)
}

fn create_sorted_dictionary_array<T: ArrowDictionaryKeyType>(
    size: usize,
    num_distinct_keys: usize,
) -> ArrayRef
where
    Standard: Distribution<T::Native>,
{
    let mut rng = seedable_rng();

    let key_array: ArrayRef =
        Arc::new(PrimitiveArray::<T>::from_iter_values((0..size).map(|_| {
            T::Native::from_usize(rng.gen_range(0..num_distinct_keys)).unwrap()
        })));
    let sorted_key_array = sort(&key_array, None).unwrap();
    let values = StringArray::from_iter_values(
        (0..num_distinct_keys).map(|i| format!("{:08}", i)),
    );

    let data = ArrayData::try_new(
        DataType::Dictionary(Box::new(T::DATA_TYPE), Box::new(DataType::Utf8)),
        size,
        None,
        sorted_key_array.data().null_buffer().cloned(),
        0,
        key_array.data().buffers().to_vec(),
        vec![values.data().clone()],
    )
    .unwrap();

    Arc::new(DictionaryArray::<T>::from(data))
}

fn bench_partition(sorted_columns: &[ArrayRef]) {
    let columns = sorted_columns
        .iter()
        .map(|arr| SortColumn {
            values: arr.clone(),
            options: None,
        })
        .collect::<Vec<_>>();

    bench_partition_with_options(&columns);
}

fn bench_partition_with_options(columns: &[SortColumn]) {
    criterion::black_box(
        lexicographical_partition_ranges(&columns)
            .unwrap()
            .collect::<Vec<_>>(),
    );
}

fn create_sorted_low_cardinality_data(length: usize) -> Vec<ArrayRef> {
    let arr = Int64Array::from_iter_values(
        iter::repeat(1)
            .take(length / 4)
            .chain(iter::repeat(2).take(length / 4))
            .chain(iter::repeat(3).take(length / 4))
            .chain(iter::repeat(4).take(length / 4)),
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
    lexsort(
        &[
            SortColumn {
                values: create_array::<Float64Type>(2u64.pow(pow) as usize, with_nulls),
                options: None,
            },
            SortColumn {
                values: create_array::<Float64Type>(2u64.pow(pow) as usize, with_nulls),
                options: None,
            },
        ],
        None,
    )
    .unwrap()
}

fn create_sorted_data(pow: u32, with_nulls: bool) -> Vec<ArrayRef> {
    lexsort(
        &[
            SortColumn {
                values: create_array::<UInt8Type>(2u64.pow(pow) as usize, with_nulls),
                options: None,
            },
            SortColumn {
                values: create_array::<UInt8Type>(2u64.pow(pow) as usize, with_nulls),
                options: None,
            },
        ],
        None,
    )
    .unwrap()
}

fn add_benchmark(c: &mut Criterion) {
    let sorted_columns = create_sorted_data(10, false);
    c.bench_function("lexicographical_partition_ranges(u8) 2^10", |b| {
        b.iter(|| bench_partition(&sorted_columns))
    });

    let sorted_columns = create_sorted_data(12, false);
    c.bench_function("lexicographical_partition_ranges(u8) 2^12", |b| {
        b.iter(|| bench_partition(&sorted_columns))
    });

    let sorted_columns = create_sorted_data(10, true);
    c.bench_function(
        "lexicographical_partition_ranges(u8) 2^10 with nulls",
        |b| b.iter(|| bench_partition(&sorted_columns)),
    );

    let sorted_columns = create_sorted_data(12, true);
    c.bench_function(
        "lexicographical_partition_ranges(u8) 2^12 with nulls",
        |b| b.iter(|| bench_partition(&sorted_columns)),
    );

    let sorted_columns = create_sorted_float_data(10, false);
    c.bench_function("lexicographical_partition_ranges(f64) 2^10", |b| {
        b.iter(|| bench_partition(&sorted_columns))
    });

    let sorted_columns = create_sorted_low_cardinality_data(1024);
    c.bench_function(
        "lexicographical_partition_ranges(low cardinality) 1024",
        |b| b.iter(|| bench_partition(&sorted_columns)),
    );

    let sorted_columns = create_sorted_dictionary_array::<Int32Type>(4096, 100);
    let sorted_columns = &[SortColumn {
        values: sorted_columns,
        options: Some(SortOptions {
            assume_sorted_dictionaries: false,
            ..Default::default()
        }),
    }];
    c.bench_function(
        "lexicographical_partition_ranges(dictionary_values) 4096",
        |b| b.iter(|| bench_partition_with_options(sorted_columns)),
    );

    let sorted_columns = create_sorted_dictionary_array::<Int32Type>(4096, 100);
    let sorted_columns = &[SortColumn {
        values: sorted_columns,
        options: Some(SortOptions {
            assume_sorted_dictionaries: true,
            ..Default::default()
        }),
    }];
    c.bench_function(
        "lexicographical_partition_ranges(dictionary_keys) 4096",
        |b| b.iter(|| bench_partition_with_options(sorted_columns)),
    );
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
