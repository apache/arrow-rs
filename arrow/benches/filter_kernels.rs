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

use std::sync::Arc;

use arrow::compute::{filter_record_batch, FilterBuilder, FilterPredicate};
use arrow::util::bench_util::*;

use arrow::array::*;
use arrow::compute::filter;
use arrow::datatypes::{Field, Float32Type, Int32Type, Int64Type, Schema, UInt8Type};

use arrow_array::types::Decimal128Type;
use criterion::{criterion_group, criterion_main, Criterion};

fn bench_filter(data_array: &dyn Array, filter_array: &BooleanArray) {
    criterion::black_box(filter(data_array, filter_array).unwrap());
}

fn bench_built_filter(filter: &FilterPredicate, array: &dyn Array) {
    criterion::black_box(filter.filter(array).unwrap());
}

fn add_benchmark(c: &mut Criterion) {
    let size = 65536;
    let filter_array = create_boolean_array(size, 0.0, 0.5);
    let dense_filter_array = create_boolean_array(size, 0.0, 1.0 - 1.0 / 1024.0);
    let sparse_filter_array = create_boolean_array(size, 0.0, 1.0 / 1024.0);

    let filter = FilterBuilder::new(&filter_array).optimize().build();
    let dense_filter = FilterBuilder::new(&dense_filter_array).optimize().build();
    let sparse_filter = FilterBuilder::new(&sparse_filter_array).optimize().build();

    let data_array = create_primitive_array::<UInt8Type>(size, 0.0);

    c.bench_function("filter optimize (kept 1/2)", |b| {
        b.iter(|| FilterBuilder::new(&filter_array).optimize().build())
    });

    c.bench_function("filter optimize high selectivity (kept 1023/1024)", |b| {
        b.iter(|| FilterBuilder::new(&dense_filter_array).optimize().build())
    });

    c.bench_function("filter optimize low selectivity (kept 1/1024)", |b| {
        b.iter(|| FilterBuilder::new(&sparse_filter_array).optimize().build())
    });

    c.bench_function("filter u8 (kept 1/2)", |b| {
        b.iter(|| bench_filter(&data_array, &filter_array))
    });
    c.bench_function("filter u8 high selectivity (kept 1023/1024)", |b| {
        b.iter(|| bench_filter(&data_array, &dense_filter_array))
    });
    c.bench_function("filter u8 low selectivity (kept 1/1024)", |b| {
        b.iter(|| bench_filter(&data_array, &sparse_filter_array))
    });

    c.bench_function("filter context u8 (kept 1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function("filter context u8 high selectivity (kept 1023/1024)", |b| {
        b.iter(|| bench_built_filter(&dense_filter, &data_array))
    });
    c.bench_function("filter context u8 low selectivity (kept 1/1024)", |b| {
        b.iter(|| bench_built_filter(&sparse_filter, &data_array))
    });

    let data_array = create_primitive_array::<Int32Type>(size, 0.0);
    c.bench_function("filter i32 (kept 1/2)", |b| {
        b.iter(|| bench_filter(&data_array, &filter_array))
    });
    c.bench_function("filter i32 high selectivity (kept 1023/1024)", |b| {
        b.iter(|| bench_filter(&data_array, &dense_filter_array))
    });
    c.bench_function("filter i32 low selectivity (kept 1/1024)", |b| {
        b.iter(|| bench_filter(&data_array, &sparse_filter_array))
    });

    c.bench_function("filter context i32 (kept 1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function(
        "filter context i32 high selectivity (kept 1023/1024)",
        |b| b.iter(|| bench_built_filter(&dense_filter, &data_array)),
    );
    c.bench_function("filter context i32 low selectivity (kept 1/1024)", |b| {
        b.iter(|| bench_built_filter(&sparse_filter, &data_array))
    });

    let data_array = create_primitive_array::<Int32Type>(size, 0.5);
    c.bench_function("filter context i32 w NULLs (kept 1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function(
        "filter context i32 w NULLs high selectivity (kept 1023/1024)",
        |b| b.iter(|| bench_built_filter(&dense_filter, &data_array)),
    );
    c.bench_function(
        "filter context i32 w NULLs low selectivity (kept 1/1024)",
        |b| b.iter(|| bench_built_filter(&sparse_filter, &data_array)),
    );

    let data_array = create_primitive_array::<UInt8Type>(size, 0.5);
    c.bench_function("filter context u8 w NULLs (kept 1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function(
        "filter context u8 w NULLs high selectivity (kept 1023/1024)",
        |b| b.iter(|| bench_built_filter(&dense_filter, &data_array)),
    );
    c.bench_function(
        "filter context u8 w NULLs low selectivity (kept 1/1024)",
        |b| b.iter(|| bench_built_filter(&sparse_filter, &data_array)),
    );

    let data_array = create_primitive_array::<Float32Type>(size, 0.5);
    c.bench_function("filter f32 (kept 1/2)", |b| {
        b.iter(|| bench_filter(&data_array, &filter_array))
    });
    c.bench_function("filter context f32 (kept 1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function(
        "filter context f32 high selectivity (kept 1023/1024)",
        |b| b.iter(|| bench_built_filter(&dense_filter, &data_array)),
    );
    c.bench_function("filter context f32 low selectivity (kept 1/1024)", |b| {
        b.iter(|| bench_built_filter(&sparse_filter, &data_array))
    });

    let data_array = create_primitive_array::<Decimal128Type>(size, 0.0);
    c.bench_function("filter decimal128 (kept 1/2)", |b| {
        b.iter(|| bench_filter(&data_array, &filter_array))
    });
    c.bench_function("filter decimal128 high selectivity (kept 1023/1024)", |b| {
        b.iter(|| bench_filter(&data_array, &dense_filter_array))
    });
    c.bench_function("filter decimal128 low selectivity (kept 1/1024)", |b| {
        b.iter(|| bench_filter(&data_array, &sparse_filter_array))
    });

    c.bench_function("filter context decimal128 (kept 1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function(
        "filter context decimal128 high selectivity (kept 1023/1024)",
        |b| b.iter(|| bench_built_filter(&dense_filter, &data_array)),
    );
    c.bench_function(
        "filter context decimal128 low selectivity (kept 1/1024)",
        |b| b.iter(|| bench_built_filter(&sparse_filter, &data_array)),
    );

    let data_array = create_string_array::<i32>(size, 0.5);
    c.bench_function("filter context string (kept 1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function(
        "filter context string high selectivity (kept 1023/1024)",
        |b| b.iter(|| bench_built_filter(&dense_filter, &data_array)),
    );
    c.bench_function("filter context string low selectivity (kept 1/1024)", |b| {
        b.iter(|| bench_built_filter(&sparse_filter, &data_array))
    });

    let data_array = create_string_dict_array::<Int32Type>(size, 0.0, 4);
    c.bench_function("filter context string dictionary (kept 1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function(
        "filter context string dictionary high selectivity (kept 1023/1024)",
        |b| b.iter(|| bench_built_filter(&dense_filter, &data_array)),
    );
    c.bench_function(
        "filter context string dictionary low selectivity (kept 1/1024)",
        |b| b.iter(|| bench_built_filter(&sparse_filter, &data_array)),
    );

    let data_array = create_string_dict_array::<Int32Type>(size, 0.5, 4);
    c.bench_function("filter context string dictionary w NULLs (kept 1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function(
        "filter context string dictionary w NULLs high selectivity (kept 1023/1024)",
        |b| b.iter(|| bench_built_filter(&dense_filter, &data_array)),
    );
    c.bench_function(
        "filter context string dictionary w NULLs low selectivity (kept 1/1024)",
        |b| b.iter(|| bench_built_filter(&sparse_filter, &data_array)),
    );

    let mut add_benchmark_for_fsb_with_length = |value_length: usize| {
        let data_array = create_fsb_array(size, 0.0, value_length);
        c.bench_function(
            format!("filter fsb with value length {value_length} (kept 1/2)").as_str(),
            |b| b.iter(|| bench_filter(&data_array, &filter_array)),
        );
        c.bench_function(
            format!(
                "filter fsb with value length {value_length} high selectivity (kept 1023/1024)"
            )
            .as_str(),
            |b| b.iter(|| bench_filter(&data_array, &dense_filter_array)),
        );
        c.bench_function(
            format!("filter fsb with value length {value_length} low selectivity (kept 1/1024)")
                .as_str(),
            |b| b.iter(|| bench_filter(&data_array, &sparse_filter_array)),
        );

        c.bench_function(
            format!("filter context fsb with value length {value_length} (kept 1/2)").as_str(),
            |b| b.iter(|| bench_built_filter(&filter, &filter_array)),
        );
        c.bench_function(
            format!(
                "filter context fsb with value length {value_length} high selectivity (kept 1023/1024)"
            )
            .as_str(),
            |b| b.iter(|| bench_built_filter(&filter, &dense_filter_array)),
        );
        c.bench_function(
            format!(
                "filter context fsb with value length {value_length} low selectivity (kept 1/1024)"
            )
            .as_str(),
            |b| b.iter(|| bench_built_filter(&filter, &sparse_filter_array)),
        );
    };

    add_benchmark_for_fsb_with_length(5);
    add_benchmark_for_fsb_with_length(20);
    add_benchmark_for_fsb_with_length(50);

    let data_array = create_primitive_array::<Float32Type>(size, 0.0);

    let field = Field::new("c1", data_array.data_type().clone(), true);
    let schema = Schema::new(vec![field]);

    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(data_array)]).unwrap();

    c.bench_function("filter single record batch", |b| {
        b.iter(|| filter_record_batch(&batch, &filter_array))
    });

    let data_array = create_string_view_array_with_len(size, 0.5, 4, false);
    c.bench_function("filter context short string view (kept 1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function(
        "filter context short string view high selectivity (kept 1023/1024)",
        |b| b.iter(|| bench_built_filter(&dense_filter, &data_array)),
    );
    c.bench_function(
        "filter context short string view low selectivity (kept 1/1024)",
        |b| b.iter(|| bench_built_filter(&sparse_filter, &data_array)),
    );

    let data_array = create_string_view_array_with_len(size, 0.5, 4, true);
    c.bench_function("filter context mixed string view (kept 1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function(
        "filter context mixed string view high selectivity (kept 1023/1024)",
        |b| b.iter(|| bench_built_filter(&dense_filter, &data_array)),
    );
    c.bench_function(
        "filter context mixed string view low selectivity (kept 1/1024)",
        |b| b.iter(|| bench_built_filter(&sparse_filter, &data_array)),
    );

    let data_array = create_primitive_run_array::<Int32Type, Int64Type>(size, size);
    c.bench_function("filter run array (kept 1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function("filter run array high selectivity (kept 1023/1024)", |b| {
        b.iter(|| bench_built_filter(&dense_filter, &data_array))
    });
    c.bench_function("filter run array low selectivity (kept 1/1024)", |b| {
        b.iter(|| bench_built_filter(&sparse_filter, &data_array))
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
