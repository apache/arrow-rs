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

use arrow::compute::{lexsort_to_indices, SortColumn};
use arrow::row::{RowConverter, SortField};
use arrow::util::bench_util::{
    create_dict_from_values, create_primitive_array, create_string_array_with_len,
};
use arrow_array::types::Int32Type;
use arrow_array::{Array, ArrayRef, UInt32Array};
use criterion::{criterion_group, criterion_main, Criterion};
use std::sync::Arc;

#[derive(Copy, Clone)]
enum Column {
    RequiredI32,
    OptionalI32,
    Required16CharString,
    Optional16CharString,
    Optional50CharString,
    Optional100Value50CharStringDict,
}

impl std::fmt::Debug for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Column::RequiredI32 => "i32",
            Column::OptionalI32 => "i32_opt",
            Column::Required16CharString => "str(16)",
            Column::Optional16CharString => "str_opt(16)",
            Column::Optional50CharString => "str_opt(50)",
            Column::Optional100Value50CharStringDict => "dict(100,str_opt(50))",
        };
        f.write_str(s)
    }
}

impl Column {
    fn generate(self, size: usize) -> ArrayRef {
        match self {
            Column::RequiredI32 => {
                Arc::new(create_primitive_array::<Int32Type>(size, 0.))
            }
            Column::OptionalI32 => {
                Arc::new(create_primitive_array::<Int32Type>(size, 0.2))
            }
            Column::Required16CharString => {
                Arc::new(create_string_array_with_len::<i32>(size, 0., 16))
            }
            Column::Optional16CharString => {
                Arc::new(create_string_array_with_len::<i32>(size, 0.2, 16))
            }
            Column::Optional50CharString => {
                Arc::new(create_string_array_with_len::<i32>(size, 0., 50))
            }
            Column::Optional100Value50CharStringDict => {
                Arc::new(create_dict_from_values::<Int32Type>(
                    size,
                    0.1,
                    &create_string_array_with_len::<i32>(100, 0., 50),
                ))
            }
        }
    }
}

fn do_bench(c: &mut Criterion, columns: &[Column], len: usize) {
    let arrays: Vec<_> = columns.iter().map(|x| x.generate(len)).collect();
    let sort_columns: Vec<_> = arrays
        .iter()
        .cloned()
        .map(|values| SortColumn {
            values,
            options: None,
        })
        .collect();

    c.bench_function(
        &format!("lexsort_to_indices({:?}): {}", columns, len),
        |b| {
            b.iter(|| {
                criterion::black_box(lexsort_to_indices(&sort_columns, None).unwrap())
            })
        },
    );

    c.bench_function(&format!("lexsort_rows({:?}): {}", columns, len), |b| {
        b.iter(|| {
            criterion::black_box({
                let fields = arrays
                    .iter()
                    .map(|a| SortField::new(a.data_type().clone()))
                    .collect();
                let mut converter = RowConverter::new(fields).unwrap();
                let rows = converter.convert_columns(&arrays).unwrap();
                let mut sort: Vec<_> = rows.iter().enumerate().collect();
                sort.sort_unstable_by(|(_, a), (_, b)| a.cmp(b));
                UInt32Array::from_iter_values(sort.iter().map(|(i, _)| *i as u32))
            })
        })
    });
}

fn add_benchmark(c: &mut Criterion) {
    let cases: &[&[Column]] = &[
        &[Column::RequiredI32, Column::OptionalI32],
        &[Column::RequiredI32, Column::Optional16CharString],
        &[Column::RequiredI32, Column::Required16CharString],
        &[Column::Optional16CharString, Column::Required16CharString],
        &[
            Column::Optional16CharString,
            Column::Optional50CharString,
            Column::Required16CharString,
        ],
        &[
            Column::Optional16CharString,
            Column::Required16CharString,
            Column::Optional16CharString,
            Column::Optional16CharString,
            Column::Optional16CharString,
        ],
        &[
            Column::OptionalI32,
            Column::Optional100Value50CharStringDict,
        ],
        &[
            Column::Optional100Value50CharStringDict,
            Column::Optional100Value50CharStringDict,
        ],
        &[
            Column::Optional100Value50CharStringDict,
            Column::Optional100Value50CharStringDict,
            Column::Optional100Value50CharStringDict,
            Column::Required16CharString,
        ],
        &[
            Column::Optional100Value50CharStringDict,
            Column::Optional100Value50CharStringDict,
            Column::Optional100Value50CharStringDict,
            Column::Optional50CharString,
        ],
        &[
            Column::Optional100Value50CharStringDict,
            Column::Optional100Value50CharStringDict,
            Column::Optional100Value50CharStringDict,
            Column::Optional50CharString,
        ],
    ];

    for case in cases {
        do_bench(c, case, 4096);
        do_bench(c, case, 4096 * 8);
    }
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
