// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow::compute::{SortColumn, lexsort_to_indices};
use arrow::row::radix::radix_sort_to_indices;
use arrow::row::{RowConverter, SortField};
use arrow::util::bench_util::{
    create_decimal128_array, create_dict_from_values, create_primitive_array,
    create_primitive_array_range, create_string_array_with_len,
};
use arrow::util::data_gen::create_random_array;
use arrow_array::types::{Float64Type, Int32Type, Int64Type};
use arrow_array::{Array, ArrayRef, Int32Array, Int64Array, UInt32Array};
use arrow_schema::{DataType, Field};
use criterion::{Criterion, criterion_group, criterion_main};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::{hint, sync::Arc};

#[derive(Copy, Clone)]
enum Column {
    RequiredI32,
    OptionalI32,
    RequiredI64,
    OptionalI64,
    RequiredF64,
    OptionalF64,
    SmallRangeI64,
    Decimal128,
    Required16CharString,
    Optional16CharString,
    Optional50CharString,
    Optional100Value50CharStringDict,
    RequiredI32List,
    OptionalI32List,
    Required4CharStringList,
    Optional4CharStringList,
    // Values in 0..10, from the given seed so that columns are independent
    TenValueI32(u64),
    // The shape of a BIGINT join key such as (ss_item_sk, ss_ticket_number)
    // with a BIGINT ticket: item numbers, then tickets in 0..len / 10
    JoinItemI32,
    JoinTicketI64,
}

impl std::fmt::Debug for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Column::RequiredI32 => "i32",
            Column::OptionalI32 => "i32_opt",
            Column::RequiredI64 => "i64",
            Column::OptionalI64 => "i64_opt",
            Column::RequiredF64 => "f64",
            Column::OptionalF64 => "f64_opt",
            Column::SmallRangeI64 => "i64(0..1000)",
            Column::Decimal128 => "decimal128(18,2)",
            Column::Required16CharString => "str(16)",
            Column::Optional16CharString => "str_opt(16)",
            Column::Optional50CharString => "str_opt(50)",
            Column::Optional100Value50CharStringDict => "dict(100,str_opt(50))",
            Column::RequiredI32List => "i32_list",
            Column::OptionalI32List => "i32_list_opt",
            Column::Required4CharStringList => "str_list(4)",
            Column::Optional4CharStringList => "str_list_opt(4)",
            Column::TenValueI32(_) => "i32(0..10)",
            Column::JoinItemI32 => "i32(1..204K)",
            Column::JoinTicketI64 => "i64(0..n/10)",
        };
        f.write_str(s)
    }
}

impl Column {
    fn generate(self, size: usize) -> ArrayRef {
        match self {
            Column::RequiredI32 => Arc::new(create_primitive_array::<Int32Type>(size, 0.)),
            Column::OptionalI32 => Arc::new(create_primitive_array::<Int32Type>(size, 0.2)),
            Column::RequiredI64 => Arc::new(create_primitive_array::<Int64Type>(size, 0.)),
            Column::OptionalI64 => Arc::new(create_primitive_array::<Int64Type>(size, 0.2)),
            Column::RequiredF64 => Arc::new(create_primitive_array::<Float64Type>(size, 0.)),
            Column::OptionalF64 => Arc::new(create_primitive_array::<Float64Type>(size, 0.2)),
            // Values share their leading encoded bytes, unlike the full-range
            // columns above, so the first radix passes split few rows.
            Column::SmallRangeI64 => {
                Arc::new(create_primitive_array_range::<Int64Type>(size, 0., 0..1000))
            }
            Column::Decimal128 => Arc::new(create_decimal128_array(size, 18, 2)),
            Column::Required16CharString => {
                Arc::new(create_string_array_with_len::<i32>(size, 0., 16))
            }
            Column::Optional16CharString => {
                Arc::new(create_string_array_with_len::<i32>(size, 0.2, 16))
            }
            Column::Optional50CharString => {
                Arc::new(create_string_array_with_len::<i32>(size, 0.2, 50))
            }
            Column::Optional100Value50CharStringDict => {
                Arc::new(create_dict_from_values::<Int32Type>(
                    size,
                    0.1,
                    &create_string_array_with_len::<i32>(100, 0., 50),
                ))
            }
            Column::RequiredI32List => {
                let field = Field::new(
                    "_1",
                    DataType::List(Arc::new(Field::new_list_field(DataType::Int32, false))),
                    true,
                );
                create_random_array(&field, size, 0., 1.).unwrap()
            }
            Column::OptionalI32List => {
                let field = Field::new(
                    "_1",
                    DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
                    true,
                );
                create_random_array(&field, size, 0.2, 1.).unwrap()
            }
            Column::Required4CharStringList => {
                let field = Field::new(
                    "_1",
                    DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, false))),
                    true,
                );
                create_random_array(&field, size, 0., 1.).unwrap()
            }
            Column::Optional4CharStringList => {
                let field = Field::new(
                    "_1",
                    DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
                    true,
                );
                create_random_array(&field, size, 0.2, 1.).unwrap()
            }
            Column::TenValueI32(seed) => {
                let mut rng = StdRng::seed_from_u64(seed);
                Arc::new(Int32Array::from_iter_values(
                    (0..size).map(|_| rng.random_range(0..10)),
                ))
            }
            Column::JoinItemI32 => {
                let mut rng = StdRng::seed_from_u64(1);
                Arc::new(Int32Array::from_iter_values(
                    (0..size).map(|_| rng.random_range(1..204_001)),
                ))
            }
            Column::JoinTicketI64 => {
                let mut rng = StdRng::seed_from_u64(2);
                let tickets = (size as i64 / 10).max(1);
                Arc::new(Int64Array::from_iter_values(
                    (0..size).map(|_| rng.random_range(0..tickets)),
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

    c.bench_function(&format!("lexsort_to_indices({columns:?}): {len}"), |b| {
        b.iter(|| hint::black_box(lexsort_to_indices(&sort_columns, None).unwrap()))
    });

    c.bench_function(&format!("lexsort_rows({columns:?}): {len}"), |b| {
        b.iter(|| {
            hint::black_box({
                let fields = arrays
                    .iter()
                    .map(|a| SortField::new(a.data_type().clone()))
                    .collect();
                let converter = RowConverter::new(fields).unwrap();
                let rows = converter.convert_columns(&arrays).unwrap();
                let mut sort: Vec<_> = rows.iter().enumerate().collect();
                sort.sort_unstable_by_key(|(_, a)| *a);
                UInt32Array::from_iter_values(sort.iter().map(|(i, _)| *i as u32))
            })
        })
    });

    c.bench_function(&format!("lexsort_radix({columns:?}): {len}"), |b| {
        b.iter(|| {
            hint::black_box({
                let fields = arrays
                    .iter()
                    .map(|a| SortField::new(a.data_type().clone()))
                    .collect();
                let converter = RowConverter::new(fields).unwrap();
                let rows = converter.convert_columns(&arrays).unwrap();
                radix_sort_to_indices(&rows).unwrap()
            })
        })
    });

    // The encoding part of lexsort_radix, so the difference is the sort
    c.bench_function(&format!("encode({columns:?}): {len}"), |b| {
        b.iter(|| {
            hint::black_box({
                let fields = arrays
                    .iter()
                    .map(|a| SortField::new(a.data_type().clone()))
                    .collect();
                let converter = RowConverter::new(fields).unwrap();
                converter.convert_columns(&arrays).unwrap()
            })
        })
    });
}

fn add_benchmark(c: &mut Criterion) {
    let cases: &[&[Column]] = &[
        // Single-column primitives
        &[Column::RequiredI32],
        &[Column::OptionalI32],
        &[Column::RequiredI64],
        &[Column::OptionalI64],
        &[Column::RequiredF64],
        &[Column::OptionalF64],
        // Single-column strings
        &[Column::Required16CharString],
        &[Column::Optional16CharString],
        // Multi-column
        &[Column::RequiredI32, Column::OptionalI32],
        &[Column::SmallRangeI64, Column::RequiredI32],
        &[Column::Decimal128, Column::RequiredI32],
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
        &[Column::OptionalI32, Column::RequiredI32List],
        &[Column::OptionalI32, Column::OptionalI32List],
        &[Column::OptionalI32List, Column::OptionalI32],
        &[Column::RequiredI32, Column::Required4CharStringList],
        &[Column::Required4CharStringList, Column::RequiredI32],
        &[Column::RequiredI32, Column::Optional4CharStringList],
        &[Column::Optional4CharStringList, Column::RequiredI32],
        &[
            Column::RequiredI32,
            Column::RequiredI32List,
            Column::Required16CharString,
        ],
        &[
            Column::OptionalI32,
            Column::OptionalI32List,
            Column::Optional50CharString,
        ],
    ];

    for case in cases {
        do_bench(c, case, 4096);
        do_bench(c, case, 4096 * 8);
    }

    // Integer keys longer than 8 bytes after the bytes every row shares, which
    // fit in the 12-byte radix key. Groups of rows that agree on their first
    // 8 such bytes grow with the row count, so these also run at 1M rows.
    let ties: &[&[Column]] = &[
        &[Column::JoinItemI32, Column::JoinTicketI64],
        &[
            Column::TenValueI32(1),
            Column::TenValueI32(2),
            Column::TenValueI32(3),
        ],
    ];
    for case in ties {
        for len in [4096, 4096 * 8, 1 << 20] {
            do_bench(c, case, len);
        }
    }
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
