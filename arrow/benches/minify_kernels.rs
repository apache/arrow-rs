// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

#[macro_use]
extern crate criterion;
use criterion::Criterion;

use arrow::array::*;
use arrow::datatypes::*;
use arrow::util::bench_util::*;
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{Field, UnionFields};
use arrow_select::minify::minify;
use std::hint;
use std::sync::Arc;

/// Rows in the parent array of each slice.
const PARENT: usize = 100_000;
/// Rows in each slice.
const SLICE: usize = 8192;

fn bench(c: &mut Criterion, name: &str, array: &dyn Array) {
    c.bench_function(name, |b| b.iter(|| hint::black_box(minify(array).unwrap())));
}

/// Slices `SLICE` rows out of the middle of `array`.
fn mid(array: &dyn Array) -> ArrayRef {
    array.slice(array.len() / 2, SLICE)
}

/// Strings where every other row is null and keeps a non-empty range.
fn strings_with_null_ranges(len: usize) -> StringArray {
    let (offsets, values, _) = create_string_array::<i32>(len, 0.0).into_parts();
    let nulls = NullBuffer::from_iter((0..len).map(|i| i % 2 == 0));
    StringArray::new(offsets, values, Some(nulls))
}

fn add_benchmark(c: &mut Criterion) {
    let ints = create_primitive_array::<Int32Type>(PARENT, 0.1);
    bench(c, "minify i32 slice", mid(&ints).as_ref());

    let strings = create_string_array::<i32>(PARENT, 0.0);
    bench(c, "minify utf8 slice no nulls", mid(&strings).as_ref());
    let null_ranges = strings_with_null_ranges(PARENT);
    bench(
        c,
        "minify utf8 slice null ranges",
        mid(&null_ranges).as_ref(),
    );

    let views = create_string_view_array_with_max_len(PARENT, 0.1, 64);
    let sparse = arrow_select::take::take(
        &views,
        &UInt32Array::from_iter_values((0..SLICE as u32).map(|i| i * 12)),
        None,
    )
    .unwrap();
    bench(c, "minify utf8view sparse", sparse.as_ref());

    let field = Arc::new(Field::new_list_field(DataType::Utf8, true));
    let lists = ListArray::new(
        field,
        OffsetBuffer::from_lengths((0..PARENT / 4).map(|_| 4)),
        Arc::new(create_string_array::<i32>(PARENT, 0.1)),
        Some(NullBuffer::from_iter((0..PARENT / 4).map(|i| i % 7 != 0))),
    );
    bench(c, "minify list<utf8> slice", mid(&lists).as_ref());

    let offsets: ScalarBuffer<i32> = (0..PARENT as i32)
        .rev()
        .map(|i| i % (PARENT as i32 - 8))
        .collect();
    let sizes: ScalarBuffer<i32> = (0..PARENT).map(|i| (i % 4) as i32).collect();
    let list_views = ListViewArray::new(
        Arc::new(Field::new_list_field(DataType::Int32, true)),
        offsets,
        sizes,
        Arc::new(create_primitive_array::<Int32Type>(PARENT, 0.0)),
        None,
    );
    bench(c, "minify listview slice", mid(&list_views).as_ref());

    let union_fields = UnionFields::try_new(
        [0, 1],
        [
            Field::new("i", DataType::Int32, true),
            Field::new("s", DataType::Utf8, true),
        ],
    )
    .unwrap();
    let type_ids: ScalarBuffer<i8> = (0..PARENT).map(|i| (i % 2) as i8).collect();
    let union_offsets: ScalarBuffer<i32> = (0..PARENT as i32).map(|i| i / 2).collect();
    let union = UnionArray::try_new(
        union_fields,
        type_ids,
        Some(union_offsets),
        vec![
            Arc::new(create_primitive_array::<Int32Type>(PARENT / 2, 0.1)),
            Arc::new(create_string_array::<i32>(PARENT / 2, 0.1)),
        ],
    )
    .unwrap();
    bench(c, "minify dense union slice", mid(&union).as_ref());

    let dict_values = create_string_array::<i32>(PARENT, 0.0);
    let keys = Int32Array::from_iter_values((0..PARENT as i32).map(|i| i % 1000));
    let dict = DictionaryArray::<Int32Type>::try_new(keys, Arc::new(dict_values)).unwrap();
    bench(
        c,
        "minify dictionary<utf8> few referenced",
        mid(&dict).as_ref(),
    );

    let run_ends = Int32Array::from_iter_values((1..=PARENT as i32 / 4).map(|i| i * 4));
    let run_values = create_primitive_array::<Int64Type>(PARENT / 4, 0.1);
    let runs = RunArray::<Int32Type>::try_new(&run_ends, &run_values).unwrap();
    bench(c, "minify run end encoded slice", mid(&runs).as_ref());

    let minimal = minify(mid(&lists).as_ref()).unwrap();
    bench(c, "minify list<utf8> already minimal", minimal.as_ref());
    let minimal = minify(mid(&null_ranges).as_ref()).unwrap();
    bench(c, "minify utf8 already minimal", minimal.as_ref());

    let large = strings_with_null_ranges(4 * 1024 * 1024);
    bench(
        c,
        "minify utf8 4M rows null ranges",
        &large.slice(1, large.len() - 2),
    );
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
