// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

#[macro_use]
extern crate criterion;
use criterion::Criterion;

use arrow::array::*;
use arrow::datatypes::*;
use arrow::util::bench_util::*;
use arrow_buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{Field, UnionFields};
use arrow_select::minify::{compacted_size, minify, row_sizes};
use std::hint;
use std::sync::Arc;

/// Rows in the parent array of each slice.
const PARENT: usize = 100_000;
/// Rows in each slice.
const SLICE: usize = 8192;

/// Benchmarks `minify`, `compacted_size` and `row_sizes` on `array`.
fn bench(c: &mut Criterion, name: &str, array: &dyn Array) {
    c.bench_function(&format!("minify {name}"), |b| {
        b.iter(|| hint::black_box(minify(array).unwrap()))
    });
    c.bench_function(&format!("compacted_size {name}"), |b| {
        b.iter(|| hint::black_box(compacted_size(array).unwrap()))
    });
    let mut sizes = vec![0; array.len()];
    c.bench_function(&format!("row_sizes {name}"), |b| {
        b.iter(|| {
            sizes.fill(0);
            row_sizes(array, &mut sizes).unwrap();
            hint::black_box(&sizes);
        })
    });
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
    bench(c, "i32 slice", mid(&ints).as_ref());

    let bools = create_boolean_array(PARENT, 0.1, 0.5);
    bench(c, "boolean slice", mid(&bools).as_ref());

    let fixed = create_fsb_array(PARENT, 0.1, 16);
    bench(c, "fixed size binary slice", mid(&fixed).as_ref());

    let fsl = FixedSizeListArray::new(
        Arc::new(Field::new_list_field(DataType::Int32, true)),
        4,
        Arc::new(create_primitive_array::<Int32Type>(4 * PARENT, 0.1)),
        None,
    );
    bench(c, "fixed size list<i32> slice", mid(&fsl).as_ref());

    let strings = create_string_array::<i32>(PARENT, 0.0);
    bench(c, "utf8 slice no nulls", mid(&strings).as_ref());
    let null_ranges = strings_with_null_ranges(PARENT);
    bench(c, "utf8 slice null ranges", mid(&null_ranges).as_ref());

    let views = create_string_view_array_with_max_len(PARENT, 0.1, 64);
    bench(c, "utf8view slice", mid(&views).as_ref());
    let sparse = arrow_select::take::take(
        &views,
        &UInt32Array::from_iter_values((0..SLICE as u32).map(|i| i * 12)),
        None,
    )
    .unwrap();
    bench(c, "utf8view sparse", sparse.as_ref());

    let field = Arc::new(Field::new_list_field(DataType::Utf8, true));
    let lists = ListArray::new(
        field,
        OffsetBuffer::from_lengths((0..PARENT / 4).map(|_| 4)),
        Arc::new(create_string_array::<i32>(PARENT, 0.1)),
        Some(NullBuffer::from_iter((0..PARENT / 4).map(|i| i % 7 != 0))),
    );
    bench(c, "list<utf8> slice", mid(&lists).as_ref());

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
    bench(c, "listview slice", mid(&list_views).as_ref());

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
    bench(c, "dense union slice", mid(&union).as_ref());

    let dict_values = create_string_array::<i32>(PARENT, 0.0);
    let keys = Int32Array::from_iter_values((0..PARENT as i32).map(|i| i % 1000));
    let dict = DictionaryArray::<Int32Type>::try_new(keys, Arc::new(dict_values)).unwrap();
    bench(c, "dictionary<utf8> few referenced", mid(&dict).as_ref());

    let struct_array = StructArray::from(vec![
        (
            Arc::new(Field::new("i", DataType::Int64, true)),
            Arc::new(create_primitive_array::<Int64Type>(PARENT, 0.1)) as ArrayRef,
        ),
        (
            Arc::new(Field::new("s", DataType::Utf8, true)),
            Arc::new(create_string_array::<i32>(PARENT, 0.1)) as ArrayRef,
        ),
    ]);
    bench(c, "struct<i64, utf8> slice", mid(&struct_array).as_ref());

    let run_ends = Int32Array::from_iter_values((1..=PARENT as i32 / 4).map(|i| i * 4));
    let run_values = create_primitive_array::<Int64Type>(PARENT / 4, 0.1);
    let runs = RunArray::<Int32Type>::try_new(&run_ends, &run_values).unwrap();
    bench(c, "run end encoded slice", mid(&runs).as_ref());

    let minimal = minify(mid(&lists).as_ref()).unwrap();
    bench(c, "list<utf8> already minimal", minimal.as_ref());
    let minimal = minify(mid(&null_ranges).as_ref()).unwrap();
    bench(c, "utf8 already minimal", minimal.as_ref());

    let large = strings_with_null_ranges(4 * 1024 * 1024);
    bench(
        c,
        "utf8 4M rows null ranges",
        &large.slice(1, large.len() - 2),
    );
}

/// Cases for review: already compact arrays, small slices of large parents,
/// many view buffers, unsorted views and large dictionaries. `gc` and `take`
/// are baselines that do comparable work.
fn add_extra(c: &mut Criterion) {
    let take_all = |a: &dyn Array| {
        let idx = UInt32Array::from_iter_values(0..a.len() as u32);
        arrow_select::take::take(a, &idx, None).unwrap()
    };
    let ints = Int32Array::from_iter_values(0..1_000_000);
    bench(c, "minify i32 1M already minimal", &ints);

    let big = create_string_array::<i32>(10_000_000, 0.1);
    let small = big.slice(5_000_000, 100);
    bench(c, "minify utf8 slice 100 of 10M", &small);
    c.bench_function("take utf8 slice 100 of 10M", |b| {
        b.iter(|| hint::black_box(take_all(&small)))
    });

    let views = create_string_view_array_with_max_len(1_000_000, 0.1, 64);
    let slice = views.slice(500_000, 1000);
    bench(c, "minify utf8view slice 1000 of 1M", &slice);
    c.bench_function("gc utf8view slice 1000 of 1M", |b| {
        b.iter(|| hint::black_box(slice.gc()))
    });
    c.bench_function("take utf8view slice 1000 of 1M", |b| {
        b.iter(|| hint::black_box(take_all(&slice)))
    });

    let compact = minify(&views).unwrap();
    bench(c, "minify utf8view 1M already minimal", compact.as_ref());
    let compact = compact.as_string_view().clone();
    c.bench_function("gc utf8view 1M already minimal", |b| {
        b.iter(|| hint::black_box(compact.gc()))
    });

    // Reversed views: the range sort runs on every call
    let rev = arrow_select::take::take(
        &compact,
        &UInt32Array::from_iter_values((0..1_000_000u32).rev()),
        None,
    )
    .unwrap();
    bench(c, "minify utf8view 1M reversed", rev.as_ref());
    let revs = rev.as_string_view().clone();
    c.bench_function("gc utf8view 1M reversed", |b| {
        b.iter(|| hint::black_box(revs.gc()))
    });

    // One data buffer per row
    let bufs: Vec<Buffer> = (0..10_000)
        .map(|i| Buffer::from_vec(format!("{i:040}").into_bytes()))
        .collect();
    let v: Vec<u128> = (0..10_000u32)
        .map(|i| builder::make_view(bufs[i as usize].as_slice(), i, 0))
        .collect();
    let many = StringViewArray::try_new(v.into(), bufs, None).unwrap();
    bench(c, "minify utf8view 10k buffers", &many);
    let many_slice = many.slice(5000, 100);
    bench(c, "minify utf8view 10k buffers slice 100", &many_slice);
    c.bench_function("gc utf8view 10k buffers slice 100", |b| {
        b.iter(|| hint::black_box(many_slice.gc()))
    });

    let keys = Int32Array::from_iter_values(0..1_000_000);
    let dict = DictionaryArray::<Int32Type>::try_new(
        keys,
        Arc::new(create_string_array::<i32>(1_000_000, 0.0)),
    )
    .unwrap();
    let dslice = dict.slice(500_000, 100);
    bench(c, "minify dictionary 1M values slice 100", &dslice);
    c.bench_function("garbage_collect_dictionary 1M values slice 100", |b| {
        b.iter(|| {
            hint::black_box(arrow_select::dictionary::garbage_collect_dictionary(&dslice).unwrap())
        })
    });

    let union_fields = UnionFields::try_new(
        [0, 1],
        [
            Field::new("i", DataType::Int32, true),
            Field::new("s", DataType::Utf8, true),
        ],
    )
    .unwrap();
    let n = 1_000_000;
    let type_ids: ScalarBuffer<i8> = (0..n).map(|i| (i % 2) as i8).collect();
    let offsets: ScalarBuffer<i32> = (0..n as i32).map(|i| i / 2).collect();
    let union = UnionArray::try_new(
        union_fields,
        type_ids,
        Some(offsets),
        vec![
            Arc::new(create_primitive_array::<Int32Type>(n / 2, 0.1)),
            Arc::new(create_string_array::<i32>(n / 2, 0.1)),
        ],
    )
    .unwrap();
    bench(
        c,
        "minify dense union slice 100 of 1M",
        &union.slice(n / 2, 100),
    );
}

criterion_group!(benches, add_benchmark, add_extra);
criterion_main!(benches);
