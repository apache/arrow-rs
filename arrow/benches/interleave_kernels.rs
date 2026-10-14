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
use std::ops::Range;

use rand::RngExt;

use arrow::datatypes::*;
use arrow::util::test_util::seedable_rng;
use arrow::{array::*, util::bench_util::*};
use arrow_buffer::ScalarBuffer;
use arrow_select::interleave::{interleave, interleave_record_batch};
use std::cell::Cell;
use std::hint;
use std::sync::Arc;

fn do_bench(
    c: &mut Criterion,
    prefix: &str,
    len: usize,
    base: &dyn Array,
    slices: &[Range<usize>],
) {
    let arrays: Vec<_> = slices
        .iter()
        .map(|r| base.slice(r.start, r.end - r.start))
        .collect();
    let values: Vec<_> = arrays.iter().map(|x| x.as_ref()).collect();
    bench_values(
        c,
        &format!("interleave {prefix} {len} {slices:?}"),
        len,
        &values,
    );
}

fn bench_values(c: &mut Criterion, name: &str, len: usize, values: &[&dyn Array]) {
    let mut rng = seedable_rng();
    let indices: Vec<_> = (0..len)
        .map(|_| {
            let array_idx = rng.random_range(0..values.len());
            let value_idx = rng.random_range(0..values[array_idx].len());
            (array_idx, value_idx)
        })
        .collect();

    c.bench_function(name, |b| {
        b.iter(|| hint::black_box(interleave(values, &indices).unwrap()))
    });
}

/// Number of sources (sorted runs) in the merge-like benchmarks
const MERGE_SOURCES: usize = 128;
/// Number of rows in each source of the merge-like benchmarks
const MERGE_SOURCE_LEN: usize = 8192;
/// Number of rows interleaved in each call of the merge-like benchmarks
const MERGE_BATCH_SIZE: usize = 8192;

/// Indices of a k-way merge of `num_sources` sources of `source_len` rows,
/// split into windows of `batch_size` rows (one window per output batch).
///
/// Like a sort preserving merge, each output row takes the next row of a random
/// (not yet exhausted) source, so the rows taken from each source are always
/// advancing.
fn merge_windows(
    num_sources: usize,
    source_len: usize,
    batch_size: usize,
) -> Vec<Vec<(usize, usize)>> {
    let mut rng = seedable_rng();
    let mut cursors = vec![0; num_sources];
    let mut active: Vec<usize> = (0..num_sources).collect();
    let mut indices = Vec::with_capacity(num_sources * source_len);
    while !active.is_empty() {
        let i = rng.random_range(0..active.len());
        let source = active[i];
        indices.push((source, cursors[source]));
        cursors[source] += 1;
        if cursors[source] == source_len {
            active.swap_remove(i);
        }
    }
    indices.chunks(batch_size).map(|w| w.to_vec()).collect()
}

/// `MERGE_SOURCES` string arrays of `MERGE_SOURCE_LEN` rows each
fn merge_sources(null_density: f32, min_len: usize, max_len: usize, seed: u64) -> Vec<ArrayRef> {
    (0..MERGE_SOURCES as u64)
        .map(|i| {
            Arc::new(
                create_string_array_with_len_range_and_prefix_and_seed::<i32>(
                    MERGE_SOURCE_LEN,
                    null_density,
                    min_len,
                    max_len,
                    "",
                    seed * MERGE_SOURCES as u64 + i,
                ),
            ) as ArrayRef
        })
        .collect()
}

/// Benchmark interleaving successive output batches of a large k-way merge,
/// each call interleaves the next window so it reads memory that was not
/// recently touched, as in a real merge
fn bench_merge_like(c: &mut Criterion, name: &str, values: &[&dyn Array]) {
    let windows = merge_windows(values.len(), MERGE_SOURCE_LEN, MERGE_BATCH_SIZE);
    let next = Cell::new(0);
    c.bench_function(name, |b| {
        b.iter(|| {
            let i = next.get();
            next.set((i + 1) % windows.len());
            hint::black_box(interleave(values, &windows[i]).unwrap())
        })
    });
}

/// Like [`bench_merge_like`] but for record batches with multiple columns
fn bench_merge_like_record_batch(c: &mut Criterion, name: &str, batches: &[RecordBatch]) {
    let batches: Vec<&RecordBatch> = batches.iter().collect();
    let windows = merge_windows(batches.len(), MERGE_SOURCE_LEN, MERGE_BATCH_SIZE);
    let next = Cell::new(0);
    c.bench_function(name, |b| {
        b.iter(|| {
            let i = next.get();
            next.set((i + 1) % windows.len());
            hint::black_box(interleave_record_batch(&batches, &windows[i]).unwrap())
        })
    });
}

fn add_merge_like_benchmark(c: &mut Criterion) {
    let label = format!("{MERGE_SOURCES}x{MERGE_SOURCE_LEN} merge {MERGE_BATCH_SIZE}");

    // (name, null density, min len, max len)
    let string_cases = [
        ("str(20, 0.0)", 0.0, 20, 20),
        ("str(4..=40, 0.1)", 0.1, 4, 40),
        ("str(36, 0.0)", 0.0, 36, 36),
        ("str(2, 0.0)", 0.0, 2, 2),
        ("str(4..=7, 0.0)", 0.0, 4, 7),
        ("str(1..=7, 0.0)", 0.0, 1, 7),
        ("str(0..=7, 0.1)", 0.1, 0, 7),
    ];
    for (seed, (name, null_density, min_len, max_len)) in string_cases.into_iter().enumerate() {
        let arrays = merge_sources(null_density, min_len, max_len, seed as u64);
        let values: Vec<&dyn Array> = arrays.iter().map(|a| a.as_ref()).collect();
        bench_merge_like(c, &format!("interleave {name} {label}"), &values);
    }

    // Multiple string columns, so consecutive columns evict each other from
    // the caches and TLB
    let num_columns = 16;
    let columns: Vec<Vec<ArrayRef>> = (0..num_columns)
        .map(|i| merge_sources(0.1, 4, 40, 100 + i as u64))
        .collect();
    let schema = Arc::new(Schema::new(
        (0..num_columns)
            .map(|i| Field::new(format!("c{i}"), DataType::Utf8, true))
            .collect::<Vec<_>>(),
    ));
    let batches: Vec<RecordBatch> = (0..MERGE_SOURCES)
        .map(|source| {
            let columns = columns.iter().map(|c| Arc::clone(&c[source])).collect();
            RecordBatch::try_new(Arc::clone(&schema), columns).unwrap()
        })
        .collect();
    bench_merge_like_record_batch(
        c,
        &format!("interleave_record_batch {num_columns} x str(4..=40, 0.1) {label}"),
        &batches,
    );
}

fn add_benchmark(c: &mut Criterion) {
    let i32 = create_primitive_array::<Int32Type>(1024, 0.);
    let i32_opt = create_primitive_array::<Int32Type>(1024, 0.5);
    let boolean = create_boolean_array(1024, 0.0, 0.5);
    let boolean_opt = create_boolean_array(1024, 0.1, 0.5);
    let string = create_string_array_with_len::<i32>(1024, 0., 20);
    let string_opt = create_string_array_with_len::<i32>(1024, 0.5, 20);
    let values = create_string_array_with_len::<i32>(10, 0.0, 20);
    let dict = create_dict_from_values::<Int32Type>(1024, 0.0, &values);

    let struct_i32_no_nulls_i32_no_nulls = StructArray::new(
        Fields::from(vec![
            Field::new("a", Int32Type::DATA_TYPE, false),
            Field::new("b", Int32Type::DATA_TYPE, false),
        ]),
        vec![
            Arc::new(create_primitive_array::<Int32Type>(1024, 0.)),
            Arc::new(create_primitive_array::<Int32Type>(1024, 0.)),
        ],
        None,
    );

    let struct_string_no_nulls_string_no_nulls = StructArray::new(
        Fields::from(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Utf8, false),
        ]),
        vec![
            Arc::new(create_string_array_with_len::<i32>(1024, 0., 20)),
            Arc::new(create_string_array_with_len::<i32>(1024, 0., 20)),
        ],
        None,
    );

    let struct_i32_no_nulls_string_no_nulls = StructArray::new(
        Fields::from(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]),
        vec![
            Arc::new(create_primitive_array::<Int32Type>(1024, 0.)),
            Arc::new(create_string_array_with_len::<i32>(1024, 0., 20)),
        ],
        None,
    );

    let values = create_string_array_with_len::<i32>(1024, 0.0, 20);
    let sparse_dict = create_sparse_dict_from_values::<Int32Type>(1024, 0.0, &values, 10..20);

    let string_view = create_string_view_array(1024, 0.0);

    // use 8192 as a standard list size for better coverage
    let list_i64 = create_primitive_list_array_with_seed::<i32, Int64Type>(8192, 0.1, 0.1, 20, 42);
    let list_i64_no_nulls =
        create_primitive_list_array_with_seed::<i32, Int64Type>(8192, 0.0, 0.0, 20, 42);

    let list_view_i64: ListViewArray =
        create_primitive_list_array_with_seed::<i32, Int64Type>(8192, 0.1, 0.1, 20, 42).into();
    let list_view_i64_no_nulls: ListViewArray =
        create_primitive_list_array_with_seed::<i32, Int64Type>(8192, 0.0, 0.0, 20, 42).into();

    // ListView with overlapping offset/size ranges: 100 unique element ranges of
    // 20 elements each, with 80 rows sharing each range (8000 rows, 2000 backing elements).
    let list_view_overlapping = {
        let num_unique = 100;
        let rows_per_unique = 80;
        let elems_per_row = 20;
        let total_rows = num_unique * rows_per_unique;
        let values = Arc::new(Int64Array::from_iter_values(
            0..((num_unique * elems_per_row) as i64),
        ));
        let offsets: Vec<i32> = (0..total_rows)
            .map(|i| ((i / rows_per_unique) * elems_per_row) as i32)
            .collect();
        let sizes = vec![elems_per_row as i32; total_rows];
        ListViewArray::new(
            Arc::new(Field::new_list_field(DataType::Int64, false)),
            ScalarBuffer::from(offsets),
            ScalarBuffer::from(sizes),
            values,
            None,
        )
    };

    let fsl_i64 = create_primitive_fixed_size_list_array::<Int64Type>(8192, 0.0, 0.0, 5);
    let fsl_i64_nulls = create_primitive_fixed_size_list_array::<Int64Type>(8192, 0.1, 0.1, 5);

    let map_str_i64 = create_string_map_array::<Int64Type>(8192, 0.0, 5, 8);
    let map_str_i64_nulls = create_string_map_array::<Int64Type>(8192, 0.1, 5, 8);

    let ree_run_ends = Int32Array::from_iter_values((1..=64).map(|i| i * 16));
    let ree_i64_values = create_primitive_array::<Int64Type>(64, 0.0);
    let ree_i64 = RunArray::<Int32Type>::try_new(&ree_run_ends, &ree_i64_values).unwrap();

    let dict_str_values = create_string_array_with_len::<i32>(20, 0.0, 12);
    let dict_for_ree = create_dict_from_values::<UInt32Type>(64, 0.0, &dict_str_values);
    let ree_dict = RunArray::<Int32Type>::try_new(&ree_run_ends, &dict_for_ree).unwrap();

    let cases: &[(&str, &dyn Array)] = &[
        ("bool(0.0)", &boolean),
        ("bool(0.1)", &boolean_opt),
        ("i32(0.0)", &i32),
        ("i32(0.5)", &i32_opt),
        ("str(20, 0.0)", &string),
        ("str(20, 0.5)", &string_opt),
        ("dict(20, 0.0)", &dict),
        ("dict_sparse(20, 0.0)", &sparse_dict),
        ("str_view(0.0)", &string_view),
        (
            "struct(i32(0.0), i32(0.0)",
            &struct_i32_no_nulls_i32_no_nulls,
        ),
        (
            "struct(str(20, 0.0), str(20, 0.0))",
            &struct_string_no_nulls_string_no_nulls,
        ),
        (
            "struct(i32(0.0), str(20, 0.0)",
            &struct_i32_no_nulls_string_no_nulls,
        ),
        ("list<i64>(0.1,0.1,20)", &list_i64),
        ("list<i64>(0.0,0.0,20)", &list_i64_no_nulls),
        ("list_view<i64>(0.1,0.1,20)", &list_view_i64),
        ("list_view<i64>(0.0,0.0,20)", &list_view_i64_no_nulls),
        ("list_view_overlapping<i64>(80x,20)", &list_view_overlapping),
        ("fixed_size_list<i64,5>(0.0,0.0)", &fsl_i64),
        ("fixed_size_list<i64,5>(0.1,0.1)", &fsl_i64_nulls),
        ("map<utf8,i64>(0.0,5,8)", &map_str_i64),
        ("map<utf8,i64>(0.1,5,8)", &map_str_i64_nulls),
        ("ree_i32<i64>(64 runs)", &ree_i64),
        ("ree_i32<dict<u32,utf8>>(64 runs)", &ree_dict),
    ];

    for (prefix, base) in cases {
        let slices: &[(usize, &[_])] = &[
            (100, &[0..100, 100..230, 450..1000]),
            (400, &[0..100, 100..230, 450..1000]),
            (1024, &[0..100, 100..230, 450..1000]),
            (1024, &[0..100, 100..230, 450..1000, 0..1000]),
        ];

        for (len, slice) in slices {
            do_bench(c, prefix, *len, *base, slice);
        }
    }

    for len in [100, 1024, 2048] {
        bench_values(
            c,
            &format!("interleave dict_distinct {len}"),
            len,
            &[&dict, &sparse_dict],
        );
    }
}

criterion_group!(benches, add_benchmark, add_merge_like_benchmark);
criterion_main!(benches);
