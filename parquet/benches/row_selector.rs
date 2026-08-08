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

use arrow_array::BooleanArray;
use arrow_buffer::BooleanBuffer;
use criterion::*;
use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use rand::RngExt;
use std::hint;

/// Run lengths for the mask conversion benchmarks. Shorter runs mean more
/// [`RowSelector`]s per row, so the RLE encoding dominates.
const MASK_RUN_LENGTHS: &[usize] = &[1, 4, 16, 32, 48, 64, 96, 128];

const MASK_ALGEBRA_ROWS: usize = 3_000_000;

/// Operand length pairs. Unequal lengths pass the longer side's tail through unchanged,
/// so the ratio decides how much of the work is the bitwise combine versus the tail.
const MASK_ALGEBRA_LENGTHS: &[(&str, usize, usize)] = &[
    ("equal", MASK_ALGEBRA_ROWS, MASK_ALGEBRA_ROWS),
    ("tail1", MASK_ALGEBRA_ROWS, MASK_ALGEBRA_ROWS - 1),
    ("tail1of3", MASK_ALGEBRA_ROWS, MASK_ALGEBRA_ROWS * 2 / 3),
    ("tail_most", MASK_ALGEBRA_ROWS, 1_000),
];

/// Bit offsets for the left and right operand. Masks come from [`BooleanBuffer::slice`],
/// so a non-zero offset is normal. Whether the two share a sub-64-bit alignment decides
/// which path the underlying bitwise helpers take, so both are covered.
const MASK_ALGEBRA_OFFSETS: &[(&str, usize, usize)] = &[
    ("both_zero", 0, 0),
    ("same_mod64", 3, 3),
    ("same_mod64_far", 3, 67),
    ("diff_mod64", 3, 5),
];

/// Generates a random RowSelection with a specified selection ratio.
///
/// # Arguments
///
/// * `total_rows` - The total number of rows in the selection.
/// * `selection_ratio` - The ratio of rows to select (e.g., 1/3 for ~33% selection).
///
/// # Returns
///
/// * A `BooleanArray` instance with randomly selected rows based on the provided ratio.
fn generate_random_row_selection(total_rows: usize, selection_ratio: f64) -> BooleanArray {
    let mut rng = rand::rng();
    let bools: Vec<bool> = (0..total_rows)
        .map(|_| rng.random_bool(selection_ratio))
        .collect();
    BooleanArray::from(bools)
}

/// Generates a mask alternating between selected and skipped runs of `run_len` rows.
fn generate_run_length_mask(total_rows: usize, run_len: usize) -> BooleanBuffer {
    BooleanBuffer::from_iter((0..total_rows).map(|row| (row / run_len).is_multiple_of(2)))
}

/// Builds a mask-backed [`RowSelection`] carrying `offset` as its bit offset.
fn mask_algebra_operand(len: usize, offset: usize, selection_ratio: f64) -> RowSelection {
    let mut rng = rand::rng();
    let bits: Vec<bool> = (0..len + offset)
        .map(|_| rng.random_bool(selection_ratio))
        .collect();
    RowSelection::from_boolean_buffer(BooleanBuffer::from(bits).slice(offset, len))
}

/// Benchmarks the bitwise `intersection`/`union` path, taken when both operands are
/// mask-backed. The `intersection`/`union` benchmarks above are selector-backed and take
/// the [`RowSelector`] merge path instead.
fn bench_mask_backed_algebra(c: &mut Criterion, selection_ratio: f64) {
    for (offset_label, left_offset, right_offset) in MASK_ALGEBRA_OFFSETS {
        for (length_label, left_len, right_len) in MASK_ALGEBRA_LENGTHS {
            let left = mask_algebra_operand(*left_len, *left_offset, selection_ratio);
            let right = mask_algebra_operand(*right_len, *right_offset, selection_ratio);
            let label = format!("{length_label}/{offset_label}");

            c.bench_with_input(
                BenchmarkId::new("mask_intersection", &label),
                &(&left, &right),
                |b, (left, right)| b.iter(|| hint::black_box(left.intersection(right))),
            );

            c.bench_with_input(
                BenchmarkId::new("mask_union", &label),
                &(&left, &right),
                |b, (left, right)| b.iter(|| hint::black_box(left.union(right))),
            );
        }
    }
}

/// Registers one mask-backed `and_then` case under `group/label`.
fn bench_and_then_case(
    c: &mut Criterion,
    group: &str,
    label: String,
    outer: &RowSelection,
    inner: &RowSelection,
) {
    c.bench_with_input(
        BenchmarkId::new(group, label),
        &(outer, inner),
        |b, (outer, inner)| b.iter(|| hint::black_box(outer.and_then(inner))),
    );
}

/// A mask-backed selection over `len` rows selecting only the row at `index`.
fn single_row_selection(len: usize, index: usize) -> RowSelection {
    RowSelection::from_boolean_buffer(BooleanBuffer::from_iter((0..len).map(|i| i == index)))
}

/// A mask-backed selection over [`MASK_ALGEBRA_ROWS`] selecting every
/// `period`-th row.
fn periodic_selection(period: usize) -> RowSelection {
    RowSelection::from_boolean_buffer(BooleanBuffer::from_iter(
        (0..MASK_ALGEBRA_ROWS).map(|i| i % period == 0),
    ))
}

/// Benchmarks mask-backed `and_then`, grouped into three case families:
///
/// 1. `outer_*/inner_random_third` — outer shape sweep against the inner
///    produced by a moderately selective filter.
/// 2. `outer_sparse_1pct/inner_*` — inner shape sweep over a sparse outer;
///    sparse and front-clustered inners exercise the early exit.
/// 3. `outer_dense_90pct/inner_*` — dense outer with a sparse inner, where
///    the word-wise kernel wins the most over per-set-bit iteration.
///
/// `inner` must have exactly `outer.row_count()` rows, so operands are built
/// per case rather than shared with the other algebra benchmarks.
fn bench_mask_backed_and_then(c: &mut Criterion, selection_ratio: f64) {
    const GROUP: &str = "mask_and_then";

    // Family 1: outer shape sweep, inner random ~33%
    let outer_cases: Vec<(&str, RowSelection)> = vec![
        (
            "outer_sparse_1pct",
            mask_algebra_operand(MASK_ALGEBRA_ROWS, 0, 0.01),
        ),
        (
            "outer_random_third",
            mask_algebra_operand(MASK_ALGEBRA_ROWS, 0, selection_ratio),
        ),
        (
            "outer_random_third_offset3",
            mask_algebra_operand(MASK_ALGEBRA_ROWS, 3, selection_ratio),
        ),
        (
            "outer_dense_90pct",
            mask_algebra_operand(MASK_ALGEBRA_ROWS, 0, 0.9),
        ),
        (
            "outer_run32",
            RowSelection::from_boolean_buffer(generate_run_length_mask(MASK_ALGEBRA_ROWS, 32)),
        ),
    ];
    for (label, outer) in &outer_cases {
        let inner = mask_algebra_operand(outer.row_count(), 3, selection_ratio);
        bench_and_then_case(
            c,
            GROUP,
            format!("{label}/inner_random_third"),
            outer,
            &inner,
        );
    }

    // A weakly selective inner: the densest shape that still avoids the
    // all-ones fast path, maximizing per-word deposit work.
    let outer = mask_algebra_operand(MASK_ALGEBRA_ROWS, 0, selection_ratio);
    let inner = mask_algebra_operand(outer.row_count(), 3, 0.9);
    bench_and_then_case(
        c,
        GROUP,
        "outer_random_third/inner_dense_90pct".to_string(),
        &outer,
        &inner,
    );

    // Family 2: inner shape sweep, outer sparse 1% — all route to the sparse
    // kernel.
    let outer = mask_algebra_operand(MASK_ALGEBRA_ROWS, 0, 0.01);
    let selected = outer.row_count();
    let inner_cases: Vec<(&str, RowSelection)> = vec![
        ("inner_first_only", single_row_selection(selected, 0)),
        (
            "inner_front_cluster",
            RowSelection::from_boolean_buffer(BooleanBuffer::from_iter(
                (0..selected).map(|i| i < selected / 100),
            )),
        ),
        (
            "inner_sparse_0_1pct",
            mask_algebra_operand(selected, 0, 0.001),
        ),
        (
            "inner_last_only",
            single_row_selection(selected, selected - 1),
        ),
    ];
    for (label, inner) in &inner_cases {
        bench_and_then_case(
            c,
            GROUP,
            format!("outer_sparse_1pct/{label}"),
            &outer,
            inner,
        );
    }

    // Family 3: dense outer, sparse inners — all route to the dense kernel.
    // Front-loaded inners exercise its early exit; a trailing bit forces a
    // full walk.
    let outer = mask_algebra_operand(MASK_ALGEBRA_ROWS, 0, 0.9);
    let selected = outer.row_count();
    let inner_cases: Vec<(&str, RowSelection)> = vec![
        ("inner_first_only", single_row_selection(selected, 0)),
        (
            "inner_front_cluster",
            RowSelection::from_boolean_buffer(BooleanBuffer::from_iter(
                (0..selected).map(|i| i < selected / 100),
            )),
        ),
        (
            "inner_last_only",
            single_row_selection(selected, selected - 1),
        ),
    ];
    for (label, inner) in &inner_cases {
        bench_and_then_case(
            c,
            GROUP,
            format!("outer_dense_90pct/{label}"),
            &outer,
            inner,
        );
    }
}

/// Benchmarks shapes near the sparse/dense kernel dispatch boundary of
/// mask-backed `and_then`.
///
/// The dispatch estimates `selected + 16 * other_true_count` against
/// `len / 20`, so with a single-bit inner the boundary sits at an outer
/// density of 1/20 words (`every_20`), and with a random ~33% inner at an
/// outer period of ~126. A discontinuity across adjacent cases would indicate
/// a misplaced threshold.
fn bench_mask_backed_and_then_boundary(c: &mut Criterion, selection_ratio: f64) {
    const GROUP: &str = "mask_and_then_boundary";

    // Family 1: periodic outer, single-bit inner; boundary at `every_20`
    for period in [32, 24, 20, 16, 8] {
        let outer = periodic_selection(period);
        let inner = single_row_selection(outer.row_count(), outer.row_count() - 1);
        bench_and_then_case(
            c,
            GROUP,
            format!("outer_every_{period}/inner_last_only"),
            &outer,
            &inner,
        );
    }

    // Family 2: periodic outer, random ~33% inner; boundary at ~`every_126`
    for period in [200, 133, 100, 50] {
        let outer = periodic_selection(period);
        let inner = mask_algebra_operand(outer.row_count(), 3, selection_ratio);
        bench_and_then_case(
            c,
            GROUP,
            format!("outer_every_{period}/inner_random_third"),
            &outer,
            &inner,
        );
    }
}

/// Benchmarks converting a mask-backed [`RowSelection`] into [`RowSelector`]s.
///
/// `RowSelection::iter` caches the RLE form, so a caller that iterates before
/// consuming should reuse that cache rather than encode the bitmap twice.
/// `mask_consume` is the same conversion with a cold cache.
fn bench_mask_backed_conversion(c: &mut Criterion, total_rows: usize, selection_ratio: f64) {
    let mut cases: Vec<(String, BooleanBuffer)> = MASK_RUN_LENGTHS
        .iter()
        .map(|&run_len| {
            (
                format!("run{run_len:02}"),
                generate_run_length_mask(total_rows, run_len),
            )
        })
        .collect();
    cases.push((
        "random".to_string(),
        generate_random_row_selection(total_rows, selection_ratio)
            .values()
            .clone(),
    ));

    for (label, mask) in cases {
        let selection = RowSelection::from_boolean_buffer(mask);

        c.bench_with_input(
            BenchmarkId::new("mask_iterate_then_consume", &label),
            &selection,
            |b, selection| {
                b.iter(|| {
                    // `clone` drops the selector cache, so each iteration
                    // starts from an unconverted selection.
                    let selection = selection.clone();
                    let rows: usize = selection.iter().map(|s| s.row_count).sum();
                    hint::black_box(rows);
                    let selectors: Vec<RowSelector> = selection.into();
                    hint::black_box(selectors);
                })
            },
        );

        c.bench_with_input(
            BenchmarkId::new("mask_consume", &label),
            &selection,
            |b, selection| {
                b.iter(|| {
                    let selectors: Vec<RowSelector> = selection.clone().into();
                    hint::black_box(selectors);
                })
            },
        );
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    let total_rows = 300_000;
    let selection_ratio = 1.0 / 3.0;

    // Generate two random RowSelections with approximately 1/3 of the rows selected.
    let row_selection_a =
        RowSelection::from_filters(&[generate_random_row_selection(total_rows, selection_ratio)]);
    let row_selection_b =
        RowSelection::from_filters(&[generate_random_row_selection(total_rows, selection_ratio)]);

    // Benchmark the intersection of the two RowSelections.
    c.bench_function("intersection", |b| {
        b.iter(|| {
            let intersection = row_selection_a.intersection(&row_selection_b);
            hint::black_box(intersection);
        })
    });

    c.bench_function("union", |b| {
        b.iter(|| {
            let union = row_selection_a.union(&row_selection_b);
            hint::black_box(union);
        })
    });

    c.bench_function("from_filters", |b| {
        let boolean_array = generate_random_row_selection(total_rows, selection_ratio);
        b.iter(|| {
            let array = boolean_array.clone();
            let selection = RowSelection::from_filters(&[array]);
            hint::black_box(selection);
        })
    });

    c.bench_function("and_then", |b| {
        let selected = row_selection_a.row_count();
        let sub_selection =
            RowSelection::from_filters(&[generate_random_row_selection(selected, selection_ratio)]);
        b.iter(|| {
            let result = row_selection_a.and_then(&sub_selection);
            hint::black_box(result);
        })
    });

    bench_mask_backed_algebra(c, selection_ratio);
    bench_mask_backed_and_then(c, selection_ratio);
    bench_mask_backed_and_then_boundary(c, selection_ratio);
    bench_mask_backed_conversion(c, total_rows, selection_ratio);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
