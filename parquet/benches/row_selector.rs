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
use std::{hint, time::Duration};

/// Run lengths for the mask conversion benchmarks. Shorter runs mean more
/// [`RowSelector`]s per row, so the RLE encoding dominates.
const MASK_RUN_LENGTHS: &[usize] = &[1, 4, 16, 32, 48, 64, 96, 128];

const MASK_ALGEBRA_ROWS: usize = 3_000_000;

const MASK_AND_THEN_ROWS: usize = 8192;
const MASK_AND_THEN_OUTER_SELECTIVITY: &[usize] = &[50, 60, 70, 75, 80, 85, 90, 95, 99];
const MASK_AND_THEN_INNER_SELECTIVITY: &[usize] = &[1, 5, 10, 20, 50, 80, 99];
const MASK_AND_THEN_LENGTHS: &[usize] = &[
    64, 128, 256, 512, 1024, 2048, 4096, 8192, 16_384, 32_768, 65_536,
];

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

fn pseudo_random_mask(len: usize, selection_percent: usize, seed: u64) -> BooleanBuffer {
    assert!(len > 1);
    assert!(selection_percent <= 100);
    let selected = (len * selection_percent / 100).clamp(1, len - 1);
    let mut ranked = (0..len)
        .map(|row| {
            let mut value = (row as u64).wrapping_add(seed);
            value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            value ^= value >> 31;
            (value, row)
        })
        .collect::<Vec<_>>();
    ranked.select_nth_unstable_by_key(selected, |(value, _)| *value);

    let mut bits = vec![false; len];
    for (_, row) in &ranked[..selected] {
        bits[*row] = true;
    }
    BooleanBuffer::from(bits)
}

fn clustered_mask(len: usize, selection_percent: usize, seed: usize) -> BooleanBuffer {
    assert!(len > 1);
    assert!(selection_percent <= 100);
    let selected = (len * selection_percent / 100).clamp(1, len - 1);
    let start = seed % len;
    let mut bits = vec![false; len];
    for row in 0..selected {
        bits[(start + row) % len] = true;
    }
    BooleanBuffer::from(bits)
}

fn mask_and_then_operands(
    rows: usize,
    outer_percent: usize,
    inner_percent: usize,
) -> (RowSelection, RowSelection) {
    let outer = pseudo_random_mask(rows, outer_percent, 0x517c_c1b7_2722_0a95);
    let inner = pseudo_random_mask(outer.count_set_bits(), inner_percent, 0x6eed_0e9d_a4d9_4a4f);
    (
        RowSelection::from_boolean_buffer(outer),
        RowSelection::from_boolean_buffer(inner),
    )
}

fn clustered_mask_and_then_operands(
    rows: usize,
    outer_percent: usize,
    inner_percent: usize,
) -> (RowSelection, RowSelection) {
    let outer = clustered_mask(rows, outer_percent, 17);
    let inner = clustered_mask(outer.count_set_bits(), inner_percent, 31);
    (
        RowSelection::from_boolean_buffer(outer),
        RowSelection::from_boolean_buffer(inner),
    )
}

/// Sweeps the inputs that decide whether the word-at-a-time mask expansion is
/// faster than the set-index implementation. Short measurement times keep the
/// full matrix practical while still providing many iterations per sample.
fn bench_mask_and_then_sweeps(c: &mut Criterion) {
    let mut selectivity = c.benchmark_group("mask_and_then_selectivity/8192");
    selectivity
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(1))
        .sample_size(30);

    for &outer_percent in MASK_AND_THEN_OUTER_SELECTIVITY {
        for &inner_percent in MASK_AND_THEN_INNER_SELECTIVITY {
            let (outer, inner) =
                mask_and_then_operands(MASK_AND_THEN_ROWS, outer_percent, inner_percent);
            selectivity.bench_function(
                format!("outer_{outer_percent:02}_inner_{inner_percent:02}"),
                |b| b.iter(|| hint::black_box(outer.and_then(&inner))),
            );
        }
    }
    selectivity.finish();

    let mut clustered = c.benchmark_group("mask_and_then_clustered_selectivity/8192");
    clustered
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(1))
        .sample_size(30);

    for &outer_percent in MASK_AND_THEN_OUTER_SELECTIVITY {
        for &inner_percent in MASK_AND_THEN_INNER_SELECTIVITY {
            let (outer, inner) =
                clustered_mask_and_then_operands(MASK_AND_THEN_ROWS, outer_percent, inner_percent);
            clustered.bench_function(
                format!("outer_{outer_percent:02}_inner_{inner_percent:02}"),
                |b| b.iter(|| hint::black_box(outer.and_then(&inner))),
            );
        }
    }
    clustered.finish();

    for &(outer_percent, inner_percent) in &[(75, 1), (75, 5), (90, 5), (99, 5), (99, 99)] {
        let mut lengths = c.benchmark_group(format!(
            "mask_and_then_length/outer_{outer_percent:02}_inner_{inner_percent:02}"
        ));
        lengths
            .warm_up_time(Duration::from_millis(500))
            .measurement_time(Duration::from_secs(1))
            .sample_size(30);

        for &rows in MASK_AND_THEN_LENGTHS {
            let (outer, inner) = mask_and_then_operands(rows, outer_percent, inner_percent);
            lengths.bench_function(rows.to_string(), |b| {
                b.iter(|| hint::black_box(outer.and_then(&inner)))
            });
        }
        lengths.finish();

        let mut clustered_lengths = c.benchmark_group(format!(
            "mask_and_then_clustered_length/outer_{outer_percent:02}_inner_{inner_percent:02}"
        ));
        clustered_lengths
            .warm_up_time(Duration::from_millis(500))
            .measurement_time(Duration::from_secs(1))
            .sample_size(30);

        for &rows in MASK_AND_THEN_LENGTHS {
            let (outer, inner) =
                clustered_mask_and_then_operands(rows, outer_percent, inner_percent);
            clustered_lengths.bench_function(rows.to_string(), |b| {
                b.iter(|| hint::black_box(outer.and_then(&inner)))
            });
        }
        clustered_lengths.finish();
    }
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

    let outer_mask = BooleanBuffer::from_iter((0..8192).map(|i| i % 100 != 0));
    let inner_mask =
        BooleanBuffer::from_iter((0..outer_mask.count_set_bits()).map(|i| i % 100 != 0));
    let outer = RowSelection::from_boolean_buffer(outer_mask);
    let inner = RowSelection::from_boolean_buffer(inner_mask);
    let composed = outer.and_then(&inner);
    let third_mask = BooleanBuffer::from_iter((0..composed.row_count()).map(|i| i % 100 != 0));
    let third = RowSelection::from_boolean_buffer(third_mask);
    c.bench_function("mask_and_then/8192/kept_99_100", |b| {
        b.iter(|| hint::black_box(outer.and_then(&inner)))
    });
    c.bench_function("mask_and_then/8192/kept_99_100_twice", |b| {
        b.iter(|| hint::black_box(composed.and_then(&third)))
    });

    let outer_mask = BooleanBuffer::from_iter((0..8192).map(|i| i % 20 != 0));
    let inner_mask =
        BooleanBuffer::from_iter((0..outer_mask.count_set_bits()).map(|i| i % 100 != 0));
    let outer = RowSelection::from_boolean_buffer(outer_mask);
    let inner = RowSelection::from_boolean_buffer(inner_mask);
    c.bench_function("mask_and_then/8192/outer_95_inner_99", |b| {
        b.iter(|| hint::black_box(outer.and_then(&inner)))
    });

    bench_mask_and_then_sweeps(c);

    bench_mask_backed_algebra(c, selection_ratio);
    bench_mask_backed_conversion(c, total_rows, selection_ratio);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
