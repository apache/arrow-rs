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

use super::*;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

const RANDOM_CASES: usize = 50_000;
const MAX_RANDOM_ROWS: usize = 262_144;
const THRESHOLDS: &[usize] = &[0, 1, 8, 16, 31, 32, 33, 64];
const SELECTIVITIES: &[usize] = &[0, 1, 5, 15, 50, 90, 99, 100];

#[test]
fn auto_construction_preserves_global_runs_and_threshold_boundary() {
    let boundary_selected = BooleanArray::from(vec![false, true, true]);
    let boundary_selected_continued = BooleanArray::from(vec![true, true, false]);
    let boundary_skipped_continued = BooleanArray::from(vec![false, false, true]);
    let empty = BooleanArray::from(Vec::<bool>::new());
    let filters = vec![
        boundary_selected,
        empty,
        boundary_selected_continued,
        boundary_skipped_continued,
    ];

    for threshold in THRESHOLDS {
        assert_auto_equivalent(&filters, *threshold, "cross-filter run merge");
    }

    let run31 = run_mask(65_536, 31, 31);
    let run32 = run_mask(65_536, 32, 32);
    let run31_filters = split_evenly(&run31, 8_192);
    let run32_filters = split_evenly(&run32, 8_192);

    assert_eq!(
        RowSelection::from_filters(&run31_filters).auto_selection_strategy(32),
        RowSelectionStrategy::Mask
    );
    assert_eq!(
        RowSelection::from_filters(&run32_filters).auto_selection_strategy(32),
        RowSelectionStrategy::Selectors
    );
    assert_auto_equivalent(&run31_filters, 32, "run31 below threshold");
    assert_auto_equivalent(&run32_filters, 32, "run32 equal to threshold");

    let empty_filters = vec![BooleanArray::from(Vec::<bool>::new())];
    assert_auto_equivalent(&[], 0, "no filters");
    assert_auto_equivalent(&empty_filters, 0, "empty filter");
}

#[test]
fn auto_construction_randomized_equivalence_50k() {
    let mut rng = StdRng::seed_from_u64(0x1077_6000_5eed);

    for case_idx in 0..RANDOM_CASES {
        let rows = random_row_count(case_idx, &mut rng);
        let selectivity = SELECTIVITIES[case_idx % SELECTIVITIES.len()];
        let shape = (case_idx / SELECTIVITIES.len()) % 6;
        let threshold = THRESHOLDS[(case_idx / (SELECTIVITIES.len() * 6)) % THRESHOLDS.len()];

        let mask = random_shape(&mut rng, rows, selectivity, shape);
        let bit_offset = if case_idx % 3 == 0 {
            rng.random_range(1..=63)
        } else {
            0
        };
        let mask = with_bit_offset(mask, bit_offset);
        let filter_count = rng.random_range(1..=64);
        let filters = random_split(&mut rng, &mask, filter_count);
        let context = format!(
            "case={case_idx} rows={rows} selectivity={selectivity} shape={shape} \
             filters={filter_count} threshold={threshold} bit_offset={bit_offset}"
        );

        assert_auto_equivalent(&filters, threshold, &context);
    }
}

fn assert_auto_equivalent(filters: &[BooleanArray], threshold: usize, context: &str) {
    let current = RowSelection::from_filters(filters);
    let current_strategy = current.auto_selection_strategy(threshold);
    let candidate = RowSelection::from_filters_auto(filters, threshold);
    let candidate_strategy = candidate.auto_selection_strategy(threshold);
    let candidate_backing = match &candidate.inner {
        RowSelectionInner::Mask(_) => RowSelectionStrategy::Mask,
        RowSelectionInner::Selectors(_) => RowSelectionStrategy::Selectors,
    };

    assert_eq!(current_strategy, candidate_backing, "backing: {context}");
    assert_eq!(current_strategy, candidate_strategy, "strategy: {context}");
    assert_eq!(
        current.total_row_count(),
        candidate.total_row_count(),
        "length: {context}"
    );
    assert_eq!(
        current.row_count(),
        candidate.row_count(),
        "selected rows: {context}"
    );
    assert_eq!(current, candidate, "logical selection: {context}");
}

fn run_mask(rows: usize, selected_run: usize, skipped_run: usize) -> BooleanBuffer {
    let period = selected_run + skipped_run;
    BooleanBuffer::from_iter((0..rows).map(|row| row % period < selected_run))
}

fn split_evenly(mask: &BooleanBuffer, batch_size: usize) -> Vec<BooleanArray> {
    (0..mask.len())
        .step_by(batch_size)
        .map(|offset| {
            let len = batch_size.min(mask.len() - offset);
            BooleanArray::new(mask.slice(offset, len), None)
        })
        .collect()
}

fn random_row_count(case_idx: usize, rng: &mut StdRng) -> usize {
    match case_idx {
        0 => 0,
        1 => 1,
        2 => 7,
        3 => 8,
        4 => 31,
        5 => 32,
        6 => 33,
        _ if case_idx.is_multiple_of(10_000) => MAX_RANDOM_ROWS,
        _ => {
            let exponent = rng.random_range(0..=18);
            let upper = (1usize << exponent).min(MAX_RANDOM_ROWS);
            rng.random_range(0..=upper)
        }
    }
}

fn random_shape(rng: &mut StdRng, rows: usize, selectivity: usize, shape: usize) -> BooleanBuffer {
    if selectivity == 0 {
        return BooleanBuffer::new_unset(rows);
    }
    if selectivity == 100 {
        return BooleanBuffer::new_set(rows);
    }

    match shape {
        0 => isolated_mask(rows, selectivity),
        1 => fixed_run_mask(rng, rows, selectivity),
        2 => geometric_run_mask(rng, rows, selectivity),
        3 => bursty_mask(rng, rows, selectivity),
        4 => {
            BooleanBuffer::from_iter((0..rows).map(|_| rng.random_bool(selectivity as f64 / 100.0)))
        }
        5 => one_cluster_mask(rng, rows, selectivity),
        _ => unreachable!(),
    }
}

fn isolated_mask(rows: usize, selectivity: usize) -> BooleanBuffer {
    if selectivity <= 50 {
        let period = 100usize.div_ceil(selectivity).max(2);
        BooleanBuffer::from_iter((0..rows).map(|row| row % period == 0))
    } else {
        let period = 100usize.div_ceil(100 - selectivity).max(2);
        BooleanBuffer::from_iter((0..rows).map(|row| row % period != 0))
    }
}

fn fixed_run_mask(rng: &mut StdRng, rows: usize, selectivity: usize) -> BooleanBuffer {
    let scale = rng.random_range(1..=8);
    let selected_run = selectivity * scale;
    let skipped_run = (100 - selectivity) * scale;
    run_mask(rows, selected_run, skipped_run)
}

fn geometric_run_mask(rng: &mut StdRng, rows: usize, selectivity: usize) -> BooleanBuffer {
    let scale = rng.random_range(1..=16);
    let selected_mean = (selectivity * scale).max(1);
    let skipped_mean = ((100 - selectivity) * scale).max(1);
    let mut builder = BooleanBufferBuilder::new(rows);
    let mut remaining = rows;
    let mut selected = true;

    while remaining != 0 {
        let mean = if selected {
            selected_mean
        } else {
            skipped_mean
        };
        let run = sample_geometric_run(rng, mean).min(remaining);
        builder.append_n(run, selected);
        remaining -= run;
        selected = !selected;
    }
    builder.finish()
}

fn sample_geometric_run(rng: &mut StdRng, mean: usize) -> usize {
    let probability = 1.0 / mean as f64;
    let cap = mean.saturating_mul(8).max(1);
    let mut run = 1usize;
    while run < cap && !rng.random_bool(probability) {
        run += 1;
    }
    run
}

fn bursty_mask(rng: &mut StdRng, rows: usize, selectivity: usize) -> BooleanBuffer {
    let mut builder = BooleanBufferBuilder::new(rows);
    let mut remaining = rows;
    let mut high = true;
    let target = selectivity as f64 / 100.0;

    while remaining != 0 {
        let block = rng.random_range(64..=2_048).min(remaining);
        let probability = if high {
            (target * 1.8).min(1.0)
        } else {
            target * 0.2
        };
        for _ in 0..block {
            builder.append(rng.random_bool(probability));
        }
        remaining -= block;
        high = !high;
    }
    builder.finish()
}

fn one_cluster_mask(rng: &mut StdRng, rows: usize, selectivity: usize) -> BooleanBuffer {
    let selected = rows.saturating_mul(selectivity) / 100;
    let start = rng.random_range(0..=rows - selected);
    let mut builder = BooleanBufferBuilder::new(rows);
    builder.append_n(start, false);
    builder.append_n(selected, true);
    builder.append_n(rows - start - selected, false);
    builder.finish()
}

fn with_bit_offset(mask: BooleanBuffer, offset: usize) -> BooleanBuffer {
    if offset == 0 {
        return mask;
    }

    let len = mask.len();
    let mut builder = BooleanBufferBuilder::new(offset + len);
    builder.append_n(offset, false);
    builder.append_buffer(&mask);
    builder.finish().slice(offset, len)
}

fn random_split(rng: &mut StdRng, mask: &BooleanBuffer, filter_count: usize) -> Vec<BooleanArray> {
    let mut filters = Vec::with_capacity(filter_count);
    let mut offset = 0usize;

    for index in 0..filter_count {
        let remaining = mask.len() - offset;
        let len = if index + 1 == filter_count {
            remaining
        } else {
            rng.random_range(0..=remaining)
        };
        filters.push(BooleanArray::new(mask.slice(offset, len), None));
        offset += len;
    }

    filters
}
