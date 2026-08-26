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

const MAX_RANDOM_ROWS: usize = 65_536;
const THRESHOLDS: &[usize] = &[0, 1, 8, 16, 31, 32, 33, 64];
const SELECTIVITIES: &[usize] = &[0, 1, 5, 15, 50, 90, 99, 100];

#[test]
fn auto_construction_preserves_global_runs_and_threshold_boundary() {
    let filters = vec![
        BooleanArray::from(vec![false, true, true]),
        BooleanArray::from(Vec::<bool>::new()),
        BooleanArray::from(vec![true, true, false]),
        BooleanArray::from(vec![false, false, true]),
    ];

    for threshold in THRESHOLDS {
        assert_auto_equivalent(&filters, *threshold, "cross-filter run merge");
    }

    let run31_filters = split_evenly(&run_mask(65_536, 31, 31), 8_192);
    let run32_filters = split_evenly(&run_mask(65_536, 32, 32), 8_192);

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

    assert_auto_equivalent(&[], 0, "no filters");
    assert_auto_equivalent(&[BooleanArray::from(Vec::<bool>::new())], 0, "empty filter");
}

#[test]
fn auto_construction_randomized_equivalence() {
    let mut rng = StdRng::seed_from_u64(0x1077_6000_5eed);
    let edge_rows = [0, 1, 7, 8, 31, 32, 33, MAX_RANDOM_ROWS];
    let mut case_idx = 0usize;

    for &threshold in THRESHOLDS {
        for &selectivity in SELECTIVITIES {
            for shape in 0..4 {
                for with_offset in [false, true] {
                    let rows = edge_rows
                        .get(case_idx)
                        .copied()
                        .unwrap_or_else(|| rng.random_range(0..=MAX_RANDOM_ROWS));
                    let mask = random_shape(&mut rng, rows, selectivity, shape);
                    let bit_offset = if with_offset {
                        rng.random_range(1..=63)
                    } else {
                        0
                    };
                    let mask = with_bit_offset(mask, bit_offset);
                    let filter_count = rng.random_range(1..=32);
                    let filters = random_split(&mut rng, &mask, filter_count);
                    let context = format!(
                        "case={case_idx} rows={rows} selectivity={selectivity} shape={shape} \
                         filters={filter_count} threshold={threshold} bit_offset={bit_offset}"
                    );

                    assert_auto_equivalent(&filters, threshold, &context);
                    case_idx += 1;
                }
            }
        }
    }
}

fn assert_auto_equivalent(filters: &[BooleanArray], threshold: usize, context: &str) {
    let reference = RowSelection::from_filters(filters);
    let reference_strategy = reference.auto_selection_strategy(threshold);
    let auto_built = RowSelection::from_filters_auto(filters, threshold);
    let auto_built_strategy = auto_built.auto_selection_strategy(threshold);
    let auto_built_backing = match &auto_built.inner {
        RowSelectionInner::Mask(_) => RowSelectionStrategy::Mask,
        RowSelectionInner::Selectors(_) => RowSelectionStrategy::Selectors,
    };

    assert_eq!(reference_strategy, auto_built_backing, "backing: {context}");
    assert_eq!(
        reference_strategy, auto_built_strategy,
        "strategy: {context}"
    );
    assert_eq!(reference, auto_built, "logical selection: {context}");
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

fn random_shape(rng: &mut StdRng, rows: usize, selectivity: usize, shape: usize) -> BooleanBuffer {
    if selectivity == 0 {
        return BooleanBuffer::new_unset(rows);
    }
    if selectivity == 100 {
        return BooleanBuffer::new_set(rows);
    }

    match shape {
        0 => isolated_mask(rows, selectivity),
        1 => {
            let scale = rng.random_range(1..=8);
            run_mask(rows, selectivity * scale, (100 - selectivity) * scale)
        }
        2 => {
            BooleanBuffer::from_iter((0..rows).map(|_| rng.random_bool(selectivity as f64 / 100.0)))
        }
        3 => one_cluster_mask(rng, rows, selectivity),
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
