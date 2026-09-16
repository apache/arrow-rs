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

use super::model::{RowGroupPattern, SelectionRun};

pub(crate) const SPARSE_1_56_RUN32: &[SelectionRun] =
    &[SelectionRun::skip(2_016), SelectionRun::select(32)];

pub(crate) const MODERATE_12_5_RUN32: &[SelectionRun] =
    &[SelectionRun::skip(224), SelectionRun::select(32)];

pub(crate) const FRAGMENTED_50_RUN1: &[SelectionRun] =
    &[SelectionRun::skip(1), SelectionRun::select(1)];

pub(crate) const CLUSTERED_50_RUN128: &[SelectionRun] =
    &[SelectionRun::skip(128), SelectionRun::select(128)];

pub(crate) const REGULAR_50_RUN32: &[SelectionRun] =
    &[SelectionRun::skip(32), SelectionRun::select(32)];

pub(crate) const DENSE_98_44_SKIP1_SELECT63: &[SelectionRun] =
    &[SelectionRun::skip(1), SelectionRun::select(63)];

/// Has the same selectivity, run density, and mean selected/skipped run lengths
/// as [`REGULAR_50_RUN32`], but a different run variance and ordering.
pub(crate) const BURSTY_50_SAME_SUMMARY: &[SelectionRun] = &[
    SelectionRun::skip(1),
    SelectionRun::select(1),
    SelectionRun::skip(1),
    SelectionRun::select(1),
    SelectionRun::skip(1),
    SelectionRun::select(1),
    SelectionRun::skip(125),
    SelectionRun::select(125),
];

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ShapeSummary {
    selected_rows: usize,
    skipped_rows: usize,
    selector_count: usize,
    selected_run_count: usize,
    skipped_run_count: usize,
}

impl ShapeSummary {
    fn from_cycle(cycle: &[SelectionRun]) -> Self {
        cycle.iter().fold(Self::default(), |mut summary, run| {
            assert!(run.len > 0, "selection run must be non-empty");
            summary.selector_count += 1;
            if run.selected {
                summary.selected_rows += run.len;
                summary.selected_run_count += 1;
            } else {
                summary.skipped_rows += run.len;
                summary.skipped_run_count += 1;
            }
            summary
        })
    }

    fn total_rows(self) -> usize {
        self.selected_rows + self.skipped_rows
    }

    fn selected_ratio(self) -> f64 {
        self.selected_rows as f64 / self.total_rows() as f64
    }

    fn run_density(self) -> f64 {
        self.selector_count as f64 / self.total_rows() as f64
    }

    fn average_selected_run(self) -> f64 {
        self.selected_rows as f64 / self.selected_run_count as f64
    }

    fn average_skipped_run(self) -> f64 {
        self.skipped_rows as f64 / self.skipped_run_count as f64
    }
}

pub(crate) fn assert_shape_contracts() {
    let regular = ShapeSummary::from_cycle(REGULAR_50_RUN32);
    let bursty = ShapeSummary::from_cycle(BURSTY_50_SAME_SUMMARY);

    assert_eq!(regular.selected_ratio(), bursty.selected_ratio());
    assert_eq!(regular.run_density(), bursty.run_density());
    assert_eq!(
        regular.average_selected_run(),
        bursty.average_selected_run()
    );
    assert_eq!(regular.average_skipped_run(), bursty.average_skipped_run());

    let dense = ShapeSummary::from_cycle(DENSE_98_44_SKIP1_SELECT63);
    assert_eq!(dense.selected_rows, 63);
    assert_eq!(dense.skipped_rows, 1);
    assert_eq!(dense.selector_count, 2);
    assert_eq!(dense.average_selected_run(), 63.0);
    assert_eq!(dense.average_skipped_run(), 1.0);
}

pub(crate) fn expand_pattern(pattern: RowGroupPattern, row_count: usize) -> Vec<i32> {
    match pattern {
        RowGroupPattern::AllSelected => vec![1; row_count],
        RowGroupPattern::Cycle(cycle) => {
            assert!(!cycle.is_empty(), "selection cycle must not be empty");
            let summary = ShapeSummary::from_cycle(cycle);
            assert_eq!(
                row_count % summary.total_rows(),
                0,
                "row group size must be divisible by cycle size"
            );

            let mut values = Vec::with_capacity(row_count);
            while values.len() < row_count {
                for run in cycle {
                    values.extend(std::iter::repeat_n(i32::from(run.selected), run.len));
                }
            }
            assert_eq!(values.len(), row_count);
            values
        }
    }
}

pub(crate) fn selected_rows(pattern: RowGroupPattern, row_count: usize) -> usize {
    match pattern {
        RowGroupPattern::AllSelected => row_count,
        RowGroupPattern::Cycle(cycle) => {
            let summary = ShapeSummary::from_cycle(cycle);
            assert_eq!(row_count % summary.total_rows(), 0);
            summary.selected_rows * (row_count / summary.total_rows())
        }
    }
}
