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

//! Concepts used to choose how a [`RowSelection`] is executed.
//!
//! The row-filter reader makes two related but separate decisions:
//!
//! ```text
//! RowSelection materialization:
//!   RowSelectionPolicy::Auto  -->  Mask or Selectors
//!
//! Row-group execution:
//!   Predicate pushdown        -->  decode predicates, build RowSelection, decode output
//!   Post-filter               -->  decode output + predicates once, then filter
//! ```
//!
//! This module keeps the vocabulary for those decisions in one place. The
//! low-level cursors live in `selection.rs`; the push decoder cost model and
//! metrics use the summaries here to explain why a plan was chosen.

use super::RowSelection;

/// Fully resolved strategy for materializing [`RowSelection`] during execution.
///
/// This is determined from a combination of user preference (via
/// [`super::RowSelectionPolicy`]) and safety considerations (for example, page
/// pruning can force a sparse mask representation).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RowSelectionStrategy {
    /// Use a queue of [`super::RowSelector`] values.
    Selectors,
    /// Use a boolean mask to materialize the selection.
    Mask,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RowGroupExecutionMode {
    Pushdown(RowSelectionStrategy),
    PostFilter,
}

impl std::fmt::Display for RowGroupExecutionMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pushdown(RowSelectionStrategy::Mask) => f.write_str("Pushdown(Mask)"),
            Self::Pushdown(RowSelectionStrategy::Selectors) => f.write_str("Pushdown(Selectors)"),
            Self::PostFilter => f.write_str("PostFilter"),
        }
    }
}

/// Why a final row-selection read plan used masks or selectors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RowSelectionStrategyReason {
    /// The caller explicitly requested masks.
    ForcedMask,
    /// The caller explicitly requested selectors.
    ForcedSelectors,
    /// Auto chose masks because the selection has no non-empty selectors.
    AutoMaskEmptySelection,
    /// Auto chose masks because average selector length is below the threshold.
    AutoMaskShortRuns,
    /// Auto chose masks because selected rows are fragmented into many short runs.
    AutoMaskFragmentedSelection,
    /// Auto chose masks because most rows are selected and selector skipping is unlikely to pay off.
    AutoMaskHighSelectedRatio,
    /// Auto chose selectors because selected rows are clustered into long runs.
    AutoSelectorClusteredSelection,
    /// Auto chose selectors because average selector length reaches the threshold.
    AutoSelectorLongRuns,
}

/// Shape summary for a [`RowSelection`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct RowSelectionShape {
    pub(crate) selected_rows: usize,
    pub(crate) skipped_rows: usize,
    pub(crate) selector_count: usize,
    pub(crate) selected_run_count: usize,
    pub(crate) skipped_run_count: usize,
}

impl RowSelectionShape {
    pub(crate) fn from_selection(selection: Option<&RowSelection>) -> Self {
        let Some(selection) = selection else {
            return Self::default();
        };

        selection
            .iter()
            .fold(Self::default(), |mut shape, selector| {
                if selector.row_count == 0 {
                    return shape;
                }

                shape.selector_count += 1;
                if selector.skip {
                    shape.skipped_rows += selector.row_count;
                    shape.skipped_run_count += 1;
                } else {
                    shape.selected_rows += selector.row_count;
                    shape.selected_run_count += 1;
                }
                shape
            })
    }

    pub(crate) fn total_rows(self) -> usize {
        self.selected_rows + self.skipped_rows
    }

    pub(crate) fn selected_ratio(self) -> f64 {
        let total = self.total_rows();
        if total == 0 {
            0.0
        } else {
            self.selected_rows as f64 / total as f64
        }
    }

    pub(crate) fn run_density(self) -> f64 {
        let total = self.total_rows();
        if total == 0 {
            0.0
        } else {
            self.selector_count as f64 / total as f64
        }
    }

    pub(crate) fn average_selected_run_length(self) -> f64 {
        average_run_length(self.selected_rows, self.selected_run_count)
    }

    pub(crate) fn average_skipped_run_length(self) -> f64 {
        average_run_length(self.skipped_rows, self.skipped_run_count)
    }

    pub(crate) fn add_assign(&mut self, other: Self) {
        self.selected_rows += other.selected_rows;
        self.skipped_rows += other.skipped_rows;
        self.selector_count += other.selector_count;
        self.selected_run_count += other.selected_run_count;
        self.skipped_run_count += other.skipped_run_count;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CostModelDecisionReason {
    /// Predicate pushdown kept almost everything and did not produce useful pruning.
    HighSelectivityNoPruning,
    /// Fragmented runs with moderate selectivity often pay many small skip/read costs.
    FragmentedModerateSelectivity,
    /// Fragmented runs with high selectivity usually decode most rows plus pay pushdown overhead.
    FragmentedHighSelectivity,
    /// Not enough row groups have been observed to classify the scan.
    ObservationIncomplete,
    /// The observed shape still looks suitable for predicate pushdown.
    PushdownStillPreferred,
}

/// Aggregate row-selection shape observed while deciding whether Auto should
/// continue predicate pushdown or switch to post-filter execution.
///
/// The classifier looks for shapes where row-level pushdown is unlikely to
/// recover its own overhead:
///
/// ```text
/// no skipped rows                 -> predicate did not prune
/// tiny selected runs + many runs  -> fragmented skip/read pattern
/// high selected ratio             -> most output rows are decoded anyway
/// ```
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct CostModelObservation {
    pub(crate) observed_row_groups: usize,
    pub(crate) shape: RowSelectionShape,
}

impl CostModelObservation {
    pub(crate) const OBSERVATION_ROW_GROUPS: usize = 1;
    const FRAGMENTED_MODERATE_SELECTIVITY_MIN_RATIO: f64 = 0.08;

    pub(crate) fn trigger_reason(self) -> CostModelDecisionReason {
        if self.observed_row_groups < Self::OBSERVATION_ROW_GROUPS {
            return CostModelDecisionReason::ObservationIncomplete;
        }

        let shape = self.shape;
        if shape.total_rows() > 0 && shape.skipped_rows == 0 && shape.selected_ratio() >= 0.95 {
            return CostModelDecisionReason::HighSelectivityNoPruning;
        }

        let fragmented = shape.average_selected_run_length() <= 4.0 && shape.run_density() >= 0.01;

        if !fragmented {
            return CostModelDecisionReason::PushdownStillPreferred;
        }

        let selected_ratio = shape.selected_ratio();
        if (Self::FRAGMENTED_MODERATE_SELECTIVITY_MIN_RATIO..0.50).contains(&selected_ratio) {
            return CostModelDecisionReason::FragmentedModerateSelectivity;
        }
        if selected_ratio < 0.50 {
            return CostModelDecisionReason::PushdownStillPreferred;
        }

        CostModelDecisionReason::FragmentedHighSelectivity
    }

    pub(crate) fn prefers_post_filter(self) -> bool {
        matches!(
            self.trigger_reason(),
            CostModelDecisionReason::HighSelectivityNoPruning
                | CostModelDecisionReason::FragmentedModerateSelectivity
                | CostModelDecisionReason::FragmentedHighSelectivity
        )
    }
}

/// Fully resolved decision for materializing a [`RowSelection`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RowSelectionStrategyDecision {
    pub(crate) strategy: RowSelectionStrategy,
    pub(crate) reason: RowSelectionStrategyReason,
    pub(crate) shape: RowSelectionShape,
}

impl RowSelectionStrategyDecision {
    pub(crate) fn new(
        strategy: RowSelectionStrategy,
        reason: RowSelectionStrategyReason,
        shape: RowSelectionShape,
    ) -> Self {
        Self {
            strategy,
            reason,
            shape,
        }
    }

    pub(crate) fn with_shape(self, shape: RowSelectionShape) -> Self {
        Self { shape, ..self }
    }

    pub(crate) fn uses_mask(self) -> bool {
        matches!(self.strategy, RowSelectionStrategy::Mask)
    }
}

fn average_run_length(rows: usize, runs: usize) -> f64 {
    if runs == 0 {
        0.0
    } else {
        rows as f64 / runs as f64
    }
}
