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

//! [ArrowReaderMetrics] for collecting metrics about the Arrow reader

use crate::arrow::arrow_reader::selection::{
    CostModelDecisionReason, RowGroupExecutionMode, RowSelectionStrategyDecision,
    RowSelectionStrategyReason,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug)]
pub(crate) enum ArrowReaderPhase {
    PredicateRangePlanning,
    PredicateDecode,
    PredicateEvaluate,
    PredicateSelectionBuild,
    PredicateSelectionMerge,
    OutputRangePlanning,
    OutputSelectionResolve,
    OutputMaskFilter,
    PostFilterPredicateProject,
    PostFilterPredicateEvaluate,
    PostFilterApplyFilter,
    PostFilterOutputProject,
    PostSelectionApplyFilter,
}

impl ArrowReaderPhase {
    const COUNT: usize = 13;
    #[cfg(all(test, feature = "async"))]
    const ALL: [Self; Self::COUNT] = [
        Self::PredicateRangePlanning,
        Self::PredicateDecode,
        Self::PredicateEvaluate,
        Self::PredicateSelectionBuild,
        Self::PredicateSelectionMerge,
        Self::OutputRangePlanning,
        Self::OutputSelectionResolve,
        Self::OutputMaskFilter,
        Self::PostFilterPredicateProject,
        Self::PostFilterPredicateEvaluate,
        Self::PostFilterApplyFilter,
        Self::PostFilterOutputProject,
        Self::PostSelectionApplyFilter,
    ];

    fn index(self) -> usize {
        match self {
            Self::PredicateRangePlanning => 0,
            Self::PredicateDecode => 1,
            Self::PredicateEvaluate => 2,
            Self::PredicateSelectionBuild => 3,
            Self::PredicateSelectionMerge => 4,
            Self::OutputRangePlanning => 5,
            Self::OutputSelectionResolve => 6,
            Self::OutputMaskFilter => 7,
            Self::PostFilterPredicateProject => 8,
            Self::PostFilterPredicateEvaluate => 9,
            Self::PostFilterApplyFilter => 10,
            Self::PostFilterOutputProject => 11,
            Self::PostSelectionApplyFilter => 12,
        }
    }

    #[cfg(all(test, feature = "async"))]
    fn name(self) -> &'static str {
        match self {
            Self::PredicateRangePlanning => "predicate_range_planning",
            Self::PredicateDecode => "predicate_decode",
            Self::PredicateEvaluate => "predicate_evaluate",
            Self::PredicateSelectionBuild => "predicate_selection_build",
            Self::PredicateSelectionMerge => "predicate_selection_merge",
            Self::OutputRangePlanning => "output_range_planning",
            Self::OutputSelectionResolve => "output_selection_resolve",
            Self::OutputMaskFilter => "output_mask_filter",
            Self::PostFilterPredicateProject => "post_filter_predicate_project",
            Self::PostFilterPredicateEvaluate => "post_filter_predicate_evaluate",
            Self::PostFilterApplyFilter => "post_filter_apply_filter",
            Self::PostFilterOutputProject => "post_filter_output_project",
            Self::PostSelectionApplyFilter => "post_selection_apply_filter",
        }
    }
}

/// This enum represents the state of Arrow reader metrics collection.
///
/// The inner metrics are stored in an `Arc<ArrowReaderMetricsInner>`
/// so cloning the `ArrowReaderMetrics` enum will not clone the inner metrics.
///
/// To access metrics, create an `ArrowReaderMetrics` via [`ArrowReaderMetrics::enabled()`]
/// and configure the `ArrowReaderBuilder` with a clone.
#[derive(Debug, Clone)]
pub enum ArrowReaderMetrics {
    /// Metrics are not collected (default)
    Disabled,
    /// Metrics are collected and stored in an `Arc`.
    ///
    /// Create this via [`ArrowReaderMetrics::enabled()`].
    Enabled(Arc<ArrowReaderMetricsInner>),
}

impl ArrowReaderMetrics {
    /// Creates a new instance of [`ArrowReaderMetrics::Disabled`]
    pub fn disabled() -> Self {
        Self::Disabled
    }

    /// Creates a new instance of [`ArrowReaderMetrics::Enabled`]
    pub fn enabled() -> Self {
        Self::Enabled(Arc::new(ArrowReaderMetricsInner::new(false)))
    }

    #[cfg(all(test, feature = "async"))]
    pub(crate) fn enabled_with_phase_profile() -> Self {
        Self::Enabled(Arc::new(ArrowReaderMetricsInner::new(true)))
    }

    /// Predicate Cache: number of records read directly from the inner reader
    ///
    /// This is the total number of records read from the inner reader (that is
    /// actually decoding). It measures the amount of work that could not be
    /// avoided with caching.
    ///
    /// It returns the number of records read across all columns, so if you read
    /// 2 columns each with 100 records, this will return 200.
    ///
    ///
    /// Returns None if metrics are disabled.
    pub fn records_read_from_inner(&self) -> Option<usize> {
        match self {
            Self::Disabled => None,
            Self::Enabled(inner) => Some(
                inner
                    .records_read_from_inner
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
        }
    }

    /// Predicate Cache: number of records read from the cache
    ///
    /// This is the total number of records read from the cache actually
    /// decoding). It measures the amount of work that was avoided with caching.
    ///
    /// It returns the number of records read across all columns, so if you read
    /// 2 columns each with 100 records from the cache, this will return 200.
    ///
    /// Returns None if metrics are disabled.
    pub fn records_read_from_cache(&self) -> Option<usize> {
        match self {
            Self::Disabled => None,
            Self::Enabled(inner) => Some(inner.records_read_from_cache.load(Ordering::Relaxed)),
        }
    }

    /// Row Selection: number of selected rows recorded in planned selections
    pub fn row_selection_selected_rows(&self) -> Option<usize> {
        self.load(|inner| &inner.row_selection_selected_rows)
    }

    /// Row Selection: number of skipped rows recorded in planned selections
    pub fn row_selection_skipped_rows(&self) -> Option<usize> {
        self.load(|inner| &inner.row_selection_skipped_rows)
    }

    /// Row Selection: number of non-empty selectors recorded in planned selections
    pub fn row_selection_selector_count(&self) -> Option<usize> {
        self.load(|inner| &inner.row_selection_selector_count)
    }

    /// Row Selection: number of selected runs recorded in planned selections
    pub fn row_selection_selected_run_count(&self) -> Option<usize> {
        self.load(|inner| &inner.row_selection_selected_run_count)
    }

    /// Row Selection: number of skipped runs recorded in planned selections
    pub fn row_selection_skipped_run_count(&self) -> Option<usize> {
        self.load(|inner| &inner.row_selection_skipped_run_count)
    }

    /// Row Selection: number of plans using mask materialization
    pub fn row_selection_mask_plan_count(&self) -> Option<usize> {
        self.load(|inner| &inner.row_selection_mask_plan_count)
    }

    /// Row Selection: number of plans using selector materialization
    pub fn row_selection_selector_plan_count(&self) -> Option<usize> {
        self.load(|inner| &inner.row_selection_selector_plan_count)
    }

    /// Row Selection: number of plans forced to masks
    pub fn row_selection_forced_mask_plan_count(&self) -> Option<usize> {
        self.load(|inner| &inner.row_selection_forced_mask_plan_count)
    }

    /// Row Selection: number of plans forced to selectors
    pub fn row_selection_forced_selector_plan_count(&self) -> Option<usize> {
        self.load(|inner| &inner.row_selection_forced_selector_plan_count)
    }

    /// Row Selection: number of Auto plans choosing masks for empty selections
    pub fn row_selection_auto_mask_empty_plan_count(&self) -> Option<usize> {
        self.load(|inner| &inner.row_selection_auto_mask_empty_plan_count)
    }

    /// Row Selection: number of Auto plans choosing masks for short runs
    pub fn row_selection_auto_mask_short_run_plan_count(&self) -> Option<usize> {
        self.load(|inner| &inner.row_selection_auto_mask_short_run_plan_count)
    }

    /// Row Selection: number of Auto plans choosing masks for fragmented selected rows
    pub fn row_selection_auto_mask_fragmented_plan_count(&self) -> Option<usize> {
        self.load(|inner| &inner.row_selection_auto_mask_fragmented_plan_count)
    }

    /// Row Selection: number of Auto plans choosing masks for high selected-row ratio
    pub fn row_selection_auto_mask_high_ratio_plan_count(&self) -> Option<usize> {
        self.load(|inner| &inner.row_selection_auto_mask_high_ratio_plan_count)
    }

    /// Row Selection: number of Auto plans choosing selectors for clustered selected rows
    pub fn row_selection_auto_selector_clustered_plan_count(&self) -> Option<usize> {
        self.load(|inner| &inner.row_selection_auto_selector_clustered_plan_count)
    }

    /// Row Selection: number of Auto plans choosing selectors for long runs
    pub fn row_selection_auto_selector_long_run_plan_count(&self) -> Option<usize> {
        self.load(|inner| &inner.row_selection_auto_selector_long_run_plan_count)
    }

    /// Cost model: number of row groups included in the observation window
    pub fn cost_model_observed_row_group_count(&self) -> Option<usize> {
        self.load(|inner| &inner.cost_model_observed_row_group_count)
    }

    /// Cost model: number of row groups executed with pushdown
    pub fn cost_model_pushdown_row_group_count(&self) -> Option<usize> {
        self.load(|inner| &inner.cost_model_pushdown_row_group_count)
    }

    /// Cost model: number of row groups executed with post-filter
    pub fn cost_model_post_filter_row_group_count(&self) -> Option<usize> {
        self.load(|inner| &inner.cost_model_post_filter_row_group_count)
    }

    /// Cost model: number of incomplete observation-window decisions
    pub fn cost_model_observation_incomplete_count(&self) -> Option<usize> {
        self.load(|inner| &inner.cost_model_observation_incomplete_count)
    }

    /// Cost model: number of times pushdown remained preferred
    pub fn cost_model_pushdown_still_preferred_count(&self) -> Option<usize> {
        self.load(|inner| &inner.cost_model_pushdown_still_preferred_count)
    }

    /// Cost model: number of high-selectivity no-pruning triggers
    pub fn cost_model_high_selectivity_no_pruning_count(&self) -> Option<usize> {
        self.load(|inner| &inner.cost_model_high_selectivity_no_pruning_count)
    }

    /// Cost model: number of fragmented moderate-selectivity triggers
    pub fn cost_model_fragmented_moderate_selectivity_count(&self) -> Option<usize> {
        self.load(|inner| &inner.cost_model_fragmented_moderate_selectivity_count)
    }

    /// Cost model: number of fragmented high-selectivity triggers
    pub fn cost_model_fragmented_high_selectivity_count(&self) -> Option<usize> {
        self.load(|inner| &inner.cost_model_fragmented_high_selectivity_count)
    }

    /// Increments the count of records read from the inner reader
    pub(crate) fn increment_inner_reads(&self, count: usize) {
        let Self::Enabled(inner) = self else {
            return;
        };
        inner
            .records_read_from_inner
            .fetch_add(count, Ordering::Relaxed);
    }

    /// Increments the count of records read from the cache
    pub(crate) fn increment_cache_reads(&self, count: usize) {
        let Self::Enabled(inner) = self else {
            return;
        };

        inner
            .records_read_from_cache
            .fetch_add(count, Ordering::Relaxed);
    }

    pub(crate) fn record_row_selection(&self, decision: RowSelectionStrategyDecision) {
        let Self::Enabled(inner) = self else {
            return;
        };

        let shape = decision.shape;
        inner
            .row_selection_selected_rows
            .fetch_add(shape.selected_rows, Ordering::Relaxed);
        inner
            .row_selection_skipped_rows
            .fetch_add(shape.skipped_rows, Ordering::Relaxed);
        inner
            .row_selection_selector_count
            .fetch_add(shape.selector_count, Ordering::Relaxed);
        inner
            .row_selection_selected_run_count
            .fetch_add(shape.selected_run_count, Ordering::Relaxed);
        inner
            .row_selection_skipped_run_count
            .fetch_add(shape.skipped_run_count, Ordering::Relaxed);

        let strategy_count = if decision.uses_mask() {
            &inner.row_selection_mask_plan_count
        } else {
            &inner.row_selection_selector_plan_count
        };
        strategy_count.fetch_add(1, Ordering::Relaxed);

        let decision_count = match decision.reason {
            RowSelectionStrategyReason::ForcedMask => &inner.row_selection_forced_mask_plan_count,
            RowSelectionStrategyReason::ForcedSelectors => {
                &inner.row_selection_forced_selector_plan_count
            }
            RowSelectionStrategyReason::AutoMaskEmptySelection => {
                &inner.row_selection_auto_mask_empty_plan_count
            }
            RowSelectionStrategyReason::AutoMaskShortRuns => {
                &inner.row_selection_auto_mask_short_run_plan_count
            }
            RowSelectionStrategyReason::AutoMaskFragmentedSelection => {
                &inner.row_selection_auto_mask_fragmented_plan_count
            }
            RowSelectionStrategyReason::AutoMaskHighSelectedRatio => {
                &inner.row_selection_auto_mask_high_ratio_plan_count
            }
            RowSelectionStrategyReason::AutoSelectorClusteredSelection => {
                &inner.row_selection_auto_selector_clustered_plan_count
            }
            RowSelectionStrategyReason::AutoSelectorLongRuns => {
                &inner.row_selection_auto_selector_long_run_plan_count
            }
        };
        decision_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_cost_model_observed_row_group(&self) {
        let Self::Enabled(inner) = self else {
            return;
        };
        inner
            .cost_model_observed_row_group_count
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_cost_model_row_group(&self, mode: RowGroupExecutionMode) {
        let Self::Enabled(inner) = self else {
            return;
        };

        let counter = match mode {
            RowGroupExecutionMode::Pushdown(_) => &inner.cost_model_pushdown_row_group_count,
            RowGroupExecutionMode::PostFilter => &inner.cost_model_post_filter_row_group_count,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_cost_model_trigger(&self, reason: CostModelDecisionReason) {
        let Self::Enabled(inner) = self else {
            return;
        };

        let counter = match reason {
            CostModelDecisionReason::HighSelectivityNoPruning => {
                &inner.cost_model_high_selectivity_no_pruning_count
            }
            CostModelDecisionReason::FragmentedModerateSelectivity => {
                &inner.cost_model_fragmented_moderate_selectivity_count
            }
            CostModelDecisionReason::FragmentedHighSelectivity => {
                &inner.cost_model_fragmented_high_selectivity_count
            }
            CostModelDecisionReason::ObservationIncomplete => {
                &inner.cost_model_observation_incomplete_count
            }
            CostModelDecisionReason::PushdownStillPreferred => {
                &inner.cost_model_pushdown_still_preferred_count
            }
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn time_phase<T>(&self, phase: ArrowReaderPhase, f: impl FnOnce() -> T) -> T {
        let Self::Enabled(inner) = self else {
            return f();
        };
        if !inner.phase_profile_enabled {
            return f();
        }

        let start = Instant::now();
        let result = f();
        inner.record_phase(phase, start.elapsed());
        result
    }

    #[cfg(all(test, feature = "async"))]
    pub(crate) fn phase_profile_report(&self) -> Option<String> {
        let Self::Enabled(inner) = self else {
            return None;
        };
        if !inner.phase_profile_enabled {
            return None;
        }

        let mut lines = vec!["phase,total_ms,count,avg_us".to_string()];
        for phase in ArrowReaderPhase::ALL {
            let idx = phase.index();
            let total_ns = inner.phase_ns[idx].load(Ordering::Relaxed);
            let count = inner.phase_counts[idx].load(Ordering::Relaxed);
            if count == 0 {
                continue;
            }

            let total_ms = total_ns as f64 / 1_000_000.0;
            let avg_us = total_ns as f64 / count as f64 / 1_000.0;
            lines.push(format!(
                "{},{total_ms:.3},{count},{avg_us:.3}",
                phase.name()
            ));
        }
        Some(lines.join("\n"))
    }

    fn load(&self, metric: fn(&ArrowReaderMetricsInner) -> &AtomicUsize) -> Option<usize> {
        match self {
            Self::Disabled => None,
            Self::Enabled(inner) => Some(metric(inner).load(Ordering::Relaxed)),
        }
    }
}

/// Holds the actual metrics for the Arrow reader.
///
/// Please see [`ArrowReaderMetrics`] for the public interface.
#[derive(Debug)]
pub struct ArrowReaderMetricsInner {
    // Metrics for Predicate Cache
    /// Total number of records read from the inner reader (uncached)
    records_read_from_inner: AtomicUsize,
    /// Total number of records read from previously cached pages
    records_read_from_cache: AtomicUsize,
    /// Total selected rows in planned row selections
    row_selection_selected_rows: AtomicUsize,
    /// Total skipped rows in planned row selections
    row_selection_skipped_rows: AtomicUsize,
    /// Total non-empty selectors in planned row selections
    row_selection_selector_count: AtomicUsize,
    /// Total selected runs in planned row selections
    row_selection_selected_run_count: AtomicUsize,
    /// Total skipped runs in planned row selections
    row_selection_skipped_run_count: AtomicUsize,
    /// Number of plans materialized with masks
    row_selection_mask_plan_count: AtomicUsize,
    /// Number of plans materialized with selectors
    row_selection_selector_plan_count: AtomicUsize,
    /// Number of plans forced to masks
    row_selection_forced_mask_plan_count: AtomicUsize,
    /// Number of plans forced to selectors
    row_selection_forced_selector_plan_count: AtomicUsize,
    /// Number of Auto plans choosing masks for empty selections
    row_selection_auto_mask_empty_plan_count: AtomicUsize,
    /// Number of Auto plans choosing masks for short runs
    row_selection_auto_mask_short_run_plan_count: AtomicUsize,
    /// Number of Auto plans using masks for fragmented selected rows
    row_selection_auto_mask_fragmented_plan_count: AtomicUsize,
    /// Number of Auto plans using masks for high selected-row ratio
    row_selection_auto_mask_high_ratio_plan_count: AtomicUsize,
    /// Number of Auto plans using selectors for clustered selected rows
    row_selection_auto_selector_clustered_plan_count: AtomicUsize,
    /// Number of Auto plans choosing selectors for long runs
    row_selection_auto_selector_long_run_plan_count: AtomicUsize,
    /// Number of row groups included in cost-model observation
    cost_model_observed_row_group_count: AtomicUsize,
    /// Number of cost-model eligible row groups executed with pushdown
    cost_model_pushdown_row_group_count: AtomicUsize,
    /// Number of row groups executed with post-filter
    cost_model_post_filter_row_group_count: AtomicUsize,
    /// Number of incomplete cost-model observations
    cost_model_observation_incomplete_count: AtomicUsize,
    /// Number of cost-model decisions that kept pushdown
    cost_model_pushdown_still_preferred_count: AtomicUsize,
    /// Number of high-selectivity no-pruning cost-model triggers
    cost_model_high_selectivity_no_pruning_count: AtomicUsize,
    /// Number of fragmented moderate-selectivity cost-model triggers
    cost_model_fragmented_moderate_selectivity_count: AtomicUsize,
    /// Number of fragmented high-selectivity cost-model triggers
    cost_model_fragmented_high_selectivity_count: AtomicUsize,
    phase_profile_enabled: bool,
    phase_ns: [AtomicU64; ArrowReaderPhase::COUNT],
    phase_counts: [AtomicUsize; ArrowReaderPhase::COUNT],
}

impl ArrowReaderMetricsInner {
    /// Creates a new instance of `ArrowReaderMetricsInner`
    pub(crate) fn new(phase_profile_enabled: bool) -> Self {
        Self {
            records_read_from_inner: AtomicUsize::new(0),
            records_read_from_cache: AtomicUsize::new(0),
            row_selection_selected_rows: AtomicUsize::new(0),
            row_selection_skipped_rows: AtomicUsize::new(0),
            row_selection_selector_count: AtomicUsize::new(0),
            row_selection_selected_run_count: AtomicUsize::new(0),
            row_selection_skipped_run_count: AtomicUsize::new(0),
            row_selection_mask_plan_count: AtomicUsize::new(0),
            row_selection_selector_plan_count: AtomicUsize::new(0),
            row_selection_forced_mask_plan_count: AtomicUsize::new(0),
            row_selection_forced_selector_plan_count: AtomicUsize::new(0),
            row_selection_auto_mask_empty_plan_count: AtomicUsize::new(0),
            row_selection_auto_mask_short_run_plan_count: AtomicUsize::new(0),
            row_selection_auto_mask_fragmented_plan_count: AtomicUsize::new(0),
            row_selection_auto_mask_high_ratio_plan_count: AtomicUsize::new(0),
            row_selection_auto_selector_clustered_plan_count: AtomicUsize::new(0),
            row_selection_auto_selector_long_run_plan_count: AtomicUsize::new(0),
            cost_model_observed_row_group_count: AtomicUsize::new(0),
            cost_model_pushdown_row_group_count: AtomicUsize::new(0),
            cost_model_post_filter_row_group_count: AtomicUsize::new(0),
            cost_model_observation_incomplete_count: AtomicUsize::new(0),
            cost_model_pushdown_still_preferred_count: AtomicUsize::new(0),
            cost_model_high_selectivity_no_pruning_count: AtomicUsize::new(0),
            cost_model_fragmented_moderate_selectivity_count: AtomicUsize::new(0),
            cost_model_fragmented_high_selectivity_count: AtomicUsize::new(0),
            phase_profile_enabled,
            phase_ns: std::array::from_fn(|_| AtomicU64::new(0)),
            phase_counts: std::array::from_fn(|_| AtomicUsize::new(0)),
        }
    }

    fn record_phase(&self, phase: ArrowReaderPhase, duration: Duration) {
        let idx = phase.index();
        self.phase_ns[idx].fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
        self.phase_counts[idx].fetch_add(1, Ordering::Relaxed);
    }
}
