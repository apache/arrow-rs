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

//! [`ReadPlan`] and [`ReadPlanBuilder`] for determining which rows to read
//! from a Parquet file

use crate::arrow::array_reader::ArrayReader;
use crate::arrow::arrow_reader::metrics::{ArrowReaderMetrics, ArrowReaderPhase};
use crate::arrow::arrow_reader::selection::{
    LoadedRowRanges, RowSelectionPolicy, RowSelectionShape, RowSelectionStrategy,
    RowSelectionStrategyDecision, RowSelectionStrategyReason,
};
use crate::arrow::arrow_reader::{
    ArrowPredicate, ParquetRecordBatchReader, RowSelection, RowSelectionCursor, RowSelector,
};
use crate::errors::{ParquetError, Result};
use arrow_array::{Array, BooleanArray};
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder};
use arrow_select::filter::prep_null_mask_filter;
use std::collections::VecDeque;

const HIGH_SELECTED_RATIO_NUMERATOR: usize = 7;
const HIGH_SELECTED_RATIO_DENOMINATOR: usize = 8;
const FRAGMENTED_SELECTED_RUN_LIMIT: usize = 4;
const CLUSTERED_SELECTED_RUN_MULTIPLIER: usize = 4;
const CLUSTERED_SKIPPED_RUN_MULTIPLIER: usize = 4;

/// Options for [`ReadPlanBuilder::with_predicate_options`].
pub struct PredicateOptions<'a> {
    array_reader: Box<dyn ArrayReader>,
    predicate: &'a mut dyn ArrowPredicate,
    limit: Option<usize>,
    total_rows: usize,
    metrics: ArrowReaderMetrics,
}

impl<'a> PredicateOptions<'a> {
    /// Create options for evaluating `predicate` against rows produced by
    /// `array_reader`.
    ///
    /// By default there is no match-count limit; the predicate is evaluated
    /// over every row the reader yields. Use [`Self::with_limit`] to enable
    /// early termination.
    pub fn new(array_reader: Box<dyn ArrayReader>, predicate: &'a mut dyn ArrowPredicate) -> Self {
        Self {
            array_reader,
            predicate,
            limit: None,
            total_rows: 0,
            metrics: ArrowReaderMetrics::disabled(),
        }
    }

    /// Stop scanning `array_reader` once `limit` matches have accumulated.
    ///
    /// Performance optimization for `LIMIT` / TopK: when the cumulative
    /// `true_count` reaches `limit`, the current filter batch is truncated
    /// at the `limit`-th match and remaining batches are never decoded.
    ///
    /// `limit` counts predicate matches, not output rows — callers applying
    /// an offset must pass `offset + limit`.
    ///
    /// `total_rows` is the row count `array_reader` would yield if iterated
    /// to completion. It is used to pad un-evaluated trailing rows as "not
    /// selected" so the returned [`RowSelection`] covers the full row group.
    ///
    /// Only valid for the *last* predicate in a filter chain: intermediate
    /// predicates' match counts do not map 1:1 to output rows.
    pub fn with_limit(mut self, limit: usize, total_rows: usize) -> Self {
        self.limit = Some(limit);
        self.total_rows = total_rows;
        self
    }

    pub(crate) fn with_metrics(mut self, metrics: ArrowReaderMetrics) -> Self {
        self.metrics = metrics;
        self
    }
}

/// A builder for [`ReadPlan`]
#[derive(Clone, Debug)]
pub struct ReadPlanBuilder {
    batch_size: usize,
    /// Which rows to select. Includes the result of all filters applied so far
    selection: Option<RowSelection>,
    /// Policy to use when materializing the row selection
    row_selection_policy: RowSelectionPolicy,
    /// Row ranges already loaded by page pruning
    loaded_row_ranges: Option<LoadedRowRanges>,
}

impl ReadPlanBuilder {
    /// Create a `ReadPlanBuilder` with the given batch size
    pub fn new(batch_size: usize) -> Self {
        Self {
            batch_size,
            selection: None,
            row_selection_policy: RowSelectionPolicy::default(),
            loaded_row_ranges: None,
        }
    }

    /// Set the current selection to the given value
    pub fn with_selection(mut self, selection: Option<RowSelection>) -> Self {
        self.selection = selection;
        self
    }

    /// Configure the policy to use when materialising the [`RowSelection`]
    ///
    /// Defaults to [`RowSelectionPolicy::Auto`]
    pub fn with_row_selection_policy(mut self, policy: RowSelectionPolicy) -> Self {
        self.row_selection_policy = policy;
        self
    }

    pub(crate) fn with_loaded_row_ranges(mut self, loaded: Option<LoadedRowRanges>) -> Self {
        self.loaded_row_ranges = loaded;
        self
    }

    /// Returns the current row selection policy
    pub fn row_selection_policy(&self) -> &RowSelectionPolicy {
        &self.row_selection_policy
    }

    /// Returns the current selection, if any
    pub fn selection(&self) -> Option<&RowSelection> {
        self.selection.as_ref()
    }

    /// Specifies the number of rows in the row group, before filtering is applied.
    ///
    /// Returns a [`LimitedReadPlanBuilder`] that can apply
    /// offset and limit.
    ///
    /// Call [`LimitedReadPlanBuilder::build_limited`] to apply the limits to this
    /// selection.
    pub(crate) fn limited(self, row_count: usize) -> LimitedReadPlanBuilder {
        LimitedReadPlanBuilder::new(self, row_count)
    }

    /// Returns true if the current plan selects any rows
    pub fn selects_any(&self) -> bool {
        self.selection
            .as_ref()
            .map(|s| s.selects_any())
            .unwrap_or(true)
    }

    /// Returns the number of rows selected, or `None` if all rows are selected.
    pub fn num_rows_selected(&self) -> Option<usize> {
        self.selection.as_ref().map(|s| s.row_count())
    }

    /// Returns the [`RowSelectionStrategy`] for this plan.
    ///
    /// Guarantees to return either `Selectors` or `Mask`, never `Auto`.
    #[cfg(test)]
    pub(crate) fn resolve_selection_strategy(&self) -> RowSelectionStrategy {
        self.resolve_selection_strategy_decision().strategy
    }

    pub(crate) fn resolve_selection_strategy_decision(&self) -> RowSelectionStrategyDecision {
        let shape = RowSelectionShape::from_selection(self.selection.as_ref());

        match self.row_selection_policy {
            RowSelectionPolicy::Selectors => RowSelectionStrategyDecision::new(
                RowSelectionStrategy::Selectors,
                RowSelectionStrategyReason::ForcedSelectors,
                shape,
            ),
            RowSelectionPolicy::Mask => RowSelectionStrategyDecision::new(
                RowSelectionStrategy::Mask,
                RowSelectionStrategyReason::ForcedMask,
                shape,
            ),
            RowSelectionPolicy::Auto { threshold, .. } => {
                if self.selection.is_none() {
                    return RowSelectionStrategyDecision::new(
                        RowSelectionStrategy::Selectors,
                        RowSelectionStrategyReason::AutoSelectorLongRuns,
                        shape,
                    );
                }

                resolve_auto_selection_strategy(threshold, shape)
            }
        }
    }

    /// Evaluates an [`ArrowPredicate`], updating this plan's `selection`
    ///
    /// If the current `selection` is `Some`, the resulting [`RowSelection`]
    /// will be the conjunction of the existing selection and the rows selected
    /// by `predicate`.
    ///
    /// Note: pre-existing selections may come from evaluating a previous predicate
    /// or if the [`ParquetRecordBatchReader`] specified an explicit
    /// [`RowSelection`] in addition to one or more predicates.
    pub fn with_predicate(
        self,
        array_reader: Box<dyn ArrayReader>,
        predicate: &mut dyn ArrowPredicate,
    ) -> Result<Self> {
        self.with_predicate_options(PredicateOptions::new(array_reader, predicate))
    }

    /// Evaluates an [`ArrowPredicate`] with the given [`PredicateOptions`],
    /// updating this plan's `selection`.
    ///
    /// Like [`Self::with_predicate`], but allows additional options such as a
    /// match-count limit for early termination (see
    /// [`PredicateOptions::with_limit`]).
    pub fn with_predicate_options(mut self, options: PredicateOptions<'_>) -> Result<Self> {
        let PredicateOptions {
            array_reader,
            predicate,
            limit,
            total_rows,
            metrics,
        } = options;

        // Target length for the concatenated filter output:
        // - Prior selection ⇒ the reader yields that many rows; `and_then`
        //   below requires the filter output to match.
        // - No prior selection ⇒ the reader yields `total_rows`. We only
        //   need to pad when `limit` may short-circuit the loop; otherwise
        //   iteration naturally exhausts.
        let expected_rows = match self.selection.as_ref() {
            Some(s) => Some(s.row_count()),
            None => limit.map(|_| total_rows),
        };

        let mut reader = ParquetRecordBatchReader::new(array_reader, self.clone().build());
        let mut filters = vec![];
        let mut processed_rows: usize = 0;
        let mut matched_rows: usize = 0;
        loop {
            let maybe_batch =
                metrics.time_phase(ArrowReaderPhase::PredicateDecode, || reader.next());
            let Some(maybe_batch) = maybe_batch else {
                break;
            };
            let maybe_batch = maybe_batch?;
            let input_rows = maybe_batch.num_rows();
            let filter = metrics.time_phase(ArrowReaderPhase::PredicateEvaluate, || {
                predicate.evaluate(maybe_batch)
            })?;
            // Since user supplied predicate, check error here to catch bugs quickly
            if filter.len() != input_rows {
                return Err(arrow_err!(
                    "ArrowPredicate predicate returned {} rows, expected {input_rows}",
                    filter.len()
                ));
            }
            let filter = match filter.null_count() {
                0 => filter,
                _ => prep_null_mask_filter(&filter),
            };

            processed_rows += input_rows;

            match limit {
                Some(limit) if matched_rows + filter.true_count() >= limit => {
                    let needed = limit - matched_rows;
                    let truncated = truncate_filter_after_n_trues(filter, needed);
                    filters.push(truncated);
                    break;
                }
                _ => {
                    matched_rows += filter.true_count();
                    filters.push(filter);
                }
            }
        }

        // Pad the tail so the filters cover `expected_rows` total. This keeps
        // the invariant that the resulting `RowSelection` spans every row the
        // reader would have produced — rows past the early break are marked
        // "not selected". When no limit is set the loop always exhausts and
        // no padding is needed.
        if let Some(expected) = expected_rows {
            if processed_rows < expected {
                let pad_len = expected - processed_rows;
                filters.push(BooleanArray::new(BooleanBuffer::new_unset(pad_len), None));
            }
        }

        // If the predicate selected all rows and there is no prior selection,
        // skip creating a RowSelection entirely — this avoids the allocation
        // and keeps selection as None which enables coalesced page fetches.
        let all_selected = filters.iter().all(|f| f.true_count() == f.len());
        if all_selected && self.selection.is_none() {
            return Ok(self);
        }
        let raw = metrics.time_phase(ArrowReaderPhase::PredicateSelectionBuild, || {
            RowSelection::from_filters(&filters)
        });
        self.selection = match self.selection.take() {
            Some(selection) => Some(
                metrics.time_phase(ArrowReaderPhase::PredicateSelectionMerge, || {
                    selection.and_then(&raw)
                }),
            ),
            None => Some(raw),
        };
        Ok(self)
    }

    /// Create a final `ReadPlan` the read plan for the scan
    pub fn build(mut self) -> ReadPlan {
        // If selection is empty, truncate
        if !self.selects_any() {
            self.selection = Some(RowSelection::from(vec![]));
        }

        self.build_with_metrics(&ArrowReaderMetrics::disabled())
    }

    /// Create a final `ReadPlan` and record row-selection planning metrics.
    pub(crate) fn build_with_metrics(mut self, metrics: &ArrowReaderMetrics) -> ReadPlan {
        // If selection is empty, truncate
        if !self.selects_any() {
            self.selection = Some(RowSelection::from(vec![]));
        }

        // Preferred strategy must not be Auto
        let selection_strategy_decision = self.resolve_selection_strategy_decision();
        let selection_strategy = selection_strategy_decision.strategy;

        let Self {
            batch_size,
            selection,
            row_selection_policy: _,
            loaded_row_ranges,
        } = self;

        let selection = selection.map(|s| s.trim());
        if matches!(metrics, ArrowReaderMetrics::Enabled(_)) && selection.is_some() {
            let shape = RowSelectionShape::from_selection(selection.as_ref());
            metrics.record_row_selection(selection_strategy_decision.with_shape(shape));
        }

        let row_selection_cursor = selection
            .map(|s| {
                let selectors: Vec<RowSelector> = s.into();
                match selection_strategy {
                    RowSelectionStrategy::Mask => match loaded_row_ranges {
                        Some(loaded) => {
                            RowSelectionCursor::new_sparse_mask_from_selectors(selectors, loaded)
                        }
                        None => RowSelectionCursor::new_mask_from_selectors(selectors),
                    },
                    RowSelectionStrategy::Selectors => RowSelectionCursor::new_selectors(selectors),
                }
            })
            .unwrap_or(RowSelectionCursor::new_all());

        ReadPlan {
            batch_size,
            row_selection_cursor,
        }
    }
}

fn resolve_auto_selection_strategy(
    threshold: usize,
    shape: RowSelectionShape,
) -> RowSelectionStrategyDecision {
    if shape.selector_count == 0 || shape.selected_rows == 0 {
        return RowSelectionStrategyDecision::new(
            RowSelectionStrategy::Mask,
            RowSelectionStrategyReason::AutoMaskEmptySelection,
            shape,
        );
    }

    if clustered_selection_at_or_above_threshold(shape, threshold) {
        return RowSelectionStrategyDecision::new(
            RowSelectionStrategy::Selectors,
            RowSelectionStrategyReason::AutoSelectorClusteredSelection,
            shape,
        );
    }

    if shape.skipped_rows > 0
        && selected_ratio_at_least(
            shape,
            HIGH_SELECTED_RATIO_NUMERATOR,
            HIGH_SELECTED_RATIO_DENOMINATOR,
        )
    {
        return RowSelectionStrategyDecision::new(
            RowSelectionStrategy::Mask,
            RowSelectionStrategyReason::AutoMaskHighSelectedRatio,
            shape,
        );
    }

    if shape.selected_run_count > 1
        && shape.average_selected_run_length() <= FRAGMENTED_SELECTED_RUN_LIMIT as f64
        && selection_density_at_or_above_threshold(shape, threshold)
    {
        return RowSelectionStrategyDecision::new(
            RowSelectionStrategy::Mask,
            RowSelectionStrategyReason::AutoMaskFragmentedSelection,
            shape,
        );
    }

    if shape.selected_run_count > 0
        && shape.average_selected_run_length()
            >= threshold.saturating_mul(CLUSTERED_SELECTED_RUN_MULTIPLIER) as f64
        && shape.average_skipped_run_length() > 0.0
        && shape.selected_ratio() <= 0.5
    {
        return RowSelectionStrategyDecision::new(
            RowSelectionStrategy::Selectors,
            RowSelectionStrategyReason::AutoSelectorClusteredSelection,
            shape,
        );
    }

    if shape.total_rows() < shape.selector_count.saturating_mul(threshold) {
        RowSelectionStrategyDecision::new(
            RowSelectionStrategy::Mask,
            RowSelectionStrategyReason::AutoMaskShortRuns,
            shape,
        )
    } else {
        RowSelectionStrategyDecision::new(
            RowSelectionStrategy::Selectors,
            RowSelectionStrategyReason::AutoSelectorLongRuns,
            shape,
        )
    }
}

fn selected_ratio_at_least(shape: RowSelectionShape, numerator: usize, denominator: usize) -> bool {
    (shape.selected_rows as u128) * (denominator as u128)
        >= (shape.total_rows() as u128) * (numerator as u128)
}

fn selection_density_at_or_above_threshold(shape: RowSelectionShape, threshold: usize) -> bool {
    (shape.total_rows() as u128) <= (shape.selector_count as u128) * (threshold as u128)
}

fn clustered_selection_at_or_above_threshold(shape: RowSelectionShape, threshold: usize) -> bool {
    average_run_length_at_least(
        shape.selected_rows,
        shape.selected_run_count,
        threshold,
        CLUSTERED_SELECTED_RUN_MULTIPLIER,
    ) && average_run_length_at_least(
        shape.skipped_rows,
        shape.skipped_run_count,
        threshold,
        CLUSTERED_SKIPPED_RUN_MULTIPLIER,
    )
}

fn average_run_length_at_least(
    rows: usize,
    runs: usize,
    threshold: usize,
    multiplier: usize,
) -> bool {
    runs > 0 && (rows as u128) >= (runs as u128) * (threshold as u128) * (multiplier as u128)
}

/// Builder for [`ReadPlan`] that applies a limit and offset to the read plan
///
/// See [`ReadPlanBuilder::limited`] to create this builder.
pub(crate) struct LimitedReadPlanBuilder {
    /// The underlying builder
    inner: ReadPlanBuilder,
    /// Total number of rows in the row group before the selection, limit or
    /// offset are applied
    row_count: usize,
    /// The offset to apply, if any
    offset: Option<usize>,
    /// The limit to apply, if any
    limit: Option<usize>,
}

impl LimitedReadPlanBuilder {
    /// Create a new `LimitedReadPlanBuilder` from the existing builder and number of rows
    fn new(inner: ReadPlanBuilder, row_count: usize) -> Self {
        Self {
            inner,
            row_count,
            offset: None,
            limit: None,
        }
    }

    /// Set the offset to apply to the read plan
    pub(crate) fn with_offset(mut self, offset: Option<usize>) -> Self {
        self.offset = offset;
        self
    }

    /// Set the limit to apply to the read plan
    pub(crate) fn with_limit(mut self, limit: Option<usize>) -> Self {
        self.limit = limit;
        self
    }

    /// Apply offset and limit, updating the selection on the underlying builder
    /// and returning it.
    pub(crate) fn build_limited(self) -> ReadPlanBuilder {
        let Self {
            mut inner,
            row_count,
            offset,
            limit,
        } = self;

        // If the selection is empty, truncate
        if !inner.selects_any() {
            inner.selection = Some(RowSelection::from(vec![]));
        }

        // If an offset is defined, apply it to the `selection`
        if let Some(offset) = offset {
            inner.selection = Some(match row_count.checked_sub(offset) {
                None => RowSelection::from(vec![]),
                Some(remaining) => inner
                    .selection
                    .map(|selection| selection.offset(offset))
                    .unwrap_or_else(|| {
                        RowSelection::from(vec![
                            RowSelector::skip(offset),
                            RowSelector::select(remaining),
                        ])
                    }),
            });
        }

        // If a limit is defined, apply it to the final `selection`
        if let Some(limit) = limit {
            inner.selection = Some(
                inner
                    .selection
                    .map(|selection| selection.limit(limit))
                    .unwrap_or_else(|| {
                        RowSelection::from(vec![RowSelector::select(limit.min(row_count))])
                    }),
            );
        }

        inner
    }
}

/// Produce a new `BooleanArray` of the same length as `filter` in which only
/// the first `n` `true` positions from `filter` remain `true`; any `true`
/// positions beyond the first `n` are replaced with `false`.
///
/// `filter` must not contain nulls (callers apply [`prep_null_mask_filter`]
/// first). If `filter` has at most `n` `true` values, a clone is returned.
fn truncate_filter_after_n_trues(filter: BooleanArray, n: usize) -> BooleanArray {
    if filter.true_count() <= n {
        return filter;
    }
    let len = filter.len();
    if n == 0 {
        return BooleanArray::new(BooleanBuffer::new_unset(len), None);
    }
    // `set_indices` scans 64 bits at a time via `trailing_zeros`, so locating
    // the `n`-th set bit is cheaper than visiting every bit. Everything up to
    // and including that position is copied verbatim; the rest is zeroed.
    let values = filter.values();
    let last_kept = values
        .set_indices()
        .nth(n - 1)
        .expect("n - 1 < true_count, checked above");

    let mut builder = BooleanBufferBuilder::new(len);
    builder.append_buffer(&values.slice(0, last_kept + 1));
    builder.append_n(len - last_kept - 1, false);
    BooleanArray::new(builder.finish(), None)
}

/// A plan reading specific rows from a Parquet Row Group.
///
/// See [`ReadPlanBuilder`] to create `ReadPlan`s
#[derive(Debug)]
pub struct ReadPlan {
    /// The number of rows to read in each batch
    batch_size: usize,
    /// Row ranges to be selected from the data source
    row_selection_cursor: RowSelectionCursor,
}

impl ReadPlan {
    /// Returns a mutable reference to the selection selectors, if any
    #[deprecated(since = "57.1.0", note = "Use `row_selection_cursor_mut` instead")]
    pub fn selection_mut(&mut self) -> Option<&mut VecDeque<RowSelector>> {
        if let RowSelectionCursor::Selectors(selectors_cursor) = &mut self.row_selection_cursor {
            Some(selectors_cursor.selectors_mut())
        } else {
            None
        }
    }

    /// Returns a mutable reference to the row selection cursor
    pub fn row_selection_cursor_mut(&mut self) -> &mut RowSelectionCursor {
        &mut self.row_selection_cursor
    }

    /// Return the number of rows to read in each output batch
    #[inline(always)]
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn builder_with_selection(selection: RowSelection) -> ReadPlanBuilder {
        ReadPlanBuilder::new(1024).with_selection(Some(selection))
    }

    fn assert_strategy_decision(
        builder: ReadPlanBuilder,
        strategy: RowSelectionStrategy,
        reason: RowSelectionStrategyReason,
        expected_shape: RowSelectionShape,
    ) {
        let decision = builder.resolve_selection_strategy_decision();
        assert_eq!(decision.strategy, strategy);
        assert_eq!(decision.reason, reason);
        assert_eq!(decision.shape, expected_shape);
    }

    fn shape(
        selected_rows: usize,
        skipped_rows: usize,
        selector_count: usize,
        selected_run_count: usize,
        skipped_run_count: usize,
    ) -> RowSelectionShape {
        RowSelectionShape {
            selected_rows,
            skipped_rows,
            selector_count,
            selected_run_count,
            skipped_run_count,
        }
    }

    #[test]
    fn row_group_execution_modes_cover_pushdown_and_post_filter() {
        use crate::arrow::arrow_reader::selection::{RowGroupExecutionMode, RowSelectionStrategy};

        assert_eq!(
            RowGroupExecutionMode::Pushdown(RowSelectionStrategy::Mask).to_string(),
            "Pushdown(Mask)"
        );
        assert_eq!(
            RowGroupExecutionMode::Pushdown(RowSelectionStrategy::Selectors).to_string(),
            "Pushdown(Selectors)"
        );
        assert_eq!(RowGroupExecutionMode::PostFilter.to_string(), "PostFilter");
    }

    #[test]
    fn cost_model_classifier_triggers_for_fragmented_high_selectivity() {
        use crate::arrow::arrow_reader::selection::{
            CostModelDecisionReason, CostModelObservation, RowSelectionShape,
        };

        let observation = CostModelObservation {
            observed_row_groups: 2,
            shape: RowSelectionShape {
                selected_rows: 128,
                skipped_rows: 64,
                selector_count: 96,
                selected_run_count: 64,
                skipped_run_count: 32,
            },
        };

        assert_eq!(
            observation.trigger_reason(),
            CostModelDecisionReason::FragmentedHighSelectivity
        );
    }

    #[test]
    fn cost_model_classifier_waits_for_observation_window() {
        use crate::arrow::arrow_reader::selection::{
            CostModelDecisionReason, CostModelObservation, RowSelectionShape,
        };

        let observation = CostModelObservation {
            observed_row_groups: 0,
            shape: RowSelectionShape {
                selected_rows: 64,
                skipped_rows: 64,
                selector_count: 64,
                selected_run_count: 32,
                skipped_run_count: 32,
            },
        };

        assert_eq!(
            observation.trigger_reason(),
            CostModelDecisionReason::ObservationIncomplete
        );
    }

    #[test]
    fn cost_model_classifier_triggers_for_high_selectivity_without_pruning() {
        use crate::arrow::arrow_reader::selection::{
            CostModelDecisionReason, CostModelObservation, RowSelectionShape,
        };

        let observation = CostModelObservation {
            observed_row_groups: 2,
            shape: RowSelectionShape {
                selected_rows: 200,
                skipped_rows: 0,
                selector_count: 2,
                selected_run_count: 2,
                skipped_run_count: 0,
            },
        };

        assert_eq!(
            observation.trigger_reason(),
            CostModelDecisionReason::HighSelectivityNoPruning
        );
    }

    #[test]
    fn cost_model_classifier_triggers_for_fragmented_moderate_selectivity() {
        use crate::arrow::arrow_reader::selection::{
            CostModelDecisionReason, CostModelObservation, RowSelectionShape,
        };

        let observation = CostModelObservation {
            observed_row_groups: 2,
            shape: RowSelectionShape {
                selected_rows: 30,
                skipped_rows: 170,
                selector_count: 60,
                selected_run_count: 30,
                skipped_run_count: 30,
            },
        };

        assert_eq!(
            observation.trigger_reason(),
            CostModelDecisionReason::FragmentedModerateSelectivity
        );
    }

    #[test]
    fn cost_model_classifier_triggers_for_fragmented_near_ten_percent_selectivity() {
        use crate::arrow::arrow_reader::selection::{
            CostModelDecisionReason, CostModelObservation, RowSelectionShape,
        };

        let observation = CostModelObservation {
            observed_row_groups: 1,
            shape: RowSelectionShape {
                selected_rows: 9,
                skipped_rows: 91,
                selector_count: 18,
                selected_run_count: 9,
                skipped_run_count: 9,
            },
        };

        assert_eq!(
            observation.trigger_reason(),
            CostModelDecisionReason::FragmentedModerateSelectivity
        );
    }

    #[test]
    fn cost_model_classifier_keeps_q38_like_low_selectivity_fragmented_pushdown() {
        use crate::arrow::arrow_reader::selection::{
            CostModelDecisionReason, CostModelObservation, RowSelectionShape,
        };

        let observation = CostModelObservation {
            observed_row_groups: 1,
            shape: RowSelectionShape {
                selected_rows: 4_870,
                skipped_rows: 57_698,
                selector_count: 6_168,
                selected_run_count: 3_084,
                skipped_run_count: 3_084,
            },
        };

        assert_eq!(
            observation.trigger_reason(),
            CostModelDecisionReason::PushdownStillPreferred
        );
    }

    #[test]
    fn cost_model_classifier_keeps_low_selectivity_fragmented_pushdown() {
        use crate::arrow::arrow_reader::selection::{
            CostModelDecisionReason, CostModelObservation, RowSelectionShape,
        };

        let observation = CostModelObservation {
            observed_row_groups: 1,
            shape: RowSelectionShape {
                selected_rows: 4,
                skipped_rows: 196,
                selector_count: 8,
                selected_run_count: 4,
                skipped_run_count: 4,
            },
        };

        assert_eq!(
            observation.trigger_reason(),
            CostModelDecisionReason::PushdownStillPreferred
        );
    }

    #[test]
    fn selection_strategy_decision_records_forced_mask() {
        let selection = RowSelection::from(vec![RowSelector::skip(2), RowSelector::select(8)]);
        let builder =
            builder_with_selection(selection).with_row_selection_policy(RowSelectionPolicy::Mask);

        assert_strategy_decision(
            builder,
            RowSelectionStrategy::Mask,
            RowSelectionStrategyReason::ForcedMask,
            shape(8, 2, 2, 1, 1),
        );
    }

    #[test]
    fn selection_strategy_decision_records_forced_selectors() {
        let selection = RowSelection::from(vec![RowSelector::skip(2), RowSelector::select(8)]);
        let builder = builder_with_selection(selection)
            .with_row_selection_policy(RowSelectionPolicy::Selectors);

        assert_strategy_decision(
            builder,
            RowSelectionStrategy::Selectors,
            RowSelectionStrategyReason::ForcedSelectors,
            shape(8, 2, 2, 1, 1),
        );
    }

    #[test]
    fn selection_strategy_decision_records_auto_empty_selection() {
        let selection = RowSelection::from(vec![]);
        let builder = builder_with_selection(selection);

        assert_strategy_decision(
            builder,
            RowSelectionStrategy::Mask,
            RowSelectionStrategyReason::AutoMaskEmptySelection,
            shape(0, 0, 0, 0, 0),
        );
    }

    #[test]
    fn selection_strategy_decision_records_auto_short_runs() {
        let selection = RowSelection::from(vec![RowSelector::select(8), RowSelector::skip(8)]);
        let builder = builder_with_selection(selection);

        assert_strategy_decision(
            builder,
            RowSelectionStrategy::Mask,
            RowSelectionStrategyReason::AutoMaskShortRuns,
            shape(8, 8, 2, 1, 1),
        );
    }

    #[test]
    fn selection_strategy_decision_records_auto_long_runs() {
        let selection = RowSelection::from(vec![RowSelector::select(3), RowSelector::skip(3)]);
        let builder = builder_with_selection(selection)
            .with_row_selection_policy(RowSelectionPolicy::Auto { threshold: 1 });

        assert_strategy_decision(
            builder,
            RowSelectionStrategy::Selectors,
            RowSelectionStrategyReason::AutoSelectorLongRuns,
            shape(3, 3, 2, 1, 1),
        );
    }

    #[test]
    fn build_metrics_records_structured_strategy_decision_shape() {
        let metrics = ArrowReaderMetrics::enabled();
        let selection = RowSelection::from(vec![RowSelector::select(8), RowSelector::skip(4)]);
        let builder = builder_with_selection(selection);

        builder.build_with_metrics(&metrics);

        assert_eq!(metrics.row_selection_selected_rows(), Some(8));
        assert_eq!(metrics.row_selection_skipped_rows(), Some(0));
        assert_eq!(metrics.row_selection_selector_count(), Some(1));
        assert_eq!(metrics.row_selection_selected_run_count(), Some(1));
        assert_eq!(metrics.row_selection_skipped_run_count(), Some(0));
        assert_eq!(metrics.row_selection_mask_plan_count(), Some(1));
        assert_eq!(metrics.row_selection_selector_plan_count(), Some(0));
        assert_eq!(
            metrics.row_selection_auto_mask_short_run_plan_count(),
            Some(1)
        );
    }

    #[test]
    fn preferred_selection_strategy_prefers_mask_by_default() {
        let selection = RowSelection::from(vec![RowSelector::select(8)]);
        let builder = builder_with_selection(selection);
        assert_eq!(
            builder.resolve_selection_strategy(),
            RowSelectionStrategy::Mask
        );
    }

    #[test]
    fn preferred_selection_strategy_prefers_selectors_when_threshold_small() {
        let selection = RowSelection::from(vec![RowSelector::select(3), RowSelector::skip(3)]);
        let builder = builder_with_selection(selection)
            .with_row_selection_policy(RowSelectionPolicy::Auto { threshold: 1 });
        assert_eq!(
            builder.resolve_selection_strategy(),
            RowSelectionStrategy::Selectors
        );
    }

    #[test]
    fn auto_strategy_prefers_mask_for_fragmented_selected_rows_at_threshold_boundary() {
        let selectors: Vec<RowSelector> = (0..64)
            .flat_map(|_| [RowSelector::select(1), RowSelector::skip(63)])
            .collect();
        let builder = builder_with_selection(RowSelection::from(selectors));

        let decision = builder.resolve_selection_strategy_decision();

        assert_eq!(decision.strategy, RowSelectionStrategy::Mask);
        assert_eq!(
            decision.reason,
            RowSelectionStrategyReason::AutoMaskFragmentedSelection
        );
        assert_eq!(decision.shape.selected_run_count, 64);
        assert_eq!(decision.shape.average_selected_run_length(), 1.0);
    }

    #[test]
    fn auto_strategy_prefers_mask_for_high_selected_ratio() {
        let selection = RowSelection::from(vec![
            RowSelector::select(900),
            RowSelector::skip(25),
            RowSelector::select(75),
        ]);
        let builder = builder_with_selection(selection);

        let decision = builder.resolve_selection_strategy_decision();

        assert_eq!(decision.strategy, RowSelectionStrategy::Mask);
        assert_eq!(
            decision.reason,
            RowSelectionStrategyReason::AutoMaskHighSelectedRatio
        );
    }

    #[test]
    fn auto_strategy_prefers_selectors_for_clustered_high_selected_ratio() {
        let selectors: Vec<RowSelector> = (0..10)
            .flat_map(|_| [RowSelector::select(9000), RowSelector::skip(1000)])
            .collect();
        let selection = RowSelection::from(selectors);
        let builder = builder_with_selection(selection);

        let decision = builder.resolve_selection_strategy_decision();

        assert_eq!(decision.strategy, RowSelectionStrategy::Selectors);
        assert_eq!(
            decision.reason,
            RowSelectionStrategyReason::AutoSelectorClusteredSelection
        );
    }

    #[test]
    fn auto_strategy_prefers_selectors_for_clustered_long_selected_runs() {
        let selection =
            RowSelection::from(vec![RowSelector::skip(9000), RowSelector::select(1000)]);
        let builder = builder_with_selection(selection);

        let decision = builder.resolve_selection_strategy_decision();

        assert_eq!(decision.strategy, RowSelectionStrategy::Selectors);
        assert_eq!(
            decision.reason,
            RowSelectionStrategyReason::AutoSelectorClusteredSelection
        );
    }

    #[test]
    fn auto_strategy_prefers_selectors_for_long_single_selected_run_with_no_skips() {
        let selection = RowSelection::from(vec![RowSelector::select(1024)]);
        let builder = builder_with_selection(selection);

        let decision = builder.resolve_selection_strategy_decision();

        assert_eq!(decision.strategy, RowSelectionStrategy::Selectors);
        assert_eq!(
            decision.reason,
            RowSelectionStrategyReason::AutoSelectorLongRuns
        );
    }

    #[test]
    fn auto_strategy_prefers_selectors_for_tiny_runs_separated_by_huge_skip() {
        let selection = RowSelection::from(vec![
            RowSelector::select(4),
            RowSelector::skip(100_000),
            RowSelector::select(4),
        ]);
        let builder = builder_with_selection(selection);

        let decision = builder.resolve_selection_strategy_decision();

        assert_eq!(decision.strategy, RowSelectionStrategy::Selectors);
        assert_eq!(
            decision.reason,
            RowSelectionStrategyReason::AutoSelectorLongRuns
        );
    }

    #[test]
    fn auto_strategy_prefers_selectors_for_huge_half_selected_ratio_without_saturation() {
        let selection = RowSelection::from(vec![
            RowSelector::select(usize::MAX / 2),
            RowSelector::skip(usize::MAX / 2),
        ]);
        let builder = builder_with_selection(selection);

        let decision = builder.resolve_selection_strategy_decision();

        assert_eq!(decision.strategy, RowSelectionStrategy::Selectors);
        assert_eq!(
            decision.reason,
            RowSelectionStrategyReason::AutoSelectorClusteredSelection
        );
    }

    #[test]
    fn build_metrics_records_shape_aware_strategy_reasons() {
        let metrics = ArrowReaderMetrics::enabled();
        let fragmented_selectors: Vec<RowSelector> = (0..64)
            .flat_map(|_| [RowSelector::select(1), RowSelector::skip(63)])
            .collect();

        builder_with_selection(RowSelection::from(fragmented_selectors))
            .build_with_metrics(&metrics);
        builder_with_selection(RowSelection::from(vec![
            RowSelector::select(900),
            RowSelector::skip(25),
            RowSelector::select(75),
        ]))
        .build_with_metrics(&metrics);
        builder_with_selection(RowSelection::from(vec![
            RowSelector::skip(9000),
            RowSelector::select(1000),
        ]))
        .build_with_metrics(&metrics);

        assert_eq!(metrics.row_selection_mask_plan_count(), Some(2));
        assert_eq!(metrics.row_selection_selector_plan_count(), Some(1));
        assert_eq!(
            metrics.row_selection_auto_mask_fragmented_plan_count(),
            Some(1)
        );
        assert_eq!(
            metrics.row_selection_auto_mask_high_ratio_plan_count(),
            Some(1)
        );
        assert_eq!(
            metrics.row_selection_auto_selector_clustered_plan_count(),
            Some(1)
        );
    }

    #[test]
    fn truncate_filter_after_n_trues_keeps_first_n_matches() {
        let f = BooleanArray::from(vec![true, false, true, true, false, true, true]);
        // true positions: 0, 2, 3, 5, 6
        let t = truncate_filter_after_n_trues(f.clone(), 3);
        assert_eq!(t.len(), f.len());
        assert_eq!(t.true_count(), 3);
        let out: Vec<bool> = (0..t.len()).map(|i| t.value(i)).collect();
        assert_eq!(
            out,
            vec![true, false, true, true, false, false, false],
            "first three trues should survive, the rest become false"
        );
    }

    #[test]
    fn truncate_filter_after_n_trues_passes_through_when_already_small_enough() {
        let f = BooleanArray::from(vec![true, false, true, false]);
        let t = truncate_filter_after_n_trues(f.clone(), 5);
        assert_eq!(t.len(), f.len());
        assert_eq!(t.true_count(), 2);
    }

    #[test]
    fn truncate_filter_after_n_trues_zero_returns_all_false() {
        let f = BooleanArray::from(vec![true, true, true]);
        let t = truncate_filter_after_n_trues(f, 0);
        assert_eq!(t.len(), 3);
        assert_eq!(t.true_count(), 0);
    }

    #[test]
    fn with_predicate_options_limit_pads_tail_when_no_prior_selection() {
        use crate::arrow::ProjectionMask;
        use crate::arrow::array_reader::StructArrayReader;
        use crate::arrow::array_reader::test_util::make_int32_page_reader;
        use crate::arrow::arrow_reader::ArrowPredicateFn;
        use arrow_schema::{DataType as ArrowType, Field, Fields};

        // 100 rows, all match the predicate. Limit stops the loop after 10
        // matches — but the resulting RowSelection must still describe the
        // full 100-row row group (90 trailing rows as "not selected"), not
        // only the 10 rows we happened to evaluate before breaking.
        const TOTAL_ROWS: usize = 100;
        const LIMIT: usize = 10;

        let data: Vec<i32> = (0..TOTAL_ROWS as i32).collect();
        let levels = vec![0; TOTAL_ROWS];
        let leaf = make_int32_page_reader(&data, &levels, &levels, 0, 0);
        let struct_type = ArrowType::Struct(Fields::from(vec![Field::new(
            "c0",
            ArrowType::Int32,
            false,
        )]));
        let struct_reader = StructArrayReader::new(struct_type, vec![leaf], 0, 0, false);

        let mut predicate = ArrowPredicateFn::new(ProjectionMask::all(), |batch| {
            Ok(BooleanArray::from(vec![true; batch.num_rows()]))
        });

        let builder = ReadPlanBuilder::new(16)
            .with_predicate_options(
                PredicateOptions::new(Box::new(struct_reader), &mut predicate)
                    .with_limit(LIMIT, TOTAL_ROWS),
            )
            .unwrap();

        let selection = builder
            .selection()
            .expect("limit-driven early break must produce a selection");

        // `row_count` counts selected rows — must equal the limit.
        assert_eq!(selection.row_count(), LIMIT);

        // Total rows covered (selects + skips) must equal the full row group
        // so downstream offset/limit math stays in absolute-row space.
        let total: usize = selection.iter().map(|s| s.row_count).sum();
        assert_eq!(
            total, TOTAL_ROWS,
            "selection must span the full row group, not only the prefix evaluated before the limit"
        );
    }
}
