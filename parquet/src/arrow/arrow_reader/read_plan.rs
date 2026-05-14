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
use crate::arrow::arrow_reader::metrics::{
    ArrowReaderMetrics, FilterDeferralDecisionReason, FilterSelectivityStat,
};
use crate::arrow::arrow_reader::selection::RowSelectionPolicy;
use crate::arrow::arrow_reader::selection::RowSelectionStrategy;
use crate::arrow::arrow_reader::{
    ArrowPredicate, ParquetRecordBatchReader, RowSelection, RowSelectionCursor, RowSelector,
};
use crate::errors::{ParquetError, Result};
use arrow_array::{Array, BooleanArray};
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder};
use arrow_select::filter::prep_null_mask_filter;
use std::collections::VecDeque;

// Fixed gates for the filter-deferral heuristic. They sit alongside the
// user-tunable knob on
// [`ArrowReaderBuilder::with_long_skip_share_threshold`], which controls
// the long-skip-share requirement; these three are intentionally not
// exposed as a public API to keep the surface area small. Predicate results
// are kept applied only when every gate is satisfied; otherwise the result
// is deferred until [`ReadPlanBuilder::build`].

/// Run length at or above this value is treated as "long" for skip-island stats.
const DEFERRAL_LONG_RUN_THRESHOLD_ROWS: usize = 100;
/// Minimum cumulative skip selectivity (`skipped_rows / row_count`) required to
/// avoid deferral once fragmentation increases.
const DEFERRAL_SKIP_SELECTIVITY_FLOOR: f64 = 0.10;
/// Minimum *incremental* skip selectivity contributed by a single predicate
/// (`new_skipped_rows / row_count`) required to avoid deferral once
/// fragmentation increases.
const DEFERRAL_DELTA_SKIP_SELECTIVITY_FLOOR: f64 = 0.02;

/// Histogram-like stats for selector runs, focused on skipped-row contiguity.
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
struct SelectionRunStats {
    total_rows: usize,
    effective_count: usize,
    skipped_rows: usize,
    long_skip_rows: usize,
}

impl SelectionRunStats {
    fn from_selection(selection: &RowSelection, long_threshold: usize) -> Self {
        selection
            .iter()
            .fold(Self::default(), |mut stats, selector| {
                let row_count = selector.row_count;
                if row_count == 0 {
                    return stats;
                }

                stats.total_rows += row_count;
                stats.effective_count += 1;

                if selector.skip {
                    stats.skipped_rows += row_count;
                }

                if selector.skip && row_count >= long_threshold {
                    stats.long_skip_rows += row_count;
                }

                stats
            })
    }

    fn skip_selectivity(self, row_count: usize) -> f64 {
        if row_count == 0 {
            return 0.0;
        }
        self.skipped_rows as f64 / row_count as f64
    }

    fn long_skip_share(self) -> f64 {
        if self.skipped_rows == 0 {
            return 0.0;
        }
        self.long_skip_rows as f64 / self.skipped_rows as f64
    }
}

#[derive(Debug, Clone, Copy)]
struct DeferralDecision {
    should_defer: bool,
    reason: FilterDeferralDecisionReason,
    current_stats: SelectionRunStats,
    absolute_stats: SelectionRunStats,
    absolute_skip_selectivity: f64,
    absolute_long_skip_share: f64,
    delta_skip_selectivity: f64,
    delta_long_skip_share: f64,
}

impl DeferralDecision {
    /// Decision used by the all-selected fast path: predicate accepted every
    /// row, no selection was materialized, all stats are zero.
    fn all_selected_fast_path() -> Self {
        Self {
            should_defer: false,
            reason: FilterDeferralDecisionReason::AllSelectedFastPath,
            current_stats: SelectionRunStats::default(),
            absolute_stats: SelectionRunStats::default(),
            absolute_skip_selectivity: 0.0,
            absolute_long_skip_share: 0.0,
            delta_skip_selectivity: 0.0,
            delta_long_skip_share: 0.0,
        }
    }
}

/// Options for [`ReadPlanBuilder::with_predicate_options`].
pub struct PredicateOptions<'a> {
    array_reader: Box<dyn ArrayReader>,
    predicate: &'a mut dyn ArrowPredicate,
    limit: Option<usize>,
    total_rows: usize,
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

    /// Set the row group's total row count without enabling a limit.
    ///
    /// The value is used to record per-predicate selectivity metrics
    /// regardless of whether early termination is active.
    pub fn with_total_rows(mut self, total_rows: usize) -> Self {
        self.total_rows = total_rows;
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
    /// Optional metrics sink for observability.
    metrics: ArrowReaderMetrics,
    /// Number of predicates evaluated so far.
    predicate_index: usize,
    /// Minimum long-skip-share required for a fragmented predicate result to
    /// remain applied; `None` disables deferral. See
    /// [`ArrowReaderBuilder::with_long_skip_share_threshold`] for semantics.
    ///
    /// [`ArrowReaderBuilder::with_long_skip_share_threshold`]: crate::arrow::arrow_reader::ArrowReaderBuilder::with_long_skip_share_threshold
    long_skip_share_threshold: Option<f64>,
    /// Accumulated deferred selections, merged via `intersection` at build time.
    deferred_selection: Option<RowSelection>,
}

impl ReadPlanBuilder {
    /// Create a `ReadPlanBuilder` with the given batch size
    pub fn new(batch_size: usize) -> Self {
        Self {
            batch_size,
            selection: None,
            row_selection_policy: RowSelectionPolicy::default(),
            metrics: ArrowReaderMetrics::disabled(),
            predicate_index: 0,
            long_skip_share_threshold: None,
            deferred_selection: None,
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

    /// Configure metrics collection for this read plan.
    pub(crate) fn with_metrics(mut self, metrics: ArrowReaderMetrics) -> Self {
        self.metrics = metrics;
        self
    }

    /// Returns the current row selection policy
    pub fn row_selection_policy(&self) -> &RowSelectionPolicy {
        &self.row_selection_policy
    }

    /// Set the long-skip-share threshold for filter deferral.
    ///
    /// See [`ArrowReaderBuilder::with_long_skip_share_threshold`] for full semantics.
    ///
    /// [`ArrowReaderBuilder::with_long_skip_share_threshold`]: crate::arrow::arrow_reader::ArrowReaderBuilder::with_long_skip_share_threshold
    pub fn with_long_skip_share_threshold(mut self, threshold: Option<f64>) -> Self {
        self.long_skip_share_threshold = threshold;
        self
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
    pub(crate) fn resolve_selection_strategy(&self) -> RowSelectionStrategy {
        match self.row_selection_policy {
            RowSelectionPolicy::Selectors => RowSelectionStrategy::Selectors,
            RowSelectionPolicy::Mask => RowSelectionStrategy::Mask,
            RowSelectionPolicy::Auto { threshold, .. } => {
                let selection = match self.selection.as_ref() {
                    Some(selection) => selection,
                    None => return RowSelectionStrategy::Selectors,
                };

                // total_rows: total number of rows selected / skipped
                // effective_count: number of non-empty selectors
                let (total_rows, effective_count) =
                    selection.iter().fold((0usize, 0usize), |(rows, count), s| {
                        if s.row_count > 0 {
                            (rows + s.row_count, count + 1)
                        } else {
                            (rows, count)
                        }
                    });

                if effective_count == 0 {
                    return RowSelectionStrategy::Mask;
                }

                if total_rows < effective_count.saturating_mul(threshold) {
                    RowSelectionStrategy::Mask
                } else {
                    RowSelectionStrategy::Selectors
                }
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
        row_count: usize,
    ) -> Result<Self> {
        self.with_predicate_options(
            PredicateOptions::new(array_reader, predicate).with_total_rows(row_count),
        )
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
        } = options;

        // `total_rows` doubles as the row group size used by per-predicate
        // selectivity metrics, regardless of whether a limit was set.
        let row_count = total_rows;

        // Build a ReadPlan for the predicate reader using only the current
        // applied selection (excluding `deferred_selection`). Deferred
        // predicates have not been intersected with `selection` yet, and
        // re-applying them here would short-circuit the next predicate's
        // read against rows it should still evaluate.
        let plan_for_reader = ReadPlanBuilder {
            batch_size: self.batch_size,
            selection: self.selection.clone(),
            row_selection_policy: self.row_selection_policy,
            metrics: ArrowReaderMetrics::disabled(),
            predicate_index: 0,
            long_skip_share_threshold: None,
            deferred_selection: None,
        };

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

        let reader = ParquetRecordBatchReader::new(array_reader, plan_for_reader.build());
        let mut filters = vec![];
        let mut processed_rows: usize = 0;
        let mut matched_rows: usize = 0;
        for maybe_batch in reader {
            let maybe_batch = maybe_batch?;
            let input_rows = maybe_batch.num_rows();
            let filter = predicate.evaluate(maybe_batch)?;
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
            self.record_predicate_stat(
                row_count,
                0,
                0,
                &DeferralDecision::all_selected_fast_path(),
            );
            return Ok(self);
        }
        let raw = RowSelection::from_filters(&filters);

        // Compute the absolute-position result
        let absolute = match self.selection.as_ref() {
            Some(selection) => selection.and_then(&raw),
            None => raw,
        };

        let current_stats = self
            .selection
            .as_ref()
            .map_or_else(SelectionRunStats::default, |s| {
                SelectionRunStats::from_selection(s, DEFERRAL_LONG_RUN_THRESHOLD_ROWS)
            });
        let current_selectors = self.selection.as_ref().map_or(0, |s| s.selector_count());
        let decision =
            self.evaluate_deferral(&absolute, row_count, current_selectors, current_stats);
        let should_defer = decision.should_defer;

        self.record_predicate_stat(
            row_count,
            current_selectors,
            absolute.selector_count(),
            &decision,
        );

        if should_defer {
            self.deferred_selection = Some(match self.deferred_selection.take() {
                Some(existing) => existing.intersection(&absolute),
                None => absolute,
            });
        } else {
            self.selection = Some(absolute);
        }

        Ok(self)
    }

    /// Emit a per-predicate selectivity stat and advance `predicate_index`.
    fn record_predicate_stat(
        &mut self,
        row_count: usize,
        current_selectors: usize,
        absolute_selectors: usize,
        decision: &DeferralDecision,
    ) {
        self.metrics
            .record_filter_selectivity_stat(FilterSelectivityStat {
                predicate_index: self.predicate_index,
                row_count,
                current_selector_count: current_selectors,
                absolute_selector_count: absolute_selectors,
                current_skipped_rows: decision.current_stats.skipped_rows,
                absolute_skipped_rows: decision.absolute_stats.skipped_rows,
                current_long_skip_rows: decision.current_stats.long_skip_rows,
                absolute_long_skip_rows: decision.absolute_stats.long_skip_rows,
                absolute_skip_selectivity: decision.absolute_skip_selectivity,
                absolute_long_skip_share: decision.absolute_long_skip_share,
                delta_skip_selectivity: decision.delta_skip_selectivity,
                delta_long_skip_share: decision.delta_long_skip_share,
                long_skip_share_threshold: self.long_skip_share_threshold,
                deferred: decision.should_defer,
                decision_reason: decision.reason,
            });
        self.predicate_index += 1;
    }

    fn evaluate_deferral(
        &self,
        absolute: &RowSelection,
        row_count: usize,
        current_selectors: usize,
        current_stats: SelectionRunStats,
    ) -> DeferralDecision {
        let absolute_stats =
            SelectionRunStats::from_selection(absolute, DEFERRAL_LONG_RUN_THRESHOLD_ROWS);

        let absolute_skip_selectivity = absolute_stats.skip_selectivity(row_count);
        let absolute_long_skip_share = absolute_stats.long_skip_share();
        let delta_skipped_rows = absolute_stats
            .skipped_rows
            .saturating_sub(current_stats.skipped_rows);
        let delta_long_skip_rows = absolute_stats
            .long_skip_rows
            .saturating_sub(current_stats.long_skip_rows);
        let delta_skip_selectivity = if row_count == 0 {
            0.0
        } else {
            delta_skipped_rows as f64 / row_count as f64
        };
        let delta_long_skip_share = if delta_skipped_rows == 0 {
            0.0
        } else {
            delta_long_skip_rows as f64 / delta_skipped_rows as f64
        };

        let Some(long_skip_share_threshold) = self.long_skip_share_threshold else {
            return DeferralDecision {
                should_defer: false,
                reason: FilterDeferralDecisionReason::ThresholdDisabled,
                current_stats,
                absolute_stats,
                absolute_skip_selectivity,
                absolute_long_skip_share,
                delta_skip_selectivity,
                delta_long_skip_share,
            };
        };

        if row_count == 0 {
            return DeferralDecision {
                should_defer: false,
                reason: FilterDeferralDecisionReason::ZeroRowCount,
                current_stats,
                absolute_stats,
                absolute_skip_selectivity,
                absolute_long_skip_share,
                delta_skip_selectivity,
                delta_long_skip_share,
            };
        }

        // If the predicate does not increase fragmentation, keep it applied.
        if absolute.selector_count() <= current_selectors {
            return DeferralDecision {
                should_defer: false,
                reason: FilterDeferralDecisionReason::FragmentationNotIncreased,
                current_stats,
                absolute_stats,
                absolute_skip_selectivity,
                absolute_long_skip_share,
                delta_skip_selectivity,
                delta_long_skip_share,
            };
        }

        let should_keep_applied = absolute_skip_selectivity >= DEFERRAL_SKIP_SELECTIVITY_FLOOR
            && absolute_long_skip_share >= long_skip_share_threshold
            && delta_skip_selectivity >= DEFERRAL_DELTA_SKIP_SELECTIVITY_FLOOR
            && delta_long_skip_share >= long_skip_share_threshold;

        DeferralDecision {
            should_defer: !should_keep_applied,
            reason: if should_keep_applied {
                FilterDeferralDecisionReason::GatesPassed
            } else {
                FilterDeferralDecisionReason::GatesFailedDeferred
            },
            current_stats,
            absolute_stats,
            absolute_skip_selectivity,
            absolute_long_skip_share,
            delta_skip_selectivity,
            delta_long_skip_share,
        }
    }

    /// Merge any deferred selection into the main selection.
    fn merge_deferred(&mut self) {
        if let Some(deferred) = self.deferred_selection.take() {
            self.selection = Some(match self.selection.take() {
                Some(selection) => selection.intersection(&deferred),
                None => deferred,
            });
        }
    }

    /// Create a final `ReadPlan` the read plan for the scan
    pub fn build(mut self) -> ReadPlan {
        // Merge any deferred selection before finalizing
        self.merge_deferred();

        // If selection is empty, truncate
        if !self.selects_any() {
            self.selection = Some(RowSelection::from(vec![]));
        }

        // Preferred strategy must not be Auto
        let selection_strategy = self.resolve_selection_strategy();

        let Self {
            batch_size,
            selection,
            row_selection_policy: _,
            metrics: _,
            predicate_index: _,
            long_skip_share_threshold: _,
            deferred_selection: _,
        } = self;

        let selection = selection.map(|s| s.trim());

        let row_selection_cursor = selection
            .map(|s| {
                let trimmed = s.trim();
                let selectors: Vec<RowSelector> = trimmed.into();
                match selection_strategy {
                    RowSelectionStrategy::Mask => {
                        RowSelectionCursor::new_mask_from_selectors(selectors)
                    }
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

        // Merge deferred selection before applying offset/limit so that
        // offset and limit operate on the correctly filtered row set.
        inner.merge_deferred();

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

    const TEST_LONG_SKIP_SHARE_THRESHOLD: f64 = 0.75;

    fn stats(selection: &RowSelection) -> SelectionRunStats {
        SelectionRunStats::from_selection(selection, DEFERRAL_LONG_RUN_THRESHOLD_ROWS)
    }

    fn builder_with_selection(selection: RowSelection) -> ReadPlanBuilder {
        ReadPlanBuilder::new(1024).with_selection(Some(selection))
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
        let selection = RowSelection::from(vec![RowSelector::select(8)]);
        let builder = builder_with_selection(selection)
            .with_row_selection_policy(RowSelectionPolicy::Auto { threshold: 1 });
        assert_eq!(
            builder.resolve_selection_strategy(),
            RowSelectionStrategy::Selectors
        );
    }

    /// Helper to build a `ReadPlanBuilder` with deferred selection set directly
    fn builder_with_deferred(
        selection: Option<RowSelection>,
        deferred: RowSelection,
    ) -> ReadPlanBuilder {
        ReadPlanBuilder {
            batch_size: 1024,
            selection,
            row_selection_policy: RowSelectionPolicy::default(),
            metrics: ArrowReaderMetrics::disabled(),
            predicate_index: 0,
            long_skip_share_threshold: None,
            deferred_selection: Some(deferred),
        }
    }

    #[test]
    fn merge_deferred_no_prior_selection() {
        // Deferred selection with no main selection: result = deferred
        let deferred = RowSelection::from(vec![RowSelector::select(90), RowSelector::skip(10)]);
        let mut builder = builder_with_deferred(None, deferred);
        builder.merge_deferred();
        let sel = builder.selection.unwrap();
        assert_eq!(sel.row_count(), 90);
        assert_eq!(sel.skipped_row_count(), 10);
        assert!(builder.deferred_selection.is_none());
    }

    #[test]
    fn merge_deferred_with_prior_selection() {
        // Main selects first 50, deferred selects rows 0..40 and 50..100
        // Intersection should select rows 0..40 (first 40 of 100)
        let main_sel = RowSelection::from(vec![RowSelector::select(50), RowSelector::skip(50)]);
        let deferred = RowSelection::from(vec![
            RowSelector::select(40),
            RowSelector::skip(10),
            RowSelector::select(50),
        ]);
        let mut builder = builder_with_deferred(Some(main_sel), deferred);
        builder.merge_deferred();
        let sel = builder.selection.unwrap();
        assert_eq!(sel.row_count(), 40);
    }

    #[test]
    fn merge_deferred_in_build() {
        // Verify that build() merges deferred before creating the ReadPlan
        let deferred = RowSelection::from(vec![RowSelector::select(80), RowSelector::skip(20)]);
        let builder = builder_with_deferred(None, deferred);
        // build() should merge and produce a plan that selects 80 rows
        let _plan = builder.build();
        // If it didn't panic, the merge worked (selection was properly set)
    }

    #[test]
    fn merge_deferred_in_build_limited() {
        // Verify that build_limited() merges deferred before applying offset/limit
        let deferred = RowSelection::from(vec![RowSelector::select(80), RowSelector::skip(20)]);
        let builder = builder_with_deferred(None, deferred);
        let limited = builder.limited(100).with_limit(Some(50)).build_limited();
        let sel = limited.selection.unwrap();
        // After merge: 80 selected, 20 skipped. After limit(50): 50 selected.
        assert_eq!(sel.row_count(), 50);
    }

    #[test]
    fn long_skip_share_threshold_setter() {
        let builder = ReadPlanBuilder::new(1024);
        assert!(builder.long_skip_share_threshold.is_none());
        assert!(builder.deferred_selection.is_none());

        let builder = builder.with_long_skip_share_threshold(Some(0.9));
        assert_eq!(builder.long_skip_share_threshold, Some(0.9));
    }

    #[test]
    fn no_deferred_when_threshold_disabled() {
        // Without threshold, deferred_selection should always remain None
        let builder = ReadPlanBuilder::new(1024);
        assert!(builder.long_skip_share_threshold.is_none());
        assert!(builder.deferred_selection.is_none());
    }

    #[test]
    fn multiple_deferred_selections_intersected() {
        // Two deferred selections should be intersected
        let deferred1 = RowSelection::from(vec![RowSelector::select(80), RowSelector::skip(20)]);
        let deferred2 = RowSelection::from(vec![
            RowSelector::skip(10),
            RowSelector::select(70),
            RowSelector::skip(20),
        ]);
        // intersection: only rows 10..80 (70 rows)
        let mut builder = builder_with_deferred(None, deferred1);
        builder.deferred_selection =
            Some(builder.deferred_selection.unwrap().intersection(&deferred2));
        builder.merge_deferred();
        let sel = builder.selection.unwrap();
        assert_eq!(sel.row_count(), 70);
    }

    #[test]
    fn selection_run_stats_tracks_skip_selectivity_and_long_skip_share() {
        let selection = RowSelection::from(vec![
            RowSelector::select(4),
            RowSelector::skip(3),
            RowSelector::select(200),
            RowSelector::skip(256),
            RowSelector::select(40),
        ]);

        let stats = SelectionRunStats::from_selection(&selection, 100);
        assert_eq!(stats.total_rows, 503);
        assert_eq!(stats.effective_count, 5);
        assert_eq!(stats.skipped_rows, 259);
        assert_eq!(stats.long_skip_rows, 256);

        let eps = 1e-12;
        assert!((stats.skip_selectivity(503) - (259.0 / 503.0)).abs() < eps);
        assert!((stats.long_skip_share() - (256.0 / 259.0)).abs() < eps);
    }

    #[test]
    fn should_not_defer_when_fragmentation_not_increased() {
        let builder = ReadPlanBuilder::new(1024)
            .with_long_skip_share_threshold(Some(TEST_LONG_SKIP_SHARE_THRESHOLD));
        let absolute = RowSelection::from(vec![
            RowSelector::select(40),
            RowSelector::skip(20),
            RowSelector::select(40),
        ]);

        assert!(
            !builder
                .evaluate_deferral(&absolute, 100, 3, SelectionRunStats::default())
                .should_defer
        );
    }

    #[test]
    fn should_not_defer_when_both_non_deferral_gates_pass() {
        let builder = ReadPlanBuilder::new(1024)
            .with_long_skip_share_threshold(Some(TEST_LONG_SKIP_SHARE_THRESHOLD));
        let absolute = RowSelection::from(vec![
            RowSelector::select(50),
            RowSelector::skip(100),
            RowSelector::select(50),
            RowSelector::skip(10),
        ]);

        assert!(
            !builder
                .evaluate_deferral(&absolute, 210, 1, SelectionRunStats::default())
                .should_defer
        );
    }

    #[test]
    fn should_defer_when_only_selectivity_gate_fails() {
        let builder = ReadPlanBuilder::new(1024)
            .with_long_skip_share_threshold(Some(TEST_LONG_SKIP_SHARE_THRESHOLD));
        let absolute = RowSelection::from(vec![
            RowSelector::select(1000),
            RowSelector::skip(100),
            RowSelector::select(1000),
        ]);

        assert!(
            builder
                .evaluate_deferral(&absolute, 2100, 1, SelectionRunStats::default())
                .should_defer
        );
    }

    #[test]
    fn should_defer_when_only_long_skip_share_gate_fails() {
        let builder = ReadPlanBuilder::new(1024)
            .with_long_skip_share_threshold(Some(TEST_LONG_SKIP_SHARE_THRESHOLD));
        let absolute = RowSelection::from(vec![
            RowSelector::select(20),
            RowSelector::skip(60),
            RowSelector::select(20),
            RowSelector::skip(20),
        ]);

        assert!(
            builder
                .evaluate_deferral(&absolute, 120, 1, SelectionRunStats::default())
                .should_defer
        );
    }

    #[test]
    fn should_defer_when_both_non_deferral_gates_fail() {
        let builder = ReadPlanBuilder::new(1024)
            .with_long_skip_share_threshold(Some(TEST_LONG_SKIP_SHARE_THRESHOLD));
        let absolute = RowSelection::from(vec![
            RowSelector::select(500),
            RowSelector::skip(50),
            RowSelector::select(500),
            RowSelector::skip(10),
        ]);

        assert!(
            builder
                .evaluate_deferral(&absolute, 1060, 1, SelectionRunStats::default())
                .should_defer
        );
    }

    #[test]
    fn long_run_threshold_counts_exactly_100_rows_as_long() {
        let builder = ReadPlanBuilder::new(1024)
            .with_long_skip_share_threshold(Some(TEST_LONG_SKIP_SHARE_THRESHOLD));
        let absolute = RowSelection::from(vec![
            RowSelector::select(10),
            RowSelector::skip(100),
            RowSelector::select(10),
            RowSelector::skip(20),
        ]);

        assert!(
            !builder
                .evaluate_deferral(&absolute, 140, 1, SelectionRunStats::default())
                .should_defer
        );
    }

    #[test]
    fn should_defer_when_absolute_gates_pass_but_delta_selectivity_is_too_small() {
        let builder = ReadPlanBuilder::new(1024)
            .with_long_skip_share_threshold(Some(TEST_LONG_SKIP_SHARE_THRESHOLD));
        let absolute = RowSelection::from(vec![
            RowSelector::select(400),
            RowSelector::skip(100),
            RowSelector::select(499),
            RowSelector::skip(1),
        ]);
        // Previously skipped 100/1000 rows already in long runs.
        let current_stats = SelectionRunStats {
            skipped_rows: 100,
            long_skip_rows: 100,
            ..SelectionRunStats::default()
        };

        assert!(
            builder
                .evaluate_deferral(&absolute, 1000, 1, current_stats)
                .should_defer
        );
    }

    #[test]
    fn should_defer_when_absolute_gates_pass_but_delta_long_skip_share_is_too_small() {
        let builder = ReadPlanBuilder::new(1024)
            .with_long_skip_share_threshold(Some(TEST_LONG_SKIP_SHARE_THRESHOLD));
        let absolute = RowSelection::from(vec![
            RowSelector::select(50),
            RowSelector::skip(100),
            RowSelector::select(50),
            RowSelector::skip(10),
        ]);
        // Absolute long-skip-share is high, but this predicate adds only short skips.
        let current = RowSelection::from(vec![RowSelector::select(50), RowSelector::skip(100)]);

        assert!(
            builder
                .evaluate_deferral(&absolute, 210, 1, stats(&current))
                .should_defer
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
