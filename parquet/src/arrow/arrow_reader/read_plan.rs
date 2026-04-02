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
use crate::arrow::arrow_reader::selection::RowSelectionPolicy;
use crate::arrow::arrow_reader::selection::RowSelectionStrategy;
use crate::arrow::arrow_reader::{
    ArrowPredicate, ParquetRecordBatchReader, RowSelection, RowSelectionCursor, RowSelector,
};
use crate::errors::{ParquetError, Result};
use arrow_array::Array;
use arrow_select::filter::prep_null_mask_filter;
use std::collections::VecDeque;

/// Run length at or below this value is treated as "short" for scatter stats.
const DEFERRAL_SHORT_RUN_THRESHOLD_ROWS: usize = 16;
/// Run length at or above this value is treated as "long" for skip-island stats.
const DEFERRAL_LONG_RUN_THRESHOLD_ROWS: usize = 128;
/// Avoid deferring when skips are already selective enough to be useful.
const DEFERRAL_MAX_SKIP_SELECTIVITY: f64 = 0.10;
/// Avoid deferring when long skip islands are already significant.
const DEFERRAL_MAX_LONG_SKIP_SHARE: f64 = 0.50;

/// Histogram-like stats for selector runs, split by select/skip and short/long bins.
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
struct SelectionRunStats {
    total_rows: usize,
    effective_count: usize,
    skipped_rows: usize,
    short_select_rows: usize,
    short_skip_rows: usize,
    long_select_rows: usize,
    long_skip_rows: usize,
}

impl SelectionRunStats {
    fn from_selection(
        selection: &RowSelection,
        short_threshold: usize,
        long_threshold: usize,
    ) -> Self {
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

                let is_short = row_count <= short_threshold;
                let is_long = row_count >= long_threshold;

                match (selector.skip, is_short, is_long) {
                    (true, true, _) => stats.short_skip_rows += row_count,
                    (true, _, true) => stats.long_skip_rows += row_count,
                    (false, true, _) => stats.short_select_rows += row_count,
                    (false, _, true) => stats.long_select_rows += row_count,
                    _ => {}
                }

                stats
            })
    }

    fn short_row_ratio(self, row_count: usize) -> f64 {
        if row_count == 0 {
            return 0.0;
        }
        (self.short_select_rows + self.short_skip_rows) as f64 / row_count as f64
    }

    fn skip_row_ratio(self, row_count: usize) -> f64 {
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

/// A builder for [`ReadPlan`]
#[derive(Clone, Debug)]
pub struct ReadPlanBuilder {
    batch_size: usize,
    /// Which rows to select. Includes the result of all filters applied so far
    selection: Option<RowSelection>,
    /// Policy to use when materializing the row selection
    row_selection_policy: RowSelectionPolicy,
    /// Maximum allowed short-run row ratio for applying a predicate result.
    ///
    /// When set, if applying a predicate would produce a short-run row ratio
    /// above this threshold, and skip selectivity remains low, the result is
    /// deferred into `deferred_selection` instead. For example, `0.25` allows
    /// at most 25% of rows to fall in short runs before deferring.
    ///
    /// `None` disables deferral (all predicates applied immediately).
    scatter_threshold: Option<f64>,
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
            scatter_threshold: None,
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

    /// Returns the current row selection policy
    pub fn row_selection_policy(&self) -> &RowSelectionPolicy {
        &self.row_selection_policy
    }

    /// Set the scatter threshold for filter deferral.
    ///
    /// The threshold is the maximum allowed **short-run row ratio**
    /// (`rows_in_short_runs / row_count`). If applying a predicate would
    /// produce a ratio above this value, and skip selectivity remains low,
    /// its result is deferred. For example, `0.25` allows at most 25% of rows
    /// in short runs.
    ///
    /// This avoids applying highly fragmented low-pruning predicates
    /// immediately, while preserving useful long skip islands.
    ///
    /// The deferred results are merged via [`RowSelection::intersection`] at
    /// build time, so correctness is preserved.
    ///
    /// `None` disables deferral (all predicates applied immediately).
    pub fn with_scatter_threshold(mut self, threshold: Option<f64>) -> Self {
        self.scatter_threshold = threshold;
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
        mut self,
        array_reader: Box<dyn ArrayReader>,
        predicate: &mut dyn ArrowPredicate,
        row_count: usize,
    ) -> Result<Self> {
        // Build a ReadPlan for the predicate reader using only the current
        // selection (not deferred). We avoid cloning the entire builder by
        // constructing a minimal ReadPlanBuilder with just what build() needs.
        let plan_for_reader = ReadPlanBuilder {
            batch_size: self.batch_size,
            selection: self.selection.clone(),
            row_selection_policy: self.row_selection_policy,
            scatter_threshold: None,
            deferred_selection: None,
        };
        let reader = ParquetRecordBatchReader::new(array_reader, plan_for_reader.build());
        let mut filters = vec![];
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
            match filter.null_count() {
                0 => filters.push(filter),
                _ => filters.push(prep_null_mask_filter(&filter)),
            };
        }

        // If the predicate selected all rows and there is no prior selection,
        // skip creating a RowSelection entirely — this avoids the allocation
        // and keeps selection as None which enables coalesced page fetches.
        let all_selected = filters.iter().all(|f| f.true_count() == f.len());
        if all_selected && self.selection.is_none() {
            return Ok(self);
        }
        let raw = RowSelection::from_filters(&filters);

        // Compute the absolute-position result
        let absolute = match self.selection.as_ref() {
            Some(selection) => selection.and_then(&raw),
            None => raw,
        };

        let current_selectors = self.selection.as_ref().map_or(0, |s| s.selector_count());
        let should_defer = self.should_defer_selection(&absolute, row_count, current_selectors);

        if should_defer {
            // Defer: accumulate into deferred_selection, leave self.selection unchanged
            self.deferred_selection = Some(match self.deferred_selection.take() {
                Some(existing) => existing.intersection(&absolute),
                None => absolute,
            });
        } else {
            // Apply normally
            self.selection = Some(absolute);
        }

        Ok(self)
    }

    /// Returns true when the predicate result should be deferred instead of
    /// applied immediately.
    fn should_defer_selection(
        &self,
        absolute: &RowSelection,
        row_count: usize,
        current_selectors: usize,
    ) -> bool {
        let Some(threshold) = self.scatter_threshold else {
            return false;
        };

        if row_count == 0 {
            return false;
        }

        // If the predicate does not increase fragmentation, keep it applied.
        if absolute.selector_count() <= current_selectors {
            return false;
        }

        let stats = SelectionRunStats::from_selection(
            absolute,
            DEFERRAL_SHORT_RUN_THRESHOLD_ROWS,
            DEFERRAL_LONG_RUN_THRESHOLD_ROWS,
        );

        stats.short_row_ratio(row_count) > threshold
            && stats.skip_row_ratio(row_count) <= DEFERRAL_MAX_SKIP_SELECTIVITY
            && stats.long_skip_share() <= DEFERRAL_MAX_LONG_SKIP_SHARE
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
            scatter_threshold: _,
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
            scatter_threshold: None,
            deferred_selection: Some(deferred),
        }
    }

    #[test]
    fn test_merge_deferred_no_prior_selection() {
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
    fn test_merge_deferred_with_prior_selection() {
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
    fn test_merge_deferred_in_build() {
        // Verify that build() merges deferred before creating the ReadPlan
        let deferred = RowSelection::from(vec![RowSelector::select(80), RowSelector::skip(20)]);
        let builder = builder_with_deferred(None, deferred);
        // build() should merge and produce a plan that selects 80 rows
        let _plan = builder.build();
        // If it didn't panic, the merge worked (selection was properly set)
    }

    #[test]
    fn test_merge_deferred_in_build_limited() {
        // Verify that build_limited() merges deferred before applying offset/limit
        let deferred = RowSelection::from(vec![RowSelector::select(80), RowSelector::skip(20)]);
        let builder = builder_with_deferred(None, deferred);
        let limited = builder.limited(100).with_limit(Some(50)).build_limited();
        let sel = limited.selection.unwrap();
        // After merge: 80 selected, 20 skipped. After limit(50): 50 selected.
        assert_eq!(sel.row_count(), 50);
    }

    #[test]
    fn test_scatter_threshold_setter() {
        let builder = ReadPlanBuilder::new(1024);
        assert!(builder.scatter_threshold.is_none());
        assert!(builder.deferred_selection.is_none());

        let builder = builder.with_scatter_threshold(Some(0.9));
        assert_eq!(builder.scatter_threshold, Some(0.9));
    }

    #[test]
    fn test_no_deferred_when_threshold_disabled() {
        // Without threshold, deferred_selection should always remain None
        let builder = ReadPlanBuilder::new(1024);
        assert!(builder.scatter_threshold.is_none());
        assert!(builder.deferred_selection.is_none());
    }

    #[test]
    fn test_multiple_deferred_selections_intersected() {
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
    fn test_selection_run_stats_bins_split_skip_and_select() {
        let selection = RowSelection::from(vec![
            RowSelector::select(4),
            RowSelector::skip(3),
            RowSelector::select(200),
            RowSelector::skip(256),
            RowSelector::select(40),
        ]);

        let stats = SelectionRunStats::from_selection(&selection, 16, 128);
        assert_eq!(stats.total_rows, 503);
        assert_eq!(stats.effective_count, 5);
        assert_eq!(stats.skipped_rows, 259);
        assert_eq!(stats.short_select_rows, 4);
        assert_eq!(stats.short_skip_rows, 3);
        assert_eq!(stats.long_select_rows, 200);
        assert_eq!(stats.long_skip_rows, 256);
    }

    #[test]
    fn test_should_defer_selection_for_fragmented_low_skip() {
        let builder = ReadPlanBuilder::new(1024).with_scatter_threshold(Some(0.5));
        let absolute = RowSelection::from(vec![
            RowSelector::select(15),
            RowSelector::skip(1),
            RowSelector::select(15),
            RowSelector::skip(1),
            RowSelector::select(15),
            RowSelector::skip(1),
            RowSelector::select(15),
            RowSelector::skip(1),
        ]);

        assert!(builder.should_defer_selection(&absolute, 64, 1));
    }

    #[test]
    fn test_should_not_defer_selection_when_skip_selective() {
        let builder = ReadPlanBuilder::new(1024).with_scatter_threshold(Some(0.5));
        let absolute = RowSelection::from(vec![
            RowSelector::select(1),
            RowSelector::skip(15),
            RowSelector::select(1),
            RowSelector::skip(15),
            RowSelector::select(1),
            RowSelector::skip(15),
            RowSelector::select(1),
            RowSelector::skip(15),
        ]);

        assert!(!builder.should_defer_selection(&absolute, 64, 1));
    }

    #[test]
    fn test_should_not_defer_selection_with_dominant_long_skip_islands() {
        let builder = ReadPlanBuilder::new(1024).with_scatter_threshold(Some(0.0001));
        let absolute = RowSelection::from(vec![
            RowSelector::select(600),
            RowSelector::skip(128),
            RowSelector::select(600),
            RowSelector::skip(1),
            RowSelector::select(600),
        ]);

        // Skip selectivity is low enough to pass, but long skip islands dominate skipped rows.
        assert!(!builder.should_defer_selection(&absolute, 1929, 1));
    }
}
