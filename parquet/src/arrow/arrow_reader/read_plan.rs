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

use crate::arrow::array_reader::{
    ArrayReader, CachedPredicateResult, CachedPredicateResultBuilder,
};
use crate::arrow::arrow_reader::{
    ArrowPredicate, ParquetRecordBatchReader, RowSelection, RowSelector,
};
use crate::arrow::ProjectionMask;
use crate::errors::{ParquetError, Result};
use arrow_array::RecordBatchReader;
use std::collections::VecDeque;

/// A builder for [`ReadPlan`]
#[derive(Clone)]
pub(crate) struct ReadPlanBuilder {
    batch_size: usize,
    /// Current to apply, includes all filters
    selection: Option<RowSelection>,
    /// Cached result of evaluating some columns with the RowSelection
    cached_predicate_result: Option<CachedPredicateResult>,
}

impl ReadPlanBuilder {
    /// Create a `ReadPlanBuilder` with the given batch size
    pub(crate) fn new(batch_size: usize) -> Self {
        Self {
            batch_size,
            selection: None,
            cached_predicate_result: None,
        }
    }

    /// Set the current selection to the given value
    pub(crate) fn with_selection(mut self, selection: Option<RowSelection>) -> Self {
        self.selection = selection;
        self
    }

    /// Returns the current selection, if any
    #[cfg(feature = "async")]
    pub(crate) fn selection(&self) -> Option<&RowSelection> {
        self.selection.as_ref()
    }

    /// Returns the currently cached predicate result, if any
    pub(crate) fn cached_predicate_result(&self) -> Option<&CachedPredicateResult> {
        self.cached_predicate_result.as_ref()
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
    pub(crate) fn selects_any(&self) -> bool {
        self.selection
            .as_ref()
            .map(|s| s.selects_any())
            .unwrap_or(true)
    }

    /// Returns the number of rows selected, or `None` if all rows are selected.
    #[cfg(feature = "async")]
    pub(crate) fn num_rows_selected(&self) -> Option<usize> {
        self.selection.as_ref().map(|s| s.row_count())
    }

    /// Evaluates an [`ArrowPredicate`], updating this plan's `selection`
    ///
    /// # Arguments
    ///
    /// * `num_original_columns`: The number of columns in the original parquet
    ///   schema.
    ///
    /// * `array_reader`: The array reader to use for evaluating the predicate.
    ///   must be configured with the projection mask specified by
    ///   [`ArrowPredicate::projection`] for the `predicate`.
    ///
    /// * `predicate`: The predicate to evaluate
    ///
    /// * `projection`: The projection mask that will be selected. This code will
    ///   potentially cache the results of filtering columns that also appear in the
    ///   projection mask.
    ///
    /// If `this.selection` is `Some`, the resulting  [`RowSelection`] will be
    /// the conjunction of it and the rows selected by `predicate` (they will be
    /// `AND`ed).
    ///
    /// Note: A pre-existing selection may come from evaluating a previous
    /// predicate or if the [`ParquetRecordBatchReader`] specifies an explicit
    /// [`RowSelection`] in addition to one or more predicates.
    pub(crate) fn with_predicate(
        mut self,
        num_original_columns: usize,
        array_reader: Box<dyn ArrayReader>,
        predicate: &mut dyn ArrowPredicate,
        projection_mask: &ProjectionMask,
    ) -> Result<Self> {
        // Prepare to decode all rows in the selection to evaluate the predicate
        let reader = ParquetRecordBatchReader::new(array_reader, self.clone().build());
        let mut cached_results_builder = CachedPredicateResultBuilder::try_new(
            num_original_columns,
            &reader.schema(),
            predicate.projection(),
            projection_mask,
            self.batch_size,
        )?;
        for maybe_batch in reader {
            let batch = maybe_batch?;
            let input_rows = batch.num_rows();
            let filter = predicate.evaluate(batch.clone())?;
            // Since user supplied predicate, check error here to catch bugs quickly
            if filter.len() != input_rows {
                return Err(arrow_err!(
                    "ArrowPredicate predicate returned {} rows, expected {input_rows}",
                    filter.len()
                ));
            }
            cached_results_builder.add(batch, filter)?;
        }

        let (raw, cached_predicate_result) = cached_results_builder.build()?;
        self.selection = match self.selection.take() {
            Some(selection) => Some(selection.and_then(&raw)),
            None => Some(raw),
        };

        self.cached_predicate_result = cached_predicate_result;
        Ok(self)
    }

    /// Create a final `ReadPlan` the read plan for the scan
    pub(crate) fn build(mut self) -> ReadPlan {
        // If selection is empty, truncate
        if !self.selects_any() {
            self.selection = Some(RowSelection::from(vec![]));
        }
        let Self {
            batch_size,
            selection,
            cached_predicate_result,
        } = self;

        let selection = selection.map(|s| s.trim().into());

        ReadPlan {
            batch_size,
            selection,
            cached_predicate_result,
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
pub(crate) struct ReadPlan {
    /// The number of rows to read in each batch
    batch_size: usize,
    /// Row ranges to be selected from the data source
    /// TODO update this to use something more efficient
    /// See <https://github.com/apache/arrow-rs/pull/7454/files#r2092962327>
    selection: Option<VecDeque<RowSelector>>,
    /// Cached result of evaluating some column(s) with the current RowSelection
    cached_predicate_result: Option<CachedPredicateResult>,
}

impl ReadPlan {
    /// Returns a mutable reference to the selection, if any
    pub(crate) fn selection_mut(&mut self) -> Option<&mut VecDeque<RowSelector>> {
        self.selection.as_mut()
    }

    /// Returns the current cached predicate result, if any
    pub(crate) fn cached_predicate_result(&self) -> Option<&CachedPredicateResult> {
        self.cached_predicate_result.as_ref()
    }

    /// Return the number of rows to read in each output batch
    #[inline(always)]
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }
}
