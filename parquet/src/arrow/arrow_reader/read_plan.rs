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
use crate::arrow::arrow_reader::{
    ArrowPredicate, ParquetRecordBatchReader, RowSelection, RowSelector,
};
use crate::errors::{ParquetError, Result};
use arrow_array::Array;
use arrow_select::filter::prep_null_mask_filter;
use std::collections::VecDeque;
use arrow_buffer::BooleanBuffer;

/// A builder for [`ReadPlan`]
#[derive(Clone)]
pub(crate) struct ReadPlanBuilder {
    batch_size: usize,
    /// Current to apply, includes all filters
    selection: Option<RowSelection>,
}

impl ReadPlanBuilder {
    /// Create a `ReadPlanBuilder` with the given batch size
    pub(crate) fn new(batch_size: usize) -> Self {
        Self {
            batch_size,
            selection: None,
        }
    }

    /// Set the current selection to the given value
    pub(crate) fn with_selection(mut self, selection: Option<RowSelection>) -> Self {
        self.selection = selection;
        self
    }

    /// Returns the current selection, if any
    pub(crate) fn selection(&self) -> Option<&RowSelection> {
        self.selection.as_ref()
    }

    /// Returns a [`LimitedReadPlanBuilder`] to apply offset and limit to the in
    /// progress plan.
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
    pub(crate) fn num_rows_selected(&self) -> Option<usize> {
        self.selection.as_ref().map(|s| s.row_count())
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
    pub(crate) fn with_predicate(
        mut self,
        array_reader: Box<dyn ArrayReader>,
        predicate: &mut dyn ArrowPredicate,
    ) -> Result<Self> {
        let reader = ParquetRecordBatchReader::new(array_reader, self.clone().build());
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

        let raw = RowSelection::from_filters(&filters);
        self.selection = match self.selection.take() {
            Some(selection) => Some(selection.and_then(&raw)),
            None => Some(raw),
        };
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
        } = self;

        // If the batch size is 0, read "all rows"
        if batch_size == 0 {
            return ReadPlan::All { batch_size: 0 };
        }

        // If no selection is provided, read all rows
        let Some(selection) = selection else {
            return ReadPlan::All { batch_size };
        };

        let iterator = SelectionIterator::new(batch_size, selection.into());
        ReadPlan::Subset { iterator }
    }
}

/// How to select the next batch of rows to read from the Parquet file
///
/// This allows the reader to dynamically choose between decoding strategies
pub(crate) enum ReadStep {
    /// Read n rows
    Read(usize),
    /// Skip n rows
    Skip(usize),
    /// Reads mask.len() rows then applies the filter mask to select just the desired
    /// rows.
    ///
    /// Any row with a 1 value in the mask will be selected and included
    /// in the output batch.
    ///
    /// This is used in situations where the overhead of preferentially decoding
    /// only the selected rows is higher than decoding all rows and then
    /// applying a mask via filter.
    Mask(BooleanBuffer),
}

impl From<RowSelector> for ReadStep {
    fn from(value: RowSelector) -> Self {
        if value.skip {
            Self::Skip(value.row_count)
        } else {
            Self::Read(value.row_count)
        }
    }
}

/// Incrementally returns [`RowSelector`]s that describe reading from a Parquet file.
///
/// The returned stream of [`RowSelector`]s is guaranteed to have:
/// 1. No empty selections (that select no rows)
/// 2. No selections that span batch_size boundaries
/// 3. No trailing skip selections
///
/// For example, if the `batch_size` is 100 and we are selecting all 200 rows
/// from a Parquet file, the selectors will be:
/// - `RowSelector::select(100)  <-- forced break at batch_size boundary`
/// - `RowSelector::select(100)`
#[derive(Debug, Clone)]
pub(crate) struct SelectionIterator {
    /// how many rows to read in each batch
    batch_size: usize,
    /// how many records have been read by RowSelection in the "current" batch
    read_records: usize,
    /// Input selectors to read from
    input_selectors: VecDeque<RowSelector>,
}

impl Iterator for SelectionIterator {
    type Item = ReadStep;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(mut front) = self.input_selectors.pop_front() {
            // RowSelectors with row_count = 0 terminate the read, so skip such
            // entries. See https://github.com/apache/arrow-rs/issues/2669
            if front.row_count == 0 {
                continue;
            }

            if front.skip {
                return Some(ReadStep::from(front));
            }

            let need_read = self.batch_size - self.read_records;

            // if there are more rows in the current RowSelector than needed to
            // finish the batch, split it up
            if front.row_count > need_read {
                // Part 1: return remaining rows to the front of the queue
                let remaining = front.row_count - need_read;
                self.input_selectors
                    .push_front(RowSelector::select(remaining));
                // Part 2: adjust the current selector to read the rows we need
                front.row_count = need_read;
            }

            self.read_records += front.row_count;
            // if read enough records to complete a batch, emit
            if self.read_records == self.batch_size {
                self.read_records = 0;
            }

            return Some(ReadStep::from(front));
        }
        // no more selectors to read, end of stream
        None
    }
}

impl SelectionIterator {
    fn new(batch_size: usize, mut input_selectors: VecDeque<RowSelector>) -> Self {
        // trim any trailing empty selectors
        while input_selectors.back().map(|x| x.skip).unwrap_or(false) {
            input_selectors.pop_back();
        }

        Self {
            batch_size,
            read_records: 0,
            input_selectors,
        }
    }

    /// Return the number of rows to read in each output batch
    fn batch_size(&self) -> usize {
        self.batch_size
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

/// A plan for reading specific rows from a Parquet Row Group.
///
/// See [`ReadPlanBuilder`] to create `ReadPlan`s
///
/// Also, note the `ReadPlan` is an iterator over [`RowSelector`]s.
#[derive(Debug)]
pub(crate) enum ReadPlan {
    /// Read all rows in `batch_sized` chunks
    All {
        /// The number of rows to read in each batch
        batch_size: usize,
    },
    /// Read only a specific subset of rows
    Subset { iterator: SelectionIterator },
}

impl Iterator for ReadPlan {
    type Item = ReadStep;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            // If we are reading all rows, return a selector that selects
            // the next batch_size rows
            Self::All { batch_size } => Some(ReadStep::Read(*batch_size)),
            Self::Subset { iterator } => iterator.next(),
        }
    }
}

impl ReadPlan {
    /// Return the number of rows to read in each output batch
    #[inline(always)]
    pub fn batch_size(&self) -> usize {
        match self {
            Self::All { batch_size } => *batch_size,
            Self::Subset { iterator } => iterator.batch_size(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_plan_select_all() {
        TestCase::new()
            .with_batch_size(100)
            .with_empty_initial_selection()
            .with_empty_expected_selection()
            .run()
    }

    #[test]
    fn test_read_plan_empty_batch_size() {
        TestCase::new()
            .with_batch_size(0)
            .with_row_count(0)
            .with_empty_initial_selection()
            .with_empty_expected_selection()
            .run()
    }

    #[test]
    fn test_read_plan_select_only_empty() {
        TestCase::new()
            .with_batch_size(100)
            .with_initial_selection(Some([RowSelector::skip(0)]))
            .with_expected_selection(Some([]))
            .run()
    }

    #[test]
    fn test_read_plan_select_subset() {
        TestCase::new()
            .with_batch_size(100)
            .with_row_count(300)
            // filter out 50 rows in the middle
            .with_initial_selection(Some([
                RowSelector::select(150),
                RowSelector::skip(50),
                RowSelector::select(100),
            ]))
            .with_expected_selection(Some([
                // broken up into batch_size chunks
                RowSelector::select(100),
                // second batch
                RowSelector::select(50),
                RowSelector::skip(50),
                RowSelector::select(50),
                // third batch has 50 as we filtered out 50 rows
                RowSelector::select(50),
            ]))
            .run()
    }

    #[test]
    fn test_read_plan_select_batch_boundaries() {
        TestCase::new()
            .with_batch_size(100)
            .with_row_count(300)
            // select all but 50 rows in the middle using 50 row batches
            .with_initial_selection(Some([
                RowSelector::select(50),
                RowSelector::select(25),
                RowSelector::select(25),
                RowSelector::select(50),
                RowSelector::skip(10),
                RowSelector::skip(30),
                RowSelector::skip(10),
                RowSelector::select(50),
                RowSelector::select(50),
            ]))
            .with_expected_selection(Some([
                // broken up into batch_size chunks, combined
                RowSelector::select(100),
                // second batch
                RowSelector::select(50),
                RowSelector::skip(50),
                RowSelector::select(50),
                // third batch
                RowSelector::select(50),
            ]))
            .run()
    }

    #[test]
    fn test_read_plan_filters_zero_row_selects() {
        TestCase::new()
            .with_batch_size(100)
            .with_row_count(300)
            .with_initial_selection(Some([
                RowSelector::select(0),
                RowSelector::select(125),
                RowSelector::select(0),
                RowSelector::skip(0),
                RowSelector::skip(50),
                RowSelector::select(25),
            ]))
            // empty selectors have been filtered out
            .with_expected_selection(Some([
                RowSelector::select(100),
                RowSelector::select(25),
                RowSelector::skip(50),
                RowSelector::select(25),
            ]))
            .run()
    }

    #[test]
    fn test_read_plan_filters_zero_row_end_skips() {
        TestCase::new()
            .with_batch_size(100)
            .with_row_count(300)
            .with_initial_selection(Some([
                RowSelector::select(125),
                RowSelector::skip(0),
                RowSelector::skip(0),
            ]))
            .with_expected_selection(Some([RowSelector::select(100), RowSelector::select(25)]))
            .run()
    }

    #[test]
    fn test_read_plan_with_limit_no_skip() {
        TestCase::new()
            .with_batch_size(100)
            .with_row_count(300)
            .with_initial_selection(Some([
                RowSelector::select(200), // limit in middle of this select
                RowSelector::skip(50),
                RowSelector::select(50),
            ]))
            .with_limit(100)
            .with_expected_selection(Some([RowSelector::select(100)]))
            .run()
    }

    #[test]
    fn test_read_plan_with_limit_after_skip() {
        TestCase::new()
            .with_batch_size(100)
            .with_row_count(300)
            .with_initial_selection(Some([
                RowSelector::select(150),
                RowSelector::skip(50), // limit is hit after this skip
                RowSelector::select(10),
            ]))
            .with_limit(200)
            .with_expected_selection(Some([
                RowSelector::select(100),
                RowSelector::select(50),
                RowSelector::skip(50),
                RowSelector::select(10),
            ]))
            .run()
    }

    #[test]
    fn test_read_plan_with_limit_after_skip_remain() {
        TestCase::new()
            .with_batch_size(100)
            .with_row_count(300)
            .with_initial_selection(Some([
                RowSelector::select(150),
                RowSelector::skip(50),
                RowSelector::select(100), // limit includes part but not all of this
            ]))
            .with_limit(175)
            .with_expected_selection(Some([
                RowSelector::select(100),
                RowSelector::select(50),
                RowSelector::skip(50),
                RowSelector::select(25),
            ]))
            .run()
    }

    #[test]
    fn test_read_plan_with_offset() {
        TestCase::new()
            .with_batch_size(100)
            .with_row_count(300)
            .with_initial_selection(Some([
                RowSelector::select(50),
                RowSelector::skip(50),
                RowSelector::select(100),
            ]))
            .with_offset(25) // skip 25 rows
            .with_expected_selection(Some([
                RowSelector::skip(25), // offset
                RowSelector::select(25),
                RowSelector::skip(50),
                RowSelector::select(75),
                // start second batch
                RowSelector::select(25),
            ]))
            .run()
    }

    #[test]
    fn test_read_plan_with_limit_and_offset() {
        TestCase::new()
            .with_batch_size(100)
            .with_row_count(300)
            .with_initial_selection(Some([
                RowSelector::select(50),
                RowSelector::skip(50),
                RowSelector::select(100),
            ]))
            .with_offset(25) // skip 25 rows
            .with_limit(110)
            .with_expected_selection(Some([
                RowSelector::skip(25), // offset
                RowSelector::select(25),
                RowSelector::skip(50),
                RowSelector::select(75),
                // start second batch
                RowSelector::select(10), // limited to 110
            ]))
            .run()
    }

    // test filtering

    /// Test harness for `ReadPlanBuilder`
    #[derive(Debug, Default)]
    struct TestCase {
        batch_size: usize,
        row_count: usize,
        /// Optional limit to apply to plan
        limit: Option<usize>,
        /// Optional offset to apply to plan
        offset: Option<usize>,
        initial_selection: Option<Vec<RowSelector>>,
        /// if Some, expect ReadPlan::Subset
        /// if None, expect ReadPlan::All
        expected_selection: Option<Vec<RowSelector>>,
    }

    impl TestCase {
        /// Create a new test case
        fn new() -> Self {
            Default::default()
        }

        /// Set the batch size
        fn with_batch_size(mut self, batch_size: usize) -> Self {
            self.batch_size = batch_size;
            self
        }

        /// Set the row count
        fn with_row_count(mut self, row_count: usize) -> Self {
            self.row_count = row_count;
            self
        }

        /// Specify a limit to apply to the read plan
        fn with_limit(mut self, limit: usize) -> Self {
            self.limit = Some(limit);
            self
        }

        /// Specify an offset to apply to the read plan
        fn with_offset(mut self, offset: usize) -> Self {
            self.offset = Some(offset);
            self
        }

        /// Set the initial selection to the given set of selectors
        fn with_initial_selection<I: IntoIterator<Item = RowSelector>>(
            mut self,
            initial: Option<I>,
        ) -> Self {
            self.initial_selection = initial.map(|initial| initial.into_iter().collect());
            self
        }
        /// Set the initial selection to None (used to make the tests self documenting)
        fn with_empty_initial_selection(mut self) -> Self {
            self.initial_selection = None;
            self
        }

        /// Set the expected plan to be RowPlan::Subset given set of selectors
        fn with_expected_selection<I: IntoIterator<Item = RowSelector>>(
            mut self,
            expected: Option<I>,
        ) -> Self {
            self.expected_selection = expected.map(|expected| expected.into_iter().collect());
            self
        }
        /// Set the expected selection to None (used to make the tests self documenting)
        fn with_empty_expected_selection(mut self) -> Self {
            self.expected_selection = None;
            self
        }

        fn run(self) {
            let Self {
                batch_size,
                row_count,
                limit,
                offset,
                initial_selection,
                expected_selection,
            } = self;

            let initial_selection = initial_selection.map(RowSelection::from);
            let plan = ReadPlanBuilder::new(batch_size)
                .with_selection(initial_selection)
                .limited(row_count)
                .with_limit(limit)
                .with_offset(offset)
                .build_limited()
                .build();

            match expected_selection {
                None => {
                    let expected_batch_size = batch_size;
                    assert!(
                        matches!(plan, ReadPlan::All { batch_size } if batch_size == expected_batch_size),
                        "Expected ReadPlan::All {{ batch_size={batch_size} }}, got {plan:#?}"
                    );
                }
                Some(expected) => {
                    // Gather the generated selectors to compare with the expected
                    let actual: Vec<RowSelector> = plan.into_iter()
                        .map(|read_step| {
                            match read_step{
                                ReadStep::Read(n) => RowSelector::select(n),
                                ReadStep::Skip(n) => RowSelector::skip(n),
                                ReadStep::Mask(mask) => {
                                    todo!()
                                }
                            }
                        })
                        .collect();
                    Self::validate_selection(&actual, batch_size);
                    // use debug formatting with newlines to generate easier to grok diffs
                    // if the test fails
                    assert_eq!(format!("{actual:#?}"), format!("{expected:#?}"));
                    // also use assert_eq! to ensure equality
                    assert_eq!(actual, expected);
                }
            };
        }

        /// Validate that the output selections obey the rules
        fn validate_selection(selectors: &[RowSelector], batch_size: usize) {
            // 1. no empty selections
            for selector in selectors.iter() {
                assert!(selector.row_count > 0, "{selector:?} empty selection");
            }

            // 2. no selections that span batch_size boundaries
            let mut current_count = 0;
            for selector in selectors.iter() {
                if selector.skip {
                    continue;
                }
                current_count += selector.row_count;
                assert!(
                    current_count <= batch_size,
                    "current_count {current_count} > batch_size {batch_size}. Plan:\n{selectors:#?}"
                );
                if current_count == batch_size {
                    current_count = 0;
                }
            }

            // 3. no trailing skip selections
            if let Some(last) = selectors.last() {
                assert!(!last.skip, "last selector {last:?} is a skip selector");
            }
        }
    }
}
