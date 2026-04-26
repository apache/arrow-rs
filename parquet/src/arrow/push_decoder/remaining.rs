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

use crate::DecodeResult;
use crate::arrow::arrow_reader::{ParquetRecordBatchReader, RowSelection};
use crate::arrow::push_decoder::reader_builder::{
    RowBudget, RowGroupBuildResult, RowGroupReaderBuilder,
};
use crate::errors::ParquetError;
use crate::file::metadata::ParquetMetaData;
use bytes::Bytes;
use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;

/// Plan for the next queued row group after row-selection slicing.
#[derive(Debug)]
enum QueuedRowGroupDecision {
    /// Hand this row group to the builder.
    Read(NextRowGroup),
    /// Skip this row group, and keep scanning with the updated budget.
    Skip { remaining_budget: RowBudget },
}

/// Work item handed from [`RowGroupFrontier`] to [`RowGroupReaderBuilder`].
#[derive(Debug)]
struct NextRowGroup {
    row_group_idx: usize,
    row_count: usize,
    selection: Option<RowSelection>,
    budget: RowBudget,
}

#[derive(Debug)]
struct RowGroupFrontier {
    /// Metadata used to resolve row counts for queued row groups.
    parquet_metadata: Arc<ParquetMetaData>,
    /// Row group indices not yet handed to the builder.
    row_groups: VecDeque<usize>,
    /// Cross-row-group cursor for the optional global row selection.
    selection: Option<RowSelection>,
    /// Offset/limit budget before the next readable row group is planned.
    budget: RowBudget,
    /// If predicates are present, row groups with selected rows must be read so
    /// the predicate can decide whether they are actually needed.
    has_predicates: bool,
}

impl RowGroupFrontier {
    fn new(
        parquet_metadata: Arc<ParquetMetaData>,
        row_groups: Vec<usize>,
        selection: Option<RowSelection>,
        budget: RowBudget,
        has_predicates: bool,
    ) -> Self {
        Self {
            parquet_metadata,
            row_groups: VecDeque::from(row_groups),
            selection,
            budget,
            has_predicates,
        }
    }

    fn row_group_num_rows(&self, row_group_idx: usize) -> Result<usize, ParquetError> {
        self.parquet_metadata
            .row_group(row_group_idx)
            .num_rows()
            .try_into()
            .map_err(|e| ParquetError::General(format!("Row count overflow: {e}")))
    }

    fn update_budget_after_row_group(&mut self, budget: RowBudget) {
        self.budget = budget;
    }

    fn clear_remaining(&mut self) {
        self.selection = None;
        self.row_groups.clear();
    }

    /// Plan whether a selected row group should be read or skipped.
    ///
    /// Selection-only skips are handled before this method is called. This
    /// method applies the remaining offset/limit budget and predicate
    /// conservatism.
    fn plan_selected_row_group(
        &self,
        next_row_group: NextRowGroup,
        selected_rows: usize,
    ) -> QueuedRowGroupDecision {
        if self.has_predicates {
            return QueuedRowGroupDecision::Read(next_row_group);
        }

        let rows_after_budget = self.budget.rows_after(selected_rows);
        if rows_after_budget != 0 {
            return QueuedRowGroupDecision::Read(next_row_group);
        }

        QueuedRowGroupDecision::Skip {
            remaining_budget: self.budget.advance(selected_rows, rows_after_budget),
        }
    }

    /// Advance queued row groups until one should be handed to the builder.
    fn next_readable_row_group(&mut self) -> Result<Option<NextRowGroup>, ParquetError> {
        loop {
            let Some(&row_group_idx) = self.row_groups.front() else {
                return Ok(None);
            };
            if self.budget.is_exhausted()
                || self
                    .selection
                    .as_ref()
                    .is_some_and(|selection| selection.row_count() == 0)
            {
                self.clear_remaining();
                return Ok(None);
            }

            let row_count = self.row_group_num_rows(row_group_idx)?;
            let (selection, selected_rows) = match self.selection.as_mut() {
                Some(selection) => {
                    let selection = selection.split_off(row_count);
                    let selected_rows = selection.row_count();
                    if selected_rows == 0 {
                        self.row_groups.pop_front();
                        continue;
                    }

                    let selection = if selected_rows == row_count {
                        None
                    } else {
                        Some(selection)
                    };
                    (selection, selected_rows)
                }
                None => (None, row_count),
            };

            let next_row_group = NextRowGroup {
                row_group_idx,
                row_count,
                selection,
                budget: self.budget,
            };

            match self.plan_selected_row_group(next_row_group, selected_rows) {
                QueuedRowGroupDecision::Read(next_row_group) => {
                    self.row_groups.pop_front();
                    return Ok(Some(next_row_group));
                }
                QueuedRowGroupDecision::Skip { remaining_budget } => {
                    self.row_groups.pop_front();
                    self.budget = remaining_budget;
                }
            }
        }
    }
}

/// State machine that tracks the remaining high level chunks (row groups) of
/// Parquet data left to read.
///
/// [`RowGroupFrontier`] owns cross-row-group scan state and selects the next
/// work item. [`RowGroupReaderBuilder`] owns decoding for the active row group.
#[derive(Debug)]
pub(crate) struct RemainingRowGroups {
    /// Cross-row-group scan state for queued work.
    frontier: RowGroupFrontier,

    /// State for building the reader for the current row group
    row_group_reader_builder: RowGroupReaderBuilder,
}

impl RemainingRowGroups {
    pub fn new(
        parquet_metadata: Arc<ParquetMetaData>,
        row_groups: Vec<usize>,
        selection: Option<RowSelection>,
        budget: RowBudget,
        has_predicates: bool,
        row_group_reader_builder: RowGroupReaderBuilder,
    ) -> Self {
        Self {
            frontier: RowGroupFrontier::new(
                parquet_metadata,
                row_groups,
                selection,
                budget,
                has_predicates,
            ),
            row_group_reader_builder,
        }
    }

    /// Push new data buffers that can be used to satisfy pending requests
    pub fn push_data(&mut self, ranges: Vec<Range<u64>>, buffers: Vec<Bytes>) {
        self.row_group_reader_builder.push_data(ranges, buffers);
    }

    /// Return the total number of bytes buffered so far
    pub fn buffered_bytes(&self) -> u64 {
        self.row_group_reader_builder.buffered_bytes()
    }

    /// Clear any staged ranges currently buffered for future decode work
    pub fn clear_all_ranges(&mut self) {
        self.row_group_reader_builder.clear_all_ranges();
    }

    /// returns [`ParquetRecordBatchReader`] suitable for reading the next
    /// group of rows from the Parquet data, or the list of data ranges still
    /// needed to proceed
    pub fn try_next_reader(
        &mut self,
    ) -> Result<DecodeResult<ParquetRecordBatchReader>, ParquetError> {
        loop {
            if !self.row_group_reader_builder.has_active_row_group() {
                // We are done with the previous row group, seek to the next one
                // from the frontier, if any.

                match self.frontier.next_readable_row_group()? {
                    Some(NextRowGroup {
                        row_group_idx,
                        row_count,
                        selection,
                        budget,
                    }) => {
                        self.row_group_reader_builder.next_row_group(
                            row_group_idx,
                            row_count,
                            selection,
                            budget,
                        )?;
                    }
                    None => return Ok(DecodeResult::Finished),
                }
            }

            match self.row_group_reader_builder.try_build()? {
                RowGroupBuildResult::Finished { remaining_budget } => {
                    self.frontier
                        .update_budget_after_row_group(remaining_budget);
                    // reader is done, proceed to the next row group
                }
                RowGroupBuildResult::NeedsData(ranges) => {
                    // need more data to proceed
                    return Ok(DecodeResult::NeedsData(ranges));
                }
                RowGroupBuildResult::Data {
                    batch_reader,
                    remaining_budget,
                } => {
                    self.frontier
                        .update_budget_after_row_group(remaining_budget);
                    // ready to read the row group
                    return Ok(DecodeResult::Data(batch_reader));
                }
            }
        }
    }
}
