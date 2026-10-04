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
use crate::arrow::arrow_reader::{ParquetRecordBatchReader, RowGroupPlan};
use crate::arrow::push_decoder::reader_builder::{
    RowGroupBuildResult, RowGroupReaderBuilder, RowGroupReaderBuilderParts,
};
use crate::arrow::push_decoder::scan_plan::{NextRowGroup, RowBudget, RowGroupFrontier, ScanPlan};
use crate::errors::ParquetError;
use crate::file::metadata::ParquetMetaData;
use arrow_schema::SchemaRef;
use bytes::Bytes;
use std::ops::Range;
use std::sync::Arc;

/// State machine that tracks the remaining high level chunks (row groups) of
/// Parquet data left to read.
///
/// [`RowGroupFrontier`] owns cross-row-group scan state and selects the next
/// work item. [`RowGroupReaderBuilder`] owns decoding for the active row group.
#[derive(Debug)]
pub(crate) struct RemainingRowGroups {
    /// The arrow schema of the decoded output. Carried only so
    /// [`Self::into_parts`] can hand it to a rebuilt builder; unused while
    /// decoding.
    schema: SchemaRef,

    /// Cross-row-group scan state for queued work.
    frontier: RowGroupFrontier,

    /// State for building the reader for the current row group
    row_group_reader_builder: RowGroupReaderBuilder,

    /// The row group that the reader builder is fetching, for
    /// [`Self::scan_plan`]. `None` when the builder requests no more bytes.
    active_row_group: Option<NextRowGroup>,
}

/// The state recovered from a [`RemainingRowGroups`] by
/// [`RemainingRowGroups::into_parts`], describing the row groups *not* yet
/// decoded so a builder reconstructed from it resumes where the decoder left off.
#[derive(Debug)]
pub(crate) struct RemainingRowGroupsParts {
    /// The arrow schema of the decoded output.
    pub schema: SchemaRef,
    /// The Parquet file metadata.
    pub metadata: Arc<ParquetMetaData>,
    /// Row groups and selections not yet handed to the reader builder.
    pub row_group_plan: RowGroupPlan,
    /// Offset still to be skipped before the next readable row group.
    pub offset: Option<usize>,
    /// Output rows still permitted across the remaining row groups.
    pub limit: Option<usize>,
    /// Builder-configurable parts of the inner row-group reader builder.
    pub reader_builder: RowGroupReaderBuilderParts,
}

impl RemainingRowGroups {
    pub fn new(
        schema: SchemaRef,
        parquet_metadata: Arc<ParquetMetaData>,
        row_group_plan: RowGroupPlan,
        budget: RowBudget,
        has_predicates: bool,
        row_group_reader_builder: RowGroupReaderBuilder,
    ) -> Result<Self, ParquetError> {
        Ok(Self {
            schema,
            frontier: RowGroupFrontier::new(
                parquet_metadata,
                row_group_plan,
                budget,
                has_predicates,
            )?,
            row_group_reader_builder,
            active_row_group: None,
        })
    }

    /// Decompose into [`RemainingRowGroupsParts`].
    ///
    /// Must be called at a row-group boundary (see
    /// [`Self::is_at_row_group_boundary`]). The inner reader builder's runtime
    /// decode state is discarded; its buffered bytes are carried through.
    pub(crate) fn into_parts(self) -> RemainingRowGroupsParts {
        let Self {
            schema,
            frontier,
            row_group_reader_builder,
            active_row_group: _,
        } = self;
        // `has_predicates` is recomputed by `build()` from the filter.
        let (parquet_metadata, row_group_plan, budget) = frontier.into_parts();
        RemainingRowGroupsParts {
            schema,
            metadata: parquet_metadata,
            row_group_plan,
            offset: budget.offset(),
            limit: budget.limit(),
            reader_builder: row_group_reader_builder.into_parts(),
        }
    }

    /// See [`super::ScanPlan`]. Plans the active row group in full, then the
    /// row groups that the frontier has not handed over.
    pub(super) fn scan_plan(&self) -> ScanPlan {
        self.row_group_reader_builder
            .scan_plan_builder(self.frontier.clone())
            .with_active_row_group(self.active_row_group.clone())
            .build()
    }

    /// Push new data buffers that can be used to satisfy pending requests
    pub fn push_data(
        &mut self,
        ranges: Vec<Range<u64>>,
        buffers: Vec<Bytes>,
    ) -> Result<(), ParquetError> {
        self.row_group_reader_builder.push_data(ranges, buffers)
    }

    /// Return the total number of bytes buffered so far
    pub fn buffered_bytes(&self) -> u64 {
        self.row_group_reader_builder.buffered_bytes()
    }

    /// Clear any staged ranges currently buffered for future decode work
    pub fn clear_all_ranges(&mut self) {
        self.row_group_reader_builder.clear_all_ranges();
    }

    /// True iff the inner row-group reader is between row groups (state
    /// `Finished`). Forward to [`RowGroupReaderBuilder::is_finished`].
    pub fn is_at_row_group_boundary(&self) -> bool {
        self.row_group_reader_builder.is_finished()
    }

    /// Number of row groups remaining (not including the one currently
    /// being decoded).
    pub fn row_groups_remaining(&self) -> usize {
        self.frontier.row_groups_remaining()
    }

    /// Peek at the file-level row-group index that the next call to
    /// [`Self::try_next_reader`] will produce a reader for, after
    /// simulating the same skip logic [`Self::try_next_reader`] applies
    /// internally (row-selection emptiness + offset/limit budget). Does
    /// not mutate state.
    ///
    /// Returns `None` when the active row group is still being decoded,
    /// when no row groups remain, or when every remaining row group
    /// would be skipped under the current selection/budget.
    ///
    /// Cost: one clone of the queued row-group plan and selections per call
    /// (the frontier is cloned so the real advance logic can run
    /// non-destructively). For callers that peek once per row-group boundary
    /// this is O(remaining row groups + selectors) per boundary.
    pub fn peek_next_row_group(&self) -> Result<Option<usize>, ParquetError> {
        if self.row_group_reader_builder.has_active_row_group() {
            return Ok(None);
        }
        self.frontier.peek_next_row_group()
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
                    Some(next_row_group) => {
                        self.active_row_group = Some(next_row_group.clone());
                        let NextRowGroup {
                            row_group_idx,
                            row_count,
                            selection,
                            budget,
                        } = next_row_group;
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
                    self.active_row_group = None;
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
                    self.active_row_group = None;
                    self.frontier
                        .update_budget_after_row_group(remaining_budget);
                    // ready to read the row group
                    return Ok(DecodeResult::Data(batch_reader));
                }
            }
        }
    }
}
