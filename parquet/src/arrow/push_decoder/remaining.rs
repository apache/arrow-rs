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
use crate::arrow::arrow_reader::{
    ParquetRecordBatchReader, RowGroupPlan, RowGroupSelection, RowSelection,
};
use crate::arrow::push_decoder::reader_builder::{
    RowBudget, RowGroupBuildResult, RowGroupReaderBuilder, RowGroupReaderBuilderParts,
};
use crate::errors::ParquetError;
use crate::file::metadata::ParquetMetaData;
use arrow_schema::SchemaRef;
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
    /// This row group's selection, or `None` when all rows are selected.
    selection: Option<RowSelection>,
    /// Budget snapshot to apply while decoding this row group.
    budget: RowBudget,
}

/// Row groups and selections that have not yet been handed to the row-group
/// reader builder.
#[derive(Debug, Clone)]
enum QueuedRowGroups {
    /// One selection cursor spans all queued row groups.
    Global {
        row_groups: VecDeque<usize>,
        selection: Option<RowSelection>,
    },
    /// Selections are already relative to their respective row groups.
    PerRowGroup(VecDeque<RowGroupSelection>),
}

impl QueuedRowGroups {
    /// Validate and queue a row-group plan for `parquet_metadata`.
    fn try_new(
        parquet_metadata: &ParquetMetaData,
        row_group_plan: RowGroupPlan,
    ) -> Result<Self, ParquetError> {
        match row_group_plan {
            RowGroupPlan::Global {
                row_groups,
                selection,
            } => Ok(Self::Global {
                row_groups: row_groups
                    .unwrap_or_else(|| (0..parquet_metadata.num_row_groups()).collect())
                    .into(),
                selection,
            }),
            RowGroupPlan::PerRowGroup(row_groups) => {
                for row_group in &row_groups {
                    let row_count =
                        parquet_metadata.row_group_num_rows(row_group.row_group_index)?;
                    if let Some(selection) = &row_group.selection {
                        let selection_rows = selection.total_row_count();
                        if selection_rows > row_count {
                            return Err(ParquetError::General(format!(
                                "Row selection for row group {} contains {selection_rows} rows, but the row group has {row_count}",
                                row_group.row_group_index
                            )));
                        }
                    }
                }
                Ok(Self::PerRowGroup(row_groups.into()))
            }
            RowGroupPlan::Conflicting => Err(RowGroupPlan::conflict_error()),
        }
    }

    /// Convert the remaining queue back into a builder configuration.
    fn into_plan(self) -> RowGroupPlan {
        match self {
            Self::Global {
                row_groups,
                selection,
            } => RowGroupPlan::Global {
                row_groups: Some(Vec::from(row_groups)),
                selection,
            },
            Self::PerRowGroup(row_groups) => RowGroupPlan::PerRowGroup(Vec::from(row_groups)),
        }
    }

    fn front(&self) -> Option<usize> {
        match self {
            Self::Global { row_groups, .. } => row_groups.front().copied(),
            Self::PerRowGroup(row_groups) => row_groups
                .front()
                .map(|row_group| row_group.row_group_index),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Global { row_groups, .. } => row_groups.len(),
            Self::PerRowGroup(row_groups) => row_groups.len(),
        }
    }

    fn row_group_indices(&self) -> Vec<usize> {
        match self {
            Self::Global { row_groups, .. } => row_groups.iter().copied().collect(),
            Self::PerRowGroup(row_groups) => row_groups
                .iter()
                .map(|row_group| row_group.row_group_index)
                .collect(),
        }
    }

    fn clear(&mut self) {
        match self {
            Self::Global {
                row_groups,
                selection,
            } => {
                row_groups.clear();
                *selection = None;
            }
            Self::PerRowGroup(row_groups) => row_groups.clear(),
        }
    }

    /// Returns `true` when a shared global selection has no selected rows left.
    /// Per-row-group selections are independent and are drained one at a time.
    fn global_selection_is_exhausted(&self) -> bool {
        matches!(
            self,
            Self::Global {
                selection: Some(selection),
                ..
            } if selection.row_count() == 0
        )
    }

    /// Remove the front row group and return its local selection.
    fn pop_front_selection(&mut self, row_count: usize) -> Option<RowSelection> {
        match self {
            Self::Global {
                row_groups,
                selection,
            } => {
                let popped = row_groups.pop_front();
                debug_assert!(popped.is_some(), "front row group checked before pop");
                selection
                    .as_mut()
                    .map(|selection| selection.split_off(row_count))
            }
            Self::PerRowGroup(row_groups) => {
                row_groups
                    .pop_front()
                    .expect("front row group checked before pop")
                    .selection
            }
        }
    }
}

#[derive(Debug, Clone)]
struct RowGroupFrontier {
    /// Metadata used to resolve row counts for queued row groups.
    parquet_metadata: Arc<ParquetMetaData>,
    /// Row groups not yet handed to the builder.
    queued: QueuedRowGroups,
    /// Offset/limit budget before the next readable row group is planned.
    budget: RowBudget,
    /// If predicates are present, row groups with selected rows must be read so
    /// the predicate can decide whether they are actually needed.
    has_predicates: bool,
}

impl RowGroupFrontier {
    fn new(
        parquet_metadata: Arc<ParquetMetaData>,
        row_group_plan: RowGroupPlan,
        budget: RowBudget,
        has_predicates: bool,
    ) -> Result<Self, ParquetError> {
        let queued = QueuedRowGroups::try_new(&parquet_metadata, row_group_plan)?;

        Ok(Self {
            parquet_metadata,
            queued,
            budget,
            has_predicates,
        })
    }

    fn update_budget_after_row_group(&mut self, budget: RowBudget) {
        self.budget = budget;
    }

    /// Peek at the next row-group index [`Self::next_readable_row_group`]
    /// would hand out, without mutating any state. Returns `None` if every
    /// remaining row group would be skipped under the current
    /// selection/budget, or if the queue is empty.
    ///
    /// Runs the real [`Self::next_readable_row_group`] advance logic on a
    /// throwaway clone of the frontier, so peek can never drift from the
    /// read path. The clone copies the queued row-group plan and selections;
    /// see
    /// [`RemainingRowGroups::peek_next_row_group`].
    fn peek_next_row_group(&self) -> Result<Option<usize>, ParquetError> {
        Ok(self
            .clone()
            .next_readable_row_group()?
            .map(|next_row_group| next_row_group.row_group_idx))
    }

    fn clear_remaining(&mut self) {
        self.queued.clear();
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
        self.next_readable_row_group_with_skips()
            .map(|(next_row_group, _)| next_row_group)
    }

    /// Advance queued row groups, returning occurrences skipped before the
    /// next readable row group so their retained bytes can be released even
    /// though they never enter the row-group reader state machine.
    fn next_readable_row_group_with_skips(
        &mut self,
    ) -> Result<(Option<NextRowGroup>, Vec<usize>), ParquetError> {
        let mut skipped = Vec::new();
        loop {
            let Some(row_group_idx) = self.queued.front() else {
                return Ok((None, skipped));
            };
            // A global selection can be exhausted before its row-group queue.
            // Per-row-group selections have no shared cursor to exhaust; empty
            // local selections are discarded by the `selected_rows == 0` path below.
            if self.budget.is_exhausted() || self.queued.global_selection_is_exhausted() {
                skipped.extend(self.queued.row_group_indices());
                self.clear_remaining();
                return Ok((None, skipped));
            }

            let row_count = self.parquet_metadata.row_group_num_rows(row_group_idx)?;
            let selection = self.queued.pop_front_selection(row_count);
            let (selection, selected_rows) = match selection {
                Some(selection) => {
                    let selected_rows = selection.row_count();
                    if selected_rows == 0 {
                        skipped.push(row_group_idx);
                        continue;
                    }
                    // An all-rows selection is equivalent to no selection
                    (
                        (selected_rows != row_count).then_some(selection),
                        selected_rows,
                    )
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
                    return Ok((Some(next_row_group), skipped));
                }
                QueuedRowGroupDecision::Skip { remaining_budget } => {
                    skipped.push(row_group_idx);
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
    /// The arrow schema of the decoded output. Carried only so
    /// [`Self::into_parts`] can hand it to a rebuilt builder; unused while
    /// decoding.
    schema: SchemaRef,

    /// Cross-row-group scan state for queued work.
    frontier: RowGroupFrontier,

    /// State for building the reader for the current row group
    row_group_reader_builder: RowGroupReaderBuilder,
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
        } = self;
        // `has_predicates` is recomputed by `build()` from the filter.
        let RowGroupFrontier {
            parquet_metadata,
            queued,
            budget,
            has_predicates: _,
        } = frontier;
        let row_group_plan = queued.into_plan();
        RemainingRowGroupsParts {
            schema,
            metadata: parquet_metadata,
            row_group_plan,
            offset: budget.offset(),
            limit: budget.limit(),
            reader_builder: row_group_reader_builder.into_parts(),
        }
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

    #[cfg(test)]
    pub(crate) fn num_buffers(&self) -> usize {
        self.row_group_reader_builder.num_buffers()
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
        self.frontier.queued.len()
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

                let (next_row_group, skipped_row_groups) =
                    self.frontier.next_readable_row_group_with_skips()?;
                for row_group_idx in skipped_row_groups {
                    self.row_group_reader_builder
                        .release_row_group(row_group_idx);
                }

                match next_row_group {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::arrow_reader::RowSelector;
    use crate::arrow::push_decoder::test::test_file_parquet_metadata;

    fn global_plan(
        row_groups: Option<Vec<usize>>,
        selection: Option<RowSelection>,
    ) -> RowGroupPlan {
        RowGroupPlan::Global {
            row_groups,
            selection,
        }
    }

    #[test]
    fn queued_row_groups_encapsulates_plan_transitions() {
        let metadata = test_file_parquet_metadata();

        let mut all_row_groups =
            QueuedRowGroups::try_new(&metadata, global_plan(None, None)).unwrap();
        assert_eq!(all_row_groups.len(), 2);
        assert_eq!(all_row_groups.front(), Some(0));
        assert!(!all_row_groups.global_selection_is_exhausted());
        assert!(all_row_groups.pop_front_selection(200).is_none());
        assert_eq!(all_row_groups.front(), Some(1));
        all_row_groups.clear();
        assert_eq!(all_row_groups.len(), 0);
        assert!(matches!(
            all_row_groups.into_plan(),
            RowGroupPlan::Global {
                row_groups: Some(row_groups),
                selection: None,
            } if row_groups.is_empty()
        ));

        let global_selection = RowSelection::from(vec![
            RowSelector::skip(10),
            RowSelector::select(5),
            RowSelector::skip(185),
            RowSelector::select(200),
        ]);
        let mut global = QueuedRowGroups::try_new(
            &metadata,
            global_plan(Some(vec![0, 1]), Some(global_selection)),
        )
        .unwrap();
        let first = global.pop_front_selection(200).unwrap();
        assert_eq!(first.row_count(), 5);
        assert!(!global.global_selection_is_exhausted());
        assert!(matches!(
            global.into_plan(),
            RowGroupPlan::Global {
                row_groups: Some(row_groups),
                selection: Some(selection),
            } if row_groups == vec![1] && selection.row_count() == 200
        ));

        let local_selection =
            RowSelection::from(vec![RowSelector::skip(5), RowSelector::select(3)]);
        let mut local = QueuedRowGroups::try_new(
            &metadata,
            RowGroupPlan::PerRowGroup(vec![
                RowGroupSelection::new(1, Some(local_selection)),
                RowGroupSelection::new(0, None),
            ]),
        )
        .unwrap();
        assert_eq!(local.front(), Some(1));
        assert_eq!(local.pop_front_selection(200).unwrap().row_count(), 3);
        assert!(!local.global_selection_is_exhausted());
        assert!(matches!(
            local.into_plan(),
            RowGroupPlan::PerRowGroup(row_groups)
                if row_groups == vec![RowGroupSelection::new(0, None)]
        ));

        let exhausted = QueuedRowGroups::try_new(
            &metadata,
            global_plan(
                Some(vec![0]),
                Some(RowSelection::from(vec![RowSelector::skip(200)])),
            ),
        )
        .unwrap();
        assert!(exhausted.global_selection_is_exhausted());
    }

    #[test]
    fn frontier_handles_global_and_local_exhaustion() {
        let metadata = test_file_parquet_metadata();
        let budget = RowBudget::new(None, None);

        let mut global = RowGroupFrontier::new(
            Arc::clone(&metadata),
            global_plan(
                Some(vec![0, 1]),
                Some(RowSelection::from(vec![RowSelector::skip(400)])),
            ),
            budget,
            false,
        )
        .unwrap();
        assert!(global.next_readable_row_group().unwrap().is_none());
        assert_eq!(global.queued.len(), 0);

        let mut local = RowGroupFrontier::new(
            Arc::clone(&metadata),
            RowGroupPlan::PerRowGroup(vec![
                RowGroupSelection::new(0, Some(RowSelection::from(vec![RowSelector::skip(200)]))),
                RowGroupSelection::new(1, None),
            ]),
            budget,
            false,
        )
        .unwrap();
        let next = local.next_readable_row_group().unwrap().unwrap();
        assert_eq!(next.row_group_idx, 1);
        assert_eq!(next.row_count, 200);
        assert!(next.selection.is_none());

        let mut exhausted_budget = RowGroupFrontier::new(
            metadata,
            RowGroupPlan::PerRowGroup(vec![RowGroupSelection::new(0, None)]),
            RowBudget::new(None, Some(0)),
            false,
        )
        .unwrap();
        assert!(
            exhausted_budget
                .next_readable_row_group()
                .unwrap()
                .is_none()
        );
        assert_eq!(exhausted_budget.queued.len(), 0);
    }

    #[test]
    fn frontier_reports_invalid_global_row_group_while_peeking() {
        let metadata = test_file_parquet_metadata();
        let frontier = RowGroupFrontier::new(
            metadata,
            global_plan(Some(vec![2]), None),
            RowBudget::new(None, None),
            false,
        )
        .unwrap();

        let error = frontier.peek_next_row_group().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Row group index 2 out of bounds for file with 2 row groups")
        );
    }

    #[test]
    fn metadata_row_count_overflow_is_reported() {
        let metadata = test_file_parquet_metadata();
        let mut builder = metadata.as_ref().clone().into_builder();
        let mut row_groups = builder.take_row_groups();
        let negative_row_group = row_groups
            .remove(0)
            .into_builder()
            .set_num_rows(-1)
            .build()
            .unwrap();
        let metadata = builder.set_row_groups(vec![negative_row_group]).build();

        let error = metadata.row_group_num_rows(0).unwrap_err();
        assert!(error.to_string().contains("Row count overflow"));
    }
}
