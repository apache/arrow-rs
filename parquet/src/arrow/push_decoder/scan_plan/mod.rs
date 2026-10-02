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

//! [`ScanPlan`]: the byte ranges that a push decoder may still read.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::iter::FusedIterator;
use std::ops::Range;
use std::sync::Arc;

use super::reader_builder::StageSchedule;
use crate::arrow::arrow_reader::{ReadPlanBuilder, RowSelection};
use crate::arrow::in_memory_row_group::{
    ColumnFetch, column_selection, columns_to_fetch, dictionary_range, page_range,
};
use crate::errors::ParquetError;
use crate::file::metadata::page_index::PageIndexProvider;
use crate::file::page_index::offset_index::PageLocation;

mod budget;
mod frontier;

pub(crate) use budget::{BudgetedReadPlan, RowBudget};
pub(crate) use frontier::{NextRowGroup, RowGroupFrontier};

/// One item of a [`ScanPlan`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct PlannedRange {
    /// Byte range in the file.
    pub range: Range<u64>,
    /// Row group index in the file.
    pub row_group: usize,
    /// Leaf column index in the file.
    pub column: usize,
    /// What the range contains.
    pub kind: PageKind,
    /// First planned row that this range serves, counted from the start of
    /// the plan. Used only to order the plan.
    pub(crate) first_row: u64,
    /// One past the last planned row that this range serves.
    pub(crate) last_row: u64,
    /// The decoding stage that first reads this range.
    pub(crate) stage: ScanStage,
    /// `true` if a predicate result can make this range unnecessary.
    pub(crate) conditional: bool,
}

impl PlannedRange {
    /// Number of bytes in this range.
    pub fn len(&self) -> u64 {
        self.range.end - self.range.start
    }

    /// Returns `true` if this range contains no bytes.
    pub fn is_empty(&self) -> bool {
        self.range.is_empty()
    }
}

/// What a [`PlannedRange`] contains.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum PageKind {
    /// The dictionary page of a column chunk.
    Dictionary,
    /// One data page.
    Data,
    /// A complete column chunk. The plan uses this when the column has no
    /// offset index, so page locations are unknown.
    ColumnChunk,
}

/// The decoding stage that first reads a [`PlannedRange`].
///
/// The decoder reads a column once per row group. A column that a predicate
/// reads is not read again for the output.
pub(crate) use super::reader_builder::Stage as ScanStage;

/// The byte ranges that a push decoder may still read, in the order that
/// decoding needs them.
///
/// Created by [`ParquetPushDecoder::scan_plan`]. Use it to fetch data before
/// the decoder asks for it.
///
/// A `ScanPlan` is an estimate of the ranges that the decoder may need. The
/// ranges in [`DecodeResult::NeedsData`] from
/// [`ParquetPushDecoder::try_decode`] are the ranges that it actually needs.
///
/// * The plan contains every range that the decoder can request after
///   the plan is made. It can contain more, for example ranges that a
///   [`RowFilter`] makes unnecessary.
/// * The ranges are generated on demand, as you iterate. The plan does not
///   calculate a range until you ask for it.
/// * The plan comes from the current state of the decoder. To plan the
///   whole scan, call [`ParquetPushDecoder::scan_plan`] once after
///   [`ParquetPushDecoderBuilder::build`] and keep the iterator. If you
///   rebuild the decoder with [`ParquetPushDecoder::into_builder`], the old
///   plan is not valid for the new decoder. Call `scan_plan` again on the new
///   decoder.
/// * Row groups are in read order. In a row group, the columns of the
///   [`RowFilter`] predicates come first, then the other output columns. The
///   ranges are ordered by the first row that they serve, and the ranges of
///   one column chunk stay in file order.
/// * There is one range for each page if the column has an offset index,
///   and one range for the entire column chunk if not.
/// * The plan does not change decoding, does no I/O and does not depend
///   on pushed data. Its state does not grow with the number of pages.
/// * If the decoder will return an error for a row group, the plan ends
///   before that row group. An example is a row selection with more rows
///   than the row group.
///
/// Keep the fetched ranges in your own cache, and answer each `NeedsData`
/// with exactly the requested ranges. See [`ParquetPushDecoder::push_ranges`].
///
/// # Example
///
/// ```
/// # use std::collections::BTreeMap;
/// # use std::ops::Range;
/// # use bytes::Bytes;
/// # use arrow_array::record_batch;
/// # use parquet::DecodeResult;
/// # use parquet::arrow::ArrowWriter;
/// # use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
/// # use parquet::arrow::push_decoder::ParquetPushDecoderBuilder;
/// # use parquet::file::metadata::PageIndexPolicy;
/// # use parquet::file::properties::WriterProperties;
/// # let file = {
/// #   let mut buffer = vec![];
/// #   let batch = record_batch!(("a", Int32, [1, 2, 3, 4])).unwrap();
/// #   let props = WriterProperties::builder().set_max_row_group_row_count(Some(2)).build();
/// #   let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props)).unwrap();
/// #   writer.write(&batch).unwrap();
/// #   writer.close().unwrap();
/// #   Bytes::from(buffer)
/// # };
/// # let fetch = |range: &Range<u64>| file.slice(range.start as usize..range.end as usize);
/// # // Join cached ranges that cover `range`, or fetch it.
/// # let read = |cache: &BTreeMap<u64, Bytes>, range: &Range<u64>| {
/// #     let mut data = Vec::new();
/// #     let mut position = range.start;
/// #     while position < range.end {
/// #         let Some((start, bytes)) = cache.range(..=position).next_back() else {
/// #             return fetch(range);
/// #         };
/// #         let end = (start + bytes.len() as u64).min(range.end);
/// #         if end <= position {
/// #             return fetch(range);
/// #         }
/// #         data.extend_from_slice(&bytes[(position - start) as usize..(end - start) as usize]);
/// #         position = end;
/// #     }
/// #     Bytes::from(data)
/// # };
/// # let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional);
/// # let metadata = ArrowReaderMetadata::load(&file, options).unwrap();
/// let mut decoder = ParquetPushDecoderBuilder::new_with_metadata(metadata)
///     .build()
///     .unwrap();
///
/// // Read ahead up to 1 MB into a cache
/// let mut cache = BTreeMap::new();
/// let mut cached = 0;
/// for planned in decoder.scan_plan() {
///     if cached + planned.len() > 1024 * 1024 {
///         break;
///     }
///     cached += planned.len();
///     cache.insert(planned.range.start, fetch(&planned.range));
/// }
///
/// // Answer each request from the cache
/// loop {
///     match decoder.try_decode().unwrap() {
///         DecodeResult::NeedsData(ranges) => {
///             // `read` takes the data from the cache. If a range is not in
///             // the cache, it fetches it here.
///             let data = ranges.iter().map(|range| read(&cache, range)).collect();
///             decoder.push_ranges(ranges, data).unwrap();
///         }
///         DecodeResult::Data(batch) => println!("{} rows", batch.num_rows()),
///         DecodeResult::Finished => break,
///     }
/// }
/// ```
///
/// [`RowFilter`]: crate::arrow::arrow_reader::RowFilter
/// [`DecodeResult::NeedsData`]: crate::DecodeResult::NeedsData
/// [`ParquetPushDecoder::scan_plan`]: super::ParquetPushDecoder::scan_plan
/// [`ParquetPushDecoder::into_builder`]: super::ParquetPushDecoder::into_builder
/// [`ParquetPushDecoder::try_decode`]: super::ParquetPushDecoder::try_decode
/// [`ParquetPushDecoder::push_ranges`]: super::ParquetPushDecoder::push_ranges
/// [`ParquetPushDecoderBuilder::build`]: super::ParquetPushDecoderBuilder::build
#[derive(Debug, Clone)]
pub struct ScanPlan {
    /// `None` for an empty plan, for example of a finished decoder.
    planner: Option<Box<Planner>>,
}

impl ScanPlan {
    /// A plan with no ranges.
    pub(crate) fn empty() -> Self {
        Self { planner: None }
    }
}

/// The state of a [`ScanPlan`] that is not empty.
#[derive(Debug, Clone)]
struct Planner {
    /// The decoder's row-group queue, selections and offset/limit budget, as
    /// they were when the plan was made.
    frontier: RowGroupFrontier,
    /// The row group that the decoder was fetching when the plan was made.
    /// It is planned first.
    active_row_group: Option<NextRowGroup>,
    /// The decoder's projection, predicates and batch size.
    columns: Arc<StageColumns>,
    /// Whether an output limit is set.
    has_limit: bool,
    /// Whether no row group has been planned yet.
    at_first_row_group: bool,
    /// Planned rows before the next row group.
    next_row: u64,
    /// The row group being planned, if any.
    current: Option<RowGroupRanges>,
    /// Whether planning has ended.
    done: bool,
}

/// The decoder's batch size and decoding stages.
#[derive(Debug)]
struct StageColumns {
    /// The output batch size, which aligns cached predicate reads.
    batch_size: usize,
    /// What each decoding stage fetches, shared with the decoder.
    stages: Arc<StageSchedule>,
}

/// Builds a [`ScanPlan`] from a decoder's row-group frontier and the columns
/// each decoding stage reads.
#[derive(Debug)]
pub(crate) struct ScanPlanBuilder {
    frontier: RowGroupFrontier,
    active_row_group: Option<NextRowGroup>,
    columns: StageColumns,
}

impl ScanPlanBuilder {
    /// Plan the row groups in `frontier`, decoding in batches of
    /// `batch_size` rows with the decoding stages `stages`.
    pub(crate) fn new(
        frontier: RowGroupFrontier,
        batch_size: usize,
        stages: Arc<StageSchedule>,
    ) -> Self {
        Self {
            frontier,
            active_row_group: None,
            columns: StageColumns { batch_size, stages },
        }
    }

    /// Plan `active_row_group` first: the row group that the decoder is
    /// fetching, which the frontier has already handed over.
    pub(crate) fn with_active_row_group(mut self, active_row_group: Option<NextRowGroup>) -> Self {
        self.active_row_group = active_row_group;
        self
    }

    pub(crate) fn build(self) -> ScanPlan {
        let Self {
            frontier,
            active_row_group,
            columns,
        } = self;
        let has_limit = frontier.budget.limit().is_some();
        let planner = Planner {
            frontier,
            active_row_group,
            columns: Arc::new(columns),
            has_limit,
            at_first_row_group: true,
            next_row: 0,
            current: None,
            done: false,
        };
        ScanPlan {
            planner: Some(Box::new(planner)),
        }
    }
}

impl Planner {
    /// Start planning the next row group the decoder will read.
    ///
    /// Returns `Ok(false)` when no row group remains. A row group that the
    /// offset/limit budget removes is planned with no ranges.
    fn plan_next_row_group(&mut self) -> Result<bool, ParquetError> {
        // Use the decoder's own row-group walk, so row groups that the
        // selection or the budget removes are skipped here too.
        let next_row_group = match self.active_row_group.take() {
            Some(active) => Some(active),
            None => self.frontier.next_readable_row_group()?,
        };
        let Some(NextRowGroup {
            row_group_idx,
            row_count,
            selection,
            budget,
        }) = next_row_group
        else {
            return Ok(false);
        };

        let filtered = self.frontier.has_predicates;
        let selection = if filtered {
            // Predicates see every selected row. Offset and limit apply to
            // their output, so they cannot be applied before decoding.
            selection
        } else {
            // Apply offset and limit as the decoder does before it requests
            // the output columns.
            let plan_builder =
                ReadPlanBuilder::new(self.columns.batch_size).with_selection(selection);
            let BudgetedReadPlan {
                plan_builder,
                rows_after_budget,
                remaining_budget,
                ..
            } = budget.apply_to_plan(plan_builder, row_count);
            self.frontier
                .update_budget_after_row_group(remaining_budget);
            if rows_after_budget == 0 {
                self.current = None;
                return Ok(true);
            }
            plan_builder.selection().cloned()
        };

        let rows = SelectedRows::new(selection.as_ref(), row_count);
        let first_row = self.next_row;
        self.next_row += rows.selected_before(row_count);

        let metadata = &self.frontier.parquet_metadata;
        let page_index = metadata
            .page_index()
            .filter(|page_index| page_index.has_offset_indexes())
            .cloned();
        let row_group = metadata.row_group(row_group_idx);

        // Predicate columns that are cached for the output are fetched with the
        // selection expanded to batch boundaries. See `fetch_ranges`.
        let stages = &self.columns.stages;
        let expanded_selection = match (&selection, stages.cache_projection()) {
            (Some(selection), Some(_)) => {
                Some(selection.expand_to_batch_boundaries(self.columns.batch_size, row_count))
            }
            _ => None,
        };

        let num_columns = row_group.columns().len();
        let mut planned_columns = vec![false; num_columns];
        let mut stage_plans = vec![];
        for (stage, fetch) in stages.stages() {
            let conditional = filtered
                && (stage != ScanStage::Predicate(0)
                    || (self.has_limit && !self.at_first_row_group));
            // The decoder reuses a column that an earlier stage read.
            let columns: Vec<StageColumn> =
                columns_to_fetch(fetch.projection, num_columns, |idx| planned_columns[idx])
                    .map(|column_idx| {
                        let (chunk_start, chunk_len) = row_group.column(column_idx).byte_range();
                        StageColumn {
                            column_idx,
                            chunk: chunk_start..chunk_start + chunk_len,
                        }
                    })
                    .collect();
            for column in &columns {
                planned_columns[column.column_idx] = true;
            }
            stage_plans.push(StagePlan {
                stage,
                conditional,
                columns,
            });
        }
        self.at_first_row_group = false;

        self.current = Some(RowGroupRanges {
            row_group: RowGroupContext {
                row_group_idx,
                row_count,
                first_row,
                rows,
                selection,
                expanded_selection,
                stages: Arc::clone(&self.columns.stages),
                page_index,
            },
            stages: stage_plans.into_iter(),
            stage: None,
        });
        Ok(true)
    }
}

impl Iterator for ScanPlan {
    type Item = PlannedRange;

    fn next(&mut self) -> Option<PlannedRange> {
        self.planner.as_mut()?.next()
    }
}

impl Planner {
    fn next(&mut self) -> Option<PlannedRange> {
        loop {
            if let Some(range) = self.current.as_mut().and_then(RowGroupRanges::next) {
                return Some(range);
            }
            if self.done {
                return None;
            }
            match self.plan_next_row_group() {
                Ok(true) => {}
                // The decoder reports the same error when it reaches this row
                // group. The plan is advisory, so it ends here.
                Ok(false) | Err(_) => {
                    self.current = None;
                    self.done = true;
                }
            }
        }
    }
}

/// Once [`ScanPlan::next`] returns `None`, it always returns `None`.
impl FusedIterator for ScanPlan {}

/// The columns that one decoding stage of a row group reads first.
#[derive(Debug, Clone)]
struct StagePlan {
    stage: ScanStage,
    /// See [`PlannedRange::conditional`].
    conditional: bool,
    columns: Vec<StageColumn>,
}

/// A column chunk that a decoding stage reads.
#[derive(Debug, Clone)]
struct StageColumn {
    column_idx: usize,
    /// Byte range of the whole column chunk.
    chunk: Range<u64>,
}

/// The planned ranges of one row group, in decode order.
///
/// Each stage is a merge of one [`ColumnCursor`] per column chunk. A cursor
/// returns the ranges of its column chunk in file order, and their first
/// rows do not decrease. A heap picks the cursor whose next range has the
/// smallest [`RangeOrder`], so the ranges of a stage are ordered by first
/// row without collecting and sorting all of them.
#[derive(Debug, Clone)]
struct RowGroupRanges {
    row_group: RowGroupContext,
    /// Stages not yet started.
    stages: std::vec::IntoIter<StagePlan>,
    /// The stage being merged.
    stage: Option<StageRanges>,
}

/// What the ranges of one row group are computed from.
#[derive(Debug, Clone)]
struct RowGroupContext {
    row_group_idx: usize,
    row_count: usize,
    /// Planned rows before this row group.
    first_row: u64,
    rows: SelectedRows,
    /// The selection the decoder uses to choose pages.
    selection: Option<RowSelection>,
    /// The selection expanded to batch boundaries, for cached predicate
    /// columns.
    expanded_selection: Option<RowSelection>,
    /// What each decoding stage fetches.
    stages: Arc<StageSchedule>,
    /// The file's page index, if it has offset indexes.
    page_index: Option<Arc<dyn PageIndexProvider>>,
}

/// The merge state of one stage.
#[derive(Debug, Clone)]
struct StageRanges {
    stage: ScanStage,
    conditional: bool,
    cursors: Vec<ColumnCursor>,
    /// The sort key of the next range of each cursor that has one, smallest
    /// first.
    heap: BinaryHeap<Reverse<(RangeOrder, usize)>>,
}

/// How the heap orders the next ranges of different column chunks: by first
/// row, then more rows first (a dictionary page), then by file offset. The
/// ranges of one column chunk keep file order, also a page with no planned
/// rows that the predicate cache reads.
type RangeOrder = (u64, Reverse<u64>, u64);

fn range_order(range: &PlannedRange) -> RangeOrder {
    (range.first_row, Reverse(range.last_row), range.range.start)
}

/// The ranges of one column chunk that are not yet returned.
#[derive(Debug, Clone)]
struct ColumnCursor {
    column_idx: usize,
    /// The next range of this column chunk.
    head: Option<PlannedRange>,
    /// Data pages the decoder reads.
    pages: PageSet,
    /// Position in `pages` of the next data page after `head`.
    next_page: usize,
}

/// The data pages of a column chunk that the decoder reads, in page order.
#[derive(Debug, Clone)]
enum PageSet {
    /// No offset index, so the column chunk is one range.
    None,
    /// Every page: there is no selection.
    All(usize),
    /// The pages at these indexes in the offset index.
    Selected(Vec<usize>),
}

impl PageSet {
    fn len(&self) -> usize {
        match self {
            Self::None => 0,
            Self::All(len) => *len,
            Self::Selected(pages) => pages.len(),
        }
    }

    /// Index in the offset index of the page at `position`.
    fn page(&self, position: usize) -> usize {
        match self {
            Self::None => unreachable!("no pages"),
            Self::All(_) => position,
            Self::Selected(pages) => pages[position],
        }
    }
}

impl RowGroupRanges {
    fn next(&mut self) -> Option<PlannedRange> {
        loop {
            if let Some(stage) = self.stage.as_mut()
                && let Some(Reverse((_, cursor_idx))) = stage.heap.pop()
            {
                let cursor = &mut stage.cursors[cursor_idx];
                let range = cursor.head.take().expect("heap entries have a head");
                cursor.head = self
                    .row_group
                    .next_data_page(cursor, stage.stage, stage.conditional);
                if let Some(head) = &cursor.head {
                    stage.heap.push(Reverse((range_order(head), cursor_idx)));
                }
                return Some(range);
            }
            let plan = self.stages.next()?;
            let cursors: Vec<_> = plan
                .columns
                .iter()
                .map(|column| {
                    self.row_group
                        .column_cursor(column, plan.stage, plan.conditional)
                })
                .collect();
            let heap = cursors
                .iter()
                .enumerate()
                .filter_map(|(idx, cursor)| {
                    let head = cursor.head.as_ref()?;
                    Some(Reverse((range_order(head), idx)))
                })
                .collect();
            self.stage = Some(StageRanges {
                stage: plan.stage,
                conditional: plan.conditional,
                cursors,
                heap,
            });
        }
    }
}

impl RowGroupContext {
    fn entry(
        &self,
        column_idx: usize,
        range: Range<u64>,
        rows: Range<u64>,
        kind: PageKind,
        stage: ScanStage,
        conditional: bool,
    ) -> PlannedRange {
        PlannedRange {
            range,
            first_row: rows.start,
            last_row: rows.end,
            row_group: self.row_group_idx,
            column: column_idx,
            kind,
            stage,
            conditional,
        }
    }

    fn locations(&self, column_idx: usize) -> Option<&[PageLocation]> {
        self.page_index
            .as_ref()?
            .offset_index(self.row_group_idx, column_idx)
            .map(|offset_index| offset_index.page_locations().as_slice())
    }

    /// Planned rows of the data page at `idx` in `locations`.
    fn page_rows(&self, locations: &[PageLocation], idx: usize) -> Range<u64> {
        let raw_end = locations
            .get(idx + 1)
            .map(|next| next.first_row_index as usize)
            .unwrap_or(self.row_count);
        let first_row = self.first_row
            + self
                .rows
                .selected_before(locations[idx].first_row_index as usize);
        first_row..self.first_row + self.rows.selected_before(raw_end)
    }

    /// Start the cursor of one column chunk.
    ///
    /// The ranges are the same bytes that `InMemoryRowGroup::fetch_ranges`
    /// requests, split at page boundaries when page locations are known.
    fn column_cursor(
        &self,
        column: &StageColumn,
        stage: ScanStage,
        conditional: bool,
    ) -> ColumnCursor {
        let StageColumn {
            column_idx,
            ref chunk,
        } = *column;
        let row_group_rows =
            self.first_row..self.first_row + self.rows.selected_before(self.row_count);

        let fetch_selection = column_selection(
            self.selection.as_ref(),
            self.expanded_selection.as_ref(),
            self.stages.fetch(stage).cache_projection,
            column_idx,
        );
        let all_locations = self.locations(column_idx);
        let fetch = ColumnFetch::new(chunk.clone(), all_locations, fetch_selection);
        // The decoder fetches a whole column chunk as one range. If page
        // locations are known, the plan splits it into the same bytes, page
        // by page.
        let locations = all_locations.filter(|l| !l.is_empty());
        let (dictionary, pages) = match (fetch, locations) {
            (ColumnFetch::Chunk { range }, Some(locations)) => (
                dictionary_range(range.start, locations),
                PageSet::All(locations.len()),
            ),
            (ColumnFetch::Chunk { range }, None) => {
                return ColumnCursor {
                    column_idx,
                    head: Some(self.entry(
                        column_idx,
                        range,
                        row_group_rows,
                        PageKind::ColumnChunk,
                        stage,
                        conditional,
                    )),
                    pages: PageSet::None,
                    next_page: 0,
                };
            }
            (
                ColumnFetch::Pages {
                    dictionary, pages, ..
                },
                _,
            ) => (dictionary, PageSet::Selected(pages)),
        };

        let mut cursor = ColumnCursor {
            column_idx,
            head: None,
            pages,
            next_page: 0,
        };
        if let Some(dictionary) = dictionary {
            let locations = all_locations.expect("pages have locations");
            // The dictionary serves exactly the rows of the data pages read.
            let rows = match cursor.pages.len() {
                0 => row_group_rows,
                len => {
                    let first = self.page_rows(locations, cursor.pages.page(0));
                    let last = self.page_rows(locations, cursor.pages.page(len - 1));
                    first.start..last.end
                }
            };
            cursor.head = Some(self.entry(
                column_idx,
                dictionary,
                rows,
                PageKind::Dictionary,
                stage,
                conditional,
            ));
        } else {
            cursor.head = self.next_data_page(&mut cursor, stage, conditional);
        }
        cursor
    }

    /// The next data page of `cursor`, if any.
    fn next_data_page(
        &self,
        cursor: &mut ColumnCursor,
        stage: ScanStage,
        conditional: bool,
    ) -> Option<PlannedRange> {
        if cursor.next_page >= cursor.pages.len() {
            return None;
        }
        let locations = self.locations(cursor.column_idx)?;
        let idx = cursor.pages.page(cursor.next_page);
        cursor.next_page += 1;
        Some(self.entry(
            cursor.column_idx,
            page_range(&locations[idx]),
            self.page_rows(locations, idx),
            PageKind::Data,
            stage,
            conditional,
        ))
    }
}

/// Counts the selected rows before a position in a row group.
#[derive(Debug, Clone)]
struct SelectedRows {
    /// `(first raw row, selected rows before it, selected)` per selector.
    /// `None` when every row is selected.
    runs: Option<Vec<(usize, u64, bool)>>,
}

impl SelectedRows {
    fn new(selection: Option<&RowSelection>, row_count: usize) -> Self {
        let runs = selection.map(|selection| {
            let mut runs = Vec::new();
            let mut raw = 0;
            let mut selected = 0;
            for selector in selection.iter() {
                if selector.row_count == 0 {
                    continue;
                }
                runs.push((raw, selected, !selector.skip));
                raw += selector.row_count;
                if !selector.skip {
                    selected += selector.row_count as u64;
                }
            }
            // A selection shorter than the row group skips the trailing rows.
            if raw < row_count {
                runs.push((raw, selected, false));
            }
            runs
        });
        Self { runs }
    }

    /// The number of selected rows in `0..raw`.
    fn selected_before(&self, raw: usize) -> u64 {
        let Some(runs) = &self.runs else {
            return raw as u64;
        };
        let idx = runs.partition_point(|(start, _, _)| *start < raw);
        let Some(&(start, selected_before, selected)) = idx.checked_sub(1).map(|idx| &runs[idx])
        else {
            return 0;
        };
        if selected {
            selected_before + (raw - start) as u64
        } else {
            selected_before
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DecodeResult;
    use crate::arrow::arrow_reader::{
        ArrowPredicateFn, ArrowReaderMetadata, ArrowReaderOptions, RowFilter, RowSelector,
    };
    use crate::arrow::push_decoder::{
        ParquetPushDecoder, ParquetPushDecoderBuilder, RowGroupSelection,
    };
    use crate::arrow::{ArrowWriter, ProjectionMask};
    use crate::column::writer::ColumnCloseResult;
    use crate::file::metadata::PageIndexPolicy;
    use crate::file::metadata::page_index::PageIndexBuilder;
    use crate::file::properties::WriterProperties;
    use crate::file::writer::SerializedFileWriter;
    use arrow::compute::concat_batches;
    use arrow::compute::kernels::cmp::{gt, lt};
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int64Type;
    use arrow_array::{ArrayRef, Int64Array, ListArray, RecordBatch, StringArray, StructArray};
    use arrow_buffer::BooleanBuffer;
    use arrow_schema::{DataType, Field};
    use bytes::Bytes;
    use std::sync::LazyLock;

    /// Two row groups of 200 rows, 50 rows per data page, three columns:
    /// `a` (Int64, plain), `b` (Int64, dictionary) and `c` (Utf8, dictionary).
    static FILE: LazyLock<Bytes> = LazyLock::new(|| {
        let a: ArrayRef = Arc::new(Int64Array::from_iter_values(0..400));
        let b: ArrayRef = Arc::new(Int64Array::from_iter_values((0..400).map(|v| v % 7)));
        let c: ArrayRef = Arc::new(StringArray::from_iter_values(
            (0..400).map(|v| format!("value {}", v % 13)),
        ));
        let batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(200))
            .set_data_page_row_count_limit(50)
            // Page limits are checked between write batches.
            .set_write_batch_size(50)
            .set_column_dictionary_enabled("a".into(), false)
            .build();
        let mut buffer = vec![];
        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        Bytes::from(buffer)
    });

    fn metadata(page_index: bool) -> ArrowReaderMetadata {
        let policy = if page_index {
            PageIndexPolicy::Required
        } else {
            PageIndexPolicy::Skip
        };
        let options = ArrowReaderOptions::new().with_page_index_policy(policy);
        ArrowReaderMetadata::load(&*FILE, options).unwrap()
    }

    fn builder() -> ParquetPushDecoderBuilder {
        ParquetPushDecoderBuilder::new_with_metadata(metadata(true)).with_batch_size(64)
    }

    fn fetch(range: &Range<u64>) -> Bytes {
        FILE.slice(range.start as usize..range.end as usize)
    }

    fn mask(leaves: impl IntoIterator<Item = usize>) -> ProjectionMask {
        ProjectionMask::leaves(metadata(true).parquet_schema(), leaves)
    }

    /// Sort and merge ranges into disjoint, non-adjacent ranges.
    fn union(ranges: impl IntoIterator<Item = Range<u64>>) -> Vec<Range<u64>> {
        let mut ranges: Vec<_> = ranges.into_iter().filter(|r| !r.is_empty()).collect();
        ranges.sort_by_key(|r| r.start);
        let mut merged: Vec<Range<u64>> = vec![];
        for range in ranges {
            match merged.last_mut() {
                Some(last) if range.start <= last.end => last.end = last.end.max(range.end),
                _ => merged.push(range),
            }
        }
        merged
    }

    /// Returns `true` if every byte of `inner` is in `outer`.
    fn covers(outer: &[Range<u64>], inner: &[Range<u64>]) -> bool {
        inner.iter().all(|range| {
            outer
                .iter()
                .any(|o| o.start <= range.start && range.end <= o.end)
        })
    }

    /// Decode the whole scan, supplying exactly the requested bytes. Returns
    /// the requested ranges and the number of output rows.
    fn demand(builder: ParquetPushDecoderBuilder) -> (Vec<Range<u64>>, u64) {
        demand_in(&FILE, builder)
    }

    /// [`demand`] for `file`.
    fn demand_in(file: &Bytes, builder: ParquetPushDecoderBuilder) -> (Vec<Range<u64>>, u64) {
        let fetch = |range: &Range<u64>| file.slice(range.start as usize..range.end as usize);
        let mut decoder = builder.build().unwrap();
        let mut requested = vec![];
        let mut rows = 0;
        loop {
            match decoder.try_decode().unwrap() {
                DecodeResult::NeedsData(ranges) => {
                    let data = ranges.iter().map(fetch).collect();
                    requested.extend(ranges.iter().cloned());
                    decoder.push_ranges(ranges, data).unwrap();
                }
                DecodeResult::Data(batch) => rows += batch.num_rows() as u64,
                DecodeResult::Finished => return (requested, rows),
            }
        }
    }

    /// Decode the whole scan. `supply` answers each
    /// [`DecodeResult::NeedsData`]. Returns the output and the bytes that
    /// the decoder still buffers when it finishes.
    fn decode_with(
        mut decoder: ParquetPushDecoder,
        mut supply: impl FnMut(&mut ParquetPushDecoder, Vec<Range<u64>>),
    ) -> (Vec<RecordBatch>, u64) {
        let mut batches = vec![];
        loop {
            match decoder.try_decode().unwrap() {
                DecodeResult::NeedsData(ranges) => supply(&mut decoder, ranges),
                DecodeResult::Data(batch) => batches.push(batch),
                DecodeResult::Finished => return (batches, decoder.buffered_bytes()),
            }
        }
    }

    /// Check the documented order of `plan`: in each stage of a row group,
    /// first rows do not decrease, and the ranges of each column chunk are in
    /// file order.
    fn check_order(name: &str, plan: &[PlannedRange]) {
        for pair in plan.windows(2) {
            let (a, b) = (&pair[0], &pair[1]);
            if (a.row_group, a.stage) == (b.row_group, b.stage) {
                assert!(a.first_row <= b.first_row, "{name}: {a:?} before {b:?}");
            }
        }
        // A scan can read a row group more than once, so count each visit.
        let mut visit = 0;
        let mut last_start = std::collections::HashMap::new();
        for (idx, p) in plan.iter().enumerate() {
            if idx > 0 && plan[idx - 1].row_group != p.row_group {
                visit += 1;
            }
            if let Some(start) = last_start.insert((visit, p.column), p.range.start) {
                assert!(start < p.range.start, "{name}: {p:?} is not in file order");
            }
        }
    }

    /// See [`decode_from_plan_in`].
    fn decode_from_plan(name: &str, builder: impl Fn() -> ParquetPushDecoderBuilder) {
        decode_from_plan_in(name, &FILE, builder)
    }

    /// Decode the whole scan of `file` from the plan, as a read-ahead caller
    /// would: keep the planned ranges in a cache, and answer each
    /// [`DecodeResult::NeedsData`] with exactly the requested ranges.
    ///
    /// Panics if:
    /// * a request is not in the planned ranges of the row groups planned so
    ///   far, after adding at most one more planned row group to the cache;
    /// * the plan is not in the documented order;
    /// * the decoder still buffers bytes when it finishes;
    /// * the output differs from a scan that fetches the requested bytes.
    fn decode_from_plan_in(
        name: &str,
        file: &Bytes,
        builder: impl Fn() -> ParquetPushDecoderBuilder,
    ) {
        let fetch = |range: &Range<u64>| file.slice(range.start as usize..range.end as usize);
        let (expected, _) = decode_with(builder().build().unwrap(), |decoder, ranges| {
            let data = ranges.iter().map(fetch).collect();
            decoder.push_ranges(ranges, data).unwrap();
        });

        let decoder = builder().build().unwrap();
        let planned: Vec<_> = decoder.scan_plan().collect();
        check_order(name, &planned);
        let mut plan = planned.into_iter().peekable();
        let mut cache: Vec<Range<u64>> = vec![];
        let (actual, buffered) = decode_with(decoder, |decoder, requested| {
            if !covers(&union(cache.clone()), &requested) {
                let row_group = plan
                    .peek()
                    .unwrap_or_else(|| panic!("{name}: {requested:?} requested after the plan"))
                    .row_group;
                while let Some(p) = plan.next_if(|p| p.row_group == row_group) {
                    cache.push(p.range);
                }
            }
            assert!(
                covers(&union(cache.clone()), &requested),
                "{name}: {requested:?} is not in the plan of the next row group"
            );
            let data = requested.iter().map(fetch).collect();
            decoder.push_ranges(requested, data).unwrap();
        });
        assert_eq!(buffered, 0, "{name}: bytes still buffered");

        let Some(schema) = expected.first().map(|batch| batch.schema()) else {
            assert!(actual.is_empty(), "{name}: unexpected output");
            return;
        };
        assert_eq!(
            concat_batches(&schema, &actual).unwrap(),
            concat_batches(&schema, &expected).unwrap(),
            "{name}: output differs"
        );
    }

    /// Check the invariants of a plan for a scan without a row filter.
    ///
    /// `builder` is called twice: once for the plan, once to decode.
    fn check_unfiltered(
        name: &str,
        builder: impl Fn() -> ParquetPushDecoderBuilder,
    ) -> Vec<PlannedRange> {
        let plan: Vec<_> = builder().build().unwrap().scan_plan().collect();
        let (requested, rows) = demand(builder());
        decode_from_plan(name, &builder);

        assert_eq!(
            union(plan.iter().map(|p| p.range.clone())),
            union(requested),
            "{name}: planned bytes differ from requested bytes"
        );
        for p in &plan {
            assert!(!p.conditional, "{name}: {p:?}");
            assert_eq!(p.stage, ScanStage::Projection, "{name}: {p:?}");
        }
        // Planned rows are output rows: for each column, the data pages tile
        // the output rows in order.
        let columns: std::collections::BTreeSet<_> = plan.iter().map(|p| p.column).collect();
        for column in columns {
            let mut next = 0;
            for p in plan
                .iter()
                .filter(|p| p.column == column && p.kind != PageKind::Dictionary)
            {
                assert_eq!(p.first_row, next, "{name}: gap before {p:?}");
                assert!(p.last_row > p.first_row, "{name}: empty {p:?}");
                next = p.last_row;
            }
            assert_eq!(next, rows, "{name}: column {column} rows");
        }
        plan
    }

    #[test]
    fn plan_matches_demand_without_row_filter() {
        fn sel(selectors: Vec<RowSelector>) -> RowSelection {
            RowSelection::from(selectors)
        }
        type Case = (&'static str, Box<dyn Fn() -> ParquetPushDecoderBuilder>);
        let cases: Vec<Case> = vec![
            ("all", Box::new(builder)),
            (
                "projection",
                Box::new(|| builder().with_projection(mask([2]))),
            ),
            (
                "row groups reversed",
                Box::new(|| builder().with_row_groups(vec![1, 0])),
            ),
            (
                "global selection",
                Box::new(|| {
                    builder().with_row_selection(sel(vec![
                        RowSelector::skip(60),
                        RowSelector::select(10),
                        RowSelector::skip(200),
                        RowSelector::select(30),
                    ]))
                }),
            ),
            (
                "mask selection",
                Box::new(|| {
                    builder().with_row_selection(RowSelection::from_boolean_buffer(
                        BooleanBuffer::from_iter((0..400).map(|i| i % 97 == 3)),
                    ))
                }),
            ),
            (
                "per row group selections with duplicates",
                Box::new(|| {
                    builder().with_row_group_selections(vec![
                        RowGroupSelection::new(
                            1,
                            Some(sel(vec![RowSelector::skip(150), RowSelector::select(10)])),
                        ),
                        RowGroupSelection::new(0, None),
                        RowGroupSelection::new(1, Some(sel(vec![RowSelector::select(5)]))),
                        RowGroupSelection::new(0, Some(sel(vec![RowSelector::skip(200)]))),
                    ])
                }),
            ),
            (
                "offset and limit",
                Box::new(|| builder().with_offset(5).with_limit(12)),
            ),
            (
                "offset past first row group",
                Box::new(|| builder().with_offset(230).with_limit(60)),
            ),
            (
                "selection, offset and limit",
                Box::new(|| {
                    builder()
                        .with_row_selection(sel(vec![
                            RowSelector::skip(190),
                            RowSelector::select(10),
                            RowSelector::select(100),
                        ]))
                        .with_offset(5)
                        .with_limit(12)
                }),
            ),
            ("limit zero", Box::new(|| builder().with_limit(0))),
            (
                "no page index",
                Box::new(|| {
                    ParquetPushDecoderBuilder::new_with_metadata(metadata(false))
                        .with_row_selection(sel(vec![
                            RowSelector::skip(60),
                            RowSelector::select(10),
                        ]))
                }),
            ),
        ];
        for (name, builder) in cases {
            check_unfiltered(name, builder);
        }
    }

    #[test]
    fn plan_has_pages_with_offset_index() {
        let plan = check_unfiltered("all", || builder().with_projection(mask([0, 1])));
        // Column a: 4 plain data pages per row group. Column b: a dictionary
        // page, then 4 data pages per row group.
        let kinds = |row_group, column| -> Vec<PageKind> {
            plan.iter()
                .filter(|p| p.row_group == row_group && p.column == column)
                .map(|p| p.kind)
                .collect()
        };
        assert_eq!(kinds(0, 0), vec![PageKind::Data; 4]);
        let mut expected = vec![PageKind::Dictionary];
        expected.extend([PageKind::Data; 4]);
        assert_eq!(kinds(1, 1), expected);

        // The dictionary page serves every row of its row group and comes
        // before the data pages.
        let dictionary = plan
            .iter()
            .position(|p| p.row_group == 1 && p.kind == PageKind::Dictionary)
            .unwrap();
        assert_eq!(
            (plan[dictionary].first_row, plan[dictionary].last_row),
            (200, 400)
        );
        assert!(plan[..dictionary].iter().all(|p| p.first_row < 200));

        // Plan order is decode order.
        assert!(plan.windows(2).all(|w| w[0].first_row <= w[1].first_row));
    }

    #[test]
    fn plan_has_column_chunks_without_offset_index() {
        let plan = check_unfiltered("no page index", || {
            ParquetPushDecoderBuilder::new_with_metadata(metadata(false)).with_limit(250)
        });
        let entries: Vec<_> = plan
            .iter()
            .map(|p| (p.row_group, p.column, p.kind, p.first_row, p.last_row))
            .collect();
        let mut expected = vec![];
        for (row_group, rows) in [(0, 0..200), (1, 200..250)] {
            for column in 0..3 {
                expected.push((
                    row_group,
                    column,
                    PageKind::ColumnChunk,
                    rows.start,
                    rows.end,
                ));
            }
        }
        assert_eq!(entries, expected);
    }

    #[test]
    fn plan_tags_row_filter_stages() {
        // Predicate on `a`, then on `b`; output `a` and `c`.
        let filter = || {
            RowFilter::new(vec![
                Box::new(ArrowPredicateFn::new(mask([0]), |batch| {
                    gt(batch.column(0), &Int64Array::new_scalar(100))
                })),
                Box::new(ArrowPredicateFn::new(mask([1]), |batch| {
                    lt(batch.column(0), &Int64Array::new_scalar(3))
                })),
            ])
        };
        for limit in [None, Some(10)] {
            let builder = || {
                // The selection keeps a few rows of scattered pages, so the
                // cached predicate column `a` is fetched with its selection
                // expanded to batch boundaries.
                let builder = builder()
                    .with_projection(mask([0, 2]))
                    .with_row_selection(RowSelection::from(vec![
                        RowSelector::skip(70),
                        RowSelector::select(3),
                        RowSelector::skip(260),
                        RowSelector::select(20),
                    ]))
                    .with_row_filter(filter());
                match limit {
                    Some(limit) => builder.with_limit(limit),
                    None => builder,
                }
            };
            let plan: Vec<_> = builder().build().unwrap().scan_plan().collect();
            let (requested, _) = demand(builder());
            assert!(
                covers(&union(plan.iter().map(|p| p.range.clone())), &requested),
                "limit {limit:?}: requested bytes are not planned"
            );
            decode_from_plan(&format!("row filter, limit {limit:?}"), builder);

            for p in &plan {
                let expected_stage = match p.column {
                    0 => ScanStage::Predicate(0),
                    1 => ScanStage::Predicate(1),
                    _ => ScanStage::Projection,
                };
                assert_eq!(p.stage, expected_stage, "{p:?}");
                let expected_conditional = match p.stage {
                    ScanStage::Predicate(0) => limit.is_some() && p.row_group > 0,
                    _ => true,
                };
                assert_eq!(
                    p.conditional, expected_conditional,
                    "limit {limit:?}: {p:?}"
                );
            }
            // Stages are in evaluation order within each row group, and the
            // offset/limit do not remove rows before the predicates run.
            for row_group in 0..2 {
                let stages: Vec<_> = plan
                    .iter()
                    .filter(|p| p.row_group == row_group)
                    .map(|p| p.stage)
                    .collect();
                assert!(stages.is_sorted(), "{stages:?}");
            }
            assert_eq!(plan.iter().map(|p| p.last_row).max(), Some(23));
        }
    }

    #[test]
    fn plan_keeps_file_order_for_zero_row_pages() {
        // One predicate reads `a` and `b`, which are also in the output, so
        // the decoder caches them and fetches them with the selection
        // expanded to batch boundaries (64 rows). Rows 64..100 are in the
        // page of rows 50..100, which has no selected rows.
        let builder = || {
            builder()
                .with_projection(mask([0, 1]))
                .with_row_selection(RowSelection::from(vec![
                    RowSelector::skip(100),
                    RowSelector::select(10),
                ]))
                .with_row_filter(RowFilter::new(vec![Box::new(ArrowPredicateFn::new(
                    mask([0, 1]),
                    |batch| gt(batch.column(0), &Int64Array::new_scalar(100)),
                ))]))
        };
        let plan: Vec<_> = builder().build().unwrap().scan_plan().collect();
        let summary: Vec<_> = plan
            .iter()
            .map(|p| (p.column, p.kind, p.first_row..p.last_row))
            .collect();
        // In each column chunk, the page with no planned rows comes before
        // the next page, in file order, as the decoder reads them.
        assert_eq!(
            summary,
            vec![
                (1, PageKind::Dictionary, 0..10),
                (0, PageKind::Data, 0..0),
                (0, PageKind::Data, 0..10),
                (1, PageKind::Data, 0..0),
                (1, PageKind::Data, 0..10),
            ]
        );
        decode_from_plan("zero row pages", builder);
    }

    /// [`FILE`] without the offset index of the column chunks for which
    /// `has_index(row_group, column)` is `false`.
    fn file_with_partial_offset_index(has_index: impl Fn(usize, usize) -> bool) -> Bytes {
        let metadata = metadata(true);
        let metadata = metadata.metadata();
        let page_index = metadata.page_index().unwrap();
        let schema = metadata.file_metadata().schema_descr().root_schema_ptr();
        let mut buffer = vec![];
        let mut writer =
            SerializedFileWriter::new(&mut buffer, schema, Default::default()).unwrap();
        for (row_group, row_group_metadata) in metadata.row_groups().iter().enumerate() {
            let mut row_group_writer = writer.next_row_group().unwrap();
            for (column, column_metadata) in row_group_metadata.columns().iter().enumerate() {
                let keep = has_index(row_group, column);
                let close = ColumnCloseResult {
                    bytes_written: column_metadata.compressed_size() as u64,
                    rows_written: row_group_metadata.num_rows() as u64,
                    metadata: column_metadata.clone(),
                    bloom_filter: None,
                    column_index: page_index
                        .column_index(row_group, column)
                        .filter(|_| keep)
                        .cloned(),
                    offset_index: page_index
                        .offset_index(row_group, column)
                        .filter(|_| keep)
                        .cloned(),
                };
                row_group_writer.append_column(&*FILE, close).unwrap();
            }
            row_group_writer.close().unwrap();
        }
        writer.close().unwrap();
        Bytes::from(buffer)
    }

    #[test]
    fn plan_with_offset_index_for_some_columns() {
        // Column `b` (dictionary) of row group 0, and column `a` of row group
        // 1, have no offset index.
        let file = file_with_partial_offset_index(|row_group, column| {
            (row_group, column) != (0, 1) && (row_group, column) != (1, 0)
        });
        let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional);
        let metadata = ArrowReaderMetadata::load(&file, options).unwrap();
        let filter = || {
            RowFilter::new(vec![Box::new(ArrowPredicateFn::new(mask([1]), |batch| {
                lt(batch.column(0), &Int64Array::new_scalar(3))
            }))])
        };
        let selection = || {
            RowSelection::from(vec![
                RowSelector::skip(60),
                RowSelector::select(10),
                RowSelector::skip(200),
                RowSelector::select(30),
            ])
        };
        let base =
            || ParquetPushDecoderBuilder::new_with_metadata(metadata.clone()).with_batch_size(64);
        type Case<'a> = (
            &'static str,
            Box<dyn Fn() -> ParquetPushDecoderBuilder + 'a>,
        );
        let cases: Vec<Case> = vec![
            ("all", Box::new(base)),
            (
                "selection",
                Box::new(|| base().with_row_selection(selection())),
            ),
            (
                "offset and limit",
                Box::new(|| base().with_offset(150).with_limit(100)),
            ),
            (
                "row filter and selection",
                Box::new(|| {
                    base()
                        .with_row_selection(selection())
                        .with_row_filter(filter())
                }),
            ),
        ];
        for (name, builder) in cases {
            let plan: Vec<_> = builder().build().unwrap().scan_plan().collect();
            for p in &plan {
                let has_index =
                    (p.row_group, p.column) != (0, 1) && (p.row_group, p.column) != (1, 0);
                assert_eq!(p.kind == PageKind::ColumnChunk, !has_index, "{name}: {p:?}");
            }
            let (requested, _) = demand_in(&file, builder());
            let planned = union(plan.iter().map(|p| p.range.clone()));
            if name.contains("filter") {
                assert!(covers(&planned, &requested), "{name}");
            } else {
                assert_eq!(planned, union(requested), "{name}");
            }
            decode_from_plan_in(name, &file, builder);
        }
    }

    /// Four row groups of 100 rows, 25 rows per data page, with nested
    /// columns: `id` (Int64), `list` (List of Int64) and `s` (Struct of
    /// Int64 `x` and Utf8 `y`). Leaves: 0 `id`, 1 `list.item`, 2 `s.x`,
    /// 3 `s.y`.
    static NESTED_FILE: LazyLock<Bytes> = LazyLock::new(|| {
        let id: ArrayRef = Arc::new(Int64Array::from_iter_values(0..400));
        let list: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(
            (0..400).map(|v| Some((0..v % 4).map(Some))),
        ));
        let s: ArrayRef = Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new("x", DataType::Int64, false)),
                Arc::new(Int64Array::from_iter_values((0..400).map(|v| v % 10))) as ArrayRef,
            ),
            (
                Arc::new(Field::new("y", DataType::Utf8, false)),
                Arc::new(StringArray::from_iter_values(
                    (0..400).map(|v| format!("y {}", v % 7)),
                )) as ArrayRef,
            ),
        ]));
        let batch = RecordBatch::try_from_iter(vec![("id", id), ("list", list), ("s", s)]).unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(100))
            .set_data_page_row_count_limit(25)
            .set_write_batch_size(25)
            .build();
        let mut buffer = vec![];
        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        Bytes::from(buffer)
    });

    #[test]
    fn plan_for_nested_columns() {
        let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
        let metadata = ArrowReaderMetadata::load(&*NESTED_FILE, options).unwrap();
        let leaves = |leaves: Vec<usize>| ProjectionMask::leaves(metadata.parquet_schema(), leaves);
        for limit in [None, Some(7), Some(150)] {
            for cache in [true, false] {
                let builder = || {
                    let builder = ParquetPushDecoderBuilder::new_with_metadata(metadata.clone())
                        .with_batch_size(16)
                        .with_projection(leaves(vec![0, 1, 2, 3]))
                        .with_row_selection(RowSelection::from(vec![
                            RowSelector::skip(30),
                            RowSelector::select(40),
                            RowSelector::skip(150),
                            RowSelector::select(120),
                        ]))
                        .with_row_filter(RowFilter::new(vec![
                            Box::new(ArrowPredicateFn::new(leaves(vec![0]), |batch| {
                                gt(batch.column(0), &Int64Array::new_scalar(40))
                            })),
                            Box::new(ArrowPredicateFn::new(leaves(vec![2]), |batch| {
                                let s = batch.column(0).as_struct();
                                lt(s.column(0), &Int64Array::new_scalar(6))
                            })),
                        ]));
                    let builder = match cache {
                        true => builder,
                        false => builder.with_max_predicate_cache_size(0),
                    };
                    match limit {
                        Some(limit) => builder.with_limit(limit),
                        None => builder,
                    }
                };
                let name = format!("nested, limit {limit:?}, cache {cache}");
                let plan: Vec<_> = builder().build().unwrap().scan_plan().collect();
                let (requested, _) = demand_in(&NESTED_FILE, builder());
                assert!(
                    covers(&union(plan.iter().map(|p| p.range.clone())), &requested),
                    "{name}: requested bytes are not planned"
                );
                // `s.y` is read only for the output.
                for p in plan.iter().filter(|p| p.column == 3) {
                    assert_eq!(p.stage, ScanStage::Projection, "{name}: {p:?}");
                }
                decode_from_plan_in(&name, &NESTED_FILE, builder);
            }
        }
    }

    /// With a row selection, the decoder requests nothing for a column chunk
    /// whose offset index lists no pages. Without one, it requests the column
    /// chunk. The plan does the same.
    #[test]
    fn plan_with_empty_offset_index() {
        let metadata = metadata(true);
        let parquet_metadata = metadata.metadata();
        let page_index = parquet_metadata.page_index().unwrap();
        let num_columns = parquet_metadata.row_group(0).columns().len();
        let mut page_index_builder =
            PageIndexBuilder::new(parquet_metadata.num_row_groups(), num_columns);
        for row_group in 0..parquet_metadata.num_row_groups() {
            for column in 0..num_columns {
                let mut offset_index = page_index.offset_index(row_group, column).unwrap().clone();
                if (row_group, column) == (0, 0) {
                    offset_index.page_locations.clear();
                }
                page_index_builder.put_offset_index(offset_index, row_group, column);
            }
        }
        let parquet_metadata = parquet_metadata
            .as_ref()
            .clone()
            .into_builder()
            .set_page_index(Some(Arc::new(page_index_builder.build())))
            .build();
        let metadata =
            ArrowReaderMetadata::try_new(Arc::new(parquet_metadata), ArrowReaderOptions::new())
                .unwrap();
        let first_request = |builder: ParquetPushDecoderBuilder| {
            let mut decoder = builder.build().unwrap();
            let plan: Vec<_> = decoder.scan_plan().filter(|p| p.row_group == 0).collect();
            let DecodeResult::NeedsData(requested) = decoder.try_decode().unwrap() else {
                panic!("expected a request");
            };
            (plan, requested)
        };

        let base = || ParquetPushDecoderBuilder::new_with_metadata(metadata.clone());
        let (plan, requested) = first_request(base());
        assert!(
            plan.iter()
                .any(|p| p.column == 0 && p.kind == PageKind::ColumnChunk),
            "{plan:?}"
        );
        assert_eq!(union(plan.into_iter().map(|p| p.range)), union(requested));

        let (plan, requested) = first_request(base().with_row_selection(RowSelection::from(vec![
            RowSelector::skip(60),
            RowSelector::select(10),
        ])));
        assert!(plan.iter().all(|p| p.column != 0), "{plan:?}");
        assert_eq!(union(plan.into_iter().map(|p| p.range)), union(requested));
    }

    #[test]
    fn plan_is_independent_of_buffers() {
        let mut decoder = builder().build().unwrap();
        let before: Vec<_> = decoder.scan_plan().collect();
        let file_range = 0..FILE.len() as u64;
        decoder
            .push_range(file_range.clone(), fetch(&file_range))
            .unwrap();
        assert_eq!(decoder.scan_plan().collect::<Vec<_>>(), before);
    }

    /// At every step of a scan, the plan covers every range that the decoder
    /// requests later, also in the middle of a row group.
    #[test]
    fn plan_covers_later_requests_at_every_step() {
        let filter = || {
            RowFilter::new(vec![
                Box::new(ArrowPredicateFn::new(mask([0]), |batch| {
                    gt(batch.column(0), &Int64Array::new_scalar(100))
                })),
                Box::new(ArrowPredicateFn::new(mask([1]), |batch| {
                    lt(batch.column(0), &Int64Array::new_scalar(3))
                })),
            ])
        };
        type Case<'a> = (
            &'static str,
            Box<dyn Fn() -> ParquetPushDecoderBuilder + 'a>,
        );
        let cases: Vec<Case> = vec![
            ("all", Box::new(builder)),
            (
                "selection and limit",
                Box::new(|| {
                    builder()
                        .with_row_selection(RowSelection::from(vec![
                            RowSelector::skip(60),
                            RowSelector::select(200),
                        ]))
                        .with_limit(150)
                }),
            ),
            (
                "row filter",
                Box::new(|| {
                    builder()
                        .with_projection(mask([0, 2]))
                        .with_row_filter(filter())
                }),
            ),
        ];
        for (name, builder) in cases {
            let mut decoder = builder().build().unwrap();
            let mut plans = vec![];
            let mut requests = vec![];
            loop {
                plans.push(union(decoder.scan_plan().map(|p| p.range)));
                match decoder.try_decode().unwrap() {
                    DecodeResult::NeedsData(ranges) => {
                        requests.push((plans.len() - 1, ranges.clone()));
                        let data = ranges.iter().map(fetch).collect();
                        decoder.push_ranges(ranges, data).unwrap();
                    }
                    DecodeResult::Data(_) => {}
                    DecodeResult::Finished => break,
                }
            }
            assert!(requests.len() > 1, "{name}");
            for (step, plan) in plans.iter().enumerate() {
                for (_, ranges) in requests.iter().filter(|(at, _)| *at >= step) {
                    assert!(
                        covers(plan, ranges),
                        "{name}: plan at step {step} misses {ranges:?}"
                    );
                }
            }
            assert!(decoder.scan_plan().next().is_none(), "{name}: finished");
        }
    }

    #[test]
    fn plan_at_boundary_is_rebuilt_decoder_plan() {
        let mut decoder = builder().build().unwrap();
        let file_range = 0..FILE.len() as u64;
        decoder
            .push_range(file_range.clone(), fetch(&file_range))
            .unwrap();
        let DecodeResult::Data(_) = decoder.try_next_reader().unwrap() else {
            panic!("expected a reader");
        };
        assert!(decoder.is_at_row_group_boundary());
        let plan: Vec<_> = decoder.scan_plan().collect();
        assert!(!plan.is_empty());
        assert!(plan.iter().all(|p| p.row_group == 1), "{plan:?}");
        // Rows count from the first row that the plan covers.
        assert_eq!(plan[0].first_row, 0);
        let decoder = decoder.into_builder().unwrap().build().unwrap();
        assert_eq!(decoder.scan_plan().collect::<Vec<_>>(), plan);
    }

    #[test]
    fn plan_ends_before_invalid_row_group() {
        let plan: Vec<_> = builder()
            .with_row_groups(vec![0, 5])
            .build()
            .unwrap()
            .scan_plan()
            .collect();
        assert!(!plan.is_empty());
        assert!(plan.iter().all(|p| p.row_group == 0), "{plan:?}");
    }

    #[test]
    fn plan_is_lazy() {
        let decoder = builder().build().unwrap();
        let mut plan = decoder.scan_plan();
        let first = plan.next().unwrap();
        assert_eq!(first.row_group, 0);
        // Only the first row group has been started, and only the first
        // stage of it.
        let planner = plan.planner.as_ref().unwrap();
        let current = planner.current.as_ref().unwrap();
        assert_eq!(current.row_group.row_group_idx, 0);
        assert_eq!(planner.next_row, 200);
        // One cursor per projected column, not one entry per page.
        assert_eq!(current.stage.as_ref().unwrap().cursors.len(), 3);
    }

    #[test]
    fn selected_rows_counts_selected_positions() {
        let selection = RowSelection::from(vec![
            RowSelector::skip(10),
            RowSelector::select(5),
            RowSelector::skip(3),
            RowSelector::select(2),
        ]);
        let rows = SelectedRows::new(Some(&selection), 30);
        let counts: Vec<_> = [0, 10, 12, 15, 18, 19, 20, 30]
            .into_iter()
            .map(|raw| rows.selected_before(raw))
            .collect();
        assert_eq!(counts, vec![0, 0, 2, 5, 5, 6, 7, 7]);
        assert_eq!(SelectedRows::new(None, 30).selected_before(30), 30);
    }
}
