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
///
/// Two items are equal if their public fields are equal.
#[derive(Debug, Clone)]
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
}

/// Compares the public fields only. `first_row` and `last_row` count from
/// the start of a plan, so they differ between two plans of the same decoder.
impl PartialEq for PlannedRange {
    fn eq(&self, other: &Self) -> bool {
        let Self {
            range,
            row_group,
            column,
            kind,
            first_row: _,
            last_row: _,
        } = self;
        *range == other.range
            && *row_group == other.row_group
            && *column == other.column
            && *kind == other.kind
    }
}

impl Eq for PlannedRange {}

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

/// A decoding stage of a row group.
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
/// * The plan covers every byte that the decoder can request after the
///   plan is made. It can cover more, for example ranges that a
///   [`RowFilter`] makes unnecessary. One request can span more than one
///   planned range: without a row selection, the decoder requests a complete
///   column chunk, and the plan lists the pages of the column chunk.
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
///   on pushed data.
/// * Without a row selection, the state of the plan does not grow with the
///   number of pages. With a row selection, the plan keeps the indexes of
///   the selected pages of each column chunk in the current decoding stage
///   of the current row group.
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
        let planner = Planner {
            frontier,
            active_row_group,
            columns: Arc::new(columns),
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
            // The decoder reuses a column that an earlier stage read.
            // `columns_to_fetch` filters, so it gives no size hint. Reserve
            // for every column to avoid growing the vector one column at a time.
            let mut columns = Vec::with_capacity(num_columns);
            columns.extend(
                columns_to_fetch(fetch.projection, num_columns, |idx| planned_columns[idx]).map(
                    |column_idx| {
                        let (chunk_start, chunk_len) = row_group.column(column_idx).byte_range();
                        StageColumn {
                            column_idx,
                            chunk: chunk_start..chunk_start + chunk_len,
                        }
                    },
                ),
            );
            // The plan keeps `columns` until the end of the row group. Release
            // the capacity that a narrow projection does not use.
            columns.shrink_to_fit();
            for column in &columns {
                planned_columns[column.column_idx] = true;
            }
            stage_plans.push(StagePlan { stage, columns });
        }

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
                cursor.head = self.row_group.next_data_page(cursor);
                if let Some(head) = &cursor.head {
                    stage.heap.push(Reverse((range_order(head), cursor_idx)));
                }
                return Some(range);
            }
            let plan = self.stages.next()?;
            let cursors: Vec<_> = plan
                .columns
                .iter()
                .map(|column| self.row_group.column_cursor(column, plan.stage))
                .collect();
            let heap = cursors
                .iter()
                .enumerate()
                .filter_map(|(idx, cursor)| {
                    let head = cursor.head.as_ref()?;
                    Some(Reverse((range_order(head), idx)))
                })
                .collect();
            self.stage = Some(StageRanges { cursors, heap });
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
    ) -> PlannedRange {
        PlannedRange {
            range,
            first_row: rows.start,
            last_row: rows.end,
            row_group: self.row_group_idx,
            column: column_idx,
            kind,
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
    fn column_cursor(&self, column: &StageColumn, stage: ScanStage) -> ColumnCursor {
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
            cursor.head = Some(self.entry(column_idx, dictionary, rows, PageKind::Dictionary));
        } else {
            cursor.head = self.next_data_page(&mut cursor);
        }
        cursor
    }

    /// The next data page of `cursor`, if any.
    fn next_data_page(&self, cursor: &mut ColumnCursor) -> Option<PlannedRange> {
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
mod tests;
