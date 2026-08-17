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

//! Shared immutable execution windows for per-column row selection.
//!
//! A plan compiles output batch and row-group boundaries once. The logical
//! selection owns lazy, symmetric selector and mask representations shared by
//! every top-level column reader. Columns retain only their ordinal and choose
//! how to replay each row-group chunk.

use super::boolean::boolean_mask_from_selectors;
use super::{
    LoadedRowRanges, MaskRunIter, RowSelection, RowSelectionInner, RowSelectionStrategy,
    RowSelector,
};
use crate::errors::{ParquetError, Result};
use arrow_buffer::BooleanBuffer;
use std::ops::Range;
use std::sync::{Arc, OnceLock};

#[derive(Debug)]
struct SelectionRepresentations {
    source: SelectionSource,
    selectors: OnceLock<Arc<[RowSelector]>>,
    mask: OnceLock<BooleanBuffer>,
}

#[derive(Debug)]
enum SelectionSource {
    Selectors(Arc<[RowSelector]>),
    Mask(BooleanBuffer),
}

impl SelectionRepresentations {
    fn new(source: RowSelection) -> Self {
        let source = match source.into_inner() {
            RowSelectionInner::Selectors(selectors) => SelectionSource::Selectors(selectors.into()),
            RowSelectionInner::Mask(mask) => SelectionSource::Mask((*mask).into_mask()),
        };
        Self {
            source,
            selectors: OnceLock::new(),
            mask: OnceLock::new(),
        }
    }

    fn selectors(&self) -> &[RowSelector] {
        match &self.source {
            SelectionSource::Selectors(selectors) => selectors,
            SelectionSource::Mask(mask) => self
                .selectors
                .get_or_init(|| MaskRunIter::new(mask).collect::<Vec<_>>().into())
                .as_ref(),
        }
    }

    fn mask(&self) -> &BooleanBuffer {
        match &self.source {
            SelectionSource::Mask(mask) => mask,
            SelectionSource::Selectors(selectors) => self
                .mask
                .get_or_init(|| boolean_mask_from_selectors(selectors)),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SelectorPosition {
    selector: usize,
    offset: usize,
}

#[derive(Debug)]
pub(crate) struct BatchWindow {
    pub(crate) chunks: Range<usize>,
    pub(crate) selected_rows: usize,
}

#[derive(Debug)]
pub(crate) struct BatchWindowChunk {
    pub(crate) row_group: usize,
    pub(crate) physical_rows: Range<usize>,
    pub(crate) selected_rows: usize,
    first_selected_offset: Option<usize>,
    selector_start: SelectorPosition,
    selector_end: SelectorPosition,
}

#[derive(Debug)]
pub(crate) struct MaskExecutionChunk {
    pub(crate) initial_skip: usize,
    pub(crate) row_count: usize,
    pub(crate) mask: BooleanBuffer,
}

#[derive(Debug)]
pub(crate) struct RowSelectionExecutionPlan {
    selection: SelectionRepresentations,
    batches: Box<[BatchWindow]>,
    chunks: Box<[BatchWindowChunk]>,
    strategies: Box<[RowSelectionStrategy]>,
    loaded_row_ranges: Box<[Option<LoadedRowRanges>]>,
    row_group_offsets: Box<[usize]>,
    column_count: usize,
}

impl RowSelectionExecutionPlan {
    pub(crate) fn try_new(
        selection: RowSelection,
        row_group_rows: &[usize],
        column_count: usize,
        strategies: Vec<RowSelectionStrategy>,
        loaded_row_ranges: Option<Vec<Option<LoadedRowRanges>>>,
        batch_size: usize,
    ) -> Result<Self> {
        if batch_size == 0 {
            return Err(general_err!(
                "Internal Error: per-column row selection requires a non-zero batch size"
            ));
        }
        let matrix_len = row_group_rows.len().saturating_mul(column_count);
        if strategies.len() != matrix_len {
            return Err(general_err!(
                "Internal Error: per-column strategy matrix has {} entries, expected {}",
                strategies.len(),
                matrix_len
            ));
        }
        let loaded_row_ranges = match loaded_row_ranges {
            Some(ranges) if ranges.len() != matrix_len => {
                return Err(general_err!(
                    "Internal Error: per-column loaded-range matrix has {} entries, expected {}",
                    ranges.len(),
                    matrix_len
                ));
            }
            Some(ranges) => ranges,
            None => std::iter::repeat_with(|| None).take(matrix_len).collect(),
        };

        let mut row_group_offsets = Vec::with_capacity(row_group_rows.len());
        let mut offset = 0usize;
        for &rows in row_group_rows {
            row_group_offsets.push(offset);
            offset = offset
                .checked_add(rows)
                .ok_or_else(|| general_err!("Internal Error: row-group row count overflow"))?;
        }

        let selection = SelectionRepresentations::new(selection.trim());
        let (chunks, batches) = compile_windows(selection.selectors(), row_group_rows, batch_size)?;
        Ok(Self {
            selection,
            batches: batches.into_boxed_slice(),
            chunks: chunks.into_boxed_slice(),
            strategies: strategies.into_boxed_slice(),
            loaded_row_ranges: loaded_row_ranges.into_boxed_slice(),
            row_group_offsets: row_group_offsets.into_boxed_slice(),
            column_count,
        })
    }

    pub(crate) fn batch(&self, batch_idx: usize) -> Option<&BatchWindow> {
        self.batches.get(batch_idx)
    }

    pub(crate) fn chunks(&self, batch: &BatchWindow) -> &[BatchWindowChunk] {
        &self.chunks[batch.chunks.clone()]
    }

    pub(crate) fn strategy(
        &self,
        chunk: &BatchWindowChunk,
        column_idx: usize,
    ) -> RowSelectionStrategy {
        self.strategies[chunk.row_group * self.column_count + column_idx]
    }

    pub(crate) fn selector_instructions(
        &self,
        chunk: &BatchWindowChunk,
    ) -> impl Iterator<Item = RowSelector> + '_ {
        SelectorWindowIter {
            selectors: self.selection.selectors(),
            position: chunk.selector_start,
            end: chunk.selector_end,
        }
    }

    /// Lower a Mask window into decode-safe fragments for one column.
    ///
    /// Sparse page loading can leave physical gaps. Each returned fragment is
    /// wholly contained in a loaded range; `initial_skip` is measured from the
    /// end of the previous fragment (or the start of `chunk`). Callers must
    /// skip any trailing rows not covered by the returned fragments.
    pub(crate) fn mask_execution_chunks(
        &self,
        chunk: &BatchWindowChunk,
        column_idx: usize,
    ) -> Result<Vec<MaskExecutionChunk>> {
        if chunk.first_selected_offset.is_none() {
            return Ok(Vec::new());
        }

        let matrix_idx = chunk.row_group * self.column_count + column_idx;
        let row_group_offset = self.row_group_offsets[chunk.row_group];
        let mask = self.selection.mask();
        let mut cursor = chunk.physical_rows.start;
        let mut selected_rows = 0usize;
        let mut fragments = Vec::new();

        let mut append_range = |range: Range<usize>| {
            let start = range.start.max(chunk.physical_rows.start);
            let end = range.end.min(chunk.physical_rows.end);
            if start >= end {
                return;
            }

            let Some(first_selected) = (start..end).find(|&row| mask.value(row)) else {
                return;
            };
            let row_count = end - first_selected;
            let fragment_mask = mask.slice(first_selected, row_count);
            selected_rows += fragment_mask.count_set_bits();
            fragments.push(MaskExecutionChunk {
                initial_skip: first_selected - cursor,
                row_count,
                mask: fragment_mask,
            });
            cursor = end;
        };

        match &self.loaded_row_ranges[matrix_idx] {
            Some(ranges) => {
                for range in ranges.ranges() {
                    append_range(row_group_offset + range.start..row_group_offset + range.end);
                }
            }
            None => append_range(chunk.physical_rows.clone()),
        }

        if selected_rows != chunk.selected_rows {
            return Err(general_err!(
                "Internal Error: loaded pages cover {selected_rows} selected rows, expected {}",
                chunk.selected_rows
            ));
        }
        Ok(fragments)
    }
}

fn compile_windows(
    selectors: &[RowSelector],
    row_group_rows: &[usize],
    batch_size: usize,
) -> Result<(Vec<BatchWindowChunk>, Vec<BatchWindow>)> {
    let mut row_group_ends = Vec::with_capacity(row_group_rows.len());
    let mut total_rows = 0usize;
    for &rows in row_group_rows {
        total_rows = total_rows
            .checked_add(rows)
            .ok_or_else(|| general_err!("Internal Error: row-group row count overflow"))?;
        row_group_ends.push(total_rows);
    }

    let selection_rows = selectors.iter().try_fold(0usize, |rows, selector| {
        rows.checked_add(selector.row_count)
    });
    if selection_rows.is_none_or(|rows| rows > total_rows) {
        return Err(general_err!(
            "Internal Error: row selection extends beyond planned row groups"
        ));
    }

    let mut chunks = Vec::new();
    let mut batches = Vec::new();
    let mut position = SelectorPosition {
        selector: 0,
        offset: 0,
    };
    let mut physical_position = 0usize;
    let mut row_group = 0usize;
    let mut batch_chunk_start = 0usize;
    let mut batch_selected_rows = 0usize;

    while position.selector < selectors.len() {
        while row_group < row_group_ends.len() && physical_position == row_group_ends[row_group] {
            row_group += 1;
        }
        if row_group == row_group_ends.len() {
            return Err(general_err!(
                "Internal Error: row selection extends beyond planned row groups"
            ));
        }

        let chunk_start = position;
        let chunk_physical_start = physical_position;
        let mut chunk_selected_rows = 0usize;
        let mut first_selected_offset = None;

        while position.selector < selectors.len()
            && physical_position < row_group_ends[row_group]
            && batch_selected_rows < batch_size
        {
            let selector = selectors[position.selector];
            let selector_remaining = selector.row_count - position.offset;
            let row_group_remaining = row_group_ends[row_group] - physical_position;
            let take = if selector.skip {
                selector_remaining.min(row_group_remaining)
            } else {
                selector_remaining
                    .min(row_group_remaining)
                    .min(batch_size - batch_selected_rows)
            };

            if !selector.skip {
                first_selected_offset.get_or_insert(physical_position - chunk_physical_start);
                chunk_selected_rows += take;
                batch_selected_rows += take;
            }
            physical_position += take;
            advance_selector_position(&mut position, selector.row_count, take);
        }

        if physical_position == chunk_physical_start {
            return Err(general_err!(
                "Internal Error: per-column window compiler made no progress"
            ));
        }
        chunks.push(BatchWindowChunk {
            row_group,
            physical_rows: chunk_physical_start..physical_position,
            selected_rows: chunk_selected_rows,
            first_selected_offset,
            selector_start: chunk_start,
            selector_end: position,
        });

        if batch_selected_rows == batch_size {
            batches.push(BatchWindow {
                chunks: batch_chunk_start..chunks.len(),
                selected_rows: batch_selected_rows,
            });
            batch_chunk_start = chunks.len();
            batch_selected_rows = 0;
        }
    }

    if batch_selected_rows != 0 {
        batches.push(BatchWindow {
            chunks: batch_chunk_start..chunks.len(),
            selected_rows: batch_selected_rows,
        });
    } else if batch_chunk_start != chunks.len() {
        return Err(general_err!(
            "Internal Error: per-column plan ends in an unselected window"
        ));
    }

    Ok((chunks, batches))
}

fn advance_selector_position(position: &mut SelectorPosition, selector_rows: usize, rows: usize) {
    position.offset += rows;
    if position.offset == selector_rows {
        position.selector += 1;
        position.offset = 0;
    }
}

struct SelectorWindowIter<'a> {
    selectors: &'a [RowSelector],
    position: SelectorPosition,
    end: SelectorPosition,
}

impl Iterator for SelectorWindowIter<'_> {
    type Item = RowSelector;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position == self.end {
            return None;
        }

        let selector = self.selectors[self.position.selector];
        let end_offset = if self.position.selector == self.end.selector {
            self.end.offset
        } else {
            selector.row_count
        };
        let row_count = end_offset - self.position.offset;
        advance_selector_position(&mut self.position, selector.row_count, row_count);
        Some(RowSelector {
            row_count,
            skip: selector.skip,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strategies(row_groups: usize, columns: usize) -> Vec<RowSelectionStrategy> {
        vec![RowSelectionStrategy::Selectors; row_groups * columns]
    }

    #[test]
    fn windows_share_selector_positions_and_cross_row_groups() {
        let selection = RowSelection::from(vec![
            RowSelector::skip(2),
            RowSelector::select(3),
            RowSelector::skip(4),
            RowSelector::select(4),
        ]);
        let plan =
            RowSelectionExecutionPlan::try_new(selection, &[5, 8], 2, strategies(2, 2), None, 5)
                .unwrap();

        let first = plan.batch(0).unwrap();
        assert_eq!(first.selected_rows, 5);
        let chunks = plan.chunks(first);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].row_group, 0);
        assert_eq!(chunks[0].selected_rows, 3);
        assert_eq!(chunks[1].row_group, 1);
        assert_eq!(chunks[1].selected_rows, 2);
        assert_eq!(
            chunks
                .iter()
                .flat_map(|chunk| plan.selector_instructions(chunk))
                .collect::<Vec<_>>(),
            vec![
                RowSelector::skip(2),
                RowSelector::select(3),
                RowSelector::skip(4),
                RowSelector::select(2),
            ]
        );

        let second = plan.batch(1).unwrap();
        assert_eq!(second.selected_rows, 2);
        assert_eq!(
            plan.selector_instructions(&plan.chunks(second)[0])
                .collect::<Vec<_>>(),
            vec![RowSelector::select(2)]
        );
        assert!(plan.batch(2).is_none());
    }

    #[test]
    fn mask_representation_is_shared_and_sliced_to_first_selected_row() {
        let selection = RowSelection::from(vec![
            RowSelector::skip(2),
            RowSelector::select(2),
            RowSelector::skip(3),
            RowSelector::select(1),
        ]);
        let plan = RowSelectionExecutionPlan::try_new(
            selection,
            &[8],
            1,
            vec![RowSelectionStrategy::Mask],
            None,
            8,
        )
        .unwrap();
        let chunk = &plan.chunks(plan.batch(0).unwrap())[0];
        let mask = plan.mask_execution_chunks(chunk, 0).unwrap();
        assert_eq!(mask.len(), 1);
        let mask = &mask[0];
        assert_eq!(mask.initial_skip, 2);
        assert_eq!(mask.row_count, 6);
        assert_eq!(
            mask.mask,
            BooleanBuffer::from(vec![true, true, false, false, false, true])
        );
        assert!(std::ptr::eq(plan.selection.mask(), plan.selection.mask()));
    }

    #[test]
    fn dual_representations_reuse_the_source_and_cache_only_the_other_form() {
        let selectors = SelectionRepresentations::new(RowSelection::from(vec![
            RowSelector::skip(1),
            RowSelector::select(2),
        ]));
        let SelectionSource::Selectors(source_selectors) = &selectors.source else {
            unreachable!()
        };
        assert_eq!(selectors.selectors().as_ptr(), source_selectors.as_ptr());
        assert!(std::ptr::eq(selectors.mask(), selectors.mask()));

        let mask = SelectionRepresentations::new(RowSelection::from(BooleanBuffer::from(vec![
            false, true, true,
        ])));
        let SelectionSource::Mask(source_mask) = &mask.source else {
            unreachable!()
        };
        assert!(std::ptr::eq(mask.mask(), source_mask));
        assert_eq!(mask.selectors().as_ptr(), mask.selectors().as_ptr());
    }

    #[test]
    fn mask_fragments_do_not_cross_unloaded_page_ranges() {
        let selection = RowSelection::from(vec![
            RowSelector::skip(1),
            RowSelector::select(1),
            RowSelector::skip(8),
            RowSelector::select(1),
        ]);
        let loaded = LoadedRowRanges::from_selection(RowSelection::from_consecutive_ranges(
            [0..4, 9..12].into_iter(),
            12,
        ));
        let plan = RowSelectionExecutionPlan::try_new(
            selection,
            &[12],
            1,
            vec![RowSelectionStrategy::Mask],
            Some(vec![Some(loaded)]),
            8,
        )
        .unwrap();

        let chunk = &plan.chunks(plan.batch(0).unwrap())[0];
        let fragments = plan.mask_execution_chunks(chunk, 0).unwrap();
        assert_eq!(fragments.len(), 2);
        assert_eq!(fragments[0].initial_skip, 1);
        assert_eq!(fragments[0].row_count, 3);
        assert_eq!(
            fragments[0].mask,
            BooleanBuffer::from(vec![true, false, false])
        );
        assert_eq!(fragments[1].initial_skip, 6);
        assert_eq!(fragments[1].row_count, 1);
        assert_eq!(fragments[1].mask, BooleanBuffer::from(vec![true]));
    }
}
