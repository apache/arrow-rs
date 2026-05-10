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

//! Row-selection policy resolution for push decoder read plans.
//!
//! This module is the final safety gate between the high-level
//! `RowSelectionPolicy` requested by the caller and the concrete cursor used by
//! the record batch reader. It handles two independent concerns:
//!
//! ```text
//! Caller policy      Selection/page shape                  Resolved plan
//! -------------------------------------------------------------------------------
//! Auto               dense, short/fragmented runs          Mask
//! Auto               sparse page-loaded ranges             Selectors
//! Auto               expensive variable-width sparse output Selectors
//! Mask               dense page-loaded ranges              dense Mask
//! Mask               sparse page-loaded ranges             SparseMaskCursor
//! Selectors          any shape                             Selectors
//! ```
//!
//! The distinction between `Auto` and explicit `Mask` matters. `Auto` may
//! choose selectors to avoid a bad strategy. Explicit `Mask` must be honored,
//! so sparse page-loaded data is represented explicitly instead of being
//! silently converted to selectors.

use crate::arrow::ProjectionMask;
use crate::arrow::arrow_reader::selection::{
    LoadedRowRanges, RowSelectionShape, RowSelectionStrategy,
};
use crate::arrow::arrow_reader::{ReadPlanBuilder, RowSelection, RowSelectionPolicy};
use crate::basic::Type as PhysicalType;
use crate::file::metadata::RowGroupMetaData;
use crate::file::page_index::offset_index::OffsetIndexMetaData;
use std::ops::Range;

#[cfg(test)]
pub(super) fn resolve_selection_policy_for_projection(
    plan_builder: ReadPlanBuilder,
    projection_mask: &ProjectionMask,
    offset_index: Option<&[OffsetIndexMetaData]>,
    total_rows: usize,
) -> ReadPlanBuilder {
    resolve_selection_policy_for_expensive_output(
        plan_builder,
        projection_mask,
        offset_index,
        total_rows,
        ExpensiveOutputProfile::default(),
    )
}

pub(super) fn resolve_selection_policy_for_expensive_output(
    plan_builder: ReadPlanBuilder,
    projection_mask: &ProjectionMask,
    offset_index: Option<&[OffsetIndexMetaData]>,
    total_rows: usize,
    output_profile: ExpensiveOutputProfile,
) -> ReadPlanBuilder {
    // Page pruning can load only the pages that intersect selected rows. If the
    // projected columns have sparse loaded ranges, a dense mask would try to
    // decode rows for pages that are not present. Auto avoids that by choosing
    // selectors; explicit Mask carries the sparse ranges to the reader.
    let loaded = loaded_ranges_for_projection(
        plan_builder.selection(),
        projection_mask,
        offset_index,
        total_rows,
    );
    let loaded_is_sparse = loaded.as_ref().is_some_and(LoadedRowRanges::is_sparse);
    let sparse_loaded = loaded.filter(LoadedRowRanges::is_sparse);

    match plan_builder.row_selection_policy() {
        RowSelectionPolicy::Auto { .. } => {
            let decision = plan_builder.resolve_selection_strategy_decision();
            match decision.strategy {
                RowSelectionStrategy::Mask
                    if loaded_is_sparse
                        || should_prefer_selectors_for_expensive_output(
                            decision.shape,
                            output_profile,
                        ) =>
                {
                    plan_builder.with_row_selection_policy(RowSelectionPolicy::Selectors)
                }
                RowSelectionStrategy::Mask => {
                    plan_builder.with_row_selection_policy(RowSelectionPolicy::Mask)
                }
                RowSelectionStrategy::Selectors => {
                    plan_builder.with_row_selection_policy(RowSelectionPolicy::Selectors)
                }
            }
        }
        RowSelectionPolicy::Mask => plan_builder.with_loaded_row_ranges(sparse_loaded),
        RowSelectionPolicy::Selectors => plan_builder,
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct ExpensiveOutputProfile {
    pub(super) variable_width_columns: usize,
    pub(super) uncompressed_bytes_per_row: f64,
}

impl ExpensiveOutputProfile {
    pub(super) fn from_row_group(
        row_group: &RowGroupMetaData,
        projection_mask: &ProjectionMask,
        total_rows: usize,
    ) -> Self {
        if total_rows == 0 {
            return Self::default();
        }

        let mut variable_width_columns = 0;
        let mut uncompressed_bytes = 0u64;
        for leaf_idx in 0..row_group.num_columns() {
            if !projection_mask.leaf_included(leaf_idx) {
                continue;
            }

            let column = row_group.column(leaf_idx);
            if column.column_type() == PhysicalType::BYTE_ARRAY {
                variable_width_columns += 1;
            }
            uncompressed_bytes += column.uncompressed_size().max(0) as u64;
        }

        Self {
            variable_width_columns,
            uncompressed_bytes_per_row: uncompressed_bytes as f64 / total_rows as f64,
        }
    }
}

fn should_prefer_selectors_for_expensive_output(
    shape: RowSelectionShape,
    output_profile: ExpensiveOutputProfile,
) -> bool {
    // Sparse, low-selectivity output over variable-width columns can be worse
    // with masks because masks decode and then filter many values that selectors
    // can skip. This is intentionally narrow; most fragmented selections remain
    // good candidates for masks.
    let selected_ratio = shape.selected_ratio();
    output_profile.variable_width_columns > 0
        && output_profile.uncompressed_bytes_per_row >= 16.0
        && selected_ratio > 0.0
        && selected_ratio < 0.10
        && shape.average_selected_run_length() <= 4.0
}

#[cfg_attr(test, allow(dead_code))]
pub(super) fn loaded_ranges_for_projection(
    selection: Option<&RowSelection>,
    projection_mask: &ProjectionMask,
    offset_index: Option<&[OffsetIndexMetaData]>,
    total_rows: usize,
) -> Option<LoadedRowRanges> {
    // Loaded ranges are row ranges backed by page data for all projected
    // columns. When projections include multiple columns, a row is safe for
    // sparse-mask decoding only if every projected column loaded the page that
    // contains that row. Therefore projected-column ranges are intersected.
    //
    // ```text
    // column A pages loaded:  [0..50) [80..100)
    // column B pages loaded:  [20..70) [80..100)
    // usable loaded ranges:   [20..50) [80..100)
    // ```
    let selection = selection?;
    let columns = offset_index?;
    let mut ranges: Option<Vec<Range<usize>>> = None;

    for (leaf_idx, column) in columns.iter().enumerate() {
        if !projection_mask.leaf_included(leaf_idx) {
            continue;
        }
        let column_ranges = selection.selected_page_row_ranges(column.page_locations(), total_rows);
        ranges = Some(match ranges {
            Some(existing) => intersect_ranges(existing, column_ranges),
            None => column_ranges,
        });
    }

    ranges.map(|ranges| LoadedRowRanges::new(coalesce_adjacent_ranges(ranges), total_rows))
}

fn intersect_ranges(left: Vec<Range<usize>>, right: Vec<Range<usize>>) -> Vec<Range<usize>> {
    let mut out = Vec::new();
    let mut left_idx = 0;
    let mut right_idx = 0;

    while left_idx < left.len() && right_idx < right.len() {
        let l = &left[left_idx];
        let r = &right[right_idx];
        let start = l.start.max(r.start);
        let end = l.end.min(r.end);

        if start < end {
            out.push(start..end);
        }

        if l.end <= r.end {
            left_idx += 1;
        } else {
            right_idx += 1;
        }
    }

    out
}

fn coalesce_adjacent_ranges(ranges: Vec<Range<usize>>) -> Vec<Range<usize>> {
    let mut out: Vec<Range<usize>> = Vec::with_capacity(ranges.len());
    for range in ranges {
        if range.is_empty() {
            continue;
        }
        if let Some(last) = out.last_mut() {
            if last.end == range.start {
                last.end = range.end;
                continue;
            }
        }
        out.push(range);
    }
    out
}
