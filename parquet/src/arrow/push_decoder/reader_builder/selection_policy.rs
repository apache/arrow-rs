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
