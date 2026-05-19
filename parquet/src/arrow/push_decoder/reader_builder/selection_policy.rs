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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::ProjectionMask;
    use crate::arrow::arrow_reader::selection::LoadedRowRanges;
    use crate::arrow::arrow_reader::{
        ReadPlanBuilder, RowSelection, RowSelectionCursor, RowSelectionPolicy, RowSelector,
    };
    use crate::file::page_index::offset_index::{OffsetIndexMetaData, PageLocation};

    #[test]
    fn test_resolve_selection_policy_preserves_mask_choice() {
        let selection = RowSelection::from(vec![
            RowSelector::select(1),
            RowSelector::skip(99),
            RowSelector::select(1),
        ]);
        let plan_builder = ReadPlanBuilder::new(1024)
            .with_selection(Some(selection))
            .with_row_selection_policy(RowSelectionPolicy::Auto { threshold: 1024 });

        assert_eq!(
            resolve_selection_policy_for_projection(
                plan_builder,
                &ProjectionMask::all(),
                None,
                101
            )
            .row_selection_policy(),
            &RowSelectionPolicy::Mask
        );
    }

    #[test]
    fn test_resolve_selection_policy_preserves_selector_choice() {
        let selection = RowSelection::from(vec![RowSelector::select(128)]);
        let plan_builder = ReadPlanBuilder::new(1024)
            .with_selection(Some(selection))
            .with_row_selection_policy(RowSelectionPolicy::Auto { threshold: 1 });

        assert_eq!(
            resolve_selection_policy_for_projection(
                plan_builder,
                &ProjectionMask::all(),
                None,
                128
            )
            .row_selection_policy(),
            &RowSelectionPolicy::Selectors
        );
    }

    #[test]
    fn test_resolve_selection_policy_respects_explicit_policy() {
        let selection = RowSelection::from(vec![RowSelector::select(1), RowSelector::skip(1)]);
        let mask_builder = ReadPlanBuilder::new(1024)
            .with_selection(Some(selection.clone()))
            .with_row_selection_policy(RowSelectionPolicy::Mask);
        let selector_builder = ReadPlanBuilder::new(1024)
            .with_selection(Some(selection))
            .with_row_selection_policy(RowSelectionPolicy::Selectors);

        assert_eq!(
            resolve_selection_policy_for_projection(mask_builder, &ProjectionMask::all(), None, 2)
                .row_selection_policy(),
            &RowSelectionPolicy::Mask
        );
        assert_eq!(
            resolve_selection_policy_for_projection(
                selector_builder,
                &ProjectionMask::all(),
                None,
                2
            )
            .row_selection_policy(),
            &RowSelectionPolicy::Selectors
        );
    }

    #[test]
    fn test_auto_sparse_loaded_ranges_force_selectors() {
        let selection = RowSelection::from(vec![
            RowSelector::select(1),
            RowSelector::skip(4),
            RowSelector::select(1),
        ]);
        let plan_builder = ReadPlanBuilder::new(1024)
            .with_selection(Some(selection))
            .with_row_selection_policy(RowSelectionPolicy::Auto { threshold: 1024 });
        let offset_index = sparse_test_offset_index();

        let plan_builder = resolve_selection_policy_for_projection(
            plan_builder,
            &ProjectionMask::all(),
            Some(&offset_index),
            6,
        );

        assert_eq!(
            plan_builder.row_selection_policy(),
            &RowSelectionPolicy::Selectors
        );
    }

    #[test]
    fn test_auto_dense_loaded_ranges_preserve_mask() {
        let selection = RowSelection::from(vec![
            RowSelector::select(1),
            RowSelector::skip(1),
            RowSelector::select(1),
            RowSelector::skip(1),
            RowSelector::select(1),
            RowSelector::skip(1),
        ]);
        let plan_builder = ReadPlanBuilder::new(1024)
            .with_selection(Some(selection))
            .with_row_selection_policy(RowSelectionPolicy::Auto { threshold: 1024 });
        let offset_index = sparse_test_offset_index();

        let plan_builder = resolve_selection_policy_for_projection(
            plan_builder,
            &ProjectionMask::all(),
            Some(&offset_index),
            6,
        );

        assert_eq!(
            plan_builder.row_selection_policy(),
            &RowSelectionPolicy::Mask
        );
    }

    #[test]
    fn test_explicit_mask_keeps_sparse_loaded_ranges() {
        let selection = RowSelection::from(vec![
            RowSelector::select(1),
            RowSelector::skip(4),
            RowSelector::select(1),
        ]);
        let plan_builder = ReadPlanBuilder::new(1024)
            .with_selection(Some(selection))
            .with_row_selection_policy(RowSelectionPolicy::Mask);
        let offset_index = sparse_test_offset_index();

        let plan_builder = resolve_selection_policy_for_projection(
            plan_builder,
            &ProjectionMask::all(),
            Some(&offset_index),
            6,
        );

        assert_eq!(
            plan_builder.row_selection_policy(),
            &RowSelectionPolicy::Mask
        );

        let mut plan = plan_builder.build();
        let RowSelectionCursor::Mask(cursor) = plan.row_selection_cursor_mut() else {
            panic!("expected mask cursor");
        };
        assert!(cursor.is_sparse());
    }

    #[test]
    fn test_loaded_ranges_intersects_many_ranges_across_projected_columns() {
        let selection = RowSelection::from(vec![
            RowSelector::skip(10),
            RowSelector::select(1),
            RowSelector::skip(39),
            RowSelector::select(1),
            RowSelector::skip(39),
            RowSelector::select(1),
            RowSelector::skip(9),
        ]);
        let offset_index = vec![
            offset_index_column(&[0, 20, 40, 60, 80]),
            offset_index_column(&[0, 15, 35, 55, 75]),
            offset_index_column(&[0, 10, 30, 50, 70, 90]),
        ];

        let loaded = loaded_ranges_for_projection(
            Some(&selection),
            &ProjectionMask::all(),
            Some(&offset_index),
            100,
        );

        assert_eq!(
            loaded,
            Some(LoadedRowRanges::new(vec![10..15, 50..55, 90..100], 100))
        );
    }

    #[test]
    fn test_auto_expensive_fragmented_output_prefers_selectors() {
        let selection = q38_like_fragmented_selection();
        let plan_builder = ReadPlanBuilder::new(1024)
            .with_selection(Some(selection))
            .with_row_selection_policy(RowSelectionPolicy::Auto { threshold: 1024 });
        let profile = ExpensiveOutputProfile {
            variable_width_columns: 1,
            uncompressed_bytes_per_row: 64.0,
        };

        let plan_builder = resolve_selection_policy_for_expensive_output(
            plan_builder,
            &ProjectionMask::all(),
            None,
            7_800,
            profile,
        );

        assert_eq!(
            plan_builder.row_selection_policy(),
            &RowSelectionPolicy::Selectors
        );
    }

    #[test]
    fn test_auto_expensive_fragmented_output_prefers_selectors_without_selector_count_gate() {
        let selection = RowSelection::from(vec![
            RowSelector::select(1),
            RowSelector::skip(12),
            RowSelector::select(1),
            RowSelector::skip(12),
            RowSelector::select(1),
            RowSelector::skip(12),
            RowSelector::select(1),
            RowSelector::skip(12),
        ]);
        let plan_builder = ReadPlanBuilder::new(1024)
            .with_selection(Some(selection))
            .with_row_selection_policy(RowSelectionPolicy::Auto { threshold: 1024 });
        let profile = ExpensiveOutputProfile {
            variable_width_columns: 1,
            uncompressed_bytes_per_row: 64.0,
        };

        let plan_builder = resolve_selection_policy_for_expensive_output(
            plan_builder,
            &ProjectionMask::all(),
            None,
            52,
            profile,
        );

        assert_eq!(
            plan_builder.row_selection_policy(),
            &RowSelectionPolicy::Selectors
        );
    }

    #[test]
    fn test_auto_cheap_fragmented_output_keeps_mask() {
        let selection = q38_like_fragmented_selection();
        let plan_builder = ReadPlanBuilder::new(1024)
            .with_selection(Some(selection))
            .with_row_selection_policy(RowSelectionPolicy::Auto { threshold: 1024 });
        let profile = ExpensiveOutputProfile {
            variable_width_columns: 1,
            uncompressed_bytes_per_row: 8.0,
        };

        let plan_builder = resolve_selection_policy_for_expensive_output(
            plan_builder,
            &ProjectionMask::all(),
            None,
            7_800,
            profile,
        );

        assert_eq!(
            plan_builder.row_selection_policy(),
            &RowSelectionPolicy::Mask
        );
    }

    #[test]
    fn test_auto_moderate_selectivity_expensive_output_keeps_mask() {
        let selection = q26_like_fragmented_selection();
        let plan_builder = ReadPlanBuilder::new(1024)
            .with_selection(Some(selection))
            .with_row_selection_policy(RowSelectionPolicy::Auto { threshold: 1024 });
        let profile = ExpensiveOutputProfile {
            variable_width_columns: 1,
            uncompressed_bytes_per_row: 64.0,
        };

        let plan_builder = resolve_selection_policy_for_expensive_output(
            plan_builder,
            &ProjectionMask::all(),
            None,
            7_200,
            profile,
        );

        assert_eq!(
            plan_builder.row_selection_policy(),
            &RowSelectionPolicy::Mask
        );
    }

    fn q38_like_fragmented_selection() -> RowSelection {
        let mut selectors = Vec::new();
        for _ in 0..600 {
            selectors.push(RowSelector::select(1));
            selectors.push(RowSelector::skip(12));
        }
        RowSelection::from(selectors)
    }

    fn q26_like_fragmented_selection() -> RowSelection {
        let mut selectors = Vec::new();
        for _ in 0..600 {
            selectors.push(RowSelector::select(2));
            selectors.push(RowSelector::skip(10));
        }
        RowSelection::from(selectors)
    }

    fn sparse_test_offset_index() -> Vec<OffsetIndexMetaData> {
        vec![OffsetIndexMetaData {
            page_locations: vec![
                PageLocation {
                    offset: 0,
                    compressed_page_size: 10,
                    first_row_index: 0,
                },
                PageLocation {
                    offset: 10,
                    compressed_page_size: 10,
                    first_row_index: 2,
                },
                PageLocation {
                    offset: 20,
                    compressed_page_size: 10,
                    first_row_index: 4,
                },
            ],
            unencoded_byte_array_data_bytes: None,
        }]
    }

    fn offset_index_column(first_rows: &[i64]) -> OffsetIndexMetaData {
        OffsetIndexMetaData {
            page_locations: first_rows
                .iter()
                .enumerate()
                .map(|(idx, first_row_index)| PageLocation {
                    offset: (idx * 10) as i64,
                    compressed_page_size: 10,
                    first_row_index: *first_row_index,
                })
                .collect(),
            unencoded_byte_array_data_bytes: None,
        }
    }
}
