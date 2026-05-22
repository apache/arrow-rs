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

//! Runtime post-filter cost decisions for push decoder row groups.
//!
//! The cost model is intentionally adaptive rather than purely static. There
//! are two ways to enter post-filter execution:
//!
//! * a narrow static rule starts there for variable-width predicate columns
//!   that are not already part of the output projection, where building
//!   fragmented pushdown selections is commonly expensive
//! * the first eligible row group runs predicate pushdown, records the actual
//!   `RowSelection` shape, and lets later row groups use post-filter if the
//!   shape suggests pushdown is doing extra work without pruning enough rows.
//!   When predicate columns are already part of the output projection, the
//!   observed selected-row ratio can also choose post-filter without requiring
//!   fragmented selected runs.
//!
//! ```text
//! Start
//!   |
//!   v
//! Observing -- incomplete observation --> Observing
//!   |
//!   +-- pushdown still preferred ------> UsePushdown
//!   |
//!   +-- post-filter preferred + supported --> UsePostFilter
//! ```
//!
//! The cost model only applies to `Auto`. Explicit `Mask` and `Selectors` are treated
//! as user intent and are not overridden here.

use super::{RowBudget, RowGroupReaderBuilder};
use crate::arrow::ProjectionMask;
use crate::arrow::arrow_reader::RowFilter;
use crate::arrow::arrow_reader::RowSelectionPolicy;
use crate::arrow::arrow_reader::selection::{
    CostModelDecisionReason, CostModelObservation, RowSelectionShape, RowSelectionStrategyDecision,
};
use crate::arrow::schema::{ParquetField, ParquetFieldType};
use crate::basic::Type as PhysicalType;

#[derive(Debug)]
pub(super) enum RowGroupCostModelState {
    /// Collect row-selection shape from early row groups before choosing a mode.
    Observing { observation: CostModelObservation },
    /// Predicate pushdown remains the execution mode for this reader.
    UsePushdown,
    /// Later row groups should decode once and evaluate predicates after decode.
    UsePostFilter,
}

impl Default for RowGroupCostModelState {
    fn default() -> Self {
        Self::Observing {
            observation: CostModelObservation::default(),
        }
    }
}

impl RowGroupReaderBuilder {
    const CHEAP_FIXED_WIDTH_READ_BYTES_PER_ROW: f64 = 24.0;

    pub(super) fn should_use_post_filter_by_cost(&self, budget: RowBudget) -> bool {
        // Keep the runtime switch narrow:
        //
        // * `Auto` means the caller allowed the reader to choose.
        // * `limit` and `offset` are applied during row-group planning; moving
        //   predicates after decode changes where short-circuiting can happen.
        // * virtual columns are not read from Parquet pages and need their
        //   existing projection path.
        matches!(self.cost_model_state, RowGroupCostModelState::UsePostFilter)
            && self.post_filter_cost_model_enabled
            && matches!(self.row_selection_policy, RowSelectionPolicy::Auto { .. })
            && budget.is_unbounded()
            && !self.has_virtual_columns()
    }

    pub(super) fn post_filter_read_projection(
        &self,
        filter: &RowFilter,
        budget: RowBudget,
    ) -> Option<ProjectionMask> {
        if !self.should_use_post_filter_by_cost(budget) {
            return None;
        }

        self.build_post_filter_read_projection(filter)
    }

    pub(super) fn post_filter_read_projection_for_filter(
        &self,
        filter: &RowFilter,
        budget: RowBudget,
    ) -> Option<ProjectionMask> {
        if !self.post_filter_supports_filter(filter, budget) {
            return None;
        }

        self.build_post_filter_read_projection(filter)
    }

    pub(super) fn should_start_with_post_filter(
        &self,
        filter: &RowFilter,
        row_group_idx: usize,
        budget: RowBudget,
    ) -> bool {
        if !self.post_filter_supports_filter(filter, budget) {
            return false;
        }

        let Some(predicate_projection) = filter.union_projection() else {
            return false;
        };
        let predicate_already_projected =
            self.projection_includes_all(&self.projection, &predicate_projection);

        self.should_start_with_post_filter_for_unprojected_variable_width_predicate(
            &predicate_projection,
            predicate_already_projected,
            row_group_idx,
        ) || self.should_start_with_post_filter_for_cheap_fixed_width_read(
            filter,
            predicate_already_projected,
            row_group_idx,
        )
    }

    fn should_start_with_post_filter_for_unprojected_variable_width_predicate(
        &self,
        predicate_projection: &ProjectionMask,
        predicate_already_projected: bool,
        row_group_idx: usize,
    ) -> bool {
        !predicate_already_projected
            && self.projection_has_variable_width_leaf(row_group_idx, predicate_projection)
    }

    fn should_start_with_post_filter_for_cheap_fixed_width_read(
        &self,
        filter: &RowFilter,
        predicate_already_projected: bool,
        row_group_idx: usize,
    ) -> bool {
        // If predicate columns are already in the output projection, pushdown
        // cannot save a deferred output read for those columns. For cheap
        // fixed-width reads, starting directly with post-filter avoids building
        // a row selection just to decode the same values again.
        //
        // Do not apply this to deferred output columns: sparse predicates can
        // still win by reading only a handful of output values.
        if !predicate_already_projected {
            return false;
        }

        // Cacheable predicate columns need one pushdown row group to reveal
        // whether selection is sparse. Starting post-filter here bypasses the
        // predicate cache before the adaptive model can observe that shape.
        if self.has_cacheable_projected_predicate(filter) {
            return false;
        }

        let Some(read_projection) = self.build_post_filter_read_projection(filter) else {
            return false;
        };

        let row_group = self.metadata.row_group(row_group_idx);
        if row_group.num_rows() == 0 {
            return false;
        }

        let mut projected_uncompressed_bytes = 0u64;
        for leaf_idx in 0..row_group.num_columns() {
            if !read_projection.leaf_included(leaf_idx) {
                continue;
            }

            let column = row_group.column(leaf_idx);
            if column.column_type() == PhysicalType::BYTE_ARRAY {
                return false;
            }
            projected_uncompressed_bytes += column.uncompressed_size().max(0) as u64;
        }

        projected_uncompressed_bytes as f64 / row_group.num_rows() as f64
            <= Self::CHEAP_FIXED_WIDTH_READ_BYTES_PER_ROW
    }

    fn has_cacheable_projected_predicate(&self, filter: &RowFilter) -> bool {
        let Some(cache_projection) = self.compute_cache_projection_inner(filter) else {
            return false;
        };

        let schema = self.metadata.file_metadata().schema_descr();
        (0..schema.num_columns()).any(|leaf_idx| cache_projection.leaf_included(leaf_idx))
    }

    fn build_post_filter_read_projection(&self, filter: &RowFilter) -> Option<ProjectionMask> {
        // Post-filter execution decodes each row once, so it needs both:
        //
        // * output columns, which will be returned to the caller
        // * predicate columns, which are needed to evaluate the RowFilter
        //
        // The final reader projects back to the original output projection
        // after predicate evaluation.
        let mut read_projection = self.projection.clone();
        read_projection.union(&filter.union_projection()?);

        if self.post_filter_supports_batch_projection(&read_projection) {
            Some(read_projection)
        } else {
            None
        }
    }

    fn post_filter_supports_batch_projection(&self, projection: &ProjectionMask) -> bool {
        // Post-filter projects decoded record batches by top-level Arrow field
        // index. A nested root is safe when it is selected as a whole root:
        // the decoded batch then contains exactly one top-level field for that
        // root and can be projected without recursively trimming children.
        //
        // Partial nested projections, such as `struct.a` without `struct.b`,
        // still need recursive array projection and remain on the pushdown path.
        let schema = self.metadata.file_metadata().schema_descr();
        projection.selects_whole_root_columns(schema)
    }

    fn projection_has_variable_width_leaf(
        &self,
        row_group_idx: usize,
        projection: &ProjectionMask,
    ) -> bool {
        let row_group = self.metadata.row_group(row_group_idx);
        (0..row_group.num_columns()).any(|leaf_idx| {
            projection.leaf_included(leaf_idx)
                && row_group.column(leaf_idx).column_type() == PhysicalType::BYTE_ARRAY
        })
    }

    fn projection_includes_all(&self, projection: &ProjectionMask, other: &ProjectionMask) -> bool {
        let schema = self.metadata.file_metadata().schema_descr();
        (0..schema.num_columns())
            .all(|leaf_idx| !other.leaf_included(leaf_idx) || projection.leaf_included(leaf_idx))
    }

    pub(super) fn observe_cost_model_candidate(
        &mut self,
        decision: RowSelectionStrategyDecision,
        row_count: usize,
        budget: RowBudget,
    ) {
        if !matches!(self.row_selection_policy, RowSelectionPolicy::Auto { .. }) {
            return;
        }

        let observation = {
            let RowGroupCostModelState::Observing { observation } = &mut self.cost_model_state
            else {
                return;
            };

            let mut shape = decision.shape;
            if shape.total_rows() == 0 {
                // `None` selection means the predicate kept the whole row group.
                // Represent it as one selected run so the cost model can
                // treat "no pruning" as an observed high-selectivity case.
                shape = RowSelectionShape {
                    selected_rows: row_count,
                    skipped_rows: 0,
                    selector_count: 1,
                    selected_run_count: 1,
                    skipped_run_count: 0,
                };
            }

            observation.observed_row_groups += 1;
            observation.shape.add_assign(shape);
            *observation
        };
        self.metrics.record_cost_model_observed_row_group();

        let reason = self.cost_model_reason_with_projection_context(observation);
        if matches!(reason, CostModelDecisionReason::ObservationIncomplete) {
            self.metrics.record_cost_model_trigger(reason);
            return;
        }

        let prefers_post_filter = observation.prefers_post_filter()
            || matches!(
                reason,
                CostModelDecisionReason::ProjectedPredicateModerateSelectivity
            );
        self.metrics.record_cost_model_trigger(reason);

        if prefers_post_filter && self.post_filter_cost_model_supported(budget) {
            self.cost_model_state = RowGroupCostModelState::UsePostFilter;
        } else {
            self.cost_model_state = RowGroupCostModelState::UsePushdown;
        }
    }

    fn cost_model_reason_with_projection_context(
        &self,
        observation: CostModelObservation,
    ) -> CostModelDecisionReason {
        let reason = observation.trigger_reason();
        if !matches!(reason, CostModelDecisionReason::PushdownStillPreferred) {
            return reason;
        }

        let Some(filter) = self.filter.as_ref() else {
            return reason;
        };
        let Some(predicate_projection) = filter.union_projection() else {
            return reason;
        };

        let selected_ratio = observation.shape.selected_ratio();
        // Projected predicates can reuse decoded predicate values, but sparse
        // or clustered filters can still win with page pruning. Keep this
        // shortcut to moderate selectivity before switching to post-filter.
        if self.projection_includes_all(&self.projection, &predicate_projection)
            && (CostModelObservation::PROJECTED_PREDICATE_MIN_RATIO
                ..CostModelObservation::PROJECTED_PREDICATE_MAX_RATIO)
                .contains(&selected_ratio)
        {
            CostModelDecisionReason::ProjectedPredicateModerateSelectivity
        } else {
            reason
        }
    }

    pub(super) fn post_filter_cost_model_supported(&self, budget: RowBudget) -> bool {
        let Some(filter) = self.filter.as_ref() else {
            return false;
        };
        self.post_filter_supports_filter(filter, budget)
    }

    fn post_filter_supports_filter(&self, filter: &RowFilter, budget: RowBudget) -> bool {
        self.post_filter_cost_model_enabled
            && matches!(self.row_selection_policy, RowSelectionPolicy::Auto { .. })
            && budget.is_unbounded()
            && !self.has_virtual_columns()
            && self.post_filter_supports_batch_projection(&self.projection)
            // The combined read projection may be whole-root even when an
            // individual predicate asks for one nested child that is completed
            // by the output projection. Check every batch projection that
            // `PostFilterState` will materialize, not only their union.
            && filter
                .predicates()
                .iter()
                .all(|predicate| self.post_filter_supports_batch_projection(predicate.projection()))
            && self.build_post_filter_read_projection(filter).is_some()
    }

    fn has_virtual_columns(&self) -> bool {
        self.fields
            .as_deref()
            .is_some_and(parquet_field_has_virtual_columns)
    }
}

fn parquet_field_has_virtual_columns(field: &ParquetField) -> bool {
    match &field.field_type {
        ParquetFieldType::Primitive { .. } => false,
        ParquetFieldType::Group { children } => {
            children.iter().any(parquet_field_has_virtual_columns)
        }
        ParquetFieldType::Virtual(_) => true,
    }
}
