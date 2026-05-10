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

//! Runtime post-filter fallback decisions for push decoder row groups.
//!
//! The fallback is intentionally adaptive rather than purely static. The first
//! eligible row group is evaluated with predicate pushdown so the reader can
//! observe the actual `RowSelection` shape produced by the predicate chain.
//! Later row groups may then switch to post-filter execution if the observed
//! shape suggests pushdown is doing extra work without pruning enough rows.
//!
//! ```text
//! Start
//!   |
//!   v
//! Observing -- incomplete observation --> Observing
//!   |
//!   +-- pushdown still preferred ------> UsePushdown
//!   |
//!   +-- fallback trigger + supported --> UsePostFilter
//! ```
//!
//! Fallback only applies to `Auto`. Explicit `Mask` and `Selectors` are treated
//! as user intent and are not overridden here.

use super::RowGroupReaderBuilder;
use crate::arrow::ProjectionMask;
use crate::arrow::arrow_reader::RowFilter;
use crate::arrow::arrow_reader::RowSelectionPolicy;
use crate::arrow::arrow_reader::selection::{
    FallbackObservation, FallbackTriggerReason, RowSelectionShape, RowSelectionStrategyDecision,
};
use crate::arrow::schema::{ParquetField, ParquetFieldType};

#[allow(dead_code)]
#[derive(Debug)]
pub(super) enum RowGroupFallbackState {
    /// Collect row-selection shape from early row groups before choosing a mode.
    Observing { observation: FallbackObservation },
    /// Predicate pushdown remains the execution mode for this reader.
    UsePushdown,
    /// Later row groups should decode once and evaluate predicates after decode.
    UsePostFilter { reason: FallbackTriggerReason },
}

impl Default for RowGroupFallbackState {
    fn default() -> Self {
        Self::Observing {
            observation: FallbackObservation::default(),
        }
    }
}

impl RowGroupReaderBuilder {
    pub(super) fn should_use_post_filter_fallback(&self) -> bool {
        // Keep the runtime switch narrow:
        //
        // * `Auto` means the caller allowed the reader to choose.
        // * `limit` and `offset` are applied during row-group planning; moving
        //   predicates after decode changes where short-circuiting can happen.
        // * virtual columns are not read from Parquet pages and need their
        //   existing projection path.
        matches!(
            self.fallback_state,
            RowGroupFallbackState::UsePostFilter { .. }
        ) && self.post_filter_fallback_enabled
            && matches!(self.row_selection_policy, RowSelectionPolicy::Auto { .. })
            && self.limit.is_none()
            && self.offset.is_none()
            && !self.has_virtual_columns()
    }

    pub(super) fn post_filter_read_projection(&self, filter: &RowFilter) -> Option<ProjectionMask> {
        if !self.should_use_post_filter_fallback() {
            return None;
        }

        self.build_post_filter_read_projection(filter)
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

        if self.post_filter_supports_projection(&read_projection) {
            Some(read_projection)
        } else {
            None
        }
    }

    fn post_filter_supports_projection(&self, projection: &ProjectionMask) -> bool {
        // The post-filter reader currently projects record batches by parquet
        // leaf column position. Nested roots can span multiple leaves and need
        // the existing array-reader projection machinery, so keep fallback to
        // primitive roots only.
        let schema = self.metadata.file_metadata().schema_descr();
        (0..schema.num_columns()).all(|leaf_idx| {
            !projection.leaf_included(leaf_idx) || schema.get_column_root(leaf_idx).is_primitive()
        })
    }

    pub(super) fn observe_fallback_candidate(
        &mut self,
        decision: RowSelectionStrategyDecision,
        row_count: usize,
    ) {
        if !matches!(self.row_selection_policy, RowSelectionPolicy::Auto { .. }) {
            return;
        }

        let RowGroupFallbackState::Observing { observation } = &mut self.fallback_state else {
            return;
        };

        let mut shape = decision.shape;
        if shape.total_rows() == 0 {
            // `None` selection means the predicate kept the whole row group.
            // Represent it as one selected run so the fallback classifier can
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
        self.metrics.record_fallback_observed_row_group();

        let reason = observation.trigger_reason();
        if matches!(reason, FallbackTriggerReason::ObservationIncomplete) {
            self.metrics.record_fallback_trigger(reason);
            return;
        }

        let should_fallback = observation.should_fallback();
        self.metrics.record_fallback_trigger(reason);

        if should_fallback && self.post_filter_fallback_supported() {
            self.fallback_state = RowGroupFallbackState::UsePostFilter { reason };
        } else {
            self.fallback_state = RowGroupFallbackState::UsePushdown;
        }
    }

    pub(super) fn post_filter_fallback_supported(&self) -> bool {
        let Some(filter) = self.filter.as_ref() else {
            return false;
        };
        self.post_filter_fallback_enabled
            && matches!(self.row_selection_policy, RowSelectionPolicy::Auto { .. })
            && self.limit.is_none()
            && self.offset.is_none()
            && !self.has_virtual_columns()
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
