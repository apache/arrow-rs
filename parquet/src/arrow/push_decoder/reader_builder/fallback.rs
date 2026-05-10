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
    Observing { observation: FallbackObservation },
    UsePushdown,
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
        let mut read_projection = self.projection.clone();
        read_projection.union(&filter.union_projection()?);

        if self.post_filter_supports_projection(&read_projection) {
            Some(read_projection)
        } else {
            None
        }
    }

    fn post_filter_supports_projection(&self, projection: &ProjectionMask) -> bool {
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
