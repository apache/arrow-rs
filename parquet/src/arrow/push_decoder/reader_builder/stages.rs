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

//! [`StageSchedule`]: what each decoding stage of a row group fetches.

use crate::arrow::ProjectionMask;

/// A decoding stage of a row group.
///
/// The decoder evaluates the [`RowFilter`] predicates in order, then decodes
/// the output columns.
///
/// [`RowFilter`]: crate::arrow::arrow_reader::RowFilter
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum Stage {
    /// Evaluation of the predicate at this index in the `RowFilter`.
    Predicate(usize),
    /// Decoding of the output columns.
    Projection,
}

/// The columns that each decoding stage of a row group fetches.
///
/// A stage does not fetch a column that an earlier stage of the same row
/// group read (see [`columns_to_fetch`]).
///
/// [`columns_to_fetch`]: crate::arrow::in_memory_row_group::columns_to_fetch
#[derive(Debug)]
pub(crate) struct StageSchedule {
    /// Columns in the output.
    projection: ProjectionMask,
    /// Columns each predicate reads, in evaluation order.
    predicate_projections: Vec<ProjectionMask>,
    /// Predicate columns whose decoded values are cached for the output, if
    /// any.
    cache_projection: Option<ProjectionMask>,
}

/// What one decoding stage fetches.
#[derive(Debug, Clone, Copy)]
pub(crate) struct StageFetch<'a> {
    /// Columns the stage decodes.
    pub(crate) projection: &'a ProjectionMask,
    /// Columns fetched with the selection expanded to batch boundaries, so
    /// that the predicate cache can serve complete batches. Only predicate
    /// stages have one.
    pub(crate) cache_projection: Option<&'a ProjectionMask>,
}

impl StageSchedule {
    pub(crate) fn new(
        projection: ProjectionMask,
        predicate_projections: Vec<ProjectionMask>,
        cache_projection: Option<ProjectionMask>,
    ) -> Self {
        Self {
            projection,
            predicate_projections,
            cache_projection,
        }
    }

    /// Predicate columns whose decoded values are cached for the output.
    pub(crate) fn cache_projection(&self) -> Option<&ProjectionMask> {
        self.cache_projection.as_ref()
    }

    /// Every stage, in decoding order, with what it fetches.
    pub(crate) fn stages(&self) -> impl Iterator<Item = (Stage, StageFetch<'_>)> + '_ {
        (0..self.predicate_projections.len())
            .map(Stage::Predicate)
            .chain(std::iter::once(Stage::Projection))
            .map(|stage| (stage, self.fetch(stage)))
    }

    /// What `stage` fetches.
    pub(crate) fn fetch(&self, stage: Stage) -> StageFetch<'_> {
        match stage {
            Stage::Predicate(idx) => StageFetch {
                projection: &self.predicate_projections[idx],
                cache_projection: self.cache_projection.as_ref(),
            },
            // The final projection fetch does not expand the selection.
            Stage::Projection => StageFetch {
                projection: &self.projection,
                cache_projection: None,
            },
        }
    }
}
