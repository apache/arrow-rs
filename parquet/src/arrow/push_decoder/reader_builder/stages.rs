// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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
/// group read (see [`Self::is_read_before`] and [`column_fetches`]).
///
/// [`column_fetches`]: crate::arrow::in_memory_row_group::column_fetches
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

    /// Returns `true` if a stage before `stage` reads leaf column
    /// `column_idx`, so `stage` does not fetch it again.
    ///
    /// The decoder keeps the column chunks that a stage read for the later
    /// stages of the row group. This gives the same answer without them.
    pub(crate) fn is_read_before(&self, stage: Stage, column_idx: usize) -> bool {
        let earlier = match stage {
            Stage::Predicate(idx) => &self.predicate_projections[..idx],
            Stage::Projection => &self.predicate_projections,
        };
        earlier
            .iter()
            .any(|projection| projection.leaf_included(column_idx))
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
