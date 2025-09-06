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

//! [`FilterInfo`] state machine for evaluating row filters

use crate::arrow::arrow_reader::{ArrowPredicate, RowFilter};
use crate::arrow::ProjectionMask;
use std::num::NonZeroUsize;

#[derive(Debug)]
pub(super) struct FilterInfo {
    /// The predicates to evaluate, in order
    ///
    /// These must be owned by FilterInfo because they may be mutated as part of
    /// evaluation so there is a bunch of complexity of handing them back and forth
    filter: RowFilter,
    /// The next filter to be evaluated
    next_predicate: NonZeroUsize,
    /// The columns to cache in the predicate cache
    cache_projection: ProjectionMask,
}

pub(super) enum AdvanceResult {
    /// advanced to the next predicate
    Continue(FilterInfo),
    /// no more predicates returns the row filter
    Done(RowFilter),
}

impl FilterInfo {
    /// Create a new FilterInfo
    pub(super) fn new(filter: RowFilter, cache_projection: ProjectionMask) -> Self {
        Self {
            filter,
            next_predicate: NonZeroUsize::new(1).expect("1 is always non-zero"),
            cache_projection,
        }
    }

    /// Advance to the next predicate, returning either the updated FilterInfo
    /// or the completed RowFilter if there are no more predicates
    pub(super) fn advance(mut self) -> AdvanceResult {
        if self.next_predicate.get() >= self.filter.predicates.len() {
            AdvanceResult::Done(self.filter)
        } else {
            self.next_predicate = self
                .next_predicate
                .checked_add(1)
                .expect("no usize overflow");
            AdvanceResult::Continue(self)
        }
    }

    /// Return the current predicate to evaluate, mutablely
    /// Panics if done() is true
    pub(super) fn current_mut(&mut self) -> &mut dyn ArrowPredicate {
        self.filter
            .predicates
            .get_mut(self.next_predicate.get() - 1)
            .expect("current predicate out of bounds")
            .as_mut()
    }

    /// Return the current predicate to evaluate
    /// Panics if done() is true
    pub(super) fn current(&self) -> &dyn ArrowPredicate {
        self.filter
            .predicates
            .get(self.next_predicate.get() - 1)
            .expect("current predicate out of bounds")
            .as_ref()
    }

    /// Returns the inner filter, consuming this FilterInfo
    pub(super) fn into_filter(self) -> RowFilter {
        self.filter
    }

    /// Return a reference to the cached projection
    pub(super) fn cache_projection(&self) -> &ProjectionMask {
        &self.cache_projection
    }
}
