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

use crate::arrow::ProjectionMask;
use crate::arrow::array_reader::{CacheOptionsBuilder, RowGroupCache};
use crate::arrow::arrow_reader::{ArrowPredicate, RowFilter};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

/// State machine for evaluating a sequence of predicates.
///
/// The `FilterInfo` owns the [`RowFilter`] being evaluated and tracks the current
/// predicate to evaluate.
#[derive(Debug)]
pub(super) struct FilterInfo {
    /// The predicates to evaluate, in order
    ///
    /// RowFilter is owned by `FilterInfo` because they may be mutated as part
    /// of evaluation. Specifically, [`ArrowPredicate`] requires &mut self for
    /// evaluation.
    filter: RowFilter,
    /// The next filter to be evaluated
    next_predicate: NonZeroUsize,
    /// Previously computed filter results
    cache_info: CacheInfo,
}

/// Predicate cache
///
/// Note this is basically the same as CacheOptionsBuilder
/// but it owns the ProjectionMask and RowGroupCache
#[derive(Debug)]
pub(super) struct CacheInfo {
    /// The columns to cache in the predicate cache.
    /// Normally these are the columns that filters may look at such that
    /// if we have a filter like `(a + 10 > 5) AND (a + b = 0)` we cache `a` to avoid re-reading it between evaluating `a + 10 > 5` and `a + b = 0`.
    cache_projection: ProjectionMask,
    row_group_cache: Arc<Mutex<RowGroupCache>>,
}

impl CacheInfo {
    pub(super) fn new(
        cache_projection: ProjectionMask,
        row_group_cache: Arc<Mutex<RowGroupCache>>,
    ) -> Self {
        Self {
            cache_projection,
            row_group_cache,
        }
    }

    pub(super) fn builder(&self) -> CacheOptionsBuilder<'_> {
        CacheOptionsBuilder::new(&self.cache_projection, &self.row_group_cache)
    }
}

pub(super) enum AdvanceResult {
    /// Advanced to the next predicate
    Continue(FilterInfo),
    /// No more predicates returns the row filter and cache info
    Done(RowFilter, CacheInfo),
}

impl FilterInfo {
    /// Create a new FilterInfo
    pub(super) fn new(filter: RowFilter, cache_info: CacheInfo) -> Self {
        Self {
            filter,
            next_predicate: NonZeroUsize::new(1).expect("1 is always non-zero"),
            cache_info,
        }
    }

    /// Advance to the next predicate
    ///
    /// Returns
    /// * [`AdvanceResult::Continue`] returning the `FilterInfo` if there are
    ///   more predicate to evaluate.
    /// * [`AdvanceResult::Done`] with the inner [`RowFilter`] and [`CacheInfo]`
    ///   if there are no more predicates
    pub(super) fn advance(mut self) -> AdvanceResult {
        if self.next_predicate.get() >= self.filter.predicates.len() {
            AdvanceResult::Done(self.filter, self.cache_info)
        } else {
            self.next_predicate = self
                .next_predicate
                .checked_add(1)
                .expect("no usize overflow");
            AdvanceResult::Continue(self)
        }
    }

    /// Return a mutable reference to the current predicate
    pub(super) fn current_mut(&mut self) -> &mut dyn ArrowPredicate {
        self.filter
            .predicates
            .get_mut(self.next_predicate.get() - 1)
            // advance ensures next_predicate is always in bounds
            .unwrap()
            .as_mut()
    }

    /// Return the current predicate to evaluate
    pub(super) fn current(&self) -> &dyn ArrowPredicate {
        self.filter
            .predicates
            .get(self.next_predicate.get() - 1)
            // advance ensures next_predicate is always in bounds
            .unwrap()
            .as_ref()
    }

    /// Return a reference to the cache projection
    pub(super) fn cache_projection(&self) -> &ProjectionMask {
        &self.cache_info.cache_projection
    }

    /// Return a cache builder to save the results of predicate evaluation
    pub(super) fn cache_builder(&self) -> CacheOptionsBuilder<'_> {
        self.cache_info.builder()
    }

    /// Returns the inner filter, consuming this FilterInfo
    pub(super) fn into_filter(self) -> RowFilter {
        self.filter
    }
}
