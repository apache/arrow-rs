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

//! [ArrowReaderMetrics] for collecting metrics about the Arrow reader

use super::selection::RowSelectionStrategy;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// This enum represents the state of Arrow reader metrics collection.
///
/// The inner metrics are stored in an `Arc<ArrowReaderMetricsInner>`
/// so cloning the `ArrowReaderMetrics` enum will not clone the inner metrics.
///
/// To access metrics, create an `ArrowReaderMetrics` via [`ArrowReaderMetrics::enabled()`]
/// and configure the `ArrowReaderBuilder` with a clone.
#[derive(Debug, Clone)]
pub enum ArrowReaderMetrics {
    /// Metrics are not collected (default)
    Disabled,
    /// Metrics are collected and stored in an `Arc`.
    ///
    /// Create this via [`ArrowReaderMetrics::enabled()`].
    Enabled(Arc<ArrowReaderMetricsInner>),
}

impl ArrowReaderMetrics {
    /// Creates a new instance of [`ArrowReaderMetrics::Disabled`]
    pub fn disabled() -> Self {
        Self::Disabled
    }

    /// Creates a new instance of [`ArrowReaderMetrics::Enabled`]
    pub fn enabled() -> Self {
        Self::Enabled(Arc::new(ArrowReaderMetricsInner::new()))
    }

    /// Predicate Cache: number of records read directly from the inner reader
    ///
    /// This is the total number of records read from the inner reader (that is
    /// actually decoding). It measures the amount of work that could not be
    /// avoided with caching.
    ///
    /// It returns the number of records read across all columns, so if you read
    /// 2 columns each with 100 records, this will return 200.
    ///
    ///
    /// Returns None if metrics are disabled.
    pub fn records_read_from_inner(&self) -> Option<usize> {
        match self {
            Self::Disabled => None,
            Self::Enabled(inner) => Some(
                inner
                    .records_read_from_inner
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
        }
    }

    /// Predicate Cache: number of records read from the cache
    ///
    /// This is the total number of records read from the cache actually
    /// decoding). It measures the amount of work that was avoided with caching.
    ///
    /// It returns the number of records read across all columns, so if you read
    /// 2 columns each with 100 records from the cache, this will return 200.
    ///
    /// Returns None if metrics are disabled.
    pub fn records_read_from_cache(&self) -> Option<usize> {
        match self {
            Self::Disabled => None,
            Self::Enabled(inner) => Some(
                inner
                    .records_read_from_cache
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
        }
    }

    /// Number of row-selection decisions using mask execution.
    ///
    /// One decision is recorded per row group and projected top-level Arrow
    /// field, for both predicate projections and the final output projection.
    ///
    /// Returns `None` if metrics are disabled.
    pub fn row_selection_mask_decisions(&self) -> Option<usize> {
        self.load(|inner| &inner.row_selection_mask_decisions)
    }

    /// Number of row-selection decisions using selector execution.
    ///
    /// One decision is recorded per row group and projected top-level Arrow
    /// field, for both predicate projections and the final output projection.
    ///
    /// Returns `None` if metrics are disabled.
    pub fn row_selection_selector_decisions(&self) -> Option<usize> {
        self.load(|inner| &inner.row_selection_selector_decisions)
    }

    /// Number of decisions made by the compatibility fallback.
    ///
    /// This is a subset of the mask and selector decision counters.
    ///
    /// Returns `None` if metrics are disabled.
    pub fn row_selection_fallback_decisions(&self) -> Option<usize> {
        self.load(|inner| &inner.row_selection_fallback_decisions)
    }

    /// Increments the count of records read from the inner reader
    pub(crate) fn increment_inner_reads(&self, count: usize) {
        let Self::Enabled(inner) = self else {
            return;
        };
        inner
            .records_read_from_inner
            .fetch_add(count, std::sync::atomic::Ordering::Relaxed);
    }

    /// Increments the count of records read from the cache
    pub(crate) fn increment_cache_reads(&self, count: usize) {
        let Self::Enabled(inner) = self else {
            return;
        };

        inner
            .records_read_from_cache
            .fetch_add(count, std::sync::atomic::Ordering::Relaxed);
    }

    /// Records `count` identical decisions at once, for the case where every
    /// projected column shares a threshold and the per-column loop is skipped.
    pub(crate) fn record_shared_row_selection_decision(
        &self,
        strategy: RowSelectionStrategy,
        fallback: bool,
        count: usize,
    ) {
        for _ in 0..count {
            self.record_row_selection_decision(strategy, fallback);
        }
    }

    pub(crate) fn record_row_selection_decision(
        &self,
        strategy: RowSelectionStrategy,
        fallback: bool,
    ) {
        let Self::Enabled(inner) = self else {
            return;
        };
        let counter = match strategy {
            RowSelectionStrategy::Mask => &inner.row_selection_mask_decisions,
            RowSelectionStrategy::Selectors => &inner.row_selection_selector_decisions,
        };
        counter.fetch_add(1, Ordering::Relaxed);
        if fallback {
            inner
                .row_selection_fallback_decisions
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn load(
        &self,
        counter: impl FnOnce(&ArrowReaderMetricsInner) -> &AtomicUsize,
    ) -> Option<usize> {
        match self {
            Self::Disabled => None,
            Self::Enabled(inner) => Some(counter(inner).load(Ordering::Relaxed)),
        }
    }
}

/// Holds the actual metrics for the Arrow reader.
///
/// Please see [`ArrowReaderMetrics`] for the public interface.
#[derive(Debug)]
pub struct ArrowReaderMetricsInner {
    // Metrics for Predicate Cache
    /// Total number of records read from the inner reader (uncached)
    records_read_from_inner: AtomicUsize,
    /// Total number of records read from previously cached pages
    records_read_from_cache: AtomicUsize,
    /// Per-column row-selection decisions using masks.
    row_selection_mask_decisions: AtomicUsize,
    /// Per-column row-selection decisions using selectors.
    row_selection_selector_decisions: AtomicUsize,
    /// Decisions made by the compatibility fallback.
    row_selection_fallback_decisions: AtomicUsize,
}

impl ArrowReaderMetricsInner {
    /// Creates a new instance of `ArrowReaderMetricsInner`
    pub(crate) fn new() -> Self {
        Self {
            records_read_from_inner: AtomicUsize::new(0),
            records_read_from_cache: AtomicUsize::new(0),
            row_selection_mask_decisions: AtomicUsize::new(0),
            row_selection_selector_decisions: AtomicUsize::new(0),
            row_selection_fallback_decisions: AtomicUsize::new(0),
        }
    }
}
