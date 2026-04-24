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

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;

/// Why a predicate was applied or deferred at the read-plan stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterDeferralDecisionReason {
    /// Predicate selected all rows with no existing selection, so no selection
    /// structure was materialized.
    AllSelectedFastPath,
    /// Deferral threshold is not configured.
    ThresholdDisabled,
    /// Row count was zero.
    ZeroRowCount,
    /// Predicate did not increase selector fragmentation.
    FragmentationNotIncreased,
    /// Predicate passed non-deferral gates and was kept applied.
    GatesPassed,
    /// Predicate failed one or more non-deferral gates and was deferred.
    GatesFailedDeferred,
}

/// Per-filter stats captured during read-plan predicate evaluation.
#[derive(Debug, Clone, PartialEq)]
pub struct FilterSelectivityStat {
    /// Zero-based predicate evaluation index within this read-plan build.
    pub predicate_index: usize,
    /// Number of rows considered by the predicate decision.
    pub row_count: usize,
    /// Selector count before applying this predicate.
    pub current_selector_count: usize,
    /// Selector count after applying this predicate.
    pub absolute_selector_count: usize,
    /// Skipped rows before applying this predicate.
    pub current_skipped_rows: usize,
    /// Skipped rows after applying this predicate.
    pub absolute_skipped_rows: usize,
    /// Long skipped rows before applying this predicate.
    pub current_long_skip_rows: usize,
    /// Long skipped rows after applying this predicate.
    pub absolute_long_skip_rows: usize,
    /// Absolute skipped/rows ratio.
    pub absolute_skip_selectivity: f64,
    /// Absolute long-skipped/skipped ratio.
    pub absolute_long_skip_share: f64,
    /// Incremental skipped/rows ratio contributed by this predicate.
    pub delta_skip_selectivity: f64,
    /// Incremental long-skipped/skipped ratio contributed by this predicate.
    pub delta_long_skip_share: f64,
    /// Threshold supplied via `with_scatter_threshold`.
    pub long_skip_share_threshold: Option<f64>,
    /// Whether this predicate result was deferred.
    pub deferred: bool,
    /// Why this predicate was applied or deferred.
    pub decision_reason: FilterDeferralDecisionReason,
}

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

    /// Per-filter selectivity/deferral stats captured during planning.
    ///
    /// Returns `None` if metrics are disabled.
    pub fn filter_selectivity_stats(&self) -> Option<Vec<FilterSelectivityStat>> {
        match self {
            Self::Disabled => None,
            Self::Enabled(inner) => {
                let stats = match inner.filter_selectivity_stats.lock() {
                    Ok(stats) => stats,
                    Err(poisoned) => poisoned.into_inner(),
                };
                Some(stats.clone())
            }
        }
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

    /// Records a per-filter selectivity stat.
    pub(crate) fn record_filter_selectivity_stat(&self, stat: FilterSelectivityStat) {
        let Self::Enabled(inner) = self else {
            return;
        };

        let mut stats = match inner.filter_selectivity_stats.lock() {
            Ok(stats) => stats,
            Err(poisoned) => poisoned.into_inner(),
        };
        stats.push(stat);
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
    /// Per-filter selectivity stats captured during read planning.
    filter_selectivity_stats: Mutex<Vec<FilterSelectivityStat>>,
}

impl ArrowReaderMetricsInner {
    /// Creates a new instance of `ArrowReaderMetricsInner`
    pub(crate) fn new() -> Self {
        Self {
            records_read_from_inner: AtomicUsize::new(0),
            records_read_from_cache: AtomicUsize::new(0),
            filter_selectivity_stats: Mutex::new(vec![]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_selectivity_stats_disabled() {
        let metrics = ArrowReaderMetrics::disabled();
        assert!(metrics.filter_selectivity_stats().is_none());
    }

    #[test]
    fn test_filter_selectivity_stats_enabled() {
        let metrics = ArrowReaderMetrics::enabled();
        metrics.record_filter_selectivity_stat(FilterSelectivityStat {
            predicate_index: 0,
            row_count: 100,
            current_selector_count: 1,
            absolute_selector_count: 3,
            current_skipped_rows: 10,
            absolute_skipped_rows: 20,
            current_long_skip_rows: 10,
            absolute_long_skip_rows: 15,
            absolute_skip_selectivity: 0.2,
            absolute_long_skip_share: 0.75,
            delta_skip_selectivity: 0.1,
            delta_long_skip_share: 0.5,
            long_skip_share_threshold: Some(0.75),
            deferred: true,
            decision_reason: FilterDeferralDecisionReason::GatesFailedDeferred,
        });

        let stats = metrics.filter_selectivity_stats().expect("metrics enabled");
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].predicate_index, 0);
        assert!(stats[0].deferred);
        assert_eq!(
            stats[0].decision_reason,
            FilterDeferralDecisionReason::GatesFailedDeferred
        );
    }
}
