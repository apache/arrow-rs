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

//! Focused benchmark for Parquet reader materialization policy decisions.
//!
//! # Background:
//!
//! As described in [Efficient Filter Pushdown in Parquet], evaluating
//! pushdown filters is a two-step process:
//!
//! 1. Build a filter mask by decoding and evaluating filter functions on
//!    the filter column(s).
//!
//! 2. Decode the rows that match the filter mask from the projected columns.
//!
//! The performance depends on factors such as the number of rows selected,
//! the clustering of results (which affects the efficiency of the filter mask),
//! and whether the same column is used for both filtering and projection.
//!
//! This benchmark isolates the reader policy choice between full post-filtering
//! and row-filter pushdown with `Auto`, forced `Selectors`, and forced `Mask`.
//!
//! [Efficient Filter Pushdown in Parquet]: https://datafusion.apache.org/blog/2025/03/21/parquet-pushdown/
//!
//! The benchmark creates an in-memory Parquet file with 500K rows and four root
//! columns:
//! - `int64`: random integers with an injected point-lookup value.
//! - `float64`: random floating-point values used for sparse and dense filters.
//! - `utf8View`: ClickBench-like string values with sparse sentinel values.
//! - `ts`: sequential timestamps used for clustered filters.
//!
//! The benchmark cases are organized by reader-level axes: selection density
//! and clustering, predicate/output overlap, deferred output payload, predicate
//! cost, projected predicates, and count/filter-only outputs.
//!
//! Full TPC-DS runs can show query-level movement that does not reproduce in
//! isolated reader probes. Keep these cases focused on stable reader-level
//! risks: moderate projected predicates with cheap deferred output can favor
//! post-filtering, while clustered selections, variable-width deferred output,
//! complex OR predicates, and sparse scalar prefixes should not be swept into
//! that shortcut without their own evidence.

mod arrow_reader_common;

use arrow::array::{BooleanArray, Float64Array, Int64Array, TimestampMillisecondArray};
use arrow::compute::kernels::cmp::{eq, gt, lt, lt_eq, neq};
use arrow::compute::{and, or};
use arrow::record_batch::RecordBatch;
use arrow_array::StringViewArray;
use arrow_reader_common::{
    COLUMN_NAMES, InMemoryReader, post_filter_projected_num_rows, projection_names,
    read_projection_for_post_filter, write_parquet_file,
};
use bytes::Bytes;
use criterion::{
    BenchmarkGroup, BenchmarkId, Criterion, criterion_group, criterion_main, measurement::WallTime,
};
use futures::StreamExt;
use parquet::arrow::arrow_reader::{ArrowPredicateFn, RowFilter, RowSelectionPolicy};
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use std::sync::Arc;

/// ProjectionCase defines the output projection mode for each benchmark case.
#[derive(Clone, Copy)]
enum ProjectionCase {
    ExcludeFilterColumn,
    CountOnly,
    Float64Only,
    Int64AndFloat64,
    Int64AndUtf8,
    TsAndUtf8,
    Utf8Only,
}

impl std::fmt::Display for ProjectionCase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProjectionCase::ExcludeFilterColumn => write!(f, "exclude_filter_column"),
            ProjectionCase::CountOnly => write!(f, "count_only"),
            ProjectionCase::Float64Only => write!(f, "float64_only"),
            ProjectionCase::Int64AndFloat64 => write!(f, "int64_and_float64"),
            ProjectionCase::Int64AndUtf8 => write!(f, "int64_and_utf8"),
            ProjectionCase::TsAndUtf8 => write!(f, "ts_and_utf8"),
            ProjectionCase::Utf8Only => write!(f, "utf8_only"),
        }
    }
}

#[derive(Clone, Copy)]
enum AsyncStrategy {
    FullPostFilter,
    PushdownAuto,
    PushdownSelectors,
    PushdownMask,
}

impl std::fmt::Display for AsyncStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AsyncStrategy::FullPostFilter => write!(f, "full_post_filter"),
            AsyncStrategy::PushdownAuto => write!(f, "pushdown_auto"),
            AsyncStrategy::PushdownSelectors => write!(f, "pushdown_selectors"),
            AsyncStrategy::PushdownMask => write!(f, "pushdown_mask"),
        }
    }
}

impl AsyncStrategy {
    fn row_selection_policy(self) -> Option<RowSelectionPolicy> {
        match self {
            AsyncStrategy::FullPostFilter => None,
            AsyncStrategy::PushdownAuto => Some(RowSelectionPolicy::default()),
            AsyncStrategy::PushdownSelectors => Some(RowSelectionPolicy::Selectors),
            AsyncStrategy::PushdownMask => Some(RowSelectionPolicy::Mask),
        }
    }
}

/// FilterType encapsulates the different filter comparisons.
/// The variants correspond to the different filter patterns.
#[derive(Clone, Copy, Debug)]
pub(crate) enum FilterType {
    /// unselective (99%) unclustered filter
    /// ```text
    /// ┌───────────────┐    ┌───────────────┐
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │               │
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// └───────────────┘    └───────────────┘
    /// ```
    /// (fragmented, approx 495K selected rows in 500K)
    UnselectiveUnclustered,
    /// unselective (90%) clustered filter
    /// ```text
    /// ┌───────────────┐    ┌───────────────┐
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │      ...      │
    /// │               │    │               │
    /// └───────────────┘    └───────────────┘
    /// ```
    /// (50 selected runs of 9K rows each in 500K)
    UnselectiveClustered,
    // Deferred-output shapes. Predicate columns are separate from the output,
    // so rejected rows can skip output-column decoding.
    /// Scalar-prefix shape derived from DataFusion ClickBench Q37:
    ///
    /// ```sql
    /// WHERE CounterID = 62
    ///   AND EventDate BETWEEN ...
    ///   AND DontCountHits = 0
    ///   AND IsRefresh = 0
    ///   AND Title <> ''
    /// ```
    ///
    /// DataFusion `Auto` does not push down the `Title <> ''` string predicate,
    /// but it can push down the scalar prefix to defer decoding `Title`.
    /// Fragmented ~0.9% selection: approx 4,500 selected rows in 500K.
    ///
    /// ```text
    /// ┌───────────────┐    ┌───────────────┐
    /// │               │    │               │
    /// │      ...      │    │      ...      │
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │               │    │               │
    /// │               │    │               │
    /// │      ...      │    │      ...      │
    /// │               │    │               │
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │               │    │               │
    /// └───────────────┘    └───────────────┘
    /// ```
    ScalarPrefixUtf8Output,
    /// Sparse fragmented scalar predicates (~7%, approx 36,000 selected rows
    /// in 500K) with a cheap fixed-width output projection, derived from a
    /// ClickBench Q41-like shape.
    ///
    /// ```text
    /// ┌───────────────┐    ┌───────────────┐
    /// │               │    │               │
    /// │      ...      │    │      ...      │
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │               │    │               │
    /// │               │    │               │
    /// │      ...      │    │      ...      │
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │               │    │               │
    /// │               │    │               │
    /// └───────────────┘    └───────────────┘
    /// ```
    SparseScalarFixedOutput,
    // Multi-predicate shapes. These focus predicate evaluation cost
    // independently of projection cost.
    /// Complex OR predicate over dictionary/string-like and scalar columns
    /// where predicate evaluation dominates reader time. Derived from TPC-DS
    /// Q41.
    /// Mixed string/scalar OR branches select approx 1% of rows.
    ///
    /// ```text
    /// ┌───────────────┐    ┌───────────────┐
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │               │    │               │
    /// │      ...      │    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │               │    │      ...      │
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │               │
    /// │               │    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │      ...      │    │               │
    /// │               │    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │               │
    /// └───────────────┘    └───────────────┘
    /// ```
    ComplexOrMixedPredicates,

    // Projected-predicate shapes. At least one predicate column is also needed
    // in the final projection.
    /// Projected-predicate shape derived from TPC-DS Q2 fact scans: the
    /// dynamic filter applies to the date key, the same date key is projected,
    /// and an additional fixed-width sales value can still be deferred by
    /// predicate pushdown.
    /// The baseline keeps an 8% sparse fixed-width point, a 50% dense
    /// fixed-width point, and a 40% variable-width output guardrail.
    ///
    /// ```text
    /// ┌───────────────┐    ┌───────────────┐
    /// │               │    │               │
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │      ...      │    │      ...      │
    /// │               │    │               │
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │               │    │               │
    /// │      ...      │    │      ...      │
    /// └───────────────┘    └───────────────┘
    /// ```
    ProjectedPredicate8Pct,
    ProjectedPredicate40Pct,
    ProjectedPredicate50Pct,
    /// Exact shape for the projected-predicate moderate-selectivity gate:
    /// a clustered 20% timestamp predicate where the predicate column is
    /// projected and the deferred output is variable-width.
    /// Clustered 20% selection: approx 100,000 selected rows in 500K.
    ///
    /// ```text
    /// ┌───────────────┐    ┌───────────────┐
    /// │               │    │               │
    /// │               │    │               │
    /// │               │    │      ...      │
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │      ...      │    │      ...      │
    /// │               │    │               │
    /// │               │    │               │
    /// └───────────────┘    └───────────────┘
    /// ```
    ClusteredTs20PctProjectedPredicate,
}

impl std::fmt::Display for FilterType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            FilterType::UnselectiveUnclustered => "float64 <= 99.0",
            FilterType::UnselectiveClustered => "ts < 9000",
            FilterType::ScalarPrefixUtf8Output => "int64 == 62 AND ts < 9000",
            FilterType::SparseScalarFixedOutput => "int64 < 8 AND ts < 9000",
            FilterType::ComplexOrMixedPredicates => {
                "(utf8View <> '' AND int64 < 8) OR (ts < 100 AND float64 > 95.0)"
            }
            FilterType::ProjectedPredicate8Pct => "int64 < 8 projected predicate with fixed output",
            FilterType::ProjectedPredicate40Pct => {
                "int64 < 40 projected predicate with fixed output"
            }
            FilterType::ProjectedPredicate50Pct => {
                "int64 < 50 projected predicate with fixed output"
            }
            FilterType::ClusteredTs20PctProjectedPredicate => {
                "ts < 2000 projected predicate with utf8 output"
            }
        };
        write!(f, "{s}")
    }
}

impl FilterType {
    /// Applies the specified filter on the given RecordBatch and returns a BooleanArray mask.
    pub(crate) fn filter_batch(&self, batch: &RecordBatch) -> arrow::error::Result<BooleanArray> {
        match self {
            // Unselective Unclustered on float64 column: NOT (float64 > 99.0)
            FilterType::UnselectiveUnclustered => {
                let array = batch.column(batch.schema().index_of("float64")?);
                lt_eq(array, &Float64Array::new_scalar(99.0))
            }
            // Unselective Clustered on ts column: ts < 9000
            FilterType::UnselectiveClustered => {
                let array = batch.column(batch.schema().index_of("ts")?);
                lt(array, &TimestampMillisecondArray::new_scalar(9000))
            }
            // ScalarPrefixUtf8Output: a cheap fragmented scalar predicate
            // evaluated before decoding a variable-width output column.
            FilterType::ScalarPrefixUtf8Output => {
                let int64 = batch.column(batch.schema().index_of("int64")?);
                let ts = batch.column(batch.schema().index_of("ts")?);
                let counter_match = eq(int64, &Int64Array::new_scalar(62))?;
                let date_like_range = lt(ts, &TimestampMillisecondArray::new_scalar(9000))?;
                and(&counter_match, &date_like_range)
            }
            FilterType::SparseScalarFixedOutput => {
                let int64 = batch.column(batch.schema().index_of("int64")?);
                let ts = batch.column(batch.schema().index_of("ts")?);
                let counter_like = lt(int64, &Int64Array::new_scalar(8))?;
                let date_like = lt(ts, &TimestampMillisecondArray::new_scalar(9000))?;
                and(&counter_like, &date_like)
            }
            FilterType::ComplexOrMixedPredicates => {
                let int64 = batch.column(batch.schema().index_of("int64")?);
                let float64 = batch.column(batch.schema().index_of("float64")?);
                let utf8 = batch.column(batch.schema().index_of("utf8View")?);
                let ts = batch.column(batch.schema().index_of("ts")?);
                let string_branch = and(
                    &neq(utf8, &StringViewArray::new_scalar(""))?,
                    &lt(int64, &Int64Array::new_scalar(8))?,
                )?;
                let scalar_branch = and(
                    &lt(ts, &TimestampMillisecondArray::new_scalar(100))?,
                    &gt(float64, &Float64Array::new_scalar(95.0))?,
                )?;
                or(&string_branch, &scalar_branch)
            }
            FilterType::ProjectedPredicate8Pct
            | FilterType::ProjectedPredicate40Pct
            | FilterType::ProjectedPredicate50Pct => {
                let int64 = batch.column(batch.schema().index_of("int64")?);
                let threshold = match self {
                    FilterType::ProjectedPredicate8Pct => 8,
                    FilterType::ProjectedPredicate40Pct => 40,
                    FilterType::ProjectedPredicate50Pct => 50,
                    _ => unreachable!(),
                };
                lt(int64, &Int64Array::new_scalar(threshold))
            }
            FilterType::ClusteredTs20PctProjectedPredicate => {
                let ts = batch.column(batch.schema().index_of("ts")?);
                lt(ts, &TimestampMillisecondArray::new_scalar(2000))
            }
        }
    }

    /// Return the indexes in the batch's schema that are used for filtering.
    fn filter_projection(&self) -> &'static [usize] {
        match self {
            FilterType::UnselectiveUnclustered => &[1],
            FilterType::UnselectiveClustered => &[3],
            FilterType::ScalarPrefixUtf8Output => &[0, 3],
            FilterType::SparseScalarFixedOutput => &[0, 3],
            FilterType::ComplexOrMixedPredicates => &[0, 1, 2, 3],
            FilterType::ProjectedPredicate8Pct
            | FilterType::ProjectedPredicate40Pct
            | FilterType::ProjectedPredicate50Pct => &[0],
            FilterType::ClusteredTs20PctProjectedPredicate => &[3],
        }
    }
}

/// A focused async-only matrix that isolates the cases most relevant to the
/// row-filter Auto policy. This is intentionally narrower than the full tuning
/// sweep used during development, and avoids repeating shapes already covered by
/// the existing row-filter strategy matrix.
///
/// The cases use structure-oriented names. Comments on [`FilterType`] keep the
/// ClickBench and TPC-DS provenance, but these are synthetic reader shapes, not
/// end-to-end query benchmarks.
///
/// Coverage is organized by reader-level dimensions instead of individual
/// queries:
/// - selection shape: sparse fragmented, dense fragmented, and clustered
///   ranges.
/// - output relationship: count-only, deferred fixed-width, deferred
///   variable-width, and projected predicate columns.
/// - predicate shape: single scalar, scalar plus variable-width predicates,
///   mixed OR predicates, and dynamic-filter-like projected predicates.
/// - policy boundary: strategy rows compare full post-filtering with `Auto`,
///   forced selectors, and forced masks for every shape.
///
/// The landed baseline intentionally keeps a small set of representative cases
/// instead of the larger local tuning sweep used during policy development.
///
/// Individual [`FilterType`] variants include shaded-row diagrams for the
/// representative selection shapes.
fn benchmark_async_auto_policy_focus(c: &mut Criterion) {
    let parquet_file = Bytes::from(write_parquet_file());
    let mut cases = Vec::new();
    push_focus_cases(
        &mut cases,
        &parquet_file,
        &[
            (
                "high_selectivity_float64",
                FilterType::UnselectiveUnclustered,
                ProjectionCase::ExcludeFilterColumn,
            ),
            (
                "high_selectivity_ts_clustered",
                FilterType::UnselectiveClustered,
                ProjectionCase::ExcludeFilterColumn,
            ),
            (
                "sparse_scalar_count_only",
                FilterType::SparseScalarFixedOutput,
                ProjectionCase::CountOnly,
            ),
            (
                "sparse_scalar_fixed_output",
                FilterType::SparseScalarFixedOutput,
                ProjectionCase::Float64Only,
            ),
            (
                "scalar_prefix_utf8_output",
                FilterType::ScalarPrefixUtf8Output,
                ProjectionCase::Utf8Only,
            ),
            (
                "complex_or_mixed_predicates",
                FilterType::ComplexOrMixedPredicates,
                ProjectionCase::Float64Only,
            ),
            (
                "projected_predicate_8pct_fixed_output",
                FilterType::ProjectedPredicate8Pct,
                ProjectionCase::Int64AndFloat64,
            ),
            (
                "projected_predicate_50pct_fixed_output",
                FilterType::ProjectedPredicate50Pct,
                ProjectionCase::Int64AndFloat64,
            ),
            (
                "projected_predicate_40pct_varwidth_output",
                FilterType::ProjectedPredicate40Pct,
                ProjectionCase::Int64AndUtf8,
            ),
            (
                "clustered_ts_20pct_varwidth_output",
                FilterType::ClusteredTs20PctProjectedPredicate,
                ProjectionCase::TsAndUtf8,
            ),
        ],
    );

    let strategies = [
        AsyncStrategy::FullPostFilter,
        AsyncStrategy::PushdownAuto,
        AsyncStrategy::PushdownMask,
        AsyncStrategy::PushdownSelectors,
    ];

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("arrow_reader_materialization_policy_async_focus");

    for case in cases {
        benchmark_async_focus_case(&mut group, &rt, case, &strategies);
    }
}

fn push_focus_cases(
    cases: &mut Vec<AsyncFocusCase>,
    parquet_file: &Bytes,
    specs: &[(&'static str, FilterType, ProjectionCase)],
) {
    cases.extend(
        specs
            .iter()
            .copied()
            .map(|(case_name, filter_type, projection_case)| {
                AsyncFocusCase::new(
                    case_name,
                    parquet_file.clone(),
                    filter_type,
                    projection_case,
                )
            }),
    );
}

struct AsyncFocusCase {
    case_name: &'static str,
    parquet_file: Bytes,
    filter_type: FilterType,
    projection_case: ProjectionCase,
}

impl AsyncFocusCase {
    fn new(
        case_name: &'static str,
        parquet_file: Bytes,
        filter_type: FilterType,
        projection_case: ProjectionCase,
    ) -> Self {
        Self {
            case_name,
            parquet_file,
            filter_type,
            projection_case,
        }
    }
}

fn benchmark_async_focus_case(
    group: &mut BenchmarkGroup<'_, WallTime>,
    rt: &tokio::runtime::Runtime,
    case: AsyncFocusCase,
    strategies: &[AsyncStrategy],
) {
    let AsyncFocusCase {
        case_name,
        parquet_file,
        filter_type,
        projection_case,
    } = case;

    let reader = InMemoryReader::try_new(&parquet_file).unwrap();
    let metadata = Arc::clone(reader.metadata());
    let schema_descr = metadata.file_metadata().schema_descr();
    let output_projection = output_projection_for(filter_type, &projection_case);
    let read_projection =
        read_projection_for_post_filter(&output_projection, filter_type.filter_projection());
    let output_column_names = projection_names(&output_projection);
    let projection_mask = ProjectionMask::roots(schema_descr, output_projection);
    let read_projection_mask = ProjectionMask::roots(schema_descr, read_projection);
    let pred_mask = ProjectionMask::roots(
        schema_descr,
        filter_type.filter_projection().iter().copied(),
    );
    let sparse_int64_pred_mask = ProjectionMask::roots(schema_descr, [0]);
    let sparse_ts_pred_mask = ProjectionMask::roots(schema_descr, [3]);

    for strategy in strategies.iter().copied() {
        let bench_id = BenchmarkId::new(
            format!("{case_name}/{projection_case}"),
            strategy.to_string(),
        );
        let rt_captured = rt.handle().clone();

        group.bench_function(bench_id, |b| {
            b.iter(|| {
                let reader = reader.clone();
                let pred_mask = pred_mask.clone();
                let sparse_int64_pred_mask = sparse_int64_pred_mask.clone();
                let sparse_ts_pred_mask = sparse_ts_pred_mask.clone();
                let projection_mask = projection_mask.clone();
                let read_projection_mask = read_projection_mask.clone();
                let output_column_names = output_column_names.clone();

                rt_captured.block_on(async {
                    if let Some(row_selection_policy) = strategy.row_selection_policy() {
                        let row_filter = row_filter_for_focus_case(
                            filter_type,
                            pred_mask,
                            sparse_int64_pred_mask,
                            sparse_ts_pred_mask,
                        );
                        benchmark_async_reader_with_policy(
                            reader,
                            projection_mask,
                            row_filter,
                            row_selection_policy,
                        )
                        .await
                    } else {
                        benchmark_async_reader_post_filter(
                            reader,
                            read_projection_mask,
                            output_column_names,
                            filter_type,
                        )
                        .await
                    }
                })
            });
        });
    }
}

fn output_projection_for(filter_type: FilterType, projection_case: &ProjectionCase) -> Vec<usize> {
    let filter_columns = filter_type.filter_projection();
    match projection_case {
        ProjectionCase::ExcludeFilterColumn => COLUMN_NAMES
            .iter()
            .enumerate()
            .map(|(idx, _)| idx)
            .filter(move |idx| !filter_columns.contains(idx))
            .collect(),
        ProjectionCase::CountOnly => vec![],
        ProjectionCase::Float64Only => vec![1],
        ProjectionCase::Int64AndFloat64 => vec![0, 1],
        ProjectionCase::Int64AndUtf8 => vec![0, 2],
        ProjectionCase::TsAndUtf8 => vec![2, 3],
        ProjectionCase::Utf8Only => vec![2],
    }
}

fn row_filter_for(filter_type: FilterType, pred_mask: ProjectionMask) -> RowFilter {
    let filter = ArrowPredicateFn::new(pred_mask, move |batch| filter_type.filter_batch(&batch));
    RowFilter::new(vec![Box::new(filter)])
}

fn row_filter_for_focus_case(
    filter_type: FilterType,
    pred_mask: ProjectionMask,
    sparse_int64_pred_mask: ProjectionMask,
    sparse_ts_pred_mask: ProjectionMask,
) -> RowFilter {
    match filter_type {
        FilterType::SparseScalarFixedOutput => {
            let int64_filter =
                ArrowPredicateFn::new(sparse_int64_pred_mask, move |batch: RecordBatch| {
                    let int64 = batch.column(batch.schema().index_of("int64")?);
                    lt(int64, &Int64Array::new_scalar(8))
                });
            let ts_filter =
                ArrowPredicateFn::new(sparse_ts_pred_mask, move |batch: RecordBatch| {
                    let ts = batch.column(batch.schema().index_of("ts")?);
                    lt(ts, &TimestampMillisecondArray::new_scalar(9000))
                });

            RowFilter::new(vec![Box::new(int64_filter), Box::new(ts_filter)])
        }
        _ => row_filter_for(filter_type, pred_mask),
    }
}

async fn benchmark_async_reader_with_policy(
    reader: InMemoryReader,
    projection_mask: ProjectionMask,
    row_filter: RowFilter,
    row_selection_policy: RowSelectionPolicy,
) {
    let mut stream = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .unwrap()
        .with_batch_size(8192)
        .with_projection(projection_mask)
        .with_row_filter(row_filter)
        .with_row_selection_policy(row_selection_policy)
        .build()
        .unwrap();
    while let Some(b) = stream.next().await {
        b.unwrap(); // consume the batches, no buffering
    }
}

async fn benchmark_async_reader_post_filter(
    reader: InMemoryReader,
    read_projection: ProjectionMask,
    output_column_names: Vec<&'static str>,
    filter_type: FilterType,
) {
    let mut stream = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .unwrap()
        .with_batch_size(8192)
        .with_projection(read_projection)
        .build()
        .unwrap();

    while let Some(b) = stream.next().await {
        let batch = b.unwrap();
        let filter = filter_type.filter_batch(&batch).unwrap();
        let output_rows =
            post_filter_projected_num_rows(&batch, &filter, &output_column_names).unwrap();
        std::hint::black_box(output_rows);
    }
}

criterion_group!(benches, benchmark_async_auto_policy_focus,);
criterion_main!(benches);
