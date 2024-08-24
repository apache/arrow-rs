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

use crate::arrow::ProjectionMask;
use arrow_array::{BooleanArray, RecordBatch};
use arrow_schema::ArrowError;

/// A predicate operating on [`RecordBatch`]
///
/// See [`RowFilter`] for more information on the use of this trait.
pub trait ArrowPredicate: Send + 'static {
    /// Returns the [`ProjectionMask`] that describes the columns required
    /// to evaluate this predicate. All projected columns will be provided in the `batch`
    /// passed to [`evaluate`](Self::evaluate)
    fn projection(&self) -> &ProjectionMask;

    /// Evaluate this predicate for the given [`RecordBatch`] containing the columns
    /// identified by [`Self::projection`]
    ///
    /// Must return a [`BooleanArray`] that has the same length as the input
    /// `batch` where each row indicates whether the row should be returned:
    /// * `true`:the row should be returned
    /// * `false` or `null`: the row should not be returned
    fn evaluate(&mut self, batch: RecordBatch) -> Result<BooleanArray, ArrowError>;
}

/// An [`ArrowPredicate`] created from an [`FnMut`]
pub struct ArrowPredicateFn<F> {
    f: F,
    projection: ProjectionMask,
}

impl<F> ArrowPredicateFn<F>
where
    F: FnMut(RecordBatch) -> Result<BooleanArray, ArrowError> + Send + 'static,
{
    /// Create a new [`ArrowPredicateFn`]. `f` will be passed batches
    /// that contains the columns specified in `projection`
    /// and returns a [`BooleanArray`] that describes which rows should
    /// be passed along
    pub fn new(projection: ProjectionMask, f: F) -> Self {
        Self { f, projection }
    }
}

impl<F> ArrowPredicate for ArrowPredicateFn<F>
where
    F: FnMut(RecordBatch) -> Result<BooleanArray, ArrowError> + Send + 'static,
{
    fn projection(&self) -> &ProjectionMask {
        &self.projection
    }

    fn evaluate(&mut self, batch: RecordBatch) -> Result<BooleanArray, ArrowError> {
        (self.f)(batch)
    }
}

/// Filter applied *during* the parquet read process
///
/// [`RowFilter`] applies predicates in order, after decoding only the columns
/// required. As predicates eliminate rows, fewer rows from subsequent columns
/// may be required, thus potentially reducing IO and decode.
///
/// A `RowFilter` consists of a list of [`ArrowPredicate`]s. Only the rows for which
/// all the predicates evaluate to `true` will be returned.
/// Any [`RowSelection`] provided to the reader will be applied prior
/// to the first predicate, and each predicate in turn will then be used to compute
/// a more refined [`RowSelection`] used when evaluating the subsequent predicates.
///
/// Once all predicates have been evaluated, the final [`RowSelection`] is applied
/// to the top-level [`ProjectionMask`] to produce the final output [`RecordBatch`].
///
/// This design has a couple of implications:
///
/// * [`RowFilter`] can be used to skip entire pages, and thus IO, in addition to CPU decode overheads
/// * Columns may be decoded multiple times if they appear in multiple [`ProjectionMask`]
/// * IO will be deferred until needed by a [`ProjectionMask`]
///
/// As such there is a trade-off between a single large predicate, or multiple predicates,
/// that will depend on the shape of the data. Whilst multiple smaller predicates may
/// minimise the amount of data scanned/decoded, it may not be faster overall.
///
/// For example, if a predicate that needs a single column of data filters out all but
/// 1% of the rows, applying it as one of the early `ArrowPredicateFn` will likely significantly
/// improve performance.
///
/// As a counter example, if a predicate needs several columns of data to evaluate but
/// leaves 99% of the rows, it may be better to not filter the data from parquet and
/// apply the filter after the RecordBatch has been fully decoded.
///
/// Additionally, even if a predicate eliminates a moderate number of rows, it may still be faster
/// to filter the data after the RecordBatch has been fully decoded, if the eliminated rows are
/// not contiguous.
///
/// [`RowSelection`]: crate::arrow::arrow_reader::RowSelection
pub struct RowFilter {
    /// A list of [`ArrowPredicate`]
    pub(crate) predicates: Vec<Box<dyn ArrowPredicate>>,
}

impl RowFilter {
    /// Create a new [`RowFilter`] from an array of [`ArrowPredicate`]
    pub fn new(predicates: Vec<Box<dyn ArrowPredicate>>) -> Self {
        Self { predicates }
    }
}
