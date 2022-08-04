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
use arrow::array::BooleanArray;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;

/// A predicate operating on [`RecordBatch`]
pub trait ArrowPredicate: Send + 'static {
    /// Returns the projection mask for this predicate
    fn projection(&self) -> &ProjectionMask;

    /// Called with a [`RecordBatch`] containing the columns identified by [`Self::mask`],
    /// with `true` values in the returned [`BooleanArray`] indicating rows
    /// matching the predicate.
    ///
    /// The returned [`BooleanArray`] must not contain any nulls
    fn filter(&mut self, batch: RecordBatch) -> ArrowResult<BooleanArray>;
}

/// An [`ArrowPredicate`] created from an [`FnMut`]
pub struct ArrowPredicateFn<F> {
    f: F,
    projection: ProjectionMask,
}

impl<F> ArrowPredicateFn<F>
where
    F: FnMut(RecordBatch) -> ArrowResult<BooleanArray> + Send + 'static,
{
    /// Create a new [`ArrowPredicateFn`]
    pub fn new(projection: ProjectionMask, f: F) -> Self {
        Self { f, projection }
    }
}

impl<F> ArrowPredicate for ArrowPredicateFn<F>
where
    F: FnMut(RecordBatch) -> ArrowResult<BooleanArray> + Send + 'static,
{
    fn projection(&self) -> &ProjectionMask {
        &self.projection
    }

    fn filter(&mut self, batch: RecordBatch) -> ArrowResult<BooleanArray> {
        (self.f)(batch)
    }
}

/// A [`RowFilter`] allows pushing down a filter predicate to skip IO and decode
///
/// This consists of a list of [`ArrowPredicate`] where only the rows that satisfy all
/// of the predicates will be returned. Any [`RowSelection`] will be applied prior
/// to the first predicate, and each predicate in turn will then be used to compute
/// a more refined [`RowSelection`] to use when evaluating the subsequent predicates.
///
/// Once all predicates have been evaluated, the resulting [`RowSelection`] will be
/// used to return just the desired rows.
///
/// This design has a couple of implications:
///
/// * [`RowFilter`] can be used to skip fetching IO, in addition to decode overheads
/// * Columns may be decoded multiple times if they appear in multiple [`ProjectionMask`]
/// * IO will be deferred until needed by a [`ProjectionMask`]
///
/// As such there is a trade-off between a single large predicate, or multiple predicates,
/// that will depend on the shape of the data. Whilst multiple smaller predicates may
/// minimise the amount of data scanned/decoded, it may not be faster overall.
///
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
