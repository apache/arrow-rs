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

//! Implements a cached column reader that provides data using
//! previously decoded / filtered arrays

mod builder;
mod reader;

pub(crate) use builder::CachedPredicateResultBuilder;
use reader::CachedArrayReader;

use crate::arrow::array_reader::ArrayReader;
use crate::errors::Result;
use arrow_array::{ArrayRef, BooleanArray};

/// The result of evaluating a predicate on a RowGroup with a specific
/// RowSelection
///
/// The flow is:
/// * Decode with a RowSelection
/// * Apply a predicate --> this result
#[derive(Clone)]
pub(crate) struct CachedPredicateResult {
    /// Map of parquet schema column index to the result of evaluating the predicate
    /// on that column.
    ///
    /// NOTE each array already has had `filters` applied
    ///
    /// If `Some`, it is a set of arrays that make up the result. Each has
    /// batch_rows rows except for the last
    arrays: Vec<Option<Vec<ArrayRef>>>,
    /// The results of evaluating the predicate (this has already been applied to the
    /// cached results).
    filters: Vec<BooleanArray>,
}

impl CachedPredicateResult {
    pub(crate) fn new(num_columns: usize, filters: Vec<BooleanArray>) -> Self {
        Self {
            arrays: vec![None; num_columns],
            filters,
        }
    }

    /// Add the specified array to the cached result
    pub fn add_result(&mut self, column_index: usize, arrays: Vec<ArrayRef>) {
        // TODO how is this possible to end up with previously cached arrays?
        //assert!(self.arrays.get(column_index).is_none(), "column index {} already has a cached array", column_index);
        self.arrays[column_index] = Some(arrays);
    }

    /// Returns an array reader for the given column index, if any, that reads from the cache rather
    /// than the original column chunk
    pub(crate) fn build_reader(&self, col_index: usize) -> Result<Option<Box<dyn ArrayReader>>> {
        let Some(array) = &self.arrays[col_index] else {
            return Ok(None);
        };

        Ok(Some(Box::new(CachedArrayReader::new(
            array.clone(),
            &self.filters,
        ))))
    }
}
