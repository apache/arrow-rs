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

//! [`IncrementalRecordBatchBuilder`] for incrementally building RecordBatches from other arrays

use crate::filter::{FilterBuilder, FilterPredicate, SlicesIterator};
use crate::incremental_array_builder::{GenericIncrementalArrayBuilder, IncrementalArrayBuilder};
use arrow_array::builder::{BinaryViewBuilder, PrimitiveBuilder, StringViewBuilder};
use arrow_array::{downcast_primitive, BooleanArray, RecordBatch};
use arrow_schema::{ArrowError, DataType, SchemaRef};
use std::borrow::Cow;
use std::collections::VecDeque;

type ArrayBuilderImpl = Box<dyn IncrementalArrayBuilder>;

/// Incrementally creates `RecordBatch`es of limited size
///
/// This structure implements the common pattern of incrementally creating
/// output batches of a specific size from an input stream of arrays.
///
/// See: <https://github.com/apache/arrow-rs/issues/6692>
///
/// This is a convenience over [`IncrementalArrayBuilder`] which is used to
/// build arrays for each column in the `RecordBatch`.
///
/// Which rows are selected from the input arrays are be chosen using one of the
/// following mechanisms:
///
/// 1.  `concat`-enated: all rows from the input array are appended
/// 2. `filter`-ed: the input array is filtered using a `BooleanArray`
/// 3. `take`-n: a subset of the input array is selected based on the indices provided in a `UInt32Array` or similar.
///
/// This structure handles multiple arrays
pub struct IncrementalRecordBatchBuilder {
    /// The schema of the RecordBatches being built
    schema: SchemaRef,
    /// The maximum size, in rows, of the arrays being built
    batch_size: usize,
    /// Should we 'optimize' the predicate before applying it?
    optimize_predicate: bool,
    /// batches that are "finished" (have batch_size rows)
    finished: VecDeque<RecordBatch>,
    /// The current arrays being built
    current: Vec<ArrayBuilderImpl>,
}

impl std::fmt::Debug for IncrementalRecordBatchBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncrementalRecordBatchBuilder")
            .field("schema", &self.schema)
            .field("batch_size", &self.batch_size)
            .field("optimize_predicate", &self.optimize_predicate)
            .field("finished", &self.finished.len())
            .field("current", &self.current.len())
            .finish()
    }
}

impl IncrementalRecordBatchBuilder {
    /// Creates a new builder with the specified batch size and schema
    ///
    /// There must be at least one column in the schema, and the batch size must be greater than 0.
    pub fn try_new(schema: SchemaRef, batch_size: usize) -> Result<Self, ArrowError> {
        if schema.fields().is_empty() {
            return Err(ArrowError::InvalidArgumentError(
                "IncrementalRecordBatchBuilder Schema must have at least one field".to_string(),
            ));
        }

        if batch_size == 0 {
            return Err(ArrowError::InvalidArgumentError(
                "IncrementalRecordBatchBuilder batch size must be greater than 0".to_string(),
            ));
        }

        let current = schema
            .fields()
            .iter()
            .map(|field| instantiate_builder(field.data_type(), batch_size))
            .collect::<Vec<_>>();

        // Optimize the predicate if we will use it more than once (have more than 1 array)
        let optimize_predicate = schema.fields().len() > 1
            || schema
                .fields()
                .iter()
                .any(|f| FilterBuilder::multiple_arrays(f.data_type()));

        Ok(Self {
            schema,
            batch_size,
            optimize_predicate,
            finished: VecDeque::new(),
            current,
        })
    }

    /// Return the current schema of the builder
    #[allow(dead_code)]
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Combines all arrays in `current` into a new array in `finished` and returns the
    /// number of rows in the array added to `self.finished`
    fn finish_current(&mut self) -> Result<usize, ArrowError> {
        debug_assert!(
            self.current
                .iter()
                .all(|b| b.is_empty() == self.current[0].is_empty()),
            "All builders in current must match is_empty"
        );

        if self.current[0].is_empty() {
            // no rows in progress, so nothing to do
            return Ok(0);
        }
        let new_arrays: Vec<_> = self
            .current
            .iter_mut()
            .map(|builder| builder.finish())
            .collect();
        let batch = RecordBatch::try_new(self.schema.clone(), new_arrays)?;

        let num_rows = batch.num_rows();
        self.finished.push_back(batch);
        Ok(num_rows)
    }

    /// returns the number of rows currently in progress
    pub fn num_current_rows(&self) -> usize {
        debug_assert!(
            self.current
                .iter()
                .all(|b| b.len() == self.current[0].len()),
            "All builders in current must have the same length"
        );
        self.current[0].len()
    }

    /// Return the next `RecordBatch` if it is ready, or `None` if not
    ///
    /// This allows the builder to be used in a streaming fashion where rows are
    /// added incrementally and produced in batches.
    ///
    /// Each batch
    #[allow(dead_code)]
    pub fn next_batch(&mut self) -> Option<RecordBatch> {
        // return the last finished batch
        self.finished.pop_front()
    }

    /// Finalize this builder, returning any remaining batches
    pub fn build(mut self) -> Result<VecDeque<RecordBatch>, ArrowError> {
        self.finish_current()?;
        let Self { finished, .. } = self;
        Ok(finished)
    }

    /// Appends all rows from the input batch to the current arrays where
    /// `filter_array` is `true`.
    ///
    /// This method optimizes for the case where the filter selects all or no rows
    /// and ensures all output arrays in `current` is at most `batch_size` rows long.
    pub fn append_filtered(
        &mut self,
        mut batch: RecordBatch,
        filter_array: &BooleanArray,
    ) -> Result<(), ArrowError> {
        let mut filter_array = Cow::Borrowed(filter_array);
        loop {
            // how many more rows do we need to fill the current array?
            let row_limit = self.batch_size - self.num_current_rows();
            match get_filter_limit(&filter_array, row_limit) {
                FilterLimit::None => {
                    break;
                }
                FilterLimit::Filter => {
                    let predicate = create_predicate(&filter_array, self.optimize_predicate);
                    let columns = batch.columns().iter();
                    for (builder, array) in self.current.iter_mut().zip(columns) {
                        builder.append_filtered(array, &predicate)?;
                    }
                    break;
                }
                FilterLimit::Concat => {
                    let columns = batch.columns().iter();
                    for (builder, array) in self.current.iter_mut().zip(columns) {
                        builder.append_array(array)?; // append the entire array
                    }
                    break;
                }
                FilterLimit::Slice { end } => {
                    // can only fit a slice of the filter into the current array
                    let sliced_filter = filter_array.slice(0, end);
                    let remain_filter = filter_array.slice(end, filter_array.len() - end);
                    let sliced_batch = batch.slice(0, end);
                    let remain_batch = batch.slice(end, batch.num_rows() - end);

                    let predicate = create_predicate(&sliced_filter, self.optimize_predicate);
                    let columns = sliced_batch.columns().iter();
                    for (builder, array) in self.current.iter_mut().zip(columns) {
                        // append the sliced array and filter
                        builder.append_filtered(array, &predicate)?;
                    }
                    let completed_rows = self.finish_current()?;
                    assert_eq!(completed_rows, self.batch_size);

                    // update the filter / array with the slices
                    filter_array = Cow::Owned(remain_filter);
                    batch = remain_batch;
                }
            }
        }

        // Finish the current batch if it is full
        assert!(
            self.num_current_rows() <= self.batch_size,
            "Current batch should not exceed batch size. Current rows: {}, batch size: {}",
            self.num_current_rows(),
            self.batch_size
        );
        if self.num_current_rows() >= self.batch_size {
            let completed_rows = self.finish_current()?;
            assert_eq!(completed_rows, self.batch_size);
        }
        Ok(())
    }
}

fn create_predicate(filter: &BooleanArray, optimize: bool) -> FilterPredicate {
    // Create a filter predicate from the BooleanArray
    let mut builder = FilterBuilder::new(filter);
    if optimize {
        // Optimize the predicate if we have more than one array or the filter is complex
        builder = builder.optimize()
    }
    builder.build()
}

#[derive(Debug)]
enum FilterLimit {
    /// The filter selects no rows
    None,
    /// Use the entire filter as is
    Filter,
    /// The filter selects all rows, so need to consult it
    Concat,
    /// Can only fit a slice of the filter 0,end
    Slice {
        /// end of the slice from the filter
        end: usize,
    },
}

/// Returns the number of rows from filter which must be taken to ensure
/// that there are no more than `row_limit` rows in a filtered result.
fn get_filter_limit(filter: &BooleanArray, row_limit: usize) -> FilterLimit {
    // Calculating the number of true values in the filter is very fast so check
    // it first for the common case.
    let true_count = filter.true_count();

    if true_count == 0 {
        // no rows selected by the filter
        return FilterLimit::None;
    }

    if true_count <= row_limit {
        return if true_count == filter.len() {
            FilterLimit::Concat
        } else {
            FilterLimit::Filter
        };
    }

    // there are more true values than remaining, so we need to slice the filter
    let mut slices_iter = SlicesIterator::new(filter);
    // how many rows are already included in filter?
    let mut in_filter_rows = 0;
    loop {
        let Some((start, end)) = slices_iter.next() else {
            panic!("slices iterator should not be empty if true_count > remaining");
        };

        let slice_len = end - start;
        if slice_len + in_filter_rows < row_limit {
            // this slice does not have enough rows, so add it to the count
            in_filter_rows += slice_len;
        } else {
            // adjust end so it only selects the remaining rows
            let rows_needed_in_slice = row_limit - in_filter_rows;
            return FilterLimit::Slice {
                end: start + rows_needed_in_slice,
            };
        }
    }
}

/// Create an incremental array builder for the given data type
///
/// Uses a generic implementation if we don't have a specific builder for the type
fn instantiate_builder(data_type: &DataType, batch_size: usize) -> ArrayBuilderImpl {
    // Create a primitive builder for the given data type
    macro_rules! primitive_builder_helper {
        ($t:ty, $DT:expr, $SZ:expr) => {
            Box::new(PrimitiveBuilder::<$t>::with_capacity($SZ).with_data_type($DT.clone()))
        };
    }

    downcast_primitive! {
        data_type => (primitive_builder_helper, data_type, batch_size),
        DataType::Utf8View => Box::new(StringViewBuilder::with_capacity(batch_size)),
        DataType::BinaryView => Box::new(BinaryViewBuilder::with_capacity(batch_size)),

        // Default to using the generic builder for all other types
        // TODO file tickets tracking specific builders for other types
        _ => Box::new(GenericIncrementalArrayBuilder::new()),
    }
}
