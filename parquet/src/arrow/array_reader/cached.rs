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

use crate::arrow::array_reader::ArrayReader;
use crate::arrow::arrow_reader::RowSelection;
use crate::arrow::ProjectionMask;
use crate::errors::Result;
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch};
use arrow_schema::{DataType, Schema};
use arrow_select::concat::concat;
use arrow_select::filter::{filter, prep_null_mask_filter};
use std::any::Any;
use std::collections::VecDeque;
use std::sync::Arc;

/// Incrementally builds the result of evaluating a ArrowPredicate on
/// a RowGroup.
pub(crate) struct CachedPredicateResultBuilder {
    /// The entire result of the predicate evaluation in memory
    ///
    /// TODO: potentially incrementally build the result of the predicate
    /// evaluation without holding all the batches in memory. See
    /// <https://github.com/apache/arrow-rs/issues/6692>
    in_progress_arrays: Vec<Box<dyn InProgressArray>>,
    filters: Vec<BooleanArray>,
}

impl CachedPredicateResultBuilder {
    /// Create a new CachedPredicateResultBuilder
    ///
    /// # Arguments:
    /// * `schema`: The schema of the filter record batch
    /// * `filter_mask`: which columns of the original parquet schema did the filter columns come from?
    /// * `projection_mask`: which columns of the original parquet schema are in the final projection?
    ///
    /// This structure does not cache filter results for the columns that are not
    /// in the projection mask. This is because the filter results are not needed
    pub(crate) fn new(
        schema: &Schema,
        filter_mask: &ProjectionMask,
        projection_mask: &ProjectionMask,
    ) -> Self {
        let mut field_iter = schema.fields.iter();

        let (filter_mask_inner, projection_mask_inner) =
            match (filter_mask.mask(), projection_mask.mask()) {
                (Some(filter_mask), Some(projection_mask)) => (filter_mask, projection_mask),
                // NB, None means all columns and we just want the intersection of the two
                (Some(filter_mask), None) => (filter_mask, filter_mask),
                (None, Some(projection_mask)) => (projection_mask, projection_mask),
                (None, None) => {
                    // this means all columns are in the projection and filter so cache them all when possible
                    let in_progress_arrays = field_iter
                        .map(|field| create_in_progress_array(true, field.data_type()))
                        .collect();
                    return {
                        Self {
                            in_progress_arrays,
                            filters: vec![],
                        }
                    };
                }
            };

        let mut in_progress_arrays = Vec::with_capacity(filter_mask_inner.len());

        for (&in_filter, &in_projection) in
            filter_mask_inner.iter().zip(projection_mask_inner.iter())
        {
            if !in_filter {
                continue;
            }
            // field is in the filter
            let field = field_iter.next().expect("mismatch in field lengths");
            in_progress_arrays.push(create_in_progress_array(in_projection, field.data_type()));
        }
        assert_eq!(in_progress_arrays.len(), schema.fields().len());

        Self {
            in_progress_arrays,
            filters: vec![],
        }
    }

    /// Add a new batch and filter to the builder
    pub(crate) fn add(&mut self, batch: RecordBatch, mut filter: BooleanArray) -> Result<()> {
        if filter.null_count() > 0 {
            filter = prep_null_mask_filter(&filter);
        }

        let (_schema, columns, _row_count) = batch.into_parts();

        for (in_progress, array) in self.in_progress_arrays.iter_mut().zip(columns.into_iter()) {
            in_progress.append(array, &filter)?;
        }

        self.filters.push(filter);
        Ok(())
    }

    /// Return (selection, maybe_cached_predicate_result) that represents the rows
    /// that were selected and batches that were evaluated.
    pub(crate) fn build(
        self,
        _batch_size: usize,
        filter_mask: &ProjectionMask,
    ) -> Result<(RowSelection, Option<CachedPredicateResult>)> {
        let Self {
            in_progress_arrays,
            filters,
        } = self;

        let new_selection = RowSelection::from_filters(&filters);

        let Some(mask) = filter_mask.mask() else {
            return Ok((new_selection, None));
        };

        let mut arrays: Vec<Option<ArrayRef>> = vec![None; mask.len()];
        let mut in_progress_arrays = VecDeque::from(in_progress_arrays);

        // Now find the location of the filter columns in the original parquet schema
        for i in 0..mask.len() {
            if mask[i] {
                let mut in_progress = in_progress_arrays
                    .pop_front()
                    .expect("insufficient in progress arrays");
                let array = in_progress.try_build()?;
                arrays[i] = array;
            }
        }
        assert!(
            in_progress_arrays.is_empty(),
            "should have found all in progress arrays"
        );

        let cached_result = CachedPredicateResult { arrays, filters };

        Ok((new_selection, Some(cached_result)))
    }
}

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
    /// NOTE each array already has the corresponding filters applied
    ///
    /// TODO: store as `Vec<Vec<ArrayRef>>` to avoid having to have one large
    /// array for each column
    arrays: Vec<Option<ArrayRef>>,
    /// The results of evaluating the predicate (this has already been applied to the
    /// cached results)
    filters: Vec<BooleanArray>,
}

impl CachedPredicateResult {
    /// Returns an array reader for the given column index, if any, that reads from the cache rather
    /// than the original column chunk
    pub(crate) fn build_reader(&self, col_index: usize) -> Result<Option<Box<dyn ArrayReader>>> {
        let Some(array) = &self.arrays[col_index] else {
            return Ok(None);
        };

        Ok(Some(Box::new(CachedArrayReader::new(
            Arc::clone(array),
            &self.filters,
        ))))
    }
}

struct CachedArrayReader {
    /// The cached array
    cached_array: ArrayRef,
    // /// The filter that was applied to the cached array (that has already been applied)
    //filter: BooleanArray,
    /// The start of the rows in cached_array that have not yet been returned
    current_offset: usize,
    /// The length of the currently "in progress" array
    current_length: usize,
}

impl CachedArrayReader {
    fn new(cached_array: ArrayRef, _filters: &[BooleanArray]) -> Self {
        //let input: Vec<&dyn Array> = filters.iter().map(|b| b as &dyn Array).collect::<Vec<_>>();
        //let filter = concat(&input).unwrap().as_boolean().clone();

        Self {
            cached_array,
            current_offset: 0,
            current_length: 0,
        }
    }
}

impl ArrayReader for CachedArrayReader {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_data_type(&self) -> &DataType {
        self.cached_array.data_type()
    }

    fn read_records(&mut self, batch_size: usize) -> Result<usize> {
        // since the entire array is cached, reads always succeed
        self.current_length += batch_size;
        Ok(batch_size)
    }

    // Produce the "in progress" batch
    fn consume_batch(&mut self) -> Result<ArrayRef> {
        let result = self
            .cached_array
            .slice(self.current_offset, self.current_length);
        self.current_offset += self.current_length;
        self.current_length = 0;
        Ok(result)
    }

    fn skip_records(&mut self, num_records: usize) -> Result<usize> {
        // todo!()
        // it would be good to verify the pattern of read/consume matches
        // the boolean array
        Ok(num_records)
    }

    fn get_def_levels(&self) -> Option<&[i16]> {
        None // TODO this is likely not right for structured types
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        None // TODO this is likely not right for structured types
    }
}

/// Progressively creates array from filtered values
///
/// TODO avoid buffering the input memory
trait InProgressArray {
    /// Appends all values of the array to the in progress array at locations where `filter[i]` is true
    /// to the in progress array
    fn append(&mut self, _array: ArrayRef, filter: &BooleanArray) -> crate::errors::Result<()>;

    /// Builds the final array, consuming all state from self. Returns None if the array
    /// cannot be created (e.g. data type not supported or out of buffer space)
    fn try_build(&mut self) -> crate::errors::Result<Option<ArrayRef>>;
}

/// Return a new InProgressArray for the given data type
///
/// if `in_projection` is false then a NoOpInProgressArray is returned (will not
/// actually cache arrays results)
fn create_in_progress_array(
    in_projection: bool,
    _data_type: &DataType,
) -> Box<dyn InProgressArray> {
    if in_projection {
        Box::new(GenericInProgressArray::new())
    } else {
        // column is not in the projection, so no need to cache
        Box::new(NoOpInProgressArray {})
    }
}

/// Placeholder that does nothing until we support the entire set of datatypes
struct NoOpInProgressArray {}

impl InProgressArray for NoOpInProgressArray {
    fn append(&mut self, _array: ArrayRef, _filter: &BooleanArray) -> crate::errors::Result<()> {
        // do nothing
        Ok(())
    }
    fn try_build(&mut self) -> crate::errors::Result<Option<ArrayRef>> {
        // do nothing
        Ok(None)
    }
}

/// a generic implementation of InProgressArray that uses filter and concat kernels
/// to create the final array
///
/// TODO: make this better with per type implementations
/// <https://github.com/apache/arrow-rs/issues/6692>
struct GenericInProgressArray {
    /// previously filtered arrays
    arrays: Vec<ArrayRef>,
}

impl GenericInProgressArray {
    fn new() -> Self {
        Self { arrays: vec![] }
    }
}
impl InProgressArray for GenericInProgressArray {
    fn append(
        &mut self,
        array: ArrayRef,
        filter_array: &BooleanArray,
    ) -> crate::errors::Result<()> {
        self.arrays.push(filter(&array, filter_array)?);
        Ok(())
    }

    fn try_build(&mut self) -> Result<Option<ArrayRef>> {
        if self.arrays.is_empty() {
            return Ok(None);
        }
        if self.arrays.len() == 1 {
            return Ok(Some(self.arrays.pop().unwrap()));
        }
        // Vomit: need to copy to a new Vec to get dyn array
        let arrays: Vec<&dyn Array> = self.arrays.iter().map(|a| a.as_ref()).collect();
        let array = concat(&arrays)?;
        Ok(Some(array))
    }
}
