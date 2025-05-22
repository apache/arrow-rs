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
use arrow_array::cast::AsArray;
use arrow_array::{new_empty_array, Array, ArrayRef, BooleanArray, RecordBatch,};
use arrow_schema::{DataType, Schema};
use arrow_select::concat::concat;
use arrow_select::filter::{filter, prep_null_mask_filter};
use std::any::Any;
use std::collections::VecDeque;
use std::sync::Arc;
use arrow_array::builder::StringViewBuilder;

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
        batch_size: usize,
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
                        .map(|field| create_in_progress_array(true, field.data_type(), batch_size))
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
            in_progress_arrays.push(create_in_progress_array(
                in_projection,
                field.data_type(),
                batch_size,
            ));
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

        let mut cached_result = CachedPredicateResult::new(mask.len(), filters);
        let mut in_progress_arrays = VecDeque::from(in_progress_arrays);

        // Now find the location of the filter columns in the original parquet schema
        for (i, &item) in mask.iter().enumerate() {
            if item {
                let mut in_progress = in_progress_arrays
                    .pop_front()
                    .expect("insufficient in progress arrays");
                if let Some(arrays) = in_progress.try_build()? {
                    cached_result.add_result(i, arrays)
                }
            }
        }
        assert!(
            in_progress_arrays.is_empty(),
            "should have found all in progress arrays"
        );

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

struct CachedArrayReader {
    /// The cached arrays. These should already be broken down into the correct batch_size chunks
    cached_arrays: VecDeque<ArrayRef>,
    data_type: DataType,
    // /// The filter that was applied to the cached array (that has already been applied)
    //filter: BooleanArray,
    /// The length of the currently "in progress" array
    current_length: usize,
}

impl CachedArrayReader {
    fn new(cached_arrays: Vec<ArrayRef>, _filters: &[BooleanArray]) -> Self {
        //let input: Vec<&dyn Array> = filters.iter().map(|b| b as &dyn Array).collect::<Vec<_>>();
        //let filter = concat(&input).unwrap().as_boolean().clone();
        let data_type = cached_arrays
            .first()
            .expect("had at least one array")
            .data_type()
            .clone();
        Self {
            cached_arrays: VecDeque::from(cached_arrays),
            data_type,
            current_length: 0,
        }
    }
}

impl ArrayReader for CachedArrayReader {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_data_type(&self) -> &DataType {
        &self.data_type
    }

    fn read_records(&mut self, batch_size: usize) -> Result<usize> {
        // since the entire array is cached, reads always succeed
        self.current_length += batch_size;
        Ok(batch_size)
    }

    // Produce the "in progress" batch
    fn consume_batch(&mut self) -> Result<ArrayRef> {
        if self.current_length == 0 {
            return Ok(new_empty_array(&self.data_type));
        }

        let next_array = self.cached_arrays.pop_front().ok_or_else(|| {
            crate::errors::ParquetError::General(
                "Internal error: no more cached arrays".to_string(),
            )
        })?;

        // the next batch is the next array in the queue
        assert_eq!(self.current_length, next_array.len());
        self.current_length = 0;
        Ok(next_array)
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
trait InProgressArray {
    /// Appends all values of the array to the in progress array at locations where `filter[i]` is true
    /// to the in progress array
    fn append(&mut self, _array: ArrayRef, filter: &BooleanArray) -> Result<()>;

    /// Builds the final array, consuming all state from self. Returns None if the array
    /// cannot be created (e.g. data type not supported or out of buffer space)
    ///
    /// Each array except the last must have `batch_size` rows
    fn try_build(&mut self) -> Result<Option<Vec<ArrayRef>>>;
}

/// Return a new InProgressArray for the given data type
///
/// If `in_projection` is false then a NoOpInProgressArray is returned (will not
/// actually cache arrays results)
///
/// May also return None if the data type is not supported or caching the array
/// results is not possible.
fn create_in_progress_array(
    in_projection: bool,
    data_type: &DataType,
    batch_size: usize,
) -> Box<dyn InProgressArray> {
    if !in_projection {
        // column is not in the projection, so no need to cache
        return Box::new(NoOpInProgressArray::new());
    }

    match data_type {
        DataType::Utf8View => Box::new(InProgressArrayImpl::new(
            batch_size,
            StringViewBuilder::with_capacity(batch_size),
        )),
        _ => {
            // TODO implement more specific types
            Box::new(InProgressArrayImpl::new(
                batch_size,
                GenericArrayBuilder::new(),
            ))
        }
    }
}

/// A builder for creating an InProgressArray. Trait so we can use Dyn dispatch
trait InProgressArrayBuilder {
    /// Appends all values of the array to the in progress array
    ///
    /// TODO: potentially pass in filter and unfiltered array to avoid a copy
    fn append(&mut self, array: ArrayRef);

    /// Finalizes the in progress array, resetting state and returning the new array.
    ///
    /// Returns None if there are no rows in progress
    fn try_build(&mut self) -> Result<Option<ArrayRef>>;
}

/// Wraps an ArrayBuilder of some type for creating arrays of batch_size rows and implements
/// InProgressArray
struct InProgressArrayImpl<B: InProgressArrayBuilder> {
    batch_size: usize,
    /// Number of rows in the "current" array
    current_rows: usize,
    /// arrays that are "finished" (have batch_size rows)
    finished: Vec<ArrayRef>,
    inner: B,
}

impl<B: InProgressArrayBuilder> InProgressArrayImpl<B> {
    fn new(batch_size: usize, inner: B) -> Self {
        Self {
            batch_size,
            current_rows: 0,
            finished: vec![],
            inner,
        }
    }

    /// Combines all arrays in `current` into a new array in `finished` and returns the
    /// number of rows in the array added to `self.finished`
    fn finish_current(&mut self) -> Result<usize> {
        if self.current_rows == 0 {
            // nothing to do
            return Ok(0);
        }
        let Some(new_array) = self.inner.try_build()? else {
            // no rows in current
            self.current_rows = 0;
            return Ok(0);
        };

        let num_rows = new_array.len();
        self.finished.push(new_array);
        self.current_rows = 0;
        Ok(num_rows)
    }

    /// Add an array to the list of current arrays
    fn add_current(&mut self, array: ArrayRef) {
        if array.is_empty() {
            // no rows to add
            return;
        }

        self.current_rows += array.len();
        self.inner.append(array);
    }
}
impl<B: InProgressArrayBuilder> InProgressArray for InProgressArrayImpl<B> {
    fn append(&mut self, array: ArrayRef, filter_array: &BooleanArray) -> Result<()> {
        let filtered = filter(&array, filter_array)?;

        if self.current_rows + filtered.len() >= self.batch_size {
            let num_rows_needed = self.batch_size - self.current_rows;
            // enough rows to form exactly batchsize
            self.add_current(filtered.slice(0, num_rows_needed));
            let finished_rows = self.finish_current()?;

            assert_eq!(finished_rows, self.batch_size);
            // add any remaining rows to the current array
            let remaining = filtered.slice(num_rows_needed, filtered.len() - num_rows_needed);
            self.add_current(remaining);
        } else {
            self.add_current(filtered);
        }
        Ok(())
    }

    fn try_build(&mut self) -> Result<Option<Vec<ArrayRef>>> {
        self.finish_current()?;
        assert_eq!(self.current_rows, 0);
        Ok(Some(std::mem::take(&mut self.finished)))
    }
}

/// Placeholder that does nothing
struct NoOpInProgressArray {}

impl InProgressArray for NoOpInProgressArray {
    fn append(&mut self, _array: ArrayRef, _filter: &BooleanArray) -> Result<()> {
        Ok(())
    }
    fn try_build(&mut self) -> Result<Option<Vec<ArrayRef>>> {
        Ok(None)
    }
}

impl NoOpInProgressArray {
    fn new() -> Self {
        Self {}
    }
}

/// Implements a GenericArrayBuilder used for any array type by using buffering and `concat`
///
/// TODO avoid this by using type specific array builders
struct GenericArrayBuilder {
    arrays: Vec<ArrayRef>,
}

impl GenericArrayBuilder {
    fn new() -> Self {
        Self { arrays: vec![] }
    }
}

impl InProgressArrayBuilder for GenericArrayBuilder {
    fn append(&mut self, array: ArrayRef) {
        self.arrays.push(array);
    }

    fn try_build(&mut self) -> Result<Option<ArrayRef>> {
        if self.arrays.is_empty() {
            return Err(crate::errors::ParquetError::General(
                "Internal: No arrays to build".to_string(),
            ));
        }
        // vomit: need to have Vec[&dyn Array] to pass to concat
        let arrays: Vec<&dyn Array> = self.arrays.iter().map(|a| a.as_ref()).collect();
        let new_array = concat(&arrays)?;
        self.arrays.clear();
        Ok(Some(new_array))
    }
}

/// Implement the InProgressArrayBuilder for StringViewBuilder
impl InProgressArrayBuilder for StringViewBuilder {
    fn append(&mut self, array: ArrayRef) {
        let array = array.as_string_view();
        self.append_array(array);
    }

    fn try_build(&mut self) -> Result<Option<ArrayRef>> {
        let new_array = self.finish();
        if new_array.is_empty() {
            Ok(None)
        } else {
            Ok(Some(Arc::new(new_array)))
        }
    }
}

