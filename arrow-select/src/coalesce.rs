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

//! [`BatchCoalescer`]  concatenates multiple [`RecordBatch`]es after
//! operations such as [`filter`] and [`take`].
//!
//! [`filter`]: crate::filter::filter
//! [`take`]: crate::take::take
use crate::filter::filter_record_batch;
use crate::take::take_record_batch;
use arrow_array::types::{BinaryViewType, StringViewType};
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, downcast_primitive};
use arrow_schema::{ArrowError, DataType, SchemaRef};
use std::collections::VecDeque;
use std::sync::Arc;
// Originally From DataFusion's coalesce module:
// https://github.com/apache/datafusion/blob/9d2f04996604e709ee440b65f41e7b882f50b788/datafusion/physical-plan/src/coalesce/mod.rs#L26-L25

mod byte_view;
mod generic;
mod primitive;

use byte_view::InProgressByteViewArray;
use generic::GenericInProgressArray;
use primitive::InProgressPrimitiveArray;

/// Concatenate multiple [`RecordBatch`]es
///
/// Implements the common pattern of incrementally creating output
/// [`RecordBatch`]es of a specific size from an input stream of
/// [`RecordBatch`]es.
///
/// This is useful after operations such as [`filter`] and [`take`] that produce
/// smaller batches, and we want to coalesce them into larger batches for
/// further processing.
///
/// # Motivation
///
/// If we use [`concat_batches`] to implement the same functionality, there are 2 potential issues:
/// 1. At least 2x peak memory (holding the input and output of concat)
/// 2. 2 copies of the data (to create the output of filter and then create the output of concat)
///
/// See: <https://github.com/apache/arrow-rs/issues/6692> for more discussions
/// about the motivation.
///
/// [`filter`]: crate::filter::filter
/// [`take`]: crate::take::take
/// [`concat_batches`]: crate::concat::concat_batches
///
/// # Example
/// ```
/// use arrow_array::record_batch;
/// use arrow_select::coalesce::{BatchCoalescer};
/// let batch1 = record_batch!(("a", Int32, [1, 2, 3])).unwrap();
/// let batch2 = record_batch!(("a", Int32, [4, 5])).unwrap();
///
/// // Create a `BatchCoalescer` that will produce batches with at least 4 rows
/// let target_batch_size = 4;
/// let mut coalescer = BatchCoalescer::new(batch1.schema(), 4);
///
/// // push the batches
/// coalescer.push_batch(batch1).unwrap();
/// // only pushed 3 rows (not yet 4, enough to produce a batch)
/// assert!(coalescer.next_completed_batch().is_none());
/// coalescer.push_batch(batch2).unwrap();
/// // now we have 5 rows, so we can produce a batch
/// let finished = coalescer.next_completed_batch().unwrap();
/// // 4 rows came out (target batch size is 4)
/// let expected = record_batch!(("a", Int32, [1, 2, 3, 4])).unwrap();
/// assert_eq!(finished, expected);
///
/// // Have no more input, but still have an in-progress batch
/// assert!(coalescer.next_completed_batch().is_none());
/// // We can finish the batch, which will produce the remaining rows
/// coalescer.finish_buffered_batch().unwrap();
/// let expected = record_batch!(("a", Int32, [5])).unwrap();
/// assert_eq!(coalescer.next_completed_batch().unwrap(), expected);
///
/// // The coalescer is now empty
/// assert!(coalescer.next_completed_batch().is_none());
/// ```
///
/// # Background
///
/// Generally speaking, larger [`RecordBatch`]es are more efficient to process
/// than smaller [`RecordBatch`]es (until the CPU cache is exceeded) because
/// there is fixed processing overhead per batch. This coalescer builds up these
/// larger batches incrementally.
///
/// ```text
/// ┌────────────────────┐
/// │    RecordBatch     │
/// │   num_rows = 100   │
/// └────────────────────┘                 ┌────────────────────┐
///                                        │                    │
/// ┌────────────────────┐     Coalesce    │                    │
/// │                    │      Batches    │                    │
/// │    RecordBatch     │                 │                    │
/// │   num_rows = 200   │  ─ ─ ─ ─ ─ ─ ▶  │                    │
/// │                    │                 │    RecordBatch     │
/// │                    │                 │   num_rows = 400   │
/// └────────────────────┘                 │                    │
///                                        │                    │
/// ┌────────────────────┐                 │                    │
/// │                    │                 │                    │
/// │    RecordBatch     │                 │                    │
/// │   num_rows = 100   │                 └────────────────────┘
/// │                    │
/// └────────────────────┘
/// ```
///
/// # Notes:
///
/// 1. Output rows are produced in the same order as the input rows
///
/// 2. The output is a sequence of batches, with all but the last being at exactly
///    `target_batch_size` rows.
#[derive(Debug)]
pub struct BatchCoalescer {
    /// The input schema
    schema: SchemaRef,
    /// The target batch size (and thus size for views allocation). This is a
    /// hard limit: the output batch will be exactly `target_batch_size`,
    /// rather than possibly being slightly above.
    target_batch_size: usize,
    /// In-progress arrays
    in_progress_arrays: Vec<Box<dyn InProgressArray>>,
    /// Buffered row count. Always less than `batch_size`
    buffered_rows: usize,
    /// Completed batches
    completed: VecDeque<RecordBatch>,
    /// Biggest coalesce batch size. See [`Self::with_biggest_coalesce_batch_size`]
    biggest_coalesce_batch_size: Option<usize>,
}

impl BatchCoalescer {
    /// Create a new `BatchCoalescer`
    ///
    /// # Arguments
    /// - `schema` - the schema of the output batches
    /// - `target_batch_size` - the number of rows in each output batch.
    ///   Typical values are `4096` or `8192` rows.
    ///
    pub fn new(schema: SchemaRef, target_batch_size: usize) -> Self {
        let in_progress_arrays = schema
            .fields()
            .iter()
            .map(|field| create_in_progress_array(field.data_type(), target_batch_size))
            .collect::<Vec<_>>();

        Self {
            schema,
            target_batch_size,
            in_progress_arrays,
            // We will for sure store at least one completed batch
            completed: VecDeque::with_capacity(1),
            buffered_rows: 0,
            biggest_coalesce_batch_size: None,
        }
    }

    /// Set the coalesce batch size limit (default `None`)
    ///
    /// This limit determine when batches should bypass coalescing. Intuitively,
    /// batches that are already large are costly to coalesce and are efficient
    /// enough to process directly without coalescing.
    ///
    /// If `Some(limit)`, batches larger than this limit will bypass coalescing
    /// when there is no buffered data, or when the previously buffered data
    /// already exceeds this limit.
    ///
    /// If `None`, all batches will be coalesced according to the
    /// target_batch_size.
    pub fn with_biggest_coalesce_batch_size(mut self, limit: Option<usize>) -> Self {
        self.biggest_coalesce_batch_size = limit;
        self
    }

    /// Get the current biggest coalesce batch size limit
    ///
    /// See [`Self::with_biggest_coalesce_batch_size`] for details
    pub fn biggest_coalesce_batch_size(&self) -> Option<usize> {
        self.biggest_coalesce_batch_size
    }

    /// Set the biggest coalesce batch size limit
    ///
    /// See [`Self::with_biggest_coalesce_batch_size`] for details
    pub fn set_biggest_coalesce_batch_size(&mut self, limit: Option<usize>) {
        self.biggest_coalesce_batch_size = limit;
    }

    /// Return the schema of the output batches
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// Push a batch into the Coalescer after applying a filter
    ///
    /// This is semantically equivalent of calling [`Self::push_batch`]
    /// with the results from  [`filter_record_batch`]
    ///
    /// # Example
    /// ```
    /// # use arrow_array::{record_batch, BooleanArray};
    /// # use arrow_select::coalesce::BatchCoalescer;
    /// let batch1 = record_batch!(("a", Int32, [1, 2, 3])).unwrap();
    /// let batch2 = record_batch!(("a", Int32, [4, 5, 6])).unwrap();
    /// // Apply a filter to each batch to pick the first and last row
    /// let filter = BooleanArray::from(vec![true, false, true]);
    /// // create a new Coalescer that targets creating 1000 row batches
    /// let mut coalescer = BatchCoalescer::new(batch1.schema(), 1000);
    /// coalescer.push_batch_with_filter(batch1, &filter);
    /// coalescer.push_batch_with_filter(batch2, &filter);
    /// // finsh and retrieve the created batch
    /// coalescer.finish_buffered_batch().unwrap();
    /// let completed_batch = coalescer.next_completed_batch().unwrap();
    /// // filtered out 2 and 5:
    /// let expected_batch = record_batch!(("a", Int32, [1, 3, 4, 6])).unwrap();
    /// assert_eq!(completed_batch, expected_batch);
    /// ```
    pub fn push_batch_with_filter(
        &mut self,
        batch: RecordBatch,
        filter: &BooleanArray,
    ) -> Result<(), ArrowError> {
        // TODO: optimize this to avoid materializing (copying the results
        // of filter to a new batch)
        let filtered_batch = filter_record_batch(&batch, filter)?;
        self.push_batch(filtered_batch)
    }

    /// Push a batch into the Coalescer after applying a set of indices
    /// This is semantically equivalent of calling [`Self::push_batch`]
    /// with the results from  [`take_record_batch`]
    ///
    /// # Example
    /// ```
    /// # use arrow_array::{record_batch, UInt64Array};
    /// # use arrow_select::coalesce::BatchCoalescer;
    /// let batch1 = record_batch!(("a", Int32, [0, 0, 0])).unwrap();
    /// let batch2 = record_batch!(("a", Int32, [1, 1, 4, 5, 1, 4])).unwrap();
    /// // Sorted indices to create a sorted output, this can be obtained with
    /// // `arrow-ord`'s sort_to_indices operation
    /// let indices = UInt64Array::from(vec![0, 1, 4, 2, 5, 3]);
    /// // create a new Coalescer that targets creating 1000 row batches
    /// let mut coalescer = BatchCoalescer::new(batch1.schema(), 1000);
    /// coalescer.push_batch(batch1);
    /// coalescer.push_batch_with_indices(batch2, &indices);
    /// // finsh and retrieve the created batch
    /// coalescer.finish_buffered_batch().unwrap();
    /// let completed_batch = coalescer.next_completed_batch().unwrap();
    /// let expected_batch = record_batch!(("a", Int32, [0, 0, 0, 1, 1, 1, 4, 4, 5])).unwrap();
    /// assert_eq!(completed_batch, expected_batch);
    /// ```
    pub fn push_batch_with_indices(
        &mut self,
        batch: RecordBatch,
        indices: &dyn Array,
    ) -> Result<(), ArrowError> {
        // todo: optimize this to avoid materializing (copying the results of take indices to a new batch)
        let taken_batch = take_record_batch(&batch, indices)?;
        self.push_batch(taken_batch)
    }

    /// Push all the rows from `batch` into the Coalescer
    ///
    /// When buffered data plus incoming rows reach `target_batch_size` ,
    /// completed batches are generated eagerly and can be retrieved via
    /// [`Self::next_completed_batch()`].
    /// Output batches contain exactly `target_batch_size` rows, so the tail of
    /// the input batch may remain buffered.
    /// Remaining partial data either waits for future input batches or can be
    /// materialized immediately by calling [`Self::finish_buffered_batch()`].
    ///
    /// # Example
    /// ```
    /// # use arrow_array::record_batch;
    /// # use arrow_select::coalesce::BatchCoalescer;
    /// let batch1 = record_batch!(("a", Int32, [1, 2, 3])).unwrap();
    /// let batch2 = record_batch!(("a", Int32, [4, 5, 6])).unwrap();
    /// // create a new Coalescer that targets creating 1000 row batches
    /// let mut coalescer = BatchCoalescer::new(batch1.schema(), 1000);
    /// coalescer.push_batch(batch1);
    /// coalescer.push_batch(batch2);
    /// // finsh and retrieve the created batch
    /// coalescer.finish_buffered_batch().unwrap();
    /// let completed_batch = coalescer.next_completed_batch().unwrap();
    /// let expected_batch = record_batch!(("a", Int32, [1, 2, 3, 4, 5, 6])).unwrap();
    /// assert_eq!(completed_batch, expected_batch);
    /// ```
    pub fn push_batch(&mut self, batch: RecordBatch) -> Result<(), ArrowError> {
        // Large batch bypass optimization:
        // When biggest_coalesce_batch_size is configured and a batch exceeds this limit,
        // we can avoid expensive split-and-merge operations by passing it through directly.
        //
        // IMPORTANT: This optimization is OPTIONAL and only active when biggest_coalesce_batch_size
        // is explicitly set via with_biggest_coalesce_batch_size(Some(limit)).
        // If not set (None), ALL batches follow normal coalescing behavior regardless of size.

        // =============================================================================
        // CASE 1: No buffer + large batch → Direct bypass
        // =============================================================================
        // Example scenario (target_batch_size=1000, biggest_coalesce_batch_size=Some(500)):
        // Input sequence: [600, 1200, 300]
        //
        // With biggest_coalesce_batch_size=Some(500) (optimization enabled):
        //   600 → large batch detected! buffered_rows=0 → Case 1: direct bypass
        //        → output: [600] (bypass, preserves large batch)
        //   1200 → large batch detected! buffered_rows=0 → Case 1: direct bypass
        //         → output: [1200] (bypass, preserves large batch)
        //   300 → normal batch, buffer: [300]
        //   Result: [600], [1200], [300] - large batches preserved, mixed sizes

        // =============================================================================
        // CASE 2: Buffer too large + large batch → Flush first, then bypass
        // =============================================================================
        // This case prevents creating extremely large merged batches that would
        // significantly exceed both target_batch_size and biggest_coalesce_batch_size.
        //
        // Example 1: Buffer exceeds limit before large batch arrives
        // target_batch_size=1000, biggest_coalesce_batch_size=Some(400)
        // Input: [350, 200, 800]
        //
        // Step 1: push_batch([350])
        //   → batch_size=350 <= 400, normal path
        //   → buffer: [350], buffered_rows=350
        //
        // Step 2: push_batch([200])
        //   → batch_size=200 <= 400, normal path
        //   → buffer: [350, 200], buffered_rows=550
        //
        // Step 3: push_batch([800])
        //   → batch_size=800 > 400, large batch path
        //   → buffered_rows=550 > 400 → Case 2: flush first
        //   → flush: output [550] (combined [350, 200])
        //   → then bypass: output [800]
        //   Result: [550], [800] - buffer flushed to prevent oversized merge
        //
        // Example 2: Multiple small batches accumulate before large batch
        // target_batch_size=1000, biggest_coalesce_batch_size=Some(300)
        // Input: [150, 100, 80, 900]
        //
        // Step 1-3: Accumulate small batches
        //   150 → buffer: [150], buffered_rows=150
        //   100 → buffer: [150, 100], buffered_rows=250
        //   80  → buffer: [150, 100, 80], buffered_rows=330
        //
        // Step 4: push_batch([900])
        //   → batch_size=900 > 300, large batch path
        //   → buffered_rows=330 > 300 → Case 2: flush first
        //   → flush: output [330] (combined [150, 100, 80])
        //   → then bypass: output [900]
        //   Result: [330], [900] - prevents merge into [1230] which would be too large

        // =============================================================================
        // CASE 3: Small buffer + large batch → Normal coalescing (no bypass)
        // =============================================================================
        // When buffer is small enough, we still merge to maintain efficiency
        // Example: target_batch_size=1000, biggest_coalesce_batch_size=Some(500)
        // Input: [300, 1200]
        //
        // Step 1: push_batch([300])
        //   → batch_size=300 <= 500, normal path
        //   → buffer: [300], buffered_rows=300
        //
        // Step 2: push_batch([1200])
        //   → batch_size=1200 > 500, large batch path
        //   → buffered_rows=300 <= 500 → Case 3: normal merge
        //   → buffer: [300, 1200] (1500 total)
        //   → 1500 > target_batch_size → split: output [1000], buffer [500]
        //   Result: [1000], [500] - normal split/merge behavior maintained

        // =============================================================================
        // Comparison: Default vs Optimized Behavior
        // =============================================================================
        // target_batch_size=1000, biggest_coalesce_batch_size=Some(500)
        // Input: [600, 1200, 300]
        //
        // DEFAULT BEHAVIOR (biggest_coalesce_batch_size=None):
        //   600 → buffer: [600]
        //   1200 → buffer: [600, 1200] (1800 rows total)
        //         → split: output [1000 rows], buffer [800 rows remaining]
        //   300 → buffer: [800, 300] (1100 rows total)
        //        → split: output [1000 rows], buffer [100 rows remaining]
        //   Result: [1000], [1000], [100] - all outputs respect target_batch_size
        //
        // OPTIMIZED BEHAVIOR (biggest_coalesce_batch_size=Some(500)):
        //   600 → Case 1: direct bypass → output: [600]
        //   1200 → Case 1: direct bypass → output: [1200]
        //   300 → normal path → buffer: [300]
        //   Result: [600], [1200], [300] - large batches preserved

        // =============================================================================
        // Benefits and Trade-offs
        // =============================================================================
        // Benefits of the optimization:
        // - Large batches stay intact (better for downstream vectorized processing)
        // - Fewer split/merge operations (better CPU performance)
        // - More predictable memory usage patterns
        // - Maintains streaming efficiency while preserving batch boundaries
        //
        // Trade-offs:
        // - Output batch sizes become variable (not always target_batch_size)
        // - May produce smaller partial batches when flushing before large batches
        // - Requires tuning biggest_coalesce_batch_size parameter for optimal performance

        // TODO, for unsorted batches, we may can filter all large batches, and coalesce all
        // small batches together?

        let batch_size = batch.num_rows();

        // Fast path: skip empty batches
        if batch_size == 0 {
            return Ok(());
        }

        // Large batch optimization: bypass coalescing for oversized batches
        if let Some(limit) = self.biggest_coalesce_batch_size {
            if batch_size > limit {
                // Case 1: No buffered data - emit large batch directly
                // Example: [] + [1200] → output [1200], buffer []
                if self.buffered_rows == 0 {
                    self.completed.push_back(batch);
                    return Ok(());
                }

                // Case 2: Buffer too large - flush then emit to avoid oversized merge
                // Example: [850] + [1200] → output [850], then output [1200]
                // This prevents creating batches much larger than both target_batch_size
                // and biggest_coalesce_batch_size, which could cause memory issues
                if self.buffered_rows > limit {
                    self.finish_buffered_batch()?;
                    self.completed.push_back(batch);
                    return Ok(());
                }

                // Case 3: Small buffer - proceed with normal coalescing
                // Example: [300] + [1200] → split and merge normally
                // This ensures small batches still get properly coalesced
                // while allowing some controlled growth beyond the limit
            }
        }

        let (_schema, arrays, mut num_rows) = batch.into_parts();

        // setup input rows
        assert_eq!(arrays.len(), self.in_progress_arrays.len());
        self.in_progress_arrays
            .iter_mut()
            .zip(arrays)
            .for_each(|(in_progress, array)| {
                in_progress.set_source(Some(array));
            });

        // If pushing this batch would exceed the target batch size,
        // finish the current batch and start a new one
        let mut offset = 0;
        while num_rows > (self.target_batch_size - self.buffered_rows) {
            let remaining_rows = self.target_batch_size - self.buffered_rows;
            debug_assert!(remaining_rows > 0);

            // Copy remaining_rows from each array
            for in_progress in self.in_progress_arrays.iter_mut() {
                in_progress.copy_rows(offset, remaining_rows)?;
            }

            self.buffered_rows += remaining_rows;
            offset += remaining_rows;
            num_rows -= remaining_rows;

            self.finish_buffered_batch()?;
        }

        // Add any the remaining rows to the buffer
        self.buffered_rows += num_rows;
        if num_rows > 0 {
            for in_progress in self.in_progress_arrays.iter_mut() {
                in_progress.copy_rows(offset, num_rows)?;
            }
        }

        // If we have reached the target batch size, finalize the buffered batch
        if self.buffered_rows >= self.target_batch_size {
            self.finish_buffered_batch()?;
        }

        // clear in progress sources (to allow the memory to be freed)
        for in_progress in self.in_progress_arrays.iter_mut() {
            in_progress.set_source(None);
        }

        Ok(())
    }

    /// Returns the number of buffered rows
    pub fn get_buffered_rows(&self) -> usize {
        self.buffered_rows
    }

    /// Concatenates any buffered batches into a single `RecordBatch` and
    /// clears any output buffers
    ///
    /// Normally this is called when the input stream is exhausted, and
    /// we want to finalize the last batch of rows.
    ///
    /// See [`Self::next_completed_batch()`] for the completed batches.
    pub fn finish_buffered_batch(&mut self) -> Result<(), ArrowError> {
        if self.buffered_rows == 0 {
            return Ok(());
        }
        let new_arrays = self
            .in_progress_arrays
            .iter_mut()
            .map(|array| array.finish())
            .collect::<Result<Vec<_>, ArrowError>>()?;

        for (array, field) in new_arrays.iter().zip(self.schema.fields().iter()) {
            debug_assert_eq!(array.data_type(), field.data_type());
            debug_assert_eq!(array.len(), self.buffered_rows);
        }

        // SAFETY: each array was created of the correct type and length.
        let batch = unsafe {
            RecordBatch::new_unchecked(Arc::clone(&self.schema), new_arrays, self.buffered_rows)
        };

        self.buffered_rows = 0;
        self.completed.push_back(batch);
        Ok(())
    }

    /// Returns true if there is any buffered data
    pub fn is_empty(&self) -> bool {
        self.buffered_rows == 0 && self.completed.is_empty()
    }

    /// Returns true if there are any completed batches
    pub fn has_completed_batch(&self) -> bool {
        !self.completed.is_empty()
    }

    /// Removes and returns the next completed batch, if any.
    pub fn next_completed_batch(&mut self) -> Option<RecordBatch> {
        self.completed.pop_front()
    }
}

/// Return a new `InProgressArray` for the given data type
fn create_in_progress_array(data_type: &DataType, batch_size: usize) -> Box<dyn InProgressArray> {
    macro_rules! instantiate_primitive {
        ($t:ty) => {
            Box::new(InProgressPrimitiveArray::<$t>::new(
                batch_size,
                data_type.clone(),
            ))
        };
    }

    downcast_primitive! {
        // Instantiate InProgressPrimitiveArray for each primitive type
        data_type => (instantiate_primitive),
        DataType::Utf8View => Box::new(InProgressByteViewArray::<StringViewType>::new(batch_size)),
        DataType::BinaryView => {
            Box::new(InProgressByteViewArray::<BinaryViewType>::new(batch_size))
        }
        _ => Box::new(GenericInProgressArray::new()),
    }
}

/// Incrementally builds up arrays
///
/// [`GenericInProgressArray`] is the default implementation that buffers
/// arrays and uses other kernels concatenates them when finished.
///
/// Some types have specialized implementations for this array types (e.g.,
/// [`StringViewArray`], etc.).
///
/// [`StringViewArray`]: arrow_array::StringViewArray
trait InProgressArray: std::fmt::Debug + Send + Sync {
    /// Set the source array.
    ///
    /// Calls to [`Self::copy_rows`] will copy rows from this array into the
    /// current in-progress array
    fn set_source(&mut self, source: Option<ArrayRef>);

    /// Copy rows from the current source array into the in-progress array
    ///
    /// The source array is set by [`Self::set_source`].
    ///
    /// Return an error if the source array is not set
    fn copy_rows(&mut self, offset: usize, len: usize) -> Result<(), ArrowError>;

    /// Finish the currently in-progress array and return it as an `ArrayRef`
    fn finish(&mut self) -> Result<ArrayRef, ArrowError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::concat::concat_batches;
    use arrow_array::builder::StringViewBuilder;
    use arrow_array::cast::AsArray;
    use arrow_array::{
        BinaryViewArray, Int32Array, Int64Array, RecordBatchOptions, StringArray, StringViewArray,
        TimestampNanosecondArray, UInt32Array, UInt64Array,
    };
    use arrow_schema::{DataType, Field, Schema};
    use rand::{Rng, SeedableRng};
    use std::ops::Range;

    #[test]
    fn test_coalesce() {
        let batch = uint32_batch(0..8);
        Test::new()
            .with_batches(std::iter::repeat_n(batch, 10))
            // expected output is exactly 21 rows (except for the final batch)
            .with_batch_size(21)
            .with_expected_output_sizes(vec![21, 21, 21, 17])
            .run();
    }

    #[test]
    fn test_coalesce_one_by_one() {
        let batch = uint32_batch(0..1); // single row input
        Test::new()
            .with_batches(std::iter::repeat_n(batch, 97))
            // expected output is exactly 20 rows (except for the final batch)
            .with_batch_size(20)
            .with_expected_output_sizes(vec![20, 20, 20, 20, 17])
            .run();
    }

    #[test]
    fn test_coalesce_empty() {
        let schema = Arc::new(Schema::new(vec![Field::new("c0", DataType::UInt32, false)]));

        Test::new()
            .with_batches(vec![])
            .with_schema(schema)
            .with_batch_size(21)
            .with_expected_output_sizes(vec![])
            .run();
    }

    #[test]
    fn test_single_large_batch_greater_than_target() {
        // test a single large batch
        let batch = uint32_batch(0..4096);
        Test::new()
            .with_batch(batch)
            .with_batch_size(1000)
            .with_expected_output_sizes(vec![1000, 1000, 1000, 1000, 96])
            .run();
    }

    #[test]
    fn test_single_large_batch_smaller_than_target() {
        // test a single large batch
        let batch = uint32_batch(0..4096);
        Test::new()
            .with_batch(batch)
            .with_batch_size(8192)
            .with_expected_output_sizes(vec![4096])
            .run();
    }

    #[test]
    fn test_single_large_batch_equal_to_target() {
        // test a single large batch
        let batch = uint32_batch(0..4096);
        Test::new()
            .with_batch(batch)
            .with_batch_size(4096)
            .with_expected_output_sizes(vec![4096])
            .run();
    }

    #[test]
    fn test_single_large_batch_equally_divisible_in_target() {
        // test a single large batch
        let batch = uint32_batch(0..4096);
        Test::new()
            .with_batch(batch)
            .with_batch_size(1024)
            .with_expected_output_sizes(vec![1024, 1024, 1024, 1024])
            .run();
    }

    #[test]
    fn test_empty_schema() {
        let schema = Schema::empty();
        let batch = RecordBatch::new_empty(schema.into());
        Test::new()
            .with_batch(batch)
            .with_expected_output_sizes(vec![])
            .run();
    }

    /// Coalesce multiple batches, 80k rows, with a 0.1% selectivity filter
    #[test]
    fn test_coalesce_filtered_001() {
        let mut filter_builder = RandomFilterBuilder {
            num_rows: 8000,
            selectivity: 0.001,
            seed: 0,
        };

        // add 10 batches of 8000 rows each
        // 80k rows, selecting 0.1% means 80 rows
        // not exactly 80 as the rows are random;
        let mut test = Test::new();
        for _ in 0..10 {
            test = test
                .with_batch(multi_column_batch(0..8000))
                .with_filter(filter_builder.next_filter())
        }
        test.with_batch_size(15)
            .with_expected_output_sizes(vec![15, 15, 15, 13])
            .run();
    }

    /// Coalesce multiple batches, 80k rows, with a 1% selectivity filter
    #[test]
    fn test_coalesce_filtered_01() {
        let mut filter_builder = RandomFilterBuilder {
            num_rows: 8000,
            selectivity: 0.01,
            seed: 0,
        };

        // add 10 batches of 8000 rows each
        // 80k rows, selecting 1% means 800 rows
        // not exactly 800 as the rows are random;
        let mut test = Test::new();
        for _ in 0..10 {
            test = test
                .with_batch(multi_column_batch(0..8000))
                .with_filter(filter_builder.next_filter())
        }
        test.with_batch_size(128)
            .with_expected_output_sizes(vec![128, 128, 128, 128, 128, 128, 15])
            .run();
    }

    /// Coalesce multiple batches, 80k rows, with a 10% selectivity filter
    #[test]
    fn test_coalesce_filtered_1() {
        let mut filter_builder = RandomFilterBuilder {
            num_rows: 8000,
            selectivity: 0.1,
            seed: 0,
        };

        // add 10 batches of 8000 rows each
        // 80k rows, selecting 10% means 8000 rows
        // not exactly 800 as the rows are random;
        let mut test = Test::new();
        for _ in 0..10 {
            test = test
                .with_batch(multi_column_batch(0..8000))
                .with_filter(filter_builder.next_filter())
        }
        test.with_batch_size(1024)
            .with_expected_output_sizes(vec![1024, 1024, 1024, 1024, 1024, 1024, 1024, 840])
            .run();
    }

    /// Coalesce multiple batches, 8k rows, with a 90% selectivity filter
    #[test]
    fn test_coalesce_filtered_90() {
        let mut filter_builder = RandomFilterBuilder {
            num_rows: 800,
            selectivity: 0.90,
            seed: 0,
        };

        // add 10 batches of 800 rows each
        // 8k rows, selecting 99% means 7200 rows
        // not exactly 7200 as the rows are random;
        let mut test = Test::new();
        for _ in 0..10 {
            test = test
                .with_batch(multi_column_batch(0..800))
                .with_filter(filter_builder.next_filter())
        }
        test.with_batch_size(1024)
            .with_expected_output_sizes(vec![1024, 1024, 1024, 1024, 1024, 1024, 1024, 13])
            .run();
    }

    #[test]
    fn test_coalesce_non_null() {
        Test::new()
            // 4040 rows of unit32
            .with_batch(uint32_batch_non_null(0..3000))
            .with_batch(uint32_batch_non_null(0..1040))
            .with_batch_size(1024)
            .with_expected_output_sizes(vec![1024, 1024, 1024, 968])
            .run();
    }
    #[test]
    fn test_utf8_split() {
        Test::new()
            // 4040 rows of utf8 strings in total, split into batches of 1024
            .with_batch(utf8_batch(0..3000))
            .with_batch(utf8_batch(0..1040))
            .with_batch_size(1024)
            .with_expected_output_sizes(vec![1024, 1024, 1024, 968])
            .run();
    }

    #[test]
    fn test_string_view_no_views() {
        let output_batches = Test::new()
            // both input batches have no views, so no need to compact
            .with_batch(stringview_batch([Some("foo"), Some("bar")]))
            .with_batch(stringview_batch([Some("baz"), Some("qux")]))
            .with_expected_output_sizes(vec![4])
            .run();

        expect_buffer_layout(
            col_as_string_view("c0", output_batches.first().unwrap()),
            vec![],
        );
    }

    #[test]
    fn test_string_view_batch_small_no_compact() {
        // view with only short strings (no buffers) --> no need to compact
        let batch = stringview_batch_repeated(1000, [Some("a"), Some("b"), Some("c")]);
        let output_batches = Test::new()
            .with_batch(batch.clone())
            .with_expected_output_sizes(vec![1000])
            .run();

        let array = col_as_string_view("c0", &batch);
        let gc_array = col_as_string_view("c0", output_batches.first().unwrap());
        assert_eq!(array.data_buffers().len(), 0);
        assert_eq!(array.data_buffers().len(), gc_array.data_buffers().len()); // no compaction

        expect_buffer_layout(gc_array, vec![]);
    }

    #[test]
    fn test_string_view_batch_large_no_compact() {
        // view with large strings (has buffers) but full --> no need to compact
        let batch = stringview_batch_repeated(1000, [Some("This string is longer than 12 bytes")]);
        let output_batches = Test::new()
            .with_batch(batch.clone())
            .with_batch_size(1000)
            .with_expected_output_sizes(vec![1000])
            .run();

        let array = col_as_string_view("c0", &batch);
        let gc_array = col_as_string_view("c0", output_batches.first().unwrap());
        assert_eq!(array.data_buffers().len(), 5);
        assert_eq!(array.data_buffers().len(), gc_array.data_buffers().len()); // no compaction

        expect_buffer_layout(
            gc_array,
            vec![
                ExpectedLayout {
                    len: 8190,
                    capacity: 8192,
                },
                ExpectedLayout {
                    len: 8190,
                    capacity: 8192,
                },
                ExpectedLayout {
                    len: 8190,
                    capacity: 8192,
                },
                ExpectedLayout {
                    len: 8190,
                    capacity: 8192,
                },
                ExpectedLayout {
                    len: 2240,
                    capacity: 8192,
                },
            ],
        );
    }

    #[test]
    fn test_string_view_batch_small_with_buffers_no_compact() {
        // view with buffers but only short views
        let short_strings = std::iter::repeat(Some("SmallString"));
        let long_strings = std::iter::once(Some("This string is longer than 12 bytes"));
        // 20 short strings, then a long ones
        let values = short_strings.take(20).chain(long_strings);
        let batch = stringview_batch_repeated(1000, values)
            // take only 10 short strings (no long ones)
            .slice(5, 10);
        let output_batches = Test::new()
            .with_batch(batch.clone())
            .with_batch_size(1000)
            .with_expected_output_sizes(vec![10])
            .run();

        let array = col_as_string_view("c0", &batch);
        let gc_array = col_as_string_view("c0", output_batches.first().unwrap());
        assert_eq!(array.data_buffers().len(), 1); // input has one buffer
        assert_eq!(gc_array.data_buffers().len(), 0); // output has no buffers as only short strings
    }

    #[test]
    fn test_string_view_batch_large_slice_compact() {
        // view with large strings (has buffers) and only partially used  --> no need to compact
        let batch = stringview_batch_repeated(1000, [Some("This string is longer than 12 bytes")])
            // slice only 22 rows, so most of the buffer is not used
            .slice(11, 22);

        let output_batches = Test::new()
            .with_batch(batch.clone())
            .with_batch_size(1000)
            .with_expected_output_sizes(vec![22])
            .run();

        let array = col_as_string_view("c0", &batch);
        let gc_array = col_as_string_view("c0", output_batches.first().unwrap());
        assert_eq!(array.data_buffers().len(), 5);

        expect_buffer_layout(
            gc_array,
            vec![ExpectedLayout {
                len: 770,
                capacity: 8192,
            }],
        );
    }

    #[test]
    fn test_string_view_mixed() {
        let large_view_batch =
            stringview_batch_repeated(1000, [Some("This string is longer than 12 bytes")]);
        let small_view_batch = stringview_batch_repeated(1000, [Some("SmallString")]);
        let mixed_batch = stringview_batch_repeated(
            1000,
            [Some("This string is longer than 12 bytes"), Some("Small")],
        );
        let mixed_batch_nulls = stringview_batch_repeated(
            1000,
            [
                Some("This string is longer than 12 bytes"),
                Some("Small"),
                None,
            ],
        );

        // Several batches with mixed inline / non inline
        // 4k rows in
        let output_batches = Test::new()
            .with_batch(large_view_batch.clone())
            .with_batch(small_view_batch)
            // this batch needs to be compacted (less than 1/2 full)
            .with_batch(large_view_batch.slice(10, 20))
            .with_batch(mixed_batch_nulls)
            // this batch needs to be compacted (less than 1/2 full)
            .with_batch(large_view_batch.slice(10, 20))
            .with_batch(mixed_batch)
            .with_expected_output_sizes(vec![1024, 1024, 1024, 968])
            .run();

        expect_buffer_layout(
            col_as_string_view("c0", output_batches.first().unwrap()),
            vec![
                ExpectedLayout {
                    len: 8190,
                    capacity: 8192,
                },
                ExpectedLayout {
                    len: 8190,
                    capacity: 8192,
                },
                ExpectedLayout {
                    len: 8190,
                    capacity: 8192,
                },
                ExpectedLayout {
                    len: 8190,
                    capacity: 8192,
                },
                ExpectedLayout {
                    len: 2240,
                    capacity: 8192,
                },
            ],
        );
    }

    #[test]
    fn test_string_view_many_small_compact() {
        // 200 rows alternating long (28) and short (≤12) strings.
        // Only the 100 long strings go into data buffers: 100 × 28 = 2800.
        let batch = stringview_batch_repeated(
            200,
            [Some("This string is 28 bytes long"), Some("small string")],
        );
        let output_batches = Test::new()
            // First allocated buffer is 8kb.
            // Appending 10 batches of 2800 bytes will use 2800 * 10 = 14kb (8kb, an 16kb and 32kbkb)
            .with_batch(batch.clone())
            .with_batch(batch.clone())
            .with_batch(batch.clone())
            .with_batch(batch.clone())
            .with_batch(batch.clone())
            .with_batch(batch.clone())
            .with_batch(batch.clone())
            .with_batch(batch.clone())
            .with_batch(batch.clone())
            .with_batch(batch.clone())
            .with_batch_size(8000)
            .with_expected_output_sizes(vec![2000]) // only 1000 rows total
            .run();

        // expect a nice even distribution of buffers
        expect_buffer_layout(
            col_as_string_view("c0", output_batches.first().unwrap()),
            vec![
                ExpectedLayout {
                    len: 8176,
                    capacity: 8192,
                },
                ExpectedLayout {
                    len: 16380,
                    capacity: 16384,
                },
                ExpectedLayout {
                    len: 3444,
                    capacity: 32768,
                },
            ],
        );
    }

    #[test]
    fn test_string_view_many_small_boundary() {
        // The strings are designed to exactly fit into buffers that are powers of 2 long
        let batch = stringview_batch_repeated(100, [Some("This string is a power of two=32")]);
        let output_batches = Test::new()
            .with_batches(std::iter::repeat_n(batch, 20))
            .with_batch_size(900)
            .with_expected_output_sizes(vec![900, 900, 200])
            .run();

        // expect each buffer to be entirely full except the last one
        expect_buffer_layout(
            col_as_string_view("c0", output_batches.first().unwrap()),
            vec![
                ExpectedLayout {
                    len: 8192,
                    capacity: 8192,
                },
                ExpectedLayout {
                    len: 16384,
                    capacity: 16384,
                },
                ExpectedLayout {
                    len: 4224,
                    capacity: 32768,
                },
            ],
        );
    }

    #[test]
    fn test_string_view_large_small() {
        // The strings are 37 bytes long, so each batch has 100 * 28 = 2800 bytes
        let mixed_batch = stringview_batch_repeated(
            200,
            [Some("This string is 28 bytes long"), Some("small string")],
        );
        // These strings aren't copied, this array has an 8k buffer
        let all_large = stringview_batch_repeated(
            50,
            [Some(
                "This buffer has only large strings in it so there are no buffer copies",
            )],
        );

        let output_batches = Test::new()
            // First allocated buffer is 8kb.
            // Appending five batches of 2800 bytes will use 2800 * 10 = 28kb (8kb, an 16kb and 32kbkb)
            .with_batch(mixed_batch.clone())
            .with_batch(mixed_batch.clone())
            .with_batch(all_large.clone())
            .with_batch(mixed_batch.clone())
            .with_batch(all_large.clone())
            .with_batch(mixed_batch.clone())
            .with_batch(mixed_batch.clone())
            .with_batch(all_large.clone())
            .with_batch(mixed_batch.clone())
            .with_batch(all_large.clone())
            .with_batch_size(8000)
            .with_expected_output_sizes(vec![1400])
            .run();

        expect_buffer_layout(
            col_as_string_view("c0", output_batches.first().unwrap()),
            vec![
                ExpectedLayout {
                    len: 8190,
                    capacity: 8192,
                },
                ExpectedLayout {
                    len: 16366,
                    capacity: 16384,
                },
                ExpectedLayout {
                    len: 6244,
                    capacity: 32768,
                },
            ],
        );
    }

    #[test]
    fn test_binary_view() {
        let values: Vec<Option<&[u8]>> = vec![
            Some(b"foo"),
            None,
            Some(b"A longer string that is more than 12 bytes"),
        ];

        let binary_view =
            BinaryViewArray::from_iter(std::iter::repeat(values.iter()).flatten().take(1000));
        let batch =
            RecordBatch::try_from_iter(vec![("c0", Arc::new(binary_view) as ArrayRef)]).unwrap();

        Test::new()
            .with_batch(batch.clone())
            .with_batch(batch.clone())
            .with_batch_size(512)
            .with_expected_output_sizes(vec![512, 512, 512, 464])
            .run();
    }

    #[derive(Debug, Clone, PartialEq)]
    struct ExpectedLayout {
        len: usize,
        capacity: usize,
    }

    /// Asserts that the buffer layout of the specified StringViewArray matches the expected layout
    fn expect_buffer_layout(array: &StringViewArray, expected: Vec<ExpectedLayout>) {
        let actual = array
            .data_buffers()
            .iter()
            .map(|b| ExpectedLayout {
                len: b.len(),
                capacity: b.capacity(),
            })
            .collect::<Vec<_>>();

        assert_eq!(
            actual, expected,
            "Expected buffer layout {expected:#?} but got {actual:#?}"
        );
    }

    /// Test for [`BatchCoalescer`]
    ///
    /// Pushes the input batches to the coalescer and verifies that the resulting
    /// batches have the expected number of rows and contents.
    #[derive(Debug, Clone)]
    struct Test {
        /// Batches to feed to the coalescer.
        input_batches: Vec<RecordBatch>,
        /// Filters to apply to the corresponding input batches.
        ///
        /// If there are no filters for the input batches, the batch will be
        /// pushed as is.
        filters: Vec<BooleanArray>,
        /// The schema. If not provided, the first batch's schema is used.
        schema: Option<SchemaRef>,
        /// Expected output sizes of the resulting batches
        expected_output_sizes: Vec<usize>,
        /// target batch size (default to 1024)
        target_batch_size: usize,
    }

    impl Default for Test {
        fn default() -> Self {
            Self {
                input_batches: vec![],
                filters: vec![],
                schema: None,
                expected_output_sizes: vec![],
                target_batch_size: 1024,
            }
        }
    }

    impl Test {
        fn new() -> Self {
            Self::default()
        }

        /// Set the target batch size
        fn with_batch_size(mut self, target_batch_size: usize) -> Self {
            self.target_batch_size = target_batch_size;
            self
        }

        /// Extend the input batches with `batch`
        fn with_batch(mut self, batch: RecordBatch) -> Self {
            self.input_batches.push(batch);
            self
        }

        /// Extend the filters with `filter`
        fn with_filter(mut self, filter: BooleanArray) -> Self {
            self.filters.push(filter);
            self
        }

        /// Extends the input batches with `batches`
        fn with_batches(mut self, batches: impl IntoIterator<Item = RecordBatch>) -> Self {
            self.input_batches.extend(batches);
            self
        }

        /// Specifies the schema for the test
        fn with_schema(mut self, schema: SchemaRef) -> Self {
            self.schema = Some(schema);
            self
        }

        /// Extends `sizes` to expected output sizes
        fn with_expected_output_sizes(mut self, sizes: impl IntoIterator<Item = usize>) -> Self {
            self.expected_output_sizes.extend(sizes);
            self
        }

        /// Runs the test -- see documentation on [`Test`] for details
        ///
        /// Returns the resulting output batches
        fn run(self) -> Vec<RecordBatch> {
            let expected_output = self.expected_output();
            let schema = self.schema();

            let Self {
                input_batches,
                filters,
                schema: _,
                target_batch_size,
                expected_output_sizes,
            } = self;

            let had_input = input_batches.iter().any(|b| b.num_rows() > 0);

            let mut coalescer = BatchCoalescer::new(Arc::clone(&schema), target_batch_size);

            // feed input batches and filters to the coalescer
            let mut filters = filters.into_iter();
            for batch in input_batches {
                if let Some(filter) = filters.next() {
                    coalescer.push_batch_with_filter(batch, &filter).unwrap();
                } else {
                    coalescer.push_batch(batch).unwrap();
                }
            }
            assert_eq!(schema, coalescer.schema());

            if had_input {
                assert!(!coalescer.is_empty(), "Coalescer should not be empty");
            } else {
                assert!(coalescer.is_empty(), "Coalescer should be empty");
            }

            coalescer.finish_buffered_batch().unwrap();
            if had_input {
                assert!(
                    coalescer.has_completed_batch(),
                    "Coalescer should have completed batches"
                );
            }

            let mut output_batches = vec![];
            while let Some(batch) = coalescer.next_completed_batch() {
                output_batches.push(batch);
            }

            // make sure we got the expected number of output batches and content
            let mut starting_idx = 0;
            let actual_output_sizes: Vec<usize> =
                output_batches.iter().map(|b| b.num_rows()).collect();
            assert_eq!(
                expected_output_sizes, actual_output_sizes,
                "Unexpected number of rows in output batches\n\
                Expected\n{expected_output_sizes:#?}\nActual:{actual_output_sizes:#?}"
            );
            let iter = expected_output_sizes
                .iter()
                .zip(output_batches.iter())
                .enumerate();

            for (i, (expected_size, batch)) in iter {
                // compare the contents of the batch after normalization (using
                // `==` compares the underlying memory layout too)
                let expected_batch = expected_output.slice(starting_idx, *expected_size);
                let expected_batch = normalize_batch(expected_batch);
                let batch = normalize_batch(batch.clone());
                assert_eq!(
                    expected_batch, batch,
                    "Unexpected content in batch {i}:\
                    \n\nExpected:\n{expected_batch:#?}\n\nActual:\n{batch:#?}"
                );
                starting_idx += *expected_size;
            }
            output_batches
        }

        /// Return the expected output schema. If not overridden by `with_schema`, it
        /// returns the schema of the first input batch.
        fn schema(&self) -> SchemaRef {
            self.schema
                .clone()
                .unwrap_or_else(|| Arc::clone(&self.input_batches[0].schema()))
        }

        /// Returns the expected output as a single `RecordBatch`
        fn expected_output(&self) -> RecordBatch {
            let schema = self.schema();
            if self.filters.is_empty() {
                return concat_batches(&schema, &self.input_batches).unwrap();
            }

            let mut filters = self.filters.iter();
            let filtered_batches = self
                .input_batches
                .iter()
                .map(|batch| {
                    if let Some(filter) = filters.next() {
                        filter_record_batch(batch, filter).unwrap()
                    } else {
                        batch.clone()
                    }
                })
                .collect::<Vec<_>>();
            concat_batches(&schema, &filtered_batches).unwrap()
        }
    }

    /// Return a RecordBatch with a UInt32Array with the specified range and
    /// every third value is null.
    fn uint32_batch<T: std::iter::Iterator<Item = u32>>(range: T) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("c0", DataType::UInt32, true)]));

        let array = UInt32Array::from_iter(range.map(|i| if i % 3 == 0 { None } else { Some(i) }));
        RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(array)]).unwrap()
    }

    /// Return a RecordBatch with a UInt32Array with no nulls specified range
    fn uint32_batch_non_null<T: std::iter::Iterator<Item = u32>>(range: T) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("c0", DataType::UInt32, false)]));

        let array = UInt32Array::from_iter_values(range);
        RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(array)]).unwrap()
    }

    /// Return a RecordBatch with a UInt64Array with no nulls specified range
    fn uint64_batch_non_null<T: std::iter::Iterator<Item = u64>>(range: T) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("c0", DataType::UInt64, false)]));

        let array = UInt64Array::from_iter_values(range);
        RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(array)]).unwrap()
    }

    /// Return a RecordBatch with a StringArrary with values `value0`, `value1`, ...
    /// and every third value is `None`.
    fn utf8_batch(range: Range<u32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("c0", DataType::Utf8, true)]));

        let array = StringArray::from_iter(range.map(|i| {
            if i % 3 == 0 {
                None
            } else {
                Some(format!("value{i}"))
            }
        }));

        RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(array)]).unwrap()
    }

    /// Return a RecordBatch with a StringViewArray with (only) the specified values
    fn stringview_batch<'a>(values: impl IntoIterator<Item = Option<&'a str>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "c0",
            DataType::Utf8View,
            false,
        )]));

        let array = StringViewArray::from_iter(values);
        RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(array)]).unwrap()
    }

    /// Return a RecordBatch with a StringViewArray with num_rows by repeating
    /// values over and over.
    fn stringview_batch_repeated<'a>(
        num_rows: usize,
        values: impl IntoIterator<Item = Option<&'a str>>,
    ) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "c0",
            DataType::Utf8View,
            true,
        )]));

        // Repeat the values to a total of num_rows
        let values: Vec<_> = values.into_iter().collect();
        let values_iter = std::iter::repeat(values.iter())
            .flatten()
            .cloned()
            .take(num_rows);

        let mut builder = StringViewBuilder::with_capacity(100).with_fixed_block_size(8192);
        for val in values_iter {
            builder.append_option(val);
        }

        let array = builder.finish();
        RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(array)]).unwrap()
    }

    /// Return a RecordBatch of 100 rows
    fn multi_column_batch(range: Range<i32>) -> RecordBatch {
        let int64_array = Int64Array::from_iter(
            range
                .clone()
                .map(|v| if v % 5 == 0 { None } else { Some(v as i64) }),
        );
        let string_view_array = StringViewArray::from_iter(range.clone().map(|v| {
            if v % 5 == 0 {
                None
            } else if v % 7 == 0 {
                Some(format!("This is a string longer than 12 bytes{v}"))
            } else {
                Some(format!("Short {v}"))
            }
        }));
        let string_array = StringArray::from_iter(range.clone().map(|v| {
            if v % 11 == 0 {
                None
            } else {
                Some(format!("Value {v}"))
            }
        }));
        let timestamp_array = TimestampNanosecondArray::from_iter(range.map(|v| {
            if v % 3 == 0 {
                None
            } else {
                Some(v as i64 * 1000) // simulate a timestamp in milliseconds
            }
        }))
        .with_timezone("America/New_York");

        RecordBatch::try_from_iter(vec![
            ("int64", Arc::new(int64_array) as ArrayRef),
            ("stringview", Arc::new(string_view_array) as ArrayRef),
            ("string", Arc::new(string_array) as ArrayRef),
            ("timestamp", Arc::new(timestamp_array) as ArrayRef),
        ])
        .unwrap()
    }

    /// Return a boolean array that filters out randomly selected rows
    /// from the input batch with a `selectivity`.
    ///
    /// For example a `selectivity` of 0.1 will filter out
    /// 90% of the rows.
    #[derive(Debug)]
    struct RandomFilterBuilder {
        num_rows: usize,
        selectivity: f64,
        /// seed for random number generator, increases by one each time
        /// `next_filter` is called
        seed: u64,
    }
    impl RandomFilterBuilder {
        /// Build the next filter with the current seed and increment the seed
        /// by one.
        fn next_filter(&mut self) -> BooleanArray {
            assert!(self.selectivity >= 0.0 && self.selectivity <= 1.0);
            let mut rng = rand::rngs::StdRng::seed_from_u64(self.seed);
            self.seed += 1;
            BooleanArray::from_iter(
                (0..self.num_rows)
                    .map(|_| rng.random_bool(self.selectivity))
                    .map(Some),
            )
        }
    }

    /// Returns the named column as a StringViewArray
    fn col_as_string_view<'b>(name: &str, batch: &'b RecordBatch) -> &'b StringViewArray {
        batch
            .column_by_name(name)
            .expect("column not found")
            .as_string_view_opt()
            .expect("column is not a string view")
    }

    /// Normalize the `RecordBatch` so that the memory layout is consistent
    /// (e.g. StringArray is compacted).
    fn normalize_batch(batch: RecordBatch) -> RecordBatch {
        // Only need to normalize StringViews (as == also tests for memory layout)
        let (schema, mut columns, row_count) = batch.into_parts();

        for column in columns.iter_mut() {
            let Some(string_view) = column.as_string_view_opt() else {
                continue;
            };

            // Re-create the StringViewArray to ensure memory layout is
            // consistent
            let mut builder = StringViewBuilder::new();
            for s in string_view.iter() {
                builder.append_option(s);
            }
            // Update the column with the new StringViewArray
            *column = Arc::new(builder.finish());
        }

        let options = RecordBatchOptions::new().with_row_count(Some(row_count));
        RecordBatch::try_new_with_options(schema, columns, &options).unwrap()
    }

    /// Helper function to create a test batch with specified number of rows
    fn create_test_batch(num_rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("c0", DataType::Int32, false)]));
        let array = Int32Array::from_iter_values(0..num_rows as i32);
        RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
    }
    #[test]
    fn test_biggest_coalesce_batch_size_none_default() {
        // Test that default behavior (None) coalesces all batches
        let mut coalescer = BatchCoalescer::new(
            Arc::new(Schema::new(vec![Field::new("c0", DataType::Int32, false)])),
            100,
        );

        // Push a large batch (1000 rows) - should be coalesced normally
        let large_batch = create_test_batch(1000);
        coalescer.push_batch(large_batch).unwrap();

        // Should produce multiple batches of target size (100)
        let mut output_batches = vec![];
        while let Some(batch) = coalescer.next_completed_batch() {
            output_batches.push(batch);
        }

        coalescer.finish_buffered_batch().unwrap();
        while let Some(batch) = coalescer.next_completed_batch() {
            output_batches.push(batch);
        }

        // Should have 10 batches of 100 rows each
        assert_eq!(output_batches.len(), 10);
        for batch in output_batches {
            assert_eq!(batch.num_rows(), 100);
        }
    }

    #[test]
    fn test_biggest_coalesce_batch_size_bypass_large_batch() {
        // Test that batches larger than biggest_coalesce_batch_size bypass coalescing
        let mut coalescer = BatchCoalescer::new(
            Arc::new(Schema::new(vec![Field::new("c0", DataType::Int32, false)])),
            100,
        );
        coalescer.set_biggest_coalesce_batch_size(Some(500));

        // Push a large batch (1000 rows) - should bypass coalescing
        let large_batch = create_test_batch(1000);
        coalescer.push_batch(large_batch.clone()).unwrap();

        // Should have one completed batch immediately (the original large batch)
        assert!(coalescer.has_completed_batch());
        let output_batch = coalescer.next_completed_batch().unwrap();
        assert_eq!(output_batch.num_rows(), 1000);

        // Should be no more completed batches
        assert!(!coalescer.has_completed_batch());
        assert_eq!(coalescer.get_buffered_rows(), 0);
    }

    #[test]
    fn test_biggest_coalesce_batch_size_coalesce_small_batch() {
        // Test that batches smaller than biggest_coalesce_batch_size are coalesced normally
        let mut coalescer = BatchCoalescer::new(
            Arc::new(Schema::new(vec![Field::new("c0", DataType::Int32, false)])),
            100,
        );
        coalescer.set_biggest_coalesce_batch_size(Some(500));

        // Push small batches that should be coalesced
        let small_batch = create_test_batch(50);
        coalescer.push_batch(small_batch.clone()).unwrap();

        // Should not have completed batch yet (only 50 rows, target is 100)
        assert!(!coalescer.has_completed_batch());
        assert_eq!(coalescer.get_buffered_rows(), 50);

        // Push another small batch
        coalescer.push_batch(small_batch).unwrap();

        // Now should have a completed batch (100 rows total)
        assert!(coalescer.has_completed_batch());
        let output_batch = coalescer.next_completed_batch().unwrap();
        assert_eq!(output_batch.num_rows(), 100);

        assert_eq!(coalescer.get_buffered_rows(), 0);
    }

    #[test]
    fn test_biggest_coalesce_batch_size_equal_boundary() {
        // Test behavior when batch size equals biggest_coalesce_batch_size
        let mut coalescer = BatchCoalescer::new(
            Arc::new(Schema::new(vec![Field::new("c0", DataType::Int32, false)])),
            100,
        );
        coalescer.set_biggest_coalesce_batch_size(Some(500));

        // Push a batch exactly equal to the limit
        let boundary_batch = create_test_batch(500);
        coalescer.push_batch(boundary_batch).unwrap();

        // Should be coalesced (not bypass) since it's equal, not greater
        let mut output_count = 0;
        while coalescer.next_completed_batch().is_some() {
            output_count += 1;
        }

        coalescer.finish_buffered_batch().unwrap();
        while coalescer.next_completed_batch().is_some() {
            output_count += 1;
        }

        // Should have 5 batches of 100 rows each
        assert_eq!(output_count, 5);
    }

    #[test]
    fn test_biggest_coalesce_batch_size_first_large_then_consecutive_bypass() {
        // Test the new consecutive large batch bypass behavior
        // Pattern: small batches -> first large batch (coalesced) -> consecutive large batches (bypass)
        let mut coalescer = BatchCoalescer::new(
            Arc::new(Schema::new(vec![Field::new("c0", DataType::Int32, false)])),
            100,
        );
        coalescer.set_biggest_coalesce_batch_size(Some(200));

        let small_batch = create_test_batch(50);

        // Push small batch first to create buffered data
        coalescer.push_batch(small_batch).unwrap();
        assert_eq!(coalescer.get_buffered_rows(), 50);
        assert!(!coalescer.has_completed_batch());

        // Push first large batch - should go through normal coalescing due to buffered data
        let large_batch1 = create_test_batch(250);
        coalescer.push_batch(large_batch1).unwrap();

        // 50 + 250 = 300 -> 3 complete batches of 100, 0 rows buffered
        let mut completed_batches = vec![];
        while let Some(batch) = coalescer.next_completed_batch() {
            completed_batches.push(batch);
        }
        assert_eq!(completed_batches.len(), 3);
        assert_eq!(coalescer.get_buffered_rows(), 0);

        // Now push consecutive large batches - they should bypass
        let large_batch2 = create_test_batch(300);
        let large_batch3 = create_test_batch(400);

        // Push second large batch - should bypass since it's consecutive and buffer is empty
        coalescer.push_batch(large_batch2).unwrap();
        assert!(coalescer.has_completed_batch());
        let output = coalescer.next_completed_batch().unwrap();
        assert_eq!(output.num_rows(), 300); // bypassed with original size
        assert_eq!(coalescer.get_buffered_rows(), 0);

        // Push third large batch - should also bypass
        coalescer.push_batch(large_batch3).unwrap();
        assert!(coalescer.has_completed_batch());
        let output = coalescer.next_completed_batch().unwrap();
        assert_eq!(output.num_rows(), 400); // bypassed with original size
        assert_eq!(coalescer.get_buffered_rows(), 0);
    }

    #[test]
    fn test_biggest_coalesce_batch_size_empty_batch() {
        // Test that empty batches don't trigger the bypass logic
        let mut coalescer = BatchCoalescer::new(
            Arc::new(Schema::new(vec![Field::new("c0", DataType::Int32, false)])),
            100,
        );
        coalescer.set_biggest_coalesce_batch_size(Some(50));

        let empty_batch = create_test_batch(0);
        coalescer.push_batch(empty_batch).unwrap();

        // Empty batch should be handled normally (no effect)
        assert!(!coalescer.has_completed_batch());
        assert_eq!(coalescer.get_buffered_rows(), 0);
    }

    #[test]
    fn test_biggest_coalesce_batch_size_with_buffered_data_no_bypass() {
        // Test that when there is buffered data, large batches do NOT bypass (unless consecutive)
        let mut coalescer = BatchCoalescer::new(
            Arc::new(Schema::new(vec![Field::new("c0", DataType::Int32, false)])),
            100,
        );
        coalescer.set_biggest_coalesce_batch_size(Some(200));

        // Add some buffered data first
        let small_batch = create_test_batch(30);
        coalescer.push_batch(small_batch.clone()).unwrap();
        coalescer.push_batch(small_batch).unwrap();
        assert_eq!(coalescer.get_buffered_rows(), 60);

        // Push large batch that would normally bypass, but shouldn't because buffered_rows > 0
        let large_batch = create_test_batch(250);
        coalescer.push_batch(large_batch).unwrap();

        // The large batch should be processed through normal coalescing logic
        // Total: 60 (buffered) + 250 (new) = 310 rows
        // Output: 3 complete batches of 100 rows each, 10 rows remain buffered

        let mut completed_batches = vec![];
        while let Some(batch) = coalescer.next_completed_batch() {
            completed_batches.push(batch);
        }

        assert_eq!(completed_batches.len(), 3);
        for batch in &completed_batches {
            assert_eq!(batch.num_rows(), 100);
        }
        assert_eq!(coalescer.get_buffered_rows(), 10);
    }

    #[test]
    fn test_biggest_coalesce_batch_size_zero_limit() {
        // Test edge case where limit is 0 (all batches bypass when no buffered data)
        let mut coalescer = BatchCoalescer::new(
            Arc::new(Schema::new(vec![Field::new("c0", DataType::Int32, false)])),
            100,
        );
        coalescer.set_biggest_coalesce_batch_size(Some(0));

        // Even a 1-row batch should bypass when there's no buffered data
        let tiny_batch = create_test_batch(1);
        coalescer.push_batch(tiny_batch).unwrap();

        assert!(coalescer.has_completed_batch());
        let output = coalescer.next_completed_batch().unwrap();
        assert_eq!(output.num_rows(), 1);
    }

    #[test]
    fn test_biggest_coalesce_batch_size_bypass_only_when_no_buffer() {
        // Test that bypass only occurs when buffered_rows == 0
        let mut coalescer = BatchCoalescer::new(
            Arc::new(Schema::new(vec![Field::new("c0", DataType::Int32, false)])),
            100,
        );
        coalescer.set_biggest_coalesce_batch_size(Some(200));

        // First, push a large batch with no buffered data - should bypass
        let large_batch = create_test_batch(300);
        coalescer.push_batch(large_batch.clone()).unwrap();

        assert!(coalescer.has_completed_batch());
        let output = coalescer.next_completed_batch().unwrap();
        assert_eq!(output.num_rows(), 300); // bypassed
        assert_eq!(coalescer.get_buffered_rows(), 0);

        // Now add some buffered data
        let small_batch = create_test_batch(50);
        coalescer.push_batch(small_batch).unwrap();
        assert_eq!(coalescer.get_buffered_rows(), 50);

        // Push the same large batch again - should NOT bypass this time (not consecutive)
        coalescer.push_batch(large_batch).unwrap();

        // Should process through normal coalescing: 50 + 300 = 350 rows
        // Output: 3 complete batches of 100 rows, 50 rows buffered
        let mut completed_batches = vec![];
        while let Some(batch) = coalescer.next_completed_batch() {
            completed_batches.push(batch);
        }

        assert_eq!(completed_batches.len(), 3);
        for batch in &completed_batches {
            assert_eq!(batch.num_rows(), 100);
        }
        assert_eq!(coalescer.get_buffered_rows(), 50);
    }

    #[test]
    fn test_biggest_coalesce_batch_size_consecutive_large_batches_scenario() {
        // Test your exact scenario: 20, 20, 30, 700, 600, 700, 900, 700, 600
        let mut coalescer = BatchCoalescer::new(
            Arc::new(Schema::new(vec![Field::new("c0", DataType::Int32, false)])),
            1000,
        );
        coalescer.set_biggest_coalesce_batch_size(Some(500));

        // Push small batches first
        coalescer.push_batch(create_test_batch(20)).unwrap();
        coalescer.push_batch(create_test_batch(20)).unwrap();
        coalescer.push_batch(create_test_batch(30)).unwrap();

        assert_eq!(coalescer.get_buffered_rows(), 70);
        assert!(!coalescer.has_completed_batch());

        // Push first large batch (700) - should coalesce due to buffered data
        coalescer.push_batch(create_test_batch(700)).unwrap();

        // 70 + 700 = 770 rows, not enough for 1000, so all stay buffered
        assert_eq!(coalescer.get_buffered_rows(), 770);
        assert!(!coalescer.has_completed_batch());

        // Push second large batch (600) - should bypass since previous was large
        coalescer.push_batch(create_test_batch(600)).unwrap();

        // Should flush buffer (770 rows) and bypass the 600
        let mut outputs = vec![];
        while let Some(batch) = coalescer.next_completed_batch() {
            outputs.push(batch);
        }
        assert_eq!(outputs.len(), 2); // one flushed buffer batch (770) + one bypassed (600)
        assert_eq!(outputs[0].num_rows(), 770);
        assert_eq!(outputs[1].num_rows(), 600);
        assert_eq!(coalescer.get_buffered_rows(), 0);

        // Push remaining large batches - should all bypass
        let remaining_batches = [700, 900, 700, 600];
        for &size in &remaining_batches {
            coalescer.push_batch(create_test_batch(size)).unwrap();

            assert!(coalescer.has_completed_batch());
            let output = coalescer.next_completed_batch().unwrap();
            assert_eq!(output.num_rows(), size);
            assert_eq!(coalescer.get_buffered_rows(), 0);
        }
    }

    #[test]
    fn test_biggest_coalesce_batch_size_truly_consecutive_large_bypass() {
        // Test truly consecutive large batches that should all bypass
        // This test ensures buffer is completely empty between large batches
        let mut coalescer = BatchCoalescer::new(
            Arc::new(Schema::new(vec![Field::new("c0", DataType::Int32, false)])),
            100,
        );
        coalescer.set_biggest_coalesce_batch_size(Some(200));

        // Push consecutive large batches with no prior buffered data
        let large_batches = vec![
            create_test_batch(300),
            create_test_batch(400),
            create_test_batch(350),
            create_test_batch(500),
        ];

        let mut all_outputs = vec![];

        for (i, large_batch) in large_batches.into_iter().enumerate() {
            let expected_size = large_batch.num_rows();

            // Buffer should be empty before each large batch
            assert_eq!(
                coalescer.get_buffered_rows(),
                0,
                "Buffer should be empty before batch {}",
                i
            );

            coalescer.push_batch(large_batch).unwrap();

            // Each large batch should bypass and produce exactly one output batch
            assert!(
                coalescer.has_completed_batch(),
                "Should have completed batch after pushing batch {}",
                i
            );

            let output = coalescer.next_completed_batch().unwrap();
            assert_eq!(
                output.num_rows(),
                expected_size,
                "Batch {} should have bypassed with original size",
                i
            );

            // Should be no more batches and buffer should be empty
            assert!(
                !coalescer.has_completed_batch(),
                "Should have no more completed batches after batch {}",
                i
            );
            assert_eq!(
                coalescer.get_buffered_rows(),
                0,
                "Buffer should be empty after batch {}",
                i
            );

            all_outputs.push(output);
        }

        // Verify we got exactly 4 output batches with original sizes
        assert_eq!(all_outputs.len(), 4);
        assert_eq!(all_outputs[0].num_rows(), 300);
        assert_eq!(all_outputs[1].num_rows(), 400);
        assert_eq!(all_outputs[2].num_rows(), 350);
        assert_eq!(all_outputs[3].num_rows(), 500);
    }

    #[test]
    fn test_biggest_coalesce_batch_size_reset_consecutive_on_small_batch() {
        // Test that small batches reset the consecutive large batch tracking
        let mut coalescer = BatchCoalescer::new(
            Arc::new(Schema::new(vec![Field::new("c0", DataType::Int32, false)])),
            100,
        );
        coalescer.set_biggest_coalesce_batch_size(Some(200));

        // Push first large batch - should bypass (no buffered data)
        coalescer.push_batch(create_test_batch(300)).unwrap();
        let output = coalescer.next_completed_batch().unwrap();
        assert_eq!(output.num_rows(), 300);

        // Push second large batch - should bypass (consecutive)
        coalescer.push_batch(create_test_batch(400)).unwrap();
        let output = coalescer.next_completed_batch().unwrap();
        assert_eq!(output.num_rows(), 400);

        // Push small batch - resets consecutive tracking
        coalescer.push_batch(create_test_batch(50)).unwrap();
        assert_eq!(coalescer.get_buffered_rows(), 50);

        // Push large batch again - should NOT bypass due to buffered data
        coalescer.push_batch(create_test_batch(350)).unwrap();

        // Should coalesce: 50 + 350 = 400 -> 4 complete batches of 100
        let mut outputs = vec![];
        while let Some(batch) = coalescer.next_completed_batch() {
            outputs.push(batch);
        }
        assert_eq!(outputs.len(), 4);
        for batch in outputs {
            assert_eq!(batch.num_rows(), 100);
        }
        assert_eq!(coalescer.get_buffered_rows(), 0);
    }

    #[test]
    fn test_coalasce_push_batch_with_indices() {
        const MID_POINT: u32 = 2333;
        const TOTAL_ROWS: u32 = 23333;
        let batch1 = uint32_batch_non_null(0..MID_POINT);
        let batch2 = uint32_batch_non_null((MID_POINT..TOTAL_ROWS).rev());

        let mut coalescer = BatchCoalescer::new(
            Arc::new(Schema::new(vec![Field::new("c0", DataType::UInt32, false)])),
            TOTAL_ROWS as usize,
        );
        coalescer.push_batch(batch1).unwrap();

        let rev_indices = (0..((TOTAL_ROWS - MID_POINT) as u64)).rev();
        let reversed_indices_batch = uint64_batch_non_null(rev_indices);

        let reverse_indices = UInt64Array::from(reversed_indices_batch.column(0).to_data());
        coalescer
            .push_batch_with_indices(batch2, &reverse_indices)
            .unwrap();

        coalescer.finish_buffered_batch().unwrap();
        let actual = coalescer.next_completed_batch().unwrap();

        let expected = uint32_batch_non_null(0..TOTAL_ROWS);

        assert_eq!(expected, actual);
    }
}
