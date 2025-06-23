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
use arrow_array::types::{BinaryViewType, StringViewType};
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch};
use arrow_schema::{ArrowError, DataType, SchemaRef};
use std::collections::VecDeque;
use std::sync::Arc;
// Originally From DataFusion's coalesce module:
// https://github.com/apache/datafusion/blob/9d2f04996604e709ee440b65f41e7b882f50b788/datafusion/physical-plan/src/coalesce/mod.rs#L26-L25

mod byte_view;
mod generic;

use byte_view::InProgressByteViewArray;
use generic::GenericInProgressArray;

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
/// [`filter`]: crate::filter::filter
/// [`take`]: crate::take::take
///
/// See: <https://github.com/apache/arrow-rs/issues/6692>
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
    /// output batch size
    batch_size: usize,
    /// In-progress arrays
    in_progress_arrays: Vec<Box<dyn InProgressArray>>,
    /// Buffered row count. Always less than `batch_size`
    buffered_rows: usize,
    /// Completed batches
    completed: VecDeque<RecordBatch>,
}

impl BatchCoalescer {
    /// Create a new `BatchCoalescer`
    ///
    /// # Arguments
    /// - `schema` - the schema of the output batches
    /// - `batch_size` - the number of rows in each output batch.
    ///   Typical values are `4096` or `8192` rows.
    ///
    pub fn new(schema: SchemaRef, batch_size: usize) -> Self {
        let in_progress_arrays = schema
            .fields()
            .iter()
            .map(|field| create_in_progress_array(field.data_type(), batch_size))
            .collect::<Vec<_>>();

        Self {
            schema,
            batch_size,
            in_progress_arrays,
            // We will for sure store at least one completed batch
            completed: VecDeque::with_capacity(1),
            buffered_rows: 0,
        }
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

    /// Push all the rows from `batch` into the Coalescer
    ///
    /// See [`Self::next_completed_batch()`] to retrieve any completed batches.
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
        let (_schema, arrays, mut num_rows) = batch.into_parts();
        if num_rows == 0 {
            return Ok(());
        }

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
        while num_rows > (self.batch_size - self.buffered_rows) {
            let remaining_rows = self.batch_size - self.buffered_rows;
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
        if self.buffered_rows >= self.batch_size {
            self.finish_buffered_batch()?;
        }

        // clear in progress sources (to allow the memory to be freed)
        for in_progress in self.in_progress_arrays.iter_mut() {
            in_progress.set_source(None);
        }

        Ok(())
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

    /// Returns the next completed batch, if any
    pub fn next_completed_batch(&mut self) -> Option<RecordBatch> {
        self.completed.pop_front()
    }
}

/// Return a new `InProgressArray` for the given data type
fn create_in_progress_array(data_type: &DataType, batch_size: usize) -> Box<dyn InProgressArray> {
    match data_type {
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
    use arrow_array::{BinaryViewArray, RecordBatchOptions, StringViewArray, UInt32Array};
    use arrow_schema::{DataType, Field, Schema};
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
        // The strings are 28 long, so each batch has 400 * 28 = 5600 bytes
        let batch = stringview_batch_repeated(
            400,
            [Some("This string is 28 bytes long"), Some("small string")],
        );
        let output_batches = Test::new()
            // First allocated buffer is 8kb.
            // Appending five batches of 5600 bytes will use 5600 * 5 = 28kb (8kb, an 16kb and 32kbkb)
            .with_batch(batch.clone())
            .with_batch(batch.clone())
            .with_batch(batch.clone())
            .with_batch(batch.clone())
            .with_batch(batch.clone())
            .with_batch_size(8000)
            .with_expected_output_sizes(vec![2000]) // only 2000 rows total
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
            .with_batches(std::iter::repeat(batch).take(20))
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
        // The strings are 37 bytes long, so each batch has 200 * 28 = 5600 bytes
        let mixed_batch = stringview_batch_repeated(
            400,
            [Some("This string is 28 bytes long"), Some("small string")],
        );
        // These strings aren't copied, this array has an 8k buffer
        let all_large = stringview_batch_repeated(
            100,
            [Some(
                "This buffer has only large strings in it so there are no buffer copies",
            )],
        );

        let output_batches = Test::new()
            // First allocated buffer is 8kb.
            // Appending five batches of 5600 bytes will use 5600 * 5 = 28kb (8kb, an 16kb and 32kbkb)
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
                    len: 8176,
                    capacity: 8192,
                },
                // this buffer was allocated but not used when the all_large batch was pushed
                ExpectedLayout {
                    len: 3024,
                    capacity: 16384,
                },
                ExpectedLayout {
                    len: 7000,
                    capacity: 8192,
                },
                ExpectedLayout {
                    len: 5600,
                    capacity: 32768,
                },
                ExpectedLayout {
                    len: 7000,
                    capacity: 8192,
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
            let Self {
                input_batches,
                schema,
                target_batch_size,
                expected_output_sizes,
            } = self;

            let schema = schema.unwrap_or_else(|| input_batches[0].schema());

            // create a single large input batch for output comparison
            let single_input_batch = concat_batches(&schema, &input_batches).unwrap();

            let mut coalescer = BatchCoalescer::new(Arc::clone(&schema), target_batch_size);

            let had_input = input_batches.iter().any(|b| b.num_rows() > 0);
            for batch in input_batches {
                coalescer.push_batch(batch).unwrap();
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
                let expected_batch = single_input_batch.slice(starting_idx, *expected_size);
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
    }

    /// Return a RecordBatch with a UInt32Array with the specified range
    fn uint32_batch(range: Range<u32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("c0", DataType::UInt32, false)]));

        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(UInt32Array::from_iter_values(range))],
        )
        .unwrap()
    }

    /// Return a RecordBatch with a StringViewArray with (only) the specified values
    fn stringview_batch<'a>(values: impl IntoIterator<Item = Option<&'a str>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "c0",
            DataType::Utf8View,
            false,
        )]));

        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringViewArray::from_iter(values))],
        )
        .unwrap()
    }

    /// Return a RecordBatch with a StringViewArray with num_rows by repating
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
}
