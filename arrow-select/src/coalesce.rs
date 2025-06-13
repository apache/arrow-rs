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
use crate::concat::concat_batches;
use crate::filter::filter_record_batch;
use arrow_array::{cast::AsArray, Array, ArrayRef, RecordBatch};
use arrow_array::{BooleanArray, StringViewArray};
use arrow_data::ByteView;
use arrow_schema::{ArrowError, SchemaRef};
use std::collections::VecDeque;
use std::sync::Arc;
// Originally From DataFusion's coalesce module:
// https://github.com/apache/datafusion/blob/9d2f04996604e709ee440b65f41e7b882f50b788/datafusion/physical-plan/src/coalesce/mod.rs#L26-L25

/// Concatenate multiple [`RecordBatch`]es
///
/// Implements the common pattern of incrementally creating output
/// [`RecordBatch`]es of a specific size from an input stream of
/// [`RecordBatch`]es.
///
/// This is useful after operations such as [`filter`] and [`take`] that produce
/// smaller batches, and we want to coalesce them into larger
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
///
/// 3. Eventually this may also be able to handle other optimizations such as a
///    combined filter/coalesce operation. See <https://github.com/apache/arrow-rs/issues/6692>
///
#[derive(Debug)]
pub struct BatchCoalescer {
    /// The input schema
    schema: SchemaRef,
    /// output batch size
    batch_size: usize,
    /// In-progress buffered batches
    buffer: Vec<RecordBatch>,
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
        Self {
            schema,
            batch_size,
            buffer: vec![],
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
        if batch.num_rows() == 0 {
            // If the batch is empty, we don't need to do anything
            return Ok(());
        }

        let mut batch = gc_string_view_batch(batch);

        // If pushing this batch would exceed the target batch size,
        // finish the current batch and start a new one
        while batch.num_rows() > (self.batch_size - self.buffered_rows) {
            let remaining_rows = self.batch_size - self.buffered_rows;
            debug_assert!(remaining_rows > 0);
            let head_batch = batch.slice(0, remaining_rows);
            batch = batch.slice(remaining_rows, batch.num_rows() - remaining_rows);
            self.buffered_rows += head_batch.num_rows();
            self.buffer.push(head_batch);
            self.finish_buffered_batch()?;
        }
        // Add the remaining rows to the buffer
        self.buffered_rows += batch.num_rows();
        self.buffer.push(batch);

        // If we have reached the target batch size, finalize the buffered batch
        if self.buffered_rows >= self.batch_size {
            self.finish_buffered_batch()?;
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
        if self.buffer.is_empty() {
            return Ok(());
        }
        let batch = concat_batches(&self.schema, &self.buffer)?;
        self.buffer.clear();
        self.buffered_rows = 0;
        self.completed.push_back(batch);
        Ok(())
    }

    /// Returns true if there is any buffered data
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty() && self.completed.is_empty()
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

/// Heuristically compact `StringViewArray`s to reduce memory usage, if needed
///
/// Decides when to consolidate the StringView into a new buffer to reduce
/// memory usage and improve string locality for better performance.
///
/// This differs from `StringViewArray::gc` because:
/// 1. It may not compact the array depending on a heuristic.
/// 2. It uses a precise block size to reduce the number of buffers to track.
///
/// # Heuristic
///
/// If the average size of each view is larger than 32 bytes, we compact the array.
///
/// `StringViewArray` include pointers to buffer that hold the underlying data.
/// One of the great benefits of `StringViewArray` is that many operations
/// (e.g., `filter`) can be done without copying the underlying data.
///
/// However, after a while (e.g., after `FilterExec` or `HashJoinExec`) the
/// `StringViewArray` may only refer to a small portion of the buffer,
/// significantly increasing memory usage.
fn gc_string_view_batch(batch: RecordBatch) -> RecordBatch {
    let (schema, columns, num_rows) = batch.into_parts();
    let new_columns: Vec<ArrayRef> = columns
        .into_iter()
        .map(|c| {
            // Try to re-create the `StringViewArray` to prevent holding the underlying buffer too long.
            let Some(s) = c.as_string_view_opt() else {
                return c;
            };
            if s.data_buffers().is_empty() {
                // If there are no data buffers, we can just return the array as is
                return c;
            }
            let ideal_buffer_size: usize = s
                .views()
                .iter()
                .map(|v| {
                    let len = (*v as u32) as usize;
                    if len > 12 {
                        len
                    } else {
                        0
                    }
                })
                .sum();
            let actual_buffer_size = s.get_buffer_memory_size();
            let buffers = s.data_buffers();

            // Re-creating the array copies data and can be time consuming.
            // We only do it if the array is sparse
            if actual_buffer_size > (ideal_buffer_size * 2) {
                if ideal_buffer_size == 0 {
                    // If the ideal buffer size is 0, all views are inlined
                    // so just reuse the views
                    return Arc::new(unsafe {
                        StringViewArray::new_unchecked(
                            s.views().clone(),
                            vec![],
                            s.nulls().cloned(),
                        )
                    });
                }
                // We set the block size to `ideal_buffer_size` so that the new StringViewArray only has one buffer, which accelerate later concat_batches.
                // See https://github.com/apache/arrow-rs/issues/6094 for more details.
                let mut buffer: Vec<u8> = Vec::with_capacity(ideal_buffer_size);

                let views: Vec<u128> = s
                    .views()
                    .as_ref()
                    .iter()
                    .cloned()
                    .map(|v| {
                        let mut b: ByteView = ByteView::from(v);

                        if b.length > 12 {
                            let offset = buffer.len() as u32;
                            buffer.extend_from_slice(
                                buffers[b.buffer_index as usize]
                                    .get(b.offset as usize..b.offset as usize + b.length as usize)
                                    .expect("Invalid buffer slice"),
                            );
                            b.offset = offset;
                            b.buffer_index = 0; // Set buffer index to 0, as we only have one buffer
                        }

                        b.into()
                    })
                    .collect();

                let buffers = if buffer.is_empty() {
                    vec![]
                } else {
                    vec![buffer.into()]
                };

                let gc_string = unsafe {
                    StringViewArray::new_unchecked(views.into(), buffers, s.nulls().cloned())
                };

                Arc::new(gc_string)
            } else {
                c
            }
        })
        .collect();
    unsafe { RecordBatch::new_unchecked(schema, new_columns, num_rows) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::StringViewBuilder;
    use arrow_array::{RecordBatchOptions, StringViewArray, UInt32Array};
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
        Test::new()
            // both input batches have no views, so no need to compact
            .with_batch(stringview_batch([Some("foo"), Some("bar")]))
            .with_batch(stringview_batch([Some("baz"), Some("qux")]))
            .with_expected_output_sizes(vec![4])
            .run();
    }

    #[test]
    fn test_string_view_batch_small_no_compact() {
        // view with only short strings (no buffers) --> no need to compact
        let batch = stringview_batch_repeated(1000, [Some("a"), Some("b"), Some("c")]);
        let gc_batches = Test::new()
            .with_batch(batch.clone())
            .with_expected_output_sizes(vec![1000])
            .run();

        let array = col_as_string_view("c0", &batch);
        let gc_array = col_as_string_view("c0", gc_batches.first().unwrap());
        assert_eq!(array.data_buffers().len(), 0);
        assert_eq!(array.data_buffers().len(), gc_array.data_buffers().len()); // no compaction
    }

    #[test]
    fn test_string_view_batch_large_no_compact() {
        // view with large strings (has buffers) but full --> no need to compact
        let batch = stringview_batch_repeated(1000, [Some("This string is longer than 12 bytes")]);
        let gc_batches = Test::new()
            .with_batch(batch.clone())
            .with_batch_size(1000)
            .with_expected_output_sizes(vec![1000])
            .run();

        let array = col_as_string_view("c0", &batch);
        let gc_array = col_as_string_view("c0", gc_batches.first().unwrap());
        assert_eq!(array.data_buffers().len(), 5);
        assert_eq!(array.data_buffers().len(), gc_array.data_buffers().len()); // no compaction
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
        let gc_batches = Test::new()
            .with_batch(batch.clone())
            .with_batch_size(1000)
            .with_expected_output_sizes(vec![10])
            .run();

        let array = col_as_string_view("c0", &batch);
        let gc_array = col_as_string_view("c0", gc_batches.first().unwrap());
        assert_eq!(array.data_buffers().len(), 1); // input has one buffer
        assert_eq!(gc_array.data_buffers().len(), 0); // output has no buffers as only short strings
    }

    #[test]
    fn test_string_view_batch_large_slice_compact() {
        // view with large strings (has buffers) and only partially used  --> no need to compact
        let batch = stringview_batch_repeated(1000, [Some("This string is longer than 12 bytes")])
            // slice only 22 rows, so most of the buffer is not used
            .slice(11, 22);

        let gc_batches = Test::new()
            .with_batch(batch.clone())
            .with_batch_size(1000)
            .with_expected_output_sizes(vec![22])
            .run();

        let array = col_as_string_view("c0", &batch);
        let gc_array = col_as_string_view("c0", gc_batches.first().unwrap());
        assert_eq!(array.data_buffers().len(), 5);
        assert_eq!(gc_array.data_buffers().len(), 1); // compacted into a single buffer
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
        let gc_batches = Test::new()
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

        let gc_array = col_as_string_view("c0", gc_batches.first().unwrap());

        assert_eq!(gc_array.data_buffers().len(), 5);
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

            for batch in input_batches {
                coalescer.push_batch(batch).unwrap();
            }
            coalescer.finish_buffered_batch().unwrap();
            let mut output_batches = vec![];
            while let Some(batch) = coalescer.next_completed_batch() {
                output_batches.push(batch);
            }

            // make sure we got the expected number of output batches and content
            let mut starting_idx = 0;
            assert_eq!(expected_output_sizes.len(), output_batches.len());
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

        RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(builder.finish())]).unwrap()
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
