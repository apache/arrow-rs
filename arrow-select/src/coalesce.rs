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
use arrow_array::{builder::StringViewBuilder, cast::AsArray, Array, RecordBatch};
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

    /// Push next batch into the Coalescer
    ///
    /// See [`Self::next_completed_batch()`] to retrieve any completed batches.
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
#[inline(never)]
fn gc_string_view_batch(batch: RecordBatch) -> RecordBatch {
    let (schema, mut columns, row_count) = batch.into_parts();

    for c in columns.iter_mut() {
        let Some(s) = c.as_string_view_opt() else {
            continue;
        };
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

        // Re-creating the array copies data and can be time consuming.
        // We only do it if the array is sparse
        if actual_buffer_size > (ideal_buffer_size * 2) {
            // We set the block size to `ideal_buffer_size` so that the new StringViewArray only has one buffer, which accelerate later concat_batches.
            // See https://github.com/apache/arrow-rs/issues/6094 for more details.
            let mut builder = StringViewBuilder::with_capacity(s.len());
            if ideal_buffer_size > 0 {
                builder = builder.with_fixed_block_size(ideal_buffer_size as u32);
            }

            for v in s.iter() {
                builder.append_option(v);
            }

            let gc_string = builder.finish();

            debug_assert!(gc_string.data_buffers().len() <= 1); // buffer count can be 0 if the `ideal_buffer_size` is 0
                                                                // Ensure the new array has the same number of rows as the input
            let new_row_count = gc_string.len();
            assert_eq!(
                new_row_count, row_count,
                "gc_string_view_batch: length mismatch got {new_row_count}, expected {row_count}"
            );
            *c = Arc::new(gc_string)
        }
    }

    // Safety: the input batch was valid, and we validated that the new output array
    // had the same number of rows as the input.
    unsafe { RecordBatch::new_unchecked(schema, columns, row_count) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::ArrayBuilder;
    use arrow_array::{ArrayRef, RecordBatchOptions, StringViewArray, UInt32Array};
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
            .run()
    }

    #[test]
    fn test_coalesce_one_by_one() {
        let batch = uint32_batch(0..1); // single row input
        Test::new()
            .with_batches(std::iter::repeat_n(batch, 97))
            // expected output is exactly 20 rows (except for the final batch)
            .with_batch_size(20)
            .with_expected_output_sizes(vec![20, 20, 20, 20, 17])
            .run()
    }

    #[test]
    fn test_coalesce_empty() {
        let schema = Arc::new(Schema::new(vec![Field::new("c0", DataType::UInt32, false)]));

        Test::new()
            .with_batches(vec![])
            .with_schema(schema)
            .with_batch_size(21)
            .with_expected_output_sizes(vec![])
            .run()
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

    /// Test for [`BatchCoalescer`]
    ///
    /// Pushes the input batches to the coalescer and verifies that the resulting
    /// batches have the expected number of rows and contents.
    #[derive(Debug, Clone, Default)]
    struct Test {
        /// Batches to feed to the coalescer. Tests must have at least one
        /// schema
        input_batches: Vec<RecordBatch>,

        /// The schema. If not provided, the first batch's schema is used.
        schema: Option<SchemaRef>,

        /// Expected output sizes of the resulting batches
        expected_output_sizes: Vec<usize>,
        /// target batch size
        target_batch_size: usize,
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
        fn run(self) {
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
            for (i, (expected_size, batch)) in
                expected_output_sizes.iter().zip(output_batches).enumerate()
            {
                // compare the contents of the batch after normalization (using
                // `==` compares the underlying memory layout too)
                let expected_batch = single_input_batch.slice(starting_idx, *expected_size);
                let expected_batch = normalize_batch(expected_batch);
                let batch = normalize_batch(batch);
                assert_eq!(
                    expected_batch, batch,
                    "Unexpected content in batch {i}:\
                    \n\nExpected:\n{expected_batch:#?}\n\nActual:\n{batch:#?}"
                );
                starting_idx += *expected_size;
            }
        }
    }

    /// Return a batch of  UInt32 with the specified range
    fn uint32_batch(range: Range<u32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("c0", DataType::UInt32, false)]));

        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(UInt32Array::from_iter_values(range))],
        )
        .unwrap()
    }

    #[test]
    fn test_gc_string_view_batch_small_no_compact() {
        // view with only short strings (no buffers) --> no need to compact
        let array = StringViewTest {
            rows: 1000,
            strings: vec![Some("a"), Some("b"), Some("c")],
        }
        .build();

        let gc_array = do_gc(array.clone());
        compare_string_array_values(&array, &gc_array);
        assert_eq!(array.data_buffers().len(), 0);
        assert_eq!(array.data_buffers().len(), gc_array.data_buffers().len()); // no compaction
    }

    #[test]
    fn test_gc_string_view_test_batch_empty() {
        let schema = Schema::empty();
        let batch = RecordBatch::new_empty(schema.into());
        let output_batch = gc_string_view_batch(batch.clone());
        assert_eq!(batch.num_columns(), output_batch.num_columns());
        assert_eq!(batch.num_rows(), output_batch.num_rows());
    }

    #[test]
    fn test_gc_string_view_batch_large_no_compact() {
        // view with large strings (has buffers) but full --> no need to compact
        let array = StringViewTest {
            rows: 1000,
            strings: vec![Some("This string is longer than 12 bytes")],
        }
        .build();

        let gc_array = do_gc(array.clone());
        compare_string_array_values(&array, &gc_array);
        assert_eq!(array.data_buffers().len(), 5);
        assert_eq!(array.data_buffers().len(), gc_array.data_buffers().len()); // no compaction
    }

    #[test]
    fn test_gc_string_view_batch_large_slice_compact() {
        // view with large strings (has buffers) and only partially used  --> no need to compact
        let array = StringViewTest {
            rows: 1000,
            strings: vec![Some("this string is longer than 12 bytes")],
        }
        .build();

        // slice only 11 rows, so most of the buffer is not used
        let array = array.slice(11, 22);

        let gc_array = do_gc(array.clone());
        compare_string_array_values(&array, &gc_array);
        assert_eq!(array.data_buffers().len(), 5);
        assert_eq!(gc_array.data_buffers().len(), 1); // compacted into a single buffer
    }

    /// Compares the values of two string view arrays
    fn compare_string_array_values(arr1: &StringViewArray, arr2: &StringViewArray) {
        assert_eq!(arr1.len(), arr2.len());
        for (s1, s2) in arr1.iter().zip(arr2.iter()) {
            assert_eq!(s1, s2);
        }
    }

    /// runs garbage collection on string view array
    /// and ensures the number of rows are the same
    fn do_gc(array: StringViewArray) -> StringViewArray {
        let batch = RecordBatch::try_from_iter(vec![("a", Arc::new(array) as ArrayRef)]).unwrap();
        let gc_batch = gc_string_view_batch(batch.clone());
        assert_eq!(batch.num_rows(), gc_batch.num_rows());
        assert_eq!(batch.schema(), gc_batch.schema());
        gc_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap()
            .clone()
    }

    /// Describes parameters for creating a `StringViewArray`
    struct StringViewTest {
        /// The number of rows in the array
        rows: usize,
        /// The strings to use in the array (repeated over and over
        strings: Vec<Option<&'static str>>,
    }

    impl StringViewTest {
        /// Create a `StringViewArray` with the parameters specified in this struct
        fn build(self) -> StringViewArray {
            let mut builder = StringViewBuilder::with_capacity(100).with_fixed_block_size(8192);
            loop {
                for &v in self.strings.iter() {
                    builder.append_option(v);
                    if builder.len() >= self.rows {
                        return builder.finish();
                    }
                }
            }
        }
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
