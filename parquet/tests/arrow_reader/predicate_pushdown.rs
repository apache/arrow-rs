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

//! Fuzz Tests for predicate evaluation in the parquet reader

use crate::predicate_cache::TestReader;
use arrow_array::{ArrayRef, BooleanArray, Int8Array, Int64Array, RecordBatch, StringViewArray};
use arrow_buffer::BooleanBufferBuilder;
use arrow_schema::ArrowError;
use arrow_select::concat::concat_batches;
use bytes::Bytes;
use futures::StreamExt;
use parquet::arrow::arrow_reader::{ArrowPredicate, RowFilter, RowSelection, RowSelectionPolicy};
use parquet::arrow::async_reader::ParquetRecordBatchStream;
use parquet::arrow::{ArrowWriter, ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::basic::Compression;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
use parquet::file::page_index::offset_index::PageLocation;
use parquet::file::properties::WriterProperties;
use std::ops::Range;
use std::sync::{Arc, OnceLock};

#[tokio::test]
async fn test_manual_single_selection() {
    // single selection from 190..400
    Test::new_three_column_parquet(vec![190..400]).run().await
}

#[tokio::test]
async fn test_manual_skip_int64() {
    // selections which skip pages in int64 column
    Test::new_three_column_parquet(vec![
        275..500, 800..900
    ]).run().await
}

#[tokio::test]
async fn test_manual_skip_utf8view() {
    // selections which skip pages in int64 column
    Test::new_three_column_parquet(vec![
        50..100, 700..750, 800..850
    ]).run().await
}


/// Test case for verifying multiple different mechanisms to evaluate parquet
/// predicates and verify that the results are as expected.
///
/// For example, given the layout from [`three_column_parquet`], we can  lets us
/// verify that reading with a selection predicate that skips certain rows and
/// pages returns the expected results.
///
/// This is especially important for testing the parquet reader's ability to
/// skip entire pages based on row selection.
///
/// ```text
///         0 ┌───────┐    0 ┌───────┐      0 ┌───────┐
///           │       │      │       │        │ SKIP  │
///           │       │      │ SKIP  │        │       │
///           │       │      │       │        └───────┘
///           │       │      │       │    184 ┌───────┐
///           │       │      └───────┘        │       │
///           │       │  264 ┌───────┐        │       │
///        ┌ ─│─ ─ ─ ─│─ ─ ─ ┼ ─ ─ ─ ┼ ─ ─ ─ ─│─ ─ ─ ─│─ ┐
///           │       │      │       │        └───────┘       Selection
///        │  │       │      │       │    359 ┌───────┐  │
///         ─ ┼ ─ ─ ─ ┼ ─ ─ ─└───────┘─ ─ ─ ─ ┼ ─ ─ ─ ┼ ─
///           └───────┘  528 ┌───────┐        │       │
///      532  ┌───────┐      │       │        │       │
///           │       │      │ SKIP  │        └───────┘
///           │       │      │       │    708 ┌───────┐
///           │       │      │       │        │       │
///           │       │      └───────┘        │       │
///        ┌ ─│─ ─ ─ ─│─ 792 ┬───────┬ ─ ─ ─ ─│─ ─ ─ ─│─ ┐   Selection
///           │       │      │       │        └───────┘
///        │  │       │      │       │    883 ┌───────┐  │
///         ─ ┼ ─ ─ ─ ┼ ─ ─ ─│─ ─ ─ ─│─ ─ ─ ─ ┼ ─ ─ ─ ┼ ─
///           │       │      │       │        │       │
///           │       │      │       │        │       │
///           └───────┘      └───────┘        └───────┘
///
///             int8           int64              utf8view
/// ```
///
struct Test {
    /// The input parquet file
    parquet: Bytes,
    /// which selections to make -- each selection is a range of row indices to
    /// include The tests verify that only rows within these ranges are returned
    /// and select
    selections: Vec<Range<usize>>,
}

impl Test {
    fn new_three_column_parquet(selections: Vec<Range<usize>>) -> Self {
        Self {
            parquet: three_column_parquet(),
            selections,
        }
    }

    /// Method is to evaluates applying the same selection using multiple
    /// different ways and compare the results to ensure they are the same and
    /// as expected.
    ///
    /// Different batch sizes and row selection policies are also tested.
    async fn run(&self) {
        for projection in self.all_projections() {
            let expected = self.expected(&projection);

            for batch_size in [100, 217, 8192] {
                for row_selection_policy in self.all_row_selection_policies() {
                    // Evaluate the selection using the RowSelection API
                    self.run_inner(
                        "row_selection",
                        projection,
                        batch_size,
                        row_selection_policy,
                        &expected,
                        |b| self.add_predicate_row_selection(b),
                    )
                    .await;

                    // Evaluate the selection using a single row filter
                    for filter_projection in self.all_projections() {
                        self.run_inner(
                            &format!("row_filter with filter projection: {filter_projection:?}"),
                            projection,
                            batch_size,
                            row_selection_policy,
                            &expected,
                            |b| self.add_predicate_row_filter(b, filter_projection),
                        )
                        .await;
                    }

                    // Evaluate the selection using a multiple row filters
                    for filter_projection in self.all_projections() {
                        self.run_inner(
                            &format!("row_filter with multiple filters: {filter_projection:?}"),
                            projection,
                            batch_size,
                            row_selection_policy,
                            &expected,
                            |b| self.add_predicate_multiple_row_filter(b, filter_projection),
                        )
                        .await;
                    }
                }
            }
        }
    }

    /// Create a reader with the specified projection and batch size, and
    /// function F to apply the selection predicates.
    ///
    /// Reads all rows and verify the results match expected
    async fn run_inner<F>(
        &self,
        test_description: &str,
        projection: &[&str],
        batch_size: usize,
        row_selection_policy: RowSelectionPolicy,
        expected: &RecordBatch,
        add_predicate: F,
    ) where
        F: Fn(
            ParquetRecordBatchStreamBuilder<TestReader>,
        ) -> ParquetRecordBatchStreamBuilder<TestReader>,
    {
        let builder = self
            .builder_with_projection(projection)
            .await
            .with_batch_size(batch_size)
            .with_row_selection_policy(row_selection_policy);
        let reader = add_predicate(builder).build().unwrap();
        let actual = self.collect_to_batch(reader).await;

        assert_eq!(
            expected, &actual,
            "description: {test_description}\n\
             selections: {projection:?}, batch size: {batch_size}, row_selection_policy:{row_selection_policy:?}\n\
             expected:\n\
             \n\
             {expected:#?}\n\
             \n\
             {actual:#?}",
        );
    }

    /// A list of columns to project
    fn all_projections(&self) -> &[&[&str]] {
        &[
            &["int8"],
            &["int64"],
            &["utf8view"],
             &["int8", "int64"],
            &["int64", "utf8view"],
            &["utf8view", "int8"], // reverse
            & ["int8", "utf8view"],
            &["int64", "utf8view"],
            &["utf8view", "int64"],
            &["int8", "int64", "utf8view"],
        ]
    }

    /// A list of all RowSelectionPolicy options to test
    fn all_row_selection_policies(&self) -> [RowSelectionPolicy; 3] {
        [
            RowSelectionPolicy::default(),
            RowSelectionPolicy::Mask,
            RowSelectionPolicy::Selectors,
        ]
    }

    /// Return a ParquetRecordBatchReaderBuilder ready to read the parquet file
    async fn builder_with_projection(
        &self,
        projection: &[&str],
    ) -> ParquetRecordBatchStreamBuilder<TestReader> {
        let test_reader = TestReader::new(self.parquet.clone());
        let builder = ParquetRecordBatchStreamBuilder::new(test_reader)
            .await
            .unwrap();

        let projection = ProjectionMask::columns(
            builder.metadata().file_metadata().schema_descr(),
            projection.iter().cloned(),
        );
        builder.with_projection(projection)
    }

    /// Apply the selections to the three_column_batch to get the expected result
    fn expected(&self, columns: &[&str]) -> RecordBatch {
        let batch = three_column_batch();
        let mut indices = columns
            .iter()
            .map(|col_name| batch.schema().index_of(col_name).unwrap())
            .collect::<Vec<_>>();

        // reader always returns columns in file order
        indices.sort_unstable();

        let batch = batch.project(&indices).unwrap();
        let batches: Vec<_> = self
            .selections
            .iter()
            .map(|range| batch.slice(range.start, range.end - range.start))
            .collect();

        concat_batches(&batch.schema(), &batches).unwrap()
    }

    async fn collect_to_batch(
        &self,
        mut reader: ParquetRecordBatchStream<TestReader>,
    ) -> RecordBatch {
        let mut batches = vec![];
        while let Some(batch) = reader.next().await {
            let batch = batch.unwrap();
            batches.push(batch);
        }
        let schema = batches[0].schema();
        concat_batches(&schema, &batches).unwrap()
    }

    /// Return a `BooleanArray` that is true for rows in `selections`
    fn selection_mask(selections: &[Range<usize>], num_rows: usize) -> BooleanArray {
        let mut boolean_buffer_builder = BooleanBufferBuilder::new(num_rows);
        let mut current_row = 0;
        for range in selections.iter() {
            assert!(range.start >= current_row);
            // fill in false for rows before the selection
            boolean_buffer_builder.append_n(range.start - current_row, false);
            // fill in true for rows in the selection
            boolean_buffer_builder.append_n(range.end - range.start, true);
            current_row = range.end;
        }
        // fill in false for any remaining rows
        if current_row < num_rows {
            boolean_buffer_builder.append_n(num_rows - current_row, false);
        }
        let nulls = None;
        BooleanArray::new(boolean_buffer_builder.build(), nulls)
    }

    // ------------------
    // Below are different predicates are added to the reader. Note these
    // methods all select the same selections, just in different ways.
    // ------------------

    /// Add a predicate to the reader that selects only the specified rows via
    /// [`ParquetRecordBatchStreamBuilder::with_row_selection`]
    fn add_predicate_row_selection<T>(
        &self,
        builder: ParquetRecordBatchStreamBuilder<T>,
    ) -> ParquetRecordBatchStreamBuilder<T> {
        let num_rows = builder.metadata().file_metadata().num_rows() as usize;
        let selection =
            RowSelection::from_consecutive_ranges(self.selections.clone().into_iter(), num_rows);
        builder.with_row_selection(selection)
    }

    /// Add an arrow predicate (row filter, pushed down predicate) via
    /// [`ParquetRecordBatchStreamBuilder::with_row_filter`]
    ///
    /// Specifies it should run on the columns in `filter_projection`
    fn add_predicate_row_filter<T>(
        &self,
        builder: ParquetRecordBatchStreamBuilder<T>,
        filter_projection: &[&str],
    ) -> ParquetRecordBatchStreamBuilder<T> {
        let num_rows = builder.metadata().file_metadata().num_rows() as usize;
        let mask = ProjectionMask::columns(
            builder.metadata().file_metadata().schema_descr(),
            filter_projection.iter().cloned(),
        );
        let predicate =
            PrecomputedArrowPredicate::new(mask, Self::selection_mask(&self.selections, num_rows));

        let filter = RowFilter::new(vec![Box::new(predicate)]);
        builder.with_row_filter(filter)
    }

    /// Adds multiple arrow predicates (row filters, pushed down predicates)
    /// that together form the total selection
    ///
    /// For example
    /// * if we have a selection like`[100-200, 300-400, 500-600]`
    /// * two columns `colA` and `colB`, in the filter projection
    ///
    /// Applies
    /// * a selection on `colA` for the entire range (100-600)
    /// * then applies a selection on `colB` for the relative offsets
    ///   within that range to select only the rows in [0-100, 200-300, 400-500]
    ///
    fn add_predicate_multiple_row_filter<T>(
        &self,
        builder: ParquetRecordBatchStreamBuilder<T>,
        filter_projection: &[&str],
    ) -> ParquetRecordBatchStreamBuilder<T> {
        // Only support up to 3 predicates for now
        let mut remaining_columns = filter_projection.to_vec();
        let first_col = remaining_columns.pop().expect("empty filter projection");

        if remaining_columns.is_empty() {
            return self.add_predicate_row_filter(builder, filter_projection);
        };

        // otherwise create a multi-column predicate
        let num_rows = builder.metadata().file_metadata().num_rows() as usize;

        // Form first selection from [overall_min..overall_max]
        // selections should be ordered and non overlapping
        let overall_min = self.selections.iter().map(|r| r.start).min().unwrap();
        assert_eq!(overall_min, self.selections.first().unwrap().start,);
        let overall_max = self.selections.iter().map(|r| r.end).max().unwrap();
        assert_eq!(overall_max, self.selections.last().unwrap().end);
        let filter_result = Self::selection_mask(&[overall_min..overall_max], num_rows);
        let schema_descr = builder.metadata().file_metadata().schema_descr();
        let mask1 = ProjectionMask::columns(schema_descr, [first_col]);
        let predicate1 = PrecomputedArrowPredicate::new(mask1, filter_result);

        // Second selection is formed from relative offsets within the overall selection
        let relative_selections: Vec<_> = self
            .selections
            .iter()
            .map(|r| (r.start - overall_min)..(r.end - overall_min))
            .collect();
        let filter_result = Self::selection_mask(&relative_selections, num_rows);
        let mask2 = ProjectionMask::columns(schema_descr, remaining_columns.into_iter());
        let predicate2 = PrecomputedArrowPredicate::new(mask2, filter_result);

        let filter = RowFilter::new(vec![Box::new(predicate1), Box::new(predicate2)]);
        builder.with_row_filter(filter)
    }
}

/// A predicate (row filter) that returns the results of a precomputed filter
/// using a precomputed BooleanArray.
///
/// This makes it easier to test various selection patterns without having to
/// implement complex predicate logic on different data types.
struct PrecomputedArrowPredicate {
    mask: ProjectionMask,
    filter_results: BooleanArray,
}

impl PrecomputedArrowPredicate {
    /// Create a new PrecomputedArrowPredicate
    fn new(mask: ProjectionMask, filter_results: BooleanArray) -> Self {
        Self {
            mask,
            filter_results,
        }
    }
}

impl ArrowPredicate for PrecomputedArrowPredicate {
    fn projection(&self) -> &ProjectionMask {
        &self.mask
    }

    /// Return the next `batch.num_rows()` values from filter_results
    fn evaluate(&mut self, batch: RecordBatch) -> Result<BooleanArray, ArrowError> {
        let num_rows = batch.num_rows();
        let result = self.filter_results.slice(0, num_rows);
        self.filter_results = self
            .filter_results
            .slice(num_rows, self.filter_results.len() - num_rows);
        Ok(result)
    }
}

/// Ensures the parquet file generated by `three_column_parquet` has the expected
/// layout (number of row groups, pages, etc.).
///
/// See diagram on [`Test`] for expected layout.
#[test]
fn test_verify_three_column_parquet_layout() {
    let parquet = three_column_parquet();
    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Required)
        .parse_and_finish(&parquet)
        .unwrap();
    // assume a single row group
    assert_eq!(metadata.num_row_groups(), 1);

    let int8_locations = page_locations(0, 0, &metadata);
    assert_eq!(
        page_row_offsets(int8_locations),
        [0, 532],
        "int8 locations: {int8_locations:?}"
    );

    let int64_locations = page_locations(0, 1, &metadata);
    assert_eq!(
        page_row_offsets(int64_locations),
        [0, 264, 528, 792],
        "int64 locations: {int64_locations:?}"
    );

    let utf8_locations = page_locations(0, 2, &metadata);
    assert_eq!(
        page_row_offsets(utf8_locations),
        [0, 184, 359, 534, 708, 883],
        "utf8 locations: {utf8_locations:?}"
    );
}

/// returns a vector of the [`PageLocation`]s for each data page in the specified column
fn page_locations(
    row_group_index: usize,
    column_index: usize,
    metadata: &ParquetMetaData,
) -> &[PageLocation] {
    let offset_index = metadata.offset_index().expect("No offset index");
    offset_index[row_group_index][column_index].page_locations()
}

/// returns a vector of row offsets for each of the location pages
fn page_row_offsets(page_locations: &[PageLocation]) -> Vec<i64> {
    page_locations
        .iter()
        .map(|loc| loc.first_row_index)
        .collect()
}

/// Return a parquet file with three columns: int8, int64, utf8view
///
/// See comments on [Test]` for the layout of this parquet file.
fn three_column_parquet() -> Bytes {
    static PARQUET_BYTES: OnceLock<Bytes> = OnceLock::new();
    PARQUET_BYTES
        .get_or_init(|| {
            let batch = three_column_batch();
            // Configure writer properties
            let props = WriterProperties::builder()
                .set_dictionary_enabled(false)
                // We can't set datapage limits per column, so use different sized columns
                // to achieve the desired layout
                .set_data_page_size_limit(2000) // bytes
                .set_compression(Compression::UNCOMPRESSED)
                .set_write_batch_size(1)
                .build();

            let mut output = vec![];
            let mut writer =
                ArrowWriter::try_new(&mut output, batch.schema(), Some(props)).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
            Bytes::from(output)
        })
        .clone()
}

fn three_column_batch() -> RecordBatch {
    static BATCH: OnceLock<RecordBatch> = OnceLock::new();
    BATCH
        .get_or_init(|| {
            let int8_values = (0..1000).map(|x| {
                if x % 17 == 0 {
                    None
                } else {
                    Some((x % 256) as i8)
                }
            });
            let int8: ArrayRef = Arc::new(int8_values.collect::<Int8Array>());

            let int64_values = (0..1000).map(|x| {
                if x % 19 == 0 {
                    None
                } else {
                    Some(x as i64 * 10)
                }
            });
            let int64: ArrayRef = Arc::new(int64_values.collect::<Int64Array>());

            let utf8_values = (0..1000)
                .map(|x| {
                    if x % 23 == 0 {
                        None
                    } else {
                        Some(format!("Row {}:", x))
                    }
                })
                .collect::<StringViewArray>();

            let utf8view: ArrayRef = Arc::new(StringViewArray::from(utf8_values));

            RecordBatch::try_from_iter(vec![
                ("int8", int8),
                ("int64", int64),
                ("utf8view", utf8view),
            ])
            .unwrap()
        })
        .clone()
}
