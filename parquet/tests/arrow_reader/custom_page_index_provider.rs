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

//! End-to-end test for custom PageIndexProvider implementation.
//!
//! This test validates reading with a custom PageIndexProvider through the
//! actual read path, ensuring that:
//! - Columns with page indexes use offset-index-driven fetch
//! - Columns without page indexes fall back to whole-column-chunk fetching
//! - Correct results are returned in both cases

use arrow::compute::concat_batches;
use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use parquet::DecodeResult;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection,
    RowSelectionPolicy,
};
use parquet::arrow::push_decoder::ParquetPushDecoderBuilder;
use parquet::file::metadata::page_index::PageIndexProvider;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData};
use parquet::file::page_index::column_index::ColumnIndexMetaData;
use parquet::file::page_index::index_reader::{decode_column_index, decode_offset_index};
use parquet::file::page_index::offset_index::OffsetIndexMetaData;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn test_read_with_custom_page_index_provider_sync() {
    run_test(Reader::Sync);
}

// this will fail until https://github.com/apache/arrow-rs/pull/11182 is merged
#[test]
#[should_panic(
    expected = r#"called `Result::unwrap()` on an `Err` value: General("Invalid column index 2, column was not fetched")"#
)]
fn test_read_with_custom_page_index_provider_push() {
    run_test(Reader::Push);
}

// this will fail until https://github.com/apache/arrow-rs/pull/11182 is merged
#[cfg(feature = "async")]
#[test]
#[should_panic(
    expected = r#"called `Result::unwrap()` on an `Err` value: General("Invalid column index 2, column was not fetched")"#
)]
fn test_read_with_custom_page_index_provider_async() {
    run_test(Reader::Async);
}

#[derive(Debug, Clone, Copy)]
enum Reader {
    Sync,
    Push,
    #[cfg(feature = "async")]
    Async,
}

/// Read `file` with `reader`, using `metadata` and `selection`
fn read(
    reader: Reader,
    file: Bytes,
    metadata: ArrowReaderMetadata,
    selection: RowSelection,
    requested: &mut Vec<Range<u64>>,
) -> Vec<RecordBatch> {
    match reader {
        Reader::Sync => ParquetRecordBatchReaderBuilder::new_with_metadata(file, metadata)
            .with_row_selection(selection)
            .with_row_selection_policy(RowSelectionPolicy::Selectors)
            .build()
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap(),
        Reader::Push => {
            let mut decoder = ParquetPushDecoderBuilder::new_with_metadata(metadata)
                .with_row_selection(selection)
                .with_row_selection_policy(RowSelectionPolicy::Selectors)
                .build()
                .unwrap();
            let mut batches = vec![];
            loop {
                match decoder.try_decode().unwrap() {
                    DecodeResult::NeedsData(ranges) => {
                        requested.extend(ranges.iter().cloned());
                        let data = ranges
                            .iter()
                            .map(|r| file.slice(r.start as usize..r.end as usize))
                            .collect();
                        decoder.push_ranges(ranges, data).unwrap();
                    }
                    DecodeResult::Data(batch) => batches.push(batch),
                    DecodeResult::Finished => return batches,
                }
            }
        }
        #[cfg(feature = "async")]
        Reader::Async => {
            use futures::TryStreamExt;
            let input = std::io::Cursor::new(file.to_vec());
            let rt = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            rt.block_on(async {
                parquet::arrow::ParquetRecordBatchStreamBuilder::new_with_metadata(input, metadata)
                    .with_row_selection(selection)
                    .with_row_selection_policy(RowSelectionPolicy::Selectors)
                    .build()
                    .unwrap()
                    .try_collect()
                    .await
                    .unwrap()
            })
        }
    }
}

fn run_test(reader: Reader) {
    // Step 1: Write a parquet file with page indexes
    let file_bytes = create_test_file();

    // Step 2: Load metadata WITHOUT page indexes initially
    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(
        file_bytes.clone(),
        ArrowReaderOptions::default().with_page_index_policy(PageIndexPolicy::Skip),
    )
    .unwrap();

    let metadata = builder.metadata().as_ref();

    // Verify we have multiple row groups
    assert_eq!(metadata.num_row_groups(), 3);

    // Step 3: Create a custom provider and populate it selectively
    // Simulate a scenario where:
    // - We only populate indexes for row groups 0 and 2 (skipping row group 1)
    // - For row group 0: populate column 0 (id) and column 1 (value)
    // - For row group 2: populate columns 0 (id) and 2 (name), so column 1 is a gap
    let provider = SelectivePageIndexProvider::new(
        file_bytes.clone(),
        metadata,
        &[(0, 0), (0, 1), (2, 0), (2, 2)],
    );

    // Step 4: Install the custom provider into metadata
    let mut metadata_builder = metadata.clone().into_builder();
    metadata_builder = metadata_builder.set_page_index(Some(Arc::new(provider)));
    let metadata_with_custom_index = Arc::new(metadata_builder.build());

    // Step 5: Create ArrowReaderMetadata with the custom page index
    let arrow_metadata = ArrowReaderMetadata::try_new(
        metadata_with_custom_index.clone(),
        ArrowReaderOptions::default(),
    )
    .unwrap();

    // Step 6: Read data with RowSelection that triggers page skipping

    // Create a RowSelection that:
    // - Selects rows 20-30 (in row group 0)
    // - Selects rows 60-70 (in row group 1)
    // - Selects rows 120-130 (in row group 2)
    let selection = RowSelection::from(vec![
        // Skip first 20 rows
        parquet::arrow::arrow_reader::RowSelector::skip(20),
        // Select rows 20-30
        parquet::arrow::arrow_reader::RowSelector::select(10),
        // Skip rows 30-60
        parquet::arrow::arrow_reader::RowSelector::skip(30),
        // Select rows 60-70
        parquet::arrow::arrow_reader::RowSelector::select(10),
        // Skip rows 70-120
        parquet::arrow::arrow_reader::RowSelector::skip(50),
        // Select rows 120-130
        parquet::arrow::arrow_reader::RowSelector::select(10),
    ]);

    // Collect all batches
    let mut requested = Vec::<Range<u64>>::new();
    let batches = read(
        reader,
        file_bytes.clone(),
        arrow_metadata,
        selection,
        &mut requested,
    );

    // Step 7: Check statistics

    // only the push test will populate requested
    if matches!(reader, Reader::Push) {
        let fetched = |rg: usize, col: usize| -> u64 {
            let (start, len) = metadata.row_group(rg).column(col).byte_range();
            requested
                .iter()
                .filter(|r| r.start >= start && r.end <= start + len)
                .map(|r| r.end - r.start)
                .sum()
        };
        let chunk_len = |rg: usize, col: usize| metadata.row_group(rg).column(col).byte_range().1;
        assert!(fetched(0, 0) < chunk_len(0, 0)); // offset index: selected pages only
        assert_eq!(fetched(0, 2), chunk_len(0, 2)); // no offset index: whole chunk
    }

    // Note: We need to get the provider reference from the metadata to check stats
    let provider_ref = metadata_with_custom_index
        .page_index()
        .unwrap()
        .as_any()
        .downcast_ref::<SelectivePageIndexProvider>()
        .expect("Expected SelectivePageIndexProvider");

    // Check that the custom provider was used
    let (hits, misses) = provider_ref.stats();
    assert!(hits > 0, "provider should have hits");
    assert!(misses > 0, "provider should have misses");

    // Since we didn't use predicates, the column index should be untouched
    let (col_hits, col_misses) = provider_ref.column_index_stats();
    assert_eq!(col_hits, 0, "column index should not be used");
    assert_eq!(col_misses, 0, "column index should not be used");

    let (off_hits, off_misses) = provider_ref.offset_index_stats();
    assert!(off_hits > 0, "offset index should have hits");
    assert!(off_misses > 0, "offset index should have misses");

    // Step 8: Verify correct results
    // We expect 30 rows total: 10 from each selected range
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 30);

    // Expected values for the selected rows:
    // rows 20-30: id=20..30, value=40..60, score=60..90
    // rows 60-70: id=60..70, value=120..140, score=180..210
    // rows 120-130: id=120..130, value=240..260, score=360..390
    let ids: Vec<i32> = (20..30).chain(60..70).chain(120..130).collect();
    let expected = make_batch(&ids);
    assert_eq!(
        concat_batches(&expected.schema(), &batches).unwrap(),
        expected
    );
}

/// A custom PageIndexProvider that only stores indexes for a subset of columns
///
/// This provider mimics a real-world scenario where an application has loaded
/// page indexes selectively based on query predicates and projections.
#[derive(Debug)]
struct SelectivePageIndexProvider {
    // indexes are accessed first by row_group index and then by column index
    column_indexes: Option<HashMap<(usize, usize), ColumnIndexMetaData>>,
    offset_indexes: Option<HashMap<(usize, usize), OffsetIndexMetaData>>,
    // Usage statistics
    column_index_hits: AtomicUsize,
    column_index_misses: AtomicUsize,
    offset_index_hits: AtomicUsize,
    offset_index_misses: AtomicUsize,
}

impl SelectivePageIndexProvider {
    fn new(file_bytes: Bytes, metadata: &ParquetMetaData, chunks: &[(usize, usize)]) -> Self {
        let mut column_indexes = HashMap::new();
        let mut offset_indexes = HashMap::new();
        for &(rg, col) in chunks {
            column_indexes.insert(
                (rg, col),
                Self::fetch_column_index(rg, col, metadata, &file_bytes)
                    .unwrap()
                    .unwrap(),
            );
            offset_indexes.insert(
                (rg, col),
                Self::fetch_offset_index(rg, col, metadata, &file_bytes)
                    .unwrap()
                    .unwrap(),
            );
        }
        Self {
            column_indexes: Some(column_indexes),
            offset_indexes: Some(offset_indexes),
            column_index_hits: AtomicUsize::new(0),
            column_index_misses: AtomicUsize::new(0),
            offset_index_hits: AtomicUsize::new(0),
            offset_index_misses: AtomicUsize::new(0),
        }
    }

    /// Get statistics about column index usage
    fn column_index_stats(&self) -> (usize, usize) {
        (
            self.column_index_hits.load(Ordering::Relaxed),
            self.column_index_misses.load(Ordering::Relaxed),
        )
    }

    /// Get statistics about offset index usage
    fn offset_index_stats(&self) -> (usize, usize) {
        (
            self.offset_index_hits.load(Ordering::Relaxed),
            self.offset_index_misses.load(Ordering::Relaxed),
        )
    }

    /// Get combined statistics as (hits, misses) for both index types
    fn stats(&self) -> (usize, usize) {
        let (col_hits, col_misses) = self.column_index_stats();
        let (off_hits, off_misses) = self.offset_index_stats();
        (col_hits + off_hits, col_misses + off_misses)
    }

    /// Fetch and parse the column index for the given row group and column
    fn fetch_column_index(
        row_group_idx: usize,
        column_idx: usize,
        metadata: &ParquetMetaData,
        file_bytes: &Bytes,
    ) -> parquet::errors::Result<Option<ColumnIndexMetaData>> {
        let column = metadata.row_group(row_group_idx).column(column_idx);
        let range = column.column_index_range();
        if let Some(range) = range {
            let idx_bytes = file_bytes.slice(range.start as usize..range.end as usize);
            Ok(Some(decode_column_index(&idx_bytes, column.column_type())?))
        } else {
            Ok(None)
        }
    }

    /// Fetch and parse the offset index for the given row group and column
    fn fetch_offset_index(
        row_group_idx: usize,
        column_idx: usize,
        metadata: &ParquetMetaData,
        file_bytes: &Bytes,
    ) -> parquet::errors::Result<Option<OffsetIndexMetaData>> {
        let column = metadata.row_group(row_group_idx).column(column_idx);
        let range = column.offset_index_range();
        if let Some(range) = range {
            let idx_bytes = file_bytes.slice(range.start as usize..range.end as usize);
            Ok(Some(decode_offset_index(&idx_bytes)?))
        } else {
            Ok(None)
        }
    }
}

impl PageIndexProvider for SelectivePageIndexProvider {
    fn has_offset_indexes(&self) -> bool {
        self.offset_indexes.is_some()
    }

    fn has_column_indexes(&self) -> bool {
        self.column_indexes.is_some()
    }

    fn column_index(
        &self,
        row_group_idx: usize,
        column_idx: usize,
    ) -> Option<&ColumnIndexMetaData> {
        let result = self
            .column_indexes
            .as_ref()?
            .get(&(row_group_idx, column_idx));

        if result.is_some() {
            self.column_index_hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.column_index_misses.fetch_add(1, Ordering::Relaxed);
        }

        result
    }

    fn offset_index(
        &self,
        row_group_idx: usize,
        column_idx: usize,
    ) -> Option<&OffsetIndexMetaData> {
        let result = self
            .offset_indexes
            .as_ref()?
            .get(&(row_group_idx, column_idx));

        if result.is_some() {
            self.offset_index_hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.offset_index_misses.fetch_add(1, Ordering::Relaxed);
        }

        result
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Create a test parquet file with multiple row groups and multiple pages per column
fn create_test_file() -> Bytes {
    // Create small pages and small row groups for testing
    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_data_page_row_count_limit(10) // Small pages for multiple pages per column
        .set_write_batch_size(10)
        .set_max_row_group_row_count(Some(50)) // Small row groups
        .build();

    // Write 3 row groups with 50 rows each (150 rows total)
    let batch = make_batch((0..150).collect::<Vec<i32>>().as_slice());
    let mut buffer = Vec::with_capacity(1024);
    let mut writer =
        ArrowWriter::try_new(&mut buffer, batch.schema().clone(), Some(props)).unwrap();
    writer.write(&batch).unwrap();

    // check that file has properties we wanted (3 row groups, 5 pages per chunk)
    let metadata = writer.close().unwrap();
    assert_eq!(metadata.num_row_groups(), 3);
    assert!(metadata.page_index().is_some());
    let page_index = metadata.page_index().unwrap();
    for row_group_idx in 0..3 {
        for column_idx in 0..4 {
            assert!(page_index.column_index(row_group_idx, column_idx).is_some());
            assert!(page_index.offset_index(row_group_idx, column_idx).is_some());
            assert!(
                page_index
                    .num_data_pages(row_group_idx, column_idx)
                    .is_some_and(|n| n == 5)
            );
        }
    }
    Bytes::from(buffer)
}

fn make_batch(ids: &[i32]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("score", DataType::Int32, false),
    ]));
    // Write 3 row groups with 50 rows each (150 rows total)
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(Int32Array::from(
                ids.iter().map(|x| x * 2).collect::<Vec<i32>>(),
            )),
            Arc::new(StringArray::from(
                ids.iter()
                    .map(|x| format!("name{x}"))
                    .collect::<Vec<String>>(),
            )),
            Arc::new(Int32Array::from(
                ids.iter().map(|x| x * 3).collect::<Vec<i32>>(),
            )),
        ],
    )
    .unwrap()
}
