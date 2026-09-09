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

use arrow_array::{Array, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection,
    RowSelectionPolicy,
};
use parquet::file::metadata::page_index::PageIndexProvider;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData};
use parquet::file::page_index::column_index::ColumnIndexMetaData;
use parquet::file::page_index::index_reader::{decode_column_index, decode_offset_index};
use parquet::file::page_index::offset_index::OffsetIndexMetaData;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fs::File;
use std::sync::Arc;
use tempfile::NamedTempFile;

/// A custom PageIndexProvider that only stores indexes for a subset of columns
///
/// This provider mimics a real-world scenario where an application has loaded
/// page indexes selectively based on query predicates and projections.
#[derive(Debug, Clone)]
struct SelectivePageIndexProvider {
    metadata: ParquetMetaData,
    file_bytes: Bytes,
    // indexes are accessed first by row_group index and then by column index
    column_indexes: Option<HashMap<usize, HashMap<usize, ColumnIndexMetaData>>>,
    offset_indexes: Option<HashMap<usize, HashMap<usize, OffsetIndexMetaData>>>,
}

impl SelectivePageIndexProvider {
    fn new(metadata: ParquetMetaData, file_bytes: Bytes) -> Self {
        Self {
            metadata,
            file_bytes,
            column_indexes: None,
            offset_indexes: None,
        }
    }

    /// Fetch and parse the column index for the given row group and column
    fn fetch_column_index(
        &mut self,
        row_group_idx: usize,
        column_idx: usize,
    ) -> parquet::errors::Result<()> {
        let map = self.column_indexes.get_or_insert_with(HashMap::new);
        let rg = map.entry(row_group_idx).or_default();
        if let Entry::Vacant(e) = rg.entry(column_idx) {
            let column = self.metadata.row_group(row_group_idx).column(column_idx);
            let range = column.column_index_range();
            if let Some(range) = range {
                let idx_bytes = self
                    .file_bytes
                    .slice(range.start as usize..range.end as usize);
                let idx = decode_column_index(&idx_bytes, column.column_type())?;
                e.insert(idx);
            }
        }
        Ok(())
    }

    /// Fetch and parse the offset index for the given row group and column
    fn fetch_offset_index(
        &mut self,
        row_group_idx: usize,
        column_idx: usize,
    ) -> parquet::errors::Result<()> {
        let map = self.offset_indexes.get_or_insert_with(HashMap::new);
        let rg = map.entry(row_group_idx).or_default();
        if let Entry::Vacant(e) = rg.entry(column_idx) {
            let column = self.metadata.row_group(row_group_idx).column(column_idx);
            let range = column.offset_index_range();
            if let Some(range) = range {
                let idx_bytes = self
                    .file_bytes
                    .slice(range.start as usize..range.end as usize);
                let idx = decode_offset_index(&idx_bytes)?;
                e.insert(idx);
            }
        }
        Ok(())
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
        self.column_indexes
            .as_ref()?
            .get(&row_group_idx)?
            .get(&column_idx)
    }

    fn offset_index(
        &self,
        row_group_idx: usize,
        column_idx: usize,
    ) -> Option<&OffsetIndexMetaData> {
        self.offset_indexes
            .as_ref()?
            .get(&row_group_idx)?
            .get(&column_idx)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Create a test parquet file with multiple row groups and multiple pages per column
fn create_test_file() -> NamedTempFile {
    let temp_file = tempfile::Builder::new()
        .prefix("custom_page_index_test")
        .suffix(".parquet")
        .tempfile()
        .expect("tempfile creation");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("score", DataType::Int32, false),
    ]));

    // Create small pages and small row groups for testing
    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_data_page_row_count_limit(10) // Small pages for multiple pages per column
        .set_write_batch_size(10)
        .set_max_row_group_row_count(Some(50)) // Small row groups
        .build();

    let file = temp_file.reopen().unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

    // Write 3 row groups with 50 rows each (150 rows total)
    for row_group in 0..3 {
        for batch_num in 0..5 {
            let offset = (row_group * 50) + (batch_num * 10);
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(
                        (offset..offset + 10).collect::<Vec<i32>>(),
                    )),
                    Arc::new(Int32Array::from(
                        (offset..offset + 10).map(|x| x * 2).collect::<Vec<i32>>(),
                    )),
                    Arc::new(StringArray::from(
                        (offset..offset + 10)
                            .map(|x| format!("name{x}"))
                            .collect::<Vec<String>>(),
                    )),
                    Arc::new(Int32Array::from(
                        (offset..offset + 10).map(|x| x * 3).collect::<Vec<i32>>(),
                    )),
                ],
            )
            .unwrap();
            writer.write(&batch).unwrap();
        }
        writer.flush().unwrap();
    }

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
    temp_file
}

#[test]
fn test_read_with_custom_page_index_provider() {
    // Step 1: Write a parquet file with page indexes
    let temp_file = create_test_file();
    let file_bytes = Bytes::from(std::fs::read(temp_file.path()).unwrap());

    // Step 2: Load metadata WITHOUT page indexes initially
    let file = File::open(temp_file.path()).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(
        file,
        ArrowReaderOptions::default().with_page_index_policy(PageIndexPolicy::Skip),
    )
    .unwrap();

    let metadata = builder.metadata().clone();

    // Verify we have multiple row groups
    assert_eq!(metadata.num_row_groups(), 3);

    // Step 3: Create a custom provider and populate it selectively
    // Simulate a scenario where:
    // - We only populate indexes for row groups 0 and 2 (skipping row group 1)
    // - For row group 0: populate column 0 (id) and column 1 (value)
    // - For row group 2: populate column 0 (id) only
    let mut provider = SelectivePageIndexProvider::new(metadata.as_ref().clone(), file_bytes);

    // Populate indexes for row group 0, columns 0 and 1
    provider.fetch_column_index(0, 0).unwrap();
    provider.fetch_offset_index(0, 0).unwrap();
    provider.fetch_column_index(0, 1).unwrap();
    provider.fetch_offset_index(0, 1).unwrap();

    // Populate indexes for row group 2, column 0 only
    provider.fetch_column_index(2, 0).unwrap();
    provider.fetch_offset_index(2, 0).unwrap();

    // Verify the provider has the expected indexes
    assert!(provider.column_index(0, 0).is_some());
    assert!(provider.offset_index(0, 0).is_some());
    assert!(provider.column_index(0, 1).is_some());
    assert!(provider.offset_index(0, 1).is_some());
    assert!(provider.column_index(0, 2).is_none()); // Not populated
    assert!(provider.column_index(2, 0).is_some());
    assert!(provider.column_index(2, 1).is_none()); // Not populated

    // Step 4: Install the custom provider into metadata
    let mut metadata_builder = metadata.as_ref().clone().into_builder();
    metadata_builder = metadata_builder.set_page_index(Some(Arc::new(provider)));
    let metadata_with_custom_index = Arc::new(metadata_builder.build());

    // Step 5: Create ArrowReaderMetadata with the custom page index
    let arrow_metadata =
        ArrowReaderMetadata::try_new(metadata_with_custom_index, ArrowReaderOptions::default())
            .unwrap();

    // Step 6: Read data with RowSelection that triggers page skipping
    let file = File::open(temp_file.path()).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(file, arrow_metadata);

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

    let reader = builder
        .with_row_selection(selection)
        // make sure we're actually using the page index and not using a mask
        .with_row_selection_policy(RowSelectionPolicy::Selectors)
        .build()
        .unwrap();

    // Collect all batches
    let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>().unwrap();

    // Step 7: Verify correct results
    // We expect 30 rows total: 10 from each selected range
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 30);

    // Verify the actual data values
    let mut all_ids = Vec::new();
    let mut all_values = Vec::new();
    let mut all_names = Vec::new();
    let mut all_scores = Vec::new();

    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let names = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let scores = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        all_ids.extend(ids.iter().flatten());
        all_values.extend(values.iter().flatten());
        all_names.extend(names.iter().flatten().map(|s| s.to_owned()));
        all_scores.extend(scores.iter().flatten());
    }

    // Expected values for the selected rows:
    // rows 20-30: id=20..30, value=40..60, score=60..90
    // rows 60-70: id=60..70, value=120..140, score=180..210
    // rows 120-130: id=120..130, value=240..260, score=360..390
    let expected_ids: Vec<i32> = (20..30).chain(60..70).chain(120..130).collect();
    let expected_values: Vec<i32> = expected_ids.iter().map(|x| x * 2).collect();
    let expected_scores: Vec<i32> = expected_ids.iter().map(|x| x * 3).collect();
    let expected_names: Vec<String> = expected_ids.iter().map(|x| format!("name{x}")).collect();

    assert_eq!(all_ids, expected_ids, "IDs don't match expected values");
    assert_eq!(
        all_values, expected_values,
        "Values don't match expected values"
    );
    assert_eq!(
        all_scores, expected_scores,
        "Scores don't match expected values"
    );
    assert_eq!(
        all_names, expected_names,
        "Names don't match expected values"
    );

    // Success! The test validates that:
    // 1. For row group 0, columns 0 and 1 had page indexes and could use page-level skipping
    // 2. For row group 0, columns 2 and 3 had no indexes but still returned correct data
    // 3. For row group 1, no columns had indexes but all returned correct data
    // 4. For row group 2, column 0 had indexes and others didn't, but all returned correct data
    // 5. The reader correctly handled the mix of indexed and non-indexed columns
}

#[test]
fn test_read_all_columns_with_partial_indexes() {
    // This test focuses on verifying that columns without page indexes
    // fall back to whole-column-chunk fetching and return correct results

    let temp_file = create_test_file();
    let file_bytes = Bytes::from(std::fs::read(temp_file.path()).unwrap());

    // Load metadata without page indexes
    let file = File::open(temp_file.path()).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(
        file,
        ArrowReaderOptions::default().with_page_index_policy(PageIndexPolicy::Skip),
    )
    .unwrap();

    let metadata = builder.metadata().clone();

    // Create provider with indexes ONLY for column 0 in all row groups
    let mut provider = SelectivePageIndexProvider::new(metadata.as_ref().clone(), file_bytes);

    for row_group_idx in 0..metadata.num_row_groups() {
        provider.fetch_column_index(row_group_idx, 0).unwrap();
        provider.fetch_offset_index(row_group_idx, 0).unwrap();
    }

    // Install provider
    let mut metadata_builder = metadata.as_ref().clone().into_builder();
    metadata_builder = metadata_builder.set_page_index(Some(Arc::new(provider)));
    let metadata_with_custom_index = Arc::new(metadata_builder.build());

    // Create ArrowReaderMetadata with the custom page index
    let arrow_metadata =
        ArrowReaderMetadata::try_new(metadata_with_custom_index, ArrowReaderOptions::default())
            .unwrap();

    // Read with row selection
    let file = File::open(temp_file.path()).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(file, arrow_metadata);

    // Select rows 10-20 from row group 0
    let selection = RowSelection::from(vec![
        parquet::arrow::arrow_reader::RowSelector::skip(10),
        parquet::arrow::arrow_reader::RowSelector::select(10),
    ]);

    let reader = builder
        .with_row_selection(selection)
        // make sure we're actually using the page index and not using a mask
        .with_row_selection_policy(RowSelectionPolicy::Selectors)
        .build()
        .unwrap();
    let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>().unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 10);

    // Verify all columns have correct data
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let names = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let scores = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        for i in 0..batch.num_rows() {
            let id = ids.value(i);
            assert_eq!(values.value(i), id * 2);
            assert_eq!(names.value(i), format!("name{id}"));
            assert_eq!(scores.value(i), id * 3);
        }
    }
}

#[test]
fn test_custom_provider_with_no_row_selection() {
    // Test that reading without RowSelection also works correctly
    // with a custom page index provider

    let temp_file = create_test_file();
    let file_bytes = Bytes::from(std::fs::read(temp_file.path()).unwrap());

    let file = File::open(temp_file.path()).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(
        file,
        ArrowReaderOptions::default().with_page_index_policy(PageIndexPolicy::Skip),
    )
    .unwrap();

    let metadata = builder.metadata().clone();

    // Create provider with indexes for first column only
    let mut provider = SelectivePageIndexProvider::new(metadata.as_ref().clone(), file_bytes);
    provider.fetch_column_index(0, 0).unwrap();
    provider.fetch_offset_index(0, 0).unwrap();

    let mut metadata_builder = metadata.as_ref().clone().into_builder();
    metadata_builder = metadata_builder.set_page_index(Some(Arc::new(provider)));
    let metadata_with_custom_index = Arc::new(metadata_builder.build());

    // Create ArrowReaderMetadata with the custom page index
    let arrow_metadata =
        ArrowReaderMetadata::try_new(metadata_with_custom_index, ArrowReaderOptions::default())
            .unwrap();

    // Read all data
    let file = File::open(temp_file.path()).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(file, arrow_metadata);

    let reader = builder.build().unwrap();

    let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>().unwrap();

    // Should read all 150 rows
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 150);

    // Verify data integrity
    let mut row_count: i32 = 0;
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let names = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let scores = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        for i in 0..batch.num_rows() {
            assert_eq!(ids.value(i), row_count);
            assert_eq!(values.value(i), row_count * 2);
            assert_eq!(names.value(i), format!("name{row_count}"));
            assert_eq!(scores.value(i), row_count * 3);
            row_count += 1;
        }
    }
}
