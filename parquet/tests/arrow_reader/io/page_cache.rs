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

//! Tests for the page cache in async reader ([`ParquetRecordBatchStreamBuilder`])

use super::{
    async_reader::{run, RecordingAsyncFileReader},
    filter_b_575_625, test_file, test_options, TestParquetFile,
};
use parquet::arrow::arrow_reader::{ArrowReaderOptions, RowSelection, RowSelector};
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::ProjectionMask;
use std::sync::Arc;

#[tokio::test]
async fn test_page_cache_miss_then_hit() {
    // Test explicit cache miss followed by cache hit pattern
    let test_file = test_file();
    let options = test_options();

    // First reader - should be cache miss
    let builder1 = async_builder(&test_file, options.clone(), true).await;
    let schema_descr = builder1.metadata().file_metadata().schema_descr_ptr();
    let builder1 = builder1
        .with_projection(ProjectionMask::columns(&schema_descr, ["b"]))
        .with_row_groups(vec![0]);

    insta::assert_debug_snapshot!(run(&test_file, builder1).await, @r#"
    [
        "Get Provided Metadata",
        "Event: Builder Configured",
        "Event: Reader Built",
        "Read Multi:",
        "  Row Group 0, column 'b': MultiPage(dictionary_page: true, data_pages: [0, 1])  (1856 bytes, 1 requests) [data]",
    ]
    "#);

    // Second reader - should be cache hit (only dictionary page read, data pages cached)
    // Clear the operation log to isolate the second test
    test_file.ops().ops.lock().unwrap().clear();
    let builder2 = async_builder(&test_file, options, true).await;
    let schema_descr = builder2.metadata().file_metadata().schema_descr_ptr();
    let builder2 = builder2
        .with_projection(ProjectionMask::columns(&schema_descr, ["b"]))
        .with_row_groups(vec![0]);

    insta::assert_debug_snapshot!(run(&test_file, builder2).await, @r#"
    [
        "Get Provided Metadata",
        "Event: Builder Configured",
        "Event: Reader Built",
        "Read Multi:",
        "  Row Group 0, column 'b': MultiPage(dictionary_page: true, data_pages: [0, 1])  (1856 bytes, 1 requests) [data]",
    ]
    "#);
}

#[tokio::test]
async fn test_page_cache_disabled() {
    // When cache is disabled, each read should go to storage
    let test_file = test_file();
    let options = test_options(); // Disabled cache
    let builder = async_builder(&test_file, options.clone(), false).await;
    let schema_descr = builder.metadata().file_metadata().schema_descr_ptr();

    let builder = builder
        .with_projection(ProjectionMask::columns(&schema_descr, ["a"]))
        .with_row_groups(vec![0]);

    // First read
    insta::assert_debug_snapshot!(run(&test_file, builder).await, @r#"
    [
        "Get Provided Metadata",
        "Event: Builder Configured",
        "Event: Reader Built",
        "Read Multi:",
        "  Row Group 0, column 'a': MultiPage(dictionary_page: true, data_pages: [0, 1])  (1856 bytes, 1 requests) [data]",
    ]
    "#);

    // Second read - should still read from storage (cache disabled)
    test_file.ops().ops.lock().unwrap().clear();
    let builder2 = async_builder(&test_file, options, false).await;
    let schema_descr = builder2.metadata().file_metadata().schema_descr_ptr();
    let builder2 = builder2
        .with_projection(ProjectionMask::columns(&schema_descr, ["a"]))
        .with_row_groups(vec![0]);
    insta::assert_debug_snapshot!(run(&test_file, builder2).await, @r#"
    [
        "Get Provided Metadata",
        "Event: Builder Configured",
        "Event: Reader Built",
        "Read Multi:",
        "  Row Group 0, column 'a': MultiPage(dictionary_page: true, data_pages: [0, 1])  (1856 bytes, 1 requests) [data]",
    ]
    "#);
}

#[tokio::test]
async fn test_page_cache_same_pages_different_projections() {
    // Reading same pages through different projections should hit cache
    let test_file = test_file();
    let options = test_options();

    // First read - columns 'a' and 'b'
    let builder1 = async_builder(&test_file, options.clone(), true).await;
    let schema_descr = builder1.metadata().file_metadata().schema_descr_ptr();
    let builder1 = builder1
        .with_projection(ProjectionMask::columns(&schema_descr, ["a", "b"]))
        .with_row_groups(vec![0]);

    insta::assert_debug_snapshot!(run(&test_file, builder1).await, @r#"
    [
        "Get Provided Metadata",
        "Event: Builder Configured",
        "Event: Reader Built",
        "Read Multi:",
        "  Row Group 0, column 'a': MultiPage(dictionary_page: true, data_pages: [0, 1])  (1856 bytes, 1 requests) [data]",
        "  Row Group 0, column 'b': MultiPage(dictionary_page: true, data_pages: [0, 1])  (1856 bytes, 1 requests) [data]",
    ]
    "#);

    // Second read - only column 'a' (should hit cache for data pages, read dictionary)
    test_file.ops().ops.lock().unwrap().clear();
    let builder2 = async_builder(&test_file, options, true).await;
    let schema_descr = builder2.metadata().file_metadata().schema_descr_ptr();
    let builder2 = builder2
        .with_projection(ProjectionMask::columns(&schema_descr, ["a"]))
        .with_row_groups(vec![0]);

    insta::assert_debug_snapshot!(run(&test_file, builder2).await, @r#"
    [
        "Get Provided Metadata",
        "Event: Builder Configured",
        "Event: Reader Built",
        "Read Multi:",
        "  Row Group 0, column 'a': MultiPage(dictionary_page: true, data_pages: [0, 1])  (1856 bytes, 1 requests) [data]",
    ]
    "#);
}

/// TODO: split out predicate phase and output phase, then test the benefit
#[tokio::test]
async fn test_page_cache_with_row_filter() {
    // Pages accessed through row filter should be cached
    let test_file = test_file();
    let options = test_options();

    // First read with filter
    let builder1 = async_builder(&test_file, options.clone(), true).await;
    let schema_descr = builder1.metadata().file_metadata().schema_descr_ptr();
    let builder1 = builder1
        .with_projection(ProjectionMask::columns(&schema_descr, ["a", "b"]))
        .with_row_filter(filter_b_575_625(&schema_descr));

    insta::assert_debug_snapshot!(run(&test_file, builder1).await, @r#"
    [
        "Get Provided Metadata",
        "Event: Builder Configured",
        "Event: Reader Built",
        "Read Multi:",
        "  Row Group 0, column 'b': MultiPage(dictionary_page: true, data_pages: [0, 1])  (1856 bytes, 1 requests) [data]",
        "Read Multi:",
        "  Row Group 0, column 'a': DictionaryPage   (1617 bytes, 1 requests) [data]",
        "  Row Group 0, column 'a': DataPage(1)      (126 bytes , 1 requests) [data]",
        "Read Multi:",
        "  Row Group 1, column 'b': MultiPage(dictionary_page: true, data_pages: [0, 1])  (1856 bytes, 1 requests) [data]",
        "Read Multi:",
        "  Row Group 1, column 'a': DictionaryPage   (1617 bytes, 1 requests) [data]",
        "  Row Group 1, column 'a': DataPage(0)      (113 bytes , 1 requests) [data]",
    ]
    "#);

    // Second read with same filter - should use cached data pages, read dictionary pages
    test_file.ops().ops.lock().unwrap().clear();
    let builder2 = async_builder(&test_file, options, true).await;
    let schema_descr = builder2.metadata().file_metadata().schema_descr_ptr();
    let builder2 = builder2
        .with_projection(ProjectionMask::columns(&schema_descr, ["a", "b"]))
        .with_row_filter(filter_b_575_625(&schema_descr));

    insta::assert_debug_snapshot!(run(&test_file, builder2).await, @r#"
    [
        "Get Provided Metadata",
        "Event: Builder Configured",
        "Event: Reader Built",
        "Read Multi:",
        "  Row Group 0, column 'b': MultiPage(dictionary_page: true, data_pages: [0, 1])  (1856 bytes, 1 requests) [data]",
        "Read Multi:",
        "  Row Group 0, column 'a': DictionaryPage   (1617 bytes, 1 requests) [data]",
        "  Row Group 0, column 'a': DataPage(1)      (126 bytes , 1 requests) [data]",
        "Read Multi:",
        "  Row Group 1, column 'b': MultiPage(dictionary_page: true, data_pages: [0, 1])  (1856 bytes, 1 requests) [data]",
        "Read Multi:",
        "  Row Group 1, column 'a': DictionaryPage   (1617 bytes, 1 requests) [data]",
        "  Row Group 1, column 'a': DataPage(0)      (113 bytes , 1 requests) [data]",
    ]
    "#);
}

#[tokio::test]
async fn test_page_cache_with_row_selection() {
    // Pages accessed through row selection should be cached
    let test_file = test_file();
    let options = test_options();

    // First read with row selection
    let builder1 = async_builder(&test_file, options.clone(), true).await;
    let schema_descr = builder1.metadata().file_metadata().schema_descr_ptr();
    let builder1 = builder1
        .with_projection(ProjectionMask::columns(&schema_descr, ["a", "b"]))
        .with_row_selection(RowSelection::from(vec![
            RowSelector::skip(175),
            RowSelector::select(50),
        ]));

    insta::assert_debug_snapshot!(run(&test_file, builder1).await, @r#"
    [
        "Get Provided Metadata",
        "Event: Builder Configured",
        "Event: Reader Built",
        "Read Multi:",
        "  Row Group 0, column 'a': DictionaryPage   (1617 bytes, 1 requests) [data]",
        "  Row Group 0, column 'a': DataPage(1)      (126 bytes , 1 requests) [data]",
        "  Row Group 0, column 'b': DictionaryPage   (1617 bytes, 1 requests) [data]",
        "  Row Group 0, column 'b': DataPage(1)      (126 bytes , 1 requests) [data]",
        "Read Multi:",
        "  Row Group 1, column 'a': DictionaryPage   (1617 bytes, 1 requests) [data]",
        "  Row Group 1, column 'a': DataPage(0)      (113 bytes , 1 requests) [data]",
        "  Row Group 1, column 'b': DictionaryPage   (1617 bytes, 1 requests) [data]",
        "  Row Group 1, column 'b': DataPage(0)      (113 bytes , 1 requests) [data]",
    ]
    "#);

    // Second read with same row selection - should use cached data pages, read dictionary pages
    test_file.ops().ops.lock().unwrap().clear();
    let builder2 = async_builder(&test_file, options, true).await;
    let schema_descr = builder2.metadata().file_metadata().schema_descr_ptr();
    let builder2 = builder2
        .with_projection(ProjectionMask::columns(&schema_descr, ["a", "b"]))
        .with_row_selection(RowSelection::from(vec![
            RowSelector::skip(175),
            RowSelector::select(50),
        ]));

    insta::assert_debug_snapshot!(run(&test_file, builder2).await, @r#"
    [
        "Get Provided Metadata",
        "Event: Builder Configured",
        "Event: Reader Built",
        "Read Multi:",
        "  Row Group 0, column 'a': DictionaryPage   (1617 bytes, 1 requests) [data]",
        "  Row Group 0, column 'a': DataPage(1)      (126 bytes , 1 requests) [data]",
        "  Row Group 0, column 'b': DictionaryPage   (1617 bytes, 1 requests) [data]",
        "  Row Group 0, column 'b': DataPage(1)      (126 bytes , 1 requests) [data]",
        "Read Multi:",
        "  Row Group 1, column 'a': DictionaryPage   (1617 bytes, 1 requests) [data]",
        "  Row Group 1, column 'a': DataPage(0)      (113 bytes , 1 requests) [data]",
        "  Row Group 1, column 'b': DictionaryPage   (1617 bytes, 1 requests) [data]",
        "  Row Group 1, column 'b': DataPage(0)      (113 bytes , 1 requests) [data]",
    ]
    "#);
}

use parquet::column::page::Page;
use parquet::file::page_cache::{PageCacheKey, PageCacheStrategy, ParquetContext};
use std::collections::HashMap;
use std::sync::Mutex;

/// Simple test page cache implementation  
struct TestPageCache {
    storage: Mutex<HashMap<PageCacheKey, Arc<Page>>>,
}

impl TestPageCache {
    fn new() -> Self {
        Self {
            storage: Mutex::new(HashMap::new()),
        }
    }
}

impl PageCacheStrategy for TestPageCache {
    fn get(&self, key: &PageCacheKey) -> Option<Arc<Page>> {
        println!("offset: {}", key.page_offset);
        self.storage.lock().unwrap().get(key).cloned()
    }

    fn put(&self, key: PageCacheKey, page: Arc<Page>) {
        self.storage.lock().unwrap().insert(key, page);
    }
}

/// Return a [`ParquetRecordBatchStreamBuilder`] for reading this file with page cache
async fn async_builder(
    test_file: &TestParquetFile,
    options: ArrowReaderOptions,
    page_cache_enabled: bool,
) -> ParquetRecordBatchStreamBuilder<RecordingAsyncFileReader> {
    // Set the global cache state based on test requirements
    if page_cache_enabled {
        // Enable cache by setting a default cache (will fail if already set, which is fine)
        let _ = ParquetContext::set_cache(Some(Arc::new(TestPageCache::new())));
    } else {
        // Disable cache by setting None (will fail if already set, which is fine)
        let _ = ParquetContext::set_cache(None);
    }

    let parquet_meta_data = if options.page_index() {
        Arc::clone(test_file.parquet_metadata())
    } else {
        // strip out the page index from the metadata
        let metadata = test_file
            .parquet_metadata()
            .as_ref()
            .clone()
            .into_builder()
            .set_column_index(None)
            .set_offset_index(None)
            .build();
        Arc::new(metadata)
    };

    let reader = RecordingAsyncFileReader {
        bytes: test_file.bytes().clone(),
        ops: Arc::clone(test_file.ops()),
        parquet_meta_data,
    };

    ParquetRecordBatchStreamBuilder::new_with_options(reader, options)
        .await
        .unwrap()
}
