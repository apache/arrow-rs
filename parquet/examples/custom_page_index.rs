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

//! Example of implementing a custom PageIndexProvider
//!
//! This example demonstrates how to create a custom page index provider that:
//! - Loads page indexes only for specified row groups and columns
//! - Caches decoded page indexes for reuse across queries
//! - Builds query-specific providers containing only the requested indexes
//! - Implements all required PageIndexProvider trait methods
//!
//! This approach can significantly reduce memory usage and reduce load time
//! when working with wide tables where only a few columns need page-level statistics.
//! This is particularly important for queries that filter on a predicate column
//! and project a small subset of columns.

use bytes::Bytes;
use parquet::DecodeResult;
use parquet::errors::{ParquetError, Result};
use parquet::file::metadata::page_index::PageIndexProvider;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataPushDecoder};
use parquet::file::page_index::column_index::ColumnIndexMetaData;
use parquet::file::page_index::index_reader::{decode_column_index, decode_offset_index};
use parquet::file::page_index::offset_index::OffsetIndexMetaData;
use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;

/// A custom [`PageIndexProvider`] containing indexes for a subset of row groups
/// and columns.
///
/// Loading is performed separately by [`SelectivePageIndexLoader`], so this
/// provider retains neither the Parquet footer metadata nor the file contents.
#[derive(Debug)]
struct SparsePageIndexProvider {
    column_indexes: HashMap<(usize, usize), Arc<ColumnIndexMetaData>>,
    offset_indexes: HashMap<(usize, usize), Arc<OffsetIndexMetaData>>,
}

/// A cache of decoded page indexes for a single Parquet file.
///
/// A cache shared by multiple files would also need to include a stable file
/// identity and freshness information in each key.
#[derive(Default)]
struct PageIndexCache {
    column_indexes: HashMap<(usize, usize), Arc<ColumnIndexMetaData>>,
    offset_indexes: HashMap<(usize, usize), Arc<OffsetIndexMetaData>>,
}

/// Loads selected page indexes and constructs a [`SparsePageIndexProvider`].
struct SelectivePageIndexLoader<'a> {
    metadata: &'a ParquetMetaData,
    file_bytes: &'a Bytes,
    cache: &'a mut PageIndexCache,
    provider: SparsePageIndexProvider,
}

impl<'a> SelectivePageIndexLoader<'a> {
    fn new(
        metadata: &'a ParquetMetaData,
        file_bytes: &'a Bytes,
        cache: &'a mut PageIndexCache,
    ) -> Self {
        Self {
            metadata,
            file_bytes,
            cache,
            provider: SparsePageIndexProvider {
                column_indexes: HashMap::new(),
                offset_indexes: HashMap::new(),
            },
        }
    }

    /// Loads and decodes the column index for the specified column chunk.
    /// Repeated requests for the same entry do nothing.
    fn load_column_index(&mut self, row_group_idx: usize, column_idx: usize) -> Result<()> {
        let key = (row_group_idx, column_idx);
        if self.provider.column_indexes.contains_key(&key) {
            return Ok(());
        }
        if let Some(index) = self.cache.column_indexes.get(&key) {
            println!("  Column index {key:?}: cache hit");
            self.provider.column_indexes.insert(key, Arc::clone(index));
            return Ok(());
        }

        let column = self.metadata.row_group(row_group_idx).column(column_idx);
        if let Some(range) = column.column_index_range() {
            println!("  Column index {key:?}: cache miss, decoding");
            let idx_bytes = self
                .file_bytes
                .slice(range.start as usize..range.end as usize);
            let index = Arc::new(decode_column_index(&idx_bytes, column.column_type())?);
            self.cache.column_indexes.insert(key, Arc::clone(&index));
            self.provider.column_indexes.insert(key, index);
        }
        Ok(())
    }

    /// Loads and decodes the offset index for the specified column chunk.
    /// Repeated requests for the same entry do nothing.
    fn load_offset_index(&mut self, row_group_idx: usize, column_idx: usize) -> Result<()> {
        let key = (row_group_idx, column_idx);
        if self.provider.offset_indexes.contains_key(&key) {
            return Ok(());
        }
        if let Some(index) = self.cache.offset_indexes.get(&key) {
            println!("  Offset index {key:?}: cache hit");
            self.provider.offset_indexes.insert(key, Arc::clone(index));
            return Ok(());
        }

        let column = self.metadata.row_group(row_group_idx).column(column_idx);
        if let Some(range) = column.offset_index_range() {
            println!("  Offset index {key:?}: cache miss, decoding");
            let idx_bytes = self
                .file_bytes
                .slice(range.start as usize..range.end as usize);
            let index = Arc::new(decode_offset_index(&idx_bytes)?);
            self.cache.offset_indexes.insert(key, Arc::clone(&index));
            self.provider.offset_indexes.insert(key, index);
        }
        Ok(())
    }

    /// Finishes loading and returns an immutable provider containing only the
    /// selected indexes.
    fn finish(self) -> SparsePageIndexProvider {
        self.provider
    }
}

// Custom providers must implement the `PageIndexProvider` trait.
impl PageIndexProvider for SparsePageIndexProvider {
    fn has_offset_indexes(&self) -> bool {
        !self.offset_indexes.is_empty()
    }

    fn has_column_indexes(&self) -> bool {
        !self.column_indexes.is_empty()
    }

    fn column_index(
        &self,
        row_group_idx: usize,
        column_idx: usize,
    ) -> Option<&ColumnIndexMetaData> {
        self.column_indexes
            .get(&(row_group_idx, column_idx))
            .map(Arc::as_ref)
    }

    fn offset_index(
        &self,
        row_group_idx: usize,
        column_idx: usize,
    ) -> Option<&OffsetIndexMetaData> {
        self.offset_indexes
            .get(&(row_group_idx, column_idx))
            .map(Arc::as_ref)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

fn main() -> Result<()> {
    // Create a sample parquet file with page indexes for this example
    let tempdir = TempDir::new().unwrap();
    let temp_path = tempdir.path().join("custom_page_index_example.parquet");
    create_sample_file(&temp_path)?;
    println!("Sample file created with page indexes\n");

    // Example 1: Use the standard PageIndex provider (all columns accessible)
    println!("=== Example 1: Standard PageIndex (all columns) ===");

    let file_bytes = Bytes::from(std::fs::read(temp_path)?);
    let file_len = file_bytes.len() as u64;
    let mut decoder = ParquetMetaDataPushDecoder::try_new(file_len)?
        .with_page_index_policy(PageIndexPolicy::Required);
    #[expect(clippy::single_range_in_vec_init)]
    decoder.push_ranges(vec![0..file_len], vec![file_bytes.clone()])?;
    let metadata = match decoder.try_decode() {
        Ok(DecodeResult::Data(metadata)) => metadata, // decode successful
        other => {
            panic!("expected DecodeResult::Data, got: {other:?}")
        }
    };

    let num_columns = metadata.file_metadata().schema_descr().num_columns();
    println!("Number of row groups: {}", metadata.num_row_groups());
    println!("Number of columns: {num_columns}");
    println!();

    println!("== All indexes should be populated");
    print_page_index(&metadata, 0)?;
    print_page_index(&metadata, 1)?;

    // Example 2: Read metadata and then add custom PageIndexProvider
    println!("=== Example 2: Selective PageIndex (columns 0, 1, 4 only) ===");

    // first read the metadata without page indexes
    decoder = ParquetMetaDataPushDecoder::try_new(file_len)?
        .with_page_index_policy(PageIndexPolicy::Skip);
    #[expect(clippy::single_range_in_vec_init)]
    decoder.push_ranges(vec![0..file_len], vec![file_bytes.clone()])?;
    let metadata = match decoder.try_decode() {
        Ok(DecodeResult::Data(metadata)) => metadata, // decode successful
        other => {
            panic!("expected DecodeResult::Data, got: {other:?}")
        }
    };

    // Store the footer-only metadata in a shared metadata cache.
    let cached_metadata = Arc::new(metadata);

    // In this scenario, a query is performed which projects columns 0, 1, and 4
    // after applying a predicate to column 0. Chunk level statistics on column 0
    // have already indicated only row group 0 satisfies the predicate. We now wish
    // to do page level filtering. We only fetch the Column Index (min/max statistics)
    // for the predicate column 0, and only fetch the Offset Index (page locations)
    // for the projected columns 0, 1, and 4. Both indexes are only fetched for
    // row group 0.
    let mut page_index_cache = PageIndexCache::default();
    println!("Query 1 page index cache lookups:");
    let mut loader =
        SelectivePageIndexLoader::new(cached_metadata.as_ref(), &file_bytes, &mut page_index_cache);
    let row_group_idx = 0;
    loader.load_column_index(row_group_idx, 0)?;
    loader.load_offset_index(row_group_idx, 0)?;
    loader.load_offset_index(row_group_idx, 1)?;
    loader.load_offset_index(row_group_idx, 4)?;
    let provider = loader.finish();

    // Create query-specific metadata by cheaply cloning the cached footer and
    // attaching the sparse provider. The provider retains only the selected,
    // decoded indexes; it does not retain `file_bytes` or another metadata copy.
    let metadata = cached_metadata
        .as_ref()
        .clone()
        .into_builder()
        .set_page_index(Some(Arc::new(provider)))
        .build();
    println!("== Indexes should be partially populated");
    print_page_index(&metadata, 0)?;
    println!("== Indexes should be unpopulated");
    print_page_index(&metadata, 1)?;

    // A second query uses the same predicate column but projects columns 0, 2,
    // and 4. It reuses three indexes loaded by the first query and loads only
    // the newly requested offset index for column 2.
    println!("=== Query 2: Selective PageIndex (columns 0, 2, 4 only) ===");
    println!("Query 2 page index cache lookups:");
    let mut loader =
        SelectivePageIndexLoader::new(cached_metadata.as_ref(), &file_bytes, &mut page_index_cache);
    loader.load_column_index(row_group_idx, 0)?;
    loader.load_offset_index(row_group_idx, 0)?;
    loader.load_offset_index(row_group_idx, 2)?;
    loader.load_offset_index(row_group_idx, 4)?;
    let provider = loader.finish();

    let metadata = cached_metadata
        .as_ref()
        .clone()
        .into_builder()
        .set_page_index(Some(Arc::new(provider)))
        .build();
    println!("== Indexes should be partially populated");
    print_page_index(&metadata, 0)?;

    Ok(())
}

//////////////////////////////////////////////
// helper functions

fn print_page_index(metadata: &ParquetMetaData, row_group_idx: usize) -> Result<()> {
    if let Some(page_index) = metadata.page_index() {
        let num_columns = metadata.file_metadata().schema_descr().num_columns();

        println!("\nIndexes for row group {row_group_idx}:");
        for col_idx in 0..num_columns {
            println!(
                "  Column {col_idx}: has offset idx {}, has column idx {}",
                page_index.offset_index(row_group_idx, col_idx).is_some(),
                page_index.column_index(row_group_idx, col_idx).is_some()
            );
        }
        println!();
    } else {
        println!("No page index in metadata");
        println!("Note: This example requires a file with page indexes.");
        println!("Try using alltypes_tiny_pages.parquet or another file with page indexes.");
        return Err(ParquetError::General("no page index".to_string()));
    }
    Ok(())
}

fn create_sample_file(temp_path: &PathBuf) -> Result<()> {
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::{EnabledStatistics, WriterProperties};

    println!("Creating sample file: {}", temp_path.display());

    // Create a sample dataset with multiple columns
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("score", DataType::Int32, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("amount", DataType::Int32, false),
    ]));

    // Create multiple row groups with multiple pages
    let file = File::create(temp_path)?;
    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_data_page_size_limit(100) // Small pages for demonstration
        .set_write_batch_size(10)
        .build();

    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

    // Write several row groups
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
                    Arc::new(StringArray::from(
                        (offset..offset + 10)
                            .map(|x| if x % 2 == 0 { "even" } else { "odd" })
                            .collect::<Vec<&str>>(),
                    )),
                    Arc::new(Int32Array::from(
                        (offset..offset + 10).map(|x| x * 4).collect::<Vec<i32>>(),
                    )),
                ],
            )?;
            writer.write(&batch)?;
        }
        writer.flush()?;
    }

    writer.close()?;
    Ok(())
}
