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
//! - Only stores page indexes for specified columns (selective storage)
//! - Uses nested HashMaps for efficient storage and lookup
//! - Implements all required PageIndexProvider trait methods
//!
//! This approach can significantly reduce memory usage when working with wide
//! tables where only a few columns need page-level statistics.

use bytes::Bytes;
use parquet::DecodeResult;
use parquet::errors::{ParquetError, Result};
use parquet::file::metadata::{PageIndexProvider, ParquetMetaData, ParquetMetaDataPushDecoder};
use parquet::file::page_index::column_index::ColumnIndexMetaData;
use parquet::file::page_index::offset_index::OffsetIndexMetaData;
use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;
use tempfile::TempDir;

/// A custom PageIndexProvider that only stores indexes for a subset of columns
///
/// This provider uses nested hash maps to store only the necessary indexes
/// to satisfy a query
#[derive(Debug, Clone)]
struct SparsePageIndexProvider {
    column_indexes: Option<HashMap<usize, HashMap<usize, ColumnIndexMetaData>>>,
    offset_indexes: Option<HashMap<usize, HashMap<usize, OffsetIndexMetaData>>>,
}

impl SparsePageIndexProvider {
    fn new(
        column_indexes: Option<HashMap<usize, HashMap<usize, ColumnIndexMetaData>>>,
        offset_indexes: Option<HashMap<usize, HashMap<usize, OffsetIndexMetaData>>>,
    ) -> Self {
        Self {
            column_indexes,
            offset_indexes,
        }
    }
}

impl PageIndexProvider for SparsePageIndexProvider {
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

fn dump_page_index(metadata: &ParquetMetaData) -> Result<()> {
    if let Some(page_index) = metadata.page_index() {
        let num_columns = metadata.file_metadata().schema_descr().num_columns();

        println!("Standard page index present");
        println!("  Has column indexes: {}", page_index.has_column_indexes());
        println!("  Has offset indexes: {}", page_index.has_offset_indexes());

        println!("\nPage counts for row group 0:");
        for col_idx in 0..num_columns {
            println!(
                "  Column {col_idx}: has offset idx {}, has column idx {}",
                page_index.offset_index(0, col_idx).is_some(),
                page_index.column_index(0, col_idx).is_some()
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

fn main() -> Result<()> {
    // Create a sample parquet file with page indexes for this example
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::{EnabledStatistics, WriterProperties};

    let tempdir = TempDir::new().unwrap();
    let temp_path = tempdir.path().join("custom_page_index_example.parquet");
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
    let file = File::create(&temp_path)?;
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
    println!("Sample file created with page indexes\n");

    // Now read it back with page indexes
    use parquet::file::metadata::PageIndexPolicy;

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

    // Example 1: Use the standard PageIndex provider (all columns accessible)
    println!("=== Example 1: Standard PageIndex (all columns) ===");
    dump_page_index(&metadata)?;

    // save original index to populate the custom one
    let page_index = metadata.page_index().cloned().unwrap();

    // Example 2: Read metadata and then add custom PageIndexProvider
    decoder = ParquetMetaDataPushDecoder::try_new(file_len)?
        .with_page_index_policy(PageIndexPolicy::Skip);
    #[expect(clippy::single_range_in_vec_init)]
    decoder.push_ranges(vec![0..file_len], vec![file_bytes])?;
    let metadata = match decoder.try_decode() {
        Ok(DecodeResult::Data(metadata)) => metadata, // decode successful
        other => {
            panic!("expected DecodeResult::Data, got: {other:?}")
        }
    };
    println!("=== Example 2: Selective PageIndex (columns 0, 1, 2 only) ===");
    let mut builder = metadata.into_builder();

    // create partial indexes. column index for column 0 only (predicate column),
    // offset index for columns 0, 1, 4 (projected columns)
    // only populate first row group
    let mut colidx = HashMap::new();
    let mut offidx = HashMap::new();
    colidx.insert(0usize, page_index.column_index(0, 0).unwrap().clone());
    offidx.insert(0usize, page_index.offset_index(0, 0).unwrap().clone());
    offidx.insert(1usize, page_index.offset_index(0, 1).unwrap().clone());
    offidx.insert(4usize, page_index.offset_index(0, 4).unwrap().clone());
    let mut colidxs = HashMap::new();
    colidxs.insert(0usize, colidx);
    let mut offidxs = HashMap::new();
    offidxs.insert(0usize, offidx);
    let provider = SparsePageIndexProvider::new(Some(colidxs), Some(offidxs));

    // add our custom provider to the metadata
    builder = builder.set_page_index(Some(Arc::new(provider)));

    let metadata = builder.build();
    dump_page_index(&metadata)?;

    Ok(())
}
