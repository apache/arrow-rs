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

//! Example demonstrating the Parquet Page Store.
//!
//! Writes Arrow RecordBatches to a content-addressed page store and reads them back.

use std::sync::Arc;

use arrow_array::{ArrayRef, Float64Array, Int32Array, RecordBatch, StringArray};
use arrow_cast::pretty::pretty_format_batches;
use parquet::arrow::page_store::{PageStoreReader, PageStoreWriter};
use parquet::file::properties::{CdcOptions, EnabledStatistics, WriterProperties};
use tempfile::TempDir;

fn main() -> parquet::errors::Result<()> {
    let tempdir = TempDir::new().unwrap();
    let store_dir = tempdir.path().join("page_store");

    // Create sample data
    let batch = RecordBatch::try_from_iter(vec![
        (
            "id",
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10])) as ArrayRef,
        ),
        (
            "value",
            Arc::new(Float64Array::from(vec![
                1.0, 2.5, 3.7, 4.2, 5.9, 6.1, 7.3, 8.8, 9.0, 10.5,
            ])) as ArrayRef,
        ),
        (
            "name",
            Arc::new(StringArray::from(vec![
                "alice", "bob", "charlie", "diana", "eve", "frank", "grace", "heidi", "ivan",
                "judy",
            ])) as ArrayRef,
        ),
    ])
    .unwrap();

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_content_defined_chunking(Some(CdcOptions::default()))
        .build();

    let metadata_path = tempdir.path().join("table.parquet");

    // Write to page store
    println!("Page store dir: {}", store_dir.display());
    println!("Metadata file:  {}", metadata_path.display());
    let mut writer = PageStoreWriter::try_new(&store_dir, batch.schema(), Some(props))?;
    writer.write(&batch)?;
    let metadata = writer.finish(&metadata_path)?;

    println!(
        "Wrote {} row group(s), {} total rows",
        metadata.num_row_groups(),
        metadata.file_metadata().num_rows()
    );

    // List page files
    let page_files: Vec<_> = std::fs::read_dir(&store_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "page"))
        .collect();
    println!("Page files in store: {}", page_files.len());

    // Read back from page store
    println!("\nReading from page store...");
    let reader = PageStoreReader::try_new(&metadata_path, &store_dir)?;
    let batches = reader.read_batches()?;

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!("Read {} batch(es), {} total rows", batches.len(), total_rows);

    // Display
    let formatted = pretty_format_batches(&batches).unwrap();
    println!("\n{formatted}");

    // Verify round-trip
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0], batch);
    println!("\nRound-trip verification: PASSED");

    Ok(())
}
