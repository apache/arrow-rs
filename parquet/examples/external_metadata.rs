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

use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow_cast::pretty::pretty_format_batches;
use futures::TryStreamExt;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::{ArrowWriter, ParquetRecordBatchStreamBuilder};
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader, ParquetMetaDataWriter};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::TempDir;

/// This example demonstrates advanced usage of the Parquet metadata APIs.
///
/// It is sometimes desired to copy the metadata for parquet files stored on
/// remote object storage (e.g. S3) to a local file or an in-memory cache, use a
/// query engine like DataFusion to analyze the metadata to determine which file
/// to read, and then read any matching files with a single subsequent object
/// store request.
///
/// # Example Overview
///
/// 1. Reads the metadata of a Parquet file using [`ParquetMetaDataReader`]
///
/// 2. Removes column statistics from the metadata (to make it smaller)
///
/// 3. Stores the metadata in a separate file using [`ParquetMetaDataWriter`]
///
/// 4. Reads the metadata from the separate file and uses that to read the
///    Parquet file, thus avoiding a second IO to read metadata or reparsing
///    the footer.
///
#[tokio::main(flavor = "current_thread")]
async fn main() -> parquet::errors::Result<()> {
    let tempdir = TempDir::new().unwrap();
    let parquet_path = create_parquet_file(&tempdir);
    let metadata_path = tempdir.path().join("thrift_metadata.dat");

    // In this example, we use a tokio file to mimic an async remote data source
    let mut remote_parquet_file = tokio::fs::File::open(&parquet_path).await?;

    let metadata = get_metadata_from_remote_parquet_file(&mut remote_parquet_file).await;
    println!(
        "Metadata from 'remote' Parquet file into memory: {} bytes",
        metadata.memory_size()
    );

    // now slim down the metadata and write it to a "local" file
    let metadata = prepare_metadata(metadata);
    write_metadata_to_local_file(metadata, &metadata_path);

    // now read the metadata from the local file and use it to read the "remote" Parquet file
    let metadata = read_metadata_from_local_file(&metadata_path);
    println!("Read metadata from file");

    let batches = read_remote_parquet_file_with_metadata(remote_parquet_file, metadata).await;

    // display the results
    let batches_string = pretty_format_batches(&batches).unwrap().to_string();
    let batches_lines: Vec<_> = batches_string.split('\n').collect();

    assert_eq!(
        batches_lines,
        [
            "+-----+-------------+",
            "| id  | description |",
            "+-----+-------------+",
            "| 100 | oranges     |",
            "| 200 | apples      |",
            "| 201 | grapefruit  |",
            "| 300 | bannanas    |",
            "| 102 | grapes      |",
            "| 33  | pears       |",
            "+-----+-------------+",
        ],
        "actual output:\n\n{batches_lines:#?}"
    );

    Ok(())
}

/// Reads the metadata from a "remote" parquet file
///
/// Note that this function models reading from a remote file source using a
/// tokio file. In a real application, you would implement [`MetadataFetch`] for
/// your own remote source.
///
/// [`MetadataFetch`]: parquet::arrow::async_reader::MetadataFetch
async fn get_metadata_from_remote_parquet_file(
    remote_file: &mut tokio::fs::File,
) -> ParquetMetaData {
    // the remote source must know the total file size (e.g. from an object store LIST operation)
    let file_size = remote_file.metadata().await.unwrap().len();

    // tell the reader to read the page index
    ParquetMetaDataReader::new()
        .with_page_indexes(true)
        .load_and_finish(remote_file, file_size as usize)
        .await
        .unwrap()
}

/// modifies the metadata to reduce its size
fn prepare_metadata(metadata: ParquetMetaData) -> ParquetMetaData {
    let orig_size = metadata.memory_size();

    let mut builder = metadata.into_builder();

    // remove column statistics to reduce the size of the metadata by converting
    // the various structures into their respective builders and modifying them
    // as needed.
    for row_group in builder.take_row_groups() {
        let mut row_group_builder = row_group.into_builder();
        for column in row_group_builder.take_columns() {
            let column = column.into_builder().clear_statistics().build().unwrap();
            row_group_builder = row_group_builder.add_column_metadata(column);
        }
        let row_group = row_group_builder.build().unwrap();
        builder = builder.add_row_group(row_group);
    }
    let metadata = builder.build();

    // verifiy that the size has indeed been reduced
    let new_size = metadata.memory_size();
    assert!(new_size < orig_size, "metadata size did not decrease");
    println!("Reduced metadata size from {} to {}", orig_size, new_size);
    metadata
}

/// writes the metadata to a file
///
/// The data is stored using the same thrift format as the Parquet file metadata
fn write_metadata_to_local_file(metadata: ParquetMetaData, file: impl AsRef<Path>) {
    let file = File::create(file).unwrap();
    ParquetMetaDataWriter::new(file, &metadata)
        .finish()
        .unwrap()
}

/// Reads the metadata from a file
///
/// This function reads the format written by `write_metadata_to_file`
fn read_metadata_from_local_file(file: impl AsRef<Path>) -> ParquetMetaData {
    let file = File::open(file).unwrap();
    ParquetMetaDataReader::new()
        .with_page_indexes(true)
        .parse_and_finish(&file)
        .unwrap()
}

/// Reads the "remote" Parquet file using the metadata
///
/// This shows how to read the Parquet file using previously read metadata
/// instead of the metadata in the Parquet file itself. This avoids an IO /
/// having to fetch and decode the metadata from the Parquet file before
/// beginning to read it.
///
/// Note that this function models reading from a remote file source using a
/// tokio file. In a real application, you would implement [`AsyncFileReader`]
/// for your own remote source.
///
/// In this example, we simply buffer the results in memory as Arrow record
/// batches but a real application would likely process the batches as they are
/// read.
///
/// [`AsyncFileReader`]: parquet::arrow::async_reader::AsyncFileReader
async fn read_remote_parquet_file_with_metadata(
    remote_file: tokio::fs::File,
    metadata: ParquetMetaData,
) -> Vec<RecordBatch> {
    let options = ArrowReaderOptions::new()
        // tell the reader to read the page index
        .with_page_index(true);
    // create a reader with pre-existing metadata
    let arrow_reader_metadata = ArrowReaderMetadata::try_new(metadata.into(), options).unwrap();
    let reader =
        ParquetRecordBatchStreamBuilder::new_with_metadata(remote_file, arrow_reader_metadata)
            .build()
            .unwrap();

    reader.try_collect::<Vec<_>>().await.unwrap()
}

/// Make a new parquet file in the temporary directory, and returns the path
fn create_parquet_file(tmpdir: &TempDir) -> PathBuf {
    let path = tmpdir.path().join("example.parquet");
    let new_file = File::create(&path).unwrap();

    let batch = RecordBatch::try_from_iter(vec![
        (
            "id",
            Arc::new(Int32Array::from(vec![100, 200, 201, 300, 102, 33])) as ArrayRef,
        ),
        (
            "description",
            Arc::new(StringArray::from(vec![
                "oranges",
                "apples",
                "grapefruit",
                "bannanas",
                "grapes",
                "pears",
            ])),
        ),
    ])
    .unwrap();

    let props = WriterProperties::builder()
        // ensure we write the page index level statistics
        .set_statistics_enabled(EnabledStatistics::Page)
        .build();

    let mut writer = ArrowWriter::try_new(new_file, batch.schema(), Some(props)).unwrap();

    writer.write(&batch).unwrap();
    writer.finish().unwrap();

    path
}
