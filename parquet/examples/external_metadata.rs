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

use arrow_array::RecordBatch;
use arrow_cast::pretty::pretty_format_batches;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::file::footer::{decode_footer, decode_metadata};
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataWriter};
use std::io::Read;
use std::path::Path;

/// This example demonstrates advanced usage of Parquet metadata.
///
/// This is designed to show how to store Parquet metadata somewhere other than
/// the Parquet file itself, and how to use that metadata to read the file. This
/// can be used, for example, to store metadata for parquet files on remote
/// object storage (e.g. S3)  in a local file, use a query engine like
/// DataFusion to figure out which files to read, and then read the files with a
/// single object store request.
///
/// Specifically it:
/// 1. Reads the metadata of a Parquet file
/// 2. Removes some column statistics from the metadata (to make them smaller)
/// 3. Stores the metadata in a separate file
/// 4. Reads the metadata from the separate file and uses that to read the Parquet file
///
/// Without this API, to implement the functionality you need to implement
/// a conversion to/from some other structs that can be serialized/deserialized.

#[tokio::main(flavor = "current_thread")]
async fn main() -> parquet::errors::Result<()> {
    let testdata = arrow::util::test_util::parquet_test_data();
    let parquet_path = format!("{testdata}/alltypes_plain.parquet");
    let metadata_path = "thift_metadata.dat"; // todo tempdir for now use local file to inspect it

    let metadata = get_metadata_from_parquet_file(&parquet_path).await;
    println!(
        "Read metadata from Parquet file into memory: {} bytes",
        metadata.memory_size()
    );
    let metadata = prepare_metadata(metadata);
    write_metadata_to_file(metadata, &metadata_path);

    // now read the metadata from the file and use it to read the Parquet file
    let metadata = read_metadata_from_file(&metadata_path);
    println!("Read metadata from file: {metadata:#?}");

    let batches = read_parquet_file_with_metadata(&parquet_path, metadata);

    // display the results
    let batches_string = pretty_format_batches(&batches).unwrap().to_string();
    let batches_lines: Vec<_> = batches_string.split('\n').collect();

    assert_eq!(batches_lines,
               [
                   "+----+----------+-------------+--------------+---------+------------+-----------+------------+------------------+------------+---------------------+",
                   "| id | bool_col | tinyint_col | smallint_col | int_col | bigint_col | float_col | double_col | date_string_col  | string_col | timestamp_col       |",
                   "+----+----------+-------------+--------------+---------+------------+-----------+------------+------------------+------------+---------------------+",
                   "| 4  | true     | 0           | 0            | 0       | 0          | 0.0       | 0.0        | 30332f30312f3039 | 30         | 2009-03-01T00:00:00 |",
                   "| 5  | false    | 1           | 1            | 1       | 10         | 1.1       | 10.1       | 30332f30312f3039 | 31         | 2009-03-01T00:01:00 |",
                   "| 6  | true     | 0           | 0            | 0       | 0          | 0.0       | 0.0        | 30342f30312f3039 | 30         | 2009-04-01T00:00:00 |",
                   "| 7  | false    | 1           | 1            | 1       | 10         | 1.1       | 10.1       | 30342f30312f3039 | 31         | 2009-04-01T00:01:00 |",
                   "| 2  | true     | 0           | 0            | 0       | 0          | 0.0       | 0.0        | 30322f30312f3039 | 30         | 2009-02-01T00:00:00 |",
                   "| 3  | false    | 1           | 1            | 1       | 10         | 1.1       | 10.1       | 30322f30312f3039 | 31         | 2009-02-01T00:01:00 |",
                   "| 0  | true     | 0           | 0            | 0       | 0          | 0.0       | 0.0        | 30312f30312f3039 | 30         | 2009-01-01T00:00:00 |",
                   "| 1  | false    | 1           | 1            | 1       | 10         | 1.1       | 10.1       | 30312f30312f3039 | 31         | 2009-01-01T00:01:00 |",
                   "+----+----------+-------------+--------------+---------+------------+-----------+------------+------------------+------------+---------------------+",
               ]

               , "actual output:\n\n{batches_lines:#?}");

    Ok(())
}

/// Reads the metadata from a parquet file
async fn get_metadata_from_parquet_file(file: impl AsRef<Path>) -> ParquetMetaData {
    // pretend we are reading the metadata from a remote object store
    let file = std::fs::File::open(file).unwrap();
    let file = tokio::fs::File::from_std(file);

    let builder = ParquetRecordBatchStreamBuilder::new(file).await.unwrap();

    // The metadata is Arc'd -- since we are going to modify it we
    // need to clone it
    builder.metadata().as_ref().clone()
}

/// modifies the metadata to reduce its size
fn prepare_metadata(metadata: ParquetMetaData) -> ParquetMetaData {
    // maybe we will do this
    metadata
}

/// writes the metadata to a file
///
/// The data is stored using the same thrift format as the Parquet file metadata
fn write_metadata_to_file(metadata: ParquetMetaData, file: impl AsRef<Path>) {
    let file = std::fs::File::create(file).unwrap();
    let writer = ParquetMetaDataWriter::new(file, &metadata);
    writer.finish().unwrap()
}

/// Reads the metadata from a file
///
/// This function reads the format written by `write_metadata_to_file`
fn read_metadata_from_file(file: impl AsRef<Path>) -> ParquetMetaData {
    let mut file = std::fs::File::open(file).unwrap();
    // This API is kind of awkward compared to the writer
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).unwrap();
    let len = buffer.len();

    let mut footer = [0; 8];
    footer.copy_from_slice(&buffer[len - 8..len]);

    let md_length = decode_footer(&footer).unwrap();
    // note this also doesn't contain the ColumnOffset or ColumnIndex
    let metadata_buffer = &buffer[len - 8 - md_length..md_length];
    decode_metadata(metadata_buffer).unwrap()
}

/// Reads the Parquet file using the metadata
///
/// This shows how to read the Parquet file using previously read metadata
/// instead of the metadata in the Parquet file itself. This avoids an IO /
/// having to fetch and decode the metadata from the Parquet file before
/// beginning to read it.
///
/// In this example, we read the results as Arrow record batches
fn read_parquet_file_with_metadata(
    file: impl AsRef<Path>,
    metadata: ParquetMetaData,
) -> Vec<RecordBatch> {
    let file = std::fs::File::open(file).unwrap();
    let options = ArrowReaderOptions::new()
        // tell the reader to read the page index
        .with_page_index(true);
    // create a reader with pre-existing metadata
    let arrow_reader_metadata = ArrowReaderMetadata::try_new(metadata.into(), options).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(file, arrow_reader_metadata)
        .build()
        .unwrap();

    reader.collect::<arrow::error::Result<Vec<_>>>().unwrap()
}
