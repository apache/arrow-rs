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

use crate::io::TestReader;
use bytes::Bytes;
use futures::TryStreamExt;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};

#[tokio::test]
async fn non_empty_offset_index_doesnt_panic_in_read_row_group() {
    use tokio::fs::File;
    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/alltypes_tiny_pages.parquet");
    let mut file = File::open(&path).await.unwrap();
    let file_size = file.metadata().await.unwrap().len();
    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Required)
        .load_and_finish(&mut file, file_size)
        .await
        .unwrap();

    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
    let arrow_reader_metadata = ArrowReaderMetadata::try_new(metadata.into(), options).unwrap();
    let reader = ParquetRecordBatchStreamBuilder::new_with_metadata(file, arrow_reader_metadata)
        .build()
        .unwrap();

    let result = reader.try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(result.len(), 8);
}

#[tokio::test]
async fn empty_offset_index_doesnt_panic_in_column_chunks() {
    use tempfile::TempDir;
    use tokio::fs::File;
    fn write_metadata_to_local_file(metadata: ParquetMetaData, file: impl AsRef<std::path::Path>) {
        use parquet::file::metadata::ParquetMetaDataWriter;
        use std::fs::File;
        let file = File::create(file).unwrap();
        ParquetMetaDataWriter::new(file, &metadata)
            .finish()
            .unwrap()
    }

    fn read_metadata_from_local_file(file: impl AsRef<std::path::Path>) -> ParquetMetaData {
        use std::fs::File;
        let file = File::open(file).unwrap();
        ParquetMetaDataReader::new()
            .with_page_index_policy(PageIndexPolicy::Required)
            .parse_and_finish(&file)
            .unwrap()
    }

    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/alltypes_plain.parquet");
    let mut file = File::open(&path).await.unwrap();
    let file_size = file.metadata().await.unwrap().len();
    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Required)
        .load_and_finish(&mut file, file_size)
        .await
        .unwrap();

    let tempdir = TempDir::new().unwrap();
    let metadata_path = tempdir.path().join("thrift_metadata.dat");
    write_metadata_to_local_file(metadata, &metadata_path);
    let metadata = read_metadata_from_local_file(&metadata_path);

    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
    let arrow_reader_metadata = ArrowReaderMetadata::try_new(metadata.into(), options).unwrap();
    let reader = ParquetRecordBatchStreamBuilder::new_with_metadata(file, arrow_reader_metadata)
        .build()
        .unwrap();

    // Panics here
    let result = reader.try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(result.len(), 1);
}

#[tokio::test]
async fn test_cached_array_reader_sparse_offset_error() {
    use futures::TryStreamExt;

    use arrow_array::{BooleanArray, RecordBatch};
    use parquet::arrow::arrow_reader::{ArrowPredicateFn, RowFilter, RowSelection, RowSelector};

    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/alltypes_tiny_pages_plain.parquet");
    let data = Bytes::from(std::fs::read(path).unwrap());

    let async_reader = TestReader::new(data);

    // Enable page index so the fetch logic loads only required pages
    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
    let builder = ParquetRecordBatchStreamBuilder::new_with_options(async_reader, options)
        .await
        .unwrap();

    // Skip the first 22 rows (entire first Parquet page) and then select the
    // next 3 rows (22, 23, 24). This means the fetch step will not include
    // the first page starting at file offset 0.
    let selection = RowSelection::from(vec![RowSelector::skip(22), RowSelector::select(3)]);

    // Trivial predicate on column 0 that always returns `true`. Using the
    // same column in both predicate and projection activates the caching
    // layer (Producer/Consumer pattern).
    let parquet_schema = builder.parquet_schema();
    let proj = ProjectionMask::leaves(parquet_schema, vec![0]);
    let always_true = ArrowPredicateFn::new(proj.clone(), |batch: RecordBatch| {
        Ok(BooleanArray::from(vec![true; batch.num_rows()]))
    });
    let filter = RowFilter::new(vec![Box::new(always_true)]);

    // Build the stream with batch size 8 so the cache reads whole batches
    // that straddle the requested row range (rows 0-7, 8-15, 16-23, …).
    let stream = builder
        .with_batch_size(8)
        .with_projection(proj)
        .with_row_selection(selection)
        .with_row_filter(filter)
        .build()
        .unwrap();

    // Collecting the stream should fail with the sparse column chunk offset
    // error we want to reproduce.
    let _result: Vec<_> = stream.try_collect().await.unwrap();
}
