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

//! Common tests for encryption related functionality

use arrow_array::cast::AsArray;
use arrow_array::types;
use arrow_schema::ArrowError;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ProjectionMask;
use std::fs::File;

pub fn read_plaintext_footer_file_without_decryption_properties() {
    let test_data = arrow::util::test_util::parquet_test_data();
    let path = format!("{test_data}/encrypt_columns_plaintext_footer.parquet.encrypted");
    let file = File::open(&path).unwrap();

    let metadata = ArrowReaderMetadata::load(&file, Default::default()).unwrap();
    let file_metadata = metadata.metadata().file_metadata();

    assert_eq!(file_metadata.num_rows(), 50);
    assert_eq!(file_metadata.schema_descr().num_columns(), 8);
    assert_eq!(
        file_metadata.created_by().unwrap(),
        "parquet-cpp-arrow version 19.0.0-SNAPSHOT"
    );

    metadata.metadata().row_groups().iter().for_each(|rg| {
        assert_eq!(rg.num_columns(), 8);
        assert_eq!(rg.num_rows(), 50);
    });

    // Should be able to read unencrypted columns. Test reading one column.
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let mask = ProjectionMask::leaves(builder.parquet_schema(), [1]);
    let record_reader = builder.with_projection(mask).build().unwrap();

    let mut row_count = 0;
    for batch in record_reader {
        let batch = batch.unwrap();
        row_count += batch.num_rows();

        let time_col = batch
            .column(0)
            .as_primitive::<types::Time32MillisecondType>();
        for (i, x) in time_col.iter().enumerate() {
            assert_eq!(x.unwrap(), i as i32);
        }
    }

    assert_eq!(row_count, file_metadata.num_rows() as usize);

    // Reading an encrypted column should fail
    let file = File::open(&path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let mask = ProjectionMask::leaves(builder.parquet_schema(), [4]);
    let mut record_reader = builder.with_projection(mask).build().unwrap();

    match record_reader.next() {
        Some(Err(ArrowError::ParquetError(s))) => {
            assert!(s.contains("protocol error"));
        }
        _ => {
            panic!("Expected ArrowError::ParquetError");
        }
    };
}

#[cfg(feature = "async")]
pub async fn read_plaintext_footer_file_without_decryption_properties_async() {
    use futures::StreamExt;
    use futures::TryStreamExt;
    use parquet::arrow::ParquetRecordBatchStreamBuilder;
    use parquet::errors::ParquetError;

    let test_data = arrow::util::test_util::parquet_test_data();
    let path = format!("{test_data}/encrypt_columns_plaintext_footer.parquet.encrypted");
    let mut file = tokio::fs::File::open(&path).await.unwrap();

    let metadata = ArrowReaderMetadata::load_async(&mut file, Default::default())
        .await
        .unwrap();
    let file_metadata = metadata.metadata().file_metadata();

    assert_eq!(file_metadata.num_rows(), 50);
    assert_eq!(file_metadata.schema_descr().num_columns(), 8);
    assert_eq!(
        file_metadata.created_by().unwrap(),
        "parquet-cpp-arrow version 19.0.0-SNAPSHOT"
    );

    metadata.metadata().row_groups().iter().for_each(|rg| {
        assert_eq!(rg.num_columns(), 8);
        assert_eq!(rg.num_rows(), 50);
    });

    // Should be able to read unencrypted columns. Test reading one column.
    let builder = ParquetRecordBatchStreamBuilder::new(file).await.unwrap();
    let mask = ProjectionMask::leaves(builder.parquet_schema(), [1]);
    let record_reader = builder.with_projection(mask).build().unwrap();
    let record_batches = record_reader.try_collect::<Vec<_>>().await.unwrap();

    let mut row_count = 0;
    for batch in record_batches {
        let batch = batch;
        row_count += batch.num_rows();

        let time_col = batch
            .column(0)
            .as_primitive::<types::Time32MillisecondType>();
        for (i, x) in time_col.iter().enumerate() {
            assert_eq!(x.unwrap(), i as i32);
        }
    }

    assert_eq!(row_count, file_metadata.num_rows() as usize);

    // Reading an encrypted column should fail
    let file = tokio::fs::File::open(&path).await.unwrap();
    let builder = ParquetRecordBatchStreamBuilder::new(file).await.unwrap();
    let mask = ProjectionMask::leaves(builder.parquet_schema(), [4]);
    let mut record_reader = builder.with_projection(mask).build().unwrap();

    match record_reader.next().await {
        Some(Err(ParquetError::ArrowError(s))) => {
            assert!(s.contains("protocol error"));
        }
        _ => {
            panic!("Expected ArrowError::ParquetError");
        }
    };
}
