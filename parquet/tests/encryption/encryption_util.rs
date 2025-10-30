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

use arrow_array::cast::AsArray;
use arrow_array::{RecordBatch, types};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use parquet::encryption::decrypt::{FileDecryptionProperties, KeyRetriever};
use parquet::encryption::encrypt::FileEncryptionProperties;
use parquet::errors::{ParquetError, Result};
use parquet::file::metadata::ParquetMetaData;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::fs::File;
use std::sync::{Arc, Mutex};

pub(crate) fn verify_encryption_double_test_data(
    record_batches: Vec<RecordBatch>,
    metadata: &ParquetMetaData,
) {
    let file_metadata = metadata.file_metadata();
    assert_eq!(file_metadata.num_rows(), 100);
    assert_eq!(file_metadata.schema_descr().num_columns(), 8);

    metadata.row_groups().iter().for_each(|rg| {
        assert_eq!(rg.num_columns(), 8);
        assert_eq!(rg.num_rows(), 50);
    });

    let mut row_count = 0;
    let wrap_at = 50;
    for batch in record_batches {
        let batch = batch;
        row_count += batch.num_rows();

        let bool_col = batch.column(0).as_boolean();
        let time_col = batch
            .column(1)
            .as_primitive::<types::Time32MillisecondType>();
        let list_col = batch.column(2).as_list::<i32>();
        let timestamp_col = batch
            .column(3)
            .as_primitive::<types::TimestampNanosecondType>();
        let f32_col = batch.column(4).as_primitive::<types::Float32Type>();
        let f64_col = batch.column(5).as_primitive::<types::Float64Type>();
        let binary_col = batch.column(6).as_binary::<i32>();
        let fixed_size_binary_col = batch.column(7).as_fixed_size_binary();

        for (i, x) in bool_col.iter().enumerate() {
            assert_eq!(x.unwrap(), i % 2 == 0);
        }
        for (i, x) in time_col.iter().enumerate() {
            assert_eq!(x.unwrap(), (i % wrap_at) as i32);
        }
        for (i, list_item) in list_col.iter().enumerate() {
            let list_item = list_item.unwrap();
            let list_item = list_item.as_primitive::<types::Int64Type>();
            assert_eq!(list_item.len(), 2);
            assert_eq!(
                list_item.value(0),
                (((i % wrap_at) * 2) * 1000000000000) as i64
            );
            assert_eq!(
                list_item.value(1),
                (((i % wrap_at) * 2 + 1) * 1000000000000) as i64
            );
        }
        for x in timestamp_col.iter() {
            assert!(x.is_some());
        }
        for (i, x) in f32_col.iter().enumerate() {
            assert_eq!(x.unwrap(), (i % wrap_at) as f32 * 1.1f32);
        }
        for (i, x) in f64_col.iter().enumerate() {
            assert_eq!(x.unwrap(), (i % wrap_at) as f64 * 1.1111111f64);
        }
        for (i, x) in binary_col.iter().enumerate() {
            assert_eq!(x.is_some(), i % 2 == 0);
            if let Some(x) = x {
                assert_eq!(&x[0..7], b"parquet");
            }
        }
        for (i, x) in fixed_size_binary_col.iter().enumerate() {
            assert_eq!(x.unwrap(), &[(i % wrap_at) as u8; 10]);
        }
    }

    assert_eq!(row_count, file_metadata.num_rows() as usize);
}

/// Verifies data read from an encrypted file from the parquet-testing repository
pub(crate) fn verify_encryption_test_data(
    record_batches: Vec<RecordBatch>,
    metadata: &ParquetMetaData,
) {
    let file_metadata = metadata.file_metadata();
    assert_eq!(file_metadata.num_rows(), 50);
    assert_eq!(file_metadata.schema_descr().num_columns(), 8);

    let mut total_rows = 0;
    metadata.row_groups().iter().for_each(|rg| {
        assert_eq!(rg.num_columns(), 8);
        total_rows += rg.num_rows();
    });
    assert_eq!(total_rows, 50);

    let mut row_count = 0;
    for batch in record_batches {
        let batch = batch;

        let row_index = |index_in_batch: usize| row_count + index_in_batch;

        let bool_col = batch.column(0).as_boolean();
        let time_col = batch
            .column(1)
            .as_primitive::<types::Time32MillisecondType>();
        let list_col = batch.column(2).as_list::<i32>();
        let timestamp_col = batch
            .column(3)
            .as_primitive::<types::TimestampNanosecondType>();
        let f32_col = batch.column(4).as_primitive::<types::Float32Type>();
        let f64_col = batch.column(5).as_primitive::<types::Float64Type>();
        let binary_col = batch.column(6).as_binary::<i32>();
        let fixed_size_binary_col = batch.column(7).as_fixed_size_binary();

        for (i, x) in bool_col.iter().enumerate() {
            assert_eq!(x.unwrap(), row_index(i) % 2 == 0);
        }
        for (i, x) in time_col.iter().enumerate() {
            assert_eq!(x.unwrap(), row_index(i) as i32);
        }
        for (i, list_item) in list_col.iter().enumerate() {
            let list_item = list_item.unwrap();
            let list_item = list_item.as_primitive::<types::Int64Type>();
            assert_eq!(list_item.len(), 2);
            assert_eq!(
                list_item.value(0),
                ((row_index(i) * 2) * 1000000000000) as i64
            );
            assert_eq!(
                list_item.value(1),
                ((row_index(i) * 2 + 1) * 1000000000000) as i64
            );
        }
        for x in timestamp_col.iter() {
            assert!(x.is_some());
        }
        for (i, x) in f32_col.iter().enumerate() {
            assert_eq!(x.unwrap(), row_index(i) as f32 * 1.1f32);
        }
        for (i, x) in f64_col.iter().enumerate() {
            assert_eq!(x.unwrap(), row_index(i) as f64 * 1.1111111f64);
        }
        for (i, x) in binary_col.iter().enumerate() {
            assert_eq!(x.is_some(), row_index(i) % 2 == 0);
            if let Some(x) = x {
                assert_eq!(&x[0..7], b"parquet");
            }
        }
        for (i, x) in fixed_size_binary_col.iter().enumerate() {
            assert_eq!(x.unwrap(), &[row_index(i) as u8; 10]);
        }

        row_count += batch.num_rows();
    }

    assert_eq!(row_count, file_metadata.num_rows() as usize);
}

/// Verifies that the column and offset indexes were successfully read from an
/// encrypted test file.
pub(crate) fn verify_column_indexes(metadata: &ParquetMetaData) {
    let offset_index = metadata.offset_index().unwrap();
    // 1 row group, 8 columns
    assert_eq!(offset_index.len(), 1);
    assert_eq!(offset_index[0].len(), 8);
    // Check float column, which is encrypted in the non-uniform test file
    let float_col_idx = 4;
    let offset_index = &offset_index[0][float_col_idx];
    assert_eq!(offset_index.page_locations.len(), 1);
    assert!(offset_index.page_locations[0].offset > 0);

    let column_index = metadata.column_index().unwrap();
    assert_eq!(column_index.len(), 1);
    assert_eq!(column_index[0].len(), 8);
    let column_index = &column_index[0][float_col_idx];

    match column_index {
        parquet::file::page_index::column_index::ColumnIndexMetaData::FLOAT(float_index) => {
            assert_eq!(float_index.num_pages(), 1);
            assert_eq!(float_index.min_value(0), Some(&0.0f32));
            assert!(
                float_index
                    .max_value(0)
                    .is_some_and(|max| (max - 53.9).abs() < 1e-6)
            );
        }
        _ => {
            panic!("Expected a float column index for column {float_col_idx}");
        }
    };
}

pub(crate) fn read_encrypted_file(
    file: &File,
    decryption_properties: Arc<FileDecryptionProperties>,
) -> std::result::Result<(Vec<RecordBatch>, ArrowReaderMetadata), ParquetError> {
    let options =
        ArrowReaderOptions::default().with_file_decryption_properties(decryption_properties);
    let metadata = ArrowReaderMetadata::load(file, options.clone())?;

    let builder =
        ParquetRecordBatchReaderBuilder::try_new_with_options(file.try_clone().unwrap(), options)?;
    let batch_reader = builder.build()?;
    let batches = batch_reader.collect::<Result<Vec<RecordBatch>, _>>()?;
    Ok((batches, metadata))
}

pub(crate) fn read_and_roundtrip_to_encrypted_file(
    file: &File,
    decryption_properties: Arc<FileDecryptionProperties>,
    encryption_properties: Arc<FileEncryptionProperties>,
) {
    // read example data
    let (batches, metadata) =
        read_encrypted_file(file, Arc::clone(&decryption_properties)).unwrap();

    // write example data to a temporary file
    let temp_file = tempfile::tempfile().unwrap();
    let props = WriterProperties::builder()
        .with_file_encryption_properties(encryption_properties)
        .build();

    let mut writer = ArrowWriter::try_new(
        temp_file.try_clone().unwrap(),
        metadata.schema().clone(),
        Some(props),
    )
    .unwrap();
    for batch in batches {
        writer.write(&batch).unwrap();
    }

    writer.close().unwrap();

    // check re-written example data
    verify_encryption_test_file_read(temp_file, decryption_properties);
}

pub(crate) fn verify_encryption_test_file_read(
    file: File,
    decryption_properties: Arc<FileDecryptionProperties>,
) {
    let options =
        ArrowReaderOptions::default().with_file_decryption_properties(decryption_properties);
    let reader_metadata = ArrowReaderMetadata::load(&file, options.clone()).unwrap();
    let metadata = reader_metadata.metadata();

    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options).unwrap();
    let record_reader = builder.build().unwrap();
    let record_batches = record_reader
        .map(|x| x.unwrap())
        .collect::<Vec<RecordBatch>>();

    verify_encryption_test_data(record_batches, metadata);
}

/// A KeyRetriever to use in Parquet encryption tests,
/// which stores a map from key names/metadata to encryption key bytes.
pub struct TestKeyRetriever {
    keys: Mutex<HashMap<String, Vec<u8>>>,
}

impl TestKeyRetriever {
    pub fn new() -> Self {
        Self {
            keys: Mutex::new(HashMap::default()),
        }
    }

    pub fn with_key(self, key_name: String, key: Vec<u8>) -> Self {
        {
            let mut keys = self.keys.lock().unwrap();
            keys.insert(key_name, key);
        }
        self
    }
}

impl KeyRetriever for TestKeyRetriever {
    fn retrieve_key(&self, key_metadata: &[u8]) -> Result<Vec<u8>> {
        let key_metadata = std::str::from_utf8(key_metadata).map_err(|e| {
            ParquetError::General(format!("Could not convert key metadata to string: {e}"))
        })?;
        let keys = self.keys.lock().unwrap();
        match keys.get(key_metadata) {
            Some(key) => Ok(key.clone()),
            None => Err(ParquetError::General(format!(
                "Could not retrieve key for metadata {key_metadata:?}"
            ))),
        }
    }
}
