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

use crate::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use crate::arrow::ArrowWriter;
use crate::encryption::decryption::FileDecryptionProperties;
use crate::encryption::encrypt::FileEncryptionProperties;
use crate::file::properties::WriterProperties;
use arrow_array::cast::AsArray;
use arrow_array::{types, RecordBatch};
use std::fs::File;

/// Tests reading an encrypted file from the parquet-testing repository
pub fn verify_encryption_test_file_read(
    file: File,
    decryption_properties: FileDecryptionProperties,
) {
    let options = ArrowReaderOptions::default()
        .with_file_decryption_properties(decryption_properties.clone());
    let metadata = ArrowReaderMetadata::load(&file, options.clone()).unwrap();
    let file_metadata = metadata.metadata.file_metadata();

    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options).unwrap();
    let record_reader = builder.build().unwrap();

    assert_eq!(file_metadata.num_rows(), 50);
    assert_eq!(file_metadata.schema_descr().num_columns(), 8);

    metadata.metadata.row_groups().iter().for_each(|rg| {
        assert_eq!(rg.num_columns(), 8);
        assert_eq!(rg.num_rows(), 50);
    });

    let mut row_count = 0;
    for batch in record_reader {
        let batch = batch.unwrap();
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
            assert_eq!(x.unwrap(), i as i32);
        }
        for (i, list_item) in list_col.iter().enumerate() {
            let list_item = list_item.unwrap();
            let list_item = list_item.as_primitive::<types::Int64Type>();
            assert_eq!(list_item.len(), 2);
            assert_eq!(list_item.value(0), ((i * 2) * 1000000000000) as i64);
            assert_eq!(list_item.value(1), ((i * 2 + 1) * 1000000000000) as i64);
        }
        for x in timestamp_col.iter() {
            assert!(x.is_some());
        }
        for (i, x) in f32_col.iter().enumerate() {
            assert_eq!(x.unwrap(), i as f32 * 1.1f32);
        }
        for (i, x) in f64_col.iter().enumerate() {
            assert_eq!(x.unwrap(), i as f64 * 1.1111111f64);
        }
        for (i, x) in binary_col.iter().enumerate() {
            assert_eq!(x.is_some(), i % 2 == 0);
            if let Some(x) = x {
                assert_eq!(&x[0..7], b"parquet");
            }
        }
        for (i, x) in fixed_size_binary_col.iter().enumerate() {
            assert_eq!(x.unwrap(), &[i as u8; 10]);
        }
    }

    assert_eq!(row_count, file_metadata.num_rows() as usize);
}

#[cfg(feature = "encryption")]
pub fn read_and_roundtrip_to_encrypted_file(
    path: &str,
    decryption_properties: FileDecryptionProperties,
    encryption_properties: FileEncryptionProperties,
) {
    let temp_file = tempfile::tempfile().unwrap();

    // read example data
    let file = File::open(path).unwrap();
    let options = ArrowReaderOptions::default()
        .with_file_decryption_properties(decryption_properties.clone());
    let metadata = ArrowReaderMetadata::load(&file, options.clone()).unwrap();

    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options).unwrap();
    let batch_reader = builder.build().unwrap();
    let batches = batch_reader
        .collect::<crate::errors::Result<Vec<RecordBatch>, _>>()
        .unwrap();

    // write example data
    let props = WriterProperties::builder()
        .with_file_encryption_properties(encryption_properties)
        .build();

    let mut writer =
        ArrowWriter::try_new(temp_file.try_clone().unwrap(), metadata.schema, Some(props)).unwrap();
    for batch in batches {
        writer.write(&batch).unwrap();
    }

    writer.close().unwrap();

    // check re-written example data
    verify_encryption_test_file_read(temp_file, decryption_properties);
}
