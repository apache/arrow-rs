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
use arrow_array::{types, RecordBatch};
use parquet::encryption::decrypt::KeyRetriever;
use parquet::errors::{ParquetError, Result};
use parquet::file::metadata::ParquetMetaData;
use std::collections::HashMap;
use std::sync::Mutex;

/// Verifies data read from an encrypted file from the parquet-testing repository
pub fn verify_encryption_test_data(record_batches: Vec<RecordBatch>, metadata: &ParquetMetaData) {
    let file_metadata = metadata.file_metadata();
    assert_eq!(file_metadata.num_rows(), 50);
    assert_eq!(file_metadata.schema_descr().num_columns(), 8);

    metadata.row_groups().iter().for_each(|rg| {
        assert_eq!(rg.num_columns(), 8);
        assert_eq!(rg.num_rows(), 50);
    });

    let mut row_count = 0;
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

/// Verifies that the column and offset indexes were successfully read from an
/// encrypted test file.
pub fn verify_column_indexes(metadata: &ParquetMetaData) {
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
        parquet::file::page_index::index::Index::FLOAT(float_index) => {
            assert_eq!(float_index.indexes.len(), 1);
            assert_eq!(float_index.indexes[0].min, Some(0.0f32));
            assert!(float_index.indexes[0]
                .max
                .is_some_and(|max| (max - 53.9).abs() < 1e-6));
        }
        _ => {
            panic!("Expected a float column index for column {}", float_col_idx);
        }
    };
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
            ParquetError::General(format!("Could not convert key metadata to string: {}", e))
        })?;
        let keys = self.keys.lock().unwrap();
        match keys.get(key_metadata) {
            Some(key) => Ok(key.clone()),
            None => Err(ParquetError::General(format!(
                "Could not retrieve key for metadata {:?}",
                key_metadata
            ))),
        }
    }
}
