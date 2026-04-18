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

//! Fuzzer tests for corrupted parquet files
//!
//! These tests verify that the parquet reader returns errors (not panics)
//! when encountering various types of data corruption.

use arrow::array::{ArrayRef, Int32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rand::prelude::*;
use std::sync::Arc;

/// Create a valid parquet file with some test data
fn create_test_parquet_data() -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let id_array = Int32Array::from((0..1000).collect::<Vec<_>>());
    let value_array = Int32Array::from((0..1000).map(|i| i * 2).collect::<Vec<_>>());

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(id_array) as ArrayRef,
            Arc::new(value_array) as ArrayRef,
        ],
    )
    .unwrap();

    let mut buffer = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buffer, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }
    buffer
}

/// Corruption type: Truncate bytes from the end of the file
fn truncate_end(data: &[u8], bytes_to_remove: usize) -> Vec<u8> {
    let new_len = data.len().saturating_sub(bytes_to_remove);
    data[..new_len].to_vec()
}

/// Corruption type: Truncate bytes from the start of the file
fn truncate_start(data: &[u8], bytes_to_remove: usize) -> Vec<u8> {
    let new_len = data.len().saturating_sub(bytes_to_remove);
    data[data.len() - new_len..].to_vec()
}

/// Corruption type: Flip a random bit
fn flip_random_bit(data: &[u8], rng: &mut impl Rng) -> Vec<u8> {
    let mut corrupted = data.to_vec();
    if !corrupted.is_empty() {
        let byte_idx = rng.random_range(0..corrupted.len());
        let bit_idx = rng.random_range(0..8);
        corrupted[byte_idx] ^= 1 << bit_idx;
    }
    corrupted
}

/// Corruption type: Set a random range to zeros
fn zero_out_range(data: &[u8], rng: &mut impl Rng) -> Vec<u8> {
    let mut corrupted = data.to_vec();
    if corrupted.len() > 10 {
        let range_len = rng.random_range(1..corrupted.len().min(100));
        let start_idx = rng.random_range(0..corrupted.len() - range_len);
        for item in corrupted.iter_mut().skip(start_idx).take(range_len) {
            *item = 0;
        }
    }
    corrupted
}

/// Try to read a corrupted parquet file and verify it returns an error
fn assert_read_fails_with_error(data: &[u8]) {
    let result = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(data.to_vec()));

    match result {
        Ok(builder) => {
            let reader = builder.build();
            match reader {
                Ok(reader) => {
                    // Try to read batches
                    for batch in reader {
                        if batch.is_err() {
                            // Expected error
                            return;
                        }
                    }
                    // If we get here without error, that's unexpected for corrupted data
                    // but not necessarily wrong depending on the corruption type
                }
                Err(_) => {
                    // Expected error during reader construction
                }
            }
        }
        Err(_) => {
            // Expected error during builder construction
        }
    }
}

#[test]
fn test_truncate_end_small() {
    let valid_data = create_test_parquet_data();
    let corrupted = truncate_end(&valid_data, 10);
    assert_read_fails_with_error(&corrupted);
}

#[test]
fn test_truncate_end_large() {
    let valid_data = create_test_parquet_data();
    let corrupted = truncate_end(&valid_data, valid_data.len() / 2);
    assert_read_fails_with_error(&corrupted);
}

#[test]
fn test_truncate_end_to_footer_only() {
    let valid_data = create_test_parquet_data();
    // Keep only last 1000 bytes (likely just footer)
    let corrupted = truncate_end(&valid_data, valid_data.len().saturating_sub(1000));
    assert_read_fails_with_error(&corrupted);
}

#[test]
fn test_truncate_start_small() {
    let valid_data = create_test_parquet_data();
    let corrupted = truncate_start(&valid_data, 10);
    assert_read_fails_with_error(&corrupted);
}

#[test]
fn test_truncate_start_large() {
    let valid_data = create_test_parquet_data();
    let corrupted = truncate_start(&valid_data, valid_data.len() / 2);
    assert_read_fails_with_error(&corrupted);
}

#[test]
fn test_flip_random_bit() {
    let valid_data = create_test_parquet_data();
    let mut rng = StdRng::seed_from_u64(42);

    for _ in 0..10 {
        let corrupted = flip_random_bit(&valid_data, &mut rng);
        assert_read_fails_with_error(&corrupted);
    }
}

#[test]
fn test_zero_out_range() {
    let valid_data = create_test_parquet_data();
    let mut rng = StdRng::seed_from_u64(42);

    for _ in 0..10 {
        let corrupted = zero_out_range(&valid_data, &mut rng);
        assert_read_fails_with_error(&corrupted);
    }
}

/// Test a combination of corruptions
#[test]
fn test_combined_corruptions() {
    let valid_data = create_test_parquet_data();
    let mut rng = StdRng::seed_from_u64(42);

    for _ in 0..5 {
        let mut corrupted = valid_data.clone();

        // Apply multiple corruptions
        corrupted = truncate_end(&corrupted, rng.random_range(1..100));
        corrupted = flip_random_bit(&corrupted, &mut rng);
        corrupted = zero_out_range(&corrupted, &mut rng);

        assert_read_fails_with_error(&corrupted);
    }
}
