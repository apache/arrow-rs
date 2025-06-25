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

//! Example showing how to convert Variant values to JSON

use arrow_schema::ArrowError;
use parquet_variant::{
    json_to_variant, variant_to_json, variant_to_json_string, variant_to_json_value,
    VariantBufferManager,
};

/// The caller must provide an object implementing the `VariantBufferManager` trait to the library.
/// This allows the library to write the constructed variant to buffers provided by the caller.
/// This way, the caller has direct control over the output buffers.
pub struct SampleVariantBufferManager {
    pub value_buffer: Box<[u8]>,
    pub metadata_buffer: Box<[u8]>,
}

impl VariantBufferManager for SampleVariantBufferManager {
    #[inline(always)]
    fn borrow_value_buffer(&mut self) -> &mut [u8] {
        &mut self.value_buffer
    }

    fn ensure_value_buffer_size(&mut self, size: usize) -> Result<(), ArrowError> {
        let cur_len = self.value_buffer.len();
        if size > cur_len {
            // Reallocate larger buffer
            let mut new_buffer = vec![0u8; size].into_boxed_slice();
            new_buffer[..cur_len].copy_from_slice(&self.value_buffer);
            self.value_buffer = new_buffer;
        }
        Ok(())
    }

    #[inline(always)]
    fn borrow_metadata_buffer(&mut self) -> &mut [u8] {
        &mut self.metadata_buffer
    }

    fn ensure_metadata_buffer_size(&mut self, size: usize) -> Result<(), ArrowError> {
        let cur_len = self.metadata_buffer.len();
        if size > cur_len {
            // Reallocate larger buffer
            let mut new_buffer = vec![0u8; size].into_boxed_slice();
            new_buffer[..cur_len].copy_from_slice(&self.metadata_buffer);
            self.metadata_buffer = new_buffer;
        }
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut variant_buffer_manager = SampleVariantBufferManager {
        value_buffer: vec![0u8; 1].into_boxed_slice(),
        metadata_buffer: vec![0u8; 1].into_boxed_slice(),
    };

    let person_string = "{\"name\":\"Alice\", \"age\":30, ".to_string()
        + "\"email\":\"alice@example.com\", \"is_active\": true, \"score\": 95.7,"
        + "\"additional_info\": null}";
    let mut value_size = 0usize;
    let mut metadata_size = 0usize;
    json_to_variant(
        &person_string,
        &mut variant_buffer_manager,
        &mut value_size,
        &mut metadata_size,
    )?;

    let variant = parquet_variant::Variant::try_new(
        &variant_buffer_manager.metadata_buffer,
        &variant_buffer_manager.value_buffer,
    )?;

    let json_string = variant_to_json_string(&variant)?;
    let json_value = variant_to_json_value(&variant)?;
    let pretty_json = serde_json::to_string_pretty(&json_value)?;
    println!("{}", pretty_json);

    let mut buffer = Vec::new();
    variant_to_json(&mut buffer, &variant)?;
    let buffer_result = String::from_utf8(buffer)?;

    // Verify all methods produce the same result
    assert_eq!(json_string, buffer_result);
    assert_eq!(json_string, serde_json::to_string(&json_value)?);

    Ok(())
}
