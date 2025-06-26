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

use parquet_variant::{
    json_to_variant, variant_to_json, variant_to_json_string, variant_to_json_value,
    SampleVecBasedVariantBufferManager,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The caller must provide an object implementing the `VariantBufferManager` trait to the library.
    // This allows the library to write the constructed variant to buffers provided by the caller.
    // This way, the caller has direct control over the output buffers.
    let mut variant_buffer_manager = SampleVecBasedVariantBufferManager {
        value_buffer: vec![0u8; 1],
        metadata_buffer: vec![0u8; 1],
    };

    let person_string = "{\"name\":\"Alice\", \"age\":30, ".to_string()
        + "\"email\":\"alice@example.com\", \"is_active\": true, \"score\": 95.7,"
        + "\"additional_info\": null}";
    let (metadata_size, value_size) = json_to_variant(&person_string, &mut variant_buffer_manager)?;

    let variant = parquet_variant::Variant::try_new(
        &variant_buffer_manager.metadata_buffer[..metadata_size],
        &variant_buffer_manager.value_buffer[..value_size],
    )?;

    let json_result = variant_to_json_string(&variant)?;
    let json_value = variant_to_json_value(&variant)?;
    let pretty_json = serde_json::to_string_pretty(&json_value)?;
    println!("{}", pretty_json);

    let mut buffer = Vec::new();
    variant_to_json(&mut buffer, &variant)?;
    let buffer_result = String::from_utf8(buffer)?;
    assert_eq!(json_result, "{\"additional_info\":null,\"age\":30,".to_string() +
    "\"email\":\"alice@example.com\",\"is_active\":true,\"name\":\"Alice\",\"score\":95.7}");
    assert_eq!(json_result, buffer_result);
    assert_eq!(json_result, serde_json::to_string(&json_value)?);

    Ok(())
}
