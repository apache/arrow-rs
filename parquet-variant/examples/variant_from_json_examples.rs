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
    SampleBoxBasedVariantBufferManager, SampleVecBasedVariantBufferManager, VariantBufferManager,
};

fn from_json_example<T: VariantBufferManager>(
    variant_buffer_manager: &mut T,
) -> Result<(), Box<dyn std::error::Error>> {
    let person_string = "{\"name\":\"Alice\", \"age\":30, ".to_string()
        + "\"email\":\"alice@example.com\", \"is_active\": true, \"score\": 95.7,"
        + "\"additional_info\": null}";
    let (metadata_size, value_size) = json_to_variant(&person_string, variant_buffer_manager)?;

    let variant = parquet_variant::Variant::try_new(
        &variant_buffer_manager.get_immutable_metadata_buffer()[..metadata_size],
        &variant_buffer_manager.get_immutable_value_buffer()[..value_size],
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The caller must provide an object implementing the `VariantBufferManager` trait to the library.
    // This allows the library to write the constructed variant to buffers provided by the caller.
    // This way, the caller has direct control over the output buffers.
    let mut box_based_buffer_manager = SampleBoxBasedVariantBufferManager {
        value_buffer: vec![0u8; 1].into_boxed_slice(),
        metadata_buffer: vec![0u8; 1].into_boxed_slice(),
    };

    let mut vec_based_buffer_manager = SampleVecBasedVariantBufferManager {
        value_buffer: vec![0u8; 1],
        metadata_buffer: vec![0u8; 1],
    };

    from_json_example(&mut box_based_buffer_manager)?;
    from_json_example(&mut vec_based_buffer_manager)?;
    Ok(())
}
