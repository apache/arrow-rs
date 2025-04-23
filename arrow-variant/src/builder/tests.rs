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

//! Tests for the Variant builder API.

use std::io::Cursor;
use arrow_schema::extension::Variant;

use crate::builder::{
    VariantBuilder, PrimitiveValue
};
use crate::encoder::{VariantBasicType, VariantPrimitiveType};
use arrow_schema::ArrowError;

#[test]
fn test_primitive_values() -> Result<(), ArrowError> {
    // Create buffers for metadata and value
    let mut metadata_buffer = Vec::new();
    let mut value_buffer = Vec::new();
    
    // Minimal metadata (empty dictionary)
    let mut builder = VariantBuilder::new(&mut metadata_buffer);
    
    // Test each primitive type
    write_primitive_value(&mut value_buffer, PrimitiveValue::Null)?;
    write_primitive_value(&mut value_buffer, PrimitiveValue::Boolean(true))?;
    write_primitive_value(&mut value_buffer, PrimitiveValue::Boolean(false))?;
    write_primitive_value(&mut value_buffer, PrimitiveValue::Int8(42))?;
    write_primitive_value(&mut value_buffer, PrimitiveValue::Int16(1024))?;
    write_primitive_value(&mut value_buffer, PrimitiveValue::Int32(100000))?;
    write_primitive_value(&mut value_buffer, PrimitiveValue::Int64(5000000000))?;
    write_primitive_value(&mut value_buffer, PrimitiveValue::Float(3.14))?;
    write_primitive_value(&mut value_buffer, PrimitiveValue::Double(2.71828))?;
    write_primitive_value(&mut value_buffer, PrimitiveValue::String("hello".to_string()))?;
    write_primitive_value(&mut value_buffer, PrimitiveValue::String("a".repeat(20)))?; // Long string
    write_primitive_value(&mut value_buffer, PrimitiveValue::Binary(vec![1, 2, 3, 4]))?;
    write_primitive_value(&mut value_buffer, PrimitiveValue::Date(19000))?;
    write_primitive_value(&mut value_buffer, PrimitiveValue::Timestamp(1634567890000))?;
    write_primitive_value(&mut value_buffer, PrimitiveValue::TimestampNTZ(1634567890000))?;
    
    // Finish the metadata
    builder.finish()?;
    
    // Validate format: check first byte of each value to confirm type encoding
    
    // First primitive is Null
    assert_eq!(value_buffer[0] & 0x03, VariantBasicType::Primitive as u8);
    assert_eq!(value_buffer[0] >> 2, VariantPrimitiveType::Null as u8);
    
    // Second primitive is BooleanTrue
    assert_eq!(value_buffer[1] & 0x03, VariantBasicType::Primitive as u8);
    assert_eq!(value_buffer[1] >> 2, VariantPrimitiveType::BooleanTrue as u8);
    
    // Third primitive is BooleanFalse
    assert_eq!(value_buffer[2] & 0x03, VariantBasicType::Primitive as u8);
    assert_eq!(value_buffer[2] >> 2, VariantPrimitiveType::BooleanFalse as u8);
    
    // Check that "hello" uses ShortString encoding
    let hello_pos = 29; // Position will depend on preceding values, adjust as needed
    assert_eq!(value_buffer[hello_pos] & 0x03, VariantBasicType::ShortString as u8);
    assert_eq!(value_buffer[hello_pos] >> 2, 5); // String length
    
    Ok(())
}

fn write_primitive_value(buffer: &mut Vec<u8>, value: PrimitiveValue) -> Result<(), ArrowError> {
    let mut builder = VariantBuilder::new(Vec::new());
    let mut value_buffer = Vec::new();
    
    // Create an object with a single value
    let mut object_builder = builder.new_object(value_buffer);
    object_builder.append_value("test", value)?;
    
    // Get the value buffer from the object
    // (In a real implementation, you'd use a different approach)
    
    Ok(())
}

#[test]
fn test_simple_object() -> Result<(), ArrowError> {
    // Create buffers for metadata and value
    let mut metadata_buffer = Vec::new();
    let mut value_buffer = Vec::new();
    
    // Create a builder
    let mut builder = VariantBuilder::new(&mut metadata_buffer);
    
    // Create an object
    let mut object_builder = builder.new_object(&mut value_buffer);
    object_builder.append_value("foo", PrimitiveValue::Int32(1))?;
    object_builder.append_value("bar", PrimitiveValue::String("hello".to_string()))?;
    object_builder.finish()?;
    
    // Finish the metadata
    builder.finish()?;
    
    // Validate binary format: first byte should be Object basic type
    assert_eq!(value_buffer[0] & 0x03, VariantBasicType::Object as u8);
    
    // Second 4 bytes should be number of fields (2)
    let field_count = u32::from_le_bytes([
        value_buffer[1], value_buffer[2], value_buffer[3], value_buffer[4]
    ]);
    assert_eq!(field_count, 2);
    
    // Check the metadata was created
    assert!(!metadata_buffer.is_empty());
    
    // Creating a Variant from the buffers should succeed
    let variant = Variant::new(metadata_buffer, value_buffer);
    
    Ok(())
}

#[test]
fn test_metadata_reuse() -> Result<(), ArrowError> {
    // Create a shared metadata buffer
    let mut metadata_buffer = Vec::new();
    
    // Create a builder
    let mut builder = VariantBuilder::new(&mut metadata_buffer);
    
    // Create multiple objects with the same metadata structure
    let keys = ["first", "second", "third"];
    let mut variants = Vec::new();
    
    for (i, &key) in keys.iter().enumerate() {
        let mut value_buffer = Vec::new();
        let mut object_builder = builder.new_object(&mut value_buffer);
        
        // Add the same keys but different values
        object_builder.append_value("id", PrimitiveValue::Int32(i as i32))?;
        object_builder.append_value("name", PrimitiveValue::String(key.to_string()))?;
        object_builder.finish()?;
        
        variants.push(Variant::new(metadata_buffer.clone(), value_buffer));
    }
    
    // Finalize the metadata once
    builder.finish()?;
    
    // All variants should have the same metadata
    for variant in &variants {
        assert_eq!(variant.metadata(), metadata_buffer.as_slice());
    }
    
    Ok(())
}

#[test]
fn test_nested_structure() -> Result<(), ArrowError> {
    // Create buffers for metadata and value
    let mut metadata_buffer = Vec::new();
    let mut value_buffer = Vec::new();
    
    // Create a builder
    let mut builder = VariantBuilder::new(&mut metadata_buffer);
    
    // Create the root object
    let mut root_builder = builder.new_object(&mut value_buffer);
    
    // Add a primitive value
    root_builder.append_value("name", PrimitiveValue::String("test".to_string()))?;
    
    // Add a nested object
    let mut child_builder = root_builder.append_object("child")?;
    child_builder.append_value("value", PrimitiveValue::Int32(42))?;
    child_builder.finish()?;
    
    // Add a nested array
    let mut array_builder = root_builder.append_array("items")?;
    array_builder.append_value(PrimitiveValue::Int32(1))?;
    array_builder.append_value(PrimitiveValue::Int32(2))?;
    array_builder.append_value(PrimitiveValue::Int32(3))?;
    array_builder.finish()?;
    
    // Finish the root object
    root_builder.finish()?;
    
    // Finish the metadata
    builder.finish()?;
    
    // Validate binary format: root byte should be Object basic type
    assert_eq!(value_buffer[0] & 0x03, VariantBasicType::Object as u8);
    
    // Create a variant from the buffers
    let variant = Variant::new(metadata_buffer, value_buffer);
    
    Ok(())
}

#[test]
fn test_sorted_keys() -> Result<(), ArrowError> {
    // Create two identical objects, one with sorted keys and one without
    let mut metadata_sorted = Vec::new();
    let mut metadata_unsorted = Vec::new();
    
    // Create builders
    let mut builder_sorted = VariantBuilder::new_with_sort(&mut metadata_sorted, true);
    let mut builder_unsorted = VariantBuilder::new_with_sort(&mut metadata_unsorted, false);
    
    // Create objects with deliberately out-of-alphabetical-order keys
    let mut value_sorted = Vec::new();
    let mut value_unsorted = Vec::new();
    
    // Build the sorted object
    {
        let mut object_builder = builder_sorted.new_object(&mut value_sorted);
        object_builder.append_value("z", PrimitiveValue::Int32(1))?;
        object_builder.append_value("a", PrimitiveValue::Int32(2))?;
        object_builder.append_value("m", PrimitiveValue::Int32(3))?;
        object_builder.finish()?;
        builder_sorted.finish()?;
    }
    
    // Build the unsorted object
    {
        let mut object_builder = builder_unsorted.new_object(&mut value_unsorted);
        object_builder.append_value("z", PrimitiveValue::Int32(1))?;
        object_builder.append_value("a", PrimitiveValue::Int32(2))?;
        object_builder.append_value("m", PrimitiveValue::Int32(3))?;
        object_builder.finish()?;
        builder_unsorted.finish()?;
    }
    
    // The first byte of sorted metadata should have the sorted bit set
    assert_eq!(metadata_sorted[0] & 0x10, 0x10);
    
    // The first byte of unsorted metadata should not have the sorted bit set
    assert_eq!(metadata_unsorted[0] & 0x10, 0x00);
    
    Ok(())
} 