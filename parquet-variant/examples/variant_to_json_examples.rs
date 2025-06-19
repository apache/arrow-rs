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

use parquet_variant::{variant_to_json, variant_to_json_string, variant_to_json_value, Variant};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let variants = vec![
        ("Null", Variant::Null),
        ("Boolean True", Variant::BooleanTrue),
        ("Boolean False", Variant::BooleanFalse),
        ("Integer 42", Variant::Int8(42)),
        ("Negative Integer", Variant::Int8(-123)),
        ("String", Variant::String("Hello, World!")),
        ("Short String", Variant::ShortString("Hi!")),
    ];

    for (name, variant) in variants {
        let json_string = variant_to_json_string(&variant)?;
        println!("   {} -> {}", name, json_string);
    }

    // Example 2: String escaping
    println!("\n🔤 2. String Escaping:");

    let special_string =
        Variant::String("Line 1\nLine 2\tTabbed\r\nWith \"quotes\" and \\backslashes");
    let escaped_json = variant_to_json_string(&special_string)?;
    println!("   Original: Line 1\\nLine 2\\tTabbed\\r\\nWith \"quotes\" and \\\\backslashes");
    println!("   JSON:     {}", escaped_json);

    let unicode_variants = vec![
        Variant::String("Hello 世界 🌍"),
        Variant::String("Emoji: 💻"),
        Variant::String("Math: α + β = γ"),
    ];

    for variant in unicode_variants {
        let json_string = variant_to_json_string(&variant)?;
        println!("   {}", json_string);
    }

    let test_variant = Variant::String("Buffer test");

    // Write to Vec<u8>
    let mut vec_buffer = Vec::new();
    variant_to_json(&mut vec_buffer, &test_variant)?;
    println!("   Vec<u8> buffer: {}", String::from_utf8(vec_buffer)?);

    // Write to String (through write! macro)
    let mut string_buffer = String::new();
    use std::fmt::Write;
    write!(string_buffer, "Prefix: ")?;

    // Convert to bytes temporarily to use the Write trait
    let mut temp_buffer = Vec::new();
    variant_to_json(&mut temp_buffer, &test_variant)?;
    string_buffer.push_str(&String::from_utf8(temp_buffer)?);
    println!("   String buffer: {}", string_buffer);

    let variants_for_value = vec![
        Variant::Null,
        Variant::BooleanTrue,
        Variant::Int8(100),
        Variant::String("Value conversion"),
    ];

    for variant in variants_for_value {
        let json_value = variant_to_json_value(&variant)?;
        println!("   {:?} -> {:?}", variant, json_value);
    }

    let start = std::time::Instant::now();
    for i in 0..1000 {
        let variant = Variant::Int8((i % 128) as i8);
        let _json = variant_to_json_string(&variant)?;
    }
    let duration = start.elapsed();
    println!("   Converted 1000 variants in {:?}", duration);

    // This would demonstrate error handling if we had invalid variants
    // For now, all our examples work, so we'll just show the pattern
    match variant_to_json_string(&Variant::String("Valid string")) {
        Ok(json) => println!("   Success: {}", json),
        Err(e) => println!("   Error: {}", e),
    }

    Ok(())
}
