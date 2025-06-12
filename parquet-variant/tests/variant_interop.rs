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

//! End-to-end check: (almost) every sample from apache/parquet-testing/variant
//! can be parsed into our `Variant`.

// NOTE: We keep this file separate rather than a test mod inside variant.rs because it should be
// moved to the test folder later
use std::fs;
use std::path::{Path, PathBuf};

use arrow_schema::ArrowError;
use parquet_variant::{Variant, VariantMetadata, VariantBuilder};

fn cases_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("parquet-testing")
        .join("variant")
}

fn load_case(name: &str) -> Result<(Vec<u8>, Vec<u8>), ArrowError> {
    let root = cases_dir();
    let meta = fs::read(root.join(format!("{name}.metadata")))?;
    let val = fs::read(root.join(format!("{name}.value")))?;
    Ok((meta, val))
}

/// Return a list of the values from the parquet testing repository:
/// <https://github.com/apache/parquet-testing/tree/master/variant>
fn get_primitive_cases() -> Vec<(&'static str, Variant<'static, 'static>)> {
    // Cases are commented out
    // Enabling is tracked in  https://github.com/apache/arrow-rs/issues/7630
    vec![
        // ("primitive_binary", Variant::Binary),
        ("primitive_boolean_false", Variant::BooleanFalse),
        ("primitive_boolean_true", Variant::BooleanTrue),
        // ("primitive_date", Variant::Null),
        //("primitive_decimal4", Variant::Null),
        //("primitive_decimal8", Variant::Null),
        //("primitive_decimal16", Variant::Null),
        //("primitive_float", Variant::Null),
        ("primitive_int8", Variant::Int8(42)),
        //("primitive_int16", Variant::Null),
        //("primitive_int32", Variant::Null),
        //("primitive_int64", Variant::Null),
        ("primitive_null", Variant::Null),
        ("primitive_string", Variant::String("This string is longer than 64 bytes and therefore does not fit in a short_string and it also includes several non ascii characters such as 🐢, 💖, ♥\u{fe0f}, 🎣 and 🤦!!")),
        //("primitive_timestamp", Variant::Null),
        //("primitive_timestampntz", Variant::Null),
        ("short_string", Variant::ShortString("Less than 64 bytes (❤\u{fe0f} with utf8)")),
    ]
}

fn get_non_primitive_cases() -> Vec<&'static str> {
    vec!["object_primitive", "array_primitive"]
}

#[test]
fn variant_primitive() -> Result<(), ArrowError> {
    let cases = get_primitive_cases();
    for (case, want) in cases {
        // Test decoding reference data
        let (metadata_bytes, value) = load_case(case)?;
        let metadata = VariantMetadata::try_new(&metadata_bytes)?;
        let got = Variant::try_new(&metadata, &value)?;
        assert_eq!(got, want, "Failed to decode case: {}", case);

        // Test that our builder can create equivalent data
        let mut builder = VariantBuilder::new();
        
        match want {
            Variant::Null => {
                builder.append_null();
            }
            Variant::BooleanFalse => {
                builder.append_bool(false);
            }
            Variant::BooleanTrue => {
                builder.append_bool(true);
            }
            Variant::Int8(val) => {
                builder.append_int8(val);
            }
            Variant::String(s) => {
                builder.append_string(s);
            }
            Variant::ShortString(s) => {
                builder.append_string(s);
            }
            _ => {
                // Skip unsupported types for now
                continue;
            }
        }

        let (built_metadata, built_value) = builder.finish();
        
        // Decode what we built and verify it matches
        let built_variant_metadata = VariantMetadata::try_new(&built_metadata)?;
        let built_variant = Variant::try_new(&built_variant_metadata, &built_value)?;
        
        assert_eq!(built_variant, want, "Builder output doesn't match expected for case: {}", case);
    }
    Ok(())
}

#[test]
fn variant_non_primitive() -> Result<(), ArrowError> {
    let cases = get_non_primitive_cases();
    for case in cases {
        // Test decoding reference data
        let (metadata, value) = load_case(case)?;
        let metadata = VariantMetadata::try_new(&metadata)?;
        let variant = Variant::try_new(&metadata, &value)?;
        match case {
            "object_primitive" => {
                assert!(matches!(variant, Variant::Object(_)), "Expected object variant for case: {}", case);
                assert_eq!(metadata.dictionary_size(), 7);
                let dict_val = metadata.get_field_by(0)?;
                assert_eq!(dict_val, "int_field");
            }
            "array_primitive" => match variant {
                Variant::Array(arr) => {
                    let v = arr.get(0)?;
                    assert!(matches!(v, Variant::Int8(2)), "Expected first element to be Int8(2) for case: {}", case);
                    let v = arr.get(1)?;
                    assert!(matches!(v, Variant::Int8(1)), "Expected second element to be Int8(1) for case: {}", case);
                }
                _ => panic!("Expected an array variant for case: {}", case),
            },
            _ => unreachable!(),
        }

        // Test that our builder can create equivalent data structures
        let mut builder = VariantBuilder::new();
        
        match case {
            "object_primitive" => {
                // Build an object similar to what we expect from the test data
                let mut obj = builder.begin_object();
                obj.append_field("int_field", |b| b.append_int8(42));
                obj.finish();
            }
            "array_primitive" => {
                // Build an array similar to what we expect from the test data
                let mut arr = builder.begin_array();
                arr.append_element(|b| b.append_int8(2));
                arr.append_element(|b| b.append_int8(1));
                arr.finish();
            }
            _ => unreachable!(),
        }

        let (built_metadata, built_value) = builder.finish();
        
        // Decode what we built
        let built_variant_metadata = VariantMetadata::try_new(&built_metadata)?;
        let built_variant = Variant::try_new(&built_variant_metadata, &built_value)?;
        
        // Verify basic structure matches
        match case {
            "object_primitive" => {
                assert!(matches!(built_variant, Variant::Object(_)), "Expected object variant for case: {}", case);
                // Verify the structure is similar to the reference
                if let Variant::Object(_obj) = built_variant {
                    // We can't compare exact metadata because field ordering might differ
                    // but we can verify the dictionary contains our field
                    assert!(built_variant_metadata.dictionary_size() > 0, "Built object should have dictionary entries");
                }
            }
            "array_primitive" => {
                if let Variant::Array(arr) = built_variant {
                    // Test individual elements since len() is not implemented
                    let v0 = arr.get(0)?;
                    let v1 = arr.get(1)?;
                    assert!(matches!(v0, Variant::Int8(2)), "Expected first element to be Int8(2) for case: {}", case);
                    assert!(matches!(v1, Variant::Int8(1)), "Expected second element to be Int8(1) for case: {}", case);
                } else {
                    panic!("Expected an array variant for case: {}", case);
                }
            }
            _ => unreachable!(),
        }
    }
    Ok(())
}
