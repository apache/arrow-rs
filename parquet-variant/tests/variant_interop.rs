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
        let (metadata_bytes, value) = load_case(case)?;
        let metadata = VariantMetadata::try_new(&metadata_bytes)?;
        let got = Variant::try_new(&metadata, &value)?;
        assert_eq!(got, want);
    }
    Ok(())
}

#[test]
fn variant_primitive_builder() -> Result<(), ArrowError> {
    let builder_cases: [(&str, fn(&mut VariantBuilder)); 6] = [
        ("primitive_boolean_false", |b: &mut VariantBuilder| b.append(&Variant::BooleanFalse)),
        ("primitive_boolean_true", |b: &mut VariantBuilder| b.append(&Variant::BooleanTrue)),
        ("primitive_int8", |b: &mut VariantBuilder| b.append(&Variant::Int8(42))),
        ("primitive_null", |b: &mut VariantBuilder| b.append(&Variant::Null)),
        ("short_string", |b: &mut VariantBuilder| b.append(&Variant::ShortString("Less than 64 bytes (❤\u{fe0f} with utf8)"))),
        ("primitive_string", |b: &mut VariantBuilder| b.append(&Variant::String("This string is longer than 64 bytes and therefore does not fit in a short_string and it also includes several non ascii characters such as 🐢, 💖, ♥\u{fe0f}, 🎣 and 🤦!!"))),
    ];

    for (case, build_fn) in builder_cases {
        let mut builder = VariantBuilder::new();
        build_fn(&mut builder);
        let (built_metadata, built_value) = builder.finish();
        let built_variant_metadata = VariantMetadata::try_new(&built_metadata)?;
        let built_variant = Variant::try_new(&built_variant_metadata, &built_value)?;

        // Load the reference data to compare against
        let (metadata_bytes, value) = load_case(case)?;
        let metadata = VariantMetadata::try_new(&metadata_bytes)?;
        let want = Variant::try_new(&metadata, &value)?;
        assert_eq!(built_variant, want, "Builder output doesn't match expected for case: {}", case);
    }
    Ok(())
}

#[test]
fn variant_non_primitive() -> Result<(), ArrowError> {
    let cases = get_non_primitive_cases();
    for case in cases {
        let (metadata, value) = load_case(case)?;
        let metadata = VariantMetadata::try_new(&metadata)?;
        let variant = Variant::try_new(&metadata, &value)?;
        match case {
            "object_primitive" => {
                assert!(matches!(variant, Variant::Object(_)));
                assert_eq!(metadata.dictionary_size(), 7);
                let dict_val = metadata.get_field_by(0)?;
                assert_eq!(dict_val, "int_field");
            }
            "array_primitive" => match variant {
                Variant::Array(arr) => {
                    let v = arr.get(0)?;
                    assert!(matches!(v, Variant::Int8(2)));
                    let v = arr.get(1)?;
                    assert!(matches!(v, Variant::Int8(1)));
                }
                _ => panic!("expected an array"),
            },
            _ => unreachable!(),
        }
    }
    Ok(())
}

#[test]
fn variant_non_primitive_builder() -> Result<(), ArrowError> {
    let builder_cases: [(&str, fn(&mut VariantBuilder)); 2] = [
        ("object_primitive", |b: &mut VariantBuilder| {
            let mut obj = b.begin_object();
            obj.append_field("int_field", |b| b.append(&Variant::Int8(42)));
            obj.finish();
        }),
        ("array_primitive", |b: &mut VariantBuilder| {
            let mut arr = b.begin_array();
            arr.append_element(|b| b.append(&Variant::Int8(2)));
            arr.append_element(|b| b.append(&Variant::Int8(1)));
            arr.finish();
        }),
    ];

    for (case, build_fn) in builder_cases {
        let mut builder = VariantBuilder::new();
        build_fn(&mut builder);
        let (built_metadata, built_value) = builder.finish();
        let built_variant_metadata = VariantMetadata::try_new(&built_metadata)?;
        let built_variant = Variant::try_new(&built_variant_metadata, &built_value)?;

        // Load the reference data to compare against
        let (metadata, value) = load_case(case)?;
        let metadata = VariantMetadata::try_new(&metadata)?;
        let want = Variant::try_new(&metadata, &value)?;

        match case {
            "object_primitive" => {
                assert!(matches!(built_variant, Variant::Object(_)), "Expected object variant for case: {}", case);
                if let Variant::Object(_obj) = built_variant {
                    assert!(built_variant_metadata.dictionary_size() > 0, "Built object should have dictionary entries");
                }
            }
            "array_primitive" => {
                if let Variant::Array(arr) = built_variant {
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