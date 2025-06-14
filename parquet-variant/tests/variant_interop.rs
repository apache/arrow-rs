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
use chrono::NaiveDate;
use parquet_variant::{Variant, VariantBuilder, VariantMetadata};
type BuilderTestFn = fn(&mut VariantBuilder);

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
        ("primitive_binary", Variant::Binary(&[0x03, 0x13, 0x37, 0xde, 0xad, 0xbe, 0xef, 0xca, 0xfe])),
        ("primitive_boolean_false", Variant::BooleanFalse),
        ("primitive_boolean_true", Variant::BooleanTrue),
        ("primitive_date", Variant::Date(NaiveDate::from_ymd_opt(2025, 4 , 16).unwrap())),
        ("primitive_decimal4", Variant::Decimal4{integer: 1234, scale: 2}),
        ("primitive_decimal8", Variant::Decimal8{integer: 1234567890, scale: 2}),
        ("primitive_decimal16", Variant::Decimal16{integer: 1234567891234567890, scale: 2}),
        ("primitive_float", Variant::Float(1234567890.1234)),
        ("primitive_double", Variant::Double(1234567890.1234)),
        ("primitive_int8", Variant::Int8(42)),
        ("primitive_int16", Variant::Int16(1234)),
        ("primitive_int32", Variant::Int32(123456)),
        ("primitive_int64", Variant::Int64(1234567890123456789)),
        ("primitive_null", Variant::Null),
        ("primitive_string", Variant::String("This string is longer than 64 bytes and therefore does not fit in a short_string and it also includes several non ascii characters such as 🐢, 💖, ♥\u{fe0f}, 🎣 and 🤦!!")),
        ("primitive_timestamp", Variant::TimestampMicros(NaiveDate::from_ymd_opt(2025, 4, 16).unwrap().and_hms_milli_opt(16, 34, 56, 780).unwrap().and_utc())),
        ("primitive_timestampntz", Variant::TimestampNtzMicros(NaiveDate::from_ymd_opt(2025, 4, 16).unwrap().and_hms_milli_opt(12, 34, 56, 780).unwrap())),
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
    let builder_cases: [(&'static str, BuilderTestFn); 6] = [
        ("primitive_boolean_false", |b: &mut VariantBuilder| {
            b.append_value(false)
        }),
        ("primitive_boolean_true", |b: &mut VariantBuilder| {
            b.append_value(true)
        }),
        ("primitive_int8", |b: &mut VariantBuilder| {
            b.append_value(42i8)
        }),
        ("primitive_null", |b: &mut VariantBuilder| {
            b.append_value(())
        }),
        ("short_string", |b: &mut VariantBuilder| {
            b.append_value("Less than 64 bytes (❤\u{fe0f} with utf8)")
        }),
        ("primitive_string", |b: &mut VariantBuilder| {
            b.append_value("This string is longer than 64 bytes and therefore does not fit in a short_string and it also includes several non ascii characters such as 🐢, 💖, ♥\u{fe0f}, 🎣 and 🤦!!")
        }),
    ];

    for (case, build_fn) in builder_cases {
        let mut builder = VariantBuilder::new();
        build_fn(&mut builder);
        let (built_metadata, built_value) = builder.finish();
        let (expected_metadata, expected_value) = load_case(case)?;

        assert_eq!(built_metadata, expected_metadata);
        assert_eq!(built_value, expected_value);
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
fn variant_array_builder() -> Result<(), ArrowError> {
    let mut builder = VariantBuilder::new();

    let mut arr = builder.new_array();
    arr.append_value(2i8);
    arr.append_value(1i8);
    arr.append_value(5i8);
    arr.append_value(9i8);
    arr.finish();

    let (built_metadata, built_value) = builder.finish();
    let (expected_metadata, expected_value) = load_case("array_primitive")?;

    assert_eq!(built_metadata, expected_metadata);
    assert_eq!(built_value, expected_value);

    Ok(())
}

#[test]
fn variant_object_builder() -> Result<(), ArrowError> {
    let mut builder = VariantBuilder::new();

    let mut obj = builder.new_object();
    obj.append_value("int_field", 1i8);

    // The double field is actually encoded as decimal4 with scale 8
    // Value: 123456789, Scale: 8 -> 1.23456789
    obj.append_value("double_field", (123456789i32, 8u8));
    obj.append_value("boolean_true_field", true);
    obj.append_value("boolean_false_field", false);
    obj.append_value("string_field", "Apache Parquet");
    obj.append_value("null_field", ());
    obj.append_value("timestamp_field", "2025-04-16T12:34:56.78");

    obj.finish();

    let (built_metadata, built_value) = builder.finish();
    let (expected_metadata, expected_value) = load_case("object_primitive")?;

    assert_eq!(built_metadata, expected_metadata);
    assert_eq!(built_value, expected_value);

    Ok(())
}
