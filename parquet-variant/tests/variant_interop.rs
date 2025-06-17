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
use parquet_variant::{Variant, VariantMetadata};

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
        let (metadata, value) = load_case(case)?;
        let got = Variant::try_new(&metadata, &value)?;
        assert_eq!(got, want);
    }
    Ok(())
}

#[test]
fn variant_non_primitive() -> Result<(), ArrowError> {
    let cases = get_non_primitive_cases();
    for case in cases {
        let (metadata, value) = load_case(case)?;
        let variant_metadata = VariantMetadata::try_new(&metadata)?;
        let variant = Variant::try_new(&metadata, &value)?;
        match case {
            "object_primitive" => {
                assert!(matches!(variant, Variant::Object(_)));
                assert_eq!(variant_metadata.dictionary_size(), 7);
                let dict_val = variant_metadata.get_field_by(0)?;
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
