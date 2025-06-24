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

use chrono::NaiveDate;
use parquet_variant::{
    ShortString, Variant, VariantBuilder, VariantDecimal16, VariantDecimal4, VariantDecimal8,
};

fn cases_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("parquet-testing")
        .join("variant")
}

struct Case {
    metadata: Vec<u8>,
    value: Vec<u8>,
}

impl Case {
    /// Load the case with the given name from the parquet testing repository.
    fn load(name: &str) -> Self {
        let root = cases_dir();
        let metadata = fs::read(root.join(format!("{name}.metadata"))).unwrap();
        let value = fs::read(root.join(format!("{name}.value"))).unwrap();
        Self { metadata, value }
    }

    /// Return the Variant for this case.
    fn variant(&self) -> Variant<'_, '_> {
        Variant::try_new(&self.metadata, &self.value).expect("Failed to parse variant")
    }
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
        ("primitive_decimal4", Variant::from(VariantDecimal4::try_new(1234i32, 2u8).unwrap())), 
        // ("primitive_decimal8", Variant::Decimal8{integer: 1234567890, scale: 2}),
        ("primitive_decimal8", Variant::Decimal8(VariantDecimal8::try_new(1234567890,2).unwrap())), 
        ("primitive_decimal16", Variant::Decimal16(VariantDecimal16::try_new(1234567891234567890, 2).unwrap())),
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
        ("short_string", Variant::ShortString(ShortString::try_new("Less than 64 bytes (❤\u{fe0f} with utf8)").unwrap())),
    ]
}
#[test]
fn variant_primitive() {
    let cases = get_primitive_cases();
    for (case, want) in cases {
        let case = Case::load(case);
        let got = case.variant();
        assert_eq!(got, want);
    }
}
#[test]
fn variant_object_empty() {
    let case = Case::load("object_empty");
    let Variant::Object(variant_object) = case.variant() else {
        panic!("expected an object");
    };
    assert_eq!(variant_object.len(), 0);
    assert!(variant_object.is_empty());
}
#[test]
fn variant_object_primitive() {
    // the data is defined in
    // https://github.com/apache/parquet-testing/blob/84d525a8731cec345852fb4ea2e7c581fbf2ef29/variant/data_dictionary.json#L46-L53
    //
    // ```json
    // " "object_primitive": {
    //         "boolean_false_field": false,
    //         "boolean_true_field": true,
    //         "double_field": 1.23456789,
    //         "int_field": 1,
    //         "null_field": null,
    //         "string_field": "Apache Parquet",
    //         "timestamp_field": "2025-04-16T12:34:56.78"
    //     },
    // ```
    let case = Case::load("object_primitive");
    let Variant::Object(variant_object) = case.variant() else {
        panic!("expected an object");
    };
    let expected_fields = vec![
        ("boolean_false_field", Variant::BooleanFalse),
        ("boolean_true_field", Variant::BooleanTrue),
        // spark wrote this as a decimal4 (not a double)
        (
            "double_field",
            Variant::Decimal4(VariantDecimal4::try_new(123456789, 8).unwrap()),
        ),
        ("int_field", Variant::Int8(1)),
        ("null_field", Variant::Null),
        (
            "string_field",
            Variant::ShortString(
                ShortString::try_new("Apache Parquet")
                    .expect("value should fit inside a short string"),
            ),
        ),
        (
            // apparently spark wrote this as a string (not a timestamp)
            "timestamp_field",
            Variant::ShortString(
                ShortString::try_new("2025-04-16T12:34:56.78")
                    .expect("value should fit inside a short string"),
            ),
        ),
    ];
    let actual_fields: Vec<_> = variant_object.iter().collect();
    assert_eq!(actual_fields, expected_fields);
}
#[test]
fn variant_array_primitive() {
    // The data is defined in
    // https://github.com/apache/parquet-testing/blob/84d525a8731cec345852fb4ea2e7c581fbf2ef29/variant/data_dictionary.json#L24-L29
    //
    // ```json
    // "array_primitive": [
    //    2,
    //    1,
    //    5,
    //    9
    // ],
    // ```
    let case = Case::load("array_primitive");
    let Variant::List(list) = case.variant() else {
        panic!("expected an array");
    };
    let expected = vec![
        Variant::Int8(2),
        Variant::Int8(1),
        Variant::Int8(5),
        Variant::Int8(9),
    ];
    let actual: Vec<_> = list.iter().collect();
    assert_eq!(actual, expected);

    // Call `get` for each individual element
    for (i, expected_value) in expected.iter().enumerate() {
        let got = list.get(i).unwrap();
        assert_eq!(&got, expected_value);
    }
}

#[test]
fn variant_array_builder() {
    let mut builder = VariantBuilder::new();

    let mut arr = builder.new_list();
    arr.append_value(2i8);
    arr.append_value(1i8);
    arr.append_value(5i8);
    arr.append_value(9i8);
    arr.finish();

    let (built_metadata, built_value) = builder.finish();
    let actual = Variant::try_new(&built_metadata, &built_value).unwrap();
    let case = Case::load("array_primitive");
    let expected = case.variant();

    assert_eq!(actual, expected);
}

#[test]
fn variant_object_builder() {
    let mut builder = VariantBuilder::new();

    let mut obj = builder.new_object();
    obj.append_value("int_field", 1i8);

    // The double field is actually encoded as decimal4 with scale 8
    // Value: 123456789, Scale: 8 -> 1.23456789
    obj.append_value(
        "double_field",
        VariantDecimal4::try_new(123456789i32, 8u8).unwrap(),
    );
    obj.append_value("boolean_true_field", true);
    obj.append_value("boolean_false_field", false);
    obj.append_value("string_field", "Apache Parquet");
    obj.append_value("null_field", ());
    obj.append_value("timestamp_field", "2025-04-16T12:34:56.78");

    obj.finish();

    let (built_metadata, built_value) = builder.finish();
    let actual = Variant::try_new(&built_metadata, &built_value).unwrap();
    let case = Case::load("object_primitive");
    let expected = case.variant();

    assert_eq!(actual, expected);
}

// TODO: Add tests for object_nested and array_nested
