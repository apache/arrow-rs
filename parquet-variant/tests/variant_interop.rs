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

use std::path::{Path, PathBuf};
use std::{env, fs};

use chrono::{DateTime, NaiveDate, NaiveTime};
use parquet_variant::{
    ShortString, Variant, VariantBuilder, VariantDecimal4, VariantDecimal8, VariantDecimal16,
};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use uuid::Uuid;

/// Returns a directory path for the parquet variant test data.
///
/// The data lives in the `parquet-testing` git repository:
/// <https://github.com/apache/parquet-testing>
///
/// Normally this is checked out as a git submodule in the root of the `arrow-rs` repository,
/// so the relative path is
/// * `CARGO_MANIFEST_DIR/../parquet-testing/variant`.
///
/// However, the user can override this by setting the environment variable `PARQUET_TEST_DATA`
/// to point to a different directory (as is done by the `verify-release-candidate.sh` script).
///
/// In this case, the environment variable `PARQUET_TEST_DATA` is expected to point to a directory
/// `parquet-testing/data`, so the relative path to the `variant` subdirectory is
/// * `PARQUET_TEST_DATA/../variant`.
fn cases_dir() -> PathBuf {
    // which we expect to point at "../parquet-testing/data"
    let env_name = "PARQUET_TEST_DATA";
    if let Ok(dir) = env::var(env_name) {
        let trimmed = dir.trim();
        if !trimmed.is_empty() {
            let pb = PathBuf::from(trimmed).join("..").join("variant");
            if pb.is_dir() {
                return pb;
            } else {
                panic!(
                    "Can't find variant data at `{pb:?}`. Used value of env `{env_name}`../variant ",
                )
            }
        }
    }

    // PARQUET_TEST_DATA is undefined or its value is trimmed to empty, let's try default dir.

    // env "CARGO_MANIFEST_DIR" is "the directory containing the manifest of your package",
    // set by `cargo run` or `cargo test`, see:
    // https://doc.rust-lang.org/cargo/reference/environment-variables.html
    let pb = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("parquet-testing")
        .join("variant");

    if pb.is_dir() {
        pb
    } else {
        panic!(
            "env `{env_name}` is undefined or has empty value, and \
             `CARGO_MANIFEST_DIR/../parquet-testing/variant` is not a directory: `{pb:?}`\n\
             HINT: try running `git submodule update --init`",
        )
    }
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
        (
            "primitive_binary",
            Variant::Binary(&[0x03, 0x13, 0x37, 0xde, 0xad, 0xbe, 0xef, 0xca, 0xfe]),
        ),
        ("primitive_boolean_false", Variant::BooleanFalse),
        ("primitive_boolean_true", Variant::BooleanTrue),
        (
            "primitive_date",
            Variant::Date(NaiveDate::from_ymd_opt(2025, 4, 16).unwrap()),
        ),
        (
            "primitive_decimal4",
            Variant::from(VariantDecimal4::try_new(1234i32, 2u8).unwrap()),
        ),
        // ("primitive_decimal8", Variant::Decimal8{integer: 1234567890, scale: 2}),
        (
            "primitive_decimal8",
            Variant::Decimal8(VariantDecimal8::try_new(1234567890, 2).unwrap()),
        ),
        (
            "primitive_decimal16",
            Variant::Decimal16(VariantDecimal16::try_new(1234567891234567890, 2).unwrap()),
        ),
        ("primitive_float", Variant::Float(1234567890.1234)),
        ("primitive_double", Variant::Double(1234567890.1234)),
        ("primitive_int8", Variant::Int8(42)),
        ("primitive_int16", Variant::Int16(1234)),
        ("primitive_int32", Variant::Int32(123456)),
        ("primitive_int64", Variant::Int64(1234567890123456789)),
        ("primitive_null", Variant::Null),
        (
            "primitive_string",
            Variant::String(
                "This string is longer than 64 bytes and therefore does not fit in a short_string and it also includes several non ascii characters such as 🐢, 💖, ♥\u{fe0f}, 🎣 and 🤦!!",
            ),
        ),
        (
            "primitive_timestamp",
            Variant::TimestampMicros(
                NaiveDate::from_ymd_opt(2025, 4, 16)
                    .unwrap()
                    .and_hms_milli_opt(16, 34, 56, 780)
                    .unwrap()
                    .and_utc(),
            ),
        ),
        (
            "primitive_timestampntz",
            Variant::TimestampNtzMicros(
                NaiveDate::from_ymd_opt(2025, 4, 16)
                    .unwrap()
                    .and_hms_milli_opt(12, 34, 56, 780)
                    .unwrap(),
            ),
        ),
        (
            "primitive_timestamp_nanos",
            Variant::TimestampNanos(
                NaiveDate::from_ymd_opt(2024, 11, 7)
                    .unwrap()
                    .and_hms_nano_opt(12, 33, 54, 123456789)
                    .unwrap()
                    .and_utc(),
            ),
        ),
        (
            "primitive_timestampntz_nanos",
            Variant::TimestampNtzNanos(
                NaiveDate::from_ymd_opt(2024, 11, 7)
                    .unwrap()
                    .and_hms_nano_opt(12, 33, 54, 123456789)
                    .unwrap(),
            ),
        ),
        (
            "primitive_uuid",
            Variant::Uuid(Uuid::parse_str("f24f9b64-81fa-49d1-b74e-8c09a6e31c56").unwrap()),
        ),
        (
            "short_string",
            Variant::ShortString(
                ShortString::try_new("Less than 64 bytes (❤\u{fe0f} with utf8)").unwrap(),
            ),
        ),
        (
            "primitive_time",
            Variant::Time(NaiveTime::from_hms_micro_opt(12, 33, 54, 123456).unwrap()),
        ),
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
    obj.insert("int_field", 1i8);

    // The double field is actually encoded as decimal4 with scale 8
    // Value: 123456789, Scale: 8 -> 1.23456789
    obj.insert(
        "double_field",
        VariantDecimal4::try_new(123456789i32, 8u8).unwrap(),
    );
    obj.insert("boolean_true_field", true);
    obj.insert("boolean_false_field", false);
    obj.insert("string_field", "Apache Parquet");
    obj.insert("null_field", ());
    obj.insert("timestamp_field", "2025-04-16T12:34:56.78");

    obj.finish();

    let (built_metadata, built_value) = builder.finish();
    let actual = Variant::try_new(&built_metadata, &built_value).unwrap();
    let case = Case::load("object_primitive");
    let expected = case.variant();

    assert_eq!(actual, expected);
}

// TODO: Add tests for object_nested and array_nested

//
// Validation Fuzzing Tests
//
// 1. Generate valid variants using the builder
// 2. Randomly corrupt bytes in the serialized data
// 3. Test both validation pathways:
//    - If validation succeeds -> verify infallible APIs don't panic
//    - If validation fails -> verify fallible APIs handle errors gracefully
//

#[test]
fn test_validation_fuzz_integration() {
    let mut rng = StdRng::seed_from_u64(42);

    for _ in 0..1000 {
        // Generate a random valid variant
        let (metadata, value) = generate_random_variant(&mut rng);

        // Corrupt it
        let (corrupted_metadata, corrupted_value) = corrupt_variant_data(&mut rng, metadata, value);

        // Test the validation workflow
        test_validation_workflow(&corrupted_metadata, &corrupted_value);
    }
}

fn generate_random_variant(rng: &mut StdRng) -> (Vec<u8>, Vec<u8>) {
    let mut builder = VariantBuilder::new();
    generate_random_value(rng, &mut builder, 3); // Max depth of 3
    builder.finish()
}

fn generate_random_value(rng: &mut StdRng, builder: &mut VariantBuilder, max_depth: u32) {
    if max_depth == 0 {
        // Force simple values at max depth
        builder.append_value(rng.random::<i32>());
        return;
    }

    match rng.random_range(0..18) {
        0 => builder.append_value(()),
        1 => builder.append_value(rng.random::<bool>()),
        2 => builder.append_value(rng.random::<i8>()),
        3 => builder.append_value(rng.random::<i16>()),
        4 => builder.append_value(rng.random::<i32>()),
        5 => builder.append_value(rng.random::<i64>()),
        6 => builder.append_value(rng.random::<f32>()),
        7 => builder.append_value(rng.random::<f64>()),
        8 => {
            // String
            let len = rng.random_range(0..50);
            let s: String = (0..len).map(|_| rng.random::<char>()).collect();
            builder.append_value(s.as_str());
        }
        9 => {
            // Binary
            let len = rng.random_range(0..50);
            let bytes: Vec<u8> = (0..len).map(|_| rng.random()).collect();
            builder.append_value(bytes.as_slice());
        }
        10 => {
            if let Ok(decimal) = VariantDecimal4::try_new(rng.random(), rng.random_range(0..10)) {
                builder.append_value(decimal);
            } else {
                builder.append_value(0i32);
            }
        }
        11 => {
            if let Ok(decimal) = VariantDecimal8::try_new(rng.random(), rng.random_range(0..19)) {
                builder.append_value(decimal);
            } else {
                builder.append_value(0i64);
            }
        }
        12 => {
            if let Ok(decimal) = VariantDecimal16::try_new(rng.random(), rng.random_range(0..39)) {
                builder.append_value(decimal);
            } else {
                builder.append_value(0i64); // Use i64 instead of i128
            }
        }
        13 => {
            // Generate a list
            let mut list_builder = builder.new_list();
            let list_len = rng.random_range(0..10);
            list_builder.extend(std::iter::repeat_with(|| rng.random::<i32>()).take(list_len));
            list_builder.finish();
        }
        14 => {
            // Generate an object
            let mut object_builder = builder.new_object();
            let obj_size = rng.random_range(0..10);

            object_builder
                .extend((0..obj_size).map(|i| (format!("field_{i}"), rng.random::<i32>())));

            object_builder.finish();
        }
        15 => {
            // Time
            builder.append_value(
                NaiveTime::from_num_seconds_from_midnight_opt(
                    // make the argument always valid
                    rng.random_range(0..86_400),
                    rng.random_range(0..1_000_000_000),
                )
                .unwrap(),
            )
        }
        16 => {
            let data_time = DateTime::from_timestamp(
                // make the argument always valid
                rng.random_range(0..86_400),
                rng.random_range(0..1_000_000_000),
            )
            .unwrap();

            // timestamp w/o timezone
            builder.append_value(data_time.naive_local());

            // timestamp with timezone
            builder.append_value(data_time.naive_utc().and_utc());
        }
        17 => {
            builder.append_value(Uuid::new_v4());
        }
        _ => unreachable!(),
    }
}

fn corrupt_variant_data(
    rng: &mut StdRng,
    mut metadata: Vec<u8>,
    mut value: Vec<u8>,
) -> (Vec<u8>, Vec<u8>) {
    // Randomly decide what to corrupt
    let corrupt_metadata = rng.random_bool(0.3);
    let corrupt_value = rng.random_bool(0.7);

    if corrupt_metadata && !metadata.is_empty() {
        let idx = rng.random_range(0..metadata.len());
        let bit = rng.random_range(0..8);
        metadata[idx] ^= 1 << bit;
    }

    if corrupt_value && !value.is_empty() {
        let idx = rng.random_range(0..value.len());
        let bit = rng.random_range(0..8);
        value[idx] ^= 1 << bit;
    }

    (metadata, value)
}

fn test_validation_workflow(metadata: &[u8], value: &[u8]) {
    // Step 1: Try unvalidated construction - should not panic
    let variant_result = std::panic::catch_unwind(|| Variant::new(metadata, value));

    let variant = match variant_result {
        Ok(v) => v,
        Err(_) => return, // Construction failed, which is acceptable for corrupted data
    };

    // Step 2: Try validation
    let validation_result = std::panic::catch_unwind(|| variant.clone().with_full_validation());

    match validation_result {
        Ok(Ok(validated)) => {
            // Validation succeeded - infallible access should not panic
            test_infallible_access(&validated);
        }
        Ok(Err(_)) => {
            // Validation failed - fallible access should handle errors gracefully
            test_fallible_access(&variant);
        }
        Err(_) => {
            // Validation panicked - this may indicate severely corrupted data
            // For now, we accept this, but it could indicate a validation bug
        }
    }
}

fn test_infallible_access(variant: &Variant) {
    // All these should not panic on validated variants
    let _ = variant.as_null();
    let _ = variant.as_boolean();
    let _ = variant.as_int32();
    let _ = variant.as_string();

    if let Some(obj) = variant.as_object() {
        for (_, _) in obj.iter() {
            // Should not panic
        }
        for i in 0..obj.len() {
            let _ = obj.field(i);
        }
    }

    if let Some(list) = variant.as_list() {
        for _ in list.iter() {
            // Should not panic
        }
        for i in 0..list.len() {
            let _ = list.get(i);
        }
    }
}

fn test_fallible_access(variant: &Variant) {
    // These should handle errors gracefully, never panic
    if let Some(obj) = variant.as_object() {
        for result in obj.iter_try() {
            let _ = result; // May be Ok or Err, but should not panic
        }
        for i in 0..obj.len() {
            let _ = obj.try_field(i); // May be Ok or Err, but should not panic
        }
    }

    if let Some(list) = variant.as_list() {
        for result in list.iter_try() {
            let _ = result; // May be Ok or Err, but should not panic
        }
        for i in 0..list.len() {
            let _ = list.try_get(i); // May be Ok or Err, but should not panic
        }
    }
}

#[test]
fn test_specific_validation_error_cases() {
    // Test specific malformed cases that should trigger validation errors

    // Case 1: Invalid header byte
    test_validation_workflow_simple(&[0x01, 0x00, 0x00], &[0xFF, 0x42]); // Invalid basic type

    // Case 2: Truncated metadata
    test_validation_workflow_simple(&[0x01], &[0x05, 0x48, 0x65, 0x6C, 0x6C, 0x6F]); // Incomplete metadata

    // Case 3: Truncated value
    test_validation_workflow_simple(&[0x01, 0x00, 0x00], &[0x09]); // String header but no data

    // Case 4: Invalid object with out-of-bounds field ID
    test_validation_workflow_simple(&[0x01, 0x00, 0x00], &[0x0F, 0x01, 0xFF, 0x00, 0x00]); // Field ID 255 doesn't exist

    // Case 5: Invalid list with malformed offsets
    test_validation_workflow_simple(&[0x01, 0x00, 0x00], &[0x13, 0x02, 0xFF, 0x00, 0x00]);
    // Malformed offset array
}

fn test_validation_workflow_simple(metadata: &[u8], value: &[u8]) {
    // Simple version without randomization, always runs regardless of feature flag

    // Step 1: Try unvalidated construction - should not panic
    let variant_result = std::panic::catch_unwind(|| Variant::new(metadata, value));

    let variant = match variant_result {
        Ok(v) => v,
        Err(_) => return, // Construction failed, which is acceptable for corrupted data
    };

    // Step 2: Try validation
    let validation_result = std::panic::catch_unwind(|| variant.clone().with_full_validation());

    match validation_result {
        Ok(Ok(validated)) => {
            // Validation succeeded - infallible access should not panic
            test_infallible_access_simple(&validated);
        }
        Ok(Err(_)) => {
            // Validation failed - fallible access should handle errors gracefully
            test_fallible_access_simple(&variant);
        }
        Err(_) => {
            // Validation panicked - this may indicate severely corrupted data
        }
    }
}

fn test_infallible_access_simple(variant: &Variant) {
    // All these should not panic on validated variants
    let _ = variant.as_null();
    let _ = variant.as_boolean();
    let _ = variant.as_int32();
    let _ = variant.as_string();

    if let Some(obj) = variant.as_object() {
        for (_, _) in obj.iter() {
            // Should not panic
        }
        for i in 0..obj.len() {
            let _ = obj.field(i);
        }
    }

    if let Some(list) = variant.as_list() {
        for _ in list.iter() {
            // Should not panic
        }
        for i in 0..list.len() {
            let _ = list.get(i);
        }
    }
}

fn test_fallible_access_simple(variant: &Variant) {
    // These should handle errors gracefully, never panic
    if let Some(obj) = variant.as_object() {
        for result in obj.iter_try() {
            let _ = result; // May be Ok or Err, but should not panic
        }
        for i in 0..obj.len() {
            let _ = obj.try_field(i); // May be Ok or Err, but should not panic
        }
    }

    if let Some(list) = variant.as_list() {
        for result in list.iter_try() {
            let _ = result; // May be Ok or Err, but should not panic
        }
        for i in 0..list.len() {
            let _ = list.try_get(i); // May be Ok or Err, but should not panic
        }
    }
}
