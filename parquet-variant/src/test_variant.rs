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

use crate::variant::{Variant, VariantMetadata};
use arrow_schema::ArrowError;

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

fn get_primitive_cases() -> Vec<(&'static str, Variant<'static, 'static>)> {
    vec![
    ("primitive_boolean_false", Variant::BooleanFalse),
    ("primitive_boolean_true", Variant::BooleanTrue),
    ("primitive_int8", Variant::Int8(42)),
    // Using the From<String> trait
    ("primitive_string", Variant::from("This string is longer than 64 bytes and therefore does not fit in a short_string and it also includes several non ascii characters such as 🐢, 💖, ♥\u{fe0f}, 🎣 and 🤦!!")),
    // Using the From<String> trait
    ("short_string", Variant::from("Less than 64 bytes (❤\u{fe0f} with utf8)")), 
    // TODO Reenable when https://github.com/apache/parquet-testing/issues/81 is fixed
    // ("primitive_null", Variant::Null),
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
