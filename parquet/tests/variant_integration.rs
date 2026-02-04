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

//! Comprehensive integration tests for Parquet files with Variant columns
//!
//! This test harness reads test case definitions from cases.json, loads expected
//! Variant values from .variant.bin files, reads Parquet files, converts StructArray
//! to VariantArray, and verifies that extracted values match expected results.
//!
//! Inspired by the arrow-go implementation: <https://github.com/apache/arrow-go/pull/455/files>

use arrow::util::test_util::parquet_test_data;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet_variant::{Variant, VariantMetadata};
use parquet_variant_compute::{VariantArray, unshred_variant};
use serde::Deserialize;
use std::path::Path;
use std::sync::LazyLock;
use std::{fs, path::PathBuf};

type Result<T> = std::result::Result<T, String>;

/// Creates a test function for a given case number.
///
/// If an error message is provided, generate an error test case that expects it.
///
/// Note the index is zero-based, while the case number is one-based
macro_rules! variant_test_case {
    ($case_num:literal $(, $expected_error:literal )? ) => {
        paste::paste! {
            #[test]
            $( #[should_panic(expected = $expected_error)] )?
            fn [<test_variant_integration_case_ $case_num>]() {
                all_cases()[$case_num - 1].run()
            }
        }
    };
}

// Generate test functions for each case
// Notes
// - case 3 is empty in cases.json for some reason
// - cases 40, 42, 87, 127 and 128 are expected to fail always (they include invalid variants)
// - the remaining cases are expected to (eventually) pass

variant_test_case!(1);
variant_test_case!(2);
// case 3 is empty in cases.json 🤷
// ```json
// {
//   "case_number" : 3
// },
// ```
variant_test_case!(3, "parquet_file must be set");
variant_test_case!(4);
variant_test_case!(5);
variant_test_case!(6);
variant_test_case!(7);
variant_test_case!(8);
variant_test_case!(9);
variant_test_case!(10);
variant_test_case!(11);
variant_test_case!(12);
variant_test_case!(13);
variant_test_case!(14);
variant_test_case!(15);
variant_test_case!(16);
variant_test_case!(17);
variant_test_case!(18);
variant_test_case!(19);
variant_test_case!(20);
variant_test_case!(21);
variant_test_case!(22);
variant_test_case!(23);
variant_test_case!(24);
variant_test_case!(25);
variant_test_case!(26);
variant_test_case!(27);
variant_test_case!(28);
variant_test_case!(29);
variant_test_case!(30);
variant_test_case!(31);
variant_test_case!(32);
variant_test_case!(33);
variant_test_case!(34);
variant_test_case!(35);
variant_test_case!(36);
variant_test_case!(37);
variant_test_case!(38);
variant_test_case!(39);
// Is an error case (should be failing as the expected error message indicates)
variant_test_case!(40, "both value and typed_value are non-null");
variant_test_case!(41);
// Is an error case (should be failing as the expected error message indicates)
variant_test_case!(42, "both value and typed_value are non-null");
// Is an error case (should be failing as the expected error message indicates)
variant_test_case!(43, "Field 'b' appears in both typed_value and value");
variant_test_case!(44);
variant_test_case!(45);
variant_test_case!(46);
variant_test_case!(47);
variant_test_case!(48);
variant_test_case!(49);
variant_test_case!(50);
variant_test_case!(51);
variant_test_case!(52);
variant_test_case!(53);
variant_test_case!(54);
variant_test_case!(55);
variant_test_case!(56);
variant_test_case!(57);
variant_test_case!(58);
variant_test_case!(59);
variant_test_case!(60);
variant_test_case!(61);
variant_test_case!(62);
variant_test_case!(63);
variant_test_case!(64);
variant_test_case!(65);
variant_test_case!(66);
variant_test_case!(67);
variant_test_case!(68);
variant_test_case!(69);
variant_test_case!(70);
variant_test_case!(71);
variant_test_case!(72);
variant_test_case!(73);
variant_test_case!(74);
variant_test_case!(75);
variant_test_case!(76);
variant_test_case!(77);
variant_test_case!(78);
variant_test_case!(79);
variant_test_case!(80);
variant_test_case!(81);
variant_test_case!(82);
variant_test_case!(83);
// Invalid case, implementations can choose to read the shredded value or error out
variant_test_case!(84);
variant_test_case!(85);
variant_test_case!(86);
// Is an error case (should be failing as the expected error message indicates)
variant_test_case!(87, "Expected object in value field");
variant_test_case!(88);
variant_test_case!(89);
variant_test_case!(90);
variant_test_case!(91);
variant_test_case!(92);
variant_test_case!(93);
variant_test_case!(94);
variant_test_case!(95);
variant_test_case!(96);
variant_test_case!(97);
variant_test_case!(98);
variant_test_case!(99);
variant_test_case!(100);
variant_test_case!(101);
variant_test_case!(102);
variant_test_case!(103);
variant_test_case!(104);
variant_test_case!(105);
variant_test_case!(106);
variant_test_case!(107);
variant_test_case!(108);
variant_test_case!(109);
variant_test_case!(110);
variant_test_case!(111);
variant_test_case!(112);
variant_test_case!(113);
variant_test_case!(114);
variant_test_case!(115);
variant_test_case!(116);
variant_test_case!(117);
variant_test_case!(118);
variant_test_case!(119);
variant_test_case!(120);
variant_test_case!(121);
variant_test_case!(122);
variant_test_case!(123);
variant_test_case!(124);
// Is an error case (should be failing as the expected error message indicates)
variant_test_case!(125, "Field 'b' appears in both typed_value and value");
variant_test_case!(126);
// Is an error case (should be failing as the expected error message indicates)
variant_test_case!(127, "Illegal shredded value type: UInt32");
// Is an error case (should be failing as the expected error message indicates)
variant_test_case!(128, "Expected object in value field");
variant_test_case!(129);
variant_test_case!(130);
variant_test_case!(131);
variant_test_case!(132);
variant_test_case!(133);
variant_test_case!(134);
variant_test_case!(135);
variant_test_case!(136);
// Is an error case (should be failing as the expected error message indicates)
variant_test_case!(137, "Illegal shredded value type: FixedSizeBinary(4)");
variant_test_case!(138);

/// Test case definition structure matching the format from
/// `parquet-testing/parquet_shredded/cases.json`
///
/// See [README] for details.
///
/// [README]: https://github.com/apache/parquet-testing/blob/master/shredded_variant/README.md
///
/// Example JSON
/// ```json
/// {
///   "case_number" : 5,
///   "test" : "testShreddedVariantPrimitives",
///   "parquet_file" : "case-005.parquet",
///   "variant_file" : "case-005_row-0.variant.bin",
///   "variant" : "Variant(metadata=VariantMetadata(dict={}), value=Variant(type=BOOLEAN_FALSE, value=false))"
/// },
/// ```
#[allow(dead_code)] // some fields are not used except when printing the struct
#[derive(Debug, Clone, Deserialize)]
struct VariantTestCase {
    /// Case number (e.g., 1, 2, 4, etc. - note: case 3 is missing any data)
    pub case_number: u32,
    /// Test method name (e.g., "testSimpleArray")
    pub test: Option<String>,
    /// Name of the parquet file (e.g., "case-001.parquet")
    pub parquet_file: Option<String>,

    /// Expected variant binary file (e.g., "case-001_row-0.variant.bin") - None for error cases
    pub variant_file: Option<String>,
    /// Multiple expected variant binary files, for multi row inputs. If there
    /// is no variant, there is no file
    pub variant_files: Option<Vec<Option<String>>>,
    /// Expected error message for negative test cases
    ///
    /// (this is the message from the cases.json file, which is from the Iceberg
    /// implementation, so it is not guaranteed to match the actual Rust error message)
    pub error_message: Option<String>,
    /// Description of the variant value (for debugging)
    pub variant_description: Option<String>,
}

/// Run a single test case
impl VariantTestCase {
    /// Run a test case. Panics on unexpected error
    fn run(&self) {
        println!("{self:#?}");

        let variant_data = self.load_variants();
        let variant_array = self.load_parquet();

        // `load_parquet` returns shredded variant values, but the test expectations are provided as
        // unshredded variant values. Unshred (failing for invalid input) so we can compare them.
        let variant_array = unshred_variant(&variant_array).unwrap();

        // if this is an error case, the expected error message should be set
        if let Some(expected_error) = &self.error_message {
            // Unshredding variant array should have already triggered the error
            panic!("Expected an error '{expected_error}`, but got no error");
        }

        assert_eq!(
            variant_array.len(),
            variant_data.len(),
            "Number of variants in parquet file does not match expected number"
        );
        for (i, expected) in variant_data.iter().enumerate() {
            if variant_array.is_null(i) {
                assert!(
                    expected.is_none(),
                    "Expected null variant at index {i}, but got {:?}",
                    variant_array.value(i)
                );
                continue;
            }
            let actual = variant_array.value(i);
            let expected = variant_data[i]
                .as_ref()
                .expect("Expected non-null variant data");

            let expected = expected.as_variant();

            // compare the variants (is this the right way to compare?)
            assert_eq!(
                actual, expected,
                "Variant data mismatch at index {}\n\nactual\n{actual:#?}\n\nexpected\n{expected:#?}",
                i
            );
        }
    }

    /// Parses the expected variant files, returning a vector of `ExpectedVariant` or None
    /// if the corresponding entry in `variant_files` is null
    fn load_variants(&self) -> Vec<Option<ExpectedVariant>> {
        let variant_files: Box<dyn Iterator<Item = Option<&String>>> =
            match (&self.variant_files, &self.variant_file) {
                (Some(files), None) => Box::new(files.iter().map(|f| f.as_ref())),
                (None, Some(file)) => Box::new(std::iter::once(Some(file))),
                // error cases may not have any variant files
                _ => Box::new(std::iter::empty()),
            };

        // load each file
        variant_files
            .map(|f| {
                let v = ExpectedVariant::try_load(&TEST_CASE_DIR.join(f?))
                    .expect("Failed to load expected variant");
                Some(v)
            })
            .collect()
    }

    /// Load the parquet file, extract the Variant column, and return as a VariantArray
    fn load_parquet(&self) -> VariantArray {
        let parquet_file = self
            .parquet_file
            .as_ref()
            .expect("parquet_file must be set");
        let path = TEST_CASE_DIR.join(parquet_file);
        let file = fs::File::open(&path)
            .unwrap_or_else(|e| panic!("cannot open parquet file {path:?}: {e}"));

        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .and_then(|b| b.build())
            .unwrap_or_else(|e| panic!("Error reading parquet reader for {path:?}: {e}"));

        let mut batches: Vec<_> = reader
            .collect::<std::result::Result<_, _>>()
            .unwrap_or_else(|e| panic!("Error reading parquet batches for {path:?}: {e}"));

        if batches.is_empty() {
            panic!("No parquet batches were found in file {path:?}");
        }
        if batches.len() > 1 {
            panic!(
                "Multiple parquet batches were found in file {path:?}, only single batch supported"
            );
        }
        let batch = batches.swap_remove(0);

        // The schema is "id", "var" for the id and variant columns
        // TODO: support the actual parquet logical type annotation somehow
        let var = batch
            .column_by_name("var")
            .unwrap_or_else(|| panic!("No 'var' column found in parquet file {path:?}"));

        VariantArray::try_new(var).unwrap_or_else(|e| {
            panic!("Error converting StructArray to VariantArray for {path:?}: {e}")
        })
    }
}

/// Variant value loaded from .variant.bin file
#[derive(Debug, Clone)]
struct ExpectedVariant {
    data: Vec<u8>,
    data_offset: usize,
}

impl ExpectedVariant {
    fn try_load(path: &Path) -> Result<Self> {
        // "Each `*.variant.bin` file contains a single variant serialized
        // by concatenating the serialized bytes of the variant metadata
        // followed by the serialized bytes of the variant value."
        let data = fs::read(path).map_err(|e| format!("cannot read variant file {path:?}: {e}"))?;
        let metadata = VariantMetadata::try_new(&data)
            .map_err(|e| format!("cannot parse variant metadata from {path:?}: {e}"))?;

        let data_offset = metadata.size();
        Ok(Self { data, data_offset })
    }

    fn as_variant(&self) -> Variant<'_, '_> {
        let metadata = &self.data[0..self.data_offset];
        let value = &self.data[self.data_offset..];
        Variant::try_new(metadata, value).expect("Invalid variant data")
    }
}

static TEST_CASE_DIR: LazyLock<PathBuf> = LazyLock::new(|| {
    PathBuf::from(parquet_test_data())
        .join("..")
        .join("shredded_variant")
});

/// All tests
static ALL_CASES: LazyLock<Result<Vec<VariantTestCase>>> = LazyLock::new(|| {
    let cases_file = TEST_CASE_DIR.join("cases.json");

    if !cases_file.exists() {
        return Err(format!("cases.json not found at {}", cases_file.display()));
    }

    let content = fs::read_to_string(&cases_file)
        .map_err(|e| format!("cannot read cases file {cases_file:?}: {e}"))?;

    serde_json::from_str::<Vec<VariantTestCase>>(content.as_str())
        .map_err(|e| format!("cannot parse json from {cases_file:?}: {e}"))
});

// return a reference to the static ALL_CASES, or panic if loading failed
fn all_cases() -> &'static [VariantTestCase] {
    ALL_CASES.as_ref().unwrap()
}
