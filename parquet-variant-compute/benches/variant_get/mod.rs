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

//! Deterministic fixtures shared by timing and allocation measurements.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field};
use parquet_variant::{Variant, VariantPath};
use parquet_variant_compute::{
    GetOptions, VariantArray, json_to_variant, shred_variant, variant_get,
};

pub const CASES: [&str; 7] = [
    "struct_control",
    "list_inbounds",
    "list_mixed",
    "list_oob",
    "nested_list",
    "object_list",
    "list_fallback",
];

#[derive(Clone, Copy, Debug, PartialEq)]
enum Value {
    Missing,
    Null,
    One,
    Two,
}

fn list(data_type: DataType, view: bool) -> DataType {
    let field = Arc::new(Field::new("item", data_type, true));
    if view {
        DataType::ListView(field)
    } else {
        DataType::List(field)
    }
}

pub struct Fixture {
    pub name: String,
    pub input: ArrayRef,
    pub options: GetOptions<'static>,
    /// Rows affected by the known missing-index-as-Variant-null bug (#11050).
    pub known_missing_as_null: usize,
}

impl Fixture {
    pub fn new(case: &str, rows: usize, view: bool, offset: usize, typed: bool) -> Self {
        use Value::{Missing, Null, One, Two};
        // (JSON input, expected Variant output, can expose #11050).
        let (pattern, schema, path): (Vec<_>, _, VariantPath<'static>) = match case {
            "struct_control" => (
                vec![(Some(r#"{"x":1}"#), One, false)],
                DataType::Struct(vec![Field::new("x", DataType::Int64, true)].into()),
                VariantPath::try_from("x").unwrap(),
            ),
            "list_inbounds" => (
                vec![(Some("[1,2]"), One, false)],
                list(DataType::Int64, view),
                VariantPath::from(0),
            ),
            "list_mixed" => (
                vec![
                    (None, Missing, false),
                    (Some("[]"), Missing, true),
                    (Some("[null]"), Null, false),
                    (Some("[1]"), One, false),
                ],
                list(DataType::Int64, view),
                VariantPath::from(0),
            ),
            "list_oob" => (
                vec![(Some("[1]"), Missing, true)],
                list(DataType::Int64, view),
                VariantPath::from(9),
            ),
            "nested_list" => (
                vec![
                    (None, Missing, false),
                    (Some("[]"), Missing, false),
                    (Some("[[]]"), Missing, true),
                    (Some("[[null]]"), Null, false),
                    (Some("[[1]]"), One, false),
                ],
                list(list(DataType::Int64, view), view),
                VariantPath::from(0).join(0),
            ),
            "object_list" => (
                vec![
                    (None, Missing, false),
                    (Some(r#"{"x":[]}"#), Missing, true),
                    (Some(r#"{"x":[null]}"#), Null, false),
                    (Some(r#"{"x":[1]}"#), One, false),
                ],
                DataType::Struct(vec![Field::new("x", list(DataType::Int64, view), true)].into()),
                VariantPath::try_from("x").unwrap().join(0),
            ),
            "list_fallback" => (
                vec![
                    (Some("[1]"), One, false),
                    (Some(r#"["two"]"#), Two, false),
                    (Some("[null]"), Null, false),
                ],
                list(DataType::Int64, view),
                VariantPath::from(0),
            ),
            _ => unreachable!("unknown fixture"),
        };
        let layout = if case == "struct_control" {
            "struct"
        } else if view {
            "list_view"
        } else {
            "list"
        };
        let output = if typed { "int64" } else { "variant" };
        let name = format!("{case}/{layout}/slice{offset}/{output}");
        let json: ArrayRef = Arc::new(StringArray::from(
            (0..rows + offset)
                .map(|i| pattern[i % pattern.len()].0)
                .collect::<Vec<_>>(),
        ));
        let original = json_to_variant(&json).unwrap();
        let shredded = shred_variant(&original, &schema).unwrap();
        // Assert the intended physical route exists, even for all-OOB inputs.
        let physical = shredded.typed_value_column().unwrap().data_type();
        assert!(matches!(
            (physical, &schema),
            (DataType::Struct(_), DataType::Struct(_))
                | (DataType::List(_), DataType::List(_))
                | (DataType::ListView(_), DataType::ListView(_))
        ));
        let input: ArrayRef = shredded.slice(offset, rows).into();
        let options = GetOptions::new_with_path(path)
            .with_as_type(typed.then(|| Arc::new(Field::new("value", DataType::Int64, true))));
        let original: ArrayRef = original.slice(offset, rows).into();
        let reference = variant_get(&original, options.clone()).unwrap();
        let result = variant_get(&input, options.clone()).unwrap();
        let mut known_missing_as_null = 0;
        for row in 0..rows {
            let (_, expected, known_bug) = pattern[(row + offset) % pattern.len()];
            let expected = if typed && matches!(expected, Null | Two) {
                Missing
            } else {
                expected
            };
            assert_eq!(
                value(&reference, row, typed),
                expected,
                "{name}: reference row {row}"
            );
            let actual = value(&result, row, typed);
            if !typed && known_bug && actual == Null {
                known_missing_as_null += 1;
            } else {
                assert_eq!(actual, expected, "{name}: shredded row {row}");
            }
        }
        let possible_mismatches = (0..rows)
            .filter(|row| !typed && pattern[(row + offset) % pattern.len()].2)
            .count();
        assert!(
            known_missing_as_null == 0 || known_missing_as_null == possible_mismatches,
            "{name}: partial validity correction"
        );
        if std::env::var_os("VARIANT_GET_REQUIRE_FIXED").is_some() {
            assert_eq!(known_missing_as_null, 0, "{name}: #11050 remains");
        }
        eprintln!("{name}/{rows}: known_missing_as_null={known_missing_as_null}");
        Self {
            name,
            input,
            options,
            known_missing_as_null,
        }
    }
}

fn value(array: &ArrayRef, row: usize, typed: bool) -> Value {
    if array.is_null(row) {
        return Value::Missing;
    }
    if typed {
        assert_eq!(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row),
            1
        );
        return Value::One;
    }
    match VariantArray::try_new(array).unwrap().value(row) {
        Variant::Null => Value::Null,
        v if v.as_int64() == Some(1) => Value::One,
        v if v.as_string() == Some("two") => Value::Two,
        v => panic!("unexpected value {v:?}"),
    }
}

pub fn for_each_fixture(mut f: impl FnMut(Fixture, usize)) {
    for rows in [64, 8192] {
        for view in [false, true] {
            for offset in [0, 3] {
                for case in CASES {
                    if case == "struct_control" && view {
                        continue;
                    }
                    for typed in [false, true] {
                        f(Fixture::new(case, rows, view, offset, typed), rows);
                    }
                }
            }
        }
    }
}
