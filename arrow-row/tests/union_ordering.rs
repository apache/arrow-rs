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

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{
    Array, ArrayRef, Int32Array, ListArray, StringArray, StructArray, UnionArray, new_empty_array,
    new_null_array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_ord::sort::{LexicographicalComparator, SortColumn};
use arrow_row::{RowConverter, Rows, SortField};
use arrow_schema::{DataType, Field, SortOptions, UnionFields, UnionMode};

// Keep the test data in terms of values: Int(None) is a null from the integer
// field, while Str(None) is a null from the string field.
#[derive(Clone, Copy)]
enum Value {
    Int(Option<i32>),
    Str(Option<&'static str>),
}
use Value::{Int, Str};

#[rustfmt::skip]
const INPUT: [(Value, Value, Option<&str>); 7] = [
    // Union input      Expected after decoding    Following sort column
    (Int(Some(2)),      Int(Some(2)),              None),       // row 0
    (Str(Some("z")),    Str(Some("z")),            Some("z")),  // row 1
    (Int(None),         Int(None),                 Some("a")),  // row 2
    (Str(None),         Int(None),                 Some("a")),  // row 3
    (Int(Some(-1)),     Int(Some(-1)),             None),       // row 4
    (Str(Some("a")),    Str(Some("a")),            Some("z")),  // row 5
    (Str(Some("z")),    Str(Some("z")),            Some("a")),  // row 6
];

// The integers below are row numbers in INPUT. The following column always sorts
// descending, with its nulls last; these options apply to the Union column only.
#[rustfmt::skip]
const SORT_CASES: [(&str, SortOptions, [usize; 7]); 4] = [
    ("ascending, nulls first",  SortOptions { descending: false, nulls_first: true  }, [2, 3, 5, 1, 6, 4, 0]),
    ("ascending, nulls last",   SortOptions { descending: false, nulls_first: false }, [5, 1, 6, 4, 0, 2, 3]),
    ("descending, nulls first", SortOptions { descending: true,  nulls_first: true  }, [2, 3, 0, 4, 1, 6, 5]),
    ("descending, nulls last",  SortOptions { descending: true,  nulls_first: false }, [0, 4, 1, 6, 5, 2, 3]),
];

fn int_string_fields() -> UnionFields {
    // Put integer first in the schema, but give string the smaller type ID.
    // Ordering uses type IDs; choosing a field for decoded nulls uses schema order.
    UnionFields::try_new(
        [127, 0],
        [
            Field::new("int", DataType::Int32, true),
            Field::new("str", DataType::Utf8, true),
        ],
    )
    .unwrap()
}

fn int_string_union(mode: UnionMode, fields: &UnionFields, values: &[Value]) -> UnionArray {
    let mut fields_iter = fields.iter();
    let int_id = fields_iter.next().unwrap().0;
    let str_id = fields_iter.next().unwrap().0;
    let mut type_ids = Vec::new();
    let mut offsets = Vec::new();
    let mut ints = Vec::new();
    let mut strings = Vec::new();
    for &value in values {
        // Dense children keep only their selected values. Sparse children have a
        // slot for every row, so fill the unselected child's slot with a null.
        match value {
            Int(value) => {
                type_ids.push(int_id);
                offsets.push(ints.len() as i32);
                ints.push(value);
                if mode == UnionMode::Sparse {
                    strings.push(None);
                }
            }
            Str(value) => {
                type_ids.push(str_id);
                offsets.push(strings.len() as i32);
                strings.push(value);
                if mode == UnionMode::Sparse {
                    ints.push(None);
                }
            }
        }
    }
    UnionArray::try_new(
        fields.clone(),
        type_ids.into(),
        (mode == UnionMode::Dense).then(|| offsets.into()),
        vec![
            Arc::new(Int32Array::from(ints)),
            Arc::new(StringArray::from(strings)),
        ],
    )
    .unwrap()
}

fn check_roundtrip(columns: &[ArrayRef], options: &[SortOptions]) -> Vec<ArrayRef> {
    let fields = |columns: &[ArrayRef]| {
        columns
            .iter()
            .zip(options)
            .map(|(column, &options)| {
                SortField::new_with_options(column.data_type().clone(), options)
            })
            .collect()
    };
    let converter = RowConverter::new(fields(columns)).unwrap();
    let rows = converter.convert_columns(columns).unwrap();
    let decoded = converter.convert_rows(&rows).unwrap();
    for column in &decoded {
        column.to_data().validate_full().unwrap();
    }

    // Nulls may change Union fields, and dictionaries decode to their value types.
    // Even then, decoding and encoding again must preserve the comparison bytes.
    let converter = RowConverter::new(fields(&decoded)).unwrap();
    let reencoded = converter.convert_columns(&decoded).unwrap();
    assert_eq!(
        rows.try_into_binary().unwrap(),
        reencoded.try_into_binary().unwrap()
    );
    decoded
}

fn check_order_matches_native(columns: &[ArrayRef], options: &[SortOptions]) -> Rows {
    let fields = columns
        .iter()
        .zip(options)
        .map(|(column, &options)| SortField::new_with_options(column.data_type().clone(), options))
        .collect();
    let converter = RowConverter::new(fields).unwrap();
    let rows = converter.convert_columns(columns).unwrap();
    let sort_columns: Vec<_> = columns
        .iter()
        .zip(options)
        .map(|(values, &options)| SortColumn {
            values: values.clone(),
            options: Some(options),
        })
        .collect();
    let comparator = LexicographicalComparator::try_new(&sort_columns).unwrap();
    for i in 0..rows.num_rows() {
        for j in 0..rows.num_rows() {
            // Check equality too: differently tagged nulls must compare equal,
            // rather than merely happening to land next to each other in a sort.
            assert_eq!(
                rows.row(i).cmp(&rows.row(j)),
                comparator.compare(i, j),
                "rows {i}, {j}, {options:?}, {:?}",
                columns[0].data_type()
            );
        }
    }

    // Starting at row 1 catches code that incorrectly assumes an unsliced array.
    let sliced: Vec<_> = columns.iter().map(|c| c.slice(1, c.len() - 1)).collect();
    let sliced_rows = converter.convert_columns(&sliced).unwrap();
    for i in 0..sliced_rows.num_rows() {
        assert_eq!(sliced_rows.row(i), rows.row(i + 1));
    }
    rows
}

#[test]
fn union_rows_follow_requested_sort_order() {
    let fields = int_string_fields();
    let input: Vec<_> = INPUT.iter().map(|row| row.0).collect();
    let expected: Vec<_> = INPUT.iter().map(|row| row.1).collect();
    let tail: ArrayRef = Arc::new(StringArray::from_iter(INPUT.iter().map(|row| row.2)));
    for mode in [UnionMode::Dense, UnionMode::Sparse] {
        let union = int_string_union(mode, &fields, &input);
        let expected = int_string_union(mode, &fields, &expected);
        let columns = [Arc::new(union) as ArrayRef, tail.clone()];
        for (name, options, expected_order) in SORT_CASES {
            let options = [
                options,
                SortOptions {
                    descending: true,
                    nulls_first: false,
                },
            ];
            let rows = check_order_matches_native(&columns, &options);
            let mut order: Vec<_> = (0..INPUT.len()).collect();
            // A stable sort keeps the equal null rows 2 and 3 in their input order.
            // Rows 1 and 6 have the same Union value; the following column orders them.
            order.sort_by(|&a, &b| rows.row(a).cmp(&rows.row(b)));
            assert_eq!(order, expected_order, "{mode:?}, {name}");
            let decoded = check_roundtrip(&columns, &options);
            assert_eq!(decoded[0].to_data(), expected.to_data(), "{mode:?}, {name}");
            assert_eq!(decoded[1].to_data(), tail.to_data());
        }
    }
}

#[test]
fn union_nulls_decode_into_first_nullable_field() {
    #[rustfmt::skip]
    let cases = [
        // Description                      Integer nullable  String nullable  Decoded null
        ("both nullable: use first field",   true,             true,            Int(None)),
        ("only string is nullable",          false,            true,            Str(None)),
        ("neither nullable: use first",      false,            false,           Int(None)),
    ];
    for (name, int_nullable, str_nullable, decoded_null) in cases {
        let fields = UnionFields::try_new(
            [127, 3],
            [
                Field::new("int", DataType::Int32, int_nullable),
                Field::new("str", DataType::Utf8, str_nullable),
            ],
        )
        .unwrap();
        let input = [Int(Some(5)), Str(None), Str(Some("a"))];
        let expected = [Int(Some(5)), decoded_null, Str(Some("a"))];
        for mode in [UnionMode::Dense, UnionMode::Sparse] {
            let union = int_string_union(mode, &fields, &input);
            let expected = int_string_union(mode, &fields, &expected);
            for (sort_name, options, _) in SORT_CASES {
                let decoded = check_roundtrip(&[Arc::new(union.clone())], &[options]);
                assert_eq!(
                    decoded[0].to_data(),
                    expected.to_data(),
                    "{mode:?}, {name}, {sort_name}"
                );
            }
        }
    }
}

#[test]
fn union_dictionary_children_decode_to_value_types() {
    let dictionary_type = DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8));
    let dictionary_field = Field::new("value", dictionary_type.clone(), true)
        .with_metadata([("source", "dictionary")]);
    let cases = [
        ("dictionary", dictionary_type, DataType::Utf8),
        (
            "dictionary inside a Struct",
            DataType::Struct(vec![dictionary_field.clone()].into()),
            DataType::Struct(vec![dictionary_field.with_data_type(DataType::Utf8)].into()),
        ),
    ];
    for (name, child_type, decoded_type) in cases {
        let child_field =
            Field::new("dictionary", child_type.clone(), true).with_metadata([("source", "union")]);
        let int_field = Field::new("int", DataType::Int32, true);
        let fields =
            UnionFields::try_new([127, 3], [child_field.clone(), int_field.clone()]).unwrap();
        let decoded_fields = UnionFields::try_new(
            [127, 3],
            [child_field.with_data_type(decoded_type), int_field],
        )
        .unwrap();
        for mode in [UnionMode::Dense, UnionMode::Sparse] {
            // Input rows: integer 5, integer NULL, integer 1. The dictionary field
            // starts unused, but becomes the home of the decoded null.
            let child = match mode {
                UnionMode::Dense => new_empty_array(&child_type),
                UnionMode::Sparse => new_null_array(&child_type, 3),
            };
            let union = UnionArray::try_new(
                fields.clone(),
                vec![3; 3].into(),
                (mode == UnionMode::Dense).then(|| vec![0, 1, 2].into()),
                vec![
                    child,
                    Arc::new(Int32Array::from(vec![Some(5), None, Some(1)])),
                ],
            )
            .unwrap();
            #[rustfmt::skip]
            let selections: [(&str, std::ops::Range<usize>, &[i8]); 3] = [
                // Description                   Input rows  Expected decoded type IDs
                ("null selects dictionary",      0..3,       &[3, 127, 3]),
                ("dictionary stays unused",      0..1,       &[3]),
                ("no rows at all",               0..0,       &[]),
            ];
            for (selection, range, expected_ids) in selections {
                let input = union.slice(range.start, range.len());
                let nulls = input
                    .logical_nulls()
                    .filter(|nulls| nulls.null_count() != 0);
                for (sort_name, options, _) in SORT_CASES {
                    let decoded = check_roundtrip(&[Arc::new(input.clone())], &[options]);
                    let decoded = decoded[0].as_union();
                    // Even empty or unused fields must get the decoded type. Comparing
                    // the whole schema also checks that both levels keep their metadata.
                    assert_eq!(
                        decoded.data_type(),
                        &DataType::Union(decoded_fields.clone(), mode),
                        "{mode:?}, {name}, {selection}, {sort_name}"
                    );
                    assert_eq!(decoded.type_ids().as_ref(), expected_ids);
                    assert_eq!(
                        decoded.logical_nulls().filter(|n| n.null_count() != 0),
                        nulls
                    );
                    for (i, &id) in expected_ids.iter().enumerate() {
                        if id == 3 {
                            assert_eq!(decoded.value(i).to_data(), input.value(i).to_data());
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn unions_inside_structs_and_lists_follow_same_ordering() {
    // Each container has seven rows. A null parent is different from a present
    // parent containing a Union null; an empty list is different from both.
    // Row     Struct field       List contents
    //  0      integer 2          [integer 2]
    //  1      NULL parent        NULL parent (hides string "z")
    //  2      integer NULL       []
    //  3      string NULL        [integer NULL, string NULL]
    //  4      integer -1         [integer -1]
    //  5      string "a"         [string "a"]
    //  6      string "z"         [string "z"]
    let values: Vec<_> = INPUT.iter().map(|row| row.0).collect();
    let tail: ArrayRef = Arc::new(StringArray::from_iter(INPUT.iter().map(|row| row.2)));
    for mode in [UnionMode::Dense, UnionMode::Sparse] {
        let union: ArrayRef = Arc::new(int_string_union(mode, &int_string_fields(), &values));
        let field = Arc::new(Field::new("union", union.data_type().clone(), true));
        let valid = Some(NullBuffer::from(vec![
            true, false, true, true, true, true, true,
        ]));
        let structure = StructArray::new(
            vec![field.clone()].into(),
            vec![union.clone()],
            valid.clone(),
        );
        let list = ListArray::new(
            field,
            OffsetBuffer::from_lengths([1, 1, 0, 2, 1, 1, 1]),
            union,
            valid,
        );
        for array in [Arc::new(structure) as ArrayRef, Arc::new(list)] {
            let columns = [array, tail.clone()];
            for (_, options, _) in SORT_CASES {
                let options = [
                    options,
                    SortOptions {
                        descending: true,
                        nulls_first: false,
                    },
                ];
                check_order_matches_native(&columns, &options);
                let decoded = check_roundtrip(&columns, &options);
                assert_eq!(decoded[1].to_data(), tail.to_data());
            }
        }
    }
}
