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

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Int32Type};
use arrow_array::ArrayAccessor;
use arrow_array::cast::AsArray;
use parquet::arrow::arrow_reader::RowSelectionPolicy;

use super::fixture::{
    CaseFixture, HETEROGENEOUS_FIXED_BINARY_WIDTH, heterogeneous_dictionary_key,
    heterogeneous_dictionary_value, heterogeneous_fixed_binary_value, heterogeneous_int32_value,
    heterogeneous_string_value,
};
use super::model::{CaseSpec, PAYLOAD_VALUE_MODULUS, ROWS_PER_GROUP};
use super::runner::{run_collect_payload0, run_with_consumer};
use super::shapes::expand_pattern;

pub(crate) async fn preflight_auto(case: &CaseSpec, fixture: &CaseFixture) {
    let actual = run_collect_payload0(fixture, RowSelectionPolicy::default()).await;
    assert_eq!(
        actual.row_count, fixture.expected_rows,
        "{} returned an unexpected number of rows",
        case.name
    );

    let expected = expected_selected_global_rows(case);
    assert_eq!(actual.payload0.len(), expected.len(), "{}", case.name);
    if let Some((output_row, (actual, expected))) = actual
        .payload0
        .iter()
        .zip(
            expected
                .iter()
                .map(|row| row.wrapping_rem(PAYLOAD_VALUE_MODULUS) as i32),
        )
        .enumerate()
        .find(|(_, (actual, expected))| **actual != *expected)
    {
        panic!(
            "{} returned the wrong source row at output {output_row}: expected {expected}, got {actual}",
            case.name
        );
    }
}

pub(crate) async fn preflight_heterogeneous(case: &CaseSpec, fixture: &CaseFixture) {
    let expected = expected_selected_global_rows(case);
    for (policy_name, policy) in [
        ("auto", RowSelectionPolicy::default()),
        ("auto_per_column", RowSelectionPolicy::AutoPerColumn),
        ("selectors", RowSelectionPolicy::Selectors),
        ("mask", RowSelectionPolicy::Mask),
    ] {
        let mut output_offset = 0;
        let row_count = run_with_consumer(fixture, policy, |batch| {
            let batch_end = output_offset + batch.num_rows();
            assert!(
                batch_end <= expected.len(),
                "{} ({policy_name}) returned too many rows",
                case.name
            );
            assert_heterogeneous_batch(
                case,
                policy_name,
                batch,
                &expected[output_offset..batch_end],
            );
            output_offset = batch_end;
        })
        .await;

        assert_eq!(
            row_count, fixture.expected_rows,
            "{} ({policy_name}) returned an unexpected number of rows",
            case.name
        );
        assert_eq!(
            output_offset,
            expected.len(),
            "{} ({policy_name}) did not return every expected row",
            case.name
        );
    }
}

fn assert_heterogeneous_batch(
    case: &CaseSpec,
    policy_name: &str,
    batch: &arrow::record_batch::RecordBatch,
    expected_rows: &[usize],
) {
    let expected_types = [
        DataType::Int32,
        DataType::Int32,
        DataType::Utf8View,
        DataType::Utf8View,
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        DataType::FixedSizeBinary(HETEROGENEOUS_FIXED_BINARY_WIDTH as i32),
        DataType::FixedSizeBinary(HETEROGENEOUS_FIXED_BINARY_WIDTH as i32),
    ];
    assert_eq!(batch.num_columns(), expected_types.len());
    for (column_idx, expected_type) in expected_types.iter().enumerate() {
        assert_eq!(
            batch.column(column_idx).data_type(),
            expected_type,
            "{} ({policy_name}) returned the wrong type for payload_{column_idx}",
            case.name
        );
    }

    let int32 = [
        batch.column(0).as_primitive::<Int32Type>(),
        batch.column(1).as_primitive::<Int32Type>(),
    ];
    let strings = [
        batch.column(2).as_string_view(),
        batch.column(3).as_string_view(),
    ];
    let dictionaries = [
        batch
            .column(4)
            .as_dictionary::<Int32Type>()
            .downcast_dict::<StringArray>()
            .unwrap(),
        batch
            .column(5)
            .as_dictionary::<Int32Type>()
            .downcast_dict::<StringArray>()
            .unwrap(),
    ];
    let fixed_binary = [
        batch.column(6).as_fixed_size_binary(),
        batch.column(7).as_fixed_size_binary(),
    ];

    for (batch_row, global_row) in expected_rows.iter().copied().enumerate() {
        for (column_idx, values) in int32.iter().enumerate() {
            assert_eq!(
                values.value(batch_row),
                heterogeneous_int32_value(column_idx, global_row),
                "{} ({policy_name}) returned the wrong payload_{column_idx} value at source row {global_row}",
                case.name
            );
        }
        for (offset, values) in strings.iter().enumerate() {
            let column_idx = offset + 2;
            assert_eq!(
                values.value(batch_row),
                heterogeneous_string_value(column_idx, global_row),
                "{} ({policy_name}) returned the wrong payload_{column_idx} value at source row {global_row}",
                case.name
            );
        }
        for (offset, values) in dictionaries.iter().enumerate() {
            let column_idx = offset + 4;
            let key = heterogeneous_dictionary_key(column_idx, global_row);
            assert_eq!(
                values.value(batch_row),
                heterogeneous_dictionary_value(column_idx, key),
                "{} ({policy_name}) returned the wrong payload_{column_idx} value at source row {global_row}",
                case.name
            );
        }
        for (offset, values) in fixed_binary.iter().enumerate() {
            let column_idx = offset + 6;
            let expected = heterogeneous_fixed_binary_value(column_idx, global_row);
            assert_eq!(
                values.value(batch_row),
                expected.as_slice(),
                "{} ({policy_name}) returned the wrong payload_{column_idx} value at source row {global_row}",
                case.name
            );
        }
    }
}

fn expected_selected_global_rows(case: &CaseSpec) -> Vec<usize> {
    case.row_groups
        .iter()
        .copied()
        .enumerate()
        .flat_map(|(row_group_idx, pattern)| {
            expand_pattern(pattern, ROWS_PER_GROUP)
                .into_iter()
                .enumerate()
                .filter(|(_, selected)| *selected == 1)
                .map(move |(row_idx, _)| row_group_idx * ROWS_PER_GROUP + row_idx)
        })
        .collect()
}
