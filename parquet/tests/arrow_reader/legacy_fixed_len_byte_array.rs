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

//! Compatibility with the malformed ARROW-RS-GH-11261-FLBA-DICT.parquet fixture.
//! See [arrow-rs #11261](https://github.com/apache/arrow-rs/issues/11261) for the defect.
//! The cases and expected values are defined by `ROW_GROUPS` and `expected` below.

use std::sync::Arc;

use super::bad_data::bad_data_dir;
use arrow::compute::{cast, concat};
use arrow_array::{
    Array, ArrayRef, FixedSizeBinaryArray, ListArray, RecordBatch, builder::FixedSizeBinaryBuilder,
};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field};
use bytes::Bytes;
use parquet::arrow::arrow_reader::{
    ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection, RowSelector,
};
use parquet::column::reader::ColumnReader;
use parquet::file::reader::{FileReader, SerializedFileReader};

// Every row group has four rows, with a nullable flat column and a list column
// containing nullable leaves. Column chunks have multiple data pages; spill
// cases mix dictionary-index and fallback pages. The final group uses Snappy.
const ROW_GROUPS: &[&str] = &[
    "v1 dictionary",
    "v1 plain",
    "v1 plain spill",
    "v1 delta length",
    "v1 delta length spill",
    "v1 delta spill",
    "v2 dictionary",
    "v2 plain",
    "v2 plain spill",
    "v2 delta length",
    "v2 delta length spill",
    "v2 delta spill",
    #[cfg(feature = "snap")]
    "v1 snappy",
];
const ROWS_PER_GROUP: usize = 4;

fn fixture() -> Bytes {
    let path = bad_data_dir().join("ARROW-RS-GH-11261-FLBA-DICT.parquet");
    // As with other corpus tests, missing data is a setup error, not a reason to skip.
    std::fs::read(&path)
        .unwrap_or_else(|e| {
            panic!(
                "failed to read {}: {e}; run `git submodule update --init`",
                path.display()
            )
        })
        .into()
}

fn expected(nested: bool) -> ArrayRef {
    // The first value deliberately resembles a BYTE_ARRAY length prefix.
    let values = [b"\x04\0\0\0".as_slice(), b"ABCD", b"wxyz"];
    let keys = [
        Some(0),
        Some(1),
        None,
        Some(2),
        Some(0),
        Some(1),
        Some(2),
        None,
        Some(0),
    ];
    let mut builder = FixedSizeBinaryBuilder::new(4);
    for key in keys {
        match key {
            Some(key) => builder.append_value(values[key]).unwrap(),
            None => builder.append_null(),
        }
    }
    let leaf: ArrayRef = Arc::new(builder.finish());
    if nested {
        Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::FixedSizeBinary(4), true)),
            OffsetBuffer::new(vec![0_i32, 3, 3, 6, 9].into()),
            leaf,
            None,
        ))
    } else {
        leaf.slice(0, ROWS_PER_GROUP)
    }
}

// Read each case independently, then together to exercise row-group transitions.
fn row_group_selections() -> impl Iterator<Item = Vec<usize>> {
    (0..ROW_GROUPS.len())
        .map(|i| vec![i])
        .chain(std::iter::once((0..ROW_GROUPS.len()).collect()))
}

fn selection(groups: usize, skip: bool) -> RowSelection {
    let mut selectors = vec![];
    for _ in 0..groups {
        if skip {
            selectors.push(RowSelector::skip(1));
        }
        selectors.push(RowSelector::select(ROWS_PER_GROUP - usize::from(skip)));
    }
    RowSelection::from(selectors)
}

fn assert_batches(batches: &[RecordBatch], groups: &[usize], skip: bool) {
    for (column, nested) in [false, true].into_iter().enumerate() {
        let expected = expected(nested);
        let start = usize::from(skip);
        let expected = expected.slice(start, expected.len() - start);
        let expected = concat(&vec![expected.as_ref(); groups.len()]).unwrap();
        let arrays: Vec<_> = batches.iter().map(|b| b.column(column).as_ref()).collect();
        let actual = concat(&arrays).unwrap();
        // Dictionary ordering is not prescribed; compare the decoded values.
        let actual = cast(actual.as_ref(), expected.data_type()).unwrap();
        assert_eq!(
            actual.to_data(),
            expected.to_data(),
            "row groups {groups:?}, column {column}, skip {skip}"
        );
    }
}

#[test]
fn read_legacy_fixed_len_byte_array() {
    let data = fixture();
    for groups in row_group_selections() {
        for skip_metadata in [false, true] {
            for batch_size in [1, 3, 1024] {
                for skip in [false, true] {
                    let options = ArrowReaderOptions::new().with_skip_arrow_metadata(skip_metadata);
                    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(
                        data.clone(),
                        options,
                    )
                    .unwrap();
                    let batches = builder
                        .with_row_groups(groups.clone())
                        .with_batch_size(batch_size)
                        .with_row_selection(selection(groups.len(), skip))
                        .build()
                        .unwrap()
                        .collect::<Result<Vec<_>, _>>()
                        .unwrap();
                    assert_batches(&batches, &groups, skip);
                }
            }
        }
    }
}

fn physical_values(array: &dyn Array, out: &mut Vec<Vec<u8>>) {
    for i in 0..array.len() {
        if array.is_null(i) {
            continue;
        }
        if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
            physical_values(list.value(i).as_ref(), out);
        } else {
            let fixed = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            out.push(fixed.value(i).to_vec());
        }
    }
}

#[test]
fn read_legacy_fixed_len_byte_array_typed() {
    let file = SerializedFileReader::new(fixture()).unwrap();
    assert_eq!(file.num_row_groups(), 13);
    assert_eq!(file.metadata().file_metadata().num_rows(), 52);
    for (index, name) in ROW_GROUPS.iter().enumerate() {
        let row_group = file.get_row_group(index).unwrap();
        assert_eq!(row_group.metadata().num_rows(), ROWS_PER_GROUP as i64);
        assert_eq!(row_group.num_columns(), 2);
        for (column, nested) in [false, true].into_iter().enumerate() {
            for skip in [false, true] {
                let ColumnReader::FixedLenByteArrayColumnReader(mut reader) =
                    row_group.get_column_reader(column).unwrap()
                else {
                    panic!("{name}: expected FLBA column");
                };
                if skip {
                    assert_eq!(reader.skip_records(1).unwrap(), 1);
                }
                let (mut values, mut def, mut rep) = (vec![], vec![], vec![]);
                while reader
                    .read_records(1, Some(&mut def), Some(&mut rep), &mut values)
                    .unwrap()
                    .0
                    != 0
                {}
                let expected = expected(nested);
                let start = usize::from(skip);
                let mut physical = vec![];
                physical_values(
                    expected.slice(start, expected.len() - start).as_ref(),
                    &mut physical,
                );
                assert_eq!(
                    values.iter().map(|v| v.data()).collect::<Vec<_>>(),
                    physical,
                    "{name}, column {column}, skip {skip}"
                );
            }
        }
    }
}

#[cfg(feature = "async")]
#[tokio::test]
async fn read_legacy_fixed_len_byte_array_async() {
    use futures::TryStreamExt;
    use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;

    let data = fixture();
    for groups in row_group_selections() {
        for skip_metadata in [false, true] {
            for batch_size in [1, 3, 1024] {
                for skip in [false, true] {
                    let options = ArrowReaderOptions::new().with_skip_arrow_metadata(skip_metadata);
                    let builder = ParquetRecordBatchStreamBuilder::new_with_options(
                        std::io::Cursor::new(data.clone()),
                        options,
                    )
                    .await
                    .unwrap();
                    let batches: Vec<_> = builder
                        .with_row_groups(groups.clone())
                        .with_batch_size(batch_size)
                        .with_row_selection(selection(groups.len(), skip))
                        .build()
                        .unwrap()
                        .try_collect()
                        .await
                        .unwrap();
                    assert_batches(&batches, &groups, skip);
                }
            }
        }
    }
}
