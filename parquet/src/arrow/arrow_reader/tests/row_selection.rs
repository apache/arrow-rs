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

//! Row selection and skipping for flat and nested data, including randomized selections.

use super::*;

/// Given a RecordBatch containing all the column data, return the expected batches given
/// a `batch_size` and `selection`
fn get_expected_batches(
    column: &RecordBatch,
    selection: &RowSelection,
    batch_size: usize,
) -> Vec<RecordBatch> {
    let mut expected_batches = vec![];

    let mut selection: VecDeque<_> = selection.clone().into();
    let mut row_offset = 0;
    let mut last_start = None;
    while row_offset < column.num_rows() && !selection.is_empty() {
        let mut batch_remaining = batch_size.min(column.num_rows() - row_offset);
        while batch_remaining > 0 && !selection.is_empty() {
            let (to_read, skip) = match selection.front_mut() {
                Some(selection) if selection.row_count > batch_remaining => {
                    selection.row_count -= batch_remaining;
                    (batch_remaining, selection.skip)
                }
                Some(_) => {
                    let select = selection.pop_front().unwrap();
                    (select.row_count, select.skip)
                }
                None => break,
            };

            batch_remaining -= to_read;

            match skip {
                true => {
                    if let Some(last_start) = last_start.take() {
                        expected_batches.push(column.slice(last_start, row_offset - last_start))
                    }
                    row_offset += to_read
                }
                false => {
                    last_start.get_or_insert(row_offset);
                    row_offset += to_read
                }
            }
        }
    }

    if let Some(last_start) = last_start.take() {
        expected_batches.push(column.slice(last_start, row_offset - last_start))
    }

    // Sanity check, all batches except the final should be the batch size
    for batch in &expected_batches[..expected_batches.len() - 1] {
        assert_eq!(batch.num_rows(), batch_size);
    }

    expected_batches
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_scan_row_with_selection() {
    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/alltypes_tiny_pages_plain.parquet");
    let test_file = File::open(&path).unwrap();

    let mut serial_reader =
        ParquetRecordBatchReader::try_new(File::open(&path).unwrap(), 7300).unwrap();
    let data = serial_reader.next().unwrap().unwrap();

    let do_test = |batch_size: usize, selection_len: usize| {
        for skip_first in [false, true] {
            let selections = create_test_selection(batch_size, data.num_rows(), skip_first).0;

            let expected = get_expected_batches(&data, &selections, batch_size);
            let skip_reader = create_skip_reader(&test_file, batch_size, selections);
            assert_eq!(
                skip_reader.collect::<Result<Vec<_>, _>>().unwrap(),
                expected,
                "batch_size: {batch_size}, selection_len: {selection_len}, skip_first: {skip_first}"
            );
        }
    };

    // total row count 7300
    // 1. test selection len more than one page row count
    do_test(1000, 1000);

    // 2. test selection len less than one page row count
    do_test(20, 20);

    // 3. test selection_len less than batch_size
    do_test(20, 5);

    // 4. test selection_len more than batch_size
    // If batch_size < selection_len
    do_test(20, 5);

    fn create_skip_reader(
        test_file: &File,
        batch_size: usize,
        selections: RowSelection,
    ) -> ParquetRecordBatchReader {
        let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
        let file = test_file.try_clone().unwrap();
        ParquetRecordBatchReaderBuilder::try_new_with_options(file, options)
            .unwrap()
            .with_batch_size(batch_size)
            .with_row_selection(selections)
            .build()
            .unwrap()
    }
}

#[test]
#[cfg(feature = "snap")]
fn test_read_nested_lists() {
    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/nested_lists.snappy.parquet");
    let file = File::open(path).unwrap();

    let f = file.try_clone().unwrap();
    let mut reader = ParquetRecordBatchReader::try_new(f, 60).unwrap();
    let expected = reader.next().unwrap().unwrap();
    assert_eq!(expected.num_rows(), 3);

    let selection = RowSelection::from(vec![
        RowSelector::skip(1),
        RowSelector::select(1),
        RowSelector::skip(1),
    ]);
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .with_row_selection(selection)
        .build()
        .unwrap();

    let actual = reader.next().unwrap().unwrap();
    assert_eq!(actual.num_rows(), 1);
    assert_eq!(actual.column(0), &expected.column(0).slice(1, 1));
}

#[test]
fn test_list_skip() {
    let mut list = ListBuilder::new(Int32Builder::new());
    list.append_value([Some(1), Some(2)]);
    list.append_value([Some(3)]);
    list.append_value([Some(4)]);
    let list = list.finish();
    let batch = RecordBatch::try_from_iter([("l", Arc::new(list) as _)]).unwrap();

    // First page contains 2 values but only 1 row
    let props = WriterProperties::builder()
        .set_data_page_row_count_limit(1)
        .set_write_batch_size(2)
        .build();

    let mut buffer = Vec::with_capacity(1024);
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let selection = vec![RowSelector::skip(2), RowSelector::select(1)];
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(buffer))
        .unwrap()
        .with_row_selection(selection.into())
        .build()
        .unwrap();
    let out = reader.next().unwrap().unwrap();
    assert_eq!(out.num_rows(), 1);
    assert_eq!(out, batch.slice(2, 1));
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_list_selection() {
    let schema = Arc::new(Schema::new(vec![Field::new_list(
        "list",
        Field::new_list_field(ArrowDataType::Utf8, true),
        false,
    )]));
    let mut buf = Vec::with_capacity(1024);

    let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), None).unwrap();

    for i in 0..2 {
        let mut list_a_builder = ListBuilder::new(StringBuilder::new());
        for j in 0..1024 {
            list_a_builder.values().append_value(format!("{i} {j}"));
            list_a_builder.append(true);
        }
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(list_a_builder.finish())]).unwrap();
        writer.write(&batch).unwrap();
    }
    let _metadata = writer.close().unwrap();

    let buf = Bytes::from(buf);
    let reader = ParquetRecordBatchReaderBuilder::try_new(buf)
        .unwrap()
        .with_row_selection(RowSelection::from(vec![
            RowSelector::skip(100),
            RowSelector::select(924),
            RowSelector::skip(100),
            RowSelector::select(924),
        ]))
        .build()
        .unwrap();

    let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
    let batch = concat_batches(&schema, &batches).unwrap();

    assert_eq!(batch.num_rows(), 924 * 2);
    let list = batch.column(0).as_list::<i32>();

    for w in list.value_offsets().windows(2) {
        assert_eq!(w[0] + 1, w[1])
    }
    let mut values = list.values().as_string::<i32>().iter();

    for i in 0..2 {
        for j in 100..1024 {
            let expected = format!("{i} {j}");
            assert_eq!(values.next().unwrap().unwrap(), &expected);
        }
    }
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_list_selection_fuzz() {
    let mut rng = rng();
    let schema = Arc::new(Schema::new(vec![Field::new_list(
        "list",
        Field::new_list(
            Field::LIST_FIELD_DEFAULT_NAME,
            Field::new_list_field(ArrowDataType::Int32, true),
            true,
        ),
        true,
    )]));
    let mut buf = Vec::with_capacity(1024);
    let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), None).unwrap();

    let mut list_a_builder = ListBuilder::new(ListBuilder::new(Int32Builder::new()));

    for _ in 0..2048 {
        if rng.random_bool(0.2) {
            list_a_builder.append(false);
            continue;
        }

        let list_a_len = rng.random_range(0..10);
        let list_b_builder = list_a_builder.values();

        for _ in 0..list_a_len {
            if rng.random_bool(0.2) {
                list_b_builder.append(false);
                continue;
            }

            let list_b_len = rng.random_range(0..10);
            let int_builder = list_b_builder.values();
            for _ in 0..list_b_len {
                match rng.random_bool(0.2) {
                    true => int_builder.append_null(),
                    false => int_builder.append_value(rng.random()),
                }
            }
            list_b_builder.append(true)
        }
        list_a_builder.append(true);
    }

    let array = Arc::new(list_a_builder.finish());
    let batch = RecordBatch::try_new(schema, vec![array]).unwrap();

    writer.write(&batch).unwrap();
    let _metadata = writer.close().unwrap();

    let buf = Bytes::from(buf);

    let cases = [
        vec![
            RowSelector::skip(100),
            RowSelector::select(924),
            RowSelector::skip(100),
            RowSelector::select(924),
        ],
        vec![
            RowSelector::select(924),
            RowSelector::skip(100),
            RowSelector::select(924),
            RowSelector::skip(100),
        ],
        vec![
            RowSelector::skip(1023),
            RowSelector::select(1),
            RowSelector::skip(1023),
            RowSelector::select(1),
        ],
        vec![
            RowSelector::select(1),
            RowSelector::skip(1023),
            RowSelector::select(1),
            RowSelector::skip(1023),
        ],
    ];

    for batch_size in [100, 1024, 2048] {
        for selection in &cases {
            let selection = RowSelection::from(selection.clone());
            let reader = ParquetRecordBatchReaderBuilder::try_new(buf.clone())
                .unwrap()
                .with_row_selection(selection.clone())
                .with_batch_size(batch_size)
                .build()
                .unwrap();

            let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
            let actual = concat_batches(batch.schema_ref(), &batches).unwrap();
            assert_eq!(actual.num_rows(), selection.row_count());

            let mut batch_offset = 0;
            let mut actual_offset = 0;
            for selector in selection.iter() {
                if selector.skip {
                    batch_offset += selector.row_count;
                    continue;
                }

                assert_eq!(
                    batch.slice(batch_offset, selector.row_count),
                    actual.slice(actual_offset, selector.row_count)
                );

                batch_offset += selector.row_count;
                actual_offset += selector.row_count;
            }
        }
    }
}
