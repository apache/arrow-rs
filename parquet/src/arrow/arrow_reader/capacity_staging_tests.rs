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

//! Real-file tests using a small output capacity in place of `i32::MAX`.

use super::{
    ArrowPredicateFn, ParquetRecordBatchReaderBuilder, RowFilter, RowSelection, RowSelectionPolicy,
};
use crate::arrow::schema::ArrowSchemaConverter;
use crate::arrow::{ArrowWriter, ProjectionMask};
use crate::basic::{Encoding, Type as PhysicalType};
use crate::file::properties::WriterProperties;
use crate::file::reader::{FileReader, SerializedFileReader};
use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, Int64Type};
use arrow_array::{
    ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, DictionaryArray, Int64Array,
    LargeBinaryArray, LargeStringArray, RecordBatch, StringArray, StructArray,
};
use arrow_schema::{DataType, Field, Schema};
use arrow_select::concat::concat_batches;
use arrow_select::filter::filter_record_batch;
use bytes::Bytes;
use std::sync::{Arc, Mutex};

const ENCODINGS: [Encoding; 4] = [
    Encoding::PLAIN,
    Encoding::RLE_DICTIONARY,
    Encoding::DELTA_LENGTH_BYTE_ARRAY,
    Encoding::DELTA_BYTE_ARRAY,
];
const POLICIES: [RowSelectionPolicy; 2] = [RowSelectionPolicy::Selectors, RowSelectionPolicy::Mask];

fn write(batch: &RecordBatch, encoding: Encoding) -> Bytes {
    let descriptor = ArrowSchemaConverter::new()
        .convert(batch.schema().as_ref())
        .unwrap();
    let mut properties = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_max_row_group_row_count(Some(16))
        .set_write_batch_size(3)
        .set_data_page_row_count_limit(3);
    for column in descriptor.columns() {
        if column.physical_type() == PhysicalType::BYTE_ARRAY {
            properties = if encoding == Encoding::RLE_DICTIONARY {
                properties.set_column_dictionary_enabled(column.path().clone(), true)
            } else {
                properties.set_column_encoding(column.path().clone(), encoding)
            };
        }
    }
    let mut data = vec![];
    let mut writer =
        ArrowWriter::try_new(&mut data, batch.schema(), Some(properties.build())).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
    let data = Bytes::from(data);
    let file = SerializedFileReader::new(data.clone()).unwrap();
    let column = descriptor
        .columns()
        .iter()
        .position(|column| column.physical_type() == PhysicalType::BYTE_ARRAY)
        .unwrap();
    assert!(
        file.metadata()
            .row_group(0)
            .column(column)
            .encodings()
            .any(|e| e == encoding),
        "fixture must exercise {encoding:?}"
    );
    data
}

fn mixed_batch() -> RecordBatch {
    let text = StringArray::from_iter(
        (0..24).map(|i| (i % 5 != 0).then(|| format!("row-{i:03}-{}", "界".repeat(1 + i % 3)))),
    );
    let bytes: Vec<_> = (0..24)
        .map(|i| vec![0xff, i, 0, 1, 2, 3, 4, 5, 6])
        .collect();
    let binary = BinaryArray::from_iter(
        bytes
            .iter()
            .enumerate()
            .map(|(i, value)| (i % 7 != 0).then_some(value.as_slice())),
    );
    RecordBatch::try_from_iter(vec![
        ("text", Arc::new(text) as ArrayRef),
        ("binary", Arc::new(binary) as ArrayRef),
        (
            "id",
            Arc::new(Int64Array::from_iter_values(0..24)) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn read_sync(
    data: &Bytes,
    capacity: Option<usize>,
    policy: RowSelectionPolicy,
    selection: Option<RowSelection>,
    range: (usize, Option<usize>),
) -> Vec<RecordBatch> {
    let mut builder = ParquetRecordBatchReaderBuilder::try_new(data.clone())
        .unwrap()
        .with_batch_size(8)
        .with_row_selection_policy(policy)
        .with_byte_array_capacity(capacity)
        .with_offset(range.0);
    if let Some(selection) = selection {
        builder = builder.with_row_selection(selection);
    }
    if let Some(limit) = range.1 {
        builder = builder.with_limit(limit);
    }
    builder.build().unwrap().map(Result::unwrap).collect()
}

fn flatten(batches: &[RecordBatch]) -> RecordBatch {
    concat_batches(&batches[0].schema(), batches.iter()).unwrap()
}

fn assert_capacity(batches: &[RecordBatch], capacity: usize) {
    for batch in batches {
        assert!(batch.num_rows() > 0 && batch.num_rows() <= 8);
        for array in batch.columns() {
            let offsets = match array.data_type() {
                DataType::Utf8 => array.as_string::<i32>().value_offsets(),
                DataType::Binary => array.as_binary::<i32>().value_offsets(),
                _ => continue,
            };
            assert!((offsets[offsets.len() - 1] - offsets[0]) as usize <= capacity);
        }
    }
}

#[test]
fn test_capacity_staging_flat_encodings_and_policies() {
    let batch = mixed_batch();
    let selected = BooleanArray::from((0..24).map(|i| i % 4 != 0).collect::<Vec<_>>());
    for encoding in ENCODINGS {
        let data = write(&batch, encoding);
        for policy in POLICIES {
            for selection in [
                None,
                Some(RowSelection::from_filters(std::slice::from_ref(&selected))),
            ] {
                let expected = if selection.is_some() {
                    filter_record_batch(&batch, &selected).unwrap()
                } else {
                    batch.clone()
                };
                let native = read_sync(&data, None, policy, selection.clone(), (0, None));
                let fitting = read_sync(
                    &data,
                    Some(i32::MAX as usize),
                    policy,
                    selection.clone(),
                    (0, None),
                );
                assert_eq!(
                    fitting, native,
                    "{encoding:?}, {policy:?}: fitting batch shape"
                );
                let split = read_sync(&data, Some(28), policy, selection, (0, None));
                assert_capacity(&split, 28);
                assert_eq!(flatten(&split), expected, "{encoding:?}, {policy:?}");
                assert!(split.len() > native.len());
            }
        }
    }
}

#[test]
fn test_capacity_staging_multicolumn_boundaries_and_nulls() {
    let batch = RecordBatch::try_from_iter(vec![
        (
            "text",
            Arc::new(StringArray::from(vec![
                Some("aa"),
                None,
                Some("bbb"),
                Some("c"),
                Some("d"),
                None,
            ])) as ArrayRef,
        ),
        (
            "binary",
            Arc::new(BinaryArray::from(vec![
                Some(b"1234".as_slice()),
                Some(b"5678".as_slice()),
                None,
                Some(b"9".as_slice()),
                Some(b"0123".as_slice()),
                Some(b"56".as_slice()),
            ])) as ArrayRef,
        ),
        (
            "id",
            Arc::new(Int64Array::from_iter_values(0..6)) as ArrayRef,
        ),
    ])
    .unwrap();
    let data = write(&batch, Encoding::PLAIN);
    let split = read_sync(
        &data,
        Some(7),
        RowSelectionPolicy::Selectors,
        None,
        (0, None),
    );
    assert_eq!(
        split.iter().map(RecordBatch::num_rows).collect::<Vec<_>>(),
        [1, 3, 2]
    );
    assert_eq!(flatten(&split), batch);
    assert_capacity(&split, 7);

    let batch = mixed_batch();
    let data = write(&batch, Encoding::DELTA_LENGTH_BYTE_ARRAY);
    for policy in POLICIES {
        let split = read_sync(&data, Some(28), policy, None, (3, Some(17)));
        assert_capacity(&split, 28);
        assert_eq!(flatten(&split), batch.slice(3, 17));
    }
}

#[test]
fn test_capacity_staging_keeps_large_and_nested_types_native() {
    let values = ["a-value-longer-than-the-testing-capacity"; 3];
    let mut lists = ListBuilder::new(StringBuilder::new());
    for value in values {
        lists.values().append_value(value);
        lists.append(true);
    }
    let nested = StructArray::from(vec![(
        Arc::new(Field::new("child", DataType::Utf8, false)),
        Arc::new(StringArray::from(values.to_vec())) as ArrayRef,
    )]);
    let bytes: Vec<_> = values.iter().map(|value| value.as_bytes()).collect();
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec!["a", "b", "c"])),
        Arc::new(LargeStringArray::from(values.to_vec())),
        Arc::new(LargeBinaryArray::from(bytes.clone())),
        Arc::new(BinaryViewArray::from(bytes)),
        Arc::new(values.into_iter().collect::<DictionaryArray<Int32Type>>()),
        Arc::new(lists.finish()),
        Arc::new(nested),
    ];
    let fields: Vec<_> = arrays
        .iter()
        .enumerate()
        .map(|(i, array)| Field::new(format!("c{i}"), array.data_type().clone(), true))
        .collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
    let data = write(&batch, Encoding::RLE_DICTIONARY);
    let native = read_sync(&data, None, RowSelectionPolicy::Selectors, None, (0, None));
    let split = read_sync(
        &data,
        Some(2),
        RowSelectionPolicy::Selectors,
        None,
        (0, None),
    );
    assert_eq!(flatten(&split), flatten(&native));
    assert_eq!(
        split.iter().map(RecordBatch::num_rows).collect::<Vec<_>>(),
        [2, 1]
    );
}

fn oversized_row_batch() -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "text",
            Arc::new(StringArray::from(vec!["a", "too-wide", "tail"])) as ArrayRef,
        ),
        ("id", Arc::new(Int64Array::from(vec![0, 1, 2])) as ArrayRef),
    ])
    .unwrap()
}

fn assert_oversized_row_error(error: impl std::fmt::Display) {
    let error = error.to_string();
    assert!(
        error.contains("single row needs 8 bytes")
            && error.contains("column text")
            && error.contains("5 byte limit"),
        "{error}"
    );
}

#[test]
fn test_capacity_staging_single_row_error_ends_stream() {
    let batch = oversized_row_batch();
    for start in [0, 1] {
        let input = batch.slice(start, batch.num_rows() - start);
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(write(&input, Encoding::PLAIN))
            .unwrap()
            .with_batch_size(3)
            .with_byte_array_capacity(Some(5))
            .build()
            .unwrap();
        if start == 0 {
            let prefix = reader.next().unwrap().unwrap();
            assert_eq!(prefix.schema(), batch.schema());
            assert_eq!(prefix, batch.slice(0, 1));
        }
        assert_oversized_row_error(reader.next().unwrap().unwrap_err());
        assert!(reader.next().is_none());
        assert!(reader.next().is_none());
    }
}

#[test]
fn test_capacity_staging_push_decoder_error_after_prefix() {
    use crate::DecodeResult;
    use crate::arrow::push_decoder::ParquetPushDecoderBuilder;

    let batch = oversized_row_batch();
    let data = write(&batch, Encoding::PLAIN);
    let file = SerializedFileReader::new(data.clone()).unwrap();
    let mut decoder = ParquetPushDecoderBuilder::try_new_decoder(Arc::new(file.metadata().clone()))
        .unwrap()
        .with_batch_size(3)
        .with_byte_array_capacity(Some(5))
        .build()
        .unwrap();
    decoder.push_range(0..data.len() as u64, data).unwrap();
    let DecodeResult::Data(prefix) = decoder.try_decode().unwrap() else {
        panic!("prefetched rows must produce the valid prefix");
    };
    assert_eq!(prefix.schema(), batch.schema());
    assert_eq!(prefix, batch.slice(0, 1));
    assert_oversized_row_error(decoder.try_decode().unwrap_err());
    assert!(matches!(
        decoder.try_decode().unwrap(),
        DecodeResult::Finished
    ));
    assert!(matches!(
        decoder.try_decode().unwrap(),
        DecodeResult::Finished
    ));
}

#[cfg(feature = "async")]
#[tokio::test]
async fn test_capacity_staging_async_error_after_prefix() {
    use crate::arrow::ParquetRecordBatchStreamBuilder;
    use futures::StreamExt;
    use std::io::Cursor;

    let batch = oversized_row_batch();
    let mut stream =
        ParquetRecordBatchStreamBuilder::new(Cursor::new(write(&batch, Encoding::PLAIN)))
            .await
            .unwrap()
            .with_batch_size(3)
            .with_byte_array_capacity(Some(5))
            .build()
            .unwrap();
    let prefix = stream.next().await.unwrap().unwrap();
    assert_eq!(prefix.schema(), batch.schema());
    assert_eq!(prefix, batch.slice(0, 1));
    assert_oversized_row_error(stream.next().await.unwrap().unwrap_err());
    assert!(stream.next().await.is_none());
    assert!(stream.next().await.is_none());
}

fn predicate_batch() -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "text",
            Arc::new(StringArray::from_iter_values(
                (0..24).map(|i| format!("value-{i:03}")),
            )) as ArrayRef,
        ),
        (
            "id",
            Arc::new(Int64Array::from_iter_values(0..24)) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn stateful_filter(projection: ProjectionMask, counts: Arc<Mutex<Vec<usize>>>) -> RowFilter {
    let predicate = ArrowPredicateFn::new(projection, move |batch: RecordBatch| {
        assert!(
            batch.num_rows() <= 2,
            "predicate input must also be capacity-safe"
        );
        let strings = batch.column(0).as_string::<i32>();
        let mut counts = counts.lock().unwrap();
        Ok(BooleanArray::from_iter(strings.iter().map(|value| {
            let id: usize = value.unwrap()[6..].parse().unwrap();
            counts[id] += 1;
            !id.is_multiple_of(3)
        })))
    });
    RowFilter::new(vec![Box::new(predicate)])
}

fn assert_predicate_output(
    batches: &[RecordBatch],
    counts: &Mutex<Vec<usize>>,
    expected: &RecordBatch,
) {
    assert_eq!(*counts.lock().unwrap(), vec![1; 24]);
    assert_capacity(batches, 20);
    let selected = BooleanArray::from((0..24).map(|i| i % 3 != 0).collect::<Vec<_>>());
    assert_eq!(
        flatten(batches),
        filter_record_batch(expected, &selected).unwrap()
    );
}

#[test]
fn test_capacity_staging_stateful_sync_predicates() {
    let batch = predicate_batch();
    let data = write(&batch, Encoding::DELTA_BYTE_ARRAY);
    for policy in POLICIES {
        let builder = ParquetRecordBatchReaderBuilder::try_new(data.clone()).unwrap();
        let projection = ProjectionMask::leaves(builder.parquet_schema(), [0]);
        let counts = Arc::new(Mutex::new(vec![0; 24]));
        let batches: Vec<_> = builder
            .with_batch_size(8)
            .with_row_selection_policy(policy)
            .with_row_filter(stateful_filter(projection, counts.clone()))
            .with_byte_array_capacity(Some(20))
            .build()
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_predicate_output(&batches, &counts, &batch);
    }
}

#[test]
fn test_capacity_staging_push_decoder_rebuild() {
    use crate::DecodeResult;
    use crate::arrow::push_decoder::ParquetPushDecoderBuilder;

    let batch = mixed_batch();
    let data = write(&batch, Encoding::DELTA_LENGTH_BYTE_ARRAY);
    let file = SerializedFileReader::new(data.clone()).unwrap();
    assert_eq!(file.metadata().num_row_groups(), 2);
    let mut decoder = ParquetPushDecoderBuilder::try_new_decoder(Arc::new(file.metadata().clone()))
        .unwrap()
        .with_batch_size(8)
        .with_byte_array_capacity(Some(28))
        .build()
        .unwrap();
    decoder.push_range(0..data.len() as u64, data).unwrap();
    let DecodeResult::Data(reader) = decoder.try_next_reader().unwrap() else {
        panic!("prefetched first row group must be ready");
    };
    let first: Vec<_> = reader.map(Result::unwrap).collect();
    assert_capacity(&first, 28);
    assert!(first.len() > 2);
    assert_eq!(flatten(&first), batch.slice(0, 16));

    let builder = decoder.into_builder().unwrap();
    assert_eq!(builder.byte_array_capacity, Some(28));
    let mut decoder = builder.build().unwrap();
    let DecodeResult::Data(reader) = decoder.try_next_reader().unwrap() else {
        panic!("rebuilt decoder must retain prefetched second row group");
    };
    let second: Vec<_> = reader.map(Result::unwrap).collect();
    assert_capacity(&second, 28);
    assert!(second.len() > 1);
    assert_eq!(flatten(&second), batch.slice(16, 8));
    assert!(matches!(
        decoder.try_next_reader().unwrap(),
        DecodeResult::Finished
    ));
}

#[cfg(feature = "async")]
#[tokio::test]
async fn test_capacity_staging_async_encodings_predicates_and_cache() {
    use super::metrics::ArrowReaderMetrics;
    use crate::arrow::ParquetRecordBatchStreamBuilder;
    use futures::TryStreamExt;
    use std::io::Cursor;

    let batch = predicate_batch();
    for encoding in ENCODINGS {
        let data = write(&batch, encoding);
        for policy in POLICIES {
            for cache_size in [0, usize::MAX] {
                let builder = ParquetRecordBatchStreamBuilder::new(Cursor::new(data.clone()))
                    .await
                    .unwrap();
                let projection = ProjectionMask::leaves(builder.parquet_schema(), [0]);
                let counts = Arc::new(Mutex::new(vec![0; 24]));
                let metrics = ArrowReaderMetrics::enabled();
                let batches: Vec<RecordBatch> = builder
                    .with_batch_size(8)
                    .with_row_selection_policy(policy)
                    .with_max_predicate_cache_size(cache_size)
                    .with_metrics(metrics.clone())
                    .with_row_filter(stateful_filter(projection, counts.clone()))
                    .with_byte_array_capacity(Some(20))
                    .build()
                    .unwrap()
                    .try_collect()
                    .await
                    .unwrap();
                assert_predicate_output(&batches, &counts, &batch);
                let reads = metrics.records_read_from_cache().unwrap();
                if cache_size == 0 {
                    assert_eq!(reads, 0);
                } else {
                    assert!(
                        reads > 0,
                        "{encoding:?}, {policy:?}: cache must be exercised"
                    );
                }
            }
        }
    }
}

fn differential_case(seed: u64) -> (Bytes, RowSelection, usize, usize) {
    use rand::{RngExt, SeedableRng, rngs::StdRng};

    let mut rng = StdRng::seed_from_u64(seed);
    let batch = mixed_batch();
    let text = StringArray::from_iter((0..24).map(|_| {
        (!rng.random_bool(0.2)).then(|| {
            let symbol = if rng.random_bool(0.5) { "界" } else { "a" };
            symbol.repeat(rng.random_range(1..=4))
        })
    }));
    let batch = RecordBatch::try_new(
        batch.schema(),
        vec![
            Arc::new(text),
            batch.column(1).clone(),
            batch.column(2).clone(),
        ],
    )
    .unwrap();
    let selected = BooleanArray::from(
        (0..24)
            .map(|i| i % 2 == 0 || rng.random_bool(0.6))
            .collect::<Vec<_>>(),
    );
    (
        write(&batch, ENCODINGS[seed as usize % ENCODINGS.len()]),
        RowSelection::from_filters(&[selected]),
        rng.random_range(0..4),
        rng.random_range(10..=20),
    )
}

fn differential_options() -> super::ArrowReaderOptions {
    use crate::arrow::schema::virtual_type::RowNumber;

    super::ArrowReaderOptions::new()
        .with_virtual_columns(vec![Arc::new(
            Field::new("row_number", DataType::Int64, false).with_extension_type(RowNumber),
        )])
        .unwrap()
}

/// A linear row-by-row oracle, independently of the splitter's offset search.
fn assert_differential(native: &[RecordBatch], split: &[RecordBatch]) -> usize {
    let mut expected_sizes = vec![];
    for batch in native {
        let offsets = [
            batch.column(0).as_string::<i32>().value_offsets(),
            batch.column(1).as_binary::<i32>().value_offsets(),
        ];
        let mut start = 0;
        for end in 1..=batch.num_rows() {
            if offsets
                .iter()
                .any(|offsets| offsets[end] - offsets[start] > 12)
            {
                assert!(end - 1 > start, "each single value must fit");
                expected_sizes.push(end - 1 - start);
                start = end - 1;
            }
        }
        expected_sizes.push(batch.num_rows() - start);
    }
    assert_eq!(
        split.iter().map(RecordBatch::num_rows).collect::<Vec<_>>(),
        expected_sizes
    );
    assert_capacity(split, 12);
    for batch in split {
        assert_eq!(batch.schema(), native[0].schema());
        assert_eq!(
            batch.column(2).as_primitive::<Int64Type>().values(),
            batch.column(3).as_primitive::<Int64Type>().values(),
        );
    }
    assert_eq!(flatten(split), flatten(native));
    usize::from(split.len() > native.len())
}

#[test]
fn test_capacity_staging_seeded_differential() {
    let mut split_cases = 0;
    for seed in 0..4 {
        let (data, selection, offset, limit) = differential_case(seed);
        for policy in POLICIES {
            let run = |capacity: Option<usize>| {
                ParquetRecordBatchReaderBuilder::try_new_with_options(
                    data.clone(),
                    differential_options(),
                )
                .unwrap()
                .with_batch_size(8)
                .with_row_selection_policy(policy)
                .with_row_selection(selection.clone())
                .with_offset(offset)
                .with_limit(limit)
                .with_byte_array_capacity(capacity)
                .build()
                .unwrap()
                .map(Result::unwrap)
                .collect::<Vec<_>>()
            };
            split_cases += assert_differential(&run(None), &run(Some(12)));
        }
    }
    assert_eq!(
        split_cases, 8,
        "every seeded case must exercise output splitting"
    );
}

#[cfg(feature = "async")]
#[tokio::test]
async fn test_capacity_staging_seeded_async_differential() {
    use crate::arrow::ParquetRecordBatchStreamBuilder;
    use futures::TryStreamExt;
    use std::io::Cursor;

    let mut split_cases = 0;
    for seed in 0..4 {
        let (data, selection, offset, limit) = differential_case(seed);
        for policy in POLICIES {
            for cache_size in [0, 4096] {
                let run = |capacity: Option<usize>| {
                    let (data, selection) = (data.clone(), selection.clone());
                    async move {
                        let builder = ParquetRecordBatchStreamBuilder::new_with_options(
                            Cursor::new(data),
                            differential_options(),
                        )
                        .await
                        .unwrap();
                        let projection = ProjectionMask::roots(builder.parquet_schema(), [0, 2]);
                        let mut seen = [false; 24];
                        let predicate =
                            ArrowPredicateFn::new(projection, move |batch: RecordBatch| {
                                if capacity.is_some() {
                                    assert_capacity(std::slice::from_ref(&batch), 12);
                                }
                                let ids = batch.column(1).as_primitive::<Int64Type>();
                                Ok(BooleanArray::from_iter(ids.values().iter().map(|&id| {
                                    assert!(
                                        !seen[id as usize],
                                        "predicate evaluated row {id} twice"
                                    );
                                    seen[id as usize] = true;
                                    Some(id % 3 != 0)
                                })))
                            });
                        builder
                            .with_batch_size(8)
                            .with_row_selection_policy(policy)
                            .with_row_selection(selection)
                            .with_max_predicate_cache_size(cache_size)
                            .with_row_filter(RowFilter::new(vec![Box::new(predicate)]))
                            .with_offset(offset)
                            .with_limit(limit)
                            .with_byte_array_capacity(capacity)
                            .build()
                            .unwrap()
                            .try_collect::<Vec<RecordBatch>>()
                            .await
                            .unwrap()
                    }
                };
                split_cases += assert_differential(&run(None).await, &run(Some(12)).await);
            }
        }
    }
    assert_eq!(
        split_cases, 16,
        "every cache/policy case must exercise splitting"
    );
}

#[cfg(feature = "async")]
#[tokio::test]
async fn test_capacity_staging_mixed_native_and_staged_cache_columns() {
    use super::metrics::ArrowReaderMetrics;
    use crate::arrow::ParquetRecordBatchStreamBuilder;
    use futures::TryStreamExt;
    use std::io::Cursor;

    let batch = RecordBatch::try_from_iter(vec![
        (
            "tiny",
            Arc::new(StringArray::from(vec!["x"; 24])) as ArrayRef,
        ),
        (
            "binary",
            Arc::new(BinaryArray::from_iter_values((0_u8..24).map(|i| [i; 9]))) as ArrayRef,
        ),
        (
            "id",
            Arc::new(Int64Array::from_iter_values(0..24)) as ArrayRef,
        ),
    ])
    .unwrap();
    let data = write(&batch, Encoding::PLAIN);
    let file = SerializedFileReader::new(data.clone()).unwrap();
    for group in file.metadata().row_groups() {
        assert!(group.column(0).unencoded_byte_array_data_bytes().unwrap() <= 20);
        assert!(group.column(1).unencoded_byte_array_data_bytes().unwrap() > 20);
    }
    for cache_size in [0, 4096] {
        let counts = Arc::new(Mutex::new(vec![0; 24]));
        let observed = counts.clone();
        let predicate = ArrowPredicateFn::new(ProjectionMask::all(), move |batch: RecordBatch| {
            assert_capacity(std::slice::from_ref(&batch), 20);
            let ids = batch.column(2).as_primitive::<Int64Type>();
            let mut counts = observed.lock().unwrap();
            Ok(BooleanArray::from_iter(ids.values().iter().map(|&id| {
                counts[id as usize] += 1;
                Some(id % 3 != 0)
            })))
        });
        let metrics = ArrowReaderMetrics::enabled();
        let batches: Vec<RecordBatch> =
            ParquetRecordBatchStreamBuilder::new(Cursor::new(data.clone()))
                .await
                .unwrap()
                .with_batch_size(8)
                .with_row_selection_policy(RowSelectionPolicy::Mask)
                .with_max_predicate_cache_size(cache_size)
                .with_metrics(metrics.clone())
                .with_row_filter(RowFilter::new(vec![Box::new(predicate)]))
                .with_byte_array_capacity(Some(20))
                .build()
                .unwrap()
                .try_collect()
                .await
                .unwrap();
        assert_predicate_output(&batches, &counts, &batch);
        assert_eq!(
            metrics.records_read_from_cache().unwrap() > 0,
            cache_size > 0
        );
    }
}
