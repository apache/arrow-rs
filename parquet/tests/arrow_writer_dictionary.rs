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

//! Integration tests for writing DictionaryArray columns through `ArrowWriter`
//! and reading them back via `ParquetRecordBatchReader`. These tests exercise
//! the full public API write-read roundtrip.

use arrow_array::builder::StringDictionaryBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::Int32Type;
use arrow_array::{Array, ArrayAccessor, DictionaryArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::sync::Arc;

/// Helper: write a single RecordBatch to an in-memory Parquet buffer.
fn write_to_parquet(batch: &RecordBatch, props: Option<WriterProperties>) -> Bytes {
    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), props).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
    Bytes::from(buf)
}

/// Helper: read all RecordBatches from an in-memory Parquet buffer.
fn read_from_parquet(data: Bytes) -> Vec<RecordBatch> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(data)
        .unwrap()
        .build()
        .unwrap();
    reader.collect::<Result<Vec<_>, _>>().unwrap()
}

/// Helper: extract string values from a column, handling both StringArray and
/// DictionaryArray<Int32, Utf8> transparently.
fn extract_strings(col: &dyn Array) -> Vec<Option<String>> {
    if let Some(sa) = col.as_any().downcast_ref::<StringArray>() {
        (0..sa.len())
            .map(|i| {
                if sa.is_null(i) {
                    None
                } else {
                    Some(sa.value(i).to_string())
                }
            })
            .collect()
    } else {
        let da = col.as_dictionary::<Int32Type>();
        let typed = da.downcast_dict::<StringArray>().unwrap();
        (0..da.len())
            .map(|i| {
                if da.is_null(i) {
                    None
                } else {
                    // TypedDictionaryArray::value() already resolves keys
                    Some(typed.value(i).to_string())
                }
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Test 1: Basic DictionaryArray roundtrip
// ---------------------------------------------------------------------------
#[test]
fn dictionary_roundtrip_low_cardinality() {
    // 15 unique values across 4096 rows — triggers the remap optimization path
    let categories = [
        "DELIVERED", "SHIPPED", "PENDING", "CANCELLED", "RETURNED",
        "PROCESSING", "ON_HOLD", "REFUNDED", "BACKORDERED", "IN_TRANSIT",
        "CONFIRMED", "DISPATCHED", "FAILED", "COMPLETED", "UNKNOWN",
    ];
    let mut builder = StringDictionaryBuilder::<Int32Type>::new();
    let mut expected = Vec::with_capacity(4096);
    for i in 0..4096 {
        let val = categories[i % categories.len()];
        builder.append_value(val);
        expected.push(Some(val.to_string()));
    }
    let dict = builder.finish();

    let schema = Arc::new(Schema::new(vec![Field::new(
        "status",
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(dict)]).unwrap();

    let data = write_to_parquet(&batch, None);
    let batches = read_from_parquet(data);

    let mut actual = Vec::new();
    for b in &batches {
        actual.extend(extract_strings(b.column(0)));
    }
    assert_eq!(actual, expected);
}

// ---------------------------------------------------------------------------
// Test 2: DictionaryArray + plain StringArray mix in the same RecordBatch
// ---------------------------------------------------------------------------
#[test]
fn dictionary_and_plain_columns_roundtrip() {
    // Column 1: DictionaryArray (low cardinality)
    let dict: DictionaryArray<Int32Type> = vec!["US", "CA", "MX", "US", "CA"]
        .into_iter()
        .collect();

    // Column 2: plain StringArray
    let plain = StringArray::from(vec!["Alice", "Bob", "Carol", "Dave", "Eve"]);

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "country",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(dict), Arc::new(plain)]).unwrap();

    let data = write_to_parquet(&batch, None);
    let batches = read_from_parquet(data);

    // Verify both columns
    let mut countries = Vec::new();
    let mut names = Vec::new();
    for b in &batches {
        countries.extend(extract_strings(b.column(0)));
        names.extend(extract_strings(b.column(1)));
    }
    assert_eq!(
        countries,
        vec![
            Some("US".into()),
            Some("CA".into()),
            Some("MX".into()),
            Some("US".into()),
            Some("CA".into()),
        ]
    );
    assert_eq!(
        names,
        vec![
            Some("Alice".into()),
            Some("Bob".into()),
            Some("Carol".into()),
            Some("Dave".into()),
            Some("Eve".into()),
        ]
    );
}

// ---------------------------------------------------------------------------
// Test 3: DictionaryArray with correct statistics (min/max)
// ---------------------------------------------------------------------------
#[test]
fn dictionary_statistics_match_plain() {
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;

    let values = vec!["cherry", "apple", "banana", "apple", "cherry"];

    // Write as plain StringArray
    let plain = StringArray::from(values.clone());
    let plain_schema =
        Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, false)]));
    let plain_batch =
        RecordBatch::try_new(plain_schema, vec![Arc::new(plain)]).unwrap();
    let plain_data = write_to_parquet(&plain_batch, None);

    // Write as DictionaryArray
    let dict: DictionaryArray<Int32Type> = values.into_iter().collect();
    let dict_schema = Arc::new(Schema::new(vec![Field::new(
        "col",
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        false,
    )]));
    let dict_batch =
        RecordBatch::try_new(dict_schema, vec![Arc::new(dict)]).unwrap();
    let dict_data = write_to_parquet(&dict_batch, None);

    // Compare statistics from both files
    let plain_reader = SerializedFileReader::new(plain_data).unwrap();
    let dict_reader = SerializedFileReader::new(dict_data).unwrap();

    let plain_stats = plain_reader
        .metadata()
        .row_group(0)
        .column(0)
        .statistics()
        .unwrap()
        .clone();
    let dict_stats = dict_reader
        .metadata()
        .row_group(0)
        .column(0)
        .statistics()
        .unwrap()
        .clone();

    assert_eq!(
        format!("{plain_stats:?}"),
        format!("{dict_stats:?}"),
        "Statistics from DictionaryArray path must match plain StringArray path"
    );
}

// ---------------------------------------------------------------------------
// Test 4: Multi-row-group with DictionaryArray
// ---------------------------------------------------------------------------
#[test]
fn dictionary_multi_row_group_roundtrip() {
    let batch1_values: DictionaryArray<Int32Type> =
        vec!["alpha", "beta", "gamma", "alpha"].into_iter().collect();
    let batch2_values: DictionaryArray<Int32Type> =
        vec!["delta", "epsilon", "delta", "gamma"]
            .into_iter()
            .collect();

    let schema = Arc::new(Schema::new(vec![Field::new(
        "col",
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        false,
    )]));

    let batch1 =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(batch1_values)]).unwrap();
    let batch2 =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(batch2_values)]).unwrap();

    // Force each batch into its own row group
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(4))
        .build();

    let mut buf = Vec::new();
    let mut writer =
        ArrowWriter::try_new(&mut buf, schema, Some(props)).unwrap();
    writer.write(&batch1).unwrap();
    writer.write(&batch2).unwrap();
    writer.close().unwrap();

    let data = Bytes::from(buf);

    // Verify row group count
    let reader_builder = ParquetRecordBatchReaderBuilder::try_new(data.clone()).unwrap();
    assert_eq!(
        reader_builder.metadata().num_row_groups(),
        2,
        "Expected 2 row groups"
    );

    // Verify all data
    let batches = read_from_parquet(data);
    let mut all_values = Vec::new();
    for b in &batches {
        all_values.extend(extract_strings(b.column(0)));
    }
    let expected: Vec<Option<String>> =
        vec!["alpha", "beta", "gamma", "alpha", "delta", "epsilon", "delta", "gamma"]
            .into_iter()
            .map(|s| Some(s.to_string()))
            .collect();
    assert_eq!(all_values, expected);
}

// ---------------------------------------------------------------------------
// Test 5: DictionaryArray with null values roundtrip
// ---------------------------------------------------------------------------
#[test]
fn dictionary_with_nulls_roundtrip() {
    let mut builder = StringDictionaryBuilder::<Int32Type>::new();
    builder.append_value("red");
    builder.append_null();
    builder.append_value("blue");
    builder.append_null();
    builder.append_value("red");
    builder.append_value("green");
    let dict = builder.finish();

    let schema = Arc::new(Schema::new(vec![Field::new(
        "color",
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        true,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(dict)]).unwrap();

    let data = write_to_parquet(&batch, None);
    let batches = read_from_parquet(data);

    let mut actual = Vec::new();
    for b in &batches {
        actual.extend(extract_strings(b.column(0)));
    }
    assert_eq!(
        actual,
        vec![
            Some("red".into()),
            None,
            Some("blue".into()),
            None,
            Some("red".into()),
            Some("green".into()),
        ]
    );
}
