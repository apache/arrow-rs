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

use super::{RecordingReader, column, encode};
use crate::io::TestReader;
use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray, StructArray};
use arrow_schema::{DataType, Field};
use bytes::Bytes;
use futures::TryStreamExt;
use parquet::{
    arrow::{ArrowWriter, ParquetRecordBatchStreamBuilder, async_reader::AsyncFileReader},
    bloom_filter::Sbbf,
    file::{
        properties::WriterProperties, reader::FileReader, serialized_reader::SerializedFileReader,
    },
};
use std::sync::Arc;

#[tokio::test]
async fn test_standalone_bloom_filter_after_nested_columns() {
    let nested = StructArray::from(vec![
        (
            Arc::new(Field::new("a", DataType::Int32, false)),
            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("b", DataType::Int32, false)),
            Arc::new(Int32Array::from(vec![3, 4])) as ArrayRef,
        ),
    ]);
    let batch = RecordBatch::try_from_iter([
        ("nested", Arc::new(nested) as ArrayRef),
        (
            "value",
            Arc::new(StringArray::from(vec!["present", "other"])) as ArrayRef,
        ),
    ])
    .unwrap();
    let props = WriterProperties::builder()
        .set_column_bloom_filter_enabled("value".into(), true)
        .build();
    let mut data = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut data, batch.schema(), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let data = Bytes::from(data);
    let metadata_reader = SerializedFileReader::new(data.clone()).unwrap();
    let row_group = metadata_reader.metadata().row_group(0);
    // The second Arrow field is the third physical Parquet leaf.
    assert_eq!(row_group.column(2).column_path().string(), "value");
    let mut reader = TestReader::new(data);
    let filter = Sbbf::read_from_column_chunk_async(row_group.column(2), &mut reader)
        .await
        .unwrap()
        .unwrap();
    assert!(filter.check("present"));
    assert!(filter.check("other"));
    assert_eq!(reader.requests().lock().unwrap().len(), 1);
}

#[tokio::test]
async fn test_get_row_group_column_bloom_filter_without_length() {
    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/data_index_bloom_encoding_stats.parquet");
    let data = Bytes::from(std::fs::read(path).unwrap());
    test_get_row_group_column_bloom_filter(data, false).await;
}

#[tokio::test]
async fn test_get_row_group_column_bloom_filter_with_length() {
    // convert to new parquet file with bloom_filter_length
    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/data_index_bloom_encoding_stats.parquet");
    let data = Bytes::from(std::fs::read(path).unwrap());
    let async_reader = TestReader::new(data.clone());
    let builder = ParquetRecordBatchStreamBuilder::new(async_reader)
        .await
        .unwrap();
    let schema = builder.schema().clone();
    let stream = builder.build().unwrap();
    let batches = stream.try_collect::<Vec<_>>().await.unwrap();

    let mut parquet_data = Vec::new();
    let props = WriterProperties::builder()
        .set_bloom_filter_enabled(true)
        .build();
    let mut writer = ArrowWriter::try_new(&mut parquet_data, schema, Some(props)).unwrap();
    for batch in batches {
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();

    // test the new parquet file
    test_get_row_group_column_bloom_filter(parquet_data.into(), true).await;
}

async fn test_get_row_group_column_bloom_filter(data: Bytes, with_length: bool) {
    let async_reader = TestReader::new(data.clone());
    let builder_requests = async_reader.requests();

    let mut builder = ParquetRecordBatchStreamBuilder::new(async_reader)
        .await
        .unwrap();

    let metadata = builder.metadata();
    assert_eq!(metadata.num_row_groups(), 1);
    let row_group = metadata.row_group(0);
    let column = row_group.column(0);
    assert_eq!(column.bloom_filter_length().is_some(), with_length);
    // Compare standalone reads with the existing sync reader and async builder.
    let mut reader = TestReader::new(data.clone());
    let standalone = Sbbf::read_from_column_chunk_async(column, &mut reader)
        .await
        .unwrap()
        .unwrap();
    let sync = Sbbf::read_from_column_chunk(column, &data)
        .unwrap()
        .unwrap();
    assert_eq!(standalone.num_blocks(), sync.num_blocks());
    assert!(standalone.check(&"Hello"));
    assert!(!standalone.check(&"Hello_Not_Exists"));

    let sbbf = builder
        .get_row_group_column_bloom_filter(0, 0)
        .await
        .unwrap()
        .unwrap();
    assert!(sbbf.check(&"Hello"));
    assert!(!sbbf.check(&"Hello_Not_Exists"));
    assert_eq!(sbbf.num_blocks(), standalone.num_blocks());
    // Delegation must preserve the same Bloom-only ranges, with or without a length.
    assert_eq!(
        *builder_requests.lock().unwrap(),
        *reader.requests().lock().unwrap()
    );
    assert_eq!(
        builder_requests.lock().unwrap().len(),
        if with_length { 1 } else { 2 }
    );
}

#[tokio::test]
async fn standalone_filter_reads_use_exact_ranges() {
    // Three blocks are valid even though the writer normally uses powers of two.
    let mut filter = Sbbf::new(&[0; 96]);
    filter.insert("present");
    let mut serialized = Vec::new();
    filter.write(&mut serialized).unwrap();
    let bytes = Bytes::from(serialized);
    let header_length = (bytes.len() - filter.num_blocks() * 32) as u64;
    let offset = 13;

    for length in [Some(bytes.len() as i32), None] {
        let metadata = column(Some(offset as i64), length);
        let responses = if length.is_some() {
            vec![bytes.clone()]
        } else {
            vec![bytes.slice(..20), bytes.slice(header_length as usize..)]
        };
        let mut reader = RecordingReader::new(responses);
        let borrowed: &mut dyn AsyncFileReader = &mut reader;
        let loaded = Sbbf::read_from_column_chunk_async(&metadata, borrowed)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.num_blocks(), 3);
        assert!(loaded.check("present"));
        // The legacy prefix overlaps the bitset; resume at the decoded header end.
        let expected = if length.is_some() {
            std::iter::once(offset..offset + bytes.len() as u64).collect()
        } else {
            vec![
                offset..offset + 20,
                offset + header_length..offset + bytes.len() as u64,
            ]
        };
        assert_eq!(reader.requests(), expected);
    }
}

#[tokio::test]
async fn missing_and_invalid_locations_do_not_read() {
    let mut reader = RecordingReader::default();
    // An orphan length does not advertise a filter without an offset.
    assert!(
        Sbbf::read_from_column_chunk_async(&column(None, Some(-1)), &mut reader)
            .await
            .unwrap()
            .is_none()
    );
    // Reject invalid signed offsets and nonpositive lengths before issuing any read.
    for metadata in [
        column(Some(-1), None),
        column(Some(0), Some(-1)),
        column(Some(0), Some(0)),
    ] {
        assert!(
            Sbbf::read_from_column_chunk_async(&metadata, &mut reader)
                .await
                .is_err()
        );
    }
    assert!(reader.requests().is_empty());
}

#[tokio::test]
async fn invalid_headers_and_bitsets_remain_errors() {
    let mut canonical = Vec::new();
    Sbbf::new(&[0; 32]).write(&mut canonical).unwrap();
    assert_eq!(encode(32, 32).as_ref(), canonical.as_slice());
    // Reject invalid bitset sizes, then both undersized and oversized payloads.
    for (declared, actual) in [(-1, 32), (0, 0), (33, 33), (64, 32), (32, 64)] {
        let bytes = encode(declared, actual);
        let mut reader = RecordingReader::new([bytes.clone()]);
        assert!(
            Sbbf::read_from_column_chunk_async(
                &column(Some(0), Some(bytes.len() as i32)),
                &mut reader,
            )
            .await
            .is_err(),
            "declared {declared}, actual {actual}"
        );
        assert_eq!(reader.requests().len(), 1);
    }
    // Decode failures must remain errors, not be mistaken for an absent filter.
    let mut unsupported = encode(32, 32).to_vec();
    unsupported[3] = 0x2c; // Unknown algorithm union variant.
    for bytes in [Bytes::from_static(&[0]), Bytes::from(unsupported)] {
        let mut reader = RecordingReader::new([bytes.clone()]);
        assert!(
            Sbbf::read_from_column_chunk_async(
                &column(Some(0), Some(bytes.len() as i32)),
                &mut reader,
            )
            .await
            .is_err()
        );
    }
}

#[tokio::test]
async fn exact_responses_and_transport_errors_are_required() {
    let bytes = encode(32, 32);
    // With a known total length, the entire response must match the requested range.
    for response_length in [bytes.len() - 1, bytes.len() + 1] {
        let mut response = bytes.to_vec();
        response.resize(response_length, 0);
        let mut reader = RecordingReader::new([response.into()]);
        assert!(
            Sbbf::read_from_column_chunk_async(
                &column(Some(0), Some(bytes.len() as i32)),
                &mut reader,
            )
            .await
            .is_err()
        );
    }
    // The header probe must also return exactly the requested bytes.
    for prefix_length in [19, 21] {
        let mut reader = RecordingReader::new([bytes.slice(..prefix_length)]);
        assert!(
            Sbbf::read_from_column_chunk_async(&column(Some(0), None), &mut reader)
                .await
                .is_err()
        );
        assert_eq!(reader.requests().len(), 1);
    }
    // Without a total length, validate the separate bitset response against the header.
    for bitset_length in [31, 33] {
        let mut reader =
            RecordingReader::new([bytes.slice(..20), Bytes::from(vec![0; bitset_length])]);
        assert!(
            Sbbf::read_from_column_chunk_async(&column(Some(0), None), &mut reader)
                .await
                .is_err()
        );
        assert_eq!(reader.requests().len(), 2);
    }
    // Propagate transport failures from either the header or bitset read.
    for failure_at in [0, 1] {
        let mut reader = RecordingReader::new([bytes.slice(..20)]);
        reader.fail_at(failure_at);
        assert!(
            Sbbf::read_from_column_chunk_async(&column(Some(0), None), &mut reader)
                .await
                .is_err()
        );
        assert_eq!(reader.requests().len(), failure_at + 1);
    }
}

#[tokio::test]
async fn extended_headers_require_an_advertised_length() {
    let mut serialized = Vec::new();
    Sbbf::new(&[0; 32]).write(&mut serialized).unwrap();
    let canonical = Bytes::from(serialized);
    let header_length = canonical.len() - 32;
    let mut bytes = canonical[..header_length - 1].to_vec();
    // Unknown binary field extends the header beyond the legacy prefix.
    bytes.extend_from_slice(&[0x18, 24]);
    bytes.extend_from_slice(&[0; 24]);
    bytes.push(0);
    bytes.extend_from_slice(&canonical[header_length..]);
    let bytes = Bytes::from(bytes);
    assert_eq!(Sbbf::from_bytes(&bytes).unwrap().num_blocks(), 1);
    let sync = RecordingReader::new([bytes.clone()]);
    assert_eq!(
        Sbbf::read_from_column_chunk(&column(Some(0), Some(bytes.len() as i32)), &sync)
            .unwrap()
            .unwrap()
            .num_blocks(),
        1
    );
    // An advertised length lets the parser skip the complete unknown field.
    let mut known = RecordingReader::new([bytes.clone()]);
    assert!(
        Sbbf::read_from_column_chunk_async(&column(Some(0), Some(bytes.len() as i32)), &mut known,)
            .await
            .unwrap()
            .is_some()
    );
    // The legacy 20-byte prefix cannot decode this header; do not return a partial filter.
    let mut legacy = RecordingReader::new([bytes.slice(..20)]);
    assert!(
        Sbbf::read_from_column_chunk_async(&column(Some(0), None), &mut legacy)
            .await
            .is_err()
    );
    assert_eq!(legacy.requests().len(), 1);
    assert_eq!(legacy.requests()[0], 0..20);
}
