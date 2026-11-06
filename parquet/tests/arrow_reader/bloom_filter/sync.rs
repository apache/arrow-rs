// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use super::{RecordingReader, column, encode};
use std::fs::File;

use bytes::Bytes;
use parquet::{
    arrow::{
        ArrowWriter,
        arrow_reader::{ArrowReaderMetadata, ParquetRecordBatchReaderBuilder},
    },
    bloom_filter::Sbbf,
    file::{metadata::ParquetMetaDataReader, properties::WriterProperties},
};

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_get_row_group_column_bloom_filter_with_length() {
    // convert to new parquet file with bloom_filter_length
    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/data_index_bloom_encoding_stats.parquet");
    let file = File::open(path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let schema = builder.schema().clone();
    let reader = builder.build().unwrap();

    let mut parquet_data = Vec::new();
    let props = WriterProperties::builder()
        .set_bloom_filter_enabled(true)
        .build();
    let mut writer = ArrowWriter::try_new(&mut parquet_data, schema, Some(props)).unwrap();
    for batch in reader {
        let batch = batch.unwrap();
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();

    // test the new parquet file
    test_get_row_group_column_bloom_filter(parquet_data.into(), true);
}

#[test]
fn test_get_row_group_column_bloom_filter_without_length() {
    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/data_index_bloom_encoding_stats.parquet");
    let data = Bytes::from(std::fs::read(path).unwrap());
    test_get_row_group_column_bloom_filter(data, false);
}

fn test_get_row_group_column_bloom_filter(data: Bytes, with_length: bool) {
    let builder = ParquetRecordBatchReaderBuilder::try_new(data.clone()).unwrap();

    let metadata = builder.metadata();
    assert_eq!(metadata.num_row_groups(), 1);
    let row_group = metadata.row_group(0);
    let column = row_group.column(0);
    assert_eq!(column.bloom_filter_length().is_some(), with_length);
    let standalone = Sbbf::read_from_column_chunk(column, &data)
        .unwrap()
        .unwrap();
    assert!(standalone.check("Hello"));
    let footer = ParquetMetaDataReader::new()
        .parse_and_finish(&data)
        .unwrap();
    let requested = ParquetMetaDataReader::read_column_bloom_filter(&data, &footer, 0, 0)
        .unwrap()
        .unwrap();
    assert!(requested.check("Hello"));
    assert!(!requested.check("Hello_Not_Exists"));
    assert_eq!(requested.num_blocks(), standalone.num_blocks());

    let sbbf = builder
        .get_row_group_column_bloom_filter(0, 0)
        .unwrap()
        .unwrap();
    assert!(sbbf.check(&"Hello"));
    assert!(!sbbf.check(&"Hello_Not_Exists"));
    assert_eq!(sbbf.num_blocks(), standalone.num_blocks());
}

#[test]
fn sync_builder_rejects_malformed_filter() {
    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/data_index_bloom_encoding_stats.parquet");
    let data = Bytes::from(std::fs::read(path).unwrap());
    let metadata = ArrowReaderMetadata::load(&data, Default::default()).unwrap();
    // Supply a zero-sized filter in place of the file's legacy header probe.
    let reader = RecordingReader::new([encode(0, 32).slice(..20), Bytes::new()]);
    let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(reader, metadata);
    assert!(builder.get_row_group_column_bloom_filter(0, 0).is_err());
}

#[test]
fn missing_and_invalid_locations_do_not_read() {
    let reader = RecordingReader::default();
    assert!(
        Sbbf::read_from_column_chunk(&column(None, Some(-1)), &reader)
            .unwrap()
            .is_none()
    );
    for metadata in [
        column(Some(-1), None),
        column(Some(0), Some(-1)),
        column(Some(0), Some(0)),
    ] {
        assert!(Sbbf::read_from_column_chunk(&metadata, &reader).is_err());
    }
    assert!(reader.requests().is_empty());
}

#[test]
fn exact_responses_and_transport_errors_are_required() {
    let bytes = encode(32, 32);
    for response_length in [bytes.len() - 1, bytes.len() + 1] {
        let mut response = bytes.to_vec();
        response.resize(response_length, 0);
        let reader = RecordingReader::new([response.into()]);
        assert!(
            Sbbf::read_from_column_chunk(&column(Some(0), Some(bytes.len() as i32)), &reader,)
                .is_err()
        );
    }
    // Validate each of the two responses independently on the legacy path.
    for prefix_length in [19, 21] {
        let reader = RecordingReader::new([bytes.slice(..prefix_length)]);
        assert!(Sbbf::read_from_column_chunk(&column(Some(0), None), &reader).is_err());
        assert_eq!(reader.requests().len(), 1);
    }
    for bitset_length in [31, 33] {
        let reader = RecordingReader::new([bytes.slice(..20), Bytes::from(vec![0; bitset_length])]);
        assert!(Sbbf::read_from_column_chunk(&column(Some(0), None), &reader).is_err());
        assert_eq!(reader.requests().len(), 2);
    }
    for failure_at in [0, 1] {
        let reader = RecordingReader::new([bytes.slice(..20)]);
        reader.fail_at(failure_at);
        let error = Sbbf::read_from_column_chunk(&column(Some(0), None), &reader).unwrap_err();
        assert!(error.to_string().contains("injected read failure"));
        assert_eq!(reader.requests().len(), failure_at + 1);
    }
}
