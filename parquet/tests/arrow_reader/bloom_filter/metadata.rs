// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use super::{RecordingReader, column};
use bytes::Bytes;
use parquet::bloom_filter::Sbbf;
use parquet::file::metadata::{
    FileMetaData, ParquetMetaData, ParquetMetaDataReader, RowGroupMetaData,
};
use parquet::schema::{parser::parse_message_type, types::SchemaDescriptor};
use std::sync::Arc;

fn metadata(offset: Option<i64>, length: Option<i32>) -> ParquetMetaData {
    let schema = Arc::new(SchemaDescriptor::new(Arc::new(
        parse_message_type("message schema { REQUIRED BYTE_ARRAY value; }").unwrap(),
    )));
    let row_group = RowGroupMetaData::builder(schema.clone())
        .set_column_metadata(vec![column(offset, length)])
        .build()
        .unwrap();
    ParquetMetaData::new(
        FileMetaData::new(1, 0, None, None, schema, None),
        vec![row_group],
    )
}

#[test]
fn sync_metadata_bloom_absence_and_invalid_requests_do_not_read() {
    let reader = RecordingReader::default();
    let absent = metadata(None, Some(-1));
    assert!(
        ParquetMetaDataReader::read_column_bloom_filter(&reader, &absent, 0, 0)
            .unwrap()
            .is_none()
    );
    for (row_group, column) in [(1, 0), (0, 1), (usize::MAX, 0), (0, usize::MAX)] {
        assert!(
            ParquetMetaDataReader::read_column_bloom_filter(&reader, &absent, row_group, column)
                .is_err()
        );
    }
    for invalid in [metadata(Some(-1), None), metadata(Some(0), Some(0))] {
        assert!(ParquetMetaDataReader::read_column_bloom_filter(&reader, &invalid, 0, 0).is_err());
    }
    assert!(reader.requests().is_empty());
}

#[test]
fn sync_metadata_bloom_selects_requested_row_group() {
    let mut original = Sbbf::new_with_num_of_bytes(32);
    original.insert("present");
    let mut bytes = Vec::new();
    original.write(&mut bytes).unwrap();
    let bytes = Bytes::from(bytes);
    let absent = metadata(None, None);
    let present = metadata(Some(9), Some(bytes.len() as i32));
    let metadata = ParquetMetaData::new(
        absent.file_metadata().clone(),
        vec![absent.row_group(0).clone(), present.row_group(0).clone()],
    );
    let reader = RecordingReader::new([bytes.clone()]);
    assert!(
        ParquetMetaDataReader::read_column_bloom_filter(&reader, &metadata, 0, 0)
            .unwrap()
            .is_none()
    );
    assert!(reader.requests().is_empty());
    let filter = ParquetMetaDataReader::read_column_bloom_filter(&reader, &metadata, 1, 0)
        .unwrap()
        .unwrap();
    assert!(filter.check("present"));
    assert_eq!(reader.requests(), vec![9..9 + bytes.len() as u64]);
}

#[cfg(feature = "async")]
mod r#async {
    use super::super::encode;
    use super::*;
    use futures::future::{BoxFuture, ready};
    use parquet::arrow::async_reader::{AsyncFileReader, MetadataFetch};
    use parquet::errors::Result;
    use std::ops::Range;

    // A metadata-only byte source: no AsyncFileReader or metadata getter needed.
    struct FetchOnly<'a>(&'a RecordingReader);

    impl MetadataFetch for FetchOnly<'_> {
        fn fetch(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
            Box::pin(ready(self.0.read(range)))
        }
    }

    #[tokio::test]
    async fn footer_loading_does_not_read_bloom_filters() {
        let testdata = arrow::util::test_util::parquet_test_data();
        let data = Bytes::from(
            std::fs::read(format!(
                "{testdata}/data_index_bloom_encoding_stats.parquet"
            ))
            .unwrap(),
        );
        let footer_start = data.len() - 8;
        let tail = parquet::file::metadata::FooterTail::try_from(&data[footer_start..]).unwrap();
        let metadata_start = footer_start - tail.metadata_length();
        let reader = RecordingReader::new([
            data.slice(footer_start..),
            data.slice(metadata_start..footer_start),
        ]);
        let metadata = ParquetMetaDataReader::new()
            .load_and_finish(FetchOnly(&reader), data.len() as u64)
            .await
            .unwrap();
        assert_eq!(
            reader.requests(),
            vec![
                footer_start as u64..data.len() as u64,
                metadata_start as u64..footer_start as u64,
            ]
        );
        assert!(metadata.page_index().is_none());
        assert!(
            metadata
                .row_group(0)
                .column(0)
                .bloom_filter_offset()
                .is_some()
        );
    }

    #[tokio::test]
    async fn metadata_fetch_only_preserves_bloom_ranges() {
        let bytes = encode(32, 32);
        for with_length in [false, true] {
            let metadata = metadata(Some(7), with_length.then_some(bytes.len() as i32));
            let responses = if with_length {
                vec![bytes.clone()]
            } else {
                vec![bytes.slice(..20), bytes.slice(15..)]
            };
            let reader = RecordingReader::new(responses);
            let filter = ParquetMetaDataReader::read_column_bloom_filter_async(
                FetchOnly(&reader),
                &metadata,
                0,
                0,
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(filter.num_blocks(), 1);
            assert!(!filter.check("absent"));
            assert_eq!(
                reader.requests(),
                if with_length {
                    std::iter::once(7..7 + bytes.len() as u64).collect()
                } else {
                    vec![7..27, 22..7 + bytes.len() as u64]
                }
            );
        }
    }

    #[tokio::test]
    async fn async_metadata_bloom_absence_and_invalid_requests_do_not_read() {
        let reader = RecordingReader::default();
        let absent = metadata(None, Some(-1));
        assert!(
            ParquetMetaDataReader::read_column_bloom_filter_async(
                FetchOnly(&reader),
                &absent,
                0,
                0
            )
            .await
            .unwrap()
            .is_none()
        );
        for (row_group, column) in [(1, 0), (0, 1)] {
            assert!(
                ParquetMetaDataReader::read_column_bloom_filter_async(
                    FetchOnly(&reader),
                    &absent,
                    row_group,
                    column,
                )
                .await
                .is_err()
            );
        }
        let invalid = metadata(Some(-1), None);
        assert!(
            ParquetMetaDataReader::read_column_bloom_filter_async(
                FetchOnly(&reader),
                &invalid,
                0,
                0
            )
            .await
            .is_err()
        );
        assert!(reader.requests().is_empty());
    }

    #[tokio::test]
    async fn metadata_fetch_errors_and_invalid_responses_propagate() {
        let bytes = encode(32, 32);
        let metadata = metadata(Some(0), Some(bytes.len() as i32));
        for length in [bytes.len() - 1, bytes.len() + 1] {
            let mut response = bytes.to_vec();
            response.resize(length, 0);
            let reader = RecordingReader::new([response.into()]);
            let error = ParquetMetaDataReader::read_column_bloom_filter_async(
                FetchOnly(&reader),
                &metadata,
                0,
                0,
            )
            .await
            .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("Bloom filter read length mismatch")
            );
            assert_eq!(reader.requests().len(), 1);
        }
        let malformed = encode(0, 32);
        let reader = RecordingReader::new([malformed.clone()]);
        assert!(
            ParquetMetaDataReader::read_column_bloom_filter_async(
                FetchOnly(&reader),
                &super::metadata(Some(0), Some(malformed.len() as i32)),
                0,
                0,
            )
            .await
            .is_err()
        );
        let reader = RecordingReader::default();
        reader.fail_at(0);
        let error = ParquetMetaDataReader::read_column_bloom_filter_async(
            FetchOnly(&reader),
            &metadata,
            0,
            0,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("injected read failure"));
    }

    #[tokio::test]
    async fn metadata_bloom_accepts_borrowed_and_boxed_async_readers() {
        let bytes = encode(32, 32);
        let metadata = metadata(Some(0), Some(bytes.len() as i32));
        let mut reader = RecordingReader::new([bytes.clone(), bytes.clone()]);
        assert!(
            ParquetMetaDataReader::read_column_bloom_filter_async(&mut reader, &metadata, 0, 0)
                .await
                .unwrap()
                .is_some()
        );
        // The source remains available for another read; no reader handoff occurs.
        assert!(
            ParquetMetaDataReader::read_column_bloom_filter_async(&mut reader, &metadata, 0, 0)
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(reader.requests().len(), 2);
        let mut boxed: Box<dyn AsyncFileReader> = Box::new(RecordingReader::new([bytes]));
        assert!(
            ParquetMetaDataReader::read_column_bloom_filter_async(&mut boxed, &metadata, 0, 0)
                .await
                .unwrap()
                .is_some()
        );
    }
}
