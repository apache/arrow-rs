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
use std::fs::File;

use bytes::Bytes;
use parquet::{
    arrow::{
        ArrowWriter,
        arrow_reader::{ArrowReaderMetadata, ParquetRecordBatchReaderBuilder},
    },
    bloom_filter::Sbbf,
    file::properties::WriterProperties,
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
fn standalone_filter_reads_use_exact_ranges() {
    let mut filter = Sbbf::new(&[0; 96]);
    filter.insert("present");
    let mut serialized = Vec::new();
    filter.write(&mut serialized).unwrap();
    let bytes = Bytes::from(serialized);
    let header_length = (bytes.len() - 96) as u64;
    let offset = 13;
    assert!(Sbbf::from_bytes(&bytes).unwrap().check("present"));

    for length in [Some(bytes.len() as i32), None] {
        let responses = if length.is_some() {
            vec![bytes.clone()]
        } else {
            vec![bytes.slice(..20), bytes.slice(header_length as usize..)]
        };
        let reader = RecordingReader::new(responses);
        let loaded = Sbbf::read_from_column_chunk(&column(Some(offset as i64), length), &reader)
            .unwrap()
            .unwrap();
        assert_eq!(loaded.num_blocks(), 3);
        assert!(loaded.check("present"));
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
fn invalid_headers_and_bitsets_remain_errors() {
    // Deserialization and sync reads must enforce the same header/payload contract.
    for (declared, actual) in [(-1, 32), (0, 0), (33, 33), (64, 32), (32, 64)] {
        let bytes = encode(declared, actual);
        let reader = RecordingReader::new([bytes.clone()]);
        let error =
            Sbbf::read_from_column_chunk(&column(Some(0), Some(bytes.len() as i32)), &reader)
                .unwrap_err();
        assert_eq!(
            error.to_string(),
            Sbbf::from_bytes(&bytes).unwrap_err().to_string()
        );
        assert_eq!(reader.requests().len(), 1);
    }
    let mut unsupported = encode(32, 32).to_vec();
    unsupported[3] = 0x2c; // Unknown algorithm union variant.
    for bytes in [Bytes::from_static(&[0]), Bytes::from(unsupported)] {
        let reader = RecordingReader::new([bytes.clone()]);
        assert!(
            Sbbf::read_from_column_chunk(&column(Some(0), Some(bytes.len() as i32)), &reader,)
                .is_err()
        );
        assert!(Sbbf::from_bytes(&bytes).is_err());
    }
    // Without an advertised length, reject invalid sizes before requesting the bitset.
    for declared in [-1, 0, 33] {
        let bytes = encode(declared, 32);
        let reader = RecordingReader::new([bytes.slice(..20)]);
        assert!(Sbbf::read_from_column_chunk(&column(Some(0), None), &reader).is_err());
        assert_eq!(reader.requests(), vec![0..20]);
    }
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
