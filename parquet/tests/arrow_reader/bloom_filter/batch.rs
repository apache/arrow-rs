// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use super::{RecordingReader, encode};
use arrow_array::{ArrayRef, Int32Array, RecordBatch};
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::{ArrowWriter, ProjectionMask};
use parquet::bloom_filter::Sbbf;
use parquet::errors::{ParquetError, Result};
use parquet::file::metadata::page_index::RowGroupPageIndex;
use parquet::file::metadata::{
    ColumnChunkMask, ColumnChunkMetaData, FileMetaData, ParquetMetaData, ParquetMetaDataReader,
    RowGroupMetaData, SbbfSet,
};
use parquet::file::properties::{ReaderProperties, WriterProperties};
use parquet::file::reader::{FileReader, RowGroupReader};
use parquet::file::serialized_reader::{
    ReadOptionsBuilder, SerializedFileReader, SerializedRowGroupReader,
};
use parquet::schema::{parser::parse_message_type, types::SchemaDescriptor};
use std::ops::Range;
use std::sync::Arc;

// Physical leaf    0: nested.a   1: nested.b   2: value
// Row group 0      filter(0)     absent        filter(2)
// Row group 1      absent        filter(4)     absent
//
// The non-square grid and holes expose swapped or compacted coordinates. Nested
// leaves distinguish physical columns from Arrow fields; 257-byte gaps expose
// spanning reads. The mixed-length variant omits the length of rg0's value filter.
fn fixture(unknown_length: bool) -> (ParquetMetaData, Bytes, Vec<Range<u64>>) {
    fixture_with_legacy(if unknown_length { &[(0, 2)] } else { &[] })
}

fn fixture_with_legacy(legacy: &[(usize, usize)]) -> (ParquetMetaData, Bytes, Vec<Range<u64>>) {
    let schema = Arc::new(SchemaDescriptor::new(Arc::new(
        parse_message_type(
            "message schema { REQUIRED GROUP nested { REQUIRED INT32 a; REQUIRED INT32 b; } REQUIRED INT32 value; }",
        )
        .unwrap(),
    )));
    let mut bytes = vec![0; 13];
    let mut ranges = Vec::new();
    let mut rows = Vec::new();
    for row in 0..2 {
        let mut columns = Vec::new();
        for col in 0..3 {
            let mut builder = ColumnChunkMetaData::builder(schema.column(col));
            if [(0, 0), (0, 2), (1, 1)].contains(&(row, col)) {
                let mut filter = Sbbf::new_with_num_of_bytes(32);
                filter.insert(&((row * 3 + col) as i32));
                let start = bytes.len();
                filter.write(&mut bytes).unwrap();
                ranges.push(start as u64..bytes.len() as u64);
                builder = builder.set_bloom_filter_offset(Some(start as i64));
                if !legacy.contains(&(row, col)) {
                    builder = builder.set_bloom_filter_length(Some((bytes.len() - start) as i32));
                }
                bytes.resize(bytes.len() + 257, 0);
            }
            columns.push(builder.build().unwrap());
        }
        rows.push(
            RowGroupMetaData::builder(schema.clone())
                .set_column_metadata(columns)
                .build()
                .unwrap(),
        );
    }
    (
        ParquetMetaData::new(FileMetaData::new(1, 0, None, None, schema, None), rows),
        Bytes::from(bytes),
        ranges,
    )
}

fn describe_read(metadata: &ParquetMetaData, filters: &[Range<u64>], read: &Range<u64>) -> String {
    for ((row, column), region) in [(0, 0), (0, 2), (1, 1)].into_iter().zip(filters) {
        let kind = if read == region {
            "complete filter"
        } else if *read == (region.start..region.start + 20) {
            "legacy header probe"
        } else if *read == (region.end - 32..region.end) {
            "bitset"
        } else {
            continue;
        };
        let path = metadata
            .row_group(row)
            .column(column)
            .column_path()
            .string();
        return format!("rg{row} {path}: {kind} [{}..{})", read.start, read.end);
    }
    format!("UNKNOWN [{}..{})", read.start, read.end)
}

fn describe_filters(filters: &SbbfSet) -> Vec<String> {
    let mut lines = Vec::new();
    for (row, values) in [[0_i32, 1, 2], [3, 4, 5]].into_iter().enumerate() {
        let mut columns = Vec::new();
        for (column, (path, value)) in ["nested.a", "nested.b", "value"]
            .into_iter()
            .zip(values)
            .enumerate()
        {
            let value = match filters.get(row, column).unwrap() {
                Some(filter) => {
                    assert!(filter.check(&value), "wrong filter at ({row}, {column})");
                    format!("filter({value})")
                }
                None => "None".to_owned(),
            };
            columns.push(format!("{path}={value}"));
        }
        lines.push(format!("rg{row}: {}", columns.join(", ")));
    }
    lines
}

fn sync_transcript(
    metadata: &ParquetMetaData,
    regions: &[Range<u64>],
    reader: &RecordingReader,
    filters: &SbbfSet,
) -> String {
    let mut lines: Vec<_> = reader
        .requests()
        .iter()
        .map(|read| format!("Read: {}", describe_read(metadata, regions, read)))
        .collect();
    lines.extend(describe_filters(filters));
    lines.join("\n")
}

fn assert_error<T>(result: Result<T>, expected: &str) {
    let error = result.err().expect("operation should fail");
    assert!(
        matches!(error, ParquetError::General(ref message) if message.contains(expected)),
        "expected {expected:?}, got {error}"
    );
}

// Two row groups with a Bloom filter on the predicate column, but not the output column.
fn written_file() -> Bytes {
    let batches: Vec<_> = [10, 30]
        .into_iter()
        .map(|start| {
            RecordBatch::try_from_iter([
                (
                    "value",
                    Arc::new(Int32Array::from(vec![start, start + 1])) as ArrayRef,
                ),
                (
                    "output",
                    Arc::new(Int32Array::from(vec![start + 100, start + 101])) as ArrayRef,
                ),
            ])
            .unwrap()
        })
        .collect();
    let properties = WriterProperties::builder()
        .set_column_bloom_filter_enabled("value".into(), true)
        .build();
    let mut writer =
        ArrowWriter::try_new(Vec::new(), batches[0].schema(), Some(properties)).unwrap();
    for batch in batches {
        writer.write(&batch).unwrap();
        writer.flush().unwrap();
    }
    Bytes::from(writer.into_inner().unwrap())
}

#[test]
fn serialized_row_group_bloom_loading_is_sparse_and_selective() {
    let (metadata, bytes, _) = fixture(true);
    let row_group = metadata.row_group(0);
    // Column 0 has an invalid filter location but must not be examined when unselected.
    let mut columns = row_group.columns().to_vec();
    columns[0] = ColumnChunkMetaData::builder(row_group.schema_descr().column(0))
        .set_bloom_filter_offset(Some(-1))
        .build()
        .unwrap();
    let row_group = RowGroupMetaData::builder(row_group.schema_descr_ptr())
        .set_column_metadata(columns)
        .set_ordinal(7)
        .build()
        .unwrap();
    let props = ReaderProperties::builder()
        .set_read_bloom_filter(true)
        .set_read_bloom_filter_columns([2, 1, 2, 99, usize::MAX])
        .build();
    let reader = SerializedRowGroupReader::new(
        Arc::new(bytes.clone()),
        &row_group,
        RowGroupPageIndex::new(7, None),
        Arc::new(props),
    )
    .unwrap();
    assert!(reader.get_column_bloom_filter(0).is_none());
    assert!(reader.get_column_bloom_filter(1).is_none());
    assert!(reader.get_column_bloom_filter(2).unwrap().check(&2_i32));

    // Setter equivalence is covered by the properties tests; test the default no-I/O path here.
    let no_reads = Arc::new(RecordingReader::from_bytes(Bytes::new()));
    let disabled = SerializedRowGroupReader::new(
        Arc::clone(&no_reads),
        &row_group,
        RowGroupPageIndex::new(7, None),
        Arc::new(ReaderProperties::builder().build()),
    )
    .unwrap();
    for column in 0..3 {
        assert!(disabled.get_column_bloom_filter(column).is_none());
    }
    assert!(no_reads.requests().is_empty());
    assert_error(
        SerializedRowGroupReader::new(
            no_reads,
            &row_group,
            RowGroupPageIndex::new(7, None),
            Arc::new(
                ReaderProperties::builder()
                    .set_read_bloom_filter(true)
                    .build(),
            ),
        ),
        "Bloom filter offset is invalid",
    );
}

#[test]
fn serialized_file_bloom_selection_survives_row_group_filtering() {
    let options = ReadOptionsBuilder::new()
        .with_predicate(Box::new(|_, index| index == 1))
        .with_reader_properties(
            ReaderProperties::builder()
                .set_read_bloom_filter_columns([0])
                .build(),
        )
        .build();
    let reader = SerializedFileReader::new_with_options(written_file(), options).unwrap();
    assert_eq!(reader.num_row_groups(), 1);
    let row_group = reader.get_row_group(0).unwrap();
    assert!(row_group.get_column_bloom_filter(0).unwrap().check(&30_i32));
    assert!(row_group.get_column_bloom_filter(1).is_none());
}

#[test]
fn sync_builder_bloom_mask_is_explicit_and_builder_remains_usable() {
    let builder = ParquetRecordBatchReaderBuilder::try_new(written_file()).unwrap();
    let metadata = builder.metadata().clone();
    let projection = ProjectionMask::leaves(builder.parquet_schema(), [1]);
    let builder = builder.with_projection(projection).with_row_groups(vec![0]);
    let mask = ColumnChunkMask::row_groups_and_columns([1], [0]);
    let filters = builder.get_bloom_filters(&mask).unwrap();
    assert!(filters.get(1, 0).unwrap().unwrap().check(&30_i32));
    assert!(filters.get(0, 0).unwrap().is_none());
    assert!(filters.get(1, 1).unwrap().is_none());
    assert!(Arc::ptr_eq(builder.metadata(), &metadata));
    // Fetching a different row group's filter must not change the data selection.
    let batches = builder
        .with_batch_size(1)
        .build()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_projected_output(&batches);
    assert!(filters.get(1, 0).unwrap().unwrap().check(&31_i32));
}

fn assert_projected_output(batches: &[RecordBatch]) {
    let values: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
            assert_eq!(batch.num_columns(), 1);
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect();
    assert_eq!(values, vec![110, 111]);
}

fn responses(bytes: &Bytes, ranges: &[Range<u64>]) -> Vec<Bytes> {
    ranges
        .iter()
        .map(|range| bytes.slice(range.start as usize..range.end as usize))
        .collect()
}

#[test]
fn selection_keeps_file_coordinates_and_distinguishes_missing_from_invalid() {
    let (metadata, bytes, regions) = fixture(false);
    let filters =
        ParquetMetaDataReader::read_bloom_filters(&bytes, &metadata, &ColumnChunkMask::default())
            .unwrap();
    insta::assert_snapshot!(describe_filters(&filters).join("\n"), @r"
    rg0: nested.a=filter(0), nested.b=None, value=filter(2)
    rg1: nested.a=None, nested.b=filter(4), value=None
    ");
    for (row, column) in [(2, 0), (0, 3), (usize::MAX, usize::MAX)] {
        assert_error(filters.get(row, column), "coordinate out of bounds");
    }

    // Only rg1's nested.b filter is selected. Selection does not renumber it to (0, 0).
    let mask = ColumnChunkMask::row_groups_and_columns([1, 1, 99], [2, 1, 2, usize::MAX]);
    let reader = RecordingReader::from_bytes(bytes);
    let filters = ParquetMetaDataReader::read_bloom_filters(&reader, &metadata, &mask).unwrap();
    insta::assert_snapshot!(sync_transcript(&metadata, &regions, &reader, &filters), @r"
    Read: rg1 nested.b: complete filter [621..668)
    rg0: nested.a=None, nested.b=None, value=None
    rg1: nested.a=None, nested.b=filter(4), value=None
    ");
}

#[test]
fn batch_sync_stages_requests_and_skips_absences() {
    let (metadata, bytes, ranges) = fixture(true);
    let reader = RecordingReader::from_bytes(bytes);
    let result =
        ParquetMetaDataReader::read_bloom_filters(&reader, &metadata, &ColumnChunkMask::all())
            .unwrap();
    insta::assert_snapshot!(sync_transcript(&metadata, &ranges, &reader, &result), @r"
    Read: rg0 nested.a: complete filter [13..60)
    Read: rg0 value: legacy header probe [317..337)
    Read: rg1 nested.b: complete filter [621..668)
    Read: rg0 value: bitset [332..364)
    rg0: nested.a=filter(0), nested.b=None, value=filter(2)
    rg1: nested.a=None, nested.b=filter(4), value=None
    ");
}

#[test]
fn unselected_and_absent_filters_need_no_io() {
    let (metadata, bytes, ranges) = fixture(false);
    let no_reads = RecordingReader::from_bytes(bytes);
    let mut transcript = Vec::new();
    for (scenario, mask) in [
        ("No chunks selected", ColumnChunkMask::none()),
        (
            "Only absent filters selected",
            ColumnChunkMask::row_groups_and_columns([1], [0, 2]),
        ),
    ] {
        let result =
            ParquetMetaDataReader::read_bloom_filters(&no_reads, &metadata, &mask).unwrap();
        transcript.push(format!(
            "{scenario}:\n{}",
            sync_transcript(&metadata, &ranges, &no_reads, &result)
        ));
    }
    insta::assert_snapshot!(transcript.join("\n"), @r"
    No chunks selected:
    rg0: nested.a=None, nested.b=None, value=None
    rg1: nested.a=None, nested.b=None, value=None
    Only absent filters selected:
    rg0: nested.a=None, nested.b=None, value=None
    rg1: nested.a=None, nested.b=None, value=None
    ");
    let absent = super::metadata(None, Some(-1));
    let result =
        ParquetMetaDataReader::read_bloom_filters(&no_reads, &absent, &ColumnChunkMask::all())
            .unwrap();
    assert!(result.get(0, 0).unwrap().is_none());
    assert_error(result.get(1, 0), "coordinate out of bounds");
    assert_error(result.get(0, 1), "coordinate out of bounds");
    assert!(no_reads.requests().is_empty());
}

#[test]
fn batch_sync_errors_fail_the_whole_request() {
    let (metadata, bytes, ranges) = fixture(false);
    let valid = responses(&bytes, &ranges);
    // A bad second filter must fail a multi-filter read, not yield a partial result.
    for (response, error) in [
        (
            valid[1].slice(..valid[1].len() - 1),
            "response length mismatch",
        ),
        (
            encode(33, 32),
            "bitset length must be a positive multiple of 32",
        ),
    ] {
        let mut buffers = valid.clone();
        buffers[1] = response;
        let reader = RecordingReader::new(buffers);
        assert_error(
            ParquetMetaDataReader::read_bloom_filters(&reader, &metadata, &ColumnChunkMask::all()),
            error,
        );
    }
    let reader = RecordingReader::new(valid);
    reader.fail_at(1);
    assert_error(
        ParquetMetaDataReader::read_bloom_filters(&reader, &metadata, &ColumnChunkMask::all()),
        "injected read failure",
    );
    let reader = RecordingReader::default();
    assert_error(
        ParquetMetaDataReader::read_bloom_filters(
            &reader,
            &super::metadata(Some(-1), None),
            &ColumnChunkMask::all(),
        ),
        "Bloom filter offset is invalid",
    );
    assert!(reader.requests().is_empty());
}

#[cfg(feature = "async")]
mod r#async {
    use super::super::metadata as bloom_metadata;
    use super::*;
    use crate::io::TestReader;
    use futures::TryStreamExt;
    use futures::future::{BoxFuture, ready};
    use parquet::arrow::ParquetRecordBatchStreamBuilder;
    use parquet::arrow::{
        arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions},
        async_reader::{AsyncFileReader, MetadataFetch},
    };
    use parquet::file::reader::ChunkReader;
    use std::collections::VecDeque;
    use std::rc::Rc;
    use std::sync::Mutex;

    struct BatchReader {
        rounds: Arc<Mutex<Vec<Vec<Range<u64>>>>>,
        bytes: Option<Bytes>,
        responses: VecDeque<Result<Vec<Bytes>>>,
    }

    impl BatchReader {
        fn new(bytes: Bytes) -> Self {
            Self {
                rounds: Arc::default(),
                bytes: Some(bytes),
                responses: VecDeque::new(),
            }
        }

        fn failing(responses: impl IntoIterator<Item = Result<Vec<Bytes>>>) -> Self {
            Self {
                rounds: Arc::default(),
                bytes: None,
                responses: responses.into_iter().collect(),
            }
        }
    }

    fn batch_transcript(
        metadata: &ParquetMetaData,
        regions: &[Range<u64>],
        rounds: &Mutex<Vec<Vec<Range<u64>>>>,
        filters: &SbbfSet,
    ) -> String {
        let mut lines = Vec::new();
        for round in rounds.lock().unwrap().iter() {
            lines.push("Read Multi:".to_owned());
            for read in round {
                lines.push(format!("  {}", describe_read(metadata, regions, read)));
            }
        }
        lines.extend(describe_filters(filters));
        lines.join("\n")
    }

    impl AsyncFileReader for BatchReader {
        fn get_bytes(&mut self, _: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
            panic!("batch adapter must use get_byte_ranges")
        }
        fn get_byte_ranges(
            &mut self,
            ranges: Vec<Range<u64>>,
        ) -> BoxFuture<'_, Result<Vec<Bytes>>> {
            self.rounds.lock().unwrap().push(ranges.clone());
            let result = if let Some(bytes) = &self.bytes {
                ranges
                    .iter()
                    .map(|range| bytes.get_bytes(range.start, (range.end - range.start) as usize))
                    .collect()
            } else {
                self.responses.pop_front().expect("unexpected batch")
            };
            Box::pin(ready(result))
        }
        fn get_metadata<'a>(
            &'a mut self,
            _: Option<&'a ArrowReaderOptions>,
        ) -> BoxFuture<'a, Result<Arc<ParquetMetaData>>> {
            panic!("must not reload footer")
        }
    }

    #[tokio::test]
    async fn known_lengths_need_one_batch() {
        let (metadata, bytes, regions) = fixture(false);
        let mut reader = BatchReader::new(bytes);
        let filters = ParquetMetaDataReader::read_bloom_filters_async(
            &mut reader,
            &metadata,
            &ColumnChunkMask::all(),
        )
        .await
        .unwrap();
        insta::assert_snapshot!(batch_transcript(&metadata, &regions, &reader.rounds, &filters), @r"
        Read Multi:
          rg0 nested.a: complete filter [13..60)
          rg0 value: complete filter [317..364)
          rg1 nested.b: complete filter [621..668)
        rg0: nested.a=filter(0), nested.b=None, value=filter(2)
        rg1: nested.a=None, nested.b=filter(4), value=None
        ");
    }

    #[tokio::test]
    async fn builder_batches_known_filters_and_legacy_probes_together() {
        let (metadata, bytes, regions) = fixture(true);
        let reader = BatchReader::new(bytes);
        let rounds = Arc::clone(&reader.rounds);
        let arrow_metadata =
            ArrowReaderMetadata::try_new(Arc::new(metadata.clone()), Default::default()).unwrap();
        let filters = ParquetRecordBatchStreamBuilder::new_with_metadata(reader, arrow_metadata)
            .get_bloom_filters(&ColumnChunkMask::default())
            .await
            .unwrap();
        insta::assert_snapshot!(batch_transcript(&metadata, &regions, &rounds, &filters), @r"
        Read Multi:
          rg0 nested.a: complete filter [13..60)
          rg0 value: legacy header probe [317..337)
          rg1 nested.b: complete filter [621..668)
        Read Multi:
          rg0 value: bitset [332..364)
        rg0: nested.a=filter(0), nested.b=None, value=filter(2)
        rg1: nested.a=None, nested.b=filter(4), value=None
        ");
    }

    #[tokio::test]
    async fn multiple_legacy_filters_share_the_second_batch() {
        let (metadata, bytes, regions) = fixture_with_legacy(&[(0, 0), (1, 1)]);
        let mut reader = BatchReader::new(bytes);
        let filters = ParquetMetaDataReader::read_bloom_filters_async(
            &mut reader,
            &metadata,
            &ColumnChunkMask::all(),
        )
        .await
        .unwrap();
        insta::assert_snapshot!(batch_transcript(&metadata, &regions, &reader.rounds, &filters), @r"
        Read Multi:
          rg0 nested.a: legacy header probe [13..33)
          rg0 value: complete filter [317..364)
          rg1 nested.b: legacy header probe [621..641)
        Read Multi:
          rg0 nested.a: bitset [28..60)
          rg1 nested.b: bitset [636..668)
        rg0: nested.a=filter(0), nested.b=None, value=filter(2)
        rg1: nested.a=None, nested.b=filter(4), value=None
        ");
    }

    #[tokio::test]
    async fn async_builder_bloom_mask_is_explicit_and_builder_remains_usable() {
        let reader = TestReader::new(written_file());
        let requests = reader.requests();
        let mut builder = ParquetRecordBatchStreamBuilder::new(reader).await.unwrap();
        let metadata = builder.metadata().clone();
        let projection = ProjectionMask::leaves(builder.parquet_schema(), [1]);
        builder = builder.with_projection(projection).with_row_groups(vec![0]);
        requests.lock().unwrap().clear();
        let filters = builder
            .get_bloom_filters(&ColumnChunkMask::row_groups_and_columns([1], [0]))
            .await
            .unwrap();
        assert!(filters.get(1, 0).unwrap().unwrap().check(&30_i32));
        assert!(filters.get(0, 0).unwrap().is_none());
        assert!(filters.get(1, 1).unwrap().is_none());
        assert!(Arc::ptr_eq(builder.metadata(), &metadata));
        let column = metadata.row_group(1).column(0);
        let start = column.bloom_filter_offset().unwrap() as usize;
        let end = start + column.bloom_filter_length().unwrap() as usize;
        assert_eq!(*requests.lock().unwrap(), vec![start..end]);
        let batches: Vec<_> = builder
            .with_batch_size(1)
            .build()
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_projected_output(&batches);
        assert!(filters.get(1, 0).unwrap().unwrap().check(&31_i32));
    }

    #[tokio::test]
    async fn batch_async_rejects_malformed_responses_and_transport_errors() {
        let (metadata, bytes, ranges) = fixture(false);
        let valid = responses(&bytes, &ranges);
        let mut short = valid.clone();
        short[1] = short[1].slice(..short[1].len() - 1);
        let mut malformed = valid.clone();
        malformed[1] = encode(33, 32);
        for (response, error) in [
            (Ok(valid[..2].to_vec()), "response count mismatch"),
            (Ok(short), "response length mismatch"),
            (
                Ok(malformed),
                "bitset length must be a positive multiple of 32",
            ),
            (Err(ParquetError::General("transport".into())), "transport"),
        ] {
            let mut reader = BatchReader::failing([response]);
            assert_error(
                ParquetMetaDataReader::read_bloom_filters_async(
                    &mut reader,
                    &metadata,
                    &ColumnChunkMask::all(),
                )
                .await,
                error,
            );
            assert_eq!(reader.rounds.lock().unwrap().len(), 1);
        }
        // Unknown lengths must validate the second round too.
        let (metadata, bytes, mut ranges) = fixture(true);
        ranges[1].end = ranges[1].start + 20;
        for (second, error) in [
            (Ok(vec![]), "response count mismatch"),
            (
                Ok(vec![Bytes::from(vec![0; 31])]),
                "response length mismatch",
            ),
            (
                Err(ParquetError::General("bitset transport".into())),
                "bitset transport",
            ),
        ] {
            let mut reader = BatchReader::failing([Ok(responses(&bytes, &ranges)), second]);
            assert_error(
                ParquetMetaDataReader::read_bloom_filters_async(
                    &mut reader,
                    &metadata,
                    &ColumnChunkMask::all(),
                )
                .await,
                error,
            );
            assert_eq!(reader.rounds.lock().unwrap().len(), 2);
        }
    }

    struct Sequential<'a>(&'a RecordingReader);
    impl MetadataFetch for Sequential<'_> {
        fn fetch(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
            Box::pin(ready(self.0.read(range)))
        }
    }

    #[tokio::test]
    async fn batch_async_default_is_sequential_and_absences_do_not_fetch() {
        let (metadata, bytes, ranges) = fixture(false);
        let reader = RecordingReader::from_bytes(bytes);
        let filters = ParquetMetaDataReader::read_bloom_filters_async(
            Sequential(&reader),
            &metadata,
            &ColumnChunkMask::all(),
        )
        .await
        .unwrap();
        insta::assert_snapshot!(sync_transcript(&metadata, &ranges, &reader, &filters), @r"
        Read: rg0 nested.a: complete filter [13..60)
        Read: rg0 value: complete filter [317..364)
        Read: rg1 nested.b: complete filter [621..668)
        rg0: nested.a=filter(0), nested.b=None, value=filter(2)
        rg1: nested.a=None, nested.b=filter(4), value=None
        ");

        let reader = RecordingReader::from_bytes(Bytes::new());
        for mask in [
            ColumnChunkMask::none(),
            ColumnChunkMask::row_groups_and_columns([1], [0, 2]),
        ] {
            let result = ParquetMetaDataReader::read_bloom_filters_async(
                Sequential(&reader),
                &metadata,
                &mask,
            )
            .await
            .unwrap();
            assert!(result.get(0, 0).unwrap().is_none());
        }
        let absent = bloom_metadata(None, Some(-1));
        let result = ParquetMetaDataReader::read_bloom_filters_async(
            Sequential(&reader),
            &absent,
            &ColumnChunkMask::all(),
        )
        .await
        .unwrap();
        assert!(result.get(0, 0).unwrap().is_none());
        assert!(reader.requests().is_empty());
    }

    // Rc makes this source non-Send even though its returned future is Send.
    struct NonSend(Rc<RecordingReader>);
    impl MetadataFetch for NonSend {
        fn fetch(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
            Box::pin(ready(self.0.read(range)))
        }
    }

    #[tokio::test]
    async fn metadata_fetch_non_send_single_reads_remain_supported() {
        let bytes = encode(32, 32);
        let reader = Rc::new(RecordingReader::from_bytes(bytes.clone()));
        assert!(
            ParquetMetaDataReader::read_column_bloom_filter_async(
                NonSend(reader.clone()),
                &bloom_metadata(Some(0), Some(bytes.len() as i32)),
                0,
                0
            )
            .await
            .unwrap()
            .is_some()
        );
        assert_eq!(reader.requests(), vec![0..bytes.len() as u64]);
    }
}
