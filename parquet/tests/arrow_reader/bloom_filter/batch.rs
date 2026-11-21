// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use super::{RecordingReader, encode};
use arrow_array::{ArrayRef, Int32Array, RecordBatch};
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::{ArrowWriter, ProjectionMask};
use parquet::bloom_filter::Sbbf;
use parquet::file::metadata::page_index::RowGroupPageIndex;
use parquet::file::metadata::{
    ColumnChunkMask, ColumnChunkMetaData, FileMetaData, ParquetMetaData, ParquetMetaDataReader,
    RowGroupMetaData,
};
use parquet::file::properties::{ReaderProperties, WriterProperties};
use parquet::file::reader::{FileReader, RowGroupReader};
use parquet::file::serialized_reader::{
    ReadOptionsBuilder, SerializedFileReader, SerializedRowGroupReader,
};
use parquet::schema::{parser::parse_message_type, types::SchemaDescriptor};
use std::ops::Range;
use std::sync::Arc;

// Three distinct filters, widely separated, in an asymmetric physical-leaf grid.
fn fixture(unknown_length: bool) -> (ParquetMetaData, Bytes, Vec<Range<u64>>) {
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
                if !unknown_length || (row, col) != (0, 2) {
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
    for unknown in [false, true] {
        let (metadata, bytes, _) = fixture(unknown);
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
        let disabled = SerializedRowGroupReader::new(
            Arc::new(RecordingReader::default()),
            &row_group,
            RowGroupPageIndex::new(7, None),
            Arc::new(ReaderProperties::builder().build()),
        )
        .unwrap();
        for column in 0..3 {
            assert!(disabled.get_column_bloom_filter(column).is_none());
        }
        assert!(
            SerializedRowGroupReader::new(
                Arc::new(RecordingReader::default()),
                &row_group,
                RowGroupPageIndex::new(7, None),
                Arc::new(
                    ReaderProperties::builder()
                        .set_read_bloom_filter(true)
                        .build()
                ),
            )
            .is_err()
        );
    }
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
fn batch_masks_preserve_physical_coordinates_and_optional_lookup() {
    let (metadata, bytes, _) = fixture(false);
    for mask in [
        ColumnChunkMask::all(),
        ColumnChunkMask::none(),
        ColumnChunkMask::columns([2, 0, 2, 99, usize::MAX]),
        ColumnChunkMask::row_groups([1, 1, 99, usize::MAX]),
        ColumnChunkMask::row_groups_and_columns([1, 0, 1, 99], [2, 2, usize::MAX]),
        ColumnChunkMask::row_groups_and_columns([1], [0, 2]),
    ] {
        let result = ParquetMetaDataReader::read_bloom_filters(&bytes, &metadata, &mask).unwrap();
        for row in 0..2 {
            for col in 0..3 {
                let filter = result.get(row, col).unwrap();
                if !mask.includes_row_group(row, 2) || !mask.includes_column(col, 3) {
                    assert!(filter.is_none());
                    continue;
                }
                let single =
                    ParquetMetaDataReader::read_column_bloom_filter(&bytes, &metadata, row, col)
                        .unwrap();
                match (filter, single) {
                    (Some(batch), Some(single)) => {
                        let mut batch_bytes = Vec::new();
                        let mut single_bytes = Vec::new();
                        batch.write(&mut batch_bytes).unwrap();
                        single.write(&mut single_bytes).unwrap();
                        assert_eq!(batch_bytes, single_bytes);
                        assert!(batch.check(&((row * 3 + col) as i32)));
                    }
                    (None, None) => {}
                    _ => panic!("wrong coverage at ({row}, {col})"),
                }
            }
        }
        for (row, col) in [(2, 0), (0, 3), (usize::MAX, usize::MAX)] {
            assert!(result.get(row, col).is_err());
        }
    }
}

#[test]
fn batch_sync_stages_requests_and_skips_absences() {
    let (metadata, bytes, ranges) = fixture(true);
    let first = vec![
        ranges[0].clone(),
        ranges[1].start..ranges[1].start + 20,
        ranges[2].clone(),
    ];
    let bitset = ranges[1].end - 32..ranges[1].end;
    let mut expected = first.clone();
    expected.push(bitset);
    let reader = RecordingReader::new(responses(&bytes, &expected));
    let result =
        ParquetMetaDataReader::read_bloom_filters(&reader, &metadata, &ColumnChunkMask::all())
            .unwrap();
    assert_eq!(reader.requests(), expected);
    assert!(result.get(0, 2).unwrap().unwrap().check(&2_i32));
    let no_reads = RecordingReader::default();
    for mask in [
        ColumnChunkMask::none(),
        ColumnChunkMask::row_groups_and_columns([1], [0, 2]),
    ] {
        let result =
            ParquetMetaDataReader::read_bloom_filters(&no_reads, &metadata, &mask).unwrap();
        for row in 0..2 {
            for col in 0..3 {
                assert!(result.get(row, col).unwrap().is_none());
            }
        }
        assert!(result.get(2, 0).is_err());
        assert!(result.get(0, 3).is_err());
    }
    let absent = super::metadata(None, Some(-1));
    let result =
        ParquetMetaDataReader::read_bloom_filters(&no_reads, &absent, &ColumnChunkMask::all())
            .unwrap();
    assert!(result.get(0, 0).unwrap().is_none());
    assert!(result.get(1, 0).is_err());
    assert!(result.get(0, 1).is_err());
    assert!(no_reads.requests().is_empty());
}

#[test]
fn batch_sync_errors_fail_the_whole_request() {
    let (metadata, bytes, ranges) = fixture(false);
    let valid = responses(&bytes, &ranges);
    // A bad second filter must fail a multi-filter read, not yield a partial result.
    for response in [valid[1].slice(..valid[1].len() - 1), encode(33, 32)] {
        let mut buffers = valid.clone();
        buffers[1] = response;
        let reader = RecordingReader::new(buffers);
        assert!(
            ParquetMetaDataReader::read_bloom_filters(&reader, &metadata, &ColumnChunkMask::all())
                .is_err()
        );
    }
    let reader = RecordingReader::new(valid);
    reader.fail_at(1);
    assert!(
        ParquetMetaDataReader::read_bloom_filters(&reader, &metadata, &ColumnChunkMask::all())
            .is_err()
    );
    let reader = RecordingReader::default();
    assert!(
        ParquetMetaDataReader::read_bloom_filters(
            &reader,
            &super::metadata(Some(-1), None),
            &ColumnChunkMask::all(),
        )
        .is_err()
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
    use parquet::errors::{ParquetError, Result};
    use std::collections::VecDeque;
    use std::rc::Rc;
    use std::sync::Mutex;

    struct BatchReader {
        rounds: Arc<Mutex<Vec<Vec<Range<u64>>>>>,

        responses: VecDeque<Result<Vec<Bytes>>>,
    }

    impl AsyncFileReader for BatchReader {
        fn get_bytes(&mut self, _: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
            panic!("batch adapter must use get_byte_ranges")
        }
        fn get_byte_ranges(
            &mut self,
            ranges: Vec<Range<u64>>,
        ) -> BoxFuture<'_, Result<Vec<Bytes>>> {
            self.rounds.lock().unwrap().push(ranges);
            Box::pin(ready(self.responses.pop_front().expect("unexpected batch")))
        }
        fn get_metadata<'a>(
            &'a mut self,
            _: Option<&'a ArrowReaderOptions>,
        ) -> BoxFuture<'a, Result<Arc<ParquetMetaData>>> {
            panic!("must not reload footer")
        }
    }

    #[tokio::test]
    async fn batch_async_gathers_each_round_through_metadata_and_builder() {
        for unknown in [false, true] {
            for via_builder in [false, true] {
                let (metadata, bytes, ranges) = fixture(unknown);
                let mut first = ranges.clone();
                if unknown {
                    first[1].end = first[1].start + 20;
                }
                let mut rounds = vec![first];
                if unknown {
                    let bitset_range = ranges[1].end - 32..ranges[1].end;
                    rounds.push(vec![bitset_range]);
                }
                let mut reader = BatchReader {
                    responses: rounds
                        .iter()
                        .map(|round| Ok(responses(&bytes, round)))
                        .collect(),
                    rounds: Arc::default(),
                };
                let actual_rounds = Arc::clone(&reader.rounds);
                let result = if via_builder {
                    let arrow_metadata = ArrowReaderMetadata::try_new(
                        Arc::new(metadata.clone()),
                        Default::default(),
                    )
                    .unwrap();
                    ParquetRecordBatchStreamBuilder::new_with_metadata(reader, arrow_metadata)
                        .get_bloom_filters(&ColumnChunkMask::default())
                        .await
                        .unwrap()
                } else {
                    ParquetMetaDataReader::read_bloom_filters_async(
                        &mut reader,
                        &metadata,
                        &ColumnChunkMask::all(),
                    )
                    .await
                    .unwrap()
                };
                assert_eq!(*actual_rounds.lock().unwrap(), rounds);
                for (row, col) in [(0, 0), (0, 2), (1, 1)] {
                    let filter = result.get(row, col).unwrap().expect("missing filter");
                    let single = ParquetMetaDataReader::read_column_bloom_filter(
                        &bytes, &metadata, row, col,
                    )
                    .unwrap()
                    .unwrap();
                    let mut actual = Vec::new();
                    let mut expected = Vec::new();
                    filter.write(&mut actual).unwrap();
                    single.write(&mut expected).unwrap();
                    assert_eq!(actual, expected);
                }
                assert!(result.get(1, 2).unwrap().is_none());
            }
        }
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
        let mut long = valid.clone();
        long[2] = Bytes::from(vec![0; long[2].len() + 1]);
        let mut malformed = valid.clone();
        malformed[1] = encode(33, 32);
        for response in [
            Ok(vec![]),
            Ok(valid[..2].to_vec()),
            Ok([valid.clone(), vec![valid[0].clone()]].concat()),
            Ok(short),
            Ok(long),
            Ok(malformed),
            Err(ParquetError::General("transport".into())),
        ] {
            let mut reader = BatchReader {
                rounds: Arc::default(),
                responses: VecDeque::from([response]),
            };
            assert!(
                ParquetMetaDataReader::read_bloom_filters_async(
                    &mut reader,
                    &metadata,
                    &ColumnChunkMask::all()
                )
                .await
                .is_err()
            );
            assert_eq!(reader.rounds.lock().unwrap().len(), 1);
        }
        // Unknown lengths must validate the second round too.
        let (metadata, bytes, mut ranges) = fixture(true);
        ranges[1].end = ranges[1].start + 20;
        for second in [
            Ok(vec![]),
            Ok(vec![Bytes::from(vec![0; 31])]),
            Err(ParquetError::General("bitset transport".into())),
        ] {
            let mut reader = BatchReader {
                rounds: Arc::default(),
                responses: VecDeque::from([Ok(responses(&bytes, &ranges)), second]),
            };
            assert!(
                ParquetMetaDataReader::read_bloom_filters_async(
                    &mut reader,
                    &metadata,
                    &ColumnChunkMask::all()
                )
                .await
                .is_err()
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
        let reader = RecordingReader::new(responses(&bytes, &ranges));
        ParquetMetaDataReader::read_bloom_filters_async(
            Sequential(&reader),
            &metadata,
            &ColumnChunkMask::all(),
        )
        .await
        .unwrap();
        assert_eq!(reader.requests(), ranges);

        let reader = RecordingReader::default();
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
        let reader = Rc::new(RecordingReader::new([bytes.clone()]));
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
