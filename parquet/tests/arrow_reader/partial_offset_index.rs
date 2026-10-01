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

//! Tests for reading files in which only some column chunks have an offset index

use arrow::compute::kernels::cmp::{gt_eq, lt};
use arrow::compute::{and, concat_batches};
use arrow_array::{ArrayRef, Int32Array, RecordBatch};
use bytes::Bytes;
use parquet::DecodeResult;
use parquet::arrow::arrow_reader::{
    ArrowPredicateFn, ArrowReaderBuilder, ArrowReaderMetadata, ArrowReaderOptions,
    ParquetRecordBatchReaderBuilder, RowFilter, RowSelection, RowSelector,
};
use parquet::arrow::push_decoder::ParquetPushDecoderBuilder;
use parquet::arrow::{ArrowWriter, ProjectionMask};
use parquet::column::writer::ColumnCloseResult;
use parquet::errors::Result;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use std::sync::Arc;

#[test]
fn test_no_offset_index_first_column() {
    check(|_row_group, column| column != 0);
}

#[test]
fn test_no_offset_index_middle_column() {
    check(|_row_group, column| column != 1);
}

#[test]
fn test_no_offset_index_last_column() {
    check(|_row_group, column| column != 2);
}

#[test]
fn test_no_offset_index_row_group() {
    check(|row_group, _column| row_group != 1);
}

/// 400 rows in columns "a" (0..400), "b" (400..800) and "c" (800..1200)
fn test_batch() -> RecordBatch {
    let column = |start| Arc::new(Int32Array::from_iter_values(start..start + 400)) as ArrayRef;
    RecordBatch::try_from_iter([("a", column(0)), ("b", column(400)), ("c", column(800))]).unwrap()
}

/// Returns [`test_batch`] as a file with 2 row groups of 200 rows and data
/// pages of 50 rows. A column chunk has a page index only if
/// `has_index(row_group, column)` is `true`.
fn test_file(has_index: impl Fn(usize, usize) -> bool) -> Bytes {
    let batch = test_batch();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(200))
        .set_data_page_row_count_limit(50)
        .set_write_batch_size(50)
        .build();
    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let source = Bytes::from(buf);

    // Copy the column chunks to a new file, without the page index of some chunks
    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Required)
        .parse_and_finish(&source)
        .unwrap();
    let page_index = metadata.page_index().unwrap();
    let schema = metadata.file_metadata().schema_descr().root_schema_ptr();
    let mut buf = Vec::new();
    let mut writer = SerializedFileWriter::new(&mut buf, schema, Default::default()).unwrap();
    for (row_group, row_group_metadata) in metadata.row_groups().iter().enumerate() {
        let mut row_group_writer = writer.next_row_group().unwrap();
        for (column, column_metadata) in row_group_metadata.columns().iter().enumerate() {
            let keep = has_index(row_group, column);
            let close = ColumnCloseResult {
                bytes_written: column_metadata.compressed_size() as u64,
                rows_written: row_group_metadata.num_rows() as u64,
                metadata: column_metadata.clone(),
                bloom_filter: None,
                column_index: page_index
                    .column_index(row_group, column)
                    .filter(|_| keep)
                    .cloned(),
                offset_index: page_index
                    .offset_index(row_group, column)
                    .filter(|_| keep)
                    .cloned(),
            };
            row_group_writer.append_column(&source, close).unwrap();
        }
        row_group_writer.close().unwrap();
    }
    writer.close().unwrap();
    Bytes::from(buf)
}

/// Makes the reader select rows 150..250
#[derive(Debug, Clone, Copy)]
enum Trigger {
    RowSelection,
    RowFilter,
}

fn options() -> ArrowReaderOptions {
    ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional)
}

fn configure<T>(builder: ArrowReaderBuilder<T>, trigger: Trigger) -> ArrowReaderBuilder<T> {
    match trigger {
        Trigger::RowSelection => builder.with_row_selection(RowSelection::from(vec![
            RowSelector::skip(150),
            RowSelector::select(100),
            RowSelector::skip(150),
        ])),
        Trigger::RowFilter => {
            let mask = ProjectionMask::roots(builder.parquet_schema(), [0]);
            let predicate = ArrowPredicateFn::new(mask, |batch: RecordBatch| {
                let a = batch.column(0);
                and(
                    &gt_eq(a, &Int32Array::new_scalar(150))?,
                    &lt(a, &Int32Array::new_scalar(250))?,
                )
            });
            builder.with_row_filter(RowFilter::new(vec![Box::new(predicate)]))
        }
    }
}

fn read_sync(file: &Bytes, trigger: Trigger) -> Result<Vec<RecordBatch>> {
    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file.clone(), options())?;
    Ok(configure(builder, trigger)
        .build()?
        .collect::<Result<_, _>>()?)
}

#[cfg(feature = "async")]
fn read_async(file: &Bytes, trigger: Trigger) -> Result<Vec<RecordBatch>> {
    use futures::TryStreamExt;
    use parquet::arrow::ParquetRecordBatchStreamBuilder;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    runtime.block_on(async {
        let input = std::io::Cursor::new(file.clone());
        let builder = ParquetRecordBatchStreamBuilder::new_with_options(input, options()).await?;
        configure(builder, trigger).build()?.try_collect().await
    })
}

fn read_push_decoder(file: &Bytes, trigger: Trigger) -> Result<Vec<RecordBatch>> {
    let metadata = ArrowReaderMetadata::load(file, options())?;
    let builder = ParquetPushDecoderBuilder::new_with_metadata(metadata);
    let mut decoder = configure(builder, trigger).build()?;
    let mut batches = vec![];
    loop {
        match decoder.try_decode()? {
            DecodeResult::NeedsData(ranges) => {
                let data = ranges
                    .iter()
                    .map(|r| file.slice(r.start as usize..r.end as usize))
                    .collect();
                decoder.push_ranges(ranges, data)?;
            }
            DecodeResult::Data(batch) => batches.push(batch),
            DecodeResult::Finished => return Ok(batches),
        }
    }
}

/// Reads rows 150..250 of a file where only the column chunks for which
/// `has_index(row_group, column)` is `true` have an offset index
fn check(has_index: impl Fn(usize, usize) -> bool) {
    let file = test_file(&has_index);
    let metadata = ArrowReaderMetadata::load(&file, options()).unwrap();
    let page_index = metadata.metadata().page_index().unwrap();
    for row_group in 0..2 {
        for column in 0..3 {
            let actual = page_index.offset_index(row_group, column).is_some();
            assert_eq!(actual, has_index(row_group, column));
        }
    }

    let expected = test_batch().slice(150, 100);
    for trigger in [Trigger::RowSelection, Trigger::RowFilter] {
        let results = [
            ("sync", read_sync(&file, trigger)),
            ("push decoder", read_push_decoder(&file, trigger)),
            #[cfg(feature = "async")]
            ("async", read_async(&file, trigger)),
        ];
        for (reader, result) in results {
            let batches = result.unwrap_or_else(|e| panic!("{reader} reader, {trigger:?}: {e}"));
            let actual = concat_batches(&expected.schema(), &batches).unwrap();
            assert_eq!(actual, expected, "{reader} reader, {trigger:?}");
        }
    }
}
