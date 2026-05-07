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

//! Benchmarks for `ReverseSerializedPageReader` (issue #9934).
//!
//! Phase 1's primary empirical claim is **per-page cost parity** with the
//! existing forward `SerializedPageReader`. These benches verify that claim
//! by draining the same column chunk in both directions and comparing
//! throughput. They also report first-page latency (forward emits page 0,
//! reverse emits the last data page first), which is the metric Phase 2 will
//! ultimately target for `ORDER BY DESC LIMIT N`.

use std::hint::black_box;
use std::sync::Arc;

use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use parquet::basic::Compression;
use parquet::column::page::PageReader;
use parquet::column::reader::ColumnReaderImpl;
use parquet::data_type::Int32Type;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
use parquet::file::properties::WriterProperties;
use parquet::file::reader::SerializedPageReader;
use parquet::file::reverse_serialized_reader::ReverseSerializedPageReader;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;

const NUM_VALUES: usize = 100_000;
const PAGE_ROW_COUNT_LIMIT: usize = 1_024;
const PAGE_SIZE_LIMIT: usize = 4_096;

fn write_int32_file(num_values: usize, compression: Compression) -> Bytes {
    let message_type = "
        message schema {
            REQUIRED INT32 value;
        }
    ";
    let schema = Arc::new(parse_message_type(message_type).unwrap());
    let props = Arc::new(
        WriterProperties::builder()
            .set_compression(compression)
            .set_dictionary_enabled(false)
            .set_data_page_row_count_limit(PAGE_ROW_COUNT_LIMIT)
            .set_data_page_size_limit(PAGE_SIZE_LIMIT)
            .build(),
    );
    let values: Vec<i32> = (0..num_values as i32).collect();
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = SerializedFileWriter::new(&mut buf, schema, props).unwrap();
        let mut row_group = writer.next_row_group().unwrap();
        let mut col = row_group.next_column().unwrap().unwrap();
        col.typed::<Int32Type>()
            .write_batch(&values, None, None)
            .unwrap();
        col.close().unwrap();
        row_group.close().unwrap();
        writer.close().unwrap();
    }
    Bytes::from(buf)
}

fn open_metadata(bytes: &Bytes) -> Arc<ParquetMetaData> {
    let mut reader = ParquetMetaDataReader::new().with_page_index_policy(PageIndexPolicy::Required);
    reader.try_parse(bytes).unwrap();
    Arc::new(reader.finish().unwrap())
}

fn drain<R: PageReader>(mut reader: R) {
    while let Some(page) = reader.get_next_page().unwrap() {
        black_box(page);
    }
}

fn bench_codec(c: &mut Criterion, codec_label: &str, compression: Compression) {
    let bytes = write_int32_file(NUM_VALUES, compression);
    let metadata = open_metadata(&bytes);
    let chunk_reader: Arc<Bytes> = Arc::new(bytes);
    let rg = metadata.row_group(0);
    let column_chunk = rg.column(0);
    let total_rows = rg.num_rows() as usize;
    let offset_index = &metadata.offset_index().unwrap()[0][0];
    let page_locations = offset_index.page_locations().clone();
    let num_pages = page_locations.len();

    let mut group = c.benchmark_group(format!("reverse_page_reader/{codec_label}"));
    group.throughput(criterion::Throughput::Elements(num_pages as u64));

    group.bench_function("forward_drain", |b| {
        b.iter(|| {
            let reader = SerializedPageReader::new(
                chunk_reader.clone(),
                column_chunk,
                total_rows,
                Some(page_locations.clone()),
            )
            .unwrap();
            drain(reader);
        });
    });

    group.bench_function("reverse_drain", |b| {
        b.iter(|| {
            let reader =
                ReverseSerializedPageReader::new(chunk_reader.clone(), column_chunk, offset_index)
                    .unwrap();
            drain(reader);
        });
    });

    group.bench_function("forward_first_page", |b| {
        b.iter(|| {
            let mut reader = SerializedPageReader::new(
                chunk_reader.clone(),
                column_chunk,
                total_rows,
                Some(page_locations.clone()),
            )
            .unwrap();
            let page = reader.get_next_page().unwrap().unwrap();
            black_box(page);
        });
    });

    group.bench_function("reverse_first_page", |b| {
        b.iter(|| {
            let mut reader =
                ReverseSerializedPageReader::new(chunk_reader.clone(), column_chunk, offset_index)
                    .unwrap();
            // Skip dictionary if any (none here — dict is disabled in this fixture).
            let page = reader.get_next_page().unwrap().unwrap();
            black_box(page);
        });
    });

    group.finish();
}

/// Simulates the existing **row-group-level reverse** strategy used by
/// DataFusion (apache/datafusion#18817): forward-decode the entire column
/// chunk, reverse the resulting value buffer, then take the first `n`
/// elements. Peak buffer = full column chunk; time-to-first-N = full decode.
fn time_to_first_n_row_group_sim(
    chunk_reader: &Arc<Bytes>,
    metadata: &Arc<ParquetMetaData>,
    n: usize,
) {
    let rg = metadata.row_group(0);
    let column_chunk = rg.column(0);
    let total_rows = rg.num_rows() as usize;
    let column_descr = metadata.file_metadata().schema_descr().column(0);
    let page_locations = metadata.offset_index().unwrap()[0][0]
        .page_locations()
        .clone();

    let forward = Box::new(
        SerializedPageReader::new(
            chunk_reader.clone(),
            column_chunk,
            total_rows,
            Some(page_locations),
        )
        .unwrap(),
    );
    let mut col_reader: ColumnReaderImpl<Int32Type> = ColumnReaderImpl::new(column_descr, forward);
    let mut values: Vec<i32> = Vec::with_capacity(total_rows);
    col_reader
        .read_records(total_rows, None, None, &mut values)
        .unwrap();
    values.reverse();
    values.truncate(n);
    black_box(values);
}

/// **Page-level reverse** strategy enabled by `ReverseSerializedPageReader`:
/// emit pages in reverse order, decode just enough pages to gather `n` values,
/// reverse those values, take the first `n`. Peak buffer = at most one page
/// worth of values when `n` is smaller than the last page's row count.
fn time_to_first_n_page_reverse(
    chunk_reader: &Arc<Bytes>,
    metadata: &Arc<ParquetMetaData>,
    n: usize,
) {
    let rg = metadata.row_group(0);
    let column_chunk = rg.column(0);
    let column_descr = metadata.file_metadata().schema_descr().column(0);
    let offset_index = &metadata.offset_index().unwrap()[0][0];

    let reverse = Box::new(
        ReverseSerializedPageReader::new(chunk_reader.clone(), column_chunk, offset_index).unwrap(),
    );
    let mut col_reader: ColumnReaderImpl<Int32Type> = ColumnReaderImpl::new(column_descr, reverse);

    // Decode pages until we have at least `n` values. Each iteration asks for
    // a generous chunk so the column reader pulls a whole page at a time.
    let mut values: Vec<i32> = Vec::with_capacity(n);
    while values.len() < n {
        let want = n.saturating_sub(values.len()).max(PAGE_ROW_COUNT_LIMIT);
        let (records, _, _) = col_reader
            .read_records(want, None, None, &mut values)
            .unwrap();
        if records == 0 {
            break;
        }
    }
    // Within each emitted page rows are in forward order; reverse so the
    // first `n` of the result correspond to the *last* `n` rows of the
    // forward chunk.
    values.reverse();
    values.truncate(n);
    black_box(values);
}

fn bench_time_to_first_n(c: &mut Criterion) {
    let bytes = write_int32_file(NUM_VALUES, Compression::UNCOMPRESSED);
    let metadata = open_metadata(&bytes);
    let chunk_reader: Arc<Bytes> = Arc::new(bytes);
    let num_pages = metadata.offset_index().unwrap()[0][0]
        .page_locations()
        .len();

    let mut group = c.benchmark_group("time_to_first_n_reversed_values");
    for &n in &[10usize, 100, 1024] {
        group.bench_function(format!("row_group_sim/n={n}/pages={num_pages}"), |b| {
            b.iter(|| time_to_first_n_row_group_sim(&chunk_reader, &metadata, n));
        });
        group.bench_function(format!("page_reverse/n={n}/pages={num_pages}"), |b| {
            b.iter(|| time_to_first_n_page_reverse(&chunk_reader, &metadata, n));
        });
    }
    group.finish();
}

fn criterion_benchmark(c: &mut Criterion) {
    bench_codec(c, "uncompressed", Compression::UNCOMPRESSED);
    bench_codec(c, "snappy", Compression::SNAPPY);
    bench_time_to_first_n(c);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
