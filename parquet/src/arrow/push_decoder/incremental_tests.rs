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

//! Tests for [`FetchGranularity::Batch`].
//!
//! Most tests check one property: a scan decoded a batch at a time gives the
//! same batches, in the same order, as the same scan decoded a row group at a
//! time. The others check what the decoder requests and how many bytes it
//! holds.

use crate::DecodeResult;
use crate::arrow::arrow_reader::metrics::ArrowReaderMetrics;
use crate::arrow::arrow_reader::{
    ArrowPredicate, ArrowPredicateFn, ArrowReaderMetadata, ArrowReaderOptions, RowFilter,
    RowSelection, RowSelectionPolicy, RowSelector,
};
use crate::arrow::push_decoder::{FetchGranularity, ParquetPushDecoder, ParquetPushDecoderBuilder};
use crate::arrow::{ArrowWriter, ProjectionMask};
use crate::file::metadata::PageIndexPolicy;
use crate::file::properties::WriterProperties;
use crate::schema::types::SchemaDescriptor;
use arrow_array::builder::{Int64Builder, ListBuilder};
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray, StructArray};
use arrow_schema::{DataType, Field};
use bytes::Bytes;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::ops::Range;
use std::sync::{Arc, LazyLock};

const ROWS_PER_ROW_GROUP: usize = 600;
const ROWS_PER_PAGE: usize = 25;
const NUM_ROWS: usize = 1800;

/// Three row groups of 600 rows, 25 rows per data page, columns:
///
/// * `a`: 0, 1, 2, ... (plain)
/// * `b`: `a % 10` (dictionary)
/// * `c`: a string (plain)
/// * `l`: a list of 0 to 3 `a` values, some lists null (plain)
/// * `s`: a struct of `a * 2` and `a % 7` (plain)
struct TestFile {
    data: Bytes,
    batch: RecordBatch,
}

static TEST_FILE: LazyLock<TestFile> = LazyLock::new(|| {
    let a: Vec<i64> = (0..NUM_ROWS as i64).collect();
    let mut lists = ListBuilder::new(Int64Builder::new());
    for &v in &a {
        if v % 11 == 5 {
            lists.append_null();
        } else {
            for i in 0..(v % 4) {
                lists.values().append_value(v * 10 + i);
            }
            lists.append(true);
        }
    }
    let s = StructArray::from(vec![
        (
            Arc::new(Field::new("x", DataType::Int64, false)),
            Arc::new(Int64Array::from_iter_values(a.iter().map(|v| v * 2))) as ArrayRef,
        ),
        (
            Arc::new(Field::new("y", DataType::Int64, false)),
            Arc::new(Int64Array::from_iter_values(a.iter().map(|v| v % 7))) as ArrayRef,
        ),
    ]);
    let batch = RecordBatch::try_from_iter(vec![
        ("a", Arc::new(Int64Array::from(a.clone())) as ArrayRef),
        (
            "b",
            Arc::new(Int64Array::from_iter_values(a.iter().map(|v| v % 10))) as ArrayRef,
        ),
        (
            "c",
            Arc::new(StringArray::from_iter_values(
                a.iter().map(|v| format!("row-{v:08}-padding")),
            )) as ArrayRef,
        ),
        ("l", Arc::new(lists.finish()) as ArrayRef),
        ("s", Arc::new(s) as ArrayRef),
    ])
    .unwrap();

    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(ROWS_PER_ROW_GROUP))
        .set_data_page_row_count_limit(ROWS_PER_PAGE)
        .set_write_batch_size(ROWS_PER_PAGE)
        .set_dictionary_enabled(false)
        .set_column_dictionary_enabled("b".into(), true)
        .build();
    let mut buffer = vec![];
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    TestFile {
        data: Bytes::from(buffer),
        batch,
    }
});

fn load_metadata(policy: PageIndexPolicy) -> ArrowReaderMetadata {
    let options = ArrowReaderOptions::new().with_page_index_policy(policy);
    ArrowReaderMetadata::load(&TEST_FILE.data, options).unwrap()
}

static WITH_PAGE_INDEX: LazyLock<ArrowReaderMetadata> =
    LazyLock::new(|| load_metadata(PageIndexPolicy::Required));
static WITHOUT_PAGE_INDEX: LazyLock<ArrowReaderMetadata> =
    LazyLock::new(|| load_metadata(PageIndexPolicy::Skip));

fn metadata(page_index: bool) -> ArrowReaderMetadata {
    if page_index {
        WITH_PAGE_INDEX.clone()
    } else {
        WITHOUT_PAGE_INDEX.clone()
    }
}

fn fetch(range: &Range<u64>) -> Bytes {
    TEST_FILE
        .data
        .slice(range.start as usize..range.end as usize)
}

fn schema() -> SchemaDescriptor {
    metadata(true)
        .metadata()
        .file_metadata()
        .schema_descr()
        .clone()
}

fn columns(names: &[&str]) -> ProjectionMask {
    ProjectionMask::columns(&schema(), names.iter().copied())
}

/// What a decode run cost.
#[derive(Debug, Default)]
struct Cost {
    /// `NeedsData` results.
    rounds: usize,
    /// Every range requested, in order.
    requested: Vec<Range<u64>>,
    /// Highest `buffered_bytes()` seen.
    peak_buffered: u64,
}

/// Decode, pushing exactly the requested ranges.
fn drive(mut decoder: ParquetPushDecoder) -> (Vec<RecordBatch>, Cost) {
    let mut batches = vec![];
    let mut cost = Cost::default();
    let mut last_buffered = 0;
    loop {
        match decoder.try_decode().unwrap() {
            DecodeResult::NeedsData(ranges) => {
                assert!(!ranges.is_empty());
                cost.rounds += 1;
                let data = ranges.iter().map(fetch).collect();
                cost.requested.extend(ranges.iter().cloned());
                decoder.push_ranges(ranges, data).unwrap();
            }
            DecodeResult::Data(batch) => {
                batches.push(batch);
                last_buffered = decoder.buffered_bytes();
            }
            DecodeResult::Finished => break,
        }
        cost.peak_buffered = cost.peak_buffered.max(decoder.buffered_bytes());
    }
    // The last batch finishes the last row group, which releases its bytes.
    assert_eq!(last_buffered, 0, "bytes left after the last batch");
    (batches, cost)
}

/// A predicate on one Int64 column.
#[derive(Clone, Copy, Debug)]
enum Cmp {
    Lt(i64),
    Ge(i64),
    ModNotZero(i64),
    All,
    None,
}

#[derive(Clone, Debug)]
struct PredicateSpec {
    column: &'static str,
    cmp: Cmp,
}

impl PredicateSpec {
    fn new(column: &'static str, cmp: Cmp) -> Self {
        Self { column, cmp }
    }

    fn build(&self) -> Box<dyn ArrowPredicate> {
        let cmp = self.cmp;
        Box::new(ArrowPredicateFn::new(
            columns(&[self.column]),
            move |batch: RecordBatch| {
                let values = batch.column(0).as_primitive::<Int64Type>();
                Ok(values
                    .iter()
                    .map(|v| {
                        let v = v.unwrap();
                        Some(match cmp {
                            Cmp::Lt(x) => v < x,
                            Cmp::Ge(x) => v >= x,
                            Cmp::ModNotZero(x) => v % x != 0,
                            Cmp::All => true,
                            Cmp::None => false,
                        })
                    })
                    .collect::<BooleanArray>())
            },
        ))
    }
}

/// A scan configuration, so that the same scan can be built for both
/// granularities.
#[derive(Clone, Debug, Default)]
struct Scan {
    page_index_off: bool,
    batch_size: Option<usize>,
    projection: Option<ProjectionMask>,
    row_groups: Option<Vec<usize>>,
    selection: Option<RowSelection>,
    limit: Option<usize>,
    offset: Option<usize>,
    /// Built again for each decoder, as predicates have state.
    predicates: Vec<PredicateSpec>,
    policy: Option<RowSelectionPolicy>,
    max_predicate_cache_size: Option<usize>,
}

impl Scan {
    fn builder(&self) -> ParquetPushDecoderBuilder {
        let mut builder =
            ParquetPushDecoderBuilder::new_with_metadata(metadata(!self.page_index_off));
        if let Some(batch_size) = self.batch_size {
            builder = builder.with_batch_size(batch_size);
        }
        if let Some(projection) = &self.projection {
            builder = builder.with_projection(projection.clone());
        }
        if let Some(row_groups) = &self.row_groups {
            builder = builder.with_row_groups(row_groups.clone());
        }
        if let Some(selection) = &self.selection {
            builder = builder.with_row_selection(selection.clone());
        }
        if let Some(limit) = self.limit {
            builder = builder.with_limit(limit);
        }
        if let Some(offset) = self.offset {
            builder = builder.with_offset(offset);
        }
        if let Some(policy) = self.policy {
            builder = builder.with_row_selection_policy(policy);
        }
        if let Some(size) = self.max_predicate_cache_size {
            builder = builder.with_max_predicate_cache_size(size);
        }
        if !self.predicates.is_empty() {
            let predicates = self.predicates.iter().map(|p| p.build()).collect();
            builder = builder.with_row_filter(RowFilter::new(predicates));
        }
        builder
    }

    fn row_group_decoder(&self) -> ParquetPushDecoder {
        self.builder().build().unwrap()
    }

    fn batch_decoder(&self) -> ParquetPushDecoder {
        self.builder()
            .with_fetch_granularity(FetchGranularity::Batch)
            .build()
            .unwrap()
    }
}

/// Assert that the scan gives the same batches for both granularities, and
/// return the costs (row group, batch).
#[track_caller]
fn assert_same_batches(scan: &Scan) -> (Cost, Cost) {
    let (expected, row_group_cost) = drive(scan.row_group_decoder());
    let (actual, batch_cost) = drive(scan.batch_decoder());
    assert_eq!(
        actual.iter().map(|b| b.num_rows()).collect::<Vec<_>>(),
        expected.iter().map(|b| b.num_rows()).collect::<Vec<_>>(),
        "batch sizes differ for {scan:?}"
    );
    for (i, (actual, expected)) in actual.iter().zip(&expected).enumerate() {
        assert_eq!(actual, expected, "batch {i} differs for {scan:?}");
    }
    // A range requested twice means the decoder released it too early.
    let mut requested = batch_cost.requested.clone();
    requested.sort_by_key(|r| (r.start, r.end));
    requested.dedup();
    assert_eq!(
        requested.len(),
        batch_cost.requested.len(),
        "a range was requested twice for {scan:?}"
    );
    (row_group_cost, batch_cost)
}

/// Sort and merge ranges into disjoint, non-adjacent ranges.
fn union(ranges: impl IntoIterator<Item = Range<u64>>) -> Vec<Range<u64>> {
    let mut ranges: Vec<_> = ranges.into_iter().filter(|r| !r.is_empty()).collect();
    ranges.sort_by_key(|r| r.start);
    let mut merged: Vec<Range<u64>> = vec![];
    for range in ranges {
        match merged.last_mut() {
            Some(last) if range.start <= last.end => last.end = last.end.max(range.end),
            _ => merged.push(range),
        }
    }
    merged
}

/// The dictionary page (if any) and the data pages of a column chunk, one
/// range each.
fn page_ranges(row_group: usize, column: usize) -> Vec<Range<u64>> {
    let meta = metadata(true);
    let (start, len) = meta
        .metadata()
        .row_group(row_group)
        .column(column)
        .byte_range();
    let page_index = meta.metadata().page_index_for_row_group(row_group);
    let locations = page_index.page_locations(column).unwrap();
    let dictionary = start..locations[0].offset as u64;
    let ranges: Vec<Range<u64>> = std::iter::once(dictionary)
        .chain(
            locations
                .iter()
                .map(|l| l.offset as u64..l.offset as u64 + l.compressed_page_size as u64),
        )
        .filter(|r| !r.is_empty())
        .collect();
    assert_eq!(ranges.last().unwrap().end, start + len);
    ranges
}

fn row_group_bytes(row_group: usize) -> u64 {
    metadata(true)
        .metadata()
        .row_group(row_group)
        .columns()
        .iter()
        .map(|c| c.byte_range().1)
        .sum()
}

// ---------------------------------------------------------------------------
// No predicates
// ---------------------------------------------------------------------------

#[test]
fn full_scan() {
    let scan = Scan {
        batch_size: Some(100),
        ..Default::default()
    };
    let (row_group, batch) = assert_same_batches(&scan);
    // The same bytes, requested in more and smaller requests.
    assert_eq!(union(row_group.requested), union(batch.requested.clone()));
    assert!(batch.rounds > 3 * 3, "{}", batch.rounds);
    assert!(
        batch.peak_buffered * 3 < row_group_bytes(0),
        "peak {} vs row group {}",
        batch.peak_buffered,
        row_group_bytes(0)
    );
}

#[test]
fn batch_sizes_and_projections() {
    for batch_size in [1, 7, 24, 25, 26, 100, 599, 600, 601, 5000] {
        for projection in [
            None,
            Some(columns(&["a"])),
            Some(columns(&["c", "b"])),
            Some(columns(&["l"])),
            Some(columns(&["s"])),
            Some(columns(&["l", "s", "b"])),
        ] {
            assert_same_batches(&Scan {
                batch_size: Some(batch_size),
                projection,
                ..Default::default()
            });
        }
    }
}

#[test]
fn row_selections_skip_pages() {
    // Select 20 rows, skip 40: most pages are read partly, some are skipped.
    let alternating: Vec<RowSelector> = (0..NUM_ROWS / 60)
        .flat_map(|_| [RowSelector::select(20), RowSelector::skip(40)])
        .collect();
    // Select a few rows far apart: most pages are skipped entirely.
    let sparse = RowSelection::from_consecutive_ranges(
        [3..4, 180..190, 700..701, 1203..1300, 1799..1800].into_iter(),
        NUM_ROWS,
    );
    for selection in [RowSelection::from(alternating), sparse] {
        for policy in [
            None,
            Some(RowSelectionPolicy::Selectors),
            Some(RowSelectionPolicy::Mask),
        ] {
            for batch_size in [8, 64, 600] {
                let (row_group, batch) = assert_same_batches(&Scan {
                    batch_size: Some(batch_size),
                    selection: Some(selection.clone()),
                    policy,
                    ..Default::default()
                });
                assert_eq!(union(row_group.requested), union(batch.requested));
            }
        }
    }
}

#[test]
fn offset_and_limit() {
    for (offset, limit) in [
        (None, Some(1)),
        (Some(650), Some(500)),
        (Some(599), None),
        (Some(1799), Some(10)),
        (Some(2000), None),
        (None, Some(0)),
    ] {
        let (row_group, batch) = assert_same_batches(&Scan {
            batch_size: Some(70),
            offset,
            limit,
            ..Default::default()
        });
        assert_eq!(union(row_group.requested), union(batch.requested));
    }
}

#[test]
fn empty_selection_requests_nothing() {
    let (_, batch) = assert_same_batches(&Scan {
        batch_size: Some(64),
        selection: Some(RowSelection::from(vec![RowSelector::skip(NUM_ROWS)])),
        ..Default::default()
    });
    assert!(batch.requested.is_empty());
}

#[test]
fn without_offset_index_requests_column_chunks() {
    let scan = Scan {
        page_index_off: true,
        batch_size: Some(100),
        selection: Some(RowSelection::from(vec![
            RowSelector::skip(100),
            RowSelector::select(1000),
        ])),
        ..Default::default()
    };
    let (row_group, batch) = assert_same_batches(&scan);
    // Without page locations, whole column chunks are requested, the same
    // bytes as in the row group mode.
    assert_eq!(union(row_group.requested), union(batch.requested.clone()));
    let chunks: Vec<Range<u64>> = metadata(false)
        .metadata()
        .row_groups()
        .iter()
        .flat_map(|rg| rg.columns().iter())
        .map(|c| {
            let (start, len) = c.byte_range();
            start..start + len
        })
        .collect();
    assert!(batch.requested.iter().all(|r| chunks.contains(r)));
}

#[test]
fn dictionary_page_is_requested_once_per_column_chunk() {
    let scan = Scan {
        batch_size: Some(50),
        projection: Some(columns(&["b"])),
        ..Default::default()
    };
    let (_, batch) = assert_same_batches(&scan);
    let meta = metadata(true);
    let meta = meta.metadata();
    let b = 1;
    for row_group in 0..meta.num_row_groups() {
        let (chunk_start, _) = meta.row_group(row_group).column(b).byte_range();
        let first_page = meta
            .page_index_for_row_group(row_group)
            .page_locations(b)
            .unwrap()[0]
            .offset as u64;
        assert!(first_page > chunk_start, "expected a dictionary page");
        let dictionary = chunk_start..first_page;
        let count = batch.requested.iter().filter(|r| **r == dictionary).count();
        assert_eq!(
            count, 1,
            "dictionary {dictionary:?} requested {count} times"
        );
    }
}

/// Push only what the decoder asks for, one batch at a time, and check that
/// each request serves exactly one batch: the first request of a row group
/// is small, and each `Data` needs no further push.
#[test]
fn partial_pushes_across_batches() {
    let meta = metadata(true);
    let batch_size = 50;
    let mut decoder = Scan {
        batch_size: Some(batch_size),
        projection: Some(columns(&["a", "c"])),
        ..Default::default()
    }
    .batch_decoder();

    let mut rows = 0;
    let mut requests_per_batch = vec![];
    let mut requests = 0;
    loop {
        match decoder.try_decode().unwrap() {
            DecodeResult::NeedsData(ranges) => {
                // Each request is for pages of this batch's rows only: with
                // 25 rows per page and 50 rows per batch, 2 pages per column.
                assert!(ranges.len() <= 4, "{ranges:?}");
                let data = ranges.iter().map(fetch).collect();
                decoder.push_ranges(ranges, data).unwrap();
                requests += 1;
            }
            DecodeResult::Data(batch) => {
                assert_eq!(
                    batch,
                    TEST_FILE
                        .batch
                        .project(&[0, 2])
                        .unwrap()
                        .slice(rows, batch.num_rows())
                );
                rows += batch.num_rows();
                requests_per_batch.push(requests);
                requests = 0;
            }
            DecodeResult::Finished => break,
        }
    }
    assert_eq!(rows, NUM_ROWS);
    assert!(
        requests_per_batch.iter().all(|&r| r == 1),
        "{requests_per_batch:?}"
    );

    // The first request of the scan is a small part of the row group.
    let mut decoder = Scan {
        batch_size: Some(batch_size),
        ..Default::default()
    }
    .batch_decoder();
    let DecodeResult::NeedsData(first) = decoder.try_decode().unwrap() else {
        panic!("expected NeedsData");
    };
    let first_bytes: u64 = first.iter().map(|r| r.end - r.start).sum();
    assert!(first_bytes * 5 < row_group_bytes(0), "{first_bytes}");
    // The pages of the first batch, and the dictionary page of `b`.
    let page_index = meta.metadata().page_index_for_row_group(0);
    for column in 0..meta.metadata().row_group(0).num_columns() {
        let locations = page_index.page_locations(column).unwrap();
        for location in &locations[..2] {
            let start = location.offset as u64;
            assert!(first.contains(&(start..start + location.compressed_page_size as u64)));
        }
        let later = locations[2].offset as u64;
        assert!(first.iter().all(|r| r.start != later));
    }
}

/// Push every planned byte of the scan up front, one buffer per page, and
/// check that the decoder releases pages as it passes them.
#[test]
fn releases_bytes_pushed_ahead() {
    let meta = metadata(true);
    let mut decoder = Scan {
        batch_size: Some(100),
        ..Default::default()
    }
    .batch_decoder();

    // Push every page of the first two row groups.
    let mut pushed = 0;
    for row_group in 0..2 {
        for (column, chunk) in meta
            .metadata()
            .row_group(row_group)
            .columns()
            .iter()
            .enumerate()
        {
            let ranges = page_ranges(row_group, column);
            pushed += chunk.byte_range().1;
            let data = ranges.iter().map(fetch).collect();
            decoder.push_ranges(ranges, data).unwrap();
        }
    }
    assert_eq!(decoder.buffered_bytes(), pushed);

    let mut resident = vec![];
    let mut rows = 0;
    loop {
        match decoder.try_decode().unwrap() {
            DecodeResult::NeedsData(ranges) => {
                // Only the third row group was not pushed.
                assert!(rows >= 2 * ROWS_PER_ROW_GROUP, "{rows}: {ranges:?}");
                let data = ranges.iter().map(fetch).collect();
                decoder.push_ranges(ranges, data).unwrap();
            }
            DecodeResult::Data(batch) => {
                rows += batch.num_rows();
                resident.push(decoder.buffered_bytes());
            }
            DecodeResult::Finished => break,
        }
    }
    assert_eq!(rows, NUM_ROWS);
    // Resident bytes decrease as the first row group is decoded, and by the
    // end of it only the second row group is left.
    assert!(
        resident.windows(2).take(5).all(|w| w[1] < w[0]),
        "{resident:?}"
    );
    assert_eq!(resident[5], row_group_bytes(1));
    assert_eq!(decoder.buffered_bytes(), 0);
}

/// Bytes that are pushed in one buffer are released in parts. The
/// accounting follows, although the allocation is freed only at the end.
#[test]
fn releases_parts_of_one_buffer() {
    let mut decoder = Scan {
        batch_size: Some(100),
        ..Default::default()
    }
    .batch_decoder();
    let file = 0..TEST_FILE.data.len() as u64;
    decoder.push_range(file.clone(), fetch(&file)).unwrap();
    let mut previous = decoder.buffered_bytes();
    let mut batches = 0;
    while let DecodeResult::Data(_) = decoder.try_decode().unwrap() {
        let buffered = decoder.buffered_bytes();
        assert!(buffered < previous, "{buffered} >= {previous}");
        previous = buffered;
        batches += 1;
    }
    assert_eq!(batches, NUM_ROWS / 100);
}

#[test]
fn row_group_boundary_is_visible_after_the_last_batch() {
    let mut decoder = Scan {
        batch_size: Some(250),
        ..Default::default()
    }
    .batch_decoder();
    let mut sizes = vec![];
    let mut boundaries = vec![];
    loop {
        match decoder.try_decode().unwrap() {
            DecodeResult::NeedsData(ranges) => {
                let data = ranges.iter().map(fetch).collect();
                decoder.push_ranges(ranges, data).unwrap();
            }
            DecodeResult::Data(batch) => {
                sizes.push(batch.num_rows());
                boundaries.push(decoder.is_at_row_group_boundary());
            }
            DecodeResult::Finished => break,
        }
    }
    assert_eq!(sizes, vec![250, 250, 100, 250, 250, 100, 250, 250, 100]);
    assert_eq!(
        boundaries,
        vec![false, false, true, false, false, true, false, false, true]
    );
}

#[test]
fn into_builder_at_a_boundary() {
    let mut decoder = Scan {
        batch_size: Some(300),
        ..Default::default()
    }
    .batch_decoder();
    let mut batches = vec![];
    let mut rebuilt = false;
    loop {
        match decoder.try_decode().unwrap() {
            DecodeResult::NeedsData(ranges) => {
                let data = ranges.iter().map(fetch).collect();
                decoder.push_ranges(ranges, data).unwrap();
            }
            DecodeResult::Data(batch) => {
                batches.push(batch);
                if !rebuilt && decoder.is_at_row_group_boundary() {
                    // Skip the second row group, keep batch granularity.
                    decoder = decoder
                        .into_builder()
                        .unwrap()
                        .with_row_groups(vec![2])
                        .build()
                        .unwrap();
                    rebuilt = true;
                }
            }
            DecodeResult::Finished => break,
        }
    }
    let rows: Vec<i64> = batches
        .iter()
        .flat_map(|b| b.column(0).as_primitive::<Int64Type>().values().to_vec())
        .collect();
    let expected: Vec<i64> = (0..600).chain(1200..1800).collect();
    assert_eq!(rows, expected);
}

#[test]
fn try_next_reader_at_boundaries_and_not_in_a_row_group() {
    let mut decoder = Scan {
        batch_size: Some(300),
        ..Default::default()
    }
    .batch_decoder();
    // `try_next_reader` still returns whole row groups.
    let file = 0..TEST_FILE.data.len() as u64;
    decoder.push_range(file.clone(), fetch(&file)).unwrap();
    let DecodeResult::Data(reader) = decoder.try_next_reader().unwrap() else {
        panic!("expected a reader");
    };
    assert_eq!(reader.map(|b| b.unwrap().num_rows()).sum::<usize>(), 600);

    // A row group started by `try_decode` must be finished by it.
    let result = decoder.try_decode().unwrap();
    let DecodeResult::Data(batch) = result else {
        panic!("expected a batch, got {result:?}");
    };
    assert_eq!(batch.num_rows(), 300);
    let err = decoder.try_next_reader().unwrap_err().to_string();
    assert!(err.contains("try_decode"), "{err}");
    let DecodeResult::Data(batch) = decoder.try_decode().unwrap() else {
        panic!("expected a batch");
    };
    assert_eq!(batch.column(0).as_primitive::<Int64Type>().value(0), 900);

    // At the boundary, `try_next_reader` works again.
    let DecodeResult::Data(reader) = decoder.try_next_reader().unwrap() else {
        panic!("expected a reader");
    };
    assert_eq!(reader.map(|b| b.unwrap().num_rows()).sum::<usize>(), 600);
    assert!(matches!(
        decoder.try_decode().unwrap(),
        DecodeResult::Finished
    ));
}

// ---------------------------------------------------------------------------
// Predicates
// ---------------------------------------------------------------------------

fn filtered(predicates: Vec<PredicateSpec>) -> Scan {
    Scan {
        batch_size: Some(100),
        predicates,
        ..Default::default()
    }
}

#[test]
fn predicates() {
    let cases = vec![
        vec![PredicateSpec::new("a", Cmp::Lt(900))],
        vec![
            PredicateSpec::new("a", Cmp::Ge(300)),
            PredicateSpec::new("b", Cmp::ModNotZero(3)),
        ],
        vec![
            PredicateSpec::new("a", Cmp::Ge(100)),
            PredicateSpec::new("a", Cmp::Lt(1500)),
            PredicateSpec::new("b", Cmp::ModNotZero(2)),
        ],
        vec![PredicateSpec::new("a", Cmp::Ge(750))],
        vec![PredicateSpec::new("a", Cmp::None)],
        vec![PredicateSpec::new("a", Cmp::All)],
        vec![
            PredicateSpec::new("b", Cmp::None),
            PredicateSpec::new("a", Cmp::All),
        ],
    ];
    for predicates in cases {
        for projection in [None, Some(columns(&["c", "l"])), Some(columns(&["a", "s"]))] {
            let mut scan = filtered(predicates.clone());
            scan.projection = projection;
            assert_same_batches(&scan);
        }
    }
}

#[test]
fn predicates_with_selection_limit_and_offset() {
    let selection = RowSelection::from(
        (0..NUM_ROWS / 100)
            .flat_map(|_| [RowSelector::select(60), RowSelector::skip(40)])
            .collect::<Vec<_>>(),
    );
    for (offset, limit) in [(None, None), (None, Some(333)), (Some(211), Some(400))] {
        for selection in [None, Some(selection.clone())] {
            let mut scan = filtered(vec![PredicateSpec::new("b", Cmp::ModNotZero(3))]);
            scan.selection = selection;
            scan.offset = offset;
            scan.limit = limit;
            assert_same_batches(&scan);
        }
    }
}

#[test]
fn predicate_batch_smaller_than_page() {
    let mut scan = filtered(vec![PredicateSpec::new("b", Cmp::ModNotZero(5))]);
    scan.batch_size = Some(10);
    assert_same_batches(&scan);
}

/// The output reads a predicate column from the predicate cache.
#[test]
fn predicate_cache_is_used() {
    let metrics = ArrowReaderMetrics::enabled();
    let mut scan = filtered(vec![PredicateSpec::new("b", Cmp::ModNotZero(3))]);
    scan.projection = Some(columns(&["a", "b"]));
    let decoder = scan
        .builder()
        .with_metrics(metrics.clone())
        .with_fetch_granularity(FetchGranularity::Batch)
        .build()
        .unwrap();
    let (batches, _) = drive(decoder);
    assert!(!batches.is_empty());
    let from_cache = metrics.records_read_from_cache().unwrap();
    assert!(from_cache > 0, "from_cache={from_cache}");
}

/// A cache too small to hold a batch makes the output read the predicate
/// columns again, at cache batch boundaries.
#[test]
fn predicate_cache_misses() {
    for size in [0, 1] {
        let mut scan = filtered(vec![
            PredicateSpec::new("a", Cmp::ModNotZero(7)),
            PredicateSpec::new("b", Cmp::ModNotZero(3)),
        ]);
        scan.projection = Some(columns(&["a", "b", "c"]));
        scan.max_predicate_cache_size = Some(size);
        scan.selection = Some(RowSelection::from_consecutive_ranges(
            [10..20, 90..400, 1000..1001].into_iter(),
            NUM_ROWS,
        ));
        assert_same_batches(&scan);
    }
}

/// Filtering and output overlap: the first batch of a row group is returned
/// before the pages of later windows are requested.
#[test]
fn predicates_do_not_wait_for_the_row_group() {
    let mut decoder = filtered(vec![PredicateSpec::new("b", Cmp::ModNotZero(2))]).batch_decoder();
    let mut requested = vec![];
    let batch = loop {
        match decoder.try_decode().unwrap() {
            DecodeResult::NeedsData(ranges) => {
                let data = ranges.iter().map(fetch).collect();
                requested.extend(ranges.iter().cloned());
                decoder.push_ranges(ranges, data).unwrap();
            }
            DecodeResult::Data(batch) => break batch,
            DecodeResult::Finished => panic!("expected a batch"),
        }
    };
    assert_eq!(batch.num_rows(), 100);
    let bytes: u64 = requested.iter().map(|r| r.end - r.start).sum();
    // Half the rows pass, so the first batch needs the predicate column for
    // about 200 rows (plus one window, to know if more rows come) and the
    // output columns for about 200 rows.
    assert!(bytes * 2 < row_group_bytes(0), "{bytes}");
    let meta = metadata(true);
    let page_index = meta.metadata().page_index_for_row_group(0);
    // No page from row 300 on is requested.
    for column in 0..meta.metadata().row_group(0).num_columns() {
        for location in &page_index.page_locations(column).unwrap()[12..] {
            let start = location.offset as u64;
            assert!(requested.iter().all(|r| r.start != start), "{column}");
        }
    }
}

/// Resident bytes stay bounded with a selective predicate, including the
/// bytes of pages that were pushed ahead but that no row needs.
#[test]
fn predicates_release_bytes_pushed_ahead() {
    let mut decoder = filtered(vec![PredicateSpec::new("a", Cmp::Ge(550))]).batch_decoder();
    // Push the first row group, one buffer per page.
    let meta = metadata(true);
    for column in 0..meta.metadata().row_group(0).num_columns() {
        let ranges = page_ranges(0, column);
        let data = ranges.iter().map(fetch).collect();
        decoder.push_ranges(ranges, data).unwrap();
    }
    // Only rows 550.. of the first row group pass; the first batch holds
    // them and 50 rows of the second row group.
    let mut first = None;
    while first.is_none() {
        match decoder.try_decode().unwrap() {
            DecodeResult::NeedsData(ranges) => {
                // Every byte of the first row group was pushed ahead and is
                // kept until the decoder is done with it.
                let (row_group_1, _) = metadata(true)
                    .metadata()
                    .row_group(1)
                    .column(0)
                    .byte_range();
                assert!(ranges.iter().all(|r| r.start >= row_group_1), "{ranges:?}");
                let data = ranges.iter().map(fetch).collect();
                decoder.push_ranges(ranges, data).unwrap();
            }
            DecodeResult::Data(batch) => first = Some(batch),
            DecodeResult::Finished => panic!("expected a batch"),
        }
    }
    assert_eq!(first.unwrap().num_rows(), 50);
    // The first row group is done and every byte of it was released.
    assert!(decoder.is_at_row_group_boundary());
    assert_eq!(decoder.buffered_bytes(), 0);
}

// ---------------------------------------------------------------------------
// Randomized
// ---------------------------------------------------------------------------

fn random_selection(rng: &mut StdRng) -> RowSelection {
    let mut selectors = vec![];
    let mut rows_left = NUM_ROWS;
    let mut skip = rng.random_bool(0.5);
    let max_run = [3, 40, 400][rng.random_range(0..3)];
    while rows_left > 0 {
        let run = rng.random_range(1..=rows_left.min(max_run));
        selectors.push(if skip {
            RowSelector::skip(run)
        } else {
            RowSelector::select(run)
        });
        rows_left -= run;
        skip = !skip;
    }
    RowSelection::from(selectors)
}

fn random_predicate(rng: &mut StdRng) -> PredicateSpec {
    match rng.random_range(0..6) {
        0 => PredicateSpec::new("a", Cmp::Lt(rng.random_range(0..NUM_ROWS as i64))),
        1 => PredicateSpec::new("a", Cmp::Ge(rng.random_range(0..NUM_ROWS as i64))),
        2 => PredicateSpec::new("b", Cmp::ModNotZero(rng.random_range(2..7))),
        3 => PredicateSpec::new("a", Cmp::ModNotZero(rng.random_range(2..40))),
        4 => PredicateSpec::new("b", Cmp::All),
        _ => PredicateSpec::new("b", Cmp::None),
    }
}

/// Combine batch size, projection, row groups, row selection, selection
/// policy, predicates, predicate cache size, offset and limit at random, and
/// check that both granularities give the same batches.
#[test]
fn randomized_equivalence() {
    let projections = [
        None,
        Some(vec!["a"]),
        Some(vec!["a", "c"]),
        Some(vec!["b", "c"]),
        Some(vec!["l"]),
        Some(vec!["s", "b"]),
        Some(vec!["l", "a", "s"]),
    ];
    for seed in 0..300u64 {
        let mut rng = StdRng::seed_from_u64(seed);
        let num_predicates = rng.random_range(0..3);
        let scan = Scan {
            page_index_off: rng.random_bool(0.1),
            batch_size: Some([1, 7, 25, 64, 100, 512][rng.random_range(0..6)]),
            projection: projections[rng.random_range(0..projections.len())]
                .as_ref()
                .map(|names| columns(names)),
            row_groups: rng
                .random_bool(0.2)
                .then(|| vec![2, 0, 1][..rng.random_range(1..4)].to_vec()),
            selection: rng.random_bool(0.5).then(|| random_selection(&mut rng)),
            limit: rng.random_bool(0.3).then(|| rng.random_range(0..NUM_ROWS)),
            offset: rng.random_bool(0.3).then(|| rng.random_range(0..NUM_ROWS)),
            predicates: (0..num_predicates)
                .map(|_| random_predicate(&mut rng))
                .collect(),
            policy: [
                None,
                Some(RowSelectionPolicy::Selectors),
                Some(RowSelectionPolicy::Mask),
            ][rng.random_range(0..3)],
            max_predicate_cache_size: [None, Some(0), Some(1)][rng.random_range(0..3)],
        };
        // A selection must cover exactly the selected row groups.
        if scan.row_groups.is_some() && scan.selection.is_some() {
            continue;
        }
        assert_same_batches(&scan);
    }
}
