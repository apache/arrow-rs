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

//! Tests for the behavior that is specific to [`FetchGranularity::Batch`].
//!
//! The tests in [`equivalence_tests`](super::equivalence_tests) check that
//! batch mode returns the same batches as the row-group mode. The tests here
//! check:
//!
//! * Request shape: the decoder requests only the pages of the next batch
//!   ([`test_partial_pushes_across_batches`],
//!   [`test_predicates_do_not_wait_for_the_row_group`],
//!   [`test_dictionary_page_is_requested_once_per_column_chunk`],
//!   [`test_mask_policy_reads_only_loaded_pages`]).
//! * Memory release: the decoder releases the bytes that it no longer needs
//!   ([`test_full_scan_holds_about_one_batch`],
//!   [`test_releases_bytes_pushed_ahead`],
//!   [`test_releases_parts_of_one_buffer`],
//!   [`test_predicates_release_bytes_pushed_ahead`],
//!   [`test_sparse_predicate_releases_pages_of_empty_windows`]).
//! * The API: row group boundaries, `into_builder`, `try_next_reader` and
//!   configuration errors.

use super::equivalence_tests::{
    Cmp, NUM_ROWS, PredicateSpec, ROWS_PER_ROW_GROUP, Scan, TEST_FILE, columns, filtered,
    load_metadata, metadata, union,
};
use crate::DecodeResult;
use crate::arrow::ArrowWriter;
use crate::arrow::arrow_reader::metrics::ArrowReaderMetrics;
use crate::arrow::arrow_reader::{
    ArrowPredicateFn, RowFilter, RowSelection, RowSelectionPolicy, RowSelector,
};
use crate::arrow::push_decoder::{FetchGranularity, ParquetPushDecoder, ParquetPushDecoderBuilder};
use crate::file::metadata::PageIndexPolicy;
use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use bytes::Bytes;
use std::ops::{ControlFlow, Range};
use std::sync::Arc;

fn fetch(range: &Range<u64>) -> Bytes {
    TEST_FILE
        .data
        .slice(range.start as usize..range.end as usize)
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

/// What [`drive_with`] passes to its callback.
enum Event<'a> {
    /// The decoder requested these ranges, and they were pushed.
    Pushed(&'a [Range<u64>]),
    /// The decoder returned a batch.
    Batch(RecordBatch),
}

/// Decode `file`, pushing exactly the requested ranges, and call `on_event`
/// after each push and for each batch. Returns the value of the first
/// [`ControlFlow::Break`], or `None` when the decoder finishes.
fn drive_with<B>(
    decoder: &mut ParquetPushDecoder,
    file: &Bytes,
    mut on_event: impl FnMut(&mut ParquetPushDecoder, Event<'_>) -> ControlFlow<B>,
) -> Option<B> {
    loop {
        let event = match decoder.try_decode().unwrap() {
            DecodeResult::NeedsData(ranges) => {
                assert!(!ranges.is_empty());
                let data = ranges
                    .iter()
                    .map(|r| file.slice(r.start as usize..r.end as usize))
                    .collect();
                decoder.push_ranges(ranges.clone(), data).unwrap();
                on_event(decoder, Event::Pushed(&ranges))
            }
            DecodeResult::Data(batch) => on_event(decoder, Event::Batch(batch)),
            DecodeResult::Finished => return None,
        };
        if let ControlFlow::Break(value) = event {
            return Some(value);
        }
    }
}

/// Decode `file`, pushing exactly the requested ranges.
fn drive_file(mut decoder: ParquetPushDecoder, file: &Bytes) -> (Vec<RecordBatch>, Cost) {
    let mut batches = vec![];
    let mut cost = Cost::default();
    let mut last_buffered = 0;
    drive_with(&mut decoder, file, |decoder, event| {
        match event {
            Event::Pushed(ranges) => {
                cost.rounds += 1;
                cost.requested.extend_from_slice(ranges);
            }
            Event::Batch(batch) => {
                batches.push(batch);
                last_buffered = decoder.buffered_bytes();
            }
        }
        cost.peak_buffered = cost.peak_buffered.max(decoder.buffered_bytes());
        ControlFlow::<()>::Continue(())
    });
    // The last batch finishes the last row group, which releases its bytes.
    assert_eq!(last_buffered, 0, "bytes left after the last batch");
    (batches, cost)
}

/// Assert that the scan gives the same batches for both granularities, and
/// return the costs (row group, batch).
#[track_caller]
fn assert_same_batches(scan: &Scan) -> (Cost, Cost) {
    let (expected, row_group_cost) = drive_file(scan.row_group_decoder(), scan.data());
    let (actual, batch_cost) = drive_file(scan.batch_decoder(), scan.data());
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

/// A full scan asks for the same bytes as the row-group mode, in more and
/// smaller requests, and holds about one batch.
#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_full_scan_holds_about_one_batch() {
    let scan = Scan {
        batch_size: Some(100),
        ..Default::default()
    };
    let (_, batch) = assert_same_batches(&scan);
    assert!(batch.rounds > 3 * 3, "{}", batch.rounds);
    // The decoder holds about one batch (100 of 600 rows) plus the
    // dictionary pages, not the row group. A third of a row group leaves
    // margin for the pages that a batch shares with the next batch.
    const MAX_FRACTION_OF_ROW_GROUP: u64 = 3;
    assert!(
        batch.peak_buffered * MAX_FRACTION_OF_ROW_GROUP < row_group_bytes(0),
        "peak {} vs row group {}",
        batch.peak_buffered,
        row_group_bytes(0)
    );
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_without_offset_index_requests_column_chunks() {
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
#[cfg_attr(miri, ignore)] // Takes too long
fn test_dictionary_page_is_requested_once_per_column_chunk() {
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
#[cfg_attr(miri, ignore)] // Takes too long
fn test_partial_pushes_across_batches() {
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
    drive_with(&mut decoder, &TEST_FILE.data, |_, event| {
        match event {
            Event::Pushed(ranges) => {
                // Each request is for pages of this batch's rows only: with
                // 25 rows per page and 50 rows per batch, 2 pages per column.
                assert!(ranges.len() <= 4, "{ranges:?}");
                requests += 1;
            }
            Event::Batch(batch) => {
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
        }
        ControlFlow::<()>::Continue(())
    });
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
#[cfg_attr(miri, ignore)] // Takes too long
fn test_releases_bytes_pushed_ahead() {
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
    drive_with(&mut decoder, &TEST_FILE.data, |decoder, event| {
        match event {
            // Only the third row group was not pushed.
            Event::Pushed(ranges) => {
                assert!(rows >= 2 * ROWS_PER_ROW_GROUP, "{rows}: {ranges:?}")
            }
            Event::Batch(batch) => {
                rows += batch.num_rows();
                resident.push(decoder.buffered_bytes());
            }
        }
        ControlFlow::<()>::Continue(())
    });
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
#[cfg_attr(miri, ignore)] // Takes too long
fn test_releases_parts_of_one_buffer() {
    let mut decoder = Scan {
        batch_size: Some(100),
        ..Default::default()
    }
    .batch_decoder();
    let file = 0..TEST_FILE.data.len() as u64;
    decoder.push_range(file.clone(), fetch(&file)).unwrap();
    let initial = decoder.buffered_bytes();
    let mut previous = initial;
    let mut batches = 0;
    let mut rows = 0;
    drive_with(&mut decoder, &TEST_FILE.data, |decoder, event| {
        let Event::Batch(batch) = event else {
            panic!("the whole file was pushed");
        };
        // A batch can end inside a page, so a batch does not always release
        // bytes. It never adds bytes.
        let buffered = decoder.buffered_bytes();
        assert!(buffered <= previous, "{buffered} > {previous}");
        previous = buffered;
        rows += batch.num_rows();
        batches += 1;
        if batches == 2 {
            assert!(buffered < initial, "{buffered} >= {initial}");
        }
        ControlFlow::<()>::Continue(())
    });
    assert_eq!(rows, NUM_ROWS);
    assert_eq!(
        batches,
        ROWS_PER_ROW_GROUP.div_ceil(100) * NUM_ROWS / ROWS_PER_ROW_GROUP
    );
    assert_eq!(decoder.buffered_bytes(), 0);
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_row_group_boundary_is_visible_after_the_last_batch() {
    let mut decoder = Scan {
        batch_size: Some(250),
        ..Default::default()
    }
    .batch_decoder();
    let mut sizes = vec![];
    let mut boundaries = vec![];
    drive_with(&mut decoder, &TEST_FILE.data, |decoder, event| {
        if let Event::Batch(batch) = event {
            sizes.push(batch.num_rows());
            boundaries.push(decoder.is_at_row_group_boundary());
        }
        ControlFlow::<()>::Continue(())
    });
    assert_eq!(sizes, vec![250, 250, 100, 250, 250, 100, 250, 250, 100]);
    assert_eq!(
        boundaries,
        vec![false, false, true, false, false, true, false, false, true]
    );
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_into_builder_at_a_boundary() {
    let mut decoder = Scan {
        batch_size: Some(300),
        ..Default::default()
    }
    .batch_decoder();
    let mut batches = vec![];
    let mut collect = |decoder: &mut ParquetPushDecoder, event: Event<'_>| {
        if let Event::Batch(batch) = event {
            batches.push(batch);
            if decoder.is_at_row_group_boundary() {
                return ControlFlow::Break(());
            }
        }
        ControlFlow::Continue(())
    };
    // Decode the first row group.
    assert!(drive_with(&mut decoder, &TEST_FILE.data, &mut collect).is_some());
    // Skip the second row group, keep batch granularity.
    let mut decoder = decoder
        .into_builder()
        .unwrap()
        .with_row_groups(vec![2])
        .build()
        .unwrap();
    drive_with(&mut decoder, &TEST_FILE.data, &mut collect);
    let rows: Vec<i64> = batches
        .iter()
        .flat_map(|b| b.column(0).as_primitive::<Int64Type>().values().to_vec())
        .collect();
    let expected: Vec<i64> = (0..600).chain(1200..1800).collect();
    assert_eq!(rows, expected);
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_try_next_reader_at_boundaries_and_not_in_a_row_group() {
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

/// A row group started by `try_next_reader` must be finished by it.
#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_try_decode_in_a_row_group_of_try_next_reader() {
    let mut decoder = Scan::default().batch_decoder();
    let DecodeResult::NeedsData(ranges) = decoder.try_next_reader().unwrap() else {
        panic!("expected NeedsData");
    };
    let err = decoder.try_decode().unwrap_err().to_string();
    assert!(err.contains("try_next_reader"), "{err}");

    let data = ranges.iter().map(fetch).collect();
    decoder.push_ranges(ranges, data).unwrap();
    let DecodeResult::Data(reader) = decoder.try_next_reader().unwrap() else {
        panic!("expected a reader");
    };
    assert_eq!(reader.map(|b| b.unwrap().num_rows()).sum::<usize>(), 600);
}

// ---------------------------------------------------------------------------
// Predicates
// ---------------------------------------------------------------------------

/// A sparse predicate: no row passes for several windows, so the queue stays
/// empty, and the pages of the predicate column are released window by
/// window.
#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_sparse_predicate_releases_pages_of_empty_windows() {
    let mut scan = filtered(vec![PredicateSpec::new("a", Cmp::Ge(450))]);
    scan.batch_size = Some(100);
    scan.projection = Some(columns(&["a"]));
    scan.row_groups = Some(vec![0]);
    let (_, cost) = assert_same_batches(&scan);
    let (_, column_a) = metadata(true)
        .metadata()
        .row_group(0)
        .column(0)
        .byte_range();
    // The windows 0..400 pass no row. Without the release, the decoder holds
    // all pages of `a` up to row 500 before the first batch.
    assert!(
        cost.peak_buffered * 2 < column_a,
        "peak {} vs column {column_a}",
        cost.peak_buffered
    );
}

/// With [`RowSelectionPolicy::Mask`], a window decodes all rows of its
/// batch, but only from the pages that the decoder holds. The caller pushes
/// only the requested ranges, so the store is sparse: a read of a page that
/// is not loaded would fail.
#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_mask_policy_reads_only_loaded_pages() {
    let mut scan = filtered(vec![PredicateSpec::new("b", Cmp::ModNotZero(3))]);
    scan.projection = Some(columns(&["a", "b", "c"]));
    scan.policy = Some(RowSelectionPolicy::Mask);
    scan.row_groups = Some(vec![0]);
    scan.selection = Some(RowSelection::from_consecutive_ranges(
        [10..20, 330..340, 590..600].into_iter(),
        ROWS_PER_ROW_GROUP,
    ));
    let (_, cost) = assert_same_batches(&scan);
    // No page of `c` without a selected row is requested.
    let meta = metadata(true);
    let page_index = meta.metadata().page_index_for_row_group(0);
    let c = 2;
    let locations = page_index.page_locations(c).unwrap();
    let mut skipped = 0;
    for (i, location) in locations.iter().enumerate() {
        let first = location.first_row_index as usize;
        let end = locations
            .get(i + 1)
            .map_or(ROWS_PER_ROW_GROUP, |l| l.first_row_index as usize);
        if [10..20, 330..340, 590..600]
            .iter()
            .any(|r| r.start < end && first < r.end)
        {
            continue;
        }
        skipped += 1;
        let start = location.offset as u64;
        assert!(
            cost.requested
                .iter()
                .all(|r| r.start > start || r.end <= start),
            "page {i} of c was requested"
        );
    }
    assert!(skipped > 10, "{skipped}");
}

/// Filtering and output overlap: the first batch of a row group is returned
/// before the pages of later windows are requested.
#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_predicates_do_not_wait_for_the_row_group() {
    let mut decoder = filtered(vec![PredicateSpec::new("b", Cmp::ModNotZero(2))]).batch_decoder();
    let mut requested = vec![];
    let batch = drive_with(&mut decoder, &TEST_FILE.data, |_, event| match event {
        Event::Pushed(ranges) => {
            requested.extend_from_slice(ranges);
            ControlFlow::Continue(())
        }
        Event::Batch(batch) => ControlFlow::Break(batch),
    })
    .expect("expected a batch");
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

/// The output reads a predicate column from the predicate cache.
#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_predicate_cache_is_used() {
    let metrics = ArrowReaderMetrics::enabled();
    let mut scan = filtered(vec![PredicateSpec::new("b", Cmp::ModNotZero(3))]);
    scan.projection = Some(columns(&["a", "b"]));
    let decoder = scan
        .builder()
        .with_metrics(metrics.clone())
        .with_fetch_granularity(FetchGranularity::Batch)
        .build()
        .unwrap();
    let (batches, _) = drive_file(decoder, &TEST_FILE.data);
    // The output reads every row of `b` from the cache. A mask can read
    // more rows than it selects, so this is a lower bound.
    let output_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert!(output_rows > 0);
    let from_cache = metrics.records_read_from_cache().unwrap();
    assert!(from_cache >= output_rows, "{from_cache} < {output_rows}");
    // Only the predicate decodes `b`, one time per row. If the output
    // decoded `b` again, this would be more than `NUM_ROWS`.
    let from_inner = metrics.records_read_from_inner().unwrap();
    assert_eq!(from_inner, NUM_ROWS);
}

/// Resident bytes stay bounded with a selective predicate, including the
/// bytes of pages that were pushed ahead but that no row needs.
#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_predicates_release_bytes_pushed_ahead() {
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
    let (row_group_1, _) = meta.metadata().row_group(1).column(0).byte_range();
    let first = drive_with(&mut decoder, &TEST_FILE.data, |_, event| match event {
        Event::Pushed(ranges) => {
            // Every byte of the first row group was pushed ahead and is
            // kept until the decoder is done with it.
            assert!(ranges.iter().all(|r| r.start >= row_group_1), "{ranges:?}");
            ControlFlow::Continue(())
        }
        Event::Batch(batch) => ControlFlow::Break(batch),
    })
    .expect("expected a batch");
    assert_eq!(first.num_rows(), 50);
    // The first row group is done and every byte of it was released.
    assert!(decoder.is_at_row_group_boundary());
    assert_eq!(decoder.buffered_bytes(), 0);
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// A batch size of 0 is an error with batch granularity. Row-group
/// granularity accepts it.
#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_batch_size_zero_is_an_error() {
    for predicates in [vec![], vec![PredicateSpec::new("a", Cmp::All)]] {
        let scan = Scan {
            batch_size: Some(0),
            predicates,
            ..Default::default()
        };
        let err = scan
            .builder()
            .with_fetch_granularity(FetchGranularity::Batch)
            .build()
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Parquet error: batch_size must be greater than 0 with FetchGranularity::Batch"
        );
        scan.row_group_decoder();
    }
}

/// `try_decode` returns the error of a predicate.
#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_predicate_error_is_returned() {
    let predicate = ArrowPredicateFn::new(columns(&["a"]), |_batch: RecordBatch| {
        Err(arrow_schema::ArrowError::ComputeError(String::from(
            "predicate failed",
        )))
    });
    let mut decoder = Scan::default()
        .builder()
        .with_row_filter(RowFilter::new(vec![Box::new(predicate)]))
        .with_fetch_granularity(FetchGranularity::Batch)
        .build()
        .unwrap();
    let file = 0..TEST_FILE.data.len() as u64;
    decoder.push_range(file.clone(), fetch(&file)).unwrap();
    let err = decoder.try_decode().unwrap_err().to_string();
    assert!(err.contains("predicate failed"), "{err}");
}

/// A scan that reads a row group two times gives its batches two times. The
/// decoder requests the pages again for the second read.
#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_row_group_read_two_times() {
    for predicates in [vec![], vec![PredicateSpec::new("b", Cmp::ModNotZero(3))]] {
        let scan = Scan {
            batch_size: Some(100),
            row_groups: Some(vec![1, 0, 1]),
            predicates,
            ..Default::default()
        };
        let (row_group, _) = drive_file(scan.row_group_decoder(), &TEST_FILE.data);
        let (batch, _) = drive_file(scan.batch_decoder(), &TEST_FILE.data);
        assert_eq!(row_group, batch);
    }
}

/// A footer whose file row count is 0, but whose row groups have rows. The
/// batch size is clamped to 0, and `build` returns an error.
#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_batch_size_zero_with_a_wrong_file_row_count() {
    use crate::file::metadata::{FileMetaData, ParquetMetaDataBuilder};
    let metadata = metadata(false);
    let metadata = metadata.metadata();
    let file = metadata.file_metadata();
    let file = FileMetaData::new(
        file.version(),
        0,
        file.created_by().map(String::from),
        file.key_value_metadata().cloned(),
        file.schema_descr_ptr(),
        file.column_orders().cloned(),
    );
    let metadata = ParquetMetaDataBuilder::new(file)
        .set_row_groups(metadata.row_groups().to_vec())
        .build();
    let err = ParquetPushDecoderBuilder::try_new_decoder(Arc::new(metadata))
        .unwrap()
        .with_batch_size(1024)
        .with_fetch_granularity(FetchGranularity::Batch)
        .build()
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "Parquet error: batch_size must be greater than 0 with FetchGranularity::Batch"
    );
}

/// `with_batch_size` clamps the batch size to the file row count, so a file
/// without rows has a batch size of 0. That is not an error.
#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_batch_size_zero_without_rows() {
    let schema = TEST_FILE.batch.schema();
    let mut buffer = vec![];
    let writer = ArrowWriter::try_new(&mut buffer, schema, None).unwrap();
    writer.close().unwrap();
    let file = Bytes::from(buffer);
    let metadata = load_metadata(&file, PageIndexPolicy::Optional);
    let decoder = ParquetPushDecoderBuilder::new_with_metadata(metadata)
        .with_batch_size(1024)
        .with_fetch_granularity(FetchGranularity::Batch)
        .build()
        .unwrap();
    let (batches, _) = drive_file(decoder, &file);
    assert!(batches.is_empty());
}
