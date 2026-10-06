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

//! Tests for [`ScanPlan`].

use super::*;
use crate::DecodeResult;
use crate::arrow::arrow_reader::{
    ArrowPredicateFn, ArrowReaderMetadata, ArrowReaderOptions, RowFilter, RowSelector,
};
use crate::arrow::push_decoder::{
    ParquetPushDecoder, ParquetPushDecoderBuilder, RowGroupSelection,
};
use crate::arrow::{ArrowWriter, ProjectionMask};
use crate::column::writer::ColumnCloseResult;
use crate::file::metadata::PageIndexPolicy;
use crate::file::metadata::page_index::PageIndexBuilder;
use crate::file::properties::WriterProperties;
use crate::file::writer::SerializedFileWriter;
use arrow::compute::concat_batches;
use arrow::compute::kernels::cmp::{gt, lt};
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{ArrayRef, Int64Array, ListArray, RecordBatch, StringArray, StructArray};
use arrow_buffer::BooleanBuffer;
use arrow_schema::{DataType, Field};
use bytes::Bytes;
use std::sync::LazyLock;

/// Two row groups of 200 rows, 50 rows per data page, three columns:
/// `a` (Int64, plain), `b` (Int64, dictionary) and `c` (Utf8, dictionary).
static FILE: LazyLock<Bytes> = LazyLock::new(|| {
    let a: ArrayRef = Arc::new(Int64Array::from_iter_values(0..400));
    let b: ArrayRef = Arc::new(Int64Array::from_iter_values((0..400).map(|v| v % 7)));
    let c: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..400).map(|v| format!("value {}", v % 13)),
    ));
    let batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(200))
        .set_data_page_row_count_limit(50)
        // Page limits are checked between write batches.
        .set_write_batch_size(50)
        .set_column_dictionary_enabled("a".into(), false)
        .build();
    let mut buffer = vec![];
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    Bytes::from(buffer)
});

fn metadata(page_index: bool) -> ArrowReaderMetadata {
    let policy = if page_index {
        PageIndexPolicy::Required
    } else {
        PageIndexPolicy::Skip
    };
    let options = ArrowReaderOptions::new().with_page_index_policy(policy);
    ArrowReaderMetadata::load(&*FILE, options).unwrap()
}

fn builder() -> ParquetPushDecoderBuilder {
    ParquetPushDecoderBuilder::new_with_metadata(metadata(true)).with_batch_size(64)
}

fn fetch(range: &Range<u64>) -> Bytes {
    FILE.slice(range.start as usize..range.end as usize)
}

fn mask(leaves: impl IntoIterator<Item = usize>) -> ProjectionMask {
    ProjectionMask::leaves(metadata(true).parquet_schema(), leaves)
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

/// Returns `true` if every byte of `inner` is in `outer`.
fn covers(outer: &[Range<u64>], inner: &[Range<u64>]) -> bool {
    inner.iter().all(|range| {
        outer
            .iter()
            .any(|o| o.start <= range.start && range.end <= o.end)
    })
}

/// Decode the whole scan, supplying exactly the requested bytes. Returns
/// the requested ranges and the number of output rows.
fn demand(builder: ParquetPushDecoderBuilder) -> (Vec<Range<u64>>, u64) {
    demand_in(&FILE, builder)
}

/// [`demand`] for `file`.
fn demand_in(file: &Bytes, builder: ParquetPushDecoderBuilder) -> (Vec<Range<u64>>, u64) {
    let fetch = |range: &Range<u64>| file.slice(range.start as usize..range.end as usize);
    let mut decoder = builder.build().unwrap();
    let mut requested = vec![];
    let mut rows = 0;
    loop {
        match decoder.try_decode().unwrap() {
            DecodeResult::NeedsData(ranges) => {
                let data = ranges.iter().map(fetch).collect();
                requested.extend(ranges.iter().cloned());
                decoder.push_ranges(ranges, data).unwrap();
            }
            DecodeResult::Data(batch) => rows += batch.num_rows() as u64,
            DecodeResult::Finished => return (requested, rows),
        }
    }
}

/// Decode the whole scan. `supply` answers each
/// [`DecodeResult::NeedsData`]. Returns the output and the bytes that
/// the decoder still buffers when it finishes.
fn decode_with(
    mut decoder: ParquetPushDecoder,
    mut supply: impl FnMut(&mut ParquetPushDecoder, Vec<Range<u64>>),
) -> (Vec<RecordBatch>, u64) {
    let mut batches = vec![];
    loop {
        match decoder.try_decode().unwrap() {
            DecodeResult::NeedsData(ranges) => supply(&mut decoder, ranges),
            DecodeResult::Data(batch) => batches.push(batch),
            DecodeResult::Finished => return (batches, decoder.buffered_bytes()),
        }
    }
}

/// Collect `plan`, and the decoding stage that reads each of its ranges.
///
/// A [`PlannedRange`] does not give its stage, so this reads the stage from
/// the state of the planner.
fn collect_with_stages(mut plan: ScanPlan) -> (Vec<PlannedRange>, Vec<ScanStage>) {
    let mut ranges = vec![];
    let mut stages = vec![];
    while let Some(range) = plan.next() {
        let planner = plan.planner.as_ref().unwrap();
        let all_stages: Vec<_> = planner.columns.stages.stages().map(|(s, _)| s).collect();
        // The planner starts the next stage only when it is asked for a range
        // of it. Thus the stages that it has not started are the stages after
        // the stage of `range`.
        let later_stages = planner.current.as_ref().unwrap().stages.len();
        ranges.push(range);
        stages.push(all_stages[all_stages.len() - 1 - later_stages]);
    }
    (ranges, stages)
}

/// Check the documented order of `plan`, whose ranges are read by `stages`:
/// in each stage of a row group, first rows do not decrease, and the ranges
/// of each column chunk are in file order.
fn check_order(name: &str, plan: &[PlannedRange], stages: &[ScanStage]) {
    for (pair, stage) in plan.windows(2).zip(stages.windows(2)) {
        let (a, b) = (&pair[0], &pair[1]);
        if (a.row_group, stage[0]) == (b.row_group, stage[1]) {
            assert!(a.first_row <= b.first_row, "{name}: {a:?} before {b:?}");
        }
    }
    // A scan can read a row group more than once, so count each visit.
    let mut visit = 0;
    let mut last_start = std::collections::HashMap::new();
    for (idx, p) in plan.iter().enumerate() {
        if idx > 0 && plan[idx - 1].row_group != p.row_group {
            visit += 1;
        }
        if let Some(start) = last_start.insert((visit, p.column), p.range.start) {
            assert!(start < p.range.start, "{name}: {p:?} is not in file order");
        }
    }
}

/// See [`decode_from_plan_in`].
fn decode_from_plan(name: &str, builder: impl Fn() -> ParquetPushDecoderBuilder) {
    decode_from_plan_in(name, &FILE, builder)
}

/// Decode the whole scan of `file` from the plan, as a read-ahead caller
/// would: keep the planned ranges in a cache, and answer each
/// [`DecodeResult::NeedsData`] with exactly the requested ranges.
///
/// Panics if:
/// * a request is not in the planned ranges of the row groups planned so
///   far, after adding at most one more planned row group to the cache;
/// * the plan is not in the documented order;
/// * the decoder still buffers bytes when it finishes;
/// * the output differs from a scan that fetches the requested bytes.
fn decode_from_plan_in(name: &str, file: &Bytes, builder: impl Fn() -> ParquetPushDecoderBuilder) {
    let fetch = |range: &Range<u64>| file.slice(range.start as usize..range.end as usize);
    let (expected, _) = decode_with(builder().build().unwrap(), |decoder, ranges| {
        let data = ranges.iter().map(fetch).collect();
        decoder.push_ranges(ranges, data).unwrap();
    });

    let decoder = builder().build().unwrap();
    let (planned, stages) = collect_with_stages(decoder.scan_plan());
    check_order(name, &planned, &stages);
    let mut plan = planned.into_iter().peekable();
    let mut cache: Vec<Range<u64>> = vec![];
    let (actual, buffered) = decode_with(decoder, |decoder, requested| {
        if !covers(&union(cache.clone()), &requested) {
            let row_group = plan
                .peek()
                .unwrap_or_else(|| panic!("{name}: {requested:?} requested after the plan"))
                .row_group;
            while let Some(p) = plan.next_if(|p| p.row_group == row_group) {
                cache.push(p.range);
            }
        }
        assert!(
            covers(&union(cache.clone()), &requested),
            "{name}: {requested:?} is not in the plan of the next row group"
        );
        let data = requested.iter().map(fetch).collect();
        decoder.push_ranges(requested, data).unwrap();
    });
    assert_eq!(buffered, 0, "{name}: bytes still buffered");

    let Some(schema) = expected.first().map(|batch| batch.schema()) else {
        assert!(actual.is_empty(), "{name}: unexpected output");
        return;
    };
    assert_eq!(
        concat_batches(&schema, &actual).unwrap(),
        concat_batches(&schema, &expected).unwrap(),
        "{name}: output differs"
    );
}

/// Check the invariants of a plan for a scan without a row filter.
///
/// `builder` is called twice: once for the plan, once to decode.
fn check_unfiltered(
    name: &str,
    builder: impl Fn() -> ParquetPushDecoderBuilder,
) -> Vec<PlannedRange> {
    let (plan, stages) = collect_with_stages(builder().build().unwrap().scan_plan());
    let (requested, rows) = demand(builder());
    decode_from_plan(name, &builder);

    assert_eq!(
        union(plan.iter().map(|p| p.range.clone())),
        union(requested),
        "{name}: planned bytes differ from requested bytes"
    );
    for (p, stage) in plan.iter().zip(&stages) {
        assert_eq!(*stage, ScanStage::Projection, "{name}: {p:?}");
    }
    // Planned rows are output rows: for each column, the data pages tile
    // the output rows in order.
    let columns: std::collections::BTreeSet<_> = plan.iter().map(|p| p.column).collect();
    for column in columns {
        let mut next = 0;
        for p in plan
            .iter()
            .filter(|p| p.column == column && p.kind != PageKind::Dictionary)
        {
            assert_eq!(p.first_row, next, "{name}: gap before {p:?}");
            assert!(p.last_row > p.first_row, "{name}: empty {p:?}");
            next = p.last_row;
        }
        assert_eq!(next, rows, "{name}: column {column} rows");
    }
    plan
}

#[test]
fn plan_matches_demand_without_row_filter() {
    fn sel(selectors: Vec<RowSelector>) -> RowSelection {
        RowSelection::from(selectors)
    }
    type Case = (&'static str, Box<dyn Fn() -> ParquetPushDecoderBuilder>);
    let cases: Vec<Case> = vec![
        ("all", Box::new(builder)),
        (
            "projection",
            Box::new(|| builder().with_projection(mask([2]))),
        ),
        (
            "row groups reversed",
            Box::new(|| builder().with_row_groups(vec![1, 0])),
        ),
        (
            "global selection",
            Box::new(|| {
                builder().with_row_selection(sel(vec![
                    RowSelector::skip(60),
                    RowSelector::select(10),
                    RowSelector::skip(200),
                    RowSelector::select(30),
                ]))
            }),
        ),
        (
            "mask selection",
            Box::new(|| {
                builder().with_row_selection(RowSelection::from_boolean_buffer(
                    BooleanBuffer::from_iter((0..400).map(|i| i % 97 == 3)),
                ))
            }),
        ),
        (
            "per row group selections with duplicates",
            Box::new(|| {
                builder().with_row_group_selections(vec![
                    RowGroupSelection::new(
                        1,
                        Some(sel(vec![RowSelector::skip(150), RowSelector::select(10)])),
                    ),
                    RowGroupSelection::new(0, None),
                    RowGroupSelection::new(1, Some(sel(vec![RowSelector::select(5)]))),
                    RowGroupSelection::new(0, Some(sel(vec![RowSelector::skip(200)]))),
                ])
            }),
        ),
        (
            "offset and limit",
            Box::new(|| builder().with_offset(5).with_limit(12)),
        ),
        (
            "offset past first row group",
            Box::new(|| builder().with_offset(230).with_limit(60)),
        ),
        (
            "selection, offset and limit",
            Box::new(|| {
                builder()
                    .with_row_selection(sel(vec![
                        RowSelector::skip(190),
                        RowSelector::select(10),
                        RowSelector::select(100),
                    ]))
                    .with_offset(5)
                    .with_limit(12)
            }),
        ),
        ("limit zero", Box::new(|| builder().with_limit(0))),
        (
            "no page index",
            Box::new(|| {
                ParquetPushDecoderBuilder::new_with_metadata(metadata(false))
                    .with_row_selection(sel(vec![RowSelector::skip(60), RowSelector::select(10)]))
            }),
        ),
    ];
    for (name, builder) in cases {
        check_unfiltered(name, builder);
    }
}

#[test]
fn plan_has_pages_with_offset_index() {
    let plan = check_unfiltered("all", || builder().with_projection(mask([0, 1])));
    // Column a: 4 plain data pages per row group. Column b: a dictionary
    // page, then 4 data pages per row group.
    let kinds = |row_group, column| -> Vec<PageKind> {
        plan.iter()
            .filter(|p| p.row_group == row_group && p.column == column)
            .map(|p| p.kind)
            .collect()
    };
    assert_eq!(kinds(0, 0), vec![PageKind::Data; 4]);
    let mut expected = vec![PageKind::Dictionary];
    expected.extend([PageKind::Data; 4]);
    assert_eq!(kinds(1, 1), expected);

    // The dictionary page serves every row of its row group and comes
    // before the data pages.
    let dictionary = plan
        .iter()
        .position(|p| p.row_group == 1 && p.kind == PageKind::Dictionary)
        .unwrap();
    assert_eq!(
        (plan[dictionary].first_row, plan[dictionary].last_row),
        (200, 400)
    );
    assert!(plan[..dictionary].iter().all(|p| p.first_row < 200));

    // Plan order is decode order.
    assert!(plan.windows(2).all(|w| w[0].first_row <= w[1].first_row));
}

#[test]
fn plan_has_column_chunks_without_offset_index() {
    let plan = check_unfiltered("no page index", || {
        ParquetPushDecoderBuilder::new_with_metadata(metadata(false)).with_limit(250)
    });
    let entries: Vec<_> = plan
        .iter()
        .map(|p| (p.row_group, p.column, p.kind, p.first_row, p.last_row))
        .collect();
    let mut expected = vec![];
    for (row_group, rows) in [(0, 0..200), (1, 200..250)] {
        for column in 0..3 {
            expected.push((
                row_group,
                column,
                PageKind::ColumnChunk,
                rows.start,
                rows.end,
            ));
        }
    }
    assert_eq!(entries, expected);
}

#[test]
fn plan_tags_row_filter_stages() {
    // Predicate on `a`, then on `b`; output `a` and `c`.
    let filter = || {
        RowFilter::new(vec![
            Box::new(ArrowPredicateFn::new(mask([0]), |batch| {
                gt(batch.column(0), &Int64Array::new_scalar(100))
            })),
            Box::new(ArrowPredicateFn::new(mask([1]), |batch| {
                lt(batch.column(0), &Int64Array::new_scalar(3))
            })),
        ])
    };
    for limit in [None, Some(10)] {
        let builder = || {
            // The selection keeps a few rows of scattered pages, so the
            // cached predicate column `a` is fetched with its selection
            // expanded to batch boundaries.
            let builder = builder()
                .with_projection(mask([0, 2]))
                .with_row_selection(RowSelection::from(vec![
                    RowSelector::skip(70),
                    RowSelector::select(3),
                    RowSelector::skip(260),
                    RowSelector::select(20),
                ]))
                .with_row_filter(filter());
            match limit {
                Some(limit) => builder.with_limit(limit),
                None => builder,
            }
        };
        let (plan, stages) = collect_with_stages(builder().build().unwrap().scan_plan());
        let (requested, _) = demand(builder());
        assert!(
            covers(&union(plan.iter().map(|p| p.range.clone())), &requested),
            "limit {limit:?}: requested bytes are not planned"
        );
        decode_from_plan(&format!("row filter, limit {limit:?}"), builder);

        for (p, stage) in plan.iter().zip(&stages) {
            let expected_stage = match p.column {
                0 => ScanStage::Predicate(0),
                1 => ScanStage::Predicate(1),
                _ => ScanStage::Projection,
            };
            assert_eq!(*stage, expected_stage, "limit {limit:?}: {p:?}");
        }
        // Stages are in evaluation order within each row group, and the
        // offset/limit do not remove rows before the predicates run.
        for row_group in 0..2 {
            let stages: Vec<_> = plan
                .iter()
                .zip(&stages)
                .filter(|(p, _)| p.row_group == row_group)
                .map(|(_, stage)| *stage)
                .collect();
            assert!(stages.is_sorted(), "{stages:?}");
        }
        assert_eq!(plan.iter().map(|p| p.last_row).max(), Some(23));
    }
}

#[test]
fn plan_keeps_file_order_for_zero_row_pages() {
    // One predicate reads `a` and `b`, which are also in the output, so
    // the decoder caches them and fetches them with the selection
    // expanded to batch boundaries (64 rows). Rows 64..100 are in the
    // page of rows 50..100, which has no selected rows.
    let builder = || {
        builder()
            .with_projection(mask([0, 1]))
            .with_row_selection(RowSelection::from(vec![
                RowSelector::skip(100),
                RowSelector::select(10),
            ]))
            .with_row_filter(RowFilter::new(vec![Box::new(ArrowPredicateFn::new(
                mask([0, 1]),
                |batch| gt(batch.column(0), &Int64Array::new_scalar(100)),
            ))]))
    };
    let plan: Vec<_> = builder().build().unwrap().scan_plan().collect();
    let summary: Vec<_> = plan
        .iter()
        .map(|p| (p.column, p.kind, p.first_row..p.last_row))
        .collect();
    // In each column chunk, the page with no planned rows comes before
    // the next page, in file order, as the decoder reads them.
    assert_eq!(
        summary,
        vec![
            (1, PageKind::Dictionary, 0..10),
            (0, PageKind::Data, 0..0),
            (0, PageKind::Data, 0..10),
            (1, PageKind::Data, 0..0),
            (1, PageKind::Data, 0..10),
        ]
    );
    decode_from_plan("zero row pages", builder);
}

/// [`FILE`] without the offset index of the column chunks for which
/// `has_index(row_group, column)` is `false`.
fn file_with_partial_offset_index(has_index: impl Fn(usize, usize) -> bool) -> Bytes {
    let metadata = metadata(true);
    let metadata = metadata.metadata();
    let page_index = metadata.page_index().unwrap();
    let schema = metadata.file_metadata().schema_descr().root_schema_ptr();
    let mut buffer = vec![];
    let mut writer = SerializedFileWriter::new(&mut buffer, schema, Default::default()).unwrap();
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
            row_group_writer.append_column(&*FILE, close).unwrap();
        }
        row_group_writer.close().unwrap();
    }
    writer.close().unwrap();
    Bytes::from(buffer)
}

#[test]
fn plan_with_offset_index_for_some_columns() {
    // Column `b` (dictionary) of row group 0, and column `a` of row group
    // 1, have no offset index.
    let file = file_with_partial_offset_index(|row_group, column| {
        (row_group, column) != (0, 1) && (row_group, column) != (1, 0)
    });
    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional);
    let metadata = ArrowReaderMetadata::load(&file, options).unwrap();
    let filter = || {
        RowFilter::new(vec![Box::new(ArrowPredicateFn::new(mask([1]), |batch| {
            lt(batch.column(0), &Int64Array::new_scalar(3))
        }))])
    };
    let selection = || {
        RowSelection::from(vec![
            RowSelector::skip(60),
            RowSelector::select(10),
            RowSelector::skip(200),
            RowSelector::select(30),
        ])
    };
    let base =
        || ParquetPushDecoderBuilder::new_with_metadata(metadata.clone()).with_batch_size(64);
    type Case<'a> = (
        &'static str,
        Box<dyn Fn() -> ParquetPushDecoderBuilder + 'a>,
    );
    let cases: Vec<Case> = vec![
        ("all", Box::new(base)),
        (
            "selection",
            Box::new(|| base().with_row_selection(selection())),
        ),
        (
            "offset and limit",
            Box::new(|| base().with_offset(150).with_limit(100)),
        ),
        (
            "row filter and selection",
            Box::new(|| {
                base()
                    .with_row_selection(selection())
                    .with_row_filter(filter())
            }),
        ),
    ];
    for (name, builder) in cases {
        let plan: Vec<_> = builder().build().unwrap().scan_plan().collect();
        for p in &plan {
            let has_index = (p.row_group, p.column) != (0, 1) && (p.row_group, p.column) != (1, 0);
            assert_eq!(p.kind == PageKind::ColumnChunk, !has_index, "{name}: {p:?}");
        }
        let (requested, _) = demand_in(&file, builder());
        let planned = union(plan.iter().map(|p| p.range.clone()));
        if name.contains("filter") {
            assert!(covers(&planned, &requested), "{name}");
        } else {
            assert_eq!(planned, union(requested), "{name}");
        }
        decode_from_plan_in(name, &file, builder);
    }
}

/// Four row groups of 100 rows, 25 rows per data page, with nested
/// columns: `id` (Int64), `list` (List of Int64) and `s` (Struct of
/// Int64 `x` and Utf8 `y`). Leaves: 0 `id`, 1 `list.item`, 2 `s.x`,
/// 3 `s.y`.
static NESTED_FILE: LazyLock<Bytes> = LazyLock::new(|| {
    let id: ArrayRef = Arc::new(Int64Array::from_iter_values(0..400));
    let list: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(
        (0..400).map(|v| Some((0..v % 4).map(Some))),
    ));
    let s: ArrayRef = Arc::new(StructArray::from(vec![
        (
            Arc::new(Field::new("x", DataType::Int64, false)),
            Arc::new(Int64Array::from_iter_values((0..400).map(|v| v % 10))) as ArrayRef,
        ),
        (
            Arc::new(Field::new("y", DataType::Utf8, false)),
            Arc::new(StringArray::from_iter_values(
                (0..400).map(|v| format!("y {}", v % 7)),
            )) as ArrayRef,
        ),
    ]));
    let batch = RecordBatch::try_from_iter(vec![("id", id), ("list", list), ("s", s)]).unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(100))
        .set_data_page_row_count_limit(25)
        .set_write_batch_size(25)
        .build();
    let mut buffer = vec![];
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    Bytes::from(buffer)
});

#[test]
fn plan_for_nested_columns() {
    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
    let metadata = ArrowReaderMetadata::load(&*NESTED_FILE, options).unwrap();
    let leaves = |leaves: Vec<usize>| ProjectionMask::leaves(metadata.parquet_schema(), leaves);
    for limit in [None, Some(7), Some(150)] {
        for cache in [true, false] {
            let builder = || {
                let builder = ParquetPushDecoderBuilder::new_with_metadata(metadata.clone())
                    .with_batch_size(16)
                    .with_projection(leaves(vec![0, 1, 2, 3]))
                    .with_row_selection(RowSelection::from(vec![
                        RowSelector::skip(30),
                        RowSelector::select(40),
                        RowSelector::skip(150),
                        RowSelector::select(120),
                    ]))
                    .with_row_filter(RowFilter::new(vec![
                        Box::new(ArrowPredicateFn::new(leaves(vec![0]), |batch| {
                            gt(batch.column(0), &Int64Array::new_scalar(40))
                        })),
                        Box::new(ArrowPredicateFn::new(leaves(vec![2]), |batch| {
                            let s = batch.column(0).as_struct();
                            lt(s.column(0), &Int64Array::new_scalar(6))
                        })),
                    ]));
                let builder = match cache {
                    true => builder,
                    false => builder.with_max_predicate_cache_size(0),
                };
                match limit {
                    Some(limit) => builder.with_limit(limit),
                    None => builder,
                }
            };
            let name = format!("nested, limit {limit:?}, cache {cache}");
            let (plan, stages) = collect_with_stages(builder().build().unwrap().scan_plan());
            let (requested, _) = demand_in(&NESTED_FILE, builder());
            assert!(
                covers(&union(plan.iter().map(|p| p.range.clone())), &requested),
                "{name}: requested bytes are not planned"
            );
            // `s.y` is read only for the output.
            for (p, stage) in plan.iter().zip(&stages).filter(|(p, _)| p.column == 3) {
                assert_eq!(*stage, ScanStage::Projection, "{name}: {p:?}");
            }
            decode_from_plan_in(&name, &NESTED_FILE, builder);
        }
    }
}

/// With a row selection, the decoder requests nothing for a column chunk
/// whose offset index lists no pages. Without one, it requests the column
/// chunk. The plan does the same.
#[test]
fn plan_with_empty_offset_index() {
    let metadata = metadata(true);
    let parquet_metadata = metadata.metadata();
    let page_index = parquet_metadata.page_index().unwrap();
    let num_columns = parquet_metadata.row_group(0).columns().len();
    let mut page_index_builder =
        PageIndexBuilder::new(parquet_metadata.num_row_groups(), num_columns);
    for row_group in 0..parquet_metadata.num_row_groups() {
        for column in 0..num_columns {
            let mut offset_index = page_index.offset_index(row_group, column).unwrap().clone();
            if (row_group, column) == (0, 0) {
                offset_index.page_locations.clear();
            }
            page_index_builder.put_offset_index(offset_index, row_group, column);
        }
    }
    let parquet_metadata = parquet_metadata
        .as_ref()
        .clone()
        .into_builder()
        .set_page_index(Some(Arc::new(page_index_builder.build())))
        .build();
    let metadata =
        ArrowReaderMetadata::try_new(Arc::new(parquet_metadata), ArrowReaderOptions::new())
            .unwrap();
    let first_request = |builder: ParquetPushDecoderBuilder| {
        let mut decoder = builder.build().unwrap();
        let plan: Vec<_> = decoder.scan_plan().filter(|p| p.row_group == 0).collect();
        let DecodeResult::NeedsData(requested) = decoder.try_decode().unwrap() else {
            panic!("expected a request");
        };
        (plan, requested)
    };

    let base = || ParquetPushDecoderBuilder::new_with_metadata(metadata.clone());
    let (plan, requested) = first_request(base());
    assert!(
        plan.iter()
            .any(|p| p.column == 0 && p.kind == PageKind::ColumnChunk),
        "{plan:?}"
    );
    assert_eq!(union(plan.into_iter().map(|p| p.range)), union(requested));

    let (plan, requested) = first_request(base().with_row_selection(RowSelection::from(vec![
        RowSelector::skip(60),
        RowSelector::select(10),
    ])));
    assert!(plan.iter().all(|p| p.column != 0), "{plan:?}");
    assert_eq!(union(plan.into_iter().map(|p| p.range)), union(requested));
}

#[test]
fn plan_is_independent_of_buffers() {
    let mut decoder = builder().build().unwrap();
    let before: Vec<_> = decoder.scan_plan().collect();
    let file_range = 0..FILE.len() as u64;
    decoder
        .push_range(file_range.clone(), fetch(&file_range))
        .unwrap();
    assert_eq!(decoder.scan_plan().collect::<Vec<_>>(), before);
}

/// At every step of a scan, the plan covers every range that the decoder
/// requests later, also in the middle of a row group.
#[test]
fn plan_covers_later_requests_at_every_step() {
    let filter = || {
        RowFilter::new(vec![
            Box::new(ArrowPredicateFn::new(mask([0]), |batch| {
                gt(batch.column(0), &Int64Array::new_scalar(100))
            })),
            Box::new(ArrowPredicateFn::new(mask([1]), |batch| {
                lt(batch.column(0), &Int64Array::new_scalar(3))
            })),
        ])
    };
    type Case<'a> = (
        &'static str,
        Box<dyn Fn() -> ParquetPushDecoderBuilder + 'a>,
    );
    let cases: Vec<Case> = vec![
        ("all", Box::new(builder)),
        (
            "selection and limit",
            Box::new(|| {
                builder()
                    .with_row_selection(RowSelection::from(vec![
                        RowSelector::skip(60),
                        RowSelector::select(200),
                    ]))
                    .with_limit(150)
            }),
        ),
        (
            "row filter",
            Box::new(|| {
                builder()
                    .with_projection(mask([0, 2]))
                    .with_row_filter(filter())
            }),
        ),
    ];
    for (name, builder) in cases {
        let mut decoder = builder().build().unwrap();
        let mut plans = vec![];
        let mut requests = vec![];
        loop {
            plans.push(union(decoder.scan_plan().map(|p| p.range)));
            match decoder.try_decode().unwrap() {
                DecodeResult::NeedsData(ranges) => {
                    requests.push((plans.len() - 1, ranges.clone()));
                    let data = ranges.iter().map(fetch).collect();
                    decoder.push_ranges(ranges, data).unwrap();
                }
                DecodeResult::Data(_) => {}
                DecodeResult::Finished => break,
            }
        }
        assert!(requests.len() > 1, "{name}");
        for (step, plan) in plans.iter().enumerate() {
            for (_, ranges) in requests.iter().filter(|(at, _)| *at >= step) {
                assert!(
                    covers(plan, ranges),
                    "{name}: plan at step {step} misses {ranges:?}"
                );
            }
        }
        assert!(decoder.scan_plan().next().is_none(), "{name}: finished");
    }
}

#[test]
fn plan_at_boundary_is_rebuilt_decoder_plan() {
    let mut decoder = builder().build().unwrap();
    let file_range = 0..FILE.len() as u64;
    decoder
        .push_range(file_range.clone(), fetch(&file_range))
        .unwrap();
    let DecodeResult::Data(_) = decoder.try_next_reader().unwrap() else {
        panic!("expected a reader");
    };
    assert!(decoder.is_at_row_group_boundary());
    let plan: Vec<_> = decoder.scan_plan().collect();
    assert!(!plan.is_empty());
    assert!(plan.iter().all(|p| p.row_group == 1), "{plan:?}");
    // Rows count from the first row that the plan covers.
    assert_eq!(plan[0].first_row, 0);
    let decoder = decoder.into_builder().unwrap().build().unwrap();
    assert_eq!(decoder.scan_plan().collect::<Vec<_>>(), plan);
}

#[test]
fn plan_ends_before_invalid_row_group() {
    let plan: Vec<_> = builder()
        .with_row_groups(vec![0, 5])
        .build()
        .unwrap()
        .scan_plan()
        .collect();
    assert!(!plan.is_empty());
    assert!(plan.iter().all(|p| p.row_group == 0), "{plan:?}");
}

#[test]
fn plan_is_lazy() {
    let decoder = builder().build().unwrap();
    let mut plan = decoder.scan_plan();
    let first = plan.next().unwrap();
    assert_eq!(first.row_group, 0);
    // Only the first row group has been started, and only the first
    // stage of it.
    let planner = plan.planner.as_ref().unwrap();
    let current = planner.current.as_ref().unwrap();
    assert_eq!(current.row_group.row_group_idx, 0);
    assert_eq!(planner.next_row, 200);
    // One cursor per projected column, not one entry per page.
    assert_eq!(current.stage.as_ref().unwrap().cursors.len(), 3);
}

#[test]
fn selected_rows_counts_selected_positions() {
    let selection = RowSelection::from(vec![
        RowSelector::skip(10),
        RowSelector::select(5),
        RowSelector::skip(3),
        RowSelector::select(2),
    ]);
    let rows = SelectedRows::new(Some(&selection), 30);
    let counts: Vec<_> = [0, 10, 12, 15, 18, 19, 20, 30]
        .into_iter()
        .map(|raw| rows.selected_before(raw))
        .collect();
    assert_eq!(counts, vec![0, 0, 2, 5, 5, 6, 7, 7]);
    assert_eq!(SelectedRows::new(None, 30).selected_before(30), 30);
}
