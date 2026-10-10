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

//! Differential tests for [`ParquetPushDecoder`].
//!
//! Each test describes a scan as a [`Scan`], and [`assert_same_rows`] checks
//! that the push decoder returns the same rows, in the same order, as
//! [`ParquetRecordBatchReader`] for the same scan. The `fuzz_*` tests combine
//! the scan options at random.
//!
//! [`ParquetRecordBatchReader`]: crate::arrow::arrow_reader::ParquetRecordBatchReader

use crate::DecodeResult;
use crate::arrow::arrow_reader::{
    ArrowPredicate, ArrowPredicateFn, ArrowReaderMetadata, ArrowReaderOptions,
    ParquetRecordBatchReaderBuilder, RowFilter, RowSelection, RowSelectionPolicy, RowSelector,
};
use crate::arrow::push_decoder::{ParquetPushDecoder, ParquetPushDecoderBuilder};
use crate::arrow::{ArrowWriter, ProjectionMask};
use crate::file::metadata::PageIndexPolicy;
use crate::file::properties::WriterProperties;
use crate::schema::types::SchemaDescriptor;
use arrow_array::builder::{Int64Builder, ListBuilder};
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Int64Array, RecordBatch, RecordBatchReader, StringArray,
    StructArray,
};
use arrow_schema::{DataType, Field, SchemaRef};
use arrow_select::concat::concat_batches;
use bytes::Bytes;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{RngExt, SeedableRng};
use std::ops::Range;
use std::sync::{Arc, LazyLock};

const ROWS_PER_ROW_GROUP: usize = 600;
const ROWS_PER_PAGE: usize = 25;
const NUM_ROWS: usize = 1800;

/// Three row groups of 600 rows, with a new data page each 25 rows or less,
/// columns:
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

/// Row groups of [`HETEROGENEOUS_FILE`].
const HETEROGENEOUS_ROW_GROUP_ROWS: [usize; 3] = [700, 700, 400];

/// The rows of [`TEST_FILE`] in row groups of 700, 700 and 400 rows. A page
/// ends after 40 rows or at about 200 bytes (checked every 5 rows), so each
/// column has its own page boundaries.
static HETEROGENEOUS_FILE: LazyLock<Bytes> = LazyLock::new(|| {
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(HETEROGENEOUS_ROW_GROUP_ROWS[0]))
        .set_data_page_row_count_limit(40)
        .set_data_page_size_limit(200)
        .set_write_batch_size(5)
        .set_dictionary_enabled(false)
        .set_column_dictionary_enabled("b".into(), true)
        .build();
    let mut buffer = vec![];
    let mut writer =
        ArrowWriter::try_new(&mut buffer, TEST_FILE.batch.schema(), Some(props)).unwrap();
    writer.write(&TEST_FILE.batch).unwrap();
    writer.close().unwrap();
    Bytes::from(buffer)
});

fn load_metadata(data: &Bytes, policy: PageIndexPolicy) -> ArrowReaderMetadata {
    let options = ArrowReaderOptions::new().with_page_index_policy(policy);
    ArrowReaderMetadata::load(data, options).unwrap()
}

static WITH_PAGE_INDEX: LazyLock<ArrowReaderMetadata> =
    LazyLock::new(|| load_metadata(&TEST_FILE.data, PageIndexPolicy::Required));
static WITHOUT_PAGE_INDEX: LazyLock<ArrowReaderMetadata> =
    LazyLock::new(|| load_metadata(&TEST_FILE.data, PageIndexPolicy::Skip));
static HETEROGENEOUS_WITH_PAGE_INDEX: LazyLock<ArrowReaderMetadata> =
    LazyLock::new(|| load_metadata(&HETEROGENEOUS_FILE, PageIndexPolicy::Required));
static HETEROGENEOUS_WITHOUT_PAGE_INDEX: LazyLock<ArrowReaderMetadata> =
    LazyLock::new(|| load_metadata(&HETEROGENEOUS_FILE, PageIndexPolicy::Skip));

fn metadata(page_index: bool) -> ArrowReaderMetadata {
    if page_index {
        WITH_PAGE_INDEX.clone()
    } else {
        WITHOUT_PAGE_INDEX.clone()
    }
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

/// Decode `file`, pushing exactly the requested ranges. Returns the batches
/// and every range that the decoder requested, in order.
fn drive_file(
    mut decoder: ParquetPushDecoder,
    file: &Bytes,
) -> (Vec<RecordBatch>, Vec<Range<u64>>) {
    let mut batches = vec![];
    let mut requested = vec![];
    let mut buffered_after_last_batch = 0;
    loop {
        match decoder.try_decode().unwrap() {
            DecodeResult::NeedsData(ranges) => {
                assert!(!ranges.is_empty());
                let data = ranges
                    .iter()
                    .map(|r| file.slice(r.start as usize..r.end as usize))
                    .collect();
                requested.extend_from_slice(&ranges);
                decoder.push_ranges(ranges, data).unwrap();
            }
            DecodeResult::Data(batch) => {
                batches.push(batch);
                buffered_after_last_batch = decoder.buffered_bytes();
            }
            DecodeResult::Finished => break,
        }
    }
    // The last batch finishes the last row group, which releases its bytes.
    assert_eq!(
        buffered_after_last_batch, 0,
        "bytes left after the last batch"
    );
    (batches, requested)
}

/// A predicate on one column: an Int64 column unless noted.
#[derive(Clone, Copy, Debug)]
enum Cmp {
    Lt(i64),
    Ge(i64),
    ModNotZero(i64),
    All,
    None,
    /// The list column `l` is not null.
    ListNotNull,
    /// Field `x` of the struct column `s` is less than the value.
    StructXLt(i64),
    /// Null where the value is a multiple of the argument, else true.
    NullEvery(i64),
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
                let column = batch.column(0);
                Ok(match cmp {
                    Cmp::ListNotNull => {
                        let list = column.as_list::<i32>();
                        (0..list.len()).map(|i| Some(!list.is_null(i))).collect()
                    }
                    Cmp::StructXLt(x) => column
                        .as_struct()
                        .column(0)
                        .as_primitive::<Int64Type>()
                        .iter()
                        .map(|v| Some(v.unwrap() < x))
                        .collect(),
                    Cmp::NullEvery(m) => column
                        .as_primitive::<Int64Type>()
                        .iter()
                        .map(|v| (v.unwrap() % m != 0).then_some(true))
                        .collect(),
                    _ => column
                        .as_primitive::<Int64Type>()
                        .iter()
                        .map(|v| {
                            let v = v.unwrap();
                            Some(match cmp {
                                Cmp::Lt(x) => v < x,
                                Cmp::Ge(x) => v >= x,
                                Cmp::ModNotZero(x) => v % x != 0,
                                Cmp::All => true,
                                Cmp::None => false,
                                _ => unreachable!(),
                            })
                        })
                        .collect::<BooleanArray>(),
                })
            },
        ))
    }
}

/// Apply the options of a [`Scan`] to an `ArrowReaderBuilder`. A macro,
/// because the builders of the two readers have different types.
macro_rules! apply_options {
    ($scan:expr, $builder:expr) => {{
        let scan: &Scan = $scan;
        let mut builder = $builder;
        if let Some(batch_size) = scan.batch_size {
            builder = builder.with_batch_size(batch_size);
        }
        if let Some(projection) = &scan.projection {
            builder = builder.with_projection(projection.clone());
        }
        if let Some(row_groups) = &scan.row_groups {
            builder = builder.with_row_groups(row_groups.clone());
        }
        if let Some(selection) = &scan.selection {
            builder = builder.with_row_selection(selection.clone());
        }
        if let Some(limit) = scan.limit {
            builder = builder.with_limit(limit);
        }
        if let Some(offset) = scan.offset {
            builder = builder.with_offset(offset);
        }
        if let Some(policy) = scan.policy {
            builder = builder.with_row_selection_policy(policy);
        }
        if let Some(size) = scan.max_predicate_cache_size {
            builder = builder.with_max_predicate_cache_size(size);
        }
        if !scan.predicates.is_empty() {
            let predicates = scan.predicates.iter().map(|p| p.build()).collect();
            builder = builder.with_row_filter(RowFilter::new(predicates));
        }
        builder
    }};
}

/// A scan configuration, so that the same scan can be built for more than
/// one reader.
#[derive(Clone, Debug, Default)]
struct Scan {
    /// Read [`HETEROGENEOUS_FILE`] instead of [`TEST_FILE`].
    heterogeneous: bool,
    page_index_off: bool,
    batch_size: Option<usize>,
    projection: Option<ProjectionMask>,
    row_groups: Option<Vec<usize>>,
    selection: Option<RowSelection>,
    limit: Option<usize>,
    offset: Option<usize>,
    /// Built again for each reader, as predicates have state.
    predicates: Vec<PredicateSpec>,
    policy: Option<RowSelectionPolicy>,
    max_predicate_cache_size: Option<usize>,
}

impl Scan {
    fn metadata(&self) -> ArrowReaderMetadata {
        match (self.heterogeneous, self.page_index_off) {
            (false, _) => metadata(!self.page_index_off),
            (true, false) => HETEROGENEOUS_WITH_PAGE_INDEX.clone(),
            (true, true) => HETEROGENEOUS_WITHOUT_PAGE_INDEX.clone(),
        }
    }

    fn data(&self) -> &'static Bytes {
        if self.heterogeneous {
            &HETEROGENEOUS_FILE
        } else {
            &TEST_FILE.data
        }
    }

    fn push_decoder(&self) -> ParquetPushDecoder {
        let builder = ParquetPushDecoderBuilder::new_with_metadata(self.metadata());
        apply_options!(self, builder).build().unwrap()
    }

    /// The batches of the scan from the sync reader.
    fn sync_batches(&self) -> (SchemaRef, Vec<RecordBatch>) {
        let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
            self.data().clone(),
            self.metadata(),
        );
        let reader = apply_options!(self, builder).build().unwrap();
        let schema = reader.schema();
        (schema, reader.map(|batch| batch.unwrap()).collect())
    }
}

/// What the push decoder did for a scan.
struct Decoded {
    /// Rows returned.
    rows: usize,
    /// Every range requested, in order.
    requested: Vec<Range<u64>>,
}

/// Assert that the push decoder gives the same rows as the sync reader for
/// `scan`, in the same order.
///
/// The batch boundaries can differ: the push decoder ends a batch at the end
/// of each row group.
#[track_caller]
fn assert_same_rows(scan: &Scan) -> Decoded {
    let (schema, expected) = scan.sync_batches();
    let (actual, requested) = drive_file(scan.push_decoder(), scan.data());
    if let Some(batch_size) = scan.batch_size {
        assert!(
            actual.iter().all(|b| b.num_rows() <= batch_size),
            "a batch has more than {batch_size} rows for {scan:?}"
        );
    }
    let expected = concat_batches(&schema, &expected).unwrap();
    let actual = concat_batches(&schema, &actual).unwrap();
    assert_eq!(actual, expected, "rows differ for {scan:?}");
    Decoded {
        rows: actual.num_rows(),
        requested,
    }
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_batch_sizes_and_projections() {
    for batch_size in [1, 7, 24, 25, 26, 100, 599, 600, 601, 5000] {
        for projection in [
            None,
            Some(columns(&["a"])),
            Some(columns(&["c", "b"])),
            Some(columns(&["l"])),
            Some(columns(&["s"])),
            Some(columns(&["l", "s", "b"])),
        ] {
            assert_same_rows(&Scan {
                batch_size: Some(batch_size),
                projection,
                ..Default::default()
            });
        }
    }
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_row_selections_skip_pages() {
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
                assert_same_rows(&Scan {
                    batch_size: Some(batch_size),
                    selection: Some(selection.clone()),
                    policy,
                    ..Default::default()
                });
            }
        }
    }
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_offset_and_limit() {
    for (offset, limit) in [
        (None, Some(1)),
        (Some(650), Some(500)),
        (Some(599), None),
        (Some(1799), Some(10)),
        (Some(2000), None),
        (None, Some(0)),
    ] {
        assert_same_rows(&Scan {
            batch_size: Some(70),
            offset,
            limit,
            ..Default::default()
        });
    }
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_empty_selection_requests_nothing() {
    let decoded = assert_same_rows(&Scan {
        batch_size: Some(64),
        selection: Some(RowSelection::from(vec![RowSelector::skip(NUM_ROWS)])),
        ..Default::default()
    });
    assert_eq!(decoded.rows, 0);
    assert!(decoded.requested.is_empty());
}

fn filtered(predicates: Vec<PredicateSpec>) -> Scan {
    Scan {
        batch_size: Some(100),
        predicates,
        ..Default::default()
    }
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_predicates() {
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
            assert_same_rows(&scan);
        }
    }
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_predicates_with_selection_limit_and_offset() {
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
            assert_same_rows(&scan);
        }
    }
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_predicate_batch_smaller_than_page() {
    let mut scan = filtered(vec![PredicateSpec::new("b", Cmp::ModNotZero(5))]);
    scan.batch_size = Some(10);
    assert_same_rows(&scan);
}

/// A random selection of `total` rows.
fn random_selection(rng: &mut StdRng, total: usize) -> RowSelection {
    let mut selectors = vec![];
    let mut rows_left = total;
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
    let value = rng.random_range(0..NUM_ROWS as i64);
    match rng.random_range(0..9) {
        0 => PredicateSpec::new("a", Cmp::Lt(value)),
        1 => PredicateSpec::new("a", Cmp::Ge(value)),
        2 => PredicateSpec::new("b", Cmp::ModNotZero(rng.random_range(2..7))),
        3 => PredicateSpec::new("a", Cmp::ModNotZero(rng.random_range(2..40))),
        4 => PredicateSpec::new("b", Cmp::All),
        5 => PredicateSpec::new("l", Cmp::ListNotNull),
        6 => PredicateSpec::new("s", Cmp::StructXLt(2 * value)),
        7 => PredicateSpec::new("a", Cmp::NullEvery(rng.random_range(2..9))),
        _ => PredicateSpec::new("b", Cmp::None),
    }
}

/// A scan with batch size, projection, row groups, row selection, selection
/// policy, predicates, predicate cache size, offset and limit at random.
fn random_scan(rng: &mut StdRng, heterogeneous: bool) -> Scan {
    let projections = [
        None,
        Some(vec!["a"]),
        Some(vec!["c"]),
        Some(vec!["a", "c"]),
        Some(vec!["b", "c"]),
        Some(vec!["l"]),
        Some(vec!["s", "b"]),
        Some(vec!["l", "a", "s"]),
    ];
    let row_group_rows = match heterogeneous {
        true => HETEROGENEOUS_ROW_GROUP_ROWS,
        false => [ROWS_PER_ROW_GROUP; 3],
    };
    // Some of the row groups, in any order.
    let row_groups = rng.random_bool(0.3).then(|| {
        let mut row_groups = vec![0, 1, 2];
        row_groups.shuffle(rng);
        row_groups.truncate(rng.random_range(1..=3));
        row_groups
    });
    // A selection must cover exactly the row groups of the scan.
    let total: usize = match &row_groups {
        Some(row_groups) => row_groups.iter().map(|&i| row_group_rows[i]).sum(),
        None => NUM_ROWS,
    };
    let num_predicates = rng.random_range(0..4);
    Scan {
        heterogeneous,
        page_index_off: rng.random_bool(0.1),
        batch_size: [
            None,
            Some(1),
            Some(7),
            Some(25),
            Some(64),
            Some(100),
            Some(512),
        ][rng.random_range(0..7)],
        projection: projections[rng.random_range(0..projections.len())]
            .as_ref()
            .map(|names| columns(names)),
        row_groups,
        selection: rng.random_bool(0.5).then(|| random_selection(rng, total)),
        limit: rng.random_bool(0.3).then(|| rng.random_range(1..=total)),
        offset: rng.random_bool(0.3).then(|| rng.random_range(0..total / 2)),
        predicates: (0..num_predicates).map(|_| random_predicate(rng)).collect(),
        policy: [
            None,
            Some(RowSelectionPolicy::Selectors),
            Some(RowSelectionPolicy::Mask),
        ][rng.random_range(0..3)],
        max_predicate_cache_size: [None, Some(0), Some(1)][rng.random_range(0..3)],
    }
}

/// Seeds of each fuzz test, so that it runs in a few seconds in a debug
/// build.
const FUZZ_SEEDS: u64 = 250;

/// Check [`FUZZ_SEEDS`] random scans. Most of the scans must return rows, so
/// that the test does not compare empty results only.
fn fuzz(heterogeneous: bool) {
    let mut scans_with_rows = 0;
    for seed in 0..FUZZ_SEEDS {
        let mut rng = StdRng::seed_from_u64(seed);
        let scan = random_scan(&mut rng, heterogeneous);
        if assert_same_rows(&scan).rows > 0 {
            scans_with_rows += 1;
        }
    }
    assert!(
        scans_with_rows * 3 >= FUZZ_SEEDS * 2,
        "only {scans_with_rows} of {FUZZ_SEEDS} scans returned rows"
    );
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_fuzz_equivalence() {
    fuzz(false);
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_fuzz_equivalence_heterogeneous_pages() {
    fuzz(true);
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_page_boundaries_differ_per_column() {
    let metadata = HETEROGENEOUS_WITH_PAGE_INDEX.metadata();
    let row_groups: Vec<i64> = metadata
        .row_groups()
        .iter()
        .map(|rg| rg.num_rows())
        .collect();
    assert_eq!(row_groups, [700, 700, 400]);
    // Pages per column in row group 0: a, b, c, l, s.x, s.y
    let page_index = metadata.page_index_for_row_group(0);
    let pages: Vec<usize> = (0..6)
        .map(|column| page_index.page_locations(column).unwrap().len())
        .collect();
    assert!(
        pages[0] != pages[2] && pages[1] != pages[2] && pages[0] != pages[3],
        "{pages:?}"
    );
}
