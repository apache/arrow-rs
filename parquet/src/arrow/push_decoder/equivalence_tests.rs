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
//! [`ParquetRecordBatchReader`] for the same scan, and that
//! [`FetchGranularity::Batch`] returns the same batches as the default
//! [`FetchGranularity::RowGroup`]. The `test_fuzz_*` tests use [`proptest`] to
//! combine the scan options at random and to shrink a scan that fails.
//!
//! [`ParquetRecordBatchReader`]: crate::arrow::arrow_reader::ParquetRecordBatchReader

use crate::DecodeResult;
use crate::arrow::arrow_reader::{
    ArrowPredicate, ArrowPredicateFn, ArrowReaderMetadata, ArrowReaderOptions,
    ParquetRecordBatchReaderBuilder, RowFilter, RowSelection, RowSelectionPolicy, RowSelector,
};
use crate::arrow::push_decoder::{FetchGranularity, ParquetPushDecoder, ParquetPushDecoderBuilder};
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
use proptest::collection::vec;
use proptest::option::weighted;
use proptest::prelude::*;
use proptest::sample::select;
use proptest::test_runner::{Config, TestRng, TestRunner};
use std::cell::Cell;
use std::ops::Range;
use std::sync::{Arc, LazyLock};

pub(super) const ROWS_PER_ROW_GROUP: usize = 600;
pub(super) const ROWS_PER_PAGE: usize = 25;
pub(super) const NUM_ROWS: usize = 1800;

/// Three row groups of 600 rows, with a new data page each 25 rows or less,
/// columns:
///
/// * `a`: 0, 1, 2, ... (plain)
/// * `b`: `a % 10` (dictionary)
/// * `c`: a string (plain)
/// * `l`: a list of 0 to 3 `a` values, some lists null (plain)
/// * `s`: a struct of `a * 2` and `a % 7` (plain)
pub(super) struct TestFile {
    pub(super) data: Bytes,
    pub(super) batch: RecordBatch,
}

pub(super) static TEST_FILE: LazyLock<TestFile> = LazyLock::new(|| {
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

pub(super) fn load_metadata(data: &Bytes, policy: PageIndexPolicy) -> ArrowReaderMetadata {
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

pub(super) fn metadata(page_index: bool) -> ArrowReaderMetadata {
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

pub(super) fn columns(names: &[&str]) -> ProjectionMask {
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
pub(super) enum Cmp {
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
pub(super) struct PredicateSpec {
    column: &'static str,
    cmp: Cmp,
}

impl PredicateSpec {
    pub(super) fn new(column: &'static str, cmp: Cmp) -> Self {
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
pub(super) struct Scan {
    /// Read [`HETEROGENEOUS_FILE`] instead of [`TEST_FILE`].
    pub(super) heterogeneous: bool,
    pub(super) page_index_off: bool,
    pub(super) batch_size: Option<usize>,
    pub(super) projection: Option<ProjectionMask>,
    pub(super) row_groups: Option<Vec<usize>>,
    pub(super) selection: Option<RowSelection>,
    pub(super) limit: Option<usize>,
    pub(super) offset: Option<usize>,
    /// Built again for each reader, as predicates have state.
    pub(super) predicates: Vec<PredicateSpec>,
    pub(super) policy: Option<RowSelectionPolicy>,
    pub(super) max_predicate_cache_size: Option<usize>,
}

impl Scan {
    fn metadata(&self) -> ArrowReaderMetadata {
        match (self.heterogeneous, self.page_index_off) {
            (false, _) => metadata(!self.page_index_off),
            (true, false) => HETEROGENEOUS_WITH_PAGE_INDEX.clone(),
            (true, true) => HETEROGENEOUS_WITHOUT_PAGE_INDEX.clone(),
        }
    }

    pub(super) fn data(&self) -> &'static Bytes {
        if self.heterogeneous {
            &HETEROGENEOUS_FILE
        } else {
            &TEST_FILE.data
        }
    }

    pub(super) fn builder(&self) -> ParquetPushDecoderBuilder {
        let builder = ParquetPushDecoderBuilder::new_with_metadata(self.metadata());
        apply_options!(self, builder)
    }

    /// A decoder with [`FetchGranularity::RowGroup`], the default.
    pub(super) fn row_group_decoder(&self) -> ParquetPushDecoder {
        self.builder().build().unwrap()
    }

    /// A decoder with [`FetchGranularity::Batch`].
    pub(super) fn batch_decoder(&self) -> ParquetPushDecoder {
        self.builder()
            .with_fetch_granularity(FetchGranularity::Batch)
            .build()
            .unwrap()
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

/// Sort and merge ranges into disjoint, non-adjacent ranges.
pub(super) fn union(ranges: impl IntoIterator<Item = Range<u64>>) -> Vec<Range<u64>> {
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
    let (actual, requested) = drive_file(scan.row_group_decoder(), scan.data());
    // `FetchGranularity::Batch` gives the same batches, with the same
    // boundaries.
    let (batch_mode, mut batch_mode_requested) = drive_file(scan.batch_decoder(), scan.data());
    // Do not print the batches: proptest runs a failing scan many times to
    // shrink it.
    assert!(batch_mode == actual, "batch mode differs for {scan:?}");
    // Without predicates, both modes read the same bytes. Batch mode asks for
    // them in more and smaller requests.
    if scan.predicates.is_empty() {
        assert_eq!(
            union(batch_mode_requested.clone()),
            union(requested.clone()),
            "batch mode read other bytes for {scan:?}"
        );
    }
    // A range requested twice means the decoder released it too early.
    let num_requested = batch_mode_requested.len();
    batch_mode_requested.sort_by_key(|r| (r.start, r.end));
    batch_mode_requested.dedup();
    assert_eq!(
        batch_mode_requested.len(),
        num_requested,
        "batch mode requested a range twice for {scan:?}"
    );
    if let Some(batch_size) = scan.batch_size {
        assert!(
            actual.iter().all(|b| b.num_rows() <= batch_size),
            "a batch has more than {batch_size} rows for {scan:?}"
        );
    }
    let expected = concat_batches(&schema, &expected).unwrap();
    let actual = concat_batches(&schema, &actual).unwrap();
    assert!(
        actual == expected,
        "rows differ for {scan:?}: {} rows, expected {}",
        actual.num_rows(),
        expected.num_rows()
    );
    Decoded {
        rows: actual.num_rows(),
        requested,
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

/// A scan with predicates and a batch size of 100.
pub(super) fn filtered(predicates: Vec<PredicateSpec>) -> Scan {
    Scan {
        batch_size: Some(100),
        predicates,
        ..Default::default()
    }
}

/// A selection of `total` rows: runs of skipped and selected rows in turn.
/// It shrinks to a selection with fewer and shorter runs.
fn selection_strategy(total: usize) -> impl Strategy<Value = RowSelection> {
    let runs = select(vec![3usize, 40, 400]).prop_flat_map(|max_run| vec(1..=max_run, 1..200));
    (any::<bool>(), runs).prop_map(move |(mut skip, runs)| {
        let mut selectors = vec![];
        let mut rows_left = total;
        // The last run takes the rows that are left.
        let last = std::iter::once(total);
        for run in runs.into_iter().chain(last) {
            let run = run.min(rows_left);
            if run == 0 {
                break;
            }
            selectors.push(match skip {
                true => RowSelector::skip(run),
                false => RowSelector::select(run),
            });
            rows_left -= run;
            skip = !skip;
        }
        RowSelection::from(selectors)
    })
}

/// No batch size (the default), a batch size next to the size of a page or of
/// a row group, or any batch size.
fn batch_size_strategy() -> impl Strategy<Value = Option<usize>> {
    let boundaries = [1, ROWS_PER_PAGE, ROWS_PER_ROW_GROUP, 700, NUM_ROWS]
        .into_iter()
        .flat_map(|size| [size.saturating_sub(1).max(1), size, size + 1])
        .collect::<Vec<_>>();
    prop_oneof![
        1 => Just(None),
        4 => select(boundaries).prop_map(Some),
        4 => (1..2 * NUM_ROWS).prop_map(Some),
    ]
}

/// A cache of 1 byte cannot hold a batch, so the output reads the predicate
/// columns again, at cache batch boundaries. A cache of 0 bytes disables the
/// cache.
#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_predicate_cache_misses() {
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
        assert_same_rows(&scan);
    }
}

fn predicate_strategy() -> impl Strategy<Value = PredicateSpec> {
    let value = 0..NUM_ROWS as i64;
    prop_oneof![
        value
            .clone()
            .prop_map(|v| PredicateSpec::new("a", Cmp::Lt(v))),
        value
            .clone()
            .prop_map(|v| PredicateSpec::new("a", Cmp::Ge(v))),
        (2..7i64).prop_map(|m| PredicateSpec::new("b", Cmp::ModNotZero(m))),
        (2..40i64).prop_map(|m| PredicateSpec::new("a", Cmp::ModNotZero(m))),
        Just(PredicateSpec::new("b", Cmp::All)),
        Just(PredicateSpec::new("l", Cmp::ListNotNull)),
        value.prop_map(|v| PredicateSpec::new("s", Cmp::StructXLt(2 * v))),
        (2..9i64).prop_map(|m| PredicateSpec::new("a", Cmp::NullEvery(m))),
        Just(PredicateSpec::new("b", Cmp::None)),
    ]
}

/// A scan with batch size, projection, row groups, row selection, selection
/// policy, predicates, predicate cache size, offset and limit at random.
fn scan_strategy(heterogeneous: bool) -> impl Strategy<Value = Scan> {
    let row_group_rows = match heterogeneous {
        true => HETEROGENEOUS_ROW_GROUP_ROWS,
        false => [ROWS_PER_ROW_GROUP; 3],
    };
    // Some of the row groups, in any order.
    let row_groups = (Just(vec![0, 1, 2]).prop_shuffle(), 1..=3usize)
        .prop_map(|(row_groups, len)| row_groups[..len].to_vec());
    weighted(0.3, row_groups)
        .prop_flat_map(move |row_groups| {
            // A selection must cover exactly the row groups of the scan.
            let total: usize = match &row_groups {
                Some(row_groups) => row_groups.iter().map(|&i| row_group_rows[i]).sum(),
                None => NUM_ROWS,
            };
            (
                Just(row_groups),
                proptest::bool::weighted(0.1),
                batch_size_strategy(),
                select(vec![
                    None,
                    Some(vec!["a"]),
                    Some(vec!["c"]),
                    Some(vec!["a", "c"]),
                    Some(vec!["b", "c"]),
                    Some(vec!["l"]),
                    Some(vec!["s", "b"]),
                    Some(vec!["l", "a", "s"]),
                ]),
                weighted(0.5, selection_strategy(total)),
                // Mostly a limit and an offset that leave rows. Sometimes a
                // limit of 0, or an offset after the last row.
                weighted(0.3, prop_oneof![9 => 1..=total, 1 => Just(0)]),
                weighted(0.3, prop_oneof![9 => 0..total / 2, 1 => total..total + 300]),
                vec(predicate_strategy(), 0..4),
                select(vec![
                    None,
                    Some(RowSelectionPolicy::Selectors),
                    Some(RowSelectionPolicy::Mask),
                ]),
                select(vec![None, Some(0), Some(1)]),
            )
        })
        .prop_map(
            move |(
                row_groups,
                page_index_off,
                batch_size,
                projection,
                selection,
                limit,
                offset,
                predicates,
                policy,
                max_predicate_cache_size,
            )| Scan {
                heterogeneous,
                page_index_off,
                batch_size,
                projection: projection.map(|names| columns(&names)),
                row_groups,
                selection,
                limit,
                offset,
                predicates,
                policy,
                max_predicate_cache_size,
            },
        )
}

/// Scans of each fuzz test, so that it runs in a few seconds in a debug
/// build.
const FUZZ_CASES: u32 = 1000;

/// Check [`FUZZ_CASES`] random scans. If a scan fails, proptest shrinks it to
/// a smaller scan that fails. Most of the scans must return rows, so that the
/// test does not compare empty results only.
///
/// The scans are the same in each run, so that a failure in CI comes from
/// the change under test.
fn fuzz(heterogeneous: bool) {
    let config = Config {
        cases: FUZZ_CASES,
        failure_persistence: None,
        ..Config::default()
    };
    let rng = TestRng::deterministic_rng(config.rng_algorithm);
    let mut runner = TestRunner::new_with_rng(config, rng);
    let scans_with_rows = Cell::new(0);
    runner
        .run(&scan_strategy(heterogeneous), |scan| {
            if assert_same_rows(&scan).rows > 0 {
                scans_with_rows.set(scans_with_rows.get() + 1);
            }
            Ok(())
        })
        .unwrap();
    assert!(
        scans_with_rows.get() * 3 >= FUZZ_CASES * 2,
        "only {} of {FUZZ_CASES} scans returned rows",
        scans_with_rows.get()
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
