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

//! Tests for [`ParquetRecordBatchReader::try_new_with_row_groups`] over a
//! [`RowGroups`](crate::arrow::array_reader::RowGroups) that only holds the data
//! pages required by the [`RowSelection`] (the pattern
//! [`RowSelection::scan_ranges`] exists to support).

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_select::concat::concat_batches;
use bytes::Bytes;

use crate::arrow::arrow_reader::selection::RowSelectionStrategy;
use crate::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReader,
    ParquetRecordBatchReaderBuilder, ReadPlanBuilder, RowSelection, RowSelector,
};
use crate::arrow::in_memory_row_group::InMemoryRowGroup;
use crate::arrow::{ArrowWriter, ProjectionMask, parquet_to_arrow_field_levels};
use crate::file::metadata::PageIndexPolicy;
use crate::file::properties::WriterProperties;

pub(super) const TOTAL_ROWS: usize = 1000;
pub(super) const PAGE_ROWS: usize = 200;
pub(super) const BATCH_SIZE: usize = 200;

/// A single row group of `TOTAL_ROWS` rows with `PAGE_ROWS` rows per data page.
pub(super) fn write_test_file() -> Bytes {
    let values: ArrayRef = Arc::new(Int64Array::from_iter_values(0..TOTAL_ROWS as i64));
    let batch = RecordBatch::try_from_iter([("value", values)]).unwrap();

    let props = WriterProperties::builder()
        .set_data_page_row_count_limit(PAGE_ROWS)
        .set_write_batch_size(PAGE_ROWS)
        .build();

    let mut buf = Vec::with_capacity(1024);
    let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    Bytes::from(buf)
}

/// Many short runs concentrated in the first and fourth data pages, with whole
/// pages skipped in between.
///
/// `RowSelectionPolicy::Auto` prefers Mask when `total_rows < effective_selectors
/// * threshold`, i.e. when the *average* selector run is shorter than 32 rows —
/// note that skips count too. Here that is `1000 < 402 * 32`.
///
/// The selection still touches only 2 of the 5 data pages, so a caller doing
/// page pruning fetches 2 pages and leaves 3 unfetched.
pub(super) fn fragmented_selection() -> RowSelection {
    let mut selectors = Vec::new();
    // rows 0..200 (page 0): select every other row
    for _ in 0..PAGE_ROWS / 2 {
        selectors.push(RowSelector::select(1));
        selectors.push(RowSelector::skip(1));
    }
    // rows 200..600 (pages 1 and 2): skipped entirely
    selectors.push(RowSelector::skip(2 * PAGE_ROWS));
    // rows 600..800 (page 3): select every other row
    for _ in 0..PAGE_ROWS / 2 {
        selectors.push(RowSelector::select(1));
        selectors.push(RowSelector::skip(1));
    }
    // rows 800..1000 (page 4): skipped entirely
    selectors.push(RowSelector::skip(PAGE_ROWS));

    let selection = RowSelection::from(selectors);
    assert_eq!(selection.row_count(), PAGE_ROWS);
    selection
}

pub(super) fn reader_options() -> ArrowReaderOptions {
    ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required)
}

/// Reading a page-pruned `RowGroups` must return exactly the selected rows.
///
/// `try_new_with_row_groups` inherits `RowSelectionPolicy::default()` (`Auto`),
/// which resolves to Mask for this selection, and has no way to be told which
/// row ranges were actually loaded. Mask execution then decodes across the
/// unselected gap and touches pages the caller never fetched.
#[test]
fn test_try_new_with_row_groups_page_pruned_fragmented_selection() {
    read_page_pruned();
}

fn read_page_pruned() {
    let data = write_test_file();
    let selection = fragmented_selection();
    let projection = ProjectionMask::all();

    let metadata = ArrowReaderMetadata::load(&data, reader_options()).unwrap();
    let metadata = metadata.metadata().clone();

    // Guard against the test going vacuous: it is only meaningful while `Auto`
    // resolves this selection to Mask.
    assert_eq!(
        ReadPlanBuilder::new(BATCH_SIZE)
            .with_selection(Some(selection.clone()))
            .resolve_selection_strategy(),
        RowSelectionStrategy::Mask,
    );
    let row_group_meta = metadata.row_group(0);
    let num_columns = row_group_meta.columns().len();
    let row_count = row_group_meta.num_rows() as usize;

    let offset_index = metadata
        .offset_index()
        .filter(|index| !index.is_empty())
        .map(|index| index[0].as_slice())
        .expect("page index required");
    let num_pages = offset_index[0].page_locations.len();
    assert!(
        num_pages > 2,
        "test needs several data pages per column chunk, got {num_pages}"
    );

    let mut row_group = InMemoryRowGroup {
        offset_index: Some(offset_index),
        column_chunks: vec![None; num_columns],
        row_count,
        row_group_idx: 0,
        metadata: metadata.as_ref(),
    };

    // Fetch only the pages the selection touches, as an engine doing page
    // pruning would.
    let fetch = row_group.fetch_ranges(&projection, Some(&selection), BATCH_SIZE, None);
    assert!(
        fetch.ranges.len() < num_pages,
        "selection should have pruned pages: fetched {} of {num_pages} pages",
        fetch.ranges.len()
    );
    let fetched: Vec<Bytes> = fetch
        .ranges
        .iter()
        .map(|range| data.slice(range.start as usize..range.end as usize))
        .collect();
    row_group.fill_column_chunks(&projection, fetch.page_start_offsets, fetched);

    let levels =
        parquet_to_arrow_field_levels(metadata.file_metadata().schema_descr(), projection, None)
            .unwrap();

    let reader = ParquetRecordBatchReader::try_new_with_row_groups(
        &levels,
        &row_group,
        BATCH_SIZE,
        Some(selection.clone()),
    )
    .unwrap();
    let actual = reader.collect::<Result<Vec<_>, _>>().expect("read failed");
    let actual = concat_batches(&actual[0].schema(), &actual).unwrap();

    // Oracle: the same selection through the high level reader, which is not
    // affected because it prepares the plan for page skipping.
    let expected = ParquetRecordBatchReaderBuilder::try_new_with_options(data, reader_options())
        .unwrap()
        .with_batch_size(BATCH_SIZE)
        .with_row_selection(selection)
        .build()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let expected = concat_batches(&expected[0].schema(), &expected).unwrap();

    assert_eq!(expected.num_rows(), PAGE_ROWS);
    assert_eq!(actual, expected);
}
