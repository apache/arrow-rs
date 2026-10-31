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

use super::*;
use crate::arrow::arrow_reader::ReadPlanBuilder;
use crate::arrow::arrow_reader::selection::RowSelectionStrategy;
use crate::arrow::in_memory_row_group::InMemoryRowGroup;
use crate::arrow::parquet_to_arrow_field_levels;

#[test]
fn try_new_with_row_groups_reads_only_selected_pages() {
    const ROWS_PER_PAGE: usize = 4;
    const ROW_COUNT: usize = 4 * ROWS_PER_PAGE;
    const BATCH_SIZE: usize = ROWS_PER_PAGE;

    let values: ArrayRef = Arc::new(Int64Array::from_iter_values(0..ROW_COUNT as i64));
    let batch = RecordBatch::try_from_iter([("value", values)]).unwrap();
    let properties = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_data_page_row_count_limit(ROWS_PER_PAGE)
        .set_write_batch_size(ROWS_PER_PAGE)
        .build();
    let mut data = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut data, batch.schema(), Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let data = Bytes::from(data);

    let selection = RowSelection::from(vec![
        RowSelector::select(1),
        RowSelector::skip(1),
        RowSelector::select(1),
        RowSelector::skip(1),
        RowSelector::skip(2 * ROWS_PER_PAGE),
        RowSelector::select(1),
        RowSelector::skip(1),
        RowSelector::select(1),
        RowSelector::skip(1),
    ]);
    assert_eq!(selection.row_count(), 4);
    assert_eq!(
        ReadPlanBuilder::new(BATCH_SIZE)
            .with_selection(Some(selection.clone()))
            .resolve_selection_strategy(),
        RowSelectionStrategy::Mask,
    );

    let options = || ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
    let arrow_metadata = ArrowReaderMetadata::load(&data, options()).unwrap();
    let metadata = arrow_metadata.metadata().clone();
    let page_index = metadata.page_index_for_row_group(0);
    let page_count = page_index.page_locations(0).unwrap().len();
    assert_eq!(page_count, 4);

    let projection = ProjectionMask::all();
    let row_group_metadata = metadata.row_group(0);
    let mut row_group = InMemoryRowGroup {
        page_index: Some(page_index),
        column_chunks: vec![None; row_group_metadata.num_columns()],
        row_count: row_group_metadata.num_rows() as usize,
        row_group_idx: 0,
        metadata: metadata.as_ref(),
    };

    let fetch = row_group.fetch_ranges(&projection, Some(&selection), BATCH_SIZE, None);
    assert_eq!(fetch.ranges.len(), 2);
    let fetched = fetch
        .ranges
        .iter()
        .map(|range| data.slice(range.start as usize..range.end as usize))
        .collect::<Vec<_>>();
    row_group.fill_column_chunks(&projection, fetch.page_start_offsets, fetched);

    let levels =
        parquet_to_arrow_field_levels(metadata.file_metadata().schema_descr(), projection, None)
            .unwrap();
    let actual = ParquetRecordBatchReader::try_new_with_row_groups(
        &levels,
        &row_group,
        BATCH_SIZE,
        Some(selection.clone()),
    )
    .unwrap()
    .collect::<std::result::Result<Vec<_>, _>>()
    .unwrap();
    let actual = concat_batches(&actual[0].schema(), &actual).unwrap();

    let expected = ParquetRecordBatchReaderBuilder::try_new_with_options(data, options())
        .unwrap()
        .with_batch_size(BATCH_SIZE)
        .with_row_selection(selection)
        .build()
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    let expected = concat_batches(&expected[0].schema(), &expected).unwrap();

    assert_eq!(actual.num_rows(), 4);
    assert_eq!(actual, expected);
}
