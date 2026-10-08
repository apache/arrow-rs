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

//! Incrementally caching page indexes with [`ParquetMetaDataPushDecoder`].
//!
//! This example keeps footer metadata and decoded page indexes in separate caches. For each
//! query it:
//!
//! 1. Finds the requested column chunks that are absent from the page-index cache
//! 2. Uses [`ParquetMetaDataPushDecoder::try_decode_page_index`] to request and decode those misses
//! 3. Moves the decoded entries into the cache
//! 4. Builds a query-specific [`PageIndex`] from shared cache entries
//!
//! A production cache shared by multiple files would also include a stable file identity and
//! freshness information in its keys.

use std::collections::{BTreeSet, HashMap};
use std::ops::Range;
use std::sync::Arc;

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::DecodeResult;
use parquet::arrow::ArrowWriter;
use parquet::errors::{ParquetError, Result};
use parquet::file::metadata::page_index::{PageIndex, PageIndexBuilder, PageIndexProvider};
use parquet::file::metadata::{
    ColumnChunkMask, PageIndexPolicy, ParquetMetaData, ParquetMetaDataPushDecoder,
};
use parquet::file::page_index::column_index::ColumnIndexMetaData;
use parquet::file::page_index::offset_index::OffsetIndexMetaData;
use parquet::file::properties::{EnabledStatistics, WriterProperties};

type ColumnChunk = (usize, usize);

/// Decoded page indexes cached independently of query-specific metadata.
#[derive(Default)]
struct PageIndexCache {
    column_indexes: HashMap<ColumnChunk, Arc<ColumnIndexMetaData>>,
    offset_indexes: HashMap<ColumnChunk, Arc<OffsetIndexMetaData>>,
}

impl PageIndexCache {
    /// Returns a page index containing exactly the entries requested for one query, loading any
    /// cache misses through `ParquetMetaDataPushDecoder` first.
    fn page_index_for_query(
        &mut self,
        metadata: &ParquetMetaData,
        file_bytes: &Bytes,
        column_index_mask: &ColumnChunkMask,
        offset_index_mask: &ColumnChunkMask,
    ) -> Result<PageIndex> {
        let num_row_groups = metadata.num_row_groups();
        let num_columns = metadata.file_metadata().schema_descr().num_columns();

        let missing_column_indexes = column_index_mask
            .column_chunk_indices(num_row_groups, num_columns)
            .filter(|key| !self.column_indexes.contains_key(key))
            .collect::<Vec<_>>();
        let missing_offset_indexes = offset_index_mask
            .column_chunk_indices(num_row_groups, num_columns)
            .filter(|key| !self.offset_indexes.contains_key(key))
            .collect::<Vec<_>>();

        println!(
            "  cache misses: {} column indexes, {} offset indexes",
            missing_column_indexes.len(),
            missing_offset_indexes.len()
        );

        if !missing_column_indexes.is_empty() || !missing_offset_indexes.is_empty() {
            // ColumnChunkMask describes a Cartesian product. This produces the smallest such mask
            // covering the misses and may therefore decode additional entries for irregular miss
            // sets. Those additional entries are useful cache population and are retained below.
            let column_miss_mask = covering_mask(&missing_column_indexes);
            let offset_miss_mask = covering_mask(&missing_offset_indexes);
            let mut decoder = ParquetMetaDataPushDecoder::try_new_with_metadata(
                file_bytes.len() as u64,
                metadata.clone(),
            )?
            .with_column_index_policy(PageIndexPolicy::Optional)
            .with_offset_index_policy(PageIndexPolicy::Optional)
            .with_column_index_mask(column_miss_mask)
            .with_offset_index_mask(offset_miss_mask);

            if let Some(page_index) = decode_page_index(&mut decoder, file_bytes)? {
                let (column_indexes, offset_indexes) = page_index.into_index_entries();
                self.column_indexes.extend(column_indexes);
                self.offset_indexes.extend(offset_indexes);
            }
        }

        // Build an immutable provider containing only the entries needed by this query. Cloning
        // the Arcs neither clones the decoded metadata nor retains the source file bytes.
        let mut builder = PageIndexBuilder::new(num_row_groups, num_columns);
        for key @ (row_group_idx, column_idx) in
            column_index_mask.column_chunk_indices(num_row_groups, num_columns)
        {
            if let Some(index) = self.column_indexes.get(&key) {
                builder.put_column_index_shared(Arc::clone(index), row_group_idx, column_idx);
            }
        }
        for key @ (row_group_idx, column_idx) in
            offset_index_mask.column_chunk_indices(num_row_groups, num_columns)
        {
            if let Some(index) = self.offset_indexes.get(&key) {
                builder.put_offset_index_shared(Arc::clone(index), row_group_idx, column_idx);
            }
        }
        Ok(builder.build())
    }
}

/// Returns the smallest Cartesian mask covering `chunks`.
fn covering_mask(chunks: &[ColumnChunk]) -> ColumnChunkMask {
    if chunks.is_empty() {
        return ColumnChunkMask::none();
    }

    let row_groups = chunks.iter().map(|&(row_group, _)| row_group);
    let columns = chunks.iter().map(|&(_, column)| column);
    ColumnChunkMask::row_groups_and_columns(
        row_groups.collect::<BTreeSet<_>>(),
        columns.collect::<BTreeSet<_>>(),
    )
}

/// Drives the page-index-only decoder by supplying exactly the byte ranges it requests.
fn decode_page_index(
    decoder: &mut ParquetMetaDataPushDecoder,
    file_bytes: &Bytes,
) -> Result<Option<PageIndex>> {
    loop {
        match decoder.try_decode_page_index()? {
            DecodeResult::Data(page_index) => return Ok(page_index),
            DecodeResult::NeedsData(ranges) => {
                let data = ranges
                    .iter()
                    .map(|range| bytes_for_range(file_bytes, range))
                    .collect();
                decoder.push_ranges(ranges, data)?;
            }
            DecodeResult::Finished => {
                return Err(ParquetError::General(
                    "page-index decoder finished without producing an index".to_string(),
                ));
            }
        }
    }
}

/// Reads and caches footer metadata without decoding page indexes.
fn decode_footer(file_bytes: &Bytes) -> Result<ParquetMetaData> {
    let mut decoder = ParquetMetaDataPushDecoder::try_new(file_bytes.len() as u64)?
        .with_page_index_policy(PageIndexPolicy::Skip);
    loop {
        match decoder.try_decode()? {
            DecodeResult::Data(metadata) => return Ok(metadata),
            DecodeResult::NeedsData(ranges) => {
                let data = ranges
                    .iter()
                    .map(|range| bytes_for_range(file_bytes, range))
                    .collect();
                decoder.push_ranges(ranges, data)?;
            }
            DecodeResult::Finished => {
                return Err(ParquetError::General(
                    "metadata decoder finished without producing metadata".to_string(),
                ));
            }
        }
    }
}

fn bytes_for_range(file_bytes: &Bytes, range: &Range<u64>) -> Bytes {
    file_bytes.slice(range.start as usize..range.end as usize)
}

fn main() -> Result<()> {
    let file_bytes = create_sample_file()?;
    let cached_metadata = Arc::new(decode_footer(&file_bytes)?);
    let mut cache = PageIndexCache::default();

    // Query 1 filters column 0 and projects columns 0, 1, and 4 in row group 0.
    let column_mask = ColumnChunkMask::row_groups_and_columns([0], [0]);
    let offset_mask = ColumnChunkMask::row_groups_and_columns([0], [0, 1, 4]);
    println!("Query 1:");
    let page_index =
        cache.page_index_for_query(&cached_metadata, &file_bytes, &column_mask, &offset_mask)?;
    assert_query_indexes(&page_index, 0, &[0], &[0, 1, 4]);

    // Attach the query-specific provider to a cheap clone of the cached footer metadata.
    let query_metadata = cached_metadata
        .as_ref()
        .clone()
        .into_builder()
        .set_page_index(Some(Arc::new(page_index)))
        .build();
    assert!(query_metadata.page_index().is_some());

    // Query 2 reuses the column index and offset indexes for columns 0 and 4. Only the offset
    // index for column 2 is decoded and added to the cache.
    let offset_mask = ColumnChunkMask::row_groups_and_columns([0], [0, 2, 4]);
    println!("Query 2:");
    let page_index =
        cache.page_index_for_query(&cached_metadata, &file_bytes, &column_mask, &offset_mask)?;
    assert_query_indexes(&page_index, 0, &[0], &[0, 2, 4]);

    println!(
        "Cache now contains {} column indexes and {} offset indexes",
        cache.column_indexes.len(),
        cache.offset_indexes.len()
    );
    Ok(())
}

fn assert_query_indexes(
    page_index: &PageIndex,
    row_group_idx: usize,
    column_indexes: &[usize],
    offset_indexes: &[usize],
) {
    for column_idx in 0..6 {
        assert_eq!(
            page_index.column_index(row_group_idx, column_idx).is_some(),
            column_indexes.contains(&column_idx)
        );
        assert_eq!(
            page_index.offset_index(row_group_idx, column_idx).is_some(),
            offset_indexes.contains(&column_idx)
        );
    }
}

fn create_sample_file() -> Result<Bytes> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("score", DataType::Int32, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("amount", DataType::Int32, false),
    ]));
    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_data_page_size_limit(100)
        .set_write_batch_size(10)
        .build();
    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, Arc::clone(&schema), Some(props))?;

    for row_group in 0..3 {
        for batch_num in 0..5 {
            let offset = row_group * 50 + batch_num * 10;
            let values = (offset..offset + 10).collect::<Vec<i32>>();
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int32Array::from(values.clone())),
                    Arc::new(Int32Array::from(
                        values.iter().map(|value| value * 2).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        values
                            .iter()
                            .map(|value| format!("name{value}"))
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int32Array::from(
                        values.iter().map(|value| value * 3).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        values
                            .iter()
                            .map(|value| if value % 2 == 0 { "even" } else { "odd" })
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int32Array::from(
                        values.iter().map(|value| value * 4).collect::<Vec<_>>(),
                    )),
                ],
            )?;
            writer.write(&batch)?;
        }
        writer.flush()?;
    }
    writer.close()?;
    Ok(Bytes::from(buffer))
}
