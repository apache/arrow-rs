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

use crate::arrow::ProjectionMask;
use crate::arrow::array_reader::RowGroups;
use crate::arrow::arrow_reader::RowSelection;
use crate::column::page::{PageIterator, PageReader};
use crate::errors::ParquetError;
use crate::file::metadata::page_index::RowGroupPageIndex;
use crate::file::metadata::{ParquetMetaData, RowGroupMetaData};
use crate::file::page_index::offset_index::PageLocation;
use crate::file::reader::{ChunkReader, Length, SerializedPageReader};
use bytes::{Buf, Bytes};
use std::ops::Range;
use std::sync::Arc;

/// An in-memory collection of column chunks
#[derive(Debug)]
pub(crate) struct InMemoryRowGroup<'a> {
    pub(crate) page_index: Option<RowGroupPageIndex>,
    /// Column chunks for this row group
    pub(crate) column_chunks: Vec<Option<Arc<ColumnChunkData>>>,
    pub(crate) row_count: usize,
    pub(crate) row_group_idx: usize,
    pub(crate) metadata: &'a ParquetMetaData,
}

/// What ranges to fetch for the columns in this row group
#[derive(Debug)]
pub(crate) struct FetchRanges {
    /// The byte ranges to fetch
    pub(crate) ranges: Vec<Range<u64>>,
    /// If `Some`, the start offsets of each page for each column chunk, or
    /// `None` for a column chunk without an offset index (fetched in full)
    pub(crate) page_start_offsets: Option<Vec<Option<Vec<u64>>>>,
}

impl InMemoryRowGroup<'_> {
    /// Returns the byte ranges to fetch for the columns specified in
    /// `projection` and `selection`.
    ///
    /// `cache_mask` indicates which columns, if any, are being cached by
    /// [`RowGroupCache`](crate::arrow::array_reader::RowGroupCache).
    /// The `selection` for Cached columns is expanded to batch boundaries to simplify
    /// accounting for what data is cached.
    ///
    /// The ranges of each column chunk come from [`ColumnFetch`].
    pub(crate) fn fetch_ranges(
        &self,
        projection: &ProjectionMask,
        selection: Option<&RowSelection>,
        batch_size: usize,
        cache_mask: Option<&ProjectionMask>,
    ) -> FetchRanges {
        let metadata = self.metadata.row_group(self.row_group_idx);
        // With a `RowSelection` and an `OffsetIndex`, only fetch the pages
        // required for the `RowSelection`
        let page_index = self.page_index.as_ref().filter(|_| selection.is_some());
        let expanded_selection = selection
            .filter(|_| page_index.is_some() && cache_mask.is_some())
            .map(|selection| selection.expand_to_batch_boundaries(batch_size, self.row_count));

        // Consider preallocating outer vec: https://github.com/apache/arrow-rs/issues/8667
        let mut page_start_offsets: Option<Vec<Option<Vec<u64>>>> = page_index.map(|_| vec![]);
        let mut ranges = vec![];
        let columns = columns_to_fetch(projection, self.column_chunks.len(), |idx| {
            self.column_chunks[idx].is_some()
        });
        for idx in columns {
            let locations = page_index
                .and_then(|page_index| page_index.offset_index(idx))
                .map(|offset_index| offset_index.page_locations().as_slice());
            let column_selection =
                column_selection(selection, expanded_selection.as_ref(), cache_mask, idx);
            let (start, len) = metadata.column(idx).byte_range();
            let fetch = ColumnFetch::new(start..start + len, locations, column_selection);
            let first = ranges.len();
            ranges.extend(fetch.ranges());
            if let Some(page_start_offsets) = page_start_offsets.as_mut() {
                page_start_offsets.push(match fetch {
                    // No offset index for this column, fetch the entire column
                    ColumnFetch::Chunk { .. } => None,
                    ColumnFetch::Pages { .. } => {
                        Some(ranges[first..].iter().map(|range| range.start).collect())
                    }
                });
            }
        }
        FetchRanges {
            ranges,
            page_start_offsets,
        }
    }

    /// Fills in `self.column_chunks` with the data fetched from `chunk_data`.
    ///
    /// This function **must** be called with the data from the ranges returned by
    /// `fetch_ranges` and the corresponding page_start_offsets, with the exact same and `selection`.
    pub(crate) fn fill_column_chunks<I>(
        &mut self,
        projection: &ProjectionMask,
        page_start_offsets: Option<Vec<Option<Vec<u64>>>>,
        chunk_data: I,
    ) where
        I: IntoIterator<Item = Bytes>,
    {
        let mut chunk_data = chunk_data.into_iter();
        let metadata = self.metadata.row_group(self.row_group_idx);
        if let Some(page_start_offsets) = page_start_offsets {
            // If we have a `RowSelection` and an `OffsetIndex` then only fetch pages required for the
            // `RowSelection`
            let mut page_start_offsets = page_start_offsets.into_iter();

            for (idx, chunk) in self.column_chunks.iter_mut().enumerate() {
                if chunk.is_some() || !projection.leaf_included(idx) {
                    continue;
                }

                match page_start_offsets.next() {
                    // No offset index: `fetch_ranges` requested the whole chunk
                    Some(None) => {
                        if let Some(data) = chunk_data.next() {
                            *chunk = Some(Arc::new(ColumnChunkData::Dense {
                                offset: metadata.column(idx).byte_range().0 as usize,
                                data,
                            }));
                        }
                    }
                    Some(Some(offsets)) => {
                        let mut chunks = Vec::with_capacity(offsets.len());
                        for _ in 0..offsets.len() {
                            chunks.push(chunk_data.next().unwrap());
                        }

                        *chunk = Some(Arc::new(ColumnChunkData::Sparse {
                            length: metadata.column(idx).byte_range().1 as usize,
                            data: offsets
                                .into_iter()
                                .map(|x| x as usize)
                                .zip(chunks)
                                .collect(),
                        }))
                    }
                    None => {}
                }
            }
        } else {
            for (idx, chunk) in self.column_chunks.iter_mut().enumerate() {
                if chunk.is_some() || !projection.leaf_included(idx) {
                    continue;
                }

                if let Some(data) = chunk_data.next() {
                    *chunk = Some(Arc::new(ColumnChunkData::Dense {
                        offset: metadata.column(idx).byte_range().0 as usize,
                        data,
                    }));
                }
            }
        }
    }
}

/// The leaf columns in `projection` that are not yet read, in column order.
///
/// A decoding stage of a row group does not fetch a column that an earlier
/// stage of the same row group read.
#[inline]
pub(crate) fn columns_to_fetch<'a>(
    projection: &'a ProjectionMask,
    num_columns: usize,
    is_read: impl Fn(usize) -> bool + 'a,
) -> impl Iterator<Item = usize> + 'a {
    (0..num_columns).filter(move |&idx| projection.leaf_included(idx) && !is_read(idx))
}

/// The selection that [`InMemoryRowGroup::fetch_ranges`] uses to choose the
/// pages of column `idx`: `expanded_selection` (the selection expanded to
/// batch boundaries) if `cache_mask` includes the column, else `selection`.
#[inline]
pub(crate) fn column_selection<'a>(
    selection: Option<&'a RowSelection>,
    expanded_selection: Option<&'a RowSelection>,
    cache_mask: Option<&ProjectionMask>,
    idx: usize,
) -> Option<&'a RowSelection> {
    match expanded_selection {
        Some(expanded) if cache_mask.is_some_and(|mask| mask.leaf_included(idx)) => Some(expanded),
        _ => selection,
    }
}

/// What [`InMemoryRowGroup::fetch_ranges`] fetches for one column chunk.
///
/// This is the one place that decides which bytes of a column chunk are
/// requested, so other users of this rule cannot differ from the decoder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ColumnFetch<'a> {
    /// The whole column chunk, as one range. This is the case if there is
    /// no selection or the column has no offset index.
    Chunk {
        /// Byte range of the column chunk.
        range: Range<u64>,
    },
    /// The dictionary page, if any, and the data pages that contain a
    /// selected row.
    Pages {
        /// Byte range of the dictionary page, if the column chunk has one.
        dictionary: Option<Range<u64>>,
        /// The page locations of the column chunk.
        locations: &'a [PageLocation],
        /// Indexes in `locations` of the data pages to fetch, in page order.
        pages: Vec<usize>,
    },
}

impl<'a> ColumnFetch<'a> {
    /// The fetch of the column chunk at byte range `chunk`, with page
    /// locations `locations` from its offset index, if any, and row
    /// selection `selection`, if any.
    #[inline]
    pub(crate) fn new(
        chunk: Range<u64>,
        locations: Option<&'a [PageLocation]>,
        selection: Option<&RowSelection>,
    ) -> Self {
        let (Some(selection), Some(locations)) = (selection, locations) else {
            return Self::Chunk { range: chunk };
        };
        // `scan_ranges` returns the ranges of the selected pages in page
        // order. Map them back to page indexes.
        let fetched = selection.scan_ranges(locations);
        let mut fetched = fetched.iter().peekable();
        let pages = locations
            .iter()
            .enumerate()
            .filter(|(_, location)| {
                fetched
                    .next_if(|range| range.start == location.offset as u64)
                    .is_some()
            })
            .map(|(idx, _)| idx)
            .collect();
        Self::Pages {
            dictionary: dictionary_range(chunk.start, locations),
            locations,
            pages,
        }
    }

    /// The byte ranges to fetch, in file order.
    pub(crate) fn ranges(&self) -> impl Iterator<Item = Range<u64>> + '_ {
        let (chunk, dictionary, pages) = match self {
            Self::Chunk { range, .. } => (Some(range.clone()), None, None),
            Self::Pages {
                dictionary,
                locations,
                pages,
            } => (None, dictionary.clone(), Some((*locations, pages))),
        };
        let data_pages = pages
            .into_iter()
            .flat_map(|(locations, pages)| pages.iter().map(|&page| page_range(&locations[page])));
        chunk.into_iter().chain(dictionary).chain(data_pages)
    }
}

/// Byte range of the dictionary page of a column chunk that starts at
/// `chunk_start`: the bytes before the first data page, if any.
#[inline]
pub(crate) fn dictionary_range(chunk_start: u64, locations: &[PageLocation]) -> Option<Range<u64>> {
    match locations.first() {
        Some(first) if first.offset as u64 != chunk_start => Some(chunk_start..first.offset as u64),
        _ => None,
    }
}

/// Byte range of a data page.
#[inline]
pub(crate) fn page_range(location: &PageLocation) -> Range<u64> {
    let start = location.offset as u64;
    start..start + location.compressed_page_size as u64
}

impl RowGroups for InMemoryRowGroup<'_> {
    fn num_rows(&self) -> usize {
        self.row_count
    }

    /// Return chunks for column i
    fn column_chunks(&self, i: usize) -> crate::errors::Result<Box<dyn PageIterator>> {
        match &self.column_chunks[i] {
            None => Err(ParquetError::General(format!(
                "Invalid column index {i}, column was not fetched"
            ))),
            Some(data) => {
                let page_locations = self
                    .page_index
                    .as_ref()
                    .and_then(|pi| pi.page_locations(i).cloned());
                let column_chunk_metadata = self.metadata.row_group(self.row_group_idx).column(i);
                let page_reader = SerializedPageReader::new(
                    data.clone(),
                    column_chunk_metadata,
                    self.row_count,
                    page_locations,
                )?;
                let page_reader = page_reader.add_crypto_context(
                    self.row_group_idx,
                    i,
                    self.metadata,
                    column_chunk_metadata,
                )?;

                let page_reader: Box<dyn PageReader> = Box::new(page_reader);

                Ok(Box::new(ColumnChunkIterator {
                    reader: Some(Ok(page_reader)),
                }))
            }
        }
    }

    fn row_groups(&self) -> Box<dyn Iterator<Item = &RowGroupMetaData> + '_> {
        Box::new(std::iter::once(self.metadata.row_group(self.row_group_idx)))
    }

    fn metadata(&self) -> &ParquetMetaData {
        self.metadata
    }
}

/// An in-memory column chunk.
/// This allows us to hold either dense column chunks or sparse column chunks and easily
/// access them by offset.
#[derive(Clone, Debug)]
pub(crate) enum ColumnChunkData {
    /// Column chunk data representing only a subset of data pages.
    /// For example if a row selection (possibly caused by a filter in a query) causes us to read only
    /// a subset of the rows in the column.
    Sparse {
        /// Length of the full column chunk
        length: usize,
        /// Subset of data pages included in this sparse chunk.
        ///
        /// Each element is a tuple of (page offset within file, page data).
        /// Each entry is a complete page and the list is ordered by offset.
        data: Vec<(usize, Bytes)>,
    },
    /// Full column chunk and the offset within the original file
    Dense { offset: usize, data: Bytes },
}

impl ColumnChunkData {
    /// Return the data for this column chunk at the given offset
    fn get(&self, start: u64) -> crate::errors::Result<Bytes> {
        match &self {
            ColumnChunkData::Sparse { data, .. } => data
                .binary_search_by_key(&start, |(offset, _)| *offset as u64)
                .map(|idx| data[idx].1.clone())
                .map_err(|_| {
                    ParquetError::General(format!(
                        "Invalid offset in sparse column chunk data: {start}, no matching page found.\
                         If you are using a `SelectionStrategyPolicy::Mask`, ensure that the OffsetIndex is provided when \
                         creating the InMemoryRowGroup."
                    ))
                }),
            ColumnChunkData::Dense { offset, data } => {
                let start = start as usize - *offset;
                Ok(data.slice(start..))
            }
        }
    }
}

impl Length for ColumnChunkData {
    /// Return the total length of the full column chunk
    fn len(&self) -> u64 {
        match &self {
            ColumnChunkData::Sparse { length, .. } => *length as u64,
            ColumnChunkData::Dense { data, .. } => data.len() as u64,
        }
    }
}

impl ChunkReader for ColumnChunkData {
    type T = bytes::buf::Reader<Bytes>;

    fn get_read(&self, start: u64) -> crate::errors::Result<Self::T> {
        Ok(self.get(start)?.reader())
    }

    fn get_bytes(&self, start: u64, length: usize) -> crate::errors::Result<Bytes> {
        let data = self.get(start)?;
        if data.len() < length {
            return Err(general_err!(
                "Internal Error: column chunk data at offset {start} has {} bytes, expected {length}",
                data.len()
            ));
        }
        Ok(data.slice(..length))
    }
}

/// Implements [`PageIterator`] for a single column chunk, yielding a single [`PageReader`]
struct ColumnChunkIterator {
    reader: Option<crate::errors::Result<Box<dyn PageReader>>>,
}

impl Iterator for ColumnChunkIterator {
    type Item = crate::errors::Result<Box<dyn PageReader>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.reader.take()
    }
}

impl PageIterator for ColumnChunkIterator {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_bytes_errors_on_short_data() {
        let dense = ColumnChunkData::Dense {
            offset: 100,
            data: Bytes::from_static(b"0123456789"),
        };
        assert_eq!(
            dense.get_bytes(105, 5).unwrap(),
            Bytes::from_static(b"56789")
        );
        let err = dense.get_bytes(105, 6).unwrap_err().to_string();
        assert!(err.contains("has 5 bytes, expected 6"), "{err}");
    }
}
