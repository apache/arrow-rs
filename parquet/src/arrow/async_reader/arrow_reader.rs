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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};
use std::{collections::VecDeque, sync::Arc};

use arrow_array::ArrayRef;
use arrow_array::{cast::AsArray, Array, RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, DataType, Schema, SchemaRef};
use arrow_select::filter::prep_null_mask_filter;

use crate::basic::PageType;
use crate::column::page::{Page, PageMetadata, PageReader};
use crate::errors::ParquetError;
use crate::{
    arrow::{
        array_reader::ArrayReader,
        arrow_reader::{RowFilter, RowSelection, RowSelector},
    },
    file::reader::{ChunkReader, SerializedPageReader},
};

pub struct FilteredParquetRecordBatchReader {
    batch_size: usize,
    array_reader: Box<dyn ArrayReader>,
    predicate_readers: Vec<Box<dyn ArrayReader>>,
    schema: SchemaRef,
    selection: VecDeque<RowSelector>,
    row_filter: Option<RowFilter>,
}

fn read_selection(
    reader: &mut dyn ArrayReader,
    selection: &RowSelection,
) -> Result<ArrayRef, ParquetError> {
    for selector in selection.iter() {
        if selector.skip {
            let skipped = reader.skip_records(selector.row_count)?;
            debug_assert_eq!(skipped, selector.row_count, "failed to skip rows");
        } else {
            let read_records = reader.read_records(selector.row_count)?;
            debug_assert_eq!(read_records, selector.row_count, "failed to read rows");
        }
    }
    reader.consume_batch()
}

/// Take the next selection from the selection queue, and return the selection
/// whose selected row count is to_select or less (if input selection is exhausted).
fn take_next_selection(
    selection: &mut VecDeque<RowSelector>,
    to_select: usize,
) -> Option<RowSelection> {
    let mut current_selected = 0;
    let mut rt = Vec::new();
    while let Some(front) = selection.pop_front() {
        if front.skip {
            rt.push(front);
            continue;
        }

        if current_selected + front.row_count <= to_select {
            rt.push(front);
            current_selected += front.row_count;
        } else {
            let select = to_select - current_selected;
            let remaining = front.row_count - select;
            rt.push(RowSelector::select(select));
            selection.push_front(RowSelector::select(remaining));

            return Some(rt.into());
        }
    }
    if !rt.is_empty() {
        return Some(rt.into());
    }
    None
}

impl FilteredParquetRecordBatchReader {
    pub(crate) fn new(
        batch_size: usize,
        array_reader: Box<dyn ArrayReader>,
        selection: RowSelection,
        filter_readers: Vec<Box<dyn ArrayReader>>,
        row_filter: Option<RowFilter>,
    ) -> Self {
        let schema = match array_reader.get_data_type() {
            DataType::Struct(ref fields) => Schema::new(fields.clone()),
            _ => unreachable!("Struct array reader's data type is not struct!"),
        };

        Self {
            batch_size,
            array_reader,
            predicate_readers: filter_readers,
            schema: Arc::new(schema),
            selection: selection.into(),
            row_filter,
        }
    }

    pub(crate) fn take_filter(&mut self) -> Option<RowFilter> {
        self.row_filter.take()
    }

    /// Take a selection, and return the new selection where the rows are filtered by the predicate.
    fn build_predicate_filter(
        &mut self,
        mut selection: RowSelection,
    ) -> Result<RowSelection, ArrowError> {
        match &mut self.row_filter {
            None => Ok(selection),
            Some(filter) => {
                debug_assert_eq!(
                    self.predicate_readers.len(),
                    filter.predicates.len(),
                    "predicate readers and predicates should have the same length"
                );

                for (predicate, reader) in filter
                    .predicates
                    .iter_mut()
                    .zip(self.predicate_readers.iter_mut())
                {
                    let array = read_selection(reader.as_mut(), &selection)?;
                    let batch = RecordBatch::from(array.as_struct_opt().ok_or_else(|| {
                        general_err!("Struct array reader should return struct array")
                    })?);
                    let input_rows = batch.num_rows();
                    let predicate_filter = predicate.evaluate(batch)?;
                    if predicate_filter.len() != input_rows {
                        return Err(ArrowError::ParquetError(format!(
                            "ArrowPredicate predicate returned {} rows, expected {input_rows}",
                            predicate_filter.len()
                        )));
                    }
                    let predicate_filter = match predicate_filter.null_count() {
                        0 => predicate_filter,
                        _ => prep_null_mask_filter(&predicate_filter),
                    };
                    let raw = RowSelection::from_filters(&[predicate_filter]);
                    selection = selection.and_then(&raw);
                }
                Ok(selection)
            }
        }
    }
}

impl Iterator for FilteredParquetRecordBatchReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        // With filter pushdown, it's very hard to predict the number of rows to return -- depends on the selectivity of the filter.
        // We can do one of the following:
        // 1. Add a coalescing step to coalesce the resulting batches.
        // 2. Ask parquet reader to collect more rows before returning.

        // Approach 1 has the drawback of extra overhead of coalesce batch, which can be painful to be efficient.
        // Code below implements approach 2, where we keep consuming the selection until we select at least 3/4 of the batch size.
        // It boils down to leveraging array_reader's ability to collect large batches natively,
        //    rather than concatenating multiple small batches.

        let mut selected = 0;
        while let Some(cur_selection) =
            take_next_selection(&mut self.selection, self.batch_size - selected)
        {
            let filtered_selection = match self.build_predicate_filter(cur_selection) {
                Ok(selection) => selection,
                Err(e) => return Some(Err(e)),
            };

            for selector in filtered_selection.iter() {
                if selector.skip {
                    self.array_reader.skip_records(selector.row_count).ok()?;
                } else {
                    self.array_reader.read_records(selector.row_count).ok()?;
                }
            }
            selected += filtered_selection.row_count();
            if selected >= (self.batch_size / 4 * 3) {
                break;
            }
        }
        if selected == 0 {
            return None;
        }

        let array = self.array_reader.consume_batch().ok()?;
        let struct_array = array
            .as_struct_opt()
            .ok_or_else(|| general_err!("Struct array reader should return struct array"))
            .ok()?;
        Some(Ok(RecordBatch::from(struct_array.clone())))
    }
}

impl RecordBatchReader for FilteredParquetRecordBatchReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

struct CachedPage {
    dict: Option<(usize, Page)>, // page offset -> page
    data: Option<(usize, Page)>, // page offset -> page
}

struct PredicatePageCacheInner {
    pages: HashMap<usize, CachedPage>, // col_id (Parquet's leaf column index) -> CachedPage
}

impl PredicatePageCacheInner {
    pub(crate) fn get_page(&self, col_id: usize, offset: usize) -> Option<Page> {
        self.pages.get(&col_id).and_then(|pages| {
            pages
                .dict
                .iter()
                .chain(pages.data.iter())
                .find(|(page_offset, _)| *page_offset == offset)
                .map(|(_, page)| page.clone())
        })
    }

    /// Insert a page into the cache.
    /// Inserting a page will override the existing page, if any.
    /// This is because we only need to cache 2 pages per column, see below.
    pub(crate) fn insert_page(&mut self, col_id: usize, offset: usize, page: Page) {
        let is_dict = page.page_type() == PageType::DICTIONARY_PAGE;

        let cached_pages = self.pages.entry(col_id);
        match cached_pages {
            Entry::Occupied(mut entry) => {
                if is_dict {
                    entry.get_mut().dict = Some((offset, page));
                } else {
                    entry.get_mut().data = Some((offset, page));
                }
            }
            Entry::Vacant(entry) => {
                let cached_page = if is_dict {
                    CachedPage {
                        dict: Some((offset, page)),
                        data: None,
                    }
                } else {
                    CachedPage {
                        dict: None,
                        data: Some((offset, page)),
                    }
                };
                entry.insert(cached_page);
            }
        }
    }
}

/// A simple cache to avoid double-decompressing pages with filter pushdown.
/// In filter pushdown, we first decompress a page, apply the filter, and then decompress the page again.
/// This double decompression is expensive, so we cache the decompressed page.
///
/// This implementation contains subtle dynamics that can be hard to understand.
///
/// ## Which columns to cache
///
/// Let's consider this example: SELECT B, C FROM table WHERE A = 42 and B = 37;
/// We have 3 columns, and the predicate is applied to column A and B, and projection is on B and C.
///
/// For column A, we need to decompress it, apply the filter (A=42), and never have to decompress it again, as it's not in the projection.
/// For column B, we need to decompress it, apply the filter (B=37), and then decompress it again, as it's in the projection.
/// For column C, we don't have predicate, so we only decompress it once.
///
/// A, C is only decompressed once, and B is decompressed twice (as it appears in both the predicate and the projection).
/// The PredicatePageCache will only cache B.
/// We use B's col_id (Parquet's leaf column index) to identify the cache entry.
///
/// ## How many pages to cache
///
/// Now we identified the columns to cache, next question is to determine the **minimal** number of pages to cache.
///
/// Let's revisit our decoding pipeline:
/// Load batch 1 -> evaluate predicates -> filter 1 -> load & emit batch 1
/// Load batch 2 -> evaluate predicates -> filter 2 -> load & emit batch 2
/// ...
/// Load batch N -> evaluate predicates -> filter N -> load & emit batch N
///
/// Assumption & observation: each page consists multiple batches.
/// Then our pipeline looks like this:
/// Load Page 1
/// Load batch 1 -> evaluate predicates -> filter 1 -> load & emit batch 1
/// Load batch 2 -> evaluate predicates -> filter 2 -> load & emit batch 2
/// Load batch 3 -> evaluate predicates -> filter 3 -> load & emit batch 3
/// Load Page 2
/// Load batch 4 -> evaluate predicates -> filter 4 -> load & emit batch 4
/// Load batch 5 -> evaluate predicates -> filter 5 -> load & emit batch 5
/// ...
///
/// This means that we only need to cache one page per column,
/// because the page that is used by the predicate is the same page, and is immediately used in loading the batch.
///
/// The only exception is the dictionary page -- the first page of each column.
/// If we encountered a dict page, we will need to immediately read next page, and cache it.
///
/// To summarize, the cache only contains 2 pages per column: one dict page and one data page.
/// This is a nice property as it means the caching memory consumption is negligible and constant to the number of columns.
///
/// ## How to identify a page
/// We use the page offset (the offset to the Parquet file) to uniquely identify a page.
pub(crate) struct PredicatePageCache {
    inner: Mutex<PredicatePageCacheInner>,
}

impl PredicatePageCache {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(PredicatePageCacheInner {
                pages: HashMap::with_capacity(capacity),
            }),
        }
    }

    fn get(&self) -> MutexGuard<PredicatePageCacheInner> {
        self.inner.lock().unwrap()
    }
}

pub(crate) struct CachedPageReader<R: ChunkReader> {
    inner: SerializedPageReader<R>,
    cache: Arc<PredicatePageCache>,
    col_id: usize,
}

impl<R: ChunkReader> CachedPageReader<R> {
    pub(crate) fn new(
        inner: SerializedPageReader<R>,
        cache: Arc<PredicatePageCache>,
        col_id: usize,
    ) -> Self {
        Self {
            inner,
            cache,
            col_id,
        }
    }
}

impl<R: ChunkReader> Iterator for CachedPageReader<R> {
    type Item = Result<Page, ParquetError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.get_next_page().transpose()
    }
}

impl<R: ChunkReader> PageReader for CachedPageReader<R> {
    fn get_next_page(&mut self) -> Result<Option<Page>, ParquetError> {
        let next_page_offset = self.inner.peek_next_page_offset()?;

        let Some(offset) = next_page_offset else {
            return Ok(None);
        };

        let mut cache = self.cache.get();

        let page = cache.get_page(self.col_id, offset);
        if let Some(page) = page {
            self.inner.skip_next_page()?;
            Ok(Some(page))
        } else {
            let inner_page = self.inner.get_next_page()?;
            let Some(inner_page) = inner_page else {
                return Ok(None);
            };
            cache.insert_page(self.col_id, offset, inner_page.clone());
            Ok(Some(inner_page))
        }
    }

    fn peek_next_page(&mut self) -> Result<Option<PageMetadata>, ParquetError> {
        self.inner.peek_next_page()
    }

    fn skip_next_page(&mut self) -> Result<(), ParquetError> {
        self.inner.skip_next_page()
    }

    fn at_record_boundary(&mut self) -> Result<bool, ParquetError> {
        self.inner.at_record_boundary()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_take_next_selection_exact_match() {
        let mut queue = VecDeque::from(vec![
            RowSelector::skip(5),
            RowSelector::select(3),
            RowSelector::skip(2),
            RowSelector::select(7),
        ]);

        // Request exactly 10 rows (5 skip + 3 select + 2 skip)
        let selection = take_next_selection(&mut queue, 3).unwrap();
        assert_eq!(
            selection,
            vec![
                RowSelector::skip(5),
                RowSelector::select(3),
                RowSelector::skip(2)
            ]
            .into()
        );

        // Check remaining queue
        assert_eq!(queue.len(), 1);
        assert_eq!(queue[0].row_count, 7);
        assert_eq!(queue[0].skip, false);
    }

    #[test]
    fn test_take_next_selection_split_required() {
        let mut queue = VecDeque::from(vec![RowSelector::select(10), RowSelector::select(10)]);

        // Request 15 rows, which should split the first selector
        let selection = take_next_selection(&mut queue, 15).unwrap();

        assert_eq!(
            selection,
            vec![RowSelector::select(10), RowSelector::select(5)].into()
        );

        // Check remaining queue - should have 5 rows from split and original 10
        assert_eq!(queue.len(), 1);
        assert_eq!(queue[0].skip, false);
        assert_eq!(queue[0].row_count, 5);
    }

    #[test]
    fn test_take_next_selection_empty_queue() {
        let mut queue = VecDeque::new();

        // Should return None for empty queue
        let selection = take_next_selection(&mut queue, 10);
        assert!(selection.is_none());

        // Test with queue that becomes empty
        queue.push_back(RowSelector::select(5));
        let selection = take_next_selection(&mut queue, 10).unwrap();
        assert_eq!(selection, vec![RowSelector::select(5)].into());

        // Queue should now be empty
        let selection = take_next_selection(&mut queue, 10);
        assert!(selection.is_none());
    }

    #[test]
    fn test_predicate_page_cache_basic_operations() {
        use super::*;

        let cache = PredicatePageCache::new(2);
        let page1 = Page::dummy_page(PageType::DATA_PAGE, 100);
        let page2 = Page::dummy_page(PageType::DICTIONARY_PAGE, 200);

        // Insert and retrieve a data page
        cache.get().insert_page(0, 1000, page1.clone());
        let retrieved = cache.get().get_page(0, 1000);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().page_type(), PageType::DATA_PAGE);

        // Insert and retrieve a dictionary page for same column
        cache.get().insert_page(0, 2000, page2.clone());
        let retrieved = cache.get().get_page(0, 2000);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().page_type(), PageType::DICTIONARY_PAGE);

        // Both pages should still be accessible
        assert!(cache.get().get_page(0, 1000).is_some());
        assert!(cache.get().get_page(0, 2000).is_some());
    }

    #[test]
    fn test_predicate_page_cache_replacement() {
        use super::*;

        let cache = PredicatePageCache::new(2);
        let data_page1 = Page::dummy_page(PageType::DATA_PAGE, 100);
        let data_page2 = Page::dummy_page(PageType::DATA_PAGE_V2, 200);

        // Insert first data page
        cache.get().insert_page(0, 1000, data_page1.clone());
        assert!(cache.get().get_page(0, 1000).is_some());

        // Insert second data page - should replace first data page
        cache.get().insert_page(0, 2000, data_page2.clone());
        assert!(cache.get().get_page(0, 2000).is_some());
        assert!(cache.get().get_page(0, 1000).is_none()); // First page should be gone
    }

    #[test]
    fn test_predicate_page_cache_multiple_columns() {
        use super::*;

        let cache = PredicatePageCache::new(2);
        let page1 = Page::dummy_page(PageType::DATA_PAGE, 100);
        let page2 = Page::dummy_page(PageType::DATA_PAGE_V2, 200);

        // Insert pages for different columns
        cache.get().insert_page(0, 1000, page1.clone());
        cache.get().insert_page(1, 1000, page2.clone());

        // Both pages should be accessible
        assert!(cache.get().get_page(0, 1000).is_some());
        assert!(cache.get().get_page(1, 1000).is_some());

        // Non-existent column should return None
        assert!(cache.get().get_page(2, 1000).is_none());
    }
}

// Helper implementation for testing
#[cfg(test)]
impl Page {
    fn dummy_page(page_type: PageType, size: usize) -> Self {
        use crate::basic::Encoding;
        match page_type {
            PageType::DATA_PAGE => Page::DataPage {
                buf: vec![0; size].into(),
                num_values: size as u32,
                encoding: Encoding::PLAIN,
                def_level_encoding: Encoding::PLAIN,
                rep_level_encoding: Encoding::PLAIN,
                statistics: None,
            },
            PageType::DICTIONARY_PAGE => Page::DictionaryPage {
                buf: vec![0; size].into(),
                num_values: size as u32,
                encoding: Encoding::PLAIN,
                is_sorted: false,
            },
            PageType::DATA_PAGE_V2 => Page::DataPageV2 {
                buf: vec![0; size].into(),
                num_values: size as u32,
                encoding: Encoding::PLAIN,
                def_levels_byte_len: 0,
                rep_levels_byte_len: 0,
                is_compressed: false,
                statistics: None,
                num_nulls: 0,
                num_rows: 0,
            },
            _ => unreachable!(),
        }
    }
}
