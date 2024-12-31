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
use std::sync::RwLock;
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

    #[inline(never)]
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
    dict: Option<(usize, Page)>,
    data: Option<(usize, Page)>,
}

struct PageCacheInner {
    pages: HashMap<usize, CachedPage>, // col_id -> CachedPage
}

/// A simple cache for decompressed pages.
/// We cache only one dictionary page and one data page per column
pub(crate) struct PageCache {
    inner: RwLock<PageCacheInner>,
}

impl PageCache {
    const CAPACITY: usize = 16;

    pub(crate) fn new() -> Self {
        Self {
            inner: RwLock::new(PageCacheInner {
                pages: HashMap::with_capacity(Self::CAPACITY),
            }),
        }
    }

    pub(crate) fn get_page(&self, col_id: usize, offset: usize) -> Option<Page> {
        let read_lock = self.inner.read().unwrap();
        read_lock.pages.get(&col_id).and_then(|pages| {
            pages
                .dict
                .iter()
                .chain(pages.data.iter())
                .find(|(page_offset, _)| *page_offset == offset)
                .map(|(_, page)| page.clone())
        })
    }

    pub(crate) fn insert_page(&self, col_id: usize, offset: usize, page: Page) {
        let mut write_lock = self.inner.write().unwrap();

        let is_dict = page.page_type() == PageType::DICTIONARY_PAGE;

        let cached_pages = write_lock.pages.entry(col_id);
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

pub(crate) struct CachedPageReader<R: ChunkReader> {
    inner: SerializedPageReader<R>,
    cache: Arc<PageCache>,
    col_id: usize,
}

impl<R: ChunkReader> CachedPageReader<R> {
    pub(crate) fn new(
        inner: SerializedPageReader<R>,
        cache: Arc<PageCache>,
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
        // self.inner.get_next_page()
        let next_page_offset = self.inner.peek_next_page_offset()?;

        let Some(offset) = next_page_offset else {
            return Ok(None);
        };

        let page = self.cache.get_page(self.col_id, offset);
        if let Some(page) = page {
            self.inner.skip_next_page()?;
            Ok(Some(page))
        } else {
            let inner_page = self.inner.get_next_page()?;
            let Some(inner_page) = inner_page else {
                return Ok(None);
            };
            self.cache
                .insert_page(self.col_id, offset, inner_page.clone());
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
}
