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

use crate::arrow::array_reader::row_group_cache::BatchID;
use crate::arrow::array_reader::{row_group_cache::RowGroupCache, ArrayReader};
use crate::arrow::arrow_reader::RowSelector;
use crate::errors::Result;
use arrow_array::{new_empty_array, ArrayRef, BooleanArray};
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder};
use arrow_schema::DataType as ArrowType;
use std::any::Any;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

/// Role of the cached array reader
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheRole {
    /// Producer role: inserts data into the cache during filter phase
    Producer,
    /// Consumer role: removes consumed data from the cache during output building phase
    Consumer,
}

/// A cached wrapper around an ArrayReader that avoids duplicate decoding
/// when the same column appears in both filter predicates and output projection.
///
/// This reader acts as a transparent layer over the inner reader, using a cache
/// to avoid redundant work when the same data is needed multiple times.
///
/// The reader can operate in two roles:
/// - Producer: During filter phase, inserts decoded data into the cache
/// - Consumer: During output building, consumes and removes data from the cache
///
/// This means the memory consumption of the cache has two stages:
/// 1. During the filter phase, the memory increases as the cache is populated
/// 2. It peaks when filters are built.
/// 3. It decreases as the cached data is consumed.
///    ▲
///    │     ╭─╮
///    │    ╱   ╲
///    │   ╱     ╲
///    │  ╱       ╲
///    │ ╱         ╲
///    │╱           ╲
///    └─────────────╲──────► Time
///    │      │      │
///    Filter  Peak  Consume
///    Phase (Built) (Decrease)
pub struct CachedArrayReader {
    /// The underlying array reader
    inner: Box<dyn ArrayReader>,
    /// Shared cache for this row group
    cache: Arc<Mutex<RowGroupCache>>,
    /// Column index for cache key generation
    column_idx: usize,
    /// Current logical position in the data stream (for cache key generation)
    outer_position: usize,
    /// Current position in the inner reader
    inner_position: usize,
    /// Batch size for the cache
    batch_size: usize,
    /// Selections to be applied to the next consume_batch()
    selections: VecDeque<RowSelector>,
    /// Role of this reader (Producer or Consumer)
    role: CacheRole,
    /// Local buffer to store batches between read_records and consume_batch calls
    /// This ensures data is available even if the shared cache evicts items
    local_buffer: HashMap<BatchID, ArrayRef>,
}

impl CachedArrayReader {
    /// Creates a new cached array reader with the specified role
    pub fn new(
        inner: Box<dyn ArrayReader>,
        cache: Arc<Mutex<RowGroupCache>>,
        column_idx: usize,
        role: CacheRole,
    ) -> Self {
        let batch_size = cache.lock().unwrap().batch_size();

        Self {
            inner,
            cache,
            column_idx,
            outer_position: 0,
            inner_position: 0,
            batch_size,
            selections: VecDeque::new(),
            role,
            local_buffer: HashMap::new(),
        }
    }

    fn get_batch_id_from_position(&self, row_id: usize) -> BatchID {
        BatchID {
            val: row_id / self.batch_size,
        }
    }

    fn fetch_batch(&mut self, batch_id: BatchID) -> Result<usize> {
        let row_id = batch_id.val * self.batch_size;
        if self.inner_position < row_id {
            let to_skip = row_id - self.inner_position;
            let skipped = self.inner.skip_records(to_skip)?;
            assert_eq!(skipped, to_skip);
            self.inner_position += skipped;
        }

        let read = self.inner.read_records(self.batch_size)?;

        // If there are no remaining records (EOF), return immediately without
        // attempting to cache an empty batch. This prevents inserting zero-length
        // arrays into the cache which can later cause panics when slicing.
        if read == 0 {
            return Ok(0);
        }

        let array = self.inner.consume_batch()?;

        // Store in both shared cache and local cache
        // The shared cache is for coordination between readers
        // The local cache ensures data is available for our consume_batch call
        let _cached = self
            .cache
            .lock()
            .unwrap()
            .insert(self.column_idx, batch_id, array.clone());
        // Note: if the shared cache is full (_cached == false), we continue without caching
        // The local cache will still store the data for this reader's use

        self.local_buffer.insert(batch_id, array);

        self.inner_position += read;
        Ok(read)
    }

    /// Remove batches from cache that have been completely consumed
    /// This is only called for Consumer role readers
    fn cleanup_consumed_batches(&mut self) {
        let current_batch_id = self.get_batch_id_from_position(self.outer_position);

        // Remove batches that are at least one batch behind the current position
        // This ensures we don't remove batches that might still be needed for the current batch
        // We can safely remove batch_id if current_batch_id > batch_id + 1
        if current_batch_id.val > 1 {
            let mut cache = self.cache.lock().unwrap();
            for batch_id_to_remove in 0..(current_batch_id.val - 1) {
                cache.remove(
                    self.column_idx,
                    BatchID {
                        val: batch_id_to_remove,
                    },
                );
            }
        }
    }
}

impl ArrayReader for CachedArrayReader {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_data_type(&self) -> &ArrowType {
        self.inner.get_data_type()
    }

    fn read_records(&mut self, num_records: usize) -> Result<usize> {
        let mut read = 0;
        while read < num_records {
            let batch_id = self.get_batch_id_from_position(self.outer_position + read);

            // Check local cache first
            let cached = if let Some(array) = self.local_buffer.get(&batch_id) {
                Some(array.clone())
            } else {
                // If not in local cache, check shared cache
                let shared_cached = self.cache.lock().unwrap().get(self.column_idx, batch_id);
                if let Some(array) = shared_cached.as_ref() {
                    // Store in local cache for later use in consume_batch
                    self.local_buffer.insert(batch_id, array.clone());
                }
                shared_cached
            };

            match cached {
                Some(array) => {
                    let array_len = array.len();
                    if array_len + batch_id.val * self.batch_size - self.outer_position > 0 {
                        // the cache batch has some records that we can select
                        let v = array_len + batch_id.val * self.batch_size - self.outer_position;
                        let select_cnt = std::cmp::min(num_records - read, v);
                        read += select_cnt;
                        self.selections.push_back(RowSelector::select(select_cnt));
                    } else {
                        // this is last batch and we have used all records from it
                        break;
                    }
                }
                None => {
                    let read_from_inner = self.fetch_batch(batch_id)?;

                    // Reached end-of-file, no more records to read
                    if read_from_inner == 0 {
                        break;
                    }

                    let select_from_this_batch = std::cmp::min(num_records - read, read_from_inner);
                    read += select_from_this_batch;
                    self.selections
                        .push_back(RowSelector::select(select_from_this_batch));
                    if read_from_inner < self.batch_size {
                        // this is last batch from inner reader
                        break;
                    }
                }
            }
        }
        self.outer_position += read;
        Ok(read)
    }

    fn skip_records(&mut self, num_records: usize) -> Result<usize> {
        let mut skipped = 0;
        while skipped < num_records {
            let size = std::cmp::min(num_records - skipped, self.batch_size);
            skipped += size;
            self.selections.push_back(RowSelector::skip(size));
            self.outer_position += size;
        }
        Ok(num_records)
    }

    fn consume_batch(&mut self) -> Result<ArrayRef> {
        let row_count = self.selections.iter().map(|s| s.row_count).sum::<usize>();
        if row_count == 0 {
            return Ok(new_empty_array(self.inner.get_data_type()));
        }

        let start_position = self.outer_position - row_count;

        let selection_buffer = row_selection_to_boolean_buffer(row_count, self.selections.iter());

        let start_batch = start_position / self.batch_size;
        let end_batch = (start_position + row_count - 1) / self.batch_size;

        let mut selected_arrays = Vec::new();
        for batch_id in start_batch..=end_batch {
            let batch_start = batch_id * self.batch_size;
            let batch_end = batch_start + self.batch_size - 1;
            let batch_id = self.get_batch_id_from_position(batch_start);

            // Calculate the overlap between the start_position and the batch
            let overlap_start = start_position.max(batch_start);
            let overlap_end = (start_position + row_count - 1).min(batch_end);

            if overlap_start > overlap_end {
                continue;
            }

            let selection_start = overlap_start - start_position;
            let selection_length = overlap_end - overlap_start + 1;
            let mask = selection_buffer.slice(selection_start, selection_length);

            if mask.count_set_bits() == 0 {
                continue;
            }

            let mask_array = BooleanArray::from(mask);
            // Read from local cache instead of shared cache to avoid cache eviction issues
            let cached = self
                .local_buffer
                .get(&batch_id)
                .expect("data must be already cached in the read_records call, this is a bug");
            let cached = cached.slice(overlap_start - batch_start, selection_length);
            let filtered = arrow_select::filter::filter(&cached, &mask_array)?;
            selected_arrays.push(filtered);
        }

        self.selections.clear();
        self.local_buffer.clear();

        // For consumers, cleanup batches that have been completely consumed
        // This reduces the memory usage of the shared cache
        if self.role == CacheRole::Consumer {
            self.cleanup_consumed_batches();
        }

        match selected_arrays.len() {
            0 => Ok(new_empty_array(self.inner.get_data_type())),
            1 => Ok(selected_arrays.into_iter().next().unwrap()),
            _ => Ok(arrow_select::concat::concat(
                &selected_arrays
                    .iter()
                    .map(|a| a.as_ref())
                    .collect::<Vec<_>>(),
            )?),
        }
    }

    fn get_def_levels(&self) -> Option<&[i16]> {
        None // we don't allow nullable parent for now.
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        None
    }
}

fn row_selection_to_boolean_buffer<'a>(
    row_count: usize,
    selection: impl Iterator<Item = &'a RowSelector>,
) -> BooleanBuffer {
    let mut buffer = BooleanBufferBuilder::new(row_count);
    for selector in selection {
        buffer.append_n(selector.row_count, !selector.skip);
    }
    buffer.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array_reader::row_group_cache::RowGroupCache;
    use crate::arrow::array_reader::ArrayReader;
    use arrow_array::{ArrayRef, Int32Array};
    use std::sync::{Arc, Mutex};

    // Mock ArrayReader for testing
    struct MockArrayReader {
        data: Vec<i32>,
        position: usize,
        records_to_consume: usize,
        data_type: ArrowType,
    }

    impl MockArrayReader {
        fn new(data: Vec<i32>) -> Self {
            Self {
                data,
                position: 0,
                records_to_consume: 0,
                data_type: ArrowType::Int32,
            }
        }
    }

    impl ArrayReader for MockArrayReader {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn get_data_type(&self) -> &ArrowType {
            &self.data_type
        }

        fn read_records(&mut self, batch_size: usize) -> Result<usize> {
            let remaining = self.data.len() - self.position;
            let to_read = std::cmp::min(batch_size, remaining);
            self.records_to_consume += to_read;
            Ok(to_read)
        }

        fn consume_batch(&mut self) -> Result<ArrayRef> {
            let start = self.position;
            let end = start + self.records_to_consume;
            let slice = &self.data[start..end];
            self.position = end;
            self.records_to_consume = 0;
            Ok(Arc::new(Int32Array::from(slice.to_vec())))
        }

        fn skip_records(&mut self, num_records: usize) -> Result<usize> {
            let remaining = self.data.len() - self.position;
            let to_skip = std::cmp::min(num_records, remaining);
            self.position += to_skip;
            Ok(to_skip)
        }

        fn get_def_levels(&self) -> Option<&[i16]> {
            None
        }

        fn get_rep_levels(&self) -> Option<&[i16]> {
            None
        }
    }

    #[test]
    fn test_cached_reader_basic() {
        let mock_reader = MockArrayReader::new(vec![1, 2, 3, 4, 5]);
        let cache = Arc::new(Mutex::new(RowGroupCache::new(3, None))); // Batch size 3
        let mut cached_reader =
            CachedArrayReader::new(Box::new(mock_reader), cache, 0, CacheRole::Producer);

        // Read 3 records
        let records_read = cached_reader.read_records(3).unwrap();
        assert_eq!(records_read, 3);

        let array = cached_reader.consume_batch().unwrap();
        assert_eq!(array.len(), 3);

        let int32_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int32_array.values(), &[1, 2, 3]);

        // Read 3 more records
        let records_read = cached_reader.read_records(3).unwrap();
        assert_eq!(records_read, 2);
    }

    #[test]
    fn test_read_skip_pattern() {
        let mock_reader = MockArrayReader::new(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let cache = Arc::new(Mutex::new(RowGroupCache::new(5, None))); // Batch size 5
        let mut cached_reader =
            CachedArrayReader::new(Box::new(mock_reader), cache, 0, CacheRole::Consumer);

        let read1 = cached_reader.read_records(2).unwrap();
        assert_eq!(read1, 2);

        let array1 = cached_reader.consume_batch().unwrap();
        assert_eq!(array1.len(), 2);
        let int32_array = array1.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int32_array.values(), &[1, 2]);

        let skipped = cached_reader.skip_records(2).unwrap();
        assert_eq!(skipped, 2);

        let read2 = cached_reader.read_records(1).unwrap();
        assert_eq!(read2, 1);

        // Consume it (should be the 5th element after skipping 3,4)
        let array2 = cached_reader.consume_batch().unwrap();
        assert_eq!(array2.len(), 1);
        let int32_array = array2.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int32_array.values(), &[5]);
    }

    #[test]
    fn test_multiple_reads_before_consume() {
        let mock_reader = MockArrayReader::new(vec![1, 2, 3, 4, 5, 6]);
        let cache = Arc::new(Mutex::new(RowGroupCache::new(3, None))); // Batch size 3
        let mut cached_reader =
            CachedArrayReader::new(Box::new(mock_reader), cache, 0, CacheRole::Consumer);

        // Multiple reads should accumulate
        let read1 = cached_reader.read_records(2).unwrap();
        assert_eq!(read1, 2);

        let read2 = cached_reader.read_records(1).unwrap();
        assert_eq!(read2, 1);

        // Consume should return all accumulated records
        let array = cached_reader.consume_batch().unwrap();
        assert_eq!(array.len(), 3);
        let int32_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int32_array.values(), &[1, 2, 3]);
    }

    #[test]
    fn test_eof_behavior() {
        let mock_reader = MockArrayReader::new(vec![1, 2, 3]);
        let cache = Arc::new(Mutex::new(RowGroupCache::new(5, None))); // Batch size 5
        let mut cached_reader =
            CachedArrayReader::new(Box::new(mock_reader), cache, 0, CacheRole::Consumer);

        // Try to read more than available
        let read1 = cached_reader.read_records(5).unwrap();
        assert_eq!(read1, 3); // Should only get 3 records (all available)

        let array1 = cached_reader.consume_batch().unwrap();
        assert_eq!(array1.len(), 3);

        // Further reads should return 0
        let read2 = cached_reader.read_records(1).unwrap();
        assert_eq!(read2, 0);

        let array2 = cached_reader.consume_batch().unwrap();
        assert_eq!(array2.len(), 0);
    }

    #[test]
    fn test_cache_sharing() {
        let cache = Arc::new(Mutex::new(RowGroupCache::new(5, None))); // Batch size 5

        // First reader - populate cache
        let mock_reader1 = MockArrayReader::new(vec![1, 2, 3, 4, 5]);
        let mut cached_reader1 = CachedArrayReader::new(
            Box::new(mock_reader1),
            cache.clone(),
            0,
            CacheRole::Producer,
        );

        cached_reader1.read_records(3).unwrap();
        let array1 = cached_reader1.consume_batch().unwrap();
        assert_eq!(array1.len(), 3);

        // Second reader with different column index should not interfere
        let mock_reader2 = MockArrayReader::new(vec![10, 20, 30, 40, 50]);
        let mut cached_reader2 = CachedArrayReader::new(
            Box::new(mock_reader2),
            cache.clone(),
            1,
            CacheRole::Consumer,
        );

        cached_reader2.read_records(2).unwrap();
        let array2 = cached_reader2.consume_batch().unwrap();
        assert_eq!(array2.len(), 2);

        // Verify the second reader got its own data, not from cache
        let int32_array = array2.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int32_array.values(), &[10, 20]);
    }

    #[test]
    fn test_consumer_removes_batches() {
        let mock_reader = MockArrayReader::new(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let cache = Arc::new(Mutex::new(RowGroupCache::new(3, None))); // Batch size 3
        let mut consumer_reader =
            CachedArrayReader::new(Box::new(mock_reader), cache.clone(), 0, CacheRole::Consumer);

        // Read first batch (positions 0-2, batch 0)
        let read1 = consumer_reader.read_records(3).unwrap();
        assert_eq!(read1, 3);
        assert_eq!(consumer_reader.outer_position, 3);
        // Check that batch 0 is in cache after read_records
        assert!(cache.lock().unwrap().get(0, BatchID { val: 0 }).is_some());

        let array1 = consumer_reader.consume_batch().unwrap();
        assert_eq!(array1.len(), 3);

        // After first consume_batch, batch 0 should still be in cache
        // (current_batch_id = 3/3 = 1, cleanup only happens if current_batch_id > 1)
        assert!(cache.lock().unwrap().get(0, BatchID { val: 0 }).is_some());

        // Read second batch (positions 3-5, batch 1)
        let read2 = consumer_reader.read_records(3).unwrap();
        assert_eq!(read2, 3);
        assert_eq!(consumer_reader.outer_position, 6);
        let array2 = consumer_reader.consume_batch().unwrap();
        assert_eq!(array2.len(), 3);

        // After second consume_batch, batch 0 should be removed
        // (current_batch_id = 6/3 = 2, cleanup removes batches 0..(2-1) = 0..1, so removes batch 0)
        assert!(cache.lock().unwrap().get(0, BatchID { val: 0 }).is_none());
        assert!(cache.lock().unwrap().get(0, BatchID { val: 1 }).is_some());

        // Read third batch (positions 6-8, batch 2)
        let read3 = consumer_reader.read_records(3).unwrap();
        assert_eq!(read3, 3);
        assert_eq!(consumer_reader.outer_position, 9);
        let array3 = consumer_reader.consume_batch().unwrap();
        assert_eq!(array3.len(), 3);

        // After third consume_batch, batches 0 and 1 should be removed
        // (current_batch_id = 9/3 = 3, cleanup removes batches 0..(3-1) = 0..2, so removes batches 0 and 1)
        assert!(cache.lock().unwrap().get(0, BatchID { val: 0 }).is_none());
        assert!(cache.lock().unwrap().get(0, BatchID { val: 1 }).is_none());
        assert!(cache.lock().unwrap().get(0, BatchID { val: 2 }).is_some());
    }

    #[test]
    fn test_producer_keeps_batches() {
        let mock_reader = MockArrayReader::new(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let cache = Arc::new(Mutex::new(RowGroupCache::new(3, None))); // Batch size 3
        let mut producer_reader =
            CachedArrayReader::new(Box::new(mock_reader), cache.clone(), 0, CacheRole::Producer);

        // Read first batch (positions 0-2)
        let read1 = producer_reader.read_records(3).unwrap();
        assert_eq!(read1, 3);
        let array1 = producer_reader.consume_batch().unwrap();
        assert_eq!(array1.len(), 3);

        // Verify batch 0 is in cache
        assert!(cache.lock().unwrap().get(0, BatchID { val: 0 }).is_some());

        // Read second batch (positions 3-5) - producer should NOT remove batch 0
        let read2 = producer_reader.read_records(3).unwrap();
        assert_eq!(read2, 3);
        let array2 = producer_reader.consume_batch().unwrap();
        assert_eq!(array2.len(), 3);

        // Verify both batch 0 and batch 1 are still present (no removal for producer)
        assert!(cache.lock().unwrap().get(0, BatchID { val: 0 }).is_some());
        assert!(cache.lock().unwrap().get(0, BatchID { val: 1 }).is_some());
    }

    #[test]
    fn test_local_cache_protects_against_eviction() {
        let mock_reader = MockArrayReader::new(vec![1, 2, 3, 4, 5, 6]);
        let cache = Arc::new(Mutex::new(RowGroupCache::new(3, None))); // Batch size 3
        let mut cached_reader =
            CachedArrayReader::new(Box::new(mock_reader), cache.clone(), 0, CacheRole::Consumer);

        // Read records which should populate both shared and local cache
        let records_read = cached_reader.read_records(3).unwrap();
        assert_eq!(records_read, 3);

        // Verify data is in both caches
        assert!(cache.lock().unwrap().get(0, BatchID { val: 0 }).is_some());
        assert!(cached_reader.local_buffer.contains_key(&BatchID { val: 0 }));

        // Simulate cache eviction by manually removing from shared cache
        cache.lock().unwrap().remove(0, BatchID { val: 0 });
        assert!(cache.lock().unwrap().get(0, BatchID { val: 0 }).is_none());

        // Even though shared cache was evicted, consume_batch should still work
        // because data is preserved in local cache
        let array = cached_reader.consume_batch().unwrap();
        assert_eq!(array.len(), 3);

        let int32_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int32_array.values(), &[1, 2, 3]);

        // Local cache should be cleared after consume_batch
        assert!(cached_reader.local_buffer.is_empty());
    }
}
