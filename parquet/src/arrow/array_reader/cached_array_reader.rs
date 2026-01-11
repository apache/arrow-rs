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

//! [`CachedArrayReader`] wrapper around [`ArrayReader`]

use crate::arrow::array_reader::row_group_cache::BatchID;
use crate::arrow::array_reader::{ArrayReader, row_group_cache::RowGroupCache};
use crate::arrow::arrow_reader::metrics::ArrowReaderMetrics;
use crate::errors::Result;
use arrow_array::{ArrayRef, BooleanArray, new_empty_array};
use arrow_buffer::BooleanBufferBuilder;
use arrow_schema::DataType as ArrowType;
use std::any::Any;
use std::collections::HashMap;
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
///
/// ```text
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
/// ```
pub struct CachedArrayReader {
    /// The underlying array reader
    inner: Box<dyn ArrayReader>,
    /// Shared cache for this row group
    shared_cache: Arc<Mutex<RowGroupCache>>,
    /// Column index for cache key generation
    column_idx: usize,
    /// Current logical position in the data stream for this reader (for cache key generation)
    outer_position: usize,
    /// Current position in `inner`
    inner_position: usize,
    /// Batch size for the cache
    batch_size: usize,
    /// Boolean buffer builder to track selections for the next consume_batch()
    selections: BooleanBufferBuilder,
    /// Role of this reader (Producer or Consumer)
    role: CacheRole,
    /// Local cache to store batches between read_records and consume_batch calls
    /// This ensures data is available even if the shared cache evicts items
    local_cache: HashMap<BatchID, ArrayRef>,
    /// Statistics to report on the Cache behavior
    metrics: ArrowReaderMetrics,
}

impl CachedArrayReader {
    /// Creates a new cached array reader with the specified role
    pub fn new(
        inner: Box<dyn ArrayReader>,
        cache: Arc<Mutex<RowGroupCache>>,
        column_idx: usize,
        role: CacheRole,
        metrics: ArrowReaderMetrics,
    ) -> Self {
        let batch_size = cache.lock().unwrap().batch_size();

        Self {
            inner,
            shared_cache: cache,
            column_idx,
            outer_position: 0,
            inner_position: 0,
            batch_size,
            selections: BooleanBufferBuilder::new(0),
            role,
            local_cache: HashMap::new(),
            metrics,
        }
    }

    fn get_batch_id_from_position(&self, row_id: usize) -> BatchID {
        BatchID {
            val: row_id / self.batch_size,
        }
    }

    /// Loads the batch with the given ID (first row offset) from the inner
    /// reader
    ///
    /// After this call the required batch will be available in
    /// `self.local_cache` and may also be stored in `self.shared_cache`.
    ///
    fn fetch_batch(&mut self, batch_id: BatchID) -> Result<usize> {
        let first_row_offset = batch_id.val * self.batch_size;
        if self.inner_position < first_row_offset {
            let to_skip = first_row_offset - self.inner_position;
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
        // The shared cache is used to reuse results between readers
        // The local cache ensures data is available for our consume_batch call
        let _cached =
            self.shared_cache
                .lock()
                .unwrap()
                .insert(self.column_idx, batch_id, array.clone());
        // Note: if the shared cache is full (_cached == false), we continue without caching
        // The local cache will still store the data for this reader's use

        self.local_cache.insert(batch_id, array);

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
            let mut cache = self.shared_cache.lock().unwrap();
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
            let batch_id = self.get_batch_id_from_position(self.outer_position);

            // Check local cache first
            let cached = if let Some(array) = self.local_cache.get(&batch_id) {
                Some(Arc::clone(array))
            } else {
                // If not in local cache, i.e., we are consumer, check shared cache
                let cache_content = self
                    .shared_cache
                    .lock()
                    .unwrap()
                    .get(self.column_idx, batch_id);
                if let Some(array) = cache_content.as_ref() {
                    // Store in local cache for later use in consume_batch
                    self.local_cache.insert(batch_id, Arc::clone(array));
                }
                cache_content
            };

            match cached {
                Some(array) => {
                    let array_len = array.len();
                    if array_len + batch_id.val * self.batch_size > self.outer_position {
                        // the cache batch has some records that we can select
                        let v = array_len + batch_id.val * self.batch_size - self.outer_position;
                        let select_cnt = std::cmp::min(num_records - read, v);
                        read += select_cnt;
                        self.metrics.increment_cache_reads(select_cnt);
                        self.outer_position += select_cnt;
                        self.selections.append_n(select_cnt, true);
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
                    self.metrics.increment_inner_reads(read_from_inner);
                    let select_from_this_batch = std::cmp::min(
                        num_records - read,
                        self.inner_position - self.outer_position,
                    );
                    read += select_from_this_batch;
                    self.outer_position += select_from_this_batch;
                    self.selections.append_n(select_from_this_batch, true);
                    if read_from_inner < self.batch_size {
                        // this is last batch from inner reader
                        break;
                    }
                }
            }
        }
        Ok(read)
    }

    fn skip_records(&mut self, num_records: usize) -> Result<usize> {
        let mut skipped = 0;
        while skipped < num_records {
            let size = std::cmp::min(num_records - skipped, self.batch_size);
            skipped += size;
            self.selections.append_n(size, false);
            self.outer_position += size;
        }
        Ok(num_records)
    }

    fn consume_batch(&mut self) -> Result<ArrayRef> {
        let row_count = self.selections.len();
        if row_count == 0 {
            return Ok(new_empty_array(self.inner.get_data_type()));
        }

        let start_position = self.outer_position - row_count;

        let selection_buffer = self.selections.finish();

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
                .local_cache
                .get(&batch_id)
                .expect("data must be already cached in the read_records call, this is a bug");
            let cached = cached.slice(overlap_start - batch_start, selection_length);
            let filtered = arrow_select::filter::filter(&cached, &mask_array)?;
            selected_arrays.push(filtered);
        }

        self.selections = BooleanBufferBuilder::new(0);

        // Only remove batches from local buffer that are completely behind current position
        // Keep the current batch and any future batches as they might still be needed
        let current_batch_id = self.get_batch_id_from_position(self.outer_position);
        self.local_cache
            .retain(|batch_id, _| batch_id.val >= current_batch_id.val);

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array_reader::ArrayReader;
    use crate::arrow::array_reader::row_group_cache::RowGroupCache;
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
        let metrics = ArrowReaderMetrics::disabled();
        let mock_reader = MockArrayReader::new(vec![1, 2, 3, 4, 5]);
        let cache = Arc::new(Mutex::new(RowGroupCache::new(3, usize::MAX))); // Batch size 3
        let mut cached_reader = CachedArrayReader::new(
            Box::new(mock_reader),
            cache,
            0,
            CacheRole::Producer,
            metrics,
        );

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
        let metrics = ArrowReaderMetrics::disabled();
        let mock_reader = MockArrayReader::new(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let cache = Arc::new(Mutex::new(RowGroupCache::new(5, usize::MAX))); // Batch size 5
        let mut cached_reader = CachedArrayReader::new(
            Box::new(mock_reader),
            cache,
            0,
            CacheRole::Consumer,
            metrics,
        );

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
        let metrics = ArrowReaderMetrics::disabled();
        let mock_reader = MockArrayReader::new(vec![1, 2, 3, 4, 5, 6]);
        let cache = Arc::new(Mutex::new(RowGroupCache::new(3, usize::MAX))); // Batch size 3
        let mut cached_reader = CachedArrayReader::new(
            Box::new(mock_reader),
            cache,
            0,
            CacheRole::Consumer,
            metrics,
        );

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
        let metrics = ArrowReaderMetrics::disabled();
        let mock_reader = MockArrayReader::new(vec![1, 2, 3]);
        let cache = Arc::new(Mutex::new(RowGroupCache::new(5, usize::MAX))); // Batch size 5
        let mut cached_reader = CachedArrayReader::new(
            Box::new(mock_reader),
            cache,
            0,
            CacheRole::Consumer,
            metrics,
        );

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
        let metrics = ArrowReaderMetrics::disabled();
        let cache = Arc::new(Mutex::new(RowGroupCache::new(5, usize::MAX))); // Batch size 5

        // First reader - populate cache
        let mock_reader1 = MockArrayReader::new(vec![1, 2, 3, 4, 5]);
        let mut cached_reader1 = CachedArrayReader::new(
            Box::new(mock_reader1),
            cache.clone(),
            0,
            CacheRole::Producer,
            metrics.clone(),
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
            metrics.clone(),
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
        let metrics = ArrowReaderMetrics::disabled();
        let mock_reader = MockArrayReader::new(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let cache = Arc::new(Mutex::new(RowGroupCache::new(3, usize::MAX))); // Batch size 3
        let mut consumer_reader = CachedArrayReader::new(
            Box::new(mock_reader),
            cache.clone(),
            0,
            CacheRole::Consumer,
            metrics,
        );

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
        let metrics = ArrowReaderMetrics::disabled();
        let mock_reader = MockArrayReader::new(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let cache = Arc::new(Mutex::new(RowGroupCache::new(3, usize::MAX))); // Batch size 3
        let mut producer_reader = CachedArrayReader::new(
            Box::new(mock_reader),
            cache.clone(),
            0,
            CacheRole::Producer,
            metrics,
        );

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
        let metrics = ArrowReaderMetrics::disabled();
        let mock_reader = MockArrayReader::new(vec![1, 2, 3, 4, 5, 6]);
        let cache = Arc::new(Mutex::new(RowGroupCache::new(3, usize::MAX))); // Batch size 3
        let mut cached_reader = CachedArrayReader::new(
            Box::new(mock_reader),
            cache.clone(),
            0,
            CacheRole::Consumer,
            metrics,
        );

        // Read records which should populate both shared and local cache
        let records_read = cached_reader.read_records(3).unwrap();
        assert_eq!(records_read, 3);

        // Verify data is in both caches
        assert!(cache.lock().unwrap().get(0, BatchID { val: 0 }).is_some());
        assert!(cached_reader.local_cache.contains_key(&BatchID { val: 0 }));

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
        assert!(cached_reader.local_cache.is_empty());
    }

    #[test]
    fn test_local_cache_is_cleared_properly() {
        let metrics = ArrowReaderMetrics::disabled();
        let mock_reader = MockArrayReader::new(vec![1, 2, 3, 4]);
        let cache = Arc::new(Mutex::new(RowGroupCache::new(3, 0))); // Batch size 3, cache 0
        let mut cached_reader = CachedArrayReader::new(
            Box::new(mock_reader),
            cache.clone(),
            0,
            CacheRole::Consumer,
            metrics,
        );

        // Read records which should populate both shared and local cache
        let records_read = cached_reader.read_records(1).unwrap();
        assert_eq!(records_read, 1);
        let array = cached_reader.consume_batch().unwrap();
        assert_eq!(array.len(), 1);

        let records_read = cached_reader.read_records(3).unwrap();
        assert_eq!(records_read, 3);
        let array = cached_reader.consume_batch().unwrap();
        assert_eq!(array.len(), 3);
    }

    #[test]
    fn test_batch_id_calculation_with_incremental_reads() {
        let metrics = ArrowReaderMetrics::disabled();
        let mock_reader = MockArrayReader::new(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let cache = Arc::new(Mutex::new(RowGroupCache::new(3, usize::MAX))); // Batch size 3

        // Create a producer to populate cache
        let mut producer = CachedArrayReader::new(
            Box::new(MockArrayReader::new(vec![1, 2, 3, 4, 5, 6, 7, 8, 9])),
            cache.clone(),
            0,
            CacheRole::Producer,
            metrics.clone(),
        );

        // Populate cache with first batch (1, 2, 3)
        producer.read_records(3).unwrap();
        producer.consume_batch().unwrap();

        // Now create a consumer that will try to read from cache
        let mut consumer = CachedArrayReader::new(
            Box::new(mock_reader),
            cache.clone(),
            0,
            CacheRole::Consumer,
            metrics,
        );

        // - We want to read 4 records starting from position 0
        // - First 3 records (positions 0-2) should come from cache (batch 0)
        // - The 4th record (position 3) should come from the next batch
        let records_read = consumer.read_records(4).unwrap();
        assert_eq!(records_read, 4);

        let array = consumer.consume_batch().unwrap();
        assert_eq!(array.len(), 4);

        let int32_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int32_array.values(), &[1, 2, 3, 4]);
    }
}
