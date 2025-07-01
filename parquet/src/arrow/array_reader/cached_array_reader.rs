use crate::arrow::array_reader::{row_group_cache::RowGroupCache, ArrayReader};
use crate::arrow::arrow_reader::RowSelector;
use crate::errors::Result;
use arrow_array::{new_empty_array, ArrayRef};
use arrow_schema::DataType as ArrowType;
use std::any::Any;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

/// A cached wrapper around an ArrayReader that avoids duplicate decoding
/// when the same column appears in both filter predicates and output projection.
///
/// This reader acts as a transparent layer over the inner reader, using a cache
/// to avoid redundant work when the same data is needed multiple times.
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
    /// Number of records accumulated to be returned in next consume_batch()
    pending_records: usize,
    /// Number of records to skip in next batch operation
    pending_skips: usize,
    /// Batch size for the cache
    batch_size: usize,
    selections: VecDeque<RowSelector>,
}

impl CachedArrayReader {
    /// Creates a new cached array reader
    pub fn new(
        inner: Box<dyn ArrayReader>,
        cache: Arc<Mutex<RowGroupCache>>,
        column_idx: usize,
    ) -> Self {
        let batch_size = cache.lock().unwrap().batch_size();
        Self {
            inner,
            cache,
            column_idx,
            outer_position: 0,
            inner_position: 0,
            pending_records: 0,
            pending_skips: 0,
            batch_size,
            selections: VecDeque::new(),
        }
    }

    /// Gets the batch size from the cache
    fn batch_size(&self) -> usize {
        self.batch_size
    }

    fn get_batch_id_from_position(&self, position: usize) -> usize {
        position / self.batch_size
    }

    fn sync_inner_position(&mut self) {}

    fn fetch_batch(&mut self, batch_id: usize) -> Result<usize> {
        let row_id = batch_id * self.batch_size;
        if self.inner_position < row_id {
            let to_skip = row_id - self.inner_position;
            let skipped = self.inner.skip_records(to_skip)?;
            self.inner_position += skipped;
        }

        let read = self.inner.read_records(self.batch_size)?;
        let array = self.inner.consume_batch()?;
        self.cache
            .lock()
            .unwrap()
            .insert(self.column_idx, batch_id, array);
        self.inner_position += read;
        Ok(read)
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
            let batch_id = self.get_batch_id_from_position(self.outer_position + read + 1); // +1 because we want to read the next batch
            let cached = self.cache.lock().unwrap().get(self.column_idx, batch_id);

            match cached {
                Some(array) => {
                    let array_len = array.len();
                    if array_len + batch_id * self.batch_size - self.outer_position > 0 {
                        // the cache batch has some records that we can select
                        let v = array_len + batch_id * self.batch_size - self.outer_position;
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
            return Ok(new_empty_array(&self.inner.get_data_type()));
        }

        let mut start_position = self.outer_position - row_count;

        let mut selected_arrays = Vec::new();
        for selector in self.selections.iter() {
            if !selector.skip {
                let batch_id = self.get_batch_id_from_position(start_position);
                let batch_start = batch_id * self.batch_size;

                let cached = self.cache.lock().unwrap().get(self.column_idx, batch_id);
                let cached = cached.expect("data must be already cached in the read_records call");

                let slice_start = start_position - batch_start;
                let sliced = cached.slice(slice_start, selector.row_count);
                selected_arrays.push(sliced);
            }
            start_position += selector.row_count;
        }

        self.selections.clear();

        match selected_arrays.len() {
            0 => Ok(new_empty_array(&self.inner.get_data_type())),
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
        self.inner.get_def_levels()
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        self.inner.get_rep_levels()
    }
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
        let cache = Arc::new(Mutex::new(RowGroupCache::new(3))); // Batch size 3
        let mut cached_reader = CachedArrayReader::new(Box::new(mock_reader), cache, 0);

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
        let cache = Arc::new(Mutex::new(RowGroupCache::new(5))); // Batch size 5
        let mut cached_reader = CachedArrayReader::new(Box::new(mock_reader), cache, 0);

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
        let cache = Arc::new(Mutex::new(RowGroupCache::new(3))); // Batch size 3
        let mut cached_reader = CachedArrayReader::new(Box::new(mock_reader), cache, 0);

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
        let cache = Arc::new(Mutex::new(RowGroupCache::new(5))); // Batch size 5
        let mut cached_reader = CachedArrayReader::new(Box::new(mock_reader), cache, 0);

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
        let cache = Arc::new(Mutex::new(RowGroupCache::new(5))); // Batch size 5

        // First reader - populate cache
        let mock_reader1 = MockArrayReader::new(vec![1, 2, 3, 4, 5]);
        let mut cached_reader1 = CachedArrayReader::new(Box::new(mock_reader1), cache.clone(), 0);

        cached_reader1.read_records(3).unwrap();
        let array1 = cached_reader1.consume_batch().unwrap();
        assert_eq!(array1.len(), 3);

        // Second reader with different column index should not interfere
        let mock_reader2 = MockArrayReader::new(vec![10, 20, 30, 40, 50]);
        let mut cached_reader2 = CachedArrayReader::new(Box::new(mock_reader2), cache.clone(), 1);

        cached_reader2.read_records(2).unwrap();
        let array2 = cached_reader2.consume_batch().unwrap();
        assert_eq!(array2.len(), 2);

        // Verify the second reader got its own data, not from cache
        let int32_array = array2.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int32_array.values(), &[10, 20]);
    }
}
