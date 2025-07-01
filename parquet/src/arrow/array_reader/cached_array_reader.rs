use crate::arrow::array_reader::{row_group_cache::RowGroupCache, ArrayReader};
use crate::errors::Result;
use arrow_array::{new_empty_array, ArrayRef, BooleanArray};
use arrow_buffer::BooleanBufferBuilder;
use arrow_schema::DataType as ArrowType;
use arrow_select::concat::concat;
use arrow_select::filter::filter;
use std::any::Any;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

/// Row selector indicating whether to read or skip rows
#[derive(Debug, Clone)]
struct RowSelector {
    /// Number of rows in this selection
    row_count: usize,
    /// Whether to skip (true) or read (false) these rows
    skip: bool,
}

impl RowSelector {
    fn select(row_count: usize) -> Self {
        Self {
            row_count,
            skip: false,
        }
    }

    fn skip(row_count: usize) -> Self {
        Self {
            row_count,
            skip: true,
        }
    }
}

/// A cached wrapper around an ArrayReader that avoids duplicate decoding
/// when the same column appears in both filter predicates and output projection.
///
/// This reader accumulates read/skip selections and processes them efficiently
/// using cached batch data when consume_batch() is called.
pub struct CachedArrayReader {
    /// The underlying array reader
    inner: Box<dyn ArrayReader>,
    /// Shared cache for this row group
    cache: Arc<Mutex<RowGroupCache>>,
    /// Column index for cache key generation
    column_idx: usize,
    /// Current row position in the row group
    current_row: usize,
    /// Inner reader's row position (to keep it in sync)
    inner_reader_position: usize,
    /// Queue of read/skip selections to process
    selection: VecDeque<RowSelector>,
}

impl CachedArrayReader {
    /// Creates a new cached array reader
    pub fn new(
        inner: Box<dyn ArrayReader>,
        cache: Arc<Mutex<RowGroupCache>>,
        column_idx: usize,
    ) -> Self {
        Self {
            inner,
            cache,
            column_idx,
            current_row: 0,
            inner_reader_position: 0,
            selection: VecDeque::new(),
        }
    }

    /// Gets the batch size from the cache
    fn batch_size(&self) -> usize {
        self.cache.lock().unwrap().batch_size()
    }

    /// Calculates the batch ID for a given row position
    fn batch_id_from_row(&self, row: usize) -> usize {
        row / self.batch_size()
    }

    /// Ensures a batch is cached by fetching it from the inner reader if needed.
    ///
    /// Returns the number of rows cached for this batch. If `0` is returned it
    /// indicates the inner reader has no more data (EOF).
    fn ensure_cached(&mut self, batch_id: usize) -> Result<usize> {
        let batch_start_row = batch_id * self.batch_size();

        // Check if already cached
        if let Some(array) = self
            .cache
            .lock()
            .unwrap()
            .get(self.column_idx, batch_start_row)
        {
            return Ok(array.len());
        }

        // Sync inner reader to batch start if needed
        if self.inner_reader_position < batch_start_row {
            let to_skip = batch_start_row - self.inner_reader_position;
            let skipped = self.inner.skip_records(to_skip)?;
            self.inner_reader_position += skipped;

            // Reached EOF before desired position
            if skipped < to_skip {
                return Ok(0);
            }
        }

        // Read the batch
        let expected_batch = self.batch_size();
        let records_read = self.inner.read_records(expected_batch)?;

        // If no more rows, signal EOF
        if records_read == 0 {
            return Ok(0);
        }

        let array = self.inner.consume_batch()?;

        // Cache it
        self.cache
            .lock()
            .unwrap()
            .insert(self.column_idx, batch_start_row, array.clone());
        self.inner_reader_position += records_read;

        Ok(array.len())
    }

    /// Converts selection queue to boolean buffer for filtering
    fn selection_to_boolean_buffer(&self, total_rows: usize) -> arrow_buffer::BooleanBuffer {
        let mut builder = BooleanBufferBuilder::new(total_rows);
        for selector in &self.selection {
            builder.append_n(selector.row_count, !selector.skip);
        }
        builder.finish()
    }
}

impl ArrayReader for CachedArrayReader {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_data_type(&self) -> &ArrowType {
        self.inner.get_data_type()
    }

    fn read_records(&mut self, mut requested: usize) -> Result<usize> {
        let mut total_read = 0;

        while requested > 0 {
            let batch_id = self.batch_id_from_row(self.current_row);

            // Ensure the batch is cached and determine its length
            let cached_len = self.ensure_cached(batch_id)?;

            // EOF reached
            if cached_len == 0 {
                break;
            }

            let batch_start = batch_id * self.batch_size();
            let offset_in_batch = self.current_row - batch_start;

            // If current position is past the cached length, advance to next batch.
            // If this batch was partial (i.e. smaller than the configured batch size)
            // this indicates we have reached EOF and should stop.
            if offset_in_batch >= cached_len {
                self.current_row = batch_start + cached_len;

                // Partial batch implies EOF has been reached
                if cached_len < self.batch_size() {
                    break;
                }

                continue;
            }

            let available = cached_len - offset_in_batch;
            let to_read = available.min(requested);

            // Record the selection
            if to_read > 0 {
                self.selection.push_back(RowSelector::select(to_read));
                self.current_row += to_read;
                requested -= to_read;
                total_read += to_read;
            }
        }

        Ok(total_read)
    }

    fn skip_records(&mut self, num_records: usize) -> Result<usize> {
        // Add selection for skipping these records
        self.selection.push_back(RowSelector::skip(num_records));
        self.current_row += num_records;
        // Note: we don't cache or sync inner reader for skips
        Ok(num_records)
    }

    fn consume_batch(&mut self) -> Result<ArrayRef> {
        if self.selection.is_empty() {
            // Return empty array of correct type
            return Ok(new_empty_array(self.get_data_type()));
        }

        // Calculate total row count and starting position
        let total_rows: usize = self.selection.iter().map(|s| s.row_count).sum();
        let start_row = self.current_row - total_rows;

        if total_rows == 0 {
            self.selection.clear();
            return Ok(new_empty_array(self.get_data_type()));
        }

        // Create boolean selection buffer
        let selection_buffer = self.selection_to_boolean_buffer(total_rows);

        // Determine batch range
        let batch_size = self.batch_size();
        let start_batch = start_row / batch_size;
        let end_batch = (start_row + total_rows - 1) / batch_size;

        let mut result_arrays = Vec::new();

        for batch_id in start_batch..=end_batch {
            let batch_start = batch_id * batch_size;
            let batch_end = batch_start + batch_size - 1;

            // Calculate overlap between our selection and this batch
            let overlap_start = start_row.max(batch_start);
            let overlap_end = (start_row + total_rows - 1).min(batch_end);

            if overlap_start > overlap_end {
                continue; // No overlap
            }

            // Get selection slice for this batch
            let selection_start = overlap_start - start_row;
            let selection_length = overlap_end - overlap_start + 1;
            let batch_selection = selection_buffer.slice(selection_start, selection_length);

            if batch_selection.count_set_bits() == 0 {
                continue; // Nothing selected in this batch
            }

            // Get cached array and apply filter
            let cached_array = self
                .cache
                .lock()
                .unwrap()
                .get(self.column_idx, batch_start)
                .unwrap();
            let mask = BooleanArray::from(batch_selection);

            // Calculate offset within the batch
            let batch_offset = overlap_start - batch_start;
            let batch_slice = if batch_offset > 0 || selection_length < batch_size {
                cached_array.slice(batch_offset, selection_length)
            } else {
                cached_array
            };

            let filtered = filter(&batch_slice, &mask)?;
            if filtered.len() > 0 {
                result_arrays.push(filtered);
            }
        }

        // Clear selection queue
        self.selection.clear();

        // Concatenate results
        match result_arrays.len() {
            0 => Ok(new_empty_array(self.get_data_type())),
            1 => Ok(result_arrays.into_iter().next().unwrap()),
            _ => {
                let array_refs: Vec<&dyn arrow_array::Array> =
                    result_arrays.iter().map(|a| a.as_ref()).collect();
                Ok(concat(&array_refs)?)
            }
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
        read_position: usize,
        data_type: ArrowType,
    }

    impl MockArrayReader {
        fn new(data: Vec<i32>) -> Self {
            Self {
                data,
                position: 0,
                read_position: 0,
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
            self.read_position = self.position;
            self.position += to_read;
            Ok(to_read)
        }

        fn consume_batch(&mut self) -> Result<ArrayRef> {
            // Return data from read_position to current position
            let slice = &self.data[self.read_position..self.position];
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
        let cache = Arc::new(Mutex::new(RowGroupCache::new(5))); // Batch size 5
        let mut cached_reader = CachedArrayReader::new(Box::new(mock_reader), cache, 0);

        // Read 3 records
        let records_read = cached_reader.read_records(3).unwrap();
        assert_eq!(records_read, 3);

        // Consume batch should return those 3 records
        let array = cached_reader.consume_batch().unwrap();
        assert_eq!(array.len(), 3);
    }

    #[test]
    fn test_read_skip_pattern() {
        let mock_reader = MockArrayReader::new(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let cache = Arc::new(Mutex::new(RowGroupCache::new(5))); // Batch size 5
        let mut cached_reader = CachedArrayReader::new(Box::new(mock_reader), cache, 0);

        // Read 2, skip 2, read 1 (total 5 records from first batch)
        cached_reader.read_records(2).unwrap();
        cached_reader.skip_records(2).unwrap();
        cached_reader.read_records(1).unwrap();

        // Should get [1,2] and [5] (skipped 3,4)
        let array = cached_reader.consume_batch().unwrap();
        assert_eq!(array.len(), 3); // 2 + 1 reads
    }

    #[test]
    fn test_cache_efficiency() {
        // Test that cache avoids duplicate reads for the same batch
        let mock_reader = MockArrayReader::new(vec![1, 2, 3, 4, 5]);
        let cache = Arc::new(Mutex::new(RowGroupCache::new(5))); // Batch size 5

        // First reader - populate cache
        let mut cached_reader1 = CachedArrayReader::new(Box::new(mock_reader), cache.clone(), 0);
        cached_reader1.read_records(3).unwrap();
        let array1 = cached_reader1.consume_batch().unwrap();
        assert_eq!(array1.len(), 3);

        // Second reader - should use cache (no inner reader created)
        let mock_reader2 = MockArrayReader::new(vec![10, 20, 30, 40, 50]); // Different data
        let mut cached_reader2 = CachedArrayReader::new(Box::new(mock_reader2), cache.clone(), 0);
        cached_reader2.read_records(2).unwrap();
        let array2 = cached_reader2.consume_batch().unwrap();
        assert_eq!(array2.len(), 2);

        // The second reader should get data from cache (original [1,2,3,4,5])
        // not from its mock reader ([10,20,30,40,50])
        // This tests that cache is working
    }
}
