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

use super::InProgressArray;
use crate::concat::concat;
use crate::filter::FilterPredicate;
use arrow_array::{Array, ArrayRef};
use arrow_schema::ArrowError;

/// Generic implementation for [`InProgressArray`] that works with any type of
/// array.
///
/// Internally, this buffers arrays and then calls other kernels such as
/// [`concat`] to produce the final array.
///
/// [`concat`]: crate::concat::concat
#[derive(Debug)]
pub(crate) struct GenericInProgressArray {
    /// The current source
    source: Option<ArrayRef>,

    /// Is [`Self::source`] is referenced in [`Self::buffered_arrays`]
    source_data_referenced_in_buffers: bool,
    /// The buffered array slices
    buffered_arrays: Vec<ArrayRef>,

    /// The number of bytes the arrays in [`Self::buffered_arrays`] takes
    total_size_of_non_shared_buffers: usize,
}

impl GenericInProgressArray {
    /// Create a new `GenericInProgressArray`
    pub(crate) fn new() -> Self {
        Self {
            source: None,
            buffered_arrays: vec![],
            total_size_of_non_shared_buffers: 0,
            source_data_referenced_in_buffers: false,
        }
    }
}
impl InProgressArray for GenericInProgressArray {
    fn set_source(&mut self, source: Option<ArrayRef>) {
        if let Some(old_source) = self.source.take() {
            //  If the source is still referenced in buffered_arrays,
            // then count it now
            if self.source_data_referenced_in_buffers {
                self.total_size_of_non_shared_buffers += old_source.get_array_memory_size();
            }
        }
        self.source_data_referenced_in_buffers = false;
        self.source = source;
    }

    fn copy_rows(&mut self, offset: usize, len: usize) -> Result<(), ArrowError> {
        let source = self.source.as_ref().ok_or_else(|| {
            ArrowError::InvalidArgumentError(
                "Internal Error: GenericInProgressArray: source not set".to_string(),
            )
        })?;
        // No need to count the size of the array that was pushed to buffered_arrays
        // since the data is shared with the `source` memory that is already counted
        self.source_data_referenced_in_buffers = true;
        let array = source.slice(offset, len);
        self.buffered_arrays.push(array);
        Ok(())
    }

    fn copy_rows_by_filter_from(
        &mut self,
        source: ArrayRef,
        filter: &FilterPredicate,
    ) -> Result<(), ArrowError> {
        let array = filter.filter(source.as_ref())?;
        self.total_size_of_non_shared_buffers += array.get_array_memory_size();
        self.buffered_arrays.push(array);
        Ok(())
    }

    fn finish(&mut self) -> Result<ArrayRef, ArrowError> {
        // Concatenate all buffered arrays into a single array, which uses 2x
        // peak memory
        let array = concat(
            &self
                .buffered_arrays
                .iter()
                .map(|array| array.as_ref())
                .collect::<Vec<_>>(),
        )?;
        self.buffered_arrays.clear();
        self.total_size_of_non_shared_buffers = 0;
        self.source_data_referenced_in_buffers = false;
        Ok(array)
    }

    fn size(&self) -> usize {
        self.total_size_of_non_shared_buffers
            + self.buffered_arrays.capacity() * size_of::<ArrayRef>()
            + self
                .source
                .as_ref()
                .map_or(0, |a| a.get_array_memory_size())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int32Array;
    use std::sync::Arc;

    fn arr(range: std::ops::Range<i32>) -> ArrayRef {
        Arc::new(Int32Array::from_iter_values(range))
    }

    #[test]
    fn test_size_empty() {
        let in_progress = GenericInProgressArray::new();
        assert_eq!(in_progress.size(), 0);
    }

    #[test]
    fn test_source_is_counted_in_memory_and_released_when_not_used() {
        let mut in_progress = GenericInProgressArray::new();

        // Starting with no used memory
        assert_eq!(in_progress.size(), 0);
        {
            let source1 = arr(0..100);
            in_progress.set_source(Some(Arc::clone(&source1)));

            // We are holding on source so this is the size
            assert_eq!(in_progress.size(), source1.get_array_memory_size());
        }

        {
            // Replacing an unused source drops the old size and counts only the new one
            let source2 = arr(0..40);
            in_progress.set_source(Some(Arc::clone(&source2)));
            assert_eq!(in_progress.size(), source2.get_array_memory_size());
        }

        // Drop the used source
        in_progress.set_source(None);

        // The source is no longer being held, so it should reduce the size
        assert_eq!(in_progress.size(), 0);
    }

    #[test]
    fn test_double_copy_on_same_source_should_not_double_count() {
        let mut in_progress = GenericInProgressArray::new();

        // Starting with no used memory
        assert_eq!(in_progress.size(), 0);

        let source = arr(0..100);
        in_progress.set_source(Some(Arc::clone(&source)));

        // We are holding on source so this is the size
        let size_before_copy = in_progress.size();
        assert_eq!(size_before_copy, source.get_array_memory_size());

        for _ in 0..2 {
            // Only copy a subset
            in_progress.copy_rows(0, 98).unwrap();

            // The size should now account for the buffered but not the actual array data since we are still holding on it
            assert!(
                in_progress.size() > size_before_copy,
                "size after copy {} should be greater than before copy {size_before_copy}",
                in_progress.size()
            );
            {
                let in_progress_size = in_progress.size() as f64;
                let source_size = source.get_array_memory_size();
                let size_if_source_and_sliced_would_be_counted = (source_size as f64) * 1.8;
                assert!(
                    in_progress_size < size_if_source_and_sliced_would_be_counted,
                    "size after copy {in_progress_size} should not include the source and sliced array (should be greater than {size_if_source_and_sliced_would_be_counted}), source size is {source_size}"
                );
            }
        }

        let size_before_clear_source = in_progress.size();

        // Drop the used source
        in_progress.set_source(None);

        // The source is still being held in the buffered
        assert_eq!(in_progress.size(), size_before_clear_source);

        {
            let source2 = arr(0..40);
            in_progress.set_source(Some(Arc::clone(&source2)));
            assert_eq!(
                in_progress.size(),
                size_before_clear_source + source2.get_array_memory_size()
            );
            in_progress.set_source(None);
        }

        in_progress.finish().unwrap();

        // There is still some memory being held by some leftover capacity but not arrays
        assert!(in_progress.size() < source.get_array_memory_size());
    }
}
