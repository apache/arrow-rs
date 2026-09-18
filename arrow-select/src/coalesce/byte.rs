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

use crate::coalesce::InProgressArray;
use crate::concat::concat as concat_arrays;
use crate::filter::{FilterIndices, FilterPredicate, FilterSelection, FilterSlices};
use arrow_array::cast::AsArray;
use arrow_array::types::ByteArrayType;
use arrow_array::{Array, ArrayRef, GenericByteArray, OffsetSizeTrait};
use arrow_buffer::{
    ArrowNativeType, Buffer, NullBuffer, NullBufferBuilder, OffsetBuffer, ScalarBuffer,
};
use arrow_schema::ArrowError;
use std::sync::Arc;

/// InProgressArray for [`StringArray`], [`BinaryArray`], [`LargeStringArray`],
/// and [`LargeBinaryArray`].
///
/// Uses two strategies depending on the path, always maintaining chronological
/// insertion order:
///
/// - **Sparse filter path** (`copy_rows_by_filter` when no buffered data yet):
///   streams bytes directly into growing `offsets`/`values` buffers. This
///   avoids calling the filter kernel and the 2× peak-memory cost of later
///   concatenation. The key win is enabling the per-column sparse filter path
///   for schemas that include Utf8/Binary columns.
///
/// - **Materialized path** (`copy_rows`, or `copy_rows_by_filter` when buffered
///   data already exists): buffers `ArrayRef` slices and concatenates in
///   `finish()`. This matches [`GenericInProgressArray`] and avoids per-call
///   overhead for large contiguous chunks (e.g., take workloads with many
///   medium-sized batches).
///
/// When switching from streaming → materialized (e.g., after a batch split
/// triggers `copy_rows`), the streaming data is flushed to `buffered_arrays`
/// first, preserving chronological order.
///
/// [`StringArray`]: arrow_array::StringArray
/// [`BinaryArray`]: arrow_array::BinaryArray
/// [`LargeStringArray`]: arrow_array::LargeStringArray
/// [`LargeBinaryArray`]: arrow_array::LargeBinaryArray
/// [`GenericInProgressArray`]: super::generic::GenericInProgressArray
pub(crate) struct InProgressByteArray<T: ByteArrayType> {
    /// The current source array, if any
    source: Option<ArrayRef>,
    /// Target batch size — used for pre-allocation hints
    batch_size: usize,
    /// Null tracking for the streaming path only (copy_rows_by_filter).
    nulls: NullBufferBuilder,
    /// Streaming path: accumulated row-end offsets.  Starts with a single `0`
    /// once capacity is first allocated; length == rows_written + 1.
    offsets: Vec<T::Offset>,
    /// Streaming path: accumulated byte values for all written rows
    values: Vec<u8>,
    /// All buffered arrays, in insertion order.  Populated by:
    ///   - copy_rows (slice of source),
    ///   - flush_streaming (converts streaming data to an array),
    ///   - copy_rows_by_filter when buffered data already exists (filter result).
    buffered_arrays: Vec<ArrayRef>,
}

// ByteArrayType doesn't implement Debug, so implement manually.
impl<T: ByteArrayType> std::fmt::Debug for InProgressByteArray<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InProgressByteArray")
            .field("batch_size", &self.batch_size)
            .field("streamed_rows", &self.offsets.len().saturating_sub(1))
            .field("streamed_bytes", &self.values.len())
            .field("buffered_arrays", &self.buffered_arrays.len())
            .finish()
    }
}

impl<T: ByteArrayType> InProgressByteArray<T> {
    pub(crate) fn new(batch_size: usize) -> Self {
        Self {
            source: None,
            batch_size,
            nulls: NullBufferBuilder::new(batch_size),
            offsets: Vec::new(),
            values: Vec::new(),
            buffered_arrays: Vec::new(),
        }
    }

    fn ensure_capacity_bytes(&mut self, avg_bytes: usize) {
        if self.offsets.is_empty() {
            self.offsets.reserve(self.batch_size + 1);
            self.values.reserve(self.batch_size * avg_bytes);
            self.offsets.push(T::Offset::usize_as(0));
        }
    }

    fn append_filtered_nulls(
        nulls: &mut NullBufferBuilder,
        source_nulls: Option<&NullBuffer>,
        filter: &FilterPredicate,
    ) {
        if let Some(filtered_nulls) = filter.filter_nulls(source_nulls) {
            nulls.append_buffer(&filtered_nulls);
        } else {
            nulls.append_n_non_nulls(filter.count());
        }
    }

    fn append_rows(
        offsets: &mut Vec<T::Offset>,
        values: &mut Vec<u8>,
        source: &GenericByteArray<T>,
        row_offset: usize,
        len: usize,
    ) {
        let src_offsets = source.value_offsets();
        let byte_start = src_offsets[row_offset].as_usize();
        let byte_end = src_offsets[row_offset + len].as_usize();

        values.extend_from_slice(&source.value_data()[byte_start..byte_end]);

        // Uniform shift on the source offset slice — LLVM can auto-vectorize.
        let src_start = src_offsets[row_offset];
        let base = *offsets.last().unwrap();
        offsets.extend(
            src_offsets[row_offset + 1..=row_offset + len]
                .iter()
                .map(|&o| o - src_start + base),
        );
    }

    fn append_rows_by_indices(
        offsets: &mut Vec<T::Offset>,
        values: &mut Vec<u8>,
        source: &GenericByteArray<T>,
        indices: FilterIndices<'_>,
    ) {
        let src_offsets = source.value_offsets();
        let src_bytes = source.value_data();
        let mut current = *offsets.last().unwrap();
        indices.for_each(|idx| {
            let start = src_offsets[idx].as_usize();
            let end = src_offsets[idx + 1].as_usize();
            values.extend_from_slice(&src_bytes[start..end]);
            current = current + T::Offset::usize_as(end - start);
            offsets.push(current);
        });
    }

    fn append_rows_by_slices(
        offsets: &mut Vec<T::Offset>,
        values: &mut Vec<u8>,
        source: &GenericByteArray<T>,
        slices: FilterSlices<'_>,
    ) {
        slices.for_each(|(start, end)| {
            Self::append_rows(offsets, values, source, start, end - start);
        });
    }

    /// Materialise any accumulated streaming data into an `ArrayRef` and
    /// append it to `buffered_arrays`, then reset the streaming state.
    ///
    /// Called by `copy_rows` to flush streaming data before adding a buffered
    /// slice, ensuring chronological order is preserved.
    fn flush_streaming_to_buffered(&mut self) {
        if self.offsets.is_empty() {
            return;
        }
        let offsets = std::mem::take(&mut self.offsets);
        let values = std::mem::take(&mut self.values);
        let nulls = self.nulls.finish();
        self.nulls = NullBufferBuilder::new(self.batch_size);
        // offsets always starts with [0] after ensure_capacity_bytes, so it's
        // non-empty here — no need to seed.
        let array = GenericByteArray::<T>::new(
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            Buffer::from_vec(values),
            nulls,
        );
        self.buffered_arrays.push(Arc::new(array));
    }
}

/// Extract source as `&GenericByteArray<T>`, taking `Option<&ArrayRef>` so
/// the caller can borrow only `self.source` and keep `self.offsets`/
/// `self.values` available for mutable borrows simultaneously.
fn byte_source<T: ByteArrayType>(
    source: Option<&ArrayRef>,
) -> Result<&GenericByteArray<T>, ArrowError> {
    Ok(source
        .ok_or_else(|| {
            ArrowError::InvalidArgumentError(
                "Internal Error: InProgressByteArray: source not set".to_string(),
            )
        })?
        .as_bytes::<T>())
}

fn avg_bytes_per_row<T: ByteArrayType>(source: &GenericByteArray<T>) -> usize {
    let n = source.len();
    if n == 0 {
        return 32;
    }
    let offsets = source.value_offsets();
    let total = offsets[n].as_usize().saturating_sub(offsets[0].as_usize());
    (total / n).clamp(1, 256)
}

impl<T: ByteArrayType + 'static> InProgressArray for InProgressByteArray<T>
where
    T::Offset: OffsetSizeTrait,
{
    fn set_source(&mut self, source: Option<ArrayRef>) {
        self.source = source;
    }

    /// Buffer a slice reference for later concatenation in `finish()`.
    ///
    /// If streaming data exists (from a prior `copy_rows_by_filter` call), it
    /// is first flushed to `buffered_arrays` to preserve insertion order.
    fn copy_rows(&mut self, offset: usize, len: usize) -> Result<(), ArrowError> {
        self.flush_streaming_to_buffered();
        let source = self.source.as_ref().ok_or_else(|| {
            ArrowError::InvalidArgumentError(
                "Internal Error: InProgressByteArray: source not set".to_string(),
            )
        })?;
        self.buffered_arrays.push(source.slice(offset, len));
        Ok(())
    }

    fn copy_rows_by_filter(&mut self, filter: &FilterPredicate) -> Result<(), ArrowError> {
        // If buffered data already exists (e.g., a residual slice from a prior
        // push_batch split), we must not stream into offsets/values because
        // the streaming data would represent rows that are chronologically
        // newer than the buffered data. Instead fall back to applying the
        // filter kernel and buffering the result.
        if !self.buffered_arrays.is_empty() {
            let source = self.source.as_ref().ok_or_else(|| {
                ArrowError::InvalidArgumentError(
                    "Internal Error: InProgressByteArray: source not set".to_string(),
                )
            })?;
            let filtered = filter.filter(source.as_ref())?;
            self.buffered_arrays.push(filtered);
            return Ok(());
        }

        // Fast streaming path: no buffered data exists, so stream directly.
        match filter.selection() {
            FilterSelection::Indices(indices) => {
                if self.offsets.is_empty() {
                    let avg = byte_source::<T>(self.source.as_ref())
                        .map(avg_bytes_per_row)
                        .unwrap_or(32);
                    self.ensure_capacity_bytes(avg);
                }
                let source = byte_source::<T>(self.source.as_ref())?;
                let avg = avg_bytes_per_row(source);
                self.offsets.reserve(filter.count());
                self.values.reserve(filter.count() * avg);
                Self::append_filtered_nulls(&mut self.nulls, source.nulls(), filter);
                Self::append_rows_by_indices(&mut self.offsets, &mut self.values, source, indices);
                Ok(())
            }
            FilterSelection::Slices(slices) => {
                if self.offsets.is_empty() {
                    let avg = byte_source::<T>(self.source.as_ref())
                        .map(avg_bytes_per_row)
                        .unwrap_or(32);
                    self.ensure_capacity_bytes(avg);
                }
                let source = byte_source::<T>(self.source.as_ref())?;
                let avg = avg_bytes_per_row(source);
                self.offsets.reserve(filter.count());
                self.values.reserve(filter.count() * avg);
                Self::append_filtered_nulls(&mut self.nulls, source.nulls(), filter);
                Self::append_rows_by_slices(&mut self.offsets, &mut self.values, source, slices);
                Ok(())
            }
            selection => self.copy_rows_by_selection(selection),
        }
    }

    fn finish(&mut self) -> Result<ArrayRef, ArrowError> {
        let mut buffered = std::mem::take(&mut self.buffered_arrays);
        let nulls = self.nulls.finish();
        self.nulls = NullBufferBuilder::new(self.batch_size);

        // Flush any remaining streaming data (pure streaming case: buffered is
        // empty and all data is in offsets/values).
        if !self.offsets.is_empty() {
            let mut offsets = std::mem::take(&mut self.offsets);
            let values = std::mem::take(&mut self.values);
            if offsets.is_empty() {
                offsets.push(T::Offset::usize_as(0));
            }
            let array = GenericByteArray::<T>::new(
                OffsetBuffer::new(ScalarBuffer::from(offsets)),
                Buffer::from_vec(values),
                nulls,
            );
            // In pure streaming mode, buffered is empty, so streaming data
            // goes first (and last). In mixed mode this branch shouldn't be
            // reached (streaming was flushed to buffered_arrays in copy_rows).
            buffered.push(Arc::new(array) as ArrayRef);
        }

        match buffered.len() {
            0 => {
                // Nothing was written — return an empty array.
                let array = GenericByteArray::<T>::new(
                    OffsetBuffer::new(ScalarBuffer::from(vec![T::Offset::usize_as(0)])),
                    Buffer::from_vec(vec![0u8; 0]),
                    None,
                );
                Ok(Arc::new(array))
            }
            1 => Ok(Arc::clone(&buffered[0])),
            _ => {
                let refs: Vec<&dyn Array> =
                    buffered.iter().map(|a| a.as_ref() as &dyn Array).collect();
                concat_arrays(&refs)
            }
        }
    }

    fn size(&self) -> usize {
        self.source
            .as_ref()
            .map_or(0, |s| s.get_array_memory_size())
            + self.offsets.capacity() * std::mem::size_of::<T::Offset>()
            + self.values.capacity()
            + self.nulls.allocated_size()
            // Count Vec overhead but not element sizes (slices share buffers
            // with self.source, so we don't double-count).
            + self.buffered_arrays.capacity() * std::mem::size_of::<ArrayRef>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::FilterBuilder;
    use arrow_array::types::{GenericBinaryType, GenericStringType};
    use arrow_array::{BinaryArray, BooleanArray, LargeBinaryArray, LargeStringArray, StringArray};

    fn str_arr(values: &[Option<&str>]) -> StringArray {
        StringArray::from(values.to_vec())
    }

    fn large_str_arr(values: &[Option<&str>]) -> LargeStringArray {
        LargeStringArray::from(values.to_vec())
    }

    fn bin_arr(values: &[Option<&[u8]>]) -> BinaryArray {
        BinaryArray::from(values.to_vec())
    }

    fn large_bin_arr(values: &[Option<&[u8]>]) -> LargeBinaryArray {
        LargeBinaryArray::from(values.to_vec())
    }

    #[test]
    fn test_copy_rows_string() {
        let source = str_arr(&[Some("hello"), Some("world"), None, Some("foo")]);
        let mut ip = InProgressByteArray::<GenericStringType<i32>>::new(4);
        ip.set_source(Some(Arc::new(source.clone())));
        ip.copy_rows(0, 4).unwrap();
        let result = ip.finish().unwrap();
        assert_eq!(result.as_bytes::<GenericStringType<i32>>(), &source);
    }

    #[test]
    fn test_copy_rows_large_string() {
        let source = large_str_arr(&[Some("alpha"), None, Some("beta")]);
        let mut ip = InProgressByteArray::<GenericStringType<i64>>::new(4);
        ip.set_source(Some(Arc::new(source.clone())));
        ip.copy_rows(1, 2).unwrap();
        let result = ip.finish().unwrap();
        assert_eq!(
            result.as_bytes::<GenericStringType<i64>>(),
            &source.slice(1, 2)
        );
    }

    #[test]
    fn test_copy_rows_binary() {
        let source = bin_arr(&[Some(b"ab"), Some(b"cde"), None]);
        let mut ip = InProgressByteArray::<GenericBinaryType<i32>>::new(4);
        ip.set_source(Some(Arc::new(source.clone())));
        ip.copy_rows(0, 3).unwrap();
        let result = ip.finish().unwrap();
        assert_eq!(result.as_bytes::<GenericBinaryType<i32>>(), &source);
    }

    #[test]
    fn test_copy_rows_large_binary() {
        let source = large_bin_arr(&[None, Some(b"x"), Some(b"yy")]);
        let mut ip = InProgressByteArray::<GenericBinaryType<i64>>::new(4);
        ip.set_source(Some(Arc::new(source.clone())));
        ip.copy_rows(0, 3).unwrap();
        let result = ip.finish().unwrap();
        assert_eq!(result.as_bytes::<GenericBinaryType<i64>>(), &source);
    }

    #[test]
    fn test_multiple_copy_rows_accumulate() {
        let source = str_arr(&[Some("a"), Some("bb"), Some("ccc"), Some("dddd")]);
        let mut ip = InProgressByteArray::<GenericStringType<i32>>::new(4);
        ip.set_source(Some(Arc::new(source.clone())));
        ip.copy_rows(0, 2).unwrap();
        ip.copy_rows(2, 2).unwrap();
        let result = ip.finish().unwrap();
        assert_eq!(result.as_bytes::<GenericStringType<i32>>(), &source);
    }

    #[test]
    fn test_finish_resets_state() {
        let source = str_arr(&[Some("x")]);
        let mut ip = InProgressByteArray::<GenericStringType<i32>>::new(4);
        ip.set_source(Some(Arc::new(source.clone())));
        ip.copy_rows(0, 1).unwrap();
        let first = ip.finish().unwrap();
        let second = ip.finish().unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(second.len(), 0);
    }

    #[test]
    fn test_copy_rows_by_filter_indices() {
        // indices path: sparse filter (keep 2 of 6)
        let source = str_arr(&[Some("a"), None, Some("c"), Some("d"), None, Some("f")]);
        let filter = BooleanArray::from(vec![false, true, false, false, true, false]);
        let predicate = FilterBuilder::new(&filter).build();

        let mut ip = InProgressByteArray::<GenericStringType<i32>>::new(6);
        ip.set_source(Some(Arc::new(source)));
        ip.copy_rows_by_filter(&predicate).unwrap();
        let result = ip.finish().unwrap();
        let result = result.as_bytes::<GenericStringType<i32>>();
        assert_eq!(result.len(), 2);
        assert!(result.is_null(0));
        assert!(result.is_null(1));
    }

    #[test]
    fn test_copy_rows_by_filter_slices() {
        // slices path: mostly dense filter
        let source = str_arr(&[Some("a"), Some("b"), Some("c"), Some("d"), Some("e")]);
        let filter = BooleanArray::from(vec![true, true, false, true, true]);
        let predicate = FilterBuilder::new(&filter).build();

        let mut ip = InProgressByteArray::<GenericStringType<i32>>::new(5);
        ip.set_source(Some(Arc::new(source)));
        ip.copy_rows_by_filter(&predicate).unwrap();
        let result = ip.finish().unwrap();
        let result = result.as_bytes::<GenericStringType<i32>>();
        let expected = str_arr(&[Some("a"), Some("b"), Some("d"), Some("e")]);
        assert_eq!(result, &expected);
    }

    #[test]
    fn test_size_empty() {
        let ip = InProgressByteArray::<GenericStringType<i32>>::new(64);
        assert_eq!(ip.size(), 0);
    }

    #[test]
    fn test_size_counts_source() {
        let mut ip = InProgressByteArray::<GenericStringType<i32>>::new(64);
        let source: ArrayRef = Arc::new(str_arr(&[Some("hello"), Some("world")]));
        let source_size = source.get_array_memory_size();
        ip.set_source(Some(Arc::clone(&source)));
        assert_eq!(ip.size(), source_size);
    }

    #[test]
    fn test_size_after_copy() {
        let mut ip = InProgressByteArray::<GenericStringType<i32>>::new(64);
        let source: ArrayRef = Arc::new(str_arr(&[Some("hello"), Some("world")]));
        let source_size = source.get_array_memory_size();
        ip.set_source(Some(Arc::clone(&source)));
        ip.copy_rows(0, 2).unwrap();
        // Source still held; slices share its buffer — no double-counting.
        assert!(ip.size() >= source_size);
    }
}
