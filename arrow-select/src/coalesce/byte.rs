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

/// Heap-allocated streaming state, present only when the sparse-filter path is active.
///
/// By boxing this, `InProgressByteArray` stays the same size as `GenericInProgressArray`
/// in the common (take / high-selectivity) path, avoiding the cache-pressure regression
/// that a larger flat struct would cause.
struct ByteStreamingState<O> {
    nulls: NullBufferBuilder,
    /// Row-end offsets starting with an initial `0`; length == rows_written + 1.
    offsets: Vec<O>,
    values: Vec<u8>,
}

/// InProgressArray for [`StringArray`], [`BinaryArray`], [`LargeStringArray`],
/// and [`LargeBinaryArray`].
///
/// Uses two strategies depending on the path, always maintaining chronological
/// insertion order:
///
/// - **Sparse filter path** (`copy_rows_by_filter` when no buffered data yet):
///   streams bytes directly into growing `offsets`/`values` buffers inside a
///   lazily-allocated [`ByteStreamingState`]. This avoids calling the filter
///   kernel and the 2× peak-memory cost of later concatenation.
///
/// - **Materialized path** (`copy_rows`, or `copy_rows_by_filter` when buffered
///   data already exists): buffers `ArrayRef` slices and concatenates in
///   `finish()`. This matches [`GenericInProgressArray`] and avoids per-call
///   overhead for large contiguous chunks.
///
/// The streaming state is only heap-allocated when the streaming path is first
/// used, so in the materialized-only path this struct has the same size as
/// `GenericInProgressArray` (~56 bytes).
///
/// [`StringArray`]: arrow_array::StringArray
/// [`BinaryArray`]: arrow_array::BinaryArray
/// [`LargeStringArray`]: arrow_array::LargeStringArray
/// [`LargeBinaryArray`]: arrow_array::LargeBinaryArray
/// [`GenericInProgressArray`]: super::generic::GenericInProgressArray
pub(crate) struct InProgressByteArray<T: ByteArrayType> {
    /// The current source array, if any.
    source: Option<ArrayRef>,
    /// Target batch size — used for pre-allocation hints.
    batch_size: usize,
    /// Streaming state; `None` until the sparse-filter path is first used.
    /// `Option<Box<_>>` is pointer-sized (8 bytes) due to niche optimisation.
    streaming: Option<Box<ByteStreamingState<T::Offset>>>,
    /// All buffered arrays, in insertion order. Populated by:
    ///   - `copy_rows` (slice of source),
    ///   - `flush_streaming` (converts streaming data to an array),
    ///   - `copy_rows_by_filter` when buffered data already exists (filter result).
    buffered_arrays: Vec<ArrayRef>,
}

// ByteArrayType doesn't implement Debug, so implement manually.
impl<T: ByteArrayType> std::fmt::Debug for InProgressByteArray<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (streamed_rows, streamed_bytes) = self.streaming.as_ref().map_or((0, 0), |s| {
            (s.offsets.len().saturating_sub(1), s.values.len())
        });
        f.debug_struct("InProgressByteArray")
            .field("batch_size", &self.batch_size)
            .field("streamed_rows", &streamed_rows)
            .field("streamed_bytes", &streamed_bytes)
            .field("buffered_arrays", &self.buffered_arrays.len())
            .finish()
    }
}

impl<T: ByteArrayType> InProgressByteArray<T>
where
    T::Offset: OffsetSizeTrait,
{
    pub(crate) fn new(batch_size: usize) -> Self {
        Self {
            source: None,
            batch_size,
            streaming: None,
            buffered_arrays: Vec::new(),
        }
    }

    /// Allocate the streaming state with capacity hints derived from `avg_bytes`.
    fn start_streaming(&mut self, avg_bytes: usize) {
        debug_assert!(self.streaming.is_none());
        let mut offsets = Vec::with_capacity(self.batch_size + 1);
        offsets.push(T::Offset::usize_as(0));
        self.streaming = Some(Box::new(ByteStreamingState {
            nulls: NullBufferBuilder::new(self.batch_size),
            offsets,
            values: Vec::with_capacity(self.batch_size * avg_bytes),
        }));
    }

    /// Materialise any accumulated streaming data into an `ArrayRef` and append
    /// it to `buffered_arrays`, then reset the streaming state.
    fn flush_streaming_to_buffered(&mut self) {
        if let Some(state) = self.streaming.take() {
            let ByteStreamingState {
                nulls: mut nb,
                offsets,
                values,
            } = *state;
            // offsets always has at least the initial [0]; if that's all there
            // is then nothing was written and we can skip the push.
            if offsets.len() <= 1 {
                return;
            }
            let nulls = nb.finish();
            let array = GenericByteArray::<T>::new(
                OffsetBuffer::new(ScalarBuffer::from(offsets)),
                Buffer::from_vec(values),
                nulls,
            );
            self.buffered_arrays.push(Arc::new(array));
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
}

/// Extract source as `&GenericByteArray<T>`, taking `Option<&ArrayRef>` so
/// the caller can borrow only `self.source` and keep `self.streaming`/
/// `self.buffered_arrays` available for mutable borrows simultaneously.
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
        if self.streaming.is_some() {
            self.flush_streaming_to_buffered();
        }
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
        // push_batch split), we must not stream into the streaming state because
        // it would represent rows chronologically newer than the buffered data.
        // Fall back to applying the filter kernel directly.
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

        // Fast streaming path: no buffered data, stream directly into offsets/values.
        match filter.selection() {
            FilterSelection::Indices(indices) => {
                if self.streaming.is_none() {
                    let avg = byte_source::<T>(self.source.as_ref())
                        .map(avg_bytes_per_row)
                        .unwrap_or(32);
                    self.start_streaming(avg);
                }
                let source = byte_source::<T>(self.source.as_ref())?;
                let state = self.streaming.as_mut().unwrap();
                state.offsets.reserve(filter.count());
                Self::append_filtered_nulls(&mut state.nulls, source.nulls(), filter);
                Self::append_rows_by_indices(
                    &mut state.offsets,
                    &mut state.values,
                    source,
                    indices,
                );
                Ok(())
            }
            FilterSelection::Slices(slices) => {
                if self.streaming.is_none() {
                    let avg = byte_source::<T>(self.source.as_ref())
                        .map(avg_bytes_per_row)
                        .unwrap_or(32);
                    self.start_streaming(avg);
                }
                let source = byte_source::<T>(self.source.as_ref())?;
                let state = self.streaming.as_mut().unwrap();
                state.offsets.reserve(filter.count());
                Self::append_filtered_nulls(&mut state.nulls, source.nulls(), filter);
                Self::append_rows_by_slices(
                    &mut state.offsets,
                    &mut state.values,
                    source,
                    slices,
                );
                Ok(())
            }
            selection => self.copy_rows_by_selection(selection),
        }
    }

    fn finish(&mut self) -> Result<ArrayRef, ArrowError> {
        // Flush any remaining streaming data (pure streaming case).
        self.flush_streaming_to_buffered();

        let result = match self.buffered_arrays.len() {
            0 => {
                let array = GenericByteArray::<T>::new(
                    OffsetBuffer::new(ScalarBuffer::from(vec![T::Offset::usize_as(0)])),
                    Buffer::from_vec(vec![0u8; 0]),
                    None,
                );
                Ok(Arc::new(array) as ArrayRef)
            }
            1 => Ok(Arc::clone(&self.buffered_arrays[0])),
            _ => {
                let refs: Vec<&dyn Array> = self
                    .buffered_arrays
                    .iter()
                    .map(|a| a.as_ref() as &dyn Array)
                    .collect();
                concat_arrays(&refs)
            }
        };

        self.buffered_arrays.clear(); // preserve Vec capacity for the next batch
        result
    }

    fn size(&self) -> usize {
        self.source
            .as_ref()
            .map_or(0, |s| s.get_array_memory_size())
            + self.streaming.as_ref().map_or(0, |s| {
                s.offsets.capacity() * std::mem::size_of::<T::Offset>()
                    + s.values.capacity()
                    + s.nulls.allocated_size()
            })
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

    #[test]
    fn test_compact_struct_size() {
        // InProgressByteArray should be no larger than GenericInProgressArray
        // (~56 bytes). A regression here indicates streaming state leaked into
        // the base struct, which would hurt cache performance in the
        // materialized (take / high-selectivity) path.
        use super::super::generic::GenericInProgressArray;
        let byte_size = std::mem::size_of::<InProgressByteArray<GenericStringType<i32>>>();
        let generic_size = std::mem::size_of::<GenericInProgressArray>();
        assert!(
            byte_size <= generic_size * 2,
            "InProgressByteArray ({byte_size}B) is more than 2× GenericInProgressArray \
             ({generic_size}B); streaming state may have leaked into the base struct"
        );
    }
}
