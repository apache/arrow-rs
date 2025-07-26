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
use arrow_array::cast::AsArray;
use arrow_array::types::ByteViewType;
use arrow_array::{Array, ArrayRef, GenericByteViewArray};
use arrow_buffer::{Buffer, NullBufferBuilder};
use arrow_data::{ByteView, MAX_INLINE_VIEW_LEN};
use arrow_schema::ArrowError;
use std::marker::PhantomData;
use std::sync::Arc;

/// InProgressArray for [`StringViewArray`] and [`BinaryViewArray`]
///
/// This structure buffers the views and data buffers as they are copied from
/// the source array, and then produces a new array when `finish` is called. It
/// also handles "garbage collection" by copying strings to a new buffer when
/// the source buffer is sparse (i.e. uses at least 2x more than the memory it
/// needs).
///
/// [`StringViewArray`]: arrow_array::StringViewArray
/// [`BinaryViewArray`]: arrow_array::BinaryViewArray
pub(crate) struct InProgressByteViewArray<B: ByteViewType> {
    /// The source array and information
    source: Option<Source>,
    /// the target batch size (and thus size for views allocation)
    batch_size: usize,
    /// The in progress views
    views: Vec<u128>,
    /// In progress nulls
    nulls: NullBufferBuilder,
    /// current buffer
    current: Option<Vec<u8>>,
    /// completed buffers
    completed: Vec<Buffer>,
    /// Allocates new buffers of increasing size as needed
    buffer_source: BufferSource,
    /// Phantom so we can use the same struct for both StringViewArray and
    /// BinaryViewArray
    _phantom: PhantomData<B>,
}

struct Source {
    /// The array to copy form
    array: ArrayRef,
    /// Should the strings from the source array be copied into new buffers?
    need_gc: bool,
    /// How many bytes were actually used in the source array's buffers?
    ideal_buffer_size: usize,
}

// manually implement Debug because ByteViewType doesn't implement Debug
impl<B: ByteViewType> std::fmt::Debug for InProgressByteViewArray<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InProgressByteViewArray")
            .field("batch_size", &self.batch_size)
            .field("views", &self.views.len())
            .field("nulls", &self.nulls)
            .field("current", &self.current.as_ref().map(|_| "Some(...)"))
            .field("completed", &self.completed.len())
            .finish()
    }
}

impl<B: ByteViewType> InProgressByteViewArray<B> {
    pub(crate) fn new(batch_size: usize) -> Self {
        let buffer_source = BufferSource::new();

        Self {
            batch_size,
            source: None,
            views: Vec::new(),                         // allocate in push
            nulls: NullBufferBuilder::new(batch_size), // no allocation
            current: None,
            completed: vec![],
            buffer_source,
            _phantom: PhantomData,
        }
    }

    /// Allocate space for output views and nulls if needed
    ///
    /// This is done on write (when we know it is necessary) rather than
    /// eagerly to avoid allocations that are not used.
    fn ensure_capacity(&mut self) {
        self.views.reserve(self.batch_size);
    }

    /// Finishes in progress buffer, if any
    fn finish_current(&mut self) {
        let Some(next_buffer) = self.current.take() else {
            return;
        };
        self.completed.push(next_buffer.into());
    }

    /// Append views to self.views, updating the buffer index if necessary
    #[inline(never)]
    fn append_views_and_update_buffer_index(&mut self, views: &[u128], buffers: &[Buffer]) {
        if let Some(buffer) = self.current.take() {
            self.completed.push(buffer.into());
        }
        let starting_buffer: u32 = self.completed.len().try_into().expect("too many buffers");
        self.completed.extend_from_slice(buffers);

        if starting_buffer == 0 {
            // If there are no buffers, we can just use the views as is
            self.views.extend_from_slice(views);
        } else {
            // If there are buffers, we need to update the buffer index
            let updated_views = views.iter().map(|v| {
                let mut byte_view = ByteView::from(*v);
                if byte_view.length > MAX_INLINE_VIEW_LEN {
                    // Small views (<=12 bytes) are inlined, so only need to update large views
                    byte_view.buffer_index += starting_buffer;
                };
                byte_view.as_u128()
            });

            self.views.extend(updated_views);
        }
    }

    /// Append views to self.views, copying data from the buffers into
    /// self.buffers and updating the buffer index as necessary.
    ///
    /// # Arguments
    /// - `views` - the views to append
    /// - `view_buffer_size` - the total number of bytes pointed to by all
    ///   views (used to allocate new buffers if needed)
    /// - `buffers` - the buffers the reviews point to
    #[inline(never)]
    fn append_views_and_copy_strings(
        &mut self,
        views: &[u128],
        view_buffer_size: usize,
        buffers: &[Buffer],
    ) {
        // Note: the calculations below are designed to avoid any reallocations
        // of the current buffer, and to only allocate new buffers when
        // necessary, which is critical for performance.

        // If there is no current buffer, allocate a new one
        let Some(current) = self.current.take() else {
            let new_buffer = self.buffer_source.next_buffer(view_buffer_size);
            self.append_views_and_copy_strings_inner(views, new_buffer, buffers);
            return;
        };

        // If there is a current buffer with enough space, append the views and
        // copy the strings into the existing buffer.
        let mut remaining_capacity = current.capacity() - current.len();
        if view_buffer_size <= remaining_capacity {
            self.append_views_and_copy_strings_inner(views, current, buffers);
            return;
        }

        // Here there is a current buffer, but it doesn't have enough space to
        // hold all the strings. Copy as many views as we can into the current
        // buffer and then allocate a new buffer for the remaining views
        //
        // TODO: should we copy the strings too at the same time?
        let mut num_view_to_current = 0;
        for view in views {
            let b = ByteView::from(*view);
            let str_len = b.length;
            if remaining_capacity < str_len as usize {
                break;
            }
            if str_len > MAX_INLINE_VIEW_LEN {
                remaining_capacity -= str_len as usize;
            }
            num_view_to_current += 1;
        }

        let first_views = &views[0..num_view_to_current];
        let string_bytes_to_copy = current.capacity() - current.len() - remaining_capacity;
        let remaining_view_buffer_size = view_buffer_size - string_bytes_to_copy;

        self.append_views_and_copy_strings_inner(first_views, current, buffers);
        let completed = self.current.take().expect("completed");
        self.completed.push(completed.into());

        // Copy any remaining views into a new buffer
        let remaining_views = &views[num_view_to_current..];
        let new_buffer = self.buffer_source.next_buffer(remaining_view_buffer_size);
        self.append_views_and_copy_strings_inner(remaining_views, new_buffer, buffers);
    }

    /// Append views to self.views, copying data from the buffers into
    /// dst_buffer, which is then set as self.current
    ///
    /// # Panics:
    /// If `self.current` is `Some`
    ///
    /// See `append_views_and_copy_strings` for more details
    #[inline(never)]
    fn append_views_and_copy_strings_inner(
        &mut self,
        views: &[u128],
        mut dst_buffer: Vec<u8>,
        buffers: &[Buffer],
    ) {
        assert!(self.current.is_none(), "current buffer should be None");

        if views.is_empty() {
            self.current = Some(dst_buffer);
            return;
        }

        let new_buffer_index: u32 = self.completed.len().try_into().expect("too many buffers");

        // In debug builds, check that the vector has enough capacity to copy
        // the views into it without reallocating.
        #[cfg(debug_assertions)]
        {
            let total_length: usize = views
                .iter()
                .filter_map(|v| {
                    let b = ByteView::from(*v);
                    if b.length > MAX_INLINE_VIEW_LEN {
                        Some(b.length as usize)
                    } else {
                        None
                    }
                })
                .sum();
            debug_assert!(
                dst_buffer.capacity() >= total_length,
                "dst_buffer capacity {} is less than total length {}",
                dst_buffer.capacity(),
                total_length
            );
        }

        // Copy the views, updating the buffer index and copying the data as needed
        let new_views = views.iter().map(|v| {
            let mut b: ByteView = ByteView::from(*v);
            if b.length > MAX_INLINE_VIEW_LEN {
                let buffer_index = b.buffer_index as usize;
                let buffer_offset = b.offset as usize;
                let str_len = b.length as usize;

                // Update view to location in current
                b.offset = dst_buffer.len() as u32;
                b.buffer_index = new_buffer_index;

                // safety: input views are validly constructed
                let src = unsafe {
                    buffers
                        .get_unchecked(buffer_index)
                        .get_unchecked(buffer_offset..buffer_offset + str_len)
                };
                dst_buffer.extend_from_slice(src);
            }
            b.as_u128()
        });
        self.views.extend(new_views);
        self.current = Some(dst_buffer);
    }
}

impl<B: ByteViewType> InProgressArray for InProgressByteViewArray<B> {
    fn set_source(&mut self, source: Option<ArrayRef>) {
        self.source = source.map(|array| {
            let s = array.as_byte_view::<B>();

            let (need_gc, ideal_buffer_size) = if s.data_buffers().is_empty() {
                (false, 0)
            } else {
                let ideal_buffer_size = s.total_buffer_bytes_used();
                // We don't use get_buffer_memory_size here, because gc is for the contents of the
                // data buffers, not views and nulls.
                let actual_buffer_size =
                    s.data_buffers().iter().map(|b| b.capacity()).sum::<usize>();
                // copying strings is expensive, so only do it if the array is
                // sparse (uses at least 2x the memory it needs)
                let need_gc =
                    ideal_buffer_size != 0 && actual_buffer_size > (ideal_buffer_size * 2);
                (need_gc, ideal_buffer_size)
            };

            Source {
                array,
                need_gc,
                ideal_buffer_size,
            }
        })
    }

    fn copy_rows(&mut self, offset: usize, len: usize) -> Result<(), ArrowError> {
        self.ensure_capacity();
        let source = self.source.take().ok_or_else(|| {
            ArrowError::InvalidArgumentError(
                "Internal Error: InProgressByteViewArray: source not set".to_string(),
            )
        })?;

        // If creating StringViewArray output, ensure input was valid utf8 too
        let s = source.array.as_byte_view::<B>();

        // add any nulls, as necessary
        if let Some(nulls) = s.nulls().as_ref() {
            let nulls = nulls.slice(offset, len);
            self.nulls.append_buffer(&nulls);
        } else {
            self.nulls.append_n_non_nulls(len);
        };

        let buffers = s.data_buffers();
        let views = &s.views().as_ref()[offset..offset + len];

        // If there are no data buffers in s (all inlined views), can append the
        // views/nulls and done
        if source.ideal_buffer_size == 0 {
            self.views.extend_from_slice(views);
            self.source = Some(source);
            return Ok(());
        }

        // Copying the strings into a buffer can be time-consuming so
        // only do it if the array is sparse
        if source.need_gc {
            self.append_views_and_copy_strings(views, source.ideal_buffer_size, buffers);
        } else {
            self.append_views_and_update_buffer_index(views, buffers);
        }
        self.source = Some(source);
        Ok(())
    }

    fn finish(&mut self) -> Result<ArrayRef, ArrowError> {
        self.finish_current();
        assert!(self.current.is_none());
        let buffers = std::mem::take(&mut self.completed);
        let views = std::mem::take(&mut self.views);
        let nulls = self.nulls.finish();
        self.nulls = NullBufferBuilder::new(self.batch_size);

        // Safety: we created valid views and buffers above and the
        // input arrays had value data and nulls
        let new_array =
            unsafe { GenericByteViewArray::<B>::new_unchecked(views.into(), buffers, nulls) };
        Ok(Arc::new(new_array))
    }
}

const STARTING_BLOCK_SIZE: usize = 4 * 1024; // (note the first size used is actually 8KiB)
const MAX_BLOCK_SIZE: usize = 1024 * 1024; // 1MiB

/// Manages allocating new buffers for `StringViewArray` in increasing sizes
#[derive(Debug)]
struct BufferSource {
    current_size: usize,
}

impl BufferSource {
    fn new() -> Self {
        Self {
            current_size: STARTING_BLOCK_SIZE,
        }
    }

    /// Return a new buffer, with a capacity of at least `min_size`
    fn next_buffer(&mut self, min_size: usize) -> Vec<u8> {
        let size = self.next_size(min_size);
        Vec::with_capacity(size)
    }

    fn next_size(&mut self, min_size: usize) -> usize {
        if self.current_size < MAX_BLOCK_SIZE {
            // If the current size is less than the max size, we can double it
            // we have fixed start/end block sizes, so we can't overflow
            self.current_size = self.current_size.saturating_mul(2);
        }
        if self.current_size >= min_size {
            self.current_size
        } else {
            // increase next size until we hit min_size or max  size
            while self.current_size <= min_size && self.current_size < MAX_BLOCK_SIZE {
                self.current_size = self.current_size.saturating_mul(2);
            }
            self.current_size.max(min_size)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_source() {
        let mut source = BufferSource::new();
        assert_eq!(source.next_buffer(1000).capacity(), 8192);
        assert_eq!(source.next_buffer(1000).capacity(), 16384);
        assert_eq!(source.next_buffer(1000).capacity(), 32768);
        assert_eq!(source.next_buffer(1000).capacity(), 65536);
        assert_eq!(source.next_buffer(1000).capacity(), 131072);
        assert_eq!(source.next_buffer(1000).capacity(), 262144);
        assert_eq!(source.next_buffer(1000).capacity(), 524288);
        assert_eq!(source.next_buffer(1000).capacity(), 1024 * 1024);
        // clamped to max size
        assert_eq!(source.next_buffer(1000).capacity(), 1024 * 1024);
        // Can override with larger size request
        assert_eq!(source.next_buffer(10_000_000).capacity(), 10_000_000);
    }

    #[test]
    fn test_buffer_source_with_min_small() {
        let mut source = BufferSource::new();
        // First buffer should be 8kb
        assert_eq!(source.next_buffer(5_600).capacity(), 8 * 1024);
        // then 16kb
        assert_eq!(source.next_buffer(5_600).capacity(), 16 * 1024);
        // then 32kb
        assert_eq!(source.next_buffer(5_600).capacity(), 32 * 1024);
    }

    #[test]
    fn test_buffer_source_with_min_large() {
        let mut source = BufferSource::new();
        assert_eq!(source.next_buffer(500_000).capacity(), 512 * 1024);
        assert_eq!(source.next_buffer(500_000).capacity(), 1024 * 1024);
        // clamped to max size
        assert_eq!(source.next_buffer(500_000).capacity(), 1024 * 1024);
        // Can override with larger size request
        assert_eq!(source.next_buffer(2_000_000).capacity(), 2_000_000);
    }
}
