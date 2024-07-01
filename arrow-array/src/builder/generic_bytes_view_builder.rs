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

use std::any::Any;
use std::marker::PhantomData;
use std::sync::Arc;

use arrow_buffer::{Buffer, BufferBuilder, NullBufferBuilder, ScalarBuffer};
use arrow_data::ByteView;
use arrow_schema::ArrowError;

use crate::builder::ArrayBuilder;
use crate::types::bytes::ByteArrayNativeType;
use crate::types::{BinaryViewType, ByteViewType, StringViewType};
use crate::{ArrayRef, GenericByteViewArray};

const DEFAULT_BLOCK_SIZE: u32 = 8 * 1024;

/// A builder for [`GenericByteViewArray`]
///
/// A [`GenericByteViewArray`] consists of a list of data blocks containing string data,
/// and a list of views into those buffers.
///
/// This builder can be used in two ways
///
/// # Append Values
///
/// To avoid bump allocating, this builder allocates data in fixed size blocks, configurable
/// using [`GenericByteViewBuilder::with_block_size`]. [`GenericByteViewBuilder::append_value`]
/// writes values larger than 12 bytes to the current in-progress block, with values smaller
/// than 12 bytes inlined into the views. If a value is appended that will not fit in the
/// in-progress block, it will be closed, and a new block of sufficient size allocated
///
/// # Append Views
///
/// Some use-cases may wish to reuse an existing allocation containing string data, for example,
/// when parsing data from a parquet data page. In such a case entire blocks can be appended
/// using [`GenericByteViewBuilder::append_block`] and then views into this block appended
/// using [`GenericByteViewBuilder::try_append_view`]
pub struct GenericByteViewBuilder<T: ByteViewType + ?Sized> {
    views_builder: BufferBuilder<u128>,
    null_buffer_builder: NullBufferBuilder,
    completed: Vec<Buffer>,
    in_progress: Vec<u8>,
    block_size: u32,
    phantom: PhantomData<T>,
}

impl<T: ByteViewType + ?Sized> GenericByteViewBuilder<T> {
    /// Creates a new [`GenericByteViewBuilder`].
    pub fn new() -> Self {
        Self::with_capacity(1024)
    }

    /// Creates a new [`GenericByteViewBuilder`] with space for `capacity` string values.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            views_builder: BufferBuilder::new(capacity),
            null_buffer_builder: NullBufferBuilder::new(capacity),
            completed: vec![],
            in_progress: vec![],
            block_size: DEFAULT_BLOCK_SIZE,
            phantom: Default::default(),
        }
    }

    /// Override the size of buffers to allocate for holding string data
    pub fn with_block_size(self, block_size: u32) -> Self {
        Self { block_size, ..self }
    }

    /// Append a new data block returning the new block offset
    ///
    /// Note: this will first flush any in-progress block
    ///
    /// This allows appending views from blocks added using [`Self::append_block`]. See
    /// [`Self::append_value`] for appending individual values
    ///
    /// ```
    /// # use arrow_array::builder::StringViewBuilder;
    /// let mut builder = StringViewBuilder::new();
    ///
    /// let block = builder.append_block(b"helloworldbingobongo".into());
    ///
    /// builder.try_append_view(block, 0, 5).unwrap();
    /// builder.try_append_view(block, 5, 5).unwrap();
    /// builder.try_append_view(block, 10, 5).unwrap();
    /// builder.try_append_view(block, 15, 5).unwrap();
    /// builder.try_append_view(block, 0, 15).unwrap();
    /// let array = builder.finish();
    ///
    /// let actual: Vec<_> = array.iter().flatten().collect();
    /// let expected = &["hello", "world", "bingo", "bongo", "helloworldbingo"];
    /// assert_eq!(actual, expected);
    /// ```
    pub fn append_block(&mut self, buffer: Buffer) -> u32 {
        assert!(buffer.len() < u32::MAX as usize);

        self.flush_in_progress();
        let offset = self.completed.len();
        self.push_completed(buffer);
        offset as u32
    }

    /// Append a view of the given `block`, `offset` and `length`
    ///
    /// # Safety
    /// (1) The block must have been added using [`Self::append_block`]
    /// (2) The range `offset..offset+length` must be within the bounds of the block
    /// (3) The data in the block must be valid of type `T`
    pub unsafe fn append_view_unchecked(&mut self, block: u32, offset: u32, len: u32) {
        let b = self.completed.get_unchecked(block as usize);
        let start = offset as usize;
        let end = start.saturating_add(len as usize);
        let b = b.get_unchecked(start..end);

        let view = make_view(b, block, offset);
        self.views_builder.append(view);
        self.null_buffer_builder.append_non_null();
    }

    /// Try to append a view of the given `block`, `offset` and `length`
    ///
    /// See [`Self::append_block`]
    pub fn try_append_view(&mut self, block: u32, offset: u32, len: u32) -> Result<(), ArrowError> {
        let b = self.completed.get(block as usize).ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!("No block found with index {block}"))
        })?;
        let start = offset as usize;
        let end = start.saturating_add(len as usize);

        let b = b.get(start..end).ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!(
                "Range {start}..{end} out of bounds for block of length {}",
                b.len()
            ))
        })?;

        if T::Native::from_bytes_checked(b).is_none() {
            return Err(ArrowError::InvalidArgumentError(
                "Invalid view data".to_string(),
            ));
        }

        unsafe {
            self.append_view_unchecked(block, offset, len);
        }
        Ok(())
    }

    /// Flushes the in progress block if any
    #[inline]
    fn flush_in_progress(&mut self) {
        if !self.in_progress.is_empty() {
            let f = Buffer::from_vec(std::mem::take(&mut self.in_progress));
            self.push_completed(f)
        }
    }

    /// Append a block to `self.completed`, checking for overflow
    #[inline]
    fn push_completed(&mut self, block: Buffer) {
        assert!(block.len() < u32::MAX as usize, "Block too large");
        assert!(self.completed.len() < u32::MAX as usize, "Too many blocks");
        self.completed.push(block);
    }

    /// Appends a value into the builder
    ///
    /// # Panics
    ///
    /// Panics if
    /// - String buffer count exceeds `u32::MAX`
    /// - String length exceeds `u32::MAX`
    #[inline]
    pub fn append_value(&mut self, value: impl AsRef<T::Native>) {
        let v: &[u8] = value.as_ref().as_ref();
        let length: u32 = v.len().try_into().unwrap();
        if length <= 12 {
            let mut view_buffer = [0; 16];
            view_buffer[0..4].copy_from_slice(&length.to_le_bytes());
            view_buffer[4..4 + v.len()].copy_from_slice(v);
            self.views_builder.append(u128::from_le_bytes(view_buffer));
            self.null_buffer_builder.append_non_null();
            return;
        }

        let required_cap = self.in_progress.len() + v.len();
        if self.in_progress.capacity() < required_cap {
            self.flush_in_progress();
            let to_reserve = v.len().max(self.block_size as usize);
            self.in_progress.reserve(to_reserve);
        };
        let offset = self.in_progress.len() as u32;
        self.in_progress.extend_from_slice(v);

        let view = ByteView {
            length,
            prefix: u32::from_le_bytes(v[0..4].try_into().unwrap()),
            buffer_index: self.completed.len() as u32,
            offset,
        };
        self.views_builder.append(view.into());
        self.null_buffer_builder.append_non_null();
    }

    /// Append an `Option` value into the builder
    #[inline]
    pub fn append_option(&mut self, value: Option<impl AsRef<T::Native>>) {
        match value {
            None => self.append_null(),
            Some(v) => self.append_value(v),
        };
    }

    /// Append a null value into the builder
    #[inline]
    pub fn append_null(&mut self) {
        self.null_buffer_builder.append_null();
        self.views_builder.append(0);
    }

    /// Builds the [`GenericByteViewArray`] and reset this builder
    pub fn finish(&mut self) -> GenericByteViewArray<T> {
        self.flush_in_progress();
        let completed = std::mem::take(&mut self.completed);
        let len = self.views_builder.len();
        let views = ScalarBuffer::new(self.views_builder.finish(), 0, len);
        let nulls = self.null_buffer_builder.finish();
        // SAFETY: valid by construction
        unsafe { GenericByteViewArray::new_unchecked(views, completed, nulls) }
    }

    /// Builds the [`GenericByteViewArray`] without resetting the builder
    pub fn finish_cloned(&self) -> GenericByteViewArray<T> {
        let mut completed = self.completed.clone();
        if !self.in_progress.is_empty() {
            completed.push(Buffer::from_slice_ref(&self.in_progress));
        }
        let len = self.views_builder.len();
        let views = Buffer::from_slice_ref(self.views_builder.as_slice());
        let views = ScalarBuffer::new(views, 0, len);
        let nulls = self.null_buffer_builder.finish_cloned();
        // SAFETY: valid by construction
        unsafe { GenericByteViewArray::new_unchecked(views, completed, nulls) }
    }

    /// Returns the current null buffer as a slice
    pub fn validity_slice(&self) -> Option<&[u8]> {
        self.null_buffer_builder.as_slice()
    }
}

impl<T: ByteViewType + ?Sized> Default for GenericByteViewBuilder<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: ByteViewType + ?Sized> std::fmt::Debug for GenericByteViewBuilder<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}ViewBuilder", T::PREFIX)?;
        f.debug_struct("")
            .field("views_builder", &self.views_builder)
            .field("in_progress", &self.in_progress)
            .field("completed", &self.completed)
            .field("null_buffer_builder", &self.null_buffer_builder)
            .finish()
    }
}

impl<T: ByteViewType + ?Sized> ArrayBuilder for GenericByteViewBuilder<T> {
    fn len(&self) -> usize {
        self.null_buffer_builder.len()
    }

    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }

    fn finish_cloned(&self) -> ArrayRef {
        Arc::new(self.finish_cloned())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

impl<T: ByteViewType + ?Sized, V: AsRef<T::Native>> Extend<Option<V>>
    for GenericByteViewBuilder<T>
{
    #[inline]
    fn extend<I: IntoIterator<Item = Option<V>>>(&mut self, iter: I) {
        for v in iter {
            self.append_option(v)
        }
    }
}

/// Array builder for [`StringViewArray`][crate::StringViewArray]
///
/// Values can be appended using [`GenericByteViewBuilder::append_value`], and nulls with
/// [`GenericByteViewBuilder::append_null`] as normal.
pub type StringViewBuilder = GenericByteViewBuilder<StringViewType>;

///  Array builder for [`BinaryViewArray`][crate::BinaryViewArray]
///
/// Values can be appended using [`GenericByteViewBuilder::append_value`], and nulls with
/// [`GenericByteViewBuilder::append_null`] as normal.
pub type BinaryViewBuilder = GenericByteViewBuilder<BinaryViewType>;

/// Create a view based on the given data, block id and offset
#[inline(always)]
pub fn make_view(data: &[u8], block_id: u32, offset: u32) -> u128 {
    let len = data.len() as u32;
    if len <= 12 {
        let mut view_buffer = [0; 16];
        view_buffer[0..4].copy_from_slice(&len.to_le_bytes());
        view_buffer[4..4 + data.len()].copy_from_slice(data);
        u128::from_le_bytes(view_buffer)
    } else {
        let view = ByteView {
            length: len,
            prefix: u32::from_le_bytes(data[0..4].try_into().unwrap()),
            buffer_index: block_id,
            offset,
        };
        view.into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Array;

    #[test]
    fn test_string_view() {
        let b1 = Buffer::from(b"world\xFFbananas\xF0\x9F\x98\x81");
        let b2 = Buffer::from(b"cupcakes");
        let b3 = Buffer::from(b"Many strings are here contained of great length and verbosity");

        let mut v = StringViewBuilder::new();
        assert_eq!(v.append_block(b1), 0);

        v.append_value("This is a very long string that exceeds the inline length");
        v.append_value("This is another very long string that exceeds the inline length");

        assert_eq!(v.append_block(b2), 2);
        assert_eq!(v.append_block(b3), 3);

        // Test short strings
        v.try_append_view(0, 0, 5).unwrap(); // world
        v.try_append_view(0, 6, 7).unwrap(); // bananas
        v.try_append_view(2, 3, 5).unwrap(); // cake
        v.try_append_view(2, 0, 3).unwrap(); // cup
        v.try_append_view(2, 0, 8).unwrap(); // cupcakes
        v.try_append_view(0, 13, 4).unwrap(); // 😁
        v.try_append_view(0, 13, 0).unwrap(); //

        // Test longer strings
        v.try_append_view(3, 0, 16).unwrap(); // Many strings are
        v.try_append_view(1, 0, 19).unwrap(); // This is a very long
        v.try_append_view(3, 13, 27).unwrap(); // here contained of great length

        v.append_value("I do so like long strings");

        let array = v.finish_cloned();
        array.to_data().validate_full().unwrap();
        assert_eq!(array.data_buffers().len(), 5);
        let actual: Vec<_> = array.iter().map(Option::unwrap).collect();
        assert_eq!(
            actual,
            &[
                "This is a very long string that exceeds the inline length",
                "This is another very long string that exceeds the inline length",
                "world",
                "bananas",
                "cakes",
                "cup",
                "cupcakes",
                "😁",
                "",
                "Many strings are",
                "This is a very long",
                "are here contained of great",
                "I do so like long strings"
            ]
        );

        let err = v.try_append_view(0, u32::MAX, 1).unwrap_err();
        assert_eq!(err.to_string(), "Invalid argument error: Range 4294967295..4294967296 out of bounds for block of length 17");

        let err = v.try_append_view(0, 1, u32::MAX).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Invalid argument error: Range 1..4294967296 out of bounds for block of length 17"
        );

        let err = v.try_append_view(0, 13, 2).unwrap_err();
        assert_eq!(err.to_string(), "Invalid argument error: Invalid view data");

        let err = v.try_append_view(0, 40, 0).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Invalid argument error: Range 40..40 out of bounds for block of length 17"
        );

        let err = v.try_append_view(5, 0, 0).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Invalid argument error: No block found with index 5"
        );
    }
}
