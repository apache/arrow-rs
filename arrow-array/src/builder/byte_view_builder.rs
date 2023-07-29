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

use crate::builder::{ArrayBuilder, BufferBuilder};
use crate::types::{BinaryViewType, ByteViewType, StringViewType};
use crate::{ArrayRef, GenericByteViewArray};
use arrow_buffer::{Buffer, NullBufferBuilder, ScalarBuffer};
use arrow_data::view::View;
use std::any::Any;
use std::marker::PhantomData;
use std::sync::Arc;

const DEFAULT_BLOCK_SIZE: u32 = 8 * 1024;

/// A builder for [`GenericByteViewArray`]
///
/// See [`Self::append_value`] for the allocation strategy
pub struct GenericByteViewBuilder<T: ByteViewType> {
    views_builder: BufferBuilder<u128>,
    null_buffer_builder: NullBufferBuilder,
    completed: Vec<Buffer>,
    in_progress: Vec<u8>,
    block_size: u32,
    phantom: PhantomData<T>,
}

impl<T: ByteViewType> GenericByteViewBuilder<T> {
    /// Creates a new [`GenericByteViewBuilder`].
    pub fn new() -> Self {
        Self::with_capacity(1024)
    }

    /// Creates a new [`GenericByteViewBuilder`] with space for `capacity` strings
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

    /// Override the minimum size of buffers to allocate for string data
    pub fn with_block_size(self, block_size: u32) -> Self {
        Self { block_size, ..self }
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
            let mut offset = [0; 16];
            offset[0..4].copy_from_slice(&length.to_le_bytes());
            offset[4..4 + v.len()].copy_from_slice(v);
            self.views_builder.append(u128::from_le_bytes(offset));
            self.null_buffer_builder.append_non_null();
            return;
        }

        let required_cap = self.in_progress.len() + v.len();
        if self.in_progress.capacity() < required_cap {
            let in_progress = Vec::with_capacity(v.len().max(self.block_size as usize));
            let flushed = std::mem::replace(&mut self.in_progress, in_progress);
            if !flushed.is_empty() {
                assert!(self.completed.len() < u32::MAX as usize);
                self.completed.push(flushed.into());
            }
        };
        let offset = self.in_progress.len() as u32;
        self.in_progress.extend_from_slice(v);

        let view = View {
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
        let mut completed = std::mem::take(&mut self.completed);
        if !self.in_progress.is_empty() {
            completed.push(std::mem::take(&mut self.in_progress).into());
        }
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
}

impl<T: ByteViewType> Default for GenericByteViewBuilder<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: ByteViewType> std::fmt::Debug for GenericByteViewBuilder<T> {
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

impl<T: ByteViewType> ArrayBuilder for GenericByteViewBuilder<T> {
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

impl<T: ByteViewType, V: AsRef<T::Native>> Extend<Option<V>>
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
