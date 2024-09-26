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
use hashbrown::hash_table::Entry;
use hashbrown::HashTable;

use crate::builder::ArrayBuilder;
use crate::types::bytes::ByteArrayNativeType;
use crate::types::{BinaryViewType, ByteViewType, StringViewType};
use crate::{ArrayRef, GenericByteViewArray};

const STARTING_BLOCK_SIZE: u32 = 8 * 1024; // 8KiB
const MAX_BLOCK_SIZE: u32 = 2 * 1024 * 1024; // 2MiB

enum BlockSizeGrowthStrategy {
    Fixed { size: u32 },
    Exponential { current_size: u32 },
}

impl BlockSizeGrowthStrategy {
    fn next_size(&mut self) -> u32 {
        match self {
            Self::Fixed { size } => *size,
            Self::Exponential { current_size } => {
                if *current_size < MAX_BLOCK_SIZE {
                    // we have fixed start/end block sizes, so we can't overflow
                    *current_size = current_size.saturating_mul(2);
                    *current_size
                } else {
                    MAX_BLOCK_SIZE
                }
            }
        }
    }
}

/// A builder for [`GenericByteViewArray`]
///
/// A [`GenericByteViewArray`] consists of a list of data blocks containing string data,
/// and a list of views into those buffers.
///
/// See examples on [`StringViewBuilder`] and [`BinaryViewBuilder`]
///
/// This builder can be used in two ways
///
/// # Append Values
///
/// To avoid bump allocating, this builder allocates data in fixed size blocks, configurable
/// using [`GenericByteViewBuilder::with_fixed_block_size`]. [`GenericByteViewBuilder::append_value`]
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
    block_size: BlockSizeGrowthStrategy,
    /// Some if deduplicating strings
    /// map `<string hash> -> <index to the views>`
    string_tracker: Option<(HashTable<usize>, ahash::RandomState)>,
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
            block_size: BlockSizeGrowthStrategy::Exponential {
                current_size: STARTING_BLOCK_SIZE,
            },
            string_tracker: None,
            phantom: Default::default(),
        }
    }

    /// Set a fixed buffer size for variable length strings
    ///
    /// The block size is the size of the buffer used to store values greater
    /// than 12 bytes. The builder allocates new buffers when the current
    /// buffer is full.
    ///
    /// By default the builder balances buffer size and buffer count by
    /// growing buffer size exponentially from 8KB up to 2MB. The
    /// first buffer allocated is 8KB, then 16KB, then 32KB, etc up to 2MB.
    ///
    /// If this method is used, any new buffers allocated are  
    /// exactly this size. This can be useful for advanced users
    /// that want to control the memory usage and buffer count.
    ///
    /// See <https://github.com/apache/arrow-rs/issues/6094> for more details on the implications.
    pub fn with_fixed_block_size(self, block_size: u32) -> Self {
        debug_assert!(block_size > 0, "Block size must be greater than 0");
        Self {
            block_size: BlockSizeGrowthStrategy::Fixed { size: block_size },
            ..self
        }
    }

    /// Override the size of buffers to allocate for holding string data
    /// Use `with_fixed_block_size` instead.
    #[deprecated(note = "Use `with_fixed_block_size` instead")]
    pub fn with_block_size(self, block_size: u32) -> Self {
        self.with_fixed_block_size(block_size)
    }

    /// Deduplicate strings while building the array
    ///
    /// This will potentially decrease the memory usage if the array have repeated strings
    /// It will also increase the time to build the array as it needs to hash the strings
    pub fn with_deduplicate_strings(self) -> Self {
        Self {
            string_tracker: Some((
                HashTable::with_capacity(self.views_builder.capacity()),
                Default::default(),
            )),
            ..self
        }
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

    /// Returns the value at the given index
    /// Useful if we want to know what value has been inserted to the builder
    /// The index has to be smaller than `self.len()`, otherwise it will panic
    pub fn get_value(&self, index: usize) -> &[u8] {
        let view = self.views_builder.as_slice().get(index).unwrap();
        let len = *view as u32;
        if len <= 12 {
            // # Safety
            // The view is valid from the builder
            unsafe { GenericByteViewArray::<T>::inline_value(view, len as usize) }
        } else {
            let view = ByteView::from(*view);
            if view.buffer_index < self.completed.len() as u32 {
                let block = &self.completed[view.buffer_index as usize];
                &block[view.offset as usize..view.offset as usize + view.length as usize]
            } else {
                &self.in_progress[view.offset as usize..view.offset as usize + view.length as usize]
            }
        }
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

        // Deduplication if:
        // (1) deduplication is enabled.
        // (2) len > 12
        if let Some((mut ht, hasher)) = self.string_tracker.take() {
            let hash_val = hasher.hash_one(v);
            let hasher_fn = |v: &_| hasher.hash_one(v);

            let entry = ht.entry(
                hash_val,
                |idx| {
                    let stored_value = self.get_value(*idx);
                    v == stored_value
                },
                hasher_fn,
            );
            match entry {
                Entry::Occupied(occupied) => {
                    // If the string already exists, we will directly use the view
                    let idx = occupied.get();
                    self.views_builder
                        .append(self.views_builder.as_slice()[*idx]);
                    self.null_buffer_builder.append_non_null();
                    self.string_tracker = Some((ht, hasher));
                    return;
                }
                Entry::Vacant(vacant) => {
                    // o.w. we insert the (string hash -> view index)
                    // the idx is current length of views_builder, as we are inserting a new view
                    vacant.insert(self.views_builder.len());
                }
            }
            self.string_tracker = Some((ht, hasher));
        }

        let required_cap = self.in_progress.len() + v.len();
        if self.in_progress.capacity() < required_cap {
            self.flush_in_progress();
            let to_reserve = v.len().max(self.block_size.next_size() as usize);
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
        if let Some((ref mut ht, _)) = self.string_tracker.as_mut() {
            ht.clear();
        }
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

    /// Return the allocated size of this builder in bytes, useful for memory accounting.
    pub fn allocated_size(&self) -> usize {
        let views = self.views_builder.capacity() * std::mem::size_of::<u128>();
        let null = self.null_buffer_builder.allocated_size();
        let buffer_size = self.completed.iter().map(|b| b.capacity()).sum::<usize>();
        let in_progress = self.in_progress.capacity();
        let tracker = match &self.string_tracker {
            Some((ht, _)) => ht.capacity() * std::mem::size_of::<usize>(),
            None => 0,
        };
        buffer_size + in_progress + tracker + views + null
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
///
/// # Example
/// ```
/// # use arrow_array::builder::StringViewBuilder;
/// # use arrow_array::StringViewArray;
/// let mut builder = StringViewBuilder::new();
/// builder.append_value("hello");
/// builder.append_null();
/// builder.append_value("world");
/// let array = builder.finish();
///
/// let expected = vec![Some("hello"), None, Some("world")];
/// let actual: Vec<_> = array.iter().collect();
/// assert_eq!(expected, actual);
/// ```
pub type StringViewBuilder = GenericByteViewBuilder<StringViewType>;

///  Array builder for [`BinaryViewArray`][crate::BinaryViewArray]
///
/// Values can be appended using [`GenericByteViewBuilder::append_value`], and nulls with
/// [`GenericByteViewBuilder::append_null`] as normal.
///
/// # Example
/// ```
/// # use arrow_array::builder::BinaryViewBuilder;
/// use arrow_array::BinaryViewArray;
/// let mut builder = BinaryViewBuilder::new();
/// builder.append_value("hello");
/// builder.append_null();
/// builder.append_value("world");
/// let array = builder.finish();
///
/// let expected: Vec<Option<&[u8]>> = vec![Some(b"hello"), None, Some(b"world")];
/// let actual: Vec<_> = array.iter().collect();
/// assert_eq!(expected, actual);
/// ```
///
pub type BinaryViewBuilder = GenericByteViewBuilder<BinaryViewType>;

/// Creates a view from a fixed length input (the compiler can generate
/// specialized code for this)
fn make_inlined_view<const LEN: usize>(data: &[u8]) -> u128 {
    let mut view_buffer = [0; 16];
    view_buffer[0..4].copy_from_slice(&(LEN as u32).to_le_bytes());
    view_buffer[4..4 + LEN].copy_from_slice(&data[..LEN]);
    u128::from_le_bytes(view_buffer)
}

/// Create a view based on the given data, block id and offset.
///
/// Note that the code below is carefully examined with x86_64 assembly code: <https://godbolt.org/z/685YPsd5G>
/// The goal is to avoid calling into `ptr::copy_non_interleave`, which makes function call (i.e., not inlined),
/// which slows down things.
#[inline(never)]
pub fn make_view(data: &[u8], block_id: u32, offset: u32) -> u128 {
    let len = data.len();

    // Generate specialized code for each potential small string length
    // to improve performance
    match len {
        0 => make_inlined_view::<0>(data),
        1 => make_inlined_view::<1>(data),
        2 => make_inlined_view::<2>(data),
        3 => make_inlined_view::<3>(data),
        4 => make_inlined_view::<4>(data),
        5 => make_inlined_view::<5>(data),
        6 => make_inlined_view::<6>(data),
        7 => make_inlined_view::<7>(data),
        8 => make_inlined_view::<8>(data),
        9 => make_inlined_view::<9>(data),
        10 => make_inlined_view::<10>(data),
        11 => make_inlined_view::<11>(data),
        12 => make_inlined_view::<12>(data),
        // When string is longer than 12 bytes, it can't be inlined, we create a ByteView instead.
        _ => {
            let view = ByteView {
                length: len as u32,
                prefix: u32::from_le_bytes(data[0..4].try_into().unwrap()),
                buffer_index: block_id,
                offset,
            };
            view.as_u128()
        }
    }
}

#[cfg(test)]
mod tests {
    use core::str;

    use super::*;
    use crate::Array;

    #[test]
    fn test_string_view_deduplicate() {
        let value_1 = "long string to test string view";
        let value_2 = "not so similar string but long";

        let mut builder = StringViewBuilder::new()
            .with_deduplicate_strings()
            .with_fixed_block_size(value_1.len() as u32 * 2); // so that we will have multiple buffers

        let values = vec![
            Some(value_1),
            Some(value_2),
            Some("short"),
            Some(value_1),
            None,
            Some(value_2),
            Some(value_1),
        ];
        builder.extend(values.clone());

        let array = builder.finish_cloned();
        array.to_data().validate_full().unwrap();
        assert_eq!(array.data_buffers().len(), 1); // without duplication we would need 3 buffers.
        let actual: Vec<_> = array.iter().collect();
        assert_eq!(actual, values);

        let view0 = array.views().first().unwrap();
        let view3 = array.views().get(3).unwrap();
        let view6 = array.views().get(6).unwrap();

        assert_eq!(view0, view3);
        assert_eq!(view0, view6);

        assert_eq!(array.views().get(1), array.views().get(5));
    }

    #[test]
    fn test_string_view_deduplicate_after_finish() {
        let mut builder = StringViewBuilder::new().with_deduplicate_strings();

        let value_1 = "long string to test string view";
        let value_2 = "not so similar string but long";
        builder.append_value(value_1);
        let _array = builder.finish();
        builder.append_value(value_2);
        let _array = builder.finish();
        builder.append_value(value_1);
        let _array = builder.finish();
    }

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
        let actual: Vec<_> = array.iter().flatten().collect();
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

    #[test]
    fn test_string_view_with_block_size_growth() {
        let mut exp_builder = StringViewBuilder::new();
        let mut fixed_builder = StringViewBuilder::new().with_fixed_block_size(STARTING_BLOCK_SIZE);

        let long_string = str::from_utf8(&[b'a'; STARTING_BLOCK_SIZE as usize]).unwrap();

        for i in 0..9 {
            // 8k, 16k, 32k, 64k, 128k, 256k, 512k, 1M, 2M
            for _ in 0..(2_u32.pow(i)) {
                exp_builder.append_value(long_string);
                fixed_builder.append_value(long_string);
            }
            exp_builder.flush_in_progress();
            fixed_builder.flush_in_progress();

            // Every step only add one buffer, but the buffer size is much larger
            assert_eq!(exp_builder.completed.len(), i as usize + 1);
            assert_eq!(
                exp_builder.completed[i as usize].len(),
                STARTING_BLOCK_SIZE as usize * 2_usize.pow(i)
            );

            // This step we added 2^i blocks, the sum of blocks should be 2^(i+1) - 1
            assert_eq!(fixed_builder.completed.len(), 2_usize.pow(i + 1) - 1);

            // Every buffer is fixed size
            assert!(fixed_builder
                .completed
                .iter()
                .all(|b| b.len() == STARTING_BLOCK_SIZE as usize));
        }

        // Add one more value, and the buffer stop growing.
        exp_builder.append_value(long_string);
        exp_builder.flush_in_progress();
        assert_eq!(
            exp_builder.completed.last().unwrap().capacity(),
            MAX_BLOCK_SIZE as usize
        );
    }
}
