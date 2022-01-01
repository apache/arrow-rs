use std::marker::PhantomData;
use std::ops::Range;

use arrow::buffer::{Buffer, MutableBuffer};

/// A buffer that supports writing new data to the end, and removing data from the front
///
/// Used by [RecordReader](`super::RecordReader`) to buffer up values before returning a
/// potentially smaller number of values, corresponding to a whole number of semantic records
pub trait BufferQueue: Sized {
    type Output: Sized;

    type Slice: ?Sized;

    /// Split out the first `len` committed items
    fn split_off(&mut self, len: usize) -> Self::Output;

    /// Returns a [`Self::Slice`] with at least `batch_size` capacity that can be used
    /// to append data to the end of this [`BufferQueue`]
    ///
    /// NB: writes to the returned slice will not update the length of [`BufferQueue`]
    /// instead a subsequent call should be made to [`BufferQueue::set_len`]
    fn spare_capacity_mut(&mut self, batch_size: usize) -> &mut Self::Slice;

    /// Sets the length of the [`BufferQueue`].
    ///
    /// Intended to be used in combination with [`BufferQueue::spare_capacity_mut`]
    ///
    /// # Panics
    ///
    /// Implementations must panic if `len` is beyond the initialized length
    ///
    /// Implementations may panic if `set_len` is called with less than what has been written
    ///
    /// This distinction is to allow for implementations that return a default initialized
    /// [BufferQueue::Slice`] which doesn't track capacity and length separately
    ///
    /// For example, [`TypedBuffer<T>`] returns a default-initialized `&mut [T]`, and does not
    /// track how much of this slice is actually written to by the caller. This is still
    /// safe as the slice is default-initialized.
    ///
    fn set_len(&mut self, len: usize);
}

/// A typed buffer similar to [`Vec<T>`] but making use of [`MutableBuffer`]
pub struct TypedBuffer<T> {
    buffer: MutableBuffer,

    /// Length in elements of size T
    len: usize,

    /// Placeholder to allow `T` as an invariant generic parameter
    _phantom: PhantomData<*mut T>,
}

impl<T> Default for TypedBuffer<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> TypedBuffer<T> {
    pub fn new() -> Self {
        Self {
            buffer: MutableBuffer::new(0),
            len: 0,
            _phantom: Default::default(),
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn as_slice(&self) -> &[T] {
        let (prefix, buf, suffix) = unsafe { self.buffer.as_slice().align_to::<T>() };
        assert!(prefix.is_empty() && suffix.is_empty());
        buf
    }

    #[inline]
    pub fn as_slice_mut(&mut self) -> &mut [T] {
        let (prefix, buf, suffix) =
            unsafe { self.buffer.as_slice_mut().align_to_mut::<T>() };
        assert!(prefix.is_empty() && suffix.is_empty());
        buf
    }
}

impl<T> BufferQueue for TypedBuffer<T> {
    type Output = Buffer;

    type Slice = [T];

    fn split_off(&mut self, len: usize) -> Self::Output {
        assert!(len <= self.len);

        let num_bytes = len * std::mem::size_of::<T>();
        let remaining_bytes = self.buffer.len() - num_bytes;
        // TODO: Optimize to reduce the copy
        // create an empty buffer, as it will be resized below
        let mut remaining = MutableBuffer::new(0);
        remaining.resize(remaining_bytes, 0);

        let new_records = remaining.as_slice_mut();

        new_records[0..remaining_bytes]
            .copy_from_slice(&self.buffer.as_slice()[num_bytes..]);

        self.buffer.resize(num_bytes, 0);
        self.len -= len;

        std::mem::replace(&mut self.buffer, remaining).into()
    }

    fn spare_capacity_mut(&mut self, batch_size: usize) -> &mut Self::Slice {
        self.buffer
            .resize((self.len + batch_size) * std::mem::size_of::<T>(), 0);

        let range = self.len..self.len + batch_size;
        &mut self.as_slice_mut()[range]
    }

    fn set_len(&mut self, len: usize) {
        self.len = len;

        let new_bytes = self.len * std::mem::size_of::<T>();
        assert!(new_bytes <= self.buffer.len());
        self.buffer.resize(new_bytes, 0);
    }
}

pub trait ValuesBuffer: BufferQueue {
    fn pad_nulls(
        &mut self,
        range: Range<usize>,
        rev_position_iter: impl Iterator<Item = usize>,
    );
}

impl<T> ValuesBuffer for TypedBuffer<T> {
    fn pad_nulls(
        &mut self,
        range: Range<usize>,
        rev_position_iter: impl Iterator<Item = usize>,
    ) {
        let slice = self.as_slice_mut();

        for (value_pos, level_pos) in range.rev().zip(rev_position_iter) {
            debug_assert!(level_pos >= value_pos);
            if level_pos <= value_pos {
                break;
            }
            slice.swap(value_pos, level_pos)
        }
    }
}
