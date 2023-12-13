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

use crate::arrow::buffer::bit_util::iter_set_bits_rev;

/// A buffer that supports writing new data to the end, and removing data from the front
///
/// Used by [RecordReader](`super::RecordReader`) to buffer up values before returning a
/// potentially smaller number of values, corresponding to a whole number of semantic records
pub trait BufferQueue: Sized {
    type Output: Sized;

    type Slice: ?Sized;

    /// Consumes the contents of this [`BufferQueue`]
    fn consume(&mut self) -> Self::Output;

    /// Returns a [`Self::Slice`] with at least `batch_size` capacity that can be used
    /// to append data to the end of this [`BufferQueue`]
    ///
    /// NB: writes to the returned slice will not update the length of [`BufferQueue`]
    /// instead a subsequent call should be made to [`BufferQueue::truncate_buffer`]
    fn get_output_slice(&mut self, batch_size: usize) -> &mut Self::Slice;

    /// Sets the length of the [`BufferQueue`].
    ///
    /// Intended to be used in combination with [`BufferQueue::get_output_slice`]
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
    /// For example, [`BufferQueue`] returns a default-initialized `&mut [T]`, and does not
    /// track how much of this slice is actually written to by the caller. This is still
    /// safe as the slice is default-initialized.
    ///
    fn truncate_buffer(&mut self, len: usize);
}

impl<T: Copy + Default> BufferQueue for Vec<T> {
    type Output = Self;

    type Slice = [T];

    fn consume(&mut self) -> Self::Output {
        std::mem::take(self)
    }

    fn get_output_slice(&mut self, batch_size: usize) -> &mut Self::Slice {
        let len = self.len();
        self.resize(len + batch_size, T::default());
        &mut self[len..]
    }

    fn truncate_buffer(&mut self, len: usize) {
        assert!(len <= self.len());
        self.truncate(len)
    }
}

/// A [`BufferQueue`] capable of storing column values
pub trait ValuesBuffer: BufferQueue {
    ///
    /// If a column contains nulls, more level data may be read than value data, as null
    /// values are not encoded. Therefore, first the levels data is read, the null count
    /// determined, and then the corresponding number of values read to a [`ValuesBuffer`].
    ///
    /// It is then necessary to move this values data into positions that correspond to
    /// the non-null level positions. This is what this method does.
    ///
    /// It is provided with:
    ///
    /// - `read_offset` - the offset in [`ValuesBuffer`] to start null padding from
    /// - `values_read` - the number of values read
    /// - `levels_read` - the number of levels read
    /// - `valid_mask` - a packed mask of valid levels
    ///
    fn pad_nulls(
        &mut self,
        read_offset: usize,
        values_read: usize,
        levels_read: usize,
        valid_mask: &[u8],
    );
}

impl<T: Copy + Default> ValuesBuffer for Vec<T> {
    fn pad_nulls(
        &mut self,
        read_offset: usize,
        values_read: usize,
        levels_read: usize,
        valid_mask: &[u8],
    ) {
        assert!(self.len() >= read_offset + levels_read);

        let values_range = read_offset..read_offset + values_read;
        for (value_pos, level_pos) in values_range.rev().zip(iter_set_bits_rev(valid_mask)) {
            debug_assert!(level_pos >= value_pos);
            if level_pos <= value_pos {
                break;
            }
            self[level_pos] = self[value_pos];
        }
    }
}
