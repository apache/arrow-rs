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

use crate::buffer::ScalarBuffer;
use crate::{ArrowNativeType, MutableBuffer, NullBuffer, OffsetBufferBuilder};
use std::ops::Deref;

/// A non-empty buffer of monotonically increasing, positive integers.
///
/// [`OffsetBuffer`] are used to represent ranges of offsets. An
/// `OffsetBuffer` of `N+1` items contains `N` such ranges. The start
/// offset for element `i` is `offsets[i]` and the end offset is
/// `offsets[i+1]`. Equal offsets represent an empty range.
///
/// # Example
///
/// This example shows how 5 distinct ranges, are represented using a
/// 6 entry `OffsetBuffer`. The first entry `(0, 3)` represents the
/// three offsets `0, 1, 2`. The entry `(3,3)` represent no offsets
/// (e.g. an empty list).
///
/// ```text
///   ┌───────┐                ┌───┐
///   │ (0,3) │                │ 0 │
///   ├───────┤                ├───┤
///   │ (3,3) │                │ 3 │
///   ├───────┤                ├───┤
///   │ (3,4) │                │ 3 │
///   ├───────┤                ├───┤
///   │ (4,5) │                │ 4 │
///   ├───────┤                ├───┤
///   │ (5,7) │                │ 5 │
///   └───────┘                ├───┤
///                            │ 7 │
///                            └───┘
///
///                        Offsets Buffer
///    Logical
///    Offsets
///
///  (offsets[i],
///   offsets[i+1])
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetBuffer<O: ArrowNativeType>(ScalarBuffer<O>);

impl<O: ArrowNativeType> OffsetBuffer<O> {
    /// Create a new [`OffsetBuffer`] from the provided [`ScalarBuffer`]
    ///
    /// # Panics
    ///
    /// Panics if `buffer` is not a non-empty buffer containing
    /// monotonically increasing values greater than or equal to zero
    pub fn new(buffer: ScalarBuffer<O>) -> Self {
        assert!(!buffer.is_empty(), "offsets cannot be empty");
        assert!(
            buffer[0] >= O::usize_as(0),
            "offsets must be greater than 0"
        );
        assert!(
            buffer.windows(2).all(|w| w[0] <= w[1]),
            "offsets must be monotonically increasing"
        );
        Self(buffer)
    }

    /// Create a new [`OffsetBuffer`] from the provided [`ScalarBuffer`]
    ///
    /// # Safety
    ///
    /// `buffer` must be a non-empty buffer containing monotonically increasing
    /// values greater than or equal to zero
    pub unsafe fn new_unchecked(buffer: ScalarBuffer<O>) -> Self {
        Self(buffer)
    }

    /// Create a new [`OffsetBuffer`] containing a single 0 value
    pub fn new_empty() -> Self {
        let buffer = MutableBuffer::from_len_zeroed(std::mem::size_of::<O>());
        Self(buffer.into_buffer().into())
    }

    /// Create a new [`OffsetBuffer`] containing `len + 1` `0` values
    pub fn new_zeroed(len: usize) -> Self {
        let len_bytes = len
            .checked_add(1)
            .and_then(|o| o.checked_mul(std::mem::size_of::<O>()))
            .expect("overflow");
        let buffer = MutableBuffer::from_len_zeroed(len_bytes);
        Self(buffer.into_buffer().into())
    }

    /// Create a new [`OffsetBuffer`] from the iterator of slice lengths
    ///
    /// ```
    /// # use arrow_buffer::OffsetBuffer;
    /// let offsets = OffsetBuffer::<i32>::from_lengths([1, 3, 5]);
    /// assert_eq!(offsets.as_ref(), &[0, 1, 4, 9]);
    /// ```
    ///
    /// If you want to create an [`OffsetBuffer`] where all lengths are the same,
    /// consider using the faster [`OffsetBuffer::from_repeated_length`] instead.
    ///
    /// # Panics
    ///
    /// Panics on overflow
    pub fn from_lengths<I>(lengths: I) -> Self
    where
        I: IntoIterator<Item = usize>,
    {
        let iter = lengths.into_iter();
        let mut out = Vec::with_capacity(iter.size_hint().0 + 1);
        out.push(O::usize_as(0));

        let mut acc = 0_usize;
        for length in iter {
            acc = acc.checked_add(length).expect("usize overflow");
            out.push(O::usize_as(acc))
        }
        // Check for overflow
        O::from_usize(acc).expect("offset overflow");
        Self(out.into())
    }

    /// Create a new [`OffsetBuffer`] where each slice has the same length
    /// `length`, repeated `n` times.
    ///
    ///
    /// Example
    /// ```
    /// # use arrow_buffer::OffsetBuffer;
    /// let offsets = OffsetBuffer::<i32>::from_repeated_length(4, 3);
    /// assert_eq!(offsets.as_ref(), &[0, 4, 8, 12]);
    /// ```
    ///
    /// # Panics
    ///
    /// Panics on overflow
    pub fn from_repeated_length(length: usize, n: usize) -> Self {
        if n == 0 {
            return Self::new_empty();
        }

        if length == 0 {
            return Self::new_zeroed(n);
        }

        // Check for overflow
        // Making sure we don't overflow usize or O when calculating the total length
        length.checked_mul(n).expect("usize overflow");

        // Check for overflow
        O::from_usize(length * n).expect("offset overflow");

        let offsets = (0..=n)
            .map(|index| O::usize_as(index * length))
            .collect::<Vec<O>>();

        Self(ScalarBuffer::from(offsets))
    }

    /// Get an Iterator over the lengths of this [`OffsetBuffer`]
    ///
    /// ```
    /// # use arrow_buffer::{OffsetBuffer, ScalarBuffer};
    /// let offsets = OffsetBuffer::<_>::new(ScalarBuffer::<i32>::from(vec![0, 1, 4, 9]));
    /// assert_eq!(offsets.lengths().collect::<Vec<usize>>(), vec![1, 3, 5]);
    /// ```
    ///
    /// Empty [`OffsetBuffer`] will return an empty iterator
    /// ```
    /// # use arrow_buffer::OffsetBuffer;
    /// let offsets = OffsetBuffer::<i32>::new_empty();
    /// assert_eq!(offsets.lengths().count(), 0);
    /// ```
    ///
    /// This can be used to merge multiple [`OffsetBuffer`]s to one
    /// ```
    /// # use arrow_buffer::{OffsetBuffer, ScalarBuffer};
    ///
    /// let buffer1 = OffsetBuffer::<i32>::from_lengths([2, 6, 3, 7, 2]);
    /// let buffer2 = OffsetBuffer::<i32>::from_lengths([1, 3, 5, 7, 9]);
    ///
    /// let merged = OffsetBuffer::<i32>::from_lengths(
    ///     vec![buffer1, buffer2].iter().flat_map(|x| x.lengths())
    /// );
    ///
    /// assert_eq!(merged.lengths().collect::<Vec<_>>(), &[2, 6, 3, 7, 2, 1, 3, 5, 7, 9]);
    /// ```
    pub fn lengths(&self) -> impl ExactSizeIterator<Item = usize> + '_ {
        self.0.windows(2).map(|x| x[1].as_usize() - x[0].as_usize())
    }

    /// Free up unused memory.
    pub fn shrink_to_fit(&mut self) {
        self.0.shrink_to_fit();
    }

    /// Returns the inner [`ScalarBuffer`]
    pub fn inner(&self) -> &ScalarBuffer<O> {
        &self.0
    }

    /// Returns the inner [`ScalarBuffer`], consuming self
    pub fn into_inner(self) -> ScalarBuffer<O> {
        self.0
    }

    /// Claim memory used by this buffer in the provided memory pool.
    #[cfg(feature = "pool")]
    pub fn claim(&self, pool: &dyn crate::MemoryPool) {
        self.0.claim(pool);
    }

    /// Returns a zero-copy slice of this buffer with length `len` and starting at `offset`
    pub fn slice(&self, offset: usize, len: usize) -> Self {
        Self(self.0.slice(offset, len.saturating_add(1)))
    }

    /// Returns true if this [`OffsetBuffer`] is equal to `other`, using pointer comparisons
    /// to determine buffer equality. This is cheaper than `PartialEq::eq` but may
    /// return false when the arrays are logically equal
    #[inline]
    pub fn ptr_eq(&self, other: &Self) -> bool {
        self.0.ptr_eq(&other.0)
    }

    /// Check if any null positions in the `null_buffer` correspond to
    /// non-empty ranges in this [`OffsetBuffer`].
    ///
    /// In variable-length array types (e.g., `StringArray`, `ListArray`),
    /// null entries may or may not have empty offset ranges. This method
    /// detects cases where a null entry has a non-empty range
    /// (i.e., `offsets[i] != offsets[i+1]`), which means the underlying
    /// data buffer contains data behind nulls.
    ///
    /// This matters because unwrapping (flattening) a list array exposes
    /// the child values, including those behind null entries. If null
    /// entries point to non-empty ranges, the unwrapped values will
    /// contain data that may not be meaningful to operate on and could
    /// cause errors (e.g., division by zero in the child values).
    ///
    /// Returns `false` if `null_buffer` is `None` or contains no nulls.
    ///
    /// # Example
    ///
    /// ```
    /// # use arrow_buffer::{OffsetBuffer, ScalarBuffer, NullBuffer};
    /// // Offsets where null at index 1 has an empty range (3..3)
    /// let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 3, 6]));
    /// let nulls = NullBuffer::from(vec![true, false, true]);
    /// assert!(!offsets.is_there_null_pointing_to_non_empty_value(Some(&nulls)));
    ///
    /// // Offsets where null at index 1 has a non-empty range (3..7)
    /// let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 7, 10]));
    /// let nulls = NullBuffer::from(vec![true, false, true]);
    /// assert!(offsets.is_there_null_pointing_to_non_empty_value(Some(&nulls)));
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if the length of the `null_buffer` does not equal `self.len() - 1`.
    pub fn is_there_null_pointing_to_non_empty_value(
        &self,
        null_buffer: Option<&NullBuffer>,
    ) -> bool {
        let Some(null_buffer) = null_buffer else {
            return false;
        };

        assert_eq!(
            self.len() - 1,
            null_buffer.len(),
            "The length of the offsets should be 1 more than the length of the null buffer"
        );

        if null_buffer.null_count() == 0 {
            return false;
        }

        // Offsets always have at least 1 value
        let initial_offset = self[0];
        let last_offset = self[self.len() - 1];

        // If all the values are null (offsets have 1 more value than the length of the array)
        if null_buffer.null_count() == self.len() - 1 {
            return last_offset != initial_offset;
        }

        let mut valid_slices_iter = null_buffer.valid_slices();

        // This is safe as we validated that are at least 1 valid value in the array
        let (start, end) = valid_slices_iter.next().unwrap();

        // If the nulls before have length greater than 0
        if self[start] != initial_offset {
            return true;
        }

        // End is exclusive, so it already point to the last offset value
        // This is valid as the length of the array is always 1 less than the length of the offsets
        let mut end_offset_of_last_valid_value = self[end];

        for (start, end) in valid_slices_iter {
            // If there is a null value that point to a non-empty value than the start offset of the valid value
            // will be different that the end offset of the last valid value
            if self[start] != end_offset_of_last_valid_value {
                return true;
            }

            // End is exclusive, so it already point to the last offset value
            // This is valid as the length of the array is always 1 less than the length of the offsets
            end_offset_of_last_valid_value = self[end];
        }

        end_offset_of_last_valid_value != last_offset
    }
}

impl<T: ArrowNativeType> Deref for OffsetBuffer<T> {
    type Target = [T];

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: ArrowNativeType> AsRef<[T]> for OffsetBuffer<T> {
    #[inline]
    fn as_ref(&self) -> &[T] {
        self
    }
}

impl<O: ArrowNativeType> From<OffsetBufferBuilder<O>> for OffsetBuffer<O> {
    fn from(value: OffsetBufferBuilder<O>) -> Self {
        value.finish()
    }
}

impl<O: ArrowNativeType> Default for OffsetBuffer<O> {
    fn default() -> Self {
        Self::new_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic(expected = "offsets cannot be empty")]
    fn empty_offsets() {
        OffsetBuffer::new(Vec::<i32>::new().into());
    }

    #[test]
    #[should_panic(expected = "offsets must be greater than 0")]
    fn negative_offsets() {
        OffsetBuffer::new(vec![-1, 0, 1].into());
    }

    #[test]
    fn offsets() {
        OffsetBuffer::new(vec![0, 1, 2, 3].into());

        let offsets = OffsetBuffer::<i32>::new_zeroed(3);
        assert_eq!(offsets.as_ref(), &[0; 4]);

        let offsets = OffsetBuffer::<i32>::new_zeroed(0);
        assert_eq!(offsets.as_ref(), &[0; 1]);
    }

    #[test]
    #[should_panic(expected = "overflow")]
    fn offsets_new_zeroed_overflow() {
        OffsetBuffer::<i32>::new_zeroed(usize::MAX);
    }

    #[test]
    #[should_panic(expected = "offsets must be monotonically increasing")]
    fn non_monotonic_offsets() {
        OffsetBuffer::new(vec![1, 2, 0].into());
    }

    #[test]
    fn from_lengths() {
        let buffer = OffsetBuffer::<i32>::from_lengths([2, 6, 3, 7, 2]);
        assert_eq!(buffer.as_ref(), &[0, 2, 8, 11, 18, 20]);

        let half_max = i32::MAX / 2;
        let buffer = OffsetBuffer::<i32>::from_lengths([half_max as usize, half_max as usize]);
        assert_eq!(buffer.as_ref(), &[0, half_max, half_max * 2]);
    }

    #[test]
    #[should_panic(expected = "offset overflow")]
    fn from_lengths_offset_overflow() {
        OffsetBuffer::<i32>::from_lengths([i32::MAX as usize, 1]);
    }

    #[test]
    #[should_panic(expected = "usize overflow")]
    fn from_lengths_usize_overflow() {
        OffsetBuffer::<i32>::from_lengths([usize::MAX, 1]);
    }

    #[test]
    #[should_panic(expected = "offset overflow")]
    fn from_repeated_lengths_offset_length_overflow() {
        OffsetBuffer::<i32>::from_repeated_length(i32::MAX as usize / 4, 5);
    }

    #[test]
    #[should_panic(expected = "offset overflow")]
    fn from_repeated_lengths_offset_repeat_overflow() {
        OffsetBuffer::<i32>::from_repeated_length(1, i32::MAX as usize + 1);
    }

    #[test]
    #[should_panic(expected = "offset overflow")]
    fn from_repeated_lengths_usize_length_overflow() {
        OffsetBuffer::<i32>::from_repeated_length(usize::MAX, 1);
    }

    #[test]
    #[should_panic(expected = "usize overflow")]
    fn from_repeated_lengths_usize_length_usize_overflow() {
        OffsetBuffer::<i32>::from_repeated_length(usize::MAX, 2);
    }

    #[test]
    #[should_panic(expected = "offset overflow")]
    fn from_repeated_lengths_usize_repeat_overflow() {
        OffsetBuffer::<i32>::from_repeated_length(1, usize::MAX);
    }

    #[test]
    fn get_lengths() {
        let offsets = OffsetBuffer::<i32>::new(ScalarBuffer::<i32>::from(vec![0, 1, 4, 9]));
        assert_eq!(offsets.lengths().collect::<Vec<usize>>(), vec![1, 3, 5]);
    }

    #[test]
    fn get_lengths_should_be_with_fixed_size() {
        let offsets = OffsetBuffer::<i32>::new(ScalarBuffer::<i32>::from(vec![0, 1, 4, 9]));
        let iter = offsets.lengths();
        assert_eq!(iter.size_hint(), (3, Some(3)));
        assert_eq!(iter.len(), 3);
    }

    #[test]
    fn get_lengths_from_empty_offset_buffer_should_be_empty_iterator() {
        let offsets = OffsetBuffer::<i32>::new_empty();
        assert_eq!(offsets.lengths().collect::<Vec<usize>>(), vec![]);
    }

    #[test]
    fn impl_eq() {
        fn are_equal<T: Eq>(a: &T, b: &T) -> bool {
            a.eq(b)
        }

        assert!(
            are_equal(
                &OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 1, 4, 9])),
                &OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 1, 4, 9]))
            ),
            "OffsetBuffer should implement Eq."
        );
    }

    #[test]
    fn impl_default() {
        let default = OffsetBuffer::<i32>::default();
        assert_eq!(default.as_ref(), &[0]);
    }

    #[test]
    fn from_repeated_length_basic() {
        // Basic case with length 4, repeated 3 times
        let buffer = OffsetBuffer::<i32>::from_repeated_length(4, 3);
        assert_eq!(buffer.as_ref(), &[0, 4, 8, 12]);

        // Verify the lengths are correct
        let lengths: Vec<usize> = buffer.lengths().collect();
        assert_eq!(lengths, vec![4, 4, 4]);
    }

    #[test]
    fn from_repeated_length_single_repeat() {
        // Length 5, repeated once
        let buffer = OffsetBuffer::<i32>::from_repeated_length(5, 1);
        assert_eq!(buffer.as_ref(), &[0, 5]);

        let lengths: Vec<usize> = buffer.lengths().collect();
        assert_eq!(lengths, vec![5]);
    }

    #[test]
    fn from_repeated_length_zero_repeats() {
        let buffer = OffsetBuffer::<i32>::from_repeated_length(10, 0);
        assert_eq!(buffer, OffsetBuffer::<i32>::new_empty());
    }

    #[test]
    fn from_repeated_length_zero_length() {
        // Zero length, repeated 5 times (all zeros)
        let buffer = OffsetBuffer::<i32>::from_repeated_length(0, 5);
        assert_eq!(buffer.as_ref(), &[0, 0, 0, 0, 0, 0]);

        // All lengths should be 0
        let lengths: Vec<usize> = buffer.lengths().collect();
        assert_eq!(lengths, vec![0, 0, 0, 0, 0]);
    }

    #[test]
    fn from_repeated_length_large_values() {
        // Test with larger values that don't overflow
        let buffer = OffsetBuffer::<i32>::from_repeated_length(1000, 100);
        assert_eq!(buffer[0], 0);

        // Verify all lengths are 1000
        let lengths: Vec<usize> = buffer.lengths().collect();
        assert_eq!(lengths.len(), 100);
        assert!(lengths.iter().all(|&len| len == 1000));
    }

    #[test]
    fn from_repeated_length_unit_length() {
        // Length 1, repeated multiple times
        let buffer = OffsetBuffer::<i32>::from_repeated_length(1, 10);
        assert_eq!(buffer.as_ref(), &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

        let lengths: Vec<usize> = buffer.lengths().collect();
        assert_eq!(lengths, vec![1; 10]);
    }

    #[test]
    fn from_repeated_length_max_safe_values() {
        // Test with maximum safe values for i32
        // i32::MAX / 3 ensures we don't overflow when repeated twice
        let third_max = (i32::MAX / 3) as usize;
        let buffer = OffsetBuffer::<i32>::from_repeated_length(third_max, 2);
        assert_eq!(
            buffer.as_ref(),
            &[0, third_max as i32, (third_max * 2) as i32]
        );
    }

    // ---------------------------------------------------------------
    // Tests for is_there_null_pointing_to_non_empty_value
    // ---------------------------------------------------------------

    #[test]
    fn null_pointing_none_null_buffer() {
        // No null buffer at all -> false
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 5, 8]));
        assert!(!offsets.is_there_null_pointing_to_non_empty_value(None));
    }

    #[test]
    fn null_pointing_all_valid() {
        // Null buffer with zero nulls -> false (early return via filter)
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 5, 8]));
        let nulls = NullBuffer::new_valid(3);
        assert!(!offsets.is_there_null_pointing_to_non_empty_value(Some(&nulls)));
    }

    #[test]
    fn null_pointing_all_null_empty_offsets() {
        // All values are null and all offsets are equal (no data behind nulls) -> false
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 0, 0, 0]));
        let nulls = NullBuffer::new_null(3);
        assert!(!offsets.is_there_null_pointing_to_non_empty_value(Some(&nulls)));
    }

    #[test]
    fn null_pointing_all_null_non_empty_offsets() {
        // All values are null but offsets span data -> true
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 2, 5, 7]));
        let nulls = NullBuffer::new_null(3);
        assert!(offsets.is_there_null_pointing_to_non_empty_value(Some(&nulls)));
    }

    #[test]
    fn null_pointing_all_null_nonzero_but_equal_offsets() {
        // All null, offsets start at non-zero but are all equal -> false
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![5, 5, 5]));
        let nulls = NullBuffer::new_null(2);
        assert!(!offsets.is_there_null_pointing_to_non_empty_value(Some(&nulls)));
    }

    #[test]
    fn null_pointing_leading_nulls_with_data() {
        // Nulls at the beginning that point to non-empty ranges -> true
        // offsets: [0, 3, 5, 8]  nulls: [false, true, true]
        // Index 0 is null with range 0..3 (non-empty)
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 5, 8]));
        let nulls = NullBuffer::from(vec![false, true, true]);
        assert!(offsets.is_there_null_pointing_to_non_empty_value(Some(&nulls)));
    }

    #[test]
    fn null_pointing_leading_nulls_without_data() {
        // Nulls at the beginning with empty ranges -> continue checking
        // offsets: [0, 0, 3, 6]  nulls: [false, true, true]
        // Index 0 is null with range 0..0 (empty)
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 0, 3, 6]));
        let nulls = NullBuffer::from(vec![false, true, true]);
        assert!(!offsets.is_there_null_pointing_to_non_empty_value(Some(&nulls)));
    }

    #[test]
    fn null_pointing_trailing_nulls_with_data() {
        // Nulls at the end that point to non-empty ranges -> true
        // offsets: [0, 3, 5, 8]  nulls: [true, true, false]
        // Index 2 is null with range 5..8 (non-empty)
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 5, 8]));
        let nulls = NullBuffer::from(vec![true, true, false]);
        assert!(offsets.is_there_null_pointing_to_non_empty_value(Some(&nulls)));
    }

    #[test]
    fn null_pointing_trailing_nulls_without_data() {
        // Nulls at the end with empty ranges -> false
        // offsets: [0, 3, 6, 6]  nulls: [true, true, false]
        // Index 2 is null with range 6..6 (empty)
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 6, 6]));
        let nulls = NullBuffer::from(vec![true, true, false]);
        assert!(!offsets.is_there_null_pointing_to_non_empty_value(Some(&nulls)));
    }

    #[test]
    fn null_pointing_middle_nulls_with_data() {
        // Null in the middle with non-empty range -> true
        // offsets: [0, 3, 7, 10]  nulls: [true, false, true]
        // Index 1 is null with range 3..7 (non-empty)
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 7, 10]));
        let nulls = NullBuffer::from(vec![true, false, true]);
        assert!(offsets.is_there_null_pointing_to_non_empty_value(Some(&nulls)));
    }

    #[test]
    fn null_pointing_middle_nulls_without_data() {
        // Null in the middle with empty range -> false
        // offsets: [0, 3, 3, 6]  nulls: [true, false, true]
        // Index 1 is null with range 3..3 (empty)
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 3, 6]));
        let nulls = NullBuffer::from(vec![true, false, true]);
        assert!(!offsets.is_there_null_pointing_to_non_empty_value(Some(&nulls)));
    }

    #[test]
    fn null_pointing_multiple_null_regions_all_empty() {
        // Multiple null regions, all with empty ranges -> false
        // offsets: [0, 0, 3, 3, 6, 6]  nulls: [false, true, false, true, false]
        // Nulls at index 0 (0..0) and index 2 (3..3) are empty
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 0, 3, 3, 6, 6]));
        let nulls = NullBuffer::from(vec![false, true, false, true, false]);
        assert!(!offsets.is_there_null_pointing_to_non_empty_value(Some(&nulls)));
    }

    #[test]
    fn null_pointing_multiple_null_regions_second_has_data() {
        // Two null regions: first empty, second non-empty -> true
        // offsets: [0, 0, 3, 5, 6]  nulls: [false, true, false, true]
        // Null at index 0 (0..0 empty), null at index 2 (3..5 non-empty)
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 0, 3, 5, 6]));
        let nulls = NullBuffer::from(vec![false, true, false, true]);
        assert!(offsets.is_there_null_pointing_to_non_empty_value(Some(&nulls)));
    }

    #[test]
    fn null_pointing_multiple_null_regions_later_gap_has_data() {
        // Three null regions: first two empty, third non-empty -> true
        // offsets: [0, 0, 3, 3, 6, 8, 10]  nulls: [false, true, false, true, false, true]
        // valid_slices: (1,2), (3,4), (5,6)
        // first slice: start=1, self[1]=0 == initial_offset=0 OK, end_offset=self[2]=3
        // loop iter 1: start=3, self[3]=3 == 3 OK (first gap empty), end_offset=self[4]=6
        // loop iter 2: start=5, self[5]=8 != 6 -> true (second gap has data)
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 0, 3, 3, 6, 8, 10]));
        let nulls = NullBuffer::from(vec![false, true, false, true, false, true]);
        assert!(offsets.is_there_null_pointing_to_non_empty_value(Some(&nulls)));
    }

    #[test]
    fn null_pointing_single_element_null_empty() {
        // Single element, null with empty range -> false
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 0]));
        let nulls = NullBuffer::new_null(1);
        assert!(!offsets.is_there_null_pointing_to_non_empty_value(Some(&nulls)));
    }

    #[test]
    fn null_pointing_single_element_null_non_empty() {
        // Single element, null with non-empty range -> true
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 5]));
        let nulls = NullBuffer::new_null(1);
        assert!(offsets.is_there_null_pointing_to_non_empty_value(Some(&nulls)));
    }

    #[test]
    fn null_pointing_single_element_valid() {
        // Single element, valid -> false (no nulls at all)
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 5]));
        let nulls = NullBuffer::new_valid(1);
        assert!(!offsets.is_there_null_pointing_to_non_empty_value(Some(&nulls)));
    }

    #[test]
    fn null_pointing_consecutive_nulls_between_valid_slices() {
        // Multiple consecutive nulls between valid regions
        // offsets: [0, 2, 2, 2, 5, 8]  nulls: [true, false, false, true, true]
        // Valid: [0], nulls: [1,2], valid: [3,4]
        // Null region [1,2] has offsets 2..2..2 (empty) -> false
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 2, 2, 2, 5, 8]));
        let nulls = NullBuffer::from(vec![true, false, false, true, true]);
        assert!(!offsets.is_there_null_pointing_to_non_empty_value(Some(&nulls)));
    }

    #[test]
    fn null_pointing_consecutive_nulls_between_valid_slices_with_data() {
        // Multiple consecutive nulls between valid regions, nulls have data
        // offsets: [0, 2, 3, 4, 5, 8]  nulls: [true, false, false, true, true]
        // valid_slices: (0,1), (3,5)
        // first slice: start=0, end=1 -> self[0]=0 == initial_offset=0 OK
        //   end_offset_of_last_valid_value = self[1] = 2
        // second slice: start=3, end=5 -> self[3]=4 != 2 -> true
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 2, 3, 4, 5, 8]));
        let nulls = NullBuffer::from(vec![true, false, false, true, true]);
        assert!(offsets.is_there_null_pointing_to_non_empty_value(Some(&nulls)));
    }

    #[test]
    fn null_pointing_nonzero_initial_offset_all_null_equal() {
        // Non-zero starting offset, all null, all offsets equal -> false
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![10, 10, 10]));
        let nulls = NullBuffer::new_null(2);
        assert!(!offsets.is_there_null_pointing_to_non_empty_value(Some(&nulls)));
    }

    #[test]
    fn null_pointing_nonzero_initial_offset_with_data() {
        // Non-zero starting offset, null has data
        // offsets: [10, 15, 20]  nulls: [false, true]
        // Null at index 0 with range 10..15 (non-empty) -> true
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![10, 15, 20]));
        let nulls = NullBuffer::from(vec![false, true]);
        assert!(offsets.is_there_null_pointing_to_non_empty_value(Some(&nulls)));
    }

    #[test]
    fn null_pointing_i64_offsets() {
        // Test with i64 offset type
        let offsets = OffsetBuffer::new(ScalarBuffer::<i64>::from(vec![0, 0, 5, 5]));
        let nulls = NullBuffer::from(vec![false, true, false]);
        // Null at index 0 (0..0 empty), valid at index 1 (0..5), null at index 2 (5..5 empty) -> false
        assert!(!offsets.is_there_null_pointing_to_non_empty_value(Some(&nulls)));
    }

    #[test]
    fn null_pointing_i64_offsets_with_data_in_null() {
        let offsets = OffsetBuffer::new(ScalarBuffer::<i64>::from(vec![0, 3, 5, 10]));
        let nulls = NullBuffer::from(vec![true, false, true]);
        // Valid at index 0 (0..3), null at index 1 (3..5 non-empty) -> true
        assert!(offsets.is_there_null_pointing_to_non_empty_value(Some(&nulls)));
    }

    #[test]
    #[should_panic(
        expected = "The length of the offsets should be 1 more than the length of the null buffer"
    )]
    fn null_pointing_mismatched_lengths() {
        // Offset buffer length - 1 != null buffer length -> panic
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 5, 8]));
        let nulls = NullBuffer::from(vec![true, false]); // length 2, but offsets has 3 elements
        offsets.is_there_null_pointing_to_non_empty_value(Some(&nulls));
    }

    #[test]
    fn null_pointing_alternating_null_valid_all_empty_nulls() {
        // Alternating null/valid pattern, all null ranges are empty
        // offsets: [0, 0, 3, 3, 6, 6, 9]  nulls: [false, true, false, true, false, true]
        // Nulls at 0,2,4 all have empty ranges
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 0, 3, 3, 6, 6, 9]));
        let nulls = NullBuffer::from(vec![false, true, false, true, false, true]);
        assert!(!offsets.is_there_null_pointing_to_non_empty_value(Some(&nulls)));
    }

    #[test]
    fn null_pointing_only_trailing_null_has_data() {
        // Only the trailing null region has data, everything else is clean
        // offsets: [0, 0, 3, 6, 8]  nulls: [false, true, true, false]
        // Null at 0 (0..0 empty), valid at 1,2 (0..3, 3..6), null at 3 (6..8 non-empty)
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 0, 3, 6, 8]));
        let nulls = NullBuffer::from(vec![false, true, true, false]);
        assert!(offsets.is_there_null_pointing_to_non_empty_value(Some(&nulls)));
    }

    #[test]
    fn null_pointing_sliced_no_nulls_in_null_region() {
        // Original: [0, 3, 3, 6, 6, 9]  -> slice(1, 3) -> [3, 3, 6, 6]
        // initial_offset=3, last_offset=6
        // nulls: [false, true, false]  (null at index 0 has range 3..3 = empty)
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 3, 6, 6, 9]));
        let sliced = offsets.slice(1, 3);
        let nulls = NullBuffer::from(vec![false, true, false]);
        assert!(!sliced.is_there_null_pointing_to_non_empty_value(Some(&nulls)));
    }

    #[test]
    fn null_pointing_sliced_null_has_data() {
        // Original: [0, 3, 7, 10, 15]  -> slice(1, 2) -> [3, 7, 10]
        // initial_offset=3, last_offset=10
        // nulls: [false, true]  (null at index 0 has range 3..7 = non-empty)
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 7, 10, 15]));
        let sliced = offsets.slice(1, 2);
        let nulls = NullBuffer::from(vec![false, true]);
        assert!(sliced.is_there_null_pointing_to_non_empty_value(Some(&nulls)));
    }

    #[test]
    fn null_pointing_sliced_all_null_equal_offsets() {
        // Original: [0, 5, 5, 5, 10]  -> slice(1, 2) -> [5, 5, 5]
        // All null, initial_offset=5, last_offset=5 -> false
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 5, 5, 5, 10]));
        let sliced = offsets.slice(1, 2);
        let nulls = NullBuffer::new_null(2);
        assert!(!sliced.is_there_null_pointing_to_non_empty_value(Some(&nulls)));
    }

    #[test]
    fn null_pointing_sliced_all_null_different_offsets() {
        // Original: [0, 3, 5, 8, 12]  -> slice(1, 2) -> [3, 5, 8]
        // All null, initial_offset=3, last_offset=8, 3 != 8 -> true
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 5, 8, 12]));
        let sliced = offsets.slice(1, 2);
        let nulls = NullBuffer::new_null(2);
        assert!(sliced.is_there_null_pointing_to_non_empty_value(Some(&nulls)));
    }

    #[test]
    fn null_pointing_sliced_leading_null_with_data() {
        // Original: [0, 2, 5, 8, 10]  -> slice(1, 3) -> [2, 5, 8, 10]
        // nulls: [false, true, true]
        // Null at index 0: range 2..5 (non-empty), initial_offset=2, self[start=1]=5 != 2 -> true
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 2, 5, 8, 10]));
        let sliced = offsets.slice(1, 3);
        let nulls = NullBuffer::from(vec![false, true, true]);
        assert!(sliced.is_there_null_pointing_to_non_empty_value(Some(&nulls)));
    }

    #[test]
    fn null_pointing_sliced_trailing_null_with_data() {
        // Original: [0, 3, 6, 9, 12]  -> slice(1, 2) -> [3, 6, 9]
        // nulls: [true, false]
        // Valid at 0: (3..6), null at 1: range 6..9 (non-empty)
        // end_offset_of_last_valid_value = self[1]=6, last_offset=9, 6 != 9 -> true
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 6, 9, 12]));
        let sliced = offsets.slice(1, 2);
        let nulls = NullBuffer::from(vec![true, false]);
        assert!(sliced.is_there_null_pointing_to_non_empty_value(Some(&nulls)));
    }

    #[test]
    fn null_pointing_sliced_trailing_null_without_data() {
        // Original: [0, 3, 6, 6, 10]  -> slice(1, 2) -> [3, 6, 6]
        // nulls: [true, false]
        // Valid at 0: (3..6), null at 1: range 6..6 (empty)
        // end_offset_of_last_valid_value = self[1]=6, last_offset=6 -> false
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 6, 6, 10]));
        let sliced = offsets.slice(1, 2);
        let nulls = NullBuffer::from(vec![true, false]);
        assert!(!sliced.is_there_null_pointing_to_non_empty_value(Some(&nulls)));
    }

    #[test]
    fn null_pointing_sliced_middle_null_with_data() {
        // Original: [0, 2, 5, 9, 12, 15]  -> slice(1, 3) -> [2, 5, 9, 12]
        // nulls: [true, false, true]
        // valid_slices: (0,1), (2,3)
        // first: start=0 -> self[0]=2 == initial_offset=2 OK, end_offset=self[1]=5
        // second: start=2 -> self[2]=9 != 5 -> true
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 2, 5, 9, 12, 15]));
        let sliced = offsets.slice(1, 3);
        let nulls = NullBuffer::from(vec![true, false, true]);
        assert!(sliced.is_there_null_pointing_to_non_empty_value(Some(&nulls)));
    }

    #[test]
    fn null_pointing_sliced_middle_null_without_data() {
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 2, 5, 5, 8, 12]));
        let sliced = offsets.slice(1, 3);
        let nulls = NullBuffer::from(vec![true, false, true]);
        assert!(!sliced.is_there_null_pointing_to_non_empty_value(Some(&nulls)));
    }

    #[test]
    fn null_pointing_sliced_single_element_null_empty() {
        // Original: [0, 3, 3, 6]  -> slice(1, 1) -> [3, 3]
        // Single element, null, range 3..3 (empty) -> false
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 3, 6]));
        let sliced = offsets.slice(1, 1);
        let nulls = NullBuffer::new_null(1);
        assert!(!sliced.is_there_null_pointing_to_non_empty_value(Some(&nulls)));
    }

    #[test]
    fn null_pointing_sliced_single_element_null_non_empty() {
        // Original: [0, 3, 7, 10]  -> slice(1, 1) -> [3, 7]
        // Single element, null, range 3..7 (non-empty) -> true
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 7, 10]));
        let sliced = offsets.slice(1, 1);
        let nulls = NullBuffer::new_null(1);
        assert!(sliced.is_there_null_pointing_to_non_empty_value(Some(&nulls)));
    }

    #[test]
    fn null_pointing_sliced_from_start() {
        // Slice from the beginning (offset=0) -- offsets stay the same
        // Original: [0, 3, 3, 6]  -> slice(0, 2) -> [0, 3, 3]
        // nulls: [true, false]  (null at 1 has range 3..3 = empty) -> false
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 3, 6]));
        let sliced = offsets.slice(0, 2);
        let nulls = NullBuffer::from(vec![true, false]);
        assert!(!sliced.is_there_null_pointing_to_non_empty_value(Some(&nulls)));
    }

    #[test]
    fn null_pointing_sliced_to_end() {
        // Slice to the end
        // Original: [0, 3, 6, 9, 12]  -> slice(2, 2) -> [6, 9, 12]
        // nulls: [false, true]  (null at 0 has range 6..9 = non-empty) -> true
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 6, 9, 12]));
        let sliced = offsets.slice(2, 2);
        let nulls = NullBuffer::from(vec![false, true]);
        assert!(sliced.is_there_null_pointing_to_non_empty_value(Some(&nulls)));
    }

    #[test]
    fn null_pointing_sliced_all_valid_no_nulls() {
        // Sliced buffer with all-valid nulls -> false (early return)
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 6, 9, 12]));
        let sliced = offsets.slice(1, 2);
        let nulls = NullBuffer::new_valid(2);
        assert!(!sliced.is_there_null_pointing_to_non_empty_value(Some(&nulls)));
    }

    #[test]
    #[should_panic(
        expected = "The length of the offsets should be 1 more than the length of the null buffer"
    )]
    fn null_pointing_mismatched_lengths_null_buffer_too_long() {
        // Null buffer longer than offsets - 1 -> panic
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 5]));
        // offsets has 3 elements -> expects null buffer of length 2
        let nulls = NullBuffer::from(vec![false, true, false]);
        offsets.is_there_null_pointing_to_non_empty_value(Some(&nulls));
    }

    #[test]
    #[should_panic(
        expected = "The length of the offsets should be 1 more than the length of the null buffer"
    )]
    fn null_pointing_sliced_mismatched_lengths() {
        // Sliced offset buffer with wrong null buffer length -> panic
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 6, 9, 12]));
        let sliced = offsets.slice(1, 2); // [3, 6, 9] -> expects null buffer of length 2
        let nulls = NullBuffer::from(vec![false, true, false]); // length 3
        sliced.is_there_null_pointing_to_non_empty_value(Some(&nulls));
    }

    #[test]
    #[should_panic(
        expected = "The length of the offsets should be 1 more than the length of the null buffer"
    )]
    fn null_pointing_all_valid_mismatched_lengths_too_short() {
        // All-valid null buffer with wrong length should still panic
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 5, 8]));
        let nulls = NullBuffer::new_valid(2); // expects 3
        offsets.is_there_null_pointing_to_non_empty_value(Some(&nulls));
    }

    #[test]
    #[should_panic(
        expected = "The length of the offsets should be 1 more than the length of the null buffer"
    )]
    fn null_pointing_all_valid_mismatched_lengths_too_long() {
        // All-valid null buffer with wrong length should still panic
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 5, 8]));
        let nulls = NullBuffer::new_valid(5); // expects 3
        offsets.is_there_null_pointing_to_non_empty_value(Some(&nulls));
    }
}
