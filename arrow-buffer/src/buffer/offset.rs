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
    /// assert!(!offsets.has_non_empty_nulls(Some(&nulls)));
    ///
    /// // Offsets where null at index 1 has a non-empty range (3..7)
    /// let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 7, 10]));
    /// let nulls = NullBuffer::from(vec![true, false, true]);
    /// assert!(offsets.has_non_empty_nulls(Some(&nulls)));
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if the length of the `null_buffer` does not equal `self.len() - 1`.
    pub fn has_non_empty_nulls(&self, null_buffer: Option<&NullBuffer>) -> bool {
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

    /// Subtract `rhs` from all offsets
    /// This will try to reuse the existing allocation as much as possible
    ///
    /// Panics: this will panic if `rhs` > the first offset or if `rhs` will lead to overflow (when `rhs` is negative)
    pub fn subtract(self, rhs: O) -> Self where O: std::ops::Sub<Output = O> + std::cmp::PartialOrd + num_traits::CheckedSub {
        if rhs == O::usize_as(0) {
            return self;
        }

        let len = self.len();

        // Offset buffer is guaranteed to be non-empty
        assert!(self[0] >= rhs, "shifted offsets will become negative which is not allowed");

        // If negative, make sure that this will not create an overflow
        if rhs < O::usize_as(0) {
            self[len - 1].checked_sub(&rhs).expect("must not overflow");
        }

        let output_buffer = match self.into_inner().into_inner().into_mutable() {
            Ok(mut mutable) => {
                // TODO - add test when the offsets are sliced and the first offset outside the slice is 0 and we shift by > 0
                mutable.typed_data_mut::<O>()
                  .iter_mut()
                  .for_each(|offset| *offset = *offset - rhs);

                mutable
            }
            Err(original_buffer) => {
                let mut output_buffer = MutableBuffer::new(len * size_of::<O>());

                let underlying_buffer = original_buffer.typed_data::<O>();

                for i in 0..len {
                    unsafe {
                        // SAFETY: Already allocated sufficient capacity
                        output_buffer.push_unchecked(
                            // SAFETY:
                            // 1. `i` is within bounds
                            // 2. we will not cause underflow as we checked that the first offset is greater than or equal to the shift
                            *underlying_buffer.get_unchecked(i) - rhs,
                        )
                    }
                }

                output_buffer
            }
        };

        let output_buffer = ScalarBuffer::<O>::from(output_buffer);

        // This is safe as we keep the following properties:
        // 1. buffer is non-empty - the output buffer is derived from a valid offset buffer
        // 2. values are greater than or equal to zero - we validated before that the first offset is greater than or equal to the shift
        //    and the first buffer value is coming from a valid offset buffer,
        //    and the input values are monotonically increasing since they are coming from a valid offset buffer so checking the first offset is enough
        // 3. monotonically increasing values - we subtract from all offset the same value, thus keeping the same property as the input buffer which is a valid offset buffer
        unsafe { OffsetBuffer::new_unchecked(output_buffer) }
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
    // Tests for has_non_empty_nulls
    // ---------------------------------------------------------------

    #[test]
    fn has_non_empty_nulls_none_null_buffer() {
        // No null buffer at all -> false
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 5, 8]));
        assert!(!offsets.has_non_empty_nulls(None));
    }

    #[test]
    fn has_non_empty_nulls_all_valid() {
        // Null buffer with zero nulls -> false (early return via filter)
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 5, 8]));
        let nulls = NullBuffer::new_valid(3);
        assert!(!offsets.has_non_empty_nulls(Some(&nulls)));
    }

    #[test]
    fn has_non_empty_nulls_all_null_empty_offsets() {
        // All values are null and all offsets are equal (no data behind nulls) -> false
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 0, 0, 0]));
        let nulls = NullBuffer::new_null(3);
        assert!(!offsets.has_non_empty_nulls(Some(&nulls)));
    }

    #[test]
    fn has_non_empty_nulls_all_null_non_empty_offsets() {
        // All values are null but offsets span data -> true
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 2, 5, 7]));
        let nulls = NullBuffer::new_null(3);
        assert!(offsets.has_non_empty_nulls(Some(&nulls)));
    }

    #[test]
    fn has_non_empty_nulls_all_null_nonzero_but_equal_offsets() {
        // All null, offsets start at non-zero but are all equal -> false
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![5, 5, 5]));
        let nulls = NullBuffer::new_null(2);
        assert!(!offsets.has_non_empty_nulls(Some(&nulls)));
    }

    #[test]
    fn has_non_empty_nulls_leading_nulls_with_data() {
        // Nulls at the beginning that point to non-empty ranges -> true
        // offsets: [0, 3, 5, 8]  nulls: [false, true, true]
        // Index 0 is null with range 0..3 (non-empty)
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 5, 8]));
        let nulls = NullBuffer::from(vec![false, true, true]);
        assert!(offsets.has_non_empty_nulls(Some(&nulls)));
    }

    #[test]
    fn has_non_empty_nulls_leading_nulls_without_data() {
        // Nulls at the beginning with empty ranges -> continue checking
        // offsets: [0, 0, 3, 6]  nulls: [false, true, true]
        // Index 0 is null with range 0..0 (empty)
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 0, 3, 6]));
        let nulls = NullBuffer::from(vec![false, true, true]);
        assert!(!offsets.has_non_empty_nulls(Some(&nulls)));
    }

    #[test]
    fn has_non_empty_nulls_only_trailing_null_has_data() {
        // Only the trailing null region has data, everything else is clean
        // offsets: [0, 0, 3, 6, 8]  nulls: [false, true, true, false]
        // Null at 0 (0..0 empty), valid at 1,2 (0..3, 3..6), null at 3 (6..8 non-empty)
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 0, 3, 6, 8]));
        let nulls = NullBuffer::from(vec![false, true, true, false]);
        assert!(offsets.has_non_empty_nulls(Some(&nulls)));
    }

    #[test]
    fn has_non_empty_nulls_trailing_nulls_without_data() {
        // Nulls at the end with empty ranges -> false
        // offsets: [0, 3, 6, 6]  nulls: [true, true, false]
        // Index 2 is null with range 6..6 (empty)
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 6, 6]));
        let nulls = NullBuffer::from(vec![true, true, false]);
        assert!(!offsets.has_non_empty_nulls(Some(&nulls)));
    }

    #[test]
    fn has_non_empty_nulls_middle_nulls_with_data() {
        // Null in the middle with non-empty range -> true
        // offsets: [0, 3, 7, 10]  nulls: [true, false, true]
        // Index 1 is null with range 3..7 (non-empty)
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 7, 10]));
        let nulls = NullBuffer::from(vec![true, false, true]);
        assert!(offsets.has_non_empty_nulls(Some(&nulls)));
    }

    #[test]
    fn has_non_empty_nulls_middle_nulls_without_data() {
        // Null in the middle with empty range -> false
        // offsets: [0, 3, 3, 6]  nulls: [true, false, true]
        // Index 1 is null with range 3..3 (empty)
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 3, 6]));
        let nulls = NullBuffer::from(vec![true, false, true]);
        assert!(!offsets.has_non_empty_nulls(Some(&nulls)));
    }

    #[test]
    fn has_non_empty_nulls_alternating_null_valid_all_empty() {
        // Alternating null/valid where every null has an empty range -> false.

        // Ends with null
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 0, 3, 3, 6, 6]));
        let nulls = NullBuffer::from(vec![false, true, false, true, false]);
        assert!(!offsets.has_non_empty_nulls(Some(&nulls)));

        // Ends with valid
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 0, 3, 3, 6, 6, 9]));
        let nulls = NullBuffer::from(vec![false, true, false, true, false, true]);
        assert!(!offsets.has_non_empty_nulls(Some(&nulls)));
    }

    #[test]
    fn has_non_empty_nulls_multiple_null_regions_second_has_data() {
        // Two null regions: first empty, second non-empty -> true
        // offsets: [0, 0, 3, 5, 6]  nulls: [false, true, false, true]
        // Null at index 0 (0..0 empty), null at index 2 (3..5 non-empty)
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 0, 3, 5, 6]));
        let nulls = NullBuffer::from(vec![false, true, false, true]);
        assert!(offsets.has_non_empty_nulls(Some(&nulls)));
    }

    #[test]
    fn has_non_empty_nulls_multiple_null_regions_later_gap_has_data() {
        // Three null regions: first two empty, third non-empty -> true
        // offsets: [0, 0, 3, 3, 6, 8, 10]  nulls: [false, true, false, true, false, true]
        // valid_slices: (1,2), (3,4), (5,6)
        // first slice: start=1, self[1]=0 == initial_offset=0 OK, end_offset=self[2]=3
        // loop iter 1: start=3, self[3]=3 == 3 OK (first gap empty), end_offset=self[4]=6
        // loop iter 2: start=5, self[5]=8 != 6 -> true (second gap has data)
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 0, 3, 3, 6, 8, 10]));
        let nulls = NullBuffer::from(vec![false, true, false, true, false, true]);
        assert!(offsets.has_non_empty_nulls(Some(&nulls)));
    }

    #[test]
    fn has_non_empty_nulls_single_element_null_empty() {
        // Single element, null with empty range -> false
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 0]));
        let nulls = NullBuffer::new_null(1);
        assert!(!offsets.has_non_empty_nulls(Some(&nulls)));
    }

    #[test]
    fn has_non_empty_nulls_single_element_null_non_empty() {
        // Single element, null with non-empty range -> true
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 5]));
        let nulls = NullBuffer::new_null(1);
        assert!(offsets.has_non_empty_nulls(Some(&nulls)));
    }

    #[test]
    fn has_non_empty_nulls_single_element_valid() {
        // Single element, valid -> false (no nulls at all)
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 5]));
        let nulls = NullBuffer::new_valid(1);
        assert!(!offsets.has_non_empty_nulls(Some(&nulls)));
    }

    #[test]
    fn has_non_empty_nulls_consecutive_nulls_between_valid_slices() {
        // Multiple consecutive nulls between valid regions
        // offsets: [0, 2, 2, 2, 5, 8]  nulls: [true, false, false, true, true]
        // Valid: [0], nulls: [1,2], valid: [3,4]
        // Null region [1,2] has offsets 2..2..2 (empty) -> false
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 2, 2, 2, 5, 8]));
        let nulls = NullBuffer::from(vec![true, false, false, true, true]);
        assert!(!offsets.has_non_empty_nulls(Some(&nulls)));
    }

    #[test]
    fn has_non_empty_nulls_consecutive_nulls_between_valid_slices_with_data() {
        // Multiple consecutive nulls between valid regions, nulls have data
        // offsets: [0, 2, 3, 4, 5, 8]  nulls: [true, false, false, true, true]
        // valid_slices: (0,1), (3,5)
        // first slice: start=0, end=1 -> self[0]=0 == initial_offset=0 OK
        //   end_offset_of_last_valid_value = self[1] = 2
        // second slice: start=3, end=5 -> self[3]=4 != 2 -> true
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 2, 3, 4, 5, 8]));
        let nulls = NullBuffer::from(vec![true, false, false, true, true]);
        assert!(offsets.has_non_empty_nulls(Some(&nulls)));
    }

    #[test]
    fn has_non_empty_nulls_nonzero_initial_offset_all_null_equal() {
        // Non-zero starting offset, all null, all offsets equal -> false
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![10, 10, 10]));
        let nulls = NullBuffer::new_null(2);
        assert!(!offsets.has_non_empty_nulls(Some(&nulls)));
    }

    #[test]
    fn has_non_empty_nulls_nonzero_initial_offset_with_data() {
        // Non-zero starting offset, null has data
        // offsets: [10, 15, 20]  nulls: [false, true]
        // Null at index 0 with range 10..15 (non-empty) -> true
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![10, 15, 20]));
        let nulls = NullBuffer::from(vec![false, true]);
        assert!(offsets.has_non_empty_nulls(Some(&nulls)));
    }

    #[test]
    fn has_non_empty_nulls_sliced_no_nulls_in_null_region() {
        // Original: [0, 3, 3, 6, 6, 9]  -> slice(1, 3) -> [3, 3, 6, 6]
        // initial_offset=3, last_offset=6
        // nulls: [false, true, false]  (null at index 0 has range 3..3 = empty)
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 3, 6, 6, 9]));
        let sliced = offsets.slice(1, 3);
        let nulls = NullBuffer::from(vec![false, true, false]);
        assert!(!sliced.has_non_empty_nulls(Some(&nulls)));
    }

    #[test]
    fn has_non_empty_nulls_sliced_null_has_data() {
        // Original: [0, 3, 7, 10, 15]  -> slice(1, 2) -> [3, 7, 10]
        // initial_offset=3, last_offset=10
        // nulls: [false, true]  (null at index 0 has range 3..7 = non-empty)
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 7, 10, 15]));
        let sliced = offsets.slice(1, 2);
        let nulls = NullBuffer::from(vec![false, true]);
        assert!(sliced.has_non_empty_nulls(Some(&nulls)));
    }

    #[test]
    #[should_panic(
        expected = "The length of the offsets should be 1 more than the length of the null buffer"
    )]
    fn has_non_empty_nulls_all_valid_mismatched_lengths_too_short() {
        // All-valid null buffer with wrong length should still panic
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 5, 8]));
        let nulls = NullBuffer::new_valid(2); // expects 3
        offsets.has_non_empty_nulls(Some(&nulls));
    }

    #[test]
    #[should_panic(
        expected = "The length of the offsets should be 1 more than the length of the null buffer"
    )]
    fn has_non_empty_nulls_all_valid_mismatched_lengths_too_long() {
        // All-valid null buffer with wrong length should still panic
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 5, 8]));
        let nulls = NullBuffer::new_valid(5); // expects 3
        offsets.has_non_empty_nulls(Some(&nulls));
    }

    #[test]
    #[should_panic(expected = "shifted offsets will become negative which is not allowed")]
    fn should_panic_for_subtract_by_value_that_will_cause_offsets_to_be_less_than_zero() {
        // self[0] = 0, rhs = 1 -> 0 >= 1 is false -> assert fires
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 6]));
        offsets.subtract(1);
    }

    #[test]
    fn subtract_by_value_that_will_cause_offsets_to_be_less_than_zero_for_outside_the_slice() {
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 1, 4, 7]));
        let sliced = offsets.slice(1, 2); // [1, 4, 7]
        drop(offsets);
        assert_eq!(sliced.as_ref(), &[1, 4, 7]);

        let result = sliced.subtract(1);
        assert_eq!(result.as_ref(), &[0, 3, 6]);
        assert_eq!(result.len(), 3);
    }

    #[test]
    #[should_panic(expected = "must not overflow")]
    fn should_panic_subtract_by_value_that_will_cause_offsets_to_overflow() {
        // rhs = -1 (negative). self[0] = 0 >= -1 passes.
        // last offset i32::MAX - (-1) = i32::MAX + 1 -> checked_sub returns None -> expect fires
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 5, i32::MAX]));
        offsets.subtract(-1);
    }

    #[test]
    fn subtract_by_value_that_will_cause_offsets_to_overflow_outside_the_slice() {
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 6, i32::MAX]));
        let sliced = offsets.slice(0, 2); // [0, 3, 6]
        assert_eq!(sliced.as_ref(), &[0, 3, 6]);

        let result = sliced.subtract(-1);
        assert_eq!(result.as_ref(), &[1, 4, 7]);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn when_shift_is_0_subtract_should_reuse_the_buffer_even_when_it_is_shared() {
        // subtract(0) hits the early `return self` before any into_mutable,
        // so the returned buffer is the exact same allocation even while shared.
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 6]));
        let shared = offsets.clone(); // refcount now 2 -> shared
        let result = offsets.subtract(0);
        assert!(
            result.ptr_eq(&shared),
            "subtract(0) must return the same underlying buffer, even when shared"
        );
    }

    #[test]
    fn should_reuse_the_underline_data_when_the_buffer_is_not_shared() {
        // Unique ownership, offset 0 -> into_mutable succeeds -> mutate in place,
        // and MutableBuffer -> Buffer keeps the same allocation.
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![2, 5, 8]));
        let ptr_before = offsets.as_ptr();
        let result = offsets.subtract(2);
        assert_eq!(
            ptr_before,
            result.as_ptr(),
            "a non-shared buffer should be mutated in place, reusing the allocation"
        );
        assert_eq!(result.as_ref(), &[0, 3, 6]);
    }

    #[test]
    fn should_create_a_new_buffer_when_the_buffer_is_shared() {
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![2, 5, 8]));
        let shared = offsets.clone();
        let ptr_before = offsets.as_ptr();
        let result = offsets.subtract(2);
        assert_ne!(
            ptr_before,
            result.as_ptr(),
            "a shared buffer must not be mutated in place; a new allocation is created"
        );
        assert_eq!(result.as_ref(), &[0, 3, 6]);
        // The shared view is untouched.
        assert_eq!(shared.as_ref(), &[2, 5, 8]);
    }

    #[test]
    fn when_shift_is_negative_it_should_shift_offsets_in_the_right_direction() {
        // rhs = -2 -> offset - (-2) = offset + 2, so all offsets move up.
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 6]));
        let result = offsets.subtract(-2);
        assert_eq!(result.as_ref(), &[2, 5, 8]);
    }

    #[test]
    fn for_sliced_unshared_buffer_shift_should_reuse_buffer_but_only_modify_the_data_in_slice() {
        // Underlying [0, 3, 6, 9, 12]; slice -> view [3, 6, 9].
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![1, 3, 6, 9, 12]));
        let sliced = offsets.slice(1, 2); // [3, 6, 9]
        drop(offsets); // uniquely owned -> eligible for in-place reuse
        assert_eq!(sliced.as_ref(), &[3, 6, 9]);

        let ptr_before = sliced.as_ptr();
        let result = sliced.subtract(1);

        // Reuses the allocation...
        assert_eq!(
            ptr_before,
            result.as_ptr(),
            "uniquely-owned sliced buffer should be mutated in place"
        );
        // ...but only the slice is shifted, and the length stays the slice length.
        assert_eq!(result.as_ref(), &[2, 5, 8]);
        assert_eq!(result.len(), 3);
        let underlying_buffer = result.inner().inner();
        // data should be shifted by 1
        assert_eq!(underlying_buffer.ptr_offset(), std::mem::size_of::<i32>());
        let original_value_ptr = unsafe {underlying_buffer.data_ptr().as_ptr() as *mut i32};
        let value_in_ptr = unsafe {*original_value_ptr};
        assert_eq!(value_in_ptr, 1);
        let last_value_ptr = unsafe {(underlying_buffer.data_ptr().as_ptr() as *mut i32).add(5)};
        assert_eq!(unsafe {*last_value_ptr}, 12);
    }

    #[test]
    fn for_sliced_shared_buffer_shifted_buffer_should_only_include_the_sliced_data() {
        // Underlying: [0, 3, 6, 9, 12]; slice(1, 2) -> view [3, 6, 9].
        // `offsets` stays alive, so the sliced buffer is shared -> Err branch.
        // The Err branch copies `len` (= 3) elements from the *sliced* typed_data,
        // so the result contains only the sliced data, shifted.
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 6, 9, 12]));
        let sliced = offsets.slice(1, 2);
        assert_eq!(sliced.as_ref(), &[3, 6, 9]);

        let result = sliced.subtract(3);

        assert_eq!(
            result.as_ref(),
            &[0, 3, 6],
            "shifted result should contain only the sliced data"
        );
        assert_eq!(result.len(), 3);

        // Assert that the underlying buffer of the result is not sliced to make sure it does not include the data outside the slice range from the original buffer
        let underlying_buffer = result.inner().inner();
        assert_eq!(underlying_buffer.ptr_offset(), 0);
        assert_eq!(underlying_buffer.len(), 3 * std::mem::size_of::<i32>());
    }
}
