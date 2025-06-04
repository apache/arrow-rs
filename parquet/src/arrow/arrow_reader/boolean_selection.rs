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

use std::ops::Range;

use arrow_array::{Array, BooleanArray};
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder, MutableBuffer};
use arrow_data::bit_iterator::BitIndexIterator;

use super::{RowSelection, RowSelector};

/// A selection of rows, similar to [`RowSelection`], but based on a boolean array
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BooleanRowSelection {
    selector: BooleanBuffer,
}

impl BooleanRowSelection {
    /// Create a new [`BooleanRowSelection] from a list of [`BooleanArray`].
    pub fn from_filters(filters: &[BooleanArray]) -> Self {
        let arrays: Vec<&dyn Array> = filters.iter().map(|x| x as &dyn Array).collect();
        let result = arrow_select::concat::concat(&arrays).unwrap().into_data();
        let (boolean_array, _null) = BooleanArray::from(result).into_parts();
        BooleanRowSelection {
            selector: boolean_array,
        }
    }

    /// Create a new [`BooleanRowSelection`] with all rows unselected
    pub fn new_unselected(row_count: usize) -> Self {
        let buffer = BooleanBuffer::new_unset(row_count);

        BooleanRowSelection { selector: buffer }
    }

    /// Create a new [`BooleanRowSelection`] with all rows selected
    pub fn new_selected(row_count: usize) -> Self {
        let buffer = BooleanBuffer::new_set(row_count);

        BooleanRowSelection { selector: buffer }
    }

    /// Returns a new [`BooleanRowSelection`] that selects the inverse of this [`BooleanRowSelection`].
    pub fn as_inverted(&self) -> Self {
        let buffer = !&self.selector;
        BooleanRowSelection { selector: buffer }
    }

    /// Returns the number of rows selected by this [`BooleanRowSelection`].
    pub fn row_count(&self) -> usize {
        self.selector.count_set_bits()
    }

    /// Create a new [`BooleanRowSelection`] from a list of consecutive ranges.
    pub fn from_consecutive_ranges(
        ranges: impl Iterator<Item = Range<usize>>,
        total_rows: usize,
    ) -> Self {
        let mut buffer = BooleanBufferBuilder::new(total_rows);
        let mut last_end = 0;

        for range in ranges {
            let len = range.end - range.start;
            if len == 0 {
                continue;
            }

            if range.start > last_end {
                buffer.append_n(range.start - last_end, false);
            }
            buffer.append_n(len, true);
            last_end = range.end;
        }

        if last_end != total_rows {
            buffer.append_n(total_rows - last_end, false);
        }

        BooleanRowSelection {
            selector: buffer.finish(),
        }
    }

    /// Compute the union of two [`BooleanRowSelection`]
    /// For example:
    /// self:      NNYYYYNNYYNYN
    /// other:     NYNNNNNNN
    ///
    /// returned:  NYYYYYNNYYNYN
    #[must_use]
    pub fn union(&self, other: &Self) -> Self {
        // use arrow::compute::kernels::boolean::or;

        let union_selectors = &self.selector | &other.selector;

        BooleanRowSelection {
            selector: union_selectors,
        }
    }

    /// Compute the intersection of two [`BooleanRowSelection`]
    /// For example:
    /// self:      NNYYYYNNYYNYN
    /// other:     NYNNNNNNY
    ///
    /// returned:  NNNNNNNNYYNYN
    #[must_use]
    pub fn intersection(&self, other: &Self) -> Self {
        let intersection_selectors = &self.selector & &other.selector;

        BooleanRowSelection {
            selector: intersection_selectors,
        }
    }

    /// Combines this [`BooleanRowSelection`] with another using logical AND on the selected bits.
    ///
    /// Unlike intersection, the `other` [`BooleanRowSelection`] must have exactly as many set bits as `self`.
    /// This method will keep only the bits in `self` that are also set in `other`
    /// at the positions corresponding to `self`'s set bits.
    pub fn and_then(&self, other: &Self) -> Self {
        // Ensure that 'other' has exactly as many set bits as 'self'
        debug_assert_eq!(
            self.row_count(),
            other.selector.len(),
            "The 'other' selection must have exactly as many set bits as 'self'."
        );

        if self.selector.len() == other.selector.len() {
            // fast path if the two selections are the same length
            // common if this is the first predicate
            debug_assert_eq!(self.row_count(), self.selector.len());
            return self.intersection(other);
        }

        let mut buffer = MutableBuffer::from_len_zeroed(self.selector.inner().len());
        buffer.copy_from_slice(self.selector.inner().as_slice());
        let mut builder = BooleanBufferBuilder::new_from_buffer(buffer, self.selector.len());

        // Create iterators for 'self' and 'other' bits
        let mut other_bits = other.selector.iter();

        for bit_idx in self.true_iter() {
            let predicate = other_bits
                .next()
                .expect("Mismatch in set bits between self and other");
            if !predicate {
                builder.set_bit(bit_idx, false);
            }
        }

        BooleanRowSelection {
            selector: builder.finish(),
        }
    }

    /// Returns an iterator over the indices of the set bits in this [`BooleanRowSelection`]
    pub fn true_iter(&self) -> BitIndexIterator<'_> {
        self.selector.set_indices()
    }

    /// Returns `true` if this [`BooleanRowSelection`] selects any rows
    pub fn selects_any(&self) -> bool {
        self.true_iter().next().is_some()
    }

    /// Returns a new [`BooleanRowSelection`] that selects the rows in this [`BooleanRowSelection`] from `offset` to `offset + len`
    pub fn slice(&self, offset: usize, len: usize) -> BooleanArray {
        BooleanArray::new(self.selector.slice(offset, len), None)
    }
}

impl From<Vec<RowSelector>> for BooleanRowSelection {
    fn from(selection: Vec<RowSelector>) -> Self {
        let selection = RowSelection::from(selection);
        RowSelection::into(selection)
    }
}

impl From<RowSelection> for BooleanRowSelection {
    fn from(selection: RowSelection) -> Self {
        let total_rows = selection.row_count();
        let mut builder = BooleanBufferBuilder::new(total_rows);

        for selector in selection.iter() {
            if selector.skip {
                builder.append_n(selector.row_count, false);
            } else {
                builder.append_n(selector.row_count, true);
            }
        }

        BooleanRowSelection {
            selector: builder.finish(),
        }
    }
}

impl From<&BooleanRowSelection> for RowSelection {
    fn from(selection: &BooleanRowSelection) -> Self {
        let array = BooleanArray::new(selection.selector.clone(), None);
        RowSelection::from_filters(&[array])
    }
}

/// Combines this [`BooleanBuffer`] with another using logical AND on the selected bits.
///
/// Unlike intersection, the `other` [`BooleanBuffer`] must have exactly as many **set bits** as `self`,
/// i.e., self.count_set_bits() == other.len().
///
/// This method will keep only the bits in `self` that are also set in `other`
/// at the positions corresponding to `self`'s set bits.
/// For example:
/// left:   NNYYYNNYYNYN
/// right:    YNY  NY N
/// result: NNYNYNNNYNNN
///
/// Optimized version of `boolean_buffer_and_then` using BMI2 PDEP instructions.
/// This function performs the same operation but uses bit manipulation instructions
/// for better performance on supported x86_64 CPUs.
pub fn boolean_buffer_and_then(left: &BooleanBuffer, right: &BooleanBuffer) -> BooleanBuffer {
    debug_assert_eq!(
        left.count_set_bits(),
        right.len(),
        "the right selection must have the same number of set bits as the left selection"
    );

    if left.len() == right.len() {
        debug_assert_eq!(left.count_set_bits(), left.len());
        return right.clone();
    }

    // Fast path for BMI2 support
    if cfg!(target_arch = "x86_64") && is_x86_feature_detected!("bmi2") {
        unsafe { boolean_buffer_and_then_bmi2(left, right) }
    } else {
        // Fallback to the original implementation
        boolean_buffer_and_then_fallback(left, right)
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "bmi2")]
unsafe fn boolean_buffer_and_then_bmi2(
    left: &BooleanBuffer,
    right: &BooleanBuffer,
) -> BooleanBuffer {
    use core::arch::x86_64::_pdep_u64;

    debug_assert_eq!(left.count_set_bits(), right.len());

    let bit_len = left.len();
    let byte_len = (bit_len + 7) / 8;
    let left_ptr = left.values().as_ptr();
    let right_ptr = right.values().as_ptr();

    let mut out = MutableBuffer::from_len_zeroed(byte_len);
    let out_ptr = out.as_mut_ptr();

    let full_words = byte_len / 8;
    let mut right_bit_idx = 0; // how many bits we have processed from right

    for word_idx in 0..full_words {
        let left_word =
            unsafe { core::ptr::read_unaligned(left_ptr.add(word_idx * 8) as *const u64) };

        if left_word == 0 {
            continue;
        }

        let need = left_word.count_ones();

        // Absolute byte & bit offset of the first needed bit inside `right`.
        let rb_byte = right_bit_idx / 8;
        let rb_bit = (right_bit_idx & 7) as u32;

        // We load two u64 words and shift/mask them to avoid branches and loops.
        let mut r_bits =
            unsafe { core::ptr::read_unaligned(right_ptr.add(rb_byte) as *const u64) } >> rb_bit;
        if rb_bit != 0 {
            let next =
                unsafe { core::ptr::read_unaligned(right_ptr.add(rb_byte + 8) as *const u64) };
            r_bits |= next << (64 - rb_bit);
        }

        // Mask off the high garbage if we asked for < 64 bits.
        r_bits &= 1u64.unbounded_shl(need).wrapping_sub(1);

        // The PDEP instruction: https://www.felixcloutier.com/x86/pdep
        // It takes left_word as the mask, and deposit the packed bits into the sparse positions of `left_word`.
        let result = _pdep_u64(r_bits, left_word);

        unsafe {
            core::ptr::write_unaligned(out_ptr.add(word_idx * 8) as *mut u64, result);
        }

        right_bit_idx += need as usize;
    }

    // Handle remaining bits that are less than 64 bits
    let tail_bits = bit_len & 63;
    if tail_bits != 0 {
        let mut mask = 0u64;
        for bit in 0..tail_bits {
            let byte = unsafe { *left_ptr.add(full_words * 8 + (bit / 8)) };
            mask |= (((byte >> (bit & 7)) & 1) as u64) << bit;
        }

        if mask != 0 {
            let need = mask.count_ones();

            let rb_byte = right_bit_idx / 8;
            let rb_bit = (right_bit_idx & 7) as u32;

            let mut r_bits =
                unsafe { core::ptr::read_unaligned(right_ptr.add(rb_byte) as *const u64) }
                    >> rb_bit;
            if rb_bit != 0 {
                let next =
                    unsafe { core::ptr::read_unaligned(right_ptr.add(rb_byte + 8) as *const u64) };
                r_bits |= next << (64 - rb_bit);
            }

            r_bits &= 1u64.unbounded_shl(need).wrapping_sub(1);

            let result = _pdep_u64(r_bits, mask);

            let tail_bytes = (tail_bits + 7) / 8;
            unsafe {
                core::ptr::copy_nonoverlapping(
                    &result.to_le_bytes()[0],
                    out_ptr.add(full_words * 8),
                    tail_bytes,
                );
            }
        }
    }

    BooleanBuffer::new(out.into(), 0, bit_len)
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use super::*;

    fn generate_random_row_selection(total_rows: usize, selection_ratio: f64) -> BooleanArray {
        let mut rng = rand::thread_rng();
        let bools: Vec<bool> = (0..total_rows)
            .map(|_| rng.gen_bool(selection_ratio))
            .collect();
        BooleanArray::from(bools)
    }

    #[test]
    fn test_boolean_row_selection_round_trip() {
        let total_rows = 1_000;
        for &selection_ratio in &[0.0, 0.1, 0.5, 0.9, 1.0] {
            let selection = generate_random_row_selection(total_rows, selection_ratio);
            let boolean_selection = BooleanRowSelection::from_filters(&[selection]);
            let row_selection = RowSelection::from(&boolean_selection);
            let boolean_selection_again = row_selection.into();
            assert_eq!(boolean_selection, boolean_selection_again);
        }
    }

    #[test]
    fn test_boolean_union_intersection() {
        let total_rows = 1_000;

        let base_boolean_selection =
            BooleanRowSelection::from_filters(&[generate_random_row_selection(total_rows, 0.1)]);
        let base_row_selection = RowSelection::from(&base_boolean_selection);
        for &selection_ratio in &[0.0, 0.1, 0.5, 0.9, 1.0] {
            let boolean_selection =
                BooleanRowSelection::from_filters(&[generate_random_row_selection(
                    total_rows,
                    selection_ratio,
                )]);
            let row_selection = RowSelection::from(&boolean_selection);

            let boolean_union = boolean_selection.union(&base_boolean_selection);
            let row_union = row_selection.union(&base_row_selection);
            assert_eq!(boolean_union, BooleanRowSelection::from(row_union));

            let boolean_intersection = boolean_selection.intersection(&base_boolean_selection);
            let row_intersection = row_selection.intersection(&base_row_selection);
            assert_eq!(
                boolean_intersection,
                BooleanRowSelection::from(row_intersection)
            );
        }
    }

    #[test]
    fn test_boolean_selection_and_then() {
        // Initial mask: 001011010101
        let self_filters = vec![BooleanArray::from(vec![
            false, false, true, false, true, true, false, true, false, true, false, true,
        ])];
        let self_selection = BooleanRowSelection::from_filters(&self_filters);

        // Predicate mask (only for selected bits): 001101
        let other_filters = vec![BooleanArray::from(vec![
            false, false, true, true, false, true,
        ])];
        let other_selection = BooleanRowSelection::from_filters(&other_filters);

        let result = self_selection.and_then(&other_selection);

        // Expected result: 000001010001
        let expected_filters = vec![BooleanArray::from(vec![
            false, false, false, false, false, true, false, true, false, false, false, true,
        ])];
        let expected_selection = BooleanRowSelection::from_filters(&expected_filters);

        assert_eq!(result, expected_selection);
    }

    #[test]
    #[should_panic(
        expected = "The 'other' selection must have exactly as many set bits as 'self'."
    )]
    fn test_and_then_mismatched_set_bits() {
        let self_filters = vec![BooleanArray::from(vec![true, true, false])];
        let self_selection = BooleanRowSelection::from_filters(&self_filters);

        // 'other' has only one set bit, but 'self' has two
        let other_filters = vec![BooleanArray::from(vec![true, false, false])];
        let other_selection = BooleanRowSelection::from_filters(&other_filters);

        // This should panic
        let _ = self_selection.and_then(&other_selection);
    }

    #[test]
    #[cfg(target_arch = "x86_64")]
    fn test_boolean_buffer_and_then_bmi2_large() {
        use super::boolean_buffer_and_then_bmi2;

        // Test with larger buffer (more than 64 bits)
        let size = 128;
        let mut left_builder = BooleanBufferBuilder::new(size);
        let mut right_bits = Vec::new();

        // Create a pattern where every 3rd bit is set in left
        for i in 0..size {
            let is_set = i % 3 == 0;
            left_builder.append(is_set);
            if is_set {
                // For right buffer, alternate between true/false
                right_bits.push(right_bits.len() % 2 == 0);
            }
        }
        let left = left_builder.finish();

        let mut right_builder = BooleanBufferBuilder::new(right_bits.len());
        for bit in right_bits {
            right_builder.append(bit);
        }
        let right = right_builder.finish();

        let result_bmi2 = unsafe { boolean_buffer_and_then_bmi2(&left, &right) };
        let result_orig = boolean_buffer_and_then_fallback(&left, &right);

        assert_eq!(result_bmi2.len(), result_orig.len());
        assert_eq!(result_bmi2.len(), size);

        // Verify they produce the same result
        for i in 0..size {
            assert_eq!(
                result_bmi2.value(i),
                result_orig.value(i),
                "Mismatch at position {}",
                i
            );
        }
    }

    #[test]
    #[cfg(target_arch = "x86_64")]
    fn test_boolean_buffer_and_then_bmi2_edge_cases() {
        use super::boolean_buffer_and_then_bmi2;

        // Test case: all bits set in left, alternating pattern in right
        let mut left_builder = BooleanBufferBuilder::new(16);
        for _ in 0..16 {
            left_builder.append(true);
        }
        let left = left_builder.finish();

        let mut right_builder = BooleanBufferBuilder::new(16);
        for i in 0..16 {
            right_builder.append(i % 2 == 0);
        }
        let right = right_builder.finish();

        let result_bmi2 = unsafe { boolean_buffer_and_then_bmi2(&left, &right) };
        let result_orig = boolean_buffer_and_then_fallback(&left, &right);

        assert_eq!(result_bmi2.len(), result_orig.len());
        for i in 0..16 {
            assert_eq!(
                result_bmi2.value(i),
                result_orig.value(i),
                "Mismatch at position {}",
                i
            );
            // Should be true for even indices, false for odd
            assert_eq!(result_bmi2.value(i), i % 2 == 0);
        }

        // Test case: no bits set in left
        let mut left_empty_builder = BooleanBufferBuilder::new(8);
        for _ in 0..8 {
            left_empty_builder.append(false);
        }
        let left_empty = left_empty_builder.finish();
        let right_empty = BooleanBufferBuilder::new(0).finish();

        let result_bmi2_empty = unsafe { boolean_buffer_and_then_bmi2(&left_empty, &right_empty) };
        let result_orig_empty = boolean_buffer_and_then_fallback(&left_empty, &right_empty);

        assert_eq!(result_bmi2_empty.len(), result_orig_empty.len());
        assert_eq!(result_bmi2_empty.len(), 8);
        for i in 0..8 {
            assert_eq!(result_bmi2_empty.value(i), false);
            assert_eq!(result_orig_empty.value(i), false);
        }
    }
}
