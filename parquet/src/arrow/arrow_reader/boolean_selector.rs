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
#[derive(Debug, Clone, PartialEq)]
pub struct BooleanRowSelection {
    selector: BooleanArray,
}

impl BooleanRowSelection {
    /// Create a new [`BooleanRowSelection] from a list of [`BooleanArray`].
    pub fn from_filters(filters: &[BooleanArray]) -> Self {
        let arrays: Vec<&dyn Array> = filters.iter().map(|x| x as &dyn Array).collect();
        let result = arrow_select::concat::concat(&arrays).unwrap().into_data();
        let boolean_array = BooleanArray::from(result);
        BooleanRowSelection {
            selector: boolean_array,
        }
    }

    /// Create a new [`BooleanRowSelection`] with all rows unselected
    pub fn new_unselected(row_count: usize) -> Self {
        let buffer = BooleanBuffer::new_unset(row_count);
        let boolean_array = BooleanArray::from(buffer);
        BooleanRowSelection { selector: boolean_array }
    }

    /// Create a new [`BooleanRowSelection`] with all rows selected
    pub fn new_selected(row_count: usize) -> Self {
        let buffer = BooleanBuffer::new_set(row_count);
        let boolean_array = BooleanArray::from(buffer);
        BooleanRowSelection { selector: boolean_array }
    }

    /// Returns a new [`BooleanRowSelection`] that selects the inverse of this [`BooleanRowSelection`].
    pub fn as_inverted(&self) -> Self {
        let buffer = !self.selector.values();
        BooleanRowSelection { selector: BooleanArray::from(buffer) }
    }

    /// Returns the number of rows selected by this [`BooleanRowSelection`].
    pub fn row_count(&self) -> usize {
        self.selector.true_count()
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
            selector: BooleanArray::from(buffer.finish()),
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

        let union_selectors = self.selector.values() | other.selector.values();

        BooleanRowSelection {
            selector: BooleanArray::from(union_selectors),
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
        let intersection_selectors = self.selector.values() & other.selector.values();

        BooleanRowSelection {
            selector: BooleanArray::from(intersection_selectors),
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

        let mut buffer = MutableBuffer::from_len_zeroed(self.selector.values().inner().len());
        buffer.copy_from_slice(self.selector.values().inner().as_slice());
        let mut builder = BooleanBufferBuilder::new_from_buffer(buffer, self.selector.len());

        // Create iterators for 'self' and 'other' bits
        let mut other_bits = other.selector.iter();

        for bit_idx in self.true_iter() {
            let predicate = other_bits
                .next()
                .expect("Mismatch in set bits between self and other");
            if !predicate.unwrap() {
                builder.set_bit(bit_idx, false);
            }
        }

        BooleanRowSelection {
            selector: BooleanArray::from(builder.finish()),
        }
    }

    /// Returns an iterator over the indices of the set bits in this [`BooleanRowSelection`]
    pub fn true_iter(&self) -> BitIndexIterator<'_> {
        self.selector.values().set_indices()
    }

    /// Returns `true` if this [`BooleanRowSelection`] selects any rows
    pub fn selects_any(&self) -> bool {
        self.true_iter().next().is_some()
    }

    /// Returns a new [`BooleanRowSelection`] that selects the rows in this [`BooleanRowSelection`] from `offset` to `offset + len`
    pub fn slice(&self, offset: usize, len: usize) -> BooleanArray {
        self.selector.slice(offset, len)
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
            selector: BooleanArray::from(builder.finish()),
        }
    }
}

impl From<&BooleanRowSelection> for RowSelection{
    fn from(selection: &BooleanRowSelection) -> Self {
        RowSelection::from_filters(&[selection.selector.clone()])
    }
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
}