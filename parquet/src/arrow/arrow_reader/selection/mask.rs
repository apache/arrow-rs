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

use crate::arrow::arrow_reader::RowSelector;
use crate::arrow::arrow_reader::selection::and_then::boolean_buffer_and_then;
use crate::arrow::arrow_reader::selection::conversion::{
    boolean_array_to_row_selectors, row_selectors_to_boolean_buffer,
};
use arrow_array::{Array, BooleanArray};
use arrow_buffer::bit_iterator::BitIndexIterator;
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder};
use std::fmt::Debug;
use std::ops::Range;

/// [`RowSelection`] based on a [`BooleanArray`]
///
/// This represents the result of a filter evaluation as a bitmap. It is
/// more efficient for sparse filter results with many small skips or selections.
///
/// It is similar to the "Bitmap Index" described in [Predicate Caching:
/// Query-Driven Secondary Indexing for Cloud Data
/// Warehouses](https://dl.acm.org/doi/10.1145/3626246.3653395)
///
/// [`RowSelection`]: super::RowSelection
///
/// Based on code from Xiangpeng Hao in <https://github.com/apache/arrow-rs/pull/6624> and
/// [LiquidCache's implementation](https://github.com/XiangpengHao/liquid-cache/blob/60323cdb5f17f88af633ad9acbc7d74f684f9912/src/parquet/src/utils.rs#L17)
#[derive(Clone, Eq, PartialEq)]
pub(super) struct BitmaskSelection {
    pub(super) mask: BooleanBuffer,
}

/// Prints out the BitMaskSelection as boolean byts
impl Debug for BitmaskSelection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BitmaskSelection {{ len={}, mask: [", self.mask.len())?;
        // print each byte as binary digits
        for bit in self.mask.iter() {
            write!(f, "{}", if bit { "1" } else { "0" })?;
        }
        write!(f, "] }}")?;
        Ok(())
    }
}

impl BitmaskSelection {
    /// Create a new [`BooleanRowSelection] from a list of [`BooleanArray`].
    ///
    /// Note this ignores the null bits in the input arrays
    /// TODO: should it call prepnull_mask directly?
    pub fn from_filters(filters: &[BooleanArray]) -> Self {
        let arrays: Vec<&dyn Array> = filters.iter().map(|x| x as &dyn Array).collect();
        let result = arrow_select::concat::concat(&arrays).unwrap().into_data();
        let (boolean_array, _null) = BooleanArray::from(result).into_parts();
        BitmaskSelection {
            mask: boolean_array,
        }
    }

    pub fn from_row_selectors(selection: &[RowSelector]) -> Self {
        BitmaskSelection {
            mask: row_selectors_to_boolean_buffer(selection),
        }
    }

    /// Convert this [`BitmaskSelection`] into a [`BooleanArray`]
    pub fn into_filter(self) -> BooleanArray {
        BooleanArray::new(self.mask, None)
    }

    /// Convert this [`BitmaskSelection`] into a vector of [`RowSelector`]
    pub fn into_row_selectors(self) -> Vec<RowSelector> {
        boolean_array_to_row_selectors(&self.into_filter())
    }

    /// Create a new [`BitmaskSelection`] with all rows unselected
    pub fn new_unselected(row_count: usize) -> Self {
        let buffer = BooleanBuffer::new_unset(row_count);
        Self { mask: buffer }
    }

    /// Create a new [`BitmaskSelection`] with all rows selected
    pub fn new_selected(row_count: usize) -> Self {
        let buffer = BooleanBuffer::new_set(row_count);
        Self { mask: buffer }
    }

    /// Returns a new [`BitmaskSelection`] that selects the inverse of this [`BitmaskSelection`].
    pub fn inverted(&self) -> Self {
        let buffer = !&self.mask;
        Self { mask: buffer }
    }

    /// Returns the number of rows selected by this [`BitmaskSelection`].
    pub fn row_count(&self) -> usize {
        self.mask.count_set_bits()
    }

    /// Create a new [`BitmaskSelection`] from a list of consecutive ranges.
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

        BitmaskSelection {
            mask: buffer.finish(),
        }
    }

    /// Compute the union of two [`BitmaskSelection`]
    /// For example:
    /// self:      NNYYYYNNYYNYN
    /// other:     NYNNNNNNN
    ///
    /// returned:  NYYYYYNNYYNYN
    #[must_use]
    pub fn union(&self, other: &Self) -> Self {
        // use arrow::compute::kernels::boolean::or;

        let union_selectors = &self.mask | &other.mask;

        BitmaskSelection {
            mask: union_selectors,
        }
    }

    /// Compute the intersection of two [`BitmaskSelection`]
    /// For example:
    /// self:      NNYYYYNNYYNYN
    /// other:     NYNNNNNNY
    ///
    /// returned:  NNNNNNNNYYNYN
    #[must_use]
    pub fn intersection(&self, other: &Self) -> Self {
        let intersection_selectors = &self.mask & &other.mask;

        BitmaskSelection {
            mask: intersection_selectors,
        }
    }

    /// Combines this [`BitmaskSelection`] with another using logical AND on the selected bits.
    ///
    /// Unlike intersection, the `other` [`BitmaskSelection`] must have exactly as many set bits as `self`.
    /// This method will keep only the bits in `self` that are also set in `other`
    /// at the positions corresponding to `self`'s set bits.
    pub fn and_then(&self, other: &Self) -> Self {
        // Ensure that 'other' has exactly as many set bits as 'self'
        debug_assert_eq!(
            self.row_count(),
            other.mask.len(),
            "The 'other' selection must have exactly as many set bits as 'self'."
        );

        BitmaskSelection {
            mask: boolean_buffer_and_then(&self.mask, &other.mask),
        }
    }

    /// Returns an iterator over the indices of the set bits in this [`BitmaskSelection`]
    pub fn true_iter(&self) -> BitIndexIterator<'_> {
        self.mask.set_indices()
    }

    /// Returns `true` if this [`BitmaskSelection`] selects any rows
    pub fn selects_any(&self) -> bool {
        self.true_iter().next().is_some()
    }

    /// Returns a new [`BitmaskSelection`] that selects the rows in this [`BitmaskSelection`] from `offset` to `offset + len`
    pub fn slice(&self, offset: usize, len: usize) -> BooleanArray {
        BooleanArray::new(self.mask.slice(offset, len), None)
    }

    /// Splits off the first `row_count` rows
    pub fn split_off(&mut self, row_count: usize) -> Self {
        if row_count >= self.mask.len() {
            let split_mask = self.mask.clone();
            self.mask = BooleanBuffer::new_unset(0);
            BitmaskSelection { mask: split_mask }
        } else {
            let split_mask = self.mask.slice(0, row_count);
            self.mask = self.mask.slice(row_count, self.mask.len() - row_count);
            BitmaskSelection { mask: split_mask }
        }
    }

    /// Applies an offset to this [`RowSelection`], skipping the first `offset` selected rows
    ///
    /// Sets the fist `offset` selected rows to false
    pub fn offset(self, offset: usize) -> Self {
        let mut total_num_set = 0;

        // iterator:
        // * first value is the start of the range (inclusive)
        // * second value is the end of the range (exclusive)
        for (start, end) in self.mask.set_slices() {
            let num_set = end - start;
            // the offset is within this range,
            if offset <= total_num_set + num_set {
                let num_in_range = offset - total_num_set;
                let slice_offset = start + num_in_range;
                // Set all bits to false before slice_offset
                // TODO try and avoid this allocation and update in place
                let remaining_len = self.mask.len() - slice_offset;
                let new_mask = {
                    let mut builder = BooleanBufferBuilder::new(self.mask.len());
                    builder.append_n(slice_offset, false);
                    builder.append_buffer(&self.mask.slice(slice_offset, remaining_len));
                    builder.finish()
                };
                return BitmaskSelection { mask: new_mask };
            } else {
                total_num_set += num_set;
            }
        }

        // if we reach here, the offset is beyond the number of selected rows,
        // so return an empty selection
        BitmaskSelection {
            mask: BooleanBuffer::new_unset(self.mask.len()),
        }
    }
}

impl From<Vec<RowSelector>> for BitmaskSelection {
    fn from(selectors: Vec<RowSelector>) -> Self {
        Self::from_row_selectors(&selectors)
    }
}

impl From<BitmaskSelection> for Vec<RowSelector> {
    fn from(selection: BitmaskSelection) -> Self {
        selection.into_row_selectors()
    }
}

impl From<BooleanArray> for BitmaskSelection {
    fn from(filter: BooleanArray) -> Self {
        Self::from_filters(&[filter])
    }
}

impl From<BitmaskSelection> for BooleanArray {
    fn from(selection: BitmaskSelection) -> Self {
        selection.into_filter()
    }
}
