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

use crate::column::value_selection::{RangesSelectionRef, SelectionRange, ValueSelectionRef};
use crate::column::writer::{LevelDataRef, LevelValueWindow};
use arrow_array::Array;
use arrow_buffer::NullBuffer;
use arrow_buffer::bit_iterator::BitIndexIterator;
use std::ops::Range;

pub(super) const LEVEL_RUN_PROBE_SIZE: usize = 128;
pub(super) const MIN_AVERAGE_LEVEL_RUN_LENGTH: usize = 8;

/// Append indices, reserving `upper_bound` of them up front.
///
/// A bitmap index iterator is not `TrustedLen`, so `Vec::extend` re-reads the
/// vector's capacity for every index it appends, and does so from an outlined
/// call. Filling the spare capacity directly keeps the append a counted loop.
/// `upper_bound` only has to be a hint: the fill repeats if it runs short.
#[inline]
fn extend_indices(
    indices: &mut Vec<usize>,
    upper_bound: usize,
    iter: impl IntoIterator<Item = usize>,
) {
    let mut iter = iter.into_iter();
    indices.reserve(upper_bound);
    loop {
        let base = indices.len();
        let spare = indices.spare_capacity_mut();
        let capacity = spare.len();
        let mut filled = 0;
        for slot in spare {
            let Some(index) = iter.next() else { break };
            slot.write(index);
            filled += 1;
        }
        // SAFETY: the loop above initialized the leading `filled` spare slots.
        unsafe { indices.set_len(base + filled) };
        if filled != capacity {
            return;
        }
        // The spare ran out before the iterator did. Grow by one through the
        // amortized path and keep filling.
        let Some(index) = iter.next() else { return };
        indices.push(index);
    }
}

/// Owned value positions for one reusable cursor tile.
#[derive(Debug, Clone)]
pub(crate) enum ValueSelection {
    Empty,
    Dense {
        offset: usize,
        len: usize,
    },
    Ranges {
        ranges: Vec<SelectionRange>,
        len: usize,
    },
    Sparse(Vec<usize>),
}

impl ValueSelection {
    pub(crate) fn as_ref(&self) -> ValueSelectionRef<'_> {
        match self {
            Self::Empty => ValueSelectionRef::Empty,
            Self::Dense { offset, len } => ValueSelectionRef::Dense {
                offset: *offset,
                len: *len,
            },
            Self::Ranges { ranges, len } => {
                ValueSelectionRef::Ranges(RangesSelectionRef::new(ranges, *len))
            }
            Self::Sparse(indices) => ValueSelectionRef::Sparse(indices),
        }
    }

    pub(super) fn clear(&mut self) {
        match self {
            Self::Sparse(indices) => indices.clear(),
            _ => *self = Self::Empty,
        }
    }

    pub(super) fn append_range(&mut self, range: Range<usize>) {
        if range.is_empty() {
            return;
        }
        let range_len = range.len();
        match self {
            Self::Empty => {
                *self = Self::Dense {
                    offset: range.start,
                    len: range_len,
                };
            }
            Self::Dense { offset, len } if *offset + *len == range.start => *len += range_len,
            Self::Dense { offset, len } => {
                let (offset, len) = (*offset, *len);
                if range.start < offset + len {
                    let mut indices: Vec<_> = (offset..offset + len).collect();
                    indices.extend(range);
                    *self = Self::Sparse(indices);
                    return;
                }
                let ranges = vec![
                    SelectionRange::new(offset..offset + len, len),
                    SelectionRange::new(range.clone(), len + range_len),
                ];
                let total_len = len + range_len;
                if Self::ranges_are_expensive(ranges.len(), total_len) {
                    let mut indices: Vec<_> = (offset..offset + len).collect();
                    indices.extend(range);
                    *self = Self::Sparse(indices);
                } else {
                    *self = Self::Ranges {
                        ranges,
                        len: total_len,
                    };
                }
            }
            Self::Ranges { ranges, len } => {
                let last_index = ranges.len() - 1;
                let selected_start = last_index
                    .checked_sub(1)
                    .map_or(0, |previous| ranges[previous].selected_end);
                let last_end = ranges[last_index].source_range(selected_start).end;
                match last_end.cmp(&range.start) {
                    std::cmp::Ordering::Equal => {
                        ranges.last_mut().unwrap().selected_end += range_len;
                    }
                    std::cmp::Ordering::Greater => {
                        let mut indices = Vec::with_capacity(*len + range_len);
                        indices.extend(ranges.iter().enumerate().flat_map(|(idx, range)| {
                            let start = idx
                                .checked_sub(1)
                                .map_or(0, |previous| ranges[previous].selected_end);
                            range.source_range(start)
                        }));
                        indices.extend(range);
                        *self = Self::Sparse(indices);
                        return;
                    }
                    std::cmp::Ordering::Less => {
                        ranges.push(SelectionRange::new(range, *len + range_len));
                    }
                }
                *len += range_len;
                if Self::ranges_are_expensive(ranges.len(), *len) {
                    let indices = ranges
                        .iter()
                        .enumerate()
                        .flat_map(|(idx, range)| {
                            let start = idx
                                .checked_sub(1)
                                .map_or(0, |previous| ranges[previous].selected_end);
                            range.source_range(start)
                        })
                        .collect();
                    *self = Self::Sparse(indices);
                }
            }
            Self::Sparse(indices) => indices.extend(range),
        }
    }

    fn extend_sparse_indices<I>(&mut self, value_count: usize, iter: I)
    where
        I: IntoIterator<Item = usize>,
    {
        let iter = iter.into_iter();
        let mut materialized = match self {
            Self::Empty => Vec::with_capacity(value_count),
            Self::Dense { offset, len } => {
                let mut indices = Vec::with_capacity(*len + value_count);
                indices.extend(*offset..*offset + *len);
                indices
            }
            Self::Ranges { ranges, len } => {
                let mut indices = Vec::with_capacity(*len + value_count);
                let mut selected_start = 0;
                for range in ranges {
                    indices.extend(range.source_range(selected_start));
                    selected_start = range.selected_end;
                }
                indices
            }
            Self::Sparse(indices) => return extend_indices(indices, value_count, iter),
        };
        extend_indices(&mut materialized, value_count, iter);
        if !materialized.is_empty() {
            *self = Self::Sparse(materialized);
        }
    }

    pub(super) fn append_sparse(&mut self, nulls: NullBuffer, offset: usize, len: usize) {
        if len == 0 {
            return;
        }
        self.extend_sparse_indices(len, nulls.valid_indices().map(|index| index + offset));
    }

    pub(super) fn append_sparse_range(&mut self, nulls: &NullBuffer, range: Range<usize>) {
        if range.is_empty() {
            return;
        }
        let bits = nulls.inner();
        self.extend_sparse_indices(
            range.len(),
            BitIndexIterator::new(bits.values(), bits.offset() + range.start, range.len())
                .map(|index| index + range.start),
        );
    }

    fn ranges_are_expensive(num_ranges: usize, num_values: usize) -> bool {
        num_ranges.saturating_mul(std::mem::size_of::<SelectionRange>())
            >= num_values.saturating_mul(std::mem::size_of::<usize>())
    }
}

/// One borrowed batch presented to the column writer.
#[derive(Clone, Copy)]
pub(crate) struct LeafBatch<'a> {
    array: &'a (dyn Array + 'static),
    def_levels: LevelDataRef<'a>,
    rep_levels: LevelDataRef<'a>,
    values: ValueSelectionRef<'a>,
}

impl<'a> LeafBatch<'a> {
    pub(crate) fn new(
        array: &'a (dyn Array + 'static),
        def_levels: LevelDataRef<'a>,
        rep_levels: LevelDataRef<'a>,
        values: ValueSelectionRef<'a>,
    ) -> Self {
        Self {
            array,
            def_levels,
            rep_levels,
            values,
        }
    }

    pub(crate) fn array(&self) -> &'a (dyn Array + 'static) {
        self.array
    }

    pub(crate) fn def_level_data(&self) -> LevelDataRef<'a> {
        self.def_levels
    }

    pub(crate) fn rep_level_data(&self) -> LevelDataRef<'a> {
        self.rep_levels
    }

    pub(crate) fn value_selection(&self) -> ValueSelectionRef<'a> {
        self.values
    }

    pub(crate) fn slice(self, window: LevelValueWindow) -> Self {
        Self {
            array: self.array,
            def_levels: self
                .def_levels
                .slice(window.levels.start, window.levels.len()),
            rep_levels: self
                .rep_levels
                .slice(window.levels.start, window.levels.len()),
            values: self.values.slice(window.values.start, window.values.len()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn selected(selection: &ValueSelection) -> Vec<usize> {
        selection.as_ref().cursor().collect()
    }

    #[test]
    fn value_selection_range_transitions() {
        let mut selection = ValueSelection::Empty;
        selection.append_range(4..4);
        assert!(matches!(&selection, ValueSelection::Empty));

        selection.append_range(10..14);
        assert!(matches!(
            &selection,
            ValueSelection::Dense { offset: 10, len: 4 }
        ));
        selection.append_range(14..18);
        assert_eq!(selected(&selection), (10..18).collect::<Vec<_>>());

        selection.append_range(20..24);
        assert!(matches!(&selection, ValueSelection::Ranges { .. }));
        selection.append_range(24..26);
        assert_eq!(
            selected(&selection),
            (10..18).chain(20..26).collect::<Vec<_>>()
        );

        selection.append_range(25..28);
        assert!(matches!(&selection, ValueSelection::Sparse(_)));
        assert_eq!(
            selected(&selection),
            (10..18).chain(20..26).chain(25..28).collect::<Vec<_>>()
        );

        selection.clear();
        assert!(matches!(&selection, ValueSelection::Sparse(indices) if indices.is_empty()));
        selection.append_range(1..3);
        assert_eq!(selected(&selection), [1, 2]);

        let mut selection = ValueSelection::Empty;
        for range in [0..3, 6..9, 12..13] {
            selection.append_range(range);
        }
        assert!(matches!(&selection, ValueSelection::Ranges { .. }));
        selection.append_range(15..16);
        assert!(matches!(&selection, ValueSelection::Sparse(_)));
        assert_eq!(selected(&selection), [0, 1, 2, 6, 7, 8, 12, 15]);
    }

    #[test]
    fn value_selection_sparse_transitions() {
        let mut all_null = ValueSelection::Empty;
        all_null.append_sparse(NullBuffer::new_null(3), 10, 3);
        assert!(matches!(all_null, ValueSelection::Empty));

        let mut empty = ValueSelection::Empty;
        empty.append_sparse(NullBuffer::from(&[true, false, true]), 10, 3);
        assert_eq!(selected(&empty), [10, 12]);

        let mut dense = ValueSelection::Empty;
        dense.append_range(5..8);
        dense.append_sparse(NullBuffer::from(&[false, true]), 20, 2);
        assert_eq!(selected(&dense), [5, 6, 7, 21]);

        let mut ranges = ValueSelection::Empty;
        ranges.append_range(0..3);
        ranges.append_range(6..9);
        assert!(matches!(&ranges, ValueSelection::Ranges { .. }));
        ranges.append_sparse(NullBuffer::from(&[true, false, true]), 20, 3);
        assert_eq!(selected(&ranges), [0, 1, 2, 6, 7, 8, 20, 22]);

        ranges.append_sparse(NullBuffer::from(&[false, true]), 30, 2);
        assert_eq!(selected(&ranges), [0, 1, 2, 6, 7, 8, 20, 22, 31]);

        let before = selected(&ranges);
        ranges.append_sparse(NullBuffer::from(&[true]), 40, 0);
        assert_eq!(selected(&ranges), before);
    }

    #[test]
    fn leaf_batch_window_preserves_levels_and_selected_values() {
        let array = arrow_array::Int32Array::from(vec![10, 20, 30, 40]);
        let batch = LeafBatch::new(
            &array,
            LevelDataRef::Materialized(&[1, 0, 1, 1]),
            LevelDataRef::Absent,
            ValueSelectionRef::Sparse(&[3, 0, 2]),
        );
        let sliced = batch.slice(LevelValueWindow {
            levels: 1..4,
            values: 1..3,
        });
        assert_eq!(
            sliced.def_level_data().cursor().collect::<Vec<_>>(),
            [0, 1, 1]
        );
        assert_eq!(
            sliced
                .value_selection()
                .cursor()
                .map(|i| array.value(i))
                .collect::<Vec<_>>(),
            [10, 30]
        );
        assert!(std::ptr::eq(batch.array(), sliced.array()));
    }
}
