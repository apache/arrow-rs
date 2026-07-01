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

use super::{RowSelection, RowSelectionInner, RowSelector};
use crate::errors::ParquetError;
use arrow_array::BooleanArray;
use arrow_buffer::bit_iterator::BitSliceIterator;
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder};
use std::cmp::Ordering;
use std::sync::OnceLock;

/// Mask-backed [`RowSelection`] storage.
///
/// `selectors` is only populated if callers use the borrowed [`RowSelection::iter`]
/// compatibility API. Internal paths that can stream or consume the bitmap avoid
/// this cache.
#[derive(Debug)]
pub(crate) struct MaskSelection {
    mask: BooleanBuffer,
    selectors: OnceLock<Vec<RowSelector>>,
}

impl MaskSelection {
    pub(super) fn new(mask: BooleanBuffer) -> Self {
        Self {
            mask,
            selectors: OnceLock::new(),
        }
    }

    pub(crate) fn mask(&self) -> &BooleanBuffer {
        &self.mask
    }

    pub(crate) fn into_mask(self) -> BooleanBuffer {
        let Self { mask, .. } = self;
        mask
    }

    pub(super) fn selectors(&self) -> &[RowSelector] {
        self.selectors
            .get_or_init(|| mask_to_selectors(&self.mask))
            .as_slice()
    }
}

impl Clone for MaskSelection {
    fn clone(&self) -> Self {
        Self::new(self.mask.clone())
    }
}

/// Streaming RLE view of a [`BooleanBuffer`], yielding owned [`RowSelector`]s
/// without allocation.
///
/// Useful as a zero-cost alternative to [`RowSelection::iter`] for mask-backed
/// selections, via [`RowSelection::as_mask`]:
///
/// ```ignore
/// if let Some(mask) = selection.as_mask() {
///     for run in MaskRunIter::new(mask) { ... }
/// }
/// ```
#[derive(Debug)]
pub struct MaskRunIter<'a> {
    slices: BitSliceIterator<'a>,
    cursor: usize,
    total: usize,
    pending: Option<RowSelector>,
    finished: bool,
}

impl<'a> MaskRunIter<'a> {
    /// Create a streaming RLE iterator over a [`BooleanBuffer`].
    pub fn new(mask: &'a BooleanBuffer) -> Self {
        Self {
            slices: mask.set_slices(),
            cursor: 0,
            total: mask.len(),
            pending: None,
            finished: false,
        }
    }
}

impl Iterator for MaskRunIter<'_> {
    type Item = RowSelector;

    fn next(&mut self) -> Option<RowSelector> {
        if let Some(p) = self.pending.take() {
            return Some(p);
        }
        if self.finished {
            return None;
        }
        match self.slices.next() {
            Some((start, end)) => {
                let select = RowSelector::select(end - start);
                if start > self.cursor {
                    let skip = RowSelector::skip(start - self.cursor);
                    self.pending = Some(select);
                    self.cursor = end;
                    Some(skip)
                } else {
                    self.cursor = end;
                    Some(select)
                }
            }
            None => {
                self.finished = true;
                if self.cursor < self.total {
                    let skip = RowSelector::skip(self.total - self.cursor);
                    self.cursor = self.total;
                    Some(skip)
                } else {
                    None
                }
            }
        }
    }
}

/// Cursor for iterating a mask-backed [`RowSelection`]
///
/// This is best for dense selections where there are many small skips
/// or selections. For example, selecting every other row.
#[derive(Debug)]
pub struct MaskCursor {
    pub(super) mask: BooleanBuffer,
    /// Current absolute offset into the selection
    pub(super) position: usize,
}

impl MaskCursor {
    /// Returns `true` when no further rows remain
    pub fn is_empty(&self) -> bool {
        self.position >= self.mask.len()
    }

    /// Advance through the mask representation, producing the next chunk summary
    pub fn next_mask_chunk(&mut self, batch_size: usize) -> Option<MaskChunk> {
        let (initial_skip, chunk_rows, selected_rows, mask_start, end_position) = {
            let mask = &self.mask;

            if self.position >= mask.len() {
                return None;
            }

            let start_position = self.position;
            let mut cursor = start_position;
            let mut initial_skip = 0;

            while cursor < mask.len() && !mask.value(cursor) {
                initial_skip += 1;
                cursor += 1;
            }

            let mask_start = cursor;
            let mut chunk_rows = 0;
            let mut selected_rows = 0;

            // Advance until enough rows have been selected to satisfy the batch size,
            // or until the mask is exhausted. This mirrors the behaviour of the legacy
            // `RowSelector` queue-based iteration.
            while cursor < mask.len() && selected_rows < batch_size {
                chunk_rows += 1;
                if mask.value(cursor) {
                    selected_rows += 1;
                }
                cursor += 1;
            }

            (initial_skip, chunk_rows, selected_rows, mask_start, cursor)
        };

        self.position = end_position;

        Some(MaskChunk {
            initial_skip,
            chunk_rows,
            selected_rows,
            mask_start,
        })
    }

    /// Materialise the boolean values for a mask-backed chunk
    pub fn mask_values_for(&self, chunk: &MaskChunk) -> Result<BooleanArray, ParquetError> {
        if chunk.mask_start.saturating_add(chunk.chunk_rows) > self.mask.len() {
            return Err(ParquetError::General(
                "Internal Error: MaskChunk exceeds mask length".to_string(),
            ));
        }
        Ok(BooleanArray::from(
            self.mask.slice(chunk.mask_start, chunk.chunk_rows),
        ))
    }
}

/// Result of computing the next chunk to read when using a [`MaskCursor`]
#[derive(Debug)]
pub struct MaskChunk {
    /// Number of leading rows to skip before reaching selected rows
    pub initial_skip: usize,
    /// Total rows covered by this chunk (selected + skipped)
    pub chunk_rows: usize,
    /// Rows actually selected within the chunk
    pub selected_rows: usize,
    /// Starting offset within the mask where the chunk begins
    pub mask_start: usize,
}

/// Materialize a [`BooleanBuffer`] into its RLE form.
pub(crate) fn mask_to_selectors(mask: &BooleanBuffer) -> Vec<RowSelector> {
    let total_rows = mask.len();
    if total_rows == 0 {
        return Vec::new();
    }
    let mut selectors: Vec<RowSelector> = Vec::new();
    let mut last_end = 0;
    for (start, end) in mask.set_slices() {
        if start > last_end {
            selectors.push(RowSelector::skip(start - last_end));
        }
        selectors.push(RowSelector::select(end - start));
        last_end = end;
    }
    if last_end != total_rows {
        selectors.push(RowSelector::skip(total_rows - last_end));
    }
    selectors
}

pub(super) fn mask_run_count(mask: &BooleanBuffer) -> usize {
    let total_rows = mask.len();
    if total_rows == 0 {
        return 0;
    }

    let mut run_count = 0;
    let mut last_end = 0;
    for (start, end) in mask.set_slices() {
        if start > last_end {
            run_count += 1;
        }
        if end > start {
            run_count += 1;
        }
        last_end = end;
    }
    if last_end < total_rows {
        run_count += 1;
    }

    run_count
}

/// Bitwise AND of two mask-backed selections. Longer side's tail passes through.
pub(super) fn intersect_masks(l: &BooleanBuffer, r: &BooleanBuffer) -> BooleanBuffer {
    if l.len() == r.len() {
        return l & r;
    }
    let common = l.len().min(r.len());
    let head = &l.slice(0, common) & &r.slice(0, common);
    let (longer, longer_len) = if l.len() > r.len() {
        (l, l.len())
    } else {
        (r, r.len())
    };
    let tail = longer.slice(common, longer_len - common);
    let mut builder = BooleanBufferBuilder::new(longer_len);
    builder.append_buffer(&head);
    builder.append_buffer(&tail);
    builder.finish()
}

/// Bitwise OR of two mask-backed selections. Longer side's tail passes through.
pub(super) fn union_masks(l: &BooleanBuffer, r: &BooleanBuffer) -> BooleanBuffer {
    if l.len() == r.len() {
        return l | r;
    }
    let common = l.len().min(r.len());
    let head = &l.slice(0, common) | &r.slice(0, common);
    let (longer, longer_len) = if l.len() > r.len() {
        (l, l.len())
    } else {
        (r, r.len())
    };
    let tail = longer.slice(common, longer_len - common);
    let mut builder = BooleanBufferBuilder::new(longer_len);
    builder.append_buffer(&head);
    builder.append_buffer(&tail);
    builder.finish()
}

/// Applies `other` to the selected rows of `mask`, preserving the original row domain.
pub(super) fn and_then_mask(mask: &BooleanBuffer, other: &RowSelection) -> BooleanBuffer {
    match &other.inner {
        RowSelectionInner::Mask(other_mask) => and_then_masks(mask, other_mask.mask()),
        RowSelectionInner::Selectors(selectors) => {
            and_then_mask_from_selectors(mask, selectors.iter().copied())
        }
    }
}

fn and_then_mask_from_selectors<I>(mask: &BooleanBuffer, other: I) -> BooleanBuffer
where
    I: IntoIterator<Item = RowSelector>,
{
    let mut builder = BooleanBufferBuilder::new(mask.len());
    let mut other_iter = other.into_iter();
    let mut current = other_iter.next();
    let mut cursor = 0usize;

    // Iterate only over the set positions in `mask`; the gaps of unset bits
    // are filled in bulk with `append_n` instead of bit-by-bit.
    for set_idx in mask.set_indices() {
        if set_idx > cursor {
            builder.append_n(set_idx - cursor, false);
        }
        cursor = set_idx + 1;

        while current.as_ref().is_some_and(|s| s.row_count == 0) {
            current = other_iter.next();
        }
        let selector = current
            .as_mut()
            .expect("selection contains less than the number of selected rows");
        let selected = !selector.skip;
        selector.row_count -= 1;
        builder.append(selected);
    }
    if cursor < mask.len() {
        builder.append_n(mask.len() - cursor, false);
    }

    if current.is_some_and(|s| s.row_count != 0) || other_iter.any(|s| s.row_count != 0) {
        panic!("selection exceeds the number of selected rows");
    }

    builder.finish()
}

fn and_then_masks(mask: &BooleanBuffer, other: &BooleanBuffer) -> BooleanBuffer {
    let selected_count = mask.count_set_bits();
    match other.len().cmp(&selected_count) {
        Ordering::Less => panic!("selection contains less than the number of selected rows"),
        Ordering::Greater => panic!("selection exceeds the number of selected rows"),
        Ordering::Equal => {}
    }

    let other_true_count = other.count_set_bits();
    if other_true_count == 0 {
        return BooleanBuffer::new_unset(mask.len());
    }
    if other_true_count == selected_count {
        return mask.clone();
    }

    let mut builder = BooleanBufferBuilder::new(mask.len());
    let mut outer_set_indices = mask.set_indices();
    let mut next_selected_ordinal = 0usize;
    let mut cursor = 0usize;

    for selected_ordinal in other.set_indices() {
        let skip = selected_ordinal - next_selected_ordinal;
        let set_idx = outer_set_indices
            .nth(skip)
            .expect("validated other length matches selected row count");
        if set_idx > cursor {
            builder.append_n(set_idx - cursor, false);
        }
        builder.append(true);
        cursor = set_idx + 1;
        next_selected_ordinal = selected_ordinal + 1;
    }

    if cursor < mask.len() {
        builder.append_n(mask.len() - cursor, false);
    }

    builder.finish()
}

/// Split a mask into `(head, tail)` at `row_count`, preserving an empty mask tail
/// when the split point is past the end.
pub(super) fn split_off_mask(
    mask: BooleanBuffer,
    row_count: usize,
) -> (BooleanBuffer, BooleanBuffer) {
    let total = mask.len();
    if row_count >= total {
        return (mask, BooleanBuffer::new_unset(0));
    }

    let head = mask.slice(0, row_count);
    let tail = mask.slice(row_count, total - row_count);
    (head, tail)
}

/// Trims trailing unset bits from a mask-backed selection.
pub(super) fn trim_mask(mask: &BooleanBuffer) -> Option<BooleanBuffer> {
    let popcount = mask.count_set_bits();
    let new_len = if popcount == 0 {
        0
    } else {
        mask.find_nth_set_bit_position(0, popcount)
    };
    (new_len != mask.len()).then(|| mask.slice(0, new_len))
}

/// Skips the first `offset` selected rows of a mask-backed selection.
pub(super) fn offset_mask(mask: BooleanBuffer, offset: usize) -> BooleanBuffer {
    let popcount = mask.count_set_bits();
    if offset >= popcount {
        return BooleanBuffer::new_unset(0);
    }
    // Position one past the `offset`-th set bit, i.e. the index of the first
    // selected row to keep.
    let pos = mask.find_nth_set_bit_position(0, offset);
    let mut builder = BooleanBufferBuilder::new(mask.len());
    builder.append_n(pos, false);
    builder.append_buffer(&mask.slice(pos, mask.len() - pos));
    builder.finish()
}

/// Keeps only the first `limit` selected rows of a mask-backed selection.
pub(super) fn limit_mask(mask: BooleanBuffer, limit: usize) -> BooleanBuffer {
    // `find_nth_set_bit_position` returns `mask.len()` when there are fewer
    // than `limit` set bits, so the slice naturally degrades to the original
    // mask in that case.
    let cut = mask.find_nth_set_bit_position(0, limit);
    mask.slice(0, cut)
}

pub(super) fn boolean_mask_from_selectors(selectors: &[RowSelector]) -> BooleanBuffer {
    let total_rows: usize = selectors.iter().map(|s| s.row_count).sum();
    let mut builder = BooleanBufferBuilder::new(total_rows);
    for selector in selectors {
        builder.append_n(selector.row_count, !selector.skip);
    }
    builder.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::BooleanArray;
    use rand::{Rng, rng};

    #[test]
    fn test_mask_iter_yields_borrowed_selectors() {
        let selection = RowSelection::from_boolean_buffer(BooleanBuffer::from(vec![
            false, false, true, true, false, true, false, false,
        ]));

        let borrowed: Vec<&RowSelector> = selection.iter().collect();
        assert_eq!(
            borrowed,
            vec![
                &RowSelector::skip(2),
                &RowSelector::select(2),
                &RowSelector::skip(1),
                &RowSelector::select(1),
                &RowSelector::skip(2),
            ]
        );
    }

    #[test]
    fn test_mask_iter_clone_drops_cache() {
        let selection = RowSelection::from_boolean_buffer(BooleanBuffer::from(vec![
            false, false, true, true, false, true, false, false,
        ]));

        let _ = selection.iter().count();
        match &selection.inner {
            RowSelectionInner::Mask(m) => assert!(m.selectors.get().is_some()),
            _ => unreachable!(),
        }

        let cloned = selection.clone();
        match &cloned.inner {
            RowSelectionInner::Mask(m) => assert!(m.selectors.get().is_none()),
            _ => unreachable!(),
        }

        let round_tripped: Vec<RowSelector> = cloned.iter().copied().collect();
        assert_eq!(
            round_tripped,
            vec![
                RowSelector::skip(2),
                RowSelector::select(2),
                RowSelector::skip(1),
                RowSelector::select(1),
                RowSelector::skip(2),
            ]
        );
    }

    #[test]
    fn test_mask_run_iter_streams_without_cache() {
        let selection = RowSelection::from_boolean_buffer(BooleanBuffer::from(vec![
            false, false, true, true, false, true, false, false,
        ]));
        let mut iter = MaskRunIter::new(selection.as_mask().unwrap());

        assert_eq!(iter.next(), Some(RowSelector::skip(2)));
        assert_eq!(iter.next(), Some(RowSelector::select(2)));
        assert_eq!(iter.next(), Some(RowSelector::skip(1)));
        assert_eq!(iter.next(), Some(RowSelector::select(1)));
        assert_eq!(iter.next(), Some(RowSelector::skip(2)));
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None);

        let selection =
            RowSelection::from_boolean_buffer(BooleanBuffer::from(vec![true, true, false]));
        let mut iter = MaskRunIter::new(selection.as_mask().unwrap());
        assert_eq!(iter.next(), Some(RowSelector::select(2)));
        assert_eq!(iter.next(), Some(RowSelector::skip(1)));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_from_boolean_buffer() {
        let bits = vec![
            false, false, true, true, false, true, false, false, true, false, false, false, false,
            false, false, true,
        ];
        let buf = BooleanBuffer::from(bits.clone());
        let selection = RowSelection::from_boolean_buffer(buf.clone());

        assert!(selection.as_mask().is_some());
        assert_eq!(selection.row_count(), 5);
        assert_eq!(selection.skipped_row_count(), 11);
        assert!(selection.selects_any());

        let from_filters = RowSelection::from_filters(&[BooleanArray::from(bits)]);
        assert_eq!(selection, from_filters);

        let bits_tail = vec![true, false, true, false, false, false];
        let trimmed = RowSelection::from_boolean_buffer(BooleanBuffer::from(bits_tail)).trim();
        assert!(trimmed.as_mask().is_some());
        assert_eq!(trimmed.as_mask().unwrap().len(), 3);
    }

    #[test]
    fn test_from_boolean_buffer_empty() {
        let empty = RowSelection::from_boolean_buffer(BooleanBuffer::from(Vec::<bool>::new()));
        assert!(empty.as_mask().is_some());
        assert_eq!(empty.row_count(), 0);
        assert_eq!(empty.skipped_row_count(), 0);
        assert!(!empty.selects_any());
        assert!(empty.selectors().is_empty());
    }

    #[test]
    fn test_from_boolean_buffer_all_unset_does_not_select() {
        let all_zero = RowSelection::from_boolean_buffer(BooleanBuffer::new_unset(1024));
        assert!(all_zero.as_mask().is_some());
        assert!(!all_zero.selects_any());
        assert_eq!(all_zero.row_count(), 0);
        assert_eq!(all_zero.skipped_row_count(), 1024);
    }

    #[test]
    fn test_from_boolean_buffer_via_from_impl() {
        let buf = BooleanBuffer::from(vec![true, false, true, true]);
        let a = RowSelection::from(buf.clone());
        let b = RowSelection::from_boolean_buffer(buf);
        assert_eq!(a, b);
        assert!(a.as_mask().is_some());
    }

    #[test]
    fn test_mask_backing_clone_preserves_backing() {
        let buf = BooleanBuffer::from(vec![true, false, true]);
        let original = RowSelection::from_boolean_buffer(buf);
        let cloned = original.clone();
        assert!(cloned.as_mask().is_some());
        assert_eq!(original, cloned);
    }

    #[test]
    fn test_mask_backing_mutation_equivalence() {
        let bits = vec![true, true, false, false, true, false, true, true];

        let from_mask = {
            let mut s = RowSelection::from_boolean_buffer(BooleanBuffer::from(bits.clone()));
            let split = s.split_off(3);
            (split, s)
        };
        let from_selectors = {
            let mut s = RowSelection::from_filters(&[BooleanArray::from(bits.clone())]);
            let split = s.split_off(3);
            (split, s)
        };
        assert_eq!(from_mask.0, from_selectors.0);
        assert_eq!(from_mask.1, from_selectors.1);
        assert!(from_mask.0.as_mask().is_some());
        assert!(from_mask.1.as_mask().is_some());

        let limited_mask =
            RowSelection::from_boolean_buffer(BooleanBuffer::from(bits.clone())).limit(3);
        let limited_sel = RowSelection::from_filters(&[BooleanArray::from(bits.clone())]).limit(3);
        assert!(limited_mask.as_mask().is_some());
        assert_eq!(limited_mask, limited_sel);

        let offset_mask =
            RowSelection::from_boolean_buffer(BooleanBuffer::from(bits.clone())).offset(2);
        let offset_sel = RowSelection::from_filters(&[BooleanArray::from(bits)]).offset(2);
        assert!(offset_mask.as_mask().is_some());
        assert_eq!(offset_mask, offset_sel);
    }

    #[test]
    fn test_mask_backing_fuzz_equivalence() {
        let mut rand = rng();
        for _ in 0..100 {
            let len = rand.random_range(0..200);
            let bits: Vec<_> = (0..len).map(|_| rand.random_bool(0.35)).collect();

            let from_mask = RowSelection::from_boolean_buffer(BooleanBuffer::from(bits.clone()));
            let from_filters = RowSelection::from_filters(&[BooleanArray::from(bits.clone())]);

            assert_eq!(from_mask, from_filters);
            assert_eq!(from_mask.row_count(), from_filters.row_count());
            assert_eq!(
                from_mask.skipped_row_count(),
                from_filters.skipped_row_count()
            );
            assert_eq!(from_mask.selects_any(), from_filters.selects_any());

            let inner_len: usize = bits.iter().map(|b| *b as usize).sum();
            let inner_bits: Vec<_> = (0..inner_len).map(|_| rand.random_bool(0.7)).collect();
            let inner = RowSelection::from_filters(&[BooleanArray::from(inner_bits.clone())]);
            let inner_mask = RowSelection::from_boolean_buffer(BooleanBuffer::from(inner_bits));
            let and_then_mask = from_mask.and_then(&inner);
            let and_then_both_masks = from_mask.and_then(&inner_mask);
            assert!(and_then_mask.as_mask().is_some());
            assert!(and_then_both_masks.as_mask().is_some());
            assert_eq!(and_then_mask, from_filters.and_then(&inner));
            assert_eq!(and_then_both_masks, and_then_mask);
        }
    }

    #[test]
    fn test_mask_and_then_preserves_backing() {
        let outer_bits = vec![false, true, true, false, true, false, true];
        let inner_bits = vec![true, false, true, false];
        let outer_mask = RowSelection::from_boolean_buffer(BooleanBuffer::from(outer_bits.clone()));
        let inner = RowSelection::from_filters(&[BooleanArray::from(inner_bits.clone())]);

        let result = outer_mask.and_then(&inner);
        assert!(result.as_mask().is_some());

        let outer_selectors = RowSelection::from_filters(&[BooleanArray::from(outer_bits)]);
        let expected = outer_selectors.and_then(&inner);
        assert_eq!(result, expected);

        let result_mask = result.as_mask().unwrap();
        let actual_bits: Vec<_> = (0..result_mask.len())
            .map(|i| result_mask.value(i))
            .collect();
        assert_eq!(
            actual_bits,
            vec![false, true, false, false, true, false, false]
        );
    }

    #[test]
    fn test_mask_and_then_mask_preserves_backing() {
        let outer_bits = vec![false, true, true, false, true, false, true, true];
        let inner_bits = vec![false, true, false, true, false];
        let outer_mask = RowSelection::from_boolean_buffer(BooleanBuffer::from(outer_bits.clone()));
        let inner_mask = RowSelection::from_boolean_buffer(BooleanBuffer::from(inner_bits));

        let result = outer_mask.and_then(&inner_mask);
        assert!(result.as_mask().is_some());

        let outer_selectors = RowSelection::from_filters(&[BooleanArray::from(outer_bits)]);
        let inner_selectors = RowSelection::from_filters(&[BooleanArray::from(vec![
            false, true, false, true, false,
        ])]);
        assert_eq!(result, outer_selectors.and_then(&inner_selectors));

        let result_mask = result.as_mask().unwrap();
        let actual_bits: Vec<_> = (0..result_mask.len())
            .map(|i| result_mask.value(i))
            .collect();
        assert_eq!(
            actual_bits,
            vec![false, false, true, false, false, false, true, false]
        );
    }

    #[test]
    fn test_selector_and_then_mask() {
        let outer =
            RowSelection::from_filters(&[BooleanArray::from(vec![false, true, true, false, true])]);
        let inner = RowSelection::from_boolean_buffer(BooleanBuffer::from(vec![true, false, true]));

        let result = outer.and_then(&inner);
        assert!(result.as_mask().is_none());
        assert_eq!(
            result,
            RowSelection::from_filters(&[BooleanArray::from(vec![
                false, true, false, false, true,
            ])])
        );
    }

    #[test]
    fn test_mask_offset_past_end_preserves_empty_mask_backing() {
        let selection =
            RowSelection::from_boolean_buffer(BooleanBuffer::from(vec![true, false, true]))
                .offset(2);

        assert!(selection.as_mask().is_some());
        assert_eq!(selection.as_mask().unwrap().len(), 0);
        assert_eq!(selection.row_count(), 0);
        assert_eq!(selection.skipped_row_count(), 0);
    }

    #[test]
    fn test_mask_limit_truncates_at_nth_selected_row() {
        let selection = RowSelection::from_boolean_buffer(BooleanBuffer::from(vec![
            false, true, false, true, false, true, false,
        ]))
        .limit(2);

        let mask = selection.as_mask().unwrap();
        assert_eq!(mask.len(), 4);
        let actual_bits: Vec<_> = (0..mask.len()).map(|i| mask.value(i)).collect();
        assert_eq!(actual_bits, vec![false, true, false, true]);
    }

    #[test]
    fn test_mask_intersection_uses_bitwise() {
        let a_bits = vec![true, true, false, true, false, true];
        let b_bits = vec![true, false, true, true, true, false];
        let a = RowSelection::from_boolean_buffer(BooleanBuffer::from(a_bits.clone()));
        let b = RowSelection::from_boolean_buffer(BooleanBuffer::from(b_bits.clone()));

        let r = a.intersection(&b);
        assert!(r.as_mask().is_some());

        let expected: Vec<bool> = a_bits.iter().zip(&b_bits).map(|(x, y)| *x && *y).collect();
        let expected_sel = RowSelection::from_filters(&[BooleanArray::from(expected)]);
        assert_eq!(r, expected_sel);
    }

    #[test]
    fn test_mask_union_uses_bitwise() {
        let a_bits = vec![true, false, false, true, false, false];
        let b_bits = vec![false, true, false, false, true, false];
        let a = RowSelection::from_boolean_buffer(BooleanBuffer::from(a_bits.clone()));
        let b = RowSelection::from_boolean_buffer(BooleanBuffer::from(b_bits.clone()));

        let r = a.union(&b);
        assert!(r.as_mask().is_some());

        let expected: Vec<bool> = a_bits.iter().zip(&b_bits).map(|(x, y)| *x || *y).collect();
        let expected_sel = RowSelection::from_filters(&[BooleanArray::from(expected)]);
        assert_eq!(r, expected_sel);
    }

    #[test]
    fn test_mixed_mask_selector_intersection_and_union() {
        let mask_bits = vec![true, false, true, false, true, false];
        let selector_bits = vec![false, true, true, false, false, true];
        let mask = RowSelection::from_boolean_buffer(BooleanBuffer::from(mask_bits.clone()));
        let selectors = RowSelection::from_filters(&[BooleanArray::from(selector_bits.clone())]);

        let intersection_bits: Vec<_> = mask_bits
            .iter()
            .zip(&selector_bits)
            .map(|(x, y)| *x && *y)
            .collect();
        let expected_intersection =
            RowSelection::from_filters(&[BooleanArray::from(intersection_bits)]);
        assert_eq!(mask.intersection(&selectors), expected_intersection);
        assert_eq!(selectors.intersection(&mask), expected_intersection);

        let union_bits: Vec<_> = mask_bits
            .iter()
            .zip(&selector_bits)
            .map(|(x, y)| *x || *y)
            .collect();
        let expected_union = RowSelection::from_filters(&[BooleanArray::from(union_bits)]);
        assert_eq!(mask.union(&selectors), expected_union);
        assert_eq!(selectors.union(&mask), expected_union);
    }

    #[test]
    fn test_mask_intersection_uneven_passes_tail_through() {
        let a_bits = vec![true, true, true, true, true];
        let b_bits = vec![true, false, true];
        let a = RowSelection::from_boolean_buffer(BooleanBuffer::from(a_bits));
        let b = RowSelection::from_boolean_buffer(BooleanBuffer::from(b_bits));

        let r = a.intersection(&b);
        let r_mask = r.as_mask().unwrap();
        assert_eq!(r_mask.len(), 5);
        let bits: Vec<bool> = (0..5).map(|i| r_mask.value(i)).collect();
        assert_eq!(bits, vec![true, false, true, true, true]);
    }

    #[test]
    fn test_mask_union_uneven_passes_tail_through() {
        let a_bits = vec![true, false, true];
        let b_bits = vec![false, true, false, true, false];
        let a = RowSelection::from_boolean_buffer(BooleanBuffer::from(a_bits));
        let b = RowSelection::from_boolean_buffer(BooleanBuffer::from(b_bits));

        let r = a.union(&b);
        let r_mask = r.as_mask().unwrap();
        assert_eq!(r_mask.len(), 5);
        let bits: Vec<bool> = (0..5).map(|i| r_mask.value(i)).collect();
        assert_eq!(bits, vec![true, true, true, true, false]);

        let a = RowSelection::from_boolean_buffer(BooleanBuffer::from(vec![
            false, true, false, false, true,
        ]));
        let b = RowSelection::from_boolean_buffer(BooleanBuffer::from(vec![true, false, false]));
        let r = a.union(&b);
        let r_mask = r.as_mask().unwrap();
        let bits: Vec<bool> = (0..5).map(|i| r_mask.value(i)).collect();
        assert_eq!(bits, vec![true, true, false, false, true]);
    }

    #[test]
    fn test_mask_split_off_preserves_backing() {
        let bits: Vec<bool> = (0..40).map(|i| i % 3 == 0).collect();
        let mut s = RowSelection::from_boolean_buffer(BooleanBuffer::from(bits.clone()));
        let head = s.split_off(15);

        assert!(head.as_mask().is_some());
        assert!(s.as_mask().is_some());

        let head_sel = RowSelection::from_filters(&[BooleanArray::from(bits[..15].to_vec())]);
        let tail_sel = RowSelection::from_filters(&[BooleanArray::from(bits[15..].to_vec())]);
        assert_eq!(head, head_sel);
        assert_eq!(s, tail_sel);
    }

    #[test]
    fn test_mask_split_off_past_end_returns_whole() {
        let bits = vec![true, false, true];
        let mut s = RowSelection::from_boolean_buffer(BooleanBuffer::from(bits.clone()));
        let head = s.split_off(100);

        assert!(head.as_mask().is_some());
        assert_eq!(head.as_mask().unwrap().len(), 3);
        // `self` keeps its mask backing and is left empty.
        assert!(s.as_mask().is_some());
        assert_eq!(s.as_mask().unwrap().len(), 0);
        assert_eq!(s.row_count(), 0);
        assert_eq!(s.skipped_row_count(), 0);
    }

    #[test]
    fn test_mask_offset_exceeds_selected_returns_empty() {
        let s =
            RowSelection::from_boolean_buffer(BooleanBuffer::from(vec![true, true, false, true]));
        let r = s.offset(10);
        assert_eq!(r.row_count(), 0);
        assert_eq!(r.skipped_row_count(), 0);

        let from_selectors =
            RowSelection::from_filters(&[BooleanArray::from(vec![true, true, false, true])])
                .offset(10);
        assert_eq!(r, from_selectors);
    }

    #[test]
    fn test_mask_limit_exceeds_selected_returns_all() {
        let bits = vec![true, true, false, true];
        let s = RowSelection::from_boolean_buffer(BooleanBuffer::from(bits.clone()));
        let r = s.limit(10);
        assert_eq!(r.row_count(), 3);

        let from_selectors = RowSelection::from_filters(&[BooleanArray::from(bits)]).limit(10);
        assert_eq!(r, from_selectors);
    }

    #[test]
    fn test_mask_trim_all_zero_collapses_to_empty() {
        let s = RowSelection::from_boolean_buffer(BooleanBuffer::new_unset(128));
        let trimmed = s.trim();
        assert!(trimmed.as_mask().is_some());
        assert_eq!(trimmed.as_mask().unwrap().len(), 0);
    }

    #[test]
    fn test_from_iter_all_mask_preserves_mask_backing() {
        let a_bits = vec![true, false, true, true];
        let b_bits = vec![false, true, false];
        let c_bits = vec![true, true, false, false, true];

        let parts = vec![
            RowSelection::from_boolean_buffer(BooleanBuffer::from(a_bits.clone())),
            RowSelection::from_boolean_buffer(BooleanBuffer::from(b_bits.clone())),
            RowSelection::from_boolean_buffer(BooleanBuffer::from(c_bits.clone())),
        ];
        let collected: RowSelection = parts.into_iter().collect();

        let combined = a_bits
            .iter()
            .chain(b_bits.iter())
            .chain(c_bits.iter())
            .copied()
            .collect::<Vec<_>>();
        let expected = RowSelection::from_filters(&[BooleanArray::from(combined)]);

        assert!(collected.as_mask().is_some());
        assert_eq!(collected, expected);
    }

    #[test]
    fn test_from_iter_mixed_backing_falls_back_to_selectors() {
        let a_bits = vec![true, false, true];
        let b_selectors = vec![RowSelector::skip(2), RowSelector::select(3)];
        let c_bits = vec![false, true];

        let parts = vec![
            RowSelection::from_boolean_buffer(BooleanBuffer::from(a_bits.clone())),
            RowSelection::from(b_selectors),
            RowSelection::from_boolean_buffer(BooleanBuffer::from(c_bits.clone())),
        ];
        let collected: RowSelection = parts.into_iter().collect();

        assert!(collected.as_mask().is_none());

        let combined_bits = vec![
            true, false, true, false, false, true, true, true, false, true,
        ];
        let expected = RowSelection::from_filters(&[BooleanArray::from(combined_bits)]);
        assert_eq!(collected, expected);
    }

    #[test]
    fn test_from_iter_empty_yields_empty_selection() {
        let collected: RowSelection = std::iter::empty::<RowSelection>().collect();
        assert_eq!(collected, RowSelection::default());
        assert!(collected.as_mask().is_some());
        assert_eq!(collected.as_mask().unwrap().len(), 0);
    }
}
