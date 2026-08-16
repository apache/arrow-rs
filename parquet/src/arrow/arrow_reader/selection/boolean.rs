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

//! The bitmap backed representation of a [`RowSelection`] and the primitives
//! operating on it: conversion to and from the run length ([`RowSelector`])
//! form, and the transforms backing `split_off`, `trim`, `offset` and `limit`.
//!
//! The bitwise set algebra lives in the `algebra` module.
//!
//! [`RowSelection`]: crate::arrow::arrow_reader::RowSelection

use super::RowSelector;
use arrow_buffer::bit_iterator::BitSliceIterator;
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder, Buffer};
use std::borrow::Cow;
use std::sync::OnceLock;

/// Mask-backed [`RowSelection`] storage.
///
/// `selectors` is only populated if callers use the borrowed
/// [`RowSelection::iter`] compatibility API. Internal paths that can stream or
/// consume the bitmap avoid this cache.
///
/// `count` caches the popcount; [`RowSelection::split_off`] propagates it to
/// both halves so repeated `row_count()` calls do not rescan the bitmap.
///
/// [`RowSelection`]: crate::arrow::arrow_reader::RowSelection
/// [`RowSelection::iter`]: crate::arrow::arrow_reader::RowSelection::iter
/// [`RowSelection::split_off`]: crate::arrow::arrow_reader::RowSelection::split_off
#[derive(Debug)]
pub(crate) struct MaskSelection {
    mask: BooleanBuffer,
    selectors: OnceLock<Vec<RowSelector>>,
    count: OnceLock<usize>,
}

impl MaskSelection {
    pub(super) fn new(mask: BooleanBuffer) -> Self {
        Self {
            mask,
            selectors: OnceLock::new(),
            count: OnceLock::new(),
        }
    }

    /// Create a selection whose selected-row count is already known.
    pub(super) fn with_count(mask: BooleanBuffer, count: usize) -> Self {
        debug_assert!(count <= mask.len());
        let cell = OnceLock::new();
        let _ = cell.set(count);
        Self {
            mask,
            selectors: OnceLock::new(),
            count: cell,
        }
    }

    pub(crate) fn mask(&self) -> &BooleanBuffer {
        &self.mask
    }

    pub(crate) fn into_mask(self) -> BooleanBuffer {
        let Self { mask, .. } = self;
        mask
    }

    /// Number of selected rows, computed once and cached.
    pub(super) fn count(&self) -> usize {
        *self.count.get_or_init(|| self.mask.count_set_bits())
    }

    /// The cached selected-row count, if it has been computed.
    pub(super) fn cached_count(&self) -> Option<usize> {
        self.count.get().copied()
    }

    pub(super) fn selectors(&self) -> &[RowSelector] {
        self.selectors
            .get_or_init(|| mask_to_selectors(&self.mask))
            .as_slice()
    }

    /// Borrows the cached RLE form, converting into a temporary if not cached.
    pub(super) fn borrowed_selectors(&self) -> Cow<'_, [RowSelector]> {
        match self.selectors.get() {
            Some(selectors) => Cow::Borrowed(selectors.as_slice()),
            None => Cow::Owned(mask_to_selectors(&self.mask)),
        }
    }

    /// The RLE form, taking the cache if it was populated.
    pub(crate) fn into_selectors(self) -> Vec<RowSelector> {
        match self.selectors.into_inner() {
            Some(selectors) => selectors,
            None => mask_to_selectors(&self.mask),
        }
    }
}

impl Clone for MaskSelection {
    fn clone(&self) -> Self {
        // Drop the selector cache but keep the cheap count cache.
        Self {
            mask: self.mask.clone(),
            selectors: OnceLock::new(),
            count: self.count.clone(),
        }
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
///
/// [`RowSelection::iter`]: crate::arrow::arrow_reader::RowSelection::iter
/// [`RowSelection::as_mask`]: crate::arrow::arrow_reader::RowSelection::as_mask
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

/// Materialize a [`BooleanBuffer`] into its RLE form.
pub(super) fn mask_to_selectors(mask: &BooleanBuffer) -> Vec<RowSelector> {
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

/// Returns whether `mask` contains at least `min_runs` alternating set/unset runs.
///
/// Stops as soon as the requested number of runs is found, avoiding a full scan
/// when callers only need to know whether a boundary has been crossed.
pub(super) fn mask_has_at_least_runs(mask: &BooleanBuffer, min_runs: usize) -> bool {
    if min_runs == 0 {
        return true;
    }

    let total_rows = mask.len();
    if total_rows == 0 {
        return false;
    }

    let mut run_count = 0;
    let mut last_end = 0;
    for (start, end) in mask.set_slices() {
        run_count += usize::from(start > last_end) + 1;
        if run_count >= min_runs {
            return true;
        }
        last_end = end;
    }

    run_count + usize::from(last_end < total_rows) >= min_runs
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

/// Position of the highest set bit in `mask`, scanning bytes from the end.
fn last_set_bit_position(mask: &BooleanBuffer) -> Option<usize> {
    let values = mask.values();
    let offset = mask.offset();
    let end = offset + mask.len();
    for byte_idx in (offset / 8..end.div_ceil(8)).rev() {
        let byte_start = byte_idx * 8;
        let mut byte = values[byte_idx];
        if end - byte_start < 8 {
            byte &= (1u8 << (end - byte_start)) - 1;
        }
        if byte_start < offset {
            byte &= !((1u8 << (offset - byte_start)) - 1);
        }
        if byte != 0 {
            return Some(byte_start + 7 - byte.leading_zeros() as usize - offset);
        }
    }
    None
}

/// Trims trailing unset bits from a mask-backed selection.
pub(super) fn trim_mask(mask: &BooleanBuffer) -> Option<BooleanBuffer> {
    let len = mask.len();
    // Fast path: final bit set means there is nothing to trim.
    if len == 0 || mask.value(len - 1) {
        return None;
    }
    let new_len = last_set_bit_position(mask).map_or(0, |pos| pos + 1);
    Some(mask.slice(0, new_len))
}

/// Skips the first `offset` selected rows of a mask-backed selection.
/// `popcount` is the caller's (possibly cached) set-bit count of `mask`.
pub(super) fn offset_mask(mask: BooleanBuffer, offset: usize, popcount: usize) -> BooleanBuffer {
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

/// Set bits `[start, start + len)` in a zero-initialized little-endian bitmap.
fn set_bit_run(buf: &mut [u8], start: usize, len: usize) {
    if len == 0 {
        return;
    }
    let end = start + len;
    let first_byte = start / 8;
    let last_byte = (end - 1) / 8;
    let start_mask = 0xFFu8 << (start % 8);
    let end_mask = 0xFFu8 >> (8 - (end - last_byte * 8));
    if first_byte == last_byte {
        buf[first_byte] |= start_mask & end_mask;
    } else {
        buf[first_byte] |= start_mask;
        buf[first_byte + 1..last_byte].fill(0xFF);
        buf[last_byte] |= end_mask;
    }
}

/// Build a bitmap from a selector sequence by filling bytes directly.
///
/// This sits on the read hot path (`Mask` strategy over a selector-backed
/// selection) where per-selector `append_n` calls are too slow.
pub(super) fn boolean_mask_from_selectors(selectors: &[RowSelector]) -> BooleanBuffer {
    let total_rows: usize = selectors.iter().map(|s| s.row_count).sum();
    let mut buf = vec![0u8; total_rows.div_ceil(8)];
    let mut position = 0usize;
    for selector in selectors {
        if !selector.skip {
            set_bit_run(&mut buf, position, selector.row_count);
        }
        position += selector.row_count;
    }
    BooleanBuffer::new(Buffer::from(buf), 0, total_rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::arrow_reader::selection::{RowSelection, RowSelectionInner};
    use arrow_array::BooleanArray;
    use rand::{RngExt, rng};

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

    /// Enough runs that the RLE form is a real allocation, so the cache reuse
    /// tests can track its pointer across the conversion.
    fn interleaved_mask() -> BooleanBuffer {
        BooleanBuffer::from((0..256).map(|i| i % 3 == 0).collect::<Vec<bool>>())
    }

    fn cached_selectors_ptr(selection: &RowSelection) -> Option<*const RowSelector> {
        match &selection.inner {
            RowSelectionInner::Mask(m) => m.selectors.get().map(|s| s.as_ptr()),
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_into_selectors_takes_the_iter_cache() {
        let selection = RowSelection::from_boolean_buffer(interleaved_mask());
        let expected: Vec<RowSelector> = selection.iter().copied().collect();

        let cached_ptr = cached_selectors_ptr(&selection).expect("iter populates the cache");
        let selectors: Vec<RowSelector> = selection.into();

        assert_eq!(selectors, expected);
        // Moved out of the cache rather than re-encoded from the bitmap.
        assert_eq!(selectors.as_ptr(), cached_ptr);
    }

    #[test]
    fn test_into_selectors_without_cache_still_converts() {
        let selection = RowSelection::from_boolean_buffer(interleaved_mask());
        assert!(cached_selectors_ptr(&selection).is_none());

        let selectors: Vec<RowSelector> = selection.into();
        assert_eq!(selectors, mask_to_selectors(&interleaved_mask()));

        // `VecDeque` goes through the same path.
        let selection = RowSelection::from_boolean_buffer(interleaved_mask());
        let _ = selection.iter().count();
        let deque: std::collections::VecDeque<RowSelector> = selection.into();
        assert_eq!(Vec::from(deque), selectors);
    }

    #[test]
    fn test_borrowed_selectors_reuses_cache_without_populating_it() {
        let selection = RowSelection::from_boolean_buffer(interleaved_mask());
        let mask = match &selection.inner {
            RowSelectionInner::Mask(m) => m,
            _ => unreachable!(),
        };

        // Uncached: converts into a temporary, leaving the cache empty.
        assert!(matches!(mask.borrowed_selectors(), Cow::Owned(_)));
        assert!(mask.selectors.get().is_none());

        let expected: Vec<RowSelector> = selection.iter().copied().collect();
        let mask = match &selection.inner {
            RowSelectionInner::Mask(m) => m,
            _ => unreachable!(),
        };
        match mask.borrowed_selectors() {
            Cow::Borrowed(selectors) => assert_eq!(selectors, expected.as_slice()),
            Cow::Owned(_) => panic!("expected the cached selectors to be reused"),
        }
    }

    #[test]
    fn test_set_algebra_agrees_whether_or_not_the_cache_is_populated() {
        let bits: Vec<bool> = (0..256).map(|i| i % 3 == 0).collect();
        let other: RowSelection = RowSelection::from_filters(&[BooleanArray::from(
            (0..256).map(|i| i % 5 != 0).collect::<Vec<bool>>(),
        )]);

        let cold = RowSelection::from_boolean_buffer(BooleanBuffer::from(bits.clone()));
        let warm = RowSelection::from_boolean_buffer(BooleanBuffer::from(bits));
        let _ = warm.iter().count();

        assert_eq!(cold.intersection(&other), warm.intersection(&other));
        assert_eq!(other.intersection(&cold), other.intersection(&warm));
        assert_eq!(cold.union(&other), warm.union(&other));
        assert_eq!(other.union(&cold), other.union(&warm));
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
    fn test_boolean_mask_from_selectors_fuzz_equivalence() {
        let mut rand = rng();
        for _ in 0..200 {
            let n_selectors = rand.random_range(0..30);
            let mut selectors = Vec::with_capacity(n_selectors);
            for _ in 0..n_selectors {
                selectors.push(RowSelector {
                    row_count: rand.random_range(0..40),
                    skip: rand.random_bool(0.5),
                });
            }

            let expected = {
                let total_rows: usize = selectors.iter().map(|s| s.row_count).sum();
                let mut builder = BooleanBufferBuilder::new(total_rows);
                for selector in &selectors {
                    builder.append_n(selector.row_count, !selector.skip);
                }
                builder.finish()
            };

            assert_eq!(boolean_mask_from_selectors(&selectors), expected);
        }
    }

    #[test]
    fn test_mask_has_at_least_runs() {
        fn assert_run_count(bits: Vec<bool>, expected_runs: usize) {
            let mask = BooleanBuffer::from(bits);
            for min_runs in 0..=expected_runs + 2 {
                assert_eq!(
                    mask_has_at_least_runs(&mask, min_runs),
                    expected_runs >= min_runs,
                    "expected {expected_runs} runs with boundary {min_runs}"
                );
            }
        }

        assert_run_count(vec![], 0);
        assert_run_count(vec![false; 8], 1);
        assert_run_count(vec![true; 8], 1);
        assert_run_count(vec![false, false, true, true, false], 3);
        assert_run_count(vec![true, false, true, false, true, false], 6);

        // Exercise the unaligned iterator path as mask-backed selections can be slices.
        let mask = BooleanBuffer::from(vec![true, false, false, true, true, false, true, true])
            .slice(1, 6);
        for min_runs in 0..=6 {
            assert_eq!(mask_has_at_least_runs(&mask, min_runs), 4 >= min_runs);
        }
    }

    #[test]
    fn test_trim_mask_fuzz_equivalence() {
        let mut rand = rng();
        for _ in 0..200 {
            let len = rand.random_range(0..200);
            let bits: Vec<bool> = (0..len).map(|_| rand.random_bool(0.3)).collect();
            let full = BooleanBuffer::from(bits.clone());
            // Exercise non-zero bit offsets via slicing
            let start = rand.random_range(0..=len);
            let slice_len = rand.random_range(0..=(len - start));
            let mask = full.slice(start, slice_len);

            let expected_len = bits[start..start + slice_len]
                .iter()
                .rposition(|&b| b)
                .map_or(0, |pos| pos + 1);

            match trim_mask(&mask) {
                Some(trimmed) => {
                    assert_ne!(expected_len, mask.len());
                    assert_eq!(trimmed.len(), expected_len);
                    assert_eq!(trimmed, mask.slice(0, expected_len));
                }
                None => assert_eq!(expected_len, mask.len()),
            }
        }
    }

    #[test]
    fn test_split_off_propagates_cached_count() {
        let bits = vec![true, false, true, true, false, false, true, false];
        let mut selection = RowSelection::from_boolean_buffer(BooleanBuffer::from(bits));
        // Populate the count cache, then verify both split halves.
        assert_eq!(selection.row_count(), 4);
        let head = selection.split_off(3);
        assert_eq!(head.row_count(), 2);
        assert_eq!(selection.row_count(), 2);
        let tail_fresh = RowSelection::from_boolean_buffer(BooleanBuffer::from(vec![
            true, false, false, true, false,
        ]));
        assert_eq!(selection, tail_fresh);

        // Splitting past the end keeps the whole selection as the head
        let head = selection.split_off(100);
        assert_eq!(head.row_count(), 2);
        assert_eq!(selection.row_count(), 0);
    }

    #[test]
    fn test_trim_and_offset_and_limit_preserve_cached_count() {
        let bits = vec![true, true, false, true, false, false];
        let selection = RowSelection::from_boolean_buffer(BooleanBuffer::from(bits.clone()));
        assert_eq!(selection.row_count(), 3);

        let trimmed = selection.trim();
        assert!(trimmed.as_mask().is_some());
        assert_eq!(trimmed.as_mask().unwrap().len(), 4);
        assert_eq!(trimmed.row_count(), 3);

        let offset = trimmed.clone().offset(1);
        assert_eq!(offset.row_count(), 2);

        let limited = trimmed.limit(2);
        assert_eq!(limited.row_count(), 2);
    }
}
