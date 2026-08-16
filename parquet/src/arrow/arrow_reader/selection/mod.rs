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

//! Logic for selecting which rows to read: [`RowSelection`] and [`RowSelector`]
//!
//! This module holds [`RowSelection`] and its public API, which dispatches to
//! one of the two backings depending on how the selection is stored:
//!
//! * `selector`: the run length backing, [`RowSelector`] and its primitives
//! * `boolean`: the bitmap backing, `MaskSelection` and its primitives
//!
//! The remaining modules hold the operations that are common to both:
//!
//! * `algebra`: `and_then`, `intersection` and `union`
//! * `ranges`: mapping a [`RowSelection`] onto page and batch ranges
//! * `cursor`: iterating a [`RowSelection`] while reading

use crate::file::page_index::offset_index::PageLocation;
use arrow_array::{Array, BooleanArray};
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder};
use arrow_select::filter::SlicesIterator;
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::ops::Range;

mod algebra;
mod boolean;
mod cursor;
mod ranges;
mod selector;

use algebra::{
    and_then_mask, and_then_row_selections, and_then_selectors_with_mask, intersect_masks,
    intersect_row_selections, union_masks, union_row_selections,
};
pub use boolean::MaskRunIter;
use boolean::{
    MaskSelection, limit_mask, mask_has_at_least_runs, offset_mask, split_off_mask, trim_mask,
};
pub(crate) use cursor::{LoadedRowRanges, MaskCursor, RowSelectionStrategy};
pub use cursor::{RowSelectionCursor, RowSelectionPolicy};
use ranges::{expand_to_batch_boundaries_from_selectors, scan_ranges_from_selectors};
pub use selector::RowSelector;
use selector::{limit_selectors, offset_selectors, split_off_selectors};

/// [`RowSelection`] represents selecting a subset of rows
/// when scanning a parquet file.
///
/// This is applied prior to reading column data, and can therefore
/// be used to skip IO to fetch data into memory
///
/// A typical use-case would be using the [`PageIndex`] to filter out rows
/// that don't satisfy a predicate
///
/// Depending on the pattern of rows to be selected, [`RowSelection`] has
/// either a bitmap or an RLE ([`RowSelector`]) based implementation.
///
/// # Example
/// ```
/// use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
///
/// let selectors = vec![
///     RowSelector::skip(5),
///     RowSelector::select(5),
///     RowSelector::select(5),
///     RowSelector::skip(5),
/// ];
///
/// // Creating a selection will combine adjacent selectors
/// let selection: RowSelection = selectors.into();
///
/// let expected = vec![
///     RowSelector::skip(5),
///     RowSelector::select(10),
///     RowSelector::skip(5),
/// ];
///
/// let actual: Vec<RowSelector> = selection.into();
/// assert_eq!(actual, expected);
///
/// // you can also create a selection from consecutive ranges
/// let ranges = vec![5..10, 10..15];
/// let selection =
///   RowSelection::from_consecutive_ranges(ranges.into_iter(), 20);
/// let actual: Vec<RowSelector> = selection.into();
/// assert_eq!(actual, expected);
///
/// // or directly from a packed bitmap, when the upstream producer already
/// // has one. The bitmap is kept as-is rather than run-length-encoded.
/// use arrow_buffer::BooleanBuffer;
/// let mask = BooleanBuffer::from(vec![true, false, true, true]);
/// let selection = RowSelection::from_boolean_buffer(mask);
/// assert_eq!(selection.row_count(), 3);
/// ```
///
/// An RLE ([`RowSelector`]) backed [`RowSelection`] maintains the following
/// invariants (they do not apply to the bitmap backed implementation):
///
/// * It contains no [`RowSelector`] of 0 rows
/// * Consecutive [`RowSelector`]s alternate skipping or selecting rows
///
/// [`PageIndex`]: crate::file::page_index::column_index::ColumnIndexMetaData
#[derive(Default, Clone)]
pub struct RowSelection {
    inner: RowSelectionInner,
}

/// Internal storage for [`RowSelection`].
#[derive(Debug, Clone)]
pub(crate) enum RowSelectionInner {
    Selectors(Vec<RowSelector>),
    Mask(Box<MaskSelection>),
}

impl Default for RowSelectionInner {
    fn default() -> Self {
        Self::Selectors(Vec::new())
    }
}

impl std::fmt::Debug for RowSelection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.inner {
            RowSelectionInner::Selectors(s) => f
                .debug_struct("RowSelection")
                .field("selectors", s)
                .finish(),
            RowSelectionInner::Mask(m) => f
                .debug_struct("RowSelection")
                .field("mask_len", &m.mask().len())
                .finish_non_exhaustive(),
        }
    }
}

impl PartialEq for RowSelection {
    fn eq(&self, other: &Self) -> bool {
        match (&self.inner, &other.inner) {
            (RowSelectionInner::Selectors(a), RowSelectionInner::Selectors(b)) => a == b,
            (RowSelectionInner::Mask(a), RowSelectionInner::Mask(b)) => a.mask() == b.mask(),
            (RowSelectionInner::Mask(mask), RowSelectionInner::Selectors(selectors))
            | (RowSelectionInner::Selectors(selectors), RowSelectionInner::Mask(mask)) => {
                if selectors
                    .iter()
                    .try_fold(0usize, |acc, selector| acc.checked_add(selector.row_count))
                    != Some(mask.mask().len())
                {
                    return false;
                }

                let mut slices = mask.mask().set_slices().peekable();
                let mut cursor = 0usize;

                for selector in selectors {
                    let end = cursor + selector.row_count;

                    if selector.skip {
                        if slices.peek().is_some_and(|(start, _)| *start < end) {
                            return false;
                        }
                    } else {
                        match slices.next() {
                            Some((start, slice_end)) if start == cursor && slice_end == end => {}
                            _ => return false,
                        }
                    }

                    cursor = end;
                }

                slices.next().is_none()
            }
        }
    }
}

impl Eq for RowSelection {}

impl RowSelection {
    /// Not `pub`: unlike `From<Vec<RowSelector>>`, this performs no
    /// validation/normalization of the selectors (e.g. combining adjacent
    /// selectors), so callers must uphold the invariants themselves.
    fn from_selectors(selectors: Vec<RowSelector>) -> Self {
        Self {
            inner: RowSelectionInner::Selectors(selectors),
        }
    }

    /// Create a [`RowSelection`] from a packed [`BooleanBuffer`].
    ///
    /// Each set bit selects a row, each unset bit skips one. Unlike
    /// [`Self::from_filters`], the bitmap is kept as-is rather than
    /// eagerly run-length-encoded. [`Self::iter`] materializes and caches the
    /// RLE form on first use; use [`MaskRunIter`] to stream the RLE form
    /// directly from the bitmap.
    pub fn from_boolean_buffer(mask: BooleanBuffer) -> Self {
        Self {
            inner: RowSelectionInner::Mask(Box::new(MaskSelection::new(mask))),
        }
    }

    fn from_mask_selection(mask: MaskSelection) -> Self {
        Self {
            inner: RowSelectionInner::Mask(Box::new(mask)),
        }
    }

    /// Returns the underlying mask if this selection is mask-backed.
    ///
    /// Public so that engines composing selections (e.g. DataFusion's
    /// [`ParquetAccessPlan::into_overall_row_selection`]) can concatenate
    /// mask-backed selections without materialising the RLE form.
    ///
    /// [`ParquetAccessPlan::into_overall_row_selection`]: https://docs.rs/datafusion-datasource-parquet/latest/datafusion_datasource_parquet/access_plan/struct.ParquetAccessPlan.html#method.into_overall_row_selection
    pub fn as_mask(&self) -> Option<&BooleanBuffer> {
        match &self.inner {
            RowSelectionInner::Mask(m) => Some(m.mask()),
            _ => None,
        }
    }

    /// Consume the selection and return its internal storage.
    pub(crate) fn into_inner(self) -> RowSelectionInner {
        self.inner
    }

    /// Choose the automatic materialisation strategy without converting between
    /// selector and mask backing.
    #[inline]
    pub(crate) fn auto_selection_strategy(&self, threshold: usize) -> RowSelectionStrategy {
        let (total_rows, effective_count) = match &self.inner {
            RowSelectionInner::Selectors(selectors) => {
                selectors.iter().fold((0usize, 0usize), |(rows, count), s| {
                    if s.row_count > 0 {
                        (rows + s.row_count, count + 1)
                    } else {
                        (rows, count)
                    }
                })
            }
            RowSelectionInner::Mask(mask) => {
                let mask = mask.mask();
                let total_rows = mask.len();

                if total_rows == 0 {
                    return RowSelectionStrategy::Mask;
                }

                // A mask is preferred when:
                //
                // total_rows < run_count * threshold
                //
                // Therefore only scan until the first run count that can make
                // the inequality true. Fragmented masks normally reach this
                // boundary near the start instead of enumerating every run.
                let min_mask_runs = total_rows
                    .checked_div(threshold)
                    .and_then(|max_selector_runs| max_selector_runs.checked_add(1));

                return match min_mask_runs {
                    Some(min_runs) if mask_has_at_least_runs(mask, min_runs) => {
                        RowSelectionStrategy::Mask
                    }
                    _ => RowSelectionStrategy::Selectors,
                };
            }
        };

        if effective_count == 0 {
            return RowSelectionStrategy::Mask;
        }

        if total_rows < effective_count.saturating_mul(threshold) {
            RowSelectionStrategy::Mask
        } else {
            RowSelectionStrategy::Selectors
        }
    }

    #[cfg(test)]
    fn selectors(&self) -> Vec<RowSelector> {
        self.iter().copied().collect()
    }

    fn into_selectors_vec(self) -> Vec<RowSelector> {
        match self.inner {
            RowSelectionInner::Selectors(s) => s,
            RowSelectionInner::Mask(m) => (*m).into_selectors(),
        }
    }

    /// Creates a [`RowSelection`] from a slice of [`BooleanArray`]
    ///
    /// # Panics
    ///
    /// Panics if any of the [`BooleanArray`] contain nulls
    pub fn from_filters(filters: &[BooleanArray]) -> Self {
        let mut next_offset = 0;
        let total_rows = filters.iter().map(|x| x.len()).sum();

        let iter = filters.iter().flat_map(|filter| {
            let offset = next_offset;
            next_offset += filter.len();
            assert_eq!(filter.null_count(), 0);
            SlicesIterator::new(filter).map(move |(start, end)| start + offset..end + offset)
        });

        Self::from_consecutive_ranges(iter, total_rows)
    }

    /// Creates a [`RowSelection`] from an iterator of consecutive ranges to keep
    pub fn from_consecutive_ranges<I: Iterator<Item = Range<usize>>>(
        ranges: I,
        total_rows: usize,
    ) -> Self {
        let mut selectors: Vec<RowSelector> = Vec::with_capacity(ranges.size_hint().0);
        let mut last_end = 0;
        for range in ranges {
            let len = range.end - range.start;
            if len == 0 {
                continue;
            }

            match range.start.cmp(&last_end) {
                Ordering::Equal => match selectors.last_mut() {
                    Some(last) => last.row_count = last.row_count.checked_add(len).unwrap(),
                    None => selectors.push(RowSelector::select(len)),
                },
                Ordering::Greater => {
                    selectors.push(RowSelector::skip(range.start - last_end));
                    selectors.push(RowSelector::select(len))
                }
                Ordering::Less => panic!("out of order"),
            }
            last_end = range.end;
        }

        if last_end != total_rows {
            selectors.push(RowSelector::skip(total_rows - last_end))
        }

        Self::from_selectors(selectors)
    }

    /// Given an offset index, return the byte ranges for all data pages selected by `self`
    ///
    /// This is useful for determining what byte ranges to fetch from underlying storage
    ///
    /// Note: this method does not make any effort to combine consecutive ranges, nor coalesce
    /// ranges that are close together. This is instead delegated to the IO subsystem to optimise,
    /// e.g. `ObjectStore::get_ranges` in the [`object_store`] crate
    ///
    /// [`object_store`]: https://crates.io/crates/object_store
    pub fn scan_ranges(&self, page_locations: &[PageLocation]) -> Vec<Range<u64>> {
        match &self.inner {
            RowSelectionInner::Selectors(selectors) => {
                scan_ranges_from_selectors(selectors.iter().copied(), page_locations)
            }
            RowSelectionInner::Mask(mask) => {
                scan_ranges_from_selectors(MaskRunIter::new(mask.mask()), page_locations)
            }
        }
    }

    /// Returns the complete row ranges of the pages selected by [`Self::scan_ranges`].
    pub(crate) fn row_ranges_for_selected_pages(
        &self,
        page_locations: &[PageLocation],
        total_rows: usize,
    ) -> Vec<Range<usize>> {
        let mut selected_pages = self.scan_ranges(page_locations).into_iter().peekable();
        let mut row_ranges = Vec::new();

        for (idx, page) in page_locations.iter().enumerate() {
            let Some(selected_page) = selected_pages.peek() else {
                break;
            };
            if selected_page.start != page.offset as u64 {
                continue;
            }
            selected_pages.next();

            let end = page_locations
                .get(idx + 1)
                .map(|next| next.first_row_index as usize)
                .unwrap_or(total_rows);
            row_ranges.push(page.first_row_index as usize..end);
        }

        row_ranges
    }

    /// Splits off the first `row_count` from this [`RowSelection`]
    pub fn split_off(&mut self, row_count: usize) -> Self {
        match std::mem::take(&mut self.inner) {
            RowSelectionInner::Mask(mask) => {
                let total = mask.cached_count();
                let (head, tail) = split_off_mask((*mask).into_mask(), row_count);
                // Popcount only the head and derive the tail by subtraction, so
                // repeated splits stay O(bitmap) overall.
                let (head, tail) = match total {
                    Some(total) => {
                        let head_count = if tail.is_empty() {
                            total
                        } else {
                            head.count_set_bits()
                        };
                        (
                            MaskSelection::with_count(head, head_count),
                            MaskSelection::with_count(tail, total - head_count),
                        )
                    }
                    None => (MaskSelection::new(head), MaskSelection::new(tail)),
                };
                self.inner = RowSelectionInner::Mask(Box::new(tail));
                Self::from_mask_selection(head)
            }
            RowSelectionInner::Selectors(selectors) => {
                let (head, tail) = split_off_selectors(selectors, row_count);
                self.inner = RowSelectionInner::Selectors(tail);
                Self::from_selectors(head)
            }
        }
    }

    /// returns a [`RowSelection`] representing rows that are selected in both
    /// input [`RowSelection`]s.
    ///
    /// This is equivalent to the logical `AND` / conjunction of the two
    /// selections.
    ///
    /// # Example
    /// If `N` means the row is not selected, and `Y` means it is
    /// selected:
    ///
    /// ```text
    /// self:     NNNNNNNNNNNNYYYYYYYYYYYYYYYYYYYYYYNNNYYYYY
    /// other:                YYYYYNNNNYYYYYYYYYYYYY   YYNNN
    ///
    /// returned: NNNNNNNNNNNNYYYYYNNNNYYYYYYYYYYYYYNNNYYNNN
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if `other` does not have a length equal to the number of rows selected
    /// by this RowSelection
    ///
    pub fn and_then(&self, other: &Self) -> Self {
        match (&self.inner, &other.inner) {
            (RowSelectionInner::Mask(mask), _) => {
                Self::from_boolean_buffer(and_then_mask(mask.mask(), other))
            }
            (RowSelectionInner::Selectors(first), RowSelectionInner::Selectors(second)) => {
                and_then_row_selections(first, second)
            }
            (RowSelectionInner::Selectors(first), RowSelectionInner::Mask(second)) => {
                and_then_selectors_with_mask(first, second.mask())
            }
        }
    }

    /// Compute the intersection of two [`RowSelection`]
    /// For example:
    /// self:      NNYYYYNNYYNYN
    /// other:     NYNNNNNNY
    ///
    /// returned:  NNNNNNNNYYNYN
    pub fn intersection(&self, other: &Self) -> Self {
        match (&self.inner, &other.inner) {
            (RowSelectionInner::Mask(l), RowSelectionInner::Mask(r)) => {
                Self::from_boolean_buffer(intersect_masks(l.mask(), r.mask()))
            }
            (RowSelectionInner::Selectors(l), RowSelectionInner::Selectors(r)) => {
                intersect_row_selections(l, r)
            }
            (RowSelectionInner::Selectors(l), RowSelectionInner::Mask(r)) => {
                intersect_row_selections(l, &r.borrowed_selectors())
            }
            (RowSelectionInner::Mask(l), RowSelectionInner::Selectors(r)) => {
                intersect_row_selections(&l.borrowed_selectors(), r)
            }
        }
    }

    /// Compute the union of two [`RowSelection`]
    /// For example:
    /// self:      NNYYYYNNYYNYN
    /// other:     NYNNNNNNN
    ///
    /// returned:  NYYYYYNNYYNYN
    pub fn union(&self, other: &Self) -> Self {
        match (&self.inner, &other.inner) {
            (RowSelectionInner::Mask(l), RowSelectionInner::Mask(r)) => {
                Self::from_boolean_buffer(union_masks(l.mask(), r.mask()))
            }
            (RowSelectionInner::Selectors(l), RowSelectionInner::Selectors(r)) => {
                union_row_selections(l, r)
            }
            (RowSelectionInner::Selectors(l), RowSelectionInner::Mask(r)) => {
                union_row_selections(l, &r.borrowed_selectors())
            }
            (RowSelectionInner::Mask(l), RowSelectionInner::Selectors(r)) => {
                union_row_selections(&l.borrowed_selectors(), r)
            }
        }
    }

    /// Returns `true` if this [`RowSelection`] selects any rows
    pub fn selects_any(&self) -> bool {
        match &self.inner {
            RowSelectionInner::Selectors(s) => s.iter().any(|x| !x.skip),
            RowSelectionInner::Mask(m) => match m.cached_count() {
                Some(count) => count > 0,
                None => m.mask().set_indices().next().is_some(),
            },
        }
    }

    /// Trims this [`RowSelection`] removing any trailing skips
    pub(crate) fn trim(self) -> Self {
        match self.inner {
            RowSelectionInner::Mask(m) => {
                let trimmed = trim_mask(m.mask());
                let cached_count = m.cached_count();
                match trimmed {
                    // Trimming only drops trailing unset bits; the count is unchanged.
                    Some(mask) => match cached_count {
                        Some(count) => {
                            Self::from_mask_selection(MaskSelection::with_count(mask, count))
                        }
                        None => Self::from_boolean_buffer(mask),
                    },
                    // Nothing to trim, hand the existing box back untouched.
                    None => Self {
                        inner: RowSelectionInner::Mask(m),
                    },
                }
            }
            RowSelectionInner::Selectors(mut selectors) => {
                while selectors.last().map(|x| x.skip).unwrap_or(false) {
                    selectors.pop();
                }
                Self::from_selectors(selectors)
            }
        }
    }

    /// Applies an offset to this [`RowSelection`], skipping the first `offset` selected rows
    pub(crate) fn offset(self, offset: usize) -> Self {
        if offset == 0 {
            return self;
        }

        match self.inner {
            RowSelectionInner::Mask(mask) => {
                let count = mask.count();
                let buffer = offset_mask((*mask).into_mask(), offset, count);
                Self::from_mask_selection(MaskSelection::with_count(
                    buffer,
                    count.saturating_sub(offset),
                ))
            }
            RowSelectionInner::Selectors(selectors) => {
                Self::from_selectors(offset_selectors(selectors, offset))
            }
        }
    }

    /// Limit this [`RowSelection`] to only select `limit` rows
    pub(crate) fn limit(self, limit: usize) -> Self {
        match self.inner {
            RowSelectionInner::Mask(mask) => {
                let cached = mask.cached_count();
                let buffer = limit_mask((*mask).into_mask(), limit);
                match cached {
                    Some(count) => Self::from_mask_selection(MaskSelection::with_count(
                        buffer,
                        count.min(limit),
                    )),
                    None => Self::from_boolean_buffer(buffer),
                }
            }
            RowSelectionInner::Selectors(selectors) => {
                Self::from_selectors(limit_selectors(selectors, limit))
            }
        }
    }

    /// Returns an iterator over the [`RowSelector`]s for this
    /// [`RowSelection`].
    ///
    /// Mask-backed selections materialize a `Vec<RowSelector>` cache on first
    /// call (one allocation, `O(set_slices)` work) so the iterator can hand out
    /// `&RowSelector`; the cache is not copied on clone. For single-pass walks
    /// over mask-backed selections, prefer streaming directly via
    /// [`Self::as_mask`] + [`MaskRunIter::new`] — that path is allocation-free
    /// and avoids populating the cache.
    pub fn iter(&self) -> impl Iterator<Item = &RowSelector> {
        match &self.inner {
            RowSelectionInner::Selectors(s) => s.iter(),
            RowSelectionInner::Mask(m) => m.selectors().iter(),
        }
    }

    /// Returns the number of selected rows
    pub fn row_count(&self) -> usize {
        match &self.inner {
            RowSelectionInner::Selectors(s) => {
                s.iter().filter(|x| !x.skip).map(|x| x.row_count).sum()
            }
            RowSelectionInner::Mask(m) => m.count(),
        }
    }

    /// Returns the number of de-selected rows
    pub fn skipped_row_count(&self) -> usize {
        match &self.inner {
            RowSelectionInner::Selectors(s) => {
                s.iter().filter(|x| x.skip).map(|x| x.row_count).sum()
            }
            RowSelectionInner::Mask(m) => m.mask().len() - m.count(),
        }
    }

    /// Expands the selection to align with batch boundaries.
    /// This is needed when using cached array readers to ensure that
    /// the cached data covers full batches.
    pub(crate) fn expand_to_batch_boundaries(&self, batch_size: usize, total_rows: usize) -> Self {
        if batch_size == 0 {
            return self.clone();
        }

        match &self.inner {
            RowSelectionInner::Selectors(selectors) => expand_to_batch_boundaries_from_selectors(
                selectors.iter().copied(),
                batch_size,
                total_rows,
            ),
            RowSelectionInner::Mask(mask) => expand_to_batch_boundaries_from_selectors(
                MaskRunIter::new(mask.mask()),
                batch_size,
                total_rows,
            ),
        }
    }
}

impl From<Vec<RowSelector>> for RowSelection {
    fn from(selectors: Vec<RowSelector>) -> Self {
        selectors.into_iter().collect()
    }
}

impl From<BooleanBuffer> for RowSelection {
    fn from(mask: BooleanBuffer) -> Self {
        Self::from_boolean_buffer(mask)
    }
}

impl FromIterator<RowSelector> for RowSelection {
    fn from_iter<T: IntoIterator<Item = RowSelector>>(iter: T) -> Self {
        let iter = iter.into_iter();

        // Capacity before filter
        let mut selectors = Vec::with_capacity(iter.size_hint().0);

        let mut filtered = iter.filter(|x| x.row_count != 0);
        if let Some(x) = filtered.next() {
            selectors.push(x);
        }

        for s in filtered {
            if s.row_count == 0 {
                continue;
            }

            // Combine consecutive selectors
            let last = selectors.last_mut().unwrap();
            if last.skip == s.skip {
                last.row_count = last.row_count.checked_add(s.row_count).unwrap();
            } else {
                selectors.push(s)
            }
        }

        Self::from_selectors(selectors)
    }
}

impl From<RowSelection> for Vec<RowSelector> {
    fn from(r: RowSelection) -> Self {
        r.into_selectors_vec()
    }
}

impl From<RowSelection> for VecDeque<RowSelector> {
    fn from(r: RowSelection) -> Self {
        r.into_selectors_vec().into()
    }
}

impl FromIterator<RowSelection> for RowSelection {
    /// Concatenate multiple [`RowSelection`]s in iterator order.
    ///
    /// When every input is mask-backed the result stays mask-backed
    /// (`BooleanBuffer`s are appended); otherwise falls back to flattening
    /// through the per-`RowSelector` form.
    fn from_iter<T: IntoIterator<Item = RowSelection>>(iter: T) -> Self {
        let items: Vec<RowSelection> = iter.into_iter().collect();

        let all_mask = items
            .iter()
            .all(|s| matches!(&s.inner, RowSelectionInner::Mask(_)));

        if all_mask {
            let total_len: usize = items
                .iter()
                .map(|s| match &s.inner {
                    RowSelectionInner::Mask(m) => m.mask().len(),
                    RowSelectionInner::Selectors(_) => unreachable!(),
                })
                .sum();
            let mut builder = BooleanBufferBuilder::new(total_len);
            for item in items {
                match item.into_inner() {
                    RowSelectionInner::Mask(m) => builder.append_buffer(m.mask()),
                    RowSelectionInner::Selectors(_) => unreachable!(),
                }
            }
            return Self::from_boolean_buffer(builder.finish());
        }

        items
            .into_iter()
            .flat_map(|s| s.into_selectors_vec())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_offset_zero_and_zero_batch_expand_are_identity() {
        let selection =
            RowSelection::from_boolean_buffer(BooleanBuffer::from(vec![true, false, true]));
        assert_eq!(selection.clone().offset(0), selection);
        assert_eq!(selection.expand_to_batch_boundaries(0, 3), selection);
    }

    #[test]
    fn test_from_filters() {
        let filters = vec![
            BooleanArray::from(vec![false, false, false, true, true, true, true]),
            BooleanArray::from(vec![true, true, false, false, true, true, true]),
            BooleanArray::from(vec![false, false, false, false]),
            BooleanArray::from(Vec::<bool>::new()),
        ];

        let selection = RowSelection::from_filters(&filters[..1]);
        assert!(selection.selects_any());
        assert_eq!(
            selection.selectors(),
            vec![RowSelector::skip(3), RowSelector::select(4)]
        );

        let selection = RowSelection::from_filters(&filters[..2]);
        assert!(selection.selects_any());
        assert_eq!(
            selection.selectors(),
            vec![
                RowSelector::skip(3),
                RowSelector::select(6),
                RowSelector::skip(2),
                RowSelector::select(3)
            ]
        );

        let selection = RowSelection::from_filters(&filters);
        assert!(selection.selects_any());
        assert_eq!(
            selection.selectors(),
            vec![
                RowSelector::skip(3),
                RowSelector::select(6),
                RowSelector::skip(2),
                RowSelector::select(3),
                RowSelector::skip(4)
            ]
        );

        let selection = RowSelection::from_filters(&filters[2..3]);
        assert!(!selection.selects_any());
        assert_eq!(selection.selectors(), vec![RowSelector::skip(4)]);
    }

    #[test]
    fn test_iter() {
        // use the iter() API to show it does what is expected and
        // avoid accidental deletion
        let selectors = vec![
            RowSelector::select(3),
            RowSelector::skip(33),
            RowSelector::select(4),
        ];

        let round_tripped: Vec<RowSelector> = RowSelection::from(selectors.clone())
            .iter()
            .copied()
            .collect();
        assert_eq!(selectors, round_tripped);
    }

    #[test]
    fn test_row_count() {
        let selection = RowSelection::from(vec![
            RowSelector::skip(34),
            RowSelector::select(12),
            RowSelector::skip(3),
            RowSelector::select(35),
        ]);

        assert_eq!(selection.row_count(), 12 + 35);
        assert_eq!(selection.skipped_row_count(), 34 + 3);

        let selection = RowSelection::from(vec![RowSelector::select(12), RowSelector::select(35)]);

        assert_eq!(selection.row_count(), 12 + 35);
        assert_eq!(selection.skipped_row_count(), 0);

        let selection = RowSelection::from(vec![RowSelector::skip(34), RowSelector::skip(3)]);

        assert_eq!(selection.row_count(), 0);
        assert_eq!(selection.skipped_row_count(), 34 + 3);

        let selection = RowSelection::from(vec![]);

        assert_eq!(selection.row_count(), 0);
        assert_eq!(selection.skipped_row_count(), 0);
    }

    #[test]
    fn test_mixed_backing_equality_mismatches() {
        let mask =
            RowSelection::from_boolean_buffer(BooleanBuffer::from(vec![true, false, true, true]));

        // Total row counts differ
        let longer = RowSelection::from(vec![
            RowSelector::select(1),
            RowSelector::skip(1),
            RowSelector::select(2),
            RowSelector::skip(1),
        ]);
        assert_ne!(mask, longer);
        assert_ne!(longer, mask);

        // A selected bit falls inside a skip run
        let skip_overlap = RowSelection::from(vec![RowSelector::skip(2), RowSelector::select(2)]);
        assert_ne!(mask, skip_overlap);

        // Select run boundaries do not line up
        let misaligned = RowSelection::from(vec![
            RowSelector::select(2),
            RowSelector::skip(1),
            RowSelector::select(1),
        ]);
        assert_ne!(mask, misaligned);

        let equal = RowSelection::from(vec![
            RowSelector::select(1),
            RowSelector::skip(1),
            RowSelector::select(2),
        ]);
        assert_eq!(mask, equal);
        assert_eq!(equal, mask);
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
