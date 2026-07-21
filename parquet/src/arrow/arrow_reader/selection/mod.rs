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

use crate::file::page_index::offset_index::PageLocation;
use arrow_array::{Array, BooleanArray};
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder};
use arrow_select::filter::SlicesIterator;
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;

mod boolean;
pub use boolean::MaskRunIter;
pub(crate) use boolean::{MaskCursor, mask_to_selectors};
use boolean::{
    MaskSelection, and_then_mask, boolean_mask_from_selectors, intersect_masks, limit_mask,
    mask_has_at_least_runs, offset_mask, split_off_mask, trim_mask, union_masks,
};

/// Policy for picking a strategy to materialize [`RowSelection`] during execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RowSelectionPolicy {
    /// Use a queue of [`RowSelector`] values
    Selectors,
    /// Use a boolean mask to materialize the selection
    Mask,
    /// Choose between [`Self::Mask`] and [`Self::Selectors`] based on selector density
    Auto {
        /// Average selector length below which masks are preferred
        threshold: usize,
    },
}

impl Default for RowSelectionPolicy {
    fn default() -> Self {
        Self::Auto { threshold: 32 }
    }
}

/// Fully resolved strategy for materializing [`RowSelection`] during execution.
///
/// This is determined by [`RowSelectionPolicy`], including selector density for
/// [`RowSelectionPolicy::Auto`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RowSelectionStrategy {
    /// Use a queue of [`RowSelector`] values
    Selectors,
    /// Use a boolean mask to materialize the selection
    Mask,
}

/// [`RowSelection`] is a collection of [`RowSelector`] used to skip rows when
/// scanning a parquet file
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct RowSelector {
    /// The number of rows
    pub row_count: usize,

    /// If true, skip `row_count` rows
    pub skip: bool,
}

impl RowSelector {
    /// Select `row_count` rows
    pub fn select(row_count: usize) -> Self {
        Self {
            row_count,
            skip: false,
        }
    }

    /// Skip `row_count` rows
    pub fn skip(row_count: usize) -> Self {
        Self {
            row_count,
            skip: true,
        }
    }
}

/// [`RowSelection`] allows selecting or skipping a provided number of rows
/// when scanning the parquet file.
///
/// This is applied prior to reading column data, and can therefore
/// be used to skip IO to fetch data into memory
///
/// A typical use-case would be using the [`PageIndex`] to filter out rows
/// that don't satisfy a predicate
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
/// A [`RowSelection`] maintains the following invariants:
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

/// Borrowed iterator over the [`RowSelector`]s of a [`RowSelection`].
#[derive(Debug)]
pub struct RowSelectionIter<'a>(std::slice::Iter<'a, RowSelector>);

impl<'a> Iterator for RowSelectionIter<'a> {
    type Item = &'a RowSelector;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

#[inline]
fn scan_ranges_from_selectors<I>(selectors: I, page_locations: &[PageLocation]) -> Vec<Range<u64>>
where
    I: IntoIterator<Item = RowSelector>,
{
    let mut ranges: Vec<Range<u64>> = vec![];
    let mut row_offset = 0;

    let mut pages = page_locations.iter().peekable();
    let mut selectors = selectors.into_iter();
    let mut current_selector = selectors.next();
    let mut current_page = pages.next();

    let mut current_page_included = false;

    while let Some((selector, page)) = current_selector.as_mut().zip(current_page) {
        if !(selector.skip || current_page_included) {
            let start = page.offset as u64;
            let end = start + page.compressed_page_size as u64;
            ranges.push(start..end);
            current_page_included = true;
        }

        if let Some(next_page) = pages.peek() {
            if row_offset + selector.row_count > next_page.first_row_index as usize {
                let remaining_in_page = next_page.first_row_index as usize - row_offset;
                selector.row_count -= remaining_in_page;
                row_offset += remaining_in_page;
                current_page = pages.next();
                current_page_included = false;

                continue;
            } else {
                if row_offset + selector.row_count == next_page.first_row_index as usize {
                    current_page = pages.next();
                    current_page_included = false;
                }
                row_offset += selector.row_count;
                current_selector = selectors.next();
            }
        } else {
            if !(selector.skip || current_page_included) {
                let start = page.offset as u64;
                let end = start + page.compressed_page_size as u64;
                ranges.push(start..end);
            }
            current_selector = selectors.next()
        }
    }

    ranges
}

#[inline]
fn expand_to_batch_boundaries_from_selectors<I>(
    selectors: I,
    batch_size: usize,
    total_rows: usize,
) -> RowSelection
where
    I: IntoIterator<Item = RowSelector>,
{
    let mut expanded_ranges = Vec::new();
    let mut row_offset = 0;

    for selector in selectors {
        if selector.skip {
            row_offset += selector.row_count;
        } else {
            let start = row_offset;
            let end = row_offset + selector.row_count;

            // Expand start to batch boundary
            let expanded_start = (start / batch_size) * batch_size;
            // Expand end to batch boundary
            let expanded_end = end.div_ceil(batch_size) * batch_size;
            let expanded_end = expanded_end.min(total_rows);

            expanded_ranges.push(expanded_start..expanded_end);
            row_offset += selector.row_count;
        }
    }

    // Sort ranges by start position
    expanded_ranges.sort_by_key(|range| range.start);

    // Merge overlapping or consecutive ranges
    let mut merged_ranges: Vec<Range<usize>> = Vec::new();
    for range in expanded_ranges {
        if let Some(last) = merged_ranges.last_mut() {
            if range.start <= last.end {
                // Overlapping or consecutive - merge them
                last.end = last.end.max(range.end);
            } else {
                // No overlap - add new range
                merged_ranges.push(range);
            }
        } else {
            // First range
            merged_ranges.push(range);
        }
    }

    RowSelection::from_consecutive_ranges(merged_ranges.into_iter(), total_rows)
}

impl RowSelection {
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
    /// `ParquetAccessPlan::into_overall_row_selection`) can concatenate
    /// mask-backed selections without materialising the RLE form.
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
            RowSelectionInner::Mask(m) => mask_to_selectors(m.mask()),
        }
    }

    /// Promote a mask-backed selection to selector backing in place.
    fn selectors_mut(&mut self) -> &mut Vec<RowSelector> {
        if let RowSelectionInner::Mask(_) = &self.inner {
            let mask = match std::mem::take(&mut self.inner) {
                RowSelectionInner::Mask(m) => m,
                RowSelectionInner::Selectors(_) => unreachable!(),
            };
            self.inner = RowSelectionInner::Selectors(mask_to_selectors(mask.mask()));
        }
        match &mut self.inner {
            RowSelectionInner::Selectors(s) => s,
            RowSelectionInner::Mask(_) => unreachable!(),
        }
    }

    /// Creates a [`RowSelection`] from a slice of [`BooleanArray`]
    ///
    /// # Panic
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
    /// e.g. [`ObjectStore::get_ranges`](object_store::ObjectStore::get_ranges)
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
        if matches!(&self.inner, RowSelectionInner::Mask(_)) {
            let mask = match std::mem::take(&mut self.inner) {
                RowSelectionInner::Mask(m) => m,
                RowSelectionInner::Selectors(_) => unreachable!(),
            };
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
            return Self::from_mask_selection(head);
        }

        let selectors = self.selectors_mut();
        let mut total_count = 0;

        // Find the index where the selector exceeds the row count
        let find = selectors.iter().position(|selector| {
            total_count += selector.row_count;
            total_count > row_count
        });

        let split_idx = match find {
            Some(idx) => idx,
            None => {
                let drained = std::mem::take(selectors);
                return Self::from_selectors(drained);
            }
        };

        let mut remaining = selectors.split_off(split_idx);

        // Always present as `split_idx < selectors.len`
        let next = remaining.first_mut().unwrap();
        let overflow = total_count - row_count;

        if next.row_count != overflow {
            selectors.push(RowSelector {
                row_count: next.row_count - overflow,
                skip: next.skip,
            })
        }
        next.row_count = overflow;

        std::mem::swap(&mut remaining, selectors);
        Self::from_selectors(remaining)
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
                let mut selectors = vec![];
                let mut first = first.iter().copied().peekable();
                let mut second = MaskRunIter::new(second.mask()).peekable();
                and_then_iter(&mut selectors, &mut first, &mut second);
                Self::from_selectors(selectors)
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
                let r = mask_to_selectors(r.mask());
                intersect_row_selections(l, &r)
            }
            (RowSelectionInner::Mask(l), RowSelectionInner::Selectors(r)) => {
                let l = mask_to_selectors(l.mask());
                intersect_row_selections(&l, r)
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
        match &self.inner {
            RowSelectionInner::Mask(l) => match &other.inner {
                RowSelectionInner::Mask(r) => {
                    Self::from_boolean_buffer(union_masks(l.mask(), r.mask()))
                }
                RowSelectionInner::Selectors(r) => {
                    let l = mask_to_selectors(l.mask());
                    union_row_selections(&l, r)
                }
            },
            RowSelectionInner::Selectors(l) => match &other.inner {
                RowSelectionInner::Mask(r) => {
                    let r = mask_to_selectors(r.mask());
                    union_row_selections(l, &r)
                }
                RowSelectionInner::Selectors(r) => union_row_selections(l, r),
            },
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
    pub(crate) fn trim(mut self) -> Self {
        if let RowSelectionInner::Mask(m) = &self.inner {
            if let Some(mask) = trim_mask(m.mask()) {
                // Trimming only drops trailing unset bits; the count is unchanged.
                return match m.cached_count() {
                    Some(count) => {
                        Self::from_mask_selection(MaskSelection::with_count(mask, count))
                    }
                    None => Self::from_boolean_buffer(mask),
                };
            }
            return self;
        }
        let selectors = self.selectors_mut();
        while selectors.last().map(|x| x.skip).unwrap_or(false) {
            selectors.pop();
        }
        self
    }

    /// Applies an offset to this [`RowSelection`], skipping the first `offset` selected rows
    pub(crate) fn offset(self, offset: usize) -> Self {
        if offset == 0 {
            return self;
        }

        let mut selectors = match self.inner {
            RowSelectionInner::Mask(mask) => {
                let count = mask.count();
                let buffer = offset_mask((*mask).into_mask(), offset, count);
                return Self::from_mask_selection(MaskSelection::with_count(
                    buffer,
                    count.saturating_sub(offset),
                ));
            }
            RowSelectionInner::Selectors(selectors) => selectors,
        };
        let mut selected_count = 0;
        let mut skipped_count = 0;

        // Find the index where the selector exceeds the row count
        let find = selectors.iter().position(|selector| match selector.skip {
            true => {
                skipped_count += selector.row_count;
                false
            }
            false => {
                selected_count += selector.row_count;
                selected_count > offset
            }
        });

        let split_idx = match find {
            Some(idx) => idx,
            None => {
                selectors.clear();
                return Self::from_selectors(selectors);
            }
        };

        let mut new_selectors = Vec::with_capacity(selectors.len() - split_idx + 1);
        new_selectors.push(RowSelector::skip(skipped_count + offset));
        new_selectors.push(RowSelector::select(selected_count - offset));
        new_selectors.extend_from_slice(&selectors[split_idx + 1..]);

        Self::from_selectors(new_selectors)
    }

    /// Limit this [`RowSelection`] to only select `limit` rows
    pub(crate) fn limit(self, mut limit: usize) -> Self {
        let mut selectors = match self.inner {
            RowSelectionInner::Mask(mask) => {
                let cached = mask.cached_count();
                let buffer = limit_mask((*mask).into_mask(), limit);
                return match cached {
                    Some(count) => Self::from_mask_selection(MaskSelection::with_count(
                        buffer,
                        count.min(limit),
                    )),
                    None => Self::from_boolean_buffer(buffer),
                };
            }
            RowSelectionInner::Selectors(selectors) => selectors,
        };
        if limit == 0 {
            selectors.clear();
        }

        for (idx, selection) in selectors.iter_mut().enumerate() {
            if !selection.skip {
                if selection.row_count >= limit {
                    selection.row_count = limit;
                    selectors.truncate(idx + 1);
                    break;
                } else {
                    limit -= selection.row_count;
                }
            }
        }
        Self::from_selectors(selectors)
    }

    /// Returns a borrowed iterator yielding the [`RowSelector`]s for this selection.
    ///
    /// Mask-backed selections materialize a `Vec<RowSelector>` cache on first
    /// call (one allocation, `O(set_slices)` work) so the iterator can hand out
    /// `&RowSelector`; the cache is not copied on clone. For single-pass walks
    /// over mask-backed selections, prefer streaming directly via
    /// [`Self::as_mask`] + [`MaskRunIter::new`] — that path is allocation-free
    /// and avoids populating the cache.
    pub fn iter(&self) -> RowSelectionIter<'_> {
        match &self.inner {
            RowSelectionInner::Selectors(s) => RowSelectionIter(s.iter()),
            RowSelectionInner::Mask(m) => RowSelectionIter(m.selectors().iter()),
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

fn and_then_row_selections(first: &[RowSelector], second: &[RowSelector]) -> RowSelection {
    let mut selectors = vec![];
    let mut first = first.iter().copied().peekable();
    let mut second = second.iter().copied().peekable();
    and_then_iter(&mut selectors, &mut first, &mut second);
    RowSelection::from_selectors(selectors)
}

fn and_then_iter<I, J>(
    selectors: &mut Vec<RowSelector>,
    first: &mut std::iter::Peekable<I>,
    second: &mut std::iter::Peekable<J>,
) where
    I: Iterator<Item = RowSelector>,
    J: Iterator<Item = RowSelector>,
{
    let mut to_skip = 0;
    while let Some(b) = second.peek_mut() {
        let a = first
            .peek_mut()
            .expect("selection exceeds the number of selected rows");

        if b.row_count == 0 {
            second.next().unwrap();
            continue;
        }

        if a.row_count == 0 {
            first.next().unwrap();
            continue;
        }

        if a.skip {
            // Records were skipped when producing second
            to_skip += a.row_count;
            first.next().unwrap();
            continue;
        }

        let skip = b.skip;
        let to_process = a.row_count.min(b.row_count);

        a.row_count -= to_process;
        b.row_count -= to_process;

        match skip {
            true => to_skip += to_process,
            false => {
                if to_skip != 0 {
                    selectors.push(RowSelector::skip(to_skip));
                    to_skip = 0;
                }
                selectors.push(RowSelector::select(to_process))
            }
        }
    }

    for v in first {
        if v.row_count != 0 {
            assert!(
                v.skip,
                "selection contains less than the number of selected rows"
            );
            to_skip += v.row_count
        }
    }

    if to_skip != 0 {
        selectors.push(RowSelector::skip(to_skip));
    }
}

/// Combine two lists of `RowSelection` return the intersection of them
/// For example:
/// self:      NNYYYYNNYYNYN
/// other:     NYNNNNNNY
///
/// returned:  NNNNNNNNYYNYN
fn intersect_row_selections(left: &[RowSelector], right: &[RowSelector]) -> RowSelection {
    let mut l_iter = left.iter().copied().peekable();
    let mut r_iter = right.iter().copied().peekable();

    let iter = std::iter::from_fn(move || {
        loop {
            let l = l_iter.peek_mut();
            let r = r_iter.peek_mut();

            match (l, r) {
                (Some(a), _) if a.row_count == 0 => {
                    l_iter.next().unwrap();
                }
                (_, Some(b)) if b.row_count == 0 => {
                    r_iter.next().unwrap();
                }
                (Some(l), Some(r)) => {
                    return match (l.skip, r.skip) {
                        // Keep both ranges
                        (false, false) => {
                            if l.row_count < r.row_count {
                                r.row_count -= l.row_count;
                                l_iter.next()
                            } else {
                                l.row_count -= r.row_count;
                                r_iter.next()
                            }
                        }
                        // skip at least one
                        _ => {
                            if l.row_count < r.row_count {
                                let skip = l.row_count;
                                r.row_count -= l.row_count;
                                l_iter.next();
                                Some(RowSelector::skip(skip))
                            } else {
                                let skip = r.row_count;
                                l.row_count -= skip;
                                r_iter.next();
                                Some(RowSelector::skip(skip))
                            }
                        }
                    };
                }
                (Some(_), None) => return l_iter.next(),
                (None, Some(_)) => return r_iter.next(),
                (None, None) => return None,
            }
        }
    });

    iter.collect()
}

/// Combine two lists of `RowSelector` return the union of them
/// For example:
/// self:      NNYYYYNNYYNYN
/// other:     NYNNNNNNY
///
/// returned:  NYYYYYNNYYNYN
///
/// This can be removed from here once RowSelection::union is in parquet::arrow
fn union_row_selections(left: &[RowSelector], right: &[RowSelector]) -> RowSelection {
    let mut l_iter = left.iter().copied().peekable();
    let mut r_iter = right.iter().copied().peekable();

    let iter = std::iter::from_fn(move || {
        loop {
            let l = l_iter.peek_mut();
            let r = r_iter.peek_mut();

            match (l, r) {
                (Some(a), _) if a.row_count == 0 => {
                    l_iter.next().unwrap();
                }
                (_, Some(b)) if b.row_count == 0 => {
                    r_iter.next().unwrap();
                }
                (Some(l), Some(r)) => {
                    return match (l.skip, r.skip) {
                        // Skip both ranges
                        (true, true) => {
                            if l.row_count < r.row_count {
                                let skip = l.row_count;
                                r.row_count -= l.row_count;
                                l_iter.next();
                                Some(RowSelector::skip(skip))
                            } else {
                                let skip = r.row_count;
                                l.row_count -= skip;
                                r_iter.next();
                                Some(RowSelector::skip(skip))
                            }
                        }
                        // Keep rows from left
                        (false, true) => {
                            if l.row_count < r.row_count {
                                r.row_count -= l.row_count;
                                l_iter.next()
                            } else {
                                let r_row_count = r.row_count;
                                l.row_count -= r_row_count;
                                r_iter.next();
                                Some(RowSelector::select(r_row_count))
                            }
                        }
                        // Keep rows from right
                        (true, false) => {
                            if l.row_count < r.row_count {
                                let l_row_count = l.row_count;
                                r.row_count -= l_row_count;
                                l_iter.next();
                                Some(RowSelector::select(l_row_count))
                            } else {
                                l.row_count -= r.row_count;
                                r_iter.next()
                            }
                        }
                        // Keep at least one
                        _ => {
                            if l.row_count < r.row_count {
                                r.row_count -= l.row_count;
                                l_iter.next()
                            } else {
                                l.row_count -= r.row_count;
                                r_iter.next()
                            }
                        }
                    };
                }
                (Some(_), None) => return l_iter.next(),
                (None, Some(_)) => return r_iter.next(),
                (None, None) => return None,
            }
        }
    });

    iter.collect()
}

/// Cursor for iterating a selector-backed [`RowSelection`]
///
/// This is best for sparse selections where large contiguous
/// blocks of rows are selected or skipped.
#[derive(Debug)]
pub struct SelectorsCursor {
    selectors: VecDeque<RowSelector>,
    /// Current absolute offset into the selection
    position: usize,
}

impl SelectorsCursor {
    /// Returns `true` when no further rows remain
    pub fn is_empty(&self) -> bool {
        self.selectors.is_empty()
    }

    pub(crate) fn selectors_mut(&mut self) -> &mut VecDeque<RowSelector> {
        &mut self.selectors
    }

    /// Return the next [`RowSelector`]
    pub(crate) fn next_selector(&mut self) -> RowSelector {
        let selector = self.selectors.pop_front().unwrap();
        self.position += selector.row_count;
        selector
    }

    /// Return a selector to the front, rewinding the position
    pub(crate) fn return_selector(&mut self, selector: RowSelector) {
        self.position = self.position.saturating_sub(selector.row_count);
        self.selectors.push_front(selector);
    }
}

/// Row ranges whose backing pages are loaded for every projected column.
#[derive(Clone, Debug)]
pub(crate) struct LoadedRowRanges(Vec<Range<usize>>);

impl LoadedRowRanges {
    pub(crate) fn from_selection(selection: RowSelection) -> Self {
        let selectors: Vec<RowSelector> = selection.into();
        let mut position = 0;
        let ranges = selectors
            .into_iter()
            .filter_map(|selector| {
                let start = position;
                position += selector.row_count;
                (!selector.skip).then_some(start..position)
            })
            .collect();
        Self(ranges)
    }

    fn end_containing(&self, row: usize) -> Option<usize> {
        let idx = self.0.partition_point(|range| range.end <= row);
        self.0
            .get(idx)
            .filter(|range| range.start <= row)
            .map(|range| range.end)
    }

    #[cfg(test)]
    pub(crate) fn ranges(&self) -> &[Range<usize>] {
        &self.0
    }
}

/// Cursor for iterating a [`RowSelection`] during execution within a
/// [`ReadPlan`](crate::arrow::arrow_reader::ReadPlan).
///
/// This keeps per-reader state such as the current position and delegates the
/// actual storage strategy to the internal `RowSelectionInner`.
#[derive(Debug)]
pub enum RowSelectionCursor {
    /// Reading all rows
    All,
    /// Use a bitmask to back the selection (dense selections)
    Mask(MaskCursor),
    /// Use a queue of selectors to back the selection (sparse selections)
    Selectors(SelectorsCursor),
}

impl RowSelectionCursor {
    /// Create a [`MaskCursor`] cursor backed by a bitmask, from an existing set of selectors
    pub(crate) fn new_mask_from_selectors(
        selectors: Vec<RowSelector>,
        loaded_row_ranges: Option<Arc<LoadedRowRanges>>,
    ) -> Self {
        debug_assert!(
            selectors
                .last()
                .map(|selector| !selector.skip)
                .unwrap_or(true),
            "Mask selectors must not end with a skip"
        );
        Self::Mask(MaskCursor {
            mask: boolean_mask_from_selectors(&selectors),
            position: 0,
            loaded_row_ranges,
        })
    }

    /// Create a [`MaskCursor`] cursor backed by an existing bitmask.
    pub(crate) fn new_mask_from_buffer(
        mask: BooleanBuffer,
        loaded_row_ranges: Option<Arc<LoadedRowRanges>>,
    ) -> Self {
        debug_assert!(
            mask.is_empty() || mask.value(mask.len() - 1),
            "Mask selections must not end with a skip"
        );
        Self::Mask(MaskCursor {
            mask,
            position: 0,
            loaded_row_ranges,
        })
    }

    /// Create a [`RowSelectionCursor::Selectors`] from the provided selectors
    pub(crate) fn new_selectors(selectors: Vec<RowSelector>) -> Self {
        Self::Selectors(SelectorsCursor {
            selectors: selectors.into(),
            position: 0,
        })
    }

    /// Create a cursor that selects all rows
    pub(crate) fn new_all() -> Self {
        Self::All
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{Rng, rng};

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
    fn test_split_off() {
        let mut selection = RowSelection::from(vec![
            RowSelector::skip(34),
            RowSelector::select(12),
            RowSelector::skip(3),
            RowSelector::select(35),
        ]);

        let split = selection.split_off(34);
        assert_eq!(split.selectors(), vec![RowSelector::skip(34)]);
        assert_eq!(
            selection.selectors(),
            vec![
                RowSelector::select(12),
                RowSelector::skip(3),
                RowSelector::select(35)
            ]
        );

        let split = selection.split_off(5);
        assert_eq!(split.selectors(), vec![RowSelector::select(5)]);
        assert_eq!(
            selection.selectors(),
            vec![
                RowSelector::select(7),
                RowSelector::skip(3),
                RowSelector::select(35)
            ]
        );

        let split = selection.split_off(8);
        assert_eq!(
            split.selectors(),
            vec![RowSelector::select(7), RowSelector::skip(1)]
        );
        assert_eq!(
            selection.selectors(),
            vec![RowSelector::skip(2), RowSelector::select(35)]
        );

        let split = selection.split_off(200);
        assert_eq!(
            split.selectors(),
            vec![RowSelector::skip(2), RowSelector::select(35)]
        );
        assert!(selection.selectors().is_empty());
    }

    #[test]
    fn test_offset() {
        let selection = RowSelection::from(vec![
            RowSelector::select(5),
            RowSelector::skip(23),
            RowSelector::select(7),
            RowSelector::skip(33),
            RowSelector::select(6),
        ]);

        let selection = selection.offset(2);
        assert_eq!(
            selection.selectors(),
            vec![
                RowSelector::skip(2),
                RowSelector::select(3),
                RowSelector::skip(23),
                RowSelector::select(7),
                RowSelector::skip(33),
                RowSelector::select(6),
            ]
        );

        let selection = selection.offset(5);
        assert_eq!(
            selection.selectors(),
            vec![
                RowSelector::skip(30),
                RowSelector::select(5),
                RowSelector::skip(33),
                RowSelector::select(6),
            ]
        );

        let selection = selection.offset(3);
        assert_eq!(
            selection.selectors(),
            vec![
                RowSelector::skip(33),
                RowSelector::select(2),
                RowSelector::skip(33),
                RowSelector::select(6),
            ]
        );

        let selection = selection.offset(2);
        assert_eq!(
            selection.selectors(),
            vec![RowSelector::skip(68), RowSelector::select(6),]
        );

        let selection = selection.offset(3);
        assert_eq!(
            selection.selectors(),
            vec![RowSelector::skip(71), RowSelector::select(3),]
        );
    }

    #[test]
    fn test_and() {
        let mut a = RowSelection::from(vec![
            RowSelector::skip(12),
            RowSelector::select(23),
            RowSelector::skip(3),
            RowSelector::select(5),
        ]);

        let b = RowSelection::from(vec![
            RowSelector::select(5),
            RowSelector::skip(4),
            RowSelector::select(15),
            RowSelector::skip(4),
        ]);

        let mut expected = RowSelection::from(vec![
            RowSelector::skip(12),
            RowSelector::select(5),
            RowSelector::skip(4),
            RowSelector::select(14),
            RowSelector::skip(3),
            RowSelector::select(1),
            RowSelector::skip(4),
        ]);

        assert_eq!(a.and_then(&b), expected);

        a.split_off(7);
        expected.split_off(7);
        assert_eq!(a.and_then(&b), expected);

        let a = RowSelection::from(vec![RowSelector::select(5), RowSelector::skip(3)]);

        let b = RowSelection::from(vec![
            RowSelector::select(2),
            RowSelector::skip(1),
            RowSelector::select(1),
            RowSelector::skip(1),
        ]);

        assert_eq!(
            a.and_then(&b).selectors(),
            vec![
                RowSelector::select(2),
                RowSelector::skip(1),
                RowSelector::select(1),
                RowSelector::skip(4)
            ]
        );
    }

    #[test]
    fn test_combine() {
        let a = vec![
            RowSelector::skip(3),
            RowSelector::skip(3),
            RowSelector::select(10),
            RowSelector::skip(4),
        ];

        let b = vec![
            RowSelector::skip(3),
            RowSelector::skip(3),
            RowSelector::select(10),
            RowSelector::skip(4),
            RowSelector::skip(0),
        ];

        let c = vec![
            RowSelector::skip(2),
            RowSelector::skip(4),
            RowSelector::select(3),
            RowSelector::select(3),
            RowSelector::select(4),
            RowSelector::skip(3),
            RowSelector::skip(1),
            RowSelector::skip(0),
        ];

        let expected = RowSelection::from(vec![
            RowSelector::skip(6),
            RowSelector::select(10),
            RowSelector::skip(4),
        ]);

        assert_eq!(RowSelection::from_iter(a), expected);
        assert_eq!(RowSelection::from_iter(b), expected);
        assert_eq!(RowSelection::from_iter(c), expected);
    }

    #[test]
    fn test_combine_2elements() {
        let a = vec![RowSelector::select(10), RowSelector::select(5)];
        let a_expect = vec![RowSelector::select(15)];
        assert_eq!(RowSelection::from_iter(a).selectors(), a_expect);

        let b = vec![RowSelector::select(10), RowSelector::skip(5)];
        let b_expect = vec![RowSelector::select(10), RowSelector::skip(5)];
        assert_eq!(RowSelection::from_iter(b).selectors(), b_expect);

        let c = vec![RowSelector::skip(10), RowSelector::select(5)];
        let c_expect = vec![RowSelector::skip(10), RowSelector::select(5)];
        assert_eq!(RowSelection::from_iter(c).selectors(), c_expect);

        let d = vec![RowSelector::skip(10), RowSelector::skip(5)];
        let d_expect = vec![RowSelector::skip(15)];
        assert_eq!(RowSelection::from_iter(d).selectors(), d_expect);
    }

    #[test]
    fn test_from_one_and_empty() {
        let a = vec![RowSelector::select(10)];
        let selection1 = RowSelection::from(a.clone());
        assert_eq!(selection1.selectors(), a);

        let b = vec![];
        let selection1 = RowSelection::from(b.clone());
        assert_eq!(selection1.selectors(), b)
    }

    #[test]
    #[should_panic(expected = "selection exceeds the number of selected rows")]
    fn test_and_longer() {
        let a = RowSelection::from(vec![
            RowSelector::select(3),
            RowSelector::skip(33),
            RowSelector::select(3),
            RowSelector::skip(33),
        ]);
        let b = RowSelection::from(vec![RowSelector::select(36)]);
        a.and_then(&b);
    }

    #[test]
    #[should_panic(expected = "selection contains less than the number of selected rows")]
    fn test_and_shorter() {
        let a = RowSelection::from(vec![
            RowSelector::select(3),
            RowSelector::skip(33),
            RowSelector::select(3),
            RowSelector::skip(33),
        ]);
        let b = RowSelection::from(vec![RowSelector::select(3)]);
        a.and_then(&b);
    }

    #[test]
    fn test_intersect_row_selection_and_combine() {
        // a size equal b size
        let a = vec![
            RowSelector::select(5),
            RowSelector::skip(4),
            RowSelector::select(1),
        ];
        let b = vec![
            RowSelector::select(8),
            RowSelector::skip(1),
            RowSelector::select(1),
        ];

        let res = intersect_row_selections(&a, &b);
        assert_eq!(
            res.selectors(),
            vec![
                RowSelector::select(5),
                RowSelector::skip(4),
                RowSelector::select(1),
            ],
        );

        // a size larger than b size
        let a = vec![
            RowSelector::select(3),
            RowSelector::skip(33),
            RowSelector::select(3),
            RowSelector::skip(33),
        ];
        let b = vec![RowSelector::select(36), RowSelector::skip(36)];
        let res = intersect_row_selections(&a, &b);
        assert_eq!(
            res.selectors(),
            vec![RowSelector::select(3), RowSelector::skip(69)]
        );

        // a size less than b size
        let a = vec![RowSelector::select(3), RowSelector::skip(7)];
        let b = vec![
            RowSelector::select(2),
            RowSelector::skip(2),
            RowSelector::select(2),
            RowSelector::skip(2),
            RowSelector::select(2),
        ];
        let res = intersect_row_selections(&a, &b);
        assert_eq!(
            res.selectors(),
            vec![RowSelector::select(2), RowSelector::skip(8)]
        );

        let a = vec![RowSelector::select(3), RowSelector::skip(7)];
        let b = vec![
            RowSelector::select(2),
            RowSelector::skip(2),
            RowSelector::select(2),
            RowSelector::skip(2),
            RowSelector::select(2),
        ];
        let res = intersect_row_selections(&a, &b);
        assert_eq!(
            res.selectors(),
            vec![RowSelector::select(2), RowSelector::skip(8)]
        );
    }

    #[test]
    fn test_and_fuzz() {
        let mut rand = rng();
        for _ in 0..100 {
            let a_len = rand.random_range(10..100);
            let a_bools: Vec<_> = (0..a_len).map(|_| rand.random_bool(0.2)).collect();
            let a = RowSelection::from_filters(&[BooleanArray::from(a_bools.clone())]);

            let b_len: usize = a_bools.iter().map(|x| *x as usize).sum();
            let b_bools: Vec<_> = (0..b_len).map(|_| rand.random_bool(0.8)).collect();
            let b = RowSelection::from_filters(&[BooleanArray::from(b_bools.clone())]);

            let mut expected_bools = vec![false; a_len];

            let mut iter_b = b_bools.iter();
            for (idx, b) in a_bools.iter().enumerate() {
                if *b && *iter_b.next().unwrap() {
                    expected_bools[idx] = true;
                }
            }

            let expected = RowSelection::from_filters(&[BooleanArray::from(expected_bools)]);

            let total_rows: usize = expected.selectors().iter().map(|s| s.row_count).sum();
            assert_eq!(a_len, total_rows);

            assert_eq!(a.and_then(&b), expected);
        }
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
    fn test_limit() {
        // Limit to existing limit should no-op
        let selection = RowSelection::from(vec![RowSelector::select(10), RowSelector::skip(90)]);
        let limited = selection.limit(10);
        assert_eq!(RowSelection::from(vec![RowSelector::select(10)]), limited);

        let selection = RowSelection::from(vec![
            RowSelector::select(10),
            RowSelector::skip(10),
            RowSelector::select(10),
            RowSelector::skip(10),
            RowSelector::select(10),
        ]);

        let limited = selection.clone().limit(5);
        let expected = vec![RowSelector::select(5)];
        assert_eq!(limited.selectors(), expected);

        let limited = selection.clone().limit(15);
        let expected = vec![
            RowSelector::select(10),
            RowSelector::skip(10),
            RowSelector::select(5),
        ];
        assert_eq!(limited.selectors(), expected);

        let limited = selection.clone().limit(0);
        let expected = vec![];
        assert_eq!(limited.selectors(), expected);

        let limited = selection.clone().limit(30);
        let expected = vec![
            RowSelector::select(10),
            RowSelector::skip(10),
            RowSelector::select(10),
            RowSelector::skip(10),
            RowSelector::select(10),
        ];
        assert_eq!(limited.selectors(), expected);

        let limited = selection.limit(100);
        let expected = vec![
            RowSelector::select(10),
            RowSelector::skip(10),
            RowSelector::select(10),
            RowSelector::skip(10),
            RowSelector::select(10),
        ];
        assert_eq!(limited.selectors(), expected);
    }

    #[test]
    fn test_scan_ranges() {
        let index = vec![
            PageLocation {
                offset: 0,
                compressed_page_size: 10,
                first_row_index: 0,
            },
            PageLocation {
                offset: 10,
                compressed_page_size: 10,
                first_row_index: 10,
            },
            PageLocation {
                offset: 20,
                compressed_page_size: 10,
                first_row_index: 20,
            },
            PageLocation {
                offset: 30,
                compressed_page_size: 10,
                first_row_index: 30,
            },
            PageLocation {
                offset: 40,
                compressed_page_size: 10,
                first_row_index: 40,
            },
            PageLocation {
                offset: 50,
                compressed_page_size: 10,
                first_row_index: 50,
            },
            PageLocation {
                offset: 60,
                compressed_page_size: 10,
                first_row_index: 60,
            },
        ];

        let selection = RowSelection::from(vec![
            // Skip first page
            RowSelector::skip(10),
            // Multiple selects in same page
            RowSelector::select(3),
            RowSelector::skip(3),
            RowSelector::select(4),
            // Select to page boundary
            RowSelector::skip(5),
            RowSelector::select(5),
            // Skip full page past page boundary
            RowSelector::skip(12),
            // Select across page boundaries
            RowSelector::select(12),
            // Skip final page
            RowSelector::skip(12),
        ]);

        let ranges = selection.scan_ranges(&index);

        // assert_eq!(mask, vec![false, true, true, false, true, true, false]);
        assert_eq!(ranges, vec![10..20, 20..30, 40..50, 50..60]);
        assert_eq!(
            selection.row_ranges_for_selected_pages(&index, 70),
            vec![10..20, 20..30, 40..50, 50..60]
        );

        let selection = RowSelection::from(vec![
            // Skip first page
            RowSelector::skip(10),
            // Multiple selects in same page
            RowSelector::select(3),
            RowSelector::skip(3),
            RowSelector::select(4),
            // Select to page boundary
            RowSelector::skip(5),
            RowSelector::select(5),
            // Skip full page past page boundary
            RowSelector::skip(12),
            // Select across page boundaries
            RowSelector::select(12),
            RowSelector::skip(1),
            // Select across page boundaries including final page
            RowSelector::select(8),
        ]);

        let ranges = selection.scan_ranges(&index);

        // assert_eq!(mask, vec![false, true, true, false, true, true, true]);
        assert_eq!(ranges, vec![10..20, 20..30, 40..50, 50..60, 60..70]);

        let selection = RowSelection::from(vec![
            // Skip first page
            RowSelector::skip(10),
            // Multiple selects in same page
            RowSelector::select(3),
            RowSelector::skip(3),
            RowSelector::select(4),
            // Select to page boundary
            RowSelector::skip(5),
            RowSelector::select(5),
            // Skip full page past page boundary
            RowSelector::skip(12),
            // Select to final page boundary
            RowSelector::select(12),
            RowSelector::skip(1),
            // Skip across final page boundary
            RowSelector::skip(8),
            // Select from final page
            RowSelector::select(4),
        ]);

        let ranges = selection.scan_ranges(&index);

        // assert_eq!(mask, vec![false, true, true, false, true, true, true]);
        assert_eq!(ranges, vec![10..20, 20..30, 40..50, 50..60, 60..70]);

        let selection = RowSelection::from(vec![
            // Skip first page
            RowSelector::skip(10),
            // Multiple selects in same page
            RowSelector::select(3),
            RowSelector::skip(3),
            RowSelector::select(4),
            // Select to remaining in page and first row of next page
            RowSelector::skip(5),
            RowSelector::select(6),
            // Skip remaining
            RowSelector::skip(50),
        ]);

        let ranges = selection.scan_ranges(&index);

        // assert_eq!(mask, vec![false, true, true, false, true, true, true]);
        assert_eq!(ranges, vec![10..20, 20..30, 30..40]);
    }

    #[test]
    fn test_loaded_mask_chunk_stops_at_trimmed_mask_end() {
        let loaded = LoadedRowRanges::from_selection(RowSelection::from_consecutive_ranges(
            std::iter::once(0..5),
            10,
        ));
        let RowSelectionCursor::Mask(mut cursor) = RowSelectionCursor::new_mask_from_selectors(
            vec![RowSelector::select(1)],
            Some(loaded.into()),
        ) else {
            unreachable!()
        };

        let chunk = cursor.next_chunk(10).unwrap();
        assert_eq!(chunk.chunk_rows, 1);
        assert!(cursor.is_empty());
    }

    #[test]
    fn test_next_mask_chunk_until_cursor_is_empty() {
        let RowSelectionCursor::Mask(mut cursor) = RowSelectionCursor::new_mask_from_selectors(
            vec![
                RowSelector::skip(2),
                RowSelector::select(2),
                RowSelector::skip(1),
                RowSelector::select(1),
            ],
            None,
        ) else {
            unreachable!()
        };

        let first = cursor.next_mask_chunk(2).unwrap();
        assert_eq!(first.initial_skip, 2);
        assert_eq!(first.chunk_rows, 2);
        assert_eq!(first.selected_rows, 2);

        let second = cursor.next_mask_chunk(2).unwrap();
        assert_eq!(second.initial_skip, 1);
        assert_eq!(second.chunk_rows, 1);
        assert_eq!(second.selected_rows, 1);

        assert!(cursor.next_mask_chunk(2).is_none());
    }

    #[test]
    fn test_from_ranges() {
        let ranges = [1..3, 4..6, 6..6, 8..8, 9..10];
        let selection = RowSelection::from_consecutive_ranges(ranges.into_iter(), 10);
        assert_eq!(
            selection.selectors(),
            vec![
                RowSelector::skip(1),
                RowSelector::select(2),
                RowSelector::skip(1),
                RowSelector::select(2),
                RowSelector::skip(3),
                RowSelector::select(1)
            ]
        );

        let out_of_order_ranges = [1..3, 8..10, 4..7];
        let result = std::panic::catch_unwind(|| {
            RowSelection::from_consecutive_ranges(out_of_order_ranges.into_iter(), 10)
        });
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_selector() {
        let selection = RowSelection::from(vec![
            RowSelector::skip(0),
            RowSelector::select(2),
            RowSelector::skip(0),
            RowSelector::select(2),
        ]);
        assert_eq!(selection.selectors(), vec![RowSelector::select(4)]);

        let selection = RowSelection::from(vec![
            RowSelector::select(0),
            RowSelector::skip(2),
            RowSelector::select(0),
            RowSelector::skip(2),
        ]);
        assert_eq!(selection.selectors(), vec![RowSelector::skip(4)]);
    }

    #[test]
    fn test_intersection() {
        let selection = RowSelection::from(vec![RowSelector::select(1048576)]);
        let result = selection.intersection(&selection);
        assert_eq!(result, selection);

        let a = RowSelection::from(vec![
            RowSelector::skip(10),
            RowSelector::select(10),
            RowSelector::skip(10),
            RowSelector::select(20),
        ]);

        let b = RowSelection::from(vec![
            RowSelector::skip(20),
            RowSelector::select(20),
            RowSelector::skip(10),
        ]);

        let result = a.intersection(&b);
        assert_eq!(
            result.selectors(),
            vec![
                RowSelector::skip(30),
                RowSelector::select(10),
                RowSelector::skip(10)
            ]
        );
    }

    #[test]
    fn test_union() {
        let selection = RowSelection::from(vec![RowSelector::select(1048576)]);
        let result = selection.union(&selection);
        assert_eq!(result, selection);

        // NYNYY
        let a = RowSelection::from(vec![
            RowSelector::skip(10),
            RowSelector::select(10),
            RowSelector::skip(10),
            RowSelector::select(20),
        ]);

        // NNYYNYN
        let b = RowSelection::from(vec![
            RowSelector::skip(20),
            RowSelector::select(20),
            RowSelector::skip(10),
            RowSelector::select(10),
            RowSelector::skip(10),
        ]);

        let result = a.union(&b);

        // NYYYYYN
        assert_eq!(
            result.iter().copied().collect::<Vec<_>>(),
            vec![
                RowSelector::skip(10),
                RowSelector::select(50),
                RowSelector::skip(10),
            ]
        );
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
    fn test_trim() {
        let selection = RowSelection::from(vec![
            RowSelector::skip(34),
            RowSelector::select(12),
            RowSelector::skip(3),
            RowSelector::select(35),
        ]);

        let expected = vec![
            RowSelector::skip(34),
            RowSelector::select(12),
            RowSelector::skip(3),
            RowSelector::select(35),
        ];

        assert_eq!(selection.trim().selectors(), expected);

        let selection = RowSelection::from(vec![
            RowSelector::skip(34),
            RowSelector::select(12),
            RowSelector::skip(3),
        ]);

        let expected = vec![RowSelector::skip(34), RowSelector::select(12)];

        assert_eq!(selection.trim().selectors(), expected);
    }
}
