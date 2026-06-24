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

use crate::arrow::ProjectionMask;
use crate::errors::ParquetError;
use crate::file::page_index::offset_index::{OffsetIndexMetaData, PageLocation};
use arrow_array::{Array, BooleanArray};
use arrow_buffer::bit_iterator::BitSliceIterator;
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder};
use arrow_select::filter::SlicesIterator;
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::ops::Range;
use std::sync::OnceLock;

/// Policy for picking a strategy to materialize [`RowSelection`] during execution.
///
/// Note that this is a user-provided preference, and the actual strategy used
/// may differ based on safety considerations (e.g. page skipping).
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
/// This is determined from a combination of user preference (via [`RowSelectionPolicy`])
/// and safety considerations (e.g. page skipping).
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
    fn new(mask: BooleanBuffer) -> Self {
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

    fn selectors(&self) -> &[RowSelector] {
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

/// Bitwise AND of two mask-backed selections. Longer side's tail passes through.
fn intersect_masks(l: &BooleanBuffer, r: &BooleanBuffer) -> BooleanBuffer {
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
fn union_masks(l: &BooleanBuffer, r: &BooleanBuffer) -> BooleanBuffer {
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
fn and_then_mask(mask: &BooleanBuffer, other: &RowSelection) -> BooleanBuffer {
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

/// Skips the first `offset` selected rows of a mask-backed selection.
fn offset_mask(mask: BooleanBuffer, offset: usize) -> BooleanBuffer {
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
fn limit_mask(mask: BooleanBuffer, limit: usize) -> BooleanBuffer {
    // `find_nth_set_bit_position` returns `mask.len()` when there are fewer
    // than `limit` set bits, so the slice naturally degrades to the original
    // mask in that case.
    let cut = mask.find_nth_set_bit_position(0, limit);
    mask.slice(0, cut)
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
    /// eagerly run-length-encoded; [`Self::iter`] streams the RLE form
    /// directly off the bitmap.
    pub fn from_boolean_buffer(mask: BooleanBuffer) -> Self {
        Self {
            inner: RowSelectionInner::Mask(Box::new(MaskSelection::new(mask))),
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
                    (0, 0)
                } else {
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

                    (total_rows, run_count)
                }
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
                let mut ranges: Vec<Range<u64>> = vec![];
                let mut row_offset = 0;

                let mut pages = page_locations.iter().peekable();
                let mut selectors = selectors.iter().cloned();
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
                            if row_offset + selector.row_count == next_page.first_row_index as usize
                            {
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
            RowSelectionInner::Mask(mask) => {
                scan_ranges_from_selectors(MaskRunIter::new(mask.mask()), page_locations)
            }
        }
    }

    /// Returns true if this selection would skip any data pages within the provided columns
    fn selection_skips_any_page(
        &self,
        projection: &ProjectionMask,
        columns: &[OffsetIndexMetaData],
    ) -> bool {
        columns.iter().enumerate().any(|(leaf_idx, column)| {
            if !projection.leaf_included(leaf_idx) {
                return false;
            }

            let locations = column.page_locations();
            if locations.is_empty() {
                return false;
            }

            let ranges = self.scan_ranges(locations);
            !ranges.is_empty() && ranges.len() < locations.len()
        })
    }

    /// Returns true if selectors should be forced, preventing mask materialisation
    pub(crate) fn should_force_selectors(
        &self,
        projection: &ProjectionMask,
        offset_index: Option<&[OffsetIndexMetaData]>,
    ) -> bool {
        match offset_index {
            Some(columns) => self.selection_skips_any_page(projection, columns),
            None => false,
        }
    }

    /// Splits off the first `row_count` from this [`RowSelection`]
    pub fn split_off(&mut self, row_count: usize) -> Self {
        if matches!(&self.inner, RowSelectionInner::Mask(_)) {
            let mask = match std::mem::take(&mut self.inner) {
                RowSelectionInner::Mask(m) => m,
                RowSelectionInner::Selectors(_) => unreachable!(),
            };
            let mask = (*mask).into_mask();
            let total = mask.len();
            if row_count >= total {
                // Whole selection moves into the returned head; leave `self` as
                // an empty mask so the backing is preserved.
                self.inner = RowSelectionInner::Mask(Box::new(MaskSelection::new(
                    BooleanBuffer::new_unset(0),
                )));
                return Self::from_boolean_buffer(mask);
            }
            let head = mask.slice(0, row_count);
            let tail = mask.slice(row_count, total - row_count);
            self.inner = RowSelectionInner::Mask(Box::new(MaskSelection::new(tail)));
            return Self::from_boolean_buffer(head);
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
            RowSelectionInner::Mask(m) => m.mask().set_indices().next().is_some(),
        }
    }

    /// Trims this [`RowSelection`] removing any trailing skips
    pub(crate) fn trim(mut self) -> Self {
        if let RowSelectionInner::Mask(m) = &self.inner {
            // Position one past the last set bit (= 0 when there are none).
            let mask = m.mask();
            let popcount = mask.count_set_bits();
            let new_len = if popcount == 0 {
                0
            } else {
                mask.find_nth_set_bit_position(0, popcount)
            };
            if new_len != mask.len() {
                return Self {
                    inner: RowSelectionInner::Mask(Box::new(MaskSelection::new(
                        mask.slice(0, new_len),
                    ))),
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
                return Self::from_boolean_buffer(offset_mask((*mask).into_mask(), offset));
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
                return Self::from_boolean_buffer(limit_mask((*mask).into_mask(), limit));
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
            RowSelectionInner::Mask(m) => m.mask().count_set_bits(),
        }
    }

    /// Returns the number of de-selected rows
    pub fn skipped_row_count(&self) -> usize {
        match &self.inner {
            RowSelectionInner::Selectors(s) => {
                s.iter().filter(|x| x.skip).map(|x| x.row_count).sum()
            }
            RowSelectionInner::Mask(m) => m.mask().len() - m.mask().count_set_bits(),
        }
    }

    /// Expands the selection to align with batch boundaries.
    /// This is needed when using cached array readers to ensure that
    /// the cached data covers full batches.
    pub(crate) fn expand_to_batch_boundaries(&self, batch_size: usize, total_rows: usize) -> Self {
        if batch_size == 0 {
            return self.clone();
        }

        // Selector-backed path is inlined here to match `main`'s generated code shape;
        // see the comment in `scan_ranges` for context.
        match &self.inner {
            RowSelectionInner::Selectors(selectors) => {
                let mut expanded_ranges = Vec::new();
                let mut row_offset = 0;

                for selector in selectors.iter().cloned() {
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
                            last.end = last.end.max(range.end);
                        } else {
                            merged_ranges.push(range);
                        }
                    } else {
                        merged_ranges.push(range);
                    }
                }

                RowSelection::from_consecutive_ranges(merged_ranges.into_iter(), total_rows)
            }
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

/// Cursor for iterating a mask-backed [`RowSelection`]
///
/// This is best for dense selections where there are many small skips
/// or selections. For example, selecting every other row.
#[derive(Debug)]
pub struct MaskCursor {
    mask: BooleanBuffer,
    /// Current absolute offset into the selection
    position: usize,
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
    pub(crate) fn new_mask_from_selectors(selectors: Vec<RowSelector>) -> Self {
        Self::Mask(MaskCursor {
            mask: boolean_mask_from_selectors(&selectors),
            position: 0,
        })
    }

    /// Create a [`MaskCursor`] cursor backed by an existing bitmask.
    pub(crate) fn new_mask_from_buffer(mask: BooleanBuffer) -> Self {
        Self::Mask(MaskCursor { mask, position: 0 })
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

fn boolean_mask_from_selectors(selectors: &[RowSelector]) -> BooleanBuffer {
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
