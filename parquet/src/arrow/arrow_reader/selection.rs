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

mod and_then;
pub(crate) mod conversion;
pub(crate) mod mask;

use crate::arrow::ProjectionMask;
use crate::arrow::arrow_reader::selection::conversion::consecutive_ranges_to_row_selectors;
use crate::arrow::arrow_reader::selection::mask::BitmaskSelection;
use crate::errors::ParquetError;
use crate::file::page_index::offset_index::{OffsetIndexMetaData, PageLocation};
use arrow_array::{Array, BooleanArray};
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder};
use arrow_select::filter::SlicesIterator;
use std::collections::VecDeque;
use std::ops::Range;

/// Policy for picking a strategy to materialise [`RowSelection`] during execution.
///
/// Note that this is a user-provided preference, and the actual strategy used
/// may differ based on safety considerations (e.g. page skipping).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RowSelectionPolicy {
    /// Use a queue of [`RowSelector`] values
    Selectors,
    /// Use a boolean mask to materialise the selection
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
    /// Use a boolean mask to materialise the selection
    Mask,
}

/// Select or skip some number of contiguous rows.
///
/// This is similar to a run-length encoding of selected rows, compared to
/// a boolean mask which explicitly represents each row.
///
/// See [`RowSelection`] for representing a full selection of rows.
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

// TODO: maybe call this `Selection` and typedef to RowSelection for backwards compatibility?

/// [`RowSelection`] represents the rows that should be selected (decoded)
/// when reading parquet data.
///
/// This is applied prior to reading column data, and can therefore
/// be used to skip IO to fetch data into memory
///
/// A typical use-case would be using the [`PageIndex`] to filter out rows
/// that don't satisfy a predicate
///
/// Internally it adaptively switches between two possible representations, each
/// better for different patterns of rows that are selected:
/// * A list of [`RowSelector`] (ranges) values (more efficient for large contiguous
///   selections or skips)
/// * A boolean mask (more efficient for sparse selections
///
/// # Example: Create with [`RowSelector`]s
/// ```
/// # use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
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
/// ```
///
/// # Example: Create with ranges
/// ```
/// # use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
/// // you can also create a selection from consecutive ranges
/// let ranges = vec![5..10, 10..15];
/// let selection = RowSelection::from_consecutive_ranges(ranges.into_iter(), 20);
/// let actual: Vec<RowSelector> = selection.into();
/// assert_eq!(actual,
///   vec![
///     RowSelector::skip(5),
///     RowSelector::select(10),
///     RowSelector::skip(5)
///   ]);
/// ```
///
/// # Example converting `RowSelection` to/from `Vec<RowSelector>`
/// ```
/// # use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
/// let selectors = vec![
///    RowSelector::skip(5),
///    RowSelector::select(10),
/// ];
/// let selection = RowSelection::from(selectors);
/// let back: Vec<RowSelector> = selection.into();
/// assert_eq!(back, vec![
///   RowSelector::skip(5),
///   RowSelector::select(10),
/// ]);
/// ```
///
/// # Example converting `RowSelection` to/from to `BooleanArray`
/// ```
/// # use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
/// # use arrow_array::BooleanArray;
/// let mask = BooleanArray::from(vec![false, false, false, true, true, false]);
/// let selection = RowSelection::from(mask.clone());
/// let back: BooleanArray = selection.into();
/// assert_eq!(back, mask);
/// ```
///
/// [`PageIndex`]: crate::file::page_index::column_index::ColumnIndexMetaData
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum RowSelection {
    /// Selection based on a list of [`RowSelector`]s
    Selectors(RowSelectorSelection),
    /// Selection based on a boolean mask
    Mask(BitmaskSelection),
}
impl Default for RowSelection {
    fn default() -> Self {
        Self::Selectors(RowSelectorSelection::default())
    }
}

impl RowSelection {
    /// Creates a [`RowSelection`] from a slice of [`BooleanArray`]
    ///
    /// # Panic
    ///
    /// Panics if any of the [`BooleanArray`] contain nulls
    pub fn from_filters(filters: &[BooleanArray]) -> Self {
        // TODO decide how to do this based on density or something??
        Self::Mask(BitmaskSelection::from_filters(filters))
        //Self::Selectors(RowSelectorSelection::from_filters(filters))
    }

    /// Creates a [`RowSelection`] from an iterator of consecutive ranges to keep
    pub fn from_consecutive_ranges<I: Iterator<Item = Range<usize>>>(
        ranges: I,
        total_rows: usize,
    ) -> Self {
        // todo should this be decided based on density or something??
        Self::Selectors(RowSelectorSelection::from_consecutive_ranges(
            ranges, total_rows,
        ))
    }

    /// Given an offset index, return the byte ranges for all data pages selected by `self`
    ///
    /// This is useful for determining what byte ranges to fetch from underlying storage
    ///
    /// Note: this method does not make any effort to combine consecutive ranges, nor coalesce
    /// ranges that are close together. This is instead delegated to the IO subsystem to optimise,
    /// e.g. [`ObjectStore::get_ranges`](object_store::ObjectStore::get_ranges)
    pub fn scan_ranges(&self, page_locations: &[PageLocation]) -> Vec<Range<u64>> {
        match self {
            Self::Selectors(selection) => selection.scan_ranges(page_locations),
            Self::Mask(mask) => mask.scan_ranges(page_locations),
        }
    }

    /// Returns true if selectors should be forced, preventing mask materialisation
    pub(crate) fn should_force_selectors(
        &self,
        projection: &ProjectionMask,
        offset_index: Option<&[OffsetIndexMetaData]>,
    ) -> bool {
        let Some(offset_index) = offset_index else {
            return false;
        };

        offset_index.iter().enumerate().any(|(leaf_idx, column)| {
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

    /// Splits off the first `row_count` from this [`RowSelection`]
    ///
    /// If the row_count exceeds the total number of rows in this selection,
    /// the entire selection is returned and this selection becomes empty.
    pub fn split_off(&mut self, row_count: usize) -> Self {
        match self {
            Self::Selectors(selection) => Self::Selectors(selection.split_off(row_count)),
            Self::Mask(mask) => Self::Mask(mask.split_off(row_count)),
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
        match (self, other) {
            (Self::Selectors(left), Self::Selectors(right)) => {
                Self::Selectors(left.and_then(right))
            }
            (Self::Mask(left), Self::Mask(right)) => Self::Mask(left.and_then(right)),
            // need to convert one to the other
            // todo figure out some heuristic here
            (Self::Selectors(left), Self::Mask(right)) => {
                let left_mask = BitmaskSelection::from_row_selectors(&left.selectors);
                Self::Mask(left_mask.and_then(right))
            }
            (Self::Mask(left), Self::Selectors(right)) => {
                let right_mask = BitmaskSelection::from_row_selectors(&right.selectors);
                Self::Mask(left.and_then(&right_mask))
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
        match (self, other) {
            (Self::Selectors(left), Self::Selectors(right)) => {
                Self::Selectors(intersect_row_selections(&left.selectors, &right.selectors))
            }
            (Self::Mask(left), Self::Mask(right)) => Self::Mask(left.intersection(right)),
            // need to convert one to the other
            _ => {
                todo!()
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
        match (self, other) {
            (Self::Selectors(left), Self::Selectors(right)) => {
                Self::Selectors(union_row_selections(&left.selectors, &right.selectors))
            }
            (Self::Mask(left), Self::Mask(right)) => Self::Mask(left.union(right)),
            // need to convert one to the other
            _ => {
                todo!()
            }
        }
    }

    /// Returns `true` if this [`RowSelection`] selects any rows
    pub fn selects_any(&self) -> bool {
        match self {
            Self::Selectors(selection) => selection.selects_any(),
            Self::Mask(mask) => mask.selects_any(),
        }
    }

    /// Trims this [`RowSelection`] removing any trailing skips
    pub(crate) fn trim(self) -> Self {
        match self {
            Self::Selectors(selection) => Self::Selectors(selection.trim()),
            Self::Mask(mask) => Self::Mask(mask.trim()),
        }
    }

    /// Applies an offset to this [`RowSelection`], skipping the first `offset` selected rows
    pub(crate) fn offset(self, offset: usize) -> Self {
        match self {
            Self::Selectors(selection) => Self::Selectors(selection.offset(offset)),
            Self::Mask(mask) => Self::Mask(mask.offset(offset)),
        }
    }

    /// Limit this [`RowSelection`] to only select `limit` rows
    pub(crate) fn limit(self, limit: usize) -> Self {
        match self {
            Self::Selectors(selection) => Self::Selectors(selection.limit(limit)),
            Self::Mask(_mask) => {
                todo!()
            }
        }
    }

    /// Returns an iterator over the [`RowSelector`]s for this
    /// [`RowSelection`].
    pub fn iter(&self) -> impl Iterator<Item = &RowSelector> {
        match self {
            Self::Selectors(selection) => selection.iter(),
            Self::Mask(_mask) => {
                todo!()
            }
        }
    }

    /// Returns the number of selected rows
    pub fn row_count(&self) -> usize {
        match self {
            Self::Selectors(selection) => selection.row_count(),
            Self::Mask(mask) => mask.row_count(),
        }
    }

    /// Returns the number of de-selected rows
    pub fn skipped_row_count(&self) -> usize {
        match self {
            Self::Selectors(selection) => selection.skipped_row_count(),
            Self::Mask(mask) => {
                todo!()
            }
        }
    }

    /// Expands the selection to align with batch boundaries.
    /// This is needed when using cached array readers to ensure that
    /// the cached data covers full batches.
    pub(crate) fn expand_to_batch_boundaries(&self, batch_size: usize, total_rows: usize) -> Self {
        match self {
            Self::Selectors(selection) => {
                Self::Selectors(selection.expand_to_batch_boundaries(batch_size, total_rows))
            }
            Self::Mask(mask) => Self::Mask(mask.expand_to_batch_boundaries(batch_size, total_rows)),
        }
    }
}

impl From<Vec<RowSelector>> for RowSelection {
    fn from(selectors: Vec<RowSelector>) -> Self {
        Self::Selectors(selectors.into_iter().collect())
    }
}

impl FromIterator<RowSelector> for RowSelection {
    fn from_iter<T: IntoIterator<Item = RowSelector>>(iter: T) -> Self {
        Self::Selectors(iter.into_iter().collect())
    }
}

impl From<RowSelection> for Vec<RowSelector> {
    fn from(r: RowSelection) -> Self {
        match r {
            RowSelection::Selectors(selection) => selection.into(),
            RowSelection::Mask(selection) => selection.into(),
        }
    }
}

impl From<RowSelection> for VecDeque<RowSelector> {
    fn from(r: RowSelection) -> Self {
        Vec::<RowSelector>::from(r).into()
    }
}

impl From<RowSelectorSelection> for RowSelection {
    fn from(r: RowSelectorSelection) -> Self {
        Self::Selectors(r)
    }
}

impl From<BitmaskSelection> for RowSelection {
    fn from(r: BitmaskSelection) -> Self {
        Self::Mask(r)
    }
}

impl From<BooleanArray> for RowSelection {
    fn from(array: BooleanArray) -> Self {
        Self::from_filters(&[array])
    }
}

impl From<RowSelection> for BooleanArray {
    fn from(selection: RowSelection) -> Self {
        match selection {
            RowSelection::Selectors(selectors) => {
                BitmaskSelection::from_row_selectors(&selectors.selectors).into()
            }
            RowSelection::Mask(mask) => mask.into(),
        }
    }
}

// TODO move to its own module
/// One possible backing of [`RowSelection`], based on Vec<RowSelector>
///
/// It is similar to the "Range Index" described in
/// [Predicate Caching: Query-Driven Secondary Indexing for Cloud Data Warehouses]
///
/// Represents the result of a filter evaluation as a series of ranges (a type
/// of run length encoding). It is more efficient for large contiguous
/// selections or skips.
///
/// For example, the selection:
/// ```text
/// skip 5
/// select 10
/// skip 5
/// ```
///
/// Represents evaluating a filter over 20 rows where the first 5 and last 5
/// rows are filtered out, and the middle 10 rows are retained.
///
/// A [`RowSelectorSelection`] maintains the following invariants:
///
/// * It contains no [`RowSelector`] of 0 rows
/// * Consecutive [`RowSelector`]s alternate skipping or selecting rows
///
///
/// [Predicate Caching: Query-Driven Secondary Indexing for Cloud Data Warehouses]: https://dl.acm.org/doi/10.1145/3626246.3653395
#[derive(Debug, Clone, Default, Eq, PartialEq)]
struct RowSelectorSelection {
    selectors: Vec<RowSelector>,
}

impl RowSelectorSelection {
    /// Create a [`RowSelection`] from a Vec of [`RowSelector`]
    ///
    /// Note this will filter any selectors with row_count == 0 and combining consecutive selectors
    pub fn from_selectors(selectors: Vec<RowSelector>) -> Self {
        // processing / simplification happens in from_iter
        selectors.into_iter().collect()
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
        Self {
            selectors: consecutive_ranges_to_row_selectors(ranges, total_rows),
        }
    }

    /// Given an offset index, return the byte ranges for all data pages selected by `self`
    ///
    /// See [`RowSelection::scan_ranges`] for more details
    pub fn scan_ranges(&self, page_locations: &[PageLocation]) -> Vec<Range<u64>> {
        let mut ranges: Vec<Range<u64>> = vec![];
        let mut row_offset = 0;

        let mut pages = page_locations.iter().peekable();
        let mut selectors = self.selectors.iter().cloned();
        let mut current_selector = selectors.next();
        let mut current_page = pages.next();

        let mut current_page_included = false;

        while let Some((selector, page)) = current_selector.as_mut().zip(current_page) {
            if !(selector.skip || current_page_included) {
                ranges.push(page_location_range(page));
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
                    ranges.push(page_location_range(page));
                }
                current_selector = selectors.next()
            }
        }

        ranges
    }

    /// Splits off the first `row_count` from this [`RowSelection`]
    pub fn split_off(&mut self, row_count: usize) -> Self {
        let mut total_count = 0;

        // Find the index where the selector exceeds the row count
        let find = self.selectors.iter().position(|selector| {
            total_count += selector.row_count;
            total_count > row_count
        });

        let split_idx = match find {
            Some(idx) => idx,
            None => {
                let selectors = std::mem::take(&mut self.selectors);
                return Self { selectors };
            }
        };

        let mut remaining = self.selectors.split_off(split_idx);

        // Always present as `split_idx < self.selectors.len`
        let next = remaining.first_mut().unwrap();
        let overflow = total_count - row_count;

        if next.row_count != overflow {
            self.selectors.push(RowSelector {
                row_count: next.row_count - overflow,
                skip: next.skip,
            })
        }
        next.row_count = overflow;

        std::mem::swap(&mut remaining, &mut self.selectors);
        Self {
            selectors: remaining,
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
        let mut selectors = vec![];
        let mut first = self.selectors.iter().cloned().peekable();
        let mut second = other.selectors.iter().cloned().peekable();

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

        Self { selectors }
    }

    /// Compute the intersection of two [`RowSelection`]
    /// For example:
    /// self:      NNYYYYNNYYNYN
    /// other:     NYNNNNNNY
    ///
    /// returned:  NNNNNNNNYYNYN
    pub fn intersection(&self, other: &Self) -> Self {
        intersect_row_selections(&self.selectors, &other.selectors)
    }

    /// Compute the union of two [`RowSelection`]
    /// For example:
    /// self:      NNYYYYNNYYNYN
    /// other:     NYNNNNNNN
    ///
    /// returned:  NYYYYYNNYYNYN
    pub fn union(&self, other: &Self) -> Self {
        union_row_selections(&self.selectors, &other.selectors)
    }

    /// Returns `true` if this [`RowSelection`] selects any rows
    pub fn selects_any(&self) -> bool {
        self.selectors.iter().any(|x| !x.skip)
    }

    /// Trims this [`RowSelection`] removing any trailing skips
    pub(crate) fn trim(mut self) -> Self {
        while self.selectors.last().map(|x| x.skip).unwrap_or(false) {
            self.selectors.pop();
        }
        self
    }

    /// Applies an offset to this [`RowSelection`], skipping the first `offset` selected rows
    pub(crate) fn offset(mut self, offset: usize) -> Self {
        if offset == 0 {
            return self;
        }

        let mut selected_count = 0;
        let mut skipped_count = 0;

        // Find the index where the selector exceeds the row count
        let find = self
            .selectors
            .iter()
            .position(|selector| match selector.skip {
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
                self.selectors.clear();
                return self;
            }
        };

        let mut selectors = Vec::with_capacity(self.selectors.len() - split_idx + 1);
        selectors.push(RowSelector::skip(skipped_count + offset));
        selectors.push(RowSelector::select(selected_count - offset));
        selectors.extend_from_slice(&self.selectors[split_idx + 1..]);

        Self { selectors }
    }

    /// Limit this [`RowSelection`] to only select `limit` rows
    pub(crate) fn limit(mut self, mut limit: usize) -> Self {
        if limit == 0 {
            self.selectors.clear();
        }

        for (idx, selection) in self.selectors.iter_mut().enumerate() {
            if !selection.skip {
                if selection.row_count >= limit {
                    selection.row_count = limit;
                    self.selectors.truncate(idx + 1);
                    break;
                } else {
                    limit -= selection.row_count;
                }
            }
        }
        self
    }

    /// Returns an iterator over the [`RowSelector`]s for this
    /// [`RowSelection`].
    pub fn iter(&self) -> impl Iterator<Item = &RowSelector> {
        self.selectors.iter()
    }

    /// Returns the number of selected rows
    pub fn row_count(&self) -> usize {
        self.iter().filter(|s| !s.skip).map(|s| s.row_count).sum()
    }

    /// Returns the number of de-selected rows
    pub fn skipped_row_count(&self) -> usize {
        self.iter().filter(|s| s.skip).map(|s| s.row_count).sum()
    }

    /// Expands the selection to align with batch boundaries.
    /// This is needed when using cached array readers to ensure that
    /// the cached data covers full batches.
    pub(crate) fn expand_to_batch_boundaries(&self, batch_size: usize, total_rows: usize) -> Self {
        if batch_size == 0 {
            return self.clone();
        }

        let mut expanded_ranges = Vec::new();
        let mut row_offset = 0;

        for selector in &self.selectors {
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

        Self::from_consecutive_ranges(merged_ranges.into_iter(), total_rows)
    }
}

impl From<Vec<RowSelector>> for RowSelectorSelection {
    fn from(selectors: Vec<RowSelector>) -> Self {
        RowSelectorSelection::from_selectors(selectors)
    }
}

/// Create a RowSelectorRowSelection from an iterator of RowSelectors
/// filtering out any selectors with row_count == 0 and combining consecutive selectors
impl FromIterator<RowSelector> for RowSelectorSelection {
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

        Self { selectors }
    }
}

impl From<RowSelectorSelection> for Vec<RowSelector> {
    fn from(r: RowSelectorSelection) -> Self {
        r.selectors
    }
}

impl From<RowSelectorSelection> for VecDeque<RowSelector> {
    fn from(r: RowSelectorSelection) -> Self {
        r.selectors.into()
    }
}

/// Combine two lists of `RowSelection` return the intersection of them
/// For example:
/// self:      NNYYYYNNYYNYN
/// other:     NYNNNNNNY
///
/// returned:  NNNNNNNNYYNYN
fn intersect_row_selections(left: &[RowSelector], right: &[RowSelector]) -> RowSelectorSelection {
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
fn union_row_selections(left: &[RowSelector], right: &[RowSelector]) -> RowSelectorSelection {
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
/// actual storage strategy to the internal `RowSelectionBacking`.
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

/// Returns the byte range covered by a given `PageLocation`
fn page_location_range(page: &PageLocation) -> Range<u64> {
    let start = page.offset as u64;
    let end = start + page.compressed_page_size as u64;
    start..end
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::Row;
    use rand::{Rng, rng};

    #[test]
    fn test_from_filters() {
        let filters = vec![
            BooleanArray::from(vec![false, false, false, true, true, true, true]),
            BooleanArray::from(vec![true, true, false, false, true, true, true]),
            BooleanArray::from(vec![false, false, false, false]),
            BooleanArray::from(Vec::<bool>::new()),
        ];

        test_selection(
            || RowSelection::from_filters(&filters[..1]),
            |selection| {
                assert!(selection.selects_any());
                assert_eq!(
                    into_selectors(selection),
                    vec![RowSelector::skip(3), RowSelector::select(4)]
                );
            },
        );
        test_selection(
            || RowSelection::from_filters(&filters[..2]),
            |selection| {
                assert!(selection.selects_any());
                assert_eq!(
                    into_selectors(selection),
                    vec![
                        RowSelector::skip(3),
                        RowSelector::select(6),
                        RowSelector::skip(2),
                        RowSelector::select(3)
                    ]
                );
            },
        );
        test_selection(
            || RowSelection::from_filters(&filters),
            |selection| {
                assert!(selection.selects_any());
                assert_eq!(
                    into_selectors(selection),
                    vec![
                        RowSelector::skip(3),
                        RowSelector::select(6),
                        RowSelector::skip(2),
                        RowSelector::select(3),
                        RowSelector::skip(4)
                    ]
                );
            },
        );

        test_selection(
            || RowSelection::from_filters(&filters[2..3]),
            |selection| {
                assert!(!selection.selects_any());
                assert_eq!(into_selectors(selection), vec![RowSelector::skip(4)]);
            },
        );
    }

    #[test]
    fn test_split_off() {
        test_selection(
            || {
                RowSelection::from(vec![
                    RowSelector::skip(34),
                    RowSelector::select(12),
                    RowSelector::skip(3),
                    RowSelector::select(35),
                ])
            },
            |selection| {
                let mut selection = selection.clone();
                let split = selection.split_off(34);
                assert_eq!(into_selectors(&split), vec![RowSelector::skip(34)]);
                assert_eq!(
                    into_selectors(&selection),
                    vec![
                        RowSelector::select(12),
                        RowSelector::skip(3),
                        RowSelector::select(35)
                    ]
                );

                let split = selection.split_off(5);
                assert_eq!(into_selectors(&split), vec![RowSelector::select(5)]);
                assert_eq!(
                    into_selectors(&selection),
                    vec![
                        RowSelector::select(7),
                        RowSelector::skip(3),
                        RowSelector::select(35)
                    ]
                );

                let split = selection.split_off(8);
                assert_eq!(
                    into_selectors(&split),
                    vec![RowSelector::select(7), RowSelector::skip(1)]
                );
                assert_eq!(
                    into_selectors(&selection),
                    vec![RowSelector::skip(2), RowSelector::select(35)]
                );

                let split = selection.split_off(200);
                assert_eq!(
                    into_selectors(&split),
                    vec![RowSelector::skip(2), RowSelector::select(35)]
                );
                assert!(into_selectors(&selection).is_empty());
            },
        );
    }

    #[test]
    fn test_offset() {
        test_selection(
            || {
                RowSelection::from(vec![
                    RowSelector::select(5),
                    RowSelector::skip(23),
                    RowSelector::select(7),
                    RowSelector::skip(33),
                    RowSelector::select(6),
                ])
            },
            |selection| {
                let selection = selection.clone();
                let selection = selection.offset(2);
                assert_eq!(
                    into_selectors(&selection),
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
                    into_selectors(&selection),
                    vec![
                        RowSelector::skip(30),
                        RowSelector::select(5),
                        RowSelector::skip(33),
                        RowSelector::select(6),
                    ]
                );

                let selection = selection.offset(3);
                assert_eq!(
                    into_selectors(&selection),
                    vec![
                        RowSelector::skip(33),
                        RowSelector::select(2),
                        RowSelector::skip(33),
                        RowSelector::select(6),
                    ]
                );

                let selection = selection.offset(2);
                assert_eq!(
                    into_selectors(&selection),
                    vec![RowSelector::skip(68), RowSelector::select(6),]
                );

                let selection = selection.offset(3);
                assert_eq!(
                    into_selectors(&selection),
                    vec![RowSelector::skip(71), RowSelector::select(3),]
                );
            },
        );
    }

    #[test]
    fn test_and() {
        test_two_selections(
            || {
                let a = RowSelection::from(vec![
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
                (a, b)
            },
            |a, b| {
                let mut a = a.clone();

                let mut expected = RowSelection::from(vec![
                    RowSelector::skip(12),
                    RowSelector::select(5),
                    RowSelector::skip(4),
                    RowSelector::select(14),
                    RowSelector::skip(3),
                    RowSelector::select(1),
                    RowSelector::skip(4),
                ]);

                // Convert to selectors to ensure equality
                assert_eq!(into_selectors(&a.and_then(&b)), into_selectors(&expected));

                a.split_off(7);
                expected.split_off(7);
                assert_eq!(into_selectors(&a.and_then(&b)), into_selectors(&expected));
            },
        );
        test_two_selections(
            || {
                let a = RowSelection::from(vec![RowSelector::select(5), RowSelector::skip(3)]);

                let b = RowSelection::from(vec![
                    RowSelector::select(2),
                    RowSelector::skip(1),
                    RowSelector::select(1),
                    RowSelector::skip(1),
                ]);
                (a, b)
            },
            |a, b| {
                assert_eq!(
                    into_selectors(&a.and_then(&b)),
                    vec![
                        RowSelector::select(2),
                        RowSelector::skip(1),
                        RowSelector::select(1),
                        RowSelector::skip(4)
                    ]
                );
            },
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

        let expected = RowSelectorSelection::from(vec![
            RowSelector::skip(6),
            RowSelector::select(10),
            RowSelector::skip(4),
        ]);

        assert_eq!(RowSelectorSelection::from_iter(a), expected);
        assert_eq!(RowSelectorSelection::from_iter(b), expected);
        assert_eq!(RowSelectorSelection::from_iter(c), expected);
    }

    #[test]
    fn test_combine_2elements() {
        test_selection(
            || RowSelection::from_iter(vec![RowSelector::select(10), RowSelector::select(5)]),
            |selection| {
                let expected = vec![RowSelector::select(15)];
                assert_eq!(into_selectors(selection), expected);
            },
        );

        test_selection(
            || RowSelection::from_iter(vec![RowSelector::select(10), RowSelector::skip(5)]),
            |selection| {
                let expected = vec![RowSelector::select(10), RowSelector::skip(5)];
                assert_eq!(into_selectors(selection), expected);
            },
        );

        test_selection(
            || RowSelection::from_iter(vec![RowSelector::skip(10), RowSelector::select(5)]),
            |selection| {
                let expected = vec![RowSelector::skip(10), RowSelector::select(5)];
                assert_eq!(into_selectors(selection), expected);
            },
        );

        test_selection(
            || RowSelection::from_iter(vec![RowSelector::skip(10), RowSelector::skip(5)]),
            |selection| {
                let expected = vec![RowSelector::skip(15)];
                assert_eq!(into_selectors(selection), expected);
            },
        );

        let a = vec![RowSelector::select(10)];
        test_selection(
            || RowSelection::from_iter(a.clone()),
            |selection| {
                assert_eq!(&into_selectors(selection), &a);
            },
        );

        let b = vec![];
        test_selection(
            || RowSelection::from_iter(b.clone()),
            |selection| {
                assert_eq!(&into_selectors(selection), &b);
            },
        );
    }

    #[test]
    #[should_panic(expected = "selection exceeds the number of selected rows")]
    fn test_and_longer_selectors() {
        let (a, b) = longer_selectors();
        let a = force_selectors(a);
        let b = force_selectors(b);
        a.and_then(&b);
    }

    #[test]
    #[should_panic(expected = "The 'other' selection must have exactly as many set bits as 'self'")]
    fn test_and_longer_mask() {
        let (a, b) = longer_selectors();
        let a = force_mask(a);
        let b = force_mask(b);
        a.and_then(&b);
    }

    /// Returns two RowSelections where the first is longer than the second
    fn longer_selectors() -> (RowSelection, RowSelection) {
        (
            RowSelection::from(vec![
                RowSelector::select(3),
                RowSelector::skip(33),
                RowSelector::select(3),
                RowSelector::skip(33),
            ]),
            RowSelection::from(vec![RowSelector::select(36)]),
        )
    }

    #[test]
    #[should_panic(expected = "selection contains less than the number of selected rows")]
    fn test_and_shorter() {
        let a = RowSelectorSelection::from(vec![
            RowSelector::select(3),
            RowSelector::skip(33),
            RowSelector::select(3),
            RowSelector::skip(33),
        ]);
        let b = RowSelectorSelection::from(vec![RowSelector::select(3)]);
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
            res.selectors,
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
            res.selectors,
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
            res.selectors,
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
            res.selectors,
            vec![RowSelector::select(2), RowSelector::skip(8)]
        );
    }

    #[test]
    fn test_and_fuzz() {
        let mut rand = rng();
        for _ in 0..100 {
            let a_len = rand.random_range(10..100);
            let a_bools: Vec<_> = (0..a_len).map(|_| rand.random_bool(0.2)).collect();
            let a = RowSelectorSelection::from_filters(&[BooleanArray::from(a_bools.clone())]);

            let b_len: usize = a_bools.iter().map(|x| *x as usize).sum();
            let b_bools: Vec<_> = (0..b_len).map(|_| rand.random_bool(0.8)).collect();
            let b = RowSelectorSelection::from_filters(&[BooleanArray::from(b_bools.clone())]);

            let mut expected_bools = vec![false; a_len];

            let mut iter_b = b_bools.iter();
            for (idx, b) in a_bools.iter().enumerate() {
                if *b && *iter_b.next().unwrap() {
                    expected_bools[idx] = true;
                }
            }

            let expected =
                RowSelectorSelection::from_filters(&[BooleanArray::from(expected_bools)]);

            let total_rows: usize = expected.selectors.iter().map(|s| s.row_count).sum();
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

        let round_tripped = RowSelectorSelection::from(selectors.clone())
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(selectors, round_tripped);
    }

    #[test]
    fn test_limit() {
        // Limit to existing limit should no-op
        let selection =
            RowSelectorSelection::from(vec![RowSelector::select(10), RowSelector::skip(90)]);
        let limited = selection.limit(10);
        assert_eq!(
            RowSelectorSelection::from(vec![RowSelector::select(10)]),
            limited
        );

        let selection = RowSelectorSelection::from(vec![
            RowSelector::select(10),
            RowSelector::skip(10),
            RowSelector::select(10),
            RowSelector::skip(10),
            RowSelector::select(10),
        ]);

        let limited = selection.clone().limit(5);
        let expected = vec![RowSelector::select(5)];
        assert_eq!(limited.selectors, expected);

        let limited = selection.clone().limit(15);
        let expected = vec![
            RowSelector::select(10),
            RowSelector::skip(10),
            RowSelector::select(5),
        ];
        assert_eq!(limited.selectors, expected);

        let limited = selection.clone().limit(0);
        let expected = vec![];
        assert_eq!(limited.selectors, expected);

        let limited = selection.clone().limit(30);
        let expected = vec![
            RowSelector::select(10),
            RowSelector::skip(10),
            RowSelector::select(10),
            RowSelector::skip(10),
            RowSelector::select(10),
        ];
        assert_eq!(limited.selectors, expected);

        let limited = selection.limit(100);
        let expected = vec![
            RowSelector::select(10),
            RowSelector::skip(10),
            RowSelector::select(10),
            RowSelector::skip(10),
            RowSelector::select(10),
        ];
        assert_eq!(limited.selectors, expected);
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

        test_selection(
            || {
                RowSelection::from(vec![
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
                ])
            },
            |selection| {
                let ranges = selection.scan_ranges(&index);

                // assert_eq!(mask, vec![false, true, true, false, true, true, false]);
                assert_eq!(ranges, vec![10..20, 20..30, 40..50, 50..60]);
            },
        );

        test_selection(
            || {
                RowSelection::from(vec![
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
                ])
            },
            |selection| {
                let ranges = selection.scan_ranges(&index);

                // assert_eq!(mask, vec![false, true, true, false, true, true, true]);
                assert_eq!(ranges, vec![10..20, 20..30, 40..50, 50..60, 60..70]);
            },
        );

        test_selection(
            || {
                RowSelection::from(vec![
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
                ])
            },
            |selection| {
                let ranges = selection.scan_ranges(&index);

                // assert_eq!(mask, vec![false, true, true, false, true, true, true]);
                assert_eq!(ranges, vec![10..20, 20..30, 40..50, 50..60, 60..70]);
            },
        );

        test_selection(
            || {
                RowSelection::from(vec![
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
                ])
            },
            |selection| {
                let ranges = selection.scan_ranges(&index);
                // assert_eq!(mask, vec![false, true, true, false, true, true, true]);
                assert_eq!(ranges, vec![10..20, 20..30, 30..40]);
            },
        );
    }

    #[test]
    fn test_from_ranges() {
        let ranges = [1..3, 4..6, 6..6, 8..8, 9..10];
        let selection = RowSelectorSelection::from_consecutive_ranges(ranges.into_iter(), 10);
        assert_eq!(
            selection.selectors,
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
            RowSelectorSelection::from_consecutive_ranges(out_of_order_ranges.into_iter(), 10)
        });
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_selector() {
        let selection = RowSelectorSelection::from(vec![
            RowSelector::skip(0),
            RowSelector::select(2),
            RowSelector::skip(0),
            RowSelector::select(2),
        ]);
        assert_eq!(selection.selectors, vec![RowSelector::select(4)]);

        let selection = RowSelectorSelection::from(vec![
            RowSelector::select(0),
            RowSelector::skip(2),
            RowSelector::select(0),
            RowSelector::skip(2),
        ]);
        assert_eq!(selection.selectors, vec![RowSelector::skip(4)]);
    }

    #[test]
    fn test_intersection() {
        let selection = RowSelectorSelection::from(vec![RowSelector::select(1048576)]);
        let result = selection.intersection(&selection);
        assert_eq!(result, selection);

        let a = RowSelectorSelection::from(vec![
            RowSelector::skip(10),
            RowSelector::select(10),
            RowSelector::skip(10),
            RowSelector::select(20),
        ]);

        let b = RowSelectorSelection::from(vec![
            RowSelector::skip(20),
            RowSelector::select(20),
            RowSelector::skip(10),
        ]);

        let result = a.intersection(&b);
        assert_eq!(
            result.selectors,
            vec![
                RowSelector::skip(30),
                RowSelector::select(10),
                RowSelector::skip(10)
            ]
        );
    }

    #[test]
    fn test_union() {
        let selection = RowSelectorSelection::from(vec![RowSelector::select(1048576)]);
        let result = selection.union(&selection);
        assert_eq!(result, selection);

        // NYNYY
        let a = RowSelectorSelection::from(vec![
            RowSelector::skip(10),
            RowSelector::select(10),
            RowSelector::skip(10),
            RowSelector::select(20),
        ]);

        // NNYYNYN
        let b = RowSelectorSelection::from(vec![
            RowSelector::skip(20),
            RowSelector::select(20),
            RowSelector::skip(10),
            RowSelector::select(10),
            RowSelector::skip(10),
        ]);

        let result = a.union(&b);

        // NYYYYYN
        assert_eq!(
            result.iter().collect::<Vec<_>>(),
            vec![
                &RowSelector::skip(10),
                &RowSelector::select(50),
                &RowSelector::skip(10),
            ]
        );
    }

    #[test]
    fn test_row_count() {
        let selection = RowSelectorSelection::from(vec![
            RowSelector::skip(34),
            RowSelector::select(12),
            RowSelector::skip(3),
            RowSelector::select(35),
        ]);

        assert_eq!(selection.row_count(), 12 + 35);
        assert_eq!(selection.skipped_row_count(), 34 + 3);

        let selection =
            RowSelectorSelection::from(vec![RowSelector::select(12), RowSelector::select(35)]);

        assert_eq!(selection.row_count(), 12 + 35);
        assert_eq!(selection.skipped_row_count(), 0);

        let selection =
            RowSelectorSelection::from(vec![RowSelector::skip(34), RowSelector::skip(3)]);

        assert_eq!(selection.row_count(), 0);
        assert_eq!(selection.skipped_row_count(), 34 + 3);

        let selection = RowSelectorSelection::from(vec![]);

        assert_eq!(selection.row_count(), 0);
        assert_eq!(selection.skipped_row_count(), 0);
    }

    #[test]
    fn test_trim() {
        test_selection(
            || {
                RowSelection::from(vec![
                    RowSelector::skip(34),
                    RowSelector::select(12),
                    RowSelector::skip(3),
                    RowSelector::select(35),
                ])
            },
            |selection| {
                let expected = vec![
                    RowSelector::skip(34),
                    RowSelector::select(12),
                    RowSelector::skip(3),
                    RowSelector::select(35),
                ];

                let trimmed = selection.clone().trim();
                assert_eq!(into_selectors(&trimmed), expected);
            },
        );

        test_selection(
            || {
                RowSelection::from(vec![
                    RowSelector::skip(34),
                    RowSelector::select(12),
                    RowSelector::skip(3),
                ])
            },
            |selection| {
                let trimmed = selection.clone().trim();
                let expected = vec![RowSelector::skip(34), RowSelector::select(12)];

                assert_eq!(into_selectors(&trimmed), expected);
            },
        );
    }

    #[test]
    fn test_expand_to_batch_boundaries() {
        test_selection(
            || {
                // selection goes up to row 28, total 30 rows
                RowSelection::from(vec![
                    // select 1 row in range 0..8
                    RowSelector::skip(5),
                    RowSelector::select(1),
                    RowSelector::skip(2),
                    // select no rows in range 8..16
                    RowSelector::skip(8),
                    // select 7 rows in range 16..24
                    RowSelector::skip(1),
                    RowSelector::select(7),
                    // select 1 rows in range 24..30
                    RowSelector::skip(4),
                    RowSelector::select(1),
                    RowSelector::skip(1),
                ])
            },
            |selection| {
                let expanded = selection.expand_to_batch_boundaries(8, 30);
                let expected = vec![
                    RowSelector::select(8),
                    RowSelector::skip(8),
                    RowSelector::select(14),
                ];

                assert_eq!(into_selectors(&expanded), expected);
            },
        );

        // test when there is no selected rows at the end
        test_selection(
            || {
                RowSelection::from(vec![
                    // select now rows in range 0..8
                    RowSelector::skip(8),
                    // select 6 rows in range 8..16
                    RowSelector::skip(1),
                    RowSelector::select(6),
                    RowSelector::skip(1),
                    // select a row in range 16..24
                    RowSelector::skip(4),
                    RowSelector::select(2),
                    RowSelector::skip(2),
                    // no rows in range 24..30
                    RowSelector::skip(6),
                ])
            },
            |selection| {
                let expanded = selection.expand_to_batch_boundaries(8, 30);
                let expected = vec![
                    RowSelector::skip(8),
                    RowSelector::select(16),
                    RowSelector::skip(6),
                ];

                assert_eq!(into_selectors(&expanded), expected);
            },
        );

        // test very sparse selection,
        test_selection(
            || {
                RowSelection::from(vec![
                    // select 1 row
                    RowSelector::skip(4),
                    RowSelector::select(1),
                    RowSelector::skip(2),
                ])
            },
            |selection| {
                let expanded = selection.expand_to_batch_boundaries(8, 1000);
                let expected = vec![RowSelector::select(8), RowSelector::skip(992)];

                assert_eq!(into_selectors(&expanded), expected);
            },
        );

        // test sliced selection
        test_selection(
            || {
                RowSelection::from(vec![
                    RowSelector::select(16),
                    RowSelector::skip(15),
                    RowSelector::select(100),
                ])
            },
            |selection| {
                // slice first
                let expanded = selection
                    .clone()
                    .offset(11)
                    .expand_to_batch_boundaries(8, 1000);
                let expected = vec![
                    RowSelector::skip(8),
                    RowSelector::select(8),
                    RowSelector::skip(8),
                    RowSelector::select(112),
                    RowSelector::skip(864),
                ];

                assert_eq!(into_selectors(&expanded), expected);
            },
        );
    }

    /// Runs `verify_fn(s1)` with both Mask and Selector backed cursors
    ///
    /// This is used to ensuring both cursor types behave identically
    /// for the same logical operations.
    ///
    /// Tests are written in terms of two closures:
    /// 1. Create a `RowSelection` using the provided `create_fn`
    /// 2. Verify the created selection using the provided `verify_fn`
    ///
    /// This function will invoke the `verify_fn` with both a Selector-backed and
    /// Mask-backed `RowSelection`, ensuring both behave identically.
    fn test_selection<C, V>(create_fn: C, verify_fn: V)
    where
        C: FnOnce() -> RowSelection,
        V: Fn(&RowSelection),
    {
        let selection = create_fn();
        test_two_selections(
            move || {
                let empty = RowSelection::from(vec![]);
                (selection, empty)
            },
            |c1, _empty| {
                verify_fn(c1);
            },
        );
    }

    /// Runs `verify_fn(s1, s2)` with all combinations of Mask and Selector backed
    /// cursors
    ///
    /// This is used to ensuring both cursor types behave identically
    /// for the same logical operations.
    ///
    /// Tests are written in terms of two closures:
    /// 1. Create 2 `RowSelection`s using the provided `create_fn`
    /// 2. Verify the created selection using the provided `verify_fn`
    ///
    /// This function will invoke the verify_fn with all combinations of Selector-backed and
    /// Mask-backed `RowSelection`, ensuring both behave identically.
    fn test_two_selections<C, V>(create_fn: C, verify_fn: V)
    where
        C: FnOnce() -> (RowSelection, RowSelection),
        V: Fn(&RowSelection, &RowSelection),
    {
        let (selection1, selection2) = create_fn();
        verify_round_trip(&selection1);
        verify_round_trip(&selection2);
        let combos = vec![
            TestCombination {
                description: "Selector-Selector",
                selection1: force_selectors(selection1.clone()),
                selection2: force_selectors(selection2.clone()),
            },
            TestCombination {
                description: "Selector-Mask",
                selection1: force_selectors(selection1.clone()),
                selection2: force_mask(selection2.clone()),
            },
            TestCombination {
                description: "Mask-Selector",
                selection1: force_mask(selection1.clone()),
                selection2: force_selectors(selection2.clone()),
            },
            TestCombination {
                description: "Mask-Mask",
                selection1: force_mask(selection1.clone()),
                selection2: force_mask(selection2.clone()),
            },
        ];

        for TestCombination {
            description,
            selection1,
            selection2,
        } in combos
        {
            println!("Verifying combination: {description}:\n\n{selection1:#?}\n\n{selection2:#?}",);
            verify_fn(&selection1, &selection2);
        }
    }

    struct TestCombination {
        description: &'static str,
        selection1: RowSelection,
        selection2: RowSelection,
    }

    fn verify_round_trip(selection: &RowSelection) {
        let selectors = force_selectors(selection.clone());
        let mask = force_mask(selection.clone());

        assert_eq!(force_mask(selectors.clone()), mask);
        assert_eq!(force_selectors(mask.clone()), selectors);
    }

    fn force_selectors(selection: RowSelection) -> RowSelection {
        let selectors: Vec<RowSelector> = selection.into();
        let selector_selection = RowSelectorSelection::from(selectors);
        let selection = RowSelection::from(selector_selection);
        assert!(matches!(selection, RowSelection::Selectors(_)));
        selection
    }

    fn force_mask(selection: RowSelection) -> RowSelection {
        let mask: BooleanArray = selection.into();
        let mask_selection = BitmaskSelection::from(mask);
        let selection = RowSelection::from(mask_selection);
        assert!(matches!(selection, RowSelection::Mask(_)));
        selection
    }

    fn into_selectors(selection: &RowSelection) -> Vec<RowSelector> {
        selection.clone().into()
    }
}
