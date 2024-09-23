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

use arrow_array::{Array, BooleanArray};
use arrow_select::filter::SlicesIterator;
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::ops::Range;

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
/// ```
///
/// A [`RowSelection`] maintains the following invariants:
///
/// * It contains no [`RowSelector`] of 0 rows
/// * Consecutive [`RowSelector`]s alternate skipping or selecting rows
///
/// [`PageIndex`]: crate::file::page_index::index::PageIndex
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct RowSelection {
    selectors: Vec<RowSelector>,
}

impl RowSelection {
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

        Self { selectors }
    }

    /// Given an offset index, return the byte ranges for all data pages selected by `self`
    ///
    /// This is useful for determining what byte ranges to fetch from underlying storage
    ///
    /// Note: this method does not make any effort to combine consecutive ranges, nor coalesce
    /// ranges that are close together. This is instead delegated to the IO subsystem to optimise,
    /// e.g. [`ObjectStore::get_ranges`](object_store::ObjectStore::get_ranges)
    pub fn scan_ranges(&self, page_locations: &[crate::format::PageLocation]) -> Vec<Range<usize>> {
        let mut ranges = vec![];
        let mut row_offset = 0;

        let mut pages = page_locations.iter().peekable();
        let mut selectors = self.selectors.iter().cloned();
        let mut current_selector = selectors.next();
        let mut current_page = pages.next();

        let mut current_page_included = false;

        while let Some((selector, page)) = current_selector.as_mut().zip(current_page) {
            if !(selector.skip || current_page_included) {
                let start = page.offset as usize;
                let end = start + page.compressed_page_size as usize;
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
                    let start = page.offset as usize;
                    let end = start + page.compressed_page_size as usize;
                    ranges.push(start..end);
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
}

impl From<Vec<RowSelector>> for RowSelection {
    fn from(selectors: Vec<RowSelector>) -> Self {
        selectors.into_iter().collect()
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

        Self { selectors }
    }
}

impl From<RowSelection> for Vec<RowSelector> {
    fn from(r: RowSelection) -> Self {
        r.selectors
    }
}

impl From<RowSelection> for VecDeque<RowSelector> {
    fn from(r: RowSelection) -> Self {
        r.selectors.into()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::PageLocation;
    use rand::{thread_rng, Rng};

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
            selection.selectors,
            vec![RowSelector::skip(3), RowSelector::select(4)]
        );

        let selection = RowSelection::from_filters(&filters[..2]);
        assert!(selection.selects_any());
        assert_eq!(
            selection.selectors,
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
            selection.selectors,
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
        assert_eq!(selection.selectors, vec![RowSelector::skip(4)]);
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
        assert_eq!(split.selectors, vec![RowSelector::skip(34)]);
        assert_eq!(
            selection.selectors,
            vec![
                RowSelector::select(12),
                RowSelector::skip(3),
                RowSelector::select(35)
            ]
        );

        let split = selection.split_off(5);
        assert_eq!(split.selectors, vec![RowSelector::select(5)]);
        assert_eq!(
            selection.selectors,
            vec![
                RowSelector::select(7),
                RowSelector::skip(3),
                RowSelector::select(35)
            ]
        );

        let split = selection.split_off(8);
        assert_eq!(
            split.selectors,
            vec![RowSelector::select(7), RowSelector::skip(1)]
        );
        assert_eq!(
            selection.selectors,
            vec![RowSelector::skip(2), RowSelector::select(35)]
        );

        let split = selection.split_off(200);
        assert_eq!(
            split.selectors,
            vec![RowSelector::skip(2), RowSelector::select(35)]
        );
        assert!(selection.selectors.is_empty());
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
            selection.selectors,
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
            selection.selectors,
            vec![
                RowSelector::skip(30),
                RowSelector::select(5),
                RowSelector::skip(33),
                RowSelector::select(6),
            ]
        );

        let selection = selection.offset(3);
        assert_eq!(
            selection.selectors,
            vec![
                RowSelector::skip(33),
                RowSelector::select(2),
                RowSelector::skip(33),
                RowSelector::select(6),
            ]
        );

        let selection = selection.offset(2);
        assert_eq!(
            selection.selectors,
            vec![RowSelector::skip(68), RowSelector::select(6),]
        );

        let selection = selection.offset(3);
        assert_eq!(
            selection.selectors,
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
            a.and_then(&b).selectors,
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
        assert_eq!(RowSelection::from_iter(a).selectors, a_expect);

        let b = vec![RowSelector::select(10), RowSelector::skip(5)];
        let b_expect = vec![RowSelector::select(10), RowSelector::skip(5)];
        assert_eq!(RowSelection::from_iter(b).selectors, b_expect);

        let c = vec![RowSelector::skip(10), RowSelector::select(5)];
        let c_expect = vec![RowSelector::skip(10), RowSelector::select(5)];
        assert_eq!(RowSelection::from_iter(c).selectors, c_expect);

        let d = vec![RowSelector::skip(10), RowSelector::skip(5)];
        let d_expect = vec![RowSelector::skip(15)];
        assert_eq!(RowSelection::from_iter(d).selectors, d_expect);
    }

    #[test]
    fn test_from_one_and_empty() {
        let a = vec![RowSelector::select(10)];
        let selection1 = RowSelection::from(a.clone());
        assert_eq!(selection1.selectors, a);

        let b = vec![];
        let selection1 = RowSelection::from(b.clone());
        assert_eq!(selection1.selectors, b)
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
        let mut rand = thread_rng();
        for _ in 0..100 {
            let a_len = rand.gen_range(10..100);
            let a_bools: Vec<_> = (0..a_len).map(|_| rand.gen_bool(0.2)).collect();
            let a = RowSelection::from_filters(&[BooleanArray::from(a_bools.clone())]);

            let b_len: usize = a_bools.iter().map(|x| *x as usize).sum();
            let b_bools: Vec<_> = (0..b_len).map(|_| rand.gen_bool(0.8)).collect();
            let b = RowSelection::from_filters(&[BooleanArray::from(b_bools.clone())]);

            let mut expected_bools = vec![false; a_len];

            let mut iter_b = b_bools.iter();
            for (idx, b) in a_bools.iter().enumerate() {
                if *b && *iter_b.next().unwrap() {
                    expected_bools[idx] = true;
                }
            }

            let expected = RowSelection::from_filters(&[BooleanArray::from(expected_bools)]);

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

        let round_tripped = RowSelection::from(selectors.clone())
            .iter()
            .cloned()
            .collect::<Vec<_>>();
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
        assert_eq!(selection.selectors, vec![RowSelector::select(4)]);

        let selection = RowSelection::from(vec![
            RowSelector::select(0),
            RowSelector::skip(2),
            RowSelector::select(0),
            RowSelector::skip(2),
        ]);
        assert_eq!(selection.selectors, vec![RowSelector::skip(4)]);
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
}
