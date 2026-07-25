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

//! The run length backed representation of a [`RowSelection`]: [`RowSelector`]
//! and the primitives operating on a sequence of them.
//!
//! This is the counterpart of the bitmap backing in the `boolean` module, and
//! provides the same set of transforms (`split_off`, `trim`, `offset`,
//! `limit`) over `Vec<RowSelector>` instead of a `BooleanBuffer`.
//!
//! [`RowSelection`]: crate::arrow::arrow_reader::RowSelection

use std::cmp::Ordering;
use std::ops::Range;

/// [`RowSelection`] is a collection of [`RowSelector`] used to skip rows when
/// scanning a parquet file
///
/// [`RowSelection`]: crate::arrow::arrow_reader::RowSelection
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

/// Borrowed iterator over the [`RowSelector`]s of a
/// [`RowSelection`](crate::arrow::arrow_reader::RowSelection).
#[derive(Debug)]
pub struct RowSelectionIter<'a>(std::slice::Iter<'a, RowSelector>);

impl<'a> RowSelectionIter<'a> {
    pub(super) fn new(selectors: &'a [RowSelector]) -> Self {
        Self(selectors.iter())
    }
}

impl<'a> Iterator for RowSelectionIter<'a> {
    type Item = &'a RowSelector;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }

    #[inline]
    fn count(self) -> usize {
        self.0.count()
    }

    #[inline]
    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.0.nth(n)
    }

    #[inline]
    fn last(self) -> Option<Self::Item> {
        self.0.last()
    }

    #[inline]
    fn fold<B, F>(self, init: B, f: F) -> B
    where
        F: FnMut(B, Self::Item) -> B,
    {
        self.0.fold(init, f)
    }
}

impl ExactSizeIterator for RowSelectionIter<'_> {}

// once it returns None, it will continue returning None
impl std::iter::FusedIterator for RowSelectionIter<'_> {}

/// Normalizes a sequence of selectors: drops the empty ones and combines
/// consecutive selectors that both skip or both select.
pub(super) fn combine_selectors<I>(iter: I) -> Vec<RowSelector>
where
    I: IntoIterator<Item = RowSelector>,
{
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

    selectors
}

/// Builds the selectors keeping `ranges` out of `total_rows` rows.
///
/// # Panics
///
/// Panics if `ranges` are not in ascending order.
pub(super) fn selectors_from_consecutive_ranges<I>(ranges: I, total_rows: usize) -> Vec<RowSelector>
where
    I: Iterator<Item = Range<usize>>,
{
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

    selectors
}

/// Splits the first `row_count` rows off `selectors`, returning them and
/// leaving the remainder in place.
pub(super) fn split_off_selectors(
    selectors: &mut Vec<RowSelector>,
    row_count: usize,
) -> Vec<RowSelector> {
    let mut total_count = 0;

    // Find the index where the selector exceeds the row count
    let find = selectors.iter().position(|selector| {
        total_count += selector.row_count;
        total_count > row_count
    });

    let split_idx = match find {
        Some(idx) => idx,
        None => return std::mem::take(selectors),
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
    remaining
}

/// Removes any trailing skips from `selectors`.
pub(super) fn trim_selectors(selectors: &mut Vec<RowSelector>) {
    while selectors.last().map(|x| x.skip).unwrap_or(false) {
        selectors.pop();
    }
}

/// Skips the first `offset` selected rows of `selectors`.
pub(super) fn offset_selectors(mut selectors: Vec<RowSelector>, offset: usize) -> Vec<RowSelector> {
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
            return selectors;
        }
    };

    let mut new_selectors = Vec::with_capacity(selectors.len() - split_idx + 1);
    new_selectors.push(RowSelector::skip(skipped_count + offset));
    new_selectors.push(RowSelector::select(selected_count - offset));
    new_selectors.extend_from_slice(&selectors[split_idx + 1..]);

    new_selectors
}

/// Keeps only the first `limit` selected rows of `selectors`.
pub(super) fn limit_selectors(
    mut selectors: Vec<RowSelector>,
    mut limit: usize,
) -> Vec<RowSelector> {
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
    selectors
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::arrow_reader::selection::RowSelection;

    #[test]
    fn test_from_selectors_skips_empty_selectors() {
        let selection = RowSelection::from(vec![
            RowSelector::select(0),
            RowSelector::skip(0),
            RowSelector::select(2),
            RowSelector::select(0),
            RowSelector::skip(1),
        ]);
        assert_eq!(
            selection.selectors(),
            vec![RowSelector::select(2), RowSelector::skip(1)]
        );
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
