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

use arrow::array::{Array, BooleanArray};
use arrow::compute::SlicesIterator;
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::ops::Range;

/// [`RowSelector`] represents a range of rows to scan from a parquet file
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
/// [`PageIndex`]: [crate::file::page_index::index::PageIndex]
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
            SlicesIterator::new(filter)
                .map(move |(start, end)| start + offset..end + offset)
        });

        Self::from_consecutive_ranges(iter, total_rows)
    }

    /// Creates a [`RowSelection`] from an iterator of consecutive ranges to keep
    fn from_consecutive_ranges<I: Iterator<Item = Range<usize>>>(
        ranges: I,
        total_rows: usize,
    ) -> Self {
        let mut selectors: Vec<RowSelector> = Vec::with_capacity(ranges.size_hint().0);
        let mut last_end = 0;
        for range in ranges {
            let len = range.end - range.start;

            match range.start.cmp(&last_end) {
                Ordering::Equal => match selectors.last_mut() {
                    Some(last) => last.row_count += len,
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

    /// Splits off the first `row_count` from this [`RowSelection`]
    pub fn split_off(&mut self, row_count: usize) -> Self {
        let mut total_count = 0;

        // Find the index where the selector exceeds the row count
        let find = self.selectors.iter().enumerate().find(|(_, selector)| {
            total_count += selector.row_count;
            total_count > row_count
        });

        let split_idx = match find {
            Some((idx, _)) => idx,
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

    /// Given a [`RowSelection`] computed under `self`, returns the [`RowSelection`]
    /// representing their conjunction
    ///
    /// For example:
    ///
    /// self:     NNNNNNNNNNNNYYYYYYYYYYYYYYYYYYYYYYNNNYYYYY
    /// other:                YYYYYNNNNYYYYYYYYYYYYY   YYNNN
    ///
    /// returned: NNNNNNNNNNNNYYYYYNNNNYYYYYYYYYYYYYYNNYNNNN
    ///
    ///
    pub fn and_then(&self, other: &Self) -> Self {
        let mut selectors = vec![];
        let mut first = self.selectors.iter().cloned().peekable();
        let mut second = other.selectors.iter().cloned().peekable();

        let mut to_skip = 0;
        while let Some(b) = second.peek_mut() {
            let a = first.peek_mut().unwrap();

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
                assert!(v.skip);
                to_skip += v.row_count
            }
        }

        if to_skip != 0 {
            selectors.push(RowSelector::skip(to_skip));
        }

        Self { selectors }
    }

    /// Returns `true` if this [`RowSelection`] selects any rows
    pub fn selects_any(&self) -> bool {
        self.selectors.iter().any(|x| !x.skip)
    }
}

impl From<Vec<RowSelector>> for RowSelection {
    fn from(selectors: Vec<RowSelector>) -> Self {
        Self { selectors }
    }
}

impl From<RowSelection> for VecDeque<RowSelector> {
    fn from(r: RowSelection) -> Self {
        r.selectors.into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
    fn test_and_fuzz() {
        let mut rand = thread_rng();
        for _ in 0..100 {
            let a_len = rand.gen_range(10..100);
            let a_bools: Vec<_> = (0..a_len).map(|x| rand.gen_bool(0.2)).collect();
            let a = RowSelection::from_filters(&[BooleanArray::from(a_bools.clone())]);

            let b_len: usize = a_bools.iter().map(|x| *x as usize).sum();
            let b_bools: Vec<_> = (0..b_len).map(|x| rand.gen_bool(0.8)).collect();
            let b = RowSelection::from_filters(&[BooleanArray::from(b_bools.clone())]);

            let mut expected_bools = vec![false; a_len];

            let mut iter_b = b_bools.iter();
            for (idx, b) in a_bools.iter().enumerate() {
                if *b && *iter_b.next().unwrap() {
                    expected_bools[idx] = true;
                }
            }

            let expected =
                RowSelection::from_filters(&[BooleanArray::from(expected_bools)]);

            let total_rows: usize = expected.selectors.iter().map(|s| s.row_count).sum();
            assert_eq!(a_len, total_rows);

            assert_eq!(a.and_then(&b), expected);
        }
    }
}
