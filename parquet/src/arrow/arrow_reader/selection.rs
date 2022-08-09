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
#[derive(Debug, Clone, Copy)]
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
#[derive(Debug, Clone, Default)]
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

    /// Splits off `row_count` from this [`RowSelection`]
    pub fn split_off(&mut self, row_count: usize) -> Self {
        let mut total_count = 0;

        // Find the index where the selector exceeds the row count
        let find = self.selectors.iter().enumerate().find(|(_, selector)| {
            total_count += selector.row_count;
            total_count >= row_count
        });

        let split_idx = match find {
            Some((idx, _)) => idx,
            None => return Self::default(),
        };

        let mut remaining = self.selectors.split_off(split_idx);
        if total_count != row_count {
            let overflow = total_count - row_count;
            let rem = remaining.first_mut().unwrap();
            rem.row_count -= overflow;

            self.selectors.push(RowSelector {
                row_count,
                skip: rem.skip,
            })
        }

        std::mem::swap(&mut remaining, &mut self.selectors);
        Self {
            selectors: remaining,
        }
    }

    /// Given a [`RowSelection`] computed under `self` returns the [`RowSelection`]
    /// representing their conjunction
    pub fn and(&self, other: &Self) -> Self {
        let mut selectors = vec![];
        let mut first = self.selectors.iter().cloned().peekable();
        let mut second = other.selectors.iter().cloned().peekable();

        let mut to_skip = 0;
        while let (Some(a), Some(b)) = (first.peek_mut(), second.peek_mut()) {
            if a.row_count == 0 {
                first.next().unwrap();
                continue;
            }

            if b.row_count == 0 {
                second.next().unwrap();
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
