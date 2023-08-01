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

//! Defines partition kernel for `ArrayRef`

use crate::sort::{LexicographicalComparator, SortColumn};
use arrow_schema::ArrowError;
use std::cmp::Ordering;
use std::ops::Range;

/// Given a list of already sorted columns, returns [`Range`]es that
/// partition the input such that each partition has equal values
/// across sort columns.
///
/// Note that the columns *MUST* be pre-sorted otherwise the result is
/// undefined.
///
/// Returns an iterator with `k` items where `k` is cardinality of the
/// sort values: Consecutive values will be connected: `(a, b)` and `(b,
/// c)`, where `start = 0` and `end = n` for the first and last range.
///
/// # Example:
///
/// For example, given columns `x`, `y` and `z`, calling
/// `lexicographical_partition_ranges(values, (x, y))` will divide the
/// rows into ranges where the values of `(x, y)` are equal:
///
/// ```text
/// ┌ ─ ┬───┬ ─ ─┌───┐─ ─ ┬───┬ ─ ─ ┐
///     │ 1 │    │ 1 │    │ A │        Range: 0..1 (x=1, y=1)
/// ├ ─ ┼───┼ ─ ─├───┤─ ─ ┼───┼ ─ ─ ┤
///     │ 1 │    │ 2 │    │ B │
/// │   ├───┤    ├───┤    ├───┤     │
///     │ 1 │    │ 2 │    │ C │        Range: 1..4 (x=1, y=2)
/// │   ├───┤    ├───┤    ├───┤     │
///     │ 1 │    │ 2 │    │ D │
/// ├ ─ ┼───┼ ─ ─├───┤─ ─ ┼───┼ ─ ─ ┤
///     │ 2 │    │ 1 │    │ E │        Range: 4..5 (x=2, y=1)
/// ├ ─ ┼───┼ ─ ─├───┤─ ─ ┼───┼ ─ ─ ┤
///     │ 3 │    │ 1 │    │ F │        Range: 5..6 (x=3, y=1)
/// └ ─ ┴───┴ ─ ─└───┘─ ─ ┴───┴ ─ ─ ┘
///
///       x        y        z     lexicographical_partition_ranges
///                               by (x,y)
/// ```
///
/// # Example Code
///
/// ```
/// # use std::{sync::Arc, ops::Range};
/// # use arrow_array::{RecordBatch, Int64Array, StringArray, ArrayRef};
/// # use arrow_ord::sort::{SortColumn, SortOptions};
/// # use arrow_ord::partition::lexicographical_partition_ranges;
/// let batch = RecordBatch::try_from_iter(vec![
///     ("x", Arc::new(Int64Array::from(vec![1, 1, 1, 1, 2, 3])) as ArrayRef),
///     ("y", Arc::new(Int64Array::from(vec![1, 2, 2, 2, 1, 1])) as ArrayRef),
///     ("z", Arc::new(StringArray::from(vec!["A", "B", "C", "D", "E", "F"])) as ArrayRef),
/// ]).unwrap();
///
/// // Lexographically partition on (x, y)
/// let sort_columns = vec![
///     SortColumn {
///         values: batch.column(0).clone(),
///         options: Some(SortOptions::default()),
///     },
///     SortColumn {
///         values: batch.column(1).clone(),
///         options: Some(SortOptions::default()),
///  },
/// ];
/// let ranges:Vec<Range<usize>> = lexicographical_partition_ranges(&sort_columns)
///   .unwrap()
///   .collect();
///
/// let expected = vec![
///     (0..1),
///     (1..4),
///     (4..5),
///     (5..6),
/// ];
///
/// assert_eq!(ranges, expected);
/// ```
pub fn lexicographical_partition_ranges(
    columns: &[SortColumn],
) -> Result<impl Iterator<Item = Range<usize>> + '_, ArrowError> {
    LexicographicalPartitionIterator::try_new(columns)
}

struct LexicographicalPartitionIterator<'a> {
    comparator: LexicographicalComparator<'a>,
    num_rows: usize,
    previous_partition_point: usize,
    partition_point: usize,
}

impl<'a> LexicographicalPartitionIterator<'a> {
    fn try_new(
        columns: &'a [SortColumn],
    ) -> Result<LexicographicalPartitionIterator, ArrowError> {
        if columns.is_empty() {
            return Err(ArrowError::InvalidArgumentError(
                "Sort requires at least one column".to_string(),
            ));
        }
        let num_rows = columns[0].values.len();
        if columns.iter().any(|item| item.values.len() != num_rows) {
            return Err(ArrowError::ComputeError(
                "Lexical sort columns have different row counts".to_string(),
            ));
        };

        let comparator = LexicographicalComparator::try_new(columns)?;
        Ok(LexicographicalPartitionIterator {
            comparator,
            num_rows,
            previous_partition_point: 0,
            partition_point: 0,
        })
    }
}

/// Returns the next partition point of the range `start..end` according to the given comparator.
/// The return value is the index of the first element of the second partition,
/// and is guaranteed to be between `start..=end` (inclusive).
///
/// The values corresponding to those indices are assumed to be partitioned according to the given comparator.
///
/// Exponential search is to remedy for the case when array size and cardinality are both large.
/// In these cases the partition point would be near the beginning of the range and
/// plain binary search would be doing some unnecessary iterations on each call.
///
/// see <https://en.wikipedia.org/wiki/Exponential_search>
#[inline]
fn exponential_search_next_partition_point(
    start: usize,
    end: usize,
    comparator: &LexicographicalComparator<'_>,
) -> usize {
    let target = start;
    let mut bound = 1;
    while bound + start < end
        && comparator.compare(bound + start, target) != Ordering::Greater
    {
        bound *= 2;
    }

    // invariant after while loop:
    // (start + bound / 2) <= target < min(end, start + bound + 1)
    // where <= and < are defined by the comparator;
    // note here we have right = min(end, start + bound + 1) because (start + bound) might
    // actually be considered and must be included.
    partition_point(start + bound / 2, end.min(start + bound + 1), |idx| {
        comparator.compare(idx, target) != Ordering::Greater
    })
}

/// Returns the partition point of the range `start..end` according to the given predicate.
/// The return value is the index of the first element of the second partition,
/// and is guaranteed to be between `start..=end` (inclusive).
///
/// The algorithm is similar to a binary search.
///
/// The values corresponding to those indices are assumed to be partitioned according to the given predicate.
///
/// See [`slice::partition_point`]
#[inline]
fn partition_point<P: Fn(usize) -> bool>(start: usize, end: usize, pred: P) -> usize {
    let mut left = start;
    let mut right = end;
    let mut size = right - left;
    while left < right {
        let mid = left + size / 2;

        let less = pred(mid);

        if less {
            left = mid + 1;
        } else {
            right = mid;
        }

        size = right - left;
    }
    left
}

impl<'a> Iterator for LexicographicalPartitionIterator<'a> {
    type Item = Range<usize>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.partition_point < self.num_rows {
            // invariant:
            // in the range [0..previous_partition_point] all values are <= the value at [previous_partition_point]
            // so in order to save time we can do binary search on the range [previous_partition_point..num_rows]
            // and find the index where any value is greater than the value at [previous_partition_point]
            self.partition_point = exponential_search_next_partition_point(
                self.partition_point,
                self.num_rows,
                &self.comparator,
            );
            let start = self.previous_partition_point;
            let end = self.partition_point;
            self.previous_partition_point = self.partition_point;
            Some(Range { start, end })
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sort::SortOptions;
    use arrow_array::*;
    use arrow_schema::DataType;
    use std::sync::Arc;

    #[test]
    fn test_partition_point() {
        let input = &[1, 1, 1, 2, 2, 2, 2, 2, 2, 3, 3, 3, 4];
        {
            let median = input[input.len() / 2];
            assert_eq!(
                9,
                partition_point(0, input.len(), |i: usize| input[i].cmp(&median)
                    != Ordering::Greater)
            );
        }
        {
            let search = input[9];
            assert_eq!(
                12,
                partition_point(9, input.len(), |i: usize| input[i].cmp(&search)
                    != Ordering::Greater)
            );
        }
        {
            let search = input[0];
            assert_eq!(
                3,
                partition_point(0, 9, |i: usize| input[i].cmp(&search)
                    != Ordering::Greater)
            );
        }
        let input = &[1, 2, 2, 2, 2, 2, 2, 2, 9];
        {
            let search = input[5];
            assert_eq!(
                8,
                partition_point(5, 9, |i: usize| input[i].cmp(&search)
                    != Ordering::Greater)
            );
        }
    }

    #[test]
    fn test_lexicographical_partition_ranges_empty() {
        let input = vec![];
        assert!(
            lexicographical_partition_ranges(&input).is_err(),
            "lexicographical_partition_ranges should reject columns with empty rows"
        );
    }

    #[test]
    fn test_lexicographical_partition_ranges_unaligned_rows() {
        let input = vec![
            SortColumn {
                values: Arc::new(Int64Array::from(vec![None, Some(-1)])) as ArrayRef,
                options: None,
            },
            SortColumn {
                values: Arc::new(StringArray::from(vec![Some("foo")])) as ArrayRef,
                options: None,
            },
        ];
        assert!(
            lexicographical_partition_ranges(&input).is_err(),
            "lexicographical_partition_ranges should reject columns with different row counts"
        );
    }

    #[test]
    fn test_lexicographical_partition_single_column() {
        let input = vec![SortColumn {
            values: Arc::new(Int64Array::from(vec![1, 2, 2, 2, 2, 2, 2, 2, 9]))
                as ArrayRef,
            options: Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
        }];
        let results = lexicographical_partition_ranges(&input).unwrap();
        assert_eq!(
            vec![(0_usize..1_usize), (1_usize..8_usize), (8_usize..9_usize)],
            results.collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_lexicographical_partition_all_equal_values() {
        let input = vec![SortColumn {
            values: Arc::new(Int64Array::from_value(1, 1000)) as ArrayRef,
            options: Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
        }];

        let results = lexicographical_partition_ranges(&input).unwrap();
        assert_eq!(vec![(0_usize..1000_usize)], results.collect::<Vec<_>>());
    }

    #[test]
    fn test_lexicographical_partition_all_null_values() {
        let input = vec![
            SortColumn {
                values: new_null_array(&DataType::Int8, 1000),
                options: Some(SortOptions {
                    descending: false,
                    nulls_first: true,
                }),
            },
            SortColumn {
                values: new_null_array(&DataType::UInt16, 1000),
                options: Some(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
            },
        ];
        let results = lexicographical_partition_ranges(&input).unwrap();
        assert_eq!(vec![(0_usize..1000_usize)], results.collect::<Vec<_>>());
    }

    #[test]
    fn test_lexicographical_partition_unique_column_1() {
        let input = vec![
            SortColumn {
                values: Arc::new(Int64Array::from(vec![None, Some(-1)])) as ArrayRef,
                options: Some(SortOptions {
                    descending: false,
                    nulls_first: true,
                }),
            },
            SortColumn {
                values: Arc::new(StringArray::from(vec![Some("foo"), Some("bar")]))
                    as ArrayRef,
                options: Some(SortOptions {
                    descending: true,
                    nulls_first: true,
                }),
            },
        ];
        let results = lexicographical_partition_ranges(&input).unwrap();
        assert_eq!(
            vec![(0_usize..1_usize), (1_usize..2_usize)],
            results.collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_lexicographical_partition_unique_column_2() {
        let input = vec![
            SortColumn {
                values: Arc::new(Int64Array::from(vec![None, Some(-1), Some(-1)]))
                    as ArrayRef,
                options: Some(SortOptions {
                    descending: false,
                    nulls_first: true,
                }),
            },
            SortColumn {
                values: Arc::new(StringArray::from(vec![
                    Some("foo"),
                    Some("bar"),
                    Some("apple"),
                ])) as ArrayRef,
                options: Some(SortOptions {
                    descending: true,
                    nulls_first: true,
                }),
            },
        ];
        let results = lexicographical_partition_ranges(&input).unwrap();
        assert_eq!(
            vec![(0_usize..1_usize), (1_usize..2_usize), (2_usize..3_usize),],
            results.collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_lexicographical_partition_non_unique_column_1() {
        let input = vec![
            SortColumn {
                values: Arc::new(Int64Array::from(vec![
                    None,
                    Some(-1),
                    Some(-1),
                    Some(1),
                ])) as ArrayRef,
                options: Some(SortOptions {
                    descending: false,
                    nulls_first: true,
                }),
            },
            SortColumn {
                values: Arc::new(StringArray::from(vec![
                    Some("foo"),
                    Some("bar"),
                    Some("bar"),
                    Some("bar"),
                ])) as ArrayRef,
                options: Some(SortOptions {
                    descending: true,
                    nulls_first: true,
                }),
            },
        ];
        let results = lexicographical_partition_ranges(&input).unwrap();
        assert_eq!(
            vec![(0_usize..1_usize), (1_usize..3_usize), (3_usize..4_usize),],
            results.collect::<Vec<_>>()
        );
    }
}
