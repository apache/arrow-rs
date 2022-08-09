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
use crate::errors::ParquetError;
use parquet_format::PageLocation;
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::ops::RangeInclusive;

type Range = RangeInclusive<usize>;

pub trait RangeOps {
    fn is_before(&self, other: &Self) -> bool;

    fn is_after(&self, other: &Self) -> bool;

    fn count(&self) -> usize;

    fn union(left: &Range, right: &Range) -> Option<Range>;

    fn intersection(left: &Range, right: &Range) -> Option<Range>;
}

impl RangeOps for Range {
    fn is_before(&self, other: &Range) -> bool {
        self.end() < other.start()
    }

    fn is_after(&self, other: &Range) -> bool {
        self.start() > other.end()
    }

    fn count(&self) -> usize {
        self.end() + 1 - self.start()
    }

    /// Return the union of the two ranges,
    /// Return `None` if there are hole between them.
    fn union(left: &Range, right: &Range) -> Option<Range> {
        if left.start() <= right.start() {
            if left.end() + 1 >= *right.start() {
                return Some(Range::new(
                    *left.start(),
                    std::cmp::max(*left.end(), *right.end()),
                ));
            }
        } else if right.end() + 1 >= *left.start() {
            return Some(Range::new(
                *right.start(),
                std::cmp::max(*left.end(), *right.end()),
            ));
        }
        None
    }

    /// Returns the intersection of the two ranges,
    /// return null if they are not overlapped.
    fn intersection(left: &Range, right: &Range) -> Option<Range> {
        if left.start() <= right.start() {
            if left.end() >= right.start() {
                return Some(Range::new(
                    *right.start(),
                    std::cmp::min(*left.end(), *right.end()),
                ));
            }
        } else if right.end() >= left.start() {
            return Some(Range::new(
                *left.start(),
                std::cmp::min(*left.end(), *right.end()),
            ));
        }
        None
    }
}

/// Struct representing row ranges in a row-group. These row ranges are calculated as a result of using
/// the column index on the filtering.
#[derive(Debug, Clone)]
pub struct RowRanges {
    pub ranges: VecDeque<Range>,
}

impl RowRanges {
    //create an empty RowRanges
    pub fn new_empty() -> Self {
        RowRanges {
            ranges: VecDeque::new(),
        }
    }

    pub fn count(&self) -> usize {
        self.ranges.len()
    }

    pub fn filter_with_mask(&self, mask: &[bool]) -> Result<RowRanges, ParquetError> {
        if self.ranges.len() != mask.len() {
            return Err(ParquetError::General(format!(
                "Mask size {} is not equal to number of pages {}",
                mask.len(),
                self.count()
            )));
        }
        let vec_range = mask
            .iter()
            .zip(self.ranges.clone())
            .filter_map(|(&f, r)| if f { Some(r) } else { None })
            .collect();
        Ok(RowRanges { ranges: vec_range })
    }

    /// Add a range to the end of the list of ranges. It maintains the disjunctive ascending order of the ranges by
    /// trying to union the specified range to the last ranges in the list. The specified range shall be larger than
    /// the last one or might be overlapped with some of the last ones.
    /// [a, b] < [c, d] if b < c
    pub fn add(&mut self, mut range: Range) {
        let count = self.count();
        if count > 0 {
            for i in 1..(count + 1) {
                let index = count - i;
                let last = self.ranges.get(index).unwrap();
                assert!(!last.is_after(&range), "Must add range in ascending!");
                // try to merge range
                match Range::union(last, &range) {
                    None => {
                        break;
                    }
                    Some(r) => {
                        range = r;
                        self.ranges.remove(index);
                    }
                }
            }
        }
        self.ranges.push_back(range);
    }

    /// Calculates the union of the two specified RowRanges object. The union of two range is calculated if there are no
    /// elements between them. Otherwise, the two disjunctive ranges are stored separately.
    /// For example:
    /// [113, 241] ∪ [221, 340] = [113, 330]
    /// [113, 230] ∪ [231, 340] = [113, 340]
    /// while
    /// [113, 230] ∪ [232, 340] = [113, 230], [232, 340]
    ///
    /// The result RowRanges object will contain all the row indexes that were contained in one of the specified objects.
    pub fn union(mut left: RowRanges, mut right: RowRanges) -> RowRanges {
        let v1 = &mut left.ranges;
        let v2 = &mut right.ranges;
        let mut result = RowRanges::new_empty();
        if v2.is_empty() {
            left.clone()
        } else {
            let mut range2 = v2.pop_front().unwrap();
            while !v1.is_empty() {
                let range1 = v1.pop_front().unwrap();
                if range1.is_after(&range2) {
                    result.add(range2);
                    range2 = range1;
                    std::mem::swap(v1, v2);
                } else {
                    result.add(range1);
                }
            }

            result.add(range2);
            while !v2.is_empty() {
                result.add(v2.pop_front().unwrap())
            }

            result
        }
    }

    /// Calculates the intersection of the two specified RowRanges object. Two ranges intersect if they have common
    /// elements otherwise the result is empty.
    /// For example:
    /// [113, 241] ∩ [221, 340] = [221, 241]
    /// while
    /// [113, 230] ∩ [231, 340] = <EMPTY>
    ///
    /// The result RowRanges object will contain all the row indexes there were contained in both of the specified objects
    #[allow(clippy::mut_range_bound)]
    pub fn intersection(left: RowRanges, right: RowRanges) -> RowRanges {
        let mut result = RowRanges::new_empty();
        let mut right_index = 0;
        for l in left.ranges.iter() {
            for i in right_index..right.ranges.len() {
                let r = right.ranges.get(i).unwrap();
                if l.is_before(r) {
                    break;
                } else if l.is_after(r) {
                    right_index = i + 1;
                    continue;
                }
                if let Some(ra) = Range::intersection(l, r) {
                    result.add(ra);
                }
            }
        }
        result
    }

    #[allow(unused)]
    pub fn row_count(&self) -> usize {
        self.ranges.iter().map(|x| x.count()).sum()
    }

    pub fn is_overlapping(&self, x: &Range) -> bool {
        self.ranges
            .binary_search_by(|y| -> Ordering {
                if y.is_before(x) {
                    Ordering::Less
                } else if y.is_after(x) {
                    Ordering::Greater
                } else {
                    Ordering::Equal
                }
            })
            .is_ok()
    }
}

/// Takes an array of [`PageLocation`], and a total number of rows, and based on the provided `page_mask`
/// returns the corresponding [`RowRanges`] to scan
pub fn compute_row_ranges(
    page_mask: &[bool],
    locations: &[PageLocation],
    total_rows: usize,
) -> Result<RowRanges, ParquetError> {
    if page_mask.len() != locations.len() {
        return Err(ParquetError::General(format!(
            "Page_mask size {} is not equal to number of locations {}",
            page_mask.len(),
            locations.len(),
        )));
    }
    let row_ranges = page_locations_to_row_ranges(locations, total_rows)?;
    row_ranges.filter_with_mask(page_mask)
}

fn page_locations_to_row_ranges(
    locations: &[PageLocation],
    total_rows: usize,
) -> Result<RowRanges, ParquetError> {
    if locations.is_empty() || total_rows == 0 {
        return Ok(RowRanges::new_empty());
    }

    // If we read directly from parquet pageIndex to construct locations,
    // the location index should be continuous
    let mut vec_range: VecDeque<Range> = locations
        .windows(2)
        .map(|x| {
            let start = x[0].first_row_index as usize;
            let end = (x[1].first_row_index - 1) as usize;
            Range::new(start, end)
        })
        .collect();

    let last = Range::new(
        locations.last().unwrap().first_row_index as usize,
        total_rows - 1,
    );
    vec_range.push_back(last);

    Ok(RowRanges { ranges: vec_range })
}

#[cfg(test)]
mod tests {
    use crate::basic::Type::INT32;
    use crate::file::page_index::index::{NativeIndex, PageIndex};
    use crate::file::page_index::range::{compute_row_ranges, Range, RowRanges};
    use parquet_format::{BoundaryOrder, PageLocation};

    #[test]
    fn test_binary_search_overlap() {
        let mut ranges = RowRanges::new_empty();
        ranges.add(Range::new(1, 3));
        ranges.add(Range::new(6, 7));

        assert!(ranges.is_overlapping(&Range::new(1, 2)));
        // include both [start, end]
        assert!(ranges.is_overlapping(&Range::new(0, 1)));
        assert!(ranges.is_overlapping(&Range::new(0, 3)));

        assert!(ranges.is_overlapping(&Range::new(0, 7)));
        assert!(ranges.is_overlapping(&Range::new(2, 7)));

        assert!(!ranges.is_overlapping(&Range::new(4, 5)));
    }

    #[test]
    fn test_add_func_ascending_disjunctive() {
        let mut ranges_1 = RowRanges::new_empty();
        ranges_1.add(Range::new(1, 3));
        ranges_1.add(Range::new(5, 6));
        ranges_1.add(Range::new(8, 9));
        assert_eq!(ranges_1.count(), 3);
    }

    #[test]
    fn test_add_func_ascending_merge() {
        let mut ranges_1 = RowRanges::new_empty();
        ranges_1.add(Range::new(1, 3));
        ranges_1.add(Range::new(4, 5));
        ranges_1.add(Range::new(6, 7));
        assert_eq!(ranges_1.count(), 1);
    }

    #[test]
    #[should_panic(expected = "Must add range in ascending!")]
    fn test_add_func_not_ascending() {
        let mut ranges_1 = RowRanges::new_empty();
        ranges_1.add(Range::new(6, 7));
        ranges_1.add(Range::new(1, 3));
        ranges_1.add(Range::new(4, 5));
        assert_eq!(ranges_1.count(), 1);
    }

    #[test]
    fn test_union_func() {
        let mut ranges_1 = RowRanges::new_empty();
        ranges_1.add(Range::new(1, 2));
        ranges_1.add(Range::new(3, 4));
        ranges_1.add(Range::new(5, 6));

        let mut ranges_2 = RowRanges::new_empty();
        ranges_2.add(Range::new(2, 3));
        ranges_2.add(Range::new(4, 5));
        ranges_2.add(Range::new(6, 7));

        let ranges = RowRanges::union(ranges_1, ranges_2);
        assert_eq!(ranges.count(), 1);
        let range = ranges.ranges.get(0).unwrap();
        assert_eq!(*range.start(), 1);
        assert_eq!(*range.end(), 7);

        let mut ranges_a = RowRanges::new_empty();
        ranges_a.add(Range::new(1, 3));
        ranges_a.add(Range::new(5, 8));
        ranges_a.add(Range::new(11, 12));

        let mut ranges_b = RowRanges::new_empty();
        ranges_b.add(Range::new(0, 2));
        ranges_b.add(Range::new(6, 7));
        ranges_b.add(Range::new(10, 11));

        let ranges = RowRanges::union(ranges_a, ranges_b);
        assert_eq!(ranges.count(), 3);

        let range_1 = ranges.ranges.get(0).unwrap();
        assert_eq!(*range_1.start(), 0);
        assert_eq!(*range_1.end(), 3);
        let range_2 = ranges.ranges.get(1).unwrap();
        assert_eq!(*range_2.start(), 5);
        assert_eq!(*range_2.end(), 8);
        let range_3 = ranges.ranges.get(2).unwrap();
        assert_eq!(*range_3.start(), 10);
        assert_eq!(*range_3.end(), 12);
    }

    #[test]
    fn test_intersection_func() {
        let mut ranges_1 = RowRanges::new_empty();
        ranges_1.add(Range::new(1, 2));
        ranges_1.add(Range::new(3, 4));
        ranges_1.add(Range::new(5, 6));

        let mut ranges_2 = RowRanges::new_empty();
        ranges_2.add(Range::new(2, 3));
        ranges_2.add(Range::new(4, 5));
        ranges_2.add(Range::new(6, 7));

        let ranges = RowRanges::intersection(ranges_1, ranges_2);
        assert_eq!(ranges.count(), 1);
        let range = ranges.ranges.get(0).unwrap();
        assert_eq!(*range.start(), 2);
        assert_eq!(*range.end(), 6);

        let mut ranges_a = RowRanges::new_empty();
        ranges_a.add(Range::new(1, 3));
        ranges_a.add(Range::new(5, 8));
        ranges_a.add(Range::new(11, 12));

        let mut ranges_b = RowRanges::new_empty();
        ranges_b.add(Range::new(0, 2));
        ranges_b.add(Range::new(6, 7));
        ranges_b.add(Range::new(10, 11));

        let ranges = RowRanges::intersection(ranges_a, ranges_b);
        assert_eq!(ranges.count(), 3);

        let range_1 = ranges.ranges.get(0).unwrap();
        assert_eq!(*range_1.start(), 1);
        assert_eq!(*range_1.end(), 2);
        let range_2 = ranges.ranges.get(1).unwrap();
        assert_eq!(*range_2.start(), 6);
        assert_eq!(*range_2.end(), 7);
        let range_3 = ranges.ranges.get(2).unwrap();
        assert_eq!(*range_3.start(), 11);
        assert_eq!(*range_3.end(), 11);
    }

    #[test]
    fn test_compute_one() {
        let locations = &[PageLocation {
            offset: 50,
            compressed_page_size: 10,
            first_row_index: 0,
        }];
        let total_rows = 10;

        let row_ranges = compute_row_ranges(&[true], locations, total_rows).unwrap();
        assert_eq!(row_ranges.count(), 1);
        assert_eq!(row_ranges.ranges.get(0).unwrap(), &Range::new(0, 9));
    }

    #[test]
    fn test_compute_multi() {
        let index: NativeIndex<i32> = NativeIndex {
            physical_type: INT32,
            indexes: vec![
                PageIndex {
                    min: Some(0),
                    max: Some(10),
                    null_count: Some(0),
                },
                PageIndex {
                    min: Some(15),
                    max: Some(20),
                    null_count: Some(0),
                },
            ],
            boundary_order: BoundaryOrder::Ascending,
        };
        let locations = &[
            PageLocation {
                offset: 100,
                compressed_page_size: 10,
                first_row_index: 0,
            },
            PageLocation {
                offset: 200,
                compressed_page_size: 20,
                first_row_index: 11,
            },
        ];
        let total_rows = 20;

        //filter `x < 11`
        let filter =
            |page: &PageIndex<i32>| page.max.as_ref().map(|&x| x < 11).unwrap_or(false);

        let mask = index.indexes.iter().map(filter).collect::<Vec<_>>();

        let row_ranges = compute_row_ranges(&mask, locations, total_rows).unwrap();

        assert_eq!(row_ranges.count(), 1);
        assert_eq!(row_ranges.ranges.get(0).unwrap(), &Range::new(0, 10));
    }
}
