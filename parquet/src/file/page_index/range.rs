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

use std::cmp::Ordering;
use std::collections::VecDeque;

/// A row range
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Range {
    /// Its start
    pub from: usize,
    /// Its end
    pub to: usize,
}

impl Range {
    // Creates a range of [from, to] (from and to are both inclusive)
    pub fn new(from: usize, to: usize) -> Self {
        assert!(from <= to);
        Self { from, to }
    }

    pub fn count(&self) -> usize {
        self.to - self.from + 1
    }

    pub fn is_before(&self, other: &Range) -> bool {
        self.to < other.from
    }

    pub fn is_after(&self, other: &Range) -> bool {
        self.from > other.to
    }

    /// Return the union of the two ranges,
    /// Return `None` if there are hole between them.
    pub fn union(left: &Range, right: &Range) -> Option<Range> {
        if left.from <= right.from {
            if left.to + 1 >= right.from {
                return Some(Range {
                    from: left.from,
                    to: std::cmp::max(left.to, right.to),
                });
            }
        } else if right.to + 1 >= left.from {
            return Some(Range {
                from: right.from,
                to: std::cmp::max(left.to, right.to),
            });
        }
        None
    }

    /// Returns the intersection of the two ranges,
    /// return null if they are not overlapped.
    pub fn intersection(left: &Range, right: &Range) -> Option<Range> {
        if left.from <= right.from {
            if left.to >= right.from {
                return Some(Range {
                    from: right.from,
                    to: std::cmp::min(left.to, right.to),
                });
            }
        } else if right.to >= left.from {
            return Some(Range {
                from: left.from,
                to: std::cmp::min(left.to, right.to),
            }
            );
        }
        None
    }
}

///Struct representing row ranges in a row-group. These row ranges are calculated as a result of using
///the column index on the filtering.
#[derive(Debug, Clone)]
pub struct RowRanges {
    pub ranges: VecDeque<Range>,
}

impl RowRanges {
    //create an empty RowRanges
    pub fn new() -> Self {
        RowRanges {
            ranges: VecDeque::new(),
        }
    }

    pub fn count(&self) -> usize {
        self.ranges.len()
    }

    //Adds a range to the end of the list of ranges. It maintains the disjunctive ascending order of the ranges by
    //trying to union the specified range to the last ranges in the list. The specified range shall be larger than
    //the last one or might be overlapped with some of the last ones.
    // [a, b] < [c, d] if b < c
    pub fn add(&mut self, range: Range) {
        let mut to_add = range;
        let f: i32 = (self.count() as i32) - 1;
        if f >= 0 {
            for i in f as usize..0 {
                let last = self.ranges.get(i).unwrap();
                assert!(!last.is_after(&range));
                // try to merge range
                match Range::union(last, &to_add) {
                    None => {
                        break;
                    }
                    Some(r) => {
                        to_add = r;
                        self.ranges.remove(i);
                    }
                }
            }
        }
        self.ranges.push_back(to_add);
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
        let mut result = RowRanges::new();
        if v2.is_empty() {
            left.clone()
        } else {
            let mut range2 = v2.pop_front().unwrap();
            while !v1.is_empty() {
                let range1 = v1.pop_front().unwrap();
                if range1.is_after(&range2) {
                    result.add(range2);
                    range2 = range1;
                    std::mem::swap(v1,v2);
                } else {
                    result.add(range1);
                }
            }
            if !v2.is_empty() { result.ranges.append( v2) }

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
        let mut result = RowRanges::new();
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

    pub fn row_count(&self) -> usize {
        self.ranges.iter().map(|x| x.count()).sum()
    }

    pub fn is_overlapping(&self, x: &Range) -> bool {
        self.ranges.binary_search_by(|y| -> Ordering {
            if y.is_before(x) {
                Ordering::Less
            } else if y.is_after(x) {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        }).is_ok()
    }
}


#[cfg(test)]
mod tests {
    use crate::file::page_index::range::{Range, RowRanges};

    #[test]
    fn test_binary_search_overlap() {
        let mut ranges = RowRanges::new();
        ranges.add(Range { from: 1, to: 3 });
        ranges.add(Range { from: 6, to: 7 });

        assert!(ranges.is_overlapping(&Range { from: 1, to: 2 }));
        // include both [start, end]
        assert!(ranges.is_overlapping(&Range { from: 0, to: 1 }));
        assert!(ranges.is_overlapping(&Range { from: 0, to: 3 }));

        assert!(ranges.is_overlapping(&Range { from: 0, to: 7 }));
        assert!(ranges.is_overlapping(&Range { from: 2, to: 7 }));

        assert!(!ranges.is_overlapping(&Range { from: 4, to: 5 }));
    }

}