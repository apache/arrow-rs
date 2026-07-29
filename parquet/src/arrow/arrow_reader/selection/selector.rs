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

/// Splits `selectors` at the first `row_count` rows, returning `(head, tail)`.
pub(super) fn split_off_selectors(
    mut selectors: Vec<RowSelector>,
    row_count: usize,
) -> (Vec<RowSelector>, Vec<RowSelector>) {
    let mut total_count = 0;

    // Find the index where the selector exceeds the row count
    let find = selectors.iter().position(|selector| {
        total_count += selector.row_count;
        total_count > row_count
    });

    let split_idx = match find {
        Some(idx) => idx,
        None => return (selectors, Vec::new()),
    };

    // `selectors` keeps the head, `tail` takes the rest. The selector straddling
    // the boundary is split between the two.
    let mut tail = selectors.split_off(split_idx);

    // Always present as `split_idx < selectors.len`
    let next = tail.first_mut().unwrap();
    let overflow = total_count - row_count;

    if next.row_count != overflow {
        selectors.push(RowSelector {
            row_count: next.row_count - overflow,
            skip: next.skip,
        })
    }
    next.row_count = overflow;

    (selectors, tail)
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
