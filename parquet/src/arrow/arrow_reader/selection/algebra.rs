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

//! Set algebra backing [`RowSelection::and_then`], [`RowSelection::intersection`]
//! and [`RowSelection::union`]
//!
//! Each operation has two implementations, picked by the backing of its
//! operands: a merge of the [`RowSelector`] runs, and a bitwise variant over
//! [`BooleanBuffer`] masks.

use super::{MaskRunIter, RowSelection, RowSelectionInner, RowSelector};
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder};
use std::cmp::Ordering;
use std::iter::Peekable;

/// Applies `second` to the rows selected by `first`, both selector-backed.
pub(super) fn and_then_row_selections(
    first: &[RowSelector],
    second: &[RowSelector],
) -> RowSelection {
    let mut selectors = vec![];
    let mut first = first.iter().copied().peekable();
    let mut second = second.iter().copied().peekable();
    and_then_iter(&mut selectors, &mut first, &mut second);
    RowSelection::from_selectors(selectors)
}

/// Applies the mask `second` to the rows selected by the selector-backed `first`.
///
/// The mask is streamed as [`RowSelector`] runs, so it is never materialized.
pub(super) fn and_then_selectors_with_mask(
    first: &[RowSelector],
    second: &BooleanBuffer,
) -> RowSelection {
    let mut selectors = vec![];
    let mut first = first.iter().copied().peekable();
    let mut second = MaskRunIter::new(second).peekable();
    and_then_iter(&mut selectors, &mut first, &mut second);
    RowSelection::from_selectors(selectors)
}

fn and_then_iter<I, J>(
    selectors: &mut Vec<RowSelector>,
    first: &mut Peekable<I>,
    second: &mut Peekable<J>,
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
pub(super) fn intersect_row_selections(
    left: &[RowSelector],
    right: &[RowSelector],
) -> RowSelection {
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
pub(super) fn union_row_selections(left: &[RowSelector], right: &[RowSelector]) -> RowSelection {
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

/// Bitwise AND of two mask-backed selections. Longer side's tail passes through.
pub(super) fn intersect_masks(l: &BooleanBuffer, r: &BooleanBuffer) -> BooleanBuffer {
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
pub(super) fn union_masks(l: &BooleanBuffer, r: &BooleanBuffer) -> BooleanBuffer {
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
pub(super) fn and_then_mask(mask: &BooleanBuffer, other: &RowSelection) -> BooleanBuffer {
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::BooleanArray;
    use rand::{RngExt, rng};

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
    fn test_mask_and_then_none_selected_returns_all_unset() {
        let outer = RowSelection::from_boolean_buffer(BooleanBuffer::from(vec![
            false, true, true, false, true,
        ]));
        let inner =
            RowSelection::from_boolean_buffer(BooleanBuffer::from(vec![false, false, false]));

        let result = outer.and_then(&inner);
        let mask = result.as_mask().unwrap();
        assert_eq!(mask.len(), 5);
        assert_eq!(mask.count_set_bits(), 0);
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

        // Swapped operands: the right side is longer and its tail passes through.
        let a = RowSelection::from_boolean_buffer(BooleanBuffer::from(vec![true, false, true]));
        let b = RowSelection::from_boolean_buffer(BooleanBuffer::from(vec![
            true, true, true, false, true,
        ]));
        let r = a.intersection(&b);
        let r_mask = r.as_mask().unwrap();
        assert_eq!(r_mask.len(), 5);
        let bits: Vec<bool> = (0..5).map(|i| r_mask.value(i)).collect();
        assert_eq!(bits, vec![true, false, true, false, true]);
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
}
