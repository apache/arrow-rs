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

//! Conversion code for [`RowSelection`] representations

use crate::arrow::arrow_reader::RowSelector;
use arrow_array::BooleanArray;
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder};
use arrow_select::filter::SlicesIterator;
use std::cmp::Ordering;
use std::ops::Range;

pub(crate) fn row_selectors_to_boolean_buffer(selectors: &[RowSelector]) -> BooleanBuffer {
    let total_rows = selectors
        .iter()
        .filter(|s| !s.skip)
        .map(|s| s.row_count)
        .sum();
    let mut builder = BooleanBufferBuilder::new(total_rows);

    for selector in selectors.iter() {
        if selector.skip {
            builder.append_n(selector.row_count, false);
        } else {
            builder.append_n(selector.row_count, true);
        }
    }
    builder.finish()
}

/// Converts an iterator of [`BooleanArray`] / [`BooleanBuffer`] into a
/// Vec of [`RowSelection`].
///
/// Call [`prep_null_mask_filter`](arrow_select::filter::prep_null_mask_filter)
/// beforehand to ensure there are no nulls.
///
/// # Panic
///
/// Panics if any of the [`BooleanArray`] contain nulls.
pub(crate) fn boolean_array_to_row_selectors(filter: &BooleanArray) -> Vec<RowSelector> {
    let total_rows = filter.len();

    let iter = SlicesIterator::new(filter).map(move |(start, end)| start..end);

    consecutive_ranges_to_row_selectors(iter, total_rows)
}

/// Creates a [`RowSelection`] from an iterator of consecutive ranges to keep
pub(crate) fn consecutive_ranges_to_row_selectors<I: Iterator<Item = Range<usize>>>(
    ranges: I,
    total_rows: usize,
) -> Vec<RowSelector> {
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

#[cfg(test)]
mod tests {
    use rand::Rng;

    use super::*;

    fn generate_random_filter(total_rows: usize, selection_ratio: f64) -> BooleanArray {
        let mut rng = rand::rng();
        let bools: Vec<bool> = (0..total_rows)
            .map(|_| rng.random_bool(selection_ratio))
            .collect();
        BooleanArray::from(bools)
    }

    #[test]
    fn test_boolean_row_selection_round_trip() {
        let total_rows = 1_000;
        for &selection_ratio in &[0.0, 0.1, 0.5, 0.9, 1.0] {
            let filter = generate_random_filter(total_rows, selection_ratio);
            let row_selection = boolean_array_to_row_selectors(&filter);
            let filter_again = row_selectors_to_boolean_buffer(&row_selection);
            assert_eq!(filter, filter_again.into());
        }
    }
}
