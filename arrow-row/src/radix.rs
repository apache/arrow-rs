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

//! MSD radix sort on row-encoded keys.
//!
//! The Arrow row format produces big-endian, memcmp-comparable byte sequences,
//! making it ideal for MSD (Most Significant Digit) radix sort without any
//! additional encoding. This gives O(n × key_width) performance instead of
//! O(n log n × comparison_cost).
//!
//! # When to use this
//!
//! Radix sort on row-encoded keys is the fastest sort strategy when:
//! - **Primitive columns** (integers, floats): ~2.4x faster than [`lexsort_to_indices`]
//!   at N=32768 despite the encoding overhead.
//! - **String columns**: 1.3–1.9x faster than the best alternative at all sizes.
//!   The advantage grows with more string columns and larger N.
//! - **Mixed dict + string columns**: ~1.3x faster than row-format comparison sort.
//! - **List columns with other columns**: Competitive or faster when lists aren't
//!   the primary sort key.
//!
//! # When NOT to use this
//!
//! - **Low-cardinality dictionary-only columns**: MSD radix sort performs poorly
//!   when many rows share long identical key prefixes, because it must recurse
//!   byte-by-byte through the shared prefix before reaching a distinguishing byte.
//!   For example, 2 low-cardinality dict columns is ~7x *slower* than
//!   [`lexsort_to_indices`]. Mixing in a string or primitive column eliminates
//!   this problem.
//! - **List-heavy sorts where a leading primitive column discriminates most rows**:
//!   [`lexsort_to_indices`] avoids encoding the expensive list column entirely for
//!   most rows, so it wins when the leading column is highly selective.
//!
//! As a rule of thumb: if your sort key is dominated by low-cardinality columns
//! with no high-cardinality column to break ties early, prefer [`lexsort_to_indices`].
//! Otherwise, radix sort is likely faster.
//!
//! [`lexsort_to_indices`]: arrow_ord::sort::lexsort_to_indices

use crate::Rows;

/// Bucket size at which we fall back to comparison sort.
const FALLBACK_THRESHOLD: usize = 64;

/// Maximum byte depth before falling back to comparison sort.
const MAX_DEPTH: usize = 128;

/// Sort row indices using MSD radix sort on row-encoded keys.
///
/// Takes [`Rows`] produced by [`RowConverter::convert_columns`] and returns
/// a `Vec<u32>` of row indices in sorted order. The caller is responsible for
/// encoding columns into row format and for using the returned indices to
/// reorder the original arrays (e.g., via [`take`]).
///
/// See the [module-level documentation](self) for guidance on when radix sort
/// is faster than [`lexsort_to_indices`].
///
/// # Example
///
/// ```
/// # use std::sync::Arc;
/// # use arrow_row::{RowConverter, SortField};
/// # use arrow_row::radix::radix_sort_to_indices;
/// # use arrow_array::{Int32Array, ArrayRef};
/// # use arrow_schema::DataType;
/// let array: ArrayRef = Arc::new(Int32Array::from(vec![5, 3, 1, 4, 2]));
/// let converter = RowConverter::new(vec![SortField::new(DataType::Int32)]).unwrap();
/// let rows = converter.convert_columns(&[array]).unwrap();
/// let indices = radix_sort_to_indices(&rows);
/// assert_eq!(indices, vec![2, 4, 1, 3, 0]); // points to [1, 2, 3, 4, 5]
/// ```
///
/// [`RowConverter::convert_columns`]: crate::RowConverter::convert_columns
/// [`take`]: https://docs.rs/arrow/latest/arrow/compute/fn.take.html
/// [`lexsort_to_indices`]: arrow_ord::sort::lexsort_to_indices
pub fn radix_sort_to_indices(rows: &Rows) -> Vec<u32> {
    let n = rows.num_rows();
    let mut indices: Vec<u32> = (0..n as u32).collect();
    msd_radix_sort(&mut indices, rows, 0);
    indices
}

fn msd_radix_sort(indices: &mut [u32], rows: &Rows, byte_pos: usize) {
    if indices.len() <= FALLBACK_THRESHOLD || byte_pos >= MAX_DEPTH {
        indices.sort_unstable_by(|&a, &b| {
            let ra = unsafe { rows.row_unchecked(a as usize) };
            let rb = unsafe { rows.row_unchecked(b as usize) };
            ra.cmp(&rb)
        });
        return;
    }

    // Histogram of byte values at byte_pos
    let mut counts = [0u32; 256];
    for &idx in indices.iter() {
        let row = unsafe { rows.row_unchecked(idx as usize) };
        let byte = row.data().get(byte_pos).copied().unwrap_or(0);
        counts[byte as usize] += 1;
    }

    // All same byte — skip to next position
    if counts.iter().filter(|&&c| c > 0).count() == 1 {
        msd_radix_sort(indices, rows, byte_pos + 1);
        return;
    }

    // Prefix sum for bucket offsets
    let mut offsets = [0u32; 257];
    for i in 0..256 {
        offsets[i + 1] = offsets[i] + counts[i];
    }

    // Out-of-place scatter into buckets
    let mut temp = vec![0u32; indices.len()];
    let mut write_pos = offsets;
    for &idx in indices.iter() {
        let row = unsafe { rows.row_unchecked(idx as usize) };
        let byte = row.data().get(byte_pos).copied().unwrap_or(0) as usize;
        temp[write_pos[byte] as usize] = idx;
        write_pos[byte] += 1;
    }
    indices.copy_from_slice(&temp);

    // Recurse into non-trivial buckets
    for bucket in 0..256 {
        let start = offsets[bucket] as usize;
        let end = offsets[bucket + 1] as usize;
        if end - start > 1 {
            msd_radix_sort(&mut indices[start..end], rows, byte_pos + 1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RowConverter, SortField};
    use arrow_array::{
        ArrayRef, BooleanArray, Float64Array, Int32Array, StringArray,
    };
    use arrow_schema::{DataType, SortOptions};
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use std::sync::Arc;

    fn assert_sorted(rows: &Rows, indices: &[u32]) {
        for i in 1..indices.len() {
            let a = unsafe { rows.row_unchecked(indices[i - 1] as usize) };
            let b = unsafe { rows.row_unchecked(indices[i] as usize) };
            assert!(a <= b, "row {} should be <= row {}", indices[i - 1], indices[i]);
        }
    }

    #[test]
    fn test_radix_sort_integers() {
        let array: ArrayRef = Arc::new(Int32Array::from(vec![5, 3, 1, 4, 2]));
        let converter = RowConverter::new(vec![SortField::new(DataType::Int32)]).unwrap();
        let rows = converter.convert_columns(&[array]).unwrap();

        let indices = radix_sort_to_indices(&rows);
        assert_sorted(&rows, &indices);
        assert_eq!(indices, vec![2, 4, 1, 3, 0]);
    }

    #[test]
    fn test_radix_sort_strings() {
        let array: ArrayRef = Arc::new(StringArray::from(vec!["banana", "apple", "cherry", "date"]));
        let converter = RowConverter::new(vec![SortField::new(DataType::Utf8)]).unwrap();
        let rows = converter.convert_columns(&[array]).unwrap();

        let indices = radix_sort_to_indices(&rows);
        assert_sorted(&rows, &indices);
        assert_eq!(indices, vec![1, 0, 2, 3]);
    }

    #[test]
    fn test_radix_sort_with_nulls() {
        let array: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(3),
            None,
            Some(1),
            None,
            Some(2),
        ]));
        let converter = RowConverter::new(vec![SortField::new(DataType::Int32)]).unwrap();
        let rows = converter.convert_columns(&[array]).unwrap();

        let indices = radix_sort_to_indices(&rows);
        assert_sorted(&rows, &indices);
    }

    #[test]
    fn test_radix_sort_multi_column() {
        let a1: ArrayRef = Arc::new(Int32Array::from(vec![1, 1, 2, 2]));
        let a2: ArrayRef = Arc::new(StringArray::from(vec!["b", "a", "d", "c"]));
        let converter = RowConverter::new(vec![
            SortField::new(DataType::Int32),
            SortField::new(DataType::Utf8),
        ])
        .unwrap();
        let rows = converter.convert_columns(&[a1, a2]).unwrap();

        let indices = radix_sort_to_indices(&rows);
        assert_sorted(&rows, &indices);
        assert_eq!(indices, vec![1, 0, 3, 2]);
    }

    #[test]
    fn test_radix_sort_empty() {
        let array: ArrayRef = Arc::new(Int32Array::from(Vec::<i32>::new()));
        let converter = RowConverter::new(vec![SortField::new(DataType::Int32)]).unwrap();
        let rows = converter.convert_columns(&[array]).unwrap();

        let indices = radix_sort_to_indices(&rows);
        assert!(indices.is_empty());
    }

    #[test]
    fn test_radix_sort_single_element() {
        let array: ArrayRef = Arc::new(Int32Array::from(vec![42]));
        let converter = RowConverter::new(vec![SortField::new(DataType::Int32)]).unwrap();
        let rows = converter.convert_columns(&[array]).unwrap();

        let indices = radix_sort_to_indices(&rows);
        assert_eq!(indices, vec![0]);
    }

    #[test]
    fn test_radix_sort_all_equal() {
        let array: ArrayRef = Arc::new(Int32Array::from(vec![7, 7, 7, 7, 7]));
        let converter = RowConverter::new(vec![SortField::new(DataType::Int32)]).unwrap();
        let rows = converter.convert_columns(&[array]).unwrap();

        let indices = radix_sort_to_indices(&rows);
        assert_sorted(&rows, &indices);
    }

    #[test]
    fn test_radix_sort_descending() {
        let array: ArrayRef = Arc::new(Int32Array::from(vec![1, 3, 2, 5, 4]));
        let options = SortOptions::default().desc();
        let converter =
            RowConverter::new(vec![SortField::new_with_options(DataType::Int32, options)])
                .unwrap();
        let rows = converter.convert_columns(&[array]).unwrap();

        let indices = radix_sort_to_indices(&rows);
        assert_sorted(&rows, &indices);
    }

    #[test]
    fn test_radix_sort_nulls_first() {
        let array: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(3),
            None,
            Some(1),
            None,
            Some(2),
        ]));
        let options = SortOptions::default().with_nulls_first(true);
        let converter =
            RowConverter::new(vec![SortField::new_with_options(DataType::Int32, options)])
                .unwrap();
        let rows = converter.convert_columns(&[array]).unwrap();

        let indices = radix_sort_to_indices(&rows);
        assert_sorted(&rows, &indices);
    }

    #[test]
    fn test_radix_sort_descending_nulls_first() {
        let array: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(3),
            None,
            Some(1),
            None,
            Some(2),
        ]));
        let options = SortOptions::default().desc().with_nulls_first(true);
        let converter =
            RowConverter::new(vec![SortField::new_with_options(DataType::Int32, options)])
                .unwrap();
        let rows = converter.convert_columns(&[array]).unwrap();

        let indices = radix_sort_to_indices(&rows);
        assert_sorted(&rows, &indices);
    }

    #[test]
    fn test_radix_sort_all_sort_option_combos() {
        let array: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(5),
            None,
            Some(3),
            None,
            Some(1),
            Some(4),
            Some(2),
        ]));

        for descending in [false, true] {
            for nulls_first in [false, true] {
                let options = SortOptions {
                    descending,
                    nulls_first,
                };
                let converter = RowConverter::new(vec![SortField::new_with_options(
                    DataType::Int32,
                    options,
                )])
                .unwrap();
                let rows = converter.convert_columns(&[array.clone()]).unwrap();

                let indices = radix_sort_to_indices(&rows);
                assert_sorted(&rows, &indices);
            }
        }
    }

    #[test]
    fn test_radix_sort_floats_with_nan() {
        let array: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(1.0),
            Some(f64::NAN),
            None,
            Some(-1.0),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            Some(0.0),
        ]));
        let converter = RowConverter::new(vec![SortField::new(DataType::Float64)]).unwrap();
        let rows = converter.convert_columns(&[array]).unwrap();

        let indices = radix_sort_to_indices(&rows);
        assert_sorted(&rows, &indices);
    }

    #[test]
    fn test_radix_sort_booleans() {
        let array: ArrayRef = Arc::new(BooleanArray::from(vec![
            Some(true),
            None,
            Some(false),
            Some(true),
            None,
            Some(false),
        ]));
        let converter = RowConverter::new(vec![SortField::new(DataType::Boolean)]).unwrap();
        let rows = converter.convert_columns(&[array]).unwrap();

        let indices = radix_sort_to_indices(&rows);
        assert_sorted(&rows, &indices);
    }

    // Tests sizes around the FALLBACK_THRESHOLD (64) to exercise both paths
    #[test]
    fn test_radix_sort_threshold_boundary() {
        let mut rng = StdRng::seed_from_u64(0xCAFE);
        for n in [1, 2, 32, 63, 64, 65, 100, 128, 256, 500, 1000] {
            let values: Vec<Option<i32>> = (0..n)
                .map(|_| {
                    if rng.random_bool(0.1) {
                        None
                    } else {
                        Some(rng.random_range(-1000..1000))
                    }
                })
                .collect();
            let array: ArrayRef = Arc::new(Int32Array::from(values));
            let converter =
                RowConverter::new(vec![SortField::new(DataType::Int32)]).unwrap();
            let rows = converter.convert_columns(&[array]).unwrap();

            let indices = radix_sort_to_indices(&rows);
            assert_sorted(&rows, &indices);
            assert_eq!(indices.len(), n, "wrong number of indices for n={n}");
        }
    }

    /// Generate random arrays and verify radix sort produces correctly sorted output
    /// across random column types, sort options, and sizes.
    #[test]
    fn test_radix_sort_fuzz() {
        let mut rng = StdRng::seed_from_u64(0xF00D);

        for iteration in 0..100 {
            let num_columns = rng.random_range(1..=4);
            let len = rng.random_range(5..500);

            let mut arrays: Vec<ArrayRef> = Vec::new();
            let mut fields: Vec<SortField> = Vec::new();

            for _ in 0..num_columns {
                let options = SortOptions {
                    descending: rng.random_bool(0.5),
                    nulls_first: rng.random_bool(0.5),
                };
                let null_rate = if rng.random_bool(0.3) { 0.0 } else { 0.2 };

                // Pick a random column type
                let (array, dt) = match rng.random_range(0..7) {
                    0 => {
                        let vals: Vec<Option<i32>> = (0..len)
                            .map(|_| {
                                if rng.random_bool(null_rate) {
                                    None
                                } else {
                                    Some(rng.random_range(-10000..10000))
                                }
                            })
                            .collect();
                        (
                            Arc::new(Int32Array::from(vals)) as ArrayRef,
                            DataType::Int32,
                        )
                    }
                    1 => {
                        let vals: Vec<Option<i64>> = (0..len)
                            .map(|_| {
                                if rng.random_bool(null_rate) {
                                    None
                                } else {
                                    Some(rng.random())
                                }
                            })
                            .collect();
                        (
                            Arc::new(arrow_array::Int64Array::from(vals)) as ArrayRef,
                            DataType::Int64,
                        )
                    }
                    2 => {
                        let vals: Vec<Option<f64>> = (0..len)
                            .map(|_| {
                                if rng.random_bool(null_rate) {
                                    None
                                } else {
                                    Some(rng.random::<f64>() * 1000.0 - 500.0)
                                }
                            })
                            .collect();
                        (
                            Arc::new(Float64Array::from(vals)) as ArrayRef,
                            DataType::Float64,
                        )
                    }
                    3 => {
                        let vals: Vec<Option<&str>> = (0..len)
                            .map(|_| {
                                if rng.random_bool(null_rate) {
                                    None
                                } else {
                                    // Fixed set of strings to get some collisions
                                    Some(
                                        ["alpha", "beta", "gamma", "delta", "epsilon",
                                         "zeta", "eta", "theta", "iota", "kappa",
                                         "a longer string for testing", ""]
                                            [rng.random_range(0..12)],
                                    )
                                }
                            })
                            .collect();
                        (
                            Arc::new(StringArray::from(vals)) as ArrayRef,
                            DataType::Utf8,
                        )
                    }
                    4 => {
                        let vals: Vec<Option<bool>> = (0..len)
                            .map(|_| {
                                if rng.random_bool(null_rate) {
                                    None
                                } else {
                                    Some(rng.random_bool(0.5))
                                }
                            })
                            .collect();
                        (
                            Arc::new(BooleanArray::from(vals)) as ArrayRef,
                            DataType::Boolean,
                        )
                    }
                    5 => {
                        // Low-cardinality i32 to create many ties
                        let vals: Vec<Option<i32>> = (0..len)
                            .map(|_| {
                                if rng.random_bool(null_rate) {
                                    None
                                } else {
                                    Some(rng.random_range(0..5))
                                }
                            })
                            .collect();
                        (
                            Arc::new(Int32Array::from(vals)) as ArrayRef,
                            DataType::Int32,
                        )
                    }
                    _ => {
                        // All-null column
                        let vals: Vec<Option<i32>> = vec![None; len];
                        (
                            Arc::new(Int32Array::from(vals)) as ArrayRef,
                            DataType::Int32,
                        )
                    }
                };

                arrays.push(array);
                fields.push(SortField::new_with_options(dt, options));
            }

            let converter = RowConverter::new(fields).unwrap();
            let rows = converter.convert_columns(&arrays).unwrap();

            let indices = radix_sort_to_indices(&rows);
            assert_sorted(&rows, &indices);
            assert_eq!(
                indices.len(),
                len,
                "iteration {iteration}: wrong index count"
            );

            // Verify every original index appears exactly once
            let mut seen = vec![false; len];
            for &idx in &indices {
                assert!(
                    !seen[idx as usize],
                    "iteration {iteration}: duplicate index {idx}"
                );
                seen[idx as usize] = true;
            }
        }
    }

    /// Verify radix sort matches comparison sort on row-encoded keys.
    /// Uses the same Rows, so any difference is a bug in the radix sort itself.
    #[test]
    fn test_radix_matches_comparison_sort() {
        let mut rng = StdRng::seed_from_u64(0xBEEF);

        for _ in 0..50 {
            let len = rng.random_range(100..1000);
            let vals: Vec<Option<i32>> = (0..len)
                .map(|_| {
                    if rng.random_bool(0.15) {
                        None
                    } else {
                        Some(rng.random_range(-500..500))
                    }
                })
                .collect();

            let options = SortOptions {
                descending: rng.random_bool(0.5),
                nulls_first: rng.random_bool(0.5),
            };

            let array: ArrayRef = Arc::new(Int32Array::from(vals));
            let converter = RowConverter::new(vec![SortField::new_with_options(
                DataType::Int32,
                options,
            )])
            .unwrap();
            let rows = converter.convert_columns(&[array]).unwrap();

            let radix = radix_sort_to_indices(&rows);

            let mut comparison: Vec<u32> = (0..len as u32).collect();
            comparison.sort_unstable_by(|&a, &b| {
                rows.row(a as usize).cmp(&rows.row(b as usize))
            });

            // Both sorts operate on the same rows, so equal-keyed elements
            // should appear in the same relative order only if both are stable
            // (they aren't), but the *row values* at each position must match.
            for i in 0..len {
                let radix_row = rows.row(radix[i] as usize);
                let cmp_row = rows.row(comparison[i] as usize);
                assert_eq!(
                    radix_row, cmp_row,
                    "mismatch at position {i}: radix idx={} vs comparison idx={}",
                    radix[i], comparison[i]
                );
            }
        }
    }

    /// Test with a multi-column schema that has mixed sort options per column.
    #[test]
    fn test_radix_sort_multi_column_mixed_options() {
        let a1: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            Some(1),
            Some(2),
            None,
        ]));
        let a2: ArrayRef = Arc::new(StringArray::from(vec![
            Some("z"),
            Some("a"),
            Some("a"),
            None,
            Some("m"),
        ]));

        // Col 1: ascending nulls_last, Col 2: descending nulls_first
        let converter = RowConverter::new(vec![
            SortField::new_with_options(
                DataType::Int32,
                SortOptions::default().asc().with_nulls_first(false),
            ),
            SortField::new_with_options(
                DataType::Utf8,
                SortOptions::default().desc().with_nulls_first(true),
            ),
        ])
        .unwrap();
        let rows = converter.convert_columns(&[a1, a2]).unwrap();

        let indices = radix_sort_to_indices(&rows);
        assert_sorted(&rows, &indices);
    }
}
