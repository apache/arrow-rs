// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! MSD radix sort on row-encoded keys.
//!
//! The Arrow row format produces big-endian, memcmp-comparable byte sequences,
//! so they can be radix sorted directly with no extra encoding. MSD (Most
//! Significant Digit) radix sort runs in O(n * key_width) instead of the
//! O(n log n * comparison_cost) of a comparison sort.
//!
//! # When to use this
//!
//! Radix sort wins when sorting by two or more columns, and the margin grows
//! with N. Comparison sort walks every column again on each comparison. Radix
//! sort pays a fixed cost per byte position no matter how many columns there
//! are or what types they are (primitives, strings, dicts, lists).
//!
//! # When NOT to use this
//!
//! Prefer [`lexsort_to_indices`] when:
//! - **Sorting by a single column.** The row encoding overhead (allocation,
//!   encoding, indirection through `Rows`) outweighs the radix advantage.
//!   Comparison sort on the columnar array skips encoding entirely.
//! - **All sort columns are low-cardinality dictionaries** with no
//!   high-cardinality column to break ties. Dictionary values encode to long
//!   shared prefixes, so the early radix passes discriminate few rows before
//!   falling back to comparison sort.
//! - **Columns with low-entropy leading bytes**, such as `Decimal128` and
//!   `Decimal256`. These encode as 16- or 32-byte big-endian integers, but
//!   real values cover only a fraction of that range, so the leading bytes
//!   barely differ across rows. Radix uses up its max depth without
//!   discriminating rows and then falls back to comparison sort.
//! - **A leading column discriminates most rows and a trailing column is
//!   expensive to encode**, such as a list. [`lexsort_to_indices`] skips
//!   encoding the trailing column for rows already resolved by the leading
//!   column.
//!
//! [`lexsort_to_indices`]: https://docs.rs/arrow-ord/latest/arrow_ord/sort/fn.lexsort_to_indices.html

use crate::Rows;
use arrow_array::UInt32Array;
use arrow_schema::ArrowError;

/// Buckets this size or smaller are sorted by comparing keys. On small inputs
/// a comparison sort beats the per-bucket cost of another radix pass.
const FALLBACK_THRESHOLD: usize = 32;

/// Sort row indices using MSD radix sort on row-encoded keys.
///
/// Takes [`Rows`] produced by [`RowConverter::convert_columns`] and returns
/// the row indices in sorted order. The caller is responsible for encoding
/// columns into row format and for using the returned indices to reorder the
/// original arrays (e.g., via [`take`]).
///
/// See the [module-level documentation](self) for guidance on when radix sort
/// is faster than [`lexsort_to_indices`].
///
/// # Errors
///
/// Returns an error if `rows` has more rows than a `u32` index can address.
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
/// let indices = radix_sort_to_indices(&rows).unwrap();
/// assert_eq!(indices.values(), &[2, 4, 1, 3, 0]); // points to [1, 2, 3, 4, 5]
/// ```
///
/// [`RowConverter::convert_columns`]: crate::RowConverter::convert_columns
/// [`take`]: https://docs.rs/arrow/latest/arrow/compute/fn.take.html
/// [`lexsort_to_indices`]: https://docs.rs/arrow-ord/latest/arrow_ord/sort/fn.lexsort_to_indices.html
pub fn radix_sort_to_indices(rows: &Rows) -> Result<UInt32Array, ArrowError> {
    let n = rows.num_rows();
    u32::try_from(n)
        .map_err(|_| ArrowError::ComputeError(format!("cannot sort {n} rows with u32 indices")))?;
    let Some(first) = rows.iter().next() else {
        return Ok(UInt32Array::from(Vec::<u32>::new()));
    };

    // Bytes that every row shares can't order rows, so the key starts after them.
    let first = first.data();
    let (prefix, max_len) =
        rows.iter()
            .skip(1)
            .fold((first.len(), first.len()), |(prefix, max_len), row| {
                let row = row.data();
                let prefix = first[..prefix]
                    .iter()
                    .zip(row)
                    .take_while(|(a, b)| a == b)
                    .count();
                (prefix, max_len.max(row.len()))
            });

    // Each entry holds the next 8 bytes of its row as a big-endian key above
    // the row's index, so entries compare by key and move as one value.
    //
    // A row shorter than the key takes its last key bytes from the next row,
    // because a fixed-size load is one instruction and a variable-length copy
    // calls memcpy. Those bytes can only reorder equal rows: no row is a proper
    // prefix of another, because every field encoding is self-delimiting, so
    // two different rows differ within the shorter one.
    let buffer = rows.buffer.as_slice();
    let mut entries: Vec<u128> = rows
        .offsets
        .windows(2)
        .enumerate()
        .map(|(i, w)| {
            let start = w[0] + prefix;
            let key = match buffer.get(start..).and_then(<[u8]>::first_chunk::<8>) {
                Some(key) => *key,
                // The last rows of the buffer are padded with zeros instead
                None => {
                    let tail = &buffer[start..];
                    let mut key = [0u8; 8];
                    key[..tail.len()].copy_from_slice(tail);
                    key
                }
            };
            (u128::from(u64::from_be_bytes(key)) << 32) | i as u128
        })
        .collect();
    let mut temp = vec![0u128; n];
    // The first key byte differs between rows unless every row is equal,
    // because the key starts after the prefix that every row shares.
    msd_radix_sort(&mut entries, &mut temp, 0, true);

    // Entries with equal keys come from rows that agree on every byte before
    // prefix + 8, so the rest of each row decides their order. When no row is
    // longer than that, equal keys mean equal rows and there is nothing left
    // to compare.
    let suffix = prefix + 8;
    if max_len > suffix {
        for run in entries.chunk_by_mut(|a, b| a >> 32 == b >> 32) {
            if run.len() > 1 {
                run.sort_unstable_by(|&a, &b| {
                    let a = rows.row(a as u32 as usize).data();
                    let b = rows.row(b as u32 as usize).data();
                    a.get(suffix..)
                        .unwrap_or_default()
                        .cmp(b.get(suffix..).unwrap_or_default())
                });
            }
        }
    }
    Ok(UInt32Array::from_iter_values(
        entries.iter().map(|&e| e as u32),
    ))
}

/// MSD radix sort of entries by their key, using ping-pong buffers.
///
/// Each level scatters from `src` into `dst`, then recurses with the
/// roles swapped (dst becomes the next level's src). This avoids an
/// O(n) `copy_from_slice` at every recursion level.
///
/// `result_in_src` tracks where the caller expects the sorted output:
/// true means `src`, false means `dst`. It flips at each scatter so
/// the final result lands in the right buffer.
///
/// `byte` is the key byte to split on, counted from the most significant.
/// Every entry in `src` agrees on the bytes before it. When every entry also
/// shares `byte`, the sort skips ahead to the first byte that differs, so the
/// recursion ends after at most 8 levels.
fn msd_radix_sort(src: &mut [u128], dst: &mut [u128], mut byte: u32, result_in_src: bool) {
    if src.len() <= FALLBACK_THRESHOLD {
        let out = if result_in_src {
            src
        } else {
            dst.copy_from_slice(src);
            dst
        };
        out.sort_unstable();
        return;
    }

    // src.len() <= u32::MAX, checked by radix_sort_to_indices
    let mut counts;
    let shift = loop {
        if byte == 8 {
            // Every key is equal, and the caller orders equal keys.
            if !result_in_src {
                dst.copy_from_slice(src);
            }
            return;
        }
        // Moves key byte `byte` to the low byte of an entry
        let shift = 32 + 56 - 8 * byte;
        counts = [0; 256];
        for &e in src.iter() {
            counts[(e >> shift) as u8 as usize] += 1;
        }
        if counts[(src[0] >> shift) as u8 as usize] as usize != src.len() {
            break shift;
        }
        let first = src[0] >> 32;
        let diff = src.iter().fold(0, |acc, &e| acc | ((e >> 32) ^ first)) as u64;
        byte = diff.leading_zeros() / 8;
    };
    let mut offsets = [0u32; 257];
    for i in 0..256 {
        offsets[i + 1] = offsets[i] + counts[i];
    }

    let mut write_pos = offsets;
    for &e in src.iter() {
        let b = (e >> shift) as u8 as usize;
        dst[write_pos[b] as usize] = e;
        write_pos[b] += 1;
    }

    // Recurse with roles swapped. After the scatter the data lives in dst, so
    // dst becomes the next level's src. Flipping result_in_src keeps each
    // level's output in the buffer its caller expects.
    for bucket in 0..256 {
        let (start, end) = (offsets[bucket] as usize, offsets[bucket + 1] as usize);
        if end - start > 1 {
            msd_radix_sort(
                &mut dst[start..end],
                &mut src[start..end],
                byte + 1,
                !result_in_src,
            );
        } else if end - start == 1 && result_in_src {
            // A singleton bucket doesn't recurse. Its entry is in dst, so copy
            // it back when the caller expects the result in src.
            src[start] = dst[start];
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::generate_column;
    use crate::{RowConverter, SortField};
    use arrow_array::{ArrayRef, BooleanArray, Float64Array, Int32Array, StringArray};
    use arrow_schema::{DataType, SortOptions};
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};
    use std::sync::Arc;

    fn sort(rows: &Rows) -> Vec<u32> {
        radix_sort_to_indices(rows).unwrap().values().to_vec()
    }

    fn assert_sorted(rows: &Rows, indices: &[u32]) {
        for i in 1..indices.len() {
            let a = rows.row(indices[i - 1] as usize);
            let b = rows.row(indices[i] as usize);
            assert!(
                a <= b,
                "row {} should be <= row {}",
                indices[i - 1],
                indices[i]
            );
        }
    }

    #[test]
    fn test_radix_sort_integers() {
        let array: ArrayRef = Arc::new(Int32Array::from(vec![5, 3, 1, 4, 2]));
        let converter = RowConverter::new(vec![SortField::new(DataType::Int32)]).unwrap();
        let rows = converter.convert_columns(&[array]).unwrap();

        let indices = sort(&rows);
        assert_sorted(&rows, &indices);
        assert_eq!(indices, vec![2, 4, 1, 3, 0]);
    }

    #[test]
    fn test_radix_sort_strings() {
        let array: ArrayRef =
            Arc::new(StringArray::from(vec!["banana", "apple", "cherry", "date"]));
        let converter = RowConverter::new(vec![SortField::new(DataType::Utf8)]).unwrap();
        let rows = converter.convert_columns(&[array]).unwrap();

        let indices = sort(&rows);
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

        let indices = sort(&rows);
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

        let indices = sort(&rows);
        assert_sorted(&rows, &indices);
        assert_eq!(indices, vec![1, 0, 3, 2]);
    }

    #[test]
    fn test_radix_sort_empty() {
        let array: ArrayRef = Arc::new(Int32Array::from(Vec::<i32>::new()));
        let converter = RowConverter::new(vec![SortField::new(DataType::Int32)]).unwrap();
        let rows = converter.convert_columns(&[array]).unwrap();

        let indices = sort(&rows);
        assert!(indices.is_empty());
    }

    #[test]
    fn test_radix_sort_single_element() {
        let array: ArrayRef = Arc::new(Int32Array::from(vec![42]));
        let converter = RowConverter::new(vec![SortField::new(DataType::Int32)]).unwrap();
        let rows = converter.convert_columns(&[array]).unwrap();

        let indices = sort(&rows);
        assert_eq!(indices, vec![0]);
    }

    #[test]
    fn test_radix_sort_all_equal() {
        let array: ArrayRef = Arc::new(Int32Array::from(vec![7, 7, 7, 7, 7]));
        let converter = RowConverter::new(vec![SortField::new(DataType::Int32)]).unwrap();
        let rows = converter.convert_columns(&[array]).unwrap();

        let indices = sort(&rows);
        assert_sorted(&rows, &indices);
    }

    #[test]
    fn test_radix_sort_descending() {
        let array: ArrayRef = Arc::new(Int32Array::from(vec![1, 3, 2, 5, 4]));
        let options = SortOptions::default().desc();
        let converter =
            RowConverter::new(vec![SortField::new_with_options(DataType::Int32, options)]).unwrap();
        let rows = converter.convert_columns(&[array]).unwrap();

        let indices = sort(&rows);
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
            RowConverter::new(vec![SortField::new_with_options(DataType::Int32, options)]).unwrap();
        let rows = converter.convert_columns(&[array]).unwrap();

        let indices = sort(&rows);
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
            RowConverter::new(vec![SortField::new_with_options(DataType::Int32, options)]).unwrap();
        let rows = converter.convert_columns(&[array]).unwrap();

        let indices = sort(&rows);
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
                let converter =
                    RowConverter::new(vec![SortField::new_with_options(DataType::Int32, options)])
                        .unwrap();
                let rows = converter
                    .convert_columns(std::slice::from_ref(&array))
                    .unwrap();

                let indices = sort(&rows);
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

        let indices = sort(&rows);
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

        let indices = sort(&rows);
        assert_sorted(&rows, &indices);
    }

    // Tests sizes around FALLBACK_THRESHOLD to exercise both the radix and
    // fallback paths.
    #[test]
    fn test_radix_sort_threshold_boundary() {
        let mut rng = StdRng::seed_from_u64(0xCAFE);
        for n in [1, 2, 31, 32, 33, 64, 100, 128, 256, 500, 1000] {
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
            let converter = RowConverter::new(vec![SortField::new(DataType::Int32)]).unwrap();
            let rows = converter.convert_columns(&[array]).unwrap();

            let indices = sort(&rows);
            assert_sorted(&rows, &indices);
            assert_eq!(indices.len(), n, "wrong number of indices for n={n}");
        }
    }

    /// Generate random arrays and verify radix sort produces correctly sorted output
    /// across random column types, sort options, and sizes. Lengths reach past
    /// FALLBACK_THRESHOLD so most iterations take the radix path, and the
    /// variable-length types (strings, lists, dictionaries, maps) exercise
    /// rows of different lengths sharing a bucket.
    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
    fn test_radix_sort_fuzz() {
        let mut rng = StdRng::seed_from_u64(0xF00D);

        for iteration in 0..200 {
            let num_columns = rng.random_range(1..=4);
            let len = rng.random_range(2..2000);

            let mut arrays: Vec<ArrayRef> = Vec::new();
            let mut fields: Vec<SortField> = Vec::new();

            for _ in 0..num_columns {
                let options = SortOptions {
                    descending: rng.random_bool(0.5),
                    nulls_first: rng.random_bool(0.5),
                };

                let array: ArrayRef = match rng.random_range(0..6) {
                    // Low-cardinality i32 to create many ties
                    0 => Arc::new(Int32Array::from_iter(
                        (0..len).map(|_| rng.random_bool(0.8).then(|| rng.random_range(0..5))),
                    )),
                    1 => Arc::new(Int32Array::new_null(len)),
                    _ => generate_column(&mut rng, len),
                };

                fields.push(SortField::new_with_options(
                    array.data_type().clone(),
                    options,
                ));
                arrays.push(array);
            }

            let converter = RowConverter::new(fields).unwrap();
            let rows = converter.convert_columns(&arrays).unwrap();

            let indices = sort(&rows);
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
            let converter =
                RowConverter::new(vec![SortField::new_with_options(DataType::Int32, options)])
                    .unwrap();
            let rows = converter.convert_columns(&[array]).unwrap();

            let radix = sort(&rows);

            let mut comparison: Vec<u32> = (0..len as u32).collect();
            comparison.sort_unstable_by(|&a, &b| rows.row(a as usize).cmp(&rows.row(b as usize)));

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

        let indices = sort(&rows);
        assert_sorted(&rows, &indices);
    }

    /// Equal rows above FALLBACK_THRESHOLD have equal keys and are ordered by
    /// comparing the rest of each row.
    #[test]
    fn test_radix_sort_all_equal_past_threshold() {
        let array: ArrayRef = Arc::new(StringArray::from(vec!["same value"; 100]));
        let converter = RowConverter::new(vec![SortField::new(DataType::Utf8)]).unwrap();
        let rows = converter.convert_columns(&[array]).unwrap();

        let mut indices = sort(&rows);
        assert_sorted(&rows, &indices);
        indices.sort_unstable();
        assert_eq!(indices, (0..100).collect::<Vec<u32>>());
    }

    /// Rows that share a prefix are keyed by the bytes after it.
    #[test]
    fn test_radix_sort_shared_prefix_past_max_depth() {
        let values: Vec<String> = (0..100)
            .map(|i| format!("shared-prefix-longer-than-max-depth-{}", (i * 37) % 100))
            .collect();
        let array: ArrayRef = Arc::new(StringArray::from(values.clone()));
        let converter = RowConverter::new(vec![SortField::new(DataType::Utf8)]).unwrap();
        let rows = converter.convert_columns(&[array]).unwrap();

        let indices = sort(&rows);
        assert_sorted(&rows, &indices);
        let sorted: Vec<&str> = indices
            .iter()
            .map(|&i| values[i as usize].as_str())
            .collect();
        let mut expected: Vec<&str> = values.iter().map(String::as_str).collect();
        expected.sort_unstable();
        assert_eq!(sorted, expected);
    }

    /// Small decimals share their first 13 encoded bytes, so the key must start
    /// after that prefix, and the trailing column orders rows with equal keys.
    #[test]
    fn test_radix_sort_shared_prefix_longer_than_key() {
        let mut rng = StdRng::seed_from_u64(0xDEC);
        let decimals: Vec<Option<i128>> = (0..500)
            .map(|_| rng.random_bool(0.9).then(|| rng.random_range(0..20)))
            .collect();
        let ints: Vec<i32> = (0..500).map(|_| rng.random_range(-5..5)).collect();
        let a1: ArrayRef = Arc::new(
            arrow_array::Decimal128Array::from(decimals)
                .with_precision_and_scale(18, 2)
                .unwrap(),
        );
        let a2: ArrayRef = Arc::new(Int32Array::from(ints));
        let converter = RowConverter::new(vec![
            SortField::new(DataType::Decimal128(18, 2)),
            SortField::new(DataType::Int32),
        ])
        .unwrap();
        let rows = converter.convert_columns(&[a1, a2]).unwrap();

        let indices = sort(&rows);
        assert_sorted(&rows, &indices);
    }

    /// Two-byte rows end inside the 8-byte key, so keys are zero-padded.
    #[test]
    fn test_radix_sort_rows_shorter_than_key() {
        let mut rng = StdRng::seed_from_u64(0x8);
        let values: Vec<Option<i8>> = (0..1000)
            .map(|_| rng.random_bool(0.8).then(|| rng.random()))
            .collect();
        let array: ArrayRef = Arc::new(arrow_array::Int8Array::from(values));
        let converter = RowConverter::new(vec![SortField::new(DataType::Int8)]).unwrap();
        let rows = converter.convert_columns(&[array]).unwrap();

        let indices = sort(&rows);
        assert_sorted(&rows, &indices);
    }

    /// Null rows of a leading column share their first five bytes, and the
    /// string column after them decides their order.
    #[test]
    fn test_radix_sort_null_bucket_then_strings() {
        let mut rng = StdRng::seed_from_u64(0x0);
        let ints: Vec<Option<i32>> = (0..1000)
            .map(|_| rng.random_bool(0.5).then(|| rng.random_range(0..4)))
            .collect();
        let strings: Vec<String> = (0..1000)
            .map(|_| format!("{:x}", rng.random::<u64>()))
            .collect();
        let a1: ArrayRef = Arc::new(Int32Array::from(ints));
        let a2: ArrayRef = Arc::new(StringArray::from(strings));
        let converter = RowConverter::new(vec![
            SortField::new(DataType::Int32),
            SortField::new(DataType::Utf8),
        ])
        .unwrap();
        let rows = converter.convert_columns(&[a1, a2]).unwrap();

        let indices = sort(&rows);
        assert_sorted(&rows, &indices);
    }
}
