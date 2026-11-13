// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! MSD radix sort on row-encoded keys.
//!
//! The Arrow row format produces big-endian, memcmp-comparable byte sequences,
//! so they can be radix sorted directly. The sort skips the bytes that every
//! row shares, radix sorts a 12-byte key taken from each row after them, and
//! sorts rows whose keys are equal again by the bytes after the key.
//!
//! # When to use this
//!
//! Radix sort is usually faster than [`lexsort_to_indices`] when sorting by
//! two or more columns, unless one of them is expensive to encode (see below).
//! [`lexsort_to_indices`] compares rows through a dynamically dispatched
//! comparator per column, calling them in turn on every comparison until one
//! column differs. Radix sort reads each row's 12-byte key once per pass. It
//! also does well when every row shares long leading bytes, such as small
//! `Decimal128` values, because it skips them. On a single non-null primitive
//! column the two are about even. Its cost includes encoding the columns into
//! [`Rows`], so a caller that already has [`Rows`] saves more.
//!
//! # When to prefer `lexsort_to_indices`
//!
//! - **A single nullable or string column, or one with few distinct values.**
//!   [`lexsort_to_indices`] sorts one column without encoding it, sets nulls
//!   aside before sorting, and sorts duplicates cheaply.
//! - **A key with an expensive column to encode, such as a list.** The row
//!   format encodes every column of every row, while [`lexsort_to_indices`]
//!   reads a later column only for rows that the earlier columns leave tied.
//! - **Large groups of rows that are equal in their first 12 bytes after the
//!   shared prefix.** Radix sort orders each such group by keying its rows
//!   again on the bytes after the key, which reads the rows in random order.
//!   Examples are strings with a few common prefixes and three or more
//!   low-cardinality columns.
//! - **A sort with a small limit.** [`lexsort_to_indices`] orders only the
//!   first `limit` rows. [`radix_sort_to_indices`] stops early too, but it
//!   still encodes every row and makes one pass over all of them, so
//!   [`lexsort_to_indices`] is faster for a `limit` below about 1% to 3% of the
//!   rows. Radix sort is faster above that.
//!
//! [`lexsort_to_indices`]: https://docs.rs/arrow-ord/latest/arrow_ord/sort/fn.lexsort_to_indices.html

use crate::Rows;
use arrow_array::UInt32Array;
use arrow_schema::ArrowError;

/// Buckets this size or smaller are sorted by comparing keys. On small inputs
/// a comparison sort beats the per-bucket cost of another radix pass.
const FALLBACK_THRESHOLD: usize = 32;

/// The bytes of each row that an entry holds as its key, in the 96 bits above
/// the row's u32 index
const KEY_BYTES: u32 = 12;

/// Sort row indices using MSD radix sort on row-encoded keys.
///
/// Takes [`Rows`] produced by [`RowConverter::convert_columns`] and returns
/// the row indices in sorted order. The caller is responsible for encoding
/// columns into row format and for using the returned indices to reorder the
/// original arrays (e.g., via [`take`]).
///
/// If `limit` is `Some`, the result holds only the first `limit` indices in
/// sorted order, or all of them if there are fewer rows. The sort stops
/// ordering rows once the first `limit` positions are final.
///
/// Rows that compare equal can appear in any order in the result.
///
/// Besides the result, the sort allocates two buffers of 16 bytes per row, and
/// for rows longer than 12 bytes past their shared prefix, a stack of at most
/// one 24-byte frame per 12 bytes of the longest row.
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
/// let indices = radix_sort_to_indices(&rows, None).unwrap();
/// assert_eq!(indices.values(), &[2, 4, 1, 3, 0]); // points to [1, 2, 3, 4, 5]
///
/// // With a limit, only the first `limit` indices are returned
/// let indices = radix_sort_to_indices(&rows, Some(2)).unwrap();
/// assert_eq!(indices.values(), &[2, 4]);
/// ```
///
/// [`RowConverter::convert_columns`]: crate::RowConverter::convert_columns
/// [`take`]: https://docs.rs/arrow/latest/arrow/compute/fn.take.html
/// [`lexsort_to_indices`]: https://docs.rs/arrow-ord/latest/arrow_ord/sort/fn.lexsort_to_indices.html
pub fn radix_sort_to_indices(rows: &Rows, limit: Option<usize>) -> Result<UInt32Array, ArrowError> {
    let n = rows.num_rows();
    // Entries store, and the result returns, each row's index as a u32
    u32::try_from(n)
        .map_err(|_| ArrowError::ComputeError(format!("cannot sort {n} rows with u32 indices")))?;
    let keep = limit.map_or(n, |limit| limit.min(n));
    let Some(first) = rows.iter().next().filter(|_| keep > 0) else {
        return Ok(UInt32Array::from(Vec::<u32>::new()));
    };
    // Input stored in key order often reaches a sort the planner can't remove.
    // The check stops at the first pair of rows out of order, so unsorted input
    // pays for little more than its first few rows.
    if rows.iter().is_sorted() {
        return Ok(UInt32Array::from_iter_values(0..keep as u32));
    }

    // Bytes that every row shares can't order rows, so the key starts after
    // them. The longest row decides below whether equal keys need more sorting.
    let first = first.data();
    let (prefix, max_len) =
        rows.iter()
            .skip(1)
            .fold((first.len(), first.len()), |(prefix, max_len), row| {
                let row = row.data();
                (common_prefix(&first[..prefix], row), max_len.max(row.len()))
            });

    // Each entry holds the next 12 bytes of its row as a big-endian key in its
    // high 96 bits and the row's index in its low 32 bits, so entries compare
    // by key and move as one value. 12 bytes cover an Int32 and an Int64 after
    // their shared null and high bytes, the shape of a BIGINT join key.
    let buffer = rows.buffer.as_slice();
    let mut entries: Vec<u128> = rows
        .offsets
        .windows(2)
        .enumerate()
        .map(|(i, w)| entry(buffer, w[0] + prefix, i))
        .collect();
    let mut temp = vec![0u128; n];
    // Only the first `keep` entries are final. The sort returns how many
    // entries it left in key order, which is at least `keep`, and the rest of
    // `entries` is not valid.
    let sorted = if keep < n {
        msd_radix_sort::<true, _>(&mut entries, &mut temp, 0, true, First(keep))
    } else {
        msd_radix_sort::<true, _>(&mut entries, &mut temp, 0, true, Unlimited)
    };
    let entries = &mut entries[..sorted];

    // Entries with equal keys come from rows that agree on every byte before
    // prefix + KEY_BYTES, so the rest of each row decides their order. When no
    // row is longer than that, equal keys mean equal rows and there is nothing
    // left to sort.
    let suffix = prefix + KEY_BYTES as usize;
    if max_len > suffix {
        let mut stack = Vec::new();
        let mut start = 0;
        for run in entries.chunk_by_mut(|&a, &b| key(a) == key(b)) {
            if start >= keep {
                break;
            }
            let limit = keep - start;
            start += run.len();
            if run.len() > 1 {
                sort_ties(rows, run, suffix, limit, &mut stack);
            }
        }
    }
    // The low 32 bits of each entry are its row's index
    Ok(UInt32Array::from_iter_values(
        entries[..keep].iter().map(|&e| e as u32),
    ))
}

/// How many leading entries `msd_radix_sort` has to put in key order.
///
/// A sort of every entry takes `Unlimited`, a zero-sized type, so it has the
/// same arguments and code as a sort without limits.
trait Limit: Copy {
    /// Whether the sort can stop early
    const LIMITED: bool;
    /// The number of leading entries that must be in key order
    fn get(self) -> usize;
    /// The limit for a bucket that starts `start` entries in
    fn skip(self, start: usize) -> Self;
}

#[derive(Clone, Copy)]
struct Unlimited;

impl Limit for Unlimited {
    const LIMITED: bool = false;
    fn get(self) -> usize {
        usize::MAX
    }
    fn skip(self, _: usize) -> Self {
        self
    }
}

#[derive(Clone, Copy)]
struct First(usize);

impl Limit for First {
    const LIMITED: bool = true;
    fn get(self) -> usize {
        self.0
    }
    fn skip(self, start: usize) -> Self {
        First(self.0 - start)
    }
}

/// MSD radix sort of entries by their key.
///
/// Each level scatters from `src` into `dst` and recurses with the two
/// swapped, so no level copies its entries back. `result_in_src` says whether
/// the caller expects the sorted entries in `src` or in `dst`.
///
/// Only the first `limit` entries need to be in key order. A limited sort
/// skips each bucket that starts at or after `limit`, and returns the length of the
/// prefix of `result` it left in key order. That prefix is at least `limit`
/// and ends at a bucket boundary, so the entries after it never extend a run
/// of equal keys. The rest of the result is not valid.
///
/// `byte` is the key byte to split on, counted from the most significant.
/// Every entry in `src` agrees on the bytes before it. When every entry also
/// shares `byte`, the sort skips ahead to the first byte that differs, so the
/// recursion ends after at most KEY_BYTES levels.
///
/// `HIGH` is whether `byte` is in the high 64 bits of an entry (key bytes 0
/// to 7) rather than the low 64 bits (key bytes 8 to 11). Fixing the half for
/// a whole pass keeps extracting a key byte to one 64-bit shift.
fn msd_radix_sort<const HIGH: bool, L: Limit>(
    src: &mut [u128],
    dst: &mut [u128],
    mut byte: u32,
    result_in_src: bool,
    limit: L,
) -> usize {
    if src.len() <= FALLBACK_THRESHOLD {
        let out = if result_in_src {
            src
        } else {
            dst.copy_from_slice(src);
            dst
        };
        // Entries compare by key, then by index. Comparing integers costs
        // less than another radix pass on a bucket this small.
        out.sort_unstable();
        return out.len();
    }

    // u32 counts halve the array zeroed at each level. They can't overflow,
    // because radix_sort_to_indices checks that src.len() <= u32::MAX.
    let mut counts: [u32; 256];
    let shift = loop {
        if byte == KEY_BYTES {
            // Every key is equal, and the caller orders equal keys.
            if !result_in_src {
                dst.copy_from_slice(src);
            }
            return src.len();
        }
        if HIGH && byte >= 8 {
            return msd_radix_sort::<false, L>(src, dst, byte, result_in_src, limit);
        }
        // Moves key byte `byte` to the low byte of its half of an entry
        let shift = (120 - 8 * byte) % 64;
        counts = [0; 256];
        for &e in src.iter() {
            counts[digit::<HIGH>(e, shift)] += 1;
        }
        if counts[digit::<HIGH>(src[0], shift)] as usize != src.len() {
            break shift;
        }
        // Every entry shares this byte. Rather than one pass per shared byte,
        // find the first byte where any key differs. A key's top 32 bits are
        // zero, so they are not key bytes. diff is 0 when every key is equal,
        // which sets byte to KEY_BYTES and ends the loop above.
        let first = key(src[0]);
        let diff = src.iter().fold(0, |acc, &e| acc | (key(e) ^ first));
        byte = (diff.leading_zeros() - 32) / 8;
    };
    // Turn counts into each bucket's start in place. The scatter then advances
    // them to each bucket's end. One array saves zeroing and copying separate
    // offset and write position arrays at every level.
    let mut sum = 0;
    for c in &mut counts {
        (*c, sum) = (sum, sum + *c);
    }
    for &e in src.iter() {
        let b = digit::<HIGH>(e, shift);
        dst[counts[b] as usize] = e;
        counts[b] += 1;
    }

    let mut start = 0;
    for &end in &counts {
        if L::LIMITED && start >= limit.get() {
            break;
        }
        let end = end as usize;
        if end - start > 1 {
            let sorted = msd_radix_sort::<HIGH, L>(
                &mut dst[start..end],
                &mut src[start..end],
                byte + 1,
                !result_in_src,
                limit.skip(start),
            );
            if L::LIMITED && sorted < end - start {
                return start + sorted;
            }
        } else if end - start == 1 && result_in_src {
            // A singleton bucket doesn't recurse. Its entry is in dst, so copy
            // it back when the caller expects the result in src.
            src[start] = dst[start];
        }
        start = end;
    }
    start
}

/// The entry for row `index`, keyed by the 12 bytes of the rows buffer that
/// start at `start`.
///
/// A row that ends within the key takes its last key bytes from the next row,
/// because a fixed-size load is one instruction and a variable-length copy
/// calls memcpy. Those bytes can only reorder equal rows: the rows sorted
/// against each other agree on every byte before the key, and no row is a
/// proper prefix of another, because every field encoding is self-delimiting,
/// so two different rows differ within the shorter one. The load reads the
/// buffer directly, because `Row::data` ends at the row.
fn entry(buffer: &[u8], start: usize, index: usize) -> u128 {
    let bytes = match buffer.get(start..).and_then(<[u8]>::first_chunk::<16>) {
        Some(bytes) => *bytes,
        // The last rows of the buffer are padded with zeros instead
        None => {
            let tail = buffer.get(start..).unwrap_or_default();
            let mut bytes = [0u8; 16];
            bytes[..tail.len()].copy_from_slice(tail);
            bytes
        }
    };
    // The last 4 bytes loaded lie past the key, and the index replaces them
    (u128::from_be_bytes(bytes) & !u128::from(u32::MAX)) | index as u128
}

/// Orders `run`, whose rows agree on every byte before `offset`, by the rest
/// of each row.
///
/// Each round keys a run's entries by the next 12 bytes of their rows and
/// sorts them, then does the same for each sub-run whose keys still tie. A row
/// is read once per round instead of once per comparison. `stack` holds one
/// frame per round in progress instead of recursion, because rows can be
/// arbitrarily long, so its length is bounded by the longest row divided by
/// 12, not by the number of rows. It is empty on entry and on return.
///
/// Only the first `limit` rows of `run` need to be in order.
fn sort_ties(rows: &Rows, run: &mut [u128], offset: usize, limit: usize, stack: &mut Vec<Frame>) {
    if let Some(next) = sort_round(rows, run, offset) {
        stack.push(Frame {
            start: 0,
            end: run.len(),
            next,
        });
    }
    while let Some(frame) = stack.last_mut() {
        let Frame { start, end, next } = *frame;
        // Sub-runs that start at or after `limit` can stay unordered
        if start == end || start >= limit {
            stack.pop();
            continue;
        }
        let first = key(run[start]);
        let len = run[start..end]
            .iter()
            .take_while(|&&e| key(e) == first)
            .count();
        frame.start = start + len;
        if len > 1
            && let Some(after) = sort_round(rows, &mut run[start..start + len], next)
        {
            stack.push(Frame {
                start,
                end: start + len,
                next: after,
            });
        }
    }
}

/// A run of entries in `sort_ties` that one round has sorted
#[derive(Clone, Copy)]
struct Frame {
    /// The start of the next sub-run to look at
    start: usize,
    /// The end of the run
    end: usize,
    /// The row offset where the run's next round starts
    next: usize,
}

/// Keys the entries of `run` by the 12 bytes of their rows at `offset` and
/// sorts them.
///
/// Returns the offset of the next round, or `None` if the key reaches the end
/// of every row in `run`, because entries that still tie then come from equal
/// rows. Stopping at the run's own longest row matters: equal short rows next
/// to each other borrow equal bytes from the rows after them, and would keep
/// tying until the key passed the longest row of the whole input.
///
/// When every entry still ties, the next round starts at the first byte where
/// any row of the run differs, found in one pass. Otherwise runs that share
/// long stretches, such as rows with the same long string, would take a round
/// of reloading every row for each 12 bytes of the stretch.
fn sort_round(rows: &Rows, run: &mut [u128], offset: usize) -> Option<usize> {
    let buffer = rows.buffer.as_slice();
    let row = |index: usize| &buffer[rows.offsets[index]..rows.offsets[index + 1]];
    let mut longest = 0;
    for e in run.iter_mut() {
        // The low 32 bits of an entry hold its row's index
        let index = *e as u32 as usize;
        let (start, end) = (rows.offsets[index], rows.offsets[index + 1]);
        longest = longest.max(end - start);
        *e = entry(buffer, start + offset, index);
    }
    run.sort_unstable();
    let mut next = offset + KEY_BYTES as usize;
    // The run is sorted, so its first and last keys are equal only if all are
    if next < longest && key(run[0]) == key(run[run.len() - 1]) {
        let rest = |e: u128| row(e as u32 as usize).get(next..).unwrap_or_default();
        let first = rest(run[0]);
        next += run[1..].iter().fold(first.len(), |shared, &e| {
            common_prefix(&first[..shared], rest(e))
        });
    }
    (next < longest).then_some(next)
}

/// The length of the longest prefix that `a` and `b` share
// `radix_sort_to_indices` calls this once per row to find the shared prefix.
// With `sort_round` as a second caller, release builds stop inlining it there,
// which adds a call per row to every sort.
#[inline(always)]
fn common_prefix(a: &[u8], b: &[u8]) -> usize {
    // Most keys share less than 8 bytes, and a plain byte loop costs the least
    // for them
    if a.len() < 8 {
        return a.iter().zip(b).take_while(|(x, y)| x == y).count();
    }
    // Compares 8 bytes at a time, because rows can share long prefixes, such
    // as the high bytes of small decimals, and every row compares them.
    let (a_chunks, _) = a.as_chunks::<8>();
    let (b_chunks, _) = b.as_chunks::<8>();
    for (i, (x, y)) in a_chunks.iter().zip(b_chunks).enumerate() {
        let diff = u64::from_be_bytes(*x) ^ u64::from_be_bytes(*y);
        if diff != 0 {
            return i * 8 + diff.leading_zeros() as usize / 8;
        }
    }
    let done = a_chunks.len().min(b_chunks.len()) * 8;
    done + a[done..]
        .iter()
        .zip(&b[done..])
        .take_while(|(x, y)| x == y)
        .count()
}

/// The 96-bit key of an entry built by [`radix_sort_to_indices`]
fn key(entry: u128) -> u128 {
    entry >> 32
}

/// The key byte of an entry that `shift` moves to the low byte of the high
/// (`HIGH`) or low 64 bits of the entry
fn digit<const HIGH: bool>(entry: u128, shift: u32) -> usize {
    let half = if HIGH {
        (entry >> 64) as u64
    } else {
        entry as u64
    };
    (half >> shift) as u8 as usize
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::generate_column;
    use crate::{RowConverter, SortField};
    use arrow_array::{
        ArrayRef, BooleanArray, Decimal128Array, FixedSizeBinaryArray, Float64Array, Int8Array,
        Int32Array, Int64Array, StringArray,
    };
    use arrow_schema::{DataType, SortOptions};
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};
    use std::sync::Arc;

    fn sort(rows: &Rows) -> Vec<u32> {
        sort_limit(rows, None)
    }

    fn sort_limit(rows: &Rows, limit: Option<usize>) -> Vec<u32> {
        radix_sort_to_indices(rows, limit)
            .unwrap()
            .values()
            .to_vec()
    }

    /// Limits at and around the boundaries of a `len`-row input
    fn limits(len: usize) -> Vec<Option<usize>> {
        let mut limits = vec![
            None,
            Some(0),
            Some(1),
            Some(len / 2),
            Some(len),
            Some(len + 1),
        ];
        limits.extend(len.checked_sub(1).map(Some));
        limits
    }

    /// Checks that sorting `rows` with `limit` returns the first `limit` rows
    /// of `expected`, a full sort of the same rows. Rows that compare equal can
    /// come back in either order, so the rows at each position must be equal.
    fn assert_limit_matches(rows: &Rows, expected: &[u32], limit: Option<usize>) {
        let indices = sort_limit(rows, limit);
        let len = limit.map_or(expected.len(), |l| l.min(expected.len()));
        assert_eq!(indices.len(), len, "limit {limit:?}");
        let mut seen = vec![false; rows.num_rows()];
        for (i, &index) in indices.iter().enumerate() {
            assert!(
                !std::mem::replace(&mut seen[index as usize], true),
                "limit {limit:?}: index {index} repeated"
            );
            assert_eq!(
                rows.row(index as usize),
                rows.row(expected[i] as usize),
                "limit {limit:?}: mismatch at position {i}"
            );
        }
    }

    fn assert_permutation(indices: &[u32], len: usize) {
        let mut sorted = indices.to_vec();
        sorted.sort_unstable();
        assert!(sorted.into_iter().eq(0..len as u32), "not a permutation");
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
            assert_permutation(&indices, n);
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

        for _ in 0..200 {
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
            assert_permutation(&indices, len);
            for limit in limits(len) {
                assert_limit_matches(&rows, &indices, limit);
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

            // Neither sort is stable, so equal rows can come back in a
            // different order, but the rows at each position must be equal.
            for i in 0..len {
                let radix_row = rows.row(radix[i] as usize);
                let cmp_row = rows.row(comparison[i] as usize);
                assert_eq!(
                    radix_row, cmp_row,
                    "mismatch at position {i}: radix idx={} vs comparison idx={}",
                    radix[i], comparison[i]
                );
            }
            for limit in limits(len) {
                assert_limit_matches(&rows, &comparison, limit);
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

    /// Equal rows longer than the key, more than FALLBACK_THRESHOLD of them,
    /// tie on their keys and go to `sort_ties`. The smaller last row keeps the
    /// input from returning early as sorted.
    #[test]
    fn test_radix_sort_equal_rows_past_threshold() {
        let mut values = vec!["the same long value"; 100];
        values.push("a");
        let array: ArrayRef = Arc::new(StringArray::from(values));
        let converter = RowConverter::new(vec![SortField::new(DataType::Utf8)]).unwrap();
        let rows = converter.convert_columns(&[array]).unwrap();

        let indices = sort(&rows);
        assert_sorted(&rows, &indices);
        assert_permutation(&indices, 101);
        assert_eq!(indices[0], 100);
    }

    /// Rows that share a prefix are keyed by the bytes after it.
    #[test]
    fn test_radix_sort_shared_string_prefix() {
        let values: Vec<String> = (0..100)
            .map(|i| format!("shared-prefix-longer-than-the-key-{}", (i * 37) % 100))
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

    #[test]
    fn test_common_prefix() {
        let a: Vec<u8> = (0..20).collect();
        let cases: &[(&[u8], &[u8], usize)] = &[
            (&[], &[], 0),
            (&[], &a, 0),
            (&a, &a, 20),
            (&a[..5], &a, 5),
            (&a, &a[..12], 12),
            (&[1, 2, 3], &[1, 2, 4], 2),
        ];
        for &(x, y, expected) in cases {
            assert_eq!(common_prefix(x, y), expected, "{x:?} {y:?}");
        }
        for i in [0, 7, 8, 9, 15, 16, 19] {
            let mut b = a.clone();
            b[i] ^= 0x80;
            assert_eq!(common_prefix(&a, &b), i, "differ at {i}");
            assert_eq!(common_prefix(&b, &a), i, "differ at {i}");
        }
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
            Decimal128Array::from(decimals)
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

    /// Two-byte rows end inside the key, so keys are zero-padded.
    #[test]
    fn test_radix_sort_rows_shorter_than_key() {
        let mut rng = StdRng::seed_from_u64(0x8);
        let values: Vec<Option<i8>> = (0..1000)
            .map(|_| rng.random_bool(0.8).then(|| rng.random()))
            .collect();
        let array: ArrayRef = Arc::new(Int8Array::from(values));
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

    /// Rows whose bytes after the shared prefix are each 0 or 1, at widths
    /// around the key size, so rows tie on their keys and the bytes after the
    /// key decide their order
    #[test]
    fn test_radix_sort_widths_around_key() {
        let mut rng = StdRng::seed_from_u64(0x12);
        for width in [7, 8, 9, 11, 12, 13, 16, 20] {
            let values: Vec<Vec<u8>> = (0..500)
                .map(|_| (0..width).map(|_| rng.random_range(0..2)).collect())
                .collect();
            let array: ArrayRef =
                Arc::new(FixedSizeBinaryArray::try_from_iter(values.iter()).unwrap());
            let converter =
                RowConverter::new(vec![SortField::new(DataType::FixedSizeBinary(width))]).unwrap();
            let rows = converter.convert_columns(&[array]).unwrap();

            let indices = sort(&rows);
            assert_sorted(&rows, &indices);
            assert_permutation(&indices, 500);
        }
    }

    /// `sort_ties` orders entries whose keys say nothing, given only their
    /// indices, by the whole row.
    #[test]
    fn test_sort_ties_orders_rows_from_offset() {
        let mut rng = StdRng::seed_from_u64(0x7135);
        for _ in 0..50 {
            let len = rng.random_range(1..300);
            let strings: ArrayRef = Arc::new(StringArray::from_iter((0..len).map(|_| {
                rng.random_bool(0.9).then(|| {
                    let n = rng.random_range(0..40);
                    "ab".repeat(n / 2) + &rng.random_range(0..3).to_string()
                })
            })));
            let ints: ArrayRef = Arc::new(Int32Array::from_iter_values(
                (0..len).map(|_| rng.random_range(0..4)),
            ));
            let converter = RowConverter::new(vec![
                SortField::new(DataType::Utf8),
                SortField::new(DataType::Int32),
            ])
            .unwrap();
            let rows = converter.convert_columns(&[strings, ints]).unwrap();

            let mut entries: Vec<u128> = (0..len as u128).collect();
            sort_ties(&rows, &mut entries, 0, usize::MAX, &mut vec![]);
            let indices: Vec<u32> = entries.iter().map(|&e| e as u32).collect();
            assert_sorted(&rows, &indices);
            assert_permutation(&indices, len);
        }
    }

    /// Sorted input, with equal rows, comes back as `0..n`, and input sorted
    /// except for its last row still comes back sorted.
    #[test]
    fn test_radix_sort_sorted_input() {
        let len = 1000;
        let keys: Vec<i64> = (0..len).map(|i| i / 3).collect();
        let names: Vec<String> = (0..len).map(|i| format!("{:>8}", i / 5)).collect();
        let converter = RowConverter::new(vec![
            SortField::new(DataType::Int64),
            SortField::new(DataType::Utf8),
        ])
        .unwrap();

        let sorted: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(keys.clone())),
            Arc::new(StringArray::from(names.clone())),
        ];
        let rows = converter.convert_columns(&sorted).unwrap();
        assert_eq!(sort(&rows), (0..len as u32).collect::<Vec<_>>());

        let mut last_out_of_order = keys;
        last_out_of_order[len as usize - 1] = -1;
        let unsorted: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(last_out_of_order)),
            Arc::new(StringArray::from(names)),
        ];
        let rows = converter.convert_columns(&unsorted).unwrap();
        let indices = sort(&rows);
        assert_sorted(&rows, &indices);
        assert_permutation(&indices, len as usize);
        assert_eq!(indices[0], len as u32 - 1);
    }

    /// When a round leaves every entry tied, `sort_round` skips to the first
    /// byte where the run's rows differ, and to the end when they are equal.
    #[test]
    fn test_sort_round_skips_bytes_the_run_shares() {
        let shared = "a value every row shares, longer than one key";
        let cases: [Vec<String>; 3] = [
            (0..40).map(|i| format!("{shared}{}", i % 7)).collect(),
            (0..40)
                .map(|i| format!("{shared}{}{shared}", i % 3))
                .collect(),
            vec![shared.to_string(); 40],
        ];
        for values in cases {
            let array: ArrayRef = Arc::new(StringArray::from(values));
            let converter = RowConverter::new(vec![SortField::new(DataType::Utf8)]).unwrap();
            let rows = converter.convert_columns(&[array]).unwrap();
            let first = rows.row(0);
            let differ = rows
                .iter()
                .map(|row| common_prefix(first.data(), row.data()))
                .min()
                .unwrap();
            let longest = rows.iter().map(|row| row.data().len()).max().unwrap();

            let mut entries: Vec<u128> = (0..rows.num_rows() as u128).collect();
            let expected = (differ < longest).then_some(differ);
            assert_eq!(sort_round(&rows, &mut entries, 0), expected);
        }
    }

    /// A run that splits into many tied sub-runs, and a run of equal short rows
    /// next to one long row, leave only a few frames on the stack.
    #[test]
    fn test_sort_ties_stack_stays_small() {
        // 1000 rows share their first 16 bytes, then split into 500 pairs that
        // share the next 16 bytes and differ in the last one.
        let pairs: Vec<String> = (0..1000)
            .map(|i| format!("{:>16}{:>16}{}", "same", i / 2, i % 2))
            .collect();
        // Equal one-byte rows borrow equal key bytes from the rows after them
        let mut short = vec!["a".to_string(); 1000];
        short.push("b".repeat(4000));

        for values in [pairs, short] {
            let len = values.len();
            let array: ArrayRef = Arc::new(StringArray::from(values));
            let converter = RowConverter::new(vec![SortField::new(DataType::Utf8)]).unwrap();
            let rows = converter.convert_columns(&[array]).unwrap();

            let mut entries: Vec<u128> = (0..len as u128).collect();
            let mut stack = Vec::new();
            sort_ties(&rows, &mut entries, 0, usize::MAX, &mut stack);
            assert!(stack.capacity() <= 8, "capacity {}", stack.capacity());
            let indices: Vec<u32> = entries.iter().map(|&e| e as u32).collect();
            assert_sorted(&rows, &indices);
            assert_permutation(&indices, len);
        }
    }

    /// Rows that tie on their key through hundreds of 12-byte rounds, and
    /// duplicates longer than the key, are ordered.
    #[test]
    fn test_radix_sort_ties_through_long_rows() {
        let shared = "x".repeat(5000);
        let values: Vec<String> = (0..300)
            .map(|i| format!("{shared}{}", (i * 7) % 20))
            .collect();
        let array: ArrayRef = Arc::new(StringArray::from(values));
        let converter = RowConverter::new(vec![SortField::new(DataType::Utf8)]).unwrap();
        let rows = converter.convert_columns(&[array]).unwrap();

        let indices = sort(&rows);
        assert_sorted(&rows, &indices);
        assert_permutation(&indices, 300);
    }

    /// A low-cardinality Int32, a small Int64, and a larger Int64, the shape of
    /// sorting lineitem by (l_linenumber, l_suppkey, l_orderkey). The 12-byte
    /// key ends before the bytes of the last column that vary, so most rows tie
    /// on their key.
    #[test]
    fn test_radix_sort_ties_past_key_on_integer_columns() {
        let mut rng = StdRng::seed_from_u64(0x0505);
        let len = 5000;
        for descending in [false, true] {
            for nulls_first in [false, true] {
                let options = SortOptions {
                    descending,
                    nulls_first,
                };
                let line: ArrayRef = Arc::new(Int32Array::from_iter(
                    (0..len).map(|_| rng.random_bool(0.95).then(|| rng.random_range(1..8))),
                ));
                let supplier: ArrayRef = Arc::new(Int64Array::from_iter_values(
                    (0..len).map(|_| rng.random_range(0..500)),
                ));
                let order: ArrayRef = Arc::new(Int64Array::from_iter_values(
                    (0..len).map(|_| rng.random_range(0..1 << 31)),
                ));
                let converter = RowConverter::new(vec![
                    SortField::new_with_options(DataType::Int32, options),
                    SortField::new_with_options(DataType::Int64, options),
                    SortField::new_with_options(DataType::Int64, options),
                ])
                .unwrap();
                let rows = converter.convert_columns(&[line, supplier, order]).unwrap();

                let indices = sort(&rows);
                assert_sorted(&rows, &indices);
                assert_permutation(&indices, len);
            }
        }
    }

    /// Small Int64 values after an Int32 have zero high bytes, the shape of a
    /// BIGINT join key such as (l_partkey, l_suppkey)
    #[test]
    fn test_radix_sort_int32_then_small_int64() {
        let mut rng = StdRng::seed_from_u64(0x14);
        let items: ArrayRef = Arc::new(Int32Array::from_iter_values(
            (0..2000).map(|_| rng.random_range(1..50)),
        ));
        let tickets: ArrayRef = Arc::new(Int64Array::from_iter_values(
            (0..2000).map(|_| rng.random_range(0..200)),
        ));
        let converter = RowConverter::new(vec![
            SortField::new(DataType::Int32),
            SortField::new(DataType::Int64),
        ])
        .unwrap();
        let rows = converter.convert_columns(&[items, tickets]).unwrap();

        let indices = sort(&rows);
        assert_sorted(&rows, &indices);
        assert_permutation(&indices, 2000);
    }

    #[test]
    fn test_radix_sort_limit_empty_input() {
        let converter = RowConverter::new(vec![SortField::new(DataType::Int32)]).unwrap();
        let rows = converter
            .convert_columns(&[Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef])
            .unwrap();
        for limit in [None, Some(0), Some(1)] {
            assert!(sort_limit(&rows, limit).is_empty());
        }
    }

    #[test]
    fn test_radix_sort_limit_values() {
        let array: ArrayRef = Arc::new(Int32Array::from(vec![5, 3, 1, 4, 2]));
        let converter = RowConverter::new(vec![SortField::new(DataType::Int32)]).unwrap();
        let rows = converter.convert_columns(&[array]).unwrap();
        assert_eq!(sort_limit(&rows, Some(0)), Vec::<u32>::new());
        assert_eq!(sort_limit(&rows, Some(2)), [2, 4]);
        assert_eq!(sort_limit(&rows, Some(5)), [2, 4, 1, 3, 0]);
        assert_eq!(sort_limit(&rows, Some(6)), [2, 4, 1, 3, 0]);
        assert_eq!(sort_limit(&rows, Some(usize::MAX)), [2, 4, 1, 3, 0]);
    }

    #[test]
    fn test_radix_sort_limit_sorted_input() {
        let array: ArrayRef = Arc::new(Int32Array::from_iter_values(0..100));
        let converter = RowConverter::new(vec![SortField::new(DataType::Int32)]).unwrap();
        let rows = converter.convert_columns(&[array]).unwrap();
        assert_eq!(sort_limit(&rows, Some(10)), (0..10).collect::<Vec<u32>>());
        assert_eq!(sort_limit(&rows, Some(200)), (0..100).collect::<Vec<u32>>());
    }

    /// Limits that end inside a run of equal keys, with rows long enough to
    /// need re-keying, and on each side of the comparison fallback threshold.
    #[test]
    fn test_radix_sort_limit_inside_ties() {
        let mut rng = StdRng::seed_from_u64(0xCAFE);
        let len = 5000;
        let low: ArrayRef = Arc::new(Int32Array::from_iter_values(
            (0..len).map(|_| rng.random_range(0..3)),
        ));
        let strings: ArrayRef = Arc::new(StringArray::from_iter_values((0..len).map(|_| {
            format!(
                "shared prefix longer than the key {}",
                rng.random_range(0..400)
            )
        })));
        let converter = RowConverter::new(vec![
            SortField::new(DataType::Int32),
            SortField::new(DataType::Utf8),
        ])
        .unwrap();
        let rows = converter.convert_columns(&[low, strings]).unwrap();
        let full = sort(&rows);
        assert_sorted(&rows, &full);
        for limit in [1, 31, 32, 33, 1000, 1666, 1667, 1668, 4999] {
            assert_limit_matches(&rows, &full, Some(limit));
        }
    }
}
