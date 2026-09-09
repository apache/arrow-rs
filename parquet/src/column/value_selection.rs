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

//! Logical value selections and their mapping to physical value storage.
//!
//! [`ValueSelectionRef`] describes selected logical value positions after level
//! planning. [`PhysicalValueSelection`] maps those positions through optional
//! dictionary keys to the physical value array.

use std::{mem::MaybeUninit, ops::Range};

use arrow_buffer::ArrowNativeType;

use crate::column::value_batch::ValueProducer;

/// Borrowed view of a selected set of values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ValueSelectionRef<'a> {
    Empty,
    Dense { offset: usize, len: usize },
    Sparse(&'a [usize]),
}

impl ValueSelectionRef<'_> {
    pub(crate) fn len(self) -> usize {
        match self {
            Self::Empty => 0,
            Self::Dense { len, .. } => len,
            Self::Sparse(indices) => indices.len(),
        }
    }

    pub(crate) fn slice(self, offset: usize, len: usize) -> Self {
        match self {
            Self::Empty => {
                debug_assert_eq!(offset, 0);
                debug_assert_eq!(len, 0);
                Self::Empty
            }
            Self::Dense {
                offset: base,
                len: selection_len,
            } => {
                // The count-based guard in `write_mini_batch` is the only other
                // bounds protection on this path; catch a miscounted window
                // here to prevent an out-of-bounds value read.
                debug_assert!(offset + len <= selection_len);
                Self::Dense {
                    offset: base + offset,
                    len,
                }
            }
            Self::Sparse(indices) => Self::Sparse(&indices[offset..offset + len]),
        }
    }

    #[inline(always)]
    pub(crate) fn index_at(self, idx: usize) -> usize {
        debug_assert!(idx < self.len());
        match self {
            Self::Empty => unreachable!("empty value selection has no values"),
            Self::Dense { offset, .. } => offset + idx,
            Self::Sparse(indices) => indices[idx],
        }
    }

    #[inline]
    pub(crate) fn try_for_each<E>(
        self,
        mut f: impl FnMut(usize) -> Result<(), E>,
    ) -> Result<(), E> {
        match self {
            Self::Empty => Ok(()),
            Self::Dense { offset, len } => {
                for idx in offset..offset + len {
                    f(idx)?;
                }
                Ok(())
            }
            Self::Sparse(indices) => {
                for &idx in indices {
                    f(idx)?;
                }
                Ok(())
            }
        }
    }
}

/// Borrowed, type-erased Arrow dictionary keys for the eight legal key widths.
#[derive(Debug, Clone, Copy)]
pub(crate) enum DictionaryKeys<'a> {
    I8(&'a [i8]),
    I16(&'a [i16]),
    I32(&'a [i32]),
    I64(&'a [i64]),
    U8(&'a [u8]),
    U16(&'a [u16]),
    U32(&'a [u32]),
    U64(&'a [u64]),
}

impl<'a> DictionaryKeys<'a> {
    /// Number of keys, i.e. logical values in the bound source.
    fn len(self) -> usize {
        match self {
            Self::I8(keys) => keys.len(),
            Self::I16(keys) => keys.len(),
            Self::I32(keys) => keys.len(),
            Self::I64(keys) => keys.len(),
            Self::U8(keys) => keys.len(),
            Self::U16(keys) => keys.len(),
            Self::U32(keys) => keys.len(),
            Self::U64(keys) => keys.len(),
        }
    }

    fn contiguous_range(self, selection: ValueSelectionRef<'_>) -> Option<Range<usize>> {
        match self {
            Self::I8(keys) => contiguous_key_range(keys, selection),
            Self::I16(keys) => contiguous_key_range(keys, selection),
            Self::I32(keys) => contiguous_key_range(keys, selection),
            Self::I64(keys) => contiguous_key_range(keys, selection),
            Self::U8(keys) => contiguous_key_range(keys, selection),
            Self::U16(keys) => contiguous_key_range(keys, selection),
            Self::U32(keys) => contiguous_key_range(keys, selection),
            Self::U64(keys) => contiguous_key_range(keys, selection),
        }
    }

    /// Visit selected dictionary values.
    #[inline]
    fn try_for_each<E>(
        self,
        selection: ValueSelectionRef<'a>,
        mut f: impl FnMut(usize) -> Result<(), E>,
    ) -> Result<(), E> {
        macro_rules! visit {
            ($keys:expr) => {
                selection.try_for_each(|row| f($keys[row].as_usize()))
            };
        }
        match self {
            Self::I8(keys) => visit!(keys),
            Self::I16(keys) => visit!(keys),
            Self::I32(keys) => visit!(keys),
            Self::I64(keys) => visit!(keys),
            Self::U8(keys) => visit!(keys),
            Self::U16(keys) => visit!(keys),
            Self::U32(keys) => visit!(keys),
            Self::U64(keys) => visit!(keys),
        }
    }
}

/// Return the mapped range when selected dictionary keys are consecutive.
/// First/last/middle probes reject cyclic and low-cardinality maps before the
/// full scan used by the high-cardinality identity-like case.
fn contiguous_key_range<K: ArrowNativeType>(
    keys: &[K],
    selection: ValueSelectionRef<'_>,
) -> Option<Range<usize>> {
    let len = selection.len();
    if len == 0 {
        return None;
    }
    let first = keys[selection.index_at(0)].as_usize();
    let end = first.checked_add(len)?;
    let expected_last = end - 1;
    if keys[selection.index_at(len - 1)].as_usize() != expected_last {
        return None;
    }
    let middle = len / 2;
    if keys[selection.index_at(middle)].as_usize() != first + middle {
        return None;
    }

    let mut expected = first;
    selection
        .try_for_each(|row| -> Result<(), ()> {
            if keys[row].as_usize() != expected {
                return Err(());
            }
            expected += 1;
            Ok(())
        })
        .ok()?;
    debug_assert_eq!(expected, end);
    Some(first..end)
}

/// A compact output-order span of physical value indices.
///
/// `Range` is a sequence of consecutive indices; `Gather` borrows explicit
/// indices. The span count contributes to the selection's logical length.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PhysicalValueSpan<'a> {
    Range { start: usize, len: usize },
    Gather(&'a [usize]),
}

impl PhysicalValueSpan<'_> {
    #[inline]
    pub(crate) fn len(self) -> usize {
        match self {
            Self::Range { len, .. } => len,
            Self::Gather(indices) => indices.len(),
        }
    }
}

/// Allocation-free composition of a value selection with an optional physical
/// dictionary mapping.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PhysicalValueSelection<'a> {
    selection: ValueSelectionRef<'a>,
    dictionary: Option<DictionaryKeys<'a>>,
}

impl<'a> PhysicalValueSelection<'a> {
    pub(crate) fn identity(selection: ValueSelectionRef<'a>) -> Self {
        Self {
            selection,
            dictionary: None,
        }
    }

    pub(crate) fn dictionary(selection: ValueSelectionRef<'a>, keys: DictionaryKeys<'a>) -> Self {
        Self {
            selection,
            dictionary: Some(keys),
        }
    }

    pub(crate) fn slice(self, offset: usize, len: usize) -> Self {
        Self {
            selection: self.selection.slice(offset, len),
            ..self
        }
    }

    pub(crate) fn len(self) -> usize {
        self.selection.len()
    }

    /// Return the source-coordinate selection when no dictionary mapping is
    /// present.
    pub(crate) fn unmapped_selection(self) -> Option<ValueSelectionRef<'a>> {
        self.dictionary.is_none().then_some(self.selection)
    }

    pub(crate) fn has_dictionary_mapping(self) -> bool {
        self.dictionary.is_some()
    }

    /// Whether caching one index per physical dictionary value is expected to
    /// avoid at least as many lookups as it introduces.
    pub(crate) fn should_cache_dictionary(self, physical_len: usize) -> bool {
        self.dictionary
            .is_some_and(|keys| physical_len.saturating_mul(2) <= keys.len())
    }

    /// Return the directly borrowable physical range, including a dictionary
    /// selection whose keys are consecutive.
    pub(crate) fn direct_physical_range(self) -> Option<Range<usize>> {
        match self.dictionary {
            None => match self.selection {
                ValueSelectionRef::Dense { offset, len } => Some(offset..offset + len),
                ValueSelectionRef::Sparse([index]) => Some(*index..*index + 1),
                _ => None,
            },
            Some(keys) => keys.contiguous_range(self.selection),
        }
    }

    /// Visit directly borrowable physical ranges, returning `false` without
    /// invoking `f` when scalar gathering is required.
    pub(crate) fn try_for_each_borrowable_range<E>(
        self,
        mut f: impl FnMut(Range<usize>) -> Result<(), E>,
    ) -> Result<bool, E> {
        match self.dictionary {
            Some(keys) => {
                let Some(range) = keys.contiguous_range(self.selection) else {
                    return Ok(false);
                };
                f(range)?;
            }
            None => match self.selection {
                ValueSelectionRef::Empty => {}
                ValueSelectionRef::Dense { offset, len } => f(offset..offset + len)?,
                ValueSelectionRef::Sparse([index]) => f(*index..*index + 1)?,
                ValueSelectionRef::Sparse(_) => return Ok(false),
            },
        }
        Ok(true)
    }

    /// Visit physical ranges and gathers in output order.
    pub(crate) fn try_for_each_span<E>(
        self,
        mut f: impl FnMut(PhysicalValueSpan<'a>) -> Result<(), E>,
    ) -> Result<(), E> {
        match self.dictionary {
            None => match self.selection {
                ValueSelectionRef::Empty => Ok(()),
                ValueSelectionRef::Dense { offset, len } => {
                    if len != 0 {
                        f(PhysicalValueSpan::Range { start: offset, len })?;
                    }
                    Ok(())
                }
                ValueSelectionRef::Sparse(indices) => {
                    if !indices.is_empty() {
                        f(PhysicalValueSpan::Gather(indices))?;
                    }
                    Ok(())
                }
            },
            Some(keys) => keys.try_for_each(self.selection, |index| {
                f(PhysicalValueSpan::Range {
                    start: index,
                    len: 1,
                })
            }),
        }
    }

    /// Write the leading values into `out`, advance past them, and return how
    /// many were written.
    ///
    /// Spans allow counted loops over contiguous output slots without
    /// interleaving them with a consumer callback that may flush the buffer.
    #[inline]
    pub(crate) fn fill_mapped<T: Copy>(
        &mut self,
        out: &mut [MaybeUninit<T>],
        map: impl Fn(usize) -> T,
    ) -> usize {
        let total = self.len();
        let filled = total.min(out.len());
        let head = self.slice(0, filled);
        *self = self.slice(filled, total - filled);
        let mut out = &mut out[..filled];
        head.try_for_each_span(|span| {
            let (slots, rest) = std::mem::take(&mut out).split_at_mut(span.len());
            match span {
                PhysicalValueSpan::Range { start, .. } => {
                    for (slot, index) in slots.iter_mut().zip(start..) {
                        slot.write(map(index));
                    }
                }
                PhysicalValueSpan::Gather(indices) => {
                    for (slot, &index) in slots.iter_mut().zip(indices) {
                        slot.write(map(index));
                    }
                }
            }
            out = rest;
            Ok::<(), ()>(())
        })
        .expect("filling from spans cannot fail");
        debug_assert!(out.is_empty(), "spans did not cover the requested prefix");
        filled
    }

    #[inline(always)]
    pub(crate) fn try_for_each_index<E>(
        self,
        f: impl FnMut(usize) -> Result<(), E>,
    ) -> Result<(), E> {
        match self.dictionary {
            None => self.selection.try_for_each(f),
            Some(keys) => keys.try_for_each(self.selection, f),
        }
    }
}

impl ValueProducer<usize> for PhysicalValueSelection<'_> {
    fn len(self) -> usize {
        PhysicalValueSelection::len(self)
    }

    fn fill(&mut self, out: &mut [MaybeUninit<usize>]) -> usize {
        self.fill_mapped(out, |index| index)
    }

    fn try_for_each<E>(self, f: impl FnMut(usize) -> Result<(), E>) -> Result<(), E> {
        self.try_for_each_index(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn borrowable_ranges_are_all_or_nothing() {
        let mut ranges = Vec::new();
        let dense =
            PhysicalValueSelection::identity(ValueSelectionRef::Dense { offset: 3, len: 4 });
        assert!(
            dense
                .try_for_each_borrowable_range(|range| {
                    ranges.push((range.start, range.end));
                    Ok::<_, ()>(())
                })
                .unwrap()
        );
        assert_eq!(ranges, [(3, 7)]);

        let sparse_indices = [1, 3];
        let sparse = PhysicalValueSelection::identity(ValueSelectionRef::Sparse(&sparse_indices));
        assert_eq!(sparse.slice(0, 1).direct_physical_range(), Some(1..2));
        assert!(
            sparse
                .slice(0, 1)
                .try_for_each_borrowable_range(|range| {
                    ranges.push((range.start, range.end));
                    Ok::<_, ()>(())
                })
                .unwrap()
        );
        assert_eq!(ranges, [(3, 7), (1, 2)]);
        assert!(
            !sparse
                .try_for_each_borrowable_range(|_| -> Result<(), ()> {
                    panic!("sparse input requires gathering")
                })
                .unwrap()
        );

        let keys = [2_i32, 3, 4];
        let mapped = PhysicalValueSelection::dictionary(
            ValueSelectionRef::Dense { offset: 0, len: 3 },
            DictionaryKeys::I32(&keys),
        );
        ranges.clear();
        assert!(
            mapped
                .try_for_each_borrowable_range(|range| {
                    ranges.push((range.start, range.end));
                    Ok::<_, ()>(())
                })
                .unwrap()
        );
        assert_eq!(ranges, [(2, 5)]);
    }

    fn physical_spans(selection: PhysicalValueSelection<'_>) -> Vec<PhysicalValueSpan<'_>> {
        let mut spans = Vec::new();
        selection
            .try_for_each_span(|span| -> Result<(), ()> {
                spans.push(span);
                Ok(())
            })
            .unwrap();
        spans
    }

    fn physical_indices(selection: PhysicalValueSelection<'_>) -> Vec<usize> {
        let mut values = Vec::new();
        selection
            .try_for_each_index(|index| -> Result<(), ()> {
                values.push(index);
                Ok(())
            })
            .unwrap();
        values
    }

    fn range(start: usize, len: usize) -> PhysicalValueSpan<'static> {
        PhysicalValueSpan::Range { start, len }
    }

    fn assert_physical_selection(selection: PhysicalValueSelection<'_>, expected: &[usize]) {
        let spans = physical_spans(selection);
        assert_eq!(
            spans.iter().map(|span| span.len()).sum::<usize>(),
            selection.len()
        );
        let expanded: Vec<usize> = spans
            .iter()
            .flat_map(|span| match span {
                PhysicalValueSpan::Range { start, len } => {
                    (*start..*start + *len).collect::<Vec<_>>()
                }
                PhysicalValueSpan::Gather(indices) => indices.to_vec(),
            })
            .collect();
        assert_eq!(expanded, expected);
        assert_eq!(physical_indices(selection), expected);
    }

    #[test]
    fn physical_identity_dense_sparse_and_slices() {
        assert!(
            physical_spans(PhysicalValueSelection::identity(ValueSelectionRef::Dense {
                offset: 3,
                len: 0
            }))
            .is_empty()
        );

        let dense =
            PhysicalValueSelection::identity(ValueSelectionRef::Dense { offset: 10, len: 8 });
        assert_eq!(
            dense.unmapped_selection(),
            Some(ValueSelectionRef::Dense { offset: 10, len: 8 })
        );
        assert_eq!(dense.direct_physical_range(), Some(10..18));
        assert_eq!(physical_spans(dense.slice(2, 4)), [range(12, 4)]);

        let sparse_values = [3, 4, 4, 5, 9, 9, 8, 1, 2];
        let sparse = PhysicalValueSelection::identity(ValueSelectionRef::Sparse(&sparse_values));
        assert_physical_selection(sparse, &sparse_values);
        assert_physical_selection(sparse.slice(1, 7), &sparse_values[1..8]);
    }

    #[test]
    fn physical_sparse_spans_match_exhaustive_flat_oracle() {
        // Every reordered/duplicated sequence over 0..3 up to length six.
        for len in 0..=6 {
            for encoded in 0..3usize.pow(len as u32) {
                let mut value = encoded;
                let mut indices = Vec::with_capacity(len);
                for _ in 0..len {
                    indices.push(value % 3);
                    value /= 3;
                }
                let selection =
                    PhysicalValueSelection::identity(ValueSelectionRef::Sparse(&indices));
                assert_physical_selection(selection, &indices);
            }
        }
    }

    #[test]
    fn physical_dictionary_widths_preserve_mapped_order() {
        macro_rules! assert_widths {
            ($($width:ty => $variant:ident),+ $(,)?) => {
                $({
                    let keys: [$width; 3] = [2, 0, 1];
                    let selection = PhysicalValueSelection::dictionary(
                        ValueSelectionRef::Dense { offset: 0, len: 3 },
                        DictionaryKeys::$variant(&keys),
                    );
                    assert_eq!(selection.direct_physical_range(), None);
                    assert_physical_selection(selection, &[2, 0, 1]);
                })+
            };
        }
        assert_widths!(
            i8 => I8,
            i16 => I16,
            i32 => I32,
            i64 => I64,
            u8 => U8,
            u16 => U16,
            u32 => U32,
            u64 => U64,
        );

        let keys = [4u8, 5, 6, 6, 7];
        let selection = PhysicalValueSelection::dictionary(
            ValueSelectionRef::Dense {
                offset: 0,
                len: keys.len(),
            },
            DictionaryKeys::U8(&keys),
        );
        assert_eq!(selection.unmapped_selection(), None);
        assert!(selection.has_dictionary_mapping());
        assert_physical_selection(selection, &[4, 5, 6, 6, 7]);
        let present = selection.slice(0, 4);
        assert_eq!(present.direct_physical_range(), None);
        let mut flat = Vec::new();
        present
            .try_for_each_index(|index| -> Result<(), ()> {
                flat.push(index);
                Ok(())
            })
            .unwrap();
        assert_eq!(flat, [4, 5, 6, 6]);

        let keys = [99_i32, 4, 5, 99, 6];
        let selected = [1_usize, 2, 4];
        let contiguous = PhysicalValueSelection::dictionary(
            ValueSelectionRef::Sparse(&selected),
            DictionaryKeys::I32(&keys),
        );
        assert_eq!(contiguous.direct_physical_range(), Some(4..7));

        for keys in [[4_i32, 5, 9, 7, 8], [4, 9, 6, 7, 8]] {
            let non_contiguous = PhysicalValueSelection::dictionary(
                ValueSelectionRef::Dense { offset: 0, len: 5 },
                DictionaryKeys::I32(&keys),
            );
            assert_eq!(non_contiguous.direct_physical_range(), None);
        }

        let mapped = PhysicalValueSelection::dictionary(
            ValueSelectionRef::Sparse(&selected),
            DictionaryKeys::I32(&keys),
        );
        assert_physical_selection(mapped, &[4, 5, 6]);
    }
}
