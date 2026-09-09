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
//! planning. [`PhysicalValueSelection`] maps those positions onto the physical
//! value array.

use std::{mem::MaybeUninit, ops::Range};

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

/// A compact output-order span of physical value indices: a sequence of
/// consecutive indices. The span length always contributes to the selection's
/// logical length.
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

/// Allocation-free view of a value selection in physical value coordinates.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PhysicalValueSelection<'a> {
    selection: ValueSelectionRef<'a>,
}

impl<'a> PhysicalValueSelection<'a> {
    pub(crate) fn identity(selection: ValueSelectionRef<'a>) -> Self {
        Self { selection }
    }

    pub(crate) fn slice(self, offset: usize, len: usize) -> Self {
        Self {
            selection: self.selection.slice(offset, len),
        }
    }

    pub(crate) fn len(self) -> usize {
        self.selection.len()
    }

    /// Return the source-coordinate selection.
    #[cfg(test)] // P16 enables native bridge use.
    pub(crate) fn unmapped_selection(self) -> Option<ValueSelectionRef<'a>> {
        Some(self.selection)
    }

    /// Return the directly borrowable physical range.
    pub(crate) fn direct_physical_range(self) -> Option<Range<usize>> {
        match self.selection {
            ValueSelectionRef::Dense { offset, len } => Some(offset..offset + len),
            ValueSelectionRef::Sparse([index]) => Some(*index..*index + 1),
            _ => None,
        }
    }

    /// Visit directly borrowable physical ranges, returning `false` without
    /// invoking `f` when scalar gathering is required.
    #[cfg(test)] // P16 enables native bridge use.
    pub(crate) fn try_for_each_borrowable_range<E>(
        self,
        mut f: impl FnMut(Range<usize>) -> Result<(), E>,
    ) -> Result<bool, E> {
        match self.selection {
            ValueSelectionRef::Empty => {}
            ValueSelectionRef::Dense { offset, len } => f(offset..offset + len)?,
            ValueSelectionRef::Sparse([index]) => f(*index..*index + 1)?,
            ValueSelectionRef::Sparse(_) => return Ok(false),
        }
        Ok(true)
    }

    /// Visit producer-provided ranges in output order.
    pub(crate) fn try_for_each_span<E>(
        self,
        mut f: impl FnMut(PhysicalValueSpan<'a>) -> Result<(), E>,
    ) -> Result<(), E> {
        match self.selection {
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
        self.selection.try_for_each(f)
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
}
