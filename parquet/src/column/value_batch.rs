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

//! Batch-oriented handoff between value producers and column encoders.

use std::{mem::MaybeUninit, slice};

#[cfg(feature = "arrow")]
use crate::column::value_selection::PhysicalValueSelection;
use crate::errors::Result;

/// Consumes native value batches without imposing a common scalar or owned
/// transfer representation.
pub(crate) trait BatchSink<B> {
    fn push_batch(&mut self, batch: B) -> Result<()>;
}

/// A bounded batch of source-provided values and their logical run lengths.
#[derive(Clone, Copy)]
pub(crate) struct RunBatch<'a, T> {
    pub(crate) values: &'a [T],
    pub(crate) counts: &'a [usize],
}

/// A copyable source that emits logical values in output order.
///
/// Non-contiguous and computed sources can be gathered into bounded batches;
/// sources that already expose a native batch can bypass this traversal.
pub(crate) trait ValueProducer<T: Copy>: Copy {
    /// Exact number of logical values this producer emits.
    fn len(self) -> usize;

    /// Write the next values into `out`, advance past them, and return how many
    /// were written. For a non-empty `out`, zero means the producer is exhausted.
    ///
    /// Filling a bounded buffer gives the compiler a counted loop rather than
    /// interleaving each value with a consumer callback that may flush the buffer.
    fn fill(&mut self, out: &mut [MaybeUninit<T>]) -> usize;

    /// Emit values in output order, for consumers that stream each value into
    /// an encoder rather than buffering batches.
    fn try_for_each<E>(self, f: impl FnMut(T) -> Result<(), E>) -> Result<(), E>;

    /// Emit source-provided run groups. The default emits one group per item.
    #[inline]
    fn for_each_run_group<E>(self, mut f: impl FnMut(T, usize) -> Result<(), E>) -> Result<(), E> {
        self.try_for_each(|value| f(value, 1))
    }
}

/// Projects a physical value selection through a copyable mapping while
/// preserving run groups.
#[cfg(feature = "arrow")]
#[derive(Clone, Copy)]
pub(crate) struct MappedValueProducer<'a, F> {
    source: PhysicalValueSelection<'a>,
    map: F,
}

#[cfg(feature = "arrow")]
pub(crate) fn map_values<F>(
    source: PhysicalValueSelection<'_>,
    map: F,
) -> MappedValueProducer<'_, F> {
    MappedValueProducer { source, map }
}

#[cfg(feature = "arrow")]
impl<T, F> ValueProducer<T> for MappedValueProducer<'_, F>
where
    T: Copy,
    F: Fn(usize) -> T + Copy,
{
    fn len(self) -> usize {
        self.source.len()
    }

    #[inline]
    fn fill(&mut self, out: &mut [MaybeUninit<T>]) -> usize {
        self.source.fill_mapped(out, self.map)
    }

    fn try_for_each<E>(self, mut f: impl FnMut(T) -> Result<(), E>) -> Result<(), E> {
        self.source.try_for_each_index(|index| f((self.map)(index)))
    }

    fn for_each_run_group<E>(self, mut f: impl FnMut(T, usize) -> Result<(), E>) -> Result<(), E> {
        self.source
            .try_for_each_index_group(|index, count| f((self.map)(index), count))
    }
}

/// View the initialized prefix of a `MaybeUninit` slice as initialized values.
///
/// # Safety
///
/// `len` must not exceed `values.len()`, and every element in `values[..len]`
/// must have been initialized as a valid `T`.
#[inline(always)]
pub(crate) unsafe fn assume_init_prefix<T>(values: &[MaybeUninit<T>], len: usize) -> &[T] {
    debug_assert!(len <= values.len());
    // SAFETY: guaranteed by the caller. `MaybeUninit<T>` has the same layout
    // and alignment as `T`.
    unsafe { slice::from_raw_parts(values.as_ptr().cast::<T>(), len) }
}

/// Gather produced values into bounded `N`-element stack batches.
#[inline(always)]
pub(crate) fn gather_tiled<const N: usize, T, P, Flush>(values: P, mut flush: Flush) -> Result<()>
where
    T: Copy,
    P: ValueProducer<T>,
    Flush: FnMut(&[T]) -> Result<()>,
{
    let mut values = values;
    let mut batch = [MaybeUninit::<T>::uninit(); N];
    loop {
        let filled = values.fill(&mut batch);
        if filled == 0 {
            return Ok(());
        }
        // SAFETY: `fill` initializes the leading `filled` slots of `batch`.
        flush(unsafe { assume_init_prefix(&batch, filled) })?;
    }
}

/// Gather run groups into bounded `(value, count)` stack batches.
#[inline(always)]
pub(crate) fn gather_run_groups_tiled<const N: usize, T, Flush>(
    values: impl ValueProducer<T>,
    mut flush: Flush,
) -> Result<()>
where
    T: Copy,
    Flush: FnMut(&[T], &[usize]) -> Result<()>,
{
    let mut value_batch = [MaybeUninit::<T>::uninit(); N];
    let mut count_batch = [MaybeUninit::<usize>::uninit(); N];
    let mut filled = 0;
    values.for_each_run_group(
        #[inline(always)]
        |value, count| -> Result<()> {
            value_batch[filled].write(value);
            count_batch[filled].write(count);
            filled += 1;
            if filled == N {
                // SAFETY: both arrays are initialized at the current slot before
                // `filled` advances, so their initialized prefixes match.
                flush(
                    unsafe { assume_init_prefix(&value_batch, filled) },
                    unsafe { assume_init_prefix(&count_batch, filled) },
                )?;
                filled = 0;
            }
            Ok(())
        },
    )?;
    if filled != 0 {
        // SAFETY: both arrays are initialized at the current slot before
        // `filled` advances, so their initialized prefixes match.
        flush(
            unsafe { assume_init_prefix(&value_batch, filled) },
            unsafe { assume_init_prefix(&count_batch, filled) },
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::ParquetError;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct PanicDefault(u32);

    impl Default for PanicDefault {
        fn default() -> Self {
            panic!("gathering must not initialize unused batch entries")
        }
    }

    #[derive(Clone, Copy)]
    struct SliceProducer<'a, T>(&'a [T]);

    impl<T: Copy> ValueProducer<T> for SliceProducer<'_, T> {
        fn len(self) -> usize {
            self.0.len()
        }

        fn fill(&mut self, out: &mut [MaybeUninit<T>]) -> usize {
            let filled = self.0.len().min(out.len());
            let (head, tail) = self.0.split_at(filled);
            self.0 = tail;
            for (slot, &value) in out.iter_mut().zip(head) {
                slot.write(value);
            }
            filled
        }

        fn try_for_each<E>(self, mut f: impl FnMut(T) -> Result<(), E>) -> Result<(), E> {
            for &value in self.0 {
                f(value)?;
            }
            Ok(())
        }
    }

    fn assert_tiled_batches<const N: usize, T>(batches: &[Vec<T>], expected: &[T])
    where
        T: Copy + std::fmt::Debug + PartialEq,
    {
        let mut actual = Vec::new();
        let mut lengths = Vec::new();
        for batch in batches {
            actual.extend_from_slice(batch);
            lengths.push(batch.len());
        }
        assert_eq!(actual, expected);
        assert_eq!(
            lengths,
            (0..expected.len() / N)
                .map(|_| N)
                .chain((!expected.len().is_multiple_of(N)).then_some(expected.len() % N))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn gather_tiled_initializes_only_emitted_values() {
        for len in [0usize, 3, 4, 5, 8, 9] {
            let input: Vec<_> = (0..len).map(|value| PanicDefault(value as u32)).collect();
            let mut batches = Vec::new();
            gather_tiled::<4, _, _, _>(SliceProducer(&input), |batch| {
                batches.push(batch.to_vec());
                Ok(())
            })
            .unwrap();

            assert_tiled_batches::<4, _>(&batches, &input);
        }

        #[cfg(feature = "arrow")]
        {
            use crate::column::value_selection::ValueSelectionRef;
            let input = [1_usize, 2, 3];
            let selection = PhysicalValueSelection::identity(ValueSelectionRef::Sparse(&input));
            let mapped = map_values(selection, |value| (value * 2) as u32);
            assert_eq!(mapped.len(), input.len());
            let mut batches = Vec::new();
            gather_tiled::<2, _, _, _>(mapped, |batch| {
                batches.push(batch.to_vec());
                Ok(())
            })
            .unwrap();
            assert_tiled_batches::<2, _>(&batches, &[2, 4, 6]);
        }

        let error = gather_tiled::<2, _, _, _>(SliceProducer(&[1_u32, 2, 3]), |_| {
            Err(ParquetError::General("stop".to_string()))
        })
        .unwrap_err();
        assert_eq!(error.to_string(), "Parquet error: stop");
    }

    #[derive(Clone, Copy)]
    struct RunGroupProducer<'a> {
        values: &'a [&'a [u8]],
        counts: &'a [usize],
        consumed: usize,
    }

    impl<'a> ValueProducer<&'a [u8]> for RunGroupProducer<'a> {
        fn len(self) -> usize {
            self.counts.iter().sum()
        }

        fn fill(&mut self, out: &mut [MaybeUninit<&'a [u8]>]) -> usize {
            let mut filled = 0;
            while filled != out.len() && !self.counts.is_empty() {
                let taken = (self.counts[0] - self.consumed).min(out.len() - filled);
                out[filled..filled + taken].fill(MaybeUninit::new(self.values[0]));
                filled += taken;
                self.consumed += taken;
                if self.consumed == self.counts[0] {
                    self.values = &self.values[1..];
                    self.counts = &self.counts[1..];
                    self.consumed = 0;
                }
            }
            filled
        }

        fn try_for_each<E>(self, mut f: impl FnMut(&'a [u8]) -> Result<(), E>) -> Result<(), E> {
            for (&value, &count) in self.values.iter().zip(self.counts) {
                for _ in 0..count {
                    f(value)?;
                }
            }
            Ok(())
        }

        fn for_each_run_group<E>(
            self,
            mut f: impl FnMut(&'a [u8], usize) -> Result<(), E>,
        ) -> Result<(), E> {
            for (&value, &count) in self.values.iter().zip(self.counts) {
                f(value, count)?;
            }
            Ok(())
        }
    }

    #[test]
    fn gather_run_groups_tiled_initializes_matching_prefixes() {
        for len in [0usize, 3, 4, 5, 8, 9] {
            let storage: Vec<_> = (0..len).map(|value| vec![value as u8]).collect();
            let values: Vec<&[u8]> = storage.iter().map(Vec::as_slice).collect();
            let counts: Vec<_> = (1..=values.len()).collect();
            let mut value_batches = Vec::new();
            let mut count_batches = Vec::new();

            gather_run_groups_tiled::<4, _, _>(
                RunGroupProducer {
                    values: &values,
                    counts: &counts,
                    consumed: 0,
                },
                |batch_values, batch_counts| {
                    assert_eq!(batch_values.len(), batch_counts.len());
                    value_batches.push(batch_values.to_vec());
                    count_batches.push(batch_counts.to_vec());
                    Ok(())
                },
            )
            .unwrap();

            assert_tiled_batches::<4, _>(&value_batches, &values);
            assert_tiled_batches::<4, _>(&count_batches, &counts);
        }

        let input = [1_u32, 2, 3, 4, 5];
        let mut values = Vec::new();
        let mut counts = Vec::new();
        gather_run_groups_tiled::<4, _, _>(SliceProducer(&input), |batch, batch_counts| {
            values.extend_from_slice(batch);
            counts.extend_from_slice(batch_counts);
            Ok(())
        })
        .unwrap();
        assert_eq!(values, input);
        assert_eq!(counts, [1; 5]);

        let error = gather_run_groups_tiled::<2, _, _>(SliceProducer(&input), |_, _| {
            Err(ParquetError::General("stop".to_string()))
        })
        .unwrap_err();
        assert_eq!(error.to_string(), "Parquet error: stop");
    }
}
