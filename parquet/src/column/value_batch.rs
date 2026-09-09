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
}

/// Projects a physical value selection through a copyable mapping.
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
}
