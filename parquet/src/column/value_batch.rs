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
    /// Exact number of logical values [`Self::try_for_each`] emits.
    fn len(self) -> usize;

    /// Emit values in output order.
    fn try_for_each<E>(self, f: impl FnMut(T) -> Result<(), E>) -> Result<(), E>;
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
    let mut batch = [MaybeUninit::<T>::uninit(); N];
    let mut filled = 0;
    values.try_for_each(
        #[inline(always)]
        |value| -> Result<()> {
            batch[filled].write(value);
            filled += 1;
            if filled == N {
                // SAFETY: this loop initialized every slot before `filled`
                // reached `N`.
                flush(unsafe { assume_init_prefix(&batch, filled) })?;
                filled = 0;
            }
            Ok(())
        },
    )?;
    if filled != 0 {
        // SAFETY: this loop initializes slots sequentially and `filled` is the
        // length of that initialized prefix.
        flush(unsafe { assume_init_prefix(&batch, filled) })?;
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

        let error = gather_tiled::<2, _, _, _>(SliceProducer(&[1_u32, 2, 3]), |_| {
            Err(ParquetError::General("stop".to_string()))
        })
        .unwrap_err();
        assert_eq!(error.to_string(), "Parquet error: stop");
    }
}
