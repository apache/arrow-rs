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

use crate::ArrowNativeType;
use crate::buffer::ScalarBuffer;

/// A buffer of monotonically increasing, positive integers used to store run-ends.
///
/// Used to compactly represent runs of the same value. Values being represented
/// are stored in a separate buffer from this struct. See [`RunArray`] for an example
/// of how this is used with a companion array to represent the values.
///
/// # Logical vs Physical
///
/// Physically, each value in the `run_ends` buffer is the cumulative length of
/// all runs in the logical representation, up to that physical index. Consider
/// the following example:
///
/// ```text
///           physical                        logical
///     ┌─────────┬─────────┐           ┌─────────┬─────────┐
///     │    3    │    0    │ ◄──────┬─ │    A    │    0    │
///     ├─────────┼─────────┤        │  ├─────────┼─────────┤
///     │    4    │    1    │ ◄────┐ ├─ │    A    │    1    │
///     ├─────────┼─────────┤      │ │  ├─────────┼─────────┤
///     │    6    │    2    │ ◄──┐ │ └─ │    A    │    2    │
///     └─────────┴─────────┘    │ │    ├─────────┼─────────┤
///      run-ends    index       │ └─── │    B    │    3    │
///                              │      ├─────────┼─────────┤
///      logical_offset = 0      ├───── │    C    │    4    │
///      logical_length = 6      │      ├─────────┼─────────┤
///                              └───── │    C    │    5    │
///                                     └─────────┴─────────┘
///                                       values     index
/// ```
///
/// A [`RunEndBuffer`] is physically the buffer and offset with length on the left.
/// In this case, the offset and length represent the whole buffer, so it is essentially
/// unsliced. See the section below on slicing for more details on how this buffer
/// handles slicing.
///
/// This means that multiple logical values are represented in the same physical index,
/// and multiple logical indices map to the same physical index. The [`RunEndBuffer`]
/// containing `[3, 4, 6]` is essentially the physical indices `[0, 0, 0, 1, 2, 2]`,
/// and having a separately stored buffer of values such as `[A, B, C]` can turn
/// this into a representation of `[A, A, A, B, C, C]`.
///
/// # Slicing
///
/// In order to provide zero-copy slicing, this struct stores a separate **logical**
/// offset and length. Consider the following example:
///
/// ```text
///           physical                        logical
///     ┌─────────┬─────────┐           ┌ ─ ─ ─ ─ ┬ ─ ─ ─ ─ ┐
///     │    3    │    0    │ ◄──────┐       A         0
///     ├─────────┼─────────┤        │  ├── ─ ─ ─ ┼ ─ ─ ─ ─ ┤
///     │    4    │    1    │ ◄────┐ │       A         1
///     ├─────────┼─────────┤      │ │  ├─────────┼─────────┤
///     │    6    │    2    │ ◄──┐ │ └─ │    A    │    2    │◄─── logical_offset
///     └─────────┴─────────┘    │ │    ├─────────┼─────────┤
///      run-ends    index       │ └─── │    B    │    3    │
///                              │      ├─────────┼─────────┤
///      logical_offset = 2      └───── │    C    │    4    │
///      logical_length = 3             ├─────────┼─────────┤
///                                          C         5     ◄─── logical_offset + logical_length
///                                     └ ─ ─ ─ ─ ┴ ─ ─ ─ ─ ┘
///                                       values     index
/// ```
///
/// The physical `run_ends` [`ScalarBuffer`] remains unchanged, in order to facilitate
/// zero-copy. However, we now offset into the **logical** representation with an
/// accompanying length. This allows us to represent values `[A, B, C]` using physical
/// indices `0, 1, 2` with the same underlying physical buffer, at the cost of two
/// extra `usize`s to represent the logical slice that was taken.
///
/// (A [`RunEndBuffer`] is considered unsliced when `logical_offset` is `0` and
/// `logical_length` is equal to the last value in `run_ends`)
///
/// [`RunArray`]: https://docs.rs/arrow/latest/arrow/array/struct.RunArray.html
/// [Run-End encoded layout]: https://arrow.apache.org/docs/format/Columnar.html#run-end-encoded-layout
#[derive(Debug, Clone)]
pub struct RunEndBuffer<E: ArrowNativeType> {
    run_ends: ScalarBuffer<E>,
    logical_length: usize,
    logical_offset: usize,
}

impl<E> RunEndBuffer<E>
where
    E: ArrowNativeType,
{
    /// Create a new [`RunEndBuffer`] from a [`ScalarBuffer`], `logical_offset`
    /// and `logical_length`.
    ///
    /// # Panics
    ///
    /// - `run_ends` does not contain strictly increasing values greater than zero
    /// - The last value of `run_ends` is less than `logical_offset + logical_length`
    pub fn new(run_ends: ScalarBuffer<E>, logical_offset: usize, logical_length: usize) -> Self {
        assert!(
            run_ends.windows(2).all(|w| w[0] < w[1]),
            "run-ends not strictly increasing"
        );

        if logical_length != 0 {
            assert!(!run_ends.is_empty(), "non-empty slice but empty run-ends");
            let end = E::from_usize(logical_offset.saturating_add(logical_length)).unwrap();
            assert!(
                *run_ends.first().unwrap() > E::usize_as(0),
                "run-ends not greater than 0"
            );
            assert!(
                *run_ends.last().unwrap() >= end,
                "slice beyond bounds of run-ends"
            );
        }

        Self {
            run_ends,
            logical_offset,
            logical_length,
        }
    }

    /// Create a new [`RunEndBuffer`] from a [`ScalarBuffer`], `logical_offset`
    /// and `logical_length`.
    ///
    /// # Safety
    ///
    /// - `run_ends` must contain strictly increasing values greater than zero
    /// - The last value of `run_ends` must be greater than or equal to `logical_offset + logical_len`
    pub unsafe fn new_unchecked(
        run_ends: ScalarBuffer<E>,
        logical_offset: usize,
        logical_length: usize,
    ) -> Self {
        Self {
            run_ends,
            logical_offset,
            logical_length,
        }
    }

    /// Returns the logical offset into the run-ends stored by this buffer.
    #[inline]
    pub fn offset(&self) -> usize {
        self.logical_offset
    }

    /// Returns the logical length of the run-ends stored by this buffer.
    #[inline]
    pub fn len(&self) -> usize {
        self.logical_length
    }

    /// Returns true if this buffer is logically empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.logical_length == 0
    }

    /// Free up unused memory.
    pub fn shrink_to_fit(&mut self) {
        // TODO(emilk): we could shrink even more in the case where we are a small sub-slice of the full buffer
        self.run_ends.shrink_to_fit();
    }

    /// Returns the physical (**unsliced**) run ends of this buffer.
    ///
    /// Take care when operating on these values as it doesn't take into account
    /// any logical slicing that may have occurred.
    #[inline]
    pub fn values(&self) -> &[E] {
        &self.run_ends
    }

    /// Returns the maximum run-end encoded in the underlying buffer; that is, the
    /// last physical run of the buffer. This does not take into account any logical
    /// slicing that may have occurred.
    #[inline]
    pub fn max_value(&self) -> usize {
        self.values().last().copied().unwrap_or_default().as_usize()
    }

    /// Performs a binary search to find the physical index for the given logical
    /// index.
    ///
    /// Useful for extracting the corresponding physical `run_ends` when this buffer
    /// is logically sliced.
    ///
    /// The result is arbitrary if `logical_index >= self.len()`.
    pub fn get_physical_index(&self, logical_index: usize) -> usize {
        let logical_index = E::usize_as(self.logical_offset + logical_index);
        let cmp = |p: &E| p.partial_cmp(&logical_index).unwrap();

        match self.run_ends.binary_search_by(cmp) {
            Ok(idx) => idx + 1,
            Err(idx) => idx,
        }
    }

    /// Returns the physical index at which the logical array starts.
    ///
    /// The same as calling `get_physical_index(0)` but with a fast path if the
    /// buffer is not logically sliced, in which case it always returns `0`.
    pub fn get_start_physical_index(&self) -> usize {
        if self.logical_offset == 0 || self.logical_length == 0 {
            return 0;
        }
        // Fallback to binary search
        self.get_physical_index(0)
    }

    /// Returns the physical index at which the logical array ends.
    ///
    /// The same as calling `get_physical_index(length - 1)` but with a fast path
    /// if the buffer is not logically sliced, in which case it returns `length - 1`.
    pub fn get_end_physical_index(&self) -> usize {
        if self.logical_length == 0 {
            return 0;
        }
        if self.max_value() == self.logical_offset + self.logical_length {
            return self.values().len() - 1;
        }
        // Fallback to binary search
        self.get_physical_index(self.logical_length - 1)
    }

    /// Slices this [`RunEndBuffer`] by the provided `logical_offset` and `logical_length`.
    ///
    /// # Panics
    ///
    /// - Specified slice (`logical_offset` + `logical_length`) exceeds existing
    ///   logical length
    pub fn slice(&self, logical_offset: usize, logical_length: usize) -> Self {
        assert!(
            logical_offset.saturating_add(logical_length) <= self.logical_length,
            "the length + offset of the sliced RunEndBuffer cannot exceed the existing length"
        );
        Self {
            run_ends: self.run_ends.clone(),
            logical_offset: self.logical_offset + logical_offset,
            logical_length,
        }
    }

    /// Returns the inner [`ScalarBuffer`].
    pub fn inner(&self) -> &ScalarBuffer<E> {
        &self.run_ends
    }

    /// Returns the inner [`ScalarBuffer`], consuming self.
    pub fn into_inner(self) -> ScalarBuffer<E> {
        self.run_ends
    }

    /// Returns the physical indices corresponding to the provided logical indices.
    ///
    /// Given a slice of logical indices, this method returns a `Vec` containing the
    /// corresponding physical indices into the run-ends buffer.
    ///
    /// This method operates by iterating the logical indices in sorted order, instead of
    /// finding the physical index for each logical index using binary search via
    /// the function [`RunEndBuffer::get_physical_index`].
    ///
    /// Running benchmarks on both approaches showed that the approach used here
    /// scaled well for larger inputs.
    ///
    /// See <https://github.com/apache/arrow-rs/pull/3622#issuecomment-1407753727> for more details.
    ///
    /// # Errors
    ///
    /// If any logical index is out of bounds (>= self.len()), returns an error containing the invalid index.
    #[inline]
    pub fn get_physical_indices<I>(&self, logical_indices: &[I]) -> Result<Vec<usize>, I>
    where
        I: ArrowNativeType,
    {
        let len = self.len();
        let offset = self.offset();

        let indices_len = logical_indices.len();

        if indices_len == 0 {
            return Ok(vec![]);
        }

        // `ordered_indices` store index into `logical_indices` and can be used
        // to iterate `logical_indices` in sorted order.
        let mut ordered_indices: Vec<usize> = (0..indices_len).collect();

        // Instead of sorting `logical_indices` directly, sort the `ordered_indices`
        // whose values are index of `logical_indices`
        ordered_indices.sort_unstable_by(|lhs, rhs| {
            logical_indices[*lhs]
                .partial_cmp(&logical_indices[*rhs])
                .unwrap()
        });

        // Return early if all the logical indices cannot be converted to physical indices.
        let largest_logical_index = logical_indices[*ordered_indices.last().unwrap()].as_usize();
        if largest_logical_index >= len {
            return Err(logical_indices[*ordered_indices.last().unwrap()]);
        }

        // Skip some physical indices based on offset.
        let skip_value = self.get_start_physical_index();

        let mut physical_indices = vec![0; indices_len];

        let mut ordered_index = 0_usize;
        for (physical_index, run_end) in self.values().iter().enumerate().skip(skip_value) {
            // Get the run end index (relative to offset) of current physical index
            let run_end_value = run_end.as_usize() - offset;

            // All the `logical_indices` that are less than current run end index
            // belongs to current physical index.
            while ordered_index < indices_len
                && logical_indices[ordered_indices[ordered_index]].as_usize() < run_end_value
            {
                physical_indices[ordered_indices[ordered_index]] = physical_index;
                ordered_index += 1;
            }
        }

        // If there are input values >= run_ends.last_value then we'll not be able to convert
        // all logical indices to physical indices.
        if ordered_index < logical_indices.len() {
            return Err(logical_indices[ordered_indices[ordered_index]]);
        }
        Ok(physical_indices)
    }
}

#[cfg(test)]
mod tests {
    use crate::buffer::RunEndBuffer;

    #[test]
    fn test_zero_length_slice() {
        let buffer = RunEndBuffer::new(vec![1_i32, 4_i32].into(), 0, 4);
        assert_eq!(buffer.get_start_physical_index(), 0);
        assert_eq!(buffer.get_end_physical_index(), 1);
        assert_eq!(buffer.get_physical_index(3), 1);

        for offset in 0..4 {
            let sliced = buffer.slice(offset, 0);
            assert_eq!(sliced.get_start_physical_index(), 0);
            assert_eq!(sliced.get_end_physical_index(), 0);
        }

        let buffer = RunEndBuffer::new(Vec::<i32>::new().into(), 0, 0);
        assert_eq!(buffer.get_start_physical_index(), 0);
        assert_eq!(buffer.get_end_physical_index(), 0);
    }
}
