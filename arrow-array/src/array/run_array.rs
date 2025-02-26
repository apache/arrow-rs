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

use std::any::Any;
use std::sync::Arc;

use arrow_buffer::{ArrowNativeType, BooleanBufferBuilder, NullBuffer, RunEndBuffer};
use arrow_data::{ArrayData, ArrayDataBuilder};
use arrow_schema::{ArrowError, DataType, Field};

use crate::{
    builder::StringRunBuilder,
    make_array,
    run_iterator::RunArrayIter,
    types::{Int16Type, Int32Type, Int64Type, RunEndIndexType},
    Array, ArrayAccessor, ArrayRef, PrimitiveArray,
};

/// An array of [run-end encoded values](https://arrow.apache.org/docs/format/Columnar.html#run-end-encoded-layout)
///
/// This encoding is variation on [run-length encoding (RLE)](https://en.wikipedia.org/wiki/Run-length_encoding)
/// and is good for representing data containing same values repeated consecutively.
///
/// [`RunArray`] contains `run_ends` array and `values` array of same length.
/// The `run_ends` array stores the indexes at which the run ends. The `values` array
/// stores the value of each run. Below example illustrates how a logical array is represented in
/// [`RunArray`]
///
///
/// ```text
/// ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─┐
///   ┌─────────────────┐  ┌─────────┐       ┌─────────────────┐
/// │ │        A        │  │    2    │ │     │        A        │
///   ├─────────────────┤  ├─────────┤       ├─────────────────┤
/// │ │        D        │  │    3    │ │     │        A        │    run length of 'A' = runs_ends[0] - 0 = 2
///   ├─────────────────┤  ├─────────┤       ├─────────────────┤
/// │ │        B        │  │    6    │ │     │        D        │    run length of 'D' = run_ends[1] - run_ends[0] = 1
///   └─────────────────┘  └─────────┘       ├─────────────────┤
/// │        values          run_ends  │     │        B        │
///                                          ├─────────────────┤
/// └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─┘     │        B        │
///                                          ├─────────────────┤
///                RunArray                  │        B        │    run length of 'B' = run_ends[2] - run_ends[1] = 3
///               length = 3                 └─────────────────┘
///
///                                             Logical array
///                                                Contents
/// ```
pub struct RunArray<R: RunEndIndexType> {
    data_type: DataType,
    run_ends: RunEndBuffer<R::Native>,
    values: ArrayRef,
}

impl<R: RunEndIndexType> Clone for RunArray<R> {
    fn clone(&self) -> Self {
        Self {
            data_type: self.data_type.clone(),
            run_ends: self.run_ends.clone(),
            values: self.values.clone(),
        }
    }
}

impl<R: RunEndIndexType> RunArray<R> {
    /// Calculates the logical length of the array encoded
    /// by the given run_ends array.
    pub fn logical_len(run_ends: &PrimitiveArray<R>) -> usize {
        let len = run_ends.len();
        if len == 0 {
            return 0;
        }
        run_ends.value(len - 1).as_usize()
    }

    /// Attempts to create RunArray using given run_ends (index where a run ends)
    /// and the values (value of the run). Returns an error if the given data is not compatible
    /// with RunEndEncoded specification.
    pub fn try_new(run_ends: &PrimitiveArray<R>, values: &dyn Array) -> Result<Self, ArrowError> {
        let run_ends_type = run_ends.data_type().clone();
        let values_type = values.data_type().clone();
        let ree_array_type = DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", run_ends_type, false)),
            Arc::new(Field::new("values", values_type, true)),
        );
        let len = RunArray::logical_len(run_ends);
        let builder = ArrayDataBuilder::new(ree_array_type)
            .len(len)
            .add_child_data(run_ends.to_data())
            .add_child_data(values.to_data());

        // `build_unchecked` is used to avoid recursive validation of child arrays.
        let array_data = unsafe { builder.build_unchecked() };

        // Safety: `validate_data` checks below
        //    1. The given array data has exactly two child arrays.
        //    2. The first child array (run_ends) has valid data type.
        //    3. run_ends array does not have null values
        //    4. run_ends array has non-zero and strictly increasing values.
        //    5. The length of run_ends array and values array are the same.
        array_data.validate_data()?;

        Ok(array_data.into())
    }

    /// Returns a reference to [`RunEndBuffer`]
    pub fn run_ends(&self) -> &RunEndBuffer<R::Native> {
        &self.run_ends
    }

    /// Returns a reference to values array
    ///
    /// Note: any slicing of this [`RunArray`] array is not applied to the returned array
    /// and must be handled separately
    pub fn values(&self) -> &ArrayRef {
        &self.values
    }

    /// Returns the physical index at which the array slice starts.
    pub fn get_start_physical_index(&self) -> usize {
        self.run_ends.get_start_physical_index()
    }

    /// Returns the physical index at which the array slice ends.
    pub fn get_end_physical_index(&self) -> usize {
        self.run_ends.get_end_physical_index()
    }

    /// Downcast this [`RunArray`] to a [`TypedRunArray`]
    ///
    /// ```
    /// use arrow_array::{Array, ArrayAccessor, RunArray, StringArray, types::Int32Type};
    ///
    /// let orig = [Some("a"), Some("b"), None];
    /// let run_array = RunArray::<Int32Type>::from_iter(orig);
    /// let typed = run_array.downcast::<StringArray>().unwrap();
    /// assert_eq!(typed.value(0), "a");
    /// assert_eq!(typed.value(1), "b");
    /// assert!(typed.values().is_null(2));
    /// ```
    ///
    pub fn downcast<V: 'static>(&self) -> Option<TypedRunArray<'_, R, V>> {
        let values = self.values.as_any().downcast_ref()?;
        Some(TypedRunArray {
            run_array: self,
            values,
        })
    }

    /// Returns index to the physical array for the given index to the logical array.
    /// This function adjusts the input logical index based on `ArrayData::offset`
    /// Performs a binary search on the run_ends array for the input index.
    ///
    /// The result is arbitrary if `logical_index >= self.len()`
    pub fn get_physical_index(&self, logical_index: usize) -> usize {
        self.run_ends.get_physical_index(logical_index)
    }

    /// Returns the physical indices of the input logical indices. Returns error if any of the logical
    /// index cannot be converted to physical index. The logical indices are sorted and iterated along
    /// with run_ends array to find matching physical index. The approach used here was chosen over
    /// finding physical index for each logical index using binary search using the function
    /// `get_physical_index`. Running benchmarks on both approaches showed that the approach used here
    /// scaled well for larger inputs.
    /// See <https://github.com/apache/arrow-rs/pull/3622#issuecomment-1407753727> for more details.
    #[inline]
    pub fn get_physical_indices<I>(&self, logical_indices: &[I]) -> Result<Vec<usize>, ArrowError>
    where
        I: ArrowNativeType,
    {
        let len = self.run_ends().len();
        let offset = self.run_ends().offset();

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
            return Err(ArrowError::InvalidArgumentError(format!(
                "Cannot convert all logical indices to physical indices. The logical index cannot be converted is {largest_logical_index}.",
            )));
        }

        // Skip some physical indices based on offset.
        let skip_value = self.get_start_physical_index();

        let mut physical_indices = vec![0; indices_len];

        let mut ordered_index = 0_usize;
        for (physical_index, run_end) in self.run_ends.values().iter().enumerate().skip(skip_value)
        {
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
            let logical_index = logical_indices[ordered_indices[ordered_index]].as_usize();
            return Err(ArrowError::InvalidArgumentError(format!(
                "Cannot convert all logical indices to physical indices. The logical index cannot be converted is {logical_index}.",
            )));
        }
        Ok(physical_indices)
    }

    /// Returns a zero-copy slice of this array with the indicated offset and length.
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        Self {
            data_type: self.data_type.clone(),
            run_ends: self.run_ends.slice(offset, length),
            values: self.values.clone(),
        }
    }
}

impl<R: RunEndIndexType> From<ArrayData> for RunArray<R> {
    // The method assumes the caller already validated the data using `ArrayData::validate_data()`
    fn from(data: ArrayData) -> Self {
        match data.data_type() {
            DataType::RunEndEncoded(_, _) => {}
            _ => {
                panic!("Invalid data type for RunArray. The data type should be DataType::RunEndEncoded");
            }
        }

        // Safety
        // ArrayData is valid
        let child = &data.child_data()[0];
        assert_eq!(child.data_type(), &R::DATA_TYPE, "Incorrect run ends type");
        let run_ends = unsafe {
            let scalar = child.buffers()[0].clone().into();
            RunEndBuffer::new_unchecked(scalar, data.offset(), data.len())
        };

        let values = make_array(data.child_data()[1].clone());
        Self {
            data_type: data.data_type().clone(),
            run_ends,
            values,
        }
    }
}

impl<R: RunEndIndexType> From<RunArray<R>> for ArrayData {
    fn from(array: RunArray<R>) -> Self {
        let len = array.run_ends.len();
        let offset = array.run_ends.offset();

        let run_ends = ArrayDataBuilder::new(R::DATA_TYPE)
            .len(array.run_ends.values().len())
            .buffers(vec![array.run_ends.into_inner().into_inner()]);

        let run_ends = unsafe { run_ends.build_unchecked() };

        let builder = ArrayDataBuilder::new(array.data_type)
            .len(len)
            .offset(offset)
            .child_data(vec![run_ends, array.values.to_data()]);

        unsafe { builder.build_unchecked() }
    }
}

impl<T: RunEndIndexType> Array for RunArray<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn to_data(&self) -> ArrayData {
        self.clone().into()
    }

    fn into_data(self) -> ArrayData {
        self.into()
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        Arc::new(self.slice(offset, length))
    }

    fn len(&self) -> usize {
        self.run_ends.len()
    }

    fn is_empty(&self) -> bool {
        self.run_ends.is_empty()
    }

    fn shrink_to_fit(&mut self) {
        self.run_ends.shrink_to_fit();
        self.values.shrink_to_fit();
    }

    fn offset(&self) -> usize {
        self.run_ends.offset()
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        None
    }

    fn logical_nulls(&self) -> Option<NullBuffer> {
        let len = self.len();
        let nulls = self.values.logical_nulls()?;
        let mut out = BooleanBufferBuilder::new(len);
        let offset = self.run_ends.offset();
        let mut valid_start = 0;
        let mut last_end = 0;
        for (idx, end) in self.run_ends.values().iter().enumerate() {
            let end = end.as_usize();
            if end < offset {
                continue;
            }
            let end = (end - offset).min(len);
            if nulls.is_null(idx) {
                if valid_start < last_end {
                    out.append_n(last_end - valid_start, true);
                }
                out.append_n(end - last_end, false);
                valid_start = end;
            }
            last_end = end;
            if end == len {
                break;
            }
        }
        if valid_start < len {
            out.append_n(len - valid_start, true)
        }
        // Sanity check
        assert_eq!(out.len(), len);
        Some(out.finish().into())
    }

    fn is_nullable(&self) -> bool {
        !self.is_empty() && self.values.is_nullable()
    }

    fn get_buffer_memory_size(&self) -> usize {
        self.run_ends.inner().inner().capacity() + self.values.get_buffer_memory_size()
    }

    fn get_array_memory_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.run_ends.inner().inner().capacity()
            + self.values.get_array_memory_size()
    }
}

impl<R: RunEndIndexType> std::fmt::Debug for RunArray<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        writeln!(
            f,
            "RunArray {{run_ends: {:?}, values: {:?}}}",
            self.run_ends.values(),
            self.values
        )
    }
}

/// Constructs a `RunArray` from an iterator of optional strings.
///
/// # Example:
/// ```
/// use arrow_array::{RunArray, PrimitiveArray, StringArray, types::Int16Type};
///
/// let test = vec!["a", "a", "b", "c", "c"];
/// let array: RunArray<Int16Type> = test
///     .iter()
///     .map(|&x| if x == "b" { None } else { Some(x) })
///     .collect();
/// assert_eq!(
///     "RunArray {run_ends: [2, 3, 5], values: StringArray\n[\n  \"a\",\n  null,\n  \"c\",\n]}\n",
///     format!("{:?}", array)
/// );
/// ```
impl<'a, T: RunEndIndexType> FromIterator<Option<&'a str>> for RunArray<T> {
    fn from_iter<I: IntoIterator<Item = Option<&'a str>>>(iter: I) -> Self {
        let it = iter.into_iter();
        let (lower, _) = it.size_hint();
        let mut builder = StringRunBuilder::with_capacity(lower, 256);
        it.for_each(|i| {
            builder.append_option(i);
        });

        builder.finish()
    }
}

/// Constructs a `RunArray` from an iterator of strings.
///
/// # Example:
///
/// ```
/// use arrow_array::{RunArray, PrimitiveArray, StringArray, types::Int16Type};
///
/// let test = vec!["a", "a", "b", "c"];
/// let array: RunArray<Int16Type> = test.into_iter().collect();
/// assert_eq!(
///     "RunArray {run_ends: [2, 3, 4], values: StringArray\n[\n  \"a\",\n  \"b\",\n  \"c\",\n]}\n",
///     format!("{:?}", array)
/// );
/// ```
impl<'a, T: RunEndIndexType> FromIterator<&'a str> for RunArray<T> {
    fn from_iter<I: IntoIterator<Item = &'a str>>(iter: I) -> Self {
        let it = iter.into_iter();
        let (lower, _) = it.size_hint();
        let mut builder = StringRunBuilder::with_capacity(lower, 256);
        it.for_each(|i| {
            builder.append_value(i);
        });

        builder.finish()
    }
}

///
/// A [`RunArray`] with `i16` run ends
///
/// # Example: Using `collect`
/// ```
/// # use arrow_array::{Array, Int16RunArray, Int16Array, StringArray};
/// # use std::sync::Arc;
///
/// let array: Int16RunArray = vec!["a", "a", "b", "c", "c"].into_iter().collect();
/// let values: Arc<dyn Array> = Arc::new(StringArray::from(vec!["a", "b", "c"]));
/// assert_eq!(array.run_ends().values(), &[2, 3, 5]);
/// assert_eq!(array.values(), &values);
/// ```
pub type Int16RunArray = RunArray<Int16Type>;

///
/// A [`RunArray`] with `i32` run ends
///
/// # Example: Using `collect`
/// ```
/// # use arrow_array::{Array, Int32RunArray, Int32Array, StringArray};
/// # use std::sync::Arc;
///
/// let array: Int32RunArray = vec!["a", "a", "b", "c", "c"].into_iter().collect();
/// let values: Arc<dyn Array> = Arc::new(StringArray::from(vec!["a", "b", "c"]));
/// assert_eq!(array.run_ends().values(), &[2, 3, 5]);
/// assert_eq!(array.values(), &values);
/// ```
pub type Int32RunArray = RunArray<Int32Type>;

///
/// A [`RunArray`] with `i64` run ends
///
/// # Example: Using `collect`
/// ```
/// # use arrow_array::{Array, Int64RunArray, Int64Array, StringArray};
/// # use std::sync::Arc;
///
/// let array: Int64RunArray = vec!["a", "a", "b", "c", "c"].into_iter().collect();
/// let values: Arc<dyn Array> = Arc::new(StringArray::from(vec!["a", "b", "c"]));
/// assert_eq!(array.run_ends().values(), &[2, 3, 5]);
/// assert_eq!(array.values(), &values);
/// ```
pub type Int64RunArray = RunArray<Int64Type>;

/// A [`RunArray`] typed typed on its child values array
///
/// Implements [`ArrayAccessor`] and [`IntoIterator`] allowing fast access to its elements
///
/// ```
/// use arrow_array::{RunArray, StringArray, types::Int32Type};
///
/// let orig = ["a", "b", "a", "b"];
/// let ree_array = RunArray::<Int32Type>::from_iter(orig);
///
/// // `TypedRunArray` allows you to access the values directly
/// let typed = ree_array.downcast::<StringArray>().unwrap();
///
/// for (maybe_val, orig) in typed.into_iter().zip(orig) {
///     assert_eq!(maybe_val.unwrap(), orig)
/// }
/// ```
pub struct TypedRunArray<'a, R: RunEndIndexType, V> {
    /// The run array
    run_array: &'a RunArray<R>,

    /// The values of the run_array
    values: &'a V,
}

// Manually implement `Clone` to avoid `V: Clone` type constraint
impl<R: RunEndIndexType, V> Clone for TypedRunArray<'_, R, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<R: RunEndIndexType, V> Copy for TypedRunArray<'_, R, V> {}

impl<R: RunEndIndexType, V> std::fmt::Debug for TypedRunArray<'_, R, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        writeln!(f, "TypedRunArray({:?})", self.run_array)
    }
}

impl<'a, R: RunEndIndexType, V> TypedRunArray<'a, R, V> {
    /// Returns the run_ends of this [`TypedRunArray`]
    pub fn run_ends(&self) -> &'a RunEndBuffer<R::Native> {
        self.run_array.run_ends()
    }

    /// Returns the values of this [`TypedRunArray`]
    pub fn values(&self) -> &'a V {
        self.values
    }

    /// Returns the run array of this [`TypedRunArray`]
    pub fn run_array(&self) -> &'a RunArray<R> {
        self.run_array
    }
}

impl<R: RunEndIndexType, V: Sync> Array for TypedRunArray<'_, R, V> {
    fn as_any(&self) -> &dyn Any {
        self.run_array
    }

    fn to_data(&self) -> ArrayData {
        self.run_array.to_data()
    }

    fn into_data(self) -> ArrayData {
        self.run_array.into_data()
    }

    fn data_type(&self) -> &DataType {
        self.run_array.data_type()
    }

    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        Arc::new(self.run_array.slice(offset, length))
    }

    fn len(&self) -> usize {
        self.run_array.len()
    }

    fn is_empty(&self) -> bool {
        self.run_array.is_empty()
    }

    fn offset(&self) -> usize {
        self.run_array.offset()
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        self.run_array.nulls()
    }

    fn logical_nulls(&self) -> Option<NullBuffer> {
        self.run_array.logical_nulls()
    }

    fn logical_null_count(&self) -> usize {
        self.run_array.logical_null_count()
    }

    fn is_nullable(&self) -> bool {
        self.run_array.is_nullable()
    }

    fn get_buffer_memory_size(&self) -> usize {
        self.run_array.get_buffer_memory_size()
    }

    fn get_array_memory_size(&self) -> usize {
        self.run_array.get_array_memory_size()
    }
}

// Array accessor converts the index of logical array to the index of the physical array
// using binary search. The time complexity is O(log N) where N is number of runs.
impl<'a, R, V> ArrayAccessor for TypedRunArray<'a, R, V>
where
    R: RunEndIndexType,
    V: Sync + Send,
    &'a V: ArrayAccessor,
    <&'a V as ArrayAccessor>::Item: Default,
{
    type Item = <&'a V as ArrayAccessor>::Item;

    fn value(&self, logical_index: usize) -> Self::Item {
        assert!(
            logical_index < self.len(),
            "Trying to access an element at index {} from a TypedRunArray of length {}",
            logical_index,
            self.len()
        );
        unsafe { self.value_unchecked(logical_index) }
    }

    unsafe fn value_unchecked(&self, logical_index: usize) -> Self::Item {
        let physical_index = self.run_array.get_physical_index(logical_index);
        self.values().value_unchecked(physical_index)
    }
}

impl<'a, R, V> IntoIterator for TypedRunArray<'a, R, V>
where
    R: RunEndIndexType,
    V: Sync + Send,
    &'a V: ArrayAccessor,
    <&'a V as ArrayAccessor>::Item: Default,
{
    type Item = Option<<&'a V as ArrayAccessor>::Item>;
    type IntoIter = RunArrayIter<'a, R, V>;

    fn into_iter(self) -> Self::IntoIter {
        RunArrayIter::new(self)
    }
}

#[cfg(test)]
mod tests {
    use rand::rng;
    use rand::seq::SliceRandom;
    use rand::Rng;

    use super::*;
    use crate::builder::PrimitiveRunBuilder;
    use crate::cast::AsArray;
    use crate::types::{Int8Type, UInt32Type};
    use crate::{Int32Array, StringArray};

    fn build_input_array(size: usize) -> Vec<Option<i32>> {
        // The input array is created by shuffling and repeating
        // the seed values random number of times.
        let mut seed: Vec<Option<i32>> = vec![
            None,
            None,
            None,
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            Some(6),
            Some(7),
            Some(8),
            Some(9),
        ];
        let mut result: Vec<Option<i32>> = Vec::with_capacity(size);
        let mut ix = 0;
        let mut rng = rng();
        // run length can go up to 8. Cap the max run length for smaller arrays to size / 2.
        let max_run_length = 8_usize.min(1_usize.max(size / 2));
        while result.len() < size {
            // shuffle the seed array if all the values are iterated.
            if ix == 0 {
                seed.shuffle(&mut rng);
            }
            // repeat the items between 1 and 8 times. Cap the length for smaller sized arrays
            let num = max_run_length.min(rand::rng().random_range(1..=max_run_length));
            for _ in 0..num {
                result.push(seed[ix]);
            }
            ix += 1;
            if ix == seed.len() {
                ix = 0
            }
        }
        result.resize(size, None);
        result
    }

    // Asserts that `logical_array[logical_indices[*]] == physical_array[physical_indices[*]]`
    fn compare_logical_and_physical_indices(
        logical_indices: &[u32],
        logical_array: &[Option<i32>],
        physical_indices: &[usize],
        physical_array: &PrimitiveArray<Int32Type>,
    ) {
        assert_eq!(logical_indices.len(), physical_indices.len());

        // check value in logical index in the logical_array matches physical index in physical_array
        logical_indices
            .iter()
            .map(|f| f.as_usize())
            .zip(physical_indices.iter())
            .for_each(|(logical_ix, physical_ix)| {
                let expected = logical_array[logical_ix];
                match expected {
                    Some(val) => {
                        assert!(physical_array.is_valid(*physical_ix));
                        let actual = physical_array.value(*physical_ix);
                        assert_eq!(val, actual);
                    }
                    None => {
                        assert!(physical_array.is_null(*physical_ix))
                    }
                };
            });
    }
    #[test]
    fn test_run_array() {
        // Construct a value array
        let value_data =
            PrimitiveArray::<Int8Type>::from_iter_values([10_i8, 11, 12, 13, 14, 15, 16, 17]);

        // Construct a run_ends array:
        let run_ends_values = [4_i16, 6, 7, 9, 13, 18, 20, 22];
        let run_ends_data =
            PrimitiveArray::<Int16Type>::from_iter_values(run_ends_values.iter().copied());

        // Construct a run ends encoded array from the above two
        let ree_array = RunArray::<Int16Type>::try_new(&run_ends_data, &value_data).unwrap();

        assert_eq!(ree_array.len(), 22);
        assert_eq!(ree_array.null_count(), 0);

        let values = ree_array.values();
        assert_eq!(value_data.into_data(), values.to_data());
        assert_eq!(&DataType::Int8, values.data_type());

        let run_ends = ree_array.run_ends();
        assert_eq!(run_ends.values(), &run_ends_values);
    }

    #[test]
    fn test_run_array_fmt_debug() {
        let mut builder = PrimitiveRunBuilder::<Int16Type, UInt32Type>::with_capacity(3);
        builder.append_value(12345678);
        builder.append_null();
        builder.append_value(22345678);
        let array = builder.finish();
        assert_eq!(
            "RunArray {run_ends: [1, 2, 3], values: PrimitiveArray<UInt32>\n[\n  12345678,\n  null,\n  22345678,\n]}\n",
            format!("{array:?}")
        );

        let mut builder = PrimitiveRunBuilder::<Int16Type, UInt32Type>::with_capacity(20);
        for _ in 0..20 {
            builder.append_value(1);
        }
        let array = builder.finish();

        assert_eq!(array.len(), 20);
        assert_eq!(array.null_count(), 0);
        assert_eq!(array.logical_null_count(), 0);

        assert_eq!(
            "RunArray {run_ends: [20], values: PrimitiveArray<UInt32>\n[\n  1,\n]}\n",
            format!("{array:?}")
        );
    }

    #[test]
    fn test_run_array_from_iter() {
        let test = vec!["a", "a", "b", "c"];
        let array: RunArray<Int16Type> = test
            .iter()
            .map(|&x| if x == "b" { None } else { Some(x) })
            .collect();
        assert_eq!(
            "RunArray {run_ends: [2, 3, 4], values: StringArray\n[\n  \"a\",\n  null,\n  \"c\",\n]}\n",
            format!("{array:?}")
        );

        assert_eq!(array.len(), 4);
        assert_eq!(array.null_count(), 0);
        assert_eq!(array.logical_null_count(), 1);

        let array: RunArray<Int16Type> = test.into_iter().collect();
        assert_eq!(
            "RunArray {run_ends: [2, 3, 4], values: StringArray\n[\n  \"a\",\n  \"b\",\n  \"c\",\n]}\n",
            format!("{array:?}")
        );
    }

    #[test]
    fn test_run_array_run_ends_as_primitive_array() {
        let test = vec!["a", "b", "c", "a"];
        let array: RunArray<Int16Type> = test.into_iter().collect();

        assert_eq!(array.len(), 4);
        assert_eq!(array.null_count(), 0);
        assert_eq!(array.logical_null_count(), 0);

        let run_ends = array.run_ends();
        assert_eq!(&[1, 2, 3, 4], run_ends.values());
    }

    #[test]
    fn test_run_array_as_primitive_array_with_null() {
        let test = vec![Some("a"), None, Some("b"), None, None, Some("a")];
        let array: RunArray<Int32Type> = test.into_iter().collect();

        assert_eq!(array.len(), 6);
        assert_eq!(array.null_count(), 0);
        assert_eq!(array.logical_null_count(), 3);

        let run_ends = array.run_ends();
        assert_eq!(&[1, 2, 3, 5, 6], run_ends.values());

        let values_data = array.values();
        assert_eq!(2, values_data.null_count());
        assert_eq!(5, values_data.len());
    }

    #[test]
    fn test_run_array_all_nulls() {
        let test = vec![None, None, None];
        let array: RunArray<Int32Type> = test.into_iter().collect();

        assert_eq!(array.len(), 3);
        assert_eq!(array.null_count(), 0);
        assert_eq!(array.logical_null_count(), 3);

        let run_ends = array.run_ends();
        assert_eq!(3, run_ends.len());
        assert_eq!(&[3], run_ends.values());

        let values_data = array.values();
        assert_eq!(1, values_data.null_count());
    }

    #[test]
    fn test_run_array_try_new() {
        let values: StringArray = [Some("foo"), Some("bar"), None, Some("baz")]
            .into_iter()
            .collect();
        let run_ends: Int32Array = [Some(1), Some(2), Some(3), Some(4)].into_iter().collect();

        let array = RunArray::<Int32Type>::try_new(&run_ends, &values).unwrap();
        assert_eq!(array.values().data_type(), &DataType::Utf8);

        assert_eq!(array.null_count(), 0);
        assert_eq!(array.logical_null_count(), 1);
        assert_eq!(array.len(), 4);
        assert_eq!(array.values().null_count(), 1);

        assert_eq!(
            "RunArray {run_ends: [1, 2, 3, 4], values: StringArray\n[\n  \"foo\",\n  \"bar\",\n  null,\n  \"baz\",\n]}\n",
            format!("{array:?}")
        );
    }

    #[test]
    fn test_run_array_int16_type_definition() {
        let array: Int16RunArray = vec!["a", "a", "b", "c", "c"].into_iter().collect();
        let values: Arc<dyn Array> = Arc::new(StringArray::from(vec!["a", "b", "c"]));
        assert_eq!(array.run_ends().values(), &[2, 3, 5]);
        assert_eq!(array.values(), &values);
    }

    #[test]
    fn test_run_array_empty_string() {
        let array: Int16RunArray = vec!["a", "a", "", "", "c"].into_iter().collect();
        let values: Arc<dyn Array> = Arc::new(StringArray::from(vec!["a", "", "c"]));
        assert_eq!(array.run_ends().values(), &[2, 4, 5]);
        assert_eq!(array.values(), &values);
    }

    #[test]
    fn test_run_array_length_mismatch() {
        let values: StringArray = [Some("foo"), Some("bar"), None, Some("baz")]
            .into_iter()
            .collect();
        let run_ends: Int32Array = [Some(1), Some(2), Some(3)].into_iter().collect();

        let actual = RunArray::<Int32Type>::try_new(&run_ends, &values);
        let expected = ArrowError::InvalidArgumentError("The run_ends array length should be the same as values array length. Run_ends array length is 3, values array length is 4".to_string());
        assert_eq!(expected.to_string(), actual.err().unwrap().to_string());
    }

    #[test]
    fn test_run_array_run_ends_with_null() {
        let values: StringArray = [Some("foo"), Some("bar"), Some("baz")]
            .into_iter()
            .collect();
        let run_ends: Int32Array = [Some(1), None, Some(3)].into_iter().collect();

        let actual = RunArray::<Int32Type>::try_new(&run_ends, &values);
        let expected = ArrowError::InvalidArgumentError(
            "Found null values in run_ends array. The run_ends array should not have null values."
                .to_string(),
        );
        assert_eq!(expected.to_string(), actual.err().unwrap().to_string());
    }

    #[test]
    fn test_run_array_run_ends_with_zeroes() {
        let values: StringArray = [Some("foo"), Some("bar"), Some("baz")]
            .into_iter()
            .collect();
        let run_ends: Int32Array = [Some(0), Some(1), Some(3)].into_iter().collect();

        let actual = RunArray::<Int32Type>::try_new(&run_ends, &values);
        let expected = ArrowError::InvalidArgumentError("The values in run_ends array should be strictly positive. Found value 0 at index 0 that does not match the criteria.".to_string());
        assert_eq!(expected.to_string(), actual.err().unwrap().to_string());
    }

    #[test]
    fn test_run_array_run_ends_non_increasing() {
        let values: StringArray = [Some("foo"), Some("bar"), Some("baz")]
            .into_iter()
            .collect();
        let run_ends: Int32Array = [Some(1), Some(4), Some(4)].into_iter().collect();

        let actual = RunArray::<Int32Type>::try_new(&run_ends, &values);
        let expected = ArrowError::InvalidArgumentError("The values in run_ends array should be strictly increasing. Found value 4 at index 2 with previous value 4 that does not match the criteria.".to_string());
        assert_eq!(expected.to_string(), actual.err().unwrap().to_string());
    }

    #[test]
    #[should_panic(expected = "Incorrect run ends type")]
    fn test_run_array_run_ends_data_type_mismatch() {
        let a = RunArray::<Int32Type>::from_iter(["32"]);
        let _ = RunArray::<Int64Type>::from(a.into_data());
    }

    #[test]
    fn test_ree_array_accessor() {
        let input_array = build_input_array(256);

        // Encode the input_array to ree_array
        let mut builder =
            PrimitiveRunBuilder::<Int16Type, Int32Type>::with_capacity(input_array.len());
        builder.extend(input_array.iter().copied());
        let run_array = builder.finish();
        let typed = run_array.downcast::<PrimitiveArray<Int32Type>>().unwrap();

        // Access every index and check if the value in the input array matches returned value.
        for (i, inp_val) in input_array.iter().enumerate() {
            if let Some(val) = inp_val {
                let actual = typed.value(i);
                assert_eq!(*val, actual)
            } else {
                let physical_ix = run_array.get_physical_index(i);
                assert!(typed.values().is_null(physical_ix));
            };
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
    fn test_get_physical_indices() {
        // Test for logical lengths starting from 10 to 250 increasing by 10
        for logical_len in (0..250).step_by(10) {
            let input_array = build_input_array(logical_len);

            // create run array using input_array
            let mut builder = PrimitiveRunBuilder::<Int32Type, Int32Type>::new();
            builder.extend(input_array.clone().into_iter());

            let run_array = builder.finish();
            let physical_values_array = run_array.values().as_primitive::<Int32Type>();

            // create an array consisting of all the indices repeated twice and shuffled.
            let mut logical_indices: Vec<u32> = (0_u32..(logical_len as u32)).collect();
            // add same indices once more
            logical_indices.append(&mut logical_indices.clone());
            let mut rng = rng();
            logical_indices.shuffle(&mut rng);

            let physical_indices = run_array.get_physical_indices(&logical_indices).unwrap();

            assert_eq!(logical_indices.len(), physical_indices.len());

            // check value in logical index in the input_array matches physical index in typed_run_array
            compare_logical_and_physical_indices(
                &logical_indices,
                &input_array,
                &physical_indices,
                physical_values_array,
            );
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
    fn test_get_physical_indices_sliced() {
        let total_len = 80;
        let input_array = build_input_array(total_len);

        // Encode the input_array to run array
        let mut builder =
            PrimitiveRunBuilder::<Int16Type, Int32Type>::with_capacity(input_array.len());
        builder.extend(input_array.iter().copied());
        let run_array = builder.finish();
        let physical_values_array = run_array.values().as_primitive::<Int32Type>();

        // test for all slice lengths.
        for slice_len in 1..=total_len {
            // create an array consisting of all the indices repeated twice and shuffled.
            let mut logical_indices: Vec<u32> = (0_u32..(slice_len as u32)).collect();
            // add same indices once more
            logical_indices.append(&mut logical_indices.clone());
            let mut rng = rng();
            logical_indices.shuffle(&mut rng);

            // test for offset = 0 and slice length = slice_len
            // slice the input array using which the run array was built.
            let sliced_input_array = &input_array[0..slice_len];

            // slice the run array
            let sliced_run_array: RunArray<Int16Type> =
                run_array.slice(0, slice_len).into_data().into();

            // Get physical indices.
            let physical_indices = sliced_run_array
                .get_physical_indices(&logical_indices)
                .unwrap();

            compare_logical_and_physical_indices(
                &logical_indices,
                sliced_input_array,
                &physical_indices,
                physical_values_array,
            );

            // test for offset = total_len - slice_len and slice length = slice_len
            // slice the input array using which the run array was built.
            let sliced_input_array = &input_array[total_len - slice_len..total_len];

            // slice the run array
            let sliced_run_array: RunArray<Int16Type> = run_array
                .slice(total_len - slice_len, slice_len)
                .into_data()
                .into();

            // Get physical indices
            let physical_indices = sliced_run_array
                .get_physical_indices(&logical_indices)
                .unwrap();

            compare_logical_and_physical_indices(
                &logical_indices,
                sliced_input_array,
                &physical_indices,
                physical_values_array,
            );
        }
    }

    #[test]
    fn test_logical_nulls() {
        let run = Int32Array::from(vec![3, 6, 9, 12]);
        let values = Int32Array::from(vec![Some(0), None, Some(1), None]);
        let array = RunArray::try_new(&run, &values).unwrap();

        let expected = [
            true, true, true, false, false, false, true, true, true, false, false, false,
        ];

        let n = array.logical_nulls().unwrap();
        assert_eq!(n.null_count(), 6);

        let slices = [(0, 12), (0, 2), (2, 5), (3, 0), (3, 3), (3, 4), (4, 8)];
        for (offset, length) in slices {
            let a = array.slice(offset, length);
            let n = a.logical_nulls().unwrap();
            let n = n.into_iter().collect::<Vec<_>>();
            assert_eq!(&n, &expected[offset..offset + length], "{offset} {length}");
        }
    }
}
