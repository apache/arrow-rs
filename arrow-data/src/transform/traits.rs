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

use super::{ArrayData, ArrayDataBuilder, MutableArrayData};
use std::fmt::Debug;

// TODO - ADD COMMENT and update comments
pub trait SpecializedMutableArrayData<'a>: Debug {
    /// Returns a new [MutableArrayData] with capacity to `capacity` slots and
    /// specialized to create an [ArrayData] from multiple `arrays`.
    ///
    /// # Arguments
    /// * `arrays` - the source arrays to copy from
    /// * `use_nulls` - a flag used to optimize insertions
    ///   - `false` if the only source of nulls are the arrays themselves
    ///   - `true` if the user plans to call [MutableArrayData::extend_nulls].
    /// * capacity - the preallocated capacity of the output array, in bytes
    ///
    /// Thus, if `use_nulls` is `false`, calling
    /// [MutableArrayData::extend_nulls] should not be used.
    fn new(arrays: Vec<&'a ArrayData>, use_nulls: bool, capacity: usize) -> Self;

    /// Extends the in progress array with a region of the input arrays
    ///
    /// # Arguments
    /// * `index` - the index of array that you what to copy values from
    /// * `start` - the start index of the chunk (inclusive)
    /// * `end` - the end index of the chunk (exclusive)
    ///
    /// # Panic
    /// This function panics if there is an invalid index,
    /// i.e. `index` >= the number of source arrays
    /// or `end` > the length of the `index`th array
    fn extend(&mut self, index: usize, start: usize, end: usize);

    /// Extends the in progress array with null elements, ignoring the input arrays.
    ///
    /// # Panics
    ///
    /// Panics if [`MutableArrayData`] not created with `use_nulls` or nullable source arrays
    fn extend_nulls(&mut self, len: usize);

    /// Returns the current length
    fn len(&self) -> usize;

    /// Returns true if len is 0
    fn is_empty(&self) -> bool;

    /// Returns the current null count
    fn null_count(&self) -> usize;

    /// Creates a [ArrayData] from the in progress array, consuming `self`.
    fn freeze(self) -> ArrayData {
        unsafe { self.into_builder().build_unchecked() }
    }

    /// Consume self and returns the in progress array as [`ArrayDataBuilder`].
    ///
    /// This is useful for extending the default behavior of MutableArrayData.
    fn into_builder(self) -> ArrayDataBuilder;
}
