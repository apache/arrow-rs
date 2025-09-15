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

use std::{any::Any, sync::Arc};

use crate::{types::RunEndIndexType, ArrayRef, ArrowPrimitiveType, RunArray};

use super::{ArrayBuilder, PrimitiveBuilder};

use arrow_buffer::ArrowNativeType;

/// Builder for [`RunArray`] of [`PrimitiveArray`](crate::array::PrimitiveArray)
///
/// # Example:
///
/// ```
///
/// # use arrow_array::builder::PrimitiveRunBuilder;
/// # use arrow_array::cast::AsArray;
/// # use arrow_array::types::{UInt32Type, Int16Type};
/// # use arrow_array::{Array, UInt32Array, Int16Array};
///
/// let mut builder =
/// PrimitiveRunBuilder::<Int16Type, UInt32Type>::new();
/// builder.append_value(1234);
/// builder.append_value(1234);
/// builder.append_value(1234);
/// builder.append_null();
/// builder.append_value(5678);
/// builder.append_value(5678);
/// let array = builder.finish();
///
/// assert_eq!(array.run_ends().values(), &[3, 4, 6]);
///
/// let av = array.values();
///
/// assert!(!av.is_null(0));
/// assert!(av.is_null(1));
/// assert!(!av.is_null(2));
///
/// // Values are polymorphic and so require a downcast.
/// let ava: &UInt32Array = av.as_primitive::<UInt32Type>();
///
/// assert_eq!(ava, &UInt32Array::from(vec![Some(1234), None, Some(5678)]));
/// ```
#[derive(Debug)]
pub struct PrimitiveRunBuilder<R, V>
where
    R: RunEndIndexType,
    V: ArrowPrimitiveType,
{
    run_ends_builder: PrimitiveBuilder<R>,
    values_builder: PrimitiveBuilder<V>,
    current_value: Option<V::Native>,
    current_run_end_index: usize,
    prev_run_end_index: usize,
}

impl<R, V> Default for PrimitiveRunBuilder<R, V>
where
    R: RunEndIndexType,
    V: ArrowPrimitiveType,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<R, V> PrimitiveRunBuilder<R, V>
where
    R: RunEndIndexType,
    V: ArrowPrimitiveType,
{
    /// Creates a new `PrimitiveRunBuilder`
    pub fn new() -> Self {
        Self {
            run_ends_builder: PrimitiveBuilder::new(),
            values_builder: PrimitiveBuilder::new(),
            current_value: None,
            current_run_end_index: 0,
            prev_run_end_index: 0,
        }
    }

    /// Creates a new `PrimitiveRunBuilder` with the provided capacity
    ///
    /// `capacity`: the expected number of run-end encoded values.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            run_ends_builder: PrimitiveBuilder::with_capacity(capacity),
            values_builder: PrimitiveBuilder::with_capacity(capacity),
            current_value: None,
            current_run_end_index: 0,
            prev_run_end_index: 0,
        }
    }
}

impl<R, V> ArrayBuilder for PrimitiveRunBuilder<R, V>
where
    R: RunEndIndexType,
    V: ArrowPrimitiveType,
{
    /// Returns the builder as a non-mutable `Any` reference.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns the builder as a mutable `Any` reference.
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    /// Returns the boxed builder as a box of `Any`.
    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    /// Returns the length of logical array encoded by
    /// the eventual runs array.
    fn len(&self) -> usize {
        self.current_run_end_index
    }

    /// Builds the array and reset this builder.
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }

    /// Builds the array without resetting the builder.
    fn finish_cloned(&self) -> ArrayRef {
        Arc::new(self.finish_cloned())
    }
}

impl<R, V> PrimitiveRunBuilder<R, V>
where
    R: RunEndIndexType,
    V: ArrowPrimitiveType,
{
    /// Appends optional value to the logical array encoded by the RunArray.
    pub fn append_option(&mut self, value: Option<V::Native>) {
        if self.current_run_end_index == 0 {
            self.current_run_end_index = 1;
            self.current_value = value;
            return;
        }
        if self.current_value != value {
            self.append_run_end();
            self.current_value = value;
        }

        self.current_run_end_index += 1;
    }

    /// Appends value to the logical array encoded by the run-ends array.
    pub fn append_value(&mut self, value: V::Native) {
        self.append_option(Some(value))
    }

    /// Appends null to the logical array encoded by the run-ends array.
    pub fn append_null(&mut self) {
        self.append_option(None)
    }

    /// Creates the RunArray and resets the builder.
    /// Panics if RunArray cannot be built.
    pub fn finish(&mut self) -> RunArray<R> {
        // write the last run end to the array.
        self.append_run_end();

        // reset the run index to zero.
        self.current_value = None;
        self.current_run_end_index = 0;

        // build the run encoded array by adding run_ends and values array as its children.
        let run_ends_array = self.run_ends_builder.finish();
        let values_array = self.values_builder.finish();
        RunArray::<R>::try_new(&run_ends_array, &values_array).unwrap()
    }

    /// Creates the RunArray and without resetting the builder.
    /// Panics if RunArray cannot be built.
    pub fn finish_cloned(&self) -> RunArray<R> {
        let mut run_ends_array = self.run_ends_builder.finish_cloned();
        let mut values_array = self.values_builder.finish_cloned();

        // Add current run if one exists
        if self.prev_run_end_index != self.current_run_end_index {
            let mut run_end_builder = run_ends_array.into_builder().unwrap();
            let mut values_builder = values_array.into_builder().unwrap();
            self.append_run_end_with_builders(&mut run_end_builder, &mut values_builder);
            run_ends_array = run_end_builder.finish();
            values_array = values_builder.finish();
        }

        RunArray::try_new(&run_ends_array, &values_array).unwrap()
    }

    // Appends the current run to the array.
    fn append_run_end(&mut self) {
        // empty array or the function called without appending any value.
        if self.prev_run_end_index == self.current_run_end_index {
            return;
        }
        let run_end_index = self.run_end_index_as_native();
        self.run_ends_builder.append_value(run_end_index);
        self.values_builder.append_option(self.current_value);
        self.prev_run_end_index = self.current_run_end_index;
    }

    // Similar to `append_run_end` but on custom builders.
    // Used in `finish_cloned` which is not suppose to mutate `self`.
    fn append_run_end_with_builders(
        &self,
        run_ends_builder: &mut PrimitiveBuilder<R>,
        values_builder: &mut PrimitiveBuilder<V>,
    ) {
        let run_end_index = self.run_end_index_as_native();
        run_ends_builder.append_value(run_end_index);
        values_builder.append_option(self.current_value);
    }

    fn run_end_index_as_native(&self) -> R::Native {
        R::Native::from_usize(self.current_run_end_index)
        .unwrap_or_else(|| panic!(
                "Cannot convert `current_run_end_index` {} from `usize` to native form of arrow datatype {}",
                self.current_run_end_index,
                R::DATA_TYPE
        ))
    }
}

impl<R, V> Extend<Option<V::Native>> for PrimitiveRunBuilder<R, V>
where
    R: RunEndIndexType,
    V: ArrowPrimitiveType,
{
    fn extend<T: IntoIterator<Item = Option<V::Native>>>(&mut self, iter: T) {
        for elem in iter {
            self.append_option(elem);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::builder::PrimitiveRunBuilder;
    use crate::cast::AsArray;
    use crate::types::{Int16Type, UInt32Type};
    use crate::{Array, UInt32Array};

    #[test]
    fn test_primitive_ree_array_builder() {
        let mut builder = PrimitiveRunBuilder::<Int16Type, UInt32Type>::new();
        builder.append_value(1234);
        builder.append_value(1234);
        builder.append_value(1234);
        builder.append_null();
        builder.append_value(5678);
        builder.append_value(5678);

        let array = builder.finish();

        assert_eq!(array.null_count(), 0);
        assert_eq!(array.logical_null_count(), 1);
        assert_eq!(array.len(), 6);

        assert_eq!(array.run_ends().values(), &[3, 4, 6]);

        let av = array.values();

        assert!(!av.is_null(0));
        assert!(av.is_null(1));
        assert!(!av.is_null(2));

        // Values are polymorphic and so require a downcast.
        let ava: &UInt32Array = av.as_primitive::<UInt32Type>();

        assert_eq!(ava, &UInt32Array::from(vec![Some(1234), None, Some(5678)]));
    }

    #[test]
    fn test_extend() {
        let mut builder = PrimitiveRunBuilder::<Int16Type, Int16Type>::new();
        builder.extend([1, 2, 2, 5, 5, 4, 4].into_iter().map(Some));
        builder.extend([4, 4, 6, 2].into_iter().map(Some));
        let array = builder.finish();

        assert_eq!(array.len(), 11);
        assert_eq!(array.null_count(), 0);
        assert_eq!(array.logical_null_count(), 0);
        assert_eq!(array.run_ends().values(), &[1, 3, 5, 9, 10, 11]);
        assert_eq!(
            array.values().as_primitive::<Int16Type>().values(),
            &[1, 2, 5, 4, 6, 2]
        );
    }
}
