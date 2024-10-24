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

use crate::builder::{ArrayBuilder, BufferBuilder};
use crate::{Array, ArrayRef, GenericListArray, OffsetSizeTrait};
use arrow_buffer::NullBufferBuilder;
use arrow_buffer::{Buffer, OffsetBuffer};
use arrow_schema::{Field, FieldRef};
use std::any::Any;
use std::sync::Arc;

/// Builder for [`GenericListArray`]
///
/// Use [`ListBuilder`] to build [`ListArray`]s and [`LargeListBuilder`] to build [`LargeListArray`]s.
///
/// # Example
///
/// Here is code that constructs a ListArray with the contents:
/// `[[A,B,C], [], NULL, [D], [NULL, F]]`
///
/// ```
/// # use std::sync::Arc;
/// # use arrow_array::{builder::ListBuilder, builder::StringBuilder, ArrayRef, StringArray, Array};
/// #
/// let values_builder = StringBuilder::new();
/// let mut builder = ListBuilder::new(values_builder);
///
/// // [A, B, C]
/// builder.values().append_value("A");
/// builder.values().append_value("B");
/// builder.values().append_value("C");
/// builder.append(true);
///
/// // [ ] (empty list)
/// builder.append(true);
///
/// // Null
/// builder.values().append_value("?"); // irrelevant
/// builder.append(false);
///
/// // [D]
/// builder.values().append_value("D");
/// builder.append(true);
///
/// // [NULL, F]
/// builder.values().append_null();
/// builder.values().append_value("F");
/// builder.append(true);
///
/// // Build the array
/// let array = builder.finish();
///
/// // Values is a string array
/// // "A", "B" "C", "?", "D", NULL, "F"
/// assert_eq!(
///   array.values().as_ref(),
///   &StringArray::from(vec![
///     Some("A"), Some("B"), Some("C"),
///     Some("?"), Some("D"), None,
///     Some("F")
///   ])
/// );
///
/// // Offsets are indexes into the values array
/// assert_eq!(
///   array.value_offsets(),
///   &[0, 3, 3, 4, 5, 7]
/// );
/// ```
///
/// [`ListBuilder`]: crate::builder::ListBuilder
/// [`ListArray`]: crate::array::ListArray
/// [`LargeListBuilder`]: crate::builder::LargeListBuilder
/// [`LargeListArray`]: crate::array::LargeListArray
#[derive(Debug)]
pub struct GenericListBuilder<OffsetSize: OffsetSizeTrait, T: ArrayBuilder> {
    offsets_builder: BufferBuilder<OffsetSize>,
    null_buffer_builder: NullBufferBuilder,
    values_builder: T,
    field: Option<FieldRef>,
}

impl<O: OffsetSizeTrait, T: ArrayBuilder + Default> Default for GenericListBuilder<O, T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<OffsetSize: OffsetSizeTrait, T: ArrayBuilder> GenericListBuilder<OffsetSize, T> {
    /// Creates a new [`GenericListBuilder`] from a given values array builder
    pub fn new(values_builder: T) -> Self {
        let capacity = values_builder.len();
        Self::with_capacity(values_builder, capacity)
    }

    /// Creates a new [`GenericListBuilder`] from a given values array builder
    /// `capacity` is the number of items to pre-allocate space for in this builder
    pub fn with_capacity(values_builder: T, capacity: usize) -> Self {
        let mut offsets_builder = BufferBuilder::<OffsetSize>::new(capacity + 1);
        offsets_builder.append(OffsetSize::zero());
        Self {
            offsets_builder,
            null_buffer_builder: NullBufferBuilder::new(capacity),
            values_builder,
            field: None,
        }
    }

    /// Override the field passed to [`GenericListArray::new`]
    ///
    /// By default a nullable field is created with the name `item`
    ///
    /// Note: [`Self::finish`] and [`Self::finish_cloned`] will panic if the
    /// field's data type does not match that of `T`
    pub fn with_field(self, field: impl Into<FieldRef>) -> Self {
        Self {
            field: Some(field.into()),
            ..self
        }
    }
}

impl<OffsetSize: OffsetSizeTrait, T: ArrayBuilder> ArrayBuilder
    for GenericListBuilder<OffsetSize, T>
where
    T: 'static,
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

    /// Returns the number of array slots in the builder
    fn len(&self) -> usize {
        self.null_buffer_builder.len()
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

impl<OffsetSize: OffsetSizeTrait, T: ArrayBuilder> GenericListBuilder<OffsetSize, T>
where
    T: 'static,
{
    /// Returns the child array builder as a mutable reference.
    ///
    /// This mutable reference can be used to append values into the child array builder,
    /// but you must call [`append`](#method.append) to delimit each distinct list value.
    pub fn values(&mut self) -> &mut T {
        &mut self.values_builder
    }

    /// Returns the child array builder as an immutable reference
    pub fn values_ref(&self) -> &T {
        &self.values_builder
    }

    /// Finish the current variable-length list array slot
    ///
    /// # Panics
    ///
    /// Panics if the length of [`Self::values`] exceeds `OffsetSize::MAX`
    #[inline]
    pub fn append(&mut self, is_valid: bool) {
        self.offsets_builder.append(self.next_offset());
        self.null_buffer_builder.append(is_valid);
    }

    /// Returns the next offset
    ///
    /// # Panics
    ///
    /// Panics if the length of [`Self::values`] exceeds `OffsetSize::MAX`
    #[inline]
    fn next_offset(&self) -> OffsetSize {
        OffsetSize::from_usize(self.values_builder.len()).unwrap()
    }

    /// Append a value to this [`GenericListBuilder`]
    ///
    /// ```
    /// # use arrow_array::builder::{Int32Builder, ListBuilder};
    /// # use arrow_array::cast::AsArray;
    /// # use arrow_array::{Array, Int32Array};
    /// # use arrow_array::types::Int32Type;
    /// let mut builder = ListBuilder::new(Int32Builder::new());
    ///
    /// builder.append_value([Some(1), Some(2), Some(3)]);
    /// builder.append_value([]);
    /// builder.append_value([None]);
    ///
    /// let array = builder.finish();
    /// assert_eq!(array.len(), 3);
    ///
    /// assert_eq!(array.value_offsets(), &[0, 3, 3, 4]);
    /// let values = array.values().as_primitive::<Int32Type>();
    /// assert_eq!(values, &Int32Array::from(vec![Some(1), Some(2), Some(3), None]));
    /// ```
    ///
    /// This is an alternative API to appending directly to [`Self::values`] and
    /// delimiting the result with [`Self::append`]
    ///
    /// ```
    /// # use arrow_array::builder::{Int32Builder, ListBuilder};
    /// # use arrow_array::cast::AsArray;
    /// # use arrow_array::{Array, Int32Array};
    /// # use arrow_array::types::Int32Type;
    /// let mut builder = ListBuilder::new(Int32Builder::new());
    ///
    /// builder.values().append_value(1);
    /// builder.values().append_value(2);
    /// builder.values().append_value(3);
    /// builder.append(true);
    /// builder.append(true);
    /// builder.values().append_null();
    /// builder.append(true);
    ///
    /// let array = builder.finish();
    /// assert_eq!(array.len(), 3);
    ///
    /// assert_eq!(array.value_offsets(), &[0, 3, 3, 4]);
    /// let values = array.values().as_primitive::<Int32Type>();
    /// assert_eq!(values, &Int32Array::from(vec![Some(1), Some(2), Some(3), None]));
    /// ```
    #[inline]
    pub fn append_value<I, V>(&mut self, i: I)
    where
        T: Extend<Option<V>>,
        I: IntoIterator<Item = Option<V>>,
    {
        self.extend(std::iter::once(Some(i)))
    }

    /// Append a null to this [`GenericListBuilder`]
    ///
    /// See [`Self::append_value`] for an example use.
    #[inline]
    pub fn append_null(&mut self) {
        self.offsets_builder.append(self.next_offset());
        self.null_buffer_builder.append_null();
    }

    /// Appends an optional value into this [`GenericListBuilder`]
    ///
    /// If `Some` calls [`Self::append_value`] otherwise calls [`Self::append_null`]
    #[inline]
    pub fn append_option<I, V>(&mut self, i: Option<I>)
    where
        T: Extend<Option<V>>,
        I: IntoIterator<Item = Option<V>>,
    {
        match i {
            Some(i) => self.append_value(i),
            None => self.append_null(),
        }
    }

    /// Builds the [`GenericListArray`] and reset this builder.
    pub fn finish(&mut self) -> GenericListArray<OffsetSize> {
        let values = self.values_builder.finish();
        let nulls = self.null_buffer_builder.finish();

        let offsets = self.offsets_builder.finish();
        // Safety: Safe by construction
        let offsets = unsafe { OffsetBuffer::new_unchecked(offsets.into()) };
        self.offsets_builder.append(OffsetSize::zero());

        let field = match &self.field {
            Some(f) => f.clone(),
            None => Arc::new(Field::new("item", values.data_type().clone(), true)),
        };

        GenericListArray::new(field, offsets, values, nulls)
    }

    /// Builds the [`GenericListArray`] without resetting the builder.
    pub fn finish_cloned(&self) -> GenericListArray<OffsetSize> {
        let values = self.values_builder.finish_cloned();
        let nulls = self.null_buffer_builder.finish_cloned();

        let offsets = Buffer::from_slice_ref(self.offsets_builder.as_slice());
        // Safety: safe by construction
        let offsets = unsafe { OffsetBuffer::new_unchecked(offsets.into()) };

        let field = match &self.field {
            Some(f) => f.clone(),
            None => Arc::new(Field::new("item", values.data_type().clone(), true)),
        };

        GenericListArray::new(field, offsets, values, nulls)
    }

    /// Returns the current offsets buffer as a slice
    pub fn offsets_slice(&self) -> &[OffsetSize] {
        self.offsets_builder.as_slice()
    }

    /// Returns the current null buffer as a slice
    pub fn validity_slice(&self) -> Option<&[u8]> {
        self.null_buffer_builder.as_slice()
    }
}

impl<O, B, V, E> Extend<Option<V>> for GenericListBuilder<O, B>
where
    O: OffsetSizeTrait,
    B: ArrayBuilder + Extend<E>,
    V: IntoIterator<Item = E>,
{
    #[inline]
    fn extend<T: IntoIterator<Item = Option<V>>>(&mut self, iter: T) {
        for v in iter {
            match v {
                Some(elements) => {
                    self.values_builder.extend(elements);
                    self.append(true);
                }
                None => self.append(false),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::{make_builder, Int32Builder, ListBuilder};
    use crate::cast::AsArray;
    use crate::types::Int32Type;
    use crate::Int32Array;
    use arrow_schema::DataType;

    fn _test_generic_list_array_builder<O: OffsetSizeTrait>() {
        let values_builder = Int32Builder::with_capacity(10);
        let mut builder = GenericListBuilder::<O, _>::new(values_builder);

        //  [[0, 1, 2], [3, 4, 5], [6, 7]]
        builder.values().append_value(0);
        builder.values().append_value(1);
        builder.values().append_value(2);
        builder.append(true);
        builder.values().append_value(3);
        builder.values().append_value(4);
        builder.values().append_value(5);
        builder.append(true);
        builder.values().append_value(6);
        builder.values().append_value(7);
        builder.append(true);
        let list_array = builder.finish();

        let list_values = list_array.values().as_primitive::<Int32Type>();
        assert_eq!(list_values.values(), &[0, 1, 2, 3, 4, 5, 6, 7]);
        assert_eq!(list_array.value_offsets(), [0, 3, 6, 8].map(O::usize_as));
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(3, list_array.len());
        assert_eq!(0, list_array.null_count());
        assert_eq!(O::from_usize(6).unwrap(), list_array.value_offsets()[2]);
        assert_eq!(O::from_usize(2).unwrap(), list_array.value_length(2));
        for i in 0..3 {
            assert!(list_array.is_valid(i));
            assert!(!list_array.is_null(i));
        }
    }

    #[test]
    fn test_list_array_builder() {
        _test_generic_list_array_builder::<i32>()
    }

    #[test]
    fn test_large_list_array_builder() {
        _test_generic_list_array_builder::<i64>()
    }

    fn _test_generic_list_array_builder_nulls<O: OffsetSizeTrait>() {
        let values_builder = Int32Builder::with_capacity(10);
        let mut builder = GenericListBuilder::<O, _>::new(values_builder);

        //  [[0, 1, 2], null, [3, null, 5], [6, 7]]
        builder.values().append_value(0);
        builder.values().append_value(1);
        builder.values().append_value(2);
        builder.append(true);
        builder.append(false);
        builder.values().append_value(3);
        builder.values().append_null();
        builder.values().append_value(5);
        builder.append(true);
        builder.values().append_value(6);
        builder.values().append_value(7);
        builder.append(true);

        let list_array = builder.finish();

        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(4, list_array.len());
        assert_eq!(1, list_array.null_count());
        assert_eq!(O::from_usize(3).unwrap(), list_array.value_offsets()[2]);
        assert_eq!(O::from_usize(3).unwrap(), list_array.value_length(2));
    }

    #[test]
    fn test_list_array_builder_nulls() {
        _test_generic_list_array_builder_nulls::<i32>()
    }

    #[test]
    fn test_large_list_array_builder_nulls() {
        _test_generic_list_array_builder_nulls::<i64>()
    }

    #[test]
    fn test_list_array_builder_finish() {
        let values_builder = Int32Array::builder(5);
        let mut builder = ListBuilder::new(values_builder);

        builder.values().append_slice(&[1, 2, 3]);
        builder.append(true);
        builder.values().append_slice(&[4, 5, 6]);
        builder.append(true);

        let mut arr = builder.finish();
        assert_eq!(2, arr.len());
        assert!(builder.is_empty());

        builder.values().append_slice(&[7, 8, 9]);
        builder.append(true);
        arr = builder.finish();
        assert_eq!(1, arr.len());
        assert!(builder.is_empty());
    }

    #[test]
    fn test_list_array_builder_finish_cloned() {
        let values_builder = Int32Array::builder(5);
        let mut builder = ListBuilder::new(values_builder);

        builder.values().append_slice(&[1, 2, 3]);
        builder.append(true);
        builder.values().append_slice(&[4, 5, 6]);
        builder.append(true);

        let mut arr = builder.finish_cloned();
        assert_eq!(2, arr.len());
        assert!(!builder.is_empty());

        builder.values().append_slice(&[7, 8, 9]);
        builder.append(true);
        arr = builder.finish();
        assert_eq!(3, arr.len());
        assert!(builder.is_empty());
    }

    #[test]
    fn test_list_list_array_builder() {
        let primitive_builder = Int32Builder::with_capacity(10);
        let values_builder = ListBuilder::new(primitive_builder);
        let mut builder = ListBuilder::new(values_builder);

        //  [[[1, 2], [3, 4]], [[5, 6, 7], null, [8]], null, [[9, 10]]]
        builder.values().values().append_value(1);
        builder.values().values().append_value(2);
        builder.values().append(true);
        builder.values().values().append_value(3);
        builder.values().values().append_value(4);
        builder.values().append(true);
        builder.append(true);

        builder.values().values().append_value(5);
        builder.values().values().append_value(6);
        builder.values().values().append_value(7);
        builder.values().append(true);
        builder.values().append(false);
        builder.values().values().append_value(8);
        builder.values().append(true);
        builder.append(true);

        builder.append(false);

        builder.values().values().append_value(9);
        builder.values().values().append_value(10);
        builder.values().append(true);
        builder.append(true);

        let l1 = builder.finish();

        assert_eq!(4, l1.len());
        assert_eq!(1, l1.null_count());

        assert_eq!(l1.value_offsets(), &[0, 2, 5, 5, 6]);
        let l2 = l1.values().as_list::<i32>();

        assert_eq!(6, l2.len());
        assert_eq!(1, l2.null_count());
        assert_eq!(l2.value_offsets(), &[0, 2, 4, 7, 7, 8, 10]);

        let i1 = l2.values().as_primitive::<Int32Type>();
        assert_eq!(10, i1.len());
        assert_eq!(0, i1.null_count());
        assert_eq!(i1.values(), &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    }

    #[test]
    fn test_extend() {
        let mut builder = ListBuilder::new(Int32Builder::new());
        builder.extend([
            Some(vec![Some(1), Some(2), Some(7), None]),
            Some(vec![]),
            Some(vec![Some(4), Some(5)]),
            None,
        ]);

        let array = builder.finish();
        assert_eq!(array.value_offsets(), [0, 4, 4, 6, 6]);
        assert_eq!(array.null_count(), 1);
        assert_eq!(array.logical_null_count(), 1);
        assert!(array.is_null(3));
        let elements = array.values().as_primitive::<Int32Type>();
        assert_eq!(elements.values(), &[1, 2, 7, 0, 4, 5]);
        assert_eq!(elements.null_count(), 1);
        assert_eq!(elements.logical_null_count(), 1);
        assert!(elements.is_null(3));
    }

    #[test]
    fn test_boxed_primitive_array_builder() {
        let values_builder = make_builder(&DataType::Int32, 5);
        let mut builder = ListBuilder::new(values_builder);

        builder
            .values()
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .expect("should be an Int32Builder")
            .append_slice(&[1, 2, 3]);
        builder.append(true);

        builder
            .values()
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .expect("should be an Int32Builder")
            .append_slice(&[4, 5, 6]);
        builder.append(true);

        let arr = builder.finish();
        assert_eq!(2, arr.len());

        let elements = arr.values().as_primitive::<Int32Type>();
        assert_eq!(elements.values(), &[1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn test_boxed_list_list_array_builder() {
        // This test is same as `test_list_list_array_builder` but uses boxed builders.
        let values_builder = make_builder(
            &DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            10,
        );
        test_boxed_generic_list_generic_list_array_builder::<i32>(values_builder);
    }

    #[test]
    fn test_boxed_large_list_large_list_array_builder() {
        // This test is same as `test_list_list_array_builder` but uses boxed builders.
        let values_builder = make_builder(
            &DataType::LargeList(Arc::new(Field::new("item", DataType::Int32, true))),
            10,
        );
        test_boxed_generic_list_generic_list_array_builder::<i64>(values_builder);
    }

    fn test_boxed_generic_list_generic_list_array_builder<O: OffsetSizeTrait + PartialEq>(
        values_builder: Box<dyn ArrayBuilder>,
    ) {
        let mut builder: GenericListBuilder<O, Box<dyn ArrayBuilder>> =
            GenericListBuilder::<O, Box<dyn ArrayBuilder>>::new(values_builder);

        //  [[[1, 2], [3, 4]], [[5, 6, 7], null, [8]], null, [[9, 10]]]
        builder
            .values()
            .as_any_mut()
            .downcast_mut::<GenericListBuilder<O, Box<dyn ArrayBuilder>>>()
            .expect("should be an (Large)ListBuilder")
            .values()
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .expect("should be an Int32Builder")
            .append_value(1);
        builder
            .values()
            .as_any_mut()
            .downcast_mut::<GenericListBuilder<O, Box<dyn ArrayBuilder>>>()
            .expect("should be an (Large)ListBuilder")
            .values()
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .expect("should be an Int32Builder")
            .append_value(2);
        builder
            .values()
            .as_any_mut()
            .downcast_mut::<GenericListBuilder<O, Box<dyn ArrayBuilder>>>()
            .expect("should be an (Large)ListBuilder")
            .append(true);
        builder
            .values()
            .as_any_mut()
            .downcast_mut::<GenericListBuilder<O, Box<dyn ArrayBuilder>>>()
            .expect("should be an (Large)ListBuilder")
            .values()
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .expect("should be an Int32Builder")
            .append_value(3);
        builder
            .values()
            .as_any_mut()
            .downcast_mut::<GenericListBuilder<O, Box<dyn ArrayBuilder>>>()
            .expect("should be an (Large)ListBuilder")
            .values()
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .expect("should be an Int32Builder")
            .append_value(4);
        builder
            .values()
            .as_any_mut()
            .downcast_mut::<GenericListBuilder<O, Box<dyn ArrayBuilder>>>()
            .expect("should be an (Large)ListBuilder")
            .append(true);
        builder.append(true);

        builder
            .values()
            .as_any_mut()
            .downcast_mut::<GenericListBuilder<O, Box<dyn ArrayBuilder>>>()
            .expect("should be an (Large)ListBuilder")
            .values()
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .expect("should be an Int32Builder")
            .append_value(5);
        builder
            .values()
            .as_any_mut()
            .downcast_mut::<GenericListBuilder<O, Box<dyn ArrayBuilder>>>()
            .expect("should be an (Large)ListBuilder")
            .values()
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .expect("should be an Int32Builder")
            .append_value(6);
        builder
            .values()
            .as_any_mut()
            .downcast_mut::<GenericListBuilder<O, Box<dyn ArrayBuilder>>>()
            .expect("should be an (Large)ListBuilder")
            .values()
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .expect("should be an (Large)ListBuilder")
            .append_value(7);
        builder
            .values()
            .as_any_mut()
            .downcast_mut::<GenericListBuilder<O, Box<dyn ArrayBuilder>>>()
            .expect("should be an (Large)ListBuilder")
            .append(true);
        builder
            .values()
            .as_any_mut()
            .downcast_mut::<GenericListBuilder<O, Box<dyn ArrayBuilder>>>()
            .expect("should be an (Large)ListBuilder")
            .append(false);
        builder
            .values()
            .as_any_mut()
            .downcast_mut::<GenericListBuilder<O, Box<dyn ArrayBuilder>>>()
            .expect("should be an (Large)ListBuilder")
            .values()
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .expect("should be an Int32Builder")
            .append_value(8);
        builder
            .values()
            .as_any_mut()
            .downcast_mut::<GenericListBuilder<O, Box<dyn ArrayBuilder>>>()
            .expect("should be an (Large)ListBuilder")
            .append(true);
        builder.append(true);

        builder.append(false);

        builder
            .values()
            .as_any_mut()
            .downcast_mut::<GenericListBuilder<O, Box<dyn ArrayBuilder>>>()
            .expect("should be an (Large)ListBuilder")
            .values()
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .expect("should be an Int32Builder")
            .append_value(9);
        builder
            .values()
            .as_any_mut()
            .downcast_mut::<GenericListBuilder<O, Box<dyn ArrayBuilder>>>()
            .expect("should be an (Large)ListBuilder")
            .values()
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .expect("should be an Int32Builder")
            .append_value(10);
        builder
            .values()
            .as_any_mut()
            .downcast_mut::<GenericListBuilder<O, Box<dyn ArrayBuilder>>>()
            .expect("should be an (Large)ListBuilder")
            .append(true);
        builder.append(true);

        let l1 = builder.finish();

        assert_eq!(4, l1.len());
        assert_eq!(1, l1.null_count());

        assert_eq!(l1.value_offsets(), &[0, 2, 5, 5, 6].map(O::usize_as));
        let l2 = l1.values().as_list::<O>();

        assert_eq!(6, l2.len());
        assert_eq!(1, l2.null_count());
        assert_eq!(l2.value_offsets(), &[0, 2, 4, 7, 7, 8, 10].map(O::usize_as));

        let i1 = l2.values().as_primitive::<Int32Type>();
        assert_eq!(10, i1.len());
        assert_eq!(0, i1.null_count());
        assert_eq!(i1.values(), &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    }

    #[test]
    fn test_with_field() {
        let field = Arc::new(Field::new("bar", DataType::Int32, false));
        let mut builder = ListBuilder::new(Int32Builder::new()).with_field(field.clone());
        builder.append_value([Some(1), Some(2), Some(3)]);
        builder.append_null(); // This is fine as nullability refers to nullability of values
        builder.append_value([Some(4)]);
        let array = builder.finish();
        assert_eq!(array.len(), 3);
        assert_eq!(array.data_type(), &DataType::List(field.clone()));

        builder.append_value([Some(4), Some(5)]);
        let array = builder.finish();
        assert_eq!(array.data_type(), &DataType::List(field));
        assert_eq!(array.len(), 1);
    }

    #[test]
    #[should_panic(expected = "Non-nullable field of ListArray \\\"item\\\" cannot contain nulls")]
    fn test_checks_nullability() {
        let field = Arc::new(Field::new("item", DataType::Int32, false));
        let mut builder = ListBuilder::new(Int32Builder::new()).with_field(field.clone());
        builder.append_value([Some(1), None]);
        builder.finish();
    }

    #[test]
    #[should_panic(expected = "ListArray expected data type Int64 got Int32")]
    fn test_checks_data_type() {
        let field = Arc::new(Field::new("item", DataType::Int64, false));
        let mut builder = ListBuilder::new(Int32Builder::new()).with_field(field.clone());
        builder.append_value([Some(1)]);
        builder.finish();
    }
}
