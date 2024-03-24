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
use arrow_buffer::{Buffer, BufferBuilder, NullBufferBuilder, OffsetBuffer, SizeBuffer};
use arrow_schema::{Field, FieldRef};
use crate::builder::ArrayBuilder;
use crate::{ArrayRef, GenericListViewArray, OffsetSizeTrait};

#[derive(Debug)]
pub struct GenericListViewBuilder<OffsetSize: OffsetSizeTrait, T: ArrayBuilder> {
    //todo use OffsetBuffer?
    offsets_builder: BufferBuilder<OffsetSize>,
    sizes_builder: BufferBuilder<OffsetSize>,
    null_buffer_builder: NullBufferBuilder,
    values_builder: T,
    field: Option<FieldRef>,
}




impl<O: OffsetSizeTrait, T: ArrayBuilder + Default> Default for GenericListViewBuilder<O, T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<OffsetSize: OffsetSizeTrait, T: ArrayBuilder> GenericListViewBuilder<OffsetSize, T> {
    /// Creates a new [`GenericListBuilder`] from a given values array builder
    pub fn new(values_builder: T) -> Self {
        let capacity = values_builder.len();
        Self::with_capacity(values_builder, capacity)
    }

    /// Creates a new [`GenericListBuilder`] from a given values array builder
    /// `capacity` is the number of items to pre-allocate space for in this builder
    pub fn with_capacity(values_builder: T, capacity: usize) -> Self {
        let offsets_builder = BufferBuilder::<OffsetSize>::new(capacity);
        let sizes_builder = BufferBuilder::<OffsetSize>::new(capacity);
        Self {
            offsets_builder,
            null_buffer_builder: NullBufferBuilder::new(capacity),
            values_builder,
            sizes_builder,
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
for GenericListViewBuilder<OffsetSize, T>
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

impl<OffsetSize: OffsetSizeTrait, T: ArrayBuilder> GenericListViewBuilder<OffsetSize, T>
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
    pub fn append(&mut self, is_valid: bool, size: usize) {
        if is_valid {
            self.offsets_builder.append(OffsetSize::from_usize(self.values_builder.len() - size).unwrap());
            let size = OffsetSize::from_usize(size).unwrap();
            self.sizes_builder.append(size);
        }
        self.null_buffer_builder.append(is_valid);
    }

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
        self.offsets_builder.append(OffsetSize::from_usize(self.values_builder.len()).unwrap());
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

    /// Builds the [`GenericListViewArray`] and reset this builder.
    pub fn finish(&mut self) -> GenericListViewArray<OffsetSize> {
        let values = self.values_builder.finish();
        let nulls = self.null_buffer_builder.finish();

        let offsets = self.offsets_builder.finish();
        // Safety: Safe by construction
        let offsets = unsafe { OffsetBuffer::new_unchecked(offsets.into()) };
        self.offsets_builder.append(OffsetSize::zero());

        let sizes = self.sizes_builder.finish();
        // Safety: Safe by construction
        let sizes = SizeBuffer::new(sizes.into());
        self.sizes_builder.append(OffsetSize::zero());

        let field = match &self.field {
            Some(f) => f.clone(),
            None => Arc::new(Field::new("item", values.data_type().clone(), true)),
        };

        GenericListViewArray::new(field, offsets, sizes, values ,nulls)
    }

    /// Builds the [`GenericListArray`] without resetting the builder.
    pub fn finish_cloned(&self) -> GenericListViewArray<OffsetSize> {
        let values = self.values_builder.finish_cloned();
        let nulls = self.null_buffer_builder.finish_cloned();

        let offsets = Buffer::from_slice_ref(self.offsets_builder.as_slice());
        // Safety: safe by construction
        let offsets = unsafe { OffsetBuffer::new_unchecked(offsets.into()) };

        //todo sizes
        let sizes = Buffer::from_slice_ref(self.sizes_builder.as_slice());
        let sizes = SizeBuffer::new(sizes.into());

        let field = match &self.field {
            Some(f) => f.clone(),
            None => Arc::new(Field::new("item", values.data_type().clone(), true)),
        };

        GenericListViewArray::new(field, offsets, sizes, values, nulls)
    }

    /// Returns the current offsets buffer as a slice
    pub fn offsets_slice(&self) -> &[OffsetSize] {
        self.offsets_builder.as_slice()
    }
}

impl<O, B, V, E> Extend<Option<V>> for GenericListViewBuilder<O, B>
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
                    todo!()
                }
                None => self.append(false, 0),
            }
        }
    }
}
