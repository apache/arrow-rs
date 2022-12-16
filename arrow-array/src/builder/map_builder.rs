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

use crate::builder::null_buffer_builder::NullBufferBuilder;
use crate::builder::{ArrayBuilder, BufferBuilder};
use crate::{Array, ArrayRef, MapArray, StructArray};
use arrow_buffer::Buffer;
use arrow_data::ArrayData;
use arrow_schema::{ArrowError, DataType, Field};
use std::any::Any;
use std::sync::Arc;

/// Creates a new `MapBuilder`
/// ```
/// # use arrow_array::builder::{Int32Builder, MapBuilder, StringBuilder};
/// # use arrow_array::{Int32Array, StringArray};
///
/// let string_builder = StringBuilder::new();
/// let int_builder = Int32Builder::with_capacity(4);
///
/// // Construct `[{"joe": 1}, {"blogs": 2, "foo": 4}, {}, null]`
/// let mut builder = MapBuilder::new(None, string_builder, int_builder);
///
/// builder.keys().append_value("joe");
/// builder.values().append_value(1);
/// builder.append(true).unwrap();
///
/// builder.keys().append_value("blogs");
/// builder.values().append_value(2);
/// builder.keys().append_value("foo");
/// builder.values().append_value(4);
/// builder.append(true).unwrap();
/// builder.append(true).unwrap();
/// builder.append(false).unwrap();
///
/// let array = builder.finish();
/// assert_eq!(array.value_offsets(), &[0, 1, 3, 3, 3]);
/// assert_eq!(*array.values(), Int32Array::from(vec![1, 2, 4]));
/// assert_eq!(*array.keys(), StringArray::from(vec!["joe", "blogs", "foo"]));
///
/// ```
#[derive(Debug)]
pub struct MapBuilder<K: ArrayBuilder, V: ArrayBuilder> {
    offsets_builder: BufferBuilder<i32>,
    null_buffer_builder: NullBufferBuilder,
    field_names: MapFieldNames,
    key_builder: K,
    value_builder: V,
}

/// Contains details of the mapping
#[derive(Debug, Clone)]
pub struct MapFieldNames {
    /// [`Field`] name for map entries
    pub entry: String,
    /// [`Field`] name for map key
    pub key: String,
    /// [`Field`] name for map value
    pub value: String,
}

impl Default for MapFieldNames {
    fn default() -> Self {
        Self {
            entry: "entries".to_string(),
            key: "keys".to_string(),
            value: "values".to_string(),
        }
    }
}

impl<K: ArrayBuilder, V: ArrayBuilder> MapBuilder<K, V> {
    /// Creates a new `MapBuilder`
    pub fn new(
        field_names: Option<MapFieldNames>,
        key_builder: K,
        value_builder: V,
    ) -> Self {
        let capacity = key_builder.len();
        Self::with_capacity(field_names, key_builder, value_builder, capacity)
    }

    /// Creates a new `MapBuilder` with capacity
    pub fn with_capacity(
        field_names: Option<MapFieldNames>,
        key_builder: K,
        value_builder: V,
        capacity: usize,
    ) -> Self {
        let mut offsets_builder = BufferBuilder::<i32>::new(capacity + 1);
        let len = 0;
        offsets_builder.append(len);
        Self {
            offsets_builder,
            null_buffer_builder: NullBufferBuilder::new(capacity),
            field_names: field_names.unwrap_or_default(),
            key_builder,
            value_builder,
        }
    }

    /// Returns the key array builder of the map
    pub fn keys(&mut self) -> &mut K {
        &mut self.key_builder
    }

    /// Returns the value array builder of the map
    pub fn values(&mut self) -> &mut V {
        &mut self.value_builder
    }

    /// Finish the current map array slot
    ///
    /// Returns an error if the key and values builders are in an inconsistent state.
    #[inline]
    pub fn append(&mut self, is_valid: bool) -> Result<(), ArrowError> {
        if self.key_builder.len() != self.value_builder.len() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Cannot append to a map builder when its keys and values have unequal lengths of {} and {}",
                self.key_builder.len(),
                self.value_builder.len()
            )));
        }
        self.offsets_builder.append(self.key_builder.len() as i32);
        self.null_buffer_builder.append(is_valid);
        Ok(())
    }

    /// Builds the [`MapArray`]
    pub fn finish(&mut self) -> MapArray {
        let len = self.len();

        // Build the keys
        let keys_arr = self.key_builder.finish();
        let values_arr = self.value_builder.finish();

        let keys_field = Field::new(
            self.field_names.key.as_str(),
            keys_arr.data_type().clone(),
            false, // always nullable
        );
        let values_field = Field::new(
            self.field_names.value.as_str(),
            values_arr.data_type().clone(),
            true,
        );

        let struct_array =
            StructArray::from(vec![(keys_field, keys_arr), (values_field, values_arr)]);

        let offset_buffer = self.offsets_builder.finish();
        let null_bit_buffer = self.null_buffer_builder.finish();
        self.offsets_builder.append(0);
        let map_field = Box::new(Field::new(
            self.field_names.entry.as_str(),
            struct_array.data_type().clone(),
            false, // always non-nullable
        ));
        let array_data = ArrayData::builder(DataType::Map(map_field, false)) // TODO: support sorted keys
            .len(len)
            .add_buffer(offset_buffer)
            .add_child_data(struct_array.into_data())
            .null_bit_buffer(null_bit_buffer);

        let array_data = unsafe { array_data.build_unchecked() };

        MapArray::from(array_data)
    }

    /// Builds the [`MapArray`] without resetting the builder.
    pub fn finish_cloned(&self) -> MapArray {
        let len = self.len();

        // Build the keys
        let keys_arr = self.key_builder.finish_cloned();
        let values_arr = self.value_builder.finish_cloned();

        let keys_field = Field::new(
            self.field_names.key.as_str(),
            keys_arr.data_type().clone(),
            false, // always nullable
        );
        let values_field = Field::new(
            self.field_names.value.as_str(),
            values_arr.data_type().clone(),
            true,
        );

        let struct_array =
            StructArray::from(vec![(keys_field, keys_arr), (values_field, values_arr)]);

        let offset_buffer = Buffer::from_slice_ref(self.offsets_builder.as_slice());
        let null_bit_buffer = self
            .null_buffer_builder
            .as_slice()
            .map(Buffer::from_slice_ref);
        let map_field = Box::new(Field::new(
            self.field_names.entry.as_str(),
            struct_array.data_type().clone(),
            false, // always non-nullable
        ));
        let array_data = ArrayData::builder(DataType::Map(map_field, false)) // TODO: support sorted keys
            .len(len)
            .add_buffer(offset_buffer)
            .add_child_data(struct_array.into_data())
            .null_bit_buffer(null_bit_buffer);

        let array_data = unsafe { array_data.build_unchecked() };

        MapArray::from(array_data)
    }
}

impl<K: ArrayBuilder, V: ArrayBuilder> ArrayBuilder for MapBuilder<K, V> {
    fn len(&self) -> usize {
        self.null_buffer_builder.len()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }

    /// Builds the array without resetting the builder.
    fn finish_cloned(&self) -> ArrayRef {
        Arc::new(self.finish_cloned())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}
