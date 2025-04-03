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
use crate::{Array, ArrayRef, MapArray, StructArray};
use arrow_buffer::Buffer;
use arrow_buffer::{NullBuffer, NullBufferBuilder};
use arrow_data::ArrayData;
use arrow_schema::{ArrowError, DataType, Field, FieldRef};
use std::any::Any;
use std::sync::Arc;

/// Builder for [`MapArray`]
///
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
/// assert_eq!(array.values().as_ref(), &Int32Array::from(vec![1, 2, 4]));
/// assert_eq!(array.keys().as_ref(), &StringArray::from(vec!["joe", "blogs", "foo"]));
///
/// ```
#[derive(Debug)]
pub struct MapBuilder<K: ArrayBuilder, V: ArrayBuilder> {
    offsets_builder: BufferBuilder<i32>,
    null_buffer_builder: NullBufferBuilder,
    field_names: MapFieldNames,
    key_builder: K,
    value_builder: V,
    key_field: Option<FieldRef>,
    value_field: Option<FieldRef>,
}

/// The [`Field`] names for a [`MapArray`]
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
    pub fn new(field_names: Option<MapFieldNames>, key_builder: K, value_builder: V) -> Self {
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
        offsets_builder.append(0);
        Self {
            offsets_builder,
            null_buffer_builder: NullBufferBuilder::new(capacity),
            field_names: field_names.unwrap_or_default(),
            key_builder,
            value_builder,
            key_field: None,
            value_field: None,
        }
    }

    /// Override the field passed to [`MapBuilder::new`]
    ///
    /// By default, a non-nullable field is created with the name `keys`
    ///
    /// Note: [`Self::finish`] and [`Self::finish_cloned`] will panic if the
    /// field's data type does not match that of `K` or the field is nullable
    pub fn with_keys_field(self, field: impl Into<FieldRef>) -> Self {
        Self {
            key_field: Some(field.into()),
            ..self
        }
    }

    /// Override the field passed to [`MapBuilder::new`]
    ///
    /// By default, a nullable field is created with the name `values`
    ///
    /// Note: [`Self::finish`] and [`Self::finish_cloned`] will panic if the
    /// field's data type does not match that of `V`
    pub fn with_values_field(self, field: impl Into<FieldRef>) -> Self {
        Self {
            value_field: Some(field.into()),
            ..self
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

    /// Returns both the key and value array builders of the map
    pub fn entries(&mut self) -> (&mut K, &mut V) {
        (&mut self.key_builder, &mut self.value_builder)
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
        let offset_buffer = self.offsets_builder.finish();
        self.offsets_builder.append(0);
        let null_bit_buffer = self.null_buffer_builder.finish();

        self.finish_helper(keys_arr, values_arr, offset_buffer, null_bit_buffer, len)
    }

    /// Builds the [`MapArray`] without resetting the builder.
    pub fn finish_cloned(&self) -> MapArray {
        let len = self.len();
        // Build the keys
        let keys_arr = self.key_builder.finish_cloned();
        let values_arr = self.value_builder.finish_cloned();
        let offset_buffer = Buffer::from_slice_ref(self.offsets_builder.as_slice());
        let nulls = self.null_buffer_builder.finish_cloned();
        self.finish_helper(keys_arr, values_arr, offset_buffer, nulls, len)
    }

    fn finish_helper(
        &self,
        keys_arr: Arc<dyn Array>,
        values_arr: Arc<dyn Array>,
        offset_buffer: Buffer,
        nulls: Option<NullBuffer>,
        len: usize,
    ) -> MapArray {
        assert!(
            keys_arr.null_count() == 0,
            "Keys array must have no null values, found {} null value(s)",
            keys_arr.null_count()
        );

        let keys_field = match &self.key_field {
            Some(f) => {
                assert!(!f.is_nullable(), "Keys field must not be nullable");
                f.clone()
            }
            None => Arc::new(Field::new(
                self.field_names.key.as_str(),
                keys_arr.data_type().clone(),
                false, // always non-nullable
            )),
        };
        let values_field = match &self.value_field {
            Some(f) => f.clone(),
            None => Arc::new(Field::new(
                self.field_names.value.as_str(),
                values_arr.data_type().clone(),
                true,
            )),
        };

        let struct_array =
            StructArray::from(vec![(keys_field, keys_arr), (values_field, values_arr)]);

        let map_field = Arc::new(Field::new(
            self.field_names.entry.as_str(),
            struct_array.data_type().clone(),
            false, // always non-nullable
        ));
        let array_data = ArrayData::builder(DataType::Map(map_field, false)) // TODO: support sorted keys
            .len(len)
            .add_buffer(offset_buffer)
            .add_child_data(struct_array.into_data())
            .nulls(nulls);

        let array_data = unsafe { array_data.build_unchecked() };

        MapArray::from(array_data)
    }

    /// Returns the current null buffer as a slice
    pub fn validity_slice(&self) -> Option<&[u8]> {
        self.null_buffer_builder.as_slice()
    }
}

impl<K: ArrayBuilder, V: ArrayBuilder> ArrayBuilder for MapBuilder<K, V> {
    fn len(&self) -> usize {
        self.null_buffer_builder.len()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::{make_builder, Int32Builder, StringBuilder};
    use crate::{Int32Array, StringArray};
    use std::collections::HashMap;

    #[test]
    #[should_panic(expected = "Keys array must have no null values, found 1 null value(s)")]
    fn test_map_builder_with_null_keys_panics() {
        let mut builder = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        builder.keys().append_null();
        builder.values().append_value(42);
        builder.append(true).unwrap();

        builder.finish();
    }

    #[test]
    fn test_boxed_map_builder() {
        let keys_builder = make_builder(&DataType::Utf8, 5);
        let values_builder = make_builder(&DataType::Int32, 5);

        let mut builder = MapBuilder::new(None, keys_builder, values_builder);
        builder
            .keys()
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .expect("should be an StringBuilder")
            .append_value("1");
        builder
            .values()
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .expect("should be an Int32Builder")
            .append_value(42);
        builder.append(true).unwrap();

        let map_array = builder.finish();

        assert_eq!(
            map_array
                .keys()
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("should be an StringArray")
                .value(0),
            "1"
        );
        assert_eq!(
            map_array
                .values()
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("should be an Int32Array")
                .value(0),
            42
        );
    }

    #[test]
    fn test_with_values_field() {
        let value_field = Arc::new(Field::new("bars", DataType::Int32, false));
        let mut builder = MapBuilder::new(None, Int32Builder::new(), Int32Builder::new())
            .with_values_field(value_field.clone());
        builder.keys().append_value(1);
        builder.values().append_value(2);
        builder.append(true).unwrap();
        builder.append(false).unwrap(); // This is fine as nullability refers to nullability of values
        builder.keys().append_value(3);
        builder.values().append_value(4);
        builder.append(true).unwrap();
        let map = builder.finish();

        assert_eq!(map.len(), 3);
        assert_eq!(
            map.data_type(),
            &DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Arc::new(Field::new("keys", DataType::Int32, false)),
                            value_field.clone()
                        ]
                        .into()
                    ),
                    false,
                )),
                false
            )
        );

        builder.keys().append_value(5);
        builder.values().append_value(6);
        builder.append(true).unwrap();
        let map = builder.finish();

        assert_eq!(map.len(), 1);
        assert_eq!(
            map.data_type(),
            &DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Arc::new(Field::new("keys", DataType::Int32, false)),
                            value_field
                        ]
                        .into()
                    ),
                    false,
                )),
                false
            )
        );
    }

    #[test]
    fn test_with_keys_field() {
        let mut key_metadata = HashMap::new();
        key_metadata.insert("foo".to_string(), "bar".to_string());
        let key_field = Arc::new(
            Field::new("keys", DataType::Int32, false).with_metadata(key_metadata.clone()),
        );
        let mut builder = MapBuilder::new(None, Int32Builder::new(), Int32Builder::new())
            .with_keys_field(key_field.clone());
        builder.keys().append_value(1);
        builder.values().append_value(2);
        builder.append(true).unwrap();
        let map = builder.finish();

        assert_eq!(map.len(), 1);
        assert_eq!(
            map.data_type(),
            &DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Arc::new(
                                Field::new("keys", DataType::Int32, false)
                                    .with_metadata(key_metadata)
                            ),
                            Arc::new(Field::new("values", DataType::Int32, true))
                        ]
                        .into()
                    ),
                    false,
                )),
                false
            )
        );
    }

    #[test]
    #[should_panic(expected = "Keys field must not be nullable")]
    fn test_with_nullable_keys_field() {
        let mut builder = MapBuilder::new(None, Int32Builder::new(), Int32Builder::new())
            .with_keys_field(Arc::new(Field::new("keys", DataType::Int32, true)));

        builder.keys().append_value(1);
        builder.values().append_value(2);
        builder.append(true).unwrap();

        builder.finish();
    }

    #[test]
    #[should_panic(expected = "Incorrect datatype")]
    fn test_keys_field_type_mismatch() {
        let mut builder = MapBuilder::new(None, Int32Builder::new(), Int32Builder::new())
            .with_keys_field(Arc::new(Field::new("keys", DataType::Utf8, false)));

        builder.keys().append_value(1);
        builder.values().append_value(2);
        builder.append(true).unwrap();

        builder.finish();
    }
}
