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

use crate::array::{get_offsets, print_long_array};
use crate::iterator::MapArrayIter;
use crate::{make_array, Array, ArrayAccessor, ArrayRef, ListArray, StringArray, StructArray};
use arrow_buffer::{ArrowNativeType, Buffer, NullBuffer, OffsetBuffer, ToByteSlice};
use arrow_data::{ArrayData, ArrayDataBuilder};
use arrow_schema::{ArrowError, DataType, Field, FieldRef};
use std::any::Any;
use std::sync::Arc;

/// An array of key-value maps
///
/// Keys should always be non-null, but values can be null.
///
/// [`MapArray`] is physically a [`ListArray`] of key values pairs stored as an `entries`
/// [`StructArray`] with 2 child fields.
///
/// See [`MapBuilder`](crate::builder::MapBuilder) for how to construct a [`MapArray`]
#[derive(Clone)]
pub struct MapArray {
    data_type: DataType,
    nulls: Option<NullBuffer>,
    /// The [`StructArray`] that is the direct child of this array
    entries: StructArray,
    /// The start and end offsets of each entry
    value_offsets: OffsetBuffer<i32>,
}

impl MapArray {
    /// Create a new [`MapArray`] from the provided parts
    ///
    /// See [`MapBuilder`](crate::builder::MapBuilder) for a higher-level interface
    /// to construct a [`MapArray`]
    ///
    /// # Errors
    ///
    /// Errors if
    ///
    /// * `offsets.len() - 1 != nulls.len()`
    /// * `offsets.last() > entries.len()`
    /// * `field.is_nullable()`
    /// * `entries.null_count() != 0`
    /// * `entries.columns().len() != 2`
    /// * `field.data_type() != entries.data_type()`
    pub fn try_new(
        field: FieldRef,
        offsets: OffsetBuffer<i32>,
        entries: StructArray,
        nulls: Option<NullBuffer>,
        ordered: bool,
    ) -> Result<Self, ArrowError> {
        let len = offsets.len() - 1; // Offsets guaranteed to not be empty
        let end_offset = offsets.last().unwrap().as_usize();
        // don't need to check other values of `offsets` because they are checked
        // during construction of `OffsetBuffer`
        if end_offset > entries.len() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Max offset of {end_offset} exceeds length of entries {}",
                entries.len()
            )));
        }

        if let Some(n) = nulls.as_ref() {
            if n.len() != len {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Incorrect length of null buffer for MapArray, expected {len} got {}",
                    n.len(),
                )));
            }
        }
        if field.is_nullable() || entries.null_count() != 0 {
            return Err(ArrowError::InvalidArgumentError(
                "MapArray entries cannot contain nulls".to_string(),
            ));
        }

        if field.data_type() != entries.data_type() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "MapArray expected data type {} got {} for {:?}",
                field.data_type(),
                entries.data_type(),
                field.name()
            )));
        }

        if entries.columns().len() != 2 {
            return Err(ArrowError::InvalidArgumentError(format!(
                "MapArray entries must contain two children, got {}",
                entries.columns().len()
            )));
        }

        Ok(Self {
            data_type: DataType::Map(field, ordered),
            nulls,
            entries,
            value_offsets: offsets,
        })
    }

    /// Create a new [`MapArray`] from the provided parts
    ///
    /// See [`MapBuilder`](crate::builder::MapBuilder) for a higher-level interface
    /// to construct a [`MapArray`]
    ///
    /// # Panics
    ///
    /// Panics if [`Self::try_new`] returns an error
    pub fn new(
        field: FieldRef,
        offsets: OffsetBuffer<i32>,
        entries: StructArray,
        nulls: Option<NullBuffer>,
        ordered: bool,
    ) -> Self {
        Self::try_new(field, offsets, entries, nulls, ordered).unwrap()
    }

    /// Deconstruct this array into its constituent parts
    pub fn into_parts(
        self,
    ) -> (
        FieldRef,
        OffsetBuffer<i32>,
        StructArray,
        Option<NullBuffer>,
        bool,
    ) {
        let (f, ordered) = match self.data_type {
            DataType::Map(f, ordered) => (f, ordered),
            _ => unreachable!(),
        };
        (f, self.value_offsets, self.entries, self.nulls, ordered)
    }

    /// Returns a reference to the offsets of this map
    ///
    /// Unlike [`Self::value_offsets`] this returns the [`OffsetBuffer`]
    /// allowing for zero-copy cloning
    #[inline]
    pub fn offsets(&self) -> &OffsetBuffer<i32> {
        &self.value_offsets
    }

    /// Returns a reference to the keys of this map
    pub fn keys(&self) -> &ArrayRef {
        self.entries.column(0)
    }

    /// Returns a reference to the values of this map
    pub fn values(&self) -> &ArrayRef {
        self.entries.column(1)
    }

    /// Returns a reference to the [`StructArray`] entries of this map
    pub fn entries(&self) -> &StructArray {
        &self.entries
    }

    /// Returns the data type of the map's keys.
    pub fn key_type(&self) -> &DataType {
        self.keys().data_type()
    }

    /// Returns the data type of the map's values.
    pub fn value_type(&self) -> &DataType {
        self.values().data_type()
    }

    /// Returns ith value of this map array.
    ///
    /// # Safety
    /// Caller must ensure that the index is within the array bounds
    pub unsafe fn value_unchecked(&self, i: usize) -> StructArray {
        let end = *self.value_offsets().get_unchecked(i + 1);
        let start = *self.value_offsets().get_unchecked(i);
        self.entries
            .slice(start.to_usize().unwrap(), (end - start).to_usize().unwrap())
    }

    /// Returns ith value of this map array.
    ///
    /// This is a [`StructArray`] containing two fields
    pub fn value(&self, i: usize) -> StructArray {
        let end = self.value_offsets()[i + 1] as usize;
        let start = self.value_offsets()[i] as usize;
        self.entries.slice(start, end - start)
    }

    /// Returns the offset values in the offsets buffer
    #[inline]
    pub fn value_offsets(&self) -> &[i32] {
        &self.value_offsets
    }

    /// Returns the length for value at index `i`.
    #[inline]
    pub fn value_length(&self, i: usize) -> i32 {
        let offsets = self.value_offsets();
        offsets[i + 1] - offsets[i]
    }

    /// Returns a zero-copy slice of this array with the indicated offset and length.
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        Self {
            data_type: self.data_type.clone(),
            nulls: self.nulls.as_ref().map(|n| n.slice(offset, length)),
            entries: self.entries.clone(),
            value_offsets: self.value_offsets.slice(offset, length),
        }
    }

    /// constructs a new iterator
    pub fn iter(&self) -> MapArrayIter<'_> {
        MapArrayIter::new(self)
    }
}

impl From<ArrayData> for MapArray {
    fn from(data: ArrayData) -> Self {
        Self::try_new_from_array_data(data)
            .expect("Expected infallible creation of MapArray from ArrayData failed")
    }
}

impl From<MapArray> for ArrayData {
    fn from(array: MapArray) -> Self {
        let len = array.len();
        let builder = ArrayDataBuilder::new(array.data_type)
            .len(len)
            .nulls(array.nulls)
            .buffers(vec![array.value_offsets.into_inner().into_inner()])
            .child_data(vec![array.entries.to_data()]);

        unsafe { builder.build_unchecked() }
    }
}

impl MapArray {
    fn try_new_from_array_data(data: ArrayData) -> Result<Self, ArrowError> {
        if !matches!(data.data_type(), DataType::Map(_, _)) {
            return Err(ArrowError::InvalidArgumentError(format!(
                "MapArray expected ArrayData with DataType::Map got {}",
                data.data_type()
            )));
        }

        if data.buffers().len() != 1 {
            return Err(ArrowError::InvalidArgumentError(format!(
                "MapArray data should contain a single buffer only (value offsets), had {}",
                data.len()
            )));
        }

        if data.child_data().len() != 1 {
            return Err(ArrowError::InvalidArgumentError(format!(
                "MapArray should contain a single child array (values array), had {}",
                data.child_data().len()
            )));
        }

        let entries = data.child_data()[0].clone();

        if let DataType::Struct(fields) = entries.data_type() {
            if fields.len() != 2 {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "MapArray should contain a struct array with 2 fields, have {} fields",
                    fields.len()
                )));
            }
        } else {
            return Err(ArrowError::InvalidArgumentError(format!(
                "MapArray should contain a struct array child, found {:?}",
                entries.data_type()
            )));
        }
        let entries = entries.into();

        // SAFETY:
        // ArrayData is valid, and verified type above
        let value_offsets = unsafe { get_offsets(&data) };

        Ok(Self {
            data_type: data.data_type().clone(),
            nulls: data.nulls().cloned(),
            entries,
            value_offsets,
        })
    }

    /// Creates map array from provided keys, values and entry_offsets.
    pub fn new_from_strings<'a>(
        keys: impl Iterator<Item = &'a str>,
        values: &dyn Array,
        entry_offsets: &[u32],
    ) -> Result<Self, ArrowError> {
        let entry_offsets_buffer = Buffer::from(entry_offsets.to_byte_slice());
        let keys_data = StringArray::from_iter_values(keys);

        let keys_field = Arc::new(Field::new("keys", DataType::Utf8, false));
        let values_field = Arc::new(Field::new(
            "values",
            values.data_type().clone(),
            values.null_count() > 0,
        ));

        let entry_struct = StructArray::from(vec![
            (keys_field, Arc::new(keys_data) as ArrayRef),
            (values_field, make_array(values.to_data())),
        ]);

        let map_data_type = DataType::Map(
            Arc::new(Field::new(
                "entries",
                entry_struct.data_type().clone(),
                false,
            )),
            false,
        );
        let map_data = ArrayData::builder(map_data_type)
            .len(entry_offsets.len() - 1)
            .add_buffer(entry_offsets_buffer)
            .add_child_data(entry_struct.into_data())
            .build()?;

        Ok(MapArray::from(map_data))
    }
}

impl Array for MapArray {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn to_data(&self) -> ArrayData {
        self.clone().into_data()
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
        self.value_offsets.len() - 1
    }

    fn is_empty(&self) -> bool {
        self.value_offsets.len() <= 1
    }

    fn offset(&self) -> usize {
        0
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        self.nulls.as_ref()
    }

    fn get_buffer_memory_size(&self) -> usize {
        let mut size = self.entries.get_buffer_memory_size();
        size += self.value_offsets.inner().inner().capacity();
        if let Some(n) = self.nulls.as_ref() {
            size += n.buffer().capacity();
        }
        size
    }

    fn get_array_memory_size(&self) -> usize {
        let mut size = std::mem::size_of::<Self>() + self.entries.get_array_memory_size();
        size += self.value_offsets.inner().inner().capacity();
        if let Some(n) = self.nulls.as_ref() {
            size += n.buffer().capacity();
        }
        size
    }
}

impl ArrayAccessor for &MapArray {
    type Item = StructArray;

    fn value(&self, index: usize) -> Self::Item {
        MapArray::value(self, index)
    }

    unsafe fn value_unchecked(&self, index: usize) -> Self::Item {
        MapArray::value(self, index)
    }
}

impl std::fmt::Debug for MapArray {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "MapArray\n[\n")?;
        print_long_array(self, f, |array, index, f| {
            std::fmt::Debug::fmt(&array.value(index), f)
        })?;
        write!(f, "]")
    }
}

impl From<MapArray> for ListArray {
    fn from(value: MapArray) -> Self {
        let field = match value.data_type() {
            DataType::Map(field, _) => field,
            _ => unreachable!("This should be a map type."),
        };
        let data_type = DataType::List(field.clone());
        let builder = value.into_data().into_builder().data_type(data_type);
        let array_data = unsafe { builder.build_unchecked() };

        ListArray::from(array_data)
    }
}

#[cfg(test)]
mod tests {
    use crate::cast::AsArray;
    use crate::types::UInt32Type;
    use crate::{Int32Array, UInt32Array};
    use arrow_schema::Fields;

    use super::*;

    fn create_from_buffers() -> MapArray {
        // Construct key and values
        let keys_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from([0, 1, 2, 3, 4, 5, 6, 7].to_byte_slice()))
            .build()
            .unwrap();
        let values_data = ArrayData::builder(DataType::UInt32)
            .len(8)
            .add_buffer(Buffer::from(
                [0u32, 10, 20, 30, 40, 50, 60, 70].to_byte_slice(),
            ))
            .build()
            .unwrap();

        // Construct a buffer for value offsets, for the nested array:
        //  [[0, 1, 2], [3, 4, 5], [6, 7]]
        let entry_offsets = Buffer::from([0, 3, 6, 8].to_byte_slice());

        let keys = Arc::new(Field::new("keys", DataType::Int32, false));
        let values = Arc::new(Field::new("values", DataType::UInt32, false));
        let entry_struct = StructArray::from(vec![
            (keys, make_array(keys_data)),
            (values, make_array(values_data)),
        ]);

        // Construct a map array from the above two
        let map_data_type = DataType::Map(
            Arc::new(Field::new(
                "entries",
                entry_struct.data_type().clone(),
                false,
            )),
            false,
        );
        let map_data = ArrayData::builder(map_data_type)
            .len(3)
            .add_buffer(entry_offsets)
            .add_child_data(entry_struct.into_data())
            .build()
            .unwrap();
        MapArray::from(map_data)
    }

    #[test]
    fn test_map_array() {
        // Construct key and values
        let key_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from([0, 1, 2, 3, 4, 5, 6, 7].to_byte_slice()))
            .build()
            .unwrap();
        let value_data = ArrayData::builder(DataType::UInt32)
            .len(8)
            .add_buffer(Buffer::from(
                [0u32, 10, 20, 0, 40, 0, 60, 70].to_byte_slice(),
            ))
            .null_bit_buffer(Some(Buffer::from(&[0b11010110])))
            .build()
            .unwrap();

        // Construct a buffer for value offsets, for the nested array:
        //  [[0, 1, 2], [3, 4, 5], [6, 7]]
        let entry_offsets = Buffer::from([0, 3, 6, 8].to_byte_slice());

        let keys_field = Arc::new(Field::new("keys", DataType::Int32, false));
        let values_field = Arc::new(Field::new("values", DataType::UInt32, true));
        let entry_struct = StructArray::from(vec![
            (keys_field.clone(), make_array(key_data)),
            (values_field.clone(), make_array(value_data.clone())),
        ]);

        // Construct a map array from the above two
        let map_data_type = DataType::Map(
            Arc::new(Field::new(
                "entries",
                entry_struct.data_type().clone(),
                false,
            )),
            false,
        );
        let map_data = ArrayData::builder(map_data_type)
            .len(3)
            .add_buffer(entry_offsets)
            .add_child_data(entry_struct.into_data())
            .build()
            .unwrap();
        let map_array = MapArray::from(map_data);

        assert_eq!(value_data, map_array.values().to_data());
        assert_eq!(&DataType::UInt32, map_array.value_type());
        assert_eq!(3, map_array.len());
        assert_eq!(0, map_array.null_count());
        assert_eq!(6, map_array.value_offsets()[2]);
        assert_eq!(2, map_array.value_length(2));

        let key_array = Arc::new(Int32Array::from(vec![0, 1, 2])) as ArrayRef;
        let value_array =
            Arc::new(UInt32Array::from(vec![None, Some(10u32), Some(20)])) as ArrayRef;
        let struct_array = StructArray::from(vec![
            (keys_field.clone(), key_array),
            (values_field.clone(), value_array),
        ]);
        assert_eq!(
            struct_array,
            StructArray::from(map_array.value(0).into_data())
        );
        assert_eq!(
            &struct_array,
            unsafe { map_array.value_unchecked(0) }
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
        );
        for i in 0..3 {
            assert!(map_array.is_valid(i));
            assert!(!map_array.is_null(i));
        }

        // Now test with a non-zero offset
        let map_array = map_array.slice(1, 2);

        assert_eq!(value_data, map_array.values().to_data());
        assert_eq!(&DataType::UInt32, map_array.value_type());
        assert_eq!(2, map_array.len());
        assert_eq!(0, map_array.null_count());
        assert_eq!(6, map_array.value_offsets()[1]);
        assert_eq!(2, map_array.value_length(1));

        let key_array = Arc::new(Int32Array::from(vec![3, 4, 5])) as ArrayRef;
        let value_array = Arc::new(UInt32Array::from(vec![None, Some(40), None])) as ArrayRef;
        let struct_array =
            StructArray::from(vec![(keys_field, key_array), (values_field, value_array)]);
        assert_eq!(
            &struct_array,
            map_array
                .value(0)
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
        );
        assert_eq!(
            &struct_array,
            unsafe { map_array.value_unchecked(0) }
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
        );
    }

    #[test]
    #[ignore = "Test fails because slice of <list<struct>> is still buggy"]
    fn test_map_array_slice() {
        let map_array = create_from_buffers();

        let sliced_array = map_array.slice(1, 2);
        assert_eq!(2, sliced_array.len());
        assert_eq!(1, sliced_array.offset());
        let sliced_array_data = sliced_array.to_data();
        for array_data in sliced_array_data.child_data() {
            assert_eq!(array_data.offset(), 1);
        }

        // Check offset and length for each non-null value.
        let sliced_map_array = sliced_array.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(3, sliced_map_array.value_offsets()[0]);
        assert_eq!(3, sliced_map_array.value_length(0));
        assert_eq!(6, sliced_map_array.value_offsets()[1]);
        assert_eq!(2, sliced_map_array.value_length(1));

        // Construct key and values
        let keys_data = ArrayData::builder(DataType::Int32)
            .len(5)
            .add_buffer(Buffer::from([3, 4, 5, 6, 7].to_byte_slice()))
            .build()
            .unwrap();
        let values_data = ArrayData::builder(DataType::UInt32)
            .len(5)
            .add_buffer(Buffer::from([30u32, 40, 50, 60, 70].to_byte_slice()))
            .build()
            .unwrap();

        // Construct a buffer for value offsets, for the nested array:
        //  [[3, 4, 5], [6, 7]]
        let entry_offsets = Buffer::from([0, 3, 5].to_byte_slice());

        let keys = Arc::new(Field::new("keys", DataType::Int32, false));
        let values = Arc::new(Field::new("values", DataType::UInt32, false));
        let entry_struct = StructArray::from(vec![
            (keys, make_array(keys_data)),
            (values, make_array(values_data)),
        ]);

        // Construct a map array from the above two
        let map_data_type = DataType::Map(
            Arc::new(Field::new(
                "entries",
                entry_struct.data_type().clone(),
                false,
            )),
            false,
        );
        let expected_map_data = ArrayData::builder(map_data_type)
            .len(2)
            .add_buffer(entry_offsets)
            .add_child_data(entry_struct.into_data())
            .build()
            .unwrap();
        let expected_map_array = MapArray::from(expected_map_data);

        assert_eq!(&expected_map_array, sliced_map_array)
    }

    #[test]
    #[should_panic(expected = "index out of bounds: the len is ")]
    fn test_map_array_index_out_of_bound() {
        let map_array = create_from_buffers();

        map_array.value(map_array.len());
    }

    #[test]
    #[should_panic(expected = "MapArray expected ArrayData with DataType::Map got Dictionary")]
    fn test_from_array_data_validation() {
        // A DictionaryArray has similar buffer layout to a MapArray
        // but the meaning of the values differs
        let struct_t = DataType::Struct(Fields::from(vec![
            Field::new("keys", DataType::Int32, true),
            Field::new("values", DataType::UInt32, true),
        ]));
        let dict_t = DataType::Dictionary(Box::new(DataType::Int32), Box::new(struct_t));
        let _ = MapArray::from(ArrayData::new_empty(&dict_t));
    }

    #[test]
    fn test_new_from_strings() {
        let keys = vec!["a", "b", "c", "d", "e", "f", "g", "h"];
        let values_data = UInt32Array::from(vec![0u32, 10, 20, 30, 40, 50, 60, 70]);

        // Construct a buffer for value offsets, for the nested array:
        //  [[a, b, c], [d, e, f], [g, h]]
        let entry_offsets = [0, 3, 6, 8];

        let map_array =
            MapArray::new_from_strings(keys.clone().into_iter(), &values_data, &entry_offsets)
                .unwrap();

        assert_eq!(
            &values_data,
            map_array.values().as_primitive::<UInt32Type>()
        );
        assert_eq!(&DataType::UInt32, map_array.value_type());
        assert_eq!(3, map_array.len());
        assert_eq!(0, map_array.null_count());
        assert_eq!(6, map_array.value_offsets()[2]);
        assert_eq!(2, map_array.value_length(2));

        let key_array = Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef;
        let value_array = Arc::new(UInt32Array::from(vec![0u32, 10, 20])) as ArrayRef;
        let keys_field = Arc::new(Field::new("keys", DataType::Utf8, false));
        let values_field = Arc::new(Field::new("values", DataType::UInt32, false));
        let struct_array =
            StructArray::from(vec![(keys_field, key_array), (values_field, value_array)]);
        assert_eq!(
            struct_array,
            StructArray::from(map_array.value(0).into_data())
        );
        assert_eq!(
            &struct_array,
            unsafe { map_array.value_unchecked(0) }
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
        );
        for i in 0..3 {
            assert!(map_array.is_valid(i));
            assert!(!map_array.is_null(i));
        }
    }

    #[test]
    fn test_try_new() {
        let offsets = OffsetBuffer::new(vec![0, 1, 4, 5].into());
        let fields = Fields::from(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("values", DataType::Int32, false),
        ]);
        let columns = vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as _,
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as _,
        ];

        let entries = StructArray::new(fields.clone(), columns, None);
        let field = Arc::new(Field::new("entries", DataType::Struct(fields), false));

        MapArray::new(field.clone(), offsets.clone(), entries.clone(), None, false);

        let nulls = NullBuffer::new_null(3);
        MapArray::new(field.clone(), offsets, entries.clone(), Some(nulls), false);

        let nulls = NullBuffer::new_null(3);
        let offsets = OffsetBuffer::new(vec![0, 1, 2, 4, 5].into());
        let err = MapArray::try_new(
            field.clone(),
            offsets.clone(),
            entries.clone(),
            Some(nulls),
            false,
        )
        .unwrap_err();

        assert_eq!(
            err.to_string(),
            "Invalid argument error: Incorrect length of null buffer for MapArray, expected 4 got 3"
        );

        let err = MapArray::try_new(field, offsets.clone(), entries.slice(0, 2), None, false)
            .unwrap_err();

        assert_eq!(
            err.to_string(),
            "Invalid argument error: Max offset of 5 exceeds length of entries 2"
        );

        let field = Arc::new(Field::new("element", DataType::Int64, false));
        let err = MapArray::try_new(field, offsets.clone(), entries, None, false)
            .unwrap_err()
            .to_string();

        assert!(
            err.starts_with("Invalid argument error: MapArray expected data type Int64 got Struct"),
            "{err}"
        );

        let fields = Fields::from(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]);
        let columns = vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as _,
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as _,
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as _,
        ];

        let s = StructArray::new(fields.clone(), columns, None);
        let field = Arc::new(Field::new("entries", DataType::Struct(fields), false));
        let err = MapArray::try_new(field, offsets, s, None, false).unwrap_err();

        assert_eq!(
            err.to_string(),
            "Invalid argument error: MapArray entries must contain two children, got 3"
        );
    }
}
