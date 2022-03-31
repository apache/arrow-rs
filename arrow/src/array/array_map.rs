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
use std::fmt;
use std::mem;

use super::make_array;
use super::{
    array::print_long_array, raw_pointer::RawPtrBox, Array, ArrayData, ArrayRef,
};
use crate::datatypes::{ArrowNativeType, DataType};
use crate::error::ArrowError;

/// A nested array type where each record is a key-value map.
/// Keys should always be non-null, but values can be null.
///
/// [MapArray] is physically a [crate::array::ListArray] that has a
/// [crate::array::StructArray] with 2 child fields.
pub struct MapArray {
    data: ArrayData,
    values: ArrayRef,
    value_offsets: RawPtrBox<i32>,
}

impl MapArray {
    /// Returns a reference to the keys of this map.
    pub fn keys(&self) -> ArrayRef {
        make_array(self.values.data().child_data()[0].clone())
    }

    /// Returns a reference to the values of this map.
    pub fn values(&self) -> ArrayRef {
        make_array(self.values.data().child_data()[1].clone())
    }

    /// Returns the data type of the map's keys.
    pub fn key_type(&self) -> DataType {
        self.values.data().child_data()[0].data_type().clone()
    }

    /// Returns the data type of the map's values.
    pub fn value_type(&self) -> DataType {
        self.values.data().child_data()[1].data_type().clone()
    }

    /// Returns ith value of this map array.
    /// # Safety
    /// Caller must ensure that the index is within the array bounds
    pub unsafe fn value_unchecked(&self, i: usize) -> ArrayRef {
        let end = *self.value_offsets().get_unchecked(i + 1);
        let start = *self.value_offsets().get_unchecked(i);
        self.values
            .slice(start.to_usize().unwrap(), (end - start).to_usize().unwrap())
    }

    /// Returns ith value of this map array.
    pub fn value(&self, i: usize) -> ArrayRef {
        let end = self.value_offsets()[i + 1] as usize;
        let start = self.value_offsets()[i] as usize;
        self.values.slice(start, end - start)
    }

    /// Returns the offset values in the offsets buffer
    #[inline]
    pub fn value_offsets(&self) -> &[i32] {
        // Soundness
        //     pointer alignment & location is ensured by RawPtrBox
        //     buffer bounds/offset is ensured by the ArrayData instance.
        unsafe {
            std::slice::from_raw_parts(
                self.value_offsets.as_ptr().add(self.data.offset()),
                self.len() + 1,
            )
        }
    }

    /// Returns the length for value at index `i`.
    #[inline]
    pub fn value_length(&self, i: usize) -> i32 {
        let offsets = self.value_offsets();
        offsets[i + 1] - offsets[i]
    }
}

impl From<ArrayData> for MapArray {
    fn from(data: ArrayData) -> Self {
        Self::try_new_from_array_data(data)
            .expect("Expected infallable creation of MapArray from ArrayData failed")
    }
}

impl MapArray {
    fn try_new_from_array_data(data: ArrayData) -> Result<Self, ArrowError> {
        if data.buffers().len() != 1 {
            return Err(ArrowError::InvalidArgumentError(
                format!("MapArray data should contain a single buffer only (value offsets), had {}",
                        data.len())));
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

        let values = make_array(entries);
        let value_offsets = data.buffers()[0].as_ptr();

        let value_offsets = unsafe { RawPtrBox::<i32>::new(value_offsets) };
        Ok(Self {
            data,
            values,
            value_offsets,
        })
    }
}

impl Array for MapArray {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data(&self) -> &ArrayData {
        &self.data
    }

    /// Returns the total number of bytes of memory occupied by the buffers owned by this [MapArray].
    fn get_buffer_memory_size(&self) -> usize {
        self.data.get_buffer_memory_size()
    }

    /// Returns the total number of bytes of memory occupied physically by this [MapArray].
    fn get_array_memory_size(&self) -> usize {
        self.data.get_array_memory_size() + mem::size_of_val(self)
    }
}

impl fmt::Debug for MapArray {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MapArray\n[\n")?;
        print_long_array(self, f, |array, index, f| {
            fmt::Debug::fmt(&array.value(index), f)
        })?;
        write!(f, "]")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        array::ArrayData,
        array::{Int32Array, StructArray, UInt32Array},
        buffer::Buffer,
        datatypes::Field,
        datatypes::ToByteSlice,
    };

    use super::*;

    fn create_from_buffers() -> MapArray {
        // Construct key and values
        let keys_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from(&[0, 1, 2, 3, 4, 5, 6, 7].to_byte_slice()))
            .build()
            .unwrap();
        let values_data = ArrayData::builder(DataType::UInt32)
            .len(8)
            .add_buffer(Buffer::from(
                &[0u32, 10, 20, 30, 40, 50, 60, 70].to_byte_slice(),
            ))
            .build()
            .unwrap();

        // Construct a buffer for value offsets, for the nested array:
        //  [[0, 1, 2], [3, 4, 5], [6, 7]]
        let entry_offsets = Buffer::from(&[0, 3, 6, 8].to_byte_slice());

        let keys = Field::new("keys", DataType::Int32, false);
        let values = Field::new("values", DataType::UInt32, false);
        let entry_struct = StructArray::from(vec![
            (keys, make_array(keys_data)),
            (values, make_array(values_data)),
        ]);

        // Construct a map array from the above two
        let map_data_type = DataType::Map(
            Box::new(Field::new(
                "entries",
                entry_struct.data_type().clone(),
                true,
            )),
            false,
        );
        let map_data = ArrayData::builder(map_data_type)
            .len(3)
            .add_buffer(entry_offsets)
            .add_child_data(entry_struct.data().clone())
            .build()
            .unwrap();
        MapArray::from(map_data)
    }

    #[test]
    fn test_map_array() {
        // Construct key and values
        let key_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from(&[0, 1, 2, 3, 4, 5, 6, 7].to_byte_slice()))
            .build()
            .unwrap();
        let value_data = ArrayData::builder(DataType::UInt32)
            .len(8)
            .add_buffer(Buffer::from(
                &[0u32, 10, 20, 0, 40, 0, 60, 70].to_byte_slice(),
            ))
            .null_bit_buffer(Buffer::from(&[0b11010110]))
            .build()
            .unwrap();

        // Construct a buffer for value offsets, for the nested array:
        //  [[0, 1, 2], [3, 4, 5], [6, 7]]
        let entry_offsets = Buffer::from(&[0, 3, 6, 8].to_byte_slice());

        let keys_field = Field::new("keys", DataType::Int32, false);
        let values_field = Field::new("values", DataType::UInt32, true);
        let entry_struct = StructArray::from(vec![
            (keys_field.clone(), make_array(key_data)),
            (values_field.clone(), make_array(value_data.clone())),
        ]);

        // Construct a map array from the above two
        let map_data_type = DataType::Map(
            Box::new(Field::new(
                "entries",
                entry_struct.data_type().clone(),
                true,
            )),
            false,
        );
        let map_data = ArrayData::builder(map_data_type)
            .len(3)
            .add_buffer(entry_offsets)
            .add_child_data(entry_struct.data().clone())
            .build()
            .unwrap();
        let map_array = MapArray::from(map_data);

        let values = map_array.values();
        assert_eq!(&value_data, values.data());
        assert_eq!(DataType::UInt32, map_array.value_type());
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
            StructArray::from(map_array.value(0).data().clone())
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
        let map_data = ArrayData::builder(map_array.data_type().clone())
            .len(2)
            .offset(1)
            .add_buffer(map_array.data().buffers()[0].clone())
            .add_child_data(map_array.data().child_data()[0].clone())
            .build()
            .unwrap();
        let map_array = MapArray::from(map_data);

        let values = map_array.values();
        assert_eq!(&value_data, values.data());
        assert_eq!(DataType::UInt32, map_array.value_type());
        assert_eq!(2, map_array.len());
        assert_eq!(0, map_array.null_count());
        assert_eq!(6, map_array.value_offsets()[1]);
        assert_eq!(2, map_array.value_length(1));

        let key_array = Arc::new(Int32Array::from(vec![3, 4, 5])) as ArrayRef;
        let value_array =
            Arc::new(UInt32Array::from(vec![None, Some(40), None])) as ArrayRef;
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
        let sliced_array_data = sliced_array.data();
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
            .add_buffer(Buffer::from(&[3, 4, 5, 6, 7].to_byte_slice()))
            .build()
            .unwrap();
        let values_data = ArrayData::builder(DataType::UInt32)
            .len(5)
            .add_buffer(Buffer::from(&[30u32, 40, 50, 60, 70].to_byte_slice()))
            .build()
            .unwrap();

        // Construct a buffer for value offsets, for the nested array:
        //  [[3, 4, 5], [6, 7]]
        let entry_offsets = Buffer::from(&[0, 3, 5].to_byte_slice());

        let keys = Field::new("keys", DataType::Int32, false);
        let values = Field::new("values", DataType::UInt32, false);
        let entry_struct = StructArray::from(vec![
            (keys, make_array(keys_data)),
            (values, make_array(values_data)),
        ]);

        // Construct a map array from the above two
        let map_data_type = DataType::Map(
            Box::new(Field::new(
                "entries",
                entry_struct.data_type().clone(),
                true,
            )),
            false,
        );
        let expected_map_data = ArrayData::builder(map_data_type)
            .len(2)
            .add_buffer(entry_offsets)
            .add_child_data(entry_struct.data().clone())
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
}
