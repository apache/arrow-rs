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
        let end = self.value_offsets()[i + 1];
        let start = self.value_offsets()[i];
        self.values
            .slice(start.to_usize().unwrap(), (end - start).to_usize().unwrap())
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

        let values = data.child_data()[0].clone();

        if let DataType::Struct(fields) = values.data_type() {
            if fields.len() != 2 {
                return Err(ArrowError::InvalidArgumentError(format!(
                "MapArray should contain a struct array with 2 fields, have {} fields",
                fields.len()
            )));
            }
        } else {
            return Err(ArrowError::InvalidArgumentError(format!(
                "MapArray should contain a struct array child, found {:?}",
                values.data_type()
            )));
        }

        let values = make_array(values);
        let value_offsets = data.buffers()[0].as_ptr();

        let value_offsets = unsafe { RawPtrBox::<i32>::new(value_offsets) };
        unsafe {
            if (*value_offsets.as_ptr().offset(0)) != 0 {
                return Err(ArrowError::InvalidArgumentError(String::from(
                    "offsets do not start at zero",
                )));
            }
        }
        Ok(Self {
            data,
            values,
            value_offsets,
        })
    }
}

impl Array for MapArray {
    fn as_any(&self) -> &Any {
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

// #[cfg(test)]
// mod tests {
//     use crate::{
//         array::ArrayData,
//         array::{Int32Array, StructArray},
//         buffer::Buffer,
//         datatypes::Field,
//         datatypes::ToByteSlice,
//         util::bit_util,
//     };

//     use super::*;

//     fn create_from_buffers() -> MapArray {
//         // Construct key and values
//         let keys_data = ArrayData::builder(DataType::Int32)
//             .len(8)
//             .add_buffer(Buffer::from(&[0, 1, 2, 3, 4, 5, 6, 7].to_byte_slice()))
//             .build();
//         let values_data = ArrayData::builder(DataType::UInt32)
//             .len(8)
//             .add_buffer(Buffer::from(&[0u32, 1, 2, 3, 4, 5, 6, 7].to_byte_slice()))
//             .build();

//         // Construct a buffer for value offsets, for the nested array:
//         //  [[0, 1, 2], [3, 4, 5], [6, 7]]
//         let entry_offsets = Buffer::from(&[0, 3, 6, 8].to_byte_slice());

//         let keys = Field::new("keys", DataType::Int32, false);
//         let values = Field::new("values", DataType::UInt32, false);
//         let entry_struct = StructArray::from(vec![
//             (keys, make_array(keys_data)),
//             (values, make_array(values_data)),
//         ]);

//         // Construct a map array from the above two
//         let map_data_type = DataType::Map(
//             Box::new(Field::new(
//                 "entries",
//                 entry_struct.data_type().clone(),
//                 true,
//             )),
//             false,
//         );
//         let map_data = ArrayData::builder(map_data_type)
//             .len(3)
//             .add_buffer(entry_offsets)
//             .add_child_data(entry_struct.data().clone())
//             .build();
//         MapArray::from(map_data)
//     }

//     #[test]
//     fn test_map_array() {
//         let map_array = create_from_buffers();

//         let values = map_array.values();
//         assert_eq!(&value_data, values.data());
//         assert_eq!(DataType::Int32, map_array.value_type());
//         assert_eq!(3, map_array.len());
//         assert_eq!(0, map_array.null_count());
//         assert_eq!(6, map_array.value_offsets()[2]);
//         assert_eq!(2, map_array.value_length(2));
//         assert_eq!(
//             0,
//             map_array
//                 .value(0)
//                 .as_any()
//                 .downcast_ref::<Int32Array>()
//                 .unwrap()
//                 .value(0)
//         );
//         assert_eq!(
//             0,
//             unsafe { map_array.value_unchecked(0) }
//                 .as_any()
//                 .downcast_ref::<Int32Array>()
//                 .unwrap()
//                 .value(0)
//         );
//         for i in 0..3 {
//             assert!(map_array.is_valid(i));
//             assert!(!map_array.is_null(i));
//         }

//         // Now test with a non-zero offset
//         let map_data = ArrayData::builder(map_array.data_type().clone())
//             .len(3)
//             .offset(1)
//             .add_buffer(map_array.data().buffers()[0].clone())
//             .add_child_data(map_array.data().child_data()[0].clone())
//             .build();
//         let map_array = MapArray::from(map_data);

//         let values = map_array.values();
//         // assert_eq!(&value_data, values.data());
//         assert_eq!(DataType::Int32, map_array.value_type());
//         assert_eq!(3, map_array.len());
//         assert_eq!(0, map_array.null_count());
//         assert_eq!(6, map_array.value_offsets()[1]);
//         assert_eq!(2, map_array.value_length(1));
//         assert_eq!(
//             3,
//             map_array
//                 .value(0)
//                 .as_any()
//                 .downcast_ref::<StructArray>()
//                 .unwrap()
//                 .value(0)
//         );
//         assert_eq!(
//             3,
//             unsafe { map_array.value_unchecked(0) }
//                 .as_any()
//                 .downcast_ref::<Int32Array>()
//                 .unwrap()
//                 .value(0)
//         );
//     }

//     #[test]
//     fn test_map_array_slice() {
//         // Construct a value array
//         let value_data = ArrayData::builder(DataType::Int32)
//             .len(10)
//             .add_buffer(Buffer::from_slice_ref(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]))
//             .build();

//         // Construct a buffer for value offsets, for the nested array:
//         //  [[0, 1], null, null, [2, 3], [4, 5], null, [6, 7, 8], null, [9]]
//         let value_offsets = Buffer::from_slice_ref(&[0, 2, 2, 2, 4, 6, 6, 9, 9, 10]);
//         // 01011001 00000001
//         let mut null_bits: [u8; 2] = [0; 2];
//         bit_util::set_bit(&mut null_bits, 0);
//         bit_util::set_bit(&mut null_bits, 3);
//         bit_util::set_bit(&mut null_bits, 4);
//         bit_util::set_bit(&mut null_bits, 6);
//         bit_util::set_bit(&mut null_bits, 8);

//         // Construct a map array from the above two
//         let list_data_type =
//             DataType::List(Box::new(Field::new("item", DataType::Int32, false)));
//         let list_data = ArrayData::builder(list_data_type)
//             .len(9)
//             .add_buffer(value_offsets)
//             .add_child_data(value_data.clone())
//             .null_bit_buffer(Buffer::from(null_bits))
//             .build();
//         let list_array = MapArray::from(list_data);

//         let values = list_array.values();
//         assert_eq!(&value_data, values.data());
//         assert_eq!(DataType::Int32, list_array.value_type());
//         assert_eq!(9, list_array.len());
//         assert_eq!(4, list_array.null_count());
//         assert_eq!(2, list_array.value_offsets()[3]);
//         assert_eq!(2, list_array.value_length(3));

//         let sliced_array = list_array.slice(1, 6);
//         assert_eq!(6, sliced_array.len());
//         assert_eq!(1, sliced_array.offset());
//         assert_eq!(3, sliced_array.null_count());

//         for i in 0..sliced_array.len() {
//             if bit_util::get_bit(&null_bits, sliced_array.offset() + i) {
//                 assert!(sliced_array.is_valid(i));
//             } else {
//                 assert!(sliced_array.is_null(i));
//             }
//         }

//         // Check offset and length for each non-null value.
//         let sliced_list_array = sliced_array.as_any().downcast_ref::<MapArray>().unwrap();
//         assert_eq!(2, sliced_list_array.value_offsets()[2]);
//         assert_eq!(2, sliced_list_array.value_length(2));
//         assert_eq!(4, sliced_list_array.value_offsets()[3]);
//         assert_eq!(2, sliced_list_array.value_length(3));
//         assert_eq!(6, sliced_list_array.value_offsets()[5]);
//         assert_eq!(3, sliced_list_array.value_length(5));
//     }

//     #[test]
//     #[should_panic(expected = "index out of bounds: the len is 10 but the index is 11")]
//     fn test_map_array_index_out_of_bound() {
//         // Construct a value array
//         let value_data = ArrayData::builder(DataType::Int32)
//             .len(10)
//             .add_buffer(Buffer::from_slice_ref(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]))
//             .build();

//         // Construct a buffer for value offsets, for the nested array:
//         //  [[0, 1], null, null, [2, 3], [4, 5], null, [6, 7, 8], null, [9]]
//         let value_offsets = Buffer::from_slice_ref(&[0i64, 2, 2, 2, 4, 6, 6, 9, 9, 10]);
//         // 01011001 00000001
//         let mut null_bits: [u8; 2] = [0; 2];
//         bit_util::set_bit(&mut null_bits, 0);
//         bit_util::set_bit(&mut null_bits, 3);
//         bit_util::set_bit(&mut null_bits, 4);
//         bit_util::set_bit(&mut null_bits, 6);
//         bit_util::set_bit(&mut null_bits, 8);

//         // Construct a map array from the above two
//         let list_data_type =
//             DataType::LargeList(Box::new(Field::new("item", DataType::Int32, false)));
//         let list_data = ArrayData::builder(list_data_type)
//             .len(9)
//             .add_buffer(value_offsets)
//             .add_child_data(value_data)
//             .null_bit_buffer(Buffer::from(null_bits))
//             .build();
//         let list_array = MapArray::from(list_data);
//         assert_eq!(9, list_array.len());

//         list_array.value(10);
//     }

//     #[test]
//     #[should_panic(
//         expected = "MapArray data should contain a single buffer only (value offsets)"
//     )]
//     fn test_map_array_invalid_buffer_len() {
//         let value_data = ArrayData::builder(DataType::Int32)
//             .len(8)
//             .add_buffer(Buffer::from_slice_ref(&[0, 1, 2, 3, 4, 5, 6, 7]))
//             .build();
//         let list_data_type =
//             DataType::List(Box::new(Field::new("item", DataType::Int32, false)));
//         let list_data = ArrayData::builder(list_data_type)
//             .len(3)
//             .add_child_data(value_data)
//             .build();
//         MapArray::from(list_data);
//     }

//     #[test]
//     #[should_panic(
//         expected = "MapArray should contain a single child array (values array)"
//     )]
//     fn test_map_array_invalid_child_array_len() {
//         let value_offsets = Buffer::from_slice_ref(&[0, 2, 5, 7]);
//         let list_data_type =
//             DataType::List(Box::new(Field::new("item", DataType::Int32, false)));
//         let list_data = ArrayData::builder(list_data_type)
//             .len(3)
//             .add_buffer(value_offsets)
//             .build();
//         MapArray::from(list_data);
//     }

//     #[test]
//     #[should_panic(expected = "offsets do not start at zero")]
//     fn test_map_array_invalid_value_offset_start() {
//         let value_data = ArrayData::builder(DataType::Int32)
//             .len(8)
//             .add_buffer(Buffer::from_slice_ref(&[0, 1, 2, 3, 4, 5, 6, 7]))
//             .build();

//         let value_offsets = Buffer::from_slice_ref(&[2, 2, 5, 7]);

//         let list_data_type =
//             DataType::List(Box::new(Field::new("item", DataType::Int32, false)));
//         let list_data = ArrayData::builder(list_data_type)
//             .len(3)
//             .add_buffer(value_offsets)
//             .add_child_data(value_data)
//             .build();
//         MapArray::from(list_data);
//     }
// }
