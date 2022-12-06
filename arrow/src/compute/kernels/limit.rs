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

//! Defines miscellaneous array kernels.

use crate::array::ArrayRef;

/// Returns the array, taking only the number of elements specified
///
/// Limit performs a zero-copy slice of the array, and is a convenience method on slice
/// where:
/// * it performs a bounds-check on the array
/// * it slices from offset 0
#[deprecated(note = "Use Array::slice")]
pub fn limit(array: &ArrayRef, num_elements: usize) -> ArrayRef {
    let lim = num_elements.min(array.len());
    array.slice(0, lim)
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use super::*;
    use crate::array::*;
    use crate::buffer::Buffer;
    use crate::datatypes::{DataType, Field};
    use crate::util::bit_util;

    use std::sync::Arc;

    #[test]
    fn test_limit_array() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![5, 6, 7, 8, 9]));
        let b = limit(&a, 3);
        let c = b.as_ref().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(3, c.len());
        assert_eq!(5, c.value(0));
        assert_eq!(6, c.value(1));
        assert_eq!(7, c.value(2));
    }

    #[test]
    fn test_limit_string_array() {
        let a: ArrayRef = Arc::new(StringArray::from(vec!["hello", " ", "world", "!"]));
        let b = limit(&a, 2);
        let c = b.as_ref().as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(2, c.len());
        assert_eq!("hello", c.value(0));
        assert_eq!(" ", c.value(1));
    }

    #[test]
    fn test_limit_array_with_null() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![None, Some(5)]));
        let b = limit(&a, 1);
        let c = b.as_ref().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(1, c.len());
        assert!(c.is_null(0));
    }

    #[test]
    fn test_limit_array_with_limit_too_large() {
        let a = Int32Array::from(vec![5, 6, 7, 8, 9]);
        let a_ref: ArrayRef = Arc::new(a);
        let b = limit(&a_ref, 6);
        let c = b.as_ref().as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(5, c.len());
        assert_eq!(5, c.value(0));
        assert_eq!(6, c.value(1));
        assert_eq!(7, c.value(2));
        assert_eq!(8, c.value(3));
        assert_eq!(9, c.value(4));
    }

    #[test]
    fn test_list_array_limit() {
        // adapted from crate::array::test::test_list_array_slice
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(10)
            .add_buffer(Buffer::from_slice_ref([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]))
            .build()
            .unwrap();

        // Construct a buffer for value offsets, for the nested array:
        //  [[0, 1], null, [2, 3], null, [4, 5], null, [6, 7, 8], null, [9]]
        let value_offsets = Buffer::from_slice_ref([0, 2, 2, 4, 4, 6, 6, 9, 9, 10]);
        // 01010101 00000001
        let mut null_bits: [u8; 2] = [0; 2];
        bit_util::set_bit(&mut null_bits, 0);
        bit_util::set_bit(&mut null_bits, 2);
        bit_util::set_bit(&mut null_bits, 4);
        bit_util::set_bit(&mut null_bits, 6);
        bit_util::set_bit(&mut null_bits, 8);

        // Construct a list array from the above two
        let list_data_type =
            DataType::List(Box::new(Field::new("item", DataType::Int32, false)));
        let list_data = ArrayData::builder(list_data_type)
            .len(9)
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .null_bit_buffer(Some(Buffer::from(null_bits)))
            .build()
            .unwrap();
        let list_array: ArrayRef = Arc::new(ListArray::from(list_data));

        let limit_array = limit(&list_array, 6);
        assert_eq!(6, limit_array.len());
        assert_eq!(0, limit_array.offset());
        assert_eq!(3, limit_array.null_count());

        // Check offset and length for each non-null value.
        let limit_array: &ListArray =
            limit_array.as_any().downcast_ref::<ListArray>().unwrap();

        for i in 0..limit_array.len() {
            let offset = limit_array.value_offsets()[i];
            let length = limit_array.value_length(i);
            if i % 2 == 0 {
                assert_eq!(2, length);
                assert_eq!(i as i32, offset);
            } else {
                assert_eq!(0, length);
            }
        }
    }

    #[test]
    fn test_struct_array_limit() {
        // adapted from crate::array::test::test_struct_array_slice
        let boolean_data = ArrayData::builder(DataType::Boolean)
            .len(5)
            .add_buffer(Buffer::from([0b00010000]))
            .null_bit_buffer(Some(Buffer::from([0b00010001])))
            .build()
            .unwrap();
        let int_data = ArrayData::builder(DataType::Int32)
            .len(5)
            .add_buffer(Buffer::from_slice_ref([0, 28, 42, 0, 0]))
            .null_bit_buffer(Some(Buffer::from([0b00000110])))
            .build()
            .unwrap();

        let field_types = vec![
            Field::new("a", DataType::Boolean, true),
            Field::new("b", DataType::Int32, true),
        ];
        let struct_array_data = ArrayData::builder(DataType::Struct(field_types))
            .len(5)
            .add_child_data(boolean_data.clone())
            .add_child_data(int_data.clone())
            .null_bit_buffer(Some(Buffer::from([0b00010111])))
            .build()
            .unwrap();
        let struct_array = StructArray::from(struct_array_data);

        assert_eq!(5, struct_array.len());
        assert_eq!(1, struct_array.null_count());
        assert_eq!(&boolean_data, struct_array.column(0).data());
        assert_eq!(&int_data, struct_array.column(1).data());

        let array: ArrayRef = Arc::new(struct_array);

        let sliced_array = limit(&array, 3);
        let sliced_array = sliced_array.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(3, sliced_array.len());
        assert_eq!(0, sliced_array.offset());
        assert_eq!(0, sliced_array.null_count());
        assert!(sliced_array.is_valid(0));
        assert!(sliced_array.is_valid(1));
        assert!(sliced_array.is_valid(2));

        let sliced_c0 = sliced_array.column(0);
        let sliced_c0 = sliced_c0.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(3, sliced_c0.len());
        assert_eq!(0, sliced_c0.offset());
        assert_eq!(2, sliced_c0.null_count());
        assert!(sliced_c0.is_valid(0));
        assert!(sliced_c0.is_null(1));
        assert!(sliced_c0.is_null(2));
        assert!(!sliced_c0.value(0));

        let sliced_c1 = sliced_array.column(1);
        let sliced_c1 = sliced_c1.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(3, sliced_c1.len());
        assert_eq!(0, sliced_c1.offset());
        assert_eq!(1, sliced_c1.null_count());
        assert!(sliced_c1.is_null(0));
        assert_eq!(28, sliced_c1.value(1));
        assert_eq!(42, sliced_c1.value(2));
    }
}
