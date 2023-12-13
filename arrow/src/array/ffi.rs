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

//! Contains functionality to load an ArrayData from the C Data Interface

use std::convert::TryFrom;

use crate::{error::Result, ffi};

use super::ArrayRef;

/// Exports an array to raw pointers of the C Data Interface provided by the consumer.
/// # Safety
/// Assumes that these pointers represent valid C Data Interfaces, both in memory
/// representation and lifetime via the `release` mechanism.
///
/// This function copies the content of two FFI structs [ffi::FFI_ArrowArray] and
/// [ffi::FFI_ArrowSchema] in the array to the location pointed by the raw pointers.
/// Usually the raw pointers are provided by the array data consumer.
#[deprecated(note = "Use FFI_ArrowArray::new and FFI_ArrowSchema::try_from")]
pub unsafe fn export_array_into_raw(
    src: ArrayRef,
    out_array: *mut ffi::FFI_ArrowArray,
    out_schema: *mut ffi::FFI_ArrowSchema,
) -> Result<()> {
    let data = src.to_data();
    let array = ffi::FFI_ArrowArray::new(&data);
    let schema = ffi::FFI_ArrowSchema::try_from(data.data_type())?;

    std::ptr::write_unaligned(out_array, array);
    std::ptr::write_unaligned(out_schema, schema);

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::array::{DictionaryArray, FixedSizeListArray, Int32Array, StringArray};
    use crate::buffer::Buffer;
    use crate::error::Result;
    use crate::util::bit_util;
    use crate::{
        array::{
            Array, ArrayData, BooleanArray, FixedSizeBinaryArray, Int64Array, StructArray,
            UInt32Array, UInt64Array,
        },
        datatypes::{DataType, Field},
        ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema},
    };
    use std::convert::TryFrom;
    use std::sync::Arc;

    fn test_round_trip(expected: &ArrayData) -> Result<()> {
        // here we export the array
        let array = FFI_ArrowArray::new(expected);
        let schema = FFI_ArrowSchema::try_from(expected.data_type())?;

        // simulate an external consumer by being the consumer
        let result = &unsafe { from_ffi(array, &schema) }?;

        assert_eq!(result, expected);
        Ok(())
    }

    #[test]
    fn test_u32() -> Result<()> {
        let array = UInt32Array::from(vec![Some(2), None, Some(1), None]);
        let data = array.into_data();
        test_round_trip(&data)
    }

    #[test]
    fn test_u64() -> Result<()> {
        let array = UInt64Array::from(vec![Some(2), None, Some(1), None]);
        let data = array.into_data();
        test_round_trip(&data)
    }

    #[test]
    fn test_i64() -> Result<()> {
        let array = Int64Array::from(vec![Some(2), None, Some(1), None]);
        let data = array.into_data();
        test_round_trip(&data)
    }

    #[test]
    fn test_struct() -> Result<()> {
        let inner = StructArray::from(vec![
            (
                Arc::new(Field::new("a1", DataType::Boolean, false)),
                Arc::new(BooleanArray::from(vec![true, true, false, false])) as Arc<dyn Array>,
            ),
            (
                Arc::new(Field::new("a2", DataType::UInt32, false)),
                Arc::new(UInt32Array::from(vec![1, 2, 3, 4])),
            ),
        ]);

        let array = StructArray::from(vec![
            (
                Arc::new(Field::new("a", inner.data_type().clone(), false)),
                Arc::new(inner) as Arc<dyn Array>,
            ),
            (
                Arc::new(Field::new("b", DataType::Boolean, false)),
                Arc::new(BooleanArray::from(vec![false, false, true, true])) as Arc<dyn Array>,
            ),
            (
                Arc::new(Field::new("c", DataType::UInt32, false)),
                Arc::new(UInt32Array::from(vec![42, 28, 19, 31])),
            ),
        ]);
        let data = array.into_data();
        test_round_trip(&data)
    }

    #[test]
    fn test_dictionary() -> Result<()> {
        let values = StringArray::from(vec![Some("foo"), Some("bar"), None]);
        let keys = Int32Array::from(vec![
            Some(0),
            Some(1),
            None,
            Some(1),
            Some(1),
            None,
            Some(1),
            Some(2),
            Some(1),
            None,
        ]);
        let array = DictionaryArray::new(keys, Arc::new(values));

        let data = array.into_data();
        test_round_trip(&data)
    }

    #[test]
    fn test_fixed_size_binary() -> Result<()> {
        let values = vec![vec![10, 10, 10], vec![20, 20, 20], vec![30, 30, 30]];
        let array = FixedSizeBinaryArray::try_from_iter(values.into_iter())?;

        let data = array.into_data();
        test_round_trip(&data)
    }

    #[test]
    fn test_fixed_size_binary_with_nulls() -> Result<()> {
        let values = vec![
            None,
            Some(vec![10, 10, 10]),
            None,
            Some(vec![20, 20, 20]),
            Some(vec![30, 30, 30]),
            None,
        ];
        let array = FixedSizeBinaryArray::try_from_sparse_iter_with_size(values.into_iter(), 3)?;

        let data = array.into_data();
        test_round_trip(&data)
    }

    #[test]
    fn test_fixed_size_list() -> Result<()> {
        let v: Vec<i64> = (0..9).collect();
        let value_data = ArrayData::builder(DataType::Int64)
            .len(9)
            .add_buffer(Buffer::from_slice_ref(v))
            .build()?;
        let list_data_type =
            DataType::FixedSizeList(Arc::new(Field::new("f", DataType::Int64, false)), 3);
        let list_data = ArrayData::builder(list_data_type)
            .len(3)
            .add_child_data(value_data)
            .build()?;
        let array = FixedSizeListArray::from(list_data);

        let data = array.into_data();
        test_round_trip(&data)
    }

    #[test]
    fn test_fixed_size_list_with_nulls() -> Result<()> {
        // 0100 0110
        let mut validity_bits: [u8; 1] = [0; 1];
        bit_util::set_bit(&mut validity_bits, 1);
        bit_util::set_bit(&mut validity_bits, 2);
        bit_util::set_bit(&mut validity_bits, 6);

        let v: Vec<i16> = (0..16).collect();
        let value_data = ArrayData::builder(DataType::Int16)
            .len(16)
            .add_buffer(Buffer::from_slice_ref(v))
            .build()?;
        let list_data_type =
            DataType::FixedSizeList(Arc::new(Field::new("f", DataType::Int16, false)), 2);
        let list_data = ArrayData::builder(list_data_type)
            .len(8)
            .null_bit_buffer(Some(Buffer::from(validity_bits)))
            .add_child_data(value_data)
            .build()?;
        let array = FixedSizeListArray::from(list_data);

        let data = array.into_data();
        test_round_trip(&data)
    }

    #[test]
    fn test_fixed_size_list_nested() -> Result<()> {
        let v: Vec<i32> = (0..16).collect();
        let value_data = ArrayData::builder(DataType::Int32)
            .len(16)
            .add_buffer(Buffer::from_slice_ref(v))
            .build()?;

        let offsets: Vec<i32> = vec![0, 2, 4, 6, 8, 10, 12, 14, 16];
        let value_offsets = Buffer::from_slice_ref(offsets);
        let inner_list_data_type =
            DataType::List(Arc::new(Field::new("item", DataType::Int32, false)));
        let inner_list_data = ArrayData::builder(inner_list_data_type.clone())
            .len(8)
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .build()?;

        // 0000 0100
        let mut validity_bits: [u8; 1] = [0; 1];
        bit_util::set_bit(&mut validity_bits, 2);

        let list_data_type =
            DataType::FixedSizeList(Arc::new(Field::new("f", inner_list_data_type, false)), 2);
        let list_data = ArrayData::builder(list_data_type)
            .len(4)
            .null_bit_buffer(Some(Buffer::from(validity_bits)))
            .add_child_data(inner_list_data)
            .build()?;

        let array = FixedSizeListArray::from(list_data);

        let data = array.into_data();
        test_round_trip(&data)
    }
}
