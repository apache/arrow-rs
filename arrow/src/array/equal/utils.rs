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

use crate::array::{data::count_nulls, ArrayData, OffsetSizeTrait};
use crate::bitmap::Bitmap;
use crate::buffer::{Buffer, MutableBuffer};
use crate::datatypes::{DataType, UnionMode};
use crate::util::bit_util;

// whether bits along the positions are equal
// `lhs_start`, `rhs_start` and `len` are _measured in bits_.
#[inline]
pub(super) fn equal_bits(
    lhs_values: &[u8],
    rhs_values: &[u8],
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    (0..len).all(|i| {
        bit_util::get_bit(lhs_values, lhs_start + i)
            == bit_util::get_bit(rhs_values, rhs_start + i)
    })
}

#[inline]
pub(super) fn equal_nulls(
    lhs: &ArrayData,
    rhs: &ArrayData,
    lhs_nulls: Option<&Buffer>,
    rhs_nulls: Option<&Buffer>,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    let lhs_null_count = count_nulls(lhs_nulls, lhs_start, len);
    let rhs_null_count = count_nulls(rhs_nulls, rhs_start, len);
    if lhs_null_count > 0 || rhs_null_count > 0 {
        let lhs_values = lhs_nulls.unwrap().as_slice();
        let rhs_values = rhs_nulls.unwrap().as_slice();
        equal_bits(
            lhs_values,
            rhs_values,
            lhs_start + lhs.offset(),
            rhs_start + rhs.offset(),
            len,
        )
    } else {
        true
    }
}

#[inline]
pub(super) fn base_equal(lhs: &ArrayData, rhs: &ArrayData) -> bool {
    let equal_type = match (lhs.data_type(), rhs.data_type()) {
        (DataType::Union(l_fields, l_mode), DataType::Union(r_fields, r_mode)) => {
            // Defer field datatype check to `union_equal`
            l_fields.len() == r_fields.len() && l_mode == r_mode
        }
        (DataType::Map(l_field, l_sorted), DataType::Map(r_field, r_sorted)) => {
            let field_equal = match (l_field.data_type(), r_field.data_type()) {
                (DataType::Struct(l_fields), DataType::Struct(r_fields))
                    if l_fields.len() == 2 && r_fields.len() == 2 =>
                {
                    let l_key_field = l_fields.get(0).unwrap();
                    let r_key_field = r_fields.get(0).unwrap();
                    let l_value_field = l_fields.get(1).unwrap();
                    let r_value_field = r_fields.get(1).unwrap();

                    // We don't enforce the equality of field names
                    let data_type_equal = l_key_field.data_type()
                        == r_key_field.data_type()
                        && l_value_field.data_type() == r_value_field.data_type();
                    let nullability_equal = l_key_field.is_nullable()
                        == r_key_field.is_nullable()
                        && l_value_field.is_nullable() == r_value_field.is_nullable();
                    let metadata_equal = l_key_field.metadata() == r_key_field.metadata()
                        && l_value_field.metadata() == r_value_field.metadata();
                    data_type_equal && nullability_equal && metadata_equal
                }
                _ => panic!("Map type should have 2 fields Struct in its field"),
            };
            field_equal && l_sorted == r_sorted
        }
        (l_data_type, r_data_type) => l_data_type == r_data_type,
    };
    equal_type && lhs.len() == rhs.len()
}

// whether the two memory regions are equal
#[inline]
pub(super) fn equal_len(
    lhs_values: &[u8],
    rhs_values: &[u8],
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    lhs_values[lhs_start..(lhs_start + len)] == rhs_values[rhs_start..(rhs_start + len)]
}

/// Computes the logical validity bitmap of the array data using the
/// parent's array data. The parent should be a list or struct, else
/// the logical bitmap of the array is returned unaltered.
///
/// Parent data is passed along with the parent's logical bitmap, as
/// nested arrays could have a logical bitmap different to the physical
/// one on the `ArrayData`.
pub(super) fn child_logical_null_buffer(
    parent_data: &ArrayData,
    logical_null_buffer: Option<&Buffer>,
    child_data: &ArrayData,
) -> Option<Buffer> {
    let parent_len = parent_data.len();
    let parent_bitmap = logical_null_buffer
        .cloned()
        .map(Bitmap::from)
        .unwrap_or_else(|| {
            let ceil = bit_util::ceil(parent_len, 8);
            Bitmap::from(Buffer::from(vec![0b11111111; ceil]))
        });
    let self_null_bitmap = child_data.null_bitmap().clone().unwrap_or_else(|| {
        let ceil = bit_util::ceil(child_data.len(), 8);
        Bitmap::from(Buffer::from(vec![0b11111111; ceil]))
    });
    match parent_data.data_type() {
        DataType::List(_) | DataType::Map(_, _) => Some(logical_list_bitmap::<i32>(
            parent_data,
            parent_bitmap,
            self_null_bitmap,
        )),
        DataType::LargeList(_) => Some(logical_list_bitmap::<i64>(
            parent_data,
            parent_bitmap,
            self_null_bitmap,
        )),
        DataType::FixedSizeList(_, len) => {
            let len = *len as usize;
            let array_offset = parent_data.offset();
            let bitmap_len = bit_util::ceil(parent_len * len, 8);
            let mut buffer = MutableBuffer::from_len_zeroed(bitmap_len);
            let null_slice = buffer.as_slice_mut();
            (array_offset..parent_len + array_offset).for_each(|index| {
                let start = index * len;
                let end = start + len;
                let mask = parent_bitmap.is_set(index);
                (start..end).for_each(|child_index| {
                    if mask && self_null_bitmap.is_set(child_index) {
                        bit_util::set_bit(null_slice, child_index);
                    }
                });
            });
            Some(buffer.into())
        }
        DataType::Struct(_) => {
            // Arrow implementations are free to pad data, which can result in null buffers not
            // having the same length.
            // Rust bitwise comparisons will return an error if left AND right is performed on
            // buffers of different length.
            // This might be a valid case during integration testing, where we read Arrow arrays
            // from IPC data, which has padding.
            //
            // We first perform a bitwise comparison, and if there is an error, we revert to a
            // slower method that indexes into the buffers one-by-one.
            let result = &parent_bitmap & &self_null_bitmap;
            if let Ok(bitmap) = result {
                return Some(bitmap.bits);
            }
            // slow path
            let array_offset = parent_data.offset();
            let mut buffer = MutableBuffer::new_null(parent_len);
            let null_slice = buffer.as_slice_mut();
            (0..parent_len).for_each(|index| {
                if parent_bitmap.is_set(index + array_offset)
                    && self_null_bitmap.is_set(index + array_offset)
                {
                    bit_util::set_bit(null_slice, index);
                }
            });
            Some(buffer.into())
        }
        DataType::Union(_, mode) => union_child_logical_null_buffer(
            parent_data,
            parent_len,
            &parent_bitmap,
            &self_null_bitmap,
            mode,
        ),
        DataType::Dictionary(_, _) => {
            unimplemented!("Logical equality not yet implemented for nested dictionaries")
        }
        data_type => panic!("Data type {:?} is not a supported nested type", data_type),
    }
}

pub(super) fn union_child_logical_null_buffer(
    parent_data: &ArrayData,
    parent_len: usize,
    parent_bitmap: &Bitmap,
    self_null_bitmap: &Bitmap,
    mode: &UnionMode,
) -> Option<Buffer> {
    match mode {
        UnionMode::Sparse => {
            // See the logic of `DataType::Struct` in `child_logical_null_buffer`.
            let result = parent_bitmap & self_null_bitmap;
            if let Ok(bitmap) = result {
                return Some(bitmap.bits);
            }

            // slow path
            let array_offset = parent_data.offset();
            let mut buffer = MutableBuffer::new_null(parent_len);
            let null_slice = buffer.as_slice_mut();
            (0..parent_len).for_each(|index| {
                if parent_bitmap.is_set(index + array_offset)
                    && self_null_bitmap.is_set(index + array_offset)
                {
                    bit_util::set_bit(null_slice, index);
                }
            });
            Some(buffer.into())
        }
        UnionMode::Dense => {
            // We don't keep bitmap in child data of Dense UnionArray
            unimplemented!("Logical equality not yet implemented for dense union arrays")
        }
    }
}

// Calculate a list child's logical bitmap/buffer
#[inline]
fn logical_list_bitmap<OffsetSize: OffsetSizeTrait>(
    parent_data: &ArrayData,
    parent_bitmap: Bitmap,
    child_bitmap: Bitmap,
) -> Buffer {
    let offsets = parent_data.buffer::<OffsetSize>(0);
    let offset_start = offsets.first().unwrap().to_usize().unwrap();
    let offset_len = offsets.get(parent_data.len()).unwrap().to_usize().unwrap();
    let mut buffer = MutableBuffer::new_null(offset_len - offset_start);
    let null_slice = buffer.as_slice_mut();

    offsets
        .windows(2)
        .enumerate()
        .take(parent_data.len())
        .for_each(|(index, window)| {
            let start = window[0].to_usize().unwrap();
            let end = window[1].to_usize().unwrap();
            let mask = parent_bitmap.is_set(index);
            (start..end).for_each(|child_index| {
                if mask && child_bitmap.is_set(child_index) {
                    bit_util::set_bit(null_slice, child_index - offset_start);
                }
            });
        });
    buffer.into()
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::datatypes::{Field, ToByteSlice};

    #[test]
    fn test_logical_null_buffer() {
        let child_data = ArrayData::builder(DataType::Int32)
            .len(11)
            .add_buffer(Buffer::from(
                vec![1i32, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11].to_byte_slice(),
            ))
            .build()
            .unwrap();

        let data = ArrayData::builder(DataType::List(Box::new(Field::new(
            "item",
            DataType::Int32,
            false,
        ))))
        .len(7)
        .add_buffer(Buffer::from(vec![0, 0, 3, 5, 6, 9, 10, 11].to_byte_slice()))
        .null_bit_buffer(Buffer::from(vec![0b01011010]))
        .add_child_data(child_data.clone())
        .build()
        .unwrap();

        // Get the child logical null buffer. The child is non-nullable, but because the list has nulls,
        // we expect the child to logically have some nulls, inherited from the parent:
        // [1, 2, 3, null, null, 6, 7, 8, 9, null, 11]
        let nulls = child_logical_null_buffer(
            &data,
            data.null_buffer(),
            data.child_data().get(0).unwrap(),
        );
        let expected = Some(Buffer::from(vec![0b11100111, 0b00000101]));
        assert_eq!(nulls, expected);

        // test with offset
        let data = ArrayData::builder(DataType::List(Box::new(Field::new(
            "item",
            DataType::Int32,
            false,
        ))))
        .len(4)
        .offset(3)
        .add_buffer(Buffer::from(vec![0, 0, 3, 5, 6, 9, 10, 11].to_byte_slice()))
        // the null_bit_buffer doesn't have an offset, i.e. cleared the 3 offset bits 0b[---]01011[010]
        .null_bit_buffer(Buffer::from(vec![0b00001011]))
        .add_child_data(child_data)
        .build()
        .unwrap();

        let nulls = child_logical_null_buffer(
            &data,
            data.null_buffer(),
            data.child_data().get(0).unwrap(),
        );

        let expected = Some(Buffer::from(vec![0b00101111]));
        assert_eq!(nulls, expected);
    }
}
