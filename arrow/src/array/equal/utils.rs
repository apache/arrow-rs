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

use crate::array::{data::count_nulls, ArrayData};
use crate::datatypes::DataType;
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
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    let lhs_null_count = count_nulls(lhs.null_buffer(), lhs_start + lhs.offset(), len);
    let rhs_null_count = count_nulls(rhs.null_buffer(), rhs_start + rhs.offset(), len);

    if lhs_null_count != rhs_null_count {
        return false;
    }

    if lhs_null_count > 0 || rhs_null_count > 0 {
        let lhs_values = lhs.null_buffer().unwrap().as_slice();
        let rhs_values = rhs.null_buffer().unwrap().as_slice();
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
            l_fields == r_fields && l_mode == r_mode
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
