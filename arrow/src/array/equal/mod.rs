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

//! Module containing functionality to compute array equality.
//! This module uses [ArrayData] and does not
//! depend on dynamic casting of `Array`.

use super::{
    Array, ArrayData, BooleanArray, Decimal128Array, DictionaryArray,
    FixedSizeBinaryArray, FixedSizeListArray, GenericBinaryArray, GenericListArray,
    GenericStringArray, MapArray, NullArray, OffsetSizeTrait, PrimitiveArray,
    StructArray,
};
use crate::datatypes::{ArrowPrimitiveType, DataType, IntervalUnit};
use half::f16;

mod boolean;
mod decimal;
mod dictionary;
mod fixed_binary;
mod fixed_list;
mod list;
mod null;
mod primitive;
mod structure;
mod union;
mod utils;
mod variable_size;

// these methods assume the same type, len and null count.
// For this reason, they are not exposed and are instead used
// to build the generic functions below (`equal_range` and `equal`).
use boolean::boolean_equal;
use decimal::decimal_equal;
use dictionary::dictionary_equal;
use fixed_binary::fixed_binary_equal;
use fixed_list::fixed_list_equal;
use list::list_equal;
use null::null_equal;
use primitive::primitive_equal;
use structure::struct_equal;
use union::union_equal;
use variable_size::variable_sized_equal;

impl PartialEq for dyn Array {
    fn eq(&self, other: &Self) -> bool {
        equal(self.data(), other.data())
    }
}

impl<T: Array> PartialEq<T> for dyn Array {
    fn eq(&self, other: &T) -> bool {
        equal(self.data(), other.data())
    }
}

impl PartialEq for NullArray {
    fn eq(&self, other: &NullArray) -> bool {
        equal(self.data(), other.data())
    }
}

impl<T: ArrowPrimitiveType> PartialEq for PrimitiveArray<T> {
    fn eq(&self, other: &PrimitiveArray<T>) -> bool {
        equal(self.data(), other.data())
    }
}

impl<K: ArrowPrimitiveType> PartialEq for DictionaryArray<K> {
    fn eq(&self, other: &Self) -> bool {
        equal(self.data(), other.data())
    }
}

impl PartialEq for BooleanArray {
    fn eq(&self, other: &BooleanArray) -> bool {
        equal(self.data(), other.data())
    }
}

impl<OffsetSize: OffsetSizeTrait> PartialEq for GenericStringArray<OffsetSize> {
    fn eq(&self, other: &Self) -> bool {
        equal(self.data(), other.data())
    }
}

impl<OffsetSize: OffsetSizeTrait> PartialEq for GenericBinaryArray<OffsetSize> {
    fn eq(&self, other: &Self) -> bool {
        equal(self.data(), other.data())
    }
}

impl PartialEq for FixedSizeBinaryArray {
    fn eq(&self, other: &Self) -> bool {
        equal(self.data(), other.data())
    }
}

impl PartialEq for Decimal128Array {
    fn eq(&self, other: &Self) -> bool {
        equal(self.data(), other.data())
    }
}

impl<OffsetSize: OffsetSizeTrait> PartialEq for GenericListArray<OffsetSize> {
    fn eq(&self, other: &Self) -> bool {
        equal(self.data(), other.data())
    }
}

impl PartialEq for MapArray {
    fn eq(&self, other: &Self) -> bool {
        equal(self.data(), other.data())
    }
}

impl PartialEq for FixedSizeListArray {
    fn eq(&self, other: &Self) -> bool {
        equal(self.data(), other.data())
    }
}

impl PartialEq for StructArray {
    fn eq(&self, other: &Self) -> bool {
        equal(self.data(), other.data())
    }
}

/// Compares the values of two [ArrayData] starting at `lhs_start` and `rhs_start` respectively
/// for `len` slots.
#[inline]
fn equal_values(
    lhs: &ArrayData,
    rhs: &ArrayData,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    match lhs.data_type() {
        DataType::Null => null_equal(lhs, rhs, lhs_start, rhs_start, len),
        DataType::Boolean => boolean_equal(lhs, rhs, lhs_start, rhs_start, len),
        DataType::UInt8 => primitive_equal::<u8>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::UInt16 => primitive_equal::<u16>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::UInt32 => primitive_equal::<u32>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::UInt64 => primitive_equal::<u64>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::Int8 => primitive_equal::<i8>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::Int16 => primitive_equal::<i16>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::Int32 => primitive_equal::<i32>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::Int64 => primitive_equal::<i64>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::Float32 => primitive_equal::<f32>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::Float64 => primitive_equal::<f64>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => {
            primitive_equal::<i32>(lhs, rhs, lhs_start, rhs_start, len)
        }
        DataType::Date64
        | DataType::Interval(IntervalUnit::DayTime)
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => {
            primitive_equal::<i64>(lhs, rhs, lhs_start, rhs_start, len)
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            primitive_equal::<i128>(lhs, rhs, lhs_start, rhs_start, len)
        }
        DataType::Utf8 | DataType::Binary => {
            variable_sized_equal::<i32>(lhs, rhs, lhs_start, rhs_start, len)
        }
        DataType::LargeUtf8 | DataType::LargeBinary => {
            variable_sized_equal::<i64>(lhs, rhs, lhs_start, rhs_start, len)
        }
        DataType::FixedSizeBinary(_) => {
            fixed_binary_equal(lhs, rhs, lhs_start, rhs_start, len)
        }
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => {
            decimal_equal(lhs, rhs, lhs_start, rhs_start, len)
        }
        DataType::List(_) => list_equal::<i32>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::LargeList(_) => list_equal::<i64>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::FixedSizeList(_, _) => {
            fixed_list_equal(lhs, rhs, lhs_start, rhs_start, len)
        }
        DataType::Struct(_) => struct_equal(lhs, rhs, lhs_start, rhs_start, len),
        DataType::Union(_, _, _) => union_equal(lhs, rhs, lhs_start, rhs_start, len),
        DataType::Dictionary(data_type, _) => match data_type.as_ref() {
            DataType::Int8 => dictionary_equal::<i8>(lhs, rhs, lhs_start, rhs_start, len),
            DataType::Int16 => {
                dictionary_equal::<i16>(lhs, rhs, lhs_start, rhs_start, len)
            }
            DataType::Int32 => {
                dictionary_equal::<i32>(lhs, rhs, lhs_start, rhs_start, len)
            }
            DataType::Int64 => {
                dictionary_equal::<i64>(lhs, rhs, lhs_start, rhs_start, len)
            }
            DataType::UInt8 => {
                dictionary_equal::<u8>(lhs, rhs, lhs_start, rhs_start, len)
            }
            DataType::UInt16 => {
                dictionary_equal::<u16>(lhs, rhs, lhs_start, rhs_start, len)
            }
            DataType::UInt32 => {
                dictionary_equal::<u32>(lhs, rhs, lhs_start, rhs_start, len)
            }
            DataType::UInt64 => {
                dictionary_equal::<u64>(lhs, rhs, lhs_start, rhs_start, len)
            }
            _ => unreachable!(),
        },
        DataType::Float16 => primitive_equal::<f16>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::Map(_, _) => list_equal::<i32>(lhs, rhs, lhs_start, rhs_start, len),
    }
}

fn equal_range(
    lhs: &ArrayData,
    rhs: &ArrayData,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    utils::equal_nulls(lhs, rhs, lhs_start, rhs_start, len)
        && equal_values(lhs, rhs, lhs_start, rhs_start, len)
}

/// Logically compares two [ArrayData].
/// Two arrays are logically equal if and only if:
/// * their data types are equal
/// * their lengths are equal
/// * their null counts are equal
/// * their null bitmaps are equal
/// * each of their items are equal
/// two items are equal when their in-memory representation is physically equal (i.e. same bit content).
/// The physical comparison depend on the data type.
/// # Panics
/// This function may panic whenever any of the [ArrayData] does not follow the Arrow specification.
/// (e.g. wrong number of buffers, buffer `len` does not correspond to the declared `len`)
pub fn equal(lhs: &ArrayData, rhs: &ArrayData) -> bool {
    utils::base_equal(lhs, rhs)
        && lhs.null_count() == rhs.null_count()
        && utils::equal_nulls(lhs, rhs, 0, 0, lhs.len())
        && equal_values(lhs, rhs, 0, 0, lhs.len())
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;
    use std::sync::Arc;

    use crate::array::{
        array::Array, ArrayData, ArrayDataBuilder, ArrayRef, BooleanArray,
        FixedSizeBinaryBuilder, FixedSizeListBuilder, GenericBinaryArray, Int32Builder,
        ListBuilder, NullArray, StringArray, StringDictionaryBuilder, StructArray,
        UnionBuilder,
    };
    use crate::array::{GenericStringArray, Int32Array};
    use crate::buffer::Buffer;
    use crate::datatypes::{Field, Int16Type, Int32Type, ToByteSlice};

    use super::*;

    #[test]
    fn test_null_equal() {
        let a = NullArray::new(12);
        let a = a.data();
        let b = NullArray::new(12);
        let b = b.data();
        test_equal(a, b, true);

        let b = NullArray::new(10);
        let b = b.data();
        test_equal(a, b, false);

        // Test the case where offset != 0

        let a_slice = a.slice(2, 3);
        let b_slice = b.slice(1, 3);
        test_equal(&a_slice, &b_slice, true);

        let a_slice = a.slice(5, 4);
        let b_slice = b.slice(3, 3);
        test_equal(&a_slice, &b_slice, false);
    }

    #[test]
    fn test_boolean_equal() {
        let a = BooleanArray::from(vec![false, false, true]);
        let a = a.data();
        let b = BooleanArray::from(vec![false, false, true]);
        let b = b.data();
        test_equal(a, b, true);

        let b = BooleanArray::from(vec![false, false, false]);
        let b = b.data();
        test_equal(a, b, false);
    }

    #[test]
    fn test_boolean_equal_nulls() {
        let a = BooleanArray::from(vec![Some(false), None, None, Some(true)]);
        let a = a.data();
        let b = BooleanArray::from(vec![Some(false), None, None, Some(true)]);
        let b = b.data();
        test_equal(a, b, true);

        let b = BooleanArray::from(vec![None, None, None, Some(true)]);
        let b = b.data();
        test_equal(a, b, false);

        let b = BooleanArray::from(vec![Some(true), None, None, Some(true)]);
        let b = b.data();
        test_equal(a, b, false);
    }

    #[test]
    fn test_boolean_equal_offset() {
        let a = BooleanArray::from(vec![false, true, false, true, false, false, true]);
        let a = a.data();
        let b =
            BooleanArray::from(vec![true, false, false, false, true, false, true, true]);
        let b = b.data();
        assert!(!equal(a, b));
        assert!(!equal(b, a));

        let a_slice = a.slice(2, 3);
        let b_slice = b.slice(3, 3);
        assert!(equal(&a_slice, &b_slice));
        assert!(equal(&b_slice, &a_slice));

        let a_slice = a.slice(3, 4);
        let b_slice = b.slice(4, 4);
        assert!(!equal(&a_slice, &b_slice));
        assert!(!equal(&b_slice, &a_slice));

        // Test the optimization cases where null_count == 0 and starts at 0 and len >= size_of(u8)

        // Elements fill in `u8`'s exactly.
        let mut vector = vec![false, false, true, true, true, true, true, true];
        let a = BooleanArray::from(vector.clone());
        let a = a.data();
        let b = BooleanArray::from(vector.clone());
        let b = b.data();
        test_equal(a, b, true);

        // Elements fill in `u8`s + suffix bits.
        vector.push(true);
        let a = BooleanArray::from(vector.clone());
        let a = a.data();
        let b = BooleanArray::from(vector);
        let b = b.data();
        test_equal(a, b, true);
    }

    #[test]
    fn test_primitive() {
        let cases = vec![
            (
                vec![Some(1), Some(2), Some(3)],
                vec![Some(1), Some(2), Some(3)],
                true,
            ),
            (
                vec![Some(1), Some(2), Some(3)],
                vec![Some(1), Some(2), Some(4)],
                false,
            ),
            (
                vec![Some(1), Some(2), None],
                vec![Some(1), Some(2), None],
                true,
            ),
            (
                vec![Some(1), None, Some(3)],
                vec![Some(1), Some(2), None],
                false,
            ),
            (
                vec![Some(1), None, None],
                vec![Some(1), Some(2), None],
                false,
            ),
        ];

        for (lhs, rhs, expected) in cases {
            let lhs = Int32Array::from(lhs);
            let lhs = lhs.data();
            let rhs = Int32Array::from(rhs);
            let rhs = rhs.data();
            test_equal(lhs, rhs, expected);
        }
    }

    #[test]
    fn test_primitive_slice() {
        let cases = vec![
            (
                vec![Some(1), Some(2), Some(3)],
                (0, 1),
                vec![Some(1), Some(2), Some(3)],
                (0, 1),
                true,
            ),
            (
                vec![Some(1), Some(2), Some(3)],
                (1, 1),
                vec![Some(1), Some(2), Some(3)],
                (2, 1),
                false,
            ),
            (
                vec![Some(1), Some(2), None],
                (1, 1),
                vec![Some(1), None, Some(2)],
                (2, 1),
                true,
            ),
            (
                vec![None, Some(2), None],
                (1, 1),
                vec![None, None, Some(2)],
                (2, 1),
                true,
            ),
            (
                vec![Some(1), None, Some(2), None, Some(3)],
                (2, 2),
                vec![None, Some(2), None, Some(3)],
                (1, 2),
                true,
            ),
            (
                vec![Some(1), Some(2), None, Some(0)],
                (2, 2),
                vec![Some(4), Some(5), Some(0), None],
                (2, 2),
                false,
            ),
        ];

        for (lhs, slice_lhs, rhs, slice_rhs, expected) in cases {
            let lhs = Int32Array::from(lhs);
            let lhs = lhs.data();
            let lhs = lhs.slice(slice_lhs.0, slice_lhs.1);
            let rhs = Int32Array::from(rhs);
            let rhs = rhs.data();
            let rhs = rhs.slice(slice_rhs.0, slice_rhs.1);

            test_equal(&lhs, &rhs, expected);
        }
    }

    fn test_equal(lhs: &ArrayData, rhs: &ArrayData, expected: bool) {
        // equality is symmetric
        assert!(equal(lhs, lhs), "\n{:?}\n{:?}", lhs, lhs);
        assert!(equal(rhs, rhs), "\n{:?}\n{:?}", rhs, rhs);

        assert_eq!(equal(lhs, rhs), expected, "\n{:?}\n{:?}", lhs, rhs);
        assert_eq!(equal(rhs, lhs), expected, "\n{:?}\n{:?}", rhs, lhs);
    }

    type OptionString = Option<String>;

    fn binary_cases() -> Vec<(Vec<OptionString>, Vec<OptionString>, bool)> {
        let base = vec![
            Some("hello".to_owned()),
            None,
            None,
            Some("world".to_owned()),
            None,
            None,
        ];
        let not_base = vec![
            Some("hello".to_owned()),
            Some("foo".to_owned()),
            None,
            Some("world".to_owned()),
            None,
            None,
        ];
        vec![
            (
                vec![Some("hello".to_owned()), Some("world".to_owned())],
                vec![Some("hello".to_owned()), Some("world".to_owned())],
                true,
            ),
            (
                vec![Some("hello".to_owned()), Some("world".to_owned())],
                vec![Some("hello".to_owned()), Some("arrow".to_owned())],
                false,
            ),
            (base.clone(), base.clone(), true),
            (base, not_base, false),
        ]
    }

    fn test_generic_string_equal<OffsetSize: OffsetSizeTrait>() {
        let cases = binary_cases();

        for (lhs, rhs, expected) in cases {
            let lhs: GenericStringArray<OffsetSize> = lhs.into_iter().collect();
            let lhs = lhs.data();
            let rhs: GenericStringArray<OffsetSize> = rhs.into_iter().collect();
            let rhs = rhs.data();
            test_equal(lhs, rhs, expected);
        }
    }

    #[test]
    fn test_string_equal() {
        test_generic_string_equal::<i32>()
    }

    #[test]
    fn test_large_string_equal() {
        test_generic_string_equal::<i64>()
    }

    fn test_generic_binary_equal<OffsetSize: OffsetSizeTrait>() {
        let cases = binary_cases();

        for (lhs, rhs, expected) in cases {
            let lhs = lhs
                .iter()
                .map(|x| x.as_deref().map(|x| x.as_bytes()))
                .collect();
            let rhs = rhs
                .iter()
                .map(|x| x.as_deref().map(|x| x.as_bytes()))
                .collect();
            let lhs = GenericBinaryArray::<OffsetSize>::from_opt_vec(lhs);
            let lhs = lhs.data();
            let rhs = GenericBinaryArray::<OffsetSize>::from_opt_vec(rhs);
            let rhs = rhs.data();
            test_equal(lhs, rhs, expected);
        }
    }

    #[test]
    fn test_binary_equal() {
        test_generic_binary_equal::<i32>()
    }

    #[test]
    fn test_large_binary_equal() {
        test_generic_binary_equal::<i64>()
    }

    #[test]
    fn test_fixed_size_binary_array() {
        let a_input_arg = vec![vec![1, 2], vec![3, 4], vec![5, 6]];
        let a = FixedSizeBinaryArray::try_from_iter(a_input_arg.into_iter()).unwrap();
        let a = a.data();

        let b_input_arg = vec![vec![1, 2], vec![3, 4], vec![5, 6]];
        let b = FixedSizeBinaryArray::try_from_iter(b_input_arg.into_iter()).unwrap();
        let b = b.data();

        test_equal(a, b, true);
    }

    #[test]
    fn test_string_offset() {
        let a = StringArray::from(vec![Some("a"), None, Some("b")]);
        let a = a.data();
        let a = a.slice(2, 1);
        let b = StringArray::from(vec![Some("b")]);
        let b = b.data();

        test_equal(&a, b, true);
    }

    #[test]
    fn test_string_offset_larger() {
        let a = StringArray::from(vec![Some("a"), None, Some("b"), None, Some("c")]);
        let a = a.data();
        let b = StringArray::from(vec![None, Some("b"), None, Some("c")]);
        let b = b.data();

        test_equal(&a.slice(2, 2), &b.slice(0, 2), false);
        test_equal(&a.slice(2, 2), &b.slice(1, 2), true);
        test_equal(&a.slice(2, 2), &b.slice(2, 2), false);
    }

    #[test]
    fn test_null() {
        let a = NullArray::new(2);
        let a = a.data();
        let b = NullArray::new(2);
        let b = b.data();
        test_equal(a, b, true);

        let b = NullArray::new(1);
        let b = b.data();
        test_equal(a, b, false);
    }

    fn create_list_array<U: AsRef<[i32]>, T: AsRef<[Option<U>]>>(data: T) -> ArrayData {
        let mut builder = ListBuilder::new(Int32Builder::with_capacity(10));
        for d in data.as_ref() {
            if let Some(v) = d {
                builder.values().append_slice(v.as_ref());
                builder.append(true);
            } else {
                builder.append(false);
            }
        }
        builder.finish().into_data()
    }

    #[test]
    fn test_list_equal() {
        let a = create_list_array(&[Some(&[1, 2, 3]), Some(&[4, 5, 6])]);
        let b = create_list_array(&[Some(&[1, 2, 3]), Some(&[4, 5, 6])]);
        test_equal(&a, &b, true);

        let b = create_list_array(&[Some(&[1, 2, 3]), Some(&[4, 5, 7])]);
        test_equal(&a, &b, false);
    }

    #[test]
    fn test_empty_offsets_list_equal() {
        let empty: Vec<i32> = vec![];
        let values = Int32Array::from(empty);
        let empty_offsets: [u8; 0] = [];

        let a = ArrayDataBuilder::new(DataType::List(Box::new(Field::new(
            "item",
            DataType::Int32,
            true,
        ))))
        .len(0)
        .add_buffer(Buffer::from(&empty_offsets))
        .add_child_data(values.data().clone())
        .null_bit_buffer(Some(Buffer::from(&empty_offsets)))
        .build()
        .unwrap();

        let b = ArrayDataBuilder::new(DataType::List(Box::new(Field::new(
            "item",
            DataType::Int32,
            true,
        ))))
        .len(0)
        .add_buffer(Buffer::from(&empty_offsets))
        .add_child_data(values.data().clone())
        .null_bit_buffer(Some(Buffer::from(&empty_offsets)))
        .build()
        .unwrap();

        test_equal(&a, &b, true);

        let c = ArrayDataBuilder::new(DataType::List(Box::new(Field::new(
            "item",
            DataType::Int32,
            true,
        ))))
        .len(0)
        .add_buffer(Buffer::from(vec![0i32, 2, 3, 4, 6, 7, 8].to_byte_slice()))
        .add_child_data(
            Int32Array::from(vec![1, 2, -1, -2, 3, 4, -3, -4])
                .data()
                .clone(),
        )
        .null_bit_buffer(Some(Buffer::from(vec![0b00001001])))
        .build()
        .unwrap();

        test_equal(&a, &c, true);
    }

    // Test the case where null_count > 0
    #[test]
    fn test_list_null() {
        let a =
            create_list_array(&[Some(&[1, 2]), None, None, Some(&[3, 4]), None, None]);
        let b =
            create_list_array(&[Some(&[1, 2]), None, None, Some(&[3, 4]), None, None]);
        test_equal(&a, &b, true);

        let b = create_list_array(&[
            Some(&[1, 2]),
            None,
            Some(&[5, 6]),
            Some(&[3, 4]),
            None,
            None,
        ]);
        test_equal(&a, &b, false);

        let b =
            create_list_array(&[Some(&[1, 2]), None, None, Some(&[3, 5]), None, None]);
        test_equal(&a, &b, false);

        // a list where the nullness of values is determined by the list's bitmap
        let c_values = Int32Array::from(vec![1, 2, -1, -2, 3, 4, -3, -4]);
        let c = ArrayDataBuilder::new(DataType::List(Box::new(Field::new(
            "item",
            DataType::Int32,
            true,
        ))))
        .len(6)
        .add_buffer(Buffer::from(vec![0i32, 2, 3, 4, 6, 7, 8].to_byte_slice()))
        .add_child_data(c_values.into_data())
        .null_bit_buffer(Some(Buffer::from(vec![0b00001001])))
        .build()
        .unwrap();

        let d_values = Int32Array::from(vec![
            Some(1),
            Some(2),
            None,
            None,
            Some(3),
            Some(4),
            None,
            None,
        ]);
        let d = ArrayDataBuilder::new(DataType::List(Box::new(Field::new(
            "item",
            DataType::Int32,
            true,
        ))))
        .len(6)
        .add_buffer(Buffer::from(vec![0i32, 2, 3, 4, 6, 7, 8].to_byte_slice()))
        .add_child_data(d_values.into_data())
        .null_bit_buffer(Some(Buffer::from(vec![0b00001001])))
        .build()
        .unwrap();
        test_equal(&c, &d, true);
    }

    // Test the case where offset != 0
    #[test]
    fn test_list_offsets() {
        let a =
            create_list_array(&[Some(&[1, 2]), None, None, Some(&[3, 4]), None, None]);
        let b =
            create_list_array(&[Some(&[1, 2]), None, None, Some(&[3, 5]), None, None]);

        let a_slice = a.slice(0, 3);
        let b_slice = b.slice(0, 3);
        test_equal(&a_slice, &b_slice, true);

        let a_slice = a.slice(0, 5);
        let b_slice = b.slice(0, 5);
        test_equal(&a_slice, &b_slice, false);

        let a_slice = a.slice(4, 1);
        let b_slice = b.slice(4, 1);
        test_equal(&a_slice, &b_slice, true);
    }

    fn create_fixed_size_binary_array<U: AsRef<[u8]>, T: AsRef<[Option<U>]>>(
        data: T,
    ) -> ArrayData {
        let mut builder = FixedSizeBinaryBuilder::with_capacity(data.as_ref().len(), 5);

        for d in data.as_ref() {
            if let Some(v) = d {
                builder.append_value(v.as_ref()).unwrap();
            } else {
                builder.append_null();
            }
        }
        builder.finish().into_data()
    }

    #[test]
    fn test_fixed_size_binary_equal() {
        let a = create_fixed_size_binary_array(&[Some(b"hello"), Some(b"world")]);
        let b = create_fixed_size_binary_array(&[Some(b"hello"), Some(b"world")]);
        test_equal(&a, &b, true);

        let b = create_fixed_size_binary_array(&[Some(b"hello"), Some(b"arrow")]);
        test_equal(&a, &b, false);
    }

    // Test the case where null_count > 0
    #[test]
    fn test_fixed_size_binary_null() {
        let a = create_fixed_size_binary_array(&[Some(b"hello"), None, Some(b"world")]);
        let b = create_fixed_size_binary_array(&[Some(b"hello"), None, Some(b"world")]);
        test_equal(&a, &b, true);

        let b = create_fixed_size_binary_array(&[Some(b"hello"), Some(b"world"), None]);
        test_equal(&a, &b, false);

        let b = create_fixed_size_binary_array(&[Some(b"hello"), None, Some(b"arrow")]);
        test_equal(&a, &b, false);
    }

    #[test]
    fn test_fixed_size_binary_offsets() {
        // Test the case where offset != 0
        let a = create_fixed_size_binary_array(&[
            Some(b"hello"),
            None,
            None,
            Some(b"world"),
            None,
            None,
        ]);
        let b = create_fixed_size_binary_array(&[
            Some(b"hello"),
            None,
            None,
            Some(b"arrow"),
            None,
            None,
        ]);

        let a_slice = a.slice(0, 3);
        let b_slice = b.slice(0, 3);
        test_equal(&a_slice, &b_slice, true);

        let a_slice = a.slice(0, 5);
        let b_slice = b.slice(0, 5);
        test_equal(&a_slice, &b_slice, false);

        let a_slice = a.slice(4, 1);
        let b_slice = b.slice(4, 1);
        test_equal(&a_slice, &b_slice, true);

        let a_slice = a.slice(3, 1);
        let b_slice = b.slice(3, 1);
        test_equal(&a_slice, &b_slice, false);
    }

    fn create_decimal_array(data: Vec<Option<i128>>) -> ArrayData {
        data.into_iter()
            .collect::<Decimal128Array>()
            .with_precision_and_scale(23, 6)
            .unwrap()
            .into()
    }

    #[test]
    fn test_decimal_equal() {
        let a = create_decimal_array(vec![Some(8_887_000_000), Some(-8_887_000_000)]);
        let b = create_decimal_array(vec![Some(8_887_000_000), Some(-8_887_000_000)]);
        test_equal(&a, &b, true);

        let b = create_decimal_array(vec![Some(15_887_000_000), Some(-8_887_000_000)]);
        test_equal(&a, &b, false);
    }

    // Test the case where null_count > 0
    #[test]
    fn test_decimal_null() {
        let a =
            create_decimal_array(vec![Some(8_887_000_000), None, Some(-8_887_000_000)]);
        let b =
            create_decimal_array(vec![Some(8_887_000_000), None, Some(-8_887_000_000)]);
        test_equal(&a, &b, true);

        let b =
            create_decimal_array(vec![Some(8_887_000_000), Some(-8_887_000_000), None]);
        test_equal(&a, &b, false);

        let b =
            create_decimal_array(vec![Some(15_887_000_000), None, Some(-8_887_000_000)]);
        test_equal(&a, &b, false);
    }

    #[test]
    fn test_decimal_offsets() {
        // Test the case where offset != 0
        let a = create_decimal_array(vec![
            Some(8_887_000_000),
            None,
            None,
            Some(-8_887_000_000),
            None,
            None,
        ]);
        let b = create_decimal_array(vec![
            None,
            Some(8_887_000_000),
            None,
            None,
            Some(15_887_000_000),
            None,
            None,
        ]);

        let a_slice = a.slice(0, 3);
        let b_slice = b.slice(1, 3);
        test_equal(&a_slice, &b_slice, true);

        let a_slice = a.slice(0, 5);
        let b_slice = b.slice(1, 5);
        test_equal(&a_slice, &b_slice, false);

        let a_slice = a.slice(4, 1);
        let b_slice = b.slice(5, 1);
        test_equal(&a_slice, &b_slice, true);

        let a_slice = a.slice(3, 3);
        let b_slice = b.slice(4, 3);
        test_equal(&a_slice, &b_slice, false);

        let a_slice = a.slice(1, 3);
        let b_slice = b.slice(2, 3);
        test_equal(&a_slice, &b_slice, false);

        let b = create_decimal_array(vec![
            None,
            None,
            None,
            Some(-8_887_000_000),
            Some(-3_000),
            None,
        ]);
        let a_slice = a.slice(1, 3);
        let b_slice = b.slice(1, 3);
        test_equal(&a_slice, &b_slice, true);
    }

    /// Create a fixed size list of 2 value lengths
    fn create_fixed_size_list_array<U: AsRef<[i32]>, T: AsRef<[Option<U>]>>(
        data: T,
    ) -> ArrayData {
        let mut builder = FixedSizeListBuilder::new(Int32Builder::with_capacity(10), 3);

        for d in data.as_ref() {
            if let Some(v) = d {
                builder.values().append_slice(v.as_ref());
                builder.append(true);
            } else {
                for _ in 0..builder.value_length() {
                    builder.values().append_null();
                }
                builder.append(false);
            }
        }
        builder.finish().into_data()
    }

    #[test]
    fn test_fixed_size_list_equal() {
        let a = create_fixed_size_list_array(&[Some(&[1, 2, 3]), Some(&[4, 5, 6])]);
        let b = create_fixed_size_list_array(&[Some(&[1, 2, 3]), Some(&[4, 5, 6])]);
        test_equal(&a, &b, true);

        let b = create_fixed_size_list_array(&[Some(&[1, 2, 3]), Some(&[4, 5, 7])]);
        test_equal(&a, &b, false);
    }

    // Test the case where null_count > 0
    #[test]
    fn test_fixed_list_null() {
        let a = create_fixed_size_list_array(&[
            Some(&[1, 2, 3]),
            None,
            None,
            Some(&[4, 5, 6]),
            None,
            None,
        ]);
        let b = create_fixed_size_list_array(&[
            Some(&[1, 2, 3]),
            None,
            None,
            Some(&[4, 5, 6]),
            None,
            None,
        ]);
        test_equal(&a, &b, true);

        let b = create_fixed_size_list_array(&[
            Some(&[1, 2, 3]),
            None,
            Some(&[7, 8, 9]),
            Some(&[4, 5, 6]),
            None,
            None,
        ]);
        test_equal(&a, &b, false);

        let b = create_fixed_size_list_array(&[
            Some(&[1, 2, 3]),
            None,
            None,
            Some(&[3, 6, 9]),
            None,
            None,
        ]);
        test_equal(&a, &b, false);

        let b = create_fixed_size_list_array(&[None, Some(&[4, 5, 6]), None, None]);

        test_equal(&a.slice(2, 4), &b, true);
        test_equal(&a.slice(3, 3), &b.slice(1, 3), true);
    }

    #[test]
    fn test_fixed_list_offsets() {
        // Test the case where offset != 0
        let a = create_fixed_size_list_array(&[
            Some(&[1, 2, 3]),
            None,
            None,
            Some(&[4, 5, 6]),
            None,
            None,
        ]);
        let b = create_fixed_size_list_array(&[
            Some(&[1, 2, 3]),
            None,
            None,
            Some(&[3, 6, 9]),
            None,
            None,
        ]);

        let a_slice = a.slice(0, 3);
        let b_slice = b.slice(0, 3);
        test_equal(&a_slice, &b_slice, true);

        let a_slice = a.slice(0, 5);
        let b_slice = b.slice(0, 5);
        test_equal(&a_slice, &b_slice, false);

        let a_slice = a.slice(4, 1);
        let b_slice = b.slice(4, 1);
        test_equal(&a_slice, &b_slice, true);
    }

    #[test]
    fn test_struct_equal() {
        let strings: ArrayRef = Arc::new(StringArray::from(vec![
            Some("joe"),
            None,
            None,
            Some("mark"),
            Some("doe"),
        ]));
        let ints: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            None,
            Some(4),
            Some(5),
        ]));

        let a =
            StructArray::try_from(vec![("f1", strings.clone()), ("f2", ints.clone())])
                .unwrap();
        let a = a.data();

        let b = StructArray::try_from(vec![("f1", strings), ("f2", ints)]).unwrap();
        let b = b.data();

        test_equal(a, b, true);
    }

    #[test]
    fn test_struct_equal_null() {
        let strings: ArrayRef = Arc::new(StringArray::from(vec![
            Some("joe"),
            None,
            None,
            Some("mark"),
            Some("doe"),
        ]));
        let ints: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            None,
            Some(4),
            Some(5),
        ]));
        let ints_non_null: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 0]));

        let a = ArrayData::builder(DataType::Struct(vec![
            Field::new("f1", DataType::Utf8, true),
            Field::new("f2", DataType::Int32, true),
        ]))
        .null_bit_buffer(Some(Buffer::from(vec![0b00001011])))
        .len(5)
        .add_child_data(strings.data_ref().clone())
        .add_child_data(ints.data_ref().clone())
        .build()
        .unwrap();
        let a = crate::array::make_array(a);

        let b = ArrayData::builder(DataType::Struct(vec![
            Field::new("f1", DataType::Utf8, true),
            Field::new("f2", DataType::Int32, true),
        ]))
        .null_bit_buffer(Some(Buffer::from(vec![0b00001011])))
        .len(5)
        .add_child_data(strings.data_ref().clone())
        .add_child_data(ints_non_null.data_ref().clone())
        .build()
        .unwrap();
        let b = crate::array::make_array(b);

        test_equal(a.data_ref(), b.data_ref(), true);

        // test with arrays that are not equal
        let c_ints_non_null: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 0, 4]));
        let c = ArrayData::builder(DataType::Struct(vec![
            Field::new("f1", DataType::Utf8, true),
            Field::new("f2", DataType::Int32, true),
        ]))
        .null_bit_buffer(Some(Buffer::from(vec![0b00001011])))
        .len(5)
        .add_child_data(strings.data_ref().clone())
        .add_child_data(c_ints_non_null.data_ref().clone())
        .build()
        .unwrap();
        let c = crate::array::make_array(c);

        test_equal(a.data_ref(), c.data_ref(), false);

        // test a nested struct
        let a = ArrayData::builder(DataType::Struct(vec![Field::new(
            "f3",
            a.data_type().clone(),
            true,
        )]))
        .null_bit_buffer(Some(Buffer::from(vec![0b00011110])))
        .len(5)
        .add_child_data(a.data_ref().clone())
        .build()
        .unwrap();
        let a = crate::array::make_array(a);

        // reconstruct b, but with different data where the first struct is null
        let strings: ArrayRef = Arc::new(StringArray::from(vec![
            Some("joanne"), // difference
            None,
            None,
            Some("mark"),
            Some("doe"),
        ]));
        let b = ArrayData::builder(DataType::Struct(vec![
            Field::new("f1", DataType::Utf8, true),
            Field::new("f2", DataType::Int32, true),
        ]))
        .null_bit_buffer(Some(Buffer::from(vec![0b00001011])))
        .len(5)
        .add_child_data(strings.data_ref().clone())
        .add_child_data(ints_non_null.data_ref().clone())
        .build()
        .unwrap();

        let b = ArrayData::builder(DataType::Struct(vec![Field::new(
            "f3",
            b.data_type().clone(),
            true,
        )]))
        .null_bit_buffer(Some(Buffer::from(vec![0b00011110])))
        .len(5)
        .add_child_data(b)
        .build()
        .unwrap();
        let b = crate::array::make_array(b);

        test_equal(a.data_ref(), b.data_ref(), true);
    }

    #[test]
    fn test_struct_equal_null_variable_size() {
        // the string arrays differ, but where the struct array is null
        let strings1: ArrayRef = Arc::new(StringArray::from(vec![
            Some("joe"),
            None,
            None,
            Some("mark"),
            Some("doel"),
        ]));
        let strings2: ArrayRef = Arc::new(StringArray::from(vec![
            Some("joel"),
            None,
            None,
            Some("mark"),
            Some("doe"),
        ]));

        let a = ArrayData::builder(DataType::Struct(vec![Field::new(
            "f1",
            DataType::Utf8,
            true,
        )]))
        .null_bit_buffer(Some(Buffer::from(vec![0b00001010])))
        .len(5)
        .add_child_data(strings1.data_ref().clone())
        .build()
        .unwrap();
        let a = crate::array::make_array(a);

        let b = ArrayData::builder(DataType::Struct(vec![Field::new(
            "f1",
            DataType::Utf8,
            true,
        )]))
        .null_bit_buffer(Some(Buffer::from(vec![0b00001010])))
        .len(5)
        .add_child_data(strings2.data_ref().clone())
        .build()
        .unwrap();
        let b = crate::array::make_array(b);

        test_equal(a.data_ref(), b.data_ref(), true);

        // test with arrays that are not equal
        let strings3: ArrayRef = Arc::new(StringArray::from(vec![
            Some("mark"),
            None,
            None,
            Some("doe"),
            Some("joe"),
        ]));
        let c = ArrayData::builder(DataType::Struct(vec![Field::new(
            "f1",
            DataType::Utf8,
            true,
        )]))
        .null_bit_buffer(Some(Buffer::from(vec![0b00001011])))
        .len(5)
        .add_child_data(strings3.data_ref().clone())
        .build()
        .unwrap();
        let c = crate::array::make_array(c);

        test_equal(a.data_ref(), c.data_ref(), false);
    }

    fn create_dictionary_array(values: &[&str], keys: &[Option<&str>]) -> ArrayData {
        let values = StringArray::from(values.to_vec());
        let mut builder = StringDictionaryBuilder::<Int16Type>::new_with_dictionary(
            keys.len(),
            &values,
        )
        .unwrap();
        for key in keys {
            if let Some(v) = key {
                builder.append(v).unwrap();
            } else {
                builder.append_null()
            }
        }
        builder.finish().into_data()
    }

    #[test]
    fn test_dictionary_equal() {
        // (a, b, c), (1, 2, 1, 3) => (a, b, a, c)
        let a = create_dictionary_array(
            &["a", "b", "c"],
            &[Some("a"), Some("b"), Some("a"), Some("c")],
        );
        // different representation (values and keys are swapped), same result
        let b = create_dictionary_array(
            &["a", "c", "b"],
            &[Some("a"), Some("b"), Some("a"), Some("c")],
        );
        test_equal(&a, &b, true);

        // different len
        let b =
            create_dictionary_array(&["a", "c", "b"], &[Some("a"), Some("b"), Some("a")]);
        test_equal(&a, &b, false);

        // different key
        let b = create_dictionary_array(
            &["a", "c", "b"],
            &[Some("a"), Some("b"), Some("a"), Some("a")],
        );
        test_equal(&a, &b, false);

        // different values, same keys
        let b = create_dictionary_array(
            &["a", "b", "d"],
            &[Some("a"), Some("b"), Some("a"), Some("d")],
        );
        test_equal(&a, &b, false);
    }

    #[test]
    fn test_dictionary_equal_null() {
        // (a, b, c), (1, 2, 1, 3) => (a, b, a, c)
        let a = create_dictionary_array(
            &["a", "b", "c"],
            &[Some("a"), None, Some("a"), Some("c")],
        );

        // equal to self
        test_equal(&a, &a, true);

        // different representation (values and keys are swapped), same result
        let b = create_dictionary_array(
            &["a", "c", "b"],
            &[Some("a"), None, Some("a"), Some("c")],
        );
        test_equal(&a, &b, true);

        // different null position
        let b = create_dictionary_array(
            &["a", "c", "b"],
            &[Some("a"), Some("b"), Some("a"), None],
        );
        test_equal(&a, &b, false);

        // different key
        let b = create_dictionary_array(
            &["a", "c", "b"],
            &[Some("a"), None, Some("a"), Some("a")],
        );
        test_equal(&a, &b, false);

        // different values, same keys
        let b = create_dictionary_array(
            &["a", "b", "d"],
            &[Some("a"), None, Some("a"), Some("d")],
        );
        test_equal(&a, &b, false);
    }

    #[test]
    fn test_non_null_empty_strings() {
        let s = StringArray::from(vec![Some(""), Some(""), Some("")]);

        let string1 = s.data();

        let string2 = ArrayData::builder(DataType::Utf8)
            .len(string1.len())
            .buffers(string1.buffers().to_vec())
            .build()
            .unwrap();

        // string2 is identical to string1 except that it has no validity buffer but since there
        // are no nulls, string1 and string2 are equal
        test_equal(string1, &string2, true);
    }

    #[test]
    fn test_null_empty_strings() {
        let s = StringArray::from(vec![Some(""), None, Some("")]);

        let string1 = s.data();

        let string2 = ArrayData::builder(DataType::Utf8)
            .len(string1.len())
            .buffers(string1.buffers().to_vec())
            .build()
            .unwrap();

        // string2 is identical to string1 except that it has no validity buffer since string1 has
        // nulls in it, string1 and string2 are not equal
        test_equal(string1, &string2, false);
    }

    #[test]
    fn test_union_equal_dense() {
        let mut builder = UnionBuilder::new_dense();
        builder.append::<Int32Type>("a", 1).unwrap();
        builder.append::<Int32Type>("b", 2).unwrap();
        builder.append::<Int32Type>("c", 3).unwrap();
        builder.append::<Int32Type>("a", 4).unwrap();
        builder.append_null::<Int32Type>("a").unwrap();
        builder.append::<Int32Type>("a", 6).unwrap();
        builder.append::<Int32Type>("b", 7).unwrap();
        let union1 = builder.build().unwrap();

        builder = UnionBuilder::new_dense();
        builder.append::<Int32Type>("a", 1).unwrap();
        builder.append::<Int32Type>("b", 2).unwrap();
        builder.append::<Int32Type>("c", 3).unwrap();
        builder.append::<Int32Type>("a", 4).unwrap();
        builder.append_null::<Int32Type>("a").unwrap();
        builder.append::<Int32Type>("a", 6).unwrap();
        builder.append::<Int32Type>("b", 7).unwrap();
        let union2 = builder.build().unwrap();

        builder = UnionBuilder::new_dense();
        builder.append::<Int32Type>("a", 1).unwrap();
        builder.append::<Int32Type>("b", 2).unwrap();
        builder.append::<Int32Type>("c", 3).unwrap();
        builder.append::<Int32Type>("a", 5).unwrap();
        builder.append::<Int32Type>("c", 4).unwrap();
        builder.append::<Int32Type>("a", 6).unwrap();
        builder.append::<Int32Type>("b", 7).unwrap();
        let union3 = builder.build().unwrap();

        builder = UnionBuilder::new_dense();
        builder.append::<Int32Type>("a", 1).unwrap();
        builder.append::<Int32Type>("b", 2).unwrap();
        builder.append::<Int32Type>("c", 3).unwrap();
        builder.append::<Int32Type>("a", 4).unwrap();
        builder.append_null::<Int32Type>("c").unwrap();
        builder.append_null::<Int32Type>("b").unwrap();
        builder.append::<Int32Type>("b", 7).unwrap();
        let union4 = builder.build().unwrap();

        test_equal(union1.data(), union2.data(), true);
        test_equal(union1.data(), union3.data(), false);
        test_equal(union1.data(), union4.data(), false);
    }

    #[test]
    fn test_union_equal_sparse() {
        let mut builder = UnionBuilder::new_sparse();
        builder.append::<Int32Type>("a", 1).unwrap();
        builder.append::<Int32Type>("b", 2).unwrap();
        builder.append::<Int32Type>("c", 3).unwrap();
        builder.append::<Int32Type>("a", 4).unwrap();
        builder.append_null::<Int32Type>("a").unwrap();
        builder.append::<Int32Type>("a", 6).unwrap();
        builder.append::<Int32Type>("b", 7).unwrap();
        let union1 = builder.build().unwrap();

        builder = UnionBuilder::new_sparse();
        builder.append::<Int32Type>("a", 1).unwrap();
        builder.append::<Int32Type>("b", 2).unwrap();
        builder.append::<Int32Type>("c", 3).unwrap();
        builder.append::<Int32Type>("a", 4).unwrap();
        builder.append_null::<Int32Type>("a").unwrap();
        builder.append::<Int32Type>("a", 6).unwrap();
        builder.append::<Int32Type>("b", 7).unwrap();
        let union2 = builder.build().unwrap();

        builder = UnionBuilder::new_sparse();
        builder.append::<Int32Type>("a", 1).unwrap();
        builder.append::<Int32Type>("b", 2).unwrap();
        builder.append::<Int32Type>("c", 3).unwrap();
        builder.append::<Int32Type>("a", 5).unwrap();
        builder.append::<Int32Type>("c", 4).unwrap();
        builder.append::<Int32Type>("a", 6).unwrap();
        builder.append::<Int32Type>("b", 7).unwrap();
        let union3 = builder.build().unwrap();

        builder = UnionBuilder::new_sparse();
        builder.append::<Int32Type>("a", 1).unwrap();
        builder.append::<Int32Type>("b", 2).unwrap();
        builder.append::<Int32Type>("c", 3).unwrap();
        builder.append::<Int32Type>("a", 4).unwrap();
        builder.append_null::<Int32Type>("a").unwrap();
        builder.append_null::<Int32Type>("a").unwrap();
        builder.append::<Int32Type>("b", 7).unwrap();
        let union4 = builder.build().unwrap();

        test_equal(union1.data(), union2.data(), true);
        test_equal(union1.data(), union3.data(), false);
        test_equal(union1.data(), union4.data(), false);
    }
}
