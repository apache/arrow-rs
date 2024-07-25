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

use crate::data::ArrayData;
use arrow_buffer::i256;
use arrow_schema::{DataType, IntervalUnit};
use half::f16;

mod boolean;
mod byte_view;
mod dictionary;
mod fixed_binary;
mod fixed_list;
mod list;
mod null;
mod primitive;
mod run;
mod structure;
mod union;
mod utils;
mod variable_size;

// these methods assume the same type, len and null count.
// For this reason, they are not exposed and are instead used
// to build the generic functions below (`equal_range` and `equal`).
use boolean::boolean_equal;
use byte_view::byte_view_equal;
use dictionary::dictionary_equal;
use fixed_binary::fixed_binary_equal;
use fixed_list::fixed_list_equal;
use list::list_equal;
use null::null_equal;
use primitive::primitive_equal;
use structure::struct_equal;
use union::union_equal;
use variable_size::variable_sized_equal;

use self::run::run_equal;

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
        DataType::Decimal128(_, _) => primitive_equal::<i128>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::Decimal256(_, _) => primitive_equal::<i256>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::Date32 | DataType::Time32(_) | DataType::Interval(IntervalUnit::YearMonth) => {
            primitive_equal::<i32>(lhs, rhs, lhs_start, rhs_start, len)
        }
        DataType::Date64
        | DataType::Interval(IntervalUnit::DayTime)
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => primitive_equal::<i64>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            primitive_equal::<i128>(lhs, rhs, lhs_start, rhs_start, len)
        }
        DataType::Utf8 | DataType::Binary => {
            variable_sized_equal::<i32>(lhs, rhs, lhs_start, rhs_start, len)
        }
        DataType::LargeUtf8 | DataType::LargeBinary => {
            variable_sized_equal::<i64>(lhs, rhs, lhs_start, rhs_start, len)
        }
        DataType::FixedSizeBinary(_) => fixed_binary_equal(lhs, rhs, lhs_start, rhs_start, len),
        DataType::BinaryView | DataType::Utf8View => {
            byte_view_equal(lhs, rhs, lhs_start, rhs_start, len)
        }
        DataType::List(_) => list_equal::<i32>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::ListView(_) | DataType::LargeListView(_) => {
            unimplemented!("ListView/LargeListView not yet implemented")
        }
        DataType::LargeList(_) => list_equal::<i64>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::FixedSizeList(_, _) => fixed_list_equal(lhs, rhs, lhs_start, rhs_start, len),
        DataType::Struct(_) => struct_equal(lhs, rhs, lhs_start, rhs_start, len),
        DataType::Union(_, _) => union_equal(lhs, rhs, lhs_start, rhs_start, len),
        DataType::Dictionary(data_type, _) => match data_type.as_ref() {
            DataType::Int8 => dictionary_equal::<i8>(lhs, rhs, lhs_start, rhs_start, len),
            DataType::Int16 => dictionary_equal::<i16>(lhs, rhs, lhs_start, rhs_start, len),
            DataType::Int32 => dictionary_equal::<i32>(lhs, rhs, lhs_start, rhs_start, len),
            DataType::Int64 => dictionary_equal::<i64>(lhs, rhs, lhs_start, rhs_start, len),
            DataType::UInt8 => dictionary_equal::<u8>(lhs, rhs, lhs_start, rhs_start, len),
            DataType::UInt16 => dictionary_equal::<u16>(lhs, rhs, lhs_start, rhs_start, len),
            DataType::UInt32 => dictionary_equal::<u32>(lhs, rhs, lhs_start, rhs_start, len),
            DataType::UInt64 => dictionary_equal::<u64>(lhs, rhs, lhs_start, rhs_start, len),
            _ => unreachable!(),
        },
        DataType::Float16 => primitive_equal::<f16>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::Map(_, _) => list_equal::<i32>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::RunEndEncoded(_, _) => run_equal(lhs, rhs, lhs_start, rhs_start, len),
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
///
/// Two arrays are logically equal if and only if:
/// * their data types are equal
/// * their lengths are equal
/// * their null counts are equal
/// * their null bitmaps are equal
/// * each of their items are equal
///
/// Two items are equal when their in-memory representation is physically equal
/// (i.e. has the same bit content).
///
/// The physical comparison depend on the data type.
///
/// # Panics
///
/// This function may panic whenever any of the [ArrayData] does not follow the
/// Arrow specification. (e.g. wrong number of buffers, buffer `len` does not
/// correspond to the declared `len`)
pub fn equal(lhs: &ArrayData, rhs: &ArrayData) -> bool {
    utils::base_equal(lhs, rhs)
        && lhs.null_count() == rhs.null_count()
        && utils::equal_nulls(lhs, rhs, 0, 0, lhs.len())
        && equal_values(lhs, rhs, 0, 0, lhs.len())
}

// See arrow/tests/array_equal.rs for tests
