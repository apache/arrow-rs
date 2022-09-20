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

//! Functions for printing array values, as strings, for debugging
//! purposes. See the `pretty` crate for additional functions for
//! record batch pretty printing.

use std::fmt::Write;
use std::sync::Arc;

use crate::array::Array;
use crate::datatypes::{
    ArrowNativeType, ArrowPrimitiveType, DataType, Field, Int16Type, Int32Type,
    Int64Type, Int8Type, TimeUnit, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
    UnionMode,
};
use crate::{array, datatypes::IntervalUnit};

use array::DictionaryArray;

use crate::error::{ArrowError, Result};

macro_rules! make_string {
    ($array_type:ty, $column: ident, $row: ident) => {{
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();

        let s = if array.is_null($row) {
            "".to_string()
        } else {
            array.value($row).to_string()
        };

        Ok(s)
    }};
}

macro_rules! make_string_interval_year_month {
    ($column: ident, $row: ident) => {{
        let array = $column
            .as_any()
            .downcast_ref::<array::IntervalYearMonthArray>()
            .unwrap();

        let s = if array.is_null($row) {
            "NULL".to_string()
        } else {
            let interval = array.value($row) as f64;
            let years = (interval / 12_f64).floor();
            let month = interval - (years * 12_f64);

            format!(
                "{} years {} mons 0 days 0 hours 0 mins 0.00 secs",
                years, month,
            )
        };

        Ok(s)
    }};
}

macro_rules! make_string_interval_day_time {
    ($column: ident, $row: ident) => {{
        let array = $column
            .as_any()
            .downcast_ref::<array::IntervalDayTimeArray>()
            .unwrap();

        let s = if array.is_null($row) {
            "NULL".to_string()
        } else {
            let value: u64 = array.value($row) as u64;

            let days_parts: i32 = ((value & 0xFFFFFFFF00000000) >> 32) as i32;
            let milliseconds_part: i32 = (value & 0xFFFFFFFF) as i32;

            let secs = milliseconds_part / 1000;
            let mins = secs / 60;
            let hours = mins / 60;

            let secs = secs - (mins * 60);
            let mins = mins - (hours * 60);

            format!(
                "0 years 0 mons {} days {} hours {} mins {}.{:02} secs",
                days_parts,
                hours,
                mins,
                secs,
                (milliseconds_part % 1000),
            )
        };

        Ok(s)
    }};
}

macro_rules! make_string_interval_month_day_nano {
    ($column: ident, $row: ident) => {{
        let array = $column
            .as_any()
            .downcast_ref::<array::IntervalMonthDayNanoArray>()
            .unwrap();

        let s = if array.is_null($row) {
            "NULL".to_string()
        } else {
            let value: u128 = array.value($row) as u128;

            let months_part: i32 =
                ((value & 0xFFFFFFFF000000000000000000000000) >> 96) as i32;
            let days_part: i32 = ((value & 0xFFFFFFFF0000000000000000) >> 64) as i32;
            let nanoseconds_part: i64 = (value & 0xFFFFFFFFFFFFFFFF) as i64;

            let secs = nanoseconds_part / 1000000000;
            let mins = secs / 60;
            let hours = mins / 60;

            let secs = secs - (mins * 60);
            let mins = mins - (hours * 60);

            format!(
                "0 years {} mons {} days {} hours {} mins {}.{:02} secs",
                months_part,
                days_part,
                hours,
                mins,
                secs,
                (nanoseconds_part % 1000000000),
            )
        };

        Ok(s)
    }};
}

macro_rules! make_string_date {
    ($array_type:ty, $column: ident, $row: ident) => {{
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();

        let s = if array.is_null($row) {
            "".to_string()
        } else {
            array
                .value_as_date($row)
                .map(|d| d.to_string())
                .unwrap_or_else(|| "ERROR CONVERTING DATE".to_string())
        };

        Ok(s)
    }};
}

macro_rules! make_string_time {
    ($array_type:ty, $column: ident, $row: ident) => {{
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();

        let s = if array.is_null($row) {
            "".to_string()
        } else {
            array
                .value_as_time($row)
                .map(|d| d.to_string())
                .unwrap_or_else(|| "ERROR CONVERTING DATE".to_string())
        };

        Ok(s)
    }};
}

macro_rules! make_string_datetime {
    ($array_type:ty, $column: ident, $row: ident) => {{
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();

        let s = if array.is_null($row) {
            "".to_string()
        } else {
            array
                .value_as_datetime($row)
                .map(|d| d.to_string())
                .unwrap_or_else(|| "ERROR CONVERTING DATE".to_string())
        };

        Ok(s)
    }};
}

// It's not possible to do array.value($row).to_string() for &[u8], let's format it as hex
macro_rules! make_string_hex {
    ($array_type:ty, $column: ident, $row: ident) => {{
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();

        let s = if array.is_null($row) {
            "".to_string()
        } else {
            let mut tmp = "".to_string();

            for character in array.value($row) {
                let _ = write!(tmp, "{:02x}", character);
            }

            tmp
        };

        Ok(s)
    }};
}

macro_rules! make_string_from_list {
    ($column: ident, $row: ident) => {{
        let list = $column
            .as_any()
            .downcast_ref::<array::ListArray>()
            .ok_or(ArrowError::InvalidArgumentError(format!(
                "Repl error: could not convert list column to list array."
            )))?
            .value($row);
        let string_values = (0..list.len())
            .map(|i| array_value_to_string(&list.clone(), i))
            .collect::<Result<Vec<String>>>()?;
        Ok(format!("[{}]", string_values.join(", ")))
    }};
}

macro_rules! make_string_from_fixed_size_list {
    ($column: ident, $row: ident) => {{
        let list = $column
            .as_any()
            .downcast_ref::<array::FixedSizeListArray>()
            .ok_or(ArrowError::InvalidArgumentError(format!(
                "Repl error: could not convert list column to list array."
            )))?
            .value($row);
        let string_values = (0..list.len())
            .map(|i| array_value_to_string(&list.clone(), i))
            .collect::<Result<Vec<String>>>()?;
        Ok(format!("[{}]", string_values.join(", ")))
    }};
}

#[inline(always)]
pub fn make_string_from_decimal(column: &Arc<dyn Array>, row: usize) -> Result<String> {
    let array = column
        .as_any()
        .downcast_ref::<array::Decimal128Array>()
        .unwrap();

    let formatted_decimal = array.value_as_string(row);
    Ok(formatted_decimal)
}

fn append_struct_field_string(
    target: &mut String,
    name: &str,
    field_col: &Arc<dyn Array>,
    row: usize,
) -> Result<()> {
    target.push('"');
    target.push_str(name);
    target.push_str("\": ");

    if field_col.is_null(row) {
        target.push_str("null");
    } else {
        match field_col.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => {
                target.push('"');
                target.push_str(array_value_to_string(field_col, row)?.as_str());
                target.push('"');
            }
            _ => {
                target.push_str(array_value_to_string(field_col, row)?.as_str());
            }
        }
    }

    Ok(())
}

/// Get the value at the given row in an array as a String.
///
/// Note this function is quite inefficient and is unlikely to be
/// suitable for converting large arrays or record batches.
pub fn array_value_to_string(column: &array::ArrayRef, row: usize) -> Result<String> {
    if column.is_null(row) {
        return Ok("".to_string());
    }
    match column.data_type() {
        DataType::Utf8 => make_string!(array::StringArray, column, row),
        DataType::LargeUtf8 => make_string!(array::LargeStringArray, column, row),
        DataType::Binary => make_string_hex!(array::BinaryArray, column, row),
        DataType::LargeBinary => make_string_hex!(array::LargeBinaryArray, column, row),
        DataType::FixedSizeBinary(_) => {
            make_string_hex!(array::FixedSizeBinaryArray, column, row)
        }
        DataType::Boolean => make_string!(array::BooleanArray, column, row),
        DataType::Int8 => make_string!(array::Int8Array, column, row),
        DataType::Int16 => make_string!(array::Int16Array, column, row),
        DataType::Int32 => make_string!(array::Int32Array, column, row),
        DataType::Int64 => make_string!(array::Int64Array, column, row),
        DataType::UInt8 => make_string!(array::UInt8Array, column, row),
        DataType::UInt16 => make_string!(array::UInt16Array, column, row),
        DataType::UInt32 => make_string!(array::UInt32Array, column, row),
        DataType::UInt64 => make_string!(array::UInt64Array, column, row),
        DataType::Float16 => make_string!(array::Float16Array, column, row),
        DataType::Float32 => make_string!(array::Float32Array, column, row),
        DataType::Float64 => make_string!(array::Float64Array, column, row),
        DataType::Decimal128(..) => make_string_from_decimal(column, row),
        DataType::Timestamp(unit, _) if *unit == TimeUnit::Second => {
            make_string_datetime!(array::TimestampSecondArray, column, row)
        }
        DataType::Timestamp(unit, _) if *unit == TimeUnit::Millisecond => {
            make_string_datetime!(array::TimestampMillisecondArray, column, row)
        }
        DataType::Timestamp(unit, _) if *unit == TimeUnit::Microsecond => {
            make_string_datetime!(array::TimestampMicrosecondArray, column, row)
        }
        DataType::Timestamp(unit, _) if *unit == TimeUnit::Nanosecond => {
            make_string_datetime!(array::TimestampNanosecondArray, column, row)
        }
        DataType::Date32 => make_string_date!(array::Date32Array, column, row),
        DataType::Date64 => make_string_date!(array::Date64Array, column, row),
        DataType::Time32(unit) if *unit == TimeUnit::Second => {
            make_string_time!(array::Time32SecondArray, column, row)
        }
        DataType::Time32(unit) if *unit == TimeUnit::Millisecond => {
            make_string_time!(array::Time32MillisecondArray, column, row)
        }
        DataType::Time64(unit) if *unit == TimeUnit::Microsecond => {
            make_string_time!(array::Time64MicrosecondArray, column, row)
        }
        DataType::Time64(unit) if *unit == TimeUnit::Nanosecond => {
            make_string_time!(array::Time64NanosecondArray, column, row)
        }
        DataType::Interval(unit) => match unit {
            IntervalUnit::DayTime => {
                make_string_interval_day_time!(column, row)
            }
            IntervalUnit::YearMonth => {
                make_string_interval_year_month!(column, row)
            }
            IntervalUnit::MonthDayNano => {
                make_string_interval_month_day_nano!(column, row)
            }
        },
        DataType::List(_) => make_string_from_list!(column, row),
        DataType::Dictionary(index_type, _value_type) => match **index_type {
            DataType::Int8 => dict_array_value_to_string::<Int8Type>(column, row),
            DataType::Int16 => dict_array_value_to_string::<Int16Type>(column, row),
            DataType::Int32 => dict_array_value_to_string::<Int32Type>(column, row),
            DataType::Int64 => dict_array_value_to_string::<Int64Type>(column, row),
            DataType::UInt8 => dict_array_value_to_string::<UInt8Type>(column, row),
            DataType::UInt16 => dict_array_value_to_string::<UInt16Type>(column, row),
            DataType::UInt32 => dict_array_value_to_string::<UInt32Type>(column, row),
            DataType::UInt64 => dict_array_value_to_string::<UInt64Type>(column, row),
            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Pretty printing not supported for {:?} due to index type",
                column.data_type()
            ))),
        },
        DataType::FixedSizeList(_, _) => make_string_from_fixed_size_list!(column, row),
        DataType::Struct(_) => {
            let st = column
                .as_any()
                .downcast_ref::<array::StructArray>()
                .ok_or_else(|| {
                    ArrowError::InvalidArgumentError(
                        "Repl error: could not convert struct column to struct array."
                            .to_string(),
                    )
                })?;

            let mut s = String::new();
            s.push('{');
            let mut kv_iter = st.columns().into_iter().zip(st.column_names().into_iter());
            if let Some((col, name)) = kv_iter.next() {
                append_struct_field_string(&mut s, name, col, row)?;
            }
            for (col, name) in kv_iter {
                s.push_str(", ");
                append_struct_field_string(&mut s, name, col, row)?;
            }
            s.push('}');

            Ok(s)
        }
        DataType::Union(field_vec, type_ids, mode) => {
            union_to_string(column, row, field_vec, type_ids, mode)
        }
        _ => Err(ArrowError::InvalidArgumentError(format!(
            "Pretty printing not implemented for {:?} type",
            column.data_type()
        ))),
    }
}

/// Converts the value of the union array at `row` to a String
fn union_to_string(
    column: &array::ArrayRef,
    row: usize,
    fields: &[Field],
    type_ids: &[i8],
    mode: &UnionMode,
) -> Result<String> {
    let list = column
        .as_any()
        .downcast_ref::<array::UnionArray>()
        .ok_or_else(|| {
            ArrowError::InvalidArgumentError(
                "Repl error: could not convert union column to union array.".to_string(),
            )
        })?;
    let type_id = list.type_id(row);
    let field_idx = type_ids.iter().position(|t| t == &type_id).ok_or_else(|| {
        ArrowError::InvalidArgumentError(format!(
            "Repl error: could not get field name for type id: {} in union array.",
            type_id,
        ))
    })?;
    let name = fields.get(field_idx).unwrap().name();

    let value = array_value_to_string(
        list.child(type_id),
        match mode {
            UnionMode::Dense => list.value_offset(row) as usize,
            UnionMode::Sparse => row,
        },
    )?;

    Ok(format!("{{{}={}}}", name, value))
}
/// Converts the value of the dictionary array at `row` to a String
fn dict_array_value_to_string<K: ArrowPrimitiveType>(
    colum: &array::ArrayRef,
    row: usize,
) -> Result<String> {
    let dict_array = colum.as_any().downcast_ref::<DictionaryArray<K>>().unwrap();

    let keys_array = dict_array.keys();

    if keys_array.is_null(row) {
        return Ok(String::from(""));
    }

    let dict_index = keys_array.value(row).to_usize().ok_or_else(|| {
        ArrowError::InvalidArgumentError(format!(
            "Can not convert value {:?} at index {:?} to usize for string conversion.",
            keys_array.value(row),
            row
        ))
    })?;

    array_value_to_string(dict_array.values(), dict_index)
}
