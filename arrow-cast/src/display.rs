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

use arrow_array::timezone::Tz;
use arrow_array::types::*;
use arrow_array::*;
use arrow_buffer::ArrowNativeType;
use arrow_schema::*;
use chrono::prelude::SecondsFormat;
use chrono::{DateTime, Utc};

fn invalid_cast_error(dt: &str, col_idx: usize, row_idx: usize) -> ArrowError {
    ArrowError::CastError(format!(
        "Cannot cast to {dt} at col index: {col_idx} row index: {row_idx}"
    ))
}

macro_rules! make_string {
    ($array_type:ty, $column: ident, $row: ident) => {{
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();

        Ok(array.value($row).to_string())
    }};
}

macro_rules! make_string_interval_year_month {
    ($column: ident, $row: ident) => {{
        let array = $column
            .as_any()
            .downcast_ref::<array::IntervalYearMonthArray>()
            .unwrap();

        let interval = array.value($row) as f64;
        let years = (interval / 12_f64).floor();
        let month = interval - (years * 12_f64);

        Ok(format!(
            "{} years {} mons 0 days 0 hours 0 mins 0.00 secs",
            years, month,
        ))
    }};
}

macro_rules! make_string_interval_day_time {
    ($column: ident, $row: ident) => {{
        let array = $column
            .as_any()
            .downcast_ref::<array::IntervalDayTimeArray>()
            .unwrap();

        let value: u64 = array.value($row) as u64;

        let days_parts: i32 = ((value & 0xFFFFFFFF00000000) >> 32) as i32;
        let milliseconds_part: i32 = (value & 0xFFFFFFFF) as i32;

        let secs = milliseconds_part / 1_000;
        let mins = secs / 60;
        let hours = mins / 60;

        let secs = secs - (mins * 60);
        let mins = mins - (hours * 60);

        let milliseconds = milliseconds_part % 1_000;

        let secs_sign = if secs < 0 || milliseconds < 0 {
            "-"
        } else {
            ""
        };

        Ok(format!(
            "0 years 0 mons {} days {} hours {} mins {}{}.{:03} secs",
            days_parts,
            hours,
            mins,
            secs_sign,
            secs.abs(),
            milliseconds.abs(),
        ))
    }};
}

macro_rules! make_string_interval_month_day_nano {
    ($column: ident, $row: ident) => {{
        let array = $column
            .as_any()
            .downcast_ref::<array::IntervalMonthDayNanoArray>()
            .unwrap();

        let value: u128 = array.value($row) as u128;

        let months_part: i32 =
            ((value & 0xFFFFFFFF000000000000000000000000) >> 96) as i32;
        let days_part: i32 = ((value & 0xFFFFFFFF0000000000000000) >> 64) as i32;
        let nanoseconds_part: i64 = (value & 0xFFFFFFFFFFFFFFFF) as i64;

        let secs = nanoseconds_part / 1_000_000_000;
        let mins = secs / 60;
        let hours = mins / 60;

        let secs = secs - (mins * 60);
        let mins = mins - (hours * 60);

        let nanoseconds = nanoseconds_part % 1_000_000_000;

        let secs_sign = if secs < 0 || nanoseconds < 0 { "-" } else { "" };

        Ok(format!(
            "0 years {} mons {} days {} hours {} mins {}{}.{:09} secs",
            months_part,
            days_part,
            hours,
            mins,
            secs_sign,
            secs.abs(),
            nanoseconds.abs(),
        ))
    }};
}

macro_rules! make_string_date {
    ($array_type:ty, $dt:expr, $column: ident, $col_idx:ident, $row_idx: ident) => {{
        Ok($column
            .as_any()
            .downcast_ref::<$array_type>()
            .ok_or_else(|| invalid_cast_error($dt, $col_idx, $row_idx))?
            .value_as_date($row_idx)
            .ok_or_else(|| invalid_cast_error($dt, $col_idx, $row_idx))?
            .to_string())
    }};
}

macro_rules! make_string_date_with_format {
    ($array_type:ty, $dt:expr, $format: ident, $column: ident, $col_idx:ident, $row_idx: ident) => {{
        Ok($column
            .as_any()
            .downcast_ref::<$array_type>()
            .ok_or_else(|| invalid_cast_error($dt, $col_idx, $row_idx))?
            .value_as_datetime($row_idx)
            .ok_or_else(|| invalid_cast_error($dt, $col_idx, $row_idx))?
            .format($format)
            .to_string())
    }};
}

macro_rules! handle_string_date {
    ($array_type:ty, $dt:expr, $format: ident, $column: ident, $col_idx:ident, $row_idx: ident) => {{
        match $format {
            Some(format) => {
                make_string_date_with_format!(
                    $array_type,
                    $dt,
                    format,
                    $column,
                    $col_idx,
                    $row_idx
                )
            }
            None => make_string_date!($array_type, $dt, $column, $col_idx, $row_idx),
        }
    }};
}

macro_rules! make_string_time {
    ($array_type:ty, $dt:expr, $column: ident, $col_idx:ident, $row_idx: ident) => {{
        Ok($column
            .as_any()
            .downcast_ref::<$array_type>()
            .ok_or_else(|| invalid_cast_error($dt, $col_idx, $row_idx))?
            .value_as_time($row_idx)
            .ok_or_else(|| invalid_cast_error($dt, $col_idx, $row_idx))?
            .to_string())
    }};
}

macro_rules! make_string_time_with_format {
    ($array_type:ty, $dt:expr, $format: ident, $column: ident, $col_idx:ident, $row_idx: ident) => {{
        Ok($column
            .as_any()
            .downcast_ref::<$array_type>()
            .ok_or_else(|| invalid_cast_error($dt, $col_idx, $row_idx))?
            .value_as_time($row_idx)
            .ok_or_else(|| invalid_cast_error($dt, $col_idx, $row_idx))?
            .format($format)
            .to_string())
    }};
}

macro_rules! handle_string_time {
    ($array_type:ty, $dt:expr, $format: ident, $column: ident, $col_idx:ident, $row_idx: ident) => {
        match $format {
            Some(format) => {
                make_string_time_with_format!(
                    $array_type,
                    $dt,
                    format,
                    $column,
                    $col_idx,
                    $row_idx
                )
            }
            None => make_string_time!($array_type, $dt, $column, $col_idx, $row_idx),
        }
    };
}

macro_rules! make_string_datetime {
    ($array_type:ty, $dt:expr, $tz_string: ident, $column: ident, $col_idx:ident, $row_idx: ident) => {{
        let array = $column
            .as_any()
            .downcast_ref::<$array_type>()
            .ok_or_else(|| invalid_cast_error($dt, $col_idx, $row_idx))?;

        let s = match $tz_string {
            Some(tz_string) => match tz_string.parse::<Tz>() {
                Ok(tz) => array
                    .value_as_datetime_with_tz($row_idx, tz)
                    .ok_or_else(|| invalid_cast_error($dt, $col_idx, $row_idx))?
                    .to_rfc3339_opts(SecondsFormat::AutoSi, true)
                    .to_string(),
                Err(_) => {
                    let datetime = array
                        .value_as_datetime($row_idx)
                        .ok_or_else(|| invalid_cast_error($dt, $col_idx, $row_idx))?;
                    format!("{:?} (Unknown Time Zone '{}')", datetime, tz_string)
                }
            },
            None => {
                let datetime = array
                    .value_as_datetime($row_idx)
                    .ok_or_else(|| invalid_cast_error($dt, $col_idx, $row_idx))?;
                format!("{:?}", datetime)
            }
        };

        Ok(s)
    }};
}

macro_rules! make_string_datetime_with_format {
    ($array_type:ty, $dt:expr, $format: ident, $tz_string: ident, $column: ident, $col_idx:ident, $row_idx: ident) => {{
        let array = $column
            .as_any()
            .downcast_ref::<$array_type>()
            .ok_or_else(|| invalid_cast_error($dt, $col_idx, $row_idx))?;
        let datetime = array
            .value_as_datetime($row_idx)
            .ok_or_else(|| invalid_cast_error($dt, $col_idx, $row_idx))?;

        let s = match $tz_string {
            Some(tz_string) => match tz_string.parse::<Tz>() {
                Ok(tz) => {
                    let utc_time = DateTime::<Utc>::from_utc(datetime, Utc);
                    let local_time = utc_time.with_timezone(&tz);
                    local_time.format($format).to_string()
                }
                Err(_) => {
                    format!("{:?} (Unknown Time Zone '{}')", datetime, tz_string)
                }
            },
            None => datetime.format($format).to_string(),
        };

        Ok(s)
    }};
}

macro_rules! handle_string_datetime {
    ($array_type:ty, $dt:expr, $format: ident, $tz_string: ident, $column: ident, $col_idx:ident, $row_idx: ident) => {
        match $format {
            Some(format) => make_string_datetime_with_format!(
                $array_type,
                $dt,
                format,
                $tz_string,
                $column,
                $col_idx,
                $row_idx
            ),
            None => make_string_datetime!(
                $array_type,
                $dt,
                $tz_string,
                $column,
                $col_idx,
                $row_idx
            ),
        }
    };
}

// It's not possible to do array.value($row).to_string() for &[u8], let's format it as hex
macro_rules! make_string_hex {
    ($array_type:ty, $column: ident, $row: ident) => {{
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();

        let mut tmp = "".to_string();

        for character in array.value($row) {
            let _ = write!(tmp, "{:02x}", character);
        }

        Ok(tmp)
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
            .collect::<Result<Vec<_>, _>>()?;
        Ok(format!("[{}]", string_values.join(", ")))
    }};
}

macro_rules! make_string_from_large_list {
    ($column: ident, $row: ident) => {{
        let list = $column
            .as_any()
            .downcast_ref::<array::LargeListArray>()
            .ok_or(ArrowError::InvalidArgumentError(format!(
                "Repl error: could not convert large list column to list array."
            )))?
            .value($row);
        let string_values = (0..list.len())
            .map(|i| array_value_to_string(&list, i))
            .collect::<Result<Vec<_>, _>>()?;
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
            .collect::<Result<Vec<_>, _>>()?;
        Ok(format!("[{}]", string_values.join(", ")))
    }};
}

macro_rules! make_string_from_duration {
    ($array_type:ty, $dt:expr, $column:ident, $col_idx:ident, $row_idx: ident) => {{
        Ok($column
            .as_any()
            .downcast_ref::<$array_type>()
            .ok_or_else(|| invalid_cast_error($dt, $col_idx, $row_idx))?
            .value_as_duration($row_idx)
            .ok_or_else(|| invalid_cast_error($dt, $col_idx, $row_idx))?
            .to_string())
    }};
}

#[inline(always)]
pub fn make_string_from_decimal(
    column: &Arc<dyn Array>,
    row: usize,
) -> Result<String, ArrowError> {
    let array = column.as_any().downcast_ref::<Decimal128Array>().unwrap();

    Ok(array.value_as_string(row))
}

fn append_struct_field_string(
    target: &mut String,
    name: &str,
    field_col: &Arc<dyn Array>,
    row: usize,
) -> Result<(), ArrowError> {
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

fn append_map_field_string(
    target: &mut String,
    field_col: &Arc<dyn Array>,
    row: usize,
) -> Result<(), ArrowError> {
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
fn array_value_to_string_internal(
    column: &ArrayRef,
    col_idx: usize,
    row_idx: usize,
    format: Option<&str>,
) -> Result<String, ArrowError> {
    if column.is_null(row_idx) {
        return Ok("".to_string());
    }
    match column.data_type() {
        DataType::Utf8 => make_string!(array::StringArray, column, row_idx),
        DataType::LargeUtf8 => make_string!(array::LargeStringArray, column, row_idx),
        DataType::Binary => make_string_hex!(array::BinaryArray, column, row_idx),
        DataType::LargeBinary => {
            make_string_hex!(array::LargeBinaryArray, column, row_idx)
        }
        DataType::FixedSizeBinary(_) => {
            make_string_hex!(array::FixedSizeBinaryArray, column, row_idx)
        }
        DataType::Boolean => make_string!(array::BooleanArray, column, row_idx),
        DataType::Int8 => make_string!(array::Int8Array, column, row_idx),
        DataType::Int16 => make_string!(array::Int16Array, column, row_idx),
        DataType::Int32 => make_string!(array::Int32Array, column, row_idx),
        DataType::Int64 => make_string!(array::Int64Array, column, row_idx),
        DataType::UInt8 => make_string!(array::UInt8Array, column, row_idx),
        DataType::UInt16 => make_string!(array::UInt16Array, column, row_idx),
        DataType::UInt32 => make_string!(array::UInt32Array, column, row_idx),
        DataType::UInt64 => make_string!(array::UInt64Array, column, row_idx),
        DataType::Float16 => make_string!(array::Float16Array, column, row_idx),
        DataType::Float32 => make_string!(array::Float32Array, column, row_idx),
        DataType::Float64 => make_string!(array::Float64Array, column, row_idx),
        DataType::Decimal128(..) => make_string_from_decimal(column, row_idx),
        DataType::Timestamp(unit, tz_string_opt) if *unit == TimeUnit::Second => {
            handle_string_datetime!(
                array::TimestampSecondArray,
                "Timestamp",
                format,
                tz_string_opt,
                column,
                col_idx,
                row_idx
            )
        }
        DataType::Timestamp(unit, tz_string_opt) if *unit == TimeUnit::Millisecond => {
            handle_string_datetime!(
                array::TimestampMillisecondArray,
                "Timestamp",
                format,
                tz_string_opt,
                column,
                col_idx,
                row_idx
            )
        }
        DataType::Timestamp(unit, tz_string_opt) if *unit == TimeUnit::Microsecond => {
            handle_string_datetime!(
                array::TimestampMicrosecondArray,
                "Timestamp",
                format,
                tz_string_opt,
                column,
                col_idx,
                row_idx
            )
        }
        DataType::Timestamp(unit, tz_string_opt) if *unit == TimeUnit::Nanosecond => {
            handle_string_datetime!(
                array::TimestampNanosecondArray,
                "Timestamp",
                format,
                tz_string_opt,
                column,
                col_idx,
                row_idx
            )
        }
        DataType::Date32 => {
            handle_string_date!(
                array::Date32Array,
                "Date32",
                format,
                column,
                col_idx,
                row_idx
            )
        }
        DataType::Date64 => {
            handle_string_date!(
                array::Date64Array,
                "Date64",
                format,
                column,
                col_idx,
                row_idx
            )
        }
        DataType::Time32(unit) if *unit == TimeUnit::Second => {
            handle_string_time!(
                array::Time32SecondArray,
                "Time32",
                format,
                column,
                col_idx,
                row_idx
            )
        }
        DataType::Time32(unit) if *unit == TimeUnit::Millisecond => {
            handle_string_time!(
                array::Time32MillisecondArray,
                "Time32",
                format,
                column,
                col_idx,
                row_idx
            )
        }
        DataType::Time64(unit) if *unit == TimeUnit::Microsecond => {
            handle_string_time!(
                array::Time64MicrosecondArray,
                "Time64",
                format,
                column,
                col_idx,
                row_idx
            )
        }
        DataType::Time64(unit) if *unit == TimeUnit::Nanosecond => {
            handle_string_time!(
                array::Time64NanosecondArray,
                "Time64",
                format,
                column,
                col_idx,
                row_idx
            )
        }
        DataType::Interval(unit) => match unit {
            IntervalUnit::DayTime => {
                make_string_interval_day_time!(column, row_idx)
            }
            IntervalUnit::YearMonth => {
                make_string_interval_year_month!(column, row_idx)
            }
            IntervalUnit::MonthDayNano => {
                make_string_interval_month_day_nano!(column, row_idx)
            }
        },
        DataType::List(_) => make_string_from_list!(column, row_idx),
        DataType::LargeList(_) => make_string_from_large_list!(column, row_idx),
        DataType::Dictionary(index_type, _value_type) => match **index_type {
            DataType::Int8 => dict_array_value_to_string::<Int8Type>(column, row_idx),
            DataType::Int16 => dict_array_value_to_string::<Int16Type>(column, row_idx),
            DataType::Int32 => dict_array_value_to_string::<Int32Type>(column, row_idx),
            DataType::Int64 => dict_array_value_to_string::<Int64Type>(column, row_idx),
            DataType::UInt8 => dict_array_value_to_string::<UInt8Type>(column, row_idx),
            DataType::UInt16 => dict_array_value_to_string::<UInt16Type>(column, row_idx),
            DataType::UInt32 => dict_array_value_to_string::<UInt32Type>(column, row_idx),
            DataType::UInt64 => dict_array_value_to_string::<UInt64Type>(column, row_idx),
            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Pretty printing not supported for {:?} due to index type",
                column.data_type()
            ))),
        },
        DataType::FixedSizeList(_, _) => {
            make_string_from_fixed_size_list!(column, row_idx)
        }
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
            let mut kv_iter = st.columns().iter().zip(st.column_names());
            if let Some((col, name)) = kv_iter.next() {
                append_struct_field_string(&mut s, name, col, row_idx)?;
            }
            for (col, name) in kv_iter {
                s.push_str(", ");
                append_struct_field_string(&mut s, name, col, row_idx)?;
            }
            s.push('}');

            Ok(s)
        }
        DataType::Map(_, _) => {
            let map_array =
                column.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
                    ArrowError::InvalidArgumentError(
                        "Repl error: could not convert column to map array.".to_string(),
                    )
                })?;
            let map_entry = map_array.value(row_idx);
            let st = map_entry
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    ArrowError::InvalidArgumentError(
                        "Repl error: could not convert map entry to struct array."
                            .to_string(),
                    )
                })?;
            let mut s = String::new();
            s.push('{');
            let entries_count = st.column(0).len();
            for i in 0..entries_count {
                if i > 0 {
                    s.push_str(", ");
                }
                append_map_field_string(&mut s, st.column(0), i)?;
                s.push_str(": ");
                append_map_field_string(&mut s, st.column(1), i)?;
            }
            s.push('}');

            Ok(s)
        }
        DataType::Union(field_vec, type_ids, mode) => {
            union_to_string(column, row_idx, field_vec, type_ids, mode)
        }
        DataType::Duration(unit) => match *unit {
            TimeUnit::Second => {
                make_string_from_duration!(
                    array::DurationSecondArray,
                    "Duration",
                    column,
                    col_idx,
                    row_idx
                )
            }
            TimeUnit::Millisecond => {
                make_string_from_duration!(
                    array::DurationMillisecondArray,
                    "Duration",
                    column,
                    col_idx,
                    row_idx
                )
            }
            TimeUnit::Microsecond => {
                make_string_from_duration!(
                    array::DurationMicrosecondArray,
                    "Duration",
                    column,
                    col_idx,
                    row_idx
                )
            }
            TimeUnit::Nanosecond => {
                make_string_from_duration!(
                    array::DurationNanosecondArray,
                    "Duration",
                    column,
                    col_idx,
                    row_idx
                )
            }
        },
        _ => Err(ArrowError::InvalidArgumentError(format!(
            "Pretty printing not implemented for {:?} type",
            column.data_type()
        ))),
    }
}

pub fn temporal_array_value_to_string(
    column: &ArrayRef,
    col_idx: usize,
    row_idx: usize,
    format: Option<&str>,
) -> Result<String, ArrowError> {
    array_value_to_string_internal(column, col_idx, row_idx, format)
}

pub fn array_value_to_string(
    column: &ArrayRef,
    row_idx: usize,
) -> Result<String, ArrowError> {
    array_value_to_string_internal(column, 0, row_idx, None)
}

/// Converts the value of the union array at `row` to a String
fn union_to_string(
    column: &ArrayRef,
    row: usize,
    fields: &[Field],
    type_ids: &[i8],
    mode: &UnionMode,
) -> Result<String, ArrowError> {
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
            "Repl error: could not get field name for type id: {type_id} in union array.",
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

    Ok(format!("{{{name}={value}}}"))
}
/// Converts the value of the dictionary array at `row` to a String
fn dict_array_value_to_string<K: ArrowPrimitiveType>(
    colum: &ArrayRef,
    row: usize,
) -> Result<String, ArrowError> {
    let dict_array = colum.as_any().downcast_ref::<DictionaryArray<K>>().unwrap();

    let keys_array = dict_array.keys();

    if keys_array.is_null(row) {
        return Ok(String::from(""));
    }

    let dict_index = keys_array.value(row).as_usize();
    array_value_to_string(dict_array.values(), dict_index)
}

/// Converts numeric type to a `String`
pub fn lexical_to_string<N: lexical_core::ToLexical>(n: N) -> String {
    let mut buf = Vec::<u8>::with_capacity(N::FORMATTED_SIZE_DECIMAL);
    unsafe {
        // JUSTIFICATION
        //  Benefit
        //      Allows using the faster serializer lexical core and convert to string
        //  Soundness
        //      Length of buf is set as written length afterwards. lexical_core
        //      creates a valid string, so doesn't need to be checked.
        let slice = std::slice::from_raw_parts_mut(buf.as_mut_ptr(), buf.capacity());
        let len = lexical_core::write(n, slice).len();
        buf.set_len(len);
        String::from_utf8_unchecked(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_map_arry_to_string() {
        let keys = vec!["a", "b", "c", "d", "e", "f", "g", "h"];
        let values_data = UInt32Array::from(vec![0u32, 10, 20, 30, 40, 50, 60, 70]);

        // Construct a buffer for value offsets, for the nested array:
        //  [[a, b, c], [d, e, f], [g, h]]
        let entry_offsets = [0, 3, 6, 8];

        let map_array = MapArray::new_from_strings(
            keys.clone().into_iter(),
            &values_data,
            &entry_offsets,
        )
        .unwrap();
        let param = Arc::new(map_array) as ArrayRef;
        assert_eq!(
            "{\"d\": 30, \"e\": 40, \"f\": 50}",
            array_value_to_string(&param, 1).unwrap()
        );
    }

    #[test]
    fn test_array_value_to_string_duration() {
        let ns_array =
            Arc::new(DurationNanosecondArray::from(vec![Some(1), None])) as ArrayRef;
        assert_eq!(
            array_value_to_string(&ns_array, 0).unwrap(),
            "PT0.000000001S"
        );
        assert_eq!(array_value_to_string(&ns_array, 1).unwrap(), "");

        let us_array =
            Arc::new(DurationMicrosecondArray::from(vec![Some(1), None])) as ArrayRef;
        assert_eq!(array_value_to_string(&us_array, 0).unwrap(), "PT0.000001S");
        assert_eq!(array_value_to_string(&us_array, 1).unwrap(), "");

        let ms_array =
            Arc::new(DurationMillisecondArray::from(vec![Some(1), None])) as ArrayRef;
        assert_eq!(array_value_to_string(&ms_array, 0).unwrap(), "PT0.001S");
        assert_eq!(array_value_to_string(&ms_array, 1).unwrap(), "");

        let s_array =
            Arc::new(DurationSecondArray::from(vec![Some(1), None])) as ArrayRef;
        assert_eq!(array_value_to_string(&s_array, 0).unwrap(), "PT1S");
        assert_eq!(array_value_to_string(&s_array, 1).unwrap(), "");
    }
}
