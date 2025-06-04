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

use arrow::datatypes::{DataType, Field, Fields, IntervalUnit, TimeUnit, UnionMode};
use arrow::error::{ArrowError, Result};
use std::sync::Arc;

/// Parse a data type from a JSON representation.
pub fn data_type_from_json(json: &serde_json::Value) -> Result<DataType> {
    use serde_json::Value;
    let default_field = Arc::new(Field::new("", DataType::Boolean, true));
    match *json {
        Value::Object(ref map) => match map.get("name") {
            Some(s) if s == "null" => Ok(DataType::Null),
            Some(s) if s == "bool" => Ok(DataType::Boolean),
            Some(s) if s == "binary" => Ok(DataType::Binary),
            Some(s) if s == "largebinary" => Ok(DataType::LargeBinary),
            Some(s) if s == "utf8" => Ok(DataType::Utf8),
            Some(s) if s == "largeutf8" => Ok(DataType::LargeUtf8),
            Some(s) if s == "fixedsizebinary" => {
                // return a list with any type as its child isn't defined in the map
                if let Some(Value::Number(size)) = map.get("byteWidth") {
                    Ok(DataType::FixedSizeBinary(size.as_i64().unwrap() as i32))
                } else {
                    Err(ArrowError::ParseError(
                        "Expecting a byteWidth for fixedsizebinary".to_string(),
                    ))
                }
            }
            Some(s) if s == "decimal" => {
                // return a list with any type as its child isn't defined in the map
                let precision = match map.get("precision") {
                    Some(p) => Ok(p.as_u64().unwrap().try_into().unwrap()),
                    None => Err(ArrowError::ParseError(
                        "Expecting a precision for decimal".to_string(),
                    )),
                }?;
                let scale = match map.get("scale") {
                    Some(s) => Ok(s.as_u64().unwrap().try_into().unwrap()),
                    _ => Err(ArrowError::ParseError(
                        "Expecting a scale for decimal".to_string(),
                    )),
                }?;
                let bit_width: usize = match map.get("bitWidth") {
                    Some(b) => b.as_u64().unwrap() as usize,
                    _ => 128, // Default bit width
                };

                match bit_width {
                    128 => Ok(DataType::Decimal128(precision, scale)),
                    256 => Ok(DataType::Decimal256(precision, scale)),
                    _ => Err(ArrowError::ParseError(
                        "Decimal bit_width invalid".to_string(),
                    )),
                }
            }
            Some(s) if s == "floatingpoint" => match map.get("precision") {
                Some(p) if p == "HALF" => Ok(DataType::Float16),
                Some(p) if p == "SINGLE" => Ok(DataType::Float32),
                Some(p) if p == "DOUBLE" => Ok(DataType::Float64),
                _ => Err(ArrowError::ParseError(
                    "floatingpoint precision missing or invalid".to_string(),
                )),
            },
            Some(s) if s == "timestamp" => {
                let unit = match map.get("unit") {
                    Some(p) if p == "SECOND" => Ok(TimeUnit::Second),
                    Some(p) if p == "MILLISECOND" => Ok(TimeUnit::Millisecond),
                    Some(p) if p == "MICROSECOND" => Ok(TimeUnit::Microsecond),
                    Some(p) if p == "NANOSECOND" => Ok(TimeUnit::Nanosecond),
                    _ => Err(ArrowError::ParseError(
                        "timestamp unit missing or invalid".to_string(),
                    )),
                };
                let tz = match map.get("timezone") {
                    None => Ok(None),
                    Some(Value::String(tz)) => Ok(Some(tz.as_str().into())),
                    _ => Err(ArrowError::ParseError(
                        "timezone must be a string".to_string(),
                    )),
                };
                Ok(DataType::Timestamp(unit?, tz?))
            }
            Some(s) if s == "date" => match map.get("unit") {
                Some(p) if p == "DAY" => Ok(DataType::Date32),
                Some(p) if p == "MILLISECOND" => Ok(DataType::Date64),
                _ => Err(ArrowError::ParseError(
                    "date unit missing or invalid".to_string(),
                )),
            },
            Some(s) if s == "time" => {
                let unit = match map.get("unit") {
                    Some(p) if p == "SECOND" => Ok(TimeUnit::Second),
                    Some(p) if p == "MILLISECOND" => Ok(TimeUnit::Millisecond),
                    Some(p) if p == "MICROSECOND" => Ok(TimeUnit::Microsecond),
                    Some(p) if p == "NANOSECOND" => Ok(TimeUnit::Nanosecond),
                    _ => Err(ArrowError::ParseError(
                        "time unit missing or invalid".to_string(),
                    )),
                };
                match map.get("bitWidth") {
                    Some(p) if p == 32 => Ok(DataType::Time32(unit?)),
                    Some(p) if p == 64 => Ok(DataType::Time64(unit?)),
                    _ => Err(ArrowError::ParseError(
                        "time bitWidth missing or invalid".to_string(),
                    )),
                }
            }
            Some(s) if s == "duration" => match map.get("unit") {
                Some(p) if p == "SECOND" => Ok(DataType::Duration(TimeUnit::Second)),
                Some(p) if p == "MILLISECOND" => Ok(DataType::Duration(TimeUnit::Millisecond)),
                Some(p) if p == "MICROSECOND" => Ok(DataType::Duration(TimeUnit::Microsecond)),
                Some(p) if p == "NANOSECOND" => Ok(DataType::Duration(TimeUnit::Nanosecond)),
                _ => Err(ArrowError::ParseError(
                    "time unit missing or invalid".to_string(),
                )),
            },
            Some(s) if s == "interval" => match map.get("unit") {
                Some(p) if p == "DAY_TIME" => Ok(DataType::Interval(IntervalUnit::DayTime)),
                Some(p) if p == "YEAR_MONTH" => Ok(DataType::Interval(IntervalUnit::YearMonth)),
                Some(p) if p == "MONTH_DAY_NANO" => {
                    Ok(DataType::Interval(IntervalUnit::MonthDayNano))
                }
                _ => Err(ArrowError::ParseError(
                    "interval unit missing or invalid".to_string(),
                )),
            },
            Some(s) if s == "int" => match map.get("isSigned") {
                Some(&Value::Bool(true)) => match map.get("bitWidth") {
                    Some(Value::Number(n)) => match n.as_u64() {
                        Some(8) => Ok(DataType::Int8),
                        Some(16) => Ok(DataType::Int16),
                        Some(32) => Ok(DataType::Int32),
                        Some(64) => Ok(DataType::Int64),
                        _ => Err(ArrowError::ParseError(
                            "int bitWidth missing or invalid".to_string(),
                        )),
                    },
                    _ => Err(ArrowError::ParseError(
                        "int bitWidth missing or invalid".to_string(),
                    )),
                },
                Some(&Value::Bool(false)) => match map.get("bitWidth") {
                    Some(Value::Number(n)) => match n.as_u64() {
                        Some(8) => Ok(DataType::UInt8),
                        Some(16) => Ok(DataType::UInt16),
                        Some(32) => Ok(DataType::UInt32),
                        Some(64) => Ok(DataType::UInt64),
                        _ => Err(ArrowError::ParseError(
                            "int bitWidth missing or invalid".to_string(),
                        )),
                    },
                    _ => Err(ArrowError::ParseError(
                        "int bitWidth missing or invalid".to_string(),
                    )),
                },
                _ => Err(ArrowError::ParseError(
                    "int signed missing or invalid".to_string(),
                )),
            },
            Some(s) if s == "list" => {
                // return a list with any type as its child isn't defined in the map
                Ok(DataType::List(default_field))
            }
            Some(s) if s == "largelist" => {
                // return a largelist with any type as its child isn't defined in the map
                Ok(DataType::LargeList(default_field))
            }
            Some(s) if s == "fixedsizelist" => {
                // return a list with any type as its child isn't defined in the map
                if let Some(Value::Number(size)) = map.get("listSize") {
                    Ok(DataType::FixedSizeList(
                        default_field,
                        size.as_i64().unwrap() as i32,
                    ))
                } else {
                    Err(ArrowError::ParseError(
                        "Expecting a listSize for fixedsizelist".to_string(),
                    ))
                }
            }
            Some(s) if s == "struct" => {
                // return an empty `struct` type as its children aren't defined in the map
                Ok(DataType::Struct(Fields::empty()))
            }
            Some(s) if s == "map" => {
                if let Some(Value::Bool(keys_sorted)) = map.get("keysSorted") {
                    // Return a map with an empty type as its children aren't defined in the map
                    Ok(DataType::Map(default_field, *keys_sorted))
                } else {
                    Err(ArrowError::ParseError(
                        "Expecting a keysSorted for map".to_string(),
                    ))
                }
            }
            Some(s) if s == "union" => {
                if let Some(Value::String(mode)) = map.get("mode") {
                    let union_mode = if mode == "SPARSE" {
                        UnionMode::Sparse
                    } else if mode == "DENSE" {
                        UnionMode::Dense
                    } else {
                        return Err(ArrowError::ParseError(format!(
                            "Unknown union mode {mode:?} for union"
                        )));
                    };
                    if let Some(values) = map.get("typeIds") {
                        let values = values.as_array().unwrap();
                        let fields = values
                            .iter()
                            .map(|t| (t.as_i64().unwrap() as i8, default_field.clone()))
                            .collect();

                        Ok(DataType::Union(fields, union_mode))
                    } else {
                        Err(ArrowError::ParseError(
                            "Expecting a typeIds for union ".to_string(),
                        ))
                    }
                } else {
                    Err(ArrowError::ParseError(
                        "Expecting a mode for union".to_string(),
                    ))
                }
            }
            Some(other) => Err(ArrowError::ParseError(format!(
                "invalid or unsupported type name: {other} in {json:?}"
            ))),
            None => Err(ArrowError::ParseError("type name missing".to_string())),
        },
        _ => Err(ArrowError::ParseError(
            "invalid json value type".to_string(),
        )),
    }
}

/// Generate a JSON representation of the data type.
pub fn data_type_to_json(data_type: &DataType) -> serde_json::Value {
    use serde_json::json;
    match data_type {
        DataType::Null => json!({"name": "null"}),
        DataType::Boolean => json!({"name": "bool"}),
        DataType::Int8 => json!({"name": "int", "bitWidth": 8, "isSigned": true}),
        DataType::Int16 => json!({"name": "int", "bitWidth": 16, "isSigned": true}),
        DataType::Int32 => json!({"name": "int", "bitWidth": 32, "isSigned": true}),
        DataType::Int64 => json!({"name": "int", "bitWidth": 64, "isSigned": true}),
        DataType::UInt8 => json!({"name": "int", "bitWidth": 8, "isSigned": false}),
        DataType::UInt16 => json!({"name": "int", "bitWidth": 16, "isSigned": false}),
        DataType::UInt32 => json!({"name": "int", "bitWidth": 32, "isSigned": false}),
        DataType::UInt64 => json!({"name": "int", "bitWidth": 64, "isSigned": false}),
        DataType::Float16 => json!({"name": "floatingpoint", "precision": "HALF"}),
        DataType::Float32 => json!({"name": "floatingpoint", "precision": "SINGLE"}),
        DataType::Float64 => json!({"name": "floatingpoint", "precision": "DOUBLE"}),
        DataType::Utf8 => json!({"name": "utf8"}),
        DataType::LargeUtf8 => json!({"name": "largeutf8"}),
        DataType::Binary => json!({"name": "binary"}),
        DataType::LargeBinary => json!({"name": "largebinary"}),
        DataType::BinaryView | DataType::Utf8View => {
            unimplemented!("BinaryView/Utf8View not implemented")
        }
        DataType::FixedSizeBinary(byte_width) => {
            json!({"name": "fixedsizebinary", "byteWidth": byte_width})
        }
        DataType::Struct(_) => json!({"name": "struct"}),
        DataType::Union(_, _) => json!({"name": "union"}),
        DataType::List(_) => json!({ "name": "list"}),
        DataType::LargeList(_) => json!({ "name": "largelist"}),
        DataType::ListView(_) | DataType::LargeListView(_) => {
            unimplemented!("ListView/LargeListView not implemented")
        }
        DataType::FixedSizeList(_, length) => {
            json!({"name":"fixedsizelist", "listSize": length})
        }
        DataType::Time32(unit) => {
            json!({"name": "time", "bitWidth": 32, "unit": match unit {
                TimeUnit::Second => "SECOND",
                TimeUnit::Millisecond => "MILLISECOND",
                TimeUnit::Microsecond => "MICROSECOND",
                TimeUnit::Nanosecond => "NANOSECOND",
            }})
        }
        DataType::Time64(unit) => {
            json!({"name": "time", "bitWidth": 64, "unit": match unit {
                TimeUnit::Second => "SECOND",
                TimeUnit::Millisecond => "MILLISECOND",
                TimeUnit::Microsecond => "MICROSECOND",
                TimeUnit::Nanosecond => "NANOSECOND",
            }})
        }
        DataType::Date32 => {
            json!({"name": "date", "unit": "DAY"})
        }
        DataType::Date64 => {
            json!({"name": "date", "unit": "MILLISECOND"})
        }
        DataType::Timestamp(unit, None) => {
            json!({"name": "timestamp", "unit": match unit {
                TimeUnit::Second => "SECOND",
                TimeUnit::Millisecond => "MILLISECOND",
                TimeUnit::Microsecond => "MICROSECOND",
                TimeUnit::Nanosecond => "NANOSECOND",
            }})
        }
        DataType::Timestamp(unit, Some(tz)) => {
            json!({"name": "timestamp", "unit": match unit {
                    TimeUnit::Second => "SECOND",
                    TimeUnit::Millisecond => "MILLISECOND",
                    TimeUnit::Microsecond => "MICROSECOND",
                    TimeUnit::Nanosecond => "NANOSECOND",
                }, "timezone": tz})
        }
        DataType::Interval(unit) => json!({"name": "interval", "unit": match unit {
            IntervalUnit::YearMonth => "YEAR_MONTH",
            IntervalUnit::DayTime => "DAY_TIME",
            IntervalUnit::MonthDayNano => "MONTH_DAY_NANO",
        }}),
        DataType::Duration(unit) => json!({"name": "duration", "unit": match unit {
            TimeUnit::Second => "SECOND",
            TimeUnit::Millisecond => "MILLISECOND",
            TimeUnit::Microsecond => "MICROSECOND",
            TimeUnit::Nanosecond => "NANOSECOND",
        }}),
        DataType::Dictionary(_, _) => json!({ "name": "dictionary"}),
        DataType::Decimal128(precision, scale) => {
            json!({"name": "decimal", "precision": precision, "scale": scale, "bitWidth": 128})
        }
        DataType::Decimal256(precision, scale) => {
            json!({"name": "decimal", "precision": precision, "scale": scale, "bitWidth": 256})
        }
        DataType::Map(_, keys_sorted) => {
            json!({"name": "map", "keysSorted": keys_sorted})
        }
        DataType::RunEndEncoded(_, _) => todo!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    #[test]
    fn parse_utf8_from_json() {
        let json = "{\"name\":\"utf8\"}";
        let value: Value = serde_json::from_str(json).unwrap();
        let dt = data_type_from_json(&value).unwrap();
        assert_eq!(DataType::Utf8, dt);
    }

    #[test]
    fn parse_int32_from_json() {
        let json = "{\"name\": \"int\", \"isSigned\": true, \"bitWidth\": 32}";
        let value: Value = serde_json::from_str(json).unwrap();
        let dt = data_type_from_json(&value).unwrap();
        assert_eq!(DataType::Int32, dt);
    }
}
