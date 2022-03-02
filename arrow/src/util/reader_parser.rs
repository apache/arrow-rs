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

use crate::compute::kernels::cast_utils::string_to_timestamp_nanos;
use crate::datatypes::*;

/// Specialized parsing implementations
/// used by csv and json reader
pub trait Parser: ArrowPrimitiveType {
    fn parse(string: &str) -> Option<Self::Native> {
        string.parse::<Self::Native>().ok()
    }

    fn parse_formatted(string: &str, _format: &str) -> Option<Self::Native> {
        Self::parse(string)
    }
}

impl Parser for Float32Type {
    fn parse(string: &str) -> Option<f32> {
        lexical_core::parse(string.as_bytes()).ok()
    }
}

impl Parser for Float64Type {
    fn parse(string: &str) -> Option<f64> {
        lexical_core::parse(string.as_bytes()).ok()
    }
}

impl Parser for UInt64Type {}

impl Parser for UInt32Type {}

impl Parser for UInt16Type {}

impl Parser for UInt8Type {}

impl Parser for Int64Type {}

impl Parser for Int32Type {}

impl Parser for Int16Type {}

impl Parser for Int8Type {}

impl Parser for TimestampNanosecondType {
    fn parse(string: &str) -> Option<i64> {
        match Self::DATA_TYPE {
            DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                string_to_timestamp_nanos(string).ok()
            }
            _ => None,
        }
    }
}

impl Parser for TimestampMicrosecondType {
    fn parse(string: &str) -> Option<i64> {
        match Self::DATA_TYPE {
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                let nanos = string_to_timestamp_nanos(string).ok();
                nanos.map(|x| x / 1000)
            }
            _ => None,
        }
    }
}

/// Number of days between 0001-01-01 and 1970-01-01
const EPOCH_DAYS_FROM_CE: i32 = 719_163;

impl Parser for Date32Type {
    fn parse(string: &str) -> Option<i32> {
        use chrono::Datelike;

        match Self::DATA_TYPE {
            DataType::Date32 => {
                let date = string.parse::<chrono::NaiveDate>().ok()?;
                Self::Native::from_i32(date.num_days_from_ce() - EPOCH_DAYS_FROM_CE)
            }
            _ => None,
        }
    }
}

impl Parser for Date64Type {
    fn parse(string: &str) -> Option<i64> {
        match Self::DATA_TYPE {
            DataType::Date64 => {
                let date_time = string.parse::<chrono::NaiveDateTime>().ok()?;
                Self::Native::from_i64(date_time.timestamp_millis())
            }
            _ => None,
        }
    }

    fn parse_formatted(string: &str, format: &str) -> Option<i64> {
        match Self::DATA_TYPE {
            DataType::Date64 => {
                use chrono::format::Fixed;
                use chrono::format::StrftimeItems;
                let fmt = StrftimeItems::new(format);
                let has_zone = fmt.into_iter().any(|item| match item {
                    chrono::format::Item::Fixed(fixed_item) => matches!(
                        fixed_item,
                        Fixed::RFC2822
                            | Fixed::RFC3339
                            | Fixed::TimezoneName
                            | Fixed::TimezoneOffsetColon
                            | Fixed::TimezoneOffsetColonZ
                            | Fixed::TimezoneOffset
                            | Fixed::TimezoneOffsetZ
                    ),
                    _ => false,
                });
                if has_zone {
                    let date_time =
                        chrono::DateTime::parse_from_str(string, format).ok()?;
                    Self::Native::from_i64(date_time.timestamp_millis())
                } else {
                    let date_time =
                        chrono::NaiveDateTime::parse_from_str(string, format).ok()?;
                    Self::Native::from_i64(date_time.timestamp_millis())
                }
            }
            _ => None,
        }
    }
}
