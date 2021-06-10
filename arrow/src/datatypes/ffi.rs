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

use crate::{
    datatypes::{DataType, Field, Schema, TimeUnit},
    error::{ArrowError, Result},
    ffi,
};

type CArrowSchema = ffi::FFI_ArrowSchema;

impl TryFrom<&CArrowSchema> for DataType {
    type Error = ArrowError;

    /// See https://arrow.apache.org/docs/format/CDataInterface.html#data-type-description-format-strings
    fn try_from(c_schema: &CArrowSchema) -> Result<Self> {
        let dtype = match c_schema.format() {
            "n" => DataType::Null,
            "b" => DataType::Boolean,
            "c" => DataType::Int8,
            "C" => DataType::UInt8,
            "s" => DataType::Int16,
            "S" => DataType::UInt16,
            "i" => DataType::Int32,
            "I" => DataType::UInt32,
            "l" => DataType::Int64,
            "L" => DataType::UInt64,
            "e" => DataType::Float16,
            "f" => DataType::Float32,
            "g" => DataType::Float64,
            "z" => DataType::Binary,
            "Z" => DataType::LargeBinary,
            "u" => DataType::Utf8,
            "U" => DataType::LargeUtf8,
            "tdD" => DataType::Date32,
            "tdm" => DataType::Date64,
            "tts" => DataType::Time32(TimeUnit::Second),
            "ttm" => DataType::Time32(TimeUnit::Millisecond),
            "ttu" => DataType::Time64(TimeUnit::Microsecond),
            "ttn" => DataType::Time64(TimeUnit::Nanosecond),
            "+l" => {
                let c_child = c_schema.child(0);
                DataType::List(Box::new(Field::try_from(c_child)?))
            }
            "+L" => {
                let c_child = c_schema.child(0);
                DataType::LargeList(Box::new(Field::try_from(c_child)?))
            }
            "+s" => {
                let fields = c_schema.children().map(Field::try_from);
                DataType::Struct(fields.collect::<Result<Vec<_>>>()?)
            }
            // Parametrized types, requiring string parse
            other => {
                match other.splitn(2, ':').collect::<Vec<&str>>().as_slice() {
                    // Decimal types in format "d:precision,scale" or "d:precision,scale,bitWidth"
                    ["d", extra] => {
                        match extra.splitn(3, ',').collect::<Vec<&str>>().as_slice() {
                            [precision, scale] => {
                                let parsed_precision = precision.parse::<usize>().map_err(|_| {
                                    ArrowError::CDataInterface(
                                        "The decimal type requires an integer precision".to_string(),
                                    )
                                })?;
                                let parsed_scale = scale.parse::<usize>().map_err(|_| {
                                    ArrowError::CDataInterface(
                                        "The decimal type requires an integer scale".to_string(),
                                    )
                                })?;
                                DataType::Decimal(parsed_precision, parsed_scale)
                            },
                            [precision, scale, bits] => {
                                if *bits != "128" {
                                    return Err(ArrowError::CDataInterface("Only 128 bit wide decimal is supported in the Rust implementation".to_string()));
                                }
                                let parsed_precision = precision.parse::<usize>().map_err(|_| {
                                    ArrowError::CDataInterface(
                                        "The decimal type requires an integer precision".to_string(),
                                    )
                                })?;
                                let parsed_scale = scale.parse::<usize>().map_err(|_| {
                                    ArrowError::CDataInterface(
                                        "The decimal type requires an integer scale".to_string(),
                                    )
                                })?;
                                DataType::Decimal(parsed_precision, parsed_scale)
                            }
                            _ => {
                                return Err(ArrowError::CDataInterface(format!(
                                    "The decimal pattern \"d:{:?}\" is not supported in the Rust implementation",
                                    extra
                                )))
                            }
                        }
                    }

                    // Timestamps in format "tts:" and "tts:America/New_York" for no timezones and timezones resp.
                    ["tss", ""] => DataType::Timestamp(TimeUnit::Second, None),
                    ["tsm", ""] => DataType::Timestamp(TimeUnit::Millisecond, None),
                    ["tsu", ""] => DataType::Timestamp(TimeUnit::Microsecond, None),
                    ["tsn", ""] => DataType::Timestamp(TimeUnit::Nanosecond, None),
                    ["tss", tz] => {
                        DataType::Timestamp(TimeUnit::Second, Some(tz.to_string()))
                    }
                    ["tsm", tz] => {
                        DataType::Timestamp(TimeUnit::Millisecond, Some(tz.to_string()))
                    }
                    ["tsu", tz] => {
                        DataType::Timestamp(TimeUnit::Microsecond, Some(tz.to_string()))
                    }
                    ["tsn", tz] => {
                        DataType::Timestamp(TimeUnit::Nanosecond, Some(tz.to_string()))
                    }
                    _ => {
                        return Err(ArrowError::CDataInterface(format!(
                            "The datatype \"{:?}\" is still not supported in Rust implementation",
                            other
                        )))
                    }
                }
            }
        };
        Ok(dtype)
    }
}

impl TryFrom<&DataType> for CArrowSchema {
    type Error = ArrowError;

    /// See https://arrow.apache.org/docs/format/CDataInterface.html#data-type-description-format-strings
    fn try_from(dtype: &DataType) -> Result<Self> {
        let format = match dtype {
            DataType::Null => "n".to_string(),
            DataType::Boolean => "b".to_string(),
            DataType::Int8 => "c".to_string(),
            DataType::UInt8 => "C".to_string(),
            DataType::Int16 => "s".to_string(),
            DataType::UInt16 => "S".to_string(),
            DataType::Int32 => "i".to_string(),
            DataType::UInt32 => "I".to_string(),
            DataType::Int64 => "l".to_string(),
            DataType::UInt64 => "L".to_string(),
            DataType::Float16 => "e".to_string(),
            DataType::Float32 => "f".to_string(),
            DataType::Float64 => "g".to_string(),
            DataType::Binary => "z".to_string(),
            DataType::LargeBinary => "Z".to_string(),
            DataType::Utf8 => "u".to_string(),
            DataType::LargeUtf8 => "U".to_string(),
            DataType::Decimal(precision, scale) => format!("d:{},{}", precision, scale),
            DataType::Date32 => "tdD".to_string(),
            DataType::Date64 => "tdm".to_string(),
            DataType::Time32(TimeUnit::Second) => "tts".to_string(),
            DataType::Time32(TimeUnit::Millisecond) => "ttm".to_string(),
            DataType::Time64(TimeUnit::Microsecond) => "ttu".to_string(),
            DataType::Time64(TimeUnit::Nanosecond) => "ttn".to_string(),
            DataType::Timestamp(TimeUnit::Second, None) => "tss:".to_string(),
            DataType::Timestamp(TimeUnit::Millisecond, None) => "tsm:".to_string(),
            DataType::Timestamp(TimeUnit::Microsecond, None) => "tsu:".to_string(),
            DataType::Timestamp(TimeUnit::Nanosecond, None) => "tsn:".to_string(),
            DataType::Timestamp(TimeUnit::Second, Some(tz)) => format!("tss:{}", tz),
            DataType::Timestamp(TimeUnit::Millisecond, Some(tz)) => format!("tsm:{}", tz),
            DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => format!("tsu:{}", tz),
            DataType::Timestamp(TimeUnit::Nanosecond, Some(tz)) => format!("tsn:{}", tz),
            DataType::List(_) => "+l".to_string(),
            DataType::LargeList(_) => "+L".to_string(),
            DataType::Struct(_) => "+s".to_string(),
            other => {
                return Err(ArrowError::CDataInterface(format!(
                    "The datatype \"{:?}\" is still not supported in Rust implementation",
                    other
                )))
            }
        };
        // allocate and hold the children
        let children = match dtype {
            DataType::List(child) | DataType::LargeList(child) => {
                vec![CArrowSchema::try_from(child.as_ref())?]
            }
            DataType::Struct(fields) => fields
                .iter()
                .map(CArrowSchema::try_from)
                .collect::<Result<Vec<_>>>()?,
            _ => vec![],
        };
        CArrowSchema::try_new(&format, children)
    }
}

impl TryFrom<&CArrowSchema> for Field {
    type Error = ArrowError;

    fn try_from(c_schema: &CArrowSchema) -> Result<Self> {
        let dtype = DataType::try_from(c_schema)?;
        // TODO: validate that it has a struct type
        let field = Field::new(c_schema.name(), dtype, c_schema.nullable());
        Ok(field)
    }
}

impl TryFrom<&Field> for CArrowSchema {
    type Error = ArrowError;

    fn try_from(field: &Field) -> Result<Self> {
        CArrowSchema::try_from(field.data_type())
            .unwrap()
            .with_name(field.name())
        // with_flags
    }
}

impl TryFrom<&CArrowSchema> for Schema {
    type Error = ArrowError;

    fn try_from(c_schema: &CArrowSchema) -> Result<Self> {
        let fields = c_schema.children().map(Field::try_from);
        let schema = Schema::new(fields.collect::<Result<Vec<_>>>()?);
        Ok(schema)
    }
}

#[cfg(test)]
mod tests {
    // TODO
}
