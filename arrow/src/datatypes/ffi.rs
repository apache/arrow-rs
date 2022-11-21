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

use std::convert::TryFrom;

use crate::datatypes::DataType::Map;
use crate::{
    datatypes::{DataType, Field, Schema, TimeUnit},
    error::{ArrowError, Result},
    ffi::{FFI_ArrowSchema, Flags},
};

impl TryFrom<&FFI_ArrowSchema> for DataType {
    type Error = ArrowError;

    /// See [CDataInterface docs](https://arrow.apache.org/docs/format/CDataInterface.html#data-type-description-format-strings)
    fn try_from(c_schema: &FFI_ArrowSchema) -> Result<Self> {
        let mut dtype = match c_schema.format() {
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
            "tDs" => DataType::Duration(TimeUnit::Second),
            "tDm" => DataType::Duration(TimeUnit::Millisecond),
            "tDu" => DataType::Duration(TimeUnit::Microsecond),
            "tDn" => DataType::Duration(TimeUnit::Nanosecond),
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
            "+m" => {
                let c_child = c_schema.child(0);
                let map_keys_sorted = c_schema.map_keys_sorted();
                DataType::Map(Box::new(Field::try_from(c_child)?), map_keys_sorted)
            }
            // Parametrized types, requiring string parse
            other => {
                match other.splitn(2, ':').collect::<Vec<&str>>().as_slice() {
                    // FixedSizeBinary type in format "w:num_bytes"
                    ["w", num_bytes] => {
                        let parsed_num_bytes = num_bytes.parse::<i32>().map_err(|_| {
                            ArrowError::CDataInterface(
                                "FixedSizeBinary requires an integer parameter representing number of bytes per element".to_string())
                        })?;
                        DataType::FixedSizeBinary(parsed_num_bytes)
                    },
                    // FixedSizeList type in format "+w:num_elems"
                    ["+w", num_elems] => {
                        let c_child = c_schema.child(0);
                        let parsed_num_elems = num_elems.parse::<i32>().map_err(|_| {
                            ArrowError::CDataInterface(
                                "The FixedSizeList type requires an integer parameter representing number of elements per list".to_string())
                        })?;
                        DataType::FixedSizeList(Box::new(Field::try_from(c_child)?), parsed_num_elems)
                    },
                    // Decimal types in format "d:precision,scale" or "d:precision,scale,bitWidth"
                    ["d", extra] => {
                        match extra.splitn(3, ',').collect::<Vec<&str>>().as_slice() {
                            [precision, scale] => {
                                let parsed_precision = precision.parse::<u8>().map_err(|_| {
                                    ArrowError::CDataInterface(
                                        "The decimal type requires an integer precision".to_string(),
                                    )
                                })?;
                                let parsed_scale = scale.parse::<i8>().map_err(|_| {
                                    ArrowError::CDataInterface(
                                        "The decimal type requires an integer scale".to_string(),
                                    )
                                })?;
                                DataType::Decimal128(parsed_precision, parsed_scale)
                            },
                            [precision, scale, bits] => {
                                if *bits != "128" {
                                    return Err(ArrowError::CDataInterface("Only 128 bit wide decimal is supported in the Rust implementation".to_string()));
                                }
                                let parsed_precision = precision.parse::<u8>().map_err(|_| {
                                    ArrowError::CDataInterface(
                                        "The decimal type requires an integer precision".to_string(),
                                    )
                                })?;
                                let parsed_scale = scale.parse::<i8>().map_err(|_| {
                                    ArrowError::CDataInterface(
                                        "The decimal type requires an integer scale".to_string(),
                                    )
                                })?;
                                DataType::Decimal128(parsed_precision, parsed_scale)
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

        if let Some(dict_schema) = c_schema.dictionary() {
            let value_type = Self::try_from(dict_schema)?;
            dtype = DataType::Dictionary(Box::new(dtype), Box::new(value_type));
        }

        Ok(dtype)
    }
}

impl TryFrom<&FFI_ArrowSchema> for Field {
    type Error = ArrowError;

    fn try_from(c_schema: &FFI_ArrowSchema) -> Result<Self> {
        let dtype = DataType::try_from(c_schema)?;
        let field = Field::new(c_schema.name(), dtype, c_schema.nullable());
        Ok(field)
    }
}

impl TryFrom<&FFI_ArrowSchema> for Schema {
    type Error = ArrowError;

    fn try_from(c_schema: &FFI_ArrowSchema) -> Result<Self> {
        // interpret it as a struct type then extract its fields
        let dtype = DataType::try_from(c_schema)?;
        if let DataType::Struct(fields) = dtype {
            Ok(Schema::new(fields))
        } else {
            Err(ArrowError::CDataInterface(
                "Unable to interpret C data struct as a Schema".to_string(),
            ))
        }
    }
}

impl TryFrom<&DataType> for FFI_ArrowSchema {
    type Error = ArrowError;

    /// See [CDataInterface docs](https://arrow.apache.org/docs/format/CDataInterface.html#data-type-description-format-strings)
    fn try_from(dtype: &DataType) -> Result<Self> {
        let format = get_format_string(dtype)?;
        // allocate and hold the children
        let children = match dtype {
            DataType::List(child)
            | DataType::LargeList(child)
            | DataType::FixedSizeList(child, _)
            | DataType::Map(child, _) => {
                vec![FFI_ArrowSchema::try_from(child.as_ref())?]
            }
            DataType::Struct(fields) => fields
                .iter()
                .map(FFI_ArrowSchema::try_from)
                .collect::<Result<Vec<_>>>()?,
            _ => vec![],
        };
        let dictionary = if let DataType::Dictionary(_, value_data_type) = dtype {
            Some(Self::try_from(value_data_type.as_ref())?)
        } else {
            None
        };

        let flags = match dtype {
            Map(_, true) => Flags::MAP_KEYS_SORTED,
            _ => Flags::empty(),
        };

        FFI_ArrowSchema::try_new(&format, children, dictionary)?.with_flags(flags)
    }
}

fn get_format_string(dtype: &DataType) -> Result<String> {
    match dtype {
        DataType::Null => Ok("n".to_string()),
        DataType::Boolean => Ok("b".to_string()),
        DataType::Int8 => Ok("c".to_string()),
        DataType::UInt8 => Ok("C".to_string()),
        DataType::Int16 => Ok("s".to_string()),
        DataType::UInt16 => Ok("S".to_string()),
        DataType::Int32 => Ok("i".to_string()),
        DataType::UInt32 => Ok("I".to_string()),
        DataType::Int64 => Ok("l".to_string()),
        DataType::UInt64 => Ok("L".to_string()),
        DataType::Float16 => Ok("e".to_string()),
        DataType::Float32 => Ok("f".to_string()),
        DataType::Float64 => Ok("g".to_string()),
        DataType::Binary => Ok("z".to_string()),
        DataType::LargeBinary => Ok("Z".to_string()),
        DataType::Utf8 => Ok("u".to_string()),
        DataType::LargeUtf8 => Ok("U".to_string()),
        DataType::FixedSizeBinary(num_bytes) => Ok(format!("w:{}", num_bytes)),
        DataType::FixedSizeList(_, num_elems) => Ok(format!("+w:{}", num_elems)),
        DataType::Decimal128(precision, scale) => {
            Ok(format!("d:{},{}", precision, scale))
        }
        DataType::Date32 => Ok("tdD".to_string()),
        DataType::Date64 => Ok("tdm".to_string()),
        DataType::Time32(TimeUnit::Second) => Ok("tts".to_string()),
        DataType::Time32(TimeUnit::Millisecond) => Ok("ttm".to_string()),
        DataType::Time64(TimeUnit::Microsecond) => Ok("ttu".to_string()),
        DataType::Time64(TimeUnit::Nanosecond) => Ok("ttn".to_string()),
        DataType::Timestamp(TimeUnit::Second, None) => Ok("tss:".to_string()),
        DataType::Timestamp(TimeUnit::Millisecond, None) => Ok("tsm:".to_string()),
        DataType::Timestamp(TimeUnit::Microsecond, None) => Ok("tsu:".to_string()),
        DataType::Timestamp(TimeUnit::Nanosecond, None) => Ok("tsn:".to_string()),
        DataType::Timestamp(TimeUnit::Second, Some(tz)) => Ok(format!("tss:{}", tz)),
        DataType::Timestamp(TimeUnit::Millisecond, Some(tz)) => Ok(format!("tsm:{}", tz)),
        DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => Ok(format!("tsu:{}", tz)),
        DataType::Timestamp(TimeUnit::Nanosecond, Some(tz)) => Ok(format!("tsn:{}", tz)),
        DataType::Duration(TimeUnit::Second) => Ok("tDs".to_string()),
        DataType::Duration(TimeUnit::Millisecond) => Ok("tDm".to_string()),
        DataType::Duration(TimeUnit::Microsecond) => Ok("tDu".to_string()),
        DataType::Duration(TimeUnit::Nanosecond) => Ok("tDn".to_string()),
        DataType::List(_) => Ok("+l".to_string()),
        DataType::LargeList(_) => Ok("+L".to_string()),
        DataType::Struct(_) => Ok("+s".to_string()),
        DataType::Map(_, _) => Ok("+m".to_string()),
        DataType::Dictionary(key_data_type, _) => get_format_string(key_data_type),
        other => Err(ArrowError::CDataInterface(format!(
            "The datatype \"{:?}\" is still not supported in Rust implementation",
            other
        ))),
    }
}

impl TryFrom<&Field> for FFI_ArrowSchema {
    type Error = ArrowError;

    fn try_from(field: &Field) -> Result<Self> {
        let mut flags = if field.is_nullable() {
            Flags::NULLABLE
        } else {
            Flags::empty()
        };

        if let Some(true) = field.dict_is_ordered() {
            flags |= Flags::DICTIONARY_ORDERED;
        }

        FFI_ArrowSchema::try_from(field.data_type())?
            .with_name(field.name())?
            .with_flags(flags)
    }
}

impl TryFrom<&Schema> for FFI_ArrowSchema {
    type Error = ArrowError;

    fn try_from(schema: &Schema) -> Result<Self> {
        let dtype = DataType::Struct(schema.fields().clone());
        let c_schema = FFI_ArrowSchema::try_from(&dtype)?;
        Ok(c_schema)
    }
}

impl TryFrom<DataType> for FFI_ArrowSchema {
    type Error = ArrowError;

    fn try_from(dtype: DataType) -> Result<Self> {
        FFI_ArrowSchema::try_from(&dtype)
    }
}

impl TryFrom<Field> for FFI_ArrowSchema {
    type Error = ArrowError;

    fn try_from(field: Field) -> Result<Self> {
        FFI_ArrowSchema::try_from(&field)
    }
}

impl TryFrom<Schema> for FFI_ArrowSchema {
    type Error = ArrowError;

    fn try_from(schema: Schema) -> Result<Self> {
        FFI_ArrowSchema::try_from(&schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datatypes::{DataType, Field, TimeUnit};
    use crate::error::Result;
    use std::convert::TryFrom;

    fn round_trip_type(dtype: DataType) -> Result<()> {
        let c_schema = FFI_ArrowSchema::try_from(&dtype)?;
        let restored = DataType::try_from(&c_schema)?;
        assert_eq!(restored, dtype);
        Ok(())
    }

    fn round_trip_field(field: Field) -> Result<()> {
        let c_schema = FFI_ArrowSchema::try_from(&field)?;
        let restored = Field::try_from(&c_schema)?;
        assert_eq!(restored, field);
        Ok(())
    }

    fn round_trip_schema(schema: Schema) -> Result<()> {
        let c_schema = FFI_ArrowSchema::try_from(&schema)?;
        let restored = Schema::try_from(&c_schema)?;
        assert_eq!(restored, schema);
        Ok(())
    }

    #[test]
    fn test_type() -> Result<()> {
        round_trip_type(DataType::Int64)?;
        round_trip_type(DataType::UInt64)?;
        round_trip_type(DataType::Float64)?;
        round_trip_type(DataType::Date64)?;
        round_trip_type(DataType::Time64(TimeUnit::Nanosecond))?;
        round_trip_type(DataType::FixedSizeBinary(12))?;
        round_trip_type(DataType::FixedSizeList(
            Box::new(Field::new("a", DataType::Int64, false)),
            5,
        ))?;
        round_trip_type(DataType::Utf8)?;
        round_trip_type(DataType::List(Box::new(Field::new(
            "a",
            DataType::Int16,
            false,
        ))))?;
        round_trip_type(DataType::Struct(vec![Field::new(
            "a",
            DataType::Utf8,
            true,
        )]))?;
        Ok(())
    }

    #[test]
    fn test_field() -> Result<()> {
        let dtype = DataType::Struct(vec![Field::new("a", DataType::Utf8, true)]);
        round_trip_field(Field::new("test", dtype, true))?;
        Ok(())
    }

    #[test]
    fn test_schema() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("address", DataType::Utf8, false),
            Field::new("priority", DataType::UInt8, false),
        ]);
        round_trip_schema(schema)?;

        // test that we can interpret struct types as schema
        let dtype = DataType::Struct(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Int16, false),
        ]);
        let c_schema = FFI_ArrowSchema::try_from(&dtype)?;
        let schema = Schema::try_from(&c_schema)?;
        assert_eq!(schema.fields().len(), 2);

        // test that we assert the input type
        let c_schema = FFI_ArrowSchema::try_from(&DataType::Float64)?;
        let result = Schema::try_from(&c_schema);
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn test_map_keys_sorted() -> Result<()> {
        let keys = Field::new("keys", DataType::Int32, false);
        let values = Field::new("values", DataType::UInt32, false);
        let entry_struct = DataType::Struct(vec![keys, values]);

        // Construct a map array from the above two
        let map_data_type =
            DataType::Map(Box::new(Field::new("entries", entry_struct, true)), true);

        let arrow_schema = FFI_ArrowSchema::try_from(map_data_type)?;
        assert!(arrow_schema.map_keys_sorted());

        Ok(())
    }

    #[test]
    fn test_dictionary_ordered() -> Result<()> {
        let schema = Schema::new(vec![Field::new_dict(
            "dict",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
            0,
            true,
        )]);

        let arrow_schema = FFI_ArrowSchema::try_from(schema)?;
        assert!(arrow_schema.child(0).dictionary_ordered());

        Ok(())
    }
}
