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

use crate::{field_from_json, field_to_json};
use arrow::datatypes::{Fields, Schema};
use arrow::error::{ArrowError, Result};
use std::collections::HashMap;

/// Generate a JSON representation of the `Schema`.
pub fn schema_to_json(schema: &Schema) -> serde_json::Value {
    serde_json::json!({
        "fields": schema.fields().iter().map(|f| field_to_json(f.as_ref())).collect::<Vec<_>>(),
        "metadata": serde_json::to_value(schema.metadata()).unwrap()
    })
}

/// Parse a `Schema` definition from a JSON representation.
pub fn schema_from_json(json: &serde_json::Value) -> Result<Schema> {
    use serde_json::Value;
    match *json {
        Value::Object(ref schema) => {
            let fields: Fields = match schema.get("fields") {
                Some(Value::Array(fields)) => {
                    fields.iter().map(field_from_json).collect::<Result<_>>()?
                }
                _ => {
                    return Err(ArrowError::ParseError(
                        "Schema fields should be an array".to_string(),
                    ))
                }
            };

            let metadata = if let Some(value) = schema.get("metadata") {
                from_metadata(value)?
            } else {
                HashMap::default()
            };

            Ok(Schema::new_with_metadata(fields, metadata))
        }
        _ => Err(ArrowError::ParseError(
            "Invalid json value type for schema".to_string(),
        )),
    }
}

/// Parse a `metadata` definition from a JSON representation.
/// The JSON can either be an Object or an Array of Objects.
fn from_metadata(json: &serde_json::Value) -> Result<HashMap<String, String>> {
    use serde_json::Value;
    match json {
        Value::Array(_) => {
            let mut hashmap = HashMap::new();
            let values: Vec<MetadataKeyValue> =
                serde_json::from_value(json.clone()).map_err(|_| {
                    ArrowError::JsonError("Unable to parse object into key-value pair".to_string())
                })?;
            for meta in values {
                hashmap.insert(meta.key.clone(), meta.value);
            }
            Ok(hashmap)
        }
        Value::Object(md) => md
            .iter()
            .map(|(k, v)| {
                if let Value::String(v) = v {
                    Ok((k.to_string(), v.to_string()))
                } else {
                    Err(ArrowError::ParseError(
                        "metadata `value` field must be a string".to_string(),
                    ))
                }
            })
            .collect::<Result<_>>(),
        _ => Err(ArrowError::ParseError(
            "`metadata` field must be an object".to_string(),
        )),
    }
}

#[derive(serde::Deserialize)]
struct MetadataKeyValue {
    key: String,
    value: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, IntervalUnit, TimeUnit};
    use serde_json::Value;
    use std::sync::Arc;

    #[test]
    fn schema_json() {
        // Add some custom metadata
        let metadata: HashMap<String, String> = [("Key".to_string(), "Value".to_string())]
            .iter()
            .cloned()
            .collect();

        let schema = Schema::new_with_metadata(
            vec![
                Field::new("c1", DataType::Utf8, false),
                Field::new("c2", DataType::Binary, false),
                Field::new("c3", DataType::FixedSizeBinary(3), false),
                Field::new("c4", DataType::Boolean, false),
                Field::new("c5", DataType::Date32, false),
                Field::new("c6", DataType::Date64, false),
                Field::new("c7", DataType::Time32(TimeUnit::Second), false),
                Field::new("c8", DataType::Time32(TimeUnit::Millisecond), false),
                Field::new("c9", DataType::Time32(TimeUnit::Microsecond), false),
                Field::new("c10", DataType::Time32(TimeUnit::Nanosecond), false),
                Field::new("c11", DataType::Time64(TimeUnit::Second), false),
                Field::new("c12", DataType::Time64(TimeUnit::Millisecond), false),
                Field::new("c13", DataType::Time64(TimeUnit::Microsecond), false),
                Field::new("c14", DataType::Time64(TimeUnit::Nanosecond), false),
                Field::new("c15", DataType::Timestamp(TimeUnit::Second, None), false),
                Field::new(
                    "c16",
                    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                    false,
                ),
                Field::new(
                    "c17",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("Africa/Johannesburg".into())),
                    false,
                ),
                Field::new(
                    "c18",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
                Field::new("c19", DataType::Interval(IntervalUnit::DayTime), false),
                Field::new("c20", DataType::Interval(IntervalUnit::YearMonth), false),
                Field::new("c21", DataType::Interval(IntervalUnit::MonthDayNano), false),
                Field::new(
                    "c22",
                    DataType::List(Arc::new(Field::new("item", DataType::Boolean, true))),
                    false,
                ),
                Field::new(
                    "c23",
                    DataType::FixedSizeList(
                        Arc::new(Field::new("bools", DataType::Boolean, false)),
                        5,
                    ),
                    false,
                ),
                Field::new(
                    "c24",
                    DataType::List(Arc::new(Field::new(
                        "inner_list",
                        DataType::List(Arc::new(Field::new(
                            "struct",
                            DataType::Struct(Fields::empty()),
                            true,
                        ))),
                        false,
                    ))),
                    true,
                ),
                Field::new(
                    "c25",
                    DataType::Struct(Fields::from(vec![
                        Field::new("a", DataType::Utf8, false),
                        Field::new("b", DataType::UInt16, false),
                    ])),
                    false,
                ),
                Field::new("c26", DataType::Interval(IntervalUnit::YearMonth), true),
                Field::new("c27", DataType::Interval(IntervalUnit::DayTime), true),
                Field::new("c28", DataType::Interval(IntervalUnit::MonthDayNano), true),
                Field::new("c29", DataType::Duration(TimeUnit::Second), false),
                Field::new("c30", DataType::Duration(TimeUnit::Millisecond), false),
                Field::new("c31", DataType::Duration(TimeUnit::Microsecond), false),
                Field::new("c32", DataType::Duration(TimeUnit::Nanosecond), false),
                Field::new_dict(
                    "c33",
                    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                    true,
                    123,
                    true,
                ),
                Field::new("c34", DataType::LargeBinary, true),
                Field::new("c35", DataType::LargeUtf8, true),
                Field::new(
                    "c36",
                    DataType::LargeList(Arc::new(Field::new(
                        "inner_large_list",
                        DataType::LargeList(Arc::new(Field::new(
                            "struct",
                            DataType::Struct(Fields::empty()),
                            false,
                        ))),
                        true,
                    ))),
                    true,
                ),
                Field::new(
                    "c37",
                    DataType::Map(
                        Arc::new(Field::new(
                            "my_entries",
                            DataType::Struct(Fields::from(vec![
                                Field::new("my_keys", DataType::Utf8, false),
                                Field::new("my_values", DataType::UInt16, true),
                            ])),
                            false,
                        )),
                        true,
                    ),
                    false,
                ),
            ],
            metadata,
        );

        let expected = schema_to_json(&schema);
        let json = r#"{
                "fields": [
                    {
                        "name": "c1",
                        "nullable": false,
                        "type": {
                            "name": "utf8"
                        },
                        "children": []
                    },
                    {
                        "name": "c2",
                        "nullable": false,
                        "type": {
                            "name": "binary"
                        },
                        "children": []
                    },
                    {
                        "name": "c3",
                        "nullable": false,
                        "type": {
                            "name": "fixedsizebinary",
                            "byteWidth": 3
                        },
                        "children": []
                    },
                    {
                        "name": "c4",
                        "nullable": false,
                        "type": {
                            "name": "bool"
                        },
                        "children": []
                    },
                    {
                        "name": "c5",
                        "nullable": false,
                        "type": {
                            "name": "date",
                            "unit": "DAY"
                        },
                        "children": []
                    },
                    {
                        "name": "c6",
                        "nullable": false,
                        "type": {
                            "name": "date",
                            "unit": "MILLISECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c7",
                        "nullable": false,
                        "type": {
                            "name": "time",
                            "bitWidth": 32,
                            "unit": "SECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c8",
                        "nullable": false,
                        "type": {
                            "name": "time",
                            "bitWidth": 32,
                            "unit": "MILLISECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c9",
                        "nullable": false,
                        "type": {
                            "name": "time",
                            "bitWidth": 32,
                            "unit": "MICROSECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c10",
                        "nullable": false,
                        "type": {
                            "name": "time",
                            "bitWidth": 32,
                            "unit": "NANOSECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c11",
                        "nullable": false,
                        "type": {
                            "name": "time",
                            "bitWidth": 64,
                            "unit": "SECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c12",
                        "nullable": false,
                        "type": {
                            "name": "time",
                            "bitWidth": 64,
                            "unit": "MILLISECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c13",
                        "nullable": false,
                        "type": {
                            "name": "time",
                            "bitWidth": 64,
                            "unit": "MICROSECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c14",
                        "nullable": false,
                        "type": {
                            "name": "time",
                            "bitWidth": 64,
                            "unit": "NANOSECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c15",
                        "nullable": false,
                        "type": {
                            "name": "timestamp",
                            "unit": "SECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c16",
                        "nullable": false,
                        "type": {
                            "name": "timestamp",
                            "unit": "MILLISECOND",
                            "timezone": "UTC"
                        },
                        "children": []
                    },
                    {
                        "name": "c17",
                        "nullable": false,
                        "type": {
                            "name": "timestamp",
                            "unit": "MICROSECOND",
                            "timezone": "Africa/Johannesburg"
                        },
                        "children": []
                    },
                    {
                        "name": "c18",
                        "nullable": false,
                        "type": {
                            "name": "timestamp",
                            "unit": "NANOSECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c19",
                        "nullable": false,
                        "type": {
                            "name": "interval",
                            "unit": "DAY_TIME"
                        },
                        "children": []
                    },
                    {
                        "name": "c20",
                        "nullable": false,
                        "type": {
                            "name": "interval",
                            "unit": "YEAR_MONTH"
                        },
                        "children": []
                    },
                    {
                        "name": "c21",
                        "nullable": false,
                        "type": {
                            "name": "interval",
                            "unit": "MONTH_DAY_NANO"
                        },
                        "children": []
                    },
                    {
                        "name": "c22",
                        "nullable": false,
                        "type": {
                            "name": "list"
                        },
                        "children": [
                            {
                                "name": "item",
                                "nullable": true,
                                "type": {
                                    "name": "bool"
                                },
                                "children": []
                            }
                        ]
                    },
                    {
                        "name": "c23",
                        "nullable": false,
                        "type": {
                            "name": "fixedsizelist",
                            "listSize": 5
                        },
                        "children": [
                            {
                                "name": "bools",
                                "nullable": false,
                                "type": {
                                    "name": "bool"
                                },
                                "children": []
                            }
                        ]
                    },
                    {
                        "name": "c24",
                        "nullable": true,
                        "type": {
                            "name": "list"
                        },
                        "children": [
                            {
                                "name": "inner_list",
                                "nullable": false,
                                "type": {
                                    "name": "list"
                                },
                                "children": [
                                    {
                                        "name": "struct",
                                        "nullable": true,
                                        "type": {
                                            "name": "struct"
                                        },
                                        "children": []
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        "name": "c25",
                        "nullable": false,
                        "type": {
                            "name": "struct"
                        },
                        "children": [
                            {
                                "name": "a",
                                "nullable": false,
                                "type": {
                                    "name": "utf8"
                                },
                                "children": []
                            },
                            {
                                "name": "b",
                                "nullable": false,
                                "type": {
                                    "name": "int",
                                    "bitWidth": 16,
                                    "isSigned": false
                                },
                                "children": []
                            }
                        ]
                    },
                    {
                        "name": "c26",
                        "nullable": true,
                        "type": {
                            "name": "interval",
                            "unit": "YEAR_MONTH"
                        },
                        "children": []
                    },
                    {
                        "name": "c27",
                        "nullable": true,
                        "type": {
                            "name": "interval",
                            "unit": "DAY_TIME"
                        },
                        "children": []
                    },
                    {
                        "name": "c28",
                        "nullable": true,
                        "type": {
                            "name": "interval",
                            "unit": "MONTH_DAY_NANO"
                        },
                        "children": []
                    },
                    {
                        "name": "c29",
                        "nullable": false,
                        "type": {
                            "name": "duration",
                            "unit": "SECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c30",
                        "nullable": false,
                        "type": {
                            "name": "duration",
                            "unit": "MILLISECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c31",
                        "nullable": false,
                        "type": {
                            "name": "duration",
                            "unit": "MICROSECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c32",
                        "nullable": false,
                        "type": {
                            "name": "duration",
                            "unit": "NANOSECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c33",
                        "nullable": true,
                        "children": [],
                        "type": {
                          "name": "utf8"
                        },
                        "dictionary": {
                          "id": 123,
                          "indexType": {
                            "name": "int",
                            "bitWidth": 32,
                            "isSigned": true
                          },
                          "isOrdered": true
                        }
                    },
                    {
                        "name": "c34",
                        "nullable": true,
                        "type": {
                          "name": "largebinary"
                        },
                        "children": []
                    },
                    {
                        "name": "c35",
                        "nullable": true,
                        "type": {
                          "name": "largeutf8"
                        },
                        "children": []
                    },
                    {
                        "name": "c36",
                        "nullable": true,
                        "type": {
                          "name": "largelist"
                        },
                        "children": [
                            {
                                "name": "inner_large_list",
                                "nullable": true,
                                "type": {
                                    "name": "largelist"
                                },
                                "children": [
                                    {
                                        "name": "struct",
                                        "nullable": false,
                                        "type": {
                                            "name": "struct"
                                        },
                                        "children": []
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        "name": "c37",
                        "nullable": false,
                        "type": {
                            "name": "map",
                            "keysSorted": true
                        },
                        "children": [
                            {
                                "name": "my_entries",
                                "nullable": false,
                                "type": {
                                    "name": "struct"
                                },
                                "children": [
                                    {
                                        "name": "my_keys",
                                        "nullable": false,
                                        "type": {
                                            "name": "utf8"
                                        },
                                        "children": []
                                    },
                                    {
                                        "name": "my_values",
                                        "nullable": true,
                                        "type": {
                                            "name": "int",
                                            "bitWidth": 16,
                                            "isSigned": false
                                        },
                                        "children": []
                                    }
                                ]
                            }
                        ]
                    }
                ],
                "metadata" : {
                    "Key": "Value"
                }
            }"#;
        let value: Value = serde_json::from_str(json).unwrap();
        assert_eq!(expected, value);

        // convert back to a schema
        let value: Value = serde_json::from_str(json).unwrap();
        let schema2 = schema_from_json(&value).unwrap();

        assert_eq!(schema, schema2);

        // Check that empty metadata produces empty value in JSON and can be parsed
        let json = r#"{
                "fields": [
                    {
                        "name": "c1",
                        "nullable": false,
                        "type": {
                            "name": "utf8"
                        },
                        "children": []
                    }
                ],
                "metadata": {}
            }"#;
        let value: Value = serde_json::from_str(json).unwrap();
        let schema = schema_from_json(&value).unwrap();
        assert!(schema.metadata.is_empty());

        // Check that metadata field is not required in the JSON.
        let json = r#"{
                "fields": [
                    {
                        "name": "c1",
                        "nullable": false,
                        "type": {
                            "name": "utf8"
                        },
                        "children": []
                    }
                ]
            }"#;
        let value: Value = serde_json::from_str(json).unwrap();
        let schema = schema_from_json(&value).unwrap();
        assert!(schema.metadata.is_empty());
    }
}
