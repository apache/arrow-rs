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

use crate::{data_type_from_json, data_type_to_json};
use arrow::datatypes::{DataType, Field};
use arrow::error::{ArrowError, Result};
use std::collections::HashMap;
use std::sync::Arc;

/// Parse a `Field` definition from a JSON representation.
pub fn field_from_json(json: &serde_json::Value) -> Result<Field> {
    use serde_json::Value;
    match *json {
        Value::Object(ref map) => {
            let name = match map.get("name") {
                Some(Value::String(name)) => name.to_string(),
                _ => {
                    return Err(ArrowError::ParseError(
                        "Field missing 'name' attribute".to_string(),
                    ));
                }
            };
            let nullable = match map.get("nullable") {
                Some(&Value::Bool(b)) => b,
                _ => {
                    return Err(ArrowError::ParseError(
                        "Field missing 'nullable' attribute".to_string(),
                    ));
                }
            };
            let data_type = match map.get("type") {
                Some(t) => data_type_from_json(t)?,
                _ => {
                    return Err(ArrowError::ParseError(
                        "Field missing 'type' attribute".to_string(),
                    ));
                }
            };

            // Referenced example file: testing/data/arrow-ipc-stream/integration/1.0.0-littleendian/generated_custom_metadata.json.gz
            let metadata = match map.get("metadata") {
                Some(Value::Array(values)) => {
                    let mut res: HashMap<String, String> = HashMap::default();
                    for value in values {
                        match value.as_object() {
                            Some(map) => {
                                if map.len() != 2 {
                                    return Err(ArrowError::ParseError(
                                        "Field 'metadata' must have exact two entries for each key-value map".to_string(),
                                    ));
                                }
                                if let (Some(k), Some(v)) = (map.get("key"), map.get("value")) {
                                    if let (Some(k_str), Some(v_str)) = (k.as_str(), v.as_str()) {
                                        res.insert(
                                            k_str.to_string().clone(),
                                            v_str.to_string().clone(),
                                        );
                                    } else {
                                        return Err(ArrowError::ParseError(
                                            "Field 'metadata' must have map value of string type"
                                                .to_string(),
                                        ));
                                    }
                                } else {
                                    return Err(ArrowError::ParseError("Field 'metadata' lacks map keys named \"key\" or \"value\"".to_string()));
                                }
                            }
                            _ => {
                                return Err(ArrowError::ParseError(
                                    "Field 'metadata' contains non-object key-value pair"
                                        .to_string(),
                                ));
                            }
                        }
                    }
                    res
                }
                // We also support map format, because Schema's metadata supports this.
                // See https://github.com/apache/arrow/pull/5907
                Some(Value::Object(values)) => {
                    let mut res: HashMap<String, String> = HashMap::default();
                    for (k, v) in values {
                        if let Some(str_value) = v.as_str() {
                            res.insert(k.clone(), str_value.to_string().clone());
                        } else {
                            return Err(ArrowError::ParseError(format!(
                                "Field 'metadata' contains non-string value for key {k}"
                            )));
                        }
                    }
                    res
                }
                Some(_) => {
                    return Err(ArrowError::ParseError(
                        "Field `metadata` is not json array".to_string(),
                    ));
                }
                _ => HashMap::default(),
            };

            // if data_type is a struct or list, get its children
            let data_type = match data_type {
                DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
                    match map.get("children") {
                        Some(Value::Array(values)) => {
                            if values.len() != 1 {
                                return Err(ArrowError::ParseError(
                                    "Field 'children' must have one element for a list data type"
                                        .to_string(),
                                ));
                            }
                            match data_type {
                                DataType::List(_) => {
                                    DataType::List(Arc::new(field_from_json(&values[0])?))
                                }
                                DataType::LargeList(_) => {
                                    DataType::LargeList(Arc::new(field_from_json(&values[0])?))
                                }
                                DataType::FixedSizeList(_, int) => DataType::FixedSizeList(
                                    Arc::new(field_from_json(&values[0])?),
                                    int,
                                ),
                                _ => unreachable!(
                                    "Data type should be a list, largelist or fixedsizelist"
                                ),
                            }
                        }
                        Some(_) => {
                            return Err(ArrowError::ParseError(
                                "Field 'children' must be an array".to_string(),
                            ))
                        }
                        None => {
                            return Err(ArrowError::ParseError(
                                "Field missing 'children' attribute".to_string(),
                            ));
                        }
                    }
                }
                DataType::Struct(_) => match map.get("children") {
                    Some(Value::Array(values)) => {
                        DataType::Struct(values.iter().map(field_from_json).collect::<Result<_>>()?)
                    }
                    Some(_) => {
                        return Err(ArrowError::ParseError(
                            "Field 'children' must be an array".to_string(),
                        ))
                    }
                    None => {
                        return Err(ArrowError::ParseError(
                            "Field missing 'children' attribute".to_string(),
                        ));
                    }
                },
                DataType::Map(_, keys_sorted) => {
                    match map.get("children") {
                        Some(Value::Array(values)) if values.len() == 1 => {
                            let child = field_from_json(&values[0])?;
                            // child must be a struct
                            match child.data_type() {
                                DataType::Struct(map_fields) if map_fields.len() == 2 => {
                                    DataType::Map(Arc::new(child), keys_sorted)
                                }
                                t => {
                                    return Err(ArrowError::ParseError(format!(
                                    "Map children should be a struct with 2 fields, found {t:?}"
                                )))
                                }
                            }
                        }
                        Some(_) => {
                            return Err(ArrowError::ParseError(
                                "Field 'children' must be an array with 1 element".to_string(),
                            ))
                        }
                        None => {
                            return Err(ArrowError::ParseError(
                                "Field missing 'children' attribute".to_string(),
                            ));
                        }
                    }
                }
                DataType::Union(fields, mode) => match map.get("children") {
                    Some(Value::Array(values)) => {
                        let fields = fields
                            .iter()
                            .zip(values)
                            .map(|((id, _), value)| Ok((id, Arc::new(field_from_json(value)?))))
                            .collect::<Result<_>>()?;

                        DataType::Union(fields, mode)
                    }
                    Some(_) => {
                        return Err(ArrowError::ParseError(
                            "Field 'children' must be an array".to_string(),
                        ))
                    }
                    None => {
                        return Err(ArrowError::ParseError(
                            "Field missing 'children' attribute".to_string(),
                        ));
                    }
                },
                _ => data_type,
            };

            let mut dict_id = 0;
            let mut dict_is_ordered = false;

            let data_type = match map.get("dictionary") {
                Some(dictionary) => {
                    let index_type = match dictionary.get("indexType") {
                        Some(t) => data_type_from_json(t)?,
                        _ => {
                            return Err(ArrowError::ParseError(
                                "Field missing 'indexType' attribute".to_string(),
                            ));
                        }
                    };
                    dict_id = match dictionary.get("id") {
                        Some(Value::Number(n)) => n.as_i64().unwrap(),
                        _ => {
                            return Err(ArrowError::ParseError(
                                "Field missing 'id' attribute".to_string(),
                            ));
                        }
                    };
                    dict_is_ordered = match dictionary.get("isOrdered") {
                        Some(&Value::Bool(n)) => n,
                        _ => {
                            return Err(ArrowError::ParseError(
                                "Field missing 'isOrdered' attribute".to_string(),
                            ));
                        }
                    };
                    DataType::Dictionary(Box::new(index_type), Box::new(data_type))
                }
                _ => data_type,
            };

            let mut field = Field::new_dict(name, data_type, nullable, dict_id, dict_is_ordered);
            field.set_metadata(metadata);
            Ok(field)
        }
        _ => Err(ArrowError::ParseError(
            "Invalid json value type for field".to_string(),
        )),
    }
}

/// Generate a JSON representation of the `Field`.
pub fn field_to_json(field: &Field) -> serde_json::Value {
    let children: Vec<serde_json::Value> = match field.data_type() {
        DataType::Struct(fields) => fields.iter().map(|x| field_to_json(x.as_ref())).collect(),
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => vec![field_to_json(field)],
        _ => vec![],
    };

    match field.data_type() {
        DataType::Dictionary(ref index_type, ref value_type) => serde_json::json!({
            "name": field.name(),
            "nullable": field.is_nullable(),
            "type": data_type_to_json(value_type),
            "children": children,
            "dictionary": {
                "id": field.dict_id().unwrap(),
                "indexType": data_type_to_json(index_type),
                "isOrdered": field.dict_is_ordered().unwrap(),
            }
        }),
        _ => serde_json::json!({
            "name": field.name(),
            "nullable": field.is_nullable(),
            "type": data_type_to_json(field.data_type()),
            "children": children
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::UnionMode;
    use serde_json::Value;

    #[test]
    fn struct_field_to_json() {
        let f = Field::new_struct(
            "address",
            vec![
                Field::new("street", DataType::Utf8, false),
                Field::new("zip", DataType::UInt16, false),
            ],
            false,
        );
        let value: Value = serde_json::from_str(
            r#"{
                "name": "address",
                "nullable": false,
                "type": {
                    "name": "struct"
                },
                "children": [
                    {
                        "name": "street",
                        "nullable": false,
                        "type": {
                            "name": "utf8"
                        },
                        "children": []
                    },
                    {
                        "name": "zip",
                        "nullable": false,
                        "type": {
                            "name": "int",
                            "bitWidth": 16,
                            "isSigned": false
                        },
                        "children": []
                    }
                ]
            }"#,
        )
        .unwrap();
        assert_eq!(value, field_to_json(&f));
    }

    #[test]
    fn map_field_to_json() {
        let f = Field::new_map(
            "my_map",
            "my_entries",
            Field::new("my_keys", DataType::Utf8, false),
            Field::new("my_values", DataType::UInt16, true),
            true,
            false,
        );
        let value: Value = serde_json::from_str(
            r#"{
                "name": "my_map",
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
            }"#,
        )
        .unwrap();
        assert_eq!(value, field_to_json(&f));
    }

    #[test]
    fn primitive_field_to_json() {
        let f = Field::new("first_name", DataType::Utf8, false);
        let value: Value = serde_json::from_str(
            r#"{
                "name": "first_name",
                "nullable": false,
                "type": {
                    "name": "utf8"
                },
                "children": []
            }"#,
        )
        .unwrap();
        assert_eq!(value, field_to_json(&f));
    }
    #[test]
    fn parse_struct_from_json() {
        let json = r#"
        {
            "name": "address",
            "type": {
                "name": "struct"
            },
            "nullable": false,
            "children": [
                {
                    "name": "street",
                    "type": {
                    "name": "utf8"
                    },
                    "nullable": false,
                    "children": []
                },
                {
                    "name": "zip",
                    "type": {
                    "name": "int",
                    "isSigned": false,
                    "bitWidth": 16
                    },
                    "nullable": false,
                    "children": []
                }
            ]
        }
        "#;
        let value: Value = serde_json::from_str(json).unwrap();
        let dt = field_from_json(&value).unwrap();

        let expected = Field::new_struct(
            "address",
            vec![
                Field::new("street", DataType::Utf8, false),
                Field::new("zip", DataType::UInt16, false),
            ],
            false,
        );

        assert_eq!(expected, dt);
    }

    #[test]
    fn parse_map_from_json() {
        let json = r#"
        {
            "name": "my_map",
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
        "#;
        let value: Value = serde_json::from_str(json).unwrap();
        let dt = field_from_json(&value).unwrap();

        let expected = Field::new_map(
            "my_map",
            "my_entries",
            Field::new("my_keys", DataType::Utf8, false),
            Field::new("my_values", DataType::UInt16, true),
            true,
            false,
        );

        assert_eq!(expected, dt);
    }

    #[test]
    fn parse_union_from_json() {
        let json = r#"
        {
            "name": "my_union",
            "nullable": false,
            "type": {
                "name": "union",
                "mode": "SPARSE",
                "typeIds": [
                    5,
                    7
                ]
            },
            "children": [
                {
                    "name": "f1",
                    "type": {
                        "name": "int",
                        "isSigned": true,
                        "bitWidth": 32
                    },
                    "nullable": true,
                    "children": []
                },
                {
                    "name": "f2",
                    "type": {
                        "name": "utf8"
                    },
                    "nullable": true,
                    "children": []
                }
            ]
        }
        "#;
        let value: Value = serde_json::from_str(json).unwrap();
        let dt = field_from_json(&value).unwrap();

        let expected = Field::new_union(
            "my_union",
            vec![5, 7],
            vec![
                Field::new("f1", DataType::Int32, true),
                Field::new("f2", DataType::Utf8, true),
            ],
            UnionMode::Sparse,
        );

        assert_eq!(expected, dt);
    }
}
