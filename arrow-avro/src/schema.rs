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

use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;

/// The metadata key used for storing the JSON encoded [`Schema`]
pub const SCHEMA_METADATA_KEY: &str = "avro.schema";

/// Either a [`PrimitiveType`] or a reference to a previously defined named type
///
/// <https://avro.apache.org/docs/1.11.1/specification/#names>
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TypeName<'a> {
    Primitive(PrimitiveType),
    Ref(&'a str),
}

/// A primitive type
///
/// <https://avro.apache.org/docs/1.11.1/specification/#primitive-types>
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PrimitiveType {
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
}

/// Additional attributes within a [`Schema`]
///
/// <https://avro.apache.org/docs/1.11.1/specification/#schema-declaration>
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Attributes<'a> {
    /// A logical type name
    ///
    /// <https://avro.apache.org/docs/1.11.1/specification/#logical-types>
    #[serde(default)]
    pub logical_type: Option<&'a str>,

    /// Additional JSON attributes
    #[serde(flatten)]
    pub additional: HashMap<&'a str, serde_json::Value>,
}

impl Attributes<'_> {
    /// Returns the field metadata for this [`Attributes`]
    pub(crate) fn field_metadata(&self) -> HashMap<String, String> {
        self.additional
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }
}

/// A type definition that is not a variant of [`ComplexType`]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Type<'a> {
    #[serde(borrow)]
    pub r#type: TypeName<'a>,
    #[serde(flatten)]
    pub attributes: Attributes<'a>,
}

/// An Avro schema
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Schema<'a> {
    #[serde(borrow)]
    TypeName(TypeName<'a>),
    #[serde(borrow)]
    Union(Vec<Schema<'a>>),
    #[serde(borrow)]
    Complex(ComplexType<'a>),
    #[serde(borrow)]
    Type(Type<'a>),
}

/// A complex type
///
/// <https://avro.apache.org/docs/1.11.1/specification/#complex-types>
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ComplexType<'a> {
    #[serde(borrow)]
    Record(Record<'a>),
    #[serde(borrow)]
    Enum(Enum<'a>),
    #[serde(borrow)]
    Array(Array<'a>),
    #[serde(borrow)]
    Map(Map<'a>),
    #[serde(borrow)]
    Fixed(Fixed<'a>),
}

/// A record
///
/// <https://avro.apache.org/docs/1.11.1/specification/#schema-record>
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record<'a> {
    #[serde(borrow)]
    pub name: &'a str,
    #[serde(borrow, default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<&'a str>,
    #[serde(borrow, default, skip_serializing_if = "Option::is_none")]
    pub doc: Option<&'a str>,
    #[serde(borrow, default)]
    pub aliases: Vec<&'a str>,
    #[serde(borrow)]
    pub fields: Vec<RecordField<'a>>,
    #[serde(flatten)]
    pub attributes: Attributes<'a>,
}

/// A field within a [`Record`]
///
/// **Modified** to preserve any `"default": null` even in out-of-spec union ordering.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordField<'a> {
    #[serde(borrow)]
    pub name: &'a str,
    #[serde(borrow, default, skip_serializing_if = "Option::is_none")]
    pub doc: Option<&'a str>,
    #[serde(borrow, default)]
    pub aliases: Vec<&'a str>,
    #[serde(borrow)]
    pub r#type: Schema<'a>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "allow_out_of_spec_default"
    )]
    pub default: Option<serde_json::Value>,
}

/// Custom parse logic that stores *any* default as raw JSON
/// (including "null" for non-null-first unions).
fn allow_out_of_spec_default<'de, D>(deserializer: D) -> Result<Option<serde_json::Value>, D::Error>
where
    D: Deserializer<'de>,
{
    match serde_json::Value::deserialize(deserializer) {
        Ok(v) => Ok(Some(v)),
        Err(_) => Ok(None),
    }
}

/// An enumeration
///
/// <https://avro.apache.org/docs/1.11.1/specification/#enums>
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Enum<'a> {
    #[serde(borrow)]
    pub name: &'a str,
    #[serde(borrow, default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<&'a str>,
    #[serde(borrow, default, skip_serializing_if = "Option::is_none")]
    pub doc: Option<&'a str>,
    #[serde(borrow, default)]
    pub aliases: Vec<&'a str>,
    #[serde(borrow)]
    pub symbols: Vec<&'a str>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<serde_json::Value>,
    #[serde(flatten)]
    pub attributes: Attributes<'a>,
}

/// An array
///
/// <https://avro.apache.org/docs/1.11.1/specification/#arrays>
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Array<'a> {
    #[serde(borrow)]
    pub items: Box<Schema<'a>>,
    #[serde(flatten)]
    pub attributes: Attributes<'a>,
}

/// A map
///
/// <https://avro.apache.org/docs/1.11.1/specification/#maps>
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Map<'a> {
    #[serde(borrow)]
    pub values: Box<Schema<'a>>,
    #[serde(flatten)]
    pub attributes: Attributes<'a>,
}

/// A fixed length binary array
///
/// <https://avro.apache.org/docs/1.11.1/specification/#fixed>
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fixed<'a> {
    #[serde(borrow)]
    pub name: &'a str,
    #[serde(borrow, default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<&'a str>,
    #[serde(borrow, default)]
    pub aliases: Vec<&'a str>,
    pub size: usize,
    #[serde(flatten)]
    pub attributes: Attributes<'a>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::AvroField;
    use arrow_schema::{DataType, Fields, TimeUnit};
    use serde_json::json;

    #[test]
    fn test_deserialize() {
        let t: Schema = serde_json::from_str("\"string\"").unwrap();
        assert_eq!(
            t,
            Schema::TypeName(TypeName::Primitive(PrimitiveType::String))
        );

        let t: Schema = serde_json::from_str("[\"int\", \"null\"]").unwrap();
        assert_eq!(
            t,
            Schema::Union(vec![
                Schema::TypeName(TypeName::Primitive(PrimitiveType::Int)),
                Schema::TypeName(TypeName::Primitive(PrimitiveType::Null)),
            ])
        );

        let t: Type = serde_json::from_str(
            r#"{
                   "type":"long",
                   "logicalType":"timestamp-micros"
                }"#,
        )
        .unwrap();

        let timestamp = Type {
            r#type: TypeName::Primitive(PrimitiveType::Long),
            attributes: Attributes {
                logical_type: Some("timestamp-micros"),
                additional: Default::default(),
            },
        };

        assert_eq!(t, timestamp);

        let t: ComplexType = serde_json::from_str(
            r#"{
                   "type":"fixed",
                   "name":"fixed",
                   "namespace":"topLevelRecord.value",
                   "aliases":[],
                   "size":11,
                   "logicalType":"decimal",
                   "precision":25,
                   "scale":2
                }"#,
        )
        .unwrap();

        let decimal = ComplexType::Fixed(Fixed {
            name: "fixed",
            namespace: Some("topLevelRecord.value"),
            aliases: vec![],
            size: 11,
            attributes: Attributes {
                logical_type: Some("decimal"),
                additional: vec![("precision", json!(25)), ("scale", json!(2))]
                    .into_iter()
                    .collect(),
            },
        });

        assert_eq!(t, decimal);

        let schema: Schema = serde_json::from_str(
            r#"{
               "type":"record",
               "name":"topLevelRecord",
               "fields":[
                  {
                     "name":"value",
                     "type":[
                        {
                           "type":"fixed",
                           "name":"fixed",
                           "namespace":"topLevelRecord.value",
                           "size":11,
                           "logicalType":"decimal",
                           "precision":25,
                           "scale":2
                        },
                        "null"
                     ]
                  }
               ]
            }"#,
        )
        .unwrap();

        assert_eq!(
            schema,
            Schema::Complex(ComplexType::Record(Record {
                name: "topLevelRecord",
                namespace: None,
                doc: None,
                aliases: vec![],
                fields: vec![RecordField {
                    name: "value",
                    doc: None,
                    aliases: vec![],
                    r#type: Schema::Union(vec![
                        Schema::Complex(decimal),
                        Schema::TypeName(TypeName::Primitive(PrimitiveType::Null)),
                    ]),
                    default: None,
                },],
                attributes: Default::default(),
            }))
        );

        let schema: Schema = serde_json::from_str(
            r#"{
                  "type": "record",
                  "name": "LongList",
                  "aliases": ["LinkedLongs"],
                  "fields" : [
                    {"name": "value", "type": "long"},
                    {"name": "next", "type": ["null", "LongList"]}
                  ]
                }"#,
        )
        .unwrap();

        assert_eq!(
            schema,
            Schema::Complex(ComplexType::Record(Record {
                name: "LongList",
                namespace: None,
                doc: None,
                aliases: vec!["LinkedLongs"],
                fields: vec![
                    RecordField {
                        name: "value",
                        doc: None,
                        aliases: vec![],
                        r#type: Schema::TypeName(TypeName::Primitive(PrimitiveType::Long)),
                        default: None,
                    },
                    RecordField {
                        name: "next",
                        doc: None,
                        aliases: vec![],
                        r#type: Schema::Union(vec![
                            Schema::TypeName(TypeName::Primitive(PrimitiveType::Null)),
                            Schema::TypeName(TypeName::Ref("LongList")),
                        ]),
                        default: None,
                    }
                ],
                attributes: Default::default(),
            }))
        );

        // Recursive schema are not supported
        let err = AvroField::try_from(&schema).unwrap_err().to_string();
        assert_eq!(err, "Parser error: Failed to resolve .LongList");

        let schema: Schema = serde_json::from_str(
            r#"{
               "type":"record",
               "name":"topLevelRecord",
               "fields":[
                  {
                     "name":"id",
                     "type":[
                        "int",
                        "null"
                     ]
                  },
                  {
                     "name":"timestamp_col",
                     "type":[
                        {
                           "type":"long",
                           "logicalType":"timestamp-micros"
                        },
                        "null"
                     ]
                  }
               ]
            }"#,
        )
        .unwrap();

        assert_eq!(
            schema,
            Schema::Complex(ComplexType::Record(Record {
                name: "topLevelRecord",
                namespace: None,
                doc: None,
                aliases: vec![],
                fields: vec![
                    RecordField {
                        name: "id",
                        doc: None,
                        aliases: vec![],
                        r#type: Schema::Union(vec![
                            Schema::TypeName(TypeName::Primitive(PrimitiveType::Int)),
                            Schema::TypeName(TypeName::Primitive(PrimitiveType::Null)),
                        ]),
                        default: None,
                    },
                    RecordField {
                        name: "timestamp_col",
                        doc: None,
                        aliases: vec![],
                        r#type: Schema::Union(vec![
                            Schema::Type(timestamp),
                            Schema::TypeName(TypeName::Primitive(PrimitiveType::Null)),
                        ]),
                        default: None,
                    }
                ],
                attributes: Default::default(),
            }))
        );
        let codec = AvroField::try_from(&schema).unwrap();
        assert_eq!(
            codec.field(),
            arrow_schema::Field::new(
                "topLevelRecord",
                DataType::Struct(Fields::from(vec![
                    arrow_schema::Field::new("id", DataType::Int32, true),
                    arrow_schema::Field::new(
                        "timestamp_col",
                        DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
                        true
                    ),
                ])),
                false
            )
        );

        let schema: Schema = serde_json::from_str(
            r#"{
                  "type": "record",
                  "name": "HandshakeRequest", "namespace":"org.apache.avro.ipc",
                  "fields": [
                    {"name": "clientHash", "type": {"type": "fixed", "name": "MD5", "size": 16}},
                    {"name": "clientProtocol", "type": ["null", "string"]},
                    {"name": "serverHash", "type": "MD5"},
                    {"name": "meta", "type": ["null", {"type": "map", "values": "bytes"}]}
                  ]
            }"#,
        )
        .unwrap();

        assert_eq!(
            schema,
            Schema::Complex(ComplexType::Record(Record {
                name: "HandshakeRequest",
                namespace: Some("org.apache.avro.ipc"),
                doc: None,
                aliases: vec![],
                fields: vec![
                    RecordField {
                        name: "clientHash",
                        doc: None,
                        aliases: vec![],
                        r#type: Schema::Complex(ComplexType::Fixed(Fixed {
                            name: "MD5",
                            namespace: None,
                            aliases: vec![],
                            size: 16,
                            attributes: Default::default(),
                        })),
                        default: None,
                    },
                    RecordField {
                        name: "clientProtocol",
                        doc: None,
                        aliases: vec![],
                        r#type: Schema::Union(vec![
                            Schema::TypeName(TypeName::Primitive(PrimitiveType::Null)),
                            Schema::TypeName(TypeName::Primitive(PrimitiveType::String)),
                        ]),
                        default: None,
                    },
                    RecordField {
                        name: "serverHash",
                        doc: None,
                        aliases: vec![],
                        r#type: Schema::TypeName(TypeName::Ref("MD5")),
                        default: None,
                    },
                    RecordField {
                        name: "meta",
                        doc: None,
                        aliases: vec![],
                        r#type: Schema::Union(vec![
                            Schema::TypeName(TypeName::Primitive(PrimitiveType::Null)),
                            Schema::Complex(ComplexType::Map(crate::schema::Map {
                                values: Box::new(Schema::TypeName(TypeName::Primitive(
                                    PrimitiveType::Bytes
                                ))),
                                attributes: Default::default(),
                            })),
                        ]),
                        default: None,
                    }
                ],
                attributes: Default::default(),
            }))
        );

        let t: Type = serde_json::from_str(
            r#"{
                   "type":"string",
                   "logicalType":"uuid"
                }"#,
        )
        .unwrap();

        let uuid = Type {
            r#type: TypeName::Primitive(PrimitiveType::String),
            attributes: Attributes {
                logical_type: Some("uuid"),
                additional: Default::default(),
            },
        };

        assert_eq!(t, uuid);

        // Ensure aliases are parsed
        let schema: Schema = serde_json::from_str(
            r#"{
                  "type": "record",
                  "name": "Foo",
                  "aliases": ["Bar"],
                  "fields" : [
                    {"name":"id","aliases":["uid"],"type":"int"}
                  ]
                }"#,
        )
        .unwrap();

        let with_aliases = Schema::Complex(ComplexType::Record(Record {
            name: "Foo",
            namespace: None,
            doc: None,
            aliases: vec!["Bar"],
            fields: vec![RecordField {
                name: "id",
                aliases: vec!["uid"],
                doc: None,
                r#type: Schema::TypeName(TypeName::Primitive(PrimitiveType::Int)),
                default: None,
            }],
            attributes: Default::default(),
        }));

        assert_eq!(schema, with_aliases);
    }

    #[test]
    fn test_default_parsing() {
        // Test that a default value is correctly parsed for a record field.
        let json_schema = r#"
        {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "a", "type": "int", "default": 10},
                {"name": "b", "type": "string", "default": "default_str"},
                {"name": "c", "type": "boolean"}
            ]
        }
        "#;
        let schema: Schema = serde_json::from_str(json_schema).unwrap();
        if let Schema::Complex(ComplexType::Record(rec)) = schema {
            assert_eq!(rec.fields.len(), 3);
            assert_eq!(rec.fields[0].default, Some(json!(10)));
            assert_eq!(rec.fields[1].default, Some(json!("default_str")));
            assert_eq!(rec.fields[2].default, None);
        } else {
            panic!("Expected record schema");
        }
    }

    #[test]
    fn test_union_int_null_with_default_null() {
        let json_schema = r#"
    {
        "type": "record",
        "name": "ImpalaNullableRecord",
        "fields": [
            {"name": "i", "type": ["int","null"], "default": null}
        ]
    }
    "#;
        let schema: Schema = serde_json::from_str(json_schema).unwrap();
        if let Schema::Complex(ComplexType::Record(rec)) = schema {
            assert_eq!(rec.fields.len(), 1);
            assert_eq!(rec.fields[0].name, "i");
            assert_eq!(rec.fields[0].default, Some(json!(null)));
            let field_codec =
                AvroField::try_from(&Schema::Complex(ComplexType::Record(rec))).unwrap();
            use arrow_schema::{DataType, Field, Fields};
            assert_eq!(
                field_codec.field(),
                Field::new(
                    "ImpalaNullableRecord",
                    DataType::Struct(Fields::from(vec![Field::new("i", DataType::Int32, true),])),
                    false
                )
            );
        } else {
            panic!("Expected record schema with union int|null, default null");
        }
    }

    #[test]
    fn test_union_impala_null_with_default_null() {
        let json_schema = r#"
        {
            "type":"record","name":"topLevelRecord","fields":[
                {"name":"id","type":["long","null"]},
                {"name":"int_array","type":[{"type":"array","items":["int","null"]},"null"]},
                {"name":"int_array_Array","type":[{"type":"array","items":[{"type":"array","items":["int","null"]},"null"]},"null"]},
                {"name":"int_map","type":[{"type":"map","values":["int","null"]},"null"]},
                {"name":"int_Map_Array","type":[{"type":"array","items":[{"type":"map","values":["int","null"]},"null"]},"null"]},
                {
                   "name":"nested_struct",
                   "type":[
                      {
                         "type":"record",
                         "name":"nested_struct",
                         "namespace":"topLevelRecord",
                         "fields":[
                            {"name":"A","type":["int","null"]},
                            {"name":"b","type":[{"type":"array","items":["int","null"]},"null"]},
                            {
                               "name":"C",
                               "type":[
                                  {
                                     "type":"record",
                                     "name":"C",
                                     "namespace":"topLevelRecord.nested_struct",
                                     "fields":[
                                        {
                                           "name":"d",
                                           "type":[
                                              {
                                                 "type":"array",
                                                 "items":[
                                                    {
                                                       "type":"array",
                                                       "items":[
                                                          {
                                                             "type":"record",
                                                             "name":"d",
                                                             "namespace":"topLevelRecord.nested_struct.C",
                                                             "fields":[
                                                                {"name":"E","type":["int","null"]},
                                                                {"name":"F","type":["string","null"]}
                                                             ]
                                                          },
                                                          "null"
                                                       ]
                                                    },
                                                    "null"
                                                 ]
                                              },
                                              "null"
                                           ]
                                        }
                                     ]
                                  },
                                  "null"
                               ]
                            },
                            {
                               "name":"g",
                               "type":[
                                  {
                                     "type":"map",
                                     "values":[
                                        {
                                           "type":"record",
                                           "name":"g",
                                           "namespace":"topLevelRecord.nested_struct",
                                           "fields":[
                                              {
                                                 "name":"H",
                                                 "type":[
                                                    {
                                                       "type":"record",
                                                       "name":"H",
                                                       "namespace":"topLevelRecord.nested_struct.g",
                                                       "fields":[
                                                          {
                                                             "name":"i",
                                                             "type":[
                                                                {
                                                                   "type":"array",
                                                                   "items":["double","null"]
                                                                },
                                                                "null"
                                                             ]
                                                          }
                                                       ]
                                                    },
                                                    "null"
                                                 ]
                                              }
                                           ]
                                        },
                                        "null"
                                     ]
                                  },
                                  "null"
                               ]
                            }
                         ]
                      },
                      "null"
                   ]
                }
            ]
        }
        "#;
        let schema: Schema = serde_json::from_str(json_schema).unwrap();
        if let Schema::Complex(ComplexType::Record(rec)) = &schema {
            assert_eq!(rec.name, "topLevelRecord");
            assert_eq!(rec.fields.len(), 6);
            let _field_codec = AvroField::try_from(&schema).unwrap();
        } else {
            panic!("Expected top-level record schema");
        }
    }
}
