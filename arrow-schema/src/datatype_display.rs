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

use std::{collections::HashMap, fmt};

use crate::DataType;

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn format_metadata(metadata: &HashMap<String, String>) -> String {
            if metadata.is_empty() {
                String::new()
            } else {
                format!(", metadata: {metadata:?}")
            }
        }

        // A lot of these can still be improved a lot.
        // _Some_ of these can be parsed with `FromStr`, but not all (YET!).
        // The goal is that the formatting should always be
        // * Terse and teadable
        // * Reversible (contain all necessary information to reverse it perfectly)

        match &self {
            Self::Null => write!(f, "Null"),
            Self::Boolean => write!(f, "Boolean"),
            Self::Int8 => write!(f, "Int8"),
            Self::Int16 => write!(f, "Int16"),
            Self::Int32 => write!(f, "Int32"),
            Self::Int64 => write!(f, "Int64"),
            Self::UInt8 => write!(f, "UInt8"),
            Self::UInt16 => write!(f, "UInt16"),
            Self::UInt32 => write!(f, "UInt32"),
            Self::UInt64 => write!(f, "UInt64"),
            Self::Float16 => write!(f, "Float16"),
            Self::Float32 => write!(f, "Float32"),
            Self::Float64 => write!(f, "Float64"),
            Self::Timestamp(time_unit, timezone) => {
                if let Some(timezone) = timezone {
                    write!(f, "Timestamp({time_unit}, {timezone:?})")
                } else {
                    write!(f, "Timestamp({time_unit})")
                }
            }
            Self::Date32 => write!(f, "Date32"),
            Self::Date64 => write!(f, "Date64"),
            Self::Time32(time_unit) => write!(f, "Time32({time_unit})"),
            Self::Time64(time_unit) => write!(f, "Time64({time_unit})"),
            Self::Duration(time_unit) => write!(f, "Duration({time_unit})"),
            Self::Interval(interval_unit) => write!(f, "Interval({interval_unit:?})"),
            Self::Binary => write!(f, "Binary"),
            Self::FixedSizeBinary(bytes_per_value) => {
                write!(f, "FixedSizeBinary({bytes_per_value:?})")
            }
            Self::LargeBinary => write!(f, "LargeBinary"),
            Self::BinaryView => write!(f, "BinaryView"),
            Self::Utf8 => write!(f, "Utf8"),
            Self::LargeUtf8 => write!(f, "LargeUtf8"),
            Self::Utf8View => write!(f, "Utf8View"),
            Self::ListView(field) => write!(f, "ListView({field})"), // TODO: make more readable
            Self::LargeListView(field) => write!(f, "LargeListView({field})"), // TODO: make more readable
            Self::List(field) | Self::LargeList(field) => {
                let type_name = if matches!(self, Self::List(_)) {
                    "List"
                } else {
                    "LargeList"
                };

                let name = field.name();
                let maybe_nullable = if field.is_nullable() { "nullable " } else { "" };
                let data_type = field.data_type();
                let field_name_str = if name == "item" {
                    String::default()
                } else {
                    format!(", field: '{name}'")
                };
                let metadata_str = format_metadata(field.metadata());

                // e.g. `LargeList(nullable Uint32)
                write!(
                    f,
                    "{type_name}({maybe_nullable}{data_type}{field_name_str}{metadata_str})"
                )
            }
            Self::FixedSizeList(field, size) => {
                let name = field.name();
                let maybe_nullable = if field.is_nullable() { "nullable " } else { "" };
                let data_type = field.data_type();
                let field_name_str = if name == "item" {
                    String::default()
                } else {
                    format!(", field: '{name}'")
                };
                let metadata_str = format_metadata(field.metadata());

                write!(
                    f,
                    "FixedSizeList({size} x {maybe_nullable}{data_type}{field_name_str}{metadata_str})",
                )
            }
            Self::Struct(fields) => {
                write!(f, "Struct(")?;
                if !fields.is_empty() {
                    let fields_str = fields
                        .iter()
                        .map(|field| {
                            let name = field.name();
                            let maybe_nullable = if field.is_nullable() { "nullable " } else { "" };
                            let data_type = field.data_type();
                            let metadata_str = format_metadata(field.metadata());
                            format!("{name:?}: {maybe_nullable}{data_type}{metadata_str}")
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    write!(f, "{fields_str}")?;
                }
                write!(f, ")")?;
                Ok(())
            }
            Self::Union(union_fields, union_mode) => {
                write!(f, "Union({union_mode:?}, ")?;
                if !union_fields.is_empty() {
                    let fields_str = union_fields
                        .iter()
                        .map(|v| {
                            let type_id = v.0;
                            let field = v.1;
                            let maybe_nullable = if field.is_nullable() { "nullable " } else { "" };
                            let data_type = field.data_type();
                            let metadata_str = format_metadata(field.metadata());
                            format!("{type_id:?}: {maybe_nullable}{data_type}{metadata_str}")
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    write!(f, "{fields_str}")?;
                }
                write!(f, ")")?;
                Ok(())
            }
            Self::Dictionary(data_type, data_type1) => {
                write!(f, "Dictionary({data_type}, {data_type1})")
            }
            Self::Decimal32(precision, scale) => write!(f, "Decimal32({precision}, {scale})"),
            Self::Decimal64(precision, scale) => write!(f, "Decimal64({precision}, {scale})"),
            Self::Decimal128(precision, scale) => write!(f, "Decimal128({precision}, {scale})"),
            Self::Decimal256(precision, scale) => write!(f, "Decimal256({precision}, {scale})"),
            Self::Map(field, sorted) => {
                write!(f, "Map(")?;
                let name = field.name();
                let maybe_nullable = if field.is_nullable() { "nullable " } else { "" };
                let data_type = field.data_type();
                let metadata_str = format_metadata(field.metadata());
                let keys_are_sorted = if *sorted { "sorted" } else { "unsorted" };

                write!(
                    f,
                    "\"{name}\": {maybe_nullable}{data_type}{metadata_str}, {keys_are_sorted})"
                )?;
                Ok(())
            }
            Self::RunEndEncoded(run_ends_field, values_field) => {
                write!(f, "RunEndEncoded({run_ends_field}, {values_field})")
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use crate::Field;

    use super::*;

    #[test]
    fn test_display_list() {
        let list_data_type = DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)));
        let list_data_type_string = list_data_type.to_string();
        let expected_string = "List(nullable Int32)";
        assert_eq!(list_data_type_string, expected_string);
    }

    #[test]
    fn test_display_list_with_named_field() {
        let list_data_type = DataType::List(Arc::new(Field::new("foo", DataType::UInt64, false)));
        let list_data_type_string = list_data_type.to_string();
        let expected_string = "List(UInt64, field: 'foo')";
        assert_eq!(list_data_type_string, expected_string);
    }

    #[test]
    fn test_display_nested_list() {
        let nested_data_type = DataType::List(Arc::new(Field::new_list_field(
            DataType::List(Arc::new(Field::new_list_field(DataType::UInt64, false))),
            false,
        )));
        let nested_data_type_string = nested_data_type.to_string();
        let nested_expected_string = "List(List(UInt64))";
        assert_eq!(nested_data_type_string, nested_expected_string);
    }

    #[test]
    fn test_display_list_with_metadata() {
        let mut field = Field::new_list_field(DataType::Int32, true);
        let metadata = HashMap::from([("foo1".to_string(), "value1".to_string())]);
        field.set_metadata(metadata);
        let list_data_type = DataType::List(Arc::new(field));
        let list_data_type_string = list_data_type.to_string();
        let expected_string = "List(nullable Int32, metadata: {\"foo1\": \"value1\"})";

        assert_eq!(list_data_type_string, expected_string);
    }

    #[test]
    fn test_display_large_list() {
        let large_list_data_type =
            DataType::LargeList(Arc::new(Field::new_list_field(DataType::Int32, true)));
        let large_list_data_type_string = large_list_data_type.to_string();
        let expected_string = "LargeList(nullable Int32)";
        assert_eq!(large_list_data_type_string, expected_string);

        // Test with named field
        let large_list_named =
            DataType::LargeList(Arc::new(Field::new("bar", DataType::UInt64, false)));
        let large_list_named_string = large_list_named.to_string();
        let expected_named_string = "LargeList(UInt64, field: 'bar')";
        assert_eq!(large_list_named_string, expected_named_string);

        // Test with metadata
        let mut field = Field::new_list_field(DataType::Int32, true);
        let metadata = HashMap::from([("key1".to_string(), "value1".to_string())]);
        field.set_metadata(metadata);
        let large_list_metadata = DataType::LargeList(Arc::new(field));
        let large_list_metadata_string = large_list_metadata.to_string();
        let expected_metadata_string =
            "LargeList(nullable Int32, metadata: {\"key1\": \"value1\"})";
        assert_eq!(large_list_metadata_string, expected_metadata_string);
    }

    #[test]
    fn test_display_fixed_size_list() {
        let fixed_size_list =
            DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::Int32, true)), 5);
        let fixed_size_list_string = fixed_size_list.to_string();
        let expected_string = "FixedSizeList(5 x nullable Int32)";
        assert_eq!(fixed_size_list_string, expected_string);

        // Test with named field
        let fixed_size_named =
            DataType::FixedSizeList(Arc::new(Field::new("baz", DataType::UInt64, false)), 3);
        let fixed_size_named_string = fixed_size_named.to_string();
        let expected_named_string = "FixedSizeList(3 x UInt64, field: 'baz')";
        assert_eq!(fixed_size_named_string, expected_named_string);

        // Test with metadata
        let mut field = Field::new_list_field(DataType::Int32, true);
        let metadata = HashMap::from([("key2".to_string(), "value2".to_string())]);
        field.set_metadata(metadata);
        let fixed_size_metadata = DataType::FixedSizeList(Arc::new(field), 4);
        let fixed_size_metadata_string = fixed_size_metadata.to_string();
        let expected_metadata_string =
            "FixedSizeList(4 x nullable Int32, metadata: {\"key2\": \"value2\"})";
        assert_eq!(fixed_size_metadata_string, expected_metadata_string);
    }

    #[test]
    fn test_display_struct() {
        let fields = vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ];
        let struct_data_type = DataType::Struct(fields.into());
        let struct_data_type_string = struct_data_type.to_string();
        let expected_string = "Struct(\"a\": Int32, \"b\": nullable Utf8)";
        assert_eq!(struct_data_type_string, expected_string);

        // Test with metadata
        let mut field_with_metadata = Field::new("b", DataType::Utf8, true);
        let metadata = HashMap::from([("key".to_string(), "value".to_string())]);
        field_with_metadata.set_metadata(metadata);
        let struct_fields_with_metadata =
            vec![Field::new("a", DataType::Int32, false), field_with_metadata];
        let struct_data_type_with_metadata = DataType::Struct(struct_fields_with_metadata.into());
        let struct_data_type_with_metadata_string = struct_data_type_with_metadata.to_string();
        let expected_string_with_metadata =
            "Struct(\"a\": Int32, \"b\": nullable Utf8, metadata: {\"key\": \"value\"})";
        assert_eq!(
            struct_data_type_with_metadata_string,
            expected_string_with_metadata
        );
    }

    #[test]
    fn test_display_union() {
        let fields = vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ];
        let type_ids = vec![0, 1];
        let union_fields = type_ids
            .into_iter()
            .zip(fields.into_iter().map(Arc::new))
            .collect();

        let union_data_type = DataType::Union(union_fields, crate::UnionMode::Sparse);
        let union_data_type_string = union_data_type.to_string();
        let expected_string = "Union(Sparse, 0: Int32, 1: nullable Utf8)";
        assert_eq!(union_data_type_string, expected_string);

        // Test with metadata
        let mut field_with_metadata = Field::new("b", DataType::Utf8, true);
        let metadata = HashMap::from([("key".to_string(), "value".to_string())]);
        field_with_metadata.set_metadata(metadata);
        let union_fields_with_metadata = vec![
            (0, Arc::new(Field::new("a", DataType::Int32, false))),
            (1, Arc::new(field_with_metadata)),
        ]
        .into_iter()
        .collect();
        let union_data_type_with_metadata =
            DataType::Union(union_fields_with_metadata, crate::UnionMode::Sparse);
        let union_data_type_with_metadata_string = union_data_type_with_metadata.to_string();
        let expected_string_with_metadata =
            "Union(Sparse, 0: Int32, 1: nullable Utf8, metadata: {\"key\": \"value\"})";
        assert_eq!(
            union_data_type_with_metadata_string,
            expected_string_with_metadata
        );
    }

    #[test]
    fn test_display_map() {
        let entry_field = Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Int32, true),
                ]
                .into(),
            ),
            false,
        );
        let map_data_type = DataType::Map(Arc::new(entry_field), true);
        let map_data_type_string = map_data_type.to_string();
        let expected_string =
            "Map(\"entries\": Struct(\"key\": Utf8, \"value\": nullable Int32), sorted)";
        assert_eq!(map_data_type_string, expected_string);

        // Test with metadata
        let mut entry_field_with_metadata = Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Int32, true),
                ]
                .into(),
            ),
            false,
        );
        let metadata = HashMap::from([("key".to_string(), "value".to_string())]);
        entry_field_with_metadata.set_metadata(metadata);
        let map_data_type_with_metadata = DataType::Map(Arc::new(entry_field_with_metadata), true);
        let map_data_type_with_metadata_string = map_data_type_with_metadata.to_string();
        let expected_string_with_metadata = "Map(\"entries\": Struct(\"key\": Utf8, \"value\": nullable Int32), metadata: {\"key\": \"value\"}, sorted)";
        assert_eq!(
            map_data_type_with_metadata_string,
            expected_string_with_metadata
        );
    }
}
