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

use arrow_schema::{ArrowError, DataType, FieldRef, Fields};

// Keywords defined by the shredding spec
pub const METADATA: &str = "metadata";
pub const VALUE: &str = "value";
pub const TYPED_VALUE: &str = "typed_value";

#[derive(Debug)]
pub struct VariantSchema {
    metadata_ref: FieldRef,
    typed_value_ref: Option<FieldRef>,
    value_ref: Option<FieldRef>,
}

impl VariantSchema {
    pub fn try_new(fields: Fields) -> Result<Self, ArrowError> {
        todo!()
    }
}

pub fn validate_value_and_typed_value(
    fields: &Fields,
    allow_both_null: bool,
) -> Result<(), ArrowError> {
    let value_field_res = fields.iter().find(|f| f.name() == VALUE);
    let typed_value_field_res = fields.iter().find(|f| f.name() == TYPED_VALUE);

    if !allow_both_null {
        if let (None, None) = (value_field_res, typed_value_field_res) {
            return Err(ArrowError::InvalidArgumentError(
                "Invalid VariantArray: StructArray must contain either `value` or `typed_value` fields or both.".to_string()
            ));
        }
    }

    if let Some(value_field) = value_field_res {
        if value_field.data_type() != &DataType::BinaryView {
            return Err(ArrowError::NotYetImplemented(format!(
                "VariantArray 'value' field must be BinaryView, got {}",
                value_field.data_type()
            )));
        }
    }

    if let Some(typed_value_field) = fields.iter().find(|f| f.name() == TYPED_VALUE) {
        // this is directly mapped from the spec's parquet physical types
        // note, there are more data types we can support
        // but for the sake of simplicity, I chose the smallest subset
        match typed_value_field.data_type() {
            DataType::Boolean
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::BinaryView => {}
            DataType::Union(union_fields, _) => {
                union_fields
                    .iter()
                    .map(|(_, f)| f.clone())
                    .try_for_each(|f| {
                        let DataType::Struct(fields) = f.data_type().clone() else {
                            return Err(ArrowError::InvalidArgumentError(
                                "Expected struct".to_string(),
                            ));
                        };

                        validate_value_and_typed_value(&fields, false)
                    })?;
            }

            foreign => {
                return Err(ArrowError::NotYetImplemented(format!(
                    "Unsupported VariantArray 'typed_value' field, got {foreign}"
                )))
            }
        }
    }

    Ok(())
}

/// Validates that the provided [`Fields`] conform to the Variant shredding specification.
///
/// # Requirements
/// - Must contain a "metadata" field of type BinaryView
/// - Must contain at least one of "value" (optional BinaryView) or "typed_value" (optional with valid Parquet type)
/// - Both "value" and "typed_value" can only be null simultaneously for shredded object fields
pub fn validate_shredded_schema(fields: &Fields) -> Result<(), ArrowError> {
    let metadata_field = fields
        .iter()
        .find(|f| f.name() == METADATA)
        .ok_or_else(|| {
            ArrowError::InvalidArgumentError(
                "Invalid VariantArray: StructArray must contain a 'metadata' field".to_string(),
            )
        })?;

    if metadata_field.is_nullable() {
        return Err(ArrowError::InvalidArgumentError(
            "Invalid VariantArray: metadata field can not be nullable".to_string(),
        ));
    }

    if metadata_field.data_type() != &DataType::BinaryView {
        return Err(ArrowError::NotYetImplemented(format!(
            "VariantArray 'metadata' field must be BinaryView, got {}",
            metadata_field.data_type()
        )));
    }

    validate_value_and_typed_value(fields, false)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_schema::{Field, UnionFields, UnionMode};

    #[test]
    fn test_regular_variant_schema() {
        // a regular variant schema
        let metadata_field = Field::new("metadata", DataType::BinaryView, false);
        let value_field = Field::new("value", DataType::BinaryView, false);

        let schema = Fields::from(vec![metadata_field, value_field]);

        validate_shredded_schema(&schema).unwrap();
    }

    #[test]
    fn test_regular_variant_schema_order_agnostic() {
        // a regular variant schema
        let metadata_field = Field::new("metadata", DataType::BinaryView, false);
        let value_field = Field::new("value", DataType::BinaryView, false);

        let schema = Fields::from(vec![value_field, metadata_field]); // note the order switch

        validate_shredded_schema(&schema).unwrap();
    }

    #[test]
    fn test_regular_variant_schema_allow_other_columns() {
        // a regular variant schema
        let metadata_field = Field::new("metadata", DataType::BinaryView, false);
        let value_field = Field::new("value", DataType::BinaryView, false);

        let trace_field = Field::new("trace_id", DataType::Utf8View, false);
        let created_at_field = Field::new("created_at", DataType::Date64, false);

        let schema = Fields::from(vec![
            metadata_field,
            trace_field,
            created_at_field,
            value_field,
        ]);

        validate_shredded_schema(&schema).unwrap();
    }

    #[test]
    fn test_regular_variant_schema_missing_metadata() {
        let value_field = Field::new("value", DataType::BinaryView, false);
        let schema = Fields::from(vec![value_field]);

        let err = validate_shredded_schema(&schema).unwrap_err();

        assert_eq!(
            err.to_string(),
            "Invalid argument error: Invalid VariantArray: StructArray must contain a 'metadata' field"
        );
    }

    #[test]
    fn test_regular_variant_schema_nullable_metadata() {
        let metadata_field = Field::new("metadata", DataType::BinaryView, true);
        let value_field = Field::new("value", DataType::BinaryView, false);

        let schema = Fields::from(vec![metadata_field, value_field]);

        let err = validate_shredded_schema(&schema).unwrap_err();

        assert_eq!(
            err.to_string(),
            "Invalid argument error: Invalid VariantArray: metadata field can not be nullable"
        );
    }

    #[test]
    fn test_regular_variant_schema_allow_nullable_value() {
        // a regular variant schema
        let metadata_field = Field::new("metadata", DataType::BinaryView, false);
        let value_field = Field::new("value", DataType::BinaryView, true);

        let schema = Fields::from(vec![metadata_field, value_field]);

        validate_shredded_schema(&schema).unwrap();
    }

    #[test]
    fn test_shredded_variant_schema() {
        let metadata_field = Field::new("metadata", DataType::BinaryView, false);
        let typed_value_field = Field::new("typed_value", DataType::Int64, false);
        let schema = Fields::from(vec![metadata_field, typed_value_field]);

        validate_shredded_schema(&schema).unwrap();
    }

    #[test]
    fn test_partially_shredded_variant_schema() {
        let metadata_field = Field::new("metadata", DataType::BinaryView, false);
        let value_field = Field::new("value", DataType::BinaryView, false);
        let typed_value_field = Field::new("typed_value", DataType::Int64, false);
        let schema = Fields::from(vec![metadata_field, value_field, typed_value_field]);

        validate_shredded_schema(&schema).unwrap();
    }

    #[test]
    fn test_partially_shredded_variant_list_schema() {
        /*
        optional group tags (VARIANT) {
            required binary metadata;
            optional binary value;
            optional group typed_value (LIST) {   # must be optional to allow a null list
                repeated group list {
                    required group element {          # shredded element
                        optional binary value;
                        optional binary typed_value (STRING);
                    }
                    required group element {          # shredded element
                        optional binary value;
                        optional int64 typed_value ;
                    }
                }
            }
        }
        */

        let metadata_field = Field::new("metadata", DataType::BinaryView, false);
        let value_field = Field::new("value", DataType::BinaryView, false);

        // Define union fields for different element types
        let string_element = {
            let value_field = Field::new("value", DataType::BinaryView, true);
            let typed_value = Field::new("typed_value", DataType::BinaryView, true);
            DataType::Struct(Fields::from(vec![value_field, typed_value]))
        };

        let int_element = {
            let value_field = Field::new("value", DataType::BinaryView, true);
            let typed_value = Field::new("typed_value", DataType::Int64, true);
            DataType::Struct(Fields::from(vec![value_field, typed_value]))
        };

        // Create union of different element types
        let union_fields = UnionFields::new(
            vec![0, 1],
            vec![
                Field::new("string_element", string_element, true),
                Field::new("int_element", int_element, true),
            ],
        );

        let typed_value_field = Field::new(
            "typed_value",
            DataType::Union(union_fields, UnionMode::Sparse),
            false,
        );
        let schema = Fields::from(vec![metadata_field, value_field, typed_value_field]);

        validate_shredded_schema(&schema).unwrap();
    }

    #[test]
    fn test_partially_shredded_variant_object_schema() {
        /*
        optional group event (VARIANT) {
            required binary metadata;
            optional binary value;                # a variant, expected to be an object
            optional group typed_value {          # shredded fields for the variant object
                required group event_type {         # shredded field for event_type
                    optional binary value;
                    optional binary typed_value (STRING);
                }
                required group event_ts {           # shredded field for event_ts
                    optional binary value;
                    optional int64 typed_value (TIMESTAMP(true, MICROS));
                }
            }
        }
        */

        let metadata_field = Field::new("metadata", DataType::BinaryView, false);
        let value_field = Field::new("value", DataType::BinaryView, false);

        // event_type
        let element_group_1 = {
            let value_field = Field::new("value", DataType::BinaryView, false);
            let typed_value = Field::new("typed_value", DataType::BinaryView, false); // this is the string case

            Fields::from(vec![value_field, typed_value])
        };

        // event_ts
        let element_group_2 = {
            let value_field = Field::new("value", DataType::BinaryView, false);
            let typed_value = Field::new("typed_value", DataType::Int64, false);

            Fields::from(vec![value_field, typed_value])
        };

        let typed_value_field = Field::new(
            "typed_value",
            DataType::Union(
                UnionFields::new(
                    vec![0, 1],
                    vec![
                        Field::new("event_type", DataType::Struct(element_group_1), true),
                        Field::new("event_ts", DataType::Struct(element_group_2), true),
                    ],
                ),
                UnionMode::Sparse,
            ),
            false,
        );
        let schema = Fields::from(vec![metadata_field, value_field, typed_value_field]);

        validate_shredded_schema(&schema).unwrap();
    }
}
