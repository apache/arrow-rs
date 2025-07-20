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

#[derive(Debug, PartialEq, Clone)]
pub enum ValueSchema {
    MissingValue,
    Value(usize),
    ShreddedValue(usize),
    PartiallyShredded {
        value_idx: usize,
        shredded_value_idx: usize,
    },
}

#[derive(Debug, Clone)]
pub struct VariantSchema {
    inner: Fields,

    metadata_idx: usize,

    // these indicies are for the top-level most `value` and `typed_value` columns
    value_schema: ValueSchema,
}

impl VariantSchema {
    /// find column metadata and ensure
    /// returns the column index
    /// # Requirements
    /// - Must contain a "metadata" field of type BinaryView
    /// - Must contain at least one of "value" (optional BinaryView) or "typed_value" (optional with valid Parquet type)
    /// - Both "value" and "typed_value" can only be null simultaneously for shredded object fields
    fn validate_metadata(fields: &Fields) -> Result<usize, ArrowError> {
        let (metadata_idx, metadata_field) = fields
            .iter()
            .enumerate()
            .find(|(_, f)| f.name() == METADATA)
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

        Ok(metadata_idx)
    }

    /// Both `value` and `typed_value` are optional fields used together to encode a single value.
    ///
    /// Values in the two fields must be interpreted according to the following table:
    ///
    /// | `value`  | `typed_value` | Meaning                                                     |
    /// |----------|---------------|-------------------------------------------------------------|
    /// | null     | null          | The value is missing; only valid for shredded object fields |
    /// | non-null | null          | The value is present and may be any type, including null    |
    /// | null     | non-null      | The value is present and is the shredded type               |
    /// | non-null | non-null      | The value is present and is a partially shredded object     |
    fn validate_value_and_typed_value(
        fields: &Fields,
        inside_shredded_object: bool,
    ) -> Result<ValueSchema, ArrowError> {
        let value_field_res = fields.iter().enumerate().find(|(_, f)| f.name() == VALUE);
        let typed_value_field_res = fields
            .iter()
            .enumerate()
            .find(|(_, f)| f.name() == TYPED_VALUE);

        // validate types
        if let Some((_, value_field)) = value_field_res {
            if value_field.data_type() != &DataType::BinaryView {
                return Err(ArrowError::NotYetImplemented(format!(
                    "VariantArray 'value' field must be BinaryView, got {}",
                    value_field.data_type()
                )));
            }
        }

        if let Some((_, typed_value_field)) = typed_value_field_res {
            match typed_value_field.data_type() {
                DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
                | DataType::Decimal32(_, _)
                | DataType::Decimal64(_, _)
                | DataType::Decimal128(_, _)
                | DataType::Date32
                | DataType::Date64
                | DataType::Time32(_)
                | DataType::Time64(_)
                | DataType::Timestamp(_, _)
                | DataType::Utf8View
                | DataType::BinaryView
                | DataType::ListView(_)
                | DataType::Struct(_) => {}
                foreign => {
                    return Err(ArrowError::NotYetImplemented(format!(
                        "Unsupported VariantArray 'typed_value' field, got {foreign}"
                    )))
                }
            }
        }

        match (value_field_res, typed_value_field_res) {
            (None, None) => {
                if inside_shredded_object {
                    return Ok(ValueSchema::MissingValue);
                }

                Err(ArrowError::InvalidArgumentError("Invalid VariantArray: StructArray must contain either `value` or `typed_value` fields or both.".to_string()))
            }
            (Some(value_field), None) => Ok(ValueSchema::Value(value_field.0)),
            (None, Some(shredded_field)) => Ok(ValueSchema::ShreddedValue(shredded_field.0)),
            (Some(_value_field), Some(_shredded_field)) => {
                todo!("how does a shredded value look like?");
                // ideally here, i would unpack the shredded_field
                // and recursively call validate_value_and_typed_value with inside_shredded_object set to true
            }
        }
    }

    pub fn try_new(fields: Fields) -> Result<Self, ArrowError> {
        let metadata_idx = Self::validate_metadata(&fields)?;
        let value_schema = Self::validate_value_and_typed_value(&fields, false)?;

        Ok(Self {
            inner: fields.clone(),
            metadata_idx,
            value_schema,
        })
    }

    pub fn inner(&self) -> &Fields {
        &self.inner
    }

    pub fn into_inner(self) -> Fields {
        self.inner
    }

    pub fn metadata_idx(&self) -> usize {
        self.metadata_idx
    }

    pub fn metadata(&self) -> &FieldRef {
        self.inner.get(self.metadata_idx).unwrap()
    }

    pub fn value_idx(&self) -> Option<usize> {
        match self.value_schema {
            ValueSchema::MissingValue => None,
            ValueSchema::ShreddedValue(_) => None,
            ValueSchema::Value(value_idx) => Some(value_idx),
            ValueSchema::PartiallyShredded { value_idx, .. } => Some(value_idx),
        }
    }

    pub fn value(&self) -> Option<&FieldRef> {
        self.value_idx().map(|i| self.inner.get(i).unwrap())
    }

    pub fn shredded_value_idx(&self) -> Option<usize> {
        match self.value_schema {
            ValueSchema::MissingValue => None,
            ValueSchema::Value(_) => None,
            ValueSchema::ShreddedValue(shredded_idx) => Some(shredded_idx),
            ValueSchema::PartiallyShredded {
                shredded_value_idx, ..
            } => Some(shredded_value_idx),
        }
    }

    pub fn shredded_value(&self) -> Option<&FieldRef> {
        self.shredded_value_idx()
            .map(|i| self.inner.get(i).unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_schema::Field;

    #[test]
    fn test_unshredded_variant_schema() {
        // a regular variant schema
        let metadata_field = Field::new("metadata", DataType::BinaryView, false);
        let value_field = Field::new("value", DataType::BinaryView, false);

        let fields = Fields::from(vec![metadata_field, value_field]);
        let variant_schema = VariantSchema::try_new(fields).unwrap();

        assert_eq!(variant_schema.metadata_idx, 0);
        assert_eq!(variant_schema.value_schema, ValueSchema::Value(1));
    }

    #[test]
    fn test_unshredded_variant_schema_order_agnostic() {
        let metadata_field = Field::new("metadata", DataType::BinaryView, false);
        let value_field = Field::new("value", DataType::BinaryView, false);

        let fields = Fields::from(vec![value_field, metadata_field]); // note the order switch
        let variant_schema = VariantSchema::try_new(fields).unwrap();

        assert_eq!(variant_schema.value_schema, ValueSchema::Value(0));
        assert_eq!(variant_schema.metadata_idx, 1);
    }

    #[test]
    fn test_shredded_variant_schema() {
        let metadata_field = Field::new("metadata", DataType::BinaryView, false);
        let shredded_field = Field::new("typed_value", DataType::Int8, true);

        let fields = Fields::from(vec![metadata_field, shredded_field]);
        let variant_schema = VariantSchema::try_new(fields).unwrap();

        assert_eq!(variant_schema.metadata_idx, 0);
        assert_eq!(variant_schema.value_schema, ValueSchema::ShreddedValue(1));
    }

    #[test]
    fn test_regular_variant_schema_missing_metadata() {
        let value_field = Field::new("value", DataType::BinaryView, false);
        let schema = Fields::from(vec![value_field]);

        let err = VariantSchema::try_new(schema).unwrap_err();

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

        let err = VariantSchema::try_new(schema).unwrap_err();

        assert_eq!(
            err.to_string(),
            "Invalid argument error: Invalid VariantArray: metadata field can not be nullable"
        );
    }
}
