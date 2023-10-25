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

//! Convert ORC schema to Arrow schema

use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};

use crate::errors::{OrcError, Result};
use crate::proto;

/// Convert ORC types into a root Arrow Schema, where expects the first
/// type to be a Struct, with subsequent fields being the child fields
/// of the struct.
///
/// Returns error if found an unsupported data type.
pub fn to_root_schema(types: &[proto::Type]) -> Result<SchemaRef> {
    if types.is_empty() {
        return Err(OrcError::SchemaConversion(
            "Empty type list when reading".to_string(),
        ));
    }
    let root = &types[0];
    let root = orc_type_to_arrow_type(root, types)?;

    match root {
        DataType::Struct(fields) => Ok(Arc::new(Schema::new(fields))),
        data_type => Err(OrcError::SchemaConversion(format!(
            "Unexpected root data type when reading: {data_type}"
        ))),
    }
}

fn orc_type_to_arrow_type(orc_type: &proto::Type, all_types: &[proto::Type]) -> Result<DataType> {
    use proto::r#type::Kind;
    match orc_type.kind() {
        Kind::Boolean => Ok(DataType::Boolean),
        Kind::Byte => Ok(DataType::Int8),
        Kind::Short => Ok(DataType::Int16),
        Kind::Int => Ok(DataType::Int32),
        Kind::Long => Ok(DataType::Int64),
        Kind::Float => Ok(DataType::Float32),
        Kind::Double => Ok(DataType::Float64),
        Kind::Binary => Ok(DataType::Binary),
        Kind::String | Kind::Varchar | Kind::Char => Ok(DataType::Utf8),
        Kind::Date => Ok(DataType::Date32),
        Kind::Timestamp => {
            // TODO: support
            Err(nyi_err!("ORC data type: Timestamp"))
        }
        Kind::TimestampInstant => {
            // TODO: support
            Err(nyi_err!("ORC data type: TimestampInstant"))
        }
        Kind::Decimal => {
            let _precision = orc_type.precision() as u8;
            let _scale = orc_type.scale() as i8;
            // TODO: support
            Err(nyi_err!("ORC data type: Decimal"))
        }
        Kind::List => {
            let _subtypes = &orc_type.subtypes;
            // TODO: support
            Err(nyi_err!("ORC data type: List"))
        }
        Kind::Map => {
            let _subtypes = &orc_type.subtypes;
            // TODO: support
            Err(nyi_err!("ORC data type: Map"))
        }
        Kind::Struct => {
            let field_names = &orc_type.field_names;
            let subtypes = &orc_type.subtypes;
            let fields = field_names
                .iter()
                .zip(subtypes)
                .map(|(name, &index)| {
                    all_types
                        .get(index as usize)
                        .ok_or_else(|| {
                            OrcError::SchemaConversion(format!(
                                "Struct column index out of bounds: {index}"
                            ))
                        })
                        .and_then(|orc_type| orc_type_to_arrow_type(orc_type, all_types))
                        .map(|dt| Field::new(name, dt, true))
                })
                .collect::<Result<Vec<_>>>()?;
            let fields = Fields::from(fields);
            Ok(DataType::Struct(fields))
        }
        Kind::Union => {
            let _subtypes = &orc_type.subtypes;
            // TODO: support
            Err(nyi_err!("ORC data type: Union"))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::proto::Type;

    use super::*;
    use proto::r#type::Kind;

    #[test]
    fn test_to_root_schema() -> Result<()> {
        // empty schema is error
        let err = to_root_schema(&[]);
        assert!(err.is_err());
        assert_eq!(
            err.err().unwrap().to_string(),
            "ORC schema error: Empty type list when reading"
        );

        // non-struct root is error
        let mut t = Type::default();
        t.set_kind(Kind::Boolean);
        let err = to_root_schema(&[t]);
        assert!(err.is_err());
        assert_eq!(
            err.err().unwrap().to_string(),
            "ORC schema error: Unexpected root data type when reading: Boolean"
        );

        Ok(())
    }
}
