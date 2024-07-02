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

use crate::cast::*;

/// Helper function that takes a map container and casts the inner datatype.
pub(crate) fn cast_map_values(
    from: &MapArray,
    to_data_type: &DataType,
    cast_options: &CastOptions,
    to_ordered: bool,
) -> Result<ArrayRef, ArrowError> {
    let entries_field = if let DataType::Map(entries_field, _) = to_data_type {
        entries_field
    } else {
        return Err(ArrowError::CastError(
            "Internal Error: to_data_type is not a map type.".to_string(),
        ));
    };

    let key_field = key_field(entries_field).ok_or(ArrowError::CastError(
        "map is missing key field".to_string(),
    ))?;
    let value_field = value_field(entries_field).ok_or(ArrowError::CastError(
        "map is missing value field".to_string(),
    ))?;

    let key_array = cast_with_options(from.keys(), key_field.data_type(), cast_options)?;
    let value_array = cast_with_options(from.values(), value_field.data_type(), cast_options)?;

    Ok(Arc::new(MapArray::new(
        entries_field.clone(),
        from.offsets().clone(),
        StructArray::new(
            Fields::from(vec![key_field, value_field]),
            vec![key_array, value_array],
            from.entries().nulls().cloned(),
        ),
        from.nulls().cloned(),
        to_ordered,
    )))
}

/// Gets the key field from the entries of a map.  For all other types returns None.
pub(crate) fn key_field(entries_field: &FieldRef) -> Option<FieldRef> {
    if let DataType::Struct(fields) = entries_field.data_type() {
        fields.first().cloned()
    } else {
        None
    }
}

/// Gets the value field from the entries of a map.  For all other types returns None.
pub(crate) fn value_field(entries_field: &FieldRef) -> Option<FieldRef> {
    if let DataType::Struct(fields) = entries_field.data_type() {
        fields.get(1).cloned()
    } else {
        None
    }
}
