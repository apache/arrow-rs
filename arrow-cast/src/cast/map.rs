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
    let key_array = cast_with_options(from.keys(), &to_data_type.key_type().ok_or(ArrowError::CastError("map is missing key type".to_string()))?, cast_options)?;
    let value_array = cast_with_options(from.values(), &to_data_type.value_type().ok_or(ArrowError::CastError("map is missing value type".to_string()))?, cast_options)?;

    let to_fields = Fields::from(vec![
        to_data_type.key_field().ok_or(ArrowError::CastError("map is missing key field".to_string()))?,
        to_data_type.value_field().ok_or(ArrowError::CastError("map is missing value field".to_string()))?,
    ]);

    let entries_field = if let DataType::Map(entries_field, _) = to_data_type {
        Some(entries_field)
    } else {
        None
    }.ok_or(ArrowError::CastError("to_data_type is not a map type.".to_string()))?;

    Ok(Arc::new(MapArray::new(
        entries_field.clone(),
        from.offsets().clone(),
        StructArray::new(to_fields, vec![key_array, value_array], from.nulls().cloned()),
        from.nulls().cloned(),
        to_ordered,
    )))
}
