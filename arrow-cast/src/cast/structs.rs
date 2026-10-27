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

pub(crate) fn cast_struct_to_struct(
    array: &StructArray,
    from_fields: Fields,
    to_fields: Fields,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    // Fast path: if field names are in the same order, we can just zip and cast
    let fields_match_order = from_fields.len() == to_fields.len()
        && from_fields
            .iter()
            .zip(to_fields.iter())
            .all(|(f1, f2)| f1.name() == f2.name());

    let fields = if fields_match_order {
        // Fast path: cast columns in order if their names match
        cast_struct_fields_in_order(array, to_fields.clone(), cast_options)?
    } else {
        let all_fields_match_by_name = to_fields.iter().all(|to_field| {
            from_fields
                .iter()
                .any(|from_field| from_field.name() == to_field.name())
        });

        if all_fields_match_by_name {
            // Slow path: match fields by name and reorder
            cast_struct_fields_by_name(array, from_fields.clone(), to_fields.clone(), cast_options)?
        } else {
            // Fallback: cast field by field in order
            cast_struct_fields_in_order(array, to_fields.clone(), cast_options)?
        }
    };

    let array = StructArray::try_new(to_fields.clone(), fields, array.nulls().cloned())?;
    Ok(Arc::new(array) as ArrayRef)
}

fn cast_struct_fields_by_name(
    array: &StructArray,
    from_fields: Fields,
    to_fields: Fields,
    cast_options: &CastOptions,
) -> Result<Vec<ArrayRef>, ArrowError> {
    to_fields
        .iter()
        .map(|to_field| {
            let from_field_idx = from_fields
                .iter()
                .position(|from_field| from_field.name() == to_field.name())
                .unwrap(); // safe because we checked above
            let column = array.column(from_field_idx);
            cast_with_options(column, to_field.data_type(), cast_options)
        })
        .collect::<Result<Vec<ArrayRef>, ArrowError>>()
}

fn cast_struct_fields_in_order(
    array: &StructArray,
    to_fields: Fields,
    cast_options: &CastOptions,
) -> Result<Vec<ArrayRef>, ArrowError> {
    array
        .columns()
        .iter()
        .zip(to_fields.iter())
        .map(|(l, field)| cast_with_options(l, field.data_type(), cast_options))
        .collect::<Result<Vec<ArrayRef>, ArrowError>>()
}
