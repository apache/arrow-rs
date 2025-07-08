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

//! Module for parsing JSON strings as Variant

use crate::{ListBuilder, ObjectBuilder, Variant, VariantBuilder, VariantBuilderExt};
use arrow_schema::ArrowError;
use serde_json::{Number, Value};

/// Converts a JSON string to Variant using [`VariantBuilder`]. The resulting `value` and `metadata`
/// buffers can be extracted using `builder.finish()`
///
/// # Arguments
/// * `json` - The JSON string to parse as Variant.
/// * `variant_builder` - Object of type `VariantBuilder` used to build the vatiant from the JSON
///   string
///
/// # Returns
///
/// * `Ok(())` if successful
/// * `Err` with error details if the conversion fails
///
/// ```rust
/// # use parquet_variant::{
/// json_to_variant, variant_to_json, variant_to_json_string, variant_to_json_value, VariantBuilder
/// };
///
/// let mut variant_builder = VariantBuilder::new();
/// let person_string = "{\"name\":\"Alice\", \"age\":30, ".to_string()
/// + "\"email\":\"alice@example.com\", \"is_active\": true, \"score\": 95.7,"
/// + "\"additional_info\": null}";
/// json_to_variant(&person_string, &mut variant_builder)?;
///
/// let (metadata, value) = variant_builder.finish();
///
/// let variant = parquet_variant::Variant::try_new(&metadata, &value)?;
///
/// let json_result = variant_to_json_string(&variant)?;
/// let json_value = variant_to_json_value(&variant)?;
///
/// let mut buffer = Vec::new();
/// variant_to_json(&mut buffer, &variant)?;
/// let buffer_result = String::from_utf8(buffer)?;
/// assert_eq!(json_result, "{\"additional_info\":null,\"age\":30,".to_string() +
/// "\"email\":\"alice@example.com\",\"is_active\":true,\"name\":\"Alice\",\"score\":95.7}");
/// assert_eq!(json_result, buffer_result);
/// assert_eq!(json_result, serde_json::to_string(&json_value)?);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub fn json_to_variant(json: &str, builder: &mut VariantBuilder) -> Result<(), ArrowError> {
    let json: Value = serde_json::from_str(json)
        .map_err(|e| ArrowError::InvalidArgumentError(format!("JSON format error: {e}")))?;

    build_json(&json, builder)?;
    Ok(())
}

fn build_json(json: &Value, builder: &mut VariantBuilder) -> Result<(), ArrowError> {
    append_json(json, builder)?;
    Ok(())
}

fn variant_from_number<'m, 'v>(n: &Number) -> Result<Variant<'m, 'v>, ArrowError> {
    if let Some(i) = n.as_i64() {
        // Find minimum Integer width to fit
        if i as i8 as i64 == i {
            Ok((i as i8).into())
        } else if i as i16 as i64 == i {
            Ok((i as i16).into())
        } else if i as i32 as i64 == i {
            Ok((i as i32).into())
        } else {
            Ok(i.into())
        }
    } else {
        // Todo: Try decimal once we implement custom JSON parsing where we have access to strings
        // Try double - currently json_to_variant does not produce decimal
        match n.as_f64() {
            Some(f) => return Ok(f.into()),
            None => Err(ArrowError::InvalidArgumentError(format!(
                "Failed to parse {n} as number",
            ))),
        }?
    }
}

fn append_json<'m, 'v>(
    json: &'v Value,
    builder: &mut impl VariantBuilderExt<'m, 'v>,
) -> Result<(), ArrowError> {
    match json {
        Value::Null => builder.append_value(Variant::Null),
        Value::Bool(b) => builder.append_value(*b),
        Value::Number(n) => {
            builder.append_value(variant_from_number(n)?);
        }
        Value::String(s) => builder.append_value(s.as_str()),
        Value::Array(arr) => {
            let mut list_builder = builder.new_list();
            for val in arr {
                append_json(val, &mut list_builder)?;
            }
            list_builder.finish();
        }
        Value::Object(obj) => {
            let mut obj_builder = builder.new_object();
            for (key, value) in obj.iter() {
                let mut field_builder = ObjectFieldBuilder {
                    key,
                    builder: &mut obj_builder,
                };
                append_json(value, &mut field_builder)?;
            }
            obj_builder.finish()?;
        }
    };
    Ok(())
}

struct ObjectFieldBuilder<'o, 'v, 's> {
    key: &'s str,
    builder: &'o mut ObjectBuilder<'v>,
}

impl<'m, 'v> VariantBuilderExt<'m, 'v> for ObjectFieldBuilder<'_, '_, '_> {
    fn append_value(&mut self, value: impl Into<Variant<'m, 'v>>) {
        self.builder.insert(self.key, value);
    }

    fn new_list(&mut self) -> ListBuilder {
        self.builder.new_list(self.key)
    }

    fn new_object(&mut self) -> ObjectBuilder {
        self.builder.new_object(self.key)
    }
}
