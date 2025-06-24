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

//! Example showing how to convert Variant values to JSON

use parquet_variant::{
    variant_to_json, variant_to_json_string, variant_to_json_value, VariantBuilder,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut builder = VariantBuilder::new();

    {
        let mut person = builder.new_object();
        person.append_value("name", "Alice");
        person.append_value("age", 30i32);
        person.append_value("email", "alice@example.com");
        person.append_value("is_active", true);
        person.append_value("score", 95.7f64);
        person.append_value("department", "Engineering");
        person.finish();
    }

    let (metadata, value) = builder.finish();
    let variant = parquet_variant::Variant::try_new(&metadata, &value)?;

    let json_string = variant_to_json_string(&variant)?;
    let json_value = variant_to_json_value(&variant)?;
    let pretty_json = serde_json::to_string_pretty(&json_value)?;
    println!("{}", pretty_json);

    let mut buffer = Vec::new();
    variant_to_json(&mut buffer, &variant)?;
    let buffer_result = String::from_utf8(buffer)?;

    // Verify all methods produce the same result
    assert_eq!(json_string, buffer_result);
    assert_eq!(json_string, serde_json::to_string(&json_value)?);

    Ok(())
}
