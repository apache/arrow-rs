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

use arrow::datatypes::{DataType, Field, Schema};
use std::collections::HashMap;
/// The tests in this file ensure a `Schema` can be manipulated
/// outside of the arrow crate

#[test]
fn schema_destructure() {
    let meta = [("foo".to_string(), "baz".to_string())]
        .into_iter()
        .collect::<HashMap<String, String>>();

    let field = Field::new("c1", DataType::Utf8, false);
    let schema = Schema::new(vec![field]).with_metadata(meta);

    // Destructuring a Schema allows rewriting metadata
    // without copying
    //
    // Model this usecase below:

    let Schema {
        fields,
        mut metadata,
    } = schema;

    metadata.insert("foo".to_string(), "bar".to_string());

    let new_schema = Schema::new(fields).with_metadata(metadata);

    assert_eq!(new_schema.metadata.get("foo").unwrap(), "bar");
}
