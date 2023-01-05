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

use serde_json::value::RawValue;

use arrow_array::builder::BooleanBuilder;
use arrow_array::Array;
use arrow_data::ArrayData;
use arrow_schema::ArrowError;

use crate::raw::ArrayDecoder;

#[derive(Default)]
pub struct BooleanArrayDecoder {}

impl ArrayDecoder for BooleanArrayDecoder {
    fn decode(&mut self, values: &[Option<&RawValue>]) -> Result<ArrayData, ArrowError> {
        let mut builder = BooleanBuilder::with_capacity(values.len());
        for v in values {
            match v {
                Some(v) => {
                    let v = v.get();
                    // First attempt to parse as primitive
                    let value = serde_json::from_str(v).map_err(|_| {
                        ArrowError::JsonError(format!(
                            "Failed to parse {v} as DataType::Boolean",
                        ))
                    })?;
                    builder.append_option(value);
                }
                None => builder.append_null(),
            }
        }

        Ok(builder.finish().into_data())
    }
}
