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

use std::borrow::Cow;
use std::marker::PhantomData;

use serde_json::value::RawValue;

use arrow_array::builder::GenericStringBuilder;
use arrow_array::{Array, OffsetSizeTrait};
use arrow_data::ArrayData;
use arrow_schema::ArrowError;

use crate::raw::ArrayDecoder;

#[derive(Default)]
pub struct StringArrayDecoder<O> {
    phantom: PhantomData<O>,
}

impl<O> ArrayDecoder for StringArrayDecoder<O>
where
    O: OffsetSizeTrait,
{
    fn decode(&mut self, values: &[Option<&RawValue>]) -> Result<ArrayData, ArrowError> {
        let bytes_capacity = values
            .iter()
            .map(|x| x.map(|x| x.get().len()).unwrap_or_default())
            .sum();

        let mut builder =
            GenericStringBuilder::<O>::with_capacity(values.len(), bytes_capacity);
        for v in values {
            match v {
                Some(v) => {
                    let v = v.get();
                    // Use Cow to allow for strings containing escapes
                    let value: Option<Cow<str>> =
                        serde_json::from_str(v).map_err(|_| {
                            ArrowError::JsonError(format!(
                                "Failed to parse \"{}\" as string",
                                v
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
