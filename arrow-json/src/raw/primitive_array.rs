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

use num::NumCast;
use std::marker::PhantomData;

use serde_json::value::RawValue;

use arrow_array::builder::PrimitiveBuilder;
use arrow_array::{Array, ArrowPrimitiveType, PrimitiveArray};
use arrow_cast::parse::Parser;
use arrow_data::ArrayData;
use arrow_schema::{ArrowError, DataType};

use crate::raw::ArrayDecoder;

pub struct PrimitiveArrayDecoder<P> {
    phantom: PhantomData<P>,
    data_type: DataType,
}

impl<P: ArrowPrimitiveType> PrimitiveArrayDecoder<P> {
    pub fn new(data_type: DataType) -> Self {
        assert!(PrimitiveArray::<P>::is_compatible(&data_type));
        Self {
            phantom: Default::default(),
            data_type,
        }
    }
}

impl<P> ArrayDecoder for PrimitiveArrayDecoder<P>
where
    P: ArrowPrimitiveType + Parser,
    P::Native: NumCast,
{
    fn decode(&mut self, values: &[Option<&RawValue>]) -> Result<ArrayData, ArrowError> {
        let mut builder = PrimitiveBuilder::<P>::with_capacity(values.len());
        for v in values {
            match v {
                Some(v) => {
                    let v = v.get();
                    // First attempt to parse as primitive
                    let value = match serde_json::from_str::<Option<f64>>(v) {
                        Ok(Some(v)) => Some(NumCast::from(v).ok_or_else(|| {
                            ArrowError::JsonError(format!(
                                "Failed to convert {v} to {}",
                                self.data_type
                            ))
                        })?),
                        Ok(None) => None,
                        // Fallback to using Parser
                        Err(_) => match serde_json::from_str(v).ok().and_then(P::parse) {
                            Some(s) => Some(s),
                            None => {
                                return Err(ArrowError::JsonError(format!(
                                    "Failed to parse {v} as {}",
                                    self.data_type
                                )))
                            }
                        },
                    };
                    builder.append_option(value);
                }
                None => builder.append_null(),
            }
        }

        Ok(builder.finish().into_data())
    }
}
