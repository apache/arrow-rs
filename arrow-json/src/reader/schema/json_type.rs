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

use crate::reader::tape::{Tape, TapeElement};

/// Abstraction of a JSON value that is only concerned with the
/// type of the value rather than the value itself.
pub trait JsonValue<'a> {
    /// Gets the type of this JSON value.
    fn get(&self) -> JsonType;

    /// Returns an iterator over the elements of this JSON array.
    ///
    /// Panics if the JSON value is not an array.
    fn elements(&self) -> impl Iterator<Item = Self>;

    /// Returns an iterator over the fields of this JSON object.
    ///
    /// Panics if the JSON value is not an object.
    fn fields(&self) -> impl Iterator<Item = (&'a str, Self)>;
}

/// The type of a JSON value
pub enum JsonType {
    Null,
    Bool,
    Int64,
    Float64,
    String,
    Array,
    Object,
}

#[derive(Copy, Clone, Debug)]
pub struct TapeValue<'a> {
    tape: &'a Tape<'a>,
    idx: u32,
}

impl<'a> TapeValue<'a> {
    pub fn new(tape: &'a Tape<'a>, idx: u32) -> Self {
        Self { tape, idx }
    }
}

impl<'a> JsonValue<'a> for TapeValue<'a> {
    fn get(&self) -> JsonType {
        match self.tape.get(self.idx) {
            TapeElement::Null => JsonType::Null,
            TapeElement::False => JsonType::Bool,
            TapeElement::True => JsonType::Bool,
            TapeElement::I64(_) | TapeElement::I32(_) => JsonType::Int64,
            TapeElement::F64(_) | TapeElement::F32(_) => JsonType::Float64,
            TapeElement::Number(s) => {
                if self.tape.get_string(s).parse::<i64>().is_ok() {
                    JsonType::Int64
                } else {
                    JsonType::Float64
                }
            }
            TapeElement::String(_) => JsonType::String,
            TapeElement::StartList(_) => JsonType::Array,
            TapeElement::EndList(_) => unreachable!(),
            TapeElement::StartObject(_) => JsonType::Object,
            TapeElement::EndObject(_) => unreachable!(),
        }
    }

    fn elements(&self) -> impl Iterator<Item = Self> {
        self.tape
            .iter_elements(self.idx)
            .map(move |idx| Self { idx, ..*self })
    }

    fn fields(&self) -> impl Iterator<Item = (&'a str, Self)> {
        self.tape
            .iter_fields(self.idx)
            .map(move |(key, idx)| (key, Self { idx, ..*self }))
    }
}

impl<'a> JsonValue<'a> for &'a serde_json::Value {
    fn get(&self) -> JsonType {
        use serde_json::Value;

        match self {
            Value::Null => JsonType::Null,
            Value::Bool(_) => JsonType::Bool,
            Value::Number(n) => {
                if n.is_i64() {
                    JsonType::Int64
                } else {
                    JsonType::Float64
                }
            }
            Value::String(_) => JsonType::String,
            Value::Array(_) => JsonType::Array,
            Value::Object(_) => JsonType::Object,
        }
    }

    fn elements(&self) -> impl Iterator<Item = Self> {
        use serde_json::Value;

        match self {
            Value::Array(elements) => elements.iter(),
            _ => panic!("Expected an array"),
        }
    }

    fn fields(&self) -> impl Iterator<Item = (&'a str, Self)> {
        use serde_json::Value;

        match self {
            Value::Object(fields) => fields.iter().map(|(key, value)| (key.as_str(), value)),
            _ => panic!("Expected an object"),
        }
    }
}
