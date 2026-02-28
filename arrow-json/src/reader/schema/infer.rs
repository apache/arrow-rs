// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.kind() (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.kind()
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;

use arrow_schema::{ArrowError, DataType, Field, Fields, Schema};
use bumpalo::Bump;

use crate::reader::tape::{Tape, TapeElement};

#[derive(Clone, Copy, Debug)]
pub struct InferredType<'t>(&'t TyKind<'t>);

#[derive(Clone, Copy, Debug)]
enum TyKind<'t> {
    Any,
    Scalar(ScalarTy),
    Array(InferredType<'t>),
    Object(&'t [(&'t str, InferredType<'t>)]),
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ScalarTy {
    Bool,
    Int64,
    Float64,
    String,
    // NOTE: Null isn't needed because it's absorbed into Never
}

pub static ANY_TY: InferredType<'static> = InferredType(&TyKind::Any);
pub static BOOL_TY: InferredType<'static> = InferredType(&TyKind::Scalar(ScalarTy::Bool));
pub static INT64_TY: InferredType<'static> = InferredType(&TyKind::Scalar(ScalarTy::Int64));
pub static FLOAT64_TY: InferredType<'static> = InferredType(&TyKind::Scalar(ScalarTy::Float64));
pub static STRING_TY: InferredType<'static> = InferredType(&TyKind::Scalar(ScalarTy::String));
pub static ARRAY_OF_ANY_TY: InferredType<'static> = InferredType(&TyKind::Array(ANY_TY));
pub static EMPTY_OBJECT_TY: InferredType<'static> = InferredType(&TyKind::Object(&[]));

/// Infers the type of the provided JSON value, given an expected type.
pub fn infer_json_type<'a, 't>(
    value: impl JsonValue<'a>,
    expected: InferredType<'t>,
    arena: &'t Bump,
) -> Result<InferredType<'t>, ArrowError> {
    let make_err = |got| {
        let expected = match expected.kind() {
            TyKind::Any => unreachable!(),
            TyKind::Scalar(_) => "a scalar value",
            TyKind::Array(_) => "an array",
            TyKind::Object(_) => "an object",
        };
        let msg = format!("Expected {expected}, found {got}");
        ArrowError::JsonError(msg)
    };

    let infer_scalar = |scalar: ScalarTy, got: &'static str| {
        Ok(match expected.kind() {
            TyKind::Any => match scalar {
                ScalarTy::Bool => BOOL_TY,
                ScalarTy::Int64 => INT64_TY,
                ScalarTy::Float64 => FLOAT64_TY,
                ScalarTy::String => STRING_TY,
            },
            TyKind::Scalar(expect) => match (expect, &scalar) {
                (ScalarTy::Bool, ScalarTy::Bool) => BOOL_TY,
                (ScalarTy::Int64, ScalarTy::Int64) => INT64_TY,
                // Mixed numbers coerce to f64
                (ScalarTy::Int64 | ScalarTy::Float64, ScalarTy::Int64 | ScalarTy::Float64) => {
                    FLOAT64_TY
                }
                // Any other combination coerces to string
                _ => STRING_TY,
            },
            _ => Err(make_err(got))?,
        })
    };

    match value.get() {
        JsonType::Null => Ok(expected),
        JsonType::Bool => infer_scalar(ScalarTy::Bool, "a boolean"),
        JsonType::Int64 => infer_scalar(ScalarTy::Int64, "a number"),
        JsonType::Float64 => infer_scalar(ScalarTy::Float64, "a number"),
        JsonType::String => infer_scalar(ScalarTy::String, "a string"),
        JsonType::Array => {
            let (expected, expected_elem) = match *expected.kind() {
                TyKind::Any => (ARRAY_OF_ANY_TY, ANY_TY),
                TyKind::Array(inner) => (expected, inner),
                _ => Err(make_err("an array"))?,
            };

            let elem = value
                .elements()
                .try_fold(expected_elem, |expected, value| {
                    let result = infer_json_type(value, expected, arena);
                    result
                })?;

            Ok(if elem.ptr_eq(expected_elem) {
                expected
            } else {
                InferredType::new_array(elem, arena)
            })
        }
        JsonType::Object => {
            let (expected, expected_fields) = match *expected.kind() {
                TyKind::Any => (EMPTY_OBJECT_TY, &[] as &[_]),
                TyKind::Object(fields) => (expected, fields),
                _ => Err(make_err("an object"))?,
            };

            let mut num_fields = expected_fields.len();
            let mut substs = HashMap::<usize, (&'t str, InferredType<'t>)>::new();

            for (key, value) in value.fields() {
                let existing_field = expected_fields
                    .iter()
                    .copied()
                    .enumerate()
                    .find(|(_, (existing_key, _))| *existing_key == key);

                match existing_field {
                    Some((field_idx, (key, expect_ty))) => {
                        let ty = infer_json_type(value, expect_ty, arena)?;
                        if !ty.ptr_eq(expect_ty) {
                            substs.insert(field_idx, (key, ty));
                        }
                    }
                    None => {
                        let field_idx = num_fields;
                        num_fields += 1;
                        let key = arena.alloc_str(key);
                        let ty = infer_json_type(value, ANY_TY, arena)?;
                        substs.insert(field_idx, (key, ty));
                    }
                };
            }

            if substs.is_empty() {
                return Ok(expected);
            }

            let fields = (0..num_fields).map(|idx| match substs.get(&idx) {
                Some(subst) => *subst,
                None => expected_fields[idx],
            });

            Ok(InferredType::new_object(fields, arena))
        }
    }
}

impl<'t> InferredType<'t> {
    fn new_array(inner: InferredType<'t>, arena: &'t Bump) -> Self {
        Self(arena.alloc(TyKind::Array(inner)))
    }

    fn new_object<F>(fields: F, arena: &'t Bump) -> Self
    where
        F: IntoIterator<Item = (&'t str, InferredType<'t>)>,
        F::IntoIter: ExactSizeIterator,
    {
        let fields = arena.alloc_slice_fill_iter(fields);
        Self(arena.alloc(TyKind::Object(fields)))
    }

    fn kind(self) -> &'t TyKind<'t> {
        self.0
    }

    fn ptr_eq(self, other: Self) -> bool {
        std::ptr::eq(self.kind(), other.kind())
    }

    pub fn into_datatype(self) -> DataType {
        match self.kind() {
            TyKind::Any => DataType::Null,
            TyKind::Scalar(s) => s.into_datatype(),
            TyKind::Array(elem) => DataType::List(elem.into_list_field().into()),
            TyKind::Object(fields) => {
                DataType::Struct(fields.iter().map(|(key, ty)| ty.into_field(*key)).collect())
            }
        }
    }

    pub fn into_field(self, name: impl Into<String>) -> Field {
        Field::new(name, self.into_datatype(), true)
    }

    pub fn into_list_field(self) -> Field {
        Field::new_list_field(self.into_datatype(), true)
    }

    pub fn into_schema(self) -> Result<Schema, ArrowError> {
        let TyKind::Object(fields) = self.kind() else {
            Err(ArrowError::JsonError(format!(
                "Expected JSON object, found {self:?}",
            )))?
        };

        let fields = fields
            .iter()
            .map(|(key, ty)| ty.into_field(*key))
            .collect::<Fields>();

        Ok(Schema::new(fields))
    }
}

impl ScalarTy {
    fn into_datatype(self) -> DataType {
        match self {
            Self::Bool => DataType::Boolean,
            Self::Int64 => DataType::Int64,
            Self::Float64 => DataType::Float64,
            Self::String => DataType::Utf8,
        }
    }
}

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
