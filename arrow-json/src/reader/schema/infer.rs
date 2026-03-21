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

use std::collections::HashMap;
use std::fmt::Display;
use std::sync::{Arc, LazyLock};

use arrow_schema::{ArrowError, DataType, Field, Fields, Schema};

use super::json_type::{JsonType, JsonValue};

/// Represents an inferred JSON type.
#[derive(Clone, Debug)]
pub(crate) struct InferTy(Arc<TyKind>);

/// The possible variants of an `InferTy`.
#[derive(Clone, Debug)]
enum TyKind {
    Any,
    Scalar(ScalarTy),
    Array(InferTy),
    Object(Arc<[InferField]>),
}

/// A field in an inferred object type.
type InferField = (Arc<str>, InferTy);

/// An inferred scalar type.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ScalarTy {
    Bool,
    Int64,
    Float64,
    String,
}

/// During the process of schema inference, types are frequently produced and discarded.
/// As an optimisation to avoid excess heap allocations, a few common types
/// are instantiated here so they can be reused.
static COMMON_TYS: LazyLock<CommonTypes> = LazyLock::new(|| CommonTypes {
    any: InferTy(TyKind::Any.into()),
    bool: InferTy(TyKind::Scalar(ScalarTy::Bool).into()),
    int64: InferTy(TyKind::Scalar(ScalarTy::Int64).into()),
    float64: InferTy(TyKind::Scalar(ScalarTy::Float64).into()),
    string: InferTy(TyKind::Scalar(ScalarTy::String).into()),
    array_of_any: InferTy::new_array(InferTy(TyKind::Any.into())),
    empty_object: InferTy::new_object(Arc::new([])),
});

struct CommonTypes {
    any: InferTy,
    bool: InferTy,
    int64: InferTy,
    float64: InferTy,
    string: InferTy,
    array_of_any: InferTy,
    empty_object: InferTy,
}

/// Infers the type of a JSON value, given an expected type.
pub fn infer_json_type<'a, T>(value: T, expected: InferTy) -> Result<InferTy, ArrowError>
where
    T: JsonValue<'a>,
{
    match value.get() {
        JsonType::Null => Ok(expected),
        JsonType::Bool => infer_scalar(ScalarTy::Bool, expected),
        JsonType::Int64 => infer_scalar(ScalarTy::Int64, expected),
        JsonType::Float64 => infer_scalar(ScalarTy::Float64, expected),
        JsonType::String => infer_scalar(ScalarTy::String, expected),
        JsonType::Array => infer_array(value.elements(), expected),
        JsonType::Object => infer_object(value.fields(), expected),
    }
}

/// Infers the type of a scalar JSON value, given an expected type.
fn infer_scalar(scalar: ScalarTy, expected: InferTy) -> Result<InferTy, ArrowError> {
    Ok(match expected.kind() {
        TyKind::Any => match scalar {
            ScalarTy::Bool => COMMON_TYS.bool.clone(),
            ScalarTy::Int64 => COMMON_TYS.int64.clone(),
            ScalarTy::Float64 => COMMON_TYS.float64.clone(),
            ScalarTy::String => COMMON_TYS.string.clone(),
        },
        TyKind::Scalar(expect) => match (expect, &scalar) {
            (ScalarTy::Bool, ScalarTy::Bool) => COMMON_TYS.bool.clone(),
            (ScalarTy::Int64, ScalarTy::Int64) => COMMON_TYS.int64.clone(),
            // Mixed numbers coerce to f64
            (ScalarTy::Int64 | ScalarTy::Float64, ScalarTy::Int64 | ScalarTy::Float64) => {
                COMMON_TYS.float64.clone()
            }
            // Any other combination coerces to string
            _ => COMMON_TYS.string.clone(),
        },
        _ => Err(ArrowError::JsonError(format!(
            "Expected {expected}, found {scalar}"
        )))?,
    })
}

/// Infers the type of a JSON array, given an expected type.
fn infer_array<'a, I>(mut elements: I, mut expected: InferTy) -> Result<InferTy, ArrowError>
where
    I: Iterator,
    I::Item: JsonValue<'a>,
{
    if let TyKind::Any = expected.kind() {
        expected = COMMON_TYS.array_of_any.clone();
    }

    let (expected_elem, expected) = match expected.kind() {
        TyKind::Array(inner) => (inner.clone(), expected),
        _ => Err(ArrowError::JsonError(format!(
            "Expected {expected}, found an array"
        )))?,
    };

    let elem = elements.try_fold(expected_elem.clone(), |expected, value| {
        infer_json_type(value, expected)
    })?;

    if elem.ptr_eq(&expected_elem) {
        return Ok(expected);
    }

    Ok(InferTy::new_array(elem))
}

/// Infers the type of a JSON object, given an expected type.
fn infer_object<'a, I, T>(fields: I, mut expected: InferTy) -> Result<InferTy, ArrowError>
where
    I: Iterator<Item = (&'a str, T)>,
    T: JsonValue<'a>,
{
    if let TyKind::Any = expected.kind() {
        expected = COMMON_TYS.empty_object.clone();
    }

    let (expected_fields, expected) = match expected.kind() {
        TyKind::Object(fields) => (fields.clone(), expected),
        _ => Err(ArrowError::JsonError(format!(
            "Expected {expected}, found an object"
        )))?,
    };

    let mut num_fields = expected_fields.len();
    let mut substs = HashMap::<usize, (Arc<str>, InferTy)>::new();

    for (key, value) in fields {
        let existing_field = expected_fields
            .iter()
            .enumerate()
            .find(|(_, (existing_key, _))| &**existing_key == key);

        match existing_field {
            Some((field_idx, (key, expect_ty))) => {
                let ty = infer_json_type(value, expect_ty.clone())?;
                if !ty.ptr_eq(expect_ty) {
                    substs.insert(field_idx, (key.clone(), ty));
                }
            }
            None => {
                let field_idx = num_fields;
                num_fields += 1;
                let ty = infer_json_type(value, COMMON_TYS.any.clone())?;
                substs.insert(field_idx, (key.into(), ty));
            }
        };
    }

    if substs.is_empty() {
        return Ok(expected);
    }

    let fields = (0..num_fields)
        .map(|idx| match substs.remove(&idx) {
            Some(subst) => subst,
            None => expected_fields[idx].clone(),
        })
        .collect();

    Ok(InferTy::new_object(fields))
}

impl InferTy {
    pub fn any() -> Self {
        COMMON_TYS.any.clone()
    }

    fn new_array(inner: impl Into<Self>) -> Self {
        Self(TyKind::Array(inner.into()).into())
    }

    fn new_object(fields: Arc<[(Arc<str>, InferTy)]>) -> Self {
        Self(TyKind::Object(fields).into())
    }

    pub fn empty_object() -> Self {
        COMMON_TYS.empty_object.clone()
    }

    fn kind(&self) -> &TyKind {
        &self.0
    }

    fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    pub fn to_datatype(&self) -> DataType {
        match self.kind() {
            TyKind::Any => DataType::Null,
            TyKind::Scalar(s) => s.into_datatype(),
            TyKind::Array(elem) => DataType::List(elem.to_list_field().into()),
            TyKind::Object(fields) => {
                DataType::Struct(fields.iter().map(|(key, ty)| ty.to_field(&**key)).collect())
            }
        }
    }

    pub fn to_field(&self, name: impl Into<String>) -> Field {
        Field::new(name, self.to_datatype(), true)
    }

    pub fn to_list_field(&self) -> Field {
        Field::new_list_field(self.to_datatype(), true)
    }

    pub fn into_schema(self) -> Result<Schema, ArrowError> {
        let TyKind::Object(fields) = self.kind() else {
            Err(ArrowError::JsonError(format!(
                "Expected JSON object, found {self:?}",
            )))?
        };

        let fields = fields
            .iter()
            .map(|(key, ty)| ty.to_field(&**key))
            .collect::<Fields>();

        Ok(Schema::new(fields))
    }
}

impl From<&InferTy> for InferTy {
    fn from(value: &InferTy) -> Self {
        Self(value.0.clone())
    }
}

impl Display for InferTy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.kind() {
            TyKind::Any => write!(f, "any value"),
            TyKind::Scalar(s) => write!(f, "{}", s),
            TyKind::Array(_) => write!(f, "an array"),
            TyKind::Object(_) => write!(f, "an object"),
        }
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

impl Display for ScalarTy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScalarTy::Bool => write!(f, "a boolean"),
            ScalarTy::Int64 => write!(f, "an integer"),
            ScalarTy::Float64 => write!(f, "a number"),
            ScalarTy::String => write!(f, "a string"),
        }
    }
}
