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
pub(crate) enum InferTy {
    Any,
    Scalar(ScalarTy),
    Array(Arc<InferTy>),
    Object(Arc<InferFields>),
}

/// An inferred scalar type.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum ScalarTy {
    Bool,
    Int64,
    Float64,
    String,
}

/// A field in an inferred object type.
pub(crate) type InferFields = [(Arc<str>, InferTy)];

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
///
/// Mixed integer and float types will coerce to float, and any other
/// mixtures will coerce to string.
fn infer_scalar(scalar: ScalarTy, expected: InferTy) -> Result<InferTy, ArrowError> {
    use ScalarTy::*;

    Ok(InferTy::Scalar(match expected {
        InferTy::Any => scalar,
        InferTy::Scalar(expect) => match (expect, &scalar) {
            (Bool, Bool) => Bool,
            (Int64, Int64) => Int64,
            (Int64 | Float64, Int64 | Float64) => Float64,
            _ => String,
        },
        _ => Err(ArrowError::JsonError(format!(
            "Expected {expected}, found {scalar}"
        )))?,
    }))
}

/// Infers the type of a JSON array, given an expected type.
fn infer_array<'a, I>(mut elements: I, expected: InferTy) -> Result<InferTy, ArrowError>
where
    I: Iterator,
    I::Item: JsonValue<'a>,
{
    let expected_elem = match expected {
        InferTy::Any => InferTy::Any.to_arc(),
        InferTy::Array(inner) => inner.clone(),
        _ => Err(ArrowError::JsonError(format!(
            "Expected {expected}, found an array"
        )))?,
    };

    let elem = elements.try_fold((*expected_elem).clone(), |expected, value| {
        infer_json_type(value, expected)
    })?;

    // If the inferred type is the same as the expected type,
    // reuse the expected type and thus any inner `Arc`s it contains,
    // to avoid excess heap allocations.
    if elem.ptr_eq(&*expected_elem) {
        Ok(InferTy::Array(expected_elem))
    } else {
        Ok(InferTy::Array(elem.to_arc()))
    }
}

/// Infers the type of a JSON object, given an expected type.
fn infer_object<'a, I, T>(fields: I, expected: InferTy) -> Result<InferTy, ArrowError>
where
    I: Iterator<Item = (&'a str, T)>,
    T: JsonValue<'a>,
{
    let expected_fields = match expected {
        InferTy::Any => InferTy::empty_fields(),
        InferTy::Object(fields) => fields.clone(),
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
                let ty = infer_json_type(value, InferTy::Any)?;
                substs.insert(field_idx, (key.into(), ty));
            }
        };
    }

    if substs.is_empty() {
        return Ok(InferTy::Object(expected_fields));
    }

    let fields = (0..num_fields)
        .map(|idx| match substs.remove(&idx) {
            Some(subst) => subst,
            None => expected_fields[idx].clone(),
        })
        .collect();

    Ok(InferTy::Object(fields))
}

macro_rules! memoize {
    ($ty:ty, $value:expr) => {{
        const VALUE: LazyLock<Arc<$ty>> = LazyLock::new(|| Arc::new($value));
        VALUE.clone()
    }};
}

impl InferTy {
    /// Returns an `InferTy` that represents any possible JSON type.
    pub const fn any() -> Self {
        Self::Any
    }

    /// Returns an `InferTy` that represents an empty JSON object.
    pub fn empty_object() -> Self {
        Self::Object(Self::empty_fields())
    }

    /// Returns an `InferFields` with no fields.
    fn empty_fields() -> Arc<InferFields> {
        memoize!(InferFields, [])
    }

    /// Returns this `InferTy` as an `Arc`.
    ///
    /// To avoid excess heaps allocations, common types are memoized in static variables.
    fn to_arc(&self) -> Arc<Self> {
        use ScalarTy::*;

        match self {
            // Use a memoized `Arc` if possible
            Self::Any => memoize!(InferTy, InferTy::Any),
            Self::Scalar(Bool) => memoize!(InferTy, InferTy::Scalar(Bool)),
            Self::Scalar(Int64) => memoize!(InferTy, InferTy::Scalar(Int64)),
            Self::Scalar(Float64) => memoize!(InferTy, InferTy::Scalar(Float64)),
            Self::Scalar(String) => memoize!(InferTy, InferTy::Scalar(String)),
            // Otherwise allocate a new `Arc`
            _ => Arc::new(self.clone()),
        }
    }

    /// Performs a shallow comparison between two types, only checking for
    /// pointer equality on any nested `Arc`s.
    fn ptr_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Any, Self::Any) => true,
            (Self::Scalar(lhs), Self::Scalar(rhs)) => lhs == rhs,
            (Self::Array(lhs), Self::Array(rhs)) => Arc::ptr_eq(lhs, rhs),
            (Self::Object(lhs), Self::Object(rhs)) => Arc::ptr_eq(lhs, rhs),
            _ => false,
        }
    }

    pub fn to_datatype(&self) -> DataType {
        match self {
            InferTy::Any => DataType::Null,
            InferTy::Scalar(s) => s.into_datatype(),
            InferTy::Array(elem) => DataType::List(elem.to_list_field().into()),
            InferTy::Object(fields) => {
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
        let InferTy::Object(fields) = self else {
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

impl Display for InferTy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InferTy::Any => write!(f, "any value"),
            InferTy::Scalar(s) => write!(f, "{}", s),
            InferTy::Array(_) => write!(f, "an array"),
            InferTy::Object(_) => write!(f, "an object"),
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
