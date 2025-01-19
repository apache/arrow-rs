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

//! Provide SQL's CONTAINS, STARTS_WITH, ENDS_WITH operators for Arrow's binary arrays

use crate::binary_predicate::BinaryPredicate;

use arrow_array::cast::AsArray;
use arrow_array::*;
use arrow_schema::*;
use arrow_select::take::take;

#[derive(Debug)]
pub(crate) enum Op {
    Contains,
    StartsWith,
    EndsWith,
}

impl TryFrom<crate::like::Op> for Op {
    type Error = ArrowError;

    fn try_from(value: crate::like::Op) -> Result<Self, Self::Error> {
        match value {
            crate::like::Op::Contains => Ok(Op::Contains),
            crate::like::Op::StartsWith => Ok(Op::StartsWith),
            crate::like::Op::EndsWith => Ok(Op::EndsWith),
            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Invalid binary operation: {value}"
            ))),
        }
    }
}

impl std::fmt::Display for Op {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Op::Contains => write!(f, "CONTAINS"),
            Op::StartsWith => write!(f, "STARTS_WITH"),
            Op::EndsWith => write!(f, "ENDS_WITH"),
        }
    }
}

pub(crate) fn binary_apply<'a, 'i, T: BinaryArrayType<'a> + 'a>(
    op: Op,
    l: T,
    l_s: bool,
    l_v: Option<&'a dyn AnyDictionaryArray>,
    r: T,
    r_s: bool,
    r_v: Option<&'a dyn AnyDictionaryArray>,
) -> Result<BooleanArray, ArrowError> {
    let l_len = l_v.map(|l| l.len()).unwrap_or(l.len());
    if r_s {
        let idx = match r_v {
            Some(dict) if dict.null_count() != 0 => return Ok(BooleanArray::new_null(l_len)),
            Some(dict) => dict.normalized_keys()[0],
            None => 0,
        };
        if r.is_null(idx) {
            return Ok(BooleanArray::new_null(l_len));
        }
        op_scalar::<T>(op, l, l_v, r.value(idx))
    } else {
        match (l_s, l_v, r_v) {
            (true, None, None) => {
                let v = l.is_valid(0).then(|| l.value(0));
                op_binary(op, std::iter::repeat(v), r.iter())
            }
            (true, Some(l_v), None) => {
                let idx = l_v.is_valid(0).then(|| l_v.normalized_keys()[0]);
                let v = idx.and_then(|idx| l.is_valid(idx).then(|| l.value(idx)));
                op_binary(op, std::iter::repeat(v), r.iter())
            }
            (true, None, Some(r_v)) => {
                let v = l.is_valid(0).then(|| l.value(0));
                op_binary(op, std::iter::repeat(v), vectored_iter(r, r_v))
            }
            (true, Some(l_v), Some(r_v)) => {
                let idx = l_v.is_valid(0).then(|| l_v.normalized_keys()[0]);
                let v = idx.and_then(|idx| l.is_valid(idx).then(|| l.value(idx)));
                op_binary(op, std::iter::repeat(v), vectored_iter(r, r_v))
            }
            (false, None, None) => op_binary(op, l.iter(), r.iter()),
            (false, Some(l_v), None) => op_binary(op, vectored_iter(l, l_v), r.iter()),
            (false, None, Some(r_v)) => op_binary(op, l.iter(), vectored_iter(r, r_v)),
            (false, Some(l_v), Some(r_v)) => {
                op_binary(op, vectored_iter(l, l_v), vectored_iter(r, r_v))
            }
        }
    }
}

#[inline(never)]
fn op_scalar<'a, T: BinaryArrayType<'a>>(
    op: Op,
    l: T,
    l_v: Option<&dyn AnyDictionaryArray>,
    r: &[u8],
) -> Result<BooleanArray, ArrowError> {
    let r = match op {
        Op::Contains => BinaryPredicate::contains(r).evaluate_array(l, false),
        Op::StartsWith => BinaryPredicate::StartsWith(r).evaluate_array(l, false),
        Op::EndsWith => BinaryPredicate::EndsWith(r).evaluate_array(l, false),
    };

    Ok(match l_v {
        Some(v) => take(&r, v.keys(), None)?.as_boolean().clone(),
        None => r,
    })
}

fn vectored_iter<'a, T: BinaryArrayType<'a> + 'a>(
    a: T,
    a_v: &'a dyn AnyDictionaryArray,
) -> impl Iterator<Item = Option<&'a [u8]>> + 'a {
    let nulls = a_v.nulls();
    let keys = a_v.normalized_keys();
    keys.into_iter().enumerate().map(move |(idx, key)| {
        if nulls.map(|n| n.is_null(idx)).unwrap_or_default() || a.is_null(key) {
            return None;
        }
        Some(a.value(key))
    })
}

#[inline(never)]
fn op_binary<'a>(
    op: Op,
    l: impl Iterator<Item = Option<&'a [u8]>>,
    r: impl Iterator<Item = Option<&'a [u8]>>,
) -> Result<BooleanArray, ArrowError> {
    match op {
        Op::Contains => Ok(l
            .zip(r)
            .map(|(l, r)| Some(bytes_contains(l?, r?)))
            .collect()),
        Op::StartsWith => Ok(l
            .zip(r)
            .map(|(l, r)| Some(BinaryPredicate::StartsWith(r?).evaluate(l?)))
            .collect()),
        Op::EndsWith => Ok(l
            .zip(r)
            .map(|(l, r)| Some(BinaryPredicate::EndsWith(r?).evaluate(l?)))
            .collect()),
    }
}

fn bytes_contains(haystack: &[u8], needle: &[u8]) -> bool {
    memchr::memmem::find(haystack, needle).is_some()
}
