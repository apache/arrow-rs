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

use crate::predicate::Predicate;
use arrow_array::cast::AsArray;
use arrow_array::*;
use arrow_schema::*;
use arrow_select::take::take;
use std::sync::Arc;

#[derive(Debug)]
enum Op {
    Like(bool),
    ILike(bool),
    Contains,
    StartsWith,
    EndsWith,
}

impl std::fmt::Display for Op {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Op::Like(false) => write!(f, "LIKE"),
            Op::Like(true) => write!(f, "NLIKE"),
            Op::ILike(false) => write!(f, "ILIKE"),
            Op::ILike(true) => write!(f, "NILIKE"),
            Op::Contains => write!(f, "CONTAINS"),
            Op::StartsWith => write!(f, "STARTS_WITH"),
            Op::EndsWith => write!(f, "ENDS_WITH"),
        }
    }
}

/// Perform SQL `left LIKE right`
///
/// There are two wildcards supported with the LIKE operator:
///
/// 1. `%` - The percent sign represents zero, one, or multiple characters
/// 2. `_` - The underscore represents a single character
///
/// For example:
/// ```
/// # use arrow_array::{StringArray, BooleanArray};
/// # use arrow_string::like::like;
/// #
/// let strings = StringArray::from(vec!["Arrow", "Arrow", "Arrow", "Ar"]);
/// let patterns = StringArray::from(vec!["A%", "B%", "A.", "A_"]);
///
/// let result = like(&strings, &patterns).unwrap();
/// assert_eq!(result, BooleanArray::from(vec![true, false, false, true]));
/// ```
pub fn like(left: &dyn Datum, right: &dyn Datum) -> Result<BooleanArray, ArrowError> {
    like_op(Op::Like(false), left, right)
}

/// Perform SQL `left ILIKE right`
///
/// This is a case-insensitive version of [`like`]
///
/// Note: this only implements loose matching as defined by the Unicode standard. For example,
/// the `ﬀ` ligature is not equivalent to `FF` and `ß` is not equivalent to `SS`
pub fn ilike(left: &dyn Datum, right: &dyn Datum) -> Result<BooleanArray, ArrowError> {
    like_op(Op::ILike(false), left, right)
}

/// Perform SQL `left NOT LIKE right`
///
/// See the documentation on [`like`] for more details
pub fn nlike(left: &dyn Datum, right: &dyn Datum) -> Result<BooleanArray, ArrowError> {
    like_op(Op::Like(true), left, right)
}

/// Perform SQL `left NOT ILIKE right`
///
/// See the documentation on [`ilike`] for more details
pub fn nilike(left: &dyn Datum, right: &dyn Datum) -> Result<BooleanArray, ArrowError> {
    like_op(Op::ILike(true), left, right)
}

/// Perform SQL `STARTSWITH(left, right)`
pub fn starts_with(
    left: &dyn Datum,
    right: &dyn Datum,
) -> Result<BooleanArray, ArrowError> {
    like_op(Op::StartsWith, left, right)
}

/// Perform SQL `ENDSWITH(left, right)`
pub fn ends_with(
    left: &dyn Datum,
    right: &dyn Datum,
) -> Result<BooleanArray, ArrowError> {
    like_op(Op::EndsWith, left, right)
}

/// Perform SQL `CONTAINS(left, right)`
pub fn contains(left: &dyn Datum, right: &dyn Datum) -> Result<BooleanArray, ArrowError> {
    like_op(Op::Contains, left, right)
}

fn like_op(op: Op, lhs: &dyn Datum, rhs: &dyn Datum) -> Result<BooleanArray, ArrowError> {
    use arrow_schema::DataType::*;
    let (l, l_s) = lhs.get();
    let (r, r_s) = rhs.get();

    if l.len() != r.len() && !l_s && !r_s {
        return Err(ArrowError::InvalidArgumentError(format!(
            "Cannot compare arrays of different lengths, got {} vs {}",
            l.len(),
            r.len()
        )));
    }

    let l_v = l.as_any_dictionary_opt();
    let l = l_v.map(|x| x.values().as_ref()).unwrap_or(l);

    let r_v = r.as_any_dictionary_opt();
    let r = r_v.map(|x| x.values().as_ref()).unwrap_or(r);

    match (l.data_type(), r.data_type()) {
        (Utf8, Utf8) => {
            apply::<i32>(op, l.as_string(), l_s, l_v, r.as_string(), r_s, r_v)
        }
        (LargeUtf8, LargeUtf8) => {
            apply::<i64>(op, l.as_string(), l_s, l_v, r.as_string(), r_s, r_v)
        }
        (l_t, r_t) => Err(ArrowError::InvalidArgumentError(format!(
            "Invalid string operation: {l_t} {op} {r_t}"
        ))),
    }
}

fn apply<O: OffsetSizeTrait>(
    op: Op,
    l: &GenericStringArray<O>,
    l_s: bool,
    l_v: Option<&dyn AnyDictionaryArray>,
    r: &GenericStringArray<O>,
    r_s: bool,
    r_v: Option<&dyn AnyDictionaryArray>,
) -> Result<BooleanArray, ArrowError> {
    let l_len = l_v.map(|l| l.len()).unwrap_or(l.len());
    if r_s {
        let idx = match r_v {
            Some(dict) if dict.null_count() != 0 => {
                return Ok(BooleanArray::new_null(l_len))
            }
            Some(dict) => dict.normalized_keys()[0],
            None => 0,
        };
        if r.is_null(idx) {
            return Ok(BooleanArray::new_null(l_len));
        }
        op_scalar(op, l, l_v, r.value(idx))
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
fn op_scalar<O: OffsetSizeTrait>(
    op: Op,
    l: &GenericStringArray<O>,
    l_v: Option<&dyn AnyDictionaryArray>,
    r: &str,
) -> Result<BooleanArray, ArrowError> {
    let r = match op {
        Op::Like(neg) => Predicate::like(r)?.evaluate_array(l, neg),
        Op::ILike(neg) => Predicate::ilike(r, l.is_ascii())?.evaluate_array(l, neg),
        Op::Contains => Predicate::Contains(r).evaluate_array(l, false),
        Op::StartsWith => Predicate::StartsWith(r).evaluate_array(l, false),
        Op::EndsWith => Predicate::EndsWith(r).evaluate_array(l, false),
    };

    Ok(match l_v {
        Some(v) => take(&r, v.keys(), None)?.as_boolean().clone(),
        None => r,
    })
}

fn vectored_iter<'a, O: OffsetSizeTrait>(
    a: &'a GenericStringArray<O>,
    a_v: &'a dyn AnyDictionaryArray,
) -> impl Iterator<Item = Option<&'a str>> + 'a {
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
    l: impl Iterator<Item = Option<&'a str>>,
    r: impl Iterator<Item = Option<&'a str>>,
) -> Result<BooleanArray, ArrowError> {
    match op {
        Op::Like(neg) => binary_predicate(l, r, neg, Predicate::like),
        Op::ILike(neg) => binary_predicate(l, r, neg, |s| Predicate::ilike(s, false)),
        Op::Contains => Ok(l.zip(r).map(|(l, r)| Some(l?.contains(r?))).collect()),
        Op::StartsWith => Ok(l.zip(r).map(|(l, r)| Some(l?.starts_with(r?))).collect()),
        Op::EndsWith => Ok(l.zip(r).map(|(l, r)| Some(l?.ends_with(r?))).collect()),
    }
}

fn binary_predicate<'a>(
    l: impl Iterator<Item = Option<&'a str>>,
    r: impl Iterator<Item = Option<&'a str>>,
    neg: bool,
    f: impl Fn(&'a str) -> Result<Predicate<'a>, ArrowError>,
) -> Result<BooleanArray, ArrowError> {
    let mut previous = None;
    l.zip(r)
        .map(|(l, r)| match (l, r) {
            (Some(l), Some(r)) => {
                let p: &Predicate = match previous {
                    Some((expr, ref predicate)) if expr == r => predicate,
                    _ => &previous.insert((r, f(r)?)).1,
                };
                Ok(Some(p.evaluate(l) != neg))
            }
            _ => Ok(None),
        })
        .collect()
}

// Deprecated kernels

fn make_scalar(data_type: &DataType, scalar: &str) -> Result<ArrayRef, ArrowError> {
    match data_type {
        DataType::Utf8 => Ok(Arc::new(StringArray::from_iter_values([scalar]))),
        DataType::LargeUtf8 => Ok(Arc::new(LargeStringArray::from_iter_values([scalar]))),
        DataType::Dictionary(_, v) => make_scalar(v.as_ref(), scalar),
        d => Err(ArrowError::InvalidArgumentError(format!(
            "Unsupported string scalar data type {d:?}",
        ))),
    }
}

macro_rules! legacy_kernels {
    ($fn_datum:ident, $fn_array:ident, $fn_scalar:ident, $fn_array_dyn:ident, $fn_scalar_dyn:ident, $deprecation:expr) => {
        #[doc(hidden)]
        #[deprecated(note = $deprecation)]
        pub fn $fn_array<O: OffsetSizeTrait>(
            left: &GenericStringArray<O>,
            right: &GenericStringArray<O>,
        ) -> Result<BooleanArray, ArrowError> {
            $fn_datum(left, right)
        }

        #[doc(hidden)]
        #[deprecated(note = $deprecation)]
        pub fn $fn_scalar<O: OffsetSizeTrait>(
            left: &GenericStringArray<O>,
            right: &str,
        ) -> Result<BooleanArray, ArrowError> {
            let scalar = GenericStringArray::<O>::from_iter_values([right]);
            $fn_datum(left, &Scalar::new(&scalar))
        }

        #[doc(hidden)]
        #[deprecated(note = $deprecation)]
        pub fn $fn_array_dyn(
            left: &dyn Array,
            right: &dyn Array,
        ) -> Result<BooleanArray, ArrowError> {
            $fn_datum(&left, &right)
        }

        #[doc(hidden)]
        #[deprecated(note = $deprecation)]
        pub fn $fn_scalar_dyn(
            left: &dyn Array,
            right: &str,
        ) -> Result<BooleanArray, ArrowError> {
            let scalar = make_scalar(left.data_type(), right)?;
            $fn_datum(&left, &Scalar::new(&scalar))
        }
    };
}

legacy_kernels!(
    like,
    like_utf8,
    like_utf8_scalar,
    like_dyn,
    like_utf8_scalar_dyn,
    "Use arrow_string::like::like"
);
legacy_kernels!(
    ilike,
    ilike_utf8,
    ilike_utf8_scalar,
    ilike_dyn,
    ilike_utf8_scalar_dyn,
    "Use arrow_string::like::ilike"
);
legacy_kernels!(
    nlike,
    nlike_utf8,
    nlike_utf8_scalar,
    nlike_dyn,
    nlike_utf8_scalar_dyn,
    "Use arrow_string::like::nlike"
);
legacy_kernels!(
    nilike,
    nilike_utf8,
    nilike_utf8_scalar,
    nilike_dyn,
    nilike_utf8_scalar_dyn,
    "Use arrow_string::like::nilike"
);
legacy_kernels!(
    contains,
    contains_utf8,
    contains_utf8_scalar,
    contains_dyn,
    contains_utf8_scalar_dyn,
    "Use arrow_string::like::contains"
);
legacy_kernels!(
    starts_with,
    starts_with_utf8,
    starts_with_utf8_scalar,
    starts_with_dyn,
    starts_with_utf8_scalar_dyn,
    "Use arrow_string::like::starts_with"
);

legacy_kernels!(
    ends_with,
    ends_with_utf8,
    ends_with_utf8_scalar,
    ends_with_dyn,
    ends_with_utf8_scalar_dyn,
    "Use arrow_string::like::ends_with"
);

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use super::*;
    use arrow_array::types::Int8Type;

    macro_rules! test_utf8 {
        ($test_name:ident, $left:expr, $right:expr, $op:expr, $expected:expr) => {
            #[test]
            fn $test_name() {
                let expected = BooleanArray::from($expected);
                let left = StringArray::from($left);
                let right = StringArray::from($right);
                let res = $op(&left, &right).unwrap();
                assert_eq!(res, expected);
            }
        };
    }

    macro_rules! test_dict_utf8 {
        ($test_name:ident, $left:expr, $right:expr, $op:expr, $expected:expr) => {
            #[test]
            fn $test_name() {
                let expected = BooleanArray::from($expected);
                let left: DictionaryArray<Int8Type> = $left.into_iter().collect();
                let right: DictionaryArray<Int8Type> = $right.into_iter().collect();
                let res = $op(&left, &right).unwrap();
                assert_eq!(res, expected);
            }
        };
    }

    macro_rules! test_utf8_scalar {
        ($test_name:ident, $left:expr, $right:expr, $op:expr, $expected:expr) => {
            #[test]
            fn $test_name() {
                let expected = BooleanArray::from($expected);

                let left = StringArray::from($left);
                let res = $op(&left, $right).unwrap();
                assert_eq!(res, expected);

                let left = LargeStringArray::from($left);
                let res = $op(&left, $right).unwrap();
                assert_eq!(res, expected);
            }
        };
        ($test_name:ident, $test_name_dyn:ident, $left:expr, $right:expr, $op:expr, $op_dyn:expr, $expected:expr) => {
            test_utf8_scalar!($test_name, $left, $right, $op, $expected);
            test_utf8_scalar!($test_name_dyn, $left, $right, $op_dyn, $expected);
        };
    }

    test_utf8!(
        test_utf8_array_like,
        vec!["arrow", "arrow", "arrow", "arrow", "arrow", "arrows", "arrow", "arrow"],
        vec!["arrow", "ar%", "%ro%", "foo", "arr", "arrow_", "arrow_", ".*"],
        like_utf8,
        vec![true, true, true, false, false, true, false, false]
    );

    test_dict_utf8!(
        test_utf8_array_like_dict,
        vec!["arrow", "arrow", "arrow", "arrow", "arrow", "arrows", "arrow", "arrow"],
        vec!["arrow", "ar%", "%ro%", "foo", "arr", "arrow_", "arrow_", ".*"],
        like_dyn,
        vec![true, true, true, false, false, true, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_like_scalar_escape_testing,
        test_utf8_array_like_scalar_dyn_escape_testing,
        vec!["varchar(255)", "int(255)", "varchar", "int"],
        "%(%)%",
        like_utf8_scalar,
        like_utf8_scalar_dyn,
        vec![true, true, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_like_scalar_escape_regex,
        test_utf8_array_like_scalar_dyn_escape_regex,
        vec![".*", "a", "*"],
        ".*",
        like_utf8_scalar,
        like_utf8_scalar_dyn,
        vec![true, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_like_scalar_escape_regex_dot,
        test_utf8_array_like_scalar_dyn_escape_regex_dot,
        vec![".", "a", "*"],
        ".",
        like_utf8_scalar,
        like_utf8_scalar_dyn,
        vec![true, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_like_scalar,
        test_utf8_array_like_scalar_dyn,
        vec!["arrow", "parquet", "datafusion", "flight"],
        "%ar%",
        like_utf8_scalar,
        like_utf8_scalar_dyn,
        vec![true, true, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_like_scalar_start,
        test_utf8_array_like_scalar_dyn_start,
        vec!["arrow", "parrow", "arrows", "arr"],
        "arrow%",
        like_utf8_scalar,
        like_utf8_scalar_dyn,
        vec![true, false, true, false]
    );

    // Replicates `test_utf8_array_like_scalar_start` `test_utf8_array_like_scalar_dyn_start` to
    // demonstrate that `SQL STARTSWITH` works as expected.
    test_utf8_scalar!(
        test_utf8_array_starts_with_scalar_start,
        test_utf8_array_starts_with_scalar_dyn_start,
        vec!["arrow", "parrow", "arrows", "arr"],
        "arrow",
        starts_with_utf8_scalar,
        starts_with_utf8_scalar_dyn,
        vec![true, false, true, false]
    );

    test_utf8_scalar!(
        test_utf8_array_like_scalar_end,
        test_utf8_array_like_scalar_dyn_end,
        vec!["arrow", "parrow", "arrows", "arr"],
        "%arrow",
        like_utf8_scalar,
        like_utf8_scalar_dyn,
        vec![true, true, false, false]
    );

    // Replicates `test_utf8_array_like_scalar_end` `test_utf8_array_like_scalar_dyn_end` to
    // demonstrate that `SQL ENDSWITH` works as expected.
    test_utf8_scalar!(
        test_utf8_array_ends_with_scalar_end,
        test_utf8_array_ends_with_scalar_dyn_end,
        vec!["arrow", "parrow", "arrows", "arr"],
        "arrow",
        ends_with_utf8_scalar,
        ends_with_utf8_scalar_dyn,
        vec![true, true, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_like_scalar_equals,
        test_utf8_array_like_scalar_dyn_equals,
        vec!["arrow", "parrow", "arrows", "arr"],
        "arrow",
        like_utf8_scalar,
        like_utf8_scalar_dyn,
        vec![true, false, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_like_scalar_one,
        test_utf8_array_like_scalar_dyn_one,
        vec!["arrow", "arrows", "parrow", "arr"],
        "arrow_",
        like_utf8_scalar,
        like_utf8_scalar_dyn,
        vec![false, true, false, false]
    );

    test_utf8_scalar!(
        test_utf8_scalar_like_escape,
        test_utf8_scalar_like_dyn_escape,
        vec!["a%", "a\\x"],
        "a\\%",
        like_utf8_scalar,
        like_utf8_scalar_dyn,
        vec![true, false]
    );

    test_utf8_scalar!(
        test_utf8_scalar_like_escape_contains,
        test_utf8_scalar_like_dyn_escape_contains,
        vec!["ba%", "ba\\x"],
        "%a\\%",
        like_utf8_scalar,
        like_utf8_scalar_dyn,
        vec![true, false]
    );

    test_utf8!(
        test_utf8_scalar_ilike_regex,
        vec!["%%%"],
        vec![r"\%_\%"],
        ilike_utf8,
        vec![true]
    );

    test_dict_utf8!(
        test_utf8_scalar_ilike_regex_dict,
        vec!["%%%"],
        vec![r"\%_\%"],
        ilike_dyn,
        vec![true]
    );

    test_utf8!(
        test_utf8_array_nlike,
        vec!["arrow", "arrow", "arrow", "arrow", "arrow", "arrows", "arrow"],
        vec!["arrow", "ar%", "%ro%", "foo", "arr", "arrow_", "arrow_"],
        nlike_utf8,
        vec![false, false, false, true, true, false, true]
    );

    test_dict_utf8!(
        test_utf8_array_nlike_dict,
        vec!["arrow", "arrow", "arrow", "arrow", "arrow", "arrows", "arrow"],
        vec!["arrow", "ar%", "%ro%", "foo", "arr", "arrow_", "arrow_"],
        nlike_dyn,
        vec![false, false, false, true, true, false, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nlike_escape_testing,
        test_utf8_array_nlike_escape_dyn_testing_dyn,
        vec!["varchar(255)", "int(255)", "varchar", "int"],
        "%(%)%",
        nlike_utf8_scalar,
        nlike_utf8_scalar_dyn,
        vec![false, false, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nlike_scalar_escape_regex,
        test_utf8_array_nlike_scalar_dyn_escape_regex,
        vec![".*", "a", "*"],
        ".*",
        nlike_utf8_scalar,
        nlike_utf8_scalar_dyn,
        vec![false, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nlike_scalar_escape_regex_dot,
        test_utf8_array_nlike_scalar_dyn_escape_regex_dot,
        vec![".", "a", "*"],
        ".",
        nlike_utf8_scalar,
        nlike_utf8_scalar_dyn,
        vec![false, true, true]
    );
    test_utf8_scalar!(
        test_utf8_array_nlike_scalar,
        test_utf8_array_nlike_scalar_dyn,
        vec!["arrow", "parquet", "datafusion", "flight"],
        "%ar%",
        nlike_utf8_scalar,
        nlike_utf8_scalar_dyn,
        vec![false, false, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nlike_scalar_start,
        test_utf8_array_nlike_scalar_dyn_start,
        vec!["arrow", "parrow", "arrows", "arr"],
        "arrow%",
        nlike_utf8_scalar,
        nlike_utf8_scalar_dyn,
        vec![false, true, false, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nlike_scalar_end,
        test_utf8_array_nlike_scalar_dyn_end,
        vec!["arrow", "parrow", "arrows", "arr"],
        "%arrow",
        nlike_utf8_scalar,
        nlike_utf8_scalar_dyn,
        vec![false, false, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nlike_scalar_equals,
        test_utf8_array_nlike_scalar_dyn_equals,
        vec!["arrow", "parrow", "arrows", "arr"],
        "arrow",
        nlike_utf8_scalar,
        nlike_utf8_scalar_dyn,
        vec![false, true, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nlike_scalar_one,
        test_utf8_array_nlike_scalar_dyn_one,
        vec!["arrow", "arrows", "parrow", "arr"],
        "arrow_",
        nlike_utf8_scalar,
        nlike_utf8_scalar_dyn,
        vec![true, false, true, true]
    );

    test_utf8!(
        test_utf8_array_ilike,
        vec!["arrow", "arrow", "ARROW", "arrow", "ARROW", "ARROWS", "arROw"],
        vec!["arrow", "ar%", "%ro%", "foo", "ar%r", "arrow_", "arrow_"],
        ilike_utf8,
        vec![true, true, true, false, false, true, false]
    );

    test_dict_utf8!(
        test_utf8_array_ilike_dict,
        vec!["arrow", "arrow", "ARROW", "arrow", "ARROW", "ARROWS", "arROw"],
        vec!["arrow", "ar%", "%ro%", "foo", "ar%r", "arrow_", "arrow_"],
        ilike_dyn,
        vec![true, true, true, false, false, true, false]
    );

    test_utf8_scalar!(
        ilike_utf8_scalar_escape_testing,
        ilike_utf8_scalar_escape_dyn_testing,
        vec!["varchar(255)", "int(255)", "varchar", "int"],
        "%(%)%",
        ilike_utf8_scalar,
        ilike_utf8_scalar_dyn,
        vec![true, true, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_ilike_scalar,
        test_utf8_array_ilike_dyn_scalar,
        vec!["arrow", "parquet", "datafusion", "flight"],
        "%AR%",
        ilike_utf8_scalar,
        ilike_utf8_scalar_dyn,
        vec![true, true, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_ilike_scalar_start,
        test_utf8_array_ilike_scalar_dyn_start,
        vec!["arrow", "parrow", "arrows", "ARR"],
        "aRRow%",
        ilike_utf8_scalar,
        ilike_utf8_scalar_dyn,
        vec![true, false, true, false]
    );

    test_utf8_scalar!(
        test_utf8_array_ilike_scalar_end,
        test_utf8_array_ilike_scalar_dyn_end,
        vec!["ArroW", "parrow", "ARRowS", "arr"],
        "%arrow",
        ilike_utf8_scalar,
        ilike_utf8_scalar_dyn,
        vec![true, true, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_ilike_scalar_equals,
        test_utf8_array_ilike_scalar_dyn_equals,
        vec!["arrow", "parrow", "arrows", "arr"],
        "Arrow",
        ilike_utf8_scalar,
        ilike_utf8_scalar_dyn,
        vec![true, false, false, false]
    );

    // We only implement loose matching
    test_utf8_scalar!(
        test_utf8_array_ilike_unicode,
        test_utf8_array_ilike_unicode_dyn,
        vec![
            "FFkoß", "FFkoSS", "FFkoss", "FFkoS", "FFkos", "ﬀkoSS", "ﬀkoß", "FFKoSS"
        ],
        "FFkoSS",
        ilike_utf8_scalar,
        ilike_utf8_scalar_dyn,
        vec![false, true, true, false, false, false, false, true]
    );

    test_utf8_scalar!(
        test_utf8_array_ilike_unicode_starts,
        test_utf8_array_ilike_unicode_start_dyn,
        vec![
            "FFkoßsdlkdf",
            "FFkoSSsdlkdf",
            "FFkosssdlkdf",
            "FFkoS",
            "FFkos",
            "ﬀkoSS",
            "ﬀkoß",
            "FfkosSsdfd",
            "FFKoSS",
        ],
        "FFkoSS%",
        ilike_utf8_scalar,
        ilike_utf8_scalar_dyn,
        vec![false, true, true, false, false, false, false, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_ilike_unicode_ends,
        test_utf8_array_ilike_unicode_ends_dyn,
        vec![
            "sdlkdfFFkoß",
            "sdlkdfFFkoSS",
            "sdlkdfFFkoss",
            "FFkoS",
            "FFkos",
            "ﬀkoSS",
            "ﬀkoß",
            "h😃klFfkosS",
            "FFKoSS",
        ],
        "%FFkoSS",
        ilike_utf8_scalar,
        ilike_utf8_scalar_dyn,
        vec![false, true, true, false, false, false, false, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_ilike_unicode_contains,
        test_utf8_array_ilike_unicode_contains_dyn,
        vec![
            "sdlkdfFkoßsdfs",
            "sdlkdfFkoSSdggs",
            "sdlkdfFkosssdsd",
            "FkoS",
            "Fkos",
            "ﬀkoSS",
            "ﬀkoß",
            "😃sadlksffkosSsh😃klF",
            "😱slgffkosSsh😃klF",
            "FFKoSS",
        ],
        "%FFkoSS%",
        ilike_utf8_scalar,
        ilike_utf8_scalar_dyn,
        vec![false, true, true, false, false, false, false, true, true, true]
    );

    // Replicates `test_utf8_array_ilike_unicode_contains` and
    // `test_utf8_array_ilike_unicode_contains_dyn` to
    // demonstrate that `SQL CONTAINS` works as expected.
    //
    // NOTE: 5 of the values were changed because the original used a case insensitive `ilike`.
    test_utf8_scalar!(
        test_utf8_array_contains_unicode_contains,
        test_utf8_array_contains_unicode_contains_dyn,
        vec![
            "sdlkdfFkoßsdfs",
            "sdlkdFFkoSSdggs", // Original was case insensitive "sdlkdfFkoSSdggs"
            "sdlkdFFkoSSsdsd", // Original was case insensitive "sdlkdfFkosssdsd"
            "FkoS",
            "Fkos",
            "ﬀkoSS",
            "ﬀkoß",
            "😃sadlksFFkoSSsh😃klF", // Original was case insensitive "😃sadlksffkosSsh😃klF"
            "😱slgFFkoSSsh😃klF",    // Original was case insensitive "😱slgffkosSsh😃klF"
            "FFkoSS",                // "FFKoSS"
        ],
        "FFkoSS",
        contains_utf8_scalar,
        contains_utf8_scalar_dyn,
        vec![false, true, true, false, false, false, false, true, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_ilike_unicode_complex,
        test_utf8_array_ilike_unicode_complex_dyn,
        vec![
            "sdlkdfFooßsdfs",
            "sdlkdfFooSSdggs",
            "sdlkdfFoosssdsd",
            "FooS",
            "Foos",
            "ﬀooSS",
            "ﬀooß",
            "😃sadlksffofsSsh😃klF",
            "😱slgffoesSsh😃klF",
            "FFKoSS",
        ],
        "%FF__SS%",
        ilike_utf8_scalar,
        ilike_utf8_scalar_dyn,
        vec![false, true, true, false, false, false, false, true, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_ilike_scalar_one,
        test_utf8_array_ilike_scalar_dyn_one,
        vec!["arrow", "arrows", "parrow", "arr"],
        "arrow_",
        ilike_utf8_scalar,
        ilike_utf8_scalar_dyn,
        vec![false, true, false, false]
    );

    test_utf8!(
        test_utf8_array_nilike,
        vec!["arrow", "arrow", "ARROW", "arrow", "ARROW", "ARROWS", "arROw"],
        vec!["arrow", "ar%", "%ro%", "foo", "ar%r", "arrow_", "arrow_"],
        nilike_utf8,
        vec![false, false, false, true, true, false, true]
    );

    test_dict_utf8!(
        test_utf8_array_nilike_dict,
        vec!["arrow", "arrow", "ARROW", "arrow", "ARROW", "ARROWS", "arROw"],
        vec!["arrow", "ar%", "%ro%", "foo", "ar%r", "arrow_", "arrow_"],
        nilike_dyn,
        vec![false, false, false, true, true, false, true]
    );

    test_utf8_scalar!(
        nilike_utf8_scalar_escape_testing,
        nilike_utf8_scalar_escape_dyn_testing,
        vec!["varchar(255)", "int(255)", "varchar", "int"],
        "%(%)%",
        nilike_utf8_scalar,
        nilike_utf8_scalar_dyn,
        vec![false, false, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nilike_scalar,
        test_utf8_array_nilike_dyn_scalar,
        vec!["arrow", "parquet", "datafusion", "flight"],
        "%AR%",
        nilike_utf8_scalar,
        nilike_utf8_scalar_dyn,
        vec![false, false, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nilike_scalar_start,
        test_utf8_array_nilike_scalar_dyn_start,
        vec!["arrow", "parrow", "arrows", "ARR"],
        "aRRow%",
        nilike_utf8_scalar,
        nilike_utf8_scalar_dyn,
        vec![false, true, false, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nilike_scalar_end,
        test_utf8_array_nilike_scalar_dyn_end,
        vec!["ArroW", "parrow", "ARRowS", "arr"],
        "%arrow",
        nilike_utf8_scalar,
        nilike_utf8_scalar_dyn,
        vec![false, false, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nilike_scalar_equals,
        test_utf8_array_nilike_scalar_dyn_equals,
        vec!["arRow", "parrow", "arrows", "arr"],
        "Arrow",
        nilike_utf8_scalar,
        nilike_utf8_scalar_dyn,
        vec![false, true, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nilike_scalar_one,
        test_utf8_array_nilike_scalar_dyn_one,
        vec!["arrow", "arrows", "parrow", "arr"],
        "arrow_",
        nilike_utf8_scalar,
        nilike_utf8_scalar_dyn,
        vec![true, false, true, true]
    );

    #[test]
    fn test_dict_like_kernels() {
        let data = vec![
            Some("Earth"),
            Some("Fire"),
            Some("Water"),
            Some("Air"),
            None,
            Some("Air"),
            Some("bbbbb\nAir"),
        ];

        let dict_array: DictionaryArray<Int8Type> = data.into_iter().collect();

        assert_eq!(
            like_utf8_scalar_dyn(&dict_array, "Air").unwrap(),
            BooleanArray::from(vec![
                Some(false),
                Some(false),
                Some(false),
                Some(true),
                None,
                Some(true),
                Some(false),
            ]),
        );

        assert_eq!(
            like_utf8_scalar_dyn(&dict_array, "Air").unwrap(),
            BooleanArray::from(vec![
                Some(false),
                Some(false),
                Some(false),
                Some(true),
                None,
                Some(true),
                Some(false),
            ]),
        );

        assert_eq!(
            like_utf8_scalar_dyn(&dict_array, "Wa%").unwrap(),
            BooleanArray::from(vec![
                Some(false),
                Some(false),
                Some(true),
                Some(false),
                None,
                Some(false),
                Some(false),
            ]),
        );

        assert_eq!(
            like_utf8_scalar_dyn(&dict_array, "Wa%").unwrap(),
            BooleanArray::from(vec![
                Some(false),
                Some(false),
                Some(true),
                Some(false),
                None,
                Some(false),
                Some(false),
            ]),
        );

        assert_eq!(
            like_utf8_scalar_dyn(&dict_array, "%r").unwrap(),
            BooleanArray::from(vec![
                Some(false),
                Some(false),
                Some(true),
                Some(true),
                None,
                Some(true),
                Some(true),
            ]),
        );

        assert_eq!(
            like_utf8_scalar_dyn(&dict_array, "%r").unwrap(),
            BooleanArray::from(vec![
                Some(false),
                Some(false),
                Some(true),
                Some(true),
                None,
                Some(true),
                Some(true),
            ]),
        );

        assert_eq!(
            like_utf8_scalar_dyn(&dict_array, "%i%").unwrap(),
            BooleanArray::from(vec![
                Some(false),
                Some(true),
                Some(false),
                Some(true),
                None,
                Some(true),
                Some(true),
            ]),
        );

        assert_eq!(
            like_utf8_scalar_dyn(&dict_array, "%i%").unwrap(),
            BooleanArray::from(vec![
                Some(false),
                Some(true),
                Some(false),
                Some(true),
                None,
                Some(true),
                Some(true),
            ]),
        );

        assert_eq!(
            like_utf8_scalar_dyn(&dict_array, "%a%r%").unwrap(),
            BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(true),
                Some(false),
                None,
                Some(false),
                Some(false),
            ]),
        );

        assert_eq!(
            like_utf8_scalar_dyn(&dict_array, "%a%r%").unwrap(),
            BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(true),
                Some(false),
                None,
                Some(false),
                Some(false),
            ]),
        );
    }

    #[test]
    fn test_dict_nlike_kernels() {
        let data = vec![
            Some("Earth"),
            Some("Fire"),
            Some("Water"),
            Some("Air"),
            None,
            Some("Air"),
            Some("bbbbb\nAir"),
        ];

        let dict_array: DictionaryArray<Int8Type> = data.into_iter().collect();

        assert_eq!(
            nlike_utf8_scalar_dyn(&dict_array, "Air").unwrap(),
            BooleanArray::from(vec![
                Some(true),
                Some(true),
                Some(true),
                Some(false),
                None,
                Some(false),
                Some(true),
            ]),
        );

        assert_eq!(
            nlike_utf8_scalar_dyn(&dict_array, "Air").unwrap(),
            BooleanArray::from(vec![
                Some(true),
                Some(true),
                Some(true),
                Some(false),
                None,
                Some(false),
                Some(true),
            ]),
        );

        assert_eq!(
            nlike_utf8_scalar_dyn(&dict_array, "Wa%").unwrap(),
            BooleanArray::from(vec![
                Some(true),
                Some(true),
                Some(false),
                Some(true),
                None,
                Some(true),
                Some(true),
            ]),
        );

        assert_eq!(
            nlike_utf8_scalar_dyn(&dict_array, "Wa%").unwrap(),
            BooleanArray::from(vec![
                Some(true),
                Some(true),
                Some(false),
                Some(true),
                None,
                Some(true),
                Some(true),
            ]),
        );

        assert_eq!(
            nlike_utf8_scalar_dyn(&dict_array, "%r").unwrap(),
            BooleanArray::from(vec![
                Some(true),
                Some(true),
                Some(false),
                Some(false),
                None,
                Some(false),
                Some(false),
            ]),
        );

        assert_eq!(
            nlike_utf8_scalar_dyn(&dict_array, "%r").unwrap(),
            BooleanArray::from(vec![
                Some(true),
                Some(true),
                Some(false),
                Some(false),
                None,
                Some(false),
                Some(false),
            ]),
        );

        assert_eq!(
            nlike_utf8_scalar_dyn(&dict_array, "%i%").unwrap(),
            BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(true),
                Some(false),
                None,
                Some(false),
                Some(false),
            ]),
        );

        assert_eq!(
            nlike_utf8_scalar_dyn(&dict_array, "%i%").unwrap(),
            BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(true),
                Some(false),
                None,
                Some(false),
                Some(false),
            ]),
        );

        assert_eq!(
            nlike_utf8_scalar_dyn(&dict_array, "%a%r%").unwrap(),
            BooleanArray::from(vec![
                Some(false),
                Some(true),
                Some(false),
                Some(true),
                None,
                Some(true),
                Some(true),
            ]),
        );

        assert_eq!(
            nlike_utf8_scalar_dyn(&dict_array, "%a%r%").unwrap(),
            BooleanArray::from(vec![
                Some(false),
                Some(true),
                Some(false),
                Some(true),
                None,
                Some(true),
                Some(true),
            ]),
        );
    }

    #[test]
    fn test_dict_ilike_kernels() {
        let data = vec![
            Some("Earth"),
            Some("Fire"),
            Some("Water"),
            Some("Air"),
            None,
            Some("Air"),
            Some("bbbbb\nAir"),
        ];

        let dict_array: DictionaryArray<Int8Type> = data.into_iter().collect();

        assert_eq!(
            ilike_utf8_scalar_dyn(&dict_array, "air").unwrap(),
            BooleanArray::from(vec![
                Some(false),
                Some(false),
                Some(false),
                Some(true),
                None,
                Some(true),
                Some(false),
            ]),
        );

        assert_eq!(
            ilike_utf8_scalar_dyn(&dict_array, "air").unwrap(),
            BooleanArray::from(vec![
                Some(false),
                Some(false),
                Some(false),
                Some(true),
                None,
                Some(true),
                Some(false),
            ]),
        );

        assert_eq!(
            ilike_utf8_scalar_dyn(&dict_array, "wa%").unwrap(),
            BooleanArray::from(vec![
                Some(false),
                Some(false),
                Some(true),
                Some(false),
                None,
                Some(false),
                Some(false),
            ]),
        );

        assert_eq!(
            ilike_utf8_scalar_dyn(&dict_array, "wa%").unwrap(),
            BooleanArray::from(vec![
                Some(false),
                Some(false),
                Some(true),
                Some(false),
                None,
                Some(false),
                Some(false),
            ]),
        );

        assert_eq!(
            ilike_utf8_scalar_dyn(&dict_array, "%R").unwrap(),
            BooleanArray::from(vec![
                Some(false),
                Some(false),
                Some(true),
                Some(true),
                None,
                Some(true),
                Some(true),
            ]),
        );

        assert_eq!(
            ilike_utf8_scalar_dyn(&dict_array, "%R").unwrap(),
            BooleanArray::from(vec![
                Some(false),
                Some(false),
                Some(true),
                Some(true),
                None,
                Some(true),
                Some(true),
            ]),
        );

        assert_eq!(
            ilike_utf8_scalar_dyn(&dict_array, "%I%").unwrap(),
            BooleanArray::from(vec![
                Some(false),
                Some(true),
                Some(false),
                Some(true),
                None,
                Some(true),
                Some(true),
            ]),
        );

        assert_eq!(
            ilike_utf8_scalar_dyn(&dict_array, "%I%").unwrap(),
            BooleanArray::from(vec![
                Some(false),
                Some(true),
                Some(false),
                Some(true),
                None,
                Some(true),
                Some(true),
            ]),
        );

        assert_eq!(
            ilike_utf8_scalar_dyn(&dict_array, "%A%r%").unwrap(),
            BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(true),
                Some(true),
                None,
                Some(true),
                Some(true),
            ]),
        );

        assert_eq!(
            ilike_utf8_scalar_dyn(&dict_array, "%A%r%").unwrap(),
            BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(true),
                Some(true),
                None,
                Some(true),
                Some(true),
            ]),
        );
    }

    #[test]
    fn test_dict_nilike_kernels() {
        let data = vec![
            Some("Earth"),
            Some("Fire"),
            Some("Water"),
            Some("Air"),
            None,
            Some("Air"),
            Some("bbbbb\nAir"),
        ];

        let dict_array: DictionaryArray<Int8Type> = data.into_iter().collect();

        assert_eq!(
            nilike_utf8_scalar_dyn(&dict_array, "air").unwrap(),
            BooleanArray::from(vec![
                Some(true),
                Some(true),
                Some(true),
                Some(false),
                None,
                Some(false),
                Some(true),
            ]),
        );

        assert_eq!(
            nilike_utf8_scalar_dyn(&dict_array, "air").unwrap(),
            BooleanArray::from(vec![
                Some(true),
                Some(true),
                Some(true),
                Some(false),
                None,
                Some(false),
                Some(true),
            ]),
        );

        assert_eq!(
            nilike_utf8_scalar_dyn(&dict_array, "wa%").unwrap(),
            BooleanArray::from(vec![
                Some(true),
                Some(true),
                Some(false),
                Some(true),
                None,
                Some(true),
                Some(true),
            ]),
        );

        assert_eq!(
            nilike_utf8_scalar_dyn(&dict_array, "wa%").unwrap(),
            BooleanArray::from(vec![
                Some(true),
                Some(true),
                Some(false),
                Some(true),
                None,
                Some(true),
                Some(true),
            ]),
        );

        assert_eq!(
            nilike_utf8_scalar_dyn(&dict_array, "%R").unwrap(),
            BooleanArray::from(vec![
                Some(true),
                Some(true),
                Some(false),
                Some(false),
                None,
                Some(false),
                Some(false),
            ]),
        );

        assert_eq!(
            nilike_utf8_scalar_dyn(&dict_array, "%R").unwrap(),
            BooleanArray::from(vec![
                Some(true),
                Some(true),
                Some(false),
                Some(false),
                None,
                Some(false),
                Some(false),
            ]),
        );

        assert_eq!(
            nilike_utf8_scalar_dyn(&dict_array, "%I%").unwrap(),
            BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(true),
                Some(false),
                None,
                Some(false),
                Some(false),
            ]),
        );

        assert_eq!(
            nilike_utf8_scalar_dyn(&dict_array, "%I%").unwrap(),
            BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(true),
                Some(false),
                None,
                Some(false),
                Some(false),
            ]),
        );

        assert_eq!(
            nilike_utf8_scalar_dyn(&dict_array, "%A%r%").unwrap(),
            BooleanArray::from(vec![
                Some(false),
                Some(true),
                Some(false),
                Some(false),
                None,
                Some(false),
                Some(false),
            ]),
        );

        assert_eq!(
            nilike_utf8_scalar_dyn(&dict_array, "%A%r%").unwrap(),
            BooleanArray::from(vec![
                Some(false),
                Some(true),
                Some(false),
                Some(false),
                None,
                Some(false),
                Some(false),
            ]),
        );
    }

    #[test]
    fn like_scalar_null() {
        let a = StringArray::new_scalar("a");
        let b = Scalar::new(StringArray::new_null(1));
        let r = like(&a, &b).unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r.null_count(), 1);
        assert!(r.is_null(0));

        let a = StringArray::from_iter_values(["a"]);
        let b = Scalar::new(StringArray::new_null(1));
        let r = like(&a, &b).unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r.null_count(), 1);
        assert!(r.is_null(0));

        let a = StringArray::from_iter_values(["a"]);
        let b = StringArray::new_null(1);
        let r = like(&a, &b).unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r.null_count(), 1);
        assert!(r.is_null(0));

        let a = StringArray::new_scalar("a");
        let b = StringArray::new_null(1);
        let r = like(&a, &b).unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r.null_count(), 1);
        assert!(r.is_null(0));
    }
}
