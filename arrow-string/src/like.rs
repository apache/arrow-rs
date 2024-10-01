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

//! Provide SQL's LIKE operators for Arrow's string arrays

use crate::predicate::Predicate;
use arrow_array::cast::AsArray;
use arrow_array::*;
use arrow_schema::*;
use arrow_select::take::take;
use iterator::ArrayIter;
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
pub fn starts_with(left: &dyn Datum, right: &dyn Datum) -> Result<BooleanArray, ArrowError> {
    like_op(Op::StartsWith, left, right)
}

/// Perform SQL `ENDSWITH(left, right)`
pub fn ends_with(left: &dyn Datum, right: &dyn Datum) -> Result<BooleanArray, ArrowError> {
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
            apply::<&GenericStringArray<i32>>(op, l.as_string(), l_s, l_v, r.as_string(), r_s, r_v)
        }
        (LargeUtf8, LargeUtf8) => {
            apply::<&GenericStringArray<i64>>(op, l.as_string(), l_s, l_v, r.as_string(), r_s, r_v)
        }
        (Utf8View, Utf8View) => apply::<&StringViewArray>(
            op,
            l.as_string_view(),
            l_s,
            l_v,
            r.as_string_view(),
            r_s,
            r_v,
        ),
        (l_t, r_t) => Err(ArrowError::InvalidArgumentError(format!(
            "Invalid string operation: {l_t} {op} {r_t}"
        ))),
    }
}

/// A trait for Arrow String Arrays, currently three types are supported:
/// - `StringArray`
/// - `LargeStringArray`
/// - `StringViewArray`
///
/// This trait helps to abstract over the different types of string arrays
/// so that we don't need to duplicate the implementation for each type.
pub trait StringArrayType<'a>: ArrayAccessor<Item = &'a str> + Sized {
    /// Returns true if all data within this string array is ASCII
    fn is_ascii(&self) -> bool;
    /// Constructs a new iterator
    fn iter(&self) -> ArrayIter<Self>;
}

impl<'a, O: OffsetSizeTrait> StringArrayType<'a> for &'a GenericStringArray<O> {
    fn is_ascii(&self) -> bool {
        GenericStringArray::<O>::is_ascii(self)
    }

    fn iter(&self) -> ArrayIter<Self> {
        GenericStringArray::<O>::iter(self)
    }
}
impl<'a> StringArrayType<'a> for &'a StringViewArray {
    fn is_ascii(&self) -> bool {
        StringViewArray::is_ascii(self)
    }

    fn iter(&self) -> ArrayIter<Self> {
        StringViewArray::iter(self)
    }
}

fn apply<'a, T: StringArrayType<'a> + 'a>(
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
fn op_scalar<'a, T: StringArrayType<'a>>(
    op: Op,
    l: T,
    l_v: Option<&dyn AnyDictionaryArray>,
    r: &str,
) -> Result<BooleanArray, ArrowError> {
    let r = match op {
        Op::Like(neg) => Predicate::like(r)?.evaluate_array(l, neg),
        Op::ILike(neg) => Predicate::ilike(r, l.is_ascii())?.evaluate_array(l, neg),
        Op::Contains => Predicate::contains(r).evaluate_array(l, false),
        Op::StartsWith => Predicate::StartsWith(r).evaluate_array(l, false),
        Op::EndsWith => Predicate::EndsWith(r).evaluate_array(l, false),
    };

    Ok(match l_v {
        Some(v) => take(&r, v.keys(), None)?.as_boolean().clone(),
        None => r,
    })
}

fn vectored_iter<'a, T: StringArrayType<'a> + 'a>(
    a: T,
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
        Op::Contains => Ok(l.zip(r).map(|(l, r)| Some(str_contains(l?, r?))).collect()),
        Op::StartsWith => Ok(l
            .zip(r)
            .map(|(l, r)| Some(Predicate::StartsWith(r?).evaluate(l?)))
            .collect()),
        Op::EndsWith => Ok(l
            .zip(r)
            .map(|(l, r)| Some(Predicate::EndsWith(r?).evaluate(l?)))
            .collect()),
    }
}

fn str_contains(haystack: &str, needle: &str) -> bool {
    memchr::memmem::find(haystack.as_bytes(), needle.as_bytes()).is_some()
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
        pub fn $fn_scalar_dyn(left: &dyn Array, right: &str) -> Result<BooleanArray, ArrowError> {
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

    /// Applying `op(left, right)`, both sides are arrays
    /// The macro tests four types of array implementations:
    /// - `StringArray`
    /// - `LargeStringArray`
    /// - `StringViewArray`
    /// - `DictionaryArray`
    macro_rules! test_utf8 {
        ($test_name:ident, $left:expr, $right:expr, $op:expr, $expected:expr) => {
            #[test]
            fn $test_name() {
                let expected = BooleanArray::from($expected);

                let left = StringArray::from($left);
                let right = StringArray::from($right);
                let res = $op(&left, &right).unwrap();
                assert_eq!(res, expected);

                let left = LargeStringArray::from($left);
                let right = LargeStringArray::from($right);
                let res = $op(&left, &right).unwrap();
                assert_eq!(res, expected);

                let left = StringViewArray::from($left);
                let right = StringViewArray::from($right);
                let res = $op(&left, &right).unwrap();
                assert_eq!(res, expected);

                let left: DictionaryArray<Int8Type> = $left.into_iter().collect();
                let right: DictionaryArray<Int8Type> = $right.into_iter().collect();
                let res = $op(&left, &right).unwrap();
                assert_eq!(res, expected);
            }
        };
    }

    /// Applying `op(left, right)`, left side is array, right side is scalar
    /// The macro tests four types of array implementations:
    /// - `StringArray`
    /// - `LargeStringArray`
    /// - `StringViewArray`
    /// - `DictionaryArray`
    macro_rules! test_utf8_scalar {
        ($test_name:ident, $left:expr, $right:expr, $op:expr, $expected:expr) => {
            #[test]
            fn $test_name() {
                let expected = BooleanArray::from($expected);

                let left = StringArray::from($left);
                let right = StringArray::from_iter_values([$right]);
                let res = $op(&left, &Scalar::new(&right)).unwrap();
                assert_eq!(res, expected);

                let left = LargeStringArray::from($left);
                let right = LargeStringArray::from_iter_values([$right]);
                let res = $op(&left, &Scalar::new(&right)).unwrap();
                assert_eq!(res, expected);

                let left = StringViewArray::from($left);
                let right = StringViewArray::from_iter_values([$right]);
                let res = $op(&left, &Scalar::new(&right)).unwrap();
                assert_eq!(res, expected);

                let left: DictionaryArray<Int8Type> = $left.into_iter().collect();
                let right: DictionaryArray<Int8Type> = [$right].into_iter().collect();
                let res = $op(&left, &Scalar::new(&right)).unwrap();
                assert_eq!(res, expected);
            }
        };
    }

    test_utf8!(
        test_utf8_array_like,
        vec![
            "arrow",
            "arrow_long_string_more than 12 bytes",
            "arrow",
            "arrow",
            "arrow",
            "arrows",
            "arrow",
            "arrow"
        ],
        vec!["arrow", "ar%", "%ro%", "foo", "arr", "arrow_", "arrow_", ".*"],
        like,
        vec![true, true, true, false, false, true, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_like_scalar_escape_testing,
        vec![
            "varchar(255)",
            "int(255)longer than 12 bytes",
            "varchar",
            "int"
        ],
        "%(%)%",
        like,
        vec![true, true, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_like_scalar_escape_regex,
        vec![".*", "a", "*"],
        ".*",
        like,
        vec![true, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_like_scalar_escape_regex_dot,
        vec![".", "a", "*"],
        ".",
        like,
        vec![true, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_like_scalar,
        vec![
            "arrow",
            "parquet",
            "datafusion",
            "flight",
            "long string arrow test 12 bytes"
        ],
        "%ar%",
        like,
        vec![true, true, false, false, true]
    );

    test_utf8_scalar!(
        test_utf8_array_like_scalar_start,
        vec![
            "arrow",
            "parrow",
            "arrows",
            "arr",
            "arrow long string longer than 12 bytes"
        ],
        "arrow%",
        like,
        vec![true, false, true, false, true]
    );

    // Replicates `test_utf8_array_like_scalar_start` `test_utf8_array_like_scalar_dyn_start` to
    // demonstrate that `SQL STARTSWITH` works as expected.
    test_utf8_scalar!(
        test_utf8_array_starts_with_scalar_start,
        vec![
            "arrow",
            "parrow",
            "arrows",
            "arr",
            "arrow long string longer than 12 bytes"
        ],
        "arrow",
        starts_with,
        vec![true, false, true, false, true]
    );

    test_utf8_scalar!(
        test_utf8_array_like_scalar_end,
        vec![
            "arrow",
            "parrow",
            "arrows",
            "arr",
            "arrow long string longer than 12 bytes"
        ],
        "%arrow",
        like,
        vec![true, true, false, false, false]
    );

    // Replicates `test_utf8_array_like_scalar_end` `test_utf8_array_like_scalar_dyn_end` to
    // demonstrate that `SQL ENDSWITH` works as expected.
    test_utf8_scalar!(
        test_utf8_array_ends_with_scalar_end,
        vec![
            "arrow",
            "parrow",
            "arrows",
            "arr",
            "arrow long string longer than 12 bytes"
        ],
        "arrow",
        ends_with,
        vec![true, true, false, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_like_scalar_equals,
        vec![
            "arrow",
            "parrow",
            "arrows",
            "arr",
            "arrow long string longer than 12 bytes"
        ],
        "arrow",
        like,
        vec![true, false, false, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_like_scalar_one,
        vec![
            "arrow",
            "arrows",
            "parrow",
            "arr",
            "arrow long string longer than 12 bytes"
        ],
        "arrow_",
        like,
        vec![false, true, false, false, false]
    );

    test_utf8_scalar!(
        test_utf8_scalar_like_escape,
        vec!["a%", "a\\x", "arrow long string longer than 12 bytes"],
        "a\\%",
        like,
        vec![true, false, false]
    );

    test_utf8_scalar!(
        test_utf8_scalar_like_escape_contains,
        vec!["ba%", "ba\\x", "arrow long string longer than 12 bytes"],
        "%a\\%",
        like,
        vec![true, false, false]
    );

    test_utf8!(
        test_utf8_scalar_ilike_regex,
        vec!["%%%"],
        vec![r"\%_\%"],
        ilike,
        vec![true]
    );

    test_utf8!(
        test_utf8_array_nlike,
        vec![
            "arrow",
            "arrow",
            "arrow long string longer than 12 bytes",
            "arrow",
            "arrow",
            "arrows",
            "arrow"
        ],
        vec!["arrow", "ar%", "%ro%", "foo", "arr", "arrow_", "arrow_"],
        nlike,
        vec![false, false, false, true, true, false, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nlike_escape_testing,
        vec![
            "varchar(255)",
            "int(255) arrow long string longer than 12 bytes",
            "varchar",
            "int"
        ],
        "%(%)%",
        nlike,
        vec![false, false, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nlike_scalar_escape_regex,
        vec![".*", "a", "*"],
        ".*",
        nlike,
        vec![false, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nlike_scalar_escape_regex_dot,
        vec![".", "a", "*"],
        ".",
        nlike,
        vec![false, true, true]
    );
    test_utf8_scalar!(
        test_utf8_array_nlike_scalar,
        vec![
            "arrow",
            "parquet",
            "datafusion",
            "flight",
            "arrow long string longer than 12 bytes"
        ],
        "%ar%",
        nlike,
        vec![false, false, true, true, false]
    );

    test_utf8_scalar!(
        test_utf8_array_nlike_scalar_start,
        vec![
            "arrow",
            "parrow",
            "arrows",
            "arr",
            "arrow long string longer than 12 bytes"
        ],
        "arrow%",
        nlike,
        vec![false, true, false, true, false]
    );

    test_utf8_scalar!(
        test_utf8_array_nlike_scalar_end,
        vec![
            "arrow",
            "parrow",
            "arrows",
            "arr",
            "arrow long string longer than 12 bytes"
        ],
        "%arrow",
        nlike,
        vec![false, false, true, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nlike_scalar_equals,
        vec![
            "arrow",
            "parrow",
            "arrows",
            "arr",
            "arrow long string longer than 12 bytes"
        ],
        "arrow",
        nlike,
        vec![false, true, true, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nlike_scalar_one,
        vec![
            "arrow",
            "arrows",
            "parrow",
            "arr",
            "arrow long string longer than 12 bytes"
        ],
        "arrow_",
        nlike,
        vec![true, false, true, true, true]
    );

    test_utf8!(
        test_utf8_array_ilike,
        vec![
            "arrow",
            "arrow",
            "ARROW long string longer than 12 bytes",
            "arrow",
            "ARROW",
            "ARROWS",
            "arROw"
        ],
        vec!["arrow", "ar%", "%ro%", "foo", "ar%r", "arrow_", "arrow_"],
        ilike,
        vec![true, true, true, false, false, true, false]
    );

    test_utf8_scalar!(
        ilike_utf8_scalar_escape_testing,
        vec![
            "varchar(255)",
            "int(255) long string longer than 12 bytes",
            "varchar",
            "int"
        ],
        "%(%)%",
        ilike,
        vec![true, true, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_ilike_scalar,
        vec![
            "arrow",
            "parquet",
            "datafusion",
            "flight",
            "arrow long string longer than 12 bytes"
        ],
        "%AR%",
        ilike,
        vec![true, true, false, false, true]
    );

    test_utf8_scalar!(
        test_utf8_array_ilike_scalar_start,
        vec![
            "arrow",
            "parrow",
            "arrows",
            "ARR",
            "arrow long string longer than 12 bytes"
        ],
        "aRRow%",
        ilike,
        vec![true, false, true, false, true]
    );

    test_utf8_scalar!(
        test_utf8_array_ilike_scalar_end,
        vec![
            "ArroW",
            "parrow",
            "ARRowS",
            "arr",
            "arrow long string longer than 12 bytes"
        ],
        "%arrow",
        ilike,
        vec![true, true, false, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_ilike_scalar_equals,
        vec![
            "arrow",
            "parrow",
            "arrows",
            "arr",
            "arrow long string longer than 12 bytes"
        ],
        "Arrow",
        ilike,
        vec![true, false, false, false, false]
    );

    // We only implement loose matching
    test_utf8_scalar!(
        test_utf8_array_ilike_unicode,
        vec![
            "FFkoß",
            "FFkoSS",
            "FFkoss",
            "FFkoS",
            "FFkos",
            "ﬀkoSS",
            "ﬀkoß",
            "FFKoSS",
            "longer than 12 bytes FFKoSS"
        ],
        "FFkoSS",
        ilike,
        vec![false, true, true, false, false, false, false, true, false]
    );

    test_utf8_scalar!(
        test_utf8_array_ilike_unicode_starts,
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
            "longer than 12 bytes FFKoSS",
        ],
        "FFkoSS%",
        ilike,
        vec![false, true, true, false, false, false, false, true, true, false]
    );

    test_utf8_scalar!(
        test_utf8_array_ilike_unicode_ends,
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
            "longer than 12 bytes FFKoSS",
        ],
        "%FFkoSS",
        ilike,
        vec![false, true, true, false, false, false, false, true, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_ilike_unicode_contains,
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
            "longer than 12 bytes FFKoSS",
        ],
        "%FFkoSS%",
        ilike,
        vec![false, true, true, false, false, false, false, true, true, true, true]
    );

    // Replicates `test_utf8_array_ilike_unicode_contains` and
    // `test_utf8_array_ilike_unicode_contains_dyn` to
    // demonstrate that `SQL CONTAINS` works as expected.
    //
    // NOTE: 5 of the values were changed because the original used a case insensitive `ilike`.
    test_utf8_scalar!(
        test_utf8_array_contains_unicode_contains,
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
            "longer than 12 bytes FFKoSS",
        ],
        "FFkoSS",
        contains,
        vec![false, true, true, false, false, false, false, true, true, true, false]
    );

    test_utf8_scalar!(
        test_utf8_array_ilike_unicode_complex,
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
            "longer than 12 bytes FFKoSS",
        ],
        "%FF__SS%",
        ilike,
        vec![false, true, true, false, false, false, false, true, true, true, true]
    );

    // 😈 is four bytes long.
    test_utf8_scalar!(
        test_uff8_array_like_multibyte,
        vec![
            "sdlkdfFooßsdfs",
            "sdlkdfFooSSdggs",
            "sdlkdfFoosssdsd",
            "FooS",
            "Foos",
            "ﬀooSS",
            "ﬀooß",
            "😃sadlksffofsSsh😈klF",
            "😱slgffoesSsh😈klF",
            "FFKoSS",
            "longer than 12 bytes FFKoSS",
        ],
        "%Ssh😈klF",
        like,
        vec![false, false, false, false, false, false, false, true, true, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_ilike_scalar_one,
        vec![
            "arrow",
            "arrows",
            "parrow",
            "arr",
            "arrow long string longer than 12 bytes"
        ],
        "arrow_",
        ilike,
        vec![false, true, false, false, false]
    );

    test_utf8!(
        test_utf8_array_nilike,
        vec![
            "arrow",
            "arrow",
            "ARROW longer than 12 bytes string",
            "arrow",
            "ARROW",
            "ARROWS",
            "arROw"
        ],
        vec!["arrow", "ar%", "%ro%", "foo", "ar%r", "arrow_", "arrow_"],
        nilike,
        vec![false, false, false, true, true, false, true]
    );

    test_utf8_scalar!(
        nilike_utf8_scalar_escape_testing,
        vec![
            "varchar(255)",
            "int(255) longer than 12 bytes string",
            "varchar",
            "int"
        ],
        "%(%)%",
        nilike,
        vec![false, false, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nilike_scalar,
        vec![
            "arrow",
            "parquet",
            "datafusion",
            "flight",
            "arrow long string longer than 12 bytes"
        ],
        "%AR%",
        nilike,
        vec![false, false, true, true, false]
    );

    test_utf8_scalar!(
        test_utf8_array_nilike_scalar_start,
        vec![
            "arrow",
            "parrow",
            "arrows",
            "ARR",
            "arrow long string longer than 12 bytes"
        ],
        "aRRow%",
        nilike,
        vec![false, true, false, true, false]
    );

    test_utf8_scalar!(
        test_utf8_array_nilike_scalar_end,
        vec![
            "ArroW",
            "parrow",
            "ARRowS",
            "arr",
            "arrow long string longer than 12 bytes"
        ],
        "%arrow",
        nilike,
        vec![false, false, true, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nilike_scalar_equals,
        vec![
            "arRow",
            "parrow",
            "arrows",
            "arr",
            "arrow long string longer than 12 bytes"
        ],
        "Arrow",
        nilike,
        vec![false, true, true, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nilike_scalar_one,
        vec![
            "arrow",
            "arrows",
            "parrow",
            "arr",
            "arrow long string longer than 12 bytes"
        ],
        "arrow_",
        nilike,
        vec![true, false, true, true, true]
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
